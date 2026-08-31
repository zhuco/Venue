use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::egui;
use venue_control_protocol::accounts::SecretValue;
use venue_control_protocol::{
    COMMAND_PATH, CommandReceipt, ControlCommandRequest, ControlEvent, ControlSnapshot,
    EVENT_STREAM_PATH, SNAPSHOT_PATH,
};

#[cfg(not(target_arch = "wasm32"))]
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const RECONNECT_INITIAL: std::time::Duration = std::time::Duration::from_millis(250);
const RECONNECT_MAX: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_SSE_BUFFER_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_SSE_FRAME_BYTES: usize = 1_024 * 1_024;

#[derive(Clone, Debug)]
pub enum ClientEvent {
    SessionExpired,
    SnapshotConnected,
    SnapshotUnavailable(String),
    StreamConnected { resumed_after: Option<i64> },
    StreamUnavailable(String),
    CommandUnavailable(String),
    EventCursor(i64),
    Snapshot(ControlSnapshot),
    Receipt(CommandReceipt),
    Notice(String),
}

pub struct ControlClient {
    events: Receiver<ClientEvent>,
    command_tx: Sender<ControlCommandRequest>,
    #[cfg(not(target_arch = "wasm32"))]
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(target_arch = "wasm32")]
    _web: WebClient,
}

impl ControlClient {
    pub fn connect(endpoint: String, context: egui::Context) -> Self {
        Self::connect_authenticated(endpoint, context, None)
    }

    pub fn connect_authenticated(
        endpoint: String,
        context: egui::Context,
        token: Option<SecretValue>,
    ) -> Self {
        let (event_tx, events) = unbounded();
        let (command_tx, command_rx) = unbounded();

        #[cfg(not(target_arch = "wasm32"))]
        let stop = {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            start_native(endpoint, event_tx, command_rx, context, stop.clone(), token);
            stop
        };

        #[cfg(target_arch = "wasm32")]
        let web = WebClient::start(endpoint, event_tx, command_rx, context, token);

        Self {
            events,
            command_tx,
            #[cfg(not(target_arch = "wasm32"))]
            stop,
            #[cfg(target_arch = "wasm32")]
            _web: web,
        }
    }

    pub fn drain(&self) -> impl Iterator<Item = ClientEvent> + '_ {
        self.events.try_iter()
    }

    pub fn send(&self, command: ControlCommandRequest) -> Result<(), ClientError> {
        command.validate().map_err(ClientError::Protocol)?;
        self.command_tx
            .send(command)
            .map_err(|_| ClientError::Closed)
    }
}

impl Drop for ControlClient {
    fn drop(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.stop.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("control command does not satisfy the protocol: {0}")]
    Protocol(venue_control_protocol::ProtocolError),
    #[error("control client is closed")]
    Closed,
}

fn path(endpoint: &str, route: &str) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if endpoint.is_empty() {
        route.to_owned()
    } else {
        format!("{endpoint}{route}")
    }
}

fn publish(sender: &Sender<ClientEvent>, context: &egui::Context, event: ClientEvent) {
    if sender.send(event).is_ok() {
        context.request_repaint();
    }
}

fn publish_control_event(
    sender: &Sender<ClientEvent>,
    context: &egui::Context,
    event: ControlEvent,
) {
    if let Err(error) = event.validate() {
        publish(
            sender,
            context,
            ClientEvent::Notice(format!("ignored invalid Control v2 event: {error}")),
        );
        return;
    }
    match event {
        ControlEvent::Snapshot(snapshot) => {
            publish(sender, context, ClientEvent::Snapshot(snapshot));
        }
        ControlEvent::CommandReceipt(receipt) => {
            publish(sender, context, ClientEvent::Receipt(receipt));
        }
        ControlEvent::Notice { message, .. } => {
            publish(sender, context, ClientEvent::Notice(message));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn start_native(
    endpoint: String,
    sender: Sender<ClientEvent>,
    commands: Receiver<ControlCommandRequest>,
    context: egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    token: Option<SecretValue>,
) {
    let spawn = std::thread::Builder::new()
        .name("venueflow-control-client".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    publish(
                        &sender,
                        &context,
                        ClientEvent::SnapshotUnavailable(format!(
                            "failed to start control runtime: {error}"
                        )),
                    );
                    return;
                }
            };
            runtime.block_on(native_loop(
                endpoint, sender, commands, context, stop, token,
            ));
        });
    if let Err(error) = spawn {
        tracing::error!(%error, "failed to spawn VenueFlow control client");
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_loop(
    endpoint: String,
    sender: Sender<ClientEvent>,
    commands: Receiver<ControlCommandRequest>,
    context: egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    token: Option<SecretValue>,
) {
    if token.is_some() && !crate::account_client::safe_endpoint(&endpoint) {
        publish(&sender, &context, ClientEvent::SessionExpired);
        return;
    }
    let Ok(headers) = crate::account_client::authorization_headers(token.as_ref()) else {
        return;
    };
    let client = match reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .default_headers(headers)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            publish(
                &sender,
                &context,
                ClientEvent::SnapshotUnavailable(format!("failed to build HTTP client: {error}")),
            );
            return;
        }
    };

    fetch_native_snapshot(&client, &endpoint, &sender, &context).await;
    let stream_sender = sender.clone();
    let stream_context = context.clone();
    let stream_client = client.clone();
    let stream_url = path(&endpoint, EVENT_STREAM_PATH);
    let stream_stop = stop.clone();
    tokio::spawn(async move {
        native_event_supervisor(
            stream_client,
            stream_url,
            stream_sender,
            stream_context,
            stream_stop,
        )
        .await;
    });

    let mut next_snapshot = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !stop.load(std::sync::atomic::Ordering::Acquire) {
        for command in commands.try_iter().take(64) {
            let response = client
                .post(path(&endpoint, COMMAND_PATH))
                .json(&command)
                .timeout(REQUEST_TIMEOUT)
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    match response.json::<CommandReceipt>().await {
                        Ok(receipt)
                            if receipt.validate().is_ok()
                                && receipt.request_id == command.request_id =>
                        {
                            publish(&sender, &context, ClientEvent::Receipt(receipt))
                        }
                        Ok(receipt) => publish(
                            &sender,
                            &context,
                            ClientEvent::CommandUnavailable(format!(
                                "invalid or mismatched command receipt for {}",
                                receipt.request_id
                            )),
                        ),
                        Err(error) => publish(
                            &sender,
                            &context,
                            ClientEvent::CommandUnavailable(format!(
                                "invalid command receipt: {error}"
                            )),
                        ),
                    }
                }
                Ok(response) => publish(
                    &sender,
                    &context,
                    ClientEvent::CommandUnavailable(format!(
                        "control command returned HTTP {}",
                        response.status()
                    )),
                ),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::CommandUnavailable(format!("control command failed: {error}")),
                ),
            }
        }

        if tokio::time::Instant::now() >= next_snapshot {
            fetch_native_snapshot(&client, &endpoint, &sender, &context).await;
            next_snapshot = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn fetch_native_snapshot(
    client: &reqwest::Client,
    endpoint: &str,
    sender: &Sender<ClientEvent>,
    context: &egui::Context,
) {
    match client
        .get(path(endpoint, SNAPSHOT_PATH))
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            match response.json::<ControlSnapshot>().await {
                Ok(snapshot) if snapshot.validate().is_ok() => {
                    publish(sender, context, ClientEvent::SnapshotConnected);
                    publish(sender, context, ClientEvent::Snapshot(snapshot));
                }
                Ok(_) => publish(
                    sender,
                    context,
                    ClientEvent::SnapshotUnavailable("snapshot validation failed".to_owned()),
                ),
                Err(error) => publish(
                    sender,
                    context,
                    ClientEvent::SnapshotUnavailable(format!("invalid snapshot: {error}")),
                ),
            }
        }
        Ok(response) if response.status().as_u16() == 401 => {
            publish(sender, context, ClientEvent::SessionExpired)
        }
        Ok(response) => publish(
            sender,
            context,
            ClientEvent::SnapshotUnavailable(format!(
                "snapshot returned HTTP {}",
                response.status()
            )),
        ),
        Err(error) => publish(
            sender,
            context,
            ClientEvent::SnapshotUnavailable(format!("snapshot unavailable: {error}")),
        ),
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_event_supervisor(
    client: reqwest::Client,
    url: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;

    let mut cursor = None;
    let mut backoff = ReconnectBackoff::default();
    while !stop.load(Ordering::Acquire) {
        match native_event_stream(&client, &url, cursor, &sender, &context, &stop).await {
            Ok(outcome) => {
                if outcome.cursor.is_some() {
                    cursor = outcome.cursor;
                }
                if outcome.made_progress {
                    backoff.reset();
                }
                if !stop.load(Ordering::Acquire) {
                    publish(
                        &sender,
                        &context,
                        ClientEvent::StreamUnavailable(
                            "event stream closed; reconnecting from its last event ID".to_owned(),
                        ),
                    );
                }
            }
            Err(error) if !stop.load(Ordering::Acquire) => publish(
                &sender,
                &context,
                ClientEvent::StreamUnavailable(format!("event stream unavailable: {error}")),
            ),
            Err(_) => return,
        }
        if wait_native_stop(&stop, backoff.next_delay()).await {
            return;
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn wait_native_stop(
    stop: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    duration: std::time::Duration,
) -> bool {
    use std::sync::atomic::Ordering;

    let deadline = tokio::time::Instant::now() + duration;
    loop {
        if stop.load(Ordering::Acquire) {
            return true;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(50))).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct StreamOutcome {
    cursor: Option<EventCursor>,
    made_progress: bool,
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_event_stream(
    client: &reqwest::Client,
    url: &str,
    cursor: Option<EventCursor>,
    sender: &Sender<ClientEvent>,
    context: &egui::Context,
    stop: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<StreamOutcome, String> {
    use futures_util::StreamExt as _;
    use std::sync::atomic::Ordering;

    let mut request = client
        .get(url)
        .header(reqwest::header::ACCEPT, "text/event-stream");
    if let Some(cursor) = cursor {
        request = request.header("Last-Event-ID", cursor.to_string());
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    publish(
        sender,
        context,
        ClientEvent::StreamConnected {
            resumed_after: cursor.map(EventCursor::value),
        },
    );
    let mut bytes = response.bytes_stream();
    let mut decoder = SseDecoder::default();
    let mut latest_cursor = cursor;
    let mut made_progress = false;
    while !stop.load(Ordering::Acquire) {
        let next =
            match tokio::time::timeout(std::time::Duration::from_millis(100), bytes.next()).await {
                Ok(next) => next,
                Err(_) => continue,
            };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|error| error.to_string())?;
        for parsed in decoder.push(&chunk)? {
            if let (Some(previous), Some(next)) = (latest_cursor, parsed.cursor) {
                if next < previous {
                    return Err("event stream cursor moved backwards".to_owned());
                }
                if next == previous {
                    continue;
                }
            }
            let control_event = match parsed.payload {
                Some(payload) => {
                    let event = serde_json::from_str::<ControlEvent>(&payload)
                        .map_err(|error| format!("invalid event frame: {error}"))?;
                    event
                        .validate()
                        .map_err(|error| format!("invalid Control v2 event: {error}"))?;
                    Some(event)
                }
                None => None,
            };
            if let Some(cursor) = parsed.cursor {
                latest_cursor = Some(cursor);
                made_progress = true;
                publish(sender, context, ClientEvent::EventCursor(cursor.value()));
            }
            if let Some(event) = control_event {
                publish_control_event(sender, context, event);
            }
        }
    }
    Ok(StreamOutcome {
        cursor: latest_cursor,
        made_progress,
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EventCursor(i64);

impl EventCursor {
    fn parse(value: &str) -> Result<Self, String> {
        let value = value
            .parse::<i64>()
            .map_err(|_| "event ID is not an integer".to_owned())?;
        if value < 0 {
            return Err("event ID is negative".to_owned());
        }
        Ok(Self(value))
    }

    const fn value(self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for EventCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedSseFrame {
    cursor: Option<EventCursor>,
    payload: Option<String>,
}

#[derive(Debug)]
struct ReconnectBackoff {
    next: std::time::Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            next: RECONNECT_INITIAL,
        }
    }
}

impl ReconnectBackoff {
    fn next_delay(&mut self) -> std::time::Duration {
        let delay = self.next;
        self.next = (self.next * 2).min(RECONNECT_MAX);
        delay
    }

    fn reset(&mut self) {
        self.next = RECONNECT_INITIAL;
    }
}

#[derive(Debug, Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<ParsedSseFrame>, String> {
        if self.buffer.len().saturating_add(chunk.len()) > MAX_SSE_BUFFER_BYTES {
            return Err("event stream exceeded the bounded receive buffer".to_owned());
        }
        self.buffer.extend_from_slice(chunk);
        let mut frames = Vec::new();
        while let Some((boundary, delimiter)) = sse_boundary(&self.buffer) {
            if boundary > MAX_SSE_FRAME_BYTES {
                return Err("event stream frame exceeded the size limit".to_owned());
            }
            let frame = std::str::from_utf8(&self.buffer[..boundary])
                .map_err(|_| "event stream frame was not UTF-8".to_owned())?;
            frames.push(parse_sse_frame(frame)?);
            self.buffer.drain(..boundary + delimiter);
        }
        Ok(frames)
    }
}

fn sse_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf <= crlf => Some((lf, 2)),
        (Some(_), Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

fn parse_sse_frame(frame: &str) -> Result<ParsedSseFrame, String> {
    let mut cursor = None;
    let mut data = Vec::new();
    for line in frame.lines() {
        if let Some(value) = line.strip_prefix("id:") {
            let value = value.trim_start();
            if !value.is_empty() {
                cursor = Some(EventCursor::parse(value)?);
            }
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start());
        }
    }
    Ok(ParsedSseFrame {
        cursor,
        payload: (!data.is_empty()).then(|| data.join("\n")),
    })
}

#[cfg(target_arch = "wasm32")]
struct WebClient {
    stop: std::rc::Rc<std::cell::Cell<bool>>,
}

#[cfg(target_arch = "wasm32")]
impl WebClient {
    fn start(
        endpoint: String,
        sender: Sender<ClientEvent>,
        commands: Receiver<ControlCommandRequest>,
        context: egui::Context,
        token: Option<SecretValue>,
    ) -> Self {
        let stop = std::rc::Rc::new(std::cell::Cell::new(false));
        if token.is_some() && !crate::account_client::safe_endpoint(&endpoint) {
            publish(&sender, &context, ClientEvent::SessionExpired);
            return Self { stop };
        }
        if let Some(token) = token.clone() {
            spawn_web_authenticated_events(
                endpoint.clone(),
                sender.clone(),
                context.clone(),
                stop.clone(),
                token,
            );
        } else {
            spawn_web_events(
                endpoint.clone(),
                sender.clone(),
                context.clone(),
                stop.clone(),
            );
        }
        spawn_web_snapshot(
            endpoint.clone(),
            sender.clone(),
            context.clone(),
            stop.clone(),
            token.clone(),
        );
        spawn_web_commands(endpoint, sender, commands, context, stop.clone(), token);
        Self { stop }
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for WebClient {
    fn drop(&mut self) {
        self.stop.set(true);
    }
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_events(
    endpoint: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
) {
    use std::{cell::Cell, rc::Rc};
    use wasm_bindgen::{JsCast as _, closure::Closure};

    wasm_bindgen_futures::spawn_local(async move {
        let mut cursor = None;
        let mut backoff = ReconnectBackoff::default();
        while !stop.get() {
            let url = event_stream_url(&endpoint, cursor);
            let source = match web_sys::EventSource::new(&url) {
                Ok(source) => source,
                Err(_) => {
                    publish(
                        &sender,
                        &context,
                        ClientEvent::StreamUnavailable(
                            "browser could not open the event stream; snapshot polling remains active"
                                .to_owned(),
                        ),
                    );
                    wasm_timer(duration_ms(backoff.next_delay())).await;
                    continue;
                }
            };
            let failed = Rc::new(Cell::new(false));
            let latest_cursor = Rc::new(Cell::new(cursor));

            let open_sender = sender.clone();
            let open_context = context.clone();
            let resumed_after = cursor.map(EventCursor::value);
            let open = Closure::wrap(Box::new(move |_event: web_sys::Event| {
                publish(
                    &open_sender,
                    &open_context,
                    ClientEvent::StreamConnected { resumed_after },
                );
            }) as Box<dyn FnMut(_)>);
            source.set_onopen(Some(open.as_ref().unchecked_ref()));

            let message_sender = sender.clone();
            let message_context = context.clone();
            let message_failed = failed.clone();
            let message_cursor = latest_cursor.clone();
            let message = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                let event_id = event.last_event_id();
                let parsed_cursor = match EventCursor::parse(&event_id) {
                    Ok(cursor) => cursor,
                    Err(error) => {
                        message_failed.set(true);
                        publish(
                            &message_sender,
                            &message_context,
                            ClientEvent::StreamUnavailable(format!(
                                "ignored Control v2 event with invalid last-event-id: {error}"
                            )),
                        );
                        return;
                    }
                };
                if message_cursor
                    .get()
                    .is_some_and(|cursor| parsed_cursor <= cursor)
                {
                    return;
                }
                let Some(payload) = event.data().as_string() else {
                    message_failed.set(true);
                    publish(
                        &message_sender,
                        &message_context,
                        ClientEvent::StreamUnavailable(
                            "ignored non-text Control v2 event".to_owned(),
                        ),
                    );
                    return;
                };
                let control_event = match serde_json::from_str::<ControlEvent>(&payload) {
                    Ok(control_event) if control_event.validate().is_ok() => control_event,
                    Ok(_) => {
                        message_failed.set(true);
                        publish(
                            &message_sender,
                            &message_context,
                            ClientEvent::StreamUnavailable(
                                "ignored invalid Control v2 event".to_owned(),
                            ),
                        );
                        return;
                    }
                    Err(error) => {
                        message_failed.set(true);
                        publish(
                            &message_sender,
                            &message_context,
                            ClientEvent::StreamUnavailable(format!(
                                "ignored invalid event frame: {error}"
                            )),
                        );
                        return;
                    }
                };
                message_cursor.set(Some(parsed_cursor));
                publish(
                    &message_sender,
                    &message_context,
                    ClientEvent::EventCursor(parsed_cursor.value()),
                );
                publish_control_event(&message_sender, &message_context, control_event);
            }) as Box<dyn FnMut(_)>);
            if source
                .add_event_listener_with_callback("control", message.as_ref().unchecked_ref())
                .is_err()
            {
                failed.set(true);
            }

            let error_sender = sender.clone();
            let error_context = context.clone();
            let error_failed = failed.clone();
            let error = Closure::wrap(Box::new(move |_event: web_sys::Event| {
                error_failed.set(true);
                publish(
                    &error_sender,
                    &error_context,
                    ClientEvent::StreamUnavailable(
                        "event stream disconnected; reconnecting from its last event ID".to_owned(),
                    ),
                );
            }) as Box<dyn FnMut(_)>);
            source.set_onerror(Some(error.as_ref().unchecked_ref()));

            while !stop.get() && !failed.get() {
                wasm_timer(100).await;
            }
            source.close();
            let next_cursor = latest_cursor.get();
            if next_cursor != cursor {
                cursor = next_cursor;
                backoff.reset();
            }
            drop((open, message, error));
            if !stop.get() {
                wasm_timer(duration_ms(backoff.next_delay())).await;
            }
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_authenticated_events(
    endpoint: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    token: SecretValue,
) {
    use futures_util::StreamExt;
    wasm_bindgen_futures::spawn_local(async move {
        let Ok(headers) = crate::account_client::authorization_headers(Some(&token)) else {
            return;
        };
        let client = reqwest::Client::new();
        let mut cursor = None;
        let mut backoff = ReconnectBackoff::default();
        while !stop.get() {
            let response = client
                .get(event_stream_url(&endpoint, cursor))
                .headers(headers.clone())
                .send()
                .await;
            if stop.get() {
                return;
            }
            match response {
                Ok(response) if response.status().is_success() => {
                    publish(
                        &sender,
                        &context,
                        ClientEvent::StreamConnected {
                            resumed_after: cursor.map(EventCursor::value),
                        },
                    );
                    let mut stream = response.bytes_stream();
                    let mut decoder = SseDecoder::default();
                    'stream: while let Some(chunk) = stream.next().await {
                        if stop.get() {
                            return;
                        }
                        let Ok(chunk) = chunk else {
                            break;
                        };
                        let Ok(frames) = decoder.push(&chunk) else {
                            break;
                        };
                        for frame in frames {
                            if let Some(next) = frame.cursor {
                                if cursor.is_some_and(|previous| next < previous) {
                                    break 'stream;
                                }
                                if cursor == Some(next) {
                                    continue;
                                }
                            }
                            if let Some(payload) = frame.payload {
                                let Ok(event) = serde_json::from_str::<ControlEvent>(&payload)
                                else {
                                    break 'stream;
                                };
                                if event.validate().is_err() {
                                    break 'stream;
                                }
                                publish_control_event(&sender, &context, event);
                            }
                            if let Some(next) = frame.cursor {
                                cursor = Some(next);
                                backoff.reset();
                                publish(&sender, &context, ClientEvent::EventCursor(next.value()));
                            }
                        }
                    }
                }
                Ok(response) if response.status().as_u16() == 401 => {
                    publish(&sender, &context, ClientEvent::SessionExpired);
                    return;
                }
                _ => (),
            }
            publish(
                &sender,
                &context,
                ClientEvent::StreamUnavailable("authenticated event stream reconnecting".into()),
            );
            wasm_timer(duration_ms(backoff.next_delay())).await;
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn duration_ms(duration: std::time::Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

#[cfg(any(target_arch = "wasm32", test))]
fn event_stream_url(endpoint: &str, cursor: Option<EventCursor>) -> String {
    let url = path(endpoint, EVENT_STREAM_PATH);
    cursor.map_or(url.clone(), |cursor| format!("{url}?after={cursor}"))
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_snapshot(
    endpoint: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    token: Option<SecretValue>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let Ok(headers) = crate::account_client::authorization_headers(token.as_ref()) else {
            return;
        };
        while !stop.get() {
            match reqwest::Client::new()
                .get(path(&endpoint, SNAPSHOT_PATH))
                .headers(headers.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    match response.json::<ControlSnapshot>().await {
                        Ok(snapshot) if snapshot.validate().is_ok() => {
                            publish(&sender, &context, ClientEvent::SnapshotConnected);
                            publish(&sender, &context, ClientEvent::Snapshot(snapshot));
                        }
                        Ok(_) => publish(
                            &sender,
                            &context,
                            ClientEvent::SnapshotUnavailable(
                                "snapshot validation failed".to_owned(),
                            ),
                        ),
                        Err(error) => publish(
                            &sender,
                            &context,
                            ClientEvent::SnapshotUnavailable(format!("invalid snapshot: {error}")),
                        ),
                    }
                }
                Ok(response) if response.status().as_u16() == 401 => {
                    publish(&sender, &context, ClientEvent::SessionExpired);
                    return;
                }
                Ok(response) => publish(
                    &sender,
                    &context,
                    ClientEvent::SnapshotUnavailable(format!(
                        "snapshot returned HTTP {}",
                        response.status()
                    )),
                ),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::SnapshotUnavailable(format!("snapshot unavailable: {error}")),
                ),
            }
            wasm_timer(3_000).await;
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_commands(
    endpoint: String,
    sender: Sender<ClientEvent>,
    commands: Receiver<ControlCommandRequest>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    token: Option<SecretValue>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let Ok(headers) = crate::account_client::authorization_headers(token.as_ref()) else {
            return;
        };
        while !stop.get() {
            for command in commands.try_iter().take(64) {
                match reqwest::Client::new()
                    .post(path(&endpoint, COMMAND_PATH))
                    .headers(headers.clone())
                    .json(&command)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {
                        match response.json::<CommandReceipt>().await {
                            Ok(receipt)
                                if receipt.validate().is_ok()
                                    && receipt.request_id == command.request_id =>
                            {
                                publish(&sender, &context, ClientEvent::Receipt(receipt));
                            }
                            Ok(receipt) => publish(
                                &sender,
                                &context,
                                ClientEvent::CommandUnavailable(format!(
                                    "invalid or mismatched command receipt for {}",
                                    receipt.request_id
                                )),
                            ),
                            Err(error) => publish(
                                &sender,
                                &context,
                                ClientEvent::CommandUnavailable(format!(
                                    "invalid command receipt: {error}"
                                )),
                            ),
                        }
                    }
                    Ok(response) => publish(
                        &sender,
                        &context,
                        ClientEvent::CommandUnavailable(format!(
                            "control command returned HTTP {}",
                            response.status()
                        )),
                    ),
                    Err(error) => publish(
                        &sender,
                        &context,
                        ClientEvent::CommandUnavailable(format!("control command failed: {error}")),
                    ),
                }
            }
            wasm_timer(100).await;
        }
    });
}

#[cfg(target_arch = "wasm32")]
async fn wasm_timer(milliseconds: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            let _ = window
                .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, milliseconds);
        } else {
            let _ = resolve.call0(&wasm_bindgen::JsValue::NULL);
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{
        EventCursor, MAX_SSE_BUFFER_BYTES, RECONNECT_INITIAL, RECONNECT_MAX, ReconnectBackoff,
        SseDecoder, event_stream_url, parse_sse_frame, path, sse_boundary,
    };

    #[test]
    fn api_paths_preserve_the_control_v2_route() {
        assert_eq!(
            path("http://control:39180/", "/v2/ui/snapshot"),
            "http://control:39180/v2/ui/snapshot"
        );
        assert_eq!(path("", "/v2/ui/events"), "/v2/ui/events");
        assert_eq!(
            event_stream_url("http://control:39180", Some(EventCursor(42))),
            "http://control:39180/v2/ui/events?after=42"
        );
    }

    #[test]
    fn sse_frame_retains_cursor_for_reconnect_and_joins_multiline_data() -> Result<(), String> {
        let frame =
            parse_sse_frame("id: 42\nevent: control\ndata: {\"type\":\ndata: \"snapshot\"}")?;
        assert_eq!(frame.cursor, Some(EventCursor(42)));
        assert_eq!(frame.payload.as_deref(), Some("{\"type\":\n\"snapshot\"}"));
        Ok(())
    }

    #[test]
    fn sse_frame_without_data_can_advance_a_cursor() -> Result<(), String> {
        let frame = parse_sse_frame("id: 43\n: heartbeat")?;
        assert_eq!(frame.cursor, Some(EventCursor(43)));
        assert_eq!(frame.payload, None);
        assert_eq!(sse_boundary(b"id: x\r\n\r\nnext"), Some((5, 4)));
        assert_eq!(sse_boundary(b"id: x\n\nnext"), Some((5, 2)));
        Ok(())
    }

    #[test]
    fn invalid_or_negative_last_event_id_is_rejected() {
        assert!(parse_sse_frame("id: cursor-43\ndata: {}").is_err());
        assert!(parse_sse_frame("id: -1\ndata: {}").is_err());
    }

    #[test]
    fn fragmented_sse_is_decoded_without_loss() -> Result<(), String> {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"id: 44\r\ndata: {\"type\":")?.is_empty());
        let frames = decoder.push(b"\"notice\"}\r\n\r\n")?;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].cursor, Some(EventCursor(44)));
        assert_eq!(frames[0].payload.as_deref(), Some("{\"type\":\"notice\"}"));
        Ok(())
    }

    #[test]
    fn sse_receive_buffer_fails_closed_at_its_bound() {
        let mut decoder = SseDecoder::default();
        let oversized = vec![b'x'; MAX_SSE_BUFFER_BYTES + 1];
        assert!(decoder.push(&oversized).is_err());
    }

    #[test]
    fn reconnect_backoff_is_capped_and_resets_after_progress() {
        let mut backoff = ReconnectBackoff::default();
        assert_eq!(backoff.next_delay(), RECONNECT_INITIAL);
        let mut last = RECONNECT_INITIAL;
        for _ in 0..16 {
            last = backoff.next_delay();
        }
        assert_eq!(last, RECONNECT_MAX);
        backoff.reset();
        assert_eq!(backoff.next_delay(), RECONNECT_INITIAL);
    }
}
