use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::egui;
use venue_control_protocol::{
    COMMAND_PATH, CommandReceipt, ControlCommandRequest, ControlEvent, ControlSnapshot,
    EVENT_STREAM_PATH, SNAPSHOT_PATH,
};

#[derive(Clone, Debug)]
pub enum ClientEvent {
    SnapshotConnected,
    StreamConnected,
    Disconnected(String),
    Snapshot(ControlSnapshot),
    Receipt(CommandReceipt),
    Notice(String),
}

pub struct ControlClient {
    events: Receiver<ClientEvent>,
    command_tx: Sender<ControlCommandRequest>,
    #[cfg(target_arch = "wasm32")]
    _web: WebClient,
}

impl ControlClient {
    pub fn connect(endpoint: String, context: egui::Context) -> Self {
        let (event_tx, events) = unbounded();
        let (command_tx, command_rx) = unbounded();

        #[cfg(not(target_arch = "wasm32"))]
        start_native(endpoint, event_tx, command_rx, context);

        #[cfg(target_arch = "wasm32")]
        let web = WebClient::start(endpoint, event_tx, command_rx, context);

        Self {
            events,
            command_tx,
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
            if snapshot.validate().is_ok() {
                publish(sender, context, ClientEvent::Snapshot(snapshot));
            } else {
                publish(
                    sender,
                    context,
                    ClientEvent::Disconnected(
                        "Control API returned an invalid snapshot".to_owned(),
                    ),
                );
            }
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
                        ClientEvent::Disconnected(format!(
                            "failed to start control runtime: {error}"
                        )),
                    );
                    return;
                }
            };
            runtime.block_on(native_loop(endpoint, sender, commands, context));
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
) {
    let client = match reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            publish(
                &sender,
                &context,
                ClientEvent::Disconnected(format!("failed to build HTTP client: {error}")),
            );
            return;
        }
    };

    fetch_native_snapshot(&client, &endpoint, &sender, &context).await;
    let stream_sender = sender.clone();
    let stream_context = context.clone();
    let stream_client = client.clone();
    let stream_url = path(&endpoint, EVENT_STREAM_PATH);
    tokio::spawn(async move {
        native_event_supervisor(stream_client, stream_url, stream_sender, stream_context).await;
    });

    let mut next_snapshot = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        for command in commands.try_iter().take(64) {
            let response = client
                .post(path(&endpoint, COMMAND_PATH))
                .json(&command)
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
                            ClientEvent::Disconnected(format!(
                                "invalid or mismatched command receipt for {}",
                                receipt.request_id
                            )),
                        ),
                        Err(error) => publish(
                            &sender,
                            &context,
                            ClientEvent::Disconnected(format!("invalid command receipt: {error}")),
                        ),
                    }
                }
                Ok(response) => publish(
                    &sender,
                    &context,
                    ClientEvent::Disconnected(format!(
                        "control command returned HTTP {}",
                        response.status()
                    )),
                ),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::Disconnected(format!("control command failed: {error}")),
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
    match client.get(path(endpoint, SNAPSHOT_PATH)).send().await {
        Ok(response) if response.status().is_success() => {
            match response.json::<ControlSnapshot>().await {
                Ok(snapshot) if snapshot.validate().is_ok() => {
                    publish(sender, context, ClientEvent::SnapshotConnected);
                    publish(sender, context, ClientEvent::Snapshot(snapshot));
                }
                Ok(_) => publish(
                    sender,
                    context,
                    ClientEvent::Disconnected("snapshot validation failed".to_owned()),
                ),
                Err(error) => publish(
                    sender,
                    context,
                    ClientEvent::Disconnected(format!("invalid snapshot: {error}")),
                ),
            }
        }
        Ok(response) => publish(
            sender,
            context,
            ClientEvent::Disconnected(format!("snapshot returned HTTP {}", response.status())),
        ),
        Err(error) => publish(
            sender,
            context,
            ClientEvent::Disconnected(format!("snapshot unavailable: {error}")),
        ),
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_event_supervisor(
    client: reqwest::Client,
    url: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
) {
    let mut cursor = None;
    let mut backoff = std::time::Duration::from_millis(250);
    loop {
        match native_event_stream(&client, &url, cursor.as_deref(), &sender, &context).await {
            Ok(next_cursor) => {
                if next_cursor.is_some() {
                    cursor = next_cursor;
                }
                publish(
                    &sender,
                    &context,
                    ClientEvent::Disconnected(
                        "event stream closed; reconnecting with its last cursor".to_owned(),
                    ),
                );
            }
            Err(error) => publish(
                &sender,
                &context,
                ClientEvent::Disconnected(format!("event stream unavailable: {error}")),
            ),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_event_stream(
    client: &reqwest::Client,
    url: &str,
    cursor: Option<&str>,
    sender: &Sender<ClientEvent>,
    context: &egui::Context,
) -> Result<Option<String>, String> {
    use futures_util::StreamExt as _;

    let mut request = client
        .get(url)
        .header(reqwest::header::ACCEPT, "text/event-stream");
    if let Some(cursor) = cursor.filter(|value| !value.trim().is_empty()) {
        request = request.header("Last-Event-ID", cursor);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    publish(sender, context, ClientEvent::StreamConnected);
    let mut bytes = response.bytes_stream();
    let mut buffer = String::new();
    let mut latest_cursor = cursor.map(str::to_owned);
    while let Some(chunk) = bytes.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(boundary) = sse_boundary(&buffer) {
            let frame = buffer[..boundary].to_owned();
            let delimiter = if buffer[boundary..].starts_with("\r\n\r\n") {
                4
            } else {
                2
            };
            buffer.drain(..boundary + delimiter);
            let parsed = parse_sse_frame(&frame);
            if let Some(cursor) = parsed.cursor {
                latest_cursor = Some(cursor);
            }
            let Some(payload) = parsed.payload else {
                continue;
            };
            match serde_json::from_str::<ControlEvent>(&payload) {
                Ok(event) => publish_control_event(sender, context, event),
                Err(error) => publish(
                    sender,
                    context,
                    ClientEvent::Notice(format!("ignored invalid event frame: {error}")),
                ),
            }
        }
    }
    Ok(latest_cursor)
}

#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[derive(Debug, Eq, PartialEq)]
struct ParsedSseFrame {
    cursor: Option<String>,
    payload: Option<String>,
}

#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
fn sse_boundary(buffer: &str) -> Option<usize> {
    buffer.find("\r\n\r\n").or_else(|| buffer.find("\n\n"))
}

#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
fn parse_sse_frame(frame: &str) -> ParsedSseFrame {
    let mut cursor = None;
    let mut data = Vec::new();
    for line in frame.lines() {
        if let Some(value) = line.strip_prefix("id:") {
            cursor = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start());
        }
    }
    ParsedSseFrame {
        cursor: cursor.filter(|value| !value.is_empty()),
        payload: (!data.is_empty()).then(|| data.join("\n")),
    }
}

#[cfg(target_arch = "wasm32")]
struct WebClient {
    _source: Option<web_sys::EventSource>,
    _open: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::Event)>>,
    _message: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::MessageEvent)>>,
    _error: Option<wasm_bindgen::closure::Closure<dyn FnMut(web_sys::ErrorEvent)>>,
}

#[cfg(target_arch = "wasm32")]
impl WebClient {
    fn start(
        endpoint: String,
        sender: Sender<ClientEvent>,
        commands: Receiver<ControlCommandRequest>,
        context: egui::Context,
    ) -> Self {
        use wasm_bindgen::{JsCast as _, closure::Closure};

        let source = web_sys::EventSource::new(&path(&endpoint, EVENT_STREAM_PATH)).ok();
        let (open, message, error) = if let Some(source_ref) = source.as_ref() {
            let open_sender = sender.clone();
            let open_context = context.clone();
            // EventSource reconnects automatically and sends its retained Last-Event-ID.
            let open = Closure::wrap(Box::new(move |_event: web_sys::Event| {
                publish(&open_sender, &open_context, ClientEvent::StreamConnected);
            }) as Box<dyn FnMut(_)>);
            source_ref.set_onopen(Some(open.as_ref().unchecked_ref()));

            let message_sender = sender.clone();
            let message_context = context.clone();
            let message = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                let Some(payload) = event.data().as_string() else {
                    return;
                };
                if let Ok(event) = serde_json::from_str::<ControlEvent>(&payload) {
                    publish_control_event(&message_sender, &message_context, event);
                }
            }) as Box<dyn FnMut(_)>);
            source_ref.set_onmessage(Some(message.as_ref().unchecked_ref()));

            let error_sender = sender.clone();
            let error_context = context.clone();
            let error = Closure::wrap(Box::new(move |_event: web_sys::ErrorEvent| {
                publish(
                    &error_sender,
                    &error_context,
                    ClientEvent::Disconnected(
                        "event stream disconnected; snapshot polling remains active".to_owned(),
                    ),
                );
            }) as Box<dyn FnMut(_)>);
            source_ref.set_onerror(Some(error.as_ref().unchecked_ref()));
            (Some(open), Some(message), Some(error))
        } else {
            publish(
                &sender,
                &context,
                ClientEvent::Notice(
                    "browser could not open the event stream; snapshot polling remains active"
                        .to_owned(),
                ),
            );
            (None, None, None)
        };

        spawn_web_snapshot(endpoint.clone(), sender.clone(), context.clone());
        spawn_web_commands(endpoint, sender, commands, context);
        Self {
            _source: source,
            _open: open,
            _message: message,
            _error: error,
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_snapshot(endpoint: String, sender: Sender<ClientEvent>, context: egui::Context) {
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            match reqwest::Client::new()
                .get(path(&endpoint, SNAPSHOT_PATH))
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
                            ClientEvent::Disconnected("snapshot validation failed".to_owned()),
                        ),
                        Err(error) => publish(
                            &sender,
                            &context,
                            ClientEvent::Disconnected(format!("invalid snapshot: {error}")),
                        ),
                    }
                }
                Ok(response) => publish(
                    &sender,
                    &context,
                    ClientEvent::Disconnected(format!(
                        "snapshot returned HTTP {}",
                        response.status()
                    )),
                ),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::Disconnected(format!("snapshot unavailable: {error}")),
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
) {
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            for command in commands.try_iter().take(64) {
                match reqwest::Client::new()
                    .post(path(&endpoint, COMMAND_PATH))
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
                                ClientEvent::Disconnected(format!(
                                    "invalid or mismatched command receipt for {}",
                                    receipt.request_id
                                )),
                            ),
                            Err(error) => publish(
                                &sender,
                                &context,
                                ClientEvent::Disconnected(format!(
                                    "invalid command receipt: {error}"
                                )),
                            ),
                        }
                    }
                    Ok(response) => publish(
                        &sender,
                        &context,
                        ClientEvent::Disconnected(format!(
                            "control command returned HTTP {}",
                            response.status()
                        )),
                    ),
                    Err(error) => publish(
                        &sender,
                        &context,
                        ClientEvent::Disconnected(format!("control command failed: {error}")),
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

#[cfg(test)]
mod tests {
    use super::{parse_sse_frame, path, sse_boundary};

    #[test]
    fn api_paths_preserve_the_control_v2_route() {
        assert_eq!(
            path("http://control:39180/", "/v2/ui/snapshot"),
            "http://control:39180/v2/ui/snapshot"
        );
        assert_eq!(path("", "/v2/ui/events"), "/v2/ui/events");
    }

    #[test]
    fn sse_frame_retains_cursor_for_reconnect_and_joins_multiline_data() {
        let frame = parse_sse_frame(
            "id: cursor-42\nevent: snapshot\ndata: {\"type\":\ndata: \"snapshot\"}",
        );
        assert_eq!(frame.cursor.as_deref(), Some("cursor-42"));
        assert_eq!(frame.payload.as_deref(), Some("{\"type\":\n\"snapshot\"}"));
    }

    #[test]
    fn sse_frame_without_data_can_advance_a_cursor() {
        let frame = parse_sse_frame("id: cursor-43\n: heartbeat");
        assert_eq!(frame.cursor.as_deref(), Some("cursor-43"));
        assert_eq!(frame.payload, None);
        assert_eq!(sse_boundary("id: x\r\n\r\nnext"), Some(5));
        assert_eq!(sse_boundary("id: x\n\nnext"), Some(5));
    }
}
