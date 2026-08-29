use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::egui;
use venue_control_protocol::{
    COMMAND_PATH, CommandReceipt, ControlCommandRequest, ControlEvent, ControlSnapshot,
    EVENT_STREAM_PATH, SNAPSHOT_PATH,
};

#[derive(Clone, Debug)]
pub enum ClientEvent {
    Connected,
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

    let stream_sender = sender.clone();
    let stream_context = context.clone();
    let stream_client = client.clone();
    let stream_url = path(&endpoint, EVENT_STREAM_PATH);
    tokio::spawn(async move {
        native_event_stream(stream_client, stream_url, stream_sender, stream_context).await;
    });

    let mut next_snapshot = tokio::time::Instant::now();
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
                        Ok(receipt) => publish(&sender, &context, ClientEvent::Receipt(receipt)),
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
            match client.get(path(&endpoint, SNAPSHOT_PATH)).send().await {
                Ok(response) if response.status().is_success() => {
                    match response.json::<ControlSnapshot>().await {
                        Ok(snapshot) if snapshot.validate().is_ok() => {
                            publish(&sender, &context, ClientEvent::Connected);
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
            next_snapshot = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_event_stream(
    client: reqwest::Client,
    url: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
) {
    use futures_util::StreamExt as _;

    let response = match client.get(url).send().await {
        Ok(response) if response.status().is_success() => response,
        Ok(response) => {
            publish(
                &sender,
                &context,
                ClientEvent::Notice(format!(
                    "event stream returned HTTP {}; snapshot polling remains active",
                    response.status()
                )),
            );
            return;
        }
        Err(error) => {
            publish(
                &sender,
                &context,
                ClientEvent::Notice(format!(
                    "event stream unavailable ({error}); snapshot polling remains active"
                )),
            );
            return;
        }
    };
    let mut bytes = response.bytes_stream();
    let mut buffer = String::new();
    while let Some(chunk) = bytes.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                publish(
                    &sender,
                    &context,
                    ClientEvent::Notice(format!("event stream ended: {error}")),
                );
                return;
            }
        };
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(boundary) = buffer.find("\n\n") {
            let frame = buffer[..boundary].to_owned();
            buffer.drain(..boundary + 2);
            let payload = frame
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(str::trim_start)
                .collect::<Vec<_>>()
                .join("\n");
            if payload.is_empty() {
                continue;
            }
            match serde_json::from_str::<ControlEvent>(&payload) {
                Ok(event) => publish_control_event(&sender, &context, event),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::Notice(format!("ignored invalid event frame: {error}")),
                ),
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
struct WebClient {
    _source: Option<web_sys::EventSource>,
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
        let (message, error) = if let Some(source_ref) = source.as_ref() {
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
                    ClientEvent::Notice(
                        "event stream disconnected; snapshot polling remains active".to_owned(),
                    ),
                );
            }) as Box<dyn FnMut(_)>);
            source_ref.set_onerror(Some(error.as_ref().unchecked_ref()));
            (Some(message), Some(error))
        } else {
            publish(
                &sender,
                &context,
                ClientEvent::Notice(
                    "browser could not open the event stream; snapshot polling remains active"
                        .to_owned(),
                ),
            );
            (None, None)
        };

        spawn_web_snapshot(endpoint.clone(), sender.clone(), context.clone());
        spawn_web_commands(endpoint, sender, commands, context);
        Self {
            _source: source,
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
                            publish(&sender, &context, ClientEvent::Connected);
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
                            Ok(receipt) => {
                                publish(&sender, &context, ClientEvent::Receipt(receipt));
                            }
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
