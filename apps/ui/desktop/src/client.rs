use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
mod execution;
mod grid;
mod terminal;
use eframe::egui;
pub(crate) use grid::GridMutation;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use venue_control_protocol::accounts::SecretValue;
use venue_control_protocol::kol::{
    ExecutorCommandSummary, TerminalAccountProjection, TerminalCancelRequest, TerminalOrderRequest,
    TerminalProjectionRequest,
};
use venue_control_protocol::{
    COMMAND_PATH, COPY_RELATION_PATH, CommandReceipt, ControlCommandRequest, ControlSnapshot,
    CopyRelationReceipt, CopyRelationRecord, CopyRelationUpsertRequest, EVENT_STREAM_PATH,
    SNAPSHOT_PATH, UiAccountScope, UiEventEnvelope,
};

#[cfg(not(target_arch = "wasm32"))]
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const RECONNECT_INITIAL: std::time::Duration = std::time::Duration::from_millis(250);
const RECONNECT_MAX: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_SSE_BUFFER_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_SSE_FRAME_BYTES: usize = 1_024 * 1_024;

#[derive(Clone, Debug)]
pub enum ClientEvent {
    TerminalAccountProjection {
        credential_id: String,
        projection: Option<TerminalAccountProjection>,
    },
    TerminalExecutions(Vec<ExecutorCommandSummary>),
    TerminalExecutionUpdated(ExecutorCommandSummary),
    TerminalExecutionsUnavailable(String),
    TerminalAccountUnavailable(String),
    GridInstances(Vec<venue_control_protocol::grid::GridInstanceSummary>),
    GridMutationApplied(Box<venue_control_protocol::grid::GridInstanceSummary>),
    GridUnavailable(String),
    GridMutationUnavailable(String),
    SessionExpired,
    SnapshotConnected,
    SnapshotUnavailable(String),
    StreamConnected {
        resumed_after: Option<i64>,
    },
    StreamUnavailable(String),
    CommandUnavailable(String),
    CopyRelationUnavailable(String),
    EventCursor(i64),
    Snapshot(ControlSnapshot),
    Receipt(CommandReceipt),
    CopyRelationConfigs(Vec<CopyRelationRecord>),
    CopyRelationReceipt(CopyRelationReceipt),
}

pub struct ControlClient {
    events: Receiver<ClientEvent>,
    command_tx: Sender<ControlCommandRequest>,
    terminal_order_tx: Sender<TerminalOrderRequest>,
    terminal_cancel_tx: Sender<TerminalCancelRequest>,
    terminal_projection_tx: Sender<TerminalProjectionRequest>,
    copy_relation_tx: Sender<CopyRelationUpsertRequest>,
    grid_mutation_tx: Sender<GridMutation>,
    stream_gates: StreamGates,
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
        let (terminal_order_tx, terminal_order_rx) = unbounded();
        let (terminal_cancel_tx, terminal_cancel_rx) = unbounded();
        let (terminal_projection_tx, terminal_projection_rx) = bounded(1);
        let (copy_relation_tx, copy_relation_rx) = unbounded();
        let (grid_mutation_tx, grid_mutation_rx) = unbounded();
        let stream_gates = StreamGates::default();

        #[cfg(not(target_arch = "wasm32"))]
        let stop = {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            start_native(
                endpoint,
                event_tx,
                command_rx,
                terminal_order_rx,
                terminal_cancel_rx,
                terminal_projection_rx,
                copy_relation_rx,
                grid_mutation_rx,
                context,
                stop.clone(),
                stream_gates.clone(),
                token,
            );
            stop
        };

        #[cfg(target_arch = "wasm32")]
        let web = WebClient::start(
            endpoint,
            event_tx,
            command_rx,
            copy_relation_rx,
            grid_mutation_rx,
            context,
            stream_gates.clone(),
            token,
        );

        Self {
            events,
            command_tx,
            terminal_order_tx,
            terminal_cancel_tx,
            terminal_projection_tx,
            copy_relation_tx,
            grid_mutation_tx,
            stream_gates,
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
        if !self.stream_gates.is_open(&command_scope(&command)) {
            return Err(ClientError::WriteGateClosed);
        }
        self.command_tx
            .send(command)
            .map_err(|_| ClientError::Closed)
    }

    pub fn send_terminal(&self, request: TerminalOrderRequest) -> Result<(), ClientError> {
        request
            .validate()
            .map_err(|_| ClientError::TerminalProtocol)?;
        self.terminal_order_tx
            .send(request)
            .map_err(|_| ClientError::Closed)
    }

    pub fn send_terminal_cancel(&self, request: TerminalCancelRequest) -> Result<(), ClientError> {
        request
            .validate()
            .map_err(|_| ClientError::TerminalProtocol)?;
        self.terminal_cancel_tx
            .send(request)
            .map_err(|_| ClientError::Closed)
    }

    pub fn subscribe_terminal(&self, request: TerminalProjectionRequest) {
        if request.validate().is_ok() {
            let _ = self.terminal_projection_tx.try_send(request);
        }
    }

    pub fn send_copy_relation(
        &self,
        request: CopyRelationUpsertRequest,
    ) -> Result<(), ClientError> {
        request.validate().map_err(ClientError::Protocol)?;
        if !self
            .stream_gates
            .is_open(&copy_scope(&request.relation.leader))
            || !self
                .stream_gates
                .is_open(&copy_scope(&request.relation.follower))
        {
            return Err(ClientError::WriteGateClosed);
        }
        self.copy_relation_tx
            .send(request)
            .map_err(|_| ClientError::Closed)
    }

    pub(crate) fn send_grid(&self, mutation: GridMutation) -> Result<(), ClientError> {
        if !mutation.validate() {
            return Err(ClientError::GridProtocol);
        }
        self.grid_mutation_tx
            .send(mutation)
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
    #[error("terminal request does not satisfy the protocol")]
    TerminalProtocol,
    #[error("grid request does not satisfy the protocol")]
    GridProtocol,
    #[error("control client is closed")]
    Closed,
    #[error("the scoped event stream is not currently healthy; writes are closed")]
    WriteGateClosed,
}

#[derive(Clone, Default)]
struct StreamGates(Arc<Mutex<StreamGateState>>);

#[derive(Default)]
struct StreamGateState {
    desired: BTreeSet<UiAccountScope>,
    open: BTreeSet<UiAccountScope>,
    running: BTreeSet<UiAccountScope>,
}

impl StreamGates {
    fn reconcile(&self, scopes: BTreeSet<UiAccountScope>) {
        if let Ok(mut state) = self.0.lock() {
            state.desired = scopes;
            let desired = state.desired.clone();
            state.open.retain(|scope| desired.contains(scope));
        }
    }

    fn is_desired(&self, scope: &UiAccountScope) -> bool {
        self.0
            .lock()
            .is_ok_and(|state| state.desired.contains(scope))
    }

    fn try_start(&self, scope: &UiAccountScope) -> bool {
        self.0.lock().is_ok_and(|mut state| {
            state.desired.contains(scope) && state.running.insert(scope.clone())
        })
    }

    fn opened(&self, scope: &UiAccountScope) {
        if let Ok(mut state) = self.0.lock()
            && state.desired.contains(scope)
        {
            state.open.insert(scope.clone());
        }
    }

    fn closed(&self, scope: &UiAccountScope) {
        if let Ok(mut state) = self.0.lock() {
            state.open.remove(scope);
        }
    }

    fn finished(&self, scope: &UiAccountScope) {
        if let Ok(mut state) = self.0.lock() {
            state.open.remove(scope);
            state.running.remove(scope);
        }
    }

    fn is_open(&self, scope: &UiAccountScope) -> bool {
        self.0.lock().is_ok_and(|state| state.open.contains(scope))
    }
}

fn command_scope(command: &ControlCommandRequest) -> UiAccountScope {
    UiAccountScope {
        venue: command.venue,
        mode: command.mode,
        trading_account_id: command.trading_account_id.clone(),
    }
}

fn copy_scope(binding: &venue_control_protocol::CopyRelationBinding) -> UiAccountScope {
    UiAccountScope {
        venue: binding.venue,
        mode: binding.mode,
        trading_account_id: binding.trading_account_id.clone(),
    }
}

fn snapshot_scopes(snapshot: &ControlSnapshot) -> BTreeSet<UiAccountScope> {
    snapshot
        .accounts
        .iter()
        .map(|account| UiAccountScope {
            venue: account.venue,
            mode: account.mode,
            trading_account_id: account.trading_account_id.clone(),
        })
        .chain(snapshot.strategies.iter().map(|strategy| UiAccountScope {
            venue: strategy.venue,
            mode: strategy.mode,
            trading_account_id: strategy.trading_account_id.clone(),
        }))
        .collect()
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

#[cfg(not(target_arch = "wasm32"))]
#[expect(
    clippy::too_many_arguments,
    reason = "thread boundary explicitly transfers the independent channels and session state"
)]
fn start_native(
    endpoint: String,
    sender: Sender<ClientEvent>,
    commands: Receiver<ControlCommandRequest>,
    terminal_orders: Receiver<TerminalOrderRequest>,
    terminal_cancellations: Receiver<TerminalCancelRequest>,
    terminal_projection: Receiver<TerminalProjectionRequest>,
    copy_relations: Receiver<CopyRelationUpsertRequest>,
    grid_mutations: Receiver<GridMutation>,
    context: egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    stream_gates: StreamGates,
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
                endpoint,
                sender,
                commands,
                terminal_orders,
                terminal_cancellations,
                terminal_projection,
                copy_relations,
                grid_mutations,
                context,
                stop,
                stream_gates,
                token,
            ));
        });
    if let Err(error) = spawn {
        tracing::error!(%error, "failed to spawn VenueFlow control client");
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[expect(
    clippy::too_many_arguments,
    reason = "runtime boundary keeps the same explicit ownership transfer as the thread boundary"
)]
async fn native_loop(
    endpoint: String,
    sender: Sender<ClientEvent>,
    commands: Receiver<ControlCommandRequest>,
    terminal_orders: Receiver<TerminalOrderRequest>,
    terminal_cancellations: Receiver<TerminalCancelRequest>,
    terminal_projection: Receiver<TerminalProjectionRequest>,
    copy_relations: Receiver<CopyRelationUpsertRequest>,
    grid_mutations: Receiver<GridMutation>,
    context: egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    stream_gates: StreamGates,
    token: Option<SecretValue>,
) {
    let authenticated = token.is_some();
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

    if authenticated {
        execution::start_native(
            client.clone(),
            endpoint.clone(),
            sender.clone(),
            context.clone(),
            stop.clone(),
            terminal_projection,
        );
        terminal::start_native(
            client.clone(),
            endpoint.clone(),
            sender.clone(),
            context.clone(),
            stop.clone(),
            terminal_orders,
            terminal_cancellations,
        );
        grid::start_native(
            client.clone(),
            endpoint.clone(),
            sender.clone(),
            context.clone(),
            stop.clone(),
            grid_mutations,
        );
    }
    let (scope_tx, scope_rx) = tokio::sync::mpsc::unbounded_channel();
    if let Some(scopes) =
        fetch_native_snapshot(&client, &endpoint, &sender, &context, authenticated).await
    {
        let _ = scope_tx.send(scopes);
    }
    if authenticated {
        fetch_native_copy_relations(&client, &endpoint, &sender, &context).await;
    }
    let stream = NativeStreamContext {
        client: client.clone(),
        endpoint: endpoint.clone(),
        sender: sender.clone(),
        context: context.clone(),
        stop: stop.clone(),
        gates: stream_gates.clone(),
        scope_tx: scope_tx.clone(),
        authenticated,
    };
    tokio::spawn(async move {
        native_event_supervisor(stream, scope_rx).await;
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
                Ok(response) if response.status().as_u16() == 401 => {
                    publish(&sender, &context, ClientEvent::SessionExpired);
                    return;
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

        for request in copy_relations.try_iter().take(16) {
            let response = client
                .post(path(&endpoint, COPY_RELATION_PATH))
                .json(&request)
                .timeout(REQUEST_TIMEOUT)
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    match response.json::<CopyRelationReceipt>().await {
                        Ok(receipt)
                            if receipt.validate().is_ok()
                                && receipt.relation_id == request.relation.relation_id =>
                        {
                            publish(&sender, &context, ClientEvent::CopyRelationReceipt(receipt));
                        }
                        Ok(_) => publish(
                            &sender,
                            &context,
                            ClientEvent::CopyRelationUnavailable(
                                "invalid or mismatched copy relation receipt".to_owned(),
                            ),
                        ),
                        Err(error) => publish(
                            &sender,
                            &context,
                            ClientEvent::CopyRelationUnavailable(format!(
                                "invalid copy relation receipt: {error}"
                            )),
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
                    ClientEvent::CopyRelationUnavailable(format!(
                        "copy relation request returned HTTP {}",
                        response.status()
                    )),
                ),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::CopyRelationUnavailable(format!(
                        "copy relation request failed: {error}"
                    )),
                ),
            }
        }

        if tokio::time::Instant::now() >= next_snapshot {
            if let Some(scopes) =
                fetch_native_snapshot(&client, &endpoint, &sender, &context, authenticated).await
            {
                let _ = scope_tx.send(scopes);
            }
            if authenticated {
                fetch_native_copy_relations(&client, &endpoint, &sender, &context).await;
            }
            next_snapshot = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn fetch_native_copy_relations(
    client: &reqwest::Client,
    endpoint: &str,
    sender: &Sender<ClientEvent>,
    context: &egui::Context,
) {
    match client
        .get(path(endpoint, COPY_RELATION_PATH))
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            match response.json::<Vec<CopyRelationRecord>>().await {
                Ok(configs) if configs.iter().all(|record| record.validate().is_ok()) => {
                    publish(sender, context, ClientEvent::CopyRelationConfigs(configs))
                }
                Ok(_) => publish(
                    sender,
                    context,
                    ClientEvent::CopyRelationUnavailable(
                        "copy relation configuration validation failed".to_owned(),
                    ),
                ),
                Err(error) => publish(
                    sender,
                    context,
                    ClientEvent::CopyRelationUnavailable(format!(
                        "invalid copy relation configuration: {error}"
                    )),
                ),
            }
        }
        Ok(response) if response.status().as_u16() == 401 => {
            publish(sender, context, ClientEvent::SessionExpired)
        }
        Ok(response) => publish(
            sender,
            context,
            ClientEvent::CopyRelationUnavailable(format!(
                "copy relation configuration returned HTTP {}",
                response.status()
            )),
        ),
        Err(error) => publish(
            sender,
            context,
            ClientEvent::CopyRelationUnavailable(format!(
                "copy relation configuration unavailable: {error}"
            )),
        ),
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn fetch_native_snapshot(
    client: &reqwest::Client,
    endpoint: &str,
    sender: &Sender<ClientEvent>,
    context: &egui::Context,
    authenticated: bool,
) -> Option<BTreeSet<UiAccountScope>> {
    match client
        .get(path(endpoint, SNAPSHOT_PATH))
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            match response.json::<ControlSnapshot>().await {
                Ok(snapshot) if snapshot.validate().is_ok() => {
                    let scopes = snapshot_scopes(&snapshot);
                    publish(sender, context, ClientEvent::SnapshotConnected);
                    publish(sender, context, ClientEvent::Snapshot(snapshot));
                    Some(scopes)
                }
                Ok(_) => {
                    publish(
                        sender,
                        context,
                        ClientEvent::SnapshotUnavailable("snapshot validation failed".to_owned()),
                    );
                    None
                }
                Err(error) => {
                    publish(
                        sender,
                        context,
                        ClientEvent::SnapshotUnavailable(format!("invalid snapshot: {error}")),
                    );
                    None
                }
            }
        }
        Ok(response) if response.status().as_u16() == 401 => {
            if authenticated {
                publish(sender, context, ClientEvent::SessionExpired);
            } else {
                publish(
                    sender,
                    context,
                    ClientEvent::SnapshotUnavailable("snapshot returned HTTP 401".to_owned()),
                );
            }
            None
        }
        Ok(response) => {
            publish(
                sender,
                context,
                ClientEvent::SnapshotUnavailable(format!(
                    "snapshot returned HTTP {}",
                    response.status()
                )),
            );
            None
        }
        Err(error) => {
            publish(
                sender,
                context,
                ClientEvent::SnapshotUnavailable(format!("snapshot unavailable: {error}")),
            );
            None
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct NativeStreamContext {
    client: reqwest::Client,
    endpoint: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    gates: StreamGates,
    scope_tx: tokio::sync::mpsc::UnboundedSender<BTreeSet<UiAccountScope>>,
    authenticated: bool,
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_event_supervisor(
    stream: NativeStreamContext,
    mut scopes: tokio::sync::mpsc::UnboundedReceiver<BTreeSet<UiAccountScope>>,
) {
    use std::sync::atomic::Ordering;

    while !stream.stop.load(Ordering::Acquire) {
        let Some(next_scopes) = scopes.recv().await else {
            return;
        };
        stream.gates.reconcile(next_scopes.clone());
        for scope in next_scopes {
            if !stream.gates.try_start(&scope) {
                continue;
            }
            let task_stream = stream.clone();
            tokio::spawn(async move {
                native_scoped_event_supervisor(task_stream, scope).await;
            });
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_scoped_event_supervisor(stream: NativeStreamContext, scope: UiAccountScope) {
    use std::sync::atomic::Ordering;
    let NativeStreamContext {
        sender,
        context,
        stop,
        gates,
        ..
    } = &stream;

    let mut cursor = None;
    let mut backoff = ReconnectBackoff::default();
    while !stop.load(Ordering::Acquire) && gates.is_desired(&scope) {
        gates.closed(&scope);
        match native_event_stream(&stream, &scope, cursor).await {
            Ok(outcome) => {
                cursor = outcome.cursor.or(cursor);
                if outcome.made_progress {
                    backoff.reset();
                }
                if !stop.load(Ordering::Acquire) && gates.is_desired(&scope) {
                    publish(
                        sender,
                        context,
                        ClientEvent::StreamUnavailable(format!(
                            "event stream for {} closed; reconnecting from its last event ID",
                            scope.trading_account_id
                        )),
                    );
                }
            }
            Err(error) if !stop.load(Ordering::Acquire) && gates.is_desired(&scope) => publish(
                sender,
                context,
                ClientEvent::StreamUnavailable(format!(
                    "event stream for {} unavailable: {error}",
                    scope.trading_account_id
                )),
            ),
            Err(_) => break,
        }
        gates.closed(&scope);
        if wait_native_stop(stop, backoff.next_delay()).await {
            break;
        }
    }
    gates.finished(&scope);
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
    stream: &NativeStreamContext,
    scope: &UiAccountScope,
    cursor: Option<EventCursor>,
) -> Result<StreamOutcome, String> {
    use futures_util::StreamExt as _;
    use std::sync::atomic::Ordering;
    let NativeStreamContext {
        client,
        endpoint,
        sender,
        context,
        stop,
        gates,
        scope_tx,
        authenticated,
    } = stream;

    let request = client
        .get(event_stream_url(endpoint, scope, cursor))
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .header(
            "Last-Event-ID",
            cursor.map_or(0, EventCursor::value).to_string(),
        );
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
    gates.opened(scope);
    let mut bytes = response.bytes_stream();
    let mut decoder = SseDecoder::default();
    let mut latest_cursor = cursor;
    let mut made_progress = false;
    while !stop.load(Ordering::Acquire) && gates.is_desired(scope) {
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
            let Some(next) = validate_invalidation_frame(&parsed, scope, latest_cursor)? else {
                continue;
            };
            latest_cursor = Some(next);
            made_progress = true;
            publish(sender, context, ClientEvent::EventCursor(next.value()));
            if let Some(scopes) =
                fetch_native_snapshot(client, endpoint, sender, context, *authenticated).await
            {
                let _ = scope_tx.send(scopes);
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

#[cfg(not(target_arch = "wasm32"))]
fn validate_invalidation_frame(
    frame: &ParsedSseFrame,
    scope: &UiAccountScope,
    latest_cursor: Option<EventCursor>,
) -> Result<Option<EventCursor>, String> {
    validate_invalidation(scope, latest_cursor, frame.cursor, frame.payload.as_deref())
}

fn validate_invalidation(
    scope: &UiAccountScope,
    latest_cursor: Option<EventCursor>,
    frame_cursor: Option<EventCursor>,
    payload: Option<&str>,
) -> Result<Option<EventCursor>, String> {
    match (frame_cursor, payload) {
        (None, None) => Ok(None), // keep-alive comment
        (Some(_), None) | (None, Some(_)) => {
            Err("event frame must contain both an ID and a schema-2 envelope".to_owned())
        }
        (Some(cursor), Some(payload)) => {
            let envelope = serde_json::from_str::<UiEventEnvelope>(payload)
                .map_err(|error| format!("invalid schema-2 invalidation: {error}"))?;
            envelope
                .validate()
                .map_err(|error| format!("invalid schema-2 invalidation: {error}"))?;
            let envelope_cursor = i64::try_from(envelope.cursor)
                .map_err(|_| "event cursor exceeds the HTTP stream range".to_owned())?;
            if cursor != EventCursor(envelope_cursor) {
                return Err("SSE ID disagrees with the invalidation cursor".to_owned());
            }
            if &envelope.scope != scope {
                return Err("invalidation scope does not match its stream".to_owned());
            }
            let previous = latest_cursor.map_or(0, EventCursor::value);
            if envelope.previous_cursor
                != u64::try_from(previous).map_err(|_| "event cursor is negative".to_owned())?
            {
                return Err(
                    "invalidation previous_cursor breaks the scoped cursor chain".to_owned(),
                );
            }
            Ok(Some(cursor))
        }
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
        copy_relations: Receiver<CopyRelationUpsertRequest>,
        grid_mutations: Receiver<GridMutation>,
        context: egui::Context,
        stream_gates: StreamGates,
        token: Option<SecretValue>,
    ) -> Self {
        let stop = std::rc::Rc::new(std::cell::Cell::new(false));
        if token.is_some() && !crate::account_client::safe_endpoint(&endpoint) {
            publish(&sender, &context, ClientEvent::SessionExpired);
            return Self { stop };
        }
        if let Some(auth_token) = token.clone() {
            grid::start_web(
                endpoint.clone(),
                sender.clone(),
                context.clone(),
                stop.clone(),
                auth_token,
                grid_mutations,
            );
        }
        let (scope_tx, scope_rx) = unbounded();
        spawn_web_events(
            endpoint.clone(),
            sender.clone(),
            context.clone(),
            stop.clone(),
            stream_gates.clone(),
            scope_rx,
            scope_tx.clone(),
            token.clone(),
        );
        spawn_web_snapshot(
            endpoint.clone(),
            sender.clone(),
            context.clone(),
            stop.clone(),
            scope_tx,
            token.clone(),
        );
        spawn_web_copy_relations(
            endpoint.clone(),
            sender.clone(),
            context.clone(),
            stop.clone(),
            token.clone(),
        );
        spawn_web_commands(
            endpoint.clone(),
            sender.clone(),
            commands,
            context.clone(),
            stop.clone(),
            token.clone(),
        );
        spawn_web_copy_relation_requests(
            endpoint,
            sender,
            copy_relations,
            context,
            stop.clone(),
            token,
        );
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
    gates: StreamGates,
    scopes: Receiver<BTreeSet<UiAccountScope>>,
    scope_tx: Sender<BTreeSet<UiAccountScope>>,
    token: Option<SecretValue>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        while !stop.get() {
            for next_scopes in scopes.try_iter() {
                gates.reconcile(next_scopes.clone());
                for scope in next_scopes {
                    if gates.try_start(&scope) {
                        spawn_web_scoped_events(
                            endpoint.clone(),
                            scope,
                            sender.clone(),
                            context.clone(),
                            stop.clone(),
                            gates.clone(),
                            scope_tx.clone(),
                            token.clone(),
                        );
                    }
                }
            }
            wasm_timer(100).await;
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn duration_ms(duration: std::time::Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

fn event_stream_url(endpoint: &str, scope: &UiAccountScope, cursor: Option<EventCursor>) -> String {
    let url = path(endpoint, EVENT_STREAM_PATH);
    let after = cursor.map_or(0, EventCursor::value);
    format!(
        "{url}?venue={}&mode={}&trading_account_id={}&after={after}",
        scope.venue.as_str(),
        scope.mode.as_str(),
        scope.trading_account_id,
    )
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_scoped_events(
    endpoint: String,
    scope: UiAccountScope,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    gates: StreamGates,
    scope_tx: Sender<BTreeSet<UiAccountScope>>,
    token: Option<SecretValue>,
) {
    if let Some(token) = token {
        spawn_web_authenticated_scoped_events(
            endpoint, scope, sender, context, stop, gates, scope_tx, token,
        );
        return;
    }
    use std::{cell::Cell, rc::Rc};
    use wasm_bindgen::{JsCast as _, closure::Closure};

    wasm_bindgen_futures::spawn_local(async move {
        let mut cursor = None;
        let mut backoff = ReconnectBackoff::default();
        while !stop.get() && gates.is_desired(&scope) {
            gates.closed(&scope);
            let source =
                match web_sys::EventSource::new(&event_stream_url(&endpoint, &scope, cursor)) {
                    Ok(source) => source,
                    Err(error) => {
                        publish(
                            &sender,
                            &context,
                            ClientEvent::StreamUnavailable(format!(
                                "browser could not open the scoped event stream: {error:?}"
                            )),
                        );
                        wasm_timer(duration_ms(backoff.next_delay())).await;
                        continue;
                    }
                };
            let failed = Rc::new(Cell::new(false));
            let latest_cursor = Rc::new(Cell::new(cursor));
            let open_sender = sender.clone();
            let open_context = context.clone();
            let open_gates = gates.clone();
            let open_scope = scope.clone();
            let resumed_after = cursor.map(EventCursor::value);
            let open = Closure::wrap(Box::new(move |_event: web_sys::Event| {
                open_gates.opened(&open_scope);
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
            let message_scope = scope.clone();
            let message_endpoint = endpoint.clone();
            let message_scopes = scope_tx.clone();
            let message = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                let parsed_cursor = EventCursor::parse(&event.last_event_id());
                let payload = event.data().as_string();
                match parsed_cursor.and_then(|id| {
                    validate_invalidation(
                        &message_scope,
                        message_cursor.get(),
                        Some(id),
                        payload.as_deref(),
                    )
                }) {
                    Ok(Some(next)) => {
                        message_cursor.set(Some(next));
                        publish(
                            &message_sender,
                            &message_context,
                            ClientEvent::EventCursor(next.value()),
                        );
                        fetch_web_snapshot_once(
                            message_endpoint.clone(),
                            message_sender.clone(),
                            message_context.clone(),
                            message_scopes.clone(),
                            None,
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        message_failed.set(true);
                        publish(
                            &message_sender,
                            &message_context,
                            ClientEvent::StreamUnavailable(format!(
                                "invalid scoped schema-2 invalidation: {error}"
                            )),
                        );
                    }
                }
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
                        "scoped event stream disconnected; writes are closed until it reconnects"
                            .to_owned(),
                    ),
                );
            }) as Box<dyn FnMut(_)>);
            source.set_onerror(Some(error.as_ref().unchecked_ref()));
            while !stop.get() && !failed.get() && gates.is_desired(&scope) {
                wasm_timer(100).await;
            }
            source.close();
            gates.closed(&scope);
            let next_cursor = latest_cursor.get();
            if next_cursor != cursor {
                cursor = next_cursor;
                backoff.reset();
            }
            drop((open, message, error));
            if !stop.get() && gates.is_desired(&scope) {
                wasm_timer(duration_ms(backoff.next_delay())).await;
            }
        }
        gates.finished(&scope);
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_authenticated_scoped_events(
    endpoint: String,
    scope: UiAccountScope,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    gates: StreamGates,
    scope_tx: Sender<BTreeSet<UiAccountScope>>,
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
        while !stop.get() && gates.is_desired(&scope) {
            gates.closed(&scope);
            let response = client
                .get(event_stream_url(&endpoint, &scope, cursor))
                .headers(headers.clone())
                .send()
                .await;
            if stop.get() {
                break;
            }
            match response {
                Ok(response) if response.status().is_success() => {
                    gates.opened(&scope);
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
                        if stop.get() || !gates.is_desired(&scope) {
                            break;
                        }
                        let Ok(chunk) = chunk else {
                            break;
                        };
                        let Ok(frames) = decoder.push(&chunk) else {
                            break;
                        };
                        for frame in frames {
                            match validate_invalidation(
                                &scope,
                                cursor,
                                frame.cursor,
                                frame.payload.as_deref(),
                            ) {
                                Ok(Some(next)) => {
                                    cursor = Some(next);
                                    backoff.reset();
                                    publish(
                                        &sender,
                                        &context,
                                        ClientEvent::EventCursor(next.value()),
                                    );
                                    fetch_web_snapshot_once(
                                        endpoint.clone(),
                                        sender.clone(),
                                        context.clone(),
                                        scope_tx.clone(),
                                        Some(token.clone()),
                                    );
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    publish(
                                        &sender,
                                        &context,
                                        ClientEvent::StreamUnavailable(format!(
                                            "invalid scoped schema-2 invalidation: {error}"
                                        )),
                                    );
                                    break 'stream;
                                }
                            }
                        }
                    }
                }
                Ok(response) if response.status().as_u16() == 401 => {
                    publish(&sender, &context, ClientEvent::SessionExpired);
                    break;
                }
                Ok(response) => publish(
                    &sender,
                    &context,
                    ClientEvent::StreamUnavailable(format!(
                        "scoped event stream returned HTTP {}",
                        response.status()
                    )),
                ),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::StreamUnavailable(format!(
                        "scoped event stream unavailable: {error}"
                    )),
                ),
            }
            gates.closed(&scope);
            if !stop.get() && gates.is_desired(&scope) {
                wasm_timer(duration_ms(backoff.next_delay())).await;
            }
        }
        gates.finished(&scope);
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_snapshot(
    endpoint: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    scope_tx: Sender<BTreeSet<UiAccountScope>>,
    token: Option<SecretValue>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        while !stop.get() {
            fetch_web_snapshot_once(
                endpoint.clone(),
                sender.clone(),
                context.clone(),
                scope_tx.clone(),
                token.clone(),
            );
            wasm_timer(3_000).await;
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn fetch_web_snapshot_once(
    endpoint: String,
    sender: Sender<ClientEvent>,
    context: egui::Context,
    scope_tx: Sender<BTreeSet<UiAccountScope>>,
    token: Option<SecretValue>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let Ok(headers) = crate::account_client::authorization_headers(token.as_ref()) else {
            return;
        };
        match reqwest::Client::new()
            .get(path(&endpoint, SNAPSHOT_PATH))
            .headers(headers)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                match response.json::<ControlSnapshot>().await {
                    Ok(snapshot) if snapshot.validate().is_ok() => {
                        let scopes = snapshot_scopes(&snapshot);
                        publish(&sender, &context, ClientEvent::SnapshotConnected);
                        publish(&sender, &context, ClientEvent::Snapshot(snapshot));
                        let _ = scope_tx.send(scopes);
                    }
                    Ok(_) => publish(
                        &sender,
                        &context,
                        ClientEvent::SnapshotUnavailable("snapshot validation failed".to_owned()),
                    ),
                    Err(error) => publish(
                        &sender,
                        &context,
                        ClientEvent::SnapshotUnavailable(format!("invalid snapshot: {error}")),
                    ),
                }
            }
            Ok(response) if response.status().as_u16() == 401 => {
                publish(&sender, &context, ClientEvent::SessionExpired)
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
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_web_copy_relations(
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
                .get(path(&endpoint, COPY_RELATION_PATH))
                .headers(headers.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    match response.json::<Vec<CopyRelationRecord>>().await {
                        Ok(configs) if configs.iter().all(|record| record.validate().is_ok()) => {
                            publish(&sender, &context, ClientEvent::CopyRelationConfigs(configs))
                        }
                        Ok(_) => publish(
                            &sender,
                            &context,
                            ClientEvent::CopyRelationUnavailable(
                                "copy relation configuration validation failed".to_owned(),
                            ),
                        ),
                        Err(error) => publish(
                            &sender,
                            &context,
                            ClientEvent::CopyRelationUnavailable(format!(
                                "invalid copy relation configuration: {error}"
                            )),
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
                    ClientEvent::CopyRelationUnavailable(format!(
                        "copy relation configuration returned HTTP {}",
                        response.status()
                    )),
                ),
                Err(error) => publish(
                    &sender,
                    &context,
                    ClientEvent::CopyRelationUnavailable(format!(
                        "copy relation configuration unavailable: {error}"
                    )),
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
                    Ok(response) if response.status().as_u16() == 401 => {
                        publish(&sender, &context, ClientEvent::SessionExpired);
                        return;
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
fn spawn_web_copy_relation_requests(
    endpoint: String,
    sender: Sender<ClientEvent>,
    requests: Receiver<CopyRelationUpsertRequest>,
    context: egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    token: Option<SecretValue>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let Ok(headers) = crate::account_client::authorization_headers(token.as_ref()) else {
            return;
        };
        while !stop.get() {
            for request in requests.try_iter().take(16) {
                match reqwest::Client::new()
                    .post(path(&endpoint, COPY_RELATION_PATH))
                    .headers(headers.clone())
                    .json(&request)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {
                        match response.json::<CopyRelationReceipt>().await {
                            Ok(receipt)
                                if receipt.validate().is_ok()
                                    && receipt.relation_id == request.relation.relation_id =>
                            {
                                publish(
                                    &sender,
                                    &context,
                                    ClientEvent::CopyRelationReceipt(receipt),
                                );
                            }
                            Ok(_) => publish(
                                &sender,
                                &context,
                                ClientEvent::CopyRelationUnavailable(
                                    "invalid or mismatched copy relation receipt".to_owned(),
                                ),
                            ),
                            Err(error) => publish(
                                &sender,
                                &context,
                                ClientEvent::CopyRelationUnavailable(format!(
                                    "invalid copy relation receipt: {error}"
                                )),
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
                        ClientEvent::CopyRelationUnavailable(format!(
                            "copy relation request returned HTTP {}",
                            response.status()
                        )),
                    ),
                    Err(error) => publish(
                        &sender,
                        &context,
                        ClientEvent::CopyRelationUnavailable(format!(
                            "copy relation request failed: {error}"
                        )),
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
        SseDecoder, StreamGates, event_stream_url, parse_sse_frame, path, sse_boundary,
        validate_invalidation_frame,
    };
    use venue_control_protocol::{
        CONTROL_SCHEMA_VERSION, GatewayMode, UiAccountScope, UiEventEnvelope, UiEventKind, VenueId,
    };

    fn scope() -> UiAccountScope {
        UiAccountScope {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        }
    }

    #[test]
    fn api_paths_preserve_the_control_v2_route() {
        assert_eq!(
            path("http://control:39180/", "/v2/ui/snapshot"),
            "http://control:39180/v2/ui/snapshot"
        );
        assert_eq!(path("", "/v2/ui/events"), "/v2/ui/events");
        assert_eq!(
            event_stream_url("http://control:39180", &scope(), Some(EventCursor(42))),
            "http://control:39180/v2/ui/events?venue=binance&mode=LIVE&trading_account_id=00000000-0000-4000-8000-000000000001&after=42"
        );
        assert_eq!(
            event_stream_url("http://control:39180", &scope(), None),
            "http://control:39180/v2/ui/events?venue=binance&mode=LIVE&trading_account_id=00000000-0000-4000-8000-000000000001&after=0"
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
    fn heartbeat_does_not_advance_a_scoped_cursor() -> Result<(), String> {
        let frame = parse_sse_frame("id: 43\n: heartbeat")?;
        assert_eq!(frame.cursor, Some(EventCursor(43)));
        assert_eq!(frame.payload, None);
        assert!(validate_invalidation_frame(&frame, &scope(), Some(EventCursor(42))).is_err());
        assert_eq!(sse_boundary(b"id: x\r\n\r\nnext"), Some((5, 4)));
        assert_eq!(sse_boundary(b"id: x\n\nnext"), Some((5, 2)));
        Ok(())
    }

    #[test]
    fn schema_two_invalidation_requires_exact_scope_and_cursor_chain() -> Result<(), String> {
        let envelope = UiEventEnvelope {
            schema_version: CONTROL_SCHEMA_VERSION,
            cursor: 43,
            previous_cursor: 42,
            event_type: UiEventKind::Snapshot,
            scope: scope(),
        };
        let frame = parse_sse_frame(&format!(
            "id: 43\nevent: control\ndata: {}",
            serde_json::to_string(&envelope).map_err(|error| error.to_string())?
        ))?;
        assert_eq!(
            validate_invalidation_frame(&frame, &scope(), Some(EventCursor(42)))?,
            Some(EventCursor(43))
        );
        assert!(validate_invalidation_frame(&frame, &scope(), Some(EventCursor(41))).is_err());
        let mut another_scope = scope();
        another_scope.trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        assert!(
            validate_invalidation_frame(&frame, &another_scope, Some(EventCursor(42))).is_err()
        );
        Ok(())
    }

    #[test]
    fn write_gate_is_open_only_for_its_healthy_scope() {
        let gates = StreamGates::default();
        let scope = scope();
        let mut another_scope = scope.clone();
        another_scope.trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        gates.reconcile([scope.clone(), another_scope.clone()].into_iter().collect());
        assert!(gates.try_start(&scope));
        gates.opened(&scope);
        assert!(gates.is_open(&scope));
        assert!(!gates.is_open(&another_scope));
        gates.closed(&scope);
        assert!(!gates.is_open(&scope));
        gates.reconcile([another_scope.clone()].into_iter().collect());
        assert!(!gates.is_open(&scope));
        assert!(!gates.is_open(&another_scope));
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
