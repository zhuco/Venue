use crate::account_scope::{AccountScope, Scoped};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
mod execution;
mod grid;
pub(crate) mod inventory_mm;
mod leader_bot;
mod stream_gates;
mod support_martingale;
mod terminal;
use eframe::egui;
pub(crate) use grid::GridMutation;
use std::collections::BTreeSet;
use stream_gates::StreamGates;
pub(crate) use support_martingale::*;
use venue_control_protocol::accounts::SecretValue;
use venue_control_protocol::kol::{
    ExecutorCommandSummary, TerminalAccountProjection, TerminalCancelRequest, TerminalOrderRequest,
    TerminalProjectionRequest,
};
use venue_control_protocol::terminal_position::TerminalPositionActionRequest;
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
    AccountScoped {
        scope: AccountScope,
        event: Box<ClientEvent>,
    },
    TerminalAccountProjection {
        credential_id: String,
        projection: Option<TerminalAccountProjection>,
    },
    TerminalExecutions(Vec<ExecutorCommandSummary>),
    TerminalExecutionUpdated(ExecutorCommandSummary),
    TerminalSubmissionUnavailable {
        request_id: String,
        message: String,
        definitely_not_submitted: bool,
    },
    TerminalExecutionsUnavailable(String),
    TerminalAccountUnavailable {
        credential_id: String,
        message: String,
    },
    GridInstances(Vec<venue_control_protocol::grid::GridInstanceSummary>),
    InventoryMm(inventory_mm::Event),
    GridMutationApplied(Box<venue_control_protocol::grid::GridInstanceSummary>),
    GridUnavailable(String),
    GridMutationUnavailable(String),
    LeaderBotAccess(venue_control_protocol::leader_bot::LeaderBotsAccess),
    LeaderBotMutationApplied(venue_control_protocol::leader_bot::LeaderBotsAccess),
    LeaderBotUnavailable {
        mutation: bool,
        definitive: bool,
        message: String,
    },
    SupportMartingaleInstances(Vec<support_martingale::SupportMartingaleListItem>),
    SupportMartingaleMutationApplied(Box<support_martingale::SupportMartingaleListItem>),
    SupportMartingalePreflightApplied(
        Box<venue_control_protocol::support_martingale::SupportMartingalePreflightResponse>,
    ),
    SupportMartingaleUnavailable(String),
    SupportMartingaleMutationUnavailable(String),
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
    terminal_order_tx: Sender<Scoped<TerminalOrderRequest>>,
    terminal_cancel_tx: Sender<Scoped<TerminalCancelRequest>>,
    terminal_position_tx: Sender<Scoped<TerminalPositionActionRequest>>,
    #[cfg(not(target_arch = "wasm32"))]
    terminal_wake: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(not(target_arch = "wasm32"))]
    terminal_projection_tx: tokio::sync::watch::Sender<Option<Scoped<TerminalProjectionRequest>>>,
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
        let (terminal_position_tx, terminal_position_rx) = bounded(1);
        #[cfg(not(target_arch = "wasm32"))]
        let terminal_wake = std::sync::Arc::new(tokio::sync::Notify::new());
        #[cfg(not(target_arch = "wasm32"))]
        let (terminal_projection_tx, terminal_projection_rx) = tokio::sync::watch::channel(None);
        let (copy_relation_tx, copy_relation_rx) = unbounded();
        let (grid_mutation_tx, grid_mutation_rx) = unbounded();
        let stream_gates = StreamGates::default();
        if token.is_some() {
            stream_gates.select(None);
        }

        #[cfg(not(target_arch = "wasm32"))]
        let stop = {
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            start_native(
                endpoint,
                event_tx,
                command_rx,
                terminal_order_rx,
                terminal_cancel_rx,
                terminal_position_rx,
                terminal_wake.clone(),
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
            terminal_position_tx,
            #[cfg(not(target_arch = "wasm32"))]
            terminal_wake,
            #[cfg(not(target_arch = "wasm32"))]
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

    pub(crate) fn has_events(&self) -> bool {
        !self.events.is_empty()
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

    pub fn send_terminal(
        &self,
        request: TerminalOrderRequest,
        scope: Option<AccountScope>,
    ) -> Result<(), ClientError> {
        request
            .validate()
            .map_err(|_| ClientError::TerminalProtocol)?;
        let scope = self.terminal_scope(scope, &request.credential_id)?;
        #[cfg(not(target_arch = "wasm32"))]
        tracing::info!(target: "venueflow::terminal_latency", request_id = %request.request_id,
            "Terminal order queued locally");
        #[cfg(not(target_arch = "wasm32"))]
        crate::latency_evidence::submission(&request.request_id, "queued");
        self.terminal_order_tx
            .send(Scoped {
                scope,
                value: request,
            })
            .map_err(|_| ClientError::Closed)?;
        #[cfg(not(target_arch = "wasm32"))]
        self.terminal_wake.notify_one();
        Ok(())
    }

    pub fn send_terminal_cancel(
        &self,
        request: TerminalCancelRequest,
        scope: Option<AccountScope>,
    ) -> Result<(), ClientError> {
        request
            .validate()
            .map_err(|_| ClientError::TerminalProtocol)?;
        let scope = self.terminal_scope(scope, &request.credential_id)?;
        self.terminal_cancel_tx
            .send(Scoped {
                scope,
                value: request,
            })
            .map_err(|_| ClientError::Closed)?;
        #[cfg(not(target_arch = "wasm32"))]
        self.terminal_wake.notify_one();
        Ok(())
    }

    fn terminal_scope(
        &self,
        scope: Option<AccountScope>,
        credential: &str,
    ) -> Result<AccountScope, ClientError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self
            .terminal_projection_tx
            .borrow()
            .as_ref()
            .map(|r| &r.scope)
            != scope.as_ref()
        {
            return Err(ClientError::WriteGateClosed);
        }
        scope
            .filter(|s| {
                s.credential_id == credential
                    && !s.trading_account_id.is_empty()
                    && s.venue == venue_control_protocol::VenueId::Binance
            })
            .ok_or(ClientError::WriteGateClosed)
    }

    pub fn subscribe_terminal(&self, request: Scoped<TerminalProjectionRequest>) {
        if request.value.validate().is_ok() {
            #[cfg(not(target_arch = "wasm32"))]
            self.terminal_projection_tx.send_if_modified(|current| {
                if current.as_ref() == Some(&request) {
                    return false;
                }
                *current = Some(request);
                true
            });
        }
    }

    pub fn clear_terminal_subscription(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.terminal_projection_tx
            .send_if_modified(|current| current.take().is_some());
    }

    pub fn select_execution_scope(&self, scope: Option<UiAccountScope>) {
        self.stream_gates.select(scope);
    }

    pub fn send_position_action(
        &self,
        request: TerminalPositionActionRequest,
        scope: Option<AccountScope>,
    ) -> Result<(), ClientError> {
        request
            .validate()
            .map_err(|_| ClientError::TerminalProtocol)?;
        let scope = self.terminal_scope(scope, &request.credential_id)?;
        self.terminal_position_tx
            .try_send(Scoped {
                scope,
                value: request,
            })
            .map_err(|_| ClientError::Closed)?;
        #[cfg(not(target_arch = "wasm32"))]
        self.terminal_wake.notify_one();
        Ok(())
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
    terminal_orders: Receiver<Scoped<TerminalOrderRequest>>,
    terminal_cancellations: Receiver<Scoped<TerminalCancelRequest>>,
    terminal_positions: Receiver<Scoped<TerminalPositionActionRequest>>,
    terminal_wake: std::sync::Arc<tokio::sync::Notify>,
    terminal_projection: tokio::sync::watch::Receiver<Option<Scoped<TerminalProjectionRequest>>>,
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
                terminal_positions,
                terminal_wake,
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
    terminal_orders: Receiver<Scoped<TerminalOrderRequest>>,
    terminal_cancellations: Receiver<Scoped<TerminalCancelRequest>>,
    terminal_positions: Receiver<Scoped<TerminalPositionActionRequest>>,
    terminal_wake: std::sync::Arc<tokio::sync::Notify>,
    terminal_projection: tokio::sync::watch::Receiver<Option<Scoped<TerminalProjectionRequest>>>,
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
    let client = match crate::server_connection::http_client_builder(&endpoint)
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
            terminal::NativeTerminalQueues::new(
                terminal_orders,
                terminal_cancellations,
                terminal_positions,
                terminal_wake,
            ),
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
        match tokio::time::timeout(std::time::Duration::from_millis(100), scopes.recv()).await {
            Ok(Some(next_scopes)) => stream.gates.reconcile(next_scopes),
            Ok(None) => return,
            Err(_) => {}
        }
        for scope in stream.gates.desired() {
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
        let outcome = tokio::select! {
            outcome = native_event_stream(&stream, &scope, cursor) => outcome,
            _ = async {
                while !stop.load(Ordering::Acquire) && gates.is_desired(&scope) {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            } => break,
        };
        match outcome {
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
    let response = tokio::time::timeout(REQUEST_TIMEOUT, request.send())
        .await
        .map_err(|_| "event stream connection timed out".to_owned())?
        .map_err(|error| error.to_string())?;
    if stop.load(Ordering::Acquire) || !gates.is_desired(scope) {
        return Ok(StreamOutcome {
            cursor,
            made_progress: false,
        });
    }
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
        if stop.load(Ordering::Acquire) || !gates.is_desired(scope) {
            break;
        }
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
        if cfg!(feature = "preview") {
            return Self { stop };
        }
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
mod tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) struct MutationProbe {
    orders: Receiver<Scoped<TerminalOrderRequest>>,
    cancels: Receiver<Scoped<TerminalCancelRequest>>,
    positions: Receiver<Scoped<TerminalPositionActionRequest>>,
}
#[cfg(all(test, not(target_arch = "wasm32")))]
impl MutationProbe {
    pub fn count(&self) -> usize {
        self.orders.len() + self.cancels.len() + self.positions.len()
    }
    pub fn take_cancel(&self) -> Scoped<TerminalCancelRequest> {
        self.cancels.try_recv().unwrap()
    }
    pub fn take_order(&self) -> Scoped<TerminalOrderRequest> {
        self.orders.try_recv().unwrap()
    }
}
#[cfg(all(test, not(target_arch = "wasm32")))]
impl ControlClient {
    pub(crate) fn fixture() -> (Self, MutationProbe) {
        let (_event_tx, events) = unbounded();
        let (command_tx, _command_rx) = unbounded();
        let (terminal_order_tx, terminal_order_rx) = unbounded();
        let (terminal_cancel_tx, terminal_cancel_rx) = unbounded();
        let (terminal_position_tx, terminal_position_rx) = bounded(1);
        #[cfg(not(target_arch = "wasm32"))]
        let terminal_wake = std::sync::Arc::new(tokio::sync::Notify::new());
        #[cfg(not(target_arch = "wasm32"))]
        let (terminal_projection_tx, _terminal_projection_rx) = tokio::sync::watch::channel(None);
        let (copy_relation_tx, _copy_relation_rx) = unbounded();
        let (grid_mutation_tx, _grid_mutation_rx) = unbounded();
        let stream_gates = StreamGates::default();

        let client = Self {
            events,
            command_tx,
            terminal_order_tx,
            terminal_cancel_tx,
            terminal_position_tx,
            #[cfg(not(target_arch = "wasm32"))]
            terminal_wake,
            #[cfg(not(target_arch = "wasm32"))]
            terminal_projection_tx,
            copy_relation_tx,
            grid_mutation_tx,
            stream_gates,
            #[cfg(not(target_arch = "wasm32"))]
            stop: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(target_arch = "wasm32")]
            _web: web,
        };
        (
            client,
            MutationProbe {
                orders: terminal_order_rx,
                cancels: terminal_cancel_rx,
                positions: terminal_position_rx,
            },
        )
    }
}
