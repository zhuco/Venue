use std::{
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
    process::ExitCode,
};

use clap::{Parser, Subcommand};
use rust_decimal::Decimal;
use venue_domain::domain::{
    CancelCommand, CommandId, ExecutionCommand, OrderCommand, OrderOwner, OrderPurpose, OrderSide,
    PositionSide, Price, Symbol,
};
use venue_gateway_api::{CapabilityFlags, GatewayApiError, GatewayBinding, GatewayMode, VenueId};
use venue_runtime::{
    AccountPhysicalGateway, StrategyBinding, StrategyInstanceKey, StrategyKind, account::AccountKey,
};

mod control_delivery;
mod control_delivery_storage;
mod control_http_client;
mod control_loop;
mod control_shutdown_journal;
mod copy_delivery_journal;
mod copy_semantic;
mod production_resident;
mod projection_outbox;
mod resident;
mod runtime_config;

#[cfg(test)]
mod control_delivery_tests;

pub(crate) use control_delivery::PersistedCopyActorTurn;
pub use control_delivery::{
    ActorDeliveryCompletion, ActorDeliveryTurn, ClaimAcceptance, ControlDeliveryError,
    ControlDeliveryInbox, ControlDeliveryJournal, ControlDeliveryJournalError,
    ControlDeliveryJournalRecord, DurableDeliveryOutput, DurableStoreResult,
    ReconciliationCompletion, ReconciliationTurn,
};
pub use control_delivery_storage::OpaqueControlDeliveryJournal;
pub use control_http_client::{
    ControlDeliveryDriver, ControlDeliveryDriverError, ControlDeliveryWork, ControlHttpClient,
    ControlHttpClientConfig, ControlHttpClientError, MAX_CONTROL_CLAIM_LIMIT,
    MAX_CONTROL_HTTP_REQUEST_BYTES, MAX_CONTROL_HTTP_RESPONSE_BYTES, MAX_CONTROL_HTTP_TIMEOUT,
    MAX_CONTROL_LEASE_DURATION_MS,
};
pub use control_loop::{ControlResidentLoop, ControlResidentLoopError};
pub(crate) use copy_delivery_journal::{CopyDeliveryJournal, CopyDeliveryJournalError};
pub use copy_semantic::{CopySemanticDelivery, CopySemanticError, FreshCopyCommandFacts};
pub use production_resident::{ProductionResident, ResidentCopyReconciliation, ResidentCopyResult};
pub use projection_outbox::{NodeProjectionOutbox, NodeProjectionOutboxError};
pub use resident::{
    GridResidentActor, ResidentActorKind, ResidentControlDelivery, ResidentError, ResidentFact,
    ResidentLoop, ResidentRecoveryState, ResidentSemanticIntent, ScalpingResidentActor,
};
pub use runtime_config::{
    NODE_RUNTIME_CONFIG_VERSION, NodeControlLoopConfig, NodeGridRecoveryPolicy,
    NodeGridRuntimeConfig, NodeRuntimeConfig, NodeRuntimeStrategy, NodeScalpingRuntimeConfig,
};

pub use venue_control_protocol::{CommandReceipt, ControlAction, ControlCommandRequest};

const REQUIRED_NEW_VENUE_GATES: &str = "Owner, WAL, unique account writer fence, signed readback, UNKNOWN reconciliation, Stop/Flatten, and operator-confirmed Canary evidence";
const LIVE_ARTIFACT_FILE_HARD_LIMIT_BYTES: u64 = 10 * 1024 * 1024;
const LIVE_ARTIFACT_ROOT_FREEZE_BYTES: u64 = 240 * 1024 * 1024;
// The import marker itself is small, but reserve a full MiB so the preflight bound remains
// conservative if the frozen journal has many 5 MiB segments.
const LEGACY_V1_IMPORT_METADATA_RESERVATION_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "venue-node", disable_version_flag = true)]
struct RawNodeArguments {
    #[arg(long, value_parser = parse_exact_mode)]
    mode: GatewayMode,

    #[arg(long)]
    trading_account_id: String,

    #[arg(long)]
    symbol: Symbol,

    /// Absolute base. The node derives <base>/<venue>/LIVE/<account> and never accepts an
    /// operator-supplied final root.
    #[arg(long)]
    artifacts_base: PathBuf,

    /// Required only for frozen Stage-7 Binance/Gate/Bitget scopes. The file is a strict,
    /// durable predecessor record; its `handoff_sha256` commits the exact v1 registry entry.
    #[arg(long)]
    legacy_v1_handoff: Option<PathBuf>,

    /// The fixed Node resident subcommand and arguments, supplied after `--`.
    #[arg(last = true, allow_hyphen_values = true)]
    runtime_arguments: Vec<OsString>,
}

/// Secret-free launch scope shared by all six fixed node binaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLaunch {
    binding: GatewayBinding,
    artifacts_base: PathBuf,
    legacy_v1_predecessor: Option<venue_runtime::LegacyV1WriterPredecessor>,
    runtime_arguments: Vec<OsString>,
}

impl NodeLaunch {
    pub fn from_environment(expected_venue: VenueId) -> Result<Self, NodeError> {
        Self::try_parse_from(expected_venue, std::env::args_os())
    }

    pub fn try_parse_from<I, T>(expected_venue: VenueId, arguments: I) -> Result<Self, NodeError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let raw = RawNodeArguments::try_parse_from(arguments)?;
        validate_artifacts_base(&raw.artifacts_base)?;
        let binding =
            GatewayBinding::new(expected_venue, raw.mode, raw.trading_account_id, raw.symbol)?;
        let legacy_v1_predecessor = raw
            .legacy_v1_handoff
            .as_deref()
            .map(load_legacy_v1_predecessor)
            .transpose()?;
        let legacy_required = matches!(
            expected_venue,
            VenueId::Binance | VenueId::Gate | VenueId::Bitget
        );
        if legacy_required != legacy_v1_predecessor.is_some() {
            return Err(NodeError::LegacyPredecessor);
        }
        if legacy_v1_predecessor
            .as_ref()
            .is_some_and(|predecessor| predecessor.exchange != expected_venue)
        {
            return Err(NodeError::LegacyPredecessor);
        }
        Ok(Self {
            binding,
            artifacts_base: raw.artifacts_base,
            legacy_v1_predecessor,
            runtime_arguments: raw.runtime_arguments,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub fn artifacts_root(&self) -> PathBuf {
        self.artifacts_base
            .join(self.binding.venue.as_str())
            .join(self.binding.mode.as_str())
            .join(&self.binding.trading_account_id)
    }

    #[must_use]
    pub const fn legacy_v1_predecessor(&self) -> Option<&venue_runtime::LegacyV1WriterPredecessor> {
        self.legacy_v1_predecessor.as_ref()
    }

    pub fn require_no_runtime_arguments(&self) -> Result<(), NodeError> {
        if self.runtime_arguments.is_empty() {
            Ok(())
        } else {
            Err(NodeError::RuntimeArguments)
        }
    }

    pub fn live_mvp_command(&self) -> Result<LiveMvpCommand, NodeError> {
        let arguments = std::iter::once(OsString::from("venue-live"))
            .chain(self.runtime_arguments.iter().cloned());
        let raw = RawLiveMvpArguments::try_parse_from(arguments)?;
        let command = LiveMvpCommand::from_raw(raw.command, &self.binding)?;
        match self.legacy_v1_predecessor.as_ref() {
            Some(predecessor) => {
                validate_legacy_v1_import_budget(&self.artifacts_base, predecessor)?;
            }
            None => validate_live_artifact_budget(&self.artifacts_base)?,
        }
        Ok(command)
    }

    pub fn validate_runtime_scope(
        &self,
        trading_account_id: &str,
        symbol: &Symbol,
    ) -> Result<(), NodeError> {
        if self.binding.trading_account_id == trading_account_id && &self.binding.symbol == symbol {
            Ok(())
        } else {
            Err(NodeError::RuntimeScope)
        }
    }
}

#[derive(Debug, Parser)]
struct RawLiveMvpArguments {
    #[command(subcommand)]
    command: RawLiveMvpCommand,
}

#[derive(Debug, Subcommand)]
enum RawLiveMvpCommand {
    Run {
        #[arg(long)]
        runtime_config: PathBuf,
    },
    /// Authenticated read-only startup plus account-WAL recovery; sends no mutation.
    Preflight {
        #[arg(long)]
        confirm_live: String,
    },
    /// One post-only entry with a fixed 10 USDT nominal ceiling.
    CanaryPlace {
        #[arg(long)]
        confirm_live: String,
        #[arg(long)]
        command_id: String,
        #[arg(long)]
        client_order_id: String,
        #[arg(long, value_parser = parse_position_side)]
        position_side: PositionSide,
        #[arg(long)]
        quantity: Decimal,
        #[arg(long)]
        limit_price: Decimal,
    },
    /// One exact cancel by the prior durable client identity.
    CanaryCancel {
        #[arg(long)]
        confirm_live: String,
        #[arg(long)]
        command_id: String,
        #[arg(long)]
        target_client_order_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveMvpCommand {
    Run(PathBuf),
    Preflight,
    Dispatch(Box<ExecutionCommand>),
}

impl LiveMvpCommand {
    fn from_raw(raw: RawLiveMvpCommand, binding: &GatewayBinding) -> Result<Self, NodeError> {
        match raw {
            RawLiveMvpCommand::Run { runtime_config } => {
                if !runtime_config.is_absolute() {
                    return Err(NodeError::RuntimeConfig);
                }
                Ok(Self::Run(runtime_config))
            }
            RawLiveMvpCommand::Preflight { confirm_live } => {
                validate_live_confirmation(&confirm_live, binding.venue)?;
                Ok(Self::Preflight)
            }
            RawLiveMvpCommand::CanaryPlace {
                confirm_live,
                command_id,
                client_order_id,
                position_side,
                quantity,
                limit_price,
            } => {
                validate_live_confirmation(&confirm_live, binding.venue)?;
                let side = match position_side {
                    PositionSide::Long => OrderSide::Buy,
                    PositionSide::Short => OrderSide::Sell,
                    PositionSide::Net => return Err(NodeError::LiveCommand),
                };
                let command = ExecutionCommand::PlaceLimit(OrderCommand {
                    time_in_force: Default::default(),
                    command_id: CommandId::new(command_id).map_err(|_| NodeError::LiveCommand)?,
                    client_order_id: CommandId::new(client_order_id)
                        .map_err(|_| NodeError::LiveCommand)?,
                    owner: live_owner(binding, OrderPurpose::Entry),
                    side,
                    position_side,
                    quantity,
                    limit_price: Price::new(limit_price).map_err(|_| NodeError::LiveCommand)?,
                    reduce_only: false,
                });
                command.validate().map_err(|_| NodeError::LiveCommand)?;
                Ok(Self::Dispatch(Box::new(command)))
            }
            RawLiveMvpCommand::CanaryCancel {
                confirm_live,
                command_id,
                target_client_order_id,
            } => {
                validate_live_confirmation(&confirm_live, binding.venue)?;
                let command = ExecutionCommand::Cancel(CancelCommand {
                    command_id: CommandId::new(command_id).map_err(|_| NodeError::LiveCommand)?,
                    owner: live_owner(binding, OrderPurpose::Entry),
                    target_client_order_id: CommandId::new(target_client_order_id)
                        .map_err(|_| NodeError::LiveCommand)?,
                });
                command.validate().map_err(|_| NodeError::LiveCommand)?;
                Ok(Self::Dispatch(Box::new(command)))
            }
        }
    }
}

fn live_owner(binding: &GatewayBinding, purpose: OrderPurpose) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: "canary".to_owned(),
        run_id: "manual-canary".to_owned(),
        exchange: binding.venue.as_str().to_owned(),
        account: binding.trading_account_id.clone(),
        symbol: binding.symbol.clone(),
        purpose,
    }
}

fn validate_live_confirmation(value: &str, venue: VenueId) -> Result<(), NodeError> {
    if value == venue.as_str() {
        Ok(())
    } else {
        Err(NodeError::LiveConfirmation)
    }
}

fn parse_position_side(value: &str) -> Result<PositionSide, &'static str> {
    match value {
        "long" => Ok(PositionSide::Long),
        "short" => Ok(PositionSide::Short),
        _ => Err("position-side must be exactly long or short"),
    }
}

/// Runtime-used adapter metadata. It proves only endpoint and credential namespace isolation; it
/// deliberately cannot represent capability, writer, WAL, or dispatch authority.
pub struct AdapterIsolation<'a> {
    pub venue: VenueId,
    pub mode: GatewayMode,
    pub endpoints: &'a [&'static str],
    pub credential_environment: &'a [&'static str],
    pub credential_prefix: &'static str,
    pub account_binding: &'static str,
}

impl AdapterIsolation<'_> {
    pub fn validate(&self, binding: &GatewayBinding) -> Result<(), NodeError> {
        if binding.venue != self.venue
            || binding.mode != self.mode
            || self.endpoints.is_empty()
            || self.credential_environment.is_empty()
            || self.account_binding.trim().is_empty()
        {
            return Err(NodeError::AdapterIsolation(self.venue));
        }
        if self
            .endpoints
            .iter()
            .any(|endpoint| !endpoint.starts_with("https://") && !endpoint.starts_with("wss://"))
        {
            return Err(NodeError::AdapterIsolation(self.venue));
        }
        if self.credential_environment.iter().any(|name| {
            !name.starts_with(self.credential_prefix)
                || name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        }) {
            return Err(NodeError::AdapterIsolation(self.venue));
        }
        for (index, name) in self.credential_environment.iter().enumerate() {
            if self.credential_environment[index + 1..].contains(name) {
                return Err(NodeError::AdapterIsolation(self.venue));
            }
        }
        Ok(())
    }
}

/// The three new adapters remain non-executable until the shared runtime supplies every listed
/// proof. Even an adapter that starts advertising flags cannot silently turn this package on.
pub fn reject_unintegrated_runtime(
    venue: VenueId,
    mode: GatewayMode,
    capabilities: CapabilityFlags,
) -> Result<(), NodeError> {
    if !capabilities.is_empty() {
        return Err(NodeError::UnexpectedAdapterCapability(venue));
    }
    Err(NodeError::IncompleteSafetyClosure {
        venue,
        mode,
        missing: REQUIRED_NEW_VENUE_GATES,
    })
}

#[must_use]
pub fn report_result(program: &str, result: Result<(), NodeError>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{program}: {error}");
            ExitCode::FAILURE
        }
    }
}

pub fn load_root_dotenv() -> Result<(), NodeError> {
    dotenvy::from_filename(".env")
        .map(|_| ())
        .map_err(|_| NodeError::Dotenv)
}

pub fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    for _ in 0..4 {
        let Some(next) = source else {
            break;
        };
        message.push_str(": ");
        message.push_str(&next.to_string());
        source = next.source();
    }
    message
}

fn run_live_mvp_with_loop<G, F>(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: G,
    run_loop: F,
    public_stream_venue: Option<VenueId>,
) -> Result<(), NodeError>
where
    G: AccountPhysicalGateway,
    F: FnOnce(ControlResidentLoop<G>) -> Result<(), NodeError>,
{
    let venue = launch.binding().venue;
    match command {
        LiveMvpCommand::Run(runtime_config) => {
            let config = NodeRuntimeConfig::load(&runtime_config, launch.binding())?;
            reject_scalping_without_public_stream(&config, public_stream_venue)?;
            let mut resident = ProductionResident::open_with_symbols(
                launch,
                config.configured_symbols(launch.binding())?,
                gateway,
            )?;
            for strategy in &config.strategies {
                let binding = config.binding_for(strategy)?;
                match strategy.strategy_kind {
                    StrategyKind::HedgedGrid => {
                        let grid = strategy.grid.as_ref().ok_or(NodeError::RuntimeConfig)?;
                        resident.register_grid_actor(
                            binding,
                            config.grid_initial_state(strategy)?,
                            grid.recovery,
                            grid.skip_inventory_replenishment_until_recovered,
                        )?;
                    }
                    StrategyKind::Scalping => resident
                        .register_scalping_actor(binding, config.scalping_binding_for(strategy)?)?,
                    StrategyKind::Copy => resident.register_actor(binding)?,
                }
            }
            let loopback =
                ControlResidentLoop::open(launch, &config, resident).map_err(|error| {
                    NodeError::LiveHost {
                        venue,
                        message: error.to_string(),
                    }
                })?;
            run_loop(loopback)
        }
        LiveMvpCommand::Preflight => {
            let resident = ProductionResident::open(launch, gateway)?;
            if resident.has_unresolved() {
                return Err(NodeError::LiveHost {
                    venue,
                    message: "signed recovery left an unresolved mutation".to_owned(),
                });
            }
            println!("{venue} LIVE preflight passed; no mutation sent");
            Ok(())
        }
        LiveMvpCommand::Dispatch(command) => {
            let mut resident = ProductionResident::open(launch, gateway)?;
            let account = AccountKey::new(
                launch.binding().venue,
                launch.binding().trading_account_id.clone(),
            )
            .map_err(|error| NodeError::LiveHost {
                venue,
                message: error.to_string(),
            })?;
            let key = StrategyInstanceKey::new(
                account,
                StrategyKind::Scalping,
                "canary",
                launch.binding().symbol.clone(),
            )
            .map_err(|error| NodeError::LiveHost {
                venue,
                message: error.to_string(),
            })?;
            let binding = StrategyBinding::new(key, "manual-canary", "canary-runtime-v1").map_err(
                |error| NodeError::LiveHost {
                    venue,
                    message: error.to_string(),
                },
            )?;
            resident.register_actor(binding.clone())?;
            resident.submit_operator_command(&binding, *command)
        }
    }
}

/// Admission follows the selected fixed receiver, not merely the configured venue name.
/// A generic signed-account pump supplies no public feed even when its adapter supports one.
fn reject_scalping_without_public_stream(
    config: &NodeRuntimeConfig,
    public_stream_venue: Option<VenueId>,
) -> Result<(), NodeError> {
    if config.has_scalping_strategy() && public_stream_venue != Some(config.venue) {
        return Err(NodeError::ScalpingPublicStreamUnavailable {
            venue: config.venue,
        });
    }
    Ok(())
}

pub fn run_live_mvp<G: AccountPhysicalGateway>(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: G,
) -> Result<(), NodeError> {
    run_live_mvp_with_loop(launch, command, gateway, ControlResidentLoop::run, None)
}

// The fixed non-Binance binaries name their resident route explicitly.  The generic loop pumps
// only Host-persisted complete signed account facts; it does not recast an adapter's one-shot BBO
// as a market-book stream or expose an adapter mutation handle.
#[cfg(feature = "bitget")]
pub fn run_live_bitget_mvp(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: venue_gateway_bitget::BitgetAccountGateway,
) -> Result<(), NodeError> {
    run_live_mvp_with_loop(
        launch,
        command,
        gateway,
        ControlResidentLoop::run_bitget,
        Some(VenueId::Bitget),
    )
}

#[cfg(feature = "bybit")]
pub fn run_live_bybit_mvp(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: venue_gateway_bybit::BybitAccountGateway,
) -> Result<(), NodeError> {
    run_live_mvp_with_loop(
        launch,
        command,
        gateway,
        ControlResidentLoop::run_bybit,
        Some(VenueId::Bybit),
    )
}

#[cfg(feature = "gate")]
pub fn run_live_gate_mvp(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: venue_gateway_gate::GateAccountGateway,
) -> Result<(), NodeError> {
    run_live_mvp_with_loop(
        launch,
        command,
        gateway,
        ControlResidentLoop::run_gate,
        Some(VenueId::Gate),
    )
}

#[cfg(feature = "hyperliquid")]
pub fn run_live_hyperliquid_mvp(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: venue_gateway_hyperliquid::HyperliquidAccountGateway,
) -> Result<(), NodeError> {
    run_live_mvp_with_loop(
        launch,
        command,
        gateway,
        ControlResidentLoop::run_hyperliquid,
        Some(VenueId::Hyperliquid),
    )
}

#[cfg(feature = "okx")]
pub fn run_live_okx_mvp(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: venue_gateway_okx::OkxAccountGateway,
) -> Result<(), NodeError> {
    run_live_mvp_with_loop(
        launch,
        command,
        gateway,
        ControlResidentLoop::run_okx,
        Some(VenueId::Okx),
    )
}

#[cfg(feature = "binance")]
pub fn run_live_binance_mvp(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: venue_gateway_binance::BinanceAccountGateway,
) -> Result<(), NodeError> {
    run_live_mvp_with_loop(
        launch,
        command,
        gateway,
        ControlResidentLoop::run_binance,
        Some(VenueId::Binance),
    )
}

fn parse_exact_mode(raw: &str) -> Result<GatewayMode, &'static str> {
    match raw {
        "LIVE" => Ok(GatewayMode::Live),
        _ => Err("gateway mode must be exactly LIVE"),
    }
}

fn validate_artifacts_base(path: &Path) -> Result<(), NodeError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(NodeError::ArtifactsBase);
    }
    Ok(())
}

fn load_legacy_v1_predecessor(
    path: &Path,
) -> Result<venue_runtime::LegacyV1WriterPredecessor, NodeError> {
    if !path.is_absolute() {
        return Err(NodeError::LegacyPredecessor);
    }
    let encoded = fs::read(path).map_err(|_| NodeError::LegacyPredecessor)?;
    let predecessor: venue_runtime::LegacyV1WriterPredecessor =
        serde_json::from_slice(&encoded).map_err(|_| NodeError::LegacyPredecessor)?;
    predecessor
        .validate()
        .map_err(|_| NodeError::LegacyPredecessor)?;
    Ok(predecessor)
}

fn validate_live_artifact_budget(root: &Path) -> Result<(), NodeError> {
    validate_live_artifact_budget_with_reservation(root, 0)
}

fn validate_legacy_v1_import_budget(
    artifacts_base: &Path,
    predecessor: &venue_runtime::LegacyV1WriterPredecessor,
) -> Result<(), NodeError> {
    let journal = predecessor.legacy_artifacts_root.join("commands.jsonl");
    let metadata = fs::symlink_metadata(&journal).map_err(|_| NodeError::LegacyPredecessor)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() == 0 {
        return Err(NodeError::LegacyPredecessor);
    }
    let reservation = metadata
        .len()
        .checked_add(LEGACY_V1_IMPORT_METADATA_RESERVATION_BYTES)
        .ok_or(NodeError::ArtifactsBudget)?;
    validate_live_artifact_budget_with_reservation(artifacts_base, reservation)
}

fn validate_live_artifact_budget_with_reservation(
    root: &Path,
    reservation_bytes: u64,
) -> Result<(), NodeError> {
    if !root.exists() {
        if reservation_bytes >= LIVE_ARTIFACT_ROOT_FREEZE_BYTES {
            return Err(NodeError::ArtifactsBudget);
        }
        return Ok(());
    }
    let mut total = 0_u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory).map_err(|_| NodeError::ArtifactsBudget)?;
        for entry in entries {
            let entry = entry.map_err(|_| NodeError::ArtifactsBudget)?;
            let file_type = entry.file_type().map_err(|_| NodeError::ArtifactsBudget)?;
            if file_type.is_symlink() {
                return Err(NodeError::ArtifactsBudget);
            }
            if file_type.is_dir() {
                pending.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                return Err(NodeError::ArtifactsBudget);
            }
            let size = entry
                .metadata()
                .map_err(|_| NodeError::ArtifactsBudget)?
                .len();
            if size > LIVE_ARTIFACT_FILE_HARD_LIMIT_BYTES {
                return Err(NodeError::ArtifactsBudget);
            }
            total = total
                .checked_add(size)
                .and_then(|used| used.checked_add(reservation_bytes))
                .ok_or(NodeError::ArtifactsBudget)?;
            if total >= LIVE_ARTIFACT_ROOT_FREEZE_BYTES {
                return Err(NodeError::ArtifactsBudget);
            }
            total = total
                .checked_sub(reservation_bytes)
                .ok_or(NodeError::ArtifactsBudget)?;
        }
    }
    if total
        .checked_add(reservation_bytes)
        .is_none_or(|used| used >= LIVE_ARTIFACT_ROOT_FREEZE_BYTES)
    {
        return Err(NodeError::ArtifactsBudget);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error(transparent)]
    Cli(#[from] clap::Error),
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
    #[error("artifacts base must be an absolute lexical path without '.' or '..'")]
    ArtifactsBase,
    #[error("LIVE artifacts exceed the 10 MiB file or 240 MiB freeze budget")]
    ArtifactsBudget,
    #[error(
        "legacy v1 predecessor handoff is required only for Binance, Gate, and Bitget and must validate exactly"
    )]
    LegacyPredecessor,
    #[error("fixed {0} node adapter isolation metadata is invalid")]
    AdapterIsolation(VenueId),
    #[error("node identity does not match the runtime configuration")]
    RuntimeScope,
    #[error(
        "runtime arguments must select exactly one fixed deployment command after '--' and must not contain --artifacts-root"
    )]
    RuntimeArguments,
    #[error(
        "LIVE command must be preflight, canary-place, or canary-cancel with valid bounded fields"
    )]
    LiveCommand,
    #[error("--confirm-live must exactly match the lowercase venue id")]
    LiveConfirmation,
    #[error("{venue} production gateway preflight failed: {message}")]
    LiveGateway { venue: VenueId, message: String },
    #[error("{venue} account writer/WAL host failed closed: {message}")]
    LiveHost { venue: VenueId, message: String },
    #[error(
        "resident Actor-applied artifacts are absent, incomplete, or do not match their durable anchor"
    )]
    ResidentArtifacts,
    #[error("resident Runtime rejected the durable actor turn or execution-lane admission")]
    ResidentRuntime,
    #[error("runtime config is missing, malformed, cross-account, stale, or unsafe")]
    RuntimeConfig,
    #[error(
        "{venue} Run configuration contains Scalping, but this adapter has no lifecycle-managed sequenced public stream receiver"
    )]
    ScalpingPublicStreamUnavailable { venue: VenueId },
    #[error(
        "{venue} operator canary was denied: current AccountRuntime recovery, configuration, signed turn, and durable identity admission are required before its execution lane may dispatch"
    )]
    RuntimeLaneAdmissionRequired { venue: VenueId },
    #[error("root .env could not be loaded")]
    Dotenv,
    #[error("{0} adapter advertised capability before the shared safety closure was integrated")]
    UnexpectedAdapterCapability(VenueId),
    #[error("{venue} {mode} node is fail-closed; missing {missing}")]
    IncompleteSafetyClosure {
        venue: VenueId,
        mode: GatewayMode,
        missing: &'static str,
    },
    #[error("existing {venue} runtime rejected launch: {message}")]
    ExistingRuntime { venue: VenueId, message: String },
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use venue_runtime::{
        AccountGatewayResult, AccountHostValidationError, AccountRecoveryReport,
        AccountRecoveryRequest, AccountRiskEvidence, SignedAccountOrderFact,
        SignedAccountPositionMode, SignedAccountSnapshot,
    };

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    fn base() -> PathBuf {
        std::env::temp_dir().join("venue-node-tests")
    }

    fn arguments(mode: &str) -> Vec<OsString> {
        vec![
            OsString::from("venue-node-bybit"),
            OsString::from("--mode"),
            OsString::from(mode),
            OsString::from("--trading-account-id"),
            OsString::from(ACCOUNT),
            OsString::from("--symbol"),
            OsString::from("BTC/USDT"),
            OsString::from("--artifacts-base"),
            base().into_os_string(),
        ]
    }

    fn live_arguments(command: &[&str]) -> Vec<OsString> {
        let mut raw = arguments("LIVE");
        raw.push(OsString::from("--"));
        raw.extend(command.iter().map(OsString::from));
        raw
    }

    #[test]
    fn node_mode_is_exactly_live() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            NodeLaunch::try_parse_from(VenueId::Bybit, arguments("LIVE"))?
                .binding()
                .mode,
            GatewayMode::Live
        );
        for rejected in ["TEST", "test", "live", "SHADOW", " LIVE ", ""] {
            assert!(NodeLaunch::try_parse_from(VenueId::Bybit, arguments(rejected)).is_err());
        }
        Ok(())
    }

    #[test]
    fn node_manifest_cannot_name_the_mutation_host_crate() {
        assert!(!include_str!("../Cargo.toml").contains("venue-execution"));
    }

    #[test]
    fn artifact_roots_are_disjoint_by_venue_and_account() -> Result<(), Box<dyn std::error::Error>>
    {
        let live = NodeLaunch::try_parse_from(VenueId::Bybit, arguments("LIVE"))?;
        let okx = NodeLaunch::try_parse_from(VenueId::Okx, arguments("LIVE"))?;
        let mut other_account = arguments("LIVE");
        other_account[4] = OsString::from("00000000-0000-4000-8000-000000000002");
        let other_account = NodeLaunch::try_parse_from(VenueId::Bybit, other_account)?;

        assert_ne!(live.artifacts_root(), okx.artifacts_root());
        assert_ne!(live.artifacts_root(), other_account.artifacts_root());
        assert!(live.artifacts_root().ends_with(Path::new(ACCOUNT)));
        Ok(())
    }

    #[test]
    fn unintegrated_adapter_is_always_fail_closed() {
        for venue in [VenueId::Bybit, VenueId::Okx, VenueId::Hyperliquid] {
            assert!(matches!(
                reject_unintegrated_runtime(venue, GatewayMode::Live, CapabilityFlags::empty()),
                Err(NodeError::IncompleteSafetyClosure { .. })
            ));
            assert!(matches!(
                reject_unintegrated_runtime(venue, GatewayMode::Live, CapabilityFlags::READ_ACCOUNT),
                Err(NodeError::UnexpectedAdapterCapability(rejected)) if rejected == venue
            ));
        }
    }

    #[test]
    fn live_preflight_requires_exact_venue_confirmation() -> Result<(), Box<dyn std::error::Error>>
    {
        let launch = NodeLaunch::try_parse_from(
            VenueId::Bybit,
            live_arguments(&["preflight", "--confirm-live", "bybit"]),
        )?;
        assert_eq!(launch.live_mvp_command()?, LiveMvpCommand::Preflight);

        let rejected = NodeLaunch::try_parse_from(
            VenueId::Bybit,
            live_arguments(&["preflight", "--confirm-live", "okx"]),
        )?;
        assert!(matches!(
            rejected.live_mvp_command(),
            Err(NodeError::LiveConfirmation)
        ));
        Ok(())
    }

    #[test]
    fn live_place_builds_owner_bound_command() -> Result<(), Box<dyn std::error::Error>> {
        let launch = NodeLaunch::try_parse_from(
            VenueId::Okx,
            live_arguments(&[
                "canary-place",
                "--confirm-live",
                "okx",
                "--command-id",
                "cmd-1",
                "--client-order-id",
                "order-1",
                "--position-side",
                "long",
                "--quantity",
                "110",
                "--limit-price",
                "0.08503",
            ]),
        )?;
        let LiveMvpCommand::Dispatch(command) = launch.live_mvp_command()? else {
            return Err("expected place command".into());
        };
        let ExecutionCommand::PlaceLimit(command) = *command else {
            return Err("expected place command".into());
        };
        assert_eq!(command.owner.exchange, "okx");
        assert_eq!(command.owner.account, ACCOUNT);
        assert_eq!(command.quantity, Decimal::from(110));
        assert_eq!(command.limit_price.value(), Decimal::new(8503, 5));
        Ok(())
    }

    #[test]
    fn live_command_rejects_oversized_artifact_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let oversized = std::fs::File::create(temp.path().join("oversized.jsonl"))?;
        oversized.set_len(LIVE_ARTIFACT_FILE_HARD_LIMIT_BYTES + 1)?;
        let mut raw = arguments("LIVE");
        raw[8] = temp.path().as_os_str().to_owned();
        raw.extend([
            OsString::from("--"),
            OsString::from("preflight"),
            OsString::from("--confirm-live"),
            OsString::from("bybit"),
        ]);
        let launch = NodeLaunch::try_parse_from(VenueId::Bybit, raw)?;
        assert!(matches!(
            launch.live_mvp_command(),
            Err(NodeError::ArtifactsBudget)
        ));
        Ok(())
    }

    #[test]
    fn legacy_import_reservation_rejects_artifact_budget_breach()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let artifacts = temp.path().join("artifacts");
        std::fs::create_dir_all(&artifacts)?;
        let existing = std::fs::File::create(artifacts.join("current.jsonl"))?;
        existing.set_len(LIVE_ARTIFACT_ROOT_FREEZE_BYTES - 512)?;
        let legacy = temp.path().join("legacy");
        std::fs::create_dir_all(&legacy)?;
        let journal = std::fs::File::create(legacy.join("commands.jsonl"))?;
        journal.set_len(1024)?;
        let predecessor = venue_runtime::LegacyV1WriterPredecessor {
            exchange: VenueId::Binance,
            successor_trading_account_id: ACCOUNT.to_owned(),
            legacy_product_account: "stage7-binance-doge".to_owned(),
            legacy_symbol: "DOGE/USDT".parse()?,
            legacy_owner_scope: "hedged-grid".to_owned(),
            legacy_strategy_instance_id: "hedged-grid".to_owned(),
            legacy_run_id: "primary".to_owned(),
            legacy_artifacts_root: legacy,
            legacy_lock_sha256: "0".repeat(64),
            legacy_lock_path: temp.path().join("legacy.lock"),
            handoff_sha256: "0".repeat(64),
        };

        assert!(matches!(
            validate_legacy_v1_import_budget(&artifacts, &predecessor),
            Err(NodeError::ArtifactsBudget)
        ));
        Ok(())
    }

    fn scalping_runtime_config(
        venue: VenueId,
    ) -> Result<NodeRuntimeConfig, Box<dyn std::error::Error>> {
        Ok(NodeRuntimeConfig {
            version: NODE_RUNTIME_CONFIG_VERSION,
            mode: GatewayMode::Live,
            venue,
            trading_account_id: ACCOUNT.to_owned(),
            node_id: "stream-fence".to_owned(),
            control: NodeControlLoopConfig {
                loopback_origin: "http://127.0.0.1:8080/".to_owned(),
                poll_interval_ms: 100,
                projection_interval_ms: 100,
                lease_duration_ms: 1_000,
                claim_limit: 1,
            },
            strategies: vec![NodeRuntimeStrategy {
                strategy_kind: StrategyKind::Scalping,
                instance_id: "scalping-doge".to_owned(),
                run_id: "stream-fence".to_owned(),
                config_digest: "stream-fence-v1".to_owned(),
                config_epoch: 1,
                symbol: "DOGE/USDT".parse()?,
                grid: None,
                scalping: Some(NodeScalpingRuntimeConfig {
                    parameter_release_id: "scalping-shadow-v1".to_owned(),
                    owner_scope: "scalping-doge".to_owned(),
                    risk_budget: venue_domain::Amount::new(
                        venue_domain::Asset::new("USDT")?,
                        rust_decimal::Decimal::TEN,
                    ),
                }),
                copy_leader_capital: None,
            }],
        })
    }

    #[test]
    fn run_config_fails_closed_when_scalping_has_no_public_stream_receiver()
    -> Result<(), Box<dyn std::error::Error>> {
        for venue in VenueId::ALL {
            let config = scalping_runtime_config(venue)?;
            assert!(matches!(
                reject_scalping_without_public_stream(&config, None),
                Err(NodeError::ScalpingPublicStreamUnavailable { venue: rejected })
                    if rejected == venue
            ));
            assert!(reject_scalping_without_public_stream(&config, Some(venue)).is_ok());
            let other = if venue == VenueId::Binance {
                VenueId::Okx
            } else {
                VenueId::Binance
            };
            assert!(reject_scalping_without_public_stream(&config, Some(other)).is_err());
        }

        let mut no_scalping = scalping_runtime_config(VenueId::Gate)?;
        no_scalping.strategies[0].strategy_kind = StrategyKind::Copy;
        assert!(reject_scalping_without_public_stream(&no_scalping, None).is_ok());
        Ok(())
    }

    struct LaneProofGateway {
        binding: GatewayBinding,
        dispatches: Arc<AtomicUsize>,
        signed_generations: Arc<AtomicUsize>,
        accepted: bool,
    }

    impl AccountPhysicalGateway for LaneProofGateway {
        type Error = io::Error;

        fn binding(&self) -> &GatewayBinding {
            &self.binding
        }

        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            if request.binding() != &self.binding || !request.unresolved().is_empty() {
                return Err(io::Error::other("unexpected recovery scope"));
            }
            AccountRecoveryReport::new(self.binding.clone(), 1, Vec::new())
                .map_err(io::Error::other)
        }

        fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
            let generation = self
                .signed_generations
                .load(Ordering::SeqCst)
                .max(1)
                .try_into()
                .map_err(|_| AccountHostValidationError::RiskEvidence)?;
            AccountRiskEvidence::complete(
                self.binding.clone(),
                now_ms(),
                generation,
                Vec::new(),
                Vec::new(),
            )
        }

        fn signed_account_snapshot(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
            let generation = self
                .signed_generations
                .fetch_add(1, Ordering::SeqCst)
                .saturating_add(1)
                .try_into()
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let orders = (self.accepted && self.dispatches.load(Ordering::SeqCst) != 0)
                .then(|| SignedAccountOrderFact {
                    client_order_id: "lane-client".to_owned(),
                    venue_order_id: Some("lane-venue".to_owned()),
                    symbol: self.binding.symbol.clone(),
                    family: venue_domain::NativeOrderFamily::UmOrder,
                    side: OrderSide::Buy,
                    position_side: PositionSide::Long,
                    quantity: Decimal::ONE,
                    limit_price: Some(Decimal::ONE),
                    time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
                    created_at_ms: Some(now_ms()),
                    reduce_only: false,
                    owner: None,
                    external: true,
                    state: Some(venue_domain::OrderState::New),
                    filled_quantity: Some(Decimal::ZERO),
                })
                .into_iter()
                .collect();
            SignedAccountSnapshot::complete(
                self.binding.clone(),
                now_ms(),
                1,
                generation,
                1,
                SignedAccountPositionMode::Hedge,
                orders,
                [PositionSide::Long, PositionSide::Short]
                    .into_iter()
                    .map(|position_side| venue_runtime::SignedAccountPositionFact {
                        symbol: self.binding.symbol.clone(),
                        position_side,
                        quantity: Decimal::ZERO,
                        entry_price: None,
                        mark_price: None,
                    })
                    .collect(),
                "fills:0".to_owned(),
                request
                    .unresolved()
                    .iter()
                    .map(|command| venue_runtime::SignedUnknownFact {
                        command_id: command.command_id().clone(),
                        result: venue_runtime::SignedUnknownResult::Unknown,
                    })
                    .collect(),
            )
        }

        fn dispatch(
            &mut self,
            _permit: venue_runtime::AccountDispatchPermit,
        ) -> AccountGatewayResult {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            if self.accepted {
                AccountGatewayResult::Accepted {
                    venue_order_id: "lane-venue".to_owned(),
                }
            } else {
                AccountGatewayResult::Unknown
            }
        }
    }

    #[test]
    fn operator_canary_persists_a_runtime_turn_then_dispatches_through_the_host_lane()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let mut raw = live_arguments(&[
            "canary-place",
            "--confirm-live",
            "bybit",
            "--command-id",
            "lane-command",
            "--client-order-id",
            "lane-client",
            "--position-side",
            "long",
            "--quantity",
            "1",
            "--limit-price",
            "1",
        ]);
        raw[8] = temp.path().as_os_str().to_owned();
        let launch = NodeLaunch::try_parse_from(VenueId::Bybit, raw)?;
        let command = launch.live_mvp_command()?;
        let dispatches = Arc::new(AtomicUsize::new(0));
        let gateway = LaneProofGateway {
            binding: launch.binding().clone(),
            dispatches: Arc::clone(&dispatches),
            signed_generations: Arc::new(AtomicUsize::new(0)),
            accepted: false,
        };
        let first = run_live_mvp(&launch, command.clone(), gateway);
        assert!(
            first.is_err(),
            "first canary unexpectedly succeeded: {first:?}"
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        let restarted_dispatches = Arc::new(AtomicUsize::new(0));
        let restarted = LaneProofGateway {
            binding: launch.binding().clone(),
            dispatches: Arc::clone(&restarted_dispatches),
            signed_generations: Arc::new(AtomicUsize::new(0)),
            accepted: false,
        };
        assert!(run_live_mvp(&launch, command, restarted).is_err());
        assert_eq!(restarted_dispatches.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn operator_canary_accepts_only_after_a_fresh_matching_signed_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let mut raw = live_arguments(&[
            "canary-place",
            "--confirm-live",
            "bybit",
            "--command-id",
            "lane-confirm-command",
            "--client-order-id",
            "lane-client",
            "--position-side",
            "long",
            "--quantity",
            "1",
            "--limit-price",
            "1",
        ]);
        raw[8] = temp.path().as_os_str().to_owned();
        let launch = NodeLaunch::try_parse_from(VenueId::Bybit, raw)?;
        let command = launch.live_mvp_command()?;
        let dispatches = Arc::new(AtomicUsize::new(0));
        let gateway = LaneProofGateway {
            binding: launch.binding().clone(),
            dispatches: Arc::clone(&dispatches),
            signed_generations: Arc::new(AtomicUsize::new(0)),
            accepted: true,
        };
        run_live_mvp(&launch, command, gateway)?;
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        Ok(())
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| duration.as_millis().try_into().ok())
            .unwrap_or(1)
    }
}
