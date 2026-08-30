use std::{
    ffi::{OsStr, OsString},
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
use venue_execution::{AccountMutationHost, AccountPhysicalGateway};
use venue_gateway_api::{CapabilityFlags, GatewayApiError, GatewayBinding, GatewayMode, VenueId};

mod async_gateway;
mod control_delivery;
mod control_delivery_storage;
mod control_http_client;
mod safe_host;
mod supervision;

#[cfg(test)]
mod control_delivery_tests;
#[cfg(test)]
mod safe_host_tests;

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

pub use async_gateway::{
    AsyncGatewayBoundaryError, AsyncGatewayCallError, AsyncGatewayTimeouts, AsyncPhysicalGateway,
    TokioPhysicalGateway, TokioRuntimeDriver, TokioRuntimeRun,
};

pub use safe_host::{
    CanaryEvidence, CommandReadbackKey, ControlCompletion, DispatchOutcome, DispatchPermit,
    FamilyReadbackCoverage, GatewayAcknowledgement, GatewayDispatchResult, GatewayRecoveryPermit,
    NodeSafetyHost, PhysicalGateway, PreparedDispatch, ReadbackCommandState, SafeHostError,
    SignedCommandReadback, SignedOwnedOrder, SignedReadbackReceipt, SignedReadbackRequest,
};
pub use supervision::{
    ActorAppliedCanaryReceipt, ActorAppliedControlReceipt, ActorCanaryTurn, ActorControlTurn,
    CanaryControlRequest, SupervisionError,
};
pub use venue_control_protocol::{CommandReceipt, ControlAction, ControlCommandRequest};

const ARTIFACT_COMMANDS: [&str; 14] = [
    "grid-start",
    "grid-shadow",
    "grid-canary",
    "grid-lifecycle-canary",
    "grid-canary-recover",
    "grid-executable-handoff",
    "grid-external-algo-cancel",
    "grid-flatten",
    "grid-stop",
    "grid-private-evidence-recover",
    "grid-public-evidence-recover",
    "grid-restart",
    "grid-legacy-binance-stop",
    "grid-legacy-binance-bridge",
];
const REQUIRED_NEW_VENUE_GATES: &str = "Owner, WAL, unique account writer fence, signed readback, UNKNOWN reconciliation, Stop/Flatten, and operator-confirmed Canary evidence";
const LIVE_ARTIFACT_FILE_HARD_LIMIT_BYTES: u64 = 10 * 1024 * 1024;
const LIVE_ARTIFACT_ROOT_FREEZE_BYTES: u64 = 240 * 1024 * 1024;

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

    /// Existing Stage 7 arguments for Binance, Gate.io, and Bitget. Put them after `--` and omit
    /// `--artifacts-root`; the node injects the isolated account root.
    #[arg(last = true, allow_hyphen_values = true)]
    runtime_arguments: Vec<OsString>,
}

/// Secret-free launch scope shared by all six fixed node binaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLaunch {
    binding: GatewayBinding,
    artifacts_base: PathBuf,
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
        Ok(Self {
            binding,
            artifacts_base: raw.artifacts_base,
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
        validate_live_artifact_budget(&self.artifacts_base)?;
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

    /// Produces arguments for the frozen Stage 7 deployment entry. The caller cannot override the
    /// final artifact root, so mode/account roots cannot collide through CLI input.
    pub fn legacy_runtime_arguments(
        &self,
        program_name: &'static str,
    ) -> Result<Vec<OsString>, NodeError> {
        if self.runtime_arguments.is_empty()
            || self
                .runtime_arguments
                .iter()
                .any(is_artifacts_root_argument)
        {
            return Err(NodeError::RuntimeArguments);
        }

        let doctor = self
            .runtime_arguments
            .iter()
            .filter(|argument| argument.as_os_str() == OsStr::new("doctor"))
            .count();
        let artifact_commands = self
            .runtime_arguments
            .iter()
            .filter(|argument| {
                ARTIFACT_COMMANDS
                    .iter()
                    .any(|command| argument.as_os_str() == OsStr::new(command))
            })
            .count();
        if doctor + artifact_commands != 1 {
            return Err(NodeError::RuntimeArguments);
        }

        let mut arguments = Vec::with_capacity(self.runtime_arguments.len() + 3);
        arguments.push(OsString::from(program_name));
        arguments.extend(self.runtime_arguments.iter().cloned());
        if artifact_commands == 1 {
            arguments.push(OsString::from("--artifacts-root"));
            arguments.push(self.artifacts_root().into_os_string());
        }
        Ok(arguments)
    }
}

#[derive(Debug, Parser)]
struct RawLiveMvpArguments {
    #[command(subcommand)]
    command: RawLiveMvpCommand,
}

#[derive(Debug, Subcommand)]
enum RawLiveMvpCommand {
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
    Preflight,
    Dispatch(Box<ExecutionCommand>),
}

impl LiveMvpCommand {
    fn from_raw(raw: RawLiveMvpCommand, binding: &GatewayBinding) -> Result<Self, NodeError> {
        match raw {
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

pub fn run_live_mvp<G: AccountPhysicalGateway>(
    launch: &NodeLaunch,
    command: LiveMvpCommand,
    gateway: G,
) -> Result<(), NodeError> {
    let venue = launch.binding().venue;
    let mut host = AccountMutationHost::open(
        launch.artifacts_root(),
        launch.binding().clone(),
        Decimal::TEN,
        gateway,
    )
    .map_err(|error| NodeError::LiveHost {
        venue,
        message: error.to_string(),
    })?;
    match command {
        LiveMvpCommand::Preflight => {
            if host.has_unresolved() {
                return Err(NodeError::LiveHost {
                    venue,
                    message: "signed recovery left an unresolved mutation".to_owned(),
                });
            }
            println!("{venue} LIVE preflight passed; no mutation sent");
            Ok(())
        }
        LiveMvpCommand::Dispatch(command) => {
            let outcome = host
                .dispatch(*command)
                .map_err(|error| NodeError::LiveHost {
                    venue,
                    message: error.to_string(),
                })?;
            println!("{venue} LIVE dispatch outcome: {outcome:?}");
            Ok(())
        }
    }
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

fn validate_live_artifact_budget(root: &Path) -> Result<(), NodeError> {
    if !root.exists() {
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
            total = total.checked_add(size).ok_or(NodeError::ArtifactsBudget)?;
            if total >= LIVE_ARTIFACT_ROOT_FREEZE_BYTES {
                return Err(NodeError::ArtifactsBudget);
            }
        }
    }
    Ok(())
}

fn is_artifacts_root_argument(argument: &OsString) -> bool {
    argument.as_os_str() == OsStr::new("--artifacts-root")
        || argument
            .to_str()
            .is_some_and(|argument| argument.starts_with("--artifacts-root="))
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
    fn legacy_arguments_receive_only_the_derived_root() -> Result<(), Box<dyn std::error::Error>> {
        let mut raw = arguments("LIVE");
        raw.extend([
            OsString::from("--"),
            OsString::from("--config"),
            OsString::from("venue.toml"),
            OsString::from("grid-stop"),
        ]);
        let launch = NodeLaunch::try_parse_from(VenueId::Binance, raw)?;
        let forwarded = launch.legacy_runtime_arguments("venue-node-binance")?;
        assert!(forwarded.contains(&OsString::from("--artifacts-root")));
        assert!(forwarded.contains(&launch.artifacts_root().into_os_string()));
        Ok(())
    }

    #[test]
    fn caller_cannot_override_the_derived_artifact_root() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut raw = arguments("LIVE");
        raw.extend([
            OsString::from("--"),
            OsString::from("grid-stop"),
            OsString::from("--artifacts-root"),
            OsString::from("C:\\other"),
        ]);
        let launch = NodeLaunch::try_parse_from(VenueId::Binance, raw)?;
        assert!(matches!(
            launch.legacy_runtime_arguments("venue-node-binance"),
            Err(NodeError::RuntimeArguments)
        ));
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
}
