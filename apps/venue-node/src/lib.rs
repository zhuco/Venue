use std::{
    ffi::{OsStr, OsString},
    path::{Component, Path, PathBuf},
    process::ExitCode,
};

use clap::Parser;
use venue_domain::domain::Symbol;
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

#[derive(Debug, Parser)]
#[command(name = "venue-node", disable_version_flag = true)]
struct RawNodeArguments {
    #[arg(long, value_parser = parse_exact_mode)]
    mode: GatewayMode,

    #[arg(long)]
    trading_account_id: String,

    #[arg(long)]
    symbol: Symbol,

    /// Absolute base. The node derives <base>/<venue>/<TEST|LIVE>/<account> and never accepts an
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

/// The frozen Stage 7 physical clients are production-only. TEST is accepted as a gateway mode,
/// but it cannot be redirected into those LIVE clients.
pub fn reject_unintegrated_legacy_test_runtime(venue: VenueId) -> Result<(), NodeError> {
    Err(NodeError::LegacyTestRuntime(venue))
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

fn parse_exact_mode(raw: &str) -> Result<GatewayMode, &'static str> {
    match raw {
        "TEST" => Ok(GatewayMode::Test),
        "LIVE" => Ok(GatewayMode::Live),
        _ => Err("gateway mode must be exactly TEST or LIVE"),
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
    #[error("fixed {0} node adapter isolation metadata is invalid")]
    AdapterIsolation(VenueId),
    #[error("node identity does not match the runtime configuration")]
    RuntimeScope,
    #[error(
        "runtime arguments must select exactly one fixed deployment command after '--' and must not contain --artifacts-root"
    )]
    RuntimeArguments,
    #[error("{0} adapter advertised capability before the shared safety closure was integrated")]
    UnexpectedAdapterCapability(VenueId),
    #[error("{venue} {mode} node is fail-closed; missing {missing}")]
    IncompleteSafetyClosure {
        venue: VenueId,
        mode: GatewayMode,
        missing: &'static str,
    },
    #[error(
        "{0} TEST node is fail-closed because the existing Stage 7 runtime is LIVE-only; no endpoint, credential, or artifact fallback is allowed"
    )]
    LegacyTestRuntime(VenueId),
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

    #[test]
    fn node_mode_is_exactly_test_or_live() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            NodeLaunch::try_parse_from(VenueId::Bybit, arguments("TEST"))?
                .binding()
                .mode,
            GatewayMode::Test
        );
        assert_eq!(
            NodeLaunch::try_parse_from(VenueId::Bybit, arguments("LIVE"))?
                .binding()
                .mode,
            GatewayMode::Live
        );
        for rejected in ["test", "live", "SHADOW", " LIVE ", ""] {
            assert!(NodeLaunch::try_parse_from(VenueId::Bybit, arguments(rejected)).is_err());
        }
        Ok(())
    }

    #[test]
    fn artifact_roots_are_disjoint_by_venue_mode_and_account()
    -> Result<(), Box<dyn std::error::Error>> {
        let live = NodeLaunch::try_parse_from(VenueId::Bybit, arguments("LIVE"))?;
        let test = NodeLaunch::try_parse_from(VenueId::Bybit, arguments("TEST"))?;
        let okx = NodeLaunch::try_parse_from(VenueId::Okx, arguments("LIVE"))?;
        let mut other_account = arguments("LIVE");
        other_account[4] = OsString::from("00000000-0000-4000-8000-000000000002");
        let other_account = NodeLaunch::try_parse_from(VenueId::Bybit, other_account)?;

        assert_ne!(live.artifacts_root(), test.artifacts_root());
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
}
