use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use venue_control_protocol::{CommandReceipt, ControlAction, ControlCommandRequest};
use venue_domain::domain::{
    CommandId, ExecutionCommand, NativeOrderFamily, OrderOwner, OrderPurpose, Symbol,
};
use venue_execution::{
    AccountCanonicalRootError, AccountCanonicalRootGuard, CommandJournal, CommandJournalError,
    CommandState, DispatchGuard, WriterLeaseAuthority, WriterLeaseError, WriterScope,
    WriterSession, acquire_account_canonical_root,
};
use venue_gateway_api::{CapabilitySnapshot, GatewayApiError, GatewayBinding, MutationCapability};
use venue_runtime::{AccountKey, AccountModelError, StrategyBinding};

use crate::{
    NodeLaunch,
    supervision::{
        ActorAppliedCanaryReceipt, ActorAppliedControlReceipt, ActorCanaryTurn, ActorControlTurn,
        CanaryControlRequest, PersistedControlCompletion, RestoredLifecycle, SupervisionError,
        SupervisionJournal,
    },
};

const COMMANDS_FILE: &str = "commands.jsonl";
const SUPERVISION_FILE: &str = "control_receipts.jsonl";
const WRITER_FILE: &str = "writer.json";
#[cfg(test)]
const DIGEST_FOR_TEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const REQUIRED_FAMILIES: [NativeOrderFamily; 3] = [
    NativeOrderFamily::UmOrder,
    NativeOrderFamily::UmConditional,
    NativeOrderFamily::UmAlgo,
];

/// The only exchange-specific surface owned by the account-node host. Implementations verify
/// native signatures and translate one consumed permit into at most one physical mutation call.
pub trait PhysicalGateway {
    type Error;

    fn binding(&self) -> &GatewayBinding;

    fn capability_snapshot(&self) -> CapabilitySnapshot;

    /// The host issues this only after canonical-root, WAL, Owner, durable control and writer
    /// metadata have all been recovered. Implementations must not connect before this call.
    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error>;

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error>;

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error>;

    fn dispatch(&mut self, permit: DispatchPermit) -> GatewayDispatchResult;
}

/// Non-cloneable startup proof. Possession means local recovery is complete, not mutation authority.
pub struct GatewayRecoveryPermit {
    binding: GatewayBinding,
    config_epoch: u64,
    unresolved_commands: usize,
    predecessor_writer_generation: Option<u64>,
    connection_generation_floor: u64,
    private_generation_floor: u64,
}

impl GatewayRecoveryPermit {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    #[must_use]
    pub const fn unresolved_commands(&self) -> usize {
        self.unresolved_commands
    }

    #[must_use]
    pub const fn predecessor_writer_generation(&self) -> Option<u64> {
        self.predecessor_writer_generation
    }

    #[must_use]
    pub const fn connection_generation_floor(&self) -> u64 {
        self.connection_generation_floor
    }

    #[must_use]
    pub const fn private_generation_floor(&self) -> u64 {
        self.private_generation_floor
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CommandReadbackKey {
    command_id: CommandId,
    family: NativeOrderFamily,
    client_id: CommandId,
}

impl CommandReadbackKey {
    #[must_use]
    pub const fn command_id(&self) -> &CommandId {
        &self.command_id
    }

    #[must_use]
    pub const fn family(&self) -> NativeOrderFamily {
        self.family
    }

    #[must_use]
    pub const fn client_id(&self) -> &CommandId {
        &self.client_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedReadbackRequest {
    binding: GatewayBinding,
    after_connection_generation: u64,
    after_private_generation: u64,
    commands: Vec<CommandReadbackKey>,
}

impl SignedReadbackRequest {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn after_connection_generation(&self) -> u64 {
        self.after_connection_generation
    }

    #[must_use]
    pub const fn after_private_generation(&self) -> u64 {
        self.after_private_generation
    }

    #[must_use]
    pub fn commands(&self) -> &[CommandReadbackKey] {
        &self.commands
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FamilyReadbackCoverage {
    family: NativeOrderFamily,
    supported: bool,
}

impl FamilyReadbackCoverage {
    #[must_use]
    pub const fn complete(family: NativeOrderFamily) -> Self {
        Self {
            family,
            supported: true,
        }
    }

    #[must_use]
    pub const fn unsupported(family: NativeOrderFamily) -> Self {
        Self {
            family,
            supported: false,
        }
    }

    #[must_use]
    pub const fn family(self) -> NativeOrderFamily {
        self.family
    }

    #[must_use]
    pub const fn supported(self) -> bool {
        self.supported
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedOwnedOrder {
    owner: OrderOwner,
    family: NativeOrderFamily,
    client_id: CommandId,
    venue_order_id: String,
}

impl SignedOwnedOrder {
    pub fn new(
        owner: OrderOwner,
        family: NativeOrderFamily,
        client_id: CommandId,
        venue_order_id: impl Into<String>,
    ) -> Result<Self, SafeHostError> {
        owner.validate().map_err(SafeHostError::OwnerCommand)?;
        let venue_order_id = venue_order_id.into();
        validate_venue_order_id(&venue_order_id)?;
        Ok(Self {
            owner,
            family,
            client_id,
            venue_order_id,
        })
    }

    #[must_use]
    pub const fn owner(&self) -> &OrderOwner {
        &self.owner
    }

    #[must_use]
    pub const fn family(&self) -> NativeOrderFamily {
        self.family
    }

    #[must_use]
    pub const fn client_id(&self) -> &CommandId {
        &self.client_id
    }

    #[must_use]
    pub fn venue_order_id(&self) -> &str {
        &self.venue_order_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadbackCommandState {
    Accepted { venue_order_id: String },
    Rejected { reason_code: String },
    ProvenAbsent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedCommandReadback {
    key: CommandReadbackKey,
    state: ReadbackCommandState,
}

impl SignedCommandReadback {
    pub fn new(
        key: CommandReadbackKey,
        state: ReadbackCommandState,
    ) -> Result<Self, SafeHostError> {
        match &state {
            ReadbackCommandState::Accepted { venue_order_id } => {
                validate_venue_order_id(venue_order_id)?;
            }
            ReadbackCommandState::Rejected { reason_code } => {
                validate_reason_code(reason_code)?;
            }
            ReadbackCommandState::ProvenAbsent => {}
        }
        Ok(Self { key, state })
    }

    #[must_use]
    pub const fn key(&self) -> &CommandReadbackKey {
        &self.key
    }

    #[must_use]
    pub const fn state(&self) -> &ReadbackCommandState {
        &self.state
    }
}

/// Adapter-verified full account evidence. The commitment is the adapter's digest of the exact
/// signed native responses; the host separately checks binding, generations, family coverage,
/// Owner routing and every requested UNKNOWN identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedReadbackReceipt {
    binding: GatewayBinding,
    connection_generation: u64,
    private_generation: u64,
    observed_ms: u64,
    commitment_sha256: String,
    family_coverage: Vec<FamilyReadbackCoverage>,
    owned_open_orders: Vec<SignedOwnedOrder>,
    nonzero_position_symbols: BTreeSet<Symbol>,
    command_results: Vec<SignedCommandReadback>,
}

impl SignedReadbackReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binding: GatewayBinding,
        connection_generation: u64,
        private_generation: u64,
        observed_ms: u64,
        commitment_sha256: impl Into<String>,
        family_coverage: Vec<FamilyReadbackCoverage>,
        owned_open_orders: Vec<SignedOwnedOrder>,
        nonzero_position_symbols: BTreeSet<Symbol>,
        command_results: Vec<SignedCommandReadback>,
    ) -> Result<Self, SafeHostError> {
        let commitment_sha256 = commitment_sha256.into();
        validate_digest(&commitment_sha256)?;
        if connection_generation == 0 || private_generation == 0 || observed_ms == 0 {
            return Err(SafeHostError::ReadbackGeneration);
        }
        Ok(Self {
            binding,
            connection_generation,
            private_generation,
            observed_ms,
            commitment_sha256,
            family_coverage,
            owned_open_orders,
            nonzero_position_symbols,
            command_results,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn observed_ms(&self) -> u64 {
        self.observed_ms
    }

    #[must_use]
    pub fn commitment_sha256(&self) -> &str {
        &self.commitment_sha256
    }

    #[must_use]
    pub fn family_coverage(&self) -> &[FamilyReadbackCoverage] {
        &self.family_coverage
    }

    #[must_use]
    pub fn owned_open_orders(&self) -> &[SignedOwnedOrder] {
        &self.owned_open_orders
    }

    #[must_use]
    pub const fn nonzero_position_symbols(&self) -> &BTreeSet<Symbol> {
        &self.nonzero_position_symbols
    }

    #[must_use]
    pub fn command_results(&self) -> &[SignedCommandReadback] {
        &self.command_results
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanaryEvidence {
    binding: GatewayBinding,
    strategy_instance_id: String,
    run_id: String,
    config_digest: String,
    capability_version: u64,
    minimum_private_generation: u64,
    confirmed_at_ms: u64,
    expires_ms: u64,
    authorized_command_id: CommandId,
    evidence_sha256: String,
}

impl CanaryEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binding: GatewayBinding,
        owner: &StrategyBinding,
        capability_version: u64,
        minimum_private_generation: u64,
        confirmed_at_ms: u64,
        expires_ms: u64,
        authorized_command_id: CommandId,
        evidence_sha256: impl Into<String>,
    ) -> Result<Self, SafeHostError> {
        let evidence_sha256 = evidence_sha256.into();
        validate_digest(&evidence_sha256)?;
        if capability_version == 0
            || minimum_private_generation == 0
            || confirmed_at_ms == 0
            || expires_ms <= confirmed_at_ms
        {
            return Err(SafeHostError::CanaryEvidence);
        }
        Ok(Self {
            binding,
            strategy_instance_id: owner.key.instance_id.clone(),
            run_id: owner.run_id.clone(),
            config_digest: owner.config_digest.clone(),
            capability_version,
            minimum_private_generation,
            confirmed_at_ms,
            expires_ms,
            authorized_command_id,
            evidence_sha256,
        })
    }

    #[must_use]
    pub fn evidence_sha256(&self) -> &str {
        &self.evidence_sha256
    }

    #[must_use]
    pub(crate) const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub(crate) fn strategy_instance_id(&self) -> &str {
        &self.strategy_instance_id
    }

    #[must_use]
    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }

    #[must_use]
    pub(crate) fn config_digest(&self) -> &str {
        &self.config_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlCompletion {
    pub request_id: String,
    pub action: ControlAction,
    pub connection_generation: u64,
    pub private_generation: u64,
    pub symbol_custody_retained: bool,
    pub readback_sha256: String,
    pub receipt: CommandReceipt,
}

#[derive(Clone, Debug)]
pub struct PreparedDispatch {
    command: ExecutionCommand,
    connection_generation: u64,
    private_generation: u64,
    writer_generation: u64,
    writer_revision: u64,
}

impl PreparedDispatch {
    #[must_use]
    pub fn command_id(&self) -> &CommandId {
        self.command.command_id()
    }
}

/// Linear physical authority: it is not Clone and is consumed by `PhysicalGateway::dispatch`.
pub struct DispatchPermit {
    binding: GatewayBinding,
    command: ExecutionCommand,
    writer_generation: u64,
    writer_revision: u64,
    readback_generation: u64,
    canary_sha256: Option<String>,
    _writer_guard: DispatchGuard,
}

impl DispatchPermit {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn command(&self) -> &ExecutionCommand {
        &self.command
    }

    #[must_use]
    pub const fn writer_generation(&self) -> u64 {
        self.writer_generation
    }

    #[must_use]
    pub const fn writer_revision(&self) -> u64 {
        self.writer_revision
    }

    #[must_use]
    pub const fn readback_generation(&self) -> u64 {
        self.readback_generation
    }

    #[must_use]
    pub fn canary_sha256(&self) -> Option<&str> {
        self.canary_sha256.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayAcknowledgement {
    venue_order_id: String,
}

impl GatewayAcknowledgement {
    pub fn new(venue_order_id: impl Into<String>) -> Result<Self, SafeHostError> {
        let venue_order_id = venue_order_id.into();
        validate_venue_order_id(&venue_order_id)?;
        Ok(Self { venue_order_id })
    }

    #[must_use]
    pub fn venue_order_id(&self) -> &str {
        &self.venue_order_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayDispatchResult {
    Acknowledged(GatewayAcknowledgement),
    Rejected { reason_code: String },
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    Accepted { venue_order_id: String },
    Rejected { reason_code: String },
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum HostLifecycle {
    Active,
    Paused,
    AwaitingActor,
    Stopping {
        request_id: String,
        action: ControlAction,
        after_connection_generation: u64,
        after_private_generation: u64,
    },
    StoppedWithCustody,
    StoppedFlat,
}

enum CanonicalRootHold {
    Machine {
        _guard: AccountCanonicalRootGuard,
    },
    #[cfg(test)]
    TestOnly,
}

pub struct NodeSafetyHost<G: PhysicalGateway> {
    binding: GatewayBinding,
    artifacts_root: PathBuf,
    owner: StrategyBinding,
    gateway: G,
    journal: CommandJournal,
    supervision: SupervisionJournal,
    writer: WriterLeaseAuthority,
    writer_session: WriterSession,
    last_readback: SignedReadbackReceipt,
    canary: Option<CanaryEvidence>,
    config_epoch: u64,
    lifecycle: HostLifecycle,
    _canonical_root: CanonicalRootHold,
}

impl<G: PhysicalGateway> NodeSafetyHost<G> {
    /// Opens the fixed canonical root, fences crash residues, obtains a newer signed account
    /// readback, resolves every UNKNOWN without resubmission, then installs the one writer.
    pub fn open(
        launch: &NodeLaunch,
        owner: StrategyBinding,
        config_epoch: u64,
        gateway: G,
        now_ms: u64,
    ) -> Result<Self, SafeHostError> {
        validate_static_scope(launch.binding(), &owner, gateway.binding())?;
        let artifacts_root = launch.artifacts_root();
        let writer_scope = writer_scope(launch.binding(), &owner);
        let guard = acquire_account_canonical_root(&writer_scope, &artifacts_root)?;
        Self::open_with_root(
            launch.binding().clone(),
            artifacts_root,
            owner,
            config_epoch,
            gateway,
            now_ms,
            CanonicalRootHold::Machine { _guard: guard },
        )
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub fn artifacts_root(&self) -> &Path {
        &self.artifacts_root
    }

    #[must_use]
    pub const fn owner(&self) -> &StrategyBinding {
        &self.owner
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    pub fn renew_writer(&mut self, now_ms: u64) -> Result<(), SafeHostError> {
        self.writer_session = self.writer.renew(&self.writer_session, now_ms)?;
        Ok(())
    }

    pub fn prepare_dispatch(
        &mut self,
        command: ExecutionCommand,
        now_ms: u64,
    ) -> Result<PreparedDispatch, SafeHostError> {
        self.validate_command(&command, now_ms)?;
        if self.journal.receipt(command.command_id()).is_some() {
            return Err(SafeHostError::CommandAlreadyJournaled);
        }
        self.journal.prepare(command.clone())?;
        Ok(PreparedDispatch {
            command,
            connection_generation: self.last_readback.connection_generation,
            private_generation: self.last_readback.private_generation,
            writer_generation: self.writer_session.generation,
            writer_revision: self.writer_session.revision,
        })
    }

    pub fn dispatch_prepared(
        &mut self,
        prepared: PreparedDispatch,
        now_ms: u64,
    ) -> Result<DispatchOutcome, SafeHostError> {
        self.dispatch_prepared_inner(prepared, now_ms, TestCrashPoint::None)
    }

    pub fn recover_unknowns(&mut self, now_ms: u64) -> Result<(), SafeHostError> {
        let commands = recovery_keys(&self.journal)?;
        if commands.is_empty() {
            return Ok(());
        }
        let request = SignedReadbackRequest {
            binding: self.binding.clone(),
            after_connection_generation: self.last_readback.connection_generation,
            after_private_generation: self.last_readback.private_generation,
            commands,
        };
        let receipt = self
            .gateway
            .signed_readback(&request)
            .map_err(|_| SafeHostError::GatewayOperation)?;
        self.verify_readback(&request, &receipt, now_ms)?;
        settle_unknowns(&mut self.journal, &request, &receipt)?;
        self.last_readback = receipt;
        Ok(())
    }

    /// Persists and scopes one authoritative control command before issuing an Actor turn.
    pub fn accept_control_command(
        &mut self,
        request: ControlCommandRequest,
        now_ms: u64,
    ) -> Result<ActorControlTurn, SafeHostError> {
        let turn = self.supervision.accept_control(
            request,
            self.last_readback.connection_generation,
            self.last_readback.private_generation,
            now_ms,
        )?;
        self.lifecycle = HostLifecycle::AwaitingActor;
        Ok(turn)
    }

    /// Reissues a crash-recovered durable turn. It never creates a second control request.
    pub fn recovered_control_turn(&self) -> Result<Option<ActorControlTurn>, SafeHostError> {
        self.supervision
            .recovered_control_turn()
            .map_err(Into::into)
    }

    /// Consumes only the receipt created from the matching linear Actor turn.
    pub fn apply_control_receipt(
        &mut self,
        receipt: ActorAppliedControlReceipt,
    ) -> Result<CommandReceipt, SafeHostError> {
        let action = receipt.request().action;
        let request_id = receipt.request().request_id.clone();
        let connection_generation = receipt.connection_generation();
        let private_generation = receipt.private_generation();
        let control_receipt = self.supervision.apply_control(receipt)?;
        self.lifecycle = match action {
            ControlAction::Pause => HostLifecycle::Paused,
            ControlAction::Resume => HostLifecycle::Active,
            ControlAction::Stop | ControlAction::Flatten => HostLifecycle::Stopping {
                request_id,
                action,
                after_connection_generation: connection_generation,
                after_private_generation: private_generation,
            },
        };
        Ok(control_receipt)
    }

    pub fn accept_canary_control(
        &mut self,
        request: CanaryControlRequest,
        now_ms: u64,
    ) -> Result<ActorCanaryTurn, SafeHostError> {
        validate_canary_static(
            &self.binding,
            &self.owner,
            Some(&request.evidence),
            self.last_readback.private_generation,
            now_ms,
        )?;
        self.supervision
            .accept_canary(
                request,
                self.last_readback.connection_generation,
                self.last_readback.private_generation,
                now_ms,
            )
            .map_err(Into::into)
    }

    pub fn apply_canary_receipt(
        &mut self,
        receipt: ActorAppliedCanaryReceipt,
    ) -> Result<CommandReceipt, SafeHostError> {
        let evidence = receipt.request().evidence.clone();
        let control_receipt = self.supervision.apply_canary(receipt)?;
        self.canary = Some(evidence);
        Ok(control_receipt)
    }

    pub fn recovered_canary_turn(&self) -> Result<Option<ActorCanaryTurn>, SafeHostError> {
        self.supervision.recovered_canary_turn().map_err(Into::into)
    }

    pub fn complete_control(&mut self, now_ms: u64) -> Result<ControlCompletion, SafeHostError> {
        let HostLifecycle::Stopping {
            request_id,
            action,
            after_connection_generation,
            after_private_generation,
        } = self.lifecycle.clone()
        else {
            return Err(SafeHostError::ControlLifecycle);
        };
        let request = SignedReadbackRequest {
            binding: self.binding.clone(),
            after_connection_generation: after_connection_generation
                .max(self.last_readback.connection_generation),
            after_private_generation: after_private_generation
                .max(self.last_readback.private_generation),
            commands: recovery_keys(&self.journal)?,
        };
        let receipt = self
            .gateway
            .signed_readback(&request)
            .map_err(|_| SafeHostError::GatewayOperation)?;
        self.verify_readback(&request, &receipt, now_ms)?;
        settle_unknowns(&mut self.journal, &request, &receipt)?;
        self.last_readback = receipt.clone();
        if receipt
            .owned_open_orders
            .iter()
            .any(|order| self.owner.matches_owner(&order.owner))
        {
            return Err(SafeHostError::ControlNotProven);
        }
        let has_position = receipt
            .nonzero_position_symbols
            .contains(&self.binding.symbol);
        if action == ControlAction::Flatten && has_position {
            return Err(SafeHostError::ControlNotProven);
        }
        let persisted = PersistedControlCompletion {
            request_id: request_id.clone(),
            action,
            connection_generation: receipt.connection_generation,
            private_generation: receipt.private_generation,
            symbol_custody_retained: has_position,
            readback_sha256: receipt.commitment_sha256.clone(),
            observed_ms: now_ms,
        };
        let control_receipt = self.supervision.complete_control(persisted)?;
        self.lifecycle = if has_position {
            HostLifecycle::StoppedWithCustody
        } else {
            HostLifecycle::StoppedFlat
        };
        let completion = ControlCompletion {
            request_id: request_id.clone(),
            action,
            connection_generation: receipt.connection_generation,
            private_generation: receipt.private_generation,
            symbol_custody_retained: has_position,
            readback_sha256: receipt.commitment_sha256.clone(),
            receipt: control_receipt,
        };
        Ok(completion)
    }

    #[cfg(test)]
    pub(crate) fn open_for_test(
        launch: &NodeLaunch,
        owner: StrategyBinding,
        gateway: G,
        canary: Option<CanaryEvidence>,
        now_ms: u64,
    ) -> Result<Self, SafeHostError> {
        validate_static_scope(launch.binding(), &owner, gateway.binding())?;
        let mut host = Self::open_with_root(
            launch.binding().clone(),
            launch.artifacts_root(),
            owner,
            1,
            gateway,
            now_ms,
            CanonicalRootHold::TestOnly,
        )?;
        if let Some(evidence) = canary {
            let turn = host.accept_canary_control(
                CanaryControlRequest {
                    request_id: format!("test-canary-{now_ms}"),
                    evidence,
                },
                now_ms,
            )?;
            let receipt = turn.persisted(1, DIGEST_FOR_TEST, now_ms)?;
            let _ = host.apply_canary_receipt(receipt)?;
        }
        Ok(host)
    }

    #[cfg(test)]
    pub(crate) fn dispatch_with_crash(
        &mut self,
        prepared: PreparedDispatch,
        now_ms: u64,
        crash_point: TestCrashPoint,
    ) -> Result<DispatchOutcome, SafeHostError> {
        self.dispatch_prepared_inner(prepared, now_ms, crash_point)
    }

    fn open_with_root(
        binding: GatewayBinding,
        artifacts_root: PathBuf,
        owner: StrategyBinding,
        config_epoch: u64,
        mut gateway: G,
        now_ms: u64,
        canonical_root: CanonicalRootHold,
    ) -> Result<Self, SafeHostError> {
        validate_static_scope(&binding, &owner, gateway.binding())?;
        let account_directory = artifacts_root.join("account");
        fs::create_dir_all(&account_directory).map_err(|source| SafeHostError::Io {
            path: account_directory.clone(),
            source,
        })?;
        let mut journal = CommandJournal::open(account_directory.join(COMMANDS_FILE))?;
        journal.fence_interrupted_dispatches()?;
        validate_recovered_owner_routes(&journal, &owner)?;
        let supervision = SupervisionJournal::open(
            account_directory.join(SUPERVISION_FILE),
            binding.clone(),
            owner.clone(),
            config_epoch,
        )?;
        let restored = supervision.projection()?;
        let scope = writer_scope(&binding, &owner);
        let writer = WriterLeaseAuthority::open(account_directory.join(WRITER_FILE), scope)?;
        let predecessor = writer.active_session()?;
        let recovery_commands = recovery_keys(&journal)?;
        let lifecycle = host_lifecycle(&restored.lifecycle);
        gateway
            .connect_after_recovery(GatewayRecoveryPermit {
                binding: binding.clone(),
                config_epoch,
                unresolved_commands: recovery_commands.len(),
                predecessor_writer_generation: predecessor
                    .as_ref()
                    .map(|session| session.generation),
                connection_generation_floor: restored.connection_generation_floor,
                private_generation_floor: restored.private_generation_floor,
            })
            .map_err(|_| SafeHostError::GatewayOperation)?;
        let request = SignedReadbackRequest {
            binding: binding.clone(),
            after_connection_generation: restored.connection_generation_floor,
            after_private_generation: restored.private_generation_floor,
            commands: recovery_commands,
        };
        let receipt = gateway
            .signed_readback(&request)
            .map_err(|_| SafeHostError::GatewayOperation)?;
        verify_readback_static(&gateway, &binding, &owner, &request, &receipt, now_ms)?;
        settle_unknowns(&mut journal, &request, &receipt)?;
        if journal.has_unresolved() {
            return Err(SafeHostError::UnknownUnresolved);
        }

        let writer_session = match predecessor {
            Some(predecessor) => writer.recover_same_scope_after_readback(
                &predecessor,
                receipt.private_generation,
                now_ms,
            )?,
            None => writer.register_initial(now_ms, receipt.private_generation)?,
        };
        Ok(Self {
            binding,
            artifacts_root,
            owner,
            gateway,
            journal,
            supervision,
            writer,
            writer_session,
            last_readback: receipt,
            canary: restored.canary,
            config_epoch,
            lifecycle,
            _canonical_root: canonical_root,
        })
    }

    fn dispatch_prepared_inner(
        &mut self,
        prepared: PreparedDispatch,
        now_ms: u64,
        crash_point: TestCrashPoint,
    ) -> Result<DispatchOutcome, SafeHostError> {
        self.validate_command(&prepared.command, now_ms)?;
        let command_id = prepared.command.command_id().clone();
        let receipt = self
            .journal
            .receipt(&command_id)
            .ok_or(SafeHostError::PreparedIdentity)?;
        if receipt.command != prepared.command || receipt.state != CommandState::Prepared {
            return Err(SafeHostError::PreparedIdentity);
        }
        if prepared.connection_generation != self.last_readback.connection_generation
            || prepared.private_generation != self.last_readback.private_generation
            || prepared.writer_generation != self.writer_session.generation
            || prepared.writer_revision != self.writer_session.revision
        {
            self.journal.transition(
                &command_id,
                CommandState::Rejected {
                    reason: "pre_wal_candidate_stale".to_owned(),
                },
            )?;
            return Err(SafeHostError::PreparedStale);
        }
        let writer_guard = match self.writer.dispatch_guard(&self.writer_session, now_ms) {
            Ok(guard) => guard,
            Err(error) => {
                self.journal.transition(
                    &command_id,
                    CommandState::Rejected {
                        reason: "writer_fenced_before_dispatch".to_owned(),
                    },
                )?;
                return Err(SafeHostError::Writer(error));
            }
        };
        self.journal
            .transition(&command_id, CommandState::Submitted)?;
        if crash_point == TestCrashPoint::AfterSubmitted {
            return Err(SafeHostError::InjectedCrash);
        }
        let risk_increasing = is_risk_increasing(&prepared.command);
        let permit = DispatchPermit {
            binding: self.binding.clone(),
            command: prepared.command,
            writer_generation: self.writer_session.generation,
            writer_revision: self.writer_session.revision,
            readback_generation: self.last_readback.private_generation,
            canary_sha256: risk_increasing
                .then(|| {
                    self.canary
                        .as_ref()
                        .map(|evidence| evidence.evidence_sha256.clone())
                })
                .flatten(),
            _writer_guard: writer_guard,
        };
        let gateway_result = self.gateway.dispatch(permit);
        if crash_point == TestCrashPoint::AfterGatewayResult {
            return Err(SafeHostError::InjectedCrash);
        }
        let outcome = match gateway_result {
            GatewayDispatchResult::Acknowledged(acknowledgement) => {
                self.journal.transition(
                    &command_id,
                    CommandState::Accepted {
                        venue_order_id: acknowledgement.venue_order_id.clone(),
                    },
                )?;
                DispatchOutcome::Accepted {
                    venue_order_id: acknowledgement.venue_order_id,
                }
            }
            GatewayDispatchResult::Rejected { reason_code } => {
                validate_reason_code(&reason_code)?;
                self.journal.transition(
                    &command_id,
                    CommandState::Rejected {
                        reason: reason_code.clone(),
                    },
                )?;
                DispatchOutcome::Rejected { reason_code }
            }
            GatewayDispatchResult::Unknown => {
                self.journal.transition(
                    &command_id,
                    CommandState::Unknown {
                        reason: "gateway_result_unknown".to_owned(),
                    },
                )?;
                DispatchOutcome::Unknown
            }
        };
        Ok(outcome)
    }

    fn validate_command(
        &self,
        command: &ExecutionCommand,
        now_ms: u64,
    ) -> Result<(), SafeHostError> {
        command.validate().map_err(SafeHostError::OwnerCommand)?;
        if !self.owner.matches_owner(command.mutation_owner()) {
            return Err(SafeHostError::OwnerRoute);
        }
        let risk_increasing = is_risk_increasing(command);
        match &self.lifecycle {
            HostLifecycle::Active => {}
            HostLifecycle::Paused
            | HostLifecycle::AwaitingActor
            | HostLifecycle::Stopping { .. }
            | HostLifecycle::StoppedWithCustody
                if !risk_increasing => {}
            HostLifecycle::Paused
            | HostLifecycle::AwaitingActor
            | HostLifecycle::Stopping { .. }
            | HostLifecycle::StoppedWithCustody
            | HostLifecycle::StoppedFlat => return Err(SafeHostError::ControlLifecycle),
        }
        let has_other_unresolved = self
            .journal
            .unresolved_command_ids()
            .iter()
            .any(|command_id| command_id != command.command_id());
        if risk_increasing && has_other_unresolved {
            return Err(SafeHostError::UnknownUnresolved);
        }
        if is_reduce_mutation(command)
            && self.journal.has_unresolved_entry_or_reduce()
            && has_other_unresolved
        {
            return Err(SafeHostError::UnknownUnresolved);
        }
        let family = command_family(command, &self.journal)?;
        if self
            .last_readback
            .family_coverage
            .iter()
            .find(|coverage| coverage.family == family)
            .is_none_or(|coverage| !coverage.supported)
        {
            return Err(SafeHostError::UnsupportedOrderFamily);
        }
        let mutation = mutation_capability(command);
        let capability = self.gateway.capability_snapshot();
        let expected_version = if risk_increasing {
            self.canary
                .as_ref()
                .ok_or(SafeHostError::CanaryEvidence)?
                .capability_version
        } else {
            capability.version
        };
        capability.authorize(&self.binding, expected_version, now_ms, mutation)?;
        if risk_increasing {
            validate_canary_static(
                &self.binding,
                &self.owner,
                self.canary.as_ref(),
                self.last_readback.private_generation,
                now_ms,
            )?;
            if self
                .canary
                .as_ref()
                .is_none_or(|evidence| evidence.authorized_command_id != *command.command_id())
            {
                return Err(SafeHostError::CanaryEvidence);
            }
        }
        Ok(())
    }

    fn verify_readback(
        &self,
        request: &SignedReadbackRequest,
        receipt: &SignedReadbackReceipt,
        now_ms: u64,
    ) -> Result<(), SafeHostError> {
        verify_readback_static(
            &self.gateway,
            &self.binding,
            &self.owner,
            request,
            receipt,
            now_ms,
        )
    }
}

fn validate_static_scope(
    binding: &GatewayBinding,
    owner: &StrategyBinding,
    gateway_binding: &GatewayBinding,
) -> Result<(), SafeHostError> {
    binding.validate()?;
    let expected_account = AccountKey::new(binding.venue, binding.trading_account_id.clone())?;
    if binding != gateway_binding
        || owner.key.account != expected_account
        || owner.key.symbol != binding.symbol
    {
        return Err(SafeHostError::BindingScope);
    }
    Ok(())
}

fn host_lifecycle(restored: &RestoredLifecycle) -> HostLifecycle {
    match restored {
        RestoredLifecycle::Active => HostLifecycle::Active,
        RestoredLifecycle::Paused => HostLifecycle::Paused,
        RestoredLifecycle::AwaitingActor(_) => HostLifecycle::AwaitingActor,
        RestoredLifecycle::Stopping {
            request_id,
            action,
            after_connection_generation,
            after_private_generation,
        } => HostLifecycle::Stopping {
            request_id: request_id.clone(),
            action: *action,
            after_connection_generation: *after_connection_generation,
            after_private_generation: *after_private_generation,
        },
        RestoredLifecycle::StoppedWithCustody => HostLifecycle::StoppedWithCustody,
        RestoredLifecycle::StoppedFlat => HostLifecycle::StoppedFlat,
    }
}

fn writer_scope(binding: &GatewayBinding, owner: &StrategyBinding) -> WriterScope {
    WriterScope {
        exchange: binding.venue.as_str().to_owned(),
        account: binding.trading_account_id.clone(),
        symbol: binding.symbol.clone(),
        owner_scope: owner.key.instance_id.clone(),
    }
}

fn validate_recovered_owner_routes(
    journal: &CommandJournal,
    owner: &StrategyBinding,
) -> Result<(), SafeHostError> {
    if journal
        .recovery_identities()
        .iter()
        .any(|(_, recovered_owner, _, _)| !owner.matches_owner(recovered_owner))
    {
        return Err(SafeHostError::OwnerRoute);
    }
    Ok(())
}

fn recovery_keys(journal: &CommandJournal) -> Result<Vec<CommandReadbackKey>, SafeHostError> {
    journal
        .unresolved_command_ids()
        .into_iter()
        .map(|command_id| {
            let identity = journal
                .order_identity(&command_id)
                .or_else(|| journal.cancel_target_identity(&command_id))
                .ok_or(SafeHostError::UnknownIdentity)?;
            Ok(CommandReadbackKey {
                command_id,
                family: identity.family,
                client_id: identity.client_id.clone(),
            })
        })
        .collect()
}

fn settle_unknowns(
    journal: &mut CommandJournal,
    request: &SignedReadbackRequest,
    receipt: &SignedReadbackReceipt,
) -> Result<(), SafeHostError> {
    let expected = request
        .commands
        .iter()
        .map(|key| (key.command_id.clone(), key.clone()))
        .collect::<BTreeMap<_, _>>();
    let actual = receipt
        .command_results
        .iter()
        .map(|result| (result.key.command_id.clone(), result))
        .collect::<BTreeMap<_, _>>();
    if actual.len() != receipt.command_results.len()
        || expected.len() != request.commands.len()
        || actual.len() != expected.len()
    {
        return Err(SafeHostError::ReadbackCommands);
    }
    for (command_id, expected_key) in expected {
        let result = actual
            .get(&command_id)
            .ok_or(SafeHostError::ReadbackCommands)?;
        if result.key != expected_key {
            return Err(SafeHostError::ReadbackCommands);
        }
        let state = match &result.state {
            ReadbackCommandState::Accepted { venue_order_id } => CommandState::Accepted {
                venue_order_id: venue_order_id.clone(),
            },
            ReadbackCommandState::Rejected { reason_code } => CommandState::Rejected {
                reason: reason_code.clone(),
            },
            ReadbackCommandState::ProvenAbsent => CommandState::Rejected {
                reason: "signed_readback_proven_absent".to_owned(),
            },
        };
        journal.transition(&command_id, state)?;
    }
    Ok(())
}

fn verify_readback_static<G: PhysicalGateway>(
    gateway: &G,
    binding: &GatewayBinding,
    owner: &StrategyBinding,
    request: &SignedReadbackRequest,
    receipt: &SignedReadbackReceipt,
    now_ms: u64,
) -> Result<(), SafeHostError> {
    gateway
        .verify_signed_readback(receipt)
        .map_err(|_| SafeHostError::ReadbackSignature)?;
    if request.binding != *binding
        || receipt.binding != *binding
        || receipt.observed_ms > now_ms
        || receipt.connection_generation < request.after_connection_generation
        || receipt.private_generation <= request.after_private_generation
    {
        return Err(SafeHostError::ReadbackScope);
    }
    let coverage = receipt
        .family_coverage
        .iter()
        .map(|entry| (entry.family, entry.supported))
        .collect::<BTreeMap<_, _>>();
    if coverage.len() != REQUIRED_FAMILIES.len()
        || coverage.keys().copied().collect::<BTreeSet<_>>() != BTreeSet::from(REQUIRED_FAMILIES)
    {
        return Err(SafeHostError::ReadbackFamilies);
    }
    for order in &receipt.owned_open_orders {
        if !owner.matches_owner(&order.owner)
            || !coverage.get(&order.family).copied().unwrap_or(false)
        {
            return Err(SafeHostError::OwnerRoute);
        }
    }
    if receipt
        .nonzero_position_symbols
        .iter()
        .any(|symbol| symbol != &binding.symbol)
    {
        return Err(SafeHostError::ReadbackScope);
    }
    let expected_commands = request.commands.iter().collect::<BTreeSet<_>>();
    let actual_commands = receipt
        .command_results
        .iter()
        .map(|result| &result.key)
        .collect::<BTreeSet<_>>();
    if expected_commands != actual_commands
        || receipt.command_results.len() != actual_commands.len()
    {
        return Err(SafeHostError::ReadbackCommands);
    }
    if request.commands.iter().any(|command| {
        coverage
            .get(&command.family)
            .is_none_or(|supported| !supported)
    }) {
        return Err(SafeHostError::ReadbackFamilies);
    }
    Ok(())
}

fn validate_canary_static(
    binding: &GatewayBinding,
    owner: &StrategyBinding,
    canary: Option<&CanaryEvidence>,
    private_generation: u64,
    now_ms: u64,
) -> Result<(), SafeHostError> {
    let evidence = canary.ok_or(SafeHostError::CanaryEvidence)?;
    if evidence.binding != *binding
        || evidence.strategy_instance_id != owner.key.instance_id
        || evidence.run_id != owner.run_id
        || evidence.config_digest != owner.config_digest
        || private_generation < evidence.minimum_private_generation
        || now_ms < evidence.confirmed_at_ms
        || now_ms >= evidence.expires_ms
    {
        return Err(SafeHostError::CanaryEvidence);
    }
    Ok(())
}

fn mutation_capability(command: &ExecutionCommand) -> MutationCapability {
    match command {
        ExecutionCommand::PlaceLimit(_) => MutationCapability::PlaceLimit,
        ExecutionCommand::PlaceMarket(_)
        | ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_) => MutationCapability::PlaceMarket,
        ExecutionCommand::Cancel(_) => MutationCapability::Cancel,
    }
}

fn command_family(
    command: &ExecutionCommand,
    journal: &CommandJournal,
) -> Result<NativeOrderFamily, SafeHostError> {
    match command {
        ExecutionCommand::Cancel(cancel) => journal
            .order_identity_by_client_id(&cancel.target_client_order_id)
            .filter(|identity| identity.owner == &cancel.owner)
            .map(|identity| identity.family)
            .ok_or(SafeHostError::OwnerRoute),
        ExecutionCommand::PlaceLimit(_)
        | ExecutionCommand::PlaceMarket(_)
        | ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_) => command
            .native_order_family()
            .ok_or(SafeHostError::UnsupportedOrderFamily),
    }
}

fn is_risk_increasing(command: &ExecutionCommand) -> bool {
    matches!(
        command,
        ExecutionCommand::PlaceLimit(command) if command.owner.purpose == OrderPurpose::Entry
    ) || matches!(command, ExecutionCommand::PlaceMarket(_))
}

fn is_reduce_mutation(command: &ExecutionCommand) -> bool {
    matches!(
        command,
        ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_)
    ) || matches!(
        command,
        ExecutionCommand::PlaceLimit(command) if command.owner.purpose != OrderPurpose::Entry
    )
}

fn validate_digest(value: &str) -> Result<(), SafeHostError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(SafeHostError::Digest)
    }
}

fn validate_venue_order_id(value: &str) -> Result<(), SafeHostError> {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(SafeHostError::VenueOrderId)
    }
}

fn validate_reason_code(value: &str) -> Result<(), SafeHostError> {
    if !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(SafeHostError::ReasonCode)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestCrashPoint {
    None,
    AfterSubmitted,
    AfterGatewayResult,
}

#[derive(Debug, thiserror::Error)]
pub enum SafeHostError {
    #[error(transparent)]
    GatewayApi(#[from] GatewayApiError),
    #[error(transparent)]
    Account(#[from] AccountModelError),
    #[error(transparent)]
    CanonicalRoot(#[from] AccountCanonicalRootError),
    #[error(transparent)]
    Writer(#[from] WriterLeaseError),
    #[error(transparent)]
    Journal(#[from] CommandJournalError),
    #[error(transparent)]
    Supervision(#[from] SupervisionError),
    #[error("node safety host I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("gateway, runtime owner, and node binding must match exactly")]
    BindingScope,
    #[error("gateway capability or readback operation failed closed")]
    GatewayOperation,
    #[error("adapter could not verify the signed readback receipt")]
    ReadbackSignature,
    #[error("signed readback binding or generation is invalid")]
    ReadbackScope,
    #[error("signed readback generation is invalid")]
    ReadbackGeneration,
    #[error("signed readback does not cover all canonical order families")]
    ReadbackFamilies,
    #[error("signed readback does not match every requested UNKNOWN identity")]
    ReadbackCommands,
    #[error("durable UNKNOWN command identity cannot be reconstructed")]
    UnknownIdentity,
    #[error("UNKNOWN mutation must be reconciled before another risk-bearing mutation")]
    UnknownUnresolved,
    #[error("command Owner does not match the fixed node route")]
    OwnerRoute,
    #[error("command owner or semantic fields are invalid: {0}")]
    OwnerCommand(venue_domain::domain::CommandError),
    #[error("command identity is already present in the WAL")]
    CommandAlreadyJournaled,
    #[error("prepared dispatch does not match the durable WAL state")]
    PreparedIdentity,
    #[error("LIVE entry mutation lacks exact, fresh operator Canary evidence")]
    CanaryEvidence,
    #[error("prepared dispatch was revoked by a newer readback or writer revision")]
    PreparedStale,
    #[error("the latest signed readback does not support this command's native order family")]
    UnsupportedOrderFamily,
    #[error("Stop/Flatten lifecycle forbids this action")]
    ControlLifecycle,
    #[error("updated signed readback does not prove Stop/Flatten completion")]
    ControlNotProven,
    #[error("SHA-256 evidence must contain exactly 64 hexadecimal characters")]
    Digest,
    #[error("venue order identity is invalid")]
    VenueOrderId,
    #[error("gateway rejection code is invalid")]
    ReasonCode,
    #[error("deterministic test crash injected")]
    InjectedCrash,
}
