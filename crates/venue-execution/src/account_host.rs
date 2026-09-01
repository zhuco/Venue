use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    Asset, CancelCommand, CommandId, ExecutionCommand, FieldState, InstrumentIdentity, MarketKind,
    NativeOrderFamily, Order, OrderOwner, OrderSide, OrderState, Position, PositionSide, Price,
};
use venue_gateway_api::GatewayBinding;

use crate::{
    AccountCanonicalRootGuard, CommandJournal, CommandJournalError, CommandState,
    LegacyV1WriterGuard, LegacyV1WriterPredecessor, WriterScope, acquire_account_canonical_root,
};

pub const COMMAND_JOURNAL_ROTATE_BYTES: u64 = 5 * 1024 * 1024;
pub const COMMAND_JOURNAL_HARD_LIMIT_BYTES: u64 = 10 * 1024 * 1024;
const MAX_RISK_EVIDENCE_AGE_MS: u64 = 60_000;
const RUNTIME_BOOTSTRAP_FILE: &str = "signed-account-bootstrap.json";
const RUNTIME_CHECKPOINT_LIMIT_BYTES: usize = 5 * 1024 * 1024;
const ACCOUNT_WRITER_LOCK_FILE: &str = "writer.lock";
const LEGACY_V1_IMPORT_FILE: &str = "legacy-v1-journal-import.json";
const LEGACY_V1_IMPORT_SCHEMA_VERSION: u16 = 2;

#[path = "account_normalization.rs"]
mod account_normalization;
pub use account_normalization::{AccountLimitNormalizationIntent, AccountPricedLimitIntent};

#[path = "account_net_reduce.rs"]
mod account_net_reduce;
use account_net_reduce::{
    NetReduceSettlement, PersistedSignedBootstrap, completed_net_reduce_settlement,
    load_previous_signed_bootstrap, validate_recovered_net_reduce_settlements,
};

#[path = "account_order_identity.rs"]
mod account_order_identity;

#[path = "account_scope.rs"]
mod account_scope;
#[path = "account_snapshot.rs"]
mod account_snapshot;
#[path = "account_symbols.rs"]
mod account_symbols;
use account_scope::{
    has_open_entry_reservation, is_risk_increasing, snapshot_covers_binding_position_mode,
    snapshot_covers_configured_symbols, validate_command_scope, wal_entry_reservation_total,
};
#[allow(unused_imports)]
pub use account_snapshot::{
    AccountQuoteToUsdtRate, AccountRiskAmount, AccountRiskEvidence, RuntimeBootstrapReceipt,
    SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
};
pub use account_symbols::AccountSymbolSet;

/// Auditable components of the account-level 10U admission calculation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRiskSummary {
    signed_positions: Decimal,
    open_entry_orders: Decimal,
    wal_entry_reservations: Decimal,
    candidate_entry: Decimal,
}

/// Exact read-only rules identity for the account binding. Copy planning does not consume a
/// static tick/minimum-notional, which some venues express as a dynamic native rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountInstrumentIdentity {
    pub identity: InstrumentIdentity,
    pub rules_generation: u64,
}

impl AccountRiskSummary {
    #[must_use]
    pub const fn signed_positions(&self) -> Decimal {
        self.signed_positions
    }

    #[must_use]
    pub const fn open_entry_orders(&self) -> Decimal {
        self.open_entry_orders
    }

    #[must_use]
    pub const fn wal_entry_reservations(&self) -> Decimal {
        self.wal_entry_reservations
    }

    #[must_use]
    pub const fn candidate_entry(&self) -> Decimal {
        self.candidate_entry
    }

    pub fn total(&self) -> Result<Decimal, AccountHostValidationError> {
        self.signed_positions
            .checked_add(self.open_entry_orders)
            .and_then(|value| value.checked_add(self.wal_entry_reservations))
            .and_then(|value| value.checked_add(self.candidate_entry))
            .ok_or(AccountHostValidationError::Notional)
    }
}

/// Minimal production boundary for the current single-machine account process. The gateway can
/// mutate only by consuming a permit created after the same host persisted `Submitted`.
pub trait AccountPhysicalGateway {
    type Error: std::error::Error + Send + Sync + 'static;

    fn binding(&self) -> &GatewayBinding;

    /// Performs a fresh signed account readback and resolves every requested durable identity.
    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error>;

    /// Returns a single complete signed account observation. The default is deliberately
    /// fail-closed so adding this host to an existing adapter never enables entry risk until the
    /// adapter has proved its position and open-order coverage.
    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        Err(AccountHostValidationError::RiskEvidence)
    }

    /// Adapters must explicitly implement the complete signed collector before Runtime can be
    /// made Ready.  The default is read-only failure, never an empty-account assertion.
    fn signed_account_snapshot(
        &mut self,
        _request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        Err(AccountHostValidationError::SignedSnapshot)
    }

    /// Verifies the adapter's wire identity against the original WAL client ID. This grants no
    /// ownership by itself: the Host also checks native order identity, family and full semantics.
    fn signed_client_order_id_matches(&self, canonical: &CommandId, signed: &str) -> bool {
        canonical.as_str() == signed
    }

    /// Current adapter-validated rules identity for the gateway binding. It remains a read-only
    /// fact: callers must still compare its generation to a fresh signed account snapshot before
    /// using it for Copy planning.
    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        Err(AccountHostValidationError::Instrument)
    }

    /// Per-symbol variant for a Host that has already registered multiple canonical symbols.
    /// Existing adapters stay fail-closed for every non-anchor symbol until they implement it.
    fn current_instrument_for(
        &mut self,
        symbol: &venue_domain::domain::Symbol,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        if symbol != &self.binding().symbol {
            return Err(AccountHostValidationError::Instrument);
        }
        self.current_instrument()
    }

    /// Maps semantic quote exposure to a current post-only `PlaceLimit`. Default failure keeps an
    /// adapter from inheriting entry capability before it proves rules, BBO and side semantics.
    fn normalize_limit_intent(
        &mut self,
        _intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        Err(AccountHostValidationError::Command)
    }

    /// Preserves an explicitly selected price and policy while applying fresh native quantity
    /// rules. It does not authorize execution and cannot fall back to automatic BBO pricing.
    fn normalize_priced_limit_intent(
        &mut self,
        _intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        Err(AccountHostValidationError::Command)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRecoveryRequest {
    binding: GatewayBinding,
    configured_symbols: AccountSymbolSet,
    unresolved: Vec<ExecutionCommand>,
    /// The last durably committed native fills watermark. `None` is an intentionally bounded
    /// first collection; adapters must not silently substitute an unbounded history scan.
    previous_fills_cursor: Option<String>,
}

impl AccountRecoveryRequest {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn configured_symbols(&self) -> &AccountSymbolSet {
        &self.configured_symbols
    }

    #[must_use]
    pub fn unresolved(&self) -> &[ExecutionCommand] {
        &self.unresolved
    }

    #[must_use]
    pub fn previous_fills_cursor(&self) -> Option<&str> {
        self.previous_fills_cursor.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRecoveryReport {
    binding: GatewayBinding,
    observed_at_ms: u64,
    outcomes: Vec<AccountRecoveryOutcome>,
}

impl AccountRecoveryReport {
    pub fn new(
        binding: GatewayBinding,
        observed_at_ms: u64,
        outcomes: Vec<AccountRecoveryOutcome>,
    ) -> Result<Self, AccountHostValidationError> {
        if observed_at_ms == 0 {
            return Err(AccountHostValidationError::Recovery);
        }
        Ok(Self {
            binding,
            observed_at_ms,
            outcomes,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn observed_at_ms(&self) -> u64 {
        self.observed_at_ms
    }

    #[must_use]
    pub fn outcomes(&self) -> &[AccountRecoveryOutcome] {
        &self.outcomes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRecoveryOutcome {
    command_id: CommandId,
    state: AccountRecoveryState,
}

impl AccountRecoveryOutcome {
    #[must_use]
    pub const fn accepted(command_id: CommandId, venue_order_id: String) -> Self {
        Self {
            command_id,
            state: AccountRecoveryState::Accepted { venue_order_id },
        }
    }

    #[must_use]
    pub const fn rejected(command_id: CommandId, reason: String) -> Self {
        Self {
            command_id,
            state: AccountRecoveryState::Rejected { reason },
        }
    }

    #[must_use]
    pub const fn still_unknown(command_id: CommandId) -> Self {
        Self {
            command_id,
            state: AccountRecoveryState::StillUnknown,
        }
    }

    #[must_use]
    pub fn command_id(&self) -> &CommandId {
        &self.command_id
    }

    #[must_use]
    pub const fn state(&self) -> &AccountRecoveryState {
        &self.state
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccountRecoveryState {
    Accepted { venue_order_id: String },
    Rejected { reason: String },
    StillUnknown,
}

/// Linear value; fields are private and this type is not cloneable or serializable.
pub struct AccountDispatchPermit {
    binding: GatewayBinding,
    command: ExecutionCommand,
    max_entry_notional: Decimal,
}

/// Linear proof that this exact command already owns a fsynced `Prepared` record in this
/// account's sole command WAL. Its fields stay private so a resident or Node caller cannot
/// manufacture a physical-dispatch capability from a command id.
#[derive(Debug)]
pub struct HostPreparedCommand {
    binding: GatewayBinding,
    command: ExecutionCommand,
    command_sha256: [u8; 32],
    receipt_sequence: u64,
    record_sha256: [u8; 32],
    cancel_target_family: Option<NativeOrderFamily>,
}

/// Read-only evidence for a currently open order that was recovered from the frozen Stage-7
/// WAL. It deliberately grants neither a new Owner nor a dispatch permit: later cancellation
/// must retain this exact historical Owner and pass through the ordinary account lane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyV1CustodyRoute {
    pub command_id: CommandId,
    pub owner: OrderOwner,
    pub family: NativeOrderFamily,
    pub client_order_id: CommandId,
    pub venue_order_id: String,
}

impl HostPreparedCommand {
    #[must_use]
    pub fn command_id(&self) -> &CommandId {
        self.command.command_id()
    }

    #[must_use]
    pub const fn command(&self) -> &ExecutionCommand {
        &self.command
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    /// Read-only allocation facts for Runtime's sealed lane admission. They are useless without
    /// this non-constructible capability, which `dispatch_prepared` re-verifies against the WAL.
    #[must_use]
    pub const fn receipt_sequence(&self) -> u64 {
        self.receipt_sequence
    }

    #[must_use]
    pub const fn receipt_digest(&self) -> [u8; 32] {
        self.record_sha256
    }

    #[must_use]
    pub const fn cancel_target_family(&self) -> Option<NativeOrderFamily> {
        self.cancel_target_family
    }
}

impl AccountDispatchPermit {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn command(&self) -> &ExecutionCommand {
        &self.command
    }

    #[must_use]
    pub const fn max_entry_notional(&self) -> Decimal {
        self.max_entry_notional
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccountGatewayResult {
    Accepted { venue_order_id: String },
    Rejected { reason: String },
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccountDispatchOutcome {
    Accepted { venue_order_id: String },
    Rejected { reason: String },
    Unknown,
}

/// Read-only projection of the sole command WAL. It is bound to an account and exact durable
/// receipt, but is neither a dispatch permit nor a way to recover the contained command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountCommandStatus {
    binding: GatewayBinding,
    command_id: CommandId,
    state: CommandState,
    sequence: u64,
    record_sha256: [u8; 32],
}

impl AccountCommandStatus {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn command_id(&self) -> &CommandId {
        &self.command_id
    }

    #[must_use]
    pub const fn state(&self) -> &CommandState {
        &self.state
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn record_sha256(&self) -> [u8; 32] {
        self.record_sha256
    }
}

#[derive(Debug)]
pub struct AccountMutationHost<G> {
    binding: GatewayBinding,
    configured_symbols: AccountSymbolSet,
    max_entry_notional: Decimal,
    journal_path: PathBuf,
    fills_cursor: Option<String>,
    latest_signed_snapshot: Option<SignedAccountSnapshot>,
    private_generation_floor: u64,
    private_generation_offset: u64,
    last_gateway_private_generation: Option<u64>,
    net_reduce_settlements: BTreeMap<CommandId, NetReduceSettlement>,
    journal: CommandJournal,
    gateway: G,
    _canonical_root: Option<AccountCanonicalRootGuard>,
    _legacy_predecessor: Option<LegacyV1WriterGuard>,
    legacy_v1_predecessor: Option<LegacyV1WriterPredecessor>,
    _account_lock: File,
}

impl<G: AccountPhysicalGateway> AccountMutationHost<G> {
    /// Executes a bounded adapter read while the account Host retains the only mutation permit
    /// issuer. The callback cannot obtain a permit; callers use this for normalized feed pumps.
    pub fn with_gateway_read<T>(
        &mut self,
        operation: impl FnOnce(&mut G) -> Result<T, G::Error>,
    ) -> Result<T, G::Error> {
        operation(&mut self.gateway)
    }

    /// Exposes only adapter-validated rules identity facts. The Host retains every mutation
    /// authority and performs no configuration-derived market/settlement inference.
    pub fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostError<G::Error>> {
        self.current_instrument_for(&self.binding.symbol.clone())
    }

    /// Returns a fresh adapter fact only for an explicitly configured symbol.  The account
    /// binding remains the common credential/WAL scope; symbol is not inferred from a command.
    pub fn current_instrument_for(
        &mut self,
        symbol: &venue_domain::domain::Symbol,
    ) -> Result<AccountInstrumentIdentity, AccountHostError<G::Error>> {
        if !self.configured_symbols.contains(symbol) {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::Scope,
            ));
        }
        let instrument = self
            .gateway
            .current_instrument_for(symbol)
            .map_err(AccountHostError::Validation)?;
        let valid_market = match instrument.identity.market {
            MarketKind::Spot => instrument.identity.settlement_asset.is_none(),
            MarketKind::LinearPerpetual => instrument
                .identity
                .settlement_asset
                .as_ref()
                .is_some_and(|asset| asset.as_str() == self.binding.symbol.quote()),
        };
        if instrument.rules_generation == 0
            || instrument.identity.symbol != *symbol
            || !valid_market
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::Instrument,
            ));
        }
        Ok(instrument)
    }

    pub fn open(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        max_entry_notional: Decimal,
        gateway: G,
    ) -> Result<Self, AccountHostError<G::Error>> {
        Self::open_with_symbols(
            artifacts_root,
            binding.clone(),
            AccountSymbolSet::single(&binding),
            max_entry_notional,
            gateway,
        )
    }

    pub fn open_with_symbols(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        configured_symbols: AccountSymbolSet,
        max_entry_notional: Decimal,
        gateway: G,
    ) -> Result<Self, AccountHostError<G::Error>> {
        Self::open_with_symbols_and_account_lock(
            artifacts_root,
            binding,
            configured_symbols,
            max_entry_notional,
            gateway,
            None,
        )
    }

    fn open_with_symbols_and_account_lock(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        configured_symbols: AccountSymbolSet,
        max_entry_notional: Decimal,
        mut gateway: G,
        preheld_account_lock: Option<File>,
    ) -> Result<Self, AccountHostError<G::Error>> {
        binding.validate().map_err(AccountHostError::Binding)?;
        if !configured_symbols.contains(&binding.symbol)
            || gateway.binding() != &binding
            || max_entry_notional != Decimal::TEN
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::Scope,
            ));
        }
        let artifacts_root = artifacts_root.into();
        validate_artifacts_root(&artifacts_root, &binding).map_err(AccountHostError::Validation)?;
        fs::create_dir_all(&artifacts_root).map_err(|source| AccountHostError::Io {
            path: artifacts_root.clone(),
            source,
        })?;
        let lock_path = artifacts_root.join(ACCOUNT_WRITER_LOCK_FILE);
        let account_lock = match preheld_account_lock {
            Some(lock) => lock,
            None => acquire_account_writer_lock(&lock_path)?,
        };
        let journal_path = artifacts_root.join("commands.jsonl");
        let historical_paths =
            journal_segment_paths(&artifacts_root).map_err(AccountHostError::Validation)?;
        for path in &historical_paths {
            require_journal_budget(path).map_err(AccountHostError::Validation)?;
        }
        require_journal_budget(&journal_path).map_err(AccountHostError::Validation)?;
        let mut journal = CommandJournal::open_segmented(&journal_path, &historical_paths)
            .map_err(AccountHostError::Journal)?;
        journal
            .fence_interrupted_dispatches()
            .map_err(AccountHostError::Journal)?;
        let unresolved_ids = journal.unresolved_command_ids();
        let unresolved = unresolved_ids
            .iter()
            .map(|command_id| {
                journal
                    .receipt(command_id)
                    .map(|receipt| receipt.command.clone())
                    .ok_or(AccountHostValidationError::Recovery)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(AccountHostError::Validation)?;
        let previous_bootstrap =
            load_previous_signed_bootstrap(&artifacts_root, &binding, RUNTIME_BOOTSTRAP_FILE)
                .map_err(AccountHostError::Validation)?;
        let previous_snapshot = previous_bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.snapshot.clone());
        let net_reduce_settlements = previous_bootstrap
            .as_ref()
            .map_or_else(BTreeMap::new, |bootstrap| {
                bootstrap.net_reduce_settlements.clone()
            });
        validate_recovered_net_reduce_settlements(
            &journal,
            previous_snapshot.as_ref(),
            &net_reduce_settlements,
        )
        .map_err(AccountHostError::Validation)?;
        let fills_cursor = previous_snapshot
            .as_ref()
            .map(|snapshot| snapshot.fills_cursor().to_owned());
        let request = AccountRecoveryRequest {
            binding: binding.clone(),
            configured_symbols: configured_symbols.clone(),
            unresolved,
            previous_fills_cursor: fills_cursor.clone(),
        };
        let report = gateway
            .reconcile(&request)
            .map_err(AccountHostError::Gateway)?;
        apply_recovery(&mut journal, &request, report).map_err(AccountHostError::Validation)?;
        rotate_if_clean_and_due(&mut journal, &journal_path).map_err(AccountHostError::Journal)?;
        require_journal_budget(&journal_path).map_err(AccountHostError::Validation)?;
        Ok(Self {
            binding,
            configured_symbols,
            max_entry_notional,
            journal_path,
            fills_cursor,
            private_generation_floor: previous_snapshot
                .as_ref()
                .map_or(0, SignedAccountSnapshot::private_generation),
            latest_signed_snapshot: previous_snapshot,
            private_generation_offset: 0,
            last_gateway_private_generation: None,
            net_reduce_settlements,
            journal,
            gateway,
            _canonical_root: None,
            _legacy_predecessor: None,
            legacy_v1_predecessor: None,
            _account_lock: account_lock,
        })
    }

    /// Takes over a frozen Stage-7 scope only from an explicit predecessor record. The legacy
    /// product account/hash are never inferred from the new trading account; both the exact v1
    /// lock and the current canonical account lock remain held for the full Host lifetime.
    pub fn open_with_legacy_v1_predecessor(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        max_entry_notional: Decimal,
        gateway: G,
        predecessor: LegacyV1WriterPredecessor,
    ) -> Result<Self, AccountHostError<G::Error>> {
        Self::open_with_symbols_and_legacy_v1_predecessor(
            artifacts_root,
            binding.clone(),
            AccountSymbolSet::single(&binding),
            max_entry_notional,
            gateway,
            predecessor,
        )
    }

    pub fn open_with_symbols_and_legacy_v1_predecessor(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        configured_symbols: AccountSymbolSet,
        max_entry_notional: Decimal,
        gateway: G,
        predecessor: LegacyV1WriterPredecessor,
    ) -> Result<Self, AccountHostError<G::Error>> {
        if predecessor.exchange != binding.venue
            || predecessor.successor_trading_account_id != binding.trading_account_id
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::LegacyPredecessor,
            ));
        }
        let artifacts_root = artifacts_root.into();
        validate_artifacts_root(&artifacts_root, &binding).map_err(AccountHostError::Validation)?;
        fs::create_dir_all(&artifacts_root).map_err(|source| AccountHostError::Io {
            path: artifacts_root.clone(),
            source,
        })?;
        let legacy = predecessor
            .acquire()
            .map_err(AccountHostError::CanonicalRoot)?;
        let account_lock =
            acquire_account_writer_lock(&artifacts_root.join(ACCOUNT_WRITER_LOCK_FILE))?;
        let scope = WriterScope {
            exchange: binding.venue.as_str().to_owned(),
            account: binding.trading_account_id.clone(),
            symbol: binding.symbol.clone(),
            owner_scope: "unified_account_runtime".to_owned(),
        };
        // Custody root remains the registered legacy root, while the unified runtime keeps its
        // own bounded WAL/checkpoint root. Rebinding Gate's existing v2 entry would fork writer
        // custody and is therefore fail-closed.
        let canonical = acquire_account_canonical_root(&scope, &predecessor.legacy_artifacts_root)
            .map_err(AccountHostError::CanonicalRoot)?;
        import_legacy_v1_journal_for_predecessor_if_needed(&predecessor, &artifacts_root)
            .map_err(AccountHostError::Validation)?;
        let mut host = Self::open_with_symbols_and_account_lock(
            artifacts_root,
            binding,
            configured_symbols,
            max_entry_notional,
            gateway,
            Some(account_lock),
        )?;
        host._canonical_root = Some(canonical);
        host._legacy_predecessor = Some(legacy);
        host.legacy_v1_predecessor = Some(predecessor);
        Ok(host)
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn configured_symbols(&self) -> &AccountSymbolSet {
        &self.configured_symbols
    }

    #[must_use]
    pub fn has_unresolved(&self) -> bool {
        self.journal.has_unresolved()
    }

    /// Read-only WAL commitment for the Runtime's actor-applied store.  It is not a command,
    /// permit, or mutation capability.
    pub fn runtime_wal_head(
        &self,
    ) -> Result<venue_storage::DurableWalHead, AccountHostError<G::Error>> {
        self.journal
            .durable_wal_head()
            .map_err(AccountHostError::Journal)
    }

    /// Verifies an Actor checkpoint's former WAL observation against this same recovered WAL.
    /// This is read-only and is used only by resident startup, never by dispatch.
    #[must_use]
    pub fn validates_historical_wal_head(&self, head: venue_storage::DurableWalHead) -> bool {
        self.journal.validates_historical_wal_head(head)
    }

    pub fn command_status(
        &self,
        command_id: &CommandId,
    ) -> Result<Option<AccountCommandStatus>, AccountHostError<G::Error>> {
        let Some(receipt) = self.journal.receipt(command_id) else {
            return Ok(None);
        };
        Ok(Some(AccountCommandStatus {
            binding: self.binding.clone(),
            command_id: command_id.clone(),
            state: receipt.state.clone(),
            sequence: receipt.sequence,
            record_sha256: receipt_sha256(receipt).map_err(AccountHostError::Validation)?,
        }))
    }

    /// Returns a detached command copied from this account's sole durable WAL receipt. It is
    /// observability only: the copy cannot construct a prepared proof or dispatch permit.
    #[must_use]
    pub fn command_snapshot(&self, command_id: &CommandId) -> Option<ExecutionCommand> {
        self.journal
            .receipt(command_id)
            .map(|receipt| receipt.command.clone())
    }

    /// Resolves a fill's native venue order id only through the Accepted WAL identity index.
    /// Ambiguous ids and an order-family mismatch deliberately remain unowned.
    #[must_use]
    pub fn command_snapshot_by_venue_order_id(
        &self,
        family: NativeOrderFamily,
        venue_order_id: &str,
    ) -> Option<ExecutionCommand> {
        let client_id = self.journal.client_id_by_venue_order_id(venue_order_id)?;
        let command_id = self.journal.command_id_by_client_id(client_id)?;
        let command = &self.journal.receipt(command_id)?.command;
        (command.native_order_family() == Some(family)).then(|| command.clone())
    }

    /// Complete accepted native identities for one exact Owner, recovered only from this Host's
    /// command WAL. Submitted/Unknown and ambiguous native ids stay out: Runtime must never
    /// manufacture a cancel route from a partial exchange observation.
    #[must_use]
    pub fn accepted_order_routes_for_owner(
        &self,
        owner: &OrderOwner,
    ) -> Vec<crate::NativeOrderRoute> {
        self.accepted_order_routes()
            .into_iter()
            .filter(|route| route.owner == *owner)
            .collect()
    }

    /// Complete accepted routes from this single WAL. The Runtime Host applies its own exact
    /// registered-owner filter before restoring router state for one actor.
    #[must_use]
    pub fn accepted_order_routes(&self) -> Vec<crate::NativeOrderRoute> {
        self.journal
            .native_order_routes()
            .into_iter()
            .filter(|route| {
                matches!(route.state, CommandState::Accepted { .. })
                    && route.venue_order_id.is_some()
            })
            .collect()
    }

    /// Refreshes the complete signed account view and returns only open native orders whose
    /// historical Owner exactly matches the frozen predecessor. This is custody evidence, not
    /// an adoption of the old Owner: unmatched/external orders remain outside this route and no
    /// command is appended. The later cancellation path must consume these exact identities.
    pub fn refresh_legacy_v1_custody_routes(
        &mut self,
    ) -> Result<Vec<LegacyV1CustodyRoute>, AccountHostError<G::Error>> {
        let snapshot = self.refresh_signed_snapshot()?;
        self.legacy_v1_custody_routes_from_snapshot(&snapshot)
    }

    /// Derives legacy cancellation custody from the last Host-persisted signed snapshot without
    /// performing another read.  A resident uses it only after it has installed that exact
    /// generation into Runtime and before it persists the Actor turn that admits a cancel.
    pub fn legacy_v1_custody_routes_from_latest_signed_snapshot(
        &self,
    ) -> Result<Vec<LegacyV1CustodyRoute>, AccountHostError<G::Error>> {
        let snapshot = self
            .latest_signed_snapshot
            .as_ref()
            .ok_or(AccountHostValidationError::LegacyPredecessor)
            .map_err(AccountHostError::Validation)?;
        self.legacy_v1_custody_routes_from_snapshot(snapshot)
    }

    fn legacy_v1_custody_routes_from_snapshot(
        &self,
        snapshot: &SignedAccountSnapshot,
    ) -> Result<Vec<LegacyV1CustodyRoute>, AccountHostError<G::Error>> {
        let predecessor = self
            .legacy_v1_predecessor
            .as_ref()
            .ok_or(AccountHostValidationError::LegacyPredecessor)
            .map_err(AccountHostError::Validation)?
            .clone();
        let now = now_ms().map_err(AccountHostError::Validation)?;
        if now < snapshot.observed_at_ms()
            || now.saturating_sub(snapshot.observed_at_ms()) > MAX_RISK_EVIDENCE_AGE_MS
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        let accepted = self.accepted_order_routes();
        let mut seen = BTreeSet::new();
        let mut routes = Vec::new();
        for fact in snapshot.open_orders() {
            let Some(owner) = fact.owner.as_ref() else {
                continue;
            };
            if fact.external || !predecessor.matches_legacy_owner(owner) {
                continue;
            }
            if !matches!(
                fact.state,
                Some(OrderState::New | OrderState::PartiallyFilled)
            ) || fact
                .filled_quantity
                .is_none_or(|filled| filled < Decimal::ZERO || filled >= fact.quantity)
            {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::LegacyPredecessor,
                ));
            }
            let Some(venue_order_id) = fact.venue_order_id.as_deref() else {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::LegacyPredecessor,
                ));
            };
            let matching = accepted
                .iter()
                .filter(|route| {
                    route.owner == *owner
                        && route.key.family == fact.family
                        && route.key.client_id.as_str() == fact.client_order_id
                        && route.venue_order_id.as_deref() == Some(venue_order_id)
                        && self
                            .journal
                            .receipt(&route.command_id)
                            .is_some_and(|receipt| {
                                self.signed_order_matches_command(&receipt.command, fact)
                            })
                })
                .collect::<Vec<_>>();
            let [route] = matching.as_slice() else {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::LegacyPredecessor,
                ));
            };
            let identity = (route.key.family, venue_order_id.to_owned());
            if !seen.insert(identity) {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::LegacyPredecessor,
                ));
            }
            routes.push(LegacyV1CustodyRoute {
                command_id: route.command_id.clone(),
                owner: route.owner.clone(),
                family: route.key.family,
                client_order_id: route.key.client_id.clone(),
                venue_order_id: venue_order_id.to_owned(),
            });
        }
        Ok(routes)
    }

    pub fn refresh_signed_snapshot(
        &mut self,
    ) -> Result<SignedAccountSnapshot, AccountHostError<G::Error>> {
        let request = AccountRecoveryRequest {
            binding: self.binding.clone(),
            configured_symbols: self.configured_symbols.clone(),
            unresolved: Vec::new(),
            previous_fills_cursor: self.fills_cursor.clone(),
        };
        let mut snapshot = self
            .gateway
            .signed_account_snapshot(&request)
            .map_err(AccountHostError::Validation)?;
        self.ratchet_private_generation(&mut snapshot)?;
        if snapshot.binding() != &self.binding {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        self.enrich_signed_order_owners(&mut snapshot);
        self.persist_signed_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    /// Re-reads the complete signed account snapshot for every unresolved WAL identity, then
    /// settles only the requested identity from its exact signed result. UNKNOWN remains durable
    /// and is never resubmitted by this read-only convergence path.
    pub fn reconcile_command_status(
        &mut self,
        command_id: &CommandId,
    ) -> Result<Option<AccountCommandStatus>, AccountHostError<G::Error>> {
        let Some(current) = self.journal.receipt(command_id).cloned() else {
            return Ok(None);
        };
        if !matches!(
            current.state,
            CommandState::Submitted | CommandState::Unknown { .. }
        ) {
            return self.command_status(command_id);
        }
        let unresolved = self
            .journal
            .unresolved_command_ids()
            .iter()
            .map(|id| {
                self.journal
                    .receipt(id)
                    .map(|receipt| receipt.command.clone())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(AccountHostValidationError::Recovery)
            .map_err(AccountHostError::Validation)?;
        let request = AccountRecoveryRequest {
            binding: self.binding.clone(),
            configured_symbols: self.configured_symbols.clone(),
            unresolved,
            previous_fills_cursor: self.fills_cursor.clone(),
        };
        let mut snapshot = self
            .gateway
            .signed_account_snapshot(&request)
            .map_err(AccountHostError::Validation)?;
        self.ratchet_private_generation(&mut snapshot)?;
        let now = now_ms().map_err(AccountHostError::Validation)?;
        if snapshot.binding() != &self.binding
            || now < snapshot.observed_at_ms()
            || now.saturating_sub(snapshot.observed_at_ms()) > MAX_RISK_EVIDENCE_AGE_MS
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        let actual = snapshot
            .unknown_results()
            .iter()
            .map(|fact| (fact.command_id.clone(), &fact.result))
            .collect::<BTreeMap<_, _>>();
        if actual.len() != request.unresolved.len()
            || request
                .unresolved
                .iter()
                .any(|command| !actual.contains_key(command.command_id()))
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        match actual
            .get(command_id)
            .ok_or(AccountHostValidationError::SignedSnapshot)
            .map_err(AccountHostError::Validation)?
        {
            SignedUnknownResult::Accepted { venue_order_id } => {
                self.journal
                    .transition(
                        command_id,
                        CommandState::Accepted {
                            venue_order_id: venue_order_id.clone(),
                        },
                    )
                    .map_err(AccountHostError::Journal)?;
            }
            SignedUnknownResult::Rejected { reason } => {
                self.journal
                    .transition(
                        command_id,
                        CommandState::Rejected {
                            reason: reason.clone(),
                        },
                    )
                    .map_err(AccountHostError::Journal)?;
            }
            SignedUnknownResult::Unknown => {}
        }
        self.enrich_signed_order_owners(&mut snapshot);
        self.persist_signed_snapshot(&snapshot)?;
        self.command_status(command_id)
    }

    pub fn durable_runtime_bootstrap(
        &mut self,
    ) -> Result<RuntimeBootstrapReceipt, AccountHostError<G::Error>> {
        self.collect_durable_runtime_snapshot()
    }

    /// Re-collects the complete signed account state for a Runtime that is already Ready. The
    /// Host requests every WAL UNKNOWN and settles only exact signed terminal outcomes before
    /// publishing the replacement checkpoint; a caller cannot clear an UNKNOWN with a snapshot.
    pub fn durable_runtime_refresh(
        &mut self,
    ) -> Result<RuntimeBootstrapReceipt, AccountHostError<G::Error>> {
        self.collect_durable_runtime_snapshot()
    }

    fn collect_durable_runtime_snapshot(
        &mut self,
    ) -> Result<RuntimeBootstrapReceipt, AccountHostError<G::Error>> {
        let unresolved = self
            .journal
            .unresolved_command_ids()
            .iter()
            .map(|command_id| {
                self.journal
                    .receipt(command_id)
                    .map(|receipt| receipt.command.clone())
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(AccountHostValidationError::Recovery)
            .map_err(AccountHostError::Validation)?;
        let request = AccountRecoveryRequest {
            binding: self.binding.clone(),
            configured_symbols: self.configured_symbols.clone(),
            unresolved,
            previous_fills_cursor: self.fills_cursor.clone(),
        };
        let mut snapshot = self
            .gateway
            .signed_account_snapshot(&request)
            .map_err(AccountHostError::Validation)?;
        self.ratchet_private_generation(&mut snapshot)?;
        self.validate_runtime_snapshot(&snapshot, &request)?;
        self.settle_signed_unknown_results(&snapshot)?;
        self.enrich_signed_order_owners(&mut snapshot);
        let external_order = snapshot.open_orders().iter().any(|fact| fact.external);
        self.persist_signed_snapshot(&snapshot)?;
        let risk_fenced = external_order
            || !snapshot.open_orders().is_empty()
            || snapshot
                .positions()
                .iter()
                .any(|position| !position.quantity.is_zero())
            || self.journal.has_unresolved();
        Ok(RuntimeBootstrapReceipt {
            snapshot,
            risk_fenced,
            wal_head: self.runtime_wal_head()?,
        })
    }

    fn validate_runtime_snapshot(
        &self,
        snapshot: &SignedAccountSnapshot,
        request: &AccountRecoveryRequest,
    ) -> Result<(), AccountHostError<G::Error>> {
        let now = now_ms().map_err(AccountHostError::Validation)?;
        if snapshot.binding() != &self.binding
            || now < snapshot.observed_at_ms()
            || now.saturating_sub(snapshot.observed_at_ms()) > MAX_RISK_EVIDENCE_AGE_MS
            || !snapshot_covers_configured_symbols(snapshot, &self.configured_symbols)
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        let actual = snapshot
            .unknown_results()
            .iter()
            .map(|fact| (fact.command_id.clone(), ()))
            .collect::<BTreeMap<_, _>>();
        if request.unresolved.len() != actual.len()
            || request
                .unresolved
                .iter()
                .any(|command| !actual.contains_key(command.command_id()))
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        Ok(())
    }

    fn settle_signed_unknown_results(
        &mut self,
        snapshot: &SignedAccountSnapshot,
    ) -> Result<(), AccountHostError<G::Error>> {
        for fact in snapshot.unknown_results() {
            let state = match &fact.result {
                SignedUnknownResult::Accepted { venue_order_id } => CommandState::Accepted {
                    venue_order_id: venue_order_id.clone(),
                },
                SignedUnknownResult::Rejected { reason } => CommandState::Rejected {
                    reason: reason.clone(),
                },
                SignedUnknownResult::Unknown => continue,
            };
            self.journal
                .transition(&fact.command_id, state)
                .map_err(AccountHostError::Journal)?;
        }
        Ok(())
    }

    fn persist_signed_snapshot(
        &mut self,
        snapshot: &SignedAccountSnapshot,
    ) -> Result<(), AccountHostError<G::Error>> {
        let net_reduce_settlements = self.completed_net_reductions(snapshot)?;
        let path = self
            .journal_path
            .parent()
            .ok_or(AccountHostValidationError::ArtifactsRoot)
            .map_err(AccountHostError::Validation)?
            .join(RUNTIME_BOOTSTRAP_FILE);
        let encoded = serde_json::to_vec(&PersistedSignedBootstrap {
            snapshot: snapshot.clone(),
            net_reduce_settlements: net_reduce_settlements.clone(),
        })
        .map_err(|_| AccountHostError::Validation(AccountHostValidationError::SignedSnapshot))?;
        if encoded.len() > RUNTIME_CHECKPOINT_LIMIT_BYTES {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        let temporary = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|source| AccountHostError::Io {
                path: temporary.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .map_err(|source| AccountHostError::Io {
                path: temporary.clone(),
                source,
            })?;
        file.sync_all().map_err(|source| AccountHostError::Io {
            path: temporary.clone(),
            source,
        })?;
        drop(file);
        fs::rename(&temporary, &path).map_err(|source| AccountHostError::Io {
            path: path.clone(),
            source,
        })?;
        crate::journal::sync_parent(&path).map_err(AccountHostError::Journal)?;
        self.net_reduce_settlements = net_reduce_settlements;
        self.fills_cursor = Some(snapshot.fills_cursor().to_owned());
        self.private_generation_floor = snapshot.private_generation();
        self.latest_signed_snapshot = Some(snapshot.clone());
        Ok(())
    }

    fn ratchet_private_generation(
        &mut self,
        snapshot: &mut SignedAccountSnapshot,
    ) -> Result<(), AccountHostError<G::Error>> {
        let gateway_generation = snapshot.private_generation();
        if let Some(previous) = self.last_gateway_private_generation {
            if gateway_generation <= previous {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::SignedSnapshot,
                ));
            }
        } else if gateway_generation <= self.private_generation_floor {
            let next = self
                .private_generation_floor
                .checked_add(1)
                .ok_or(AccountHostValidationError::SignedSnapshot)
                .map_err(AccountHostError::Validation)?;
            self.private_generation_offset = next
                .checked_sub(gateway_generation)
                .ok_or(AccountHostValidationError::SignedSnapshot)
                .map_err(AccountHostError::Validation)?;
        }
        let normalized = gateway_generation
            .checked_add(self.private_generation_offset)
            .ok_or(AccountHostValidationError::SignedSnapshot)
            .map_err(AccountHostError::Validation)?;
        if normalized <= self.private_generation_floor {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        if normalized != gateway_generation {
            snapshot
                .rebase_private_generation(normalized)
                .map_err(AccountHostError::Validation)?;
        }
        self.last_gateway_private_generation = Some(gateway_generation);
        Ok(())
    }

    /// Fsyncs the sole WAL's `Prepared` record and returns its linear dispatch capability.
    /// Repeating the exact command returns a capability for the same durable receipt; a changed
    /// command id or any non-Prepared receipt never receives another capability.
    pub fn prepare_for_lane(
        &mut self,
        command: ExecutionCommand,
    ) -> Result<HostPreparedCommand, AccountHostError<G::Error>> {
        validate_command_scope(&command, &self.binding, &self.configured_symbols)
            .map_err(AccountHostError::Validation)?;
        self.validate_net_command_against_signed_snapshot(&command)?;
        let command_id = command.command_id().clone();
        if let Some(receipt) = self.journal.receipt(&command_id) {
            if receipt.command != command {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::Duplicate,
                ));
            }
            return self.host_prepared_command(command, receipt);
        }
        if is_risk_increasing(&command) && self.journal.has_unresolved() {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::UnknownFence,
            ));
        }
        if is_risk_increasing(&command) && has_open_entry_reservation(&self.journal) {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::OpenEntryFence,
            ));
        }
        if is_risk_increasing(&command) {
            self.require_account_risk_headroom(&command)
                .map_err(AccountHostError::Validation)?;
        }
        rotate_if_clean_and_due(&mut self.journal, &self.journal_path)
            .map_err(AccountHostError::Journal)?;
        require_append_budget(&self.journal_path, &command)
            .map_err(AccountHostError::Validation)?;
        let receipt = self
            .journal
            .prepare(command.clone())
            .map_err(AccountHostError::Journal)?
            .clone();
        self.host_prepared_command(command, &receipt)
    }

    /// The sole legacy mutation entrance.  It re-derives the route from the latest persisted
    /// signed snapshot, retains the historical Owner, and can append only an exact Cancel.
    pub fn prepare_legacy_v1_custody_cancel_for_lane(
        &mut self,
        route: &LegacyV1CustodyRoute,
    ) -> Result<HostPreparedCommand, AccountHostError<G::Error>> {
        if !self
            .legacy_v1_custody_routes_from_latest_signed_snapshot()?
            .iter()
            .any(|candidate| candidate == route)
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::LegacyPredecessor,
            ));
        }
        let command = ExecutionCommand::Cancel(CancelCommand {
            command_id: legacy_v1_custody_cancel_command_id(route)
                .map_err(AccountHostError::Validation)?,
            owner: route.owner.clone(),
            target_client_order_id: route.client_order_id.clone(),
        });
        self.prepare_legacy_v1_cancel_after_signed_route(command, route)
    }

    fn prepare_legacy_v1_cancel_after_signed_route(
        &mut self,
        command: ExecutionCommand,
        route: &LegacyV1CustodyRoute,
    ) -> Result<HostPreparedCommand, AccountHostError<G::Error>> {
        let ExecutionCommand::Cancel(cancel) = &command else {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::LegacyPredecessor,
            ));
        };
        if cancel.owner != route.owner
            || cancel.target_client_order_id != route.client_order_id
            || route.owner.exchange != self.binding.venue.as_str()
            || !self.configured_symbols.contains(&route.owner.symbol)
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::LegacyPredecessor,
            ));
        }
        self.validate_net_command_against_signed_snapshot(&command)?;
        let command_id = command.command_id().clone();
        if let Some(receipt) = self.journal.receipt(&command_id) {
            if receipt.command != command {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::Duplicate,
                ));
            }
            return self.host_prepared_command(command, receipt);
        }
        rotate_if_clean_and_due(&mut self.journal, &self.journal_path)
            .map_err(AccountHostError::Journal)?;
        require_append_budget(&self.journal_path, &command)
            .map_err(AccountHostError::Validation)?;
        let receipt = self
            .journal
            .prepare(command.clone())
            .map_err(AccountHostError::Journal)?
            .clone();
        self.host_prepared_command(command, &receipt)
    }

    /// Consumes one Prepared capability after re-reading its exact WAL receipt. In particular,
    /// this never calls `prepare`: dispatch-after-crash is either the already prepared command
    /// or a fail-closed error, never a second exchange mutation.
    pub fn dispatch_prepared(
        &mut self,
        prepared: HostPreparedCommand,
    ) -> Result<AccountDispatchOutcome, AccountHostError<G::Error>> {
        if prepared.binding != self.binding {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::PreparedCommand,
            ));
        }
        let command_id = prepared.command.command_id().clone();
        let receipt = self
            .journal
            .receipt(&command_id)
            .ok_or(AccountHostError::Validation(
                AccountHostValidationError::PreparedCommand,
            ))?;
        self.verify_prepared_command(&prepared, receipt)?;
        if let Err(error) = self.validate_net_command_against_signed_snapshot(&prepared.command) {
            // The exact proof remains Prepared and no gateway call has occurred. Preserve the
            // signed-position admission failure durably so a stale Net reduction cannot retain
            // a reservation after its private-generation or freshness fence expires.
            self.journal
                .transition(
                    &command_id,
                    CommandState::Rejected {
                        reason: "dispatch_signed_position_recheck_failed".to_owned(),
                    },
                )
                .map_err(AccountHostError::Journal)?;
            return Err(error);
        }
        // A Prepared WAL record may have waited behind the lane. Re-read the signed evidence at
        // the physical dispatch boundary so neither an expired rate nor a changed account total
        // can reach the venue merely because admission happened earlier.
        if is_risk_increasing(&prepared.command)
            && let Err(error) = self.require_account_risk_headroom(&prepared.command)
        {
            // This proof is still exactly Prepared and has not crossed the physical boundary.
            // Make that non-dispatch durable so its entry reservation cannot survive a failed
            // final risk check and fence the account forever.
            self.journal
                .transition(
                    &command_id,
                    CommandState::Rejected {
                        reason: "dispatch_risk_recheck_failed".to_owned(),
                    },
                )
                .map_err(AccountHostError::Journal)?;
            return Err(AccountHostError::Validation(error));
        }
        self.journal
            .transition(&command_id, CommandState::Submitted)
            .map_err(AccountHostError::Journal)?;
        let result = self.gateway.dispatch(AccountDispatchPermit {
            binding: self.binding.clone(),
            command: prepared.command,
            max_entry_notional: self.max_entry_notional,
        });
        let outcome = match result {
            AccountGatewayResult::Accepted { venue_order_id } if valid_text(&venue_order_id) => {
                self.journal
                    .transition(
                        &command_id,
                        CommandState::Accepted {
                            venue_order_id: venue_order_id.clone(),
                        },
                    )
                    .map_err(AccountHostError::Journal)?;
                AccountDispatchOutcome::Accepted { venue_order_id }
            }
            AccountGatewayResult::Rejected { reason } if valid_text(&reason) => {
                self.journal
                    .transition(
                        &command_id,
                        CommandState::Rejected {
                            reason: reason.clone(),
                        },
                    )
                    .map_err(AccountHostError::Journal)?;
                AccountDispatchOutcome::Rejected { reason }
            }
            AccountGatewayResult::Accepted { .. } | AccountGatewayResult::Rejected { .. } => {
                self.journal
                    .transition(
                        &command_id,
                        CommandState::Unknown {
                            reason: "invalid_gateway_outcome".to_owned(),
                        },
                    )
                    .map_err(AccountHostError::Journal)?;
                AccountDispatchOutcome::Unknown
            }
            AccountGatewayResult::Unknown => {
                self.journal
                    .transition(
                        &command_id,
                        CommandState::Unknown {
                            reason: "gateway_result_unknown".to_owned(),
                        },
                    )
                    .map_err(AccountHostError::Journal)?;
                AccountDispatchOutcome::Unknown
            }
        };
        rotate_if_clean_and_due(&mut self.journal, &self.journal_path)
            .map_err(AccountHostError::Journal)?;
        require_journal_budget(&self.journal_path).map_err(AccountHostError::Validation)?;
        Ok(outcome)
    }

    /// Terminates an exact WAL `Prepared` proof that the resident has deliberately removed from
    /// its in-memory lane before any physical dispatch.  This cannot affect Submitted or
    /// Unknown commands: both the proof and current receipt must still be byte-for-byte the
    /// original Prepared record.
    pub fn reject_prepared_without_dispatch(
        &mut self,
        prepared: &HostPreparedCommand,
        reason: &str,
    ) -> Result<(), AccountHostError<G::Error>> {
        if prepared.binding != self.binding || !valid_text(reason) {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::PreparedCommand,
            ));
        }
        let command_id = prepared.command_id();
        let receipt = self
            .journal
            .receipt(command_id)
            .ok_or(AccountHostError::Validation(
                AccountHostValidationError::PreparedCommand,
            ))?;
        self.verify_prepared_command(prepared, receipt)?;
        self.journal
            .transition(
                command_id,
                CommandState::Rejected {
                    reason: reason.to_owned(),
                },
            )
            .map(|_| ())
            .map_err(AccountHostError::Journal)
    }

    /// Test-only convenience; production composition cannot invoke a direct command dispatch.
    #[cfg(test)]
    fn dispatch(
        &mut self,
        command: ExecutionCommand,
    ) -> Result<AccountDispatchOutcome, AccountHostError<G::Error>> {
        let prepared = self.prepare_for_lane(command)?;
        self.dispatch_prepared(prepared)
    }

    fn host_prepared_command(
        &self,
        command: ExecutionCommand,
        receipt: &crate::CommandReceipt,
    ) -> Result<HostPreparedCommand, AccountHostError<G::Error>> {
        if receipt.command != command || !matches!(receipt.state, CommandState::Prepared) {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::PreparedCommand,
            ));
        }
        let command_sha256 = crate::execution_command_sha256(&command)
            .map_err(|_| AccountHostError::Validation(AccountHostValidationError::Command))?;
        if receipt.command_sha256 != hex_sha256(command_sha256) {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::PreparedCommand,
            ));
        }
        Ok(HostPreparedCommand {
            binding: self.binding.clone(),
            command,
            command_sha256,
            receipt_sequence: receipt.sequence,
            record_sha256: receipt_sha256(receipt).map_err(AccountHostError::Validation)?,
            cancel_target_family: self
                .journal
                .cancel_target_identity(receipt.command.command_id())
                .map(|identity| identity.family),
        })
    }

    fn verify_prepared_command(
        &self,
        prepared: &HostPreparedCommand,
        receipt: &crate::CommandReceipt,
    ) -> Result<(), AccountHostError<G::Error>> {
        if !matches!(receipt.state, CommandState::Prepared)
            || receipt.command != prepared.command
            || receipt.sequence != prepared.receipt_sequence
            || receipt.command_sha256 != hex_sha256(prepared.command_sha256)
            || receipt_sha256(receipt).map_err(AccountHostError::Validation)?
                != prepared.record_sha256
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::PreparedCommand,
            ));
        }
        Ok(())
    }

    fn require_account_risk_headroom(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<AccountRiskSummary, AccountHostValidationError> {
        let evidence = self.gateway.risk_evidence()?;
        evidence.validate_for(&self.binding, now_ms()?)?;
        let candidate_entry = quote_notional(command)?;
        let summary = AccountRiskSummary {
            signed_positions: evidence.signed_position_total()?,
            open_entry_orders: evidence.open_entry_order_total()?,
            wal_entry_reservations: wal_entry_reservation_total(&self.journal, &evidence)?,
            candidate_entry: evidence
                .value_in_usdt(&candidate_entry.asset, candidate_entry.value)?,
        };
        if summary.total()? > self.max_entry_notional {
            return Err(AccountHostValidationError::AccountRiskLimit);
        }
        Ok(summary)
    }

    fn validate_net_command_against_signed_snapshot(
        &self,
        command: &ExecutionCommand,
    ) -> Result<(), AccountHostError<G::Error>> {
        let (owner, position_side, position_generation) = match command {
            ExecutionCommand::PlaceLimit(command) => (&command.owner, command.position_side, None),
            ExecutionCommand::PlaceMarket(command) => (&command.owner, command.position_side, None),
            ExecutionCommand::MarketReduce(command) => (
                &command.owner,
                command.position_side,
                Some(command.position_generation),
            ),
            ExecutionCommand::StopMarketCloseAll(command) => (
                &command.owner,
                command.position_side,
                Some(command.position_generation),
            ),
            ExecutionCommand::StopMarketFullPosition(command) => (
                &command.owner,
                command.position_side,
                Some(command.position_generation),
            ),
            ExecutionCommand::Cancel(_) => return Ok(()),
        };
        if position_side != PositionSide::Net {
            return Ok(());
        }
        let snapshot = self
            .latest_signed_snapshot
            .as_ref()
            .ok_or(AccountHostValidationError::SignedSnapshot)
            .map_err(AccountHostError::Validation)?;
        let now = now_ms().map_err(AccountHostError::Validation)?;
        if snapshot.binding() != &self.binding
            || snapshot.position_mode() != SignedAccountPositionMode::Net
            || now < snapshot.observed_at_ms()
            || now.saturating_sub(snapshot.observed_at_ms()) > MAX_RISK_EVIDENCE_AGE_MS
            || position_generation
                .is_some_and(|generation| generation != snapshot.private_generation())
        {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        let positions = snapshot
            .positions()
            .iter()
            .filter(|position| {
                position.symbol == owner.symbol && position.position_side == PositionSide::Net
            })
            .collect::<Vec<_>>();
        let [fact] = positions.as_slice() else {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        };
        let mut quantity = fact.quantity;
        if matches!(command, ExecutionCommand::MarketReduce(_)) {
            // An unresolved reduction stays reserved across a later signed generation. A
            // reconnect must not make an Unknown physical reduction disappear from admission.
            let reserved = self.pending_net_reduce_quantity(command.command_id(), &owner.symbol)?;
            let remaining = quantity
                .abs()
                .checked_sub(reserved)
                .filter(|value| !value.is_sign_negative())
                .ok_or(AccountHostValidationError::SignedSnapshot)
                .map_err(AccountHostError::Validation)?;
            quantity = if quantity.is_sign_negative() {
                -remaining
            } else {
                remaining
            };
        }
        let position = Position {
            symbol: fact.symbol.clone(),
            side: PositionSide::Net,
            quantity,
            entry_price: fact.entry_price.map(Price::new).transpose().map_err(|_| {
                AccountHostError::Validation(AccountHostValidationError::SignedSnapshot)
            })?,
            mark_price: fact.mark_price.map(Price::new).transpose().map_err(|_| {
                AccountHostError::Validation(AccountHostValidationError::SignedSnapshot)
            })?,
        };
        let valid = match command {
            ExecutionCommand::PlaceLimit(command) => {
                command.validate_with_authoritative_position(&position)
            }
            ExecutionCommand::PlaceMarket(command) => {
                command.validate_with_authoritative_position(&position)
            }
            ExecutionCommand::MarketReduce(command) => {
                command.validate_with_authoritative_position(&position)
            }
            ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                Err(venue_domain::domain::CommandError::PositionSide)
            }
            ExecutionCommand::Cancel(_) => Ok(()),
        };
        valid.map_err(|_| AccountHostError::Validation(AccountHostValidationError::Command))
    }

    fn pending_net_reduce_quantity(
        &self,
        excluding: &CommandId,
        symbol: &venue_domain::domain::Symbol,
    ) -> Result<Decimal, AccountHostError<G::Error>> {
        self.journal
            .commands()
            .filter_map(|candidate| {
                let ExecutionCommand::MarketReduce(reduce) = candidate else {
                    return None;
                };
                if reduce.command_id == *excluding
                    || reduce.position_side != PositionSide::Net
                    || reduce.owner.symbol != *symbol
                {
                    return None;
                }
                let receipt = self.journal.receipt(&reduce.command_id)?;
                let retained = match receipt.state {
                    CommandState::Rejected { .. } => false,
                    CommandState::Accepted { .. } => {
                        !self.net_reduce_settlements.contains_key(&reduce.command_id)
                    }
                    CommandState::Prepared
                    | CommandState::Submitted
                    | CommandState::Unknown { .. } => true,
                };
                retained.then_some(reduce.quantity)
            })
            .try_fold(Decimal::ZERO, |total, quantity| {
                total
                    .checked_add(quantity)
                    .ok_or(AccountHostError::Validation(
                        AccountHostValidationError::Notional,
                    ))
            })
    }

    fn completed_net_reductions(
        &self,
        snapshot: &SignedAccountSnapshot,
    ) -> Result<BTreeMap<CommandId, NetReduceSettlement>, AccountHostError<G::Error>> {
        if snapshot.binding() != &self.binding {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        let mut settlements = self.net_reduce_settlements.clone();
        if snapshot.position_mode() != SignedAccountPositionMode::Net {
            return Ok(settlements);
        }
        if !snapshot_covers_configured_symbols(snapshot, &self.configured_symbols) {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::SignedSnapshot,
            ));
        }
        let candidates = self
            .journal
            .commands()
            .filter_map(|command| {
                let ExecutionCommand::MarketReduce(reduce) = command else {
                    return None;
                };
                (reduce.position_side == PositionSide::Net).then_some(reduce.clone())
            })
            .collect::<Vec<_>>();
        for reduce in candidates {
            if self.net_reduce_settlements.contains_key(&reduce.command_id) {
                continue;
            }
            let Some(receipt) = self.journal.receipt(&reduce.command_id) else {
                return Err(AccountHostError::Validation(
                    AccountHostValidationError::Recovery,
                ));
            };
            let CommandState::Accepted { venue_order_id } = &receipt.state else {
                continue;
            };
            let Some(settlement) =
                completed_net_reduce_settlement(snapshot, &reduce, venue_order_id)
                    .map_err(AccountHostError::Validation)?
            else {
                continue;
            };
            settlements.insert(reduce.command_id.clone(), settlement);
        }
        Ok(settlements)
    }
}

fn apply_recovery(
    journal: &mut CommandJournal,
    request: &AccountRecoveryRequest,
    report: AccountRecoveryReport,
) -> Result<(), AccountHostValidationError> {
    if report.binding != request.binding || report.observed_at_ms == 0 {
        return Err(AccountHostValidationError::Recovery);
    }
    let expected = request
        .unresolved
        .iter()
        .map(|command| (command.command_id().clone(), command))
        .collect::<BTreeMap<_, _>>();
    let actual = report
        .outcomes
        .iter()
        .map(|outcome| (outcome.command_id.clone(), outcome))
        .collect::<BTreeMap<_, _>>();
    if expected.len() != request.unresolved.len()
        || actual.len() != report.outcomes.len()
        || expected.len() != actual.len()
    {
        return Err(AccountHostValidationError::Recovery);
    }
    for (command_id, outcome) in actual {
        if !expected.contains_key(&command_id) {
            return Err(AccountHostValidationError::Recovery);
        }
        let state = match &outcome.state {
            AccountRecoveryState::Accepted { venue_order_id } if valid_text(venue_order_id) => {
                Some(CommandState::Accepted {
                    venue_order_id: venue_order_id.clone(),
                })
            }
            AccountRecoveryState::Rejected { reason } if valid_text(reason) => {
                Some(CommandState::Rejected {
                    reason: reason.clone(),
                })
            }
            AccountRecoveryState::StillUnknown => None,
            AccountRecoveryState::Accepted { .. } | AccountRecoveryState::Rejected { .. } => {
                return Err(AccountHostValidationError::Recovery);
            }
        };
        if let Some(state) = state {
            journal
                .transition(&command_id, state)
                .map_err(|_| AccountHostValidationError::Recovery)?;
        }
    }
    Ok(())
}

fn validate_artifacts_root(
    root: &Path,
    binding: &GatewayBinding,
) -> Result<(), AccountHostValidationError> {
    if !root.is_absolute()
        || !root.ends_with(
            Path::new(binding.venue.as_str())
                .join(binding.mode.as_str())
                .join(&binding.trading_account_id),
        )
    {
        return Err(AccountHostValidationError::ArtifactsRoot);
    }
    Ok(())
}

/// Compares an exact adapter readback to the durable command before an UNKNOWN may settle.
/// A matching client id alone never proves that a different order carried out this command.
pub fn command_matches_readback_order(command: &ExecutionCommand, order: &Order) -> bool {
    let expected_client_id = match command {
        ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
        _ => match command.native_client_id() {
            Some(client_id) => client_id.as_str(),
            None => return false,
        },
    };
    if order.client_order_id != FieldState::Known(expected_client_id.to_owned()) {
        return false;
    }
    match command {
        ExecutionCommand::PlaceLimit(command) => {
            command.owner.symbol == order.symbol
                && command.side == order.side
                && order.position_side == FieldState::Known(command.position_side)
                && command.quantity == order.quantity
                && order.limit_price == Some(command.limit_price)
                && order.time_in_force == FieldState::Known(command.time_in_force)
                && command.reduce_only == order.reduce_only
        }
        ExecutionCommand::PlaceMarket(command) => {
            command.owner.symbol == order.symbol
                && command.side == order.side
                && order.position_side == FieldState::Known(command.position_side)
                && command.quantity == order.quantity
                && order.limit_price.is_none()
                && !order.reduce_only
        }
        ExecutionCommand::MarketReduce(command) => {
            command.owner.symbol == order.symbol
                && command.side == order.side
                && order.position_side == FieldState::Known(command.position_side)
                && command.quantity == order.quantity
                && order.limit_price.is_none()
                && order.reduce_only
        }
        ExecutionCommand::StopMarketFullPosition(command) => {
            command.owner.symbol == order.symbol
                && command.side == order.side
                && order.position_side == FieldState::Known(command.position_side)
                && command.quantity == order.quantity
                && order.limit_price == Some(command.trigger_price)
                && order.reduce_only
        }
        ExecutionCommand::Cancel(_) => matches!(
            order.state,
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
        ),
        ExecutionCommand::StopMarketCloseAll(_) => false,
    }
}

/// One-time custody-preserving import of a frozen Stage-7 command journal.  The caller already
/// holds the predecessor's exclusive lock, so the source cannot advance while it is verified and
/// copied.  The source remains untouched; a partial destination intentionally remains fenced
/// instead of being cleaned or resumed automatically.
/// Verifies that every preserved physical mutation belongs to the exact immutable predecessor
/// Owner before importing its frozen WAL. A matching legacy scope alone is insufficient: the
/// successor must never select an arbitrary strategy/run from a mixed historical journal.
fn import_legacy_v1_journal_for_predecessor_if_needed(
    predecessor: &LegacyV1WriterPredecessor,
    destination_root: &Path,
) -> Result<(), AccountHostValidationError> {
    let legacy_root = fs::canonicalize(&predecessor.legacy_artifacts_root)
        .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    let source = legacy_root.join("commands.jsonl");
    let source =
        fs::canonicalize(&source).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    if !source.starts_with(&legacy_root) {
        return Err(AccountHostValidationError::LegacyPredecessor);
    }
    let journal =
        CommandJournal::open(&source).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    if journal
        .commands()
        .any(|command| !predecessor.matches_legacy_owner(command.mutation_owner()))
    {
        return Err(AccountHostValidationError::LegacyPredecessor);
    }
    import_legacy_v1_journal_if_needed(&legacy_root, destination_root)
}

fn import_legacy_v1_journal_if_needed(
    legacy_root: &Path,
    destination_root: &Path,
) -> Result<(), AccountHostValidationError> {
    let legacy_root =
        fs::canonicalize(legacy_root).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    let source = legacy_root.join("commands.jsonl");
    let source =
        fs::canonicalize(&source).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    if !source.starts_with(&legacy_root)
        || !fs::metadata(&source)
            .map(|metadata| metadata.is_file() && metadata.len() > 0)
            .unwrap_or(false)
    {
        return Err(AccountHostValidationError::LegacyPredecessor);
    }
    let source_sha256 = file_sha256(&source)?;
    let source_bytes = fs::metadata(&source)
        .map_err(|_| AccountHostValidationError::LegacyPredecessor)?
        .len();
    let marker = destination_root.join(LEGACY_V1_IMPORT_FILE);
    if marker.exists() {
        let encoded =
            fs::read(&marker).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        let imported: LegacyV1JournalImport = serde_json::from_slice(&encoded)
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        if imported.schema_version != LEGACY_V1_IMPORT_SCHEMA_VERSION
            || imported.source_path != source
            || imported.source_sha256 != source_sha256
            || imported.source_bytes != source_bytes
            || imported.segments.is_empty()
        {
            return Err(AccountHostValidationError::LegacyPredecessor);
        }
        let paths = journal_segment_paths(destination_root)?;
        if paths.len() < imported.segments.len()
            || imported
                .segments
                .iter()
                .enumerate()
                .any(|(offset, expected)| {
                    let index = offset.saturating_add(1);
                    let Ok(path) = legacy_import_segment_path(destination_root, index) else {
                        return true;
                    };
                    paths.get(offset) != Some(&path)
                        || path.file_name().and_then(|value| value.to_str())
                            != Some(expected.path.as_str())
                        || !fs::metadata(&path)
                            .map(|metadata| metadata.is_file() && metadata.len() == expected.bytes)
                            .unwrap_or(false)
                        || file_sha256(&path).ok().as_deref() != Some(expected.sha256.as_str())
                })
        {
            return Err(AccountHostValidationError::LegacyPredecessor);
        }
        return Ok(());
    }
    validate_legacy_import_destination(destination_root)?;
    // This verifies every record's sequence, command hash, state transition, and persisted
    // command shape before any successor file is created.  An unresolved old command must be
    // reconciled by the old writer first; it can never be relabelled as a new Host receipt.
    let legacy_journal =
        CommandJournal::open(&source).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    if legacy_journal.has_unresolved() {
        return Err(AccountHostValidationError::LegacyPredecessor);
    }
    fs::create_dir_all(destination_root)
        .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    let source_file =
        File::open(&source).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    let mut reader = BufReader::new(source_file);
    let mut line = Vec::new();
    let mut segment_count = 0_usize;
    let mut segment: Option<File> = None;
    let mut segment_bytes = 0_u64;
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        if read == 0 {
            break;
        }
        let line_bytes =
            u64::try_from(line.len()).map_err(|_| AccountHostValidationError::JournalBudget)?;
        if line_bytes == 0 || line_bytes > COMMAND_JOURNAL_ROTATE_BYTES {
            return Err(AccountHostValidationError::JournalBudget);
        }
        if segment.is_none()
            || segment_bytes
                .checked_add(line_bytes)
                .is_none_or(|next| next > COMMAND_JOURNAL_ROTATE_BYTES)
        {
            if let Some(file) = segment.take() {
                file.sync_all()
                    .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
            }
            segment_count = segment_count
                .checked_add(1)
                .ok_or(AccountHostValidationError::JournalBudget)?;
            let path = legacy_import_stage_path(destination_root, segment_count)?;
            segment = Some(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map_err(|_| AccountHostValidationError::LegacyPredecessor)?,
            );
            segment_bytes = 0;
        }
        segment
            .as_mut()
            .ok_or(AccountHostValidationError::LegacyPredecessor)?
            .write_all(&line)
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        segment_bytes = segment_bytes
            .checked_add(line_bytes)
            .ok_or(AccountHostValidationError::JournalBudget)?;
    }
    if let Some(file) = segment {
        file.sync_all()
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    }
    if segment_count == 0 {
        return Err(AccountHostValidationError::LegacyPredecessor);
    }
    let mut segments = Vec::with_capacity(segment_count);
    for index in 1..=segment_count {
        let source = legacy_import_stage_path(destination_root, index)?;
        let destination = legacy_import_segment_path(destination_root, index)?;
        fs::rename(source, &destination)
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        let bytes = fs::metadata(&destination)
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?
            .len();
        segments.push(LegacyV1JournalImportSegment {
            path: destination
                .file_name()
                .and_then(|value| value.to_str())
                .ok_or(AccountHostValidationError::LegacyPredecessor)?
                .to_owned(),
            bytes,
            sha256: file_sha256(&destination)?,
        });
    }
    let marker_body = serde_json::to_vec(&LegacyV1JournalImport {
        schema_version: LEGACY_V1_IMPORT_SCHEMA_VERSION,
        source_path: source,
        source_sha256,
        source_bytes,
        segments,
    })
    .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    write_new_synced(&marker, &marker_body)?;
    Ok(())
}

fn validate_legacy_import_destination(
    destination_root: &Path,
) -> Result<(), AccountHostValidationError> {
    if !destination_root.exists() {
        return Ok(());
    }
    for entry in
        fs::read_dir(destination_root).map_err(|_| AccountHostValidationError::LegacyPredecessor)?
    {
        let entry = entry.map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        let file_type = entry
            .file_type()
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        let metadata = entry
            .metadata()
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        if entry.file_name() != ACCOUNT_WRITER_LOCK_FILE
            || !file_type.is_file()
            || metadata.len() != 0
        {
            return Err(AccountHostValidationError::LegacyPredecessor);
        }
    }
    Ok(())
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct LegacyV1JournalImport {
    schema_version: u16,
    source_path: PathBuf,
    source_sha256: String,
    source_bytes: u64,
    segments: Vec<LegacyV1JournalImportSegment>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct LegacyV1JournalImportSegment {
    path: String,
    bytes: u64,
    sha256: String,
}

fn legacy_import_stage_path(
    root: &Path,
    index: usize,
) -> Result<PathBuf, AccountHostValidationError> {
    if index == 0 || index > 999_999 {
        return Err(AccountHostValidationError::JournalBudget);
    }
    Ok(root.join(format!(".legacy-v1-import-{index:06}.stage")))
}

fn legacy_import_segment_path(
    root: &Path,
    index: usize,
) -> Result<PathBuf, AccountHostValidationError> {
    if index == 0 || index > 999_999 {
        return Err(AccountHostValidationError::JournalBudget);
    }
    Ok(root.join(format!("commands-{index:06}.jsonl")))
}

fn acquire_account_writer_lock<E: std::error::Error + 'static>(
    lock_path: &Path,
) -> Result<File, AccountHostError<E>> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|source| AccountHostError::Io {
            path: lock_path.to_path_buf(),
            source,
        })?;
    lock.try_lock_exclusive()
        .map_err(|source| AccountHostError::Io {
            path: lock_path.to_path_buf(),
            source,
        })?;
    Ok(lock)
}

fn file_sha256(path: &Path) -> Result<String, AccountHostValidationError> {
    let mut file = File::open(path).map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)
            .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), AccountHostValidationError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| AccountHostValidationError::LegacyPredecessor)
}

fn command_matches_signed_order(command: &ExecutionCommand, fact: &SignedAccountOrderFact) -> bool {
    if command.native_order_family() != Some(fact.family) {
        return false;
    }
    match command {
        ExecutionCommand::PlaceLimit(order) => {
            order.owner.symbol == fact.symbol
                && order.side == fact.side
                && order.position_side == fact.position_side
                && order.quantity == fact.quantity
                && Some(order.limit_price.value()) == fact.limit_price
                && Some(order.time_in_force) == fact.time_in_force
                && order.reduce_only == fact.reduce_only
        }
        ExecutionCommand::PlaceMarket(order) => {
            order.owner.symbol == fact.symbol
                && order.side == fact.side
                && order.position_side == fact.position_side
                && order.quantity == fact.quantity
                && fact.limit_price.is_none()
                && !fact.reduce_only
        }
        ExecutionCommand::MarketReduce(order) => {
            order.owner.symbol == fact.symbol
                && order.side == fact.side
                && order.position_side == fact.position_side
                && order.quantity == fact.quantity
                && fact.limit_price.is_none()
                && fact.reduce_only
        }
        ExecutionCommand::StopMarketFullPosition(order) => {
            order.owner.symbol == fact.symbol
                && order.side == fact.side
                && order.position_side == fact.position_side
                && order.quantity == fact.quantity
                && Some(order.trigger_price.value()) == fact.limit_price
                && fact.reduce_only
        }
        ExecutionCommand::StopMarketCloseAll(_) | ExecutionCommand::Cancel(_) => false,
    }
}

fn quote_notional(
    command: &ExecutionCommand,
) -> Result<AccountRiskAmount, AccountHostValidationError> {
    let ExecutionCommand::PlaceLimit(place) = command else {
        return Err(AccountHostValidationError::Command);
    };
    let value = place
        .quantity
        .checked_mul(place.limit_price.value())
        .filter(|notional| *notional > Decimal::ZERO)
        .ok_or(AccountHostValidationError::Notional)?;
    let asset = Asset::new(place.owner.symbol.quote())
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    Ok(AccountRiskAmount { asset, value })
}

fn sum_notional(values: &[Decimal]) -> Result<Decimal, AccountHostValidationError> {
    values.iter().try_fold(Decimal::ZERO, |total, notional| {
        total
            .checked_add(notional.abs())
            .ok_or(AccountHostValidationError::Notional)
    })
}

fn now_ms() -> Result<u64, AccountHostValidationError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| AccountHostValidationError::RiskEvidence)
}

fn require_journal_budget(path: &Path) -> Result<(), AccountHostValidationError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.len() <= COMMAND_JOURNAL_HARD_LIMIT_BYTES => Ok(()),
        Ok(_) => Err(AccountHostValidationError::JournalHardLimit),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(AccountHostValidationError::JournalBudget),
    }
}

fn require_append_budget(
    path: &Path,
    command: &ExecutionCommand,
) -> Result<(), AccountHostValidationError> {
    require_journal_budget(path)?;
    let current = fs::metadata(path).map_or(0, |metadata| metadata.len());
    let encoded = serde_json::to_vec(command).map_err(|_| AccountHostValidationError::Command)?;
    let reserve = u64::try_from(encoded.len())
        .ok()
        .and_then(|size| size.checked_mul(4))
        .and_then(|size| size.checked_add(4096))
        .ok_or(AccountHostValidationError::JournalBudget)?;
    if (current >= COMMAND_JOURNAL_ROTATE_BYTES && is_risk_increasing(command))
        || current
            .checked_add(reserve)
            .is_none_or(|next| next > COMMAND_JOURNAL_HARD_LIMIT_BYTES)
    {
        return Err(AccountHostValidationError::RotationRequired);
    }
    Ok(())
}

fn rotate_if_clean_and_due(
    journal: &mut CommandJournal,
    active_path: &Path,
) -> Result<(), CommandJournalError> {
    let due = fs::metadata(active_path)
        .map(|metadata| metadata.len() >= COMMAND_JOURNAL_ROTATE_BYTES)
        .unwrap_or(false);
    if !due || journal.has_unresolved() {
        return Ok(());
    }
    let root = active_path.parent().ok_or(CommandJournalError::Sequence)?;
    let next = journal_segment_paths(root)
        .map_err(|_| CommandJournalError::Sequence)?
        .len()
        .checked_add(1)
        .ok_or(CommandJournalError::Sequence)?;
    let archive_path = root.join(format!("commands-{next:06}.jsonl"));
    journal.rotate_active(&archive_path)
}

fn journal_segment_paths(root: &Path) -> Result<Vec<PathBuf>, AccountHostValidationError> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(AccountHostValidationError::JournalBudget),
    };
    let mut indexed = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| AccountHostValidationError::JournalBudget)?;
        let file_type = entry
            .file_type()
            .map_err(|_| AccountHostValidationError::JournalBudget)?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(index) = name
            .strip_prefix("commands-")
            .and_then(|value| value.strip_suffix(".jsonl"))
            .filter(|value| value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<usize>().ok())
        else {
            continue;
        };
        indexed.push((index, entry.path()));
    }
    indexed.sort_by_key(|(index, _)| *index);
    if indexed
        .iter()
        .enumerate()
        .any(|(offset, (index, _))| *index != offset.saturating_add(1))
    {
        return Err(AccountHostValidationError::JournalBudget);
    }
    Ok(indexed.into_iter().map(|(_, path)| path).collect())
}

fn valid_text(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn hex_sha256(value: [u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn legacy_v1_custody_cancel_command_id(
    route: &LegacyV1CustodyRoute,
) -> Result<CommandId, AccountHostValidationError> {
    let encoded = serde_json::to_vec(&(
        &route.command_id,
        &route.owner,
        route.family,
        &route.client_order_id,
        &route.venue_order_id,
    ))
    .map_err(|_| AccountHostValidationError::LegacyPredecessor)?;
    let digest = hex_sha256(Sha256::digest(encoded).into());
    // `CommandId` is capped at 36 bytes; keep the migration prefix short while retaining 128
    // bits of deterministic route identity for idempotent recovery.
    CommandId::new(format!("lc-{}", &digest[..32]))
        .map_err(|_| AccountHostValidationError::LegacyPredecessor)
}

/// `CommandReceipt` is a struct-only canonical serde shape. This digest binds the dispatch
/// capability to the exact fsynced Prepared record, rather than merely to a semantic command.
fn receipt_sha256(receipt: &crate::CommandReceipt) -> Result<[u8; 32], AccountHostValidationError> {
    serde_json::to_vec(receipt)
        .map(|encoded| Sha256::digest(encoded).into())
        .map_err(|_| AccountHostValidationError::PreparedCommand)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AccountHostValidationError {
    #[error("account host binding or gateway scope does not match")]
    Scope,
    #[error("account artifacts root is not the derived venue/LIVE/account path")]
    ArtifactsRoot,
    #[error("account recovery did not cover every unresolved command exactly once")]
    Recovery,
    #[error("execution command is invalid")]
    Command,
    #[error("risk-increasing command is fenced by an unresolved mutation")]
    UnknownFence,
    #[error("risk-increasing command is fenced by an existing entry reservation")]
    OpenEntryFence,
    #[error("complete fresh signed account-risk evidence is required before increasing risk")]
    RiskEvidence,
    #[error(
        "account-wide signed exposure, open entries, WAL reservations, and the candidate exceed 10U"
    )]
    AccountRiskLimit,
    #[error("risk-increasing market entry is disabled for the initial LIVE profile")]
    MarketEntryDisabled,
    #[error("entry notional is invalid or exceeds the fixed 10U ceiling")]
    Notional,
    #[error("command identity already exists in the account WAL")]
    Duplicate,
    #[error("prepared command proof does not match this account's current WAL receipt")]
    PreparedCommand,
    #[error("command journal requires a clean, reconciled rotation before another mutation")]
    RotationRequired,
    #[error("command journal exceeds the 10 MiB hard limit")]
    JournalHardLimit,
    #[error("command journal size cannot be verified safely")]
    JournalBudget,
    #[error("complete signed account snapshot is unavailable, stale, or incomplete")]
    SignedSnapshot,
    #[error("current adapter-validated instrument is unavailable or inconsistent")]
    Instrument,
    #[error("legacy Stage-7 predecessor handoff is missing, inconsistent, or unavailable")]
    LegacyPredecessor,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountHostError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Binding(venue_gateway_api::GatewayApiError),
    #[error(transparent)]
    Validation(AccountHostValidationError),
    #[error(transparent)]
    Journal(CommandJournalError),
    #[error(transparent)]
    CanonicalRoot(crate::AccountCanonicalRootError),
    #[error("account artifact I/O failed for {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("account gateway readback failed")]
    Gateway(#[source] E),
}

#[cfg(test)]
#[path = "account_cursor_tests.rs"]
mod account_cursor_tests;

#[cfg(test)]
#[path = "account_host_tests.rs"]
mod account_host_tests;
