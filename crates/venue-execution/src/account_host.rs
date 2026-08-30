use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

use fs2::FileExt;
use rust_decimal::Decimal;
use venue_domain::domain::{CommandId, ExecutionCommand};
use venue_gateway_api::GatewayBinding;

use crate::{CommandJournal, CommandJournalError, CommandState};

pub const COMMAND_JOURNAL_ROTATE_BYTES: u64 = 5 * 1024 * 1024;
pub const COMMAND_JOURNAL_HARD_LIMIT_BYTES: u64 = 10 * 1024 * 1024;

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

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRecoveryRequest {
    binding: GatewayBinding,
    unresolved: Vec<ExecutionCommand>,
}

impl AccountRecoveryRequest {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub fn unresolved(&self) -> &[ExecutionCommand] {
        &self.unresolved
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
    pub const fn command_id(&self) -> &CommandId {
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

#[derive(Debug)]
pub struct AccountMutationHost<G> {
    binding: GatewayBinding,
    max_entry_notional: Decimal,
    journal_path: PathBuf,
    journal: CommandJournal,
    gateway: G,
    _account_lock: File,
}

impl<G: AccountPhysicalGateway> AccountMutationHost<G> {
    pub fn open(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        max_entry_notional: Decimal,
        mut gateway: G,
    ) -> Result<Self, AccountHostError<G::Error>> {
        binding.validate().map_err(AccountHostError::Binding)?;
        if gateway.binding() != &binding || max_entry_notional != Decimal::TEN {
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
        let lock_path = artifacts_root.join("writer.lock");
        let account_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| AccountHostError::Io {
                path: lock_path.clone(),
                source,
            })?;
        account_lock
            .try_lock_exclusive()
            .map_err(|source| AccountHostError::Io {
                path: lock_path,
                source,
            })?;
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
        let request = AccountRecoveryRequest {
            binding: binding.clone(),
            unresolved,
        };
        let report = gateway
            .reconcile(&request)
            .map_err(AccountHostError::Gateway)?;
        apply_recovery(&mut journal, &request, report).map_err(AccountHostError::Validation)?;
        rotate_if_clean_and_due(&mut journal, &journal_path).map_err(AccountHostError::Journal)?;
        require_journal_budget(&journal_path).map_err(AccountHostError::Validation)?;
        Ok(Self {
            binding,
            max_entry_notional,
            journal_path,
            journal,
            gateway,
            _account_lock: account_lock,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub fn has_unresolved(&self) -> bool {
        self.journal.has_unresolved()
    }

    pub fn dispatch(
        &mut self,
        command: ExecutionCommand,
    ) -> Result<AccountDispatchOutcome, AccountHostError<G::Error>> {
        validate_command_scope(&command, &self.binding, self.max_entry_notional)
            .map_err(AccountHostError::Validation)?;
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
        rotate_if_clean_and_due(&mut self.journal, &self.journal_path)
            .map_err(AccountHostError::Journal)?;
        require_append_budget(&self.journal_path, &command)
            .map_err(AccountHostError::Validation)?;
        let command_id = command.command_id().clone();
        if self.journal.receipt(&command_id).is_some() {
            return Err(AccountHostError::Validation(
                AccountHostValidationError::Duplicate,
            ));
        }
        self.journal
            .prepare(command.clone())
            .map_err(AccountHostError::Journal)?;
        self.journal
            .transition(&command_id, CommandState::Submitted)
            .map_err(AccountHostError::Journal)?;
        let result = self.gateway.dispatch(AccountDispatchPermit {
            binding: self.binding.clone(),
            command,
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

fn validate_command_scope(
    command: &ExecutionCommand,
    binding: &GatewayBinding,
    max_entry_notional: Decimal,
) -> Result<(), AccountHostValidationError> {
    command
        .validate()
        .map_err(|_| AccountHostValidationError::Command)?;
    let owner = command.mutation_owner();
    if owner.exchange != binding.venue.as_str()
        || owner.account != binding.trading_account_id
        || owner.symbol != binding.symbol
    {
        return Err(AccountHostValidationError::Scope);
    }
    match command {
        ExecutionCommand::PlaceLimit(limit) if is_risk_increasing(command) => {
            let notional = limit
                .quantity
                .checked_mul(limit.limit_price.value())
                .ok_or(AccountHostValidationError::Notional)?;
            if notional <= Decimal::ZERO || notional > max_entry_notional {
                return Err(AccountHostValidationError::Notional);
            }
        }
        ExecutionCommand::PlaceMarket(_) => {
            return Err(AccountHostValidationError::MarketEntryDisabled);
        }
        ExecutionCommand::PlaceLimit(_)
        | ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_)
        | ExecutionCommand::Cancel(_) => {}
    }
    Ok(())
}

fn is_risk_increasing(command: &ExecutionCommand) -> bool {
    match command {
        ExecutionCommand::PlaceLimit(command) => !command.reduce_only,
        ExecutionCommand::PlaceMarket(_) => true,
        ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_)
        | ExecutionCommand::Cancel(_) => false,
    }
}

fn has_open_entry_reservation(journal: &CommandJournal) -> bool {
    journal.commands().any(|command| {
        let ExecutionCommand::PlaceLimit(place) = command else {
            return false;
        };
        if place.reduce_only || journal.has_accepted_cancel_for(&place.client_order_id) {
            return false;
        }
        journal.receipt(&place.command_id).is_some_and(|receipt| {
            matches!(
                receipt.state,
                CommandState::Accepted { .. } | CommandState::Unknown { .. }
            )
        })
    })
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
    #[error("risk-increasing market entry is disabled for the initial LIVE profile")]
    MarketEntryDisabled,
    #[error("entry notional is invalid or exceeds the fixed 10U ceiling")]
    Notional,
    #[error("command identity already exists in the account WAL")]
    Duplicate,
    #[error("command journal requires a clean, reconciled rotation before another mutation")]
    RotationRequired,
    #[error("command journal exceeds the 10 MiB hard limit")]
    JournalHardLimit,
    #[error("command journal size cannot be verified safely")]
    JournalBudget,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountHostError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Binding(venue_gateway_api::GatewayApiError),
    #[error(transparent)]
    Validation(AccountHostValidationError),
    #[error(transparent)]
    Journal(CommandJournalError),
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
mod tests {
    use std::{io, io::Write};

    use rust_decimal::Decimal;
    use tempfile::TempDir;
    use venue_domain::domain::{
        CancelCommand, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    #[derive(Debug)]
    struct Gateway {
        binding: GatewayBinding,
        result: AccountGatewayResult,
        dispatches: usize,
    }

    impl AccountPhysicalGateway for Gateway {
        type Error = io::Error;

        fn binding(&self) -> &GatewayBinding {
            &self.binding
        }

        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            let outcomes = request
                .unresolved()
                .iter()
                .map(|command| AccountRecoveryOutcome::still_unknown(command.command_id().clone()))
                .collect();
            AccountRecoveryReport::new(self.binding.clone(), 1, outcomes).map_err(io::Error::other)
        }

        fn dispatch(&mut self, _permit: AccountDispatchPermit) -> AccountGatewayResult {
            self.dispatches += 1;
            self.result.clone()
        }
    }

    #[derive(Debug)]
    struct RecoveringGateway {
        binding: GatewayBinding,
        dispatches: usize,
    }

    impl AccountPhysicalGateway for RecoveringGateway {
        type Error = io::Error;

        fn binding(&self) -> &GatewayBinding {
            &self.binding
        }

        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            let outcomes = request
                .unresolved()
                .iter()
                .map(|command| {
                    AccountRecoveryOutcome::accepted(
                        command.command_id().clone(),
                        "recovered-order".to_owned(),
                    )
                })
                .collect();
            AccountRecoveryReport::new(self.binding.clone(), 2, outcomes).map_err(io::Error::other)
        }

        fn dispatch(&mut self, _permit: AccountDispatchPermit) -> AccountGatewayResult {
            self.dispatches += 1;
            AccountGatewayResult::Unknown
        }
    }

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?)
    }

    fn root(temp: &TempDir) -> PathBuf {
        temp.path().join("okx").join("LIVE").join(ACCOUNT)
    }

    fn command(notional: Decimal) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        let identity = notional.normalize().to_string().replace('.', "-");
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new(format!("cmd-{identity}"))?,
            client_order_id: CommandId::new(format!("client-{identity}"))?,
            owner: owner()?,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(notional)?,
            reduce_only: false,
        }))
    }

    fn indexed_command(index: usize) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new(format!("cmd-segment-{index}"))?,
            client_order_id: CommandId::new(format!("client-segment-{index}"))?,
            owner: owner()?,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        }))
    }

    fn owner() -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "canary".to_owned(),
            run_id: "run-1".to_owned(),
            exchange: "okx".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        })
    }

    #[test]
    fn host_persists_submitted_before_one_dispatch_and_records_unknown()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let gateway = Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Unknown,
            dispatches: 0,
        };
        let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
        assert_eq!(
            host.dispatch(command(Decimal::TEN)?)?,
            AccountDispatchOutcome::Unknown
        );
        assert!(host.has_unresolved());
        assert_eq!(host.gateway.dispatches, 1);
        Ok(())
    }

    #[test]
    fn restart_resolves_unknown_by_readback_without_redispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        {
            let gateway = Gateway {
                binding: binding.clone(),
                result: AccountGatewayResult::Unknown,
                dispatches: 0,
            };
            let mut host =
                AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, gateway)?;
            assert_eq!(
                host.dispatch(command(Decimal::TEN)?)?,
                AccountDispatchOutcome::Unknown
            );
            assert_eq!(host.gateway.dispatches, 1);
        }

        let gateway = RecoveringGateway {
            binding: binding.clone(),
            dispatches: 0,
        };
        let mut reopened = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
        assert!(!reopened.has_unresolved());
        assert_eq!(reopened.gateway.dispatches, 0);
        assert!(matches!(
            reopened.dispatch(command(Decimal::new(9, 0))?),
            Err(AccountHostError::Validation(
                AccountHostValidationError::OpenEntryFence
            ))
        ));
        assert_eq!(reopened.gateway.dispatches, 0);
        Ok(())
    }

    #[test]
    fn host_rejects_more_than_ten_usdt_before_gateway_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let gateway = Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Accepted {
                venue_order_id: "1".to_owned(),
            },
            dispatches: 0,
        };
        let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
        assert!(matches!(
            host.dispatch(command(Decimal::new(1001, 2))?),
            Err(AccountHostError::Validation(
                AccountHostValidationError::Notional
            ))
        ));
        assert_eq!(host.gateway.dispatches, 0);
        Ok(())
    }

    #[test]
    fn host_requires_an_accepted_cancel_before_another_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let gateway = Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Accepted {
                venue_order_id: "1".to_owned(),
            },
            dispatches: 0,
        };
        let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
        assert!(matches!(
            host.dispatch(command(Decimal::TEN)?)?,
            AccountDispatchOutcome::Accepted { .. }
        ));
        assert!(matches!(
            host.dispatch(command(Decimal::new(9, 0))?),
            Err(AccountHostError::Validation(
                AccountHostValidationError::OpenEntryFence
            ))
        ));
        assert_eq!(host.gateway.dispatches, 1);
        Ok(())
    }

    #[test]
    fn second_host_cannot_acquire_the_same_account_lock() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let make_gateway = || Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Unknown,
            dispatches: 0,
        };
        let _first =
            AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, make_gateway())?;
        assert!(
            AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, make_gateway())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn five_mib_journal_stops_before_any_new_physical_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let account_root = root(&temp);
        fs::create_dir_all(&account_root)?;
        let journal_path = account_root.join("commands.jsonl");
        let mut journal = File::create(&journal_path)?;
        journal.write_all(&vec![b' '; COMMAND_JOURNAL_ROTATE_BYTES as usize])?;
        journal.sync_all()?;

        assert!(matches!(
            require_append_budget(&journal_path, &command(Decimal::TEN)?),
            Err(AccountHostValidationError::RotationRequired)
        ));
        assert!(fs::metadata(journal_path)?.len() <= COMMAND_JOURNAL_HARD_LIMIT_BYTES);
        Ok(())
    }

    #[test]
    fn clean_five_mib_segment_rotates_and_retains_cancel_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let account_root = root(&temp);
        fs::create_dir_all(&account_root)?;
        let journal_path = account_root.join("commands.jsonl");
        let mut bytes = Vec::new();
        let mut sequence = 1_u64;
        let mut index = 0_usize;
        while bytes.len() < COMMAND_JOURNAL_ROTATE_BYTES as usize {
            let command = indexed_command(index)?;
            let hash = crate::execution_command_sha256(&command)?
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            for state in [
                CommandState::Prepared,
                CommandState::Submitted,
                CommandState::Accepted {
                    venue_order_id: format!("venue-{index}"),
                },
            ] {
                serde_json::to_writer(
                    &mut bytes,
                    &crate::CommandReceipt {
                        sequence,
                        command: command.clone(),
                        command_sha256: hash.clone(),
                        state,
                    },
                )?;
                bytes.push(b'\n');
                sequence = sequence.checked_add(1).ok_or("sequence overflow")?;
            }
            index = index.checked_add(1).ok_or("index overflow")?;
        }
        let mut file = File::create(&journal_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;

        let binding = binding()?;
        let gateway = Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Accepted {
                venue_order_id: "cancel-1".to_owned(),
            },
            dispatches: 0,
        };
        let mut host =
            AccountMutationHost::open(account_root.clone(), binding, Decimal::TEN, gateway)?;
        assert!(account_root.join("commands-000001.jsonl").is_file());
        assert_eq!(fs::metadata(&journal_path)?.len(), 0);

        let cancel = ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("cancel-segment-0")?,
            owner: owner()?,
            target_client_order_id: CommandId::new("client-segment-0")?,
        });
        assert!(matches!(
            host.dispatch(cancel)?,
            AccountDispatchOutcome::Accepted { .. }
        ));
        assert_eq!(host.gateway.dispatches, 1);
        Ok(())
    }
}
