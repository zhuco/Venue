use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::domain::{
    CancelCommand, CommandId, ExecutionCommand, MarketOrderCommand, MarketReduceCommand,
    NativeOrderFamily, OrderCommand, OrderOwner, StopMarketCloseAllCommand,
    StopMarketFullPositionCommand,
};
use crate::execution_command_sha256;
use crate::{NativeOrderRoute, NativeOrderRouteKey};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_storage::{DurableWalHead, DurableWalHeadFormat};

/// The WAL keeps the native family beside its client identity so UNKNOWN resolution cannot query
/// an ordinary UM order endpoint for a PAPI conditional strategy (or the reverse).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderReadbackIdentity<'a> {
    pub owner: &'a OrderOwner,
    pub family: NativeOrderFamily,
    pub client_id: &'a CommandId,
}

/// A durable receipt is written before an adapter can issue a mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub sequence: u64,
    pub command: ExecutionCommand,
    pub command_sha256: String,
    pub state: CommandState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "detail")]
pub enum CommandState {
    Prepared,
    Submitted,
    Accepted { venue_order_id: String },
    Rejected { reason: String },
    Unknown { reason: String },
}

impl CommandState {
    pub const fn terminal(&self) -> bool {
        matches!(self, Self::Accepted { .. } | Self::Rejected { .. })
    }
}

/// Single-writer command WAL. A malformed or partial tail fails closed because it may describe
/// a mutation whose exchange result cannot be inferred locally.
#[derive(Debug)]
pub struct CommandJournal {
    path: PathBuf,
    receipts: BTreeMap<CommandId, CommandReceipt>,
    prepared_commands: BTreeMap<CommandId, ExecutionCommand>,
    first_command_sequences: BTreeMap<CommandId, u64>,
    current_wal_root: [u8; 32],
    current_record_count: u64,
    client_ids: BTreeMap<CommandId, CommandId>,
    unresolved_ids: BTreeSet<CommandId>,
    unresolved_entry_or_reduce: usize,
    unresolved_cancel_targets: BTreeMap<CommandId, usize>,
    accepted_cancel_targets: BTreeMap<CommandId, usize>,
    venue_order_client_ids: BTreeMap<String, Option<CommandId>>,
    next_sequence: u64,
    durable_len: u64,
    needs_parent_sync: bool,
}

impl CommandJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, CommandJournalError> {
        Self::open_segmented(path, &[])
    }

    /// A read-only commitment to the sole command WAL.  Actor-applied checkpoints bind to this
    /// exact head so a restarted resident cannot reuse a turn against an older command history.
    pub fn durable_wal_head(&self) -> Result<DurableWalHead, CommandJournalError> {
        // A command's later state transition does not allocate a new command identity.  The
        // actor head therefore commits the ordered prepared-command set; Unknown/Accepted state
        // remains in this same WAL and is separately reconciled by the Host.
        let tail_sequence = self
            .next_sequence
            .checked_sub(1)
            .ok_or(CommandJournalError::Sequence)?;
        DurableWalHead::new_v2(
            self.current_wal_root,
            tail_sequence,
            self.current_record_count,
        )
        .map_err(|_| CommandJournalError::Sequence)
    }

    /// Checks a previously committed Actor head against this WAL's replayed record prefix.
    /// This is startup-only: it rebuilds the requested prefix from first-command sequence facts,
    /// so dispatch never scans a historical WAL or precomputes every transition head.
    #[must_use]
    pub fn validates_historical_wal_head(&self, head: DurableWalHead) -> bool {
        let tail_sequence = match self.next_sequence.checked_sub(1) {
            Some(sequence) => sequence,
            None => return false,
        };
        let valid_shape = match head.format_version() {
            DurableWalHeadFormat::V1 => DurableWalHead::new(
                head.root_sha256(),
                head.tail_sequence(),
                head.record_count(),
            ),
            DurableWalHeadFormat::V2 => DurableWalHead::new_v2(
                head.root_sha256(),
                head.tail_sequence(),
                head.record_count(),
            ),
        };
        if valid_shape.is_err() || head.tail_sequence() > tail_sequence {
            return false;
        }
        match head.format_version() {
            DurableWalHeadFormat::V1 => {
                let commands = self
                    .prepared_commands
                    .iter()
                    .filter(|(command_id, _)| {
                        self.first_command_sequences
                            .get(*command_id)
                            .is_some_and(|sequence| *sequence <= head.tail_sequence())
                    })
                    .map(|(_, command)| command);
                durable_head_v1_from_ordered(commands, head.tail_sequence())
                    .is_ok_and(|expected| expected == head)
            }
            DurableWalHeadFormat::V2 => self
                .v2_head_for_prefix(head.tail_sequence())
                .is_ok_and(|expected| expected == head),
        }
    }

    pub(crate) fn open_segmented(
        path: impl Into<PathBuf>,
        historical_paths: &[PathBuf],
    ) -> Result<Self, CommandJournalError> {
        let path = path.into();
        let mut receipts: BTreeMap<CommandId, CommandReceipt> = BTreeMap::new();
        let mut prepared_commands: BTreeMap<CommandId, ExecutionCommand> = BTreeMap::new();
        let mut first_command_sequences: BTreeMap<CommandId, u64> = BTreeMap::new();
        let mut client_ids: BTreeMap<CommandId, CommandId> = BTreeMap::new();
        let mut next_sequence = 1;
        let mut active_replay = None;
        for replay_path in historical_paths
            .iter()
            .map(PathBuf::as_path)
            .chain(std::iter::once(path.as_path()))
        {
            let replay = read_all(replay_path)?;
            active_replay = Some((replay.durable_len, replay.existed));
            for receipt in replay.receipts {
                if receipt.sequence != next_sequence {
                    return Err(CommandJournalError::Sequence);
                }
                receipt
                    .command
                    .validate_persisted_shape()
                    .map_err(CommandJournalError::Command)?;
                if receipt.command_sha256 != command_hash(&receipt.command)? {
                    return Err(CommandJournalError::Hash);
                }
                let command_id = receipt.command.command_id().clone();
                let client_id = receipt.command.native_client_id().cloned();
                if let Some(previous) = receipts.get(&command_id) {
                    if previous.command_sha256 != receipt.command_sha256
                        || !allowed_transition(&previous.state, &receipt.state)
                    {
                        return Err(CommandJournalError::Transition);
                    }
                } else {
                    if let ExecutionCommand::Cancel(command) = &receipt.command {
                        validate_cancel_target(&receipts, &client_ids, command)?;
                    }
                    if let Some(client_id) = client_id
                        && client_ids.insert(client_id, command_id.clone()).is_some()
                    {
                        return Err(CommandJournalError::Duplicate);
                    }
                }
                prepared_commands
                    .entry(command_id.clone())
                    .or_insert_with(|| receipt.command.clone());
                first_command_sequences
                    .entry(command_id.clone())
                    .or_insert(receipt.sequence);
                receipts.insert(command_id, receipt);
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(CommandJournalError::Sequence)?;
            }
        }
        let (active_durable_len, active_existed) =
            active_replay.ok_or(CommandJournalError::Sequence)?;
        let current_wal_head = durable_head_v2_from_prepared(
            &prepared_commands,
            &first_command_sequences,
            next_sequence
                .checked_sub(1)
                .ok_or(CommandJournalError::Sequence)?,
        )?;
        let mut journal = Self {
            path,
            receipts,
            prepared_commands,
            first_command_sequences,
            current_wal_root: current_wal_head.root_sha256(),
            current_record_count: current_wal_head.record_count(),
            client_ids,
            unresolved_ids: BTreeSet::new(),
            unresolved_entry_or_reduce: 0,
            unresolved_cancel_targets: BTreeMap::new(),
            accepted_cancel_targets: BTreeMap::new(),
            venue_order_client_ids: BTreeMap::new(),
            next_sequence,
            durable_len: active_durable_len,
            needs_parent_sync: !active_existed,
        };
        journal.rebuild_query_indexes();
        Ok(journal)
    }

    pub(crate) fn rotate_active(&mut self, archive_path: &Path) -> Result<(), CommandJournalError> {
        if self.has_unresolved() || archive_path.exists() {
            return Err(CommandJournalError::Sequence);
        }
        fs::rename(&self.path, archive_path).map_err(|source| CommandJournalError::Io {
            path: self.path.clone(),
            source,
        })?;
        sync_parent(archive_path)?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.sync_data().map_err(|source| CommandJournalError::Io {
            path: self.path.clone(),
            source,
        })?;
        sync_parent(&self.path)?;
        self.durable_len = 0;
        self.needs_parent_sync = false;
        Ok(())
    }

    pub fn prepare(
        &mut self,
        command: ExecutionCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        command
            .validate_persisted_shape()
            .map_err(CommandJournalError::Command)?;
        let command_hash = command_hash(&command)?;
        let command_id = command.command_id().clone();
        if self.receipts.contains_key(&command_id) {
            let same_command = self
                .receipts
                .get(&command_id)
                .is_some_and(|existing| existing.command_sha256 == command_hash);
            if !same_command {
                return Err(CommandJournalError::Conflict);
            }
            return self
                .receipts
                .get(&command_id)
                .ok_or(CommandJournalError::Missing);
        }
        if command
            .native_client_id()
            .is_some_and(|client_id| self.client_ids.contains_key(client_id))
        {
            return Err(CommandJournalError::ClientId);
        }
        if let ExecutionCommand::Cancel(cancel) = &command {
            validate_cancel_target(&self.receipts, &self.client_ids, cancel)?;
        }
        let receipt = CommandReceipt {
            sequence: self.next_sequence,
            command: command.clone(),
            command_sha256: command_hash,
            state: CommandState::Prepared,
        };
        self.append(&receipt)?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(CommandJournalError::Sequence)?;
        if let Some(client_order_id) = command.native_client_id() {
            self.client_ids
                .insert(client_order_id.clone(), command_id.clone());
        }
        self.prepared_commands.insert(command_id.clone(), command);
        self.first_command_sequences
            .insert(command_id.clone(), receipt.sequence);
        self.append_current_v2_commitment(&receipt.command, receipt.sequence)?;
        self.add_query_indexes(&command_id, &receipt);
        self.receipts.insert(command_id.clone(), receipt);
        self.receipts
            .get(&command_id)
            .ok_or(CommandJournalError::Missing)
    }

    pub fn prepare_place(
        &mut self,
        command: OrderCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        self.prepare(ExecutionCommand::PlaceLimit(command))
    }

    pub fn prepare_market(
        &mut self,
        command: MarketOrderCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        self.prepare(ExecutionCommand::PlaceMarket(command))
    }

    pub fn prepare_market_reduce(
        &mut self,
        command: MarketReduceCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        self.prepare(ExecutionCommand::MarketReduce(command))
    }

    pub fn prepare_stop_market_close_all(
        &mut self,
        command: StopMarketCloseAllCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        self.prepare(ExecutionCommand::StopMarketCloseAll(command))
    }

    pub fn prepare_stop_market_full_position(
        &mut self,
        command: StopMarketFullPositionCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        self.prepare(ExecutionCommand::StopMarketFullPosition(command))
    }

    pub fn prepare_cancel(
        &mut self,
        command: CancelCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        self.prepare(ExecutionCommand::Cancel(command))
    }

    /// Persists one already-admitted batch as `Prepared` with a single durability barrier.
    /// No child is `Submitted` here: the account lane still crosses the physical boundary one
    /// command at a time and can durably reject the untouched suffix after the first failure.
    pub fn prepare_batch(
        &mut self,
        commands: Vec<ExecutionCommand>,
    ) -> Result<(), CommandJournalError> {
        if commands.is_empty() {
            return Ok(());
        }
        let mut staged_client_ids = BTreeSet::new();
        let mut staged_command_ids = BTreeSet::new();
        let mut receipts = Vec::with_capacity(commands.len());
        let mut sequence = self.next_sequence;
        for command in commands {
            command
                .validate_persisted_shape()
                .map_err(CommandJournalError::Command)?;
            let command_id = command.command_id().clone();
            if self.receipts.contains_key(&command_id)
                || !staged_command_ids.insert(command_id.clone())
            {
                return Err(CommandJournalError::Conflict);
            }
            if let Some(client_id) = command.native_client_id()
                && (self.client_ids.contains_key(client_id)
                    || !staged_client_ids.insert(client_id.clone()))
            {
                return Err(CommandJournalError::ClientId);
            }
            if let ExecutionCommand::Cancel(cancel) = &command {
                validate_cancel_target(&self.receipts, &self.client_ids, cancel)?;
            }
            receipts.push(CommandReceipt {
                sequence,
                command_sha256: command_hash(&command)?,
                command,
                state: CommandState::Prepared,
            });
            sequence = sequence
                .checked_add(1)
                .ok_or(CommandJournalError::Sequence)?;
        }
        self.append_batch(&receipts)?;
        self.next_sequence = sequence;
        for receipt in receipts {
            let command_id = receipt.command.command_id().clone();
            if let Some(client_id) = receipt.command.native_client_id() {
                self.client_ids
                    .insert(client_id.clone(), command_id.clone());
            }
            self.prepared_commands
                .insert(command_id.clone(), receipt.command.clone());
            self.first_command_sequences
                .insert(command_id.clone(), receipt.sequence);
            self.append_current_v2_commitment(&receipt.command, receipt.sequence)?;
            self.add_query_indexes(&command_id, &receipt);
            self.receipts.insert(command_id, receipt);
        }
        Ok(())
    }

    /// Persists one already-decided physical batch with a single durability barrier. Every
    /// command still has the same Prepared -> Submitted recovery history; only the writes are
    /// grouped so a grid fill never pays one fsync per child mutation.
    pub fn prepare_submitted_batch(
        &mut self,
        commands: Vec<ExecutionCommand>,
    ) -> Result<(), CommandJournalError> {
        if commands.is_empty() {
            return Ok(());
        }
        let mut staged_client_ids = BTreeSet::new();
        let mut staged_command_ids = BTreeSet::new();
        let mut receipts = Vec::with_capacity(commands.len().saturating_mul(2));
        let mut sequence = self.next_sequence;
        for command in commands {
            command
                .validate_persisted_shape()
                .map_err(CommandJournalError::Command)?;
            let command_id = command.command_id().clone();
            if self.receipts.contains_key(&command_id)
                || !staged_command_ids.insert(command_id.clone())
            {
                return Err(CommandJournalError::Conflict);
            }
            if let Some(client_id) = command.native_client_id()
                && (self.client_ids.contains_key(client_id)
                    || !staged_client_ids.insert(client_id.clone()))
            {
                return Err(CommandJournalError::ClientId);
            }
            if let ExecutionCommand::Cancel(cancel) = &command {
                validate_cancel_target(&self.receipts, &self.client_ids, cancel)?;
            }
            let command_sha256 = command_hash(&command)?;
            receipts.push(CommandReceipt {
                sequence,
                command: command.clone(),
                command_sha256: command_sha256.clone(),
                state: CommandState::Prepared,
            });
            sequence = sequence
                .checked_add(1)
                .ok_or(CommandJournalError::Sequence)?;
            receipts.push(CommandReceipt {
                sequence,
                command,
                command_sha256,
                state: CommandState::Submitted,
            });
            sequence = sequence
                .checked_add(1)
                .ok_or(CommandJournalError::Sequence)?;
        }
        self.append_batch(&receipts)?;
        self.next_sequence = sequence;
        for receipt in receipts
            .iter()
            .filter(|receipt| matches!(receipt.state, CommandState::Prepared))
        {
            let command_id = receipt.command.command_id().clone();
            self.prepared_commands
                .insert(command_id.clone(), receipt.command.clone());
            self.first_command_sequences
                .insert(command_id, receipt.sequence);
            self.append_current_v2_commitment(&receipt.command, receipt.sequence)?;
        }
        for receipt in receipts
            .into_iter()
            .filter(|receipt| matches!(receipt.state, CommandState::Submitted))
        {
            let command_id = receipt.command.command_id().clone();
            if let Some(client_id) = receipt.command.native_client_id() {
                self.client_ids
                    .insert(client_id.clone(), command_id.clone());
            }
            self.add_query_indexes(&command_id, &receipt);
            self.receipts.insert(command_id, receipt);
        }
        Ok(())
    }

    pub fn transition(
        &mut self,
        command_id: &CommandId,
        state: CommandState,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        let previous = self
            .receipts
            .get(command_id)
            .cloned()
            .ok_or(CommandJournalError::Missing)?;
        if previous.state.terminal() || !allowed_transition(&previous.state, &state) {
            return Err(CommandJournalError::Transition);
        }
        let receipt = CommandReceipt {
            sequence: self.next_sequence,
            command: previous.command.clone(),
            command_sha256: previous.command_sha256.clone(),
            state,
        };
        self.append(&receipt)?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(CommandJournalError::Sequence)?;
        self.replace_query_indexes(command_id, &previous, &receipt);
        self.receipts.insert(command_id.clone(), receipt);
        self.receipts
            .get(command_id)
            .ok_or(CommandJournalError::Missing)
    }

    pub fn receipt(&self, command_id: &CommandId) -> Option<&CommandReceipt> {
        self.receipts.get(command_id)
    }

    fn append_current_v2_commitment(
        &mut self,
        command: &ExecutionCommand,
        prepared_sequence: u64,
    ) -> Result<(), CommandJournalError> {
        let next_count = self
            .current_record_count
            .checked_add(1)
            .ok_or(CommandJournalError::Sequence)?;
        self.current_wal_root = v2_next_root(
            self.current_wal_root,
            prepared_sequence,
            next_count,
            command,
        )?;
        self.current_record_count = next_count;
        Ok(())
    }

    fn v2_head_for_prefix(
        &self,
        tail_sequence: u64,
    ) -> Result<DurableWalHead, CommandJournalError> {
        durable_head_v2_from_prepared(
            &self.prepared_commands,
            &self.first_command_sequences,
            tail_sequence,
        )
    }

    /// Exposes the latest durable form of each semantic command for read-only recovery checks.
    /// Callers must not infer exchange state from this iterator; it is only an identity ledger.
    #[doc(hidden)]
    pub fn commands(&self) -> impl Iterator<Item = &ExecutionCommand> {
        self.receipts.values().map(|receipt| &receipt.command)
    }

    pub fn place_by_client_id(&self, client_order_id: &CommandId) -> Option<&OrderCommand> {
        let command_id = self.client_ids.get(client_order_id)?;
        match &self.receipts.get(command_id)?.command {
            ExecutionCommand::PlaceLimit(command) => Some(command),
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_)
            | ExecutionCommand::Cancel(_) => None,
        }
    }

    pub fn market_reduce_by_client_id(
        &self,
        client_order_id: &CommandId,
    ) -> Option<&MarketReduceCommand> {
        let command_id = self.client_ids.get(client_order_id)?;
        match &self.receipts.get(command_id)?.command {
            ExecutionCommand::MarketReduce(command) => Some(command),
            ExecutionCommand::PlaceLimit(_)
            | ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_)
            | ExecutionCommand::Cancel(_) => None,
        }
    }

    pub fn stop_full_by_client_id(
        &self,
        client_algo_id: &CommandId,
    ) -> Option<&StopMarketFullPositionCommand> {
        let command_id = self.client_ids.get(client_algo_id)?;
        match &self.receipts.get(command_id)?.command {
            ExecutionCommand::StopMarketFullPosition(command) => Some(command),
            ExecutionCommand::PlaceLimit(_)
            | ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::Cancel(_) => None,
        }
    }

    pub fn owner_by_client_id(
        &self,
        client_order_id: &CommandId,
    ) -> Option<&crate::domain::OrderOwner> {
        let command_id = self.client_ids.get(client_order_id)?;
        self.receipts.get(command_id)?.command.owner()
    }

    /// Resolves the durable semantic command identity for a native client identity. Recovery uses
    /// this together with the owner lookup; exchange payloads never get to invent either value.
    pub fn command_id_by_client_id(&self, client_id: &CommandId) -> Option<&CommandId> {
        self.client_ids.get(client_id)
    }

    /// Resolves an exchange-native order id back to the one client identity admitted by this
    /// WAL. Some signed trade-history surfaces (notably Binance userTrades) omit client order id;
    /// the runtime may recover it only from an Accepted receipt, never by parsing strategy names
    /// from venue payloads.
    pub fn client_id_by_venue_order_id(&self, venue_order_id: &str) -> Option<&CommandId> {
        self.venue_order_client_ids
            .get(venue_order_id)
            .and_then(Option::as_ref)
    }

    /// Read-only order-identity projection rebuilt from this command WAL. It adds no owner
    /// journal: every route is the exact latest receipt and native identity already persisted
    /// here. Ambiguous venue ids remain absent rather than being guessed across families.
    pub fn native_order_routes(&self) -> Vec<NativeOrderRoute> {
        self.receipts
            .values()
            .filter_map(|receipt| {
                let family = receipt.command.native_order_family()?;
                let client_id = receipt.command.native_client_id()?.clone();
                let owner = receipt.command.owner()?.clone();
                let venue_order_id = match &receipt.state {
                    CommandState::Accepted { venue_order_id }
                        if self
                            .client_id_by_venue_order_id(venue_order_id)
                            .is_some_and(|mapped| mapped == &client_id) =>
                    {
                        Some(venue_order_id.clone())
                    }
                    CommandState::Accepted { .. } => return None,
                    _ => None,
                };
                Some(NativeOrderRoute {
                    command_id: receipt.command.command_id().clone(),
                    owner,
                    key: NativeOrderRouteKey { family, client_id },
                    venue_order_id,
                    state: receipt.state.clone(),
                })
            })
            .collect()
    }

    pub fn order_identity_by_client_id(
        &self,
        client_id: &CommandId,
    ) -> Option<OrderReadbackIdentity<'_>> {
        let command_id = self.client_ids.get(client_id)?;
        self.order_identity(command_id)
    }

    /// Resolves a cancellation's target from the durable original command rather than from a
    /// caller-provided family. This survives restart and prevents a conditional strategy ID from
    /// being queried as a normal UM order.
    pub fn cancel_target_identity(
        &self,
        cancel_command_id: &CommandId,
    ) -> Option<OrderReadbackIdentity<'_>> {
        let cancel = match &self.receipts.get(cancel_command_id)?.command {
            ExecutionCommand::Cancel(cancel) => cancel,
            ExecutionCommand::PlaceLimit(_)
            | ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return None;
            }
        };
        let identity = self.order_identity_by_client_id(&cancel.target_client_order_id)?;
        (identity.owner == &cancel.owner).then_some(identity)
    }

    pub fn order_identity(&self, command_id: &CommandId) -> Option<OrderReadbackIdentity<'_>> {
        match &self.receipts.get(command_id)?.command {
            ExecutionCommand::PlaceLimit(command) => Some(OrderReadbackIdentity {
                owner: &command.owner,
                family: NativeOrderFamily::UmOrder,
                client_id: &command.client_order_id,
            }),
            ExecutionCommand::PlaceMarket(command) => Some(OrderReadbackIdentity {
                owner: &command.owner,
                family: NativeOrderFamily::UmOrder,
                client_id: &command.client_order_id,
            }),
            ExecutionCommand::MarketReduce(command) => Some(OrderReadbackIdentity {
                owner: &command.owner,
                family: NativeOrderFamily::UmOrder,
                client_id: &command.client_order_id,
            }),
            ExecutionCommand::StopMarketCloseAll(command) => Some(OrderReadbackIdentity {
                owner: &command.owner,
                family: NativeOrderFamily::UmConditional,
                client_id: &command.client_strategy_id,
            }),
            ExecutionCommand::StopMarketFullPosition(command) => Some(OrderReadbackIdentity {
                owner: &command.owner,
                family: NativeOrderFamily::UmAlgo,
                client_id: &command.client_algo_id,
            }),
            ExecutionCommand::Cancel(_) => None,
        }
    }

    /// Returns cloned durable identities for bounded crash-recovery indexing. The caller still has
    /// to bind each identity to its recovered run scope; raw venue IDs are never authoritative.
    pub fn recovery_identities(
        &self,
    ) -> Vec<(CommandId, OrderOwner, NativeOrderFamily, CommandId)> {
        self.receipts
            .iter()
            .filter_map(|(command_id, receipt)| {
                Some((
                    command_id.clone(),
                    receipt.command.owner()?.clone(),
                    receipt.command.native_order_family()?,
                    receipt.command.native_client_id()?.clone(),
                ))
            })
            .collect()
    }

    pub fn place(&self, command_id: &CommandId) -> Option<&OrderCommand> {
        match &self.receipts.get(command_id)?.command {
            ExecutionCommand::PlaceLimit(command) => Some(command),
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_)
            | ExecutionCommand::Cancel(_) => None,
        }
    }

    pub fn market_reduce(&self, command_id: &CommandId) -> Option<&MarketReduceCommand> {
        match &self.receipts.get(command_id)?.command {
            ExecutionCommand::MarketReduce(command) => Some(command),
            ExecutionCommand::PlaceLimit(_)
            | ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_)
            | ExecutionCommand::Cancel(_) => None,
        }
    }

    pub fn cancel(&self, command_id: &CommandId) -> Option<&CancelCommand> {
        match &self.receipts.get(command_id)?.command {
            ExecutionCommand::Cancel(command) => Some(command),
            ExecutionCommand::PlaceLimit(_)
            | ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => None,
        }
    }

    pub fn has_unresolved(&self) -> bool {
        !self.unresolved_ids.is_empty()
    }

    pub fn unresolved_command_ids(&self) -> Vec<CommandId> {
        self.unresolved_ids.iter().cloned().collect()
    }

    /// Returns only UNKNOWN commands whose readback cannot create new exposure. Recovery may
    /// query these exact durable identities without resubmitting anything. UNKNOWN entry or
    /// reduction commands are excluded until fill recovery proves their complete lifecycle.
    pub fn unknown_protection_or_cancel_command_ids(&self) -> Vec<CommandId> {
        self.receipts
            .iter()
            .filter(|(_, receipt)| {
                matches!(receipt.state, CommandState::Unknown { .. })
                    && matches!(
                        receipt.command,
                        ExecutionCommand::StopMarketCloseAll(_)
                            | ExecutionCommand::StopMarketFullPosition(_)
                            | ExecutionCommand::Cancel(_)
                    )
            })
            .map(|(command_id, _)| command_id.clone())
            .collect()
    }

    /// Recovery linearizes crashes around dispatch without guessing an exchange outcome.
    /// Prepared proves the gateway was never called; Submitted is ambiguous and becomes UNKNOWN.
    pub fn fence_interrupted_dispatches(&mut self) -> Result<(u32, u32), CommandJournalError> {
        let pending = self
            .receipts
            .iter()
            .filter_map(|(command_id, receipt)| match receipt.state {
                CommandState::Prepared => Some((command_id.clone(), true)),
                CommandState::Submitted => Some((command_id.clone(), false)),
                CommandState::Accepted { .. }
                | CommandState::Rejected { .. }
                | CommandState::Unknown { .. } => None,
            })
            .collect::<Vec<_>>();
        let mut rejected = 0_u32;
        let mut unknown = 0_u32;
        for (command_id, never_dispatched) in pending {
            if never_dispatched {
                self.transition(
                    &command_id,
                    CommandState::Rejected {
                        reason: "recovery_proved_never_dispatched".to_owned(),
                    },
                )?;
                rejected = rejected.saturating_add(1);
            } else {
                self.transition(
                    &command_id,
                    CommandState::Unknown {
                        reason: "recovery_interrupted_dispatch".to_owned(),
                    },
                )?;
                unknown = unknown.saturating_add(1);
            }
        }
        Ok((rejected, unknown))
    }

    /// Emergency flattening may proceed past an UNKNOWN protection or cancel, because neither can
    /// increase exposure. An unresolved entry or reduction remains ambiguous and blocks another
    /// full-size reduction until exact readback settles it.
    pub fn has_unresolved_entry_or_reduce(&self) -> bool {
        self.unresolved_entry_or_reduce != 0
    }

    pub fn has_unresolved_cancel_for(&self, target_client_id: &CommandId) -> bool {
        self.unresolved_cancel_targets
            .get(target_client_id)
            .is_some_and(|count| *count != 0)
    }

    /// A terminally accepted cancel can briefly precede removal from a signed open-order
    /// projection. Runtime reconciliation may use this exact target identity only to wait for a
    /// newer readback; it does not reinterpret the order as cancelled or authorize mutation.
    pub fn has_accepted_cancel_for(&self, target_client_id: &CommandId) -> bool {
        self.accepted_cancel_targets
            .get(target_client_id)
            .is_some_and(|count| *count != 0)
    }

    fn rebuild_query_indexes(&mut self) {
        self.unresolved_ids.clear();
        self.unresolved_entry_or_reduce = 0;
        self.unresolved_cancel_targets.clear();
        self.accepted_cancel_targets.clear();
        self.venue_order_client_ids.clear();
        let receipts = self
            .receipts
            .iter()
            .map(|(command_id, receipt)| (command_id.clone(), receipt.clone()))
            .collect::<Vec<_>>();
        for (command_id, receipt) in receipts {
            self.add_query_indexes(&command_id, &receipt);
        }
    }

    fn add_query_indexes(&mut self, command_id: &CommandId, receipt: &CommandReceipt) {
        if unresolved(&receipt.state) && self.unresolved_ids.insert(command_id.clone()) {
            if entry_or_reduce(&receipt.command) {
                self.unresolved_entry_or_reduce = self.unresolved_entry_or_reduce.saturating_add(1);
            }
            if let ExecutionCommand::Cancel(cancel) = &receipt.command {
                increment_count(
                    &mut self.unresolved_cancel_targets,
                    cancel.target_client_order_id.clone(),
                );
            }
        }
        if matches!(receipt.state, CommandState::Accepted { .. }) {
            if let ExecutionCommand::Cancel(cancel) = &receipt.command {
                increment_count(
                    &mut self.accepted_cancel_targets,
                    cancel.target_client_order_id.clone(),
                );
            }
            if let (Some(client_id), CommandState::Accepted { venue_order_id }) =
                (receipt.command.native_client_id(), &receipt.state)
            {
                match self.venue_order_client_ids.get_mut(venue_order_id) {
                    None => {
                        self.venue_order_client_ids
                            .insert(venue_order_id.clone(), Some(client_id.clone()));
                    }
                    Some(Some(existing)) if existing == client_id => {}
                    Some(value) => *value = None,
                }
            }
        }
    }

    fn replace_query_indexes(
        &mut self,
        command_id: &CommandId,
        previous: &CommandReceipt,
        next: &CommandReceipt,
    ) {
        if unresolved(&previous.state) && !unresolved(&next.state) {
            if self.unresolved_ids.remove(command_id) && entry_or_reduce(&previous.command) {
                self.unresolved_entry_or_reduce = self.unresolved_entry_or_reduce.saturating_sub(1);
            }
            if let ExecutionCommand::Cancel(cancel) = &previous.command {
                decrement_count(
                    &mut self.unresolved_cancel_targets,
                    &cancel.target_client_order_id,
                );
            }
        }
        self.add_query_indexes(command_id, next);
    }

    fn append(&mut self, receipt: &CommandReceipt) -> Result<(), CommandJournalError> {
        let encoded = serde_json::to_vec(receipt).map_err(CommandJournalError::Encode)?;
        self.append_encoded(&[encoded.as_slice(), b"\n"])
    }

    fn append_batch(&mut self, receipts: &[CommandReceipt]) -> Result<(), CommandJournalError> {
        let mut encoded = Vec::new();
        for receipt in receipts {
            serde_json::to_writer(&mut encoded, receipt).map_err(CommandJournalError::Encode)?;
            encoded.push(b'\n');
        }
        self.append_encoded(&[encoded.as_slice()])
    }

    fn append_encoded(&mut self, chunks: &[&[u8]]) -> Result<(), CommandJournalError> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.try_lock_exclusive()
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        let disk_len = file
            .metadata()
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })?
            .len();
        if disk_len != self.durable_len {
            return Err(CommandJournalError::Sequence);
        }
        let appended_len = chunks.iter().try_fold(0_u64, |total, chunk| {
            let chunk_len =
                u64::try_from(chunk.len()).map_err(|_| CommandJournalError::Sequence)?;
            total
                .checked_add(chunk_len)
                .ok_or(CommandJournalError::Sequence)
        })?;
        for chunk in chunks {
            file.write_all(chunk)
                .map_err(|source| CommandJournalError::Io {
                    path: self.path.clone(),
                    source,
                })?;
        }
        file.sync_data().map_err(|source| CommandJournalError::Io {
            path: self.path.clone(),
            source,
        })?;
        if self.needs_parent_sync {
            sync_parent(&self.path)?;
            self.needs_parent_sync = false;
        }
        self.durable_len = self
            .durable_len
            .checked_add(appended_len)
            .ok_or(CommandJournalError::Sequence)?;
        Ok(())
    }
}

fn unresolved(state: &CommandState) -> bool {
    matches!(
        state,
        CommandState::Prepared | CommandState::Submitted | CommandState::Unknown { .. }
    )
}

fn entry_or_reduce(command: &ExecutionCommand) -> bool {
    match command {
        ExecutionCommand::PlaceLimit(command) => matches!(
            command.owner.purpose,
            crate::domain::OrderPurpose::Entry | crate::domain::OrderPurpose::Reduce
        ),
        ExecutionCommand::PlaceMarket(command) => {
            command.owner.purpose == crate::domain::OrderPurpose::Entry
        }
        ExecutionCommand::MarketReduce(_) => true,
        ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_)
        | ExecutionCommand::Cancel(_) => false,
    }
}

fn increment_count(counts: &mut BTreeMap<CommandId, usize>, key: CommandId) {
    let count = counts.entry(key).or_default();
    *count = count.saturating_add(1);
}

fn decrement_count(counts: &mut BTreeMap<CommandId, usize>, key: &CommandId) {
    let Some(count) = counts.get_mut(key) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(key);
    }
}

struct JournalReplay {
    receipts: Vec<CommandReceipt>,
    durable_len: u64,
    existed: bool,
}

fn read_all(path: &Path) -> Result<JournalReplay, CommandJournalError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(JournalReplay {
                receipts: Vec::new(),
                durable_len: 0,
                existed: false,
            });
        }
        Err(source) => {
            return Err(CommandJournalError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(CommandJournalError::Truncated);
    }
    let durable_len = u64::try_from(bytes.len()).map_err(|_| CommandJournalError::Sequence)?;
    let body = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
    if body
        .split(|byte| *byte == b'\n')
        .any(|line| line.is_empty())
        && !body.is_empty()
    {
        return Err(CommandJournalError::Sequence);
    }
    let receipts = if body.is_empty() {
        Vec::new()
    } else {
        body.split(|byte| *byte == b'\n')
            .map(|line| serde_json::from_slice(line).map_err(CommandJournalError::Decode))
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(JournalReplay {
        receipts,
        durable_len,
        existed: true,
    })
}

#[cfg(unix)]
pub(crate) fn sync_parent(path: &Path) -> Result<(), CommandJournalError> {
    let parent = path.parent().ok_or(CommandJournalError::Sequence)?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| CommandJournalError::Io {
            path: parent.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
pub(crate) fn sync_parent(_path: &Path) -> Result<(), CommandJournalError> {
    Ok(())
}

fn durable_head_v1_from_ordered<'a>(
    commands: impl IntoIterator<Item = &'a ExecutionCommand>,
    tail_sequence: u64,
) -> Result<DurableWalHead, CommandJournalError> {
    let ordered = commands.into_iter().collect::<Vec<_>>();
    let encoded = serde_json::to_vec(&ordered).map_err(|_| CommandJournalError::Hash)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.execution.command-wal-head.v1");
    digest.update(encoded);
    let record_count = ordered
        .len()
        .try_into()
        .map_err(|_| CommandJournalError::Sequence)?;
    DurableWalHead::new(digest.finalize().into(), tail_sequence, record_count)
        .map_err(|_| CommandJournalError::Sequence)
}

fn durable_head_v2_from_prepared(
    commands: &BTreeMap<CommandId, ExecutionCommand>,
    first_command_sequences: &BTreeMap<CommandId, u64>,
    tail_sequence: u64,
) -> Result<DurableWalHead, CommandJournalError> {
    let mut ordered = first_command_sequences
        .iter()
        .filter(|(_, sequence)| **sequence <= tail_sequence)
        .map(|(command_id, sequence)| {
            commands
                .get(command_id)
                .map(|command| (*sequence, command))
                .ok_or(CommandJournalError::Missing)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ordered.sort_by_key(|(sequence, _)| *sequence);
    let mut root = v2_empty_root();
    let mut record_count = 0_u64;
    for (sequence, command) in ordered {
        record_count = record_count
            .checked_add(1)
            .ok_or(CommandJournalError::Sequence)?;
        root = v2_next_root(root, sequence, record_count, command)?;
    }
    DurableWalHead::new_v2(root, tail_sequence, record_count)
        .map_err(|_| CommandJournalError::Sequence)
}

fn v2_empty_root() -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue.execution.command-wal-head.v2.empty");
    digest.finalize().into()
}

fn v2_next_root(
    previous_root: [u8; 32],
    prepared_sequence: u64,
    record_count: u64,
    command: &ExecutionCommand,
) -> Result<[u8; 32], CommandJournalError> {
    let command_digest = execution_command_sha256(command).map_err(CommandJournalError::Encode)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.execution.command-wal-head.v2.append");
    digest.update(previous_root);
    digest.update(prepared_sequence.to_be_bytes());
    digest.update(record_count.to_be_bytes());
    digest.update(command_digest);
    Ok(digest.finalize().into())
}

fn command_hash(command: &ExecutionCommand) -> Result<String, CommandJournalError> {
    Ok(execution_command_sha256(command)
        .map_err(CommandJournalError::Encode)?
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_cancel_target(
    receipts: &BTreeMap<CommandId, CommandReceipt>,
    client_ids: &BTreeMap<CommandId, CommandId>,
    cancel: &CancelCommand,
) -> Result<(), CommandJournalError> {
    let target_command_id = client_ids
        .get(&cancel.target_client_order_id)
        .ok_or(CommandJournalError::Target)?;
    let target = receipts
        .get(target_command_id)
        .ok_or(CommandJournalError::Target)?;
    let Some(target_owner) = target.command.owner() else {
        return Err(CommandJournalError::Target);
    };
    if target_owner != &cancel.owner {
        return Err(CommandJournalError::Owner);
    }
    Ok(())
}

fn allowed_transition(previous: &CommandState, next: &CommandState) -> bool {
    matches!(
        (previous, next),
        (CommandState::Prepared, CommandState::Submitted)
            | (CommandState::Prepared, CommandState::Rejected { .. })
            | (CommandState::Submitted, CommandState::Accepted { .. })
            | (CommandState::Submitted, CommandState::Rejected { .. })
            | (CommandState::Submitted, CommandState::Unknown { .. })
            | (CommandState::Unknown { .. }, CommandState::Accepted { .. })
            | (CommandState::Unknown { .. }, CommandState::Rejected { .. })
    )
}

#[derive(Debug, thiserror::Error)]
pub enum CommandJournalError {
    #[error("execution journal I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("execution journal encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("execution journal decoding failed: {0}")]
    Decode(serde_json::Error),
    #[error("execution journal has a truncated tail and cannot prove mutation state")]
    Truncated,
    #[error("execution journal sequence is invalid or exhausted")]
    Sequence,
    #[error("execution journal reuses a command or client order identity")]
    Duplicate,
    #[error("command journal record has an invalid payload hash")]
    Hash,
    #[error("command is invalid: {0}")]
    Command(crate::domain::CommandError),
    #[error("command identity was reused with different content")]
    Conflict,
    #[error("client order identity is already bound to another command")]
    ClientId,
    #[error("cancel target is not a previously journaled client order")]
    Target,
    #[error("cancel owner does not match the target order owner")]
    Owner,
    #[error("command is missing from the execution journal")]
    Missing,
    #[error("command state transition is not allowed")]
    Transition,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::domain::{
        CancelCommand, CommandId, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
        StopMarketCloseAllCommand,
    };

    use super::*;

    fn command() -> Result<OrderCommand, Box<dyn std::error::Error>> {
        Ok(OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("command_1")?,
            client_order_id: CommandId::new("client_1")?,
            owner: OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        })
    }

    fn indexed_command(index: usize) -> Result<OrderCommand, Box<dyn std::error::Error>> {
        let mut planned = command()?;
        planned.command_id = CommandId::new(format!("incremental_command_{index:05}"))?;
        planned.client_order_id = CommandId::new(format!("incremental_client_{index:05}"))?;
        Ok(planned)
    }

    #[test]
    fn command_is_durable_before_submission_and_unknown_blocks_new_risk()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let mut journal = CommandJournal::open(&path)?;
        let planned = command()?;

        assert_eq!(
            journal.prepare_place(planned.clone())?.state,
            CommandState::Prepared
        );
        assert_eq!(journal.prepare_place(planned.clone())?.sequence, 1);
        journal.transition(&planned.command_id, CommandState::Submitted)?;
        journal.transition(
            &planned.command_id,
            CommandState::Unknown {
                reason: "timeout".to_owned(),
            },
        )?;
        assert!(journal.has_unresolved());

        let recovered = CommandJournal::open(path)?;
        assert!(matches!(
            recovered
                .receipt(&planned.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));
        Ok(())
    }

    #[test]
    fn limit_policy_is_part_of_wal_identity_and_survives_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let mut journal = CommandJournal::open(&path)?;
        let post_only = command()?;
        journal.prepare_place(post_only.clone())?;
        let mut changed = post_only.clone();
        changed.time_in_force = venue_domain::LimitTimeInForce::Gtc;
        assert!(journal.prepare_place(changed).is_err());
        let mut gtc = indexed_command(2)?;
        gtc.time_in_force = venue_domain::LimitTimeInForce::Gtc;
        journal.prepare_place(gtc.clone())?;
        drop(journal);
        let recovered = CommandJournal::open(&path)?;
        assert_eq!(
            recovered
                .receipt(&post_only.command_id)
                .ok_or("legacy missing")?
                .command,
            ExecutionCommand::PlaceLimit(post_only)
        );
        assert_eq!(
            recovered
                .receipt(&gtc.command_id)
                .ok_or("GTC missing")?
                .command,
            ExecutionCommand::PlaceLimit(gtc)
        );
        Ok(())
    }

    #[test]
    fn historical_wal_heads_require_an_exact_replayed_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let planned = command()?;
        let mut journal = CommandJournal::open(&path)?;
        let empty = journal.durable_wal_head()?;
        assert_eq!(empty.format_version(), DurableWalHeadFormat::V2);

        journal.prepare_place(planned.clone())?;
        let prepared = journal.durable_wal_head()?;
        let legacy_prepared = durable_head_v1_from_ordered(
            [&ExecutionCommand::PlaceLimit(planned.clone())],
            prepared.tail_sequence(),
        )?;
        journal.transition(&planned.command_id, CommandState::Submitted)?;
        let submitted = journal.durable_wal_head()?;
        journal.transition(
            &planned.command_id,
            CommandState::Accepted {
                venue_order_id: "venue-1".to_owned(),
            },
        )?;
        let accepted = journal.durable_wal_head()?;

        assert_eq!(prepared.root_sha256(), submitted.root_sha256());
        assert_eq!(submitted.root_sha256(), accepted.root_sha256());
        assert_ne!(prepared.tail_sequence(), submitted.tail_sequence());
        assert_ne!(submitted.tail_sequence(), accepted.tail_sequence());
        for head in [empty, prepared, submitted, accepted, legacy_prepared] {
            assert!(journal.validates_historical_wal_head(head));
        }
        let forged_tail = DurableWalHead::new(
            accepted.root_sha256(),
            accepted
                .tail_sequence()
                .checked_add(1)
                .ok_or("tail overflow")?,
            accepted.record_count(),
        )?;
        assert!(!journal.validates_historical_wal_head(forged_tail));

        drop(journal);
        let recovered = CommandJournal::open(path)?;
        for head in [empty, prepared, submitted, accepted, legacy_prepared] {
            assert!(recovered.validates_historical_wal_head(head));
        }
        assert!(!recovered.validates_historical_wal_head(forged_tail));
        Ok(())
    }

    #[test]
    fn incremental_v2_prepared_growth_reopens_with_exact_current_and_prefix_heads()
    -> Result<(), Box<dyn std::error::Error>> {
        const COMMANDS: usize = 2_048;
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let mut journal = CommandJournal::open(&path)?;
        let mut prefix = None;
        for index in (0..COMMANDS).rev() {
            let planned = indexed_command(index)?;
            journal.prepare_place(planned)?;
            if index == COMMANDS / 2 {
                prefix = Some(journal.durable_wal_head()?);
            }
        }
        let current = journal.durable_wal_head()?;
        let prefix = prefix.ok_or("prefix head missing")?;
        assert_eq!(current.format_version(), DurableWalHeadFormat::V2);
        assert_eq!(current.record_count(), COMMANDS as u64);
        assert!(journal.validates_historical_wal_head(prefix));
        assert!(journal.validates_historical_wal_head(current));

        drop(journal);
        let reopened = CommandJournal::open(path)?;
        assert!(reopened.validates_historical_wal_head(prefix));
        assert!(reopened.validates_historical_wal_head(current));
        Ok(())
    }

    #[test]
    fn submitted_batch_and_hot_indexes_survive_replay() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let first = command()?;
        let mut second = command()?;
        second.command_id = CommandId::new("command_2")?;
        second.client_order_id = CommandId::new("client_2")?;
        let mut journal = CommandJournal::open(&path)?;

        journal.prepare_submitted_batch(vec![
            ExecutionCommand::PlaceLimit(first.clone()),
            ExecutionCommand::PlaceLimit(second.clone()),
        ])?;
        assert!(journal.has_unresolved());
        assert!(journal.has_unresolved_entry_or_reduce());
        assert_eq!(
            journal
                .receipt(&first.command_id)
                .map(|receipt| (&receipt.state, receipt.sequence)),
            Some((&CommandState::Submitted, 2))
        );
        assert_eq!(
            journal
                .receipt(&second.command_id)
                .map(|receipt| (&receipt.state, receipt.sequence)),
            Some((&CommandState::Submitted, 4))
        );

        journal.transition(
            &first.command_id,
            CommandState::Accepted {
                venue_order_id: "venue_1".to_owned(),
            },
        )?;
        journal.transition(
            &second.command_id,
            CommandState::Accepted {
                venue_order_id: "venue_2".to_owned(),
            },
        )?;
        assert!(!journal.has_unresolved());
        assert_eq!(
            journal.client_id_by_venue_order_id("venue_1"),
            Some(&first.client_order_id)
        );

        let cancel = CancelCommand {
            command_id: CommandId::new("cancel_1")?,
            owner: first.owner.clone(),
            target_client_order_id: first.client_order_id.clone(),
        };
        journal.prepare_submitted_batch(vec![ExecutionCommand::Cancel(cancel.clone())])?;
        assert!(journal.has_unresolved_cancel_for(&first.client_order_id));
        journal.transition(
            &cancel.command_id,
            CommandState::Accepted {
                venue_order_id: "venue_1".to_owned(),
            },
        )?;
        assert!(!journal.has_unresolved());
        assert!(!journal.has_unresolved_cancel_for(&first.client_order_id));
        assert!(journal.has_accepted_cancel_for(&first.client_order_id));

        let recovered = CommandJournal::open(path)?;
        assert!(!recovered.has_unresolved());
        assert_eq!(
            recovered.client_id_by_venue_order_id("venue_2"),
            Some(&second.client_order_id)
        );
        assert!(recovered.has_accepted_cancel_for(&first.client_order_id));
        Ok(())
    }

    #[test]
    fn interrupted_dispatches_are_fenced_without_resubmission()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let prepared = command()?;
        let mut submitted_entry = command()?;
        submitted_entry.command_id = CommandId::new("command_submitted")?;
        submitted_entry.client_order_id = CommandId::new("client_submitted")?;
        let stop = StopMarketCloseAllCommand {
            command_id: CommandId::new("protect_submitted")?,
            client_strategy_id: CommandId::new("venue_protect_submitted")?,
            owner: OrderOwner {
                purpose: OrderPurpose::Protection,
                ..prepared.owner.clone()
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            stop_price: Price::new(Decimal::new(9, 1))?,
            position_generation: 1,
        };
        let mut journal = CommandJournal::open(&path)?;
        journal.prepare_place(prepared.clone())?;
        journal.prepare_place(submitted_entry.clone())?;
        journal.transition(&submitted_entry.command_id, CommandState::Submitted)?;
        journal.prepare_stop_market_close_all(stop.clone())?;
        journal.transition(&stop.command_id, CommandState::Submitted)?;

        assert_eq!(journal.fence_interrupted_dispatches()?, (1, 2));
        assert!(matches!(
            journal
                .receipt(&prepared.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Rejected { .. })
        ));
        assert!(matches!(
            journal
                .receipt(&submitted_entry.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));
        assert_eq!(
            journal.unknown_protection_or_cancel_command_ids(),
            vec![stop.command_id]
        );
        assert_eq!(journal.fence_interrupted_dispatches()?, (0, 0));

        let recovered = CommandJournal::open(path)?;
        assert!(recovered.has_unresolved_entry_or_reduce());
        Ok(())
    }

    #[test]
    fn command_identity_cannot_be_rebound() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut journal = CommandJournal::open(directory.path().join("commands.jsonl"))?;
        let original = command()?;
        journal.prepare_place(original.clone())?;
        let mut conflicting = original;
        conflicting.quantity = Decimal::new(2, 0);

        assert!(matches!(
            journal.prepare_place(conflicting),
            Err(CommandJournalError::Conflict)
        ));
        Ok(())
    }

    #[test]
    fn net_reduction_replays_original_wal_without_current_inventory()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let entry = command()?;
        let reduce = MarketReduceCommand {
            command_id: CommandId::new("net_reduce")?,
            client_order_id: CommandId::new("net_reduce_client")?,
            owner: OrderOwner {
                purpose: OrderPurpose::ExposureTakeProfit,
                ..entry.owner
            },
            position_side: PositionSide::Net,
            side: OrderSide::Sell,
            quantity: Decimal::ONE,
            risk_episode_id: CommandId::new("net_reduce_episode")?,
            position_generation: 9,
        };
        let original = ExecutionCommand::MarketReduce(reduce.clone());
        let mut journal = CommandJournal::open(&path)?;
        journal.prepare(original.clone())?;
        journal.transition(&reduce.command_id, CommandState::Submitted)?;
        journal.transition(
            &reduce.command_id,
            CommandState::Unknown {
                reason: "connection_lost".to_owned(),
            },
        )?;
        drop(journal);
        let mut recovered = CommandJournal::open(&path)?;
        let receipt = recovered
            .receipt(&reduce.command_id)
            .ok_or(CommandJournalError::Missing)?;
        assert_eq!(receipt.command, original);
        assert!(matches!(receipt.state, CommandState::Unknown { .. }));
        assert_eq!(recovered.fence_interrupted_dispatches()?, (0, 0));
        assert_eq!(
            recovered
                .receipt(&reduce.command_id)
                .map(|receipt| &receipt.command),
            Some(&original)
        );
        Ok(())
    }

    #[test]
    fn cancel_is_durable_and_recovers_its_owner_and_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let planned = command()?;
        let cancel = CancelCommand {
            command_id: CommandId::new("cancel_1")?,
            owner: planned.owner.clone(),
            target_client_order_id: planned.client_order_id.clone(),
        };
        let mut journal = CommandJournal::open(&path)?;
        journal.prepare_place(planned.clone())?;
        assert_eq!(journal.prepare_cancel(cancel.clone())?.sequence, 2);

        let recovered = CommandJournal::open(path)?;
        let receipt = recovered
            .receipt(&cancel.command_id)
            .ok_or(CommandJournalError::Missing)?;
        assert_eq!(receipt.state, CommandState::Prepared);
        assert_eq!(receipt.command, ExecutionCommand::Cancel(cancel.clone()));
        assert_eq!(
            recovered
                .place_by_client_id(&cancel.target_client_order_id)
                .map(|target| &target.owner),
            Some(&cancel.owner)
        );
        Ok(())
    }

    #[test]
    fn cancel_requires_an_existing_target_with_the_same_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut journal = CommandJournal::open(directory.path().join("commands.jsonl"))?;
        let planned = command()?;
        journal.prepare_place(planned.clone())?;

        let missing_target = CancelCommand {
            command_id: CommandId::new("cancel_missing")?,
            owner: planned.owner.clone(),
            target_client_order_id: CommandId::new("client_missing")?,
        };
        assert!(matches!(
            journal.prepare_cancel(missing_target),
            Err(CommandJournalError::Target)
        ));

        let mut different_owner = planned.owner.clone();
        different_owner.account = "secondary".to_owned();
        let wrong_owner = CancelCommand {
            command_id: CommandId::new("cancel_other_owner")?,
            owner: different_owner,
            target_client_order_id: planned.client_order_id,
        };
        assert!(matches!(
            journal.prepare_cancel(wrong_owner),
            Err(CommandJournalError::Owner)
        ));
        Ok(())
    }

    #[test]
    fn stop_market_close_all_cancel_is_durable_and_scoped_to_its_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let owner = OrderOwner {
            strategy_instance_id: "scalping_1".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: OrderPurpose::Protection,
        };
        let stop = StopMarketCloseAllCommand {
            command_id: CommandId::new("protect_1")?,
            client_strategy_id: CommandId::new("venue_protect_1")?,
            owner: owner.clone(),
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            stop_price: Price::new(Decimal::new(9, 1))?,
            position_generation: 1,
        };
        let cancel = CancelCommand {
            command_id: CommandId::new("cancel_protect_1")?,
            owner: owner.clone(),
            target_client_order_id: stop.client_strategy_id.clone(),
        };
        let mut journal = CommandJournal::open(&path)?;
        journal.prepare_stop_market_close_all(stop.clone())?;
        assert_eq!(journal.prepare_cancel(cancel.clone())?.sequence, 2);

        let recovered = CommandJournal::open(path)?;
        assert_eq!(
            recovered
                .receipt(&stop.command_id)
                .map(|receipt| &receipt.command),
            Some(&ExecutionCommand::StopMarketCloseAll(stop.clone()))
        );
        assert_eq!(
            recovered.owner_by_client_id(&stop.client_strategy_id),
            Some(&owner)
        );
        assert!(matches!(
            recovered
                .cancel_target_identity(&cancel.command_id)
                .map(|identity| identity.family),
            Some(NativeOrderFamily::UmConditional)
        ));
        assert_eq!(
            recovered
                .receipt(&cancel.command_id)
                .map(|receipt| &receipt.command),
            Some(&ExecutionCommand::Cancel(cancel.clone()))
        );

        let mut recovered = recovered;
        let mut other_owner = owner;
        other_owner.account = "secondary".to_owned();
        assert!(matches!(
            recovered.prepare_cancel(CancelCommand {
                command_id: CommandId::new("cancel_protect_other")?,
                owner: other_owner,
                target_client_order_id: stop.client_strategy_id,
            }),
            Err(CommandJournalError::Owner)
        ));
        Ok(())
    }

    #[test]
    fn each_dispatch_crash_window_recovers_without_reusing_the_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;

        let prepared_path = directory.path().join("prepared.jsonl");
        let prepared = command()?;
        CommandJournal::open(&prepared_path)?.prepare_place(prepared.clone())?;
        let mut recovered = CommandJournal::open(&prepared_path)?;
        assert_eq!(recovered.fence_interrupted_dispatches()?, (1, 0));
        assert!(matches!(
            recovered.transition(&prepared.command_id, CommandState::Submitted),
            Err(CommandJournalError::Transition)
        ));

        let submitted_path = directory.path().join("submitted.jsonl");
        let submitted = command()?;
        let mut journal = CommandJournal::open(&submitted_path)?;
        journal.prepare_place(submitted.clone())?;
        journal.transition(&submitted.command_id, CommandState::Submitted)?;
        drop(journal);
        let mut recovered = CommandJournal::open(&submitted_path)?;
        assert_eq!(recovered.fence_interrupted_dispatches()?, (0, 1));
        let before_retry = fs::metadata(&submitted_path)?.len();
        assert!(matches!(
            recovered.prepare_place(submitted.clone())?.state,
            CommandState::Unknown { .. }
        ));
        assert_eq!(fs::metadata(&submitted_path)?.len(), before_retry);
        assert!(matches!(
            recovered.transition(&submitted.command_id, CommandState::Submitted),
            Err(CommandJournalError::Transition)
        ));
        recovered.transition(
            &submitted.command_id,
            CommandState::Accepted {
                venue_order_id: "signed_readback_order".to_owned(),
            },
        )?;
        drop(recovered);
        let settled = CommandJournal::open(&submitted_path)?;
        assert!(!settled.has_unresolved());
        assert_eq!(
            settled.client_id_by_venue_order_id("signed_readback_order"),
            Some(&submitted.client_order_id)
        );
        Ok(())
    }

    #[test]
    fn stale_concurrent_journal_cannot_append_a_sequence_fork()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let mut first = CommandJournal::open(&path)?;
        let mut stale = CommandJournal::open(&path)?;
        let first_command = command()?;
        let mut stale_command = command()?;
        stale_command.command_id = CommandId::new("stale_command")?;
        stale_command.client_order_id = CommandId::new("stale_client")?;

        first.prepare_place(first_command.clone())?;
        assert!(matches!(
            stale.prepare_place(stale_command.clone()),
            Err(CommandJournalError::Sequence)
        ));

        let recovered = CommandJournal::open(path)?;
        assert!(recovered.receipt(&first_command.command_id).is_some());
        assert!(recovered.receipt(&stale_command.command_id).is_none());
        Ok(())
    }

    #[test]
    fn torn_blank_hash_and_transition_forks_all_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("commands.jsonl");
        let planned = command()?;
        CommandJournal::open(&path)?.prepare_place(planned.clone())?;
        let original = fs::read(&path)?;

        let mut torn = original.clone();
        torn.extend_from_slice(b"{\"sequence\":2");
        fs::write(&path, torn)?;
        assert!(matches!(
            CommandJournal::open(&path),
            Err(CommandJournalError::Truncated)
        ));

        let mut blank = original.clone();
        blank.extend_from_slice(b"\n");
        fs::write(&path, blank)?;
        assert!(matches!(
            CommandJournal::open(&path),
            Err(CommandJournalError::Sequence)
        ));

        let mut receipt: CommandReceipt = serde_json::from_slice(
            original
                .strip_suffix(b"\n")
                .ok_or(CommandJournalError::Truncated)?,
        )?;
        receipt.command_sha256 = "0".repeat(64);
        let mut bad_hash = serde_json::to_vec(&receipt)?;
        bad_hash.push(b'\n');
        fs::write(&path, bad_hash)?;
        assert!(matches!(
            CommandJournal::open(&path),
            Err(CommandJournalError::Hash)
        ));

        let first: CommandReceipt = serde_json::from_slice(
            original
                .strip_suffix(b"\n")
                .ok_or(CommandJournalError::Truncated)?,
        )?;
        let fork = CommandReceipt {
            sequence: 2,
            ..first.clone()
        };
        let mut transition_fork = original;
        serde_json::to_writer(&mut transition_fork, &fork)?;
        transition_fork.push(b'\n');
        fs::write(&path, transition_fork)?;
        assert!(matches!(
            CommandJournal::open(path),
            Err(CommandJournalError::Transition)
        ));
        Ok(())
    }
}
