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
use serde::{Deserialize, Serialize};

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
    client_ids: BTreeMap<CommandId, CommandId>,
    unresolved_ids: BTreeSet<CommandId>,
    unresolved_entry_or_reduce: usize,
    unresolved_cancel_targets: BTreeMap<CommandId, usize>,
    accepted_cancel_targets: BTreeMap<CommandId, usize>,
    venue_order_client_ids: BTreeMap<String, Option<CommandId>>,
    next_sequence: u64,
}

impl CommandJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, CommandJournalError> {
        let path = path.into();
        let mut receipts: BTreeMap<CommandId, CommandReceipt> = BTreeMap::new();
        let mut client_ids: BTreeMap<CommandId, CommandId> = BTreeMap::new();
        let mut next_sequence = 1;
        for receipt in read_all(&path)? {
            if receipt.sequence != next_sequence {
                return Err(CommandJournalError::Sequence);
            }
            receipt
                .command
                .validate()
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
            receipts.insert(command_id, receipt);
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or(CommandJournalError::Sequence)?;
        }
        let mut journal = Self {
            path,
            receipts,
            client_ids,
            unresolved_ids: BTreeSet::new(),
            unresolved_entry_or_reduce: 0,
            unresolved_cancel_targets: BTreeMap::new(),
            accepted_cancel_targets: BTreeMap::new(),
            venue_order_client_ids: BTreeMap::new(),
            next_sequence,
        };
        journal.rebuild_query_indexes();
        Ok(journal)
    }

    pub fn prepare(
        &mut self,
        command: ExecutionCommand,
    ) -> Result<&CommandReceipt, CommandJournalError> {
        command.validate().map_err(CommandJournalError::Command)?;
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
            command.validate().map_err(CommandJournalError::Command)?;
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

    fn append(&self, receipt: &CommandReceipt) -> Result<(), CommandJournalError> {
        let encoded = serde_json::to_vec(receipt).map_err(CommandJournalError::Encode)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_data())
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })
    }

    fn append_batch(&self, receipts: &[CommandReceipt]) -> Result<(), CommandJournalError> {
        let mut encoded = Vec::new();
        for receipt in receipts {
            serde_json::to_writer(&mut encoded, receipt).map_err(CommandJournalError::Encode)?;
            encoded.push(b'\n');
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_data())
            .map_err(|source| CommandJournalError::Io {
                path: self.path.clone(),
                source,
            })
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

fn read_all(path: &Path) -> Result<Vec<CommandReceipt>, CommandJournalError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
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
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).map_err(CommandJournalError::Decode))
        .collect()
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
}
