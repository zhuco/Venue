use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use venue_control_protocol::{AccountDeliveryBinding, ControlAction};
use venue_domain::{CommandId, ExecutionCommand};
use venue_storage::OpaqueJournal;

const SCHEMA_VERSION: u16 = 1;
const ROTATE_BYTES: u64 = 5 * 1024 * 1024;
const HARD_LIMIT_BYTES: u64 = 10 * 1024 * 1024;

/// Durable local progress for a semantic Stop/Flatten. The command WAL remains the mutation
/// authority; this journal retains only the selected scope and command identities needed to
/// resume readback without reconstructing or replaying a different request after restart.
#[derive(Debug)]
pub(crate) struct ControlShutdownJournal {
    journal: OpaqueJournal,
    path: PathBuf,
    checkpoint_path: PathBuf,
    binding: AccountDeliveryBinding,
    segment: u64,
    next_sequence: u64,
    operation: Option<ShutdownOperation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ShutdownOperation {
    pub action: ControlAction,
    pub commands: BTreeMap<CommandId, ShutdownCommand>,
    pub phase: ShutdownPhase,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ShutdownCommand {
    pub command: ExecutionCommand,
    pub signed_private_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShutdownPhase {
    CancelOwnedOrders,
    AwaitOwnedOrdersZero,
    ReduceOwnedPosition,
    AwaitPositionZero,
    ResidualPositionCustody,
    Reconciled,
    NeedsAttention,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredRecord {
    schema_version: u16,
    event: Event,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredCheckpoint {
    schema_version: u16,
    binding: AccountDeliveryBinding,
    segment: u64,
    operation: Option<ShutdownOperation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    Root {
        binding: AccountDeliveryBinding,
        segment: u64,
    },
    Begin {
        action: ControlAction,
    },
    Command {
        command: ShutdownCommand,
    },
    Phase {
        phase: ShutdownPhase,
    },
}

impl ControlShutdownJournal {
    pub(crate) fn recover(
        path: impl Into<PathBuf>,
        binding: AccountDeliveryBinding,
    ) -> Result<Self, ControlShutdownJournalError> {
        let path = path.into();
        let checkpoint_path = checkpoint_path(&path)?;
        let mut journal =
            OpaqueJournal::open(&path).map_err(|_| ControlShutdownJournalError::Storage)?;
        let records = journal
            .recover()
            .map_err(|_| ControlShutdownJournalError::Storage)?;
        let empty_checkpoint = (records.is_empty() && checkpoint_path.exists())
            .then(|| load_latest_checkpoint(&checkpoint_path, &binding))
            .transpose()?;
        let segment = match records.first() {
            Some(record) => match parse_record(record)?.event {
                Event::Root {
                    binding: root,
                    segment,
                } if root == binding && segment > 0 => segment,
                _ => return Err(ControlShutdownJournalError::Corrupt),
            },
            None => empty_checkpoint
                .as_ref()
                .map_or(1, |checkpoint| checkpoint.segment),
        };
        let checkpoint = match empty_checkpoint {
            Some(checkpoint) => Some(checkpoint),
            None => load_checkpoint(&checkpoint_path, &binding, segment)?,
        };
        let mut recovered = Self {
            journal,
            path,
            checkpoint_path,
            binding,
            segment,
            next_sequence: 1,
            operation: checkpoint.and_then(|checkpoint| checkpoint.operation),
        };
        if records.is_empty() {
            recovered.append(Event::Root {
                binding: recovered.binding.clone(),
                segment: recovered.segment,
            })?;
            return Ok(recovered);
        }
        for record in records {
            if record.sequence != recovered.next_sequence {
                return Err(ControlShutdownJournalError::Corrupt);
            }
            let stored = parse_record(&record)?;
            if record.sequence == 1 {
                if stored.event
                    != (Event::Root {
                        binding: recovered.binding.clone(),
                        segment: recovered.segment,
                    })
                {
                    return Err(ControlShutdownJournalError::Corrupt);
                }
            } else {
                recovered.apply(stored.event)?;
            }
            recovered.next_sequence = recovered
                .next_sequence
                .checked_add(1)
                .ok_or(ControlShutdownJournalError::Storage)?;
        }
        Ok(recovered)
    }

    pub(crate) fn begin(
        &mut self,
        action: ControlAction,
    ) -> Result<(), ControlShutdownJournalError> {
        if !matches!(action, ControlAction::Stop | ControlAction::Flatten) {
            return Ok(());
        }
        match self.operation.as_ref().map(|operation| operation.action) {
            Some(existing) if existing == action || existing == ControlAction::Flatten => Ok(()),
            _ => self.append(Event::Begin { action }),
        }
    }

    #[must_use]
    pub(crate) fn operation(&self) -> Option<&ShutdownOperation> {
        self.operation.as_ref()
    }

    pub(crate) fn plan_command(
        &mut self,
        command: ExecutionCommand,
        signed_private_generation: u64,
    ) -> Result<(), ControlShutdownJournalError> {
        if signed_private_generation == 0 {
            return Err(ControlShutdownJournalError::Corrupt);
        }
        let planned = ShutdownCommand {
            command,
            signed_private_generation,
        };
        if let Some(existing) = self
            .operation
            .as_ref()
            .and_then(|operation| operation.commands.get(planned.command.command_id()))
        {
            return (existing == &planned)
                .then_some(())
                .ok_or(ControlShutdownJournalError::Conflict);
        }
        self.append(Event::Command { command: planned })
    }

    pub(crate) fn set_phase(
        &mut self,
        phase: ShutdownPhase,
    ) -> Result<(), ControlShutdownJournalError> {
        if self
            .operation
            .as_ref()
            .is_some_and(|operation| operation.phase == phase)
        {
            return Ok(());
        }
        self.append(Event::Phase { phase })
    }

    fn append(&mut self, event: Event) -> Result<(), ControlShutdownJournalError> {
        let encoded = serde_json::to_vec(&StoredRecord {
            schema_version: SCHEMA_VERSION,
            event: event.clone(),
        })
        .map_err(|_| ControlShutdownJournalError::Storage)?;
        self.rotate_if_due(encoded.len())?;
        let mut candidate = self.operation.clone();
        if !matches!(&event, Event::Root { .. }) {
            Self::validate_event_scope(&self.binding, &event)?;
            Self::apply_to(&mut candidate, event)?;
        }
        self.journal
            .append(self.next_sequence, &encoded)
            .map_err(|_| ControlShutdownJournalError::Storage)?;
        self.operation = candidate;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ControlShutdownJournalError::Storage)?;
        Ok(())
    }

    fn apply(&mut self, event: Event) -> Result<(), ControlShutdownJournalError> {
        Self::validate_event_scope(&self.binding, &event)?;
        Self::apply_to(&mut self.operation, event)
    }

    fn validate_event_scope(
        binding: &AccountDeliveryBinding,
        event: &Event,
    ) -> Result<(), ControlShutdownJournalError> {
        let Event::Command { command } = event else {
            return Ok(());
        };
        let owner = command.command.mutation_owner();
        if command.command.validate_persisted_shape().is_err()
            || command.signed_private_generation == 0
            || owner.exchange != binding.venue.as_str()
            || owner.account != binding.trading_account_id
            || owner.symbol != binding.symbol
            || owner.strategy_instance_id != binding.instance_id
        {
            return Err(ControlShutdownJournalError::Corrupt);
        }
        Ok(())
    }

    fn apply_to(
        operation: &mut Option<ShutdownOperation>,
        event: Event,
    ) -> Result<(), ControlShutdownJournalError> {
        match event {
            Event::Root { .. } => Err(ControlShutdownJournalError::Corrupt),
            Event::Begin { action }
                if matches!(action, ControlAction::Stop | ControlAction::Flatten) =>
            {
                match operation.as_mut() {
                    Some(existing)
                        if existing.action == action
                            || existing.action == ControlAction::Flatten =>
                    {
                        Ok(())
                    }
                    Some(existing) => {
                        existing.action = ControlAction::Flatten;
                        existing.phase = ShutdownPhase::CancelOwnedOrders;
                        Ok(())
                    }
                    None => {
                        *operation = Some(ShutdownOperation {
                            action,
                            commands: BTreeMap::new(),
                            phase: ShutdownPhase::CancelOwnedOrders,
                        });
                        Ok(())
                    }
                }
            }
            Event::Begin { .. } => Err(ControlShutdownJournalError::Corrupt),
            Event::Command { command } => {
                let operation = operation
                    .as_mut()
                    .ok_or(ControlShutdownJournalError::Corrupt)?;
                match operation
                    .commands
                    .insert(command.command.command_id().clone(), command.clone())
                {
                    Some(existing) if existing == command => Ok(()),
                    Some(_) => Err(ControlShutdownJournalError::Conflict),
                    None => Ok(()),
                }
            }
            Event::Phase { phase } => {
                let operation = operation
                    .as_mut()
                    .ok_or(ControlShutdownJournalError::Corrupt)?;
                operation.phase = phase;
                Ok(())
            }
        }
    }

    fn rotate_if_due(&mut self, next_record_len: usize) -> Result<(), ControlShutdownJournalError> {
        let existing = fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let estimated = estimate_opaque_append_bytes(next_record_len)?;
        if estimated > HARD_LIMIT_BYTES {
            return Err(ControlShutdownJournalError::Storage);
        }
        if existing
            .checked_add(estimated)
            .ok_or(ControlShutdownJournalError::Storage)?
            <= ROTATE_BYTES
        {
            return Ok(());
        }
        let next_segment = self
            .segment
            .checked_add(1)
            .ok_or(ControlShutdownJournalError::Storage)?;
        persist_checkpoint(
            &self.checkpoint_path,
            &StoredCheckpoint {
                schema_version: SCHEMA_VERSION,
                binding: self.binding.clone(),
                segment: next_segment,
                operation: self.operation.clone(),
            },
        )?;
        let archive = archive_path(&self.path, self.segment)?;
        fs::rename(&self.path, archive).map_err(|_| ControlShutdownJournalError::Storage)?;
        self.journal =
            OpaqueJournal::open(&self.path).map_err(|_| ControlShutdownJournalError::Storage)?;
        self.segment = next_segment;
        self.next_sequence = 1;
        self.append(Event::Root {
            binding: self.binding.clone(),
            segment: self.segment,
        })
    }
}

fn parse_record(
    record: &venue_storage::OpaqueJournalRecord,
) -> Result<StoredRecord, ControlShutdownJournalError> {
    let stored: StoredRecord = serde_json::from_slice(&record.payload)
        .map_err(|_| ControlShutdownJournalError::Corrupt)?;
    (stored.schema_version == SCHEMA_VERSION)
        .then_some(stored)
        .ok_or(ControlShutdownJournalError::Corrupt)
}

fn checkpoint_path(path: &Path) -> Result<PathBuf, ControlShutdownJournalError> {
    let parent = path.parent().ok_or(ControlShutdownJournalError::Storage)?;
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or(ControlShutdownJournalError::Storage)?;
    Ok(parent.join(format!("{stem}.checkpoint.json")))
}

fn load_checkpoint(
    path: &Path,
    binding: &AccountDeliveryBinding,
    segment: u64,
) -> Result<Option<StoredCheckpoint>, ControlShutdownJournalError> {
    if segment == 1 {
        return Ok(None);
    }
    let checkpoint = load_latest_checkpoint(path, binding)?;
    (checkpoint.segment == segment)
        .then_some(checkpoint)
        .ok_or(ControlShutdownJournalError::Corrupt)
        .map(Some)
}

fn load_latest_checkpoint(
    path: &Path,
    binding: &AccountDeliveryBinding,
) -> Result<StoredCheckpoint, ControlShutdownJournalError> {
    let encoded = fs::read(path).map_err(|_| ControlShutdownJournalError::Corrupt)?;
    let checkpoint: StoredCheckpoint =
        serde_json::from_slice(&encoded).map_err(|_| ControlShutdownJournalError::Corrupt)?;
    if checkpoint.schema_version != SCHEMA_VERSION
        || checkpoint.binding != *binding
        || checkpoint.segment < 2
    {
        return Err(ControlShutdownJournalError::Corrupt);
    }
    Ok(checkpoint)
}

fn persist_checkpoint(
    path: &Path,
    checkpoint: &StoredCheckpoint,
) -> Result<(), ControlShutdownJournalError> {
    let encoded =
        serde_json::to_vec(checkpoint).map_err(|_| ControlShutdownJournalError::Storage)?;
    if encoded.len() > ROTATE_BYTES as usize {
        return Err(ControlShutdownJournalError::Storage);
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, encoded).map_err(|_| ControlShutdownJournalError::Storage)?;
    fs::OpenOptions::new()
        .write(true)
        .open(&temporary)
        .and_then(|file| file.sync_all())
        .map_err(|_| ControlShutdownJournalError::Storage)?;
    if path.exists() {
        fs::remove_file(path).map_err(|_| ControlShutdownJournalError::Storage)?;
    }
    fs::rename(temporary, path).map_err(|_| ControlShutdownJournalError::Storage)
}

fn archive_path(path: &Path, segment: u64) -> Result<PathBuf, ControlShutdownJournalError> {
    let parent = path.parent().ok_or(ControlShutdownJournalError::Storage)?;
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or(ControlShutdownJournalError::Storage)?;
    let archive = parent.join(format!("{stem}.segment-{segment}.jsonl"));
    (!archive.exists())
        .then_some(archive)
        .ok_or(ControlShutdownJournalError::Storage)
}

fn estimate_opaque_append_bytes(payload_len: usize) -> Result<u64, ControlShutdownJournalError> {
    let payload = u64::try_from(payload_len).map_err(|_| ControlShutdownJournalError::Storage)?;
    payload
        .checked_mul(2)
        .and_then(|size| size.checked_add(1024))
        .ok_or(ControlShutdownJournalError::Storage)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ControlShutdownJournalError {
    #[error("control shutdown journal is unavailable")]
    Storage,
    #[error("control shutdown journal is corrupt")]
    Corrupt,
    #[error("control shutdown journal contains conflicting command identity")]
    Conflict,
}

#[cfg(test)]
mod tests {
    use venue_control_protocol::{GatewayMode, VenueId};
    use venue_domain::{CancelCommand, OrderOwner, OrderPurpose, Symbol};

    use super::*;

    fn binding() -> Result<AccountDeliveryBinding, Box<dyn std::error::Error>> {
        Ok(AccountDeliveryBinding {
            venue: VenueId::Bybit,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            instance_id: "grid-doge".to_owned(),
            config_epoch: 1,
        })
    }

    #[test]
    fn planned_unknown_cancel_survives_restart_without_new_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("control-shutdown.jsonl");
        let scope = binding()?;
        let command_id = CommandId::new("shutdown-cancel-a")?;
        let command = ExecutionCommand::Cancel(CancelCommand {
            command_id: command_id.clone(),
            target_client_order_id: CommandId::new("grid-order-a")?,
            owner: OrderOwner {
                strategy_instance_id: "grid-doge".to_owned(),
                run_id: "run-grid".to_owned(),
                exchange: "bybit".to_owned(),
                account: scope.trading_account_id.clone(),
                symbol: "DOGE/USDT".parse::<Symbol>()?,
                purpose: OrderPurpose::Entry,
            },
        });
        let mut journal = ControlShutdownJournal::recover(&path, scope.clone())
            .map_err(|error| format!("initial: {error}"))?;
        journal
            .begin(ControlAction::Flatten)
            .map_err(|error| format!("begin: {error}"))?;
        journal
            .plan_command(command.clone(), 7)
            .map_err(|error| format!("plan: {error}"))?;
        journal
            .set_phase(ShutdownPhase::AwaitOwnedOrdersZero)
            .map_err(|error| format!("phase: {error}"))?;
        journal.rotate_if_due((ROTATE_BYTES / 2) as usize)?;
        assert_eq!(journal.segment, 2);
        assert!(
            temporary
                .path()
                .join("control-shutdown.segment-1.jsonl")
                .exists()
        );
        drop(journal);

        let recovered = ControlShutdownJournal::recover(path, scope)
            .map_err(|error| format!("recover: {error}"))?;
        let operation = recovered.operation().ok_or("operation missing")?;
        assert_eq!(operation.action, ControlAction::Flatten);
        assert_eq!(operation.phase, ShutdownPhase::AwaitOwnedOrdersZero);
        assert_eq!(
            operation.commands.get(&command_id),
            Some(&ShutdownCommand {
                command,
                signed_private_generation: 7,
            })
        );
        Ok(())
    }

    #[test]
    fn same_command_id_with_different_generation_conflicts()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let scope = binding()?;
        let mut journal = ControlShutdownJournal::recover(
            temporary.path().join("shutdown.jsonl"),
            scope.clone(),
        )?;
        journal.begin(ControlAction::Stop)?;
        let command = ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("shutdown-cancel-b")?,
            target_client_order_id: CommandId::new("grid-order-b")?,
            owner: OrderOwner {
                strategy_instance_id: scope.instance_id.clone(),
                run_id: "run-grid".to_owned(),
                exchange: "bybit".to_owned(),
                account: scope.trading_account_id.clone(),
                symbol: scope.symbol.clone(),
                purpose: OrderPurpose::Entry,
            },
        });
        journal.plan_command(command.clone(), 3)?;
        assert_eq!(
            journal.plan_command(command, 4),
            Err(ControlShutdownJournalError::Conflict)
        );
        Ok(())
    }

    #[test]
    fn foreign_owner_and_second_root_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("shutdown.jsonl");
        let scope = binding()?;
        let mut journal = ControlShutdownJournal::recover(&path, scope.clone())?;
        journal.begin(ControlAction::Stop)?;
        let foreign = ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("shutdown-cancel-c")?,
            target_client_order_id: CommandId::new("grid-order-c")?,
            owner: OrderOwner {
                strategy_instance_id: "other".to_owned(),
                run_id: "run-other".to_owned(),
                exchange: "bybit".to_owned(),
                account: scope.trading_account_id.clone(),
                symbol: scope.symbol.clone(),
                purpose: OrderPurpose::Entry,
            },
        });
        assert_eq!(
            journal.plan_command(foreign, 1),
            Err(ControlShutdownJournalError::Corrupt)
        );
        let second_root = serde_json::to_vec(&StoredRecord {
            schema_version: SCHEMA_VERSION,
            event: Event::Root {
                binding: scope.clone(),
                segment: 1,
            },
        })?;
        journal
            .journal
            .append(journal.next_sequence, &second_root)?;
        drop(journal);
        assert!(matches!(
            ControlShutdownJournal::recover(path, scope),
            Err(ControlShutdownJournalError::Corrupt)
        ));
        Ok(())
    }

    #[test]
    fn oversized_append_is_rejected_before_a_file_can_cross_hard_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let mut journal =
            ControlShutdownJournal::recover(temporary.path().join("shutdown.jsonl"), binding()?)?;
        assert_eq!(
            journal.rotate_if_due(HARD_LIMIT_BYTES as usize),
            Err(ControlShutdownJournalError::Storage)
        );
        Ok(())
    }
}
