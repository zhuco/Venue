use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, CommandReceipt, CommandState as ControlCommandState, ControlAction,
    ControlCommandRequest,
};
use venue_gateway_api::GatewayBinding;
use venue_runtime::StrategyBinding;

use crate::CanaryEvidence;

const JOURNAL_SCHEMA: u16 = 1;
const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanaryControlRequest {
    pub request_id: String,
    pub evidence: CanaryEvidence,
}

impl CanaryControlRequest {
    pub(crate) fn validate(&self) -> Result<(), SupervisionError> {
        if self.request_id.trim().is_empty() || self.request_id.len() > 128 {
            return Err(SupervisionError::RequestIdentity);
        }
        Ok(())
    }
}

/// Linear turn issued only after the exact control request is durable in the node journal.
pub struct ActorControlTurn {
    request: ControlCommandRequest,
    connection_generation: u64,
    private_generation: u64,
    turn_sequence: u64,
}

impl ActorControlTurn {
    #[must_use]
    pub const fn request(&self) -> &ControlCommandRequest {
        &self.request
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
    pub const fn turn_sequence(&self) -> u64 {
        self.turn_sequence
    }

    /// The actor calls this only after its inbox/checkpoint transaction is durable.
    pub fn persisted(
        self,
        durable_sequence: u64,
        applied_sha256: impl Into<String>,
        observed_ms: u64,
    ) -> Result<ActorAppliedControlReceipt, SupervisionError> {
        let applied_sha256 = applied_sha256.into();
        validate_digest(&applied_sha256)?;
        if durable_sequence == 0 || observed_ms == 0 {
            return Err(SupervisionError::ActorReceipt);
        }
        Ok(ActorAppliedControlReceipt {
            request: self.request,
            connection_generation: self.connection_generation,
            private_generation: self.private_generation,
            turn_sequence: self.turn_sequence,
            durable_sequence,
            applied_sha256,
            observed_ms,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorAppliedControlReceipt {
    request: ControlCommandRequest,
    connection_generation: u64,
    private_generation: u64,
    turn_sequence: u64,
    durable_sequence: u64,
    applied_sha256: String,
    observed_ms: u64,
}

impl ActorAppliedControlReceipt {
    #[must_use]
    pub const fn request(&self) -> &ControlCommandRequest {
        &self.request
    }

    #[must_use]
    pub fn applied_sha256(&self) -> &str {
        &self.applied_sha256
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }
}

/// Linear turn for installing command-bound Canary evidence after actor persistence.
pub struct ActorCanaryTurn {
    request: CanaryControlRequest,
    connection_generation: u64,
    private_generation: u64,
    turn_sequence: u64,
}

impl ActorCanaryTurn {
    #[must_use]
    pub const fn request(&self) -> &CanaryControlRequest {
        &self.request
    }

    pub fn persisted(
        self,
        durable_sequence: u64,
        applied_sha256: impl Into<String>,
        observed_ms: u64,
    ) -> Result<ActorAppliedCanaryReceipt, SupervisionError> {
        let applied_sha256 = applied_sha256.into();
        validate_digest(&applied_sha256)?;
        if durable_sequence == 0 || observed_ms == 0 {
            return Err(SupervisionError::ActorReceipt);
        }
        Ok(ActorAppliedCanaryReceipt {
            request: self.request,
            connection_generation: self.connection_generation,
            private_generation: self.private_generation,
            turn_sequence: self.turn_sequence,
            durable_sequence,
            applied_sha256,
            observed_ms,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorAppliedCanaryReceipt {
    request: CanaryControlRequest,
    connection_generation: u64,
    private_generation: u64,
    turn_sequence: u64,
    durable_sequence: u64,
    applied_sha256: String,
    observed_ms: u64,
}

impl ActorAppliedCanaryReceipt {
    #[must_use]
    pub const fn request(&self) -> &CanaryControlRequest {
        &self.request
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PersistedControlCompletion {
    pub request_id: String,
    pub action: ControlAction,
    pub connection_generation: u64,
    pub private_generation: u64,
    pub symbol_custody_retained: bool,
    pub readback_sha256: String,
    pub observed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RestoredLifecycle {
    Active,
    Paused,
    AwaitingActor(ControlCommandRequest),
    Stopping {
        request_id: String,
        action: ControlAction,
        after_connection_generation: u64,
        after_private_generation: u64,
    },
    StoppedWithCustody,
    StoppedFlat,
}

#[derive(Clone, Debug)]
pub(crate) struct SupervisionProjection {
    pub lifecycle: RestoredLifecycle,
    pub canary: Option<CanaryEvidence>,
    pub pending_canary: Option<CanaryControlRequest>,
    pub connection_generation_floor: u64,
    pub private_generation_floor: u64,
    pub next_turn_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SupervisionEvent {
    Root {
        binding: GatewayBinding,
        owner: StrategyBinding,
        config_epoch: u64,
    },
    ControlAccepted {
        request: ControlCommandRequest,
        connection_generation: u64,
        private_generation: u64,
        turn_sequence: u64,
        observed_ms: u64,
    },
    ControlActorApplied {
        receipt: ActorAppliedControlReceipt,
        control_receipt: CommandReceipt,
    },
    ControlCompleted {
        completion: PersistedControlCompletion,
        control_receipt: CommandReceipt,
    },
    CanaryAccepted {
        request: CanaryControlRequest,
        connection_generation: u64,
        private_generation: u64,
        turn_sequence: u64,
        observed_ms: u64,
    },
    CanaryActorApplied {
        receipt: ActorAppliedCanaryReceipt,
        control_receipt: CommandReceipt,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JournalRecord {
    schema: u16,
    sequence: u64,
    previous_sha256: String,
    event: SupervisionEvent,
    record_sha256: String,
}

#[derive(Serialize)]
struct RecordCommitment<'a> {
    schema: u16,
    sequence: u64,
    previous_sha256: &'a str,
    event: &'a SupervisionEvent,
}

pub(crate) struct SupervisionJournal {
    path: PathBuf,
    file: File,
    durable_len: u64,
    durable_sha256: String,
    next_sequence: u64,
    tail_sha256: String,
    binding: GatewayBinding,
    owner: StrategyBinding,
    config_epoch: u64,
    events: Vec<SupervisionEvent>,
}

impl SupervisionJournal {
    pub fn open(
        path: PathBuf,
        binding: GatewayBinding,
        owner: StrategyBinding,
        config_epoch: u64,
    ) -> Result<Self, SupervisionError> {
        if config_epoch == 0 {
            return Err(SupervisionError::ConfigEpoch);
        }
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|source| io_error(&path, source))?;
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            let complete_len = bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |index| index + 1);
            file.set_len(complete_len as u64)
                .map_err(|source| io_error(&path, source))?;
            file.sync_all().map_err(|source| io_error(&path, source))?;
            bytes.truncate(complete_len);
        }
        let mut events = Vec::new();
        let mut expected_sequence = 1_u64;
        let mut tail_sha256 = zero_digest();
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            if line.len() > MAX_RECORD_BYTES {
                return Err(SupervisionError::RecordTooLarge);
            }
            let record: JournalRecord =
                serde_json::from_slice(line).map_err(|_| SupervisionError::CorruptJournal)?;
            if record.schema != JOURNAL_SCHEMA
                || record.sequence != expected_sequence
                || record.previous_sha256 != tail_sha256
                || record.record_sha256 != record_digest(&record)?
            {
                return Err(SupervisionError::CorruptJournal);
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or(SupervisionError::SequenceOverflow)?;
            tail_sha256 = record.record_sha256;
            events.push(record.event);
        }
        let mut journal = Self {
            path,
            file,
            durable_len: bytes.len() as u64,
            durable_sha256: hex_digest(Sha256::digest(&bytes)),
            next_sequence: expected_sequence,
            tail_sha256,
            binding,
            owner,
            config_epoch,
            events,
        };
        if journal.events.is_empty() {
            journal.append(SupervisionEvent::Root {
                binding: journal.binding.clone(),
                owner: journal.owner.clone(),
                config_epoch,
            })?;
            if !existed {
                sync_parent(journal.path.parent())?;
            }
        }
        journal.validate_root()?;
        let _ = journal.projection()?;
        Ok(journal)
    }

    pub fn projection(&self) -> Result<SupervisionProjection, SupervisionError> {
        let mut lifecycle = RestoredLifecycle::Active;
        let mut pending_control: Option<(ControlCommandRequest, u64, u64, u64)> = None;
        let mut pending_canary: Option<(CanaryControlRequest, u64, u64, u64)> = None;
        let mut canary = None;
        let mut connection_floor = 0_u64;
        let mut private_floor = 0_u64;
        let mut max_turn = 0_u64;
        for event in self.events.iter().skip(1) {
            match event {
                SupervisionEvent::Root { .. } => return Err(SupervisionError::CorruptJournal),
                SupervisionEvent::ControlAccepted {
                    request,
                    connection_generation,
                    private_generation,
                    turn_sequence,
                    ..
                } => {
                    if pending_control.is_some() {
                        return Err(SupervisionError::ControlBusy);
                    }
                    validate_request_scope(request, &self.binding, &self.owner, self.config_epoch)?;
                    pending_control = Some((
                        request.clone(),
                        *connection_generation,
                        *private_generation,
                        *turn_sequence,
                    ));
                    lifecycle = RestoredLifecycle::AwaitingActor(request.clone());
                    max_turn = max_turn.max(*turn_sequence);
                }
                SupervisionEvent::ControlActorApplied {
                    receipt,
                    control_receipt,
                } => {
                    let Some((request, connection, private, turn)) = pending_control.take() else {
                        return Err(SupervisionError::ActorReceipt);
                    };
                    validate_actor_control(receipt, &request, connection, private, turn)?;
                    control_receipt
                        .validate()
                        .map_err(|_| SupervisionError::ActorReceipt)?;
                    let expected_state =
                        if matches!(request.action, ControlAction::Pause | ControlAction::Resume) {
                            ControlCommandState::Applied
                        } else {
                            ControlCommandState::Accepted
                        };
                    if control_receipt.request_id != request.request_id
                        || control_receipt.state != expected_state
                    {
                        return Err(SupervisionError::ActorReceipt);
                    }
                    connection_floor = connection_floor.max(receipt.connection_generation);
                    private_floor = private_floor.max(receipt.private_generation);
                    lifecycle = match request.action {
                        ControlAction::Pause => RestoredLifecycle::Paused,
                        ControlAction::Resume => RestoredLifecycle::Active,
                        ControlAction::Stop | ControlAction::Flatten => {
                            RestoredLifecycle::Stopping {
                                request_id: request.request_id,
                                action: request.action,
                                after_connection_generation: receipt.connection_generation,
                                after_private_generation: receipt.private_generation,
                            }
                        }
                    };
                }
                SupervisionEvent::ControlCompleted {
                    completion,
                    control_receipt,
                } => {
                    let RestoredLifecycle::Stopping {
                        request_id, action, ..
                    } = &lifecycle
                    else {
                        return Err(SupervisionError::ControlCompletion);
                    };
                    if completion.request_id != *request_id || completion.action != *action {
                        return Err(SupervisionError::ControlCompletion);
                    }
                    control_receipt
                        .validate()
                        .map_err(|_| SupervisionError::ControlCompletion)?;
                    if control_receipt.request_id != completion.request_id
                        || control_receipt.state != ControlCommandState::Applied
                    {
                        return Err(SupervisionError::ControlCompletion);
                    }
                    connection_floor = connection_floor.max(completion.connection_generation);
                    private_floor = private_floor.max(completion.private_generation);
                    lifecycle = if completion.symbol_custody_retained {
                        RestoredLifecycle::StoppedWithCustody
                    } else {
                        RestoredLifecycle::StoppedFlat
                    };
                }
                SupervisionEvent::CanaryAccepted {
                    request,
                    connection_generation,
                    private_generation,
                    turn_sequence,
                    ..
                } => {
                    if pending_canary.is_some() {
                        return Err(SupervisionError::ControlBusy);
                    }
                    request.validate()?;
                    validate_canary_scope(request, &self.binding, &self.owner)?;
                    pending_canary = Some((
                        request.clone(),
                        *connection_generation,
                        *private_generation,
                        *turn_sequence,
                    ));
                    max_turn = max_turn.max(*turn_sequence);
                }
                SupervisionEvent::CanaryActorApplied {
                    receipt,
                    control_receipt,
                } => {
                    let Some((request, connection, private, turn)) = pending_canary.take() else {
                        return Err(SupervisionError::ActorReceipt);
                    };
                    validate_actor_canary(receipt, &request, connection, private, turn)?;
                    control_receipt
                        .validate()
                        .map_err(|_| SupervisionError::ActorReceipt)?;
                    if control_receipt.request_id != request.request_id
                        || control_receipt.state != ControlCommandState::Applied
                    {
                        return Err(SupervisionError::ActorReceipt);
                    }
                    connection_floor = connection_floor.max(receipt.connection_generation);
                    private_floor = private_floor.max(receipt.private_generation);
                    canary = Some(request.evidence);
                }
            }
        }
        if let Some((request, ..)) = pending_control {
            lifecycle = RestoredLifecycle::AwaitingActor(request);
        }
        Ok(SupervisionProjection {
            lifecycle,
            canary,
            pending_canary: pending_canary.map(|pending| pending.0),
            connection_generation_floor: connection_floor,
            private_generation_floor: private_floor,
            next_turn_sequence: max_turn
                .checked_add(1)
                .ok_or(SupervisionError::SequenceOverflow)?,
        })
    }

    pub fn accept_control(
        &mut self,
        request: ControlCommandRequest,
        connection_generation: u64,
        private_generation: u64,
        observed_ms: u64,
    ) -> Result<ActorControlTurn, SupervisionError> {
        validate_request_scope(&request, &self.binding, &self.owner, self.config_epoch)?;
        self.reject_reused_request_id(&request.request_id)?;
        let projection = self.projection()?;
        if matches!(projection.lifecycle, RestoredLifecycle::AwaitingActor(_))
            || projection.pending_canary.is_some()
        {
            return Err(SupervisionError::ControlBusy);
        }
        validate_lifecycle_action(&projection.lifecycle, request.action)?;
        let turn_sequence = projection.next_turn_sequence;
        self.append(SupervisionEvent::ControlAccepted {
            request: request.clone(),
            connection_generation,
            private_generation,
            turn_sequence,
            observed_ms,
        })?;
        Ok(ActorControlTurn {
            request,
            connection_generation,
            private_generation,
            turn_sequence,
        })
    }

    pub fn recovered_control_turn(&self) -> Result<Option<ActorControlTurn>, SupervisionError> {
        let mut accepted = None;
        for event in self.events.iter().rev() {
            match event {
                SupervisionEvent::ControlActorApplied { .. } => return Ok(None),
                SupervisionEvent::ControlAccepted {
                    request,
                    connection_generation,
                    private_generation,
                    turn_sequence,
                    ..
                } => {
                    accepted = Some(ActorControlTurn {
                        request: request.clone(),
                        connection_generation: *connection_generation,
                        private_generation: *private_generation,
                        turn_sequence: *turn_sequence,
                    });
                    break;
                }
                _ => {}
            }
        }
        Ok(accepted)
    }

    pub fn apply_control(
        &mut self,
        receipt: ActorAppliedControlReceipt,
    ) -> Result<CommandReceipt, SupervisionError> {
        let turn = self
            .recovered_control_turn()?
            .ok_or(SupervisionError::ActorReceipt)?;
        validate_actor_control(
            &receipt,
            &turn.request,
            turn.connection_generation,
            turn.private_generation,
            turn.turn_sequence,
        )?;
        let terminal = matches!(
            receipt.request.action,
            ControlAction::Pause | ControlAction::Resume
        );
        let control_receipt = command_receipt(
            &receipt.request.request_id,
            if terminal {
                ControlCommandState::Applied
            } else {
                ControlCommandState::Accepted
            },
            receipt.observed_ms,
            receipt.turn_sequence,
            if terminal {
                "actor_applied"
            } else {
                "actor_applied_signed_completion_pending"
            },
        );
        self.append(SupervisionEvent::ControlActorApplied {
            receipt,
            control_receipt: control_receipt.clone(),
        })?;
        Ok(control_receipt)
    }

    pub fn complete_control(
        &mut self,
        completion: PersistedControlCompletion,
    ) -> Result<CommandReceipt, SupervisionError> {
        validate_digest(&completion.readback_sha256)?;
        let receipt = command_receipt(
            &completion.request_id,
            ControlCommandState::Applied,
            completion.observed_ms,
            self.next_sequence,
            if completion.symbol_custody_retained {
                "stopped_with_symbol_custody"
            } else {
                "stopped_flat"
            },
        );
        self.append(SupervisionEvent::ControlCompleted {
            completion,
            control_receipt: receipt.clone(),
        })?;
        Ok(receipt)
    }

    pub fn accept_canary(
        &mut self,
        request: CanaryControlRequest,
        connection_generation: u64,
        private_generation: u64,
        observed_ms: u64,
    ) -> Result<ActorCanaryTurn, SupervisionError> {
        request.validate()?;
        validate_canary_scope(&request, &self.binding, &self.owner)?;
        self.reject_reused_request_id(&request.request_id)?;
        let projection = self.projection()?;
        if matches!(projection.lifecycle, RestoredLifecycle::AwaitingActor(_))
            || projection.pending_canary.is_some()
            || !matches!(
                projection.lifecycle,
                RestoredLifecycle::Active | RestoredLifecycle::Paused
            )
        {
            return Err(SupervisionError::ControlBusy);
        }
        let turn_sequence = projection.next_turn_sequence;
        self.append(SupervisionEvent::CanaryAccepted {
            request: request.clone(),
            connection_generation,
            private_generation,
            turn_sequence,
            observed_ms,
        })?;
        Ok(ActorCanaryTurn {
            request,
            connection_generation,
            private_generation,
            turn_sequence,
        })
    }

    pub fn apply_canary(
        &mut self,
        receipt: ActorAppliedCanaryReceipt,
    ) -> Result<CommandReceipt, SupervisionError> {
        let projection = self.projection()?;
        let request = projection
            .pending_canary
            .ok_or(SupervisionError::ActorReceipt)?;
        let accepted = self.events.iter().rev().find_map(|event| match event {
            SupervisionEvent::CanaryAccepted {
                request: candidate,
                connection_generation,
                private_generation,
                turn_sequence,
                ..
            } if candidate == &request => {
                Some((*connection_generation, *private_generation, *turn_sequence))
            }
            _ => None,
        });
        let (connection, private, turn) = accepted.ok_or(SupervisionError::ActorReceipt)?;
        validate_actor_canary(&receipt, &request, connection, private, turn)?;
        let control_receipt = command_receipt(
            &receipt.request.request_id,
            ControlCommandState::Applied,
            receipt.observed_ms,
            receipt.turn_sequence,
            "command_bound_canary_applied",
        );
        self.append(SupervisionEvent::CanaryActorApplied {
            receipt,
            control_receipt: control_receipt.clone(),
        })?;
        Ok(control_receipt)
    }

    pub fn recovered_canary_turn(&self) -> Result<Option<ActorCanaryTurn>, SupervisionError> {
        for event in self.events.iter().rev() {
            match event {
                SupervisionEvent::CanaryActorApplied { .. } => return Ok(None),
                SupervisionEvent::CanaryAccepted {
                    request,
                    connection_generation,
                    private_generation,
                    turn_sequence,
                    ..
                } => {
                    return Ok(Some(ActorCanaryTurn {
                        request: request.clone(),
                        connection_generation: *connection_generation,
                        private_generation: *private_generation,
                        turn_sequence: *turn_sequence,
                    }));
                }
                _ => {}
            }
        }
        Ok(None)
    }

    fn validate_root(&self) -> Result<(), SupervisionError> {
        match self.events.first() {
            Some(SupervisionEvent::Root {
                binding,
                owner,
                config_epoch,
            }) if binding == &self.binding
                && owner == &self.owner
                && *config_epoch == self.config_epoch =>
            {
                Ok(())
            }
            _ => Err(SupervisionError::RootScope),
        }
    }

    fn reject_reused_request_id(&self, request_id: &str) -> Result<(), SupervisionError> {
        let reused = self.events.iter().any(|event| match event {
            SupervisionEvent::ControlAccepted { request, .. } => request.request_id == request_id,
            SupervisionEvent::CanaryAccepted { request, .. } => request.request_id == request_id,
            _ => false,
        });
        if reused {
            Err(SupervisionError::DuplicateRequest)
        } else {
            Ok(())
        }
    }

    fn append(&mut self, event: SupervisionEvent) -> Result<(), SupervisionError> {
        let disk_len = self
            .file
            .metadata()
            .map_err(|source| io_error(&self.path, source))?
            .len();
        if disk_len != self.durable_len {
            return Err(SupervisionError::ExternalAdvance);
        }
        let mut durable_bytes = Vec::new();
        self.file
            .seek(SeekFrom::Start(0))
            .and_then(|_| self.file.read_to_end(&mut durable_bytes))
            .map_err(|source| io_error(&self.path, source))?;
        if durable_bytes.len() as u64 != self.durable_len
            || hex_digest(Sha256::digest(&durable_bytes)) != self.durable_sha256
        {
            return Err(SupervisionError::ExternalAdvance);
        }
        let mut record = JournalRecord {
            schema: JOURNAL_SCHEMA,
            sequence: self.next_sequence,
            previous_sha256: self.tail_sha256.clone(),
            event: event.clone(),
            record_sha256: String::new(),
        };
        record.record_sha256 = record_digest(&record)?;
        let mut encoded = serde_json::to_vec(&record).map_err(|_| SupervisionError::Encoding)?;
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(SupervisionError::RecordTooLarge);
        }
        encoded.push(b'\n');
        self.file
            .seek(SeekFrom::End(0))
            .and_then(|_| self.file.write_all(&encoded))
            .and_then(|_| self.file.sync_all())
            .map_err(|source| io_error(&self.path, source))?;
        self.durable_len = self
            .durable_len
            .checked_add(encoded.len() as u64)
            .ok_or(SupervisionError::SequenceOverflow)?;
        durable_bytes.extend_from_slice(&encoded);
        self.durable_sha256 = hex_digest(Sha256::digest(&durable_bytes));
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(SupervisionError::SequenceOverflow)?;
        self.tail_sha256 = record.record_sha256;
        self.events.push(event);
        Ok(())
    }
}

fn validate_request_scope(
    request: &ControlCommandRequest,
    binding: &GatewayBinding,
    owner: &StrategyBinding,
    config_epoch: u64,
) -> Result<(), SupervisionError> {
    request
        .validate()
        .map_err(|_| SupervisionError::RequestScope)?;
    if request.venue != binding.venue
        || request.mode != binding.mode
        || request.trading_account_id != binding.trading_account_id
        || request.symbol != binding.symbol
        || request.instance_id != owner.key.instance_id
        || request.expected_config_epoch != config_epoch
    {
        return Err(SupervisionError::RequestScope);
    }
    Ok(())
}

fn validate_canary_scope(
    request: &CanaryControlRequest,
    binding: &GatewayBinding,
    owner: &StrategyBinding,
) -> Result<(), SupervisionError> {
    if request.evidence.binding() != binding
        || request.evidence.strategy_instance_id() != owner.key.instance_id
        || request.evidence.run_id() != owner.run_id
        || request.evidence.config_digest() != owner.config_digest
    {
        return Err(SupervisionError::RequestScope);
    }
    Ok(())
}

fn validate_lifecycle_action(
    lifecycle: &RestoredLifecycle,
    action: ControlAction,
) -> Result<(), SupervisionError> {
    let valid = matches!(
        (lifecycle, action),
        (
            RestoredLifecycle::Active,
            ControlAction::Pause | ControlAction::Stop | ControlAction::Flatten
        ) | (
            RestoredLifecycle::Paused,
            ControlAction::Resume | ControlAction::Stop | ControlAction::Flatten
        )
    );
    if valid {
        Ok(())
    } else {
        Err(SupervisionError::Lifecycle)
    }
}

fn validate_actor_control(
    receipt: &ActorAppliedControlReceipt,
    request: &ControlCommandRequest,
    connection_generation: u64,
    private_generation: u64,
    turn_sequence: u64,
) -> Result<(), SupervisionError> {
    if &receipt.request != request
        || receipt.connection_generation != connection_generation
        || receipt.private_generation != private_generation
        || receipt.turn_sequence != turn_sequence
        || receipt.durable_sequence == 0
        || receipt.observed_ms == 0
    {
        return Err(SupervisionError::ActorReceipt);
    }
    validate_digest(&receipt.applied_sha256)
}

fn validate_actor_canary(
    receipt: &ActorAppliedCanaryReceipt,
    request: &CanaryControlRequest,
    connection_generation: u64,
    private_generation: u64,
    turn_sequence: u64,
) -> Result<(), SupervisionError> {
    if &receipt.request != request
        || receipt.connection_generation != connection_generation
        || receipt.private_generation != private_generation
        || receipt.turn_sequence != turn_sequence
        || receipt.durable_sequence == 0
        || receipt.observed_ms == 0
    {
        return Err(SupervisionError::ActorReceipt);
    }
    validate_digest(&receipt.applied_sha256)
}

fn command_receipt(
    request_id: &str,
    state: ControlCommandState,
    observed_ms: u64,
    sequence: u64,
    detail: &str,
) -> CommandReceipt {
    CommandReceipt {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: request_id.to_owned(),
        state,
        receipt_id: format!("venue-node-{sequence}-{request_id}"),
        observed_ms,
        detail: detail.to_owned(),
    }
}

fn record_digest(record: &JournalRecord) -> Result<String, SupervisionError> {
    let commitment = RecordCommitment {
        schema: record.schema,
        sequence: record.sequence,
        previous_sha256: &record.previous_sha256,
        event: &record.event,
    };
    let encoded = serde_json::to_vec(&commitment).map_err(|_| SupervisionError::Encoding)?;
    Ok(hex_digest(Sha256::digest(encoded)))
}

fn validate_digest(value: &str) -> Result<(), SupervisionError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(SupervisionError::ActorReceipt)
    }
}

fn zero_digest() -> String {
    "0".repeat(64)
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sync_parent(parent: Option<&Path>) -> Result<(), SupervisionError> {
    #[cfg(unix)]
    if let Some(parent) = parent {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| io_error(parent, source))?;
    }
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

fn io_error(path: &Path, source: std::io::Error) -> SupervisionError {
    SupervisionError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisionError {
    #[error("supervision journal I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("supervision journal contains a corrupt complete record")]
    CorruptJournal,
    #[error("supervision journal root does not match binding, Owner, or config epoch")]
    RootScope,
    #[error("control request scope, confirmation, or generation is invalid")]
    RequestScope,
    #[error("control request identity is invalid")]
    RequestIdentity,
    #[error("control request identity was already durably consumed")]
    DuplicateRequest,
    #[error("control action is invalid for the durable lifecycle")]
    Lifecycle,
    #[error("another durable control turn is awaiting Actor application")]
    ControlBusy,
    #[error("Actor-applied receipt does not match the durable control turn")]
    ActorReceipt,
    #[error("Stop/Flatten completion does not match the durable stopping fence")]
    ControlCompletion,
    #[error("configuration epoch must be positive")]
    ConfigEpoch,
    #[error("supervision journal record is too large")]
    RecordTooLarge,
    #[error("supervision journal was externally advanced")]
    ExternalAdvance,
    #[error("supervision journal sequence overflow")]
    SequenceOverflow,
    #[error("supervision journal encoding failed")]
    Encoding,
}
