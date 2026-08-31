//! Durable, replay-safe Node-to-Control read-model delivery.
//!
//! This is intentionally only a projection transport. It consumes neither Runtime turns nor an
//! `AccountRuntimeHost` permit, and an HTTP acknowledgement cannot authorize an exchange write.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use venue_control_protocol::{AccountDeliveryBinding, NodeProjectionEnvelope};

use crate::{
    ControlDeliveryJournal, ControlDeliveryJournalError, ControlHttpClient, ControlHttpClientError,
};

const PROJECTION_OUTBOX_SCHEMA_VERSION: u16 = 1;
const MAX_PROJECTION_RECORD_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectionIdentity {
    binding: AccountDeliveryBinding,
    node_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRecord {
    schema_version: u16,
    event: OutboxEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum OutboxEvent {
    Root {
        binding: AccountDeliveryBinding,
        node_id: String,
    },
    Enqueued {
        envelope: Box<NodeProjectionEnvelope>,
    },
    Acknowledged {
        node_generation: u64,
        sequence: u64,
        digest: [u8; 32],
    },
}

/// A projection stays pending until Control echoes that exact envelope. The underlying journal
/// owns the filesystem lock, hash chain, crash-tail repair, and fsync boundary.
pub struct NodeProjectionOutbox<J> {
    journal: J,
    identity: ProjectionIdentity,
    next_record_sequence: u64,
    last_enqueued: Option<(u64, u64, [u8; 32])>,
    pending: BTreeMap<(u64, u64), NodeProjectionEnvelope>,
}

impl<J: ControlDeliveryJournal> NodeProjectionOutbox<J> {
    pub fn recover(
        mut journal: J,
        binding: AccountDeliveryBinding,
        node_id: impl Into<String>,
    ) -> Result<Self, NodeProjectionOutboxError> {
        binding
            .validate()
            .map_err(|_| NodeProjectionOutboxError::Identity)?;
        let identity = ProjectionIdentity {
            binding,
            node_id: node_id.into(),
        };
        if identity.node_id.trim().is_empty() || identity.node_id.len() > 128 {
            return Err(NodeProjectionOutboxError::Identity);
        }
        let records = journal.recover()?;
        let mut outbox = Self {
            journal,
            identity,
            next_record_sequence: 1,
            last_enqueued: None,
            pending: BTreeMap::new(),
        };
        for record in records {
            if record.sequence != outbox.next_record_sequence
                || record.payload.len() > MAX_PROJECTION_RECORD_BYTES
            {
                return Err(NodeProjectionOutboxError::CorruptJournal);
            }
            let persisted: PersistedRecord = serde_json::from_slice(&record.payload)
                .map_err(|_| NodeProjectionOutboxError::CorruptJournal)?;
            if persisted.schema_version != PROJECTION_OUTBOX_SCHEMA_VERSION {
                return Err(NodeProjectionOutboxError::CorruptJournal);
            }
            outbox.apply(record.sequence, persisted.event)?;
            outbox.next_record_sequence = outbox
                .next_record_sequence
                .checked_add(1)
                .ok_or(NodeProjectionOutboxError::SequenceOverflow)?;
        }
        if outbox.next_record_sequence == 1 {
            outbox.append(OutboxEvent::Root {
                binding: outbox.identity.binding.clone(),
                node_id: outbox.identity.node_id.clone(),
            })?;
        }
        Ok(outbox)
    }

    /// Fsyncs an envelope before it can be sent. Repeating the same pending envelope is safe;
    /// replacing bytes at an existing `(generation, sequence)` fails closed.
    pub fn enqueue(
        &mut self,
        envelope: NodeProjectionEnvelope,
    ) -> Result<(), NodeProjectionOutboxError> {
        self.validate_envelope(&envelope)?;
        let key = (envelope.node_generation, envelope.sequence);
        if let Some(existing) = self.pending.get(&key) {
            return if existing == &envelope {
                Ok(())
            } else {
                Err(NodeProjectionOutboxError::ReplayConflict)
            };
        }
        self.validate_next_envelope(&envelope)?;
        self.append(OutboxEvent::Enqueued {
            envelope: Box::new(envelope),
        })
    }

    /// Replays the oldest exact envelope first. A transport failure leaves its durable record
    /// pending for the next resident turn; no retry can skip a cursor gap.
    pub async fn flush(
        &mut self,
        client: &ControlHttpClient,
    ) -> Result<Vec<NodeProjectionEnvelope>, NodeProjectionOutboxError> {
        let mut acknowledged = Vec::new();
        while let Some((key, envelope)) = self
            .pending
            .first_key_value()
            .map(|(key, envelope)| (*key, envelope.clone()))
        {
            client.publish_projection(&envelope).await?;
            self.append(OutboxEvent::Acknowledged {
                node_generation: key.0,
                sequence: key.1,
                digest: envelope.digest,
            })?;
            acknowledged.push(envelope);
        }
        Ok(acknowledged)
    }

    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn oldest_pending(&self) -> Option<&NodeProjectionEnvelope> {
        self.pending.first_key_value().map(|(_, value)| value)
    }

    /// Returns the only cursor that may be used for the next durable envelope. The caller still
    /// supplies its content and digest; this outbox verifies contiguity before accepting it.
    #[must_use]
    pub fn next_cursor(&self, node_generation: u64) -> (u64, [u8; 32]) {
        match self.last_enqueued {
            Some((generation, sequence, digest)) if generation == node_generation => {
                (sequence.saturating_add(1), digest)
            }
            _ => (1, [0; 32]),
        }
    }

    fn validate_envelope(
        &self,
        envelope: &NodeProjectionEnvelope,
    ) -> Result<(), NodeProjectionOutboxError> {
        envelope
            .validate()
            .map_err(|_| NodeProjectionOutboxError::InvalidEnvelope)?;
        if envelope.binding != self.identity.binding || envelope.node_id != self.identity.node_id {
            return Err(NodeProjectionOutboxError::Identity);
        }
        Ok(())
    }

    fn validate_next_envelope(
        &self,
        envelope: &NodeProjectionEnvelope,
    ) -> Result<(), NodeProjectionOutboxError> {
        let Some((generation, sequence, digest)) = self.last_enqueued else {
            return (envelope.sequence == 1 && envelope.previous_digest == [0; 32])
                .then_some(())
                .ok_or(NodeProjectionOutboxError::Sequence);
        };
        if envelope.node_generation < generation {
            return Err(NodeProjectionOutboxError::Sequence);
        }
        if envelope.node_generation == generation {
            return (envelope.sequence == sequence.saturating_add(1)
                && envelope.previous_digest == digest)
                .then_some(())
                .ok_or(NodeProjectionOutboxError::Sequence);
        }
        (envelope.sequence == 1 && envelope.previous_digest == [0; 32])
            .then_some(())
            .ok_or(NodeProjectionOutboxError::Sequence)
    }

    fn append(&mut self, event: OutboxEvent) -> Result<(), NodeProjectionOutboxError> {
        let record = PersistedRecord {
            schema_version: PROJECTION_OUTBOX_SCHEMA_VERSION,
            event,
        };
        let payload = serde_json::to_vec(&record)?;
        if payload.len() > MAX_PROJECTION_RECORD_BYTES {
            return Err(NodeProjectionOutboxError::RecordTooLarge);
        }
        let sequence = self.journal.append(self.next_record_sequence, &payload)?;
        if sequence != self.next_record_sequence {
            return Err(NodeProjectionOutboxError::JournalSequence);
        }
        self.apply(sequence, record.event)?;
        self.next_record_sequence = self
            .next_record_sequence
            .checked_add(1)
            .ok_or(NodeProjectionOutboxError::SequenceOverflow)?;
        Ok(())
    }

    fn apply(
        &mut self,
        record_sequence: u64,
        event: OutboxEvent,
    ) -> Result<(), NodeProjectionOutboxError> {
        match event {
            OutboxEvent::Root { binding, node_id } => {
                if record_sequence != 1
                    || binding != self.identity.binding
                    || node_id != self.identity.node_id
                {
                    return Err(NodeProjectionOutboxError::CorruptJournal);
                }
            }
            OutboxEvent::Enqueued { envelope } => {
                self.validate_envelope(&envelope)
                    .map_err(|_| NodeProjectionOutboxError::CorruptJournal)?;
                self.validate_next_envelope(&envelope)
                    .map_err(|_| NodeProjectionOutboxError::CorruptJournal)?;
                let key = (envelope.node_generation, envelope.sequence);
                if self.pending.insert(key, *envelope).is_some() {
                    return Err(NodeProjectionOutboxError::CorruptJournal);
                }
                let current = self
                    .pending
                    .get(&key)
                    .ok_or(NodeProjectionOutboxError::CorruptJournal)?;
                self.last_enqueued = Some((key.0, key.1, current.digest));
            }
            OutboxEvent::Acknowledged {
                node_generation,
                sequence,
                digest,
            } => {
                let key = (node_generation, sequence);
                let Some((oldest, envelope)) = self.pending.first_key_value() else {
                    return Err(NodeProjectionOutboxError::CorruptJournal);
                };
                if *oldest != key || envelope.digest != digest {
                    return Err(NodeProjectionOutboxError::CorruptJournal);
                }
                self.pending.remove(&key);
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodeProjectionOutboxError {
    #[error("node projection outbox identity is invalid or does not match its durable root")]
    Identity,
    #[error("node projection envelope is invalid")]
    InvalidEnvelope,
    #[error("node projection cursor is non-contiguous")]
    Sequence,
    #[error("node projection replay conflicts with a durable envelope")]
    ReplayConflict,
    #[error("node projection outbox journal is corrupt")]
    CorruptJournal,
    #[error("node projection outbox record exceeds 2 MiB")]
    RecordTooLarge,
    #[error("node projection outbox record sequence is exhausted")]
    SequenceOverflow,
    #[error("node projection outbox journal returned a non-fencing sequence")]
    JournalSequence,
    #[error(transparent)]
    Journal(#[from] ControlDeliveryJournalError),
    #[error(transparent)]
    Http(#[from] ControlHttpClientError),
    #[error("node projection outbox JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rust_decimal::Decimal;
    use venue_control_protocol::{
        ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountSummary, CONTROL_SCHEMA_VERSION, ConnectionState,
        ControlSnapshot, ExecutionFactsSnapshot, GatewayMode, HealthState, NodeProjectionEnvelope,
        StrategyKind, StrategyLifecycle, StrategySummary, VenueId,
    };

    use super::*;
    use crate::ControlDeliveryJournalRecord;

    #[derive(Clone, Default)]
    struct MemoryJournal(Arc<Mutex<Vec<ControlDeliveryJournalRecord>>>);

    impl ControlDeliveryJournal for MemoryJournal {
        fn recover(
            &mut self,
        ) -> Result<Vec<ControlDeliveryJournalRecord>, ControlDeliveryJournalError> {
            self.0
                .lock()
                .map(|records| records.clone())
                .map_err(|_| ControlDeliveryJournalError::Unavailable)
        }

        fn append(
            &mut self,
            expected_sequence: u64,
            payload: &[u8],
        ) -> Result<u64, ControlDeliveryJournalError> {
            let mut records = self
                .0
                .lock()
                .map_err(|_| ControlDeliveryJournalError::Unavailable)?;
            if expected_sequence != records.len() as u64 + 1 {
                return Err(ControlDeliveryJournalError::SequenceConflict);
            }
            records.push(ControlDeliveryJournalRecord {
                sequence: expected_sequence,
                payload: payload.to_vec(),
            });
            Ok(expected_sequence)
        }
    }

    #[test]
    fn outbox_persists_replay_before_any_transport_and_rejects_cursor_gaps()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = MemoryJournal::default();
        let binding = binding()?;
        let mut outbox = NodeProjectionOutbox::recover(journal.clone(), binding.clone(), "node-a")?;
        let first = projection(binding.clone(), 1, 1, [7; 32], [0; 32], 100)?;
        outbox.enqueue(first.clone())?;
        assert_eq!(outbox.pending_len(), 1);
        let recovered = NodeProjectionOutbox::recover(journal, binding.clone(), "node-a")?;
        assert_eq!(recovered.pending_len(), 1);
        assert_eq!(recovered.oldest_pending(), Some(&first));

        let mut gap = projection(binding, 1, 3, [8; 32], [7; 32], 101)?;
        assert!(matches!(
            outbox.enqueue(gap.clone()),
            Err(NodeProjectionOutboxError::Sequence)
        ));
        gap.sequence = 2;
        gap.previous_digest = [6; 32];
        assert!(matches!(
            outbox.enqueue(gap),
            Err(NodeProjectionOutboxError::Sequence)
        ));
        Ok(())
    }

    fn binding() -> Result<AccountDeliveryBinding, Box<dyn std::error::Error>> {
        Ok(AccountDeliveryBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            instance_id: "grid-btc".to_owned(),
            config_epoch: 1,
        })
    }

    fn projection(
        binding: AccountDeliveryBinding,
        generation: u64,
        sequence: u64,
        digest: [u8; 32],
        previous_digest: [u8; 32],
        generated_ms: u64,
    ) -> Result<NodeProjectionEnvelope, Box<dyn std::error::Error>> {
        Ok(NodeProjectionEnvelope {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            binding: binding.clone(),
            node_id: "node-a".to_owned(),
            node_generation: generation,
            sequence,
            previous_digest,
            digest,
            copy_execution_evidence: Vec::new(),
            copy_planning_facts: Vec::new(),
            snapshot: ControlSnapshot {
                schema_version: CONTROL_SCHEMA_VERSION,
                generated_ms,
                connection: ConnectionState::Live,
                accounts: vec![AccountSummary {
                    venue: binding.venue,
                    mode: binding.mode,
                    trading_account_id: binding.trading_account_id.clone(),
                    health: HealthState::Healthy,
                    equity: Some(Decimal::ONE),
                    available_margin: Some(Decimal::ONE),
                    unrealized_pnl: Some(Decimal::ZERO),
                    balances: Vec::new(),
                    private_generation: 1,
                    writer_generation: 1,
                    last_reconciled_ms: generated_ms - 1,
                }],
                strategies: vec![StrategySummary {
                    instance_id: binding.instance_id.clone(),
                    kind: StrategyKind::Grid,
                    venue: binding.venue,
                    mode: binding.mode,
                    trading_account_id: binding.trading_account_id.clone(),
                    symbol: binding.symbol.clone(),
                    lifecycle: StrategyLifecycle::Running,
                    config_epoch: binding.config_epoch,
                    open_orders: 0,
                    long_quantity: Decimal::ZERO,
                    short_quantity: Decimal::ZERO,
                    realized_pnl: Some(Decimal::ZERO),
                    unrealized_pnl: Some(Decimal::ZERO),
                    last_receipt_ms: generated_ms - 1,
                    attention: None,
                }],
                copy_relations: Vec::new(),
                markets: Vec::new(),
                ledger: Vec::new(),
            },
            facts: ExecutionFactsSnapshot {
                schema_version: CONTROL_SCHEMA_VERSION,
                generated_ms,
                orders: Vec::new(),
                positions: Vec::new(),
                fills: Vec::new(),
                reconciliation: Vec::new(),
                copy_ledger: Vec::new(),
                drift: Vec::new(),
                execution: Vec::new(),
                risk: Vec::new(),
                health: Vec::new(),
            },
        })
    }
}
