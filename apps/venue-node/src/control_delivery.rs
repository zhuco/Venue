use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryAck, AccountDeliveryBinding,
    AccountDeliveryClaim, AccountDeliveryLease, AccountDeliveryPayload, AccountDeliveryPurpose,
    AccountDeliveryReceipt, AccountDeliveryReceiptState,
};

const EVENT_SCHEMA_VERSION: u16 = 1;
const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;
const MAX_DETAIL_BYTES: usize = 4 * 1024;

macro_rules! no_authority_methods {
    () => {
        #[must_use]
        pub const fn grants_gateway_capability(&self) -> bool {
            false
        }

        #[must_use]
        pub const fn grants_writer_lease(&self) -> bool {
            false
        }

        #[must_use]
        pub const fn grants_wal_authority(&self) -> bool {
            false
        }

        #[must_use]
        pub const fn grants_dispatch_permit(&self) -> bool {
            false
        }
    };
}

/// Opaque bytes recovered by the shared durable journal implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlDeliveryJournalRecord {
    pub sequence: u64,
    pub payload: Vec<u8>,
}

/// Minimal adapter required from the shared journal layer.
///
/// Implementations own file locking, hash chaining, crash-tail repair, fsync, and corruption
/// detection. `append` may return success only after `payload` is durable at exactly
/// `expected_sequence`; this module deliberately does not duplicate those algorithms.
pub trait ControlDeliveryJournal {
    fn recover(&mut self)
    -> Result<Vec<ControlDeliveryJournalRecord>, ControlDeliveryJournalError>;

    fn append(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
    ) -> Result<u64, ControlDeliveryJournalError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ControlDeliveryJournalError {
    #[error("shared durable journal is unavailable")]
    Unavailable,
    #[error("shared durable journal sequence was fenced by another writer")]
    SequenceConflict,
    #[error("shared durable journal is corrupt")]
    Corrupt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableStoreResult {
    Stored,
    Existing,
}

/// A transport-neutral ACK or receipt outbox item already persisted by the shared journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableDeliveryOutput<T> {
    value: T,
    store_result: DurableStoreResult,
    durable_sequence: u64,
}

impl<T> DurableDeliveryOutput<T> {
    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    #[must_use]
    pub const fn store_result(&self) -> DurableStoreResult {
        self.store_result
    }

    #[must_use]
    pub const fn durable_sequence(&self) -> u64 {
        self.durable_sequence
    }

    no_authority_methods!();
}

#[derive(Debug)]
pub enum ClaimAcceptance {
    Install(DurableDeliveryOutput<AccountDeliveryAck>),
    Reconcile(ReconciliationTurn),
}

/// Semantic actor input issued only after the exact ACK was accepted by Control and that
/// confirmation was itself journaled. It is not an execution request.
#[derive(Debug)]
pub struct ActorDeliveryTurn {
    claim: AccountDeliveryClaim,
    durable_inbox_digest: [u8; 32],
}

impl ActorDeliveryTurn {
    #[must_use]
    pub const fn lease(&self) -> &AccountDeliveryLease {
        &self.claim.lease
    }

    #[must_use]
    pub const fn payload(&self) -> &AccountDeliveryPayload {
        &self.claim.payload
    }

    #[must_use]
    pub const fn durable_inbox_digest(&self) -> [u8; 32] {
        self.durable_inbox_digest
    }

    pub fn applied(
        self,
        observed_ms: u64,
        account_fact_digest: [u8; 32],
        detail: impl Into<String>,
    ) -> Result<ActorDeliveryCompletion, ControlDeliveryError> {
        if account_fact_digest == [0; 32] {
            return Err(ControlDeliveryError::InvalidCompletion);
        }
        self.complete(
            AccountDeliveryReceiptState::Applied,
            observed_ms,
            account_fact_digest,
            detail.into(),
        )
    }

    pub fn rejected(
        self,
        observed_ms: u64,
        account_fact_digest: [u8; 32],
        detail: impl Into<String>,
    ) -> Result<ActorDeliveryCompletion, ControlDeliveryError> {
        self.complete(
            AccountDeliveryReceiptState::Rejected,
            observed_ms,
            account_fact_digest,
            detail.into(),
        )
    }

    pub fn unknown(
        self,
        observed_ms: u64,
        account_fact_digest: [u8; 32],
        detail: impl Into<String>,
    ) -> Result<ActorDeliveryCompletion, ControlDeliveryError> {
        self.complete(
            AccountDeliveryReceiptState::Unknown,
            observed_ms,
            account_fact_digest,
            detail.into(),
        )
    }

    fn complete(
        self,
        state: AccountDeliveryReceiptState,
        observed_ms: u64,
        account_fact_digest: [u8; 32],
        detail: String,
    ) -> Result<ActorDeliveryCompletion, ControlDeliveryError> {
        validate_lease_time(&self.claim.lease, observed_ms)?;
        validate_detail(state, &detail)?;
        Ok(ActorDeliveryCompletion {
            claim: self.claim,
            durable_inbox_digest: self.durable_inbox_digest,
            state,
            observed_ms,
            account_fact_digest,
            detail,
        })
    }

    no_authority_methods!();
}

#[derive(Debug)]
pub struct ActorDeliveryCompletion {
    claim: AccountDeliveryClaim,
    durable_inbox_digest: [u8; 32],
    state: AccountDeliveryReceiptState,
    observed_ms: u64,
    account_fact_digest: [u8; 32],
    detail: String,
}

impl ActorDeliveryCompletion {
    no_authority_methods!();
}

/// Read-only fact-resolution work for an exact next-sequence reconciliation claim.
#[derive(Debug)]
pub struct ReconciliationTurn {
    claim: AccountDeliveryClaim,
    durable_inbox_digest: [u8; 32],
}

impl ReconciliationTurn {
    #[must_use]
    pub const fn lease(&self) -> &AccountDeliveryLease {
        &self.claim.lease
    }

    #[must_use]
    pub const fn payload(&self) -> &AccountDeliveryPayload {
        &self.claim.payload
    }

    pub fn reconciled(
        self,
        observed_ms: u64,
        account_fact_digest: [u8; 32],
        detail: impl Into<String>,
    ) -> Result<ReconciliationCompletion, ControlDeliveryError> {
        validate_lease_time(&self.claim.lease, observed_ms)?;
        if account_fact_digest == [0; 32] {
            return Err(ControlDeliveryError::InvalidCompletion);
        }
        let detail = detail.into();
        validate_detail(AccountDeliveryReceiptState::Reconciled, &detail)?;
        Ok(ReconciliationCompletion {
            claim: self.claim,
            durable_inbox_digest: self.durable_inbox_digest,
            observed_ms,
            account_fact_digest,
            detail,
        })
    }

    no_authority_methods!();
}

#[derive(Debug)]
pub struct ReconciliationCompletion {
    claim: AccountDeliveryClaim,
    durable_inbox_digest: [u8; 32],
    observed_ms: u64,
    account_fact_digest: [u8; 32],
    detail: String,
}

impl ReconciliationCompletion {
    no_authority_methods!();
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PersistedEvent {
    schema_version: u16,
    event: Event,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    Root {
        binding: AccountDeliveryBinding,
        node_id: String,
    },
    ClaimAccepted {
        claim: Box<AccountDeliveryClaim>,
        durable_inbox_digest: [u8; 32],
        ack: Option<AccountDeliveryAck>,
    },
    AckConfirmed {
        ack: AccountDeliveryAck,
        confirmed_ms: u64,
    },
    CompletionRecorded {
        durable_inbox_digest: [u8; 32],
        receipt: AccountDeliveryReceipt,
    },
    ReceiptConfirmed {
        receipt: AccountDeliveryReceipt,
        confirmed_ms: u64,
    },
    FailedClosed {
        reason: FailureReason,
        observed_ms: u64,
        conflicting_sha256: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FailureReason {
    Binding,
    ClaimIdentity,
    ClaimSequence,
    Ack,
    Completion,
    Receipt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedClaim {
    claim: AccountDeliveryClaim,
    durable_inbox_digest: [u8; 32],
    ack: Option<AccountDeliveryAck>,
    durable_sequence: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DeliveryProjection {
    claims: BTreeMap<u64, AcceptedClaim>,
    ack_confirmed: BTreeMap<u64, AccountDeliveryAck>,
    receipts: BTreeMap<u64, (AccountDeliveryReceipt, u64)>,
    receipt_confirmed: BTreeMap<u64, AccountDeliveryReceipt>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Projection {
    deliveries: BTreeMap<String, DeliveryProjection>,
    failed_closed: bool,
}

/// Transport-neutral Control-to-Node inbox state machine backed by a shared durable journal.
pub struct ControlDeliveryInbox<J> {
    journal: J,
    binding: AccountDeliveryBinding,
    node_id: String,
    next_sequence: u64,
    projection: Projection,
}

impl<J: ControlDeliveryJournal> ControlDeliveryInbox<J> {
    pub fn recover(
        mut journal: J,
        binding: AccountDeliveryBinding,
        node_id: impl Into<String>,
    ) -> Result<Self, ControlDeliveryError> {
        binding
            .validate()
            .map_err(|_| ControlDeliveryError::Binding)?;
        let node_id = node_id.into();
        if node_id.trim().is_empty() || node_id.len() > 128 {
            return Err(ControlDeliveryError::NodeIdentity);
        }
        let records = journal.recover()?;
        let mut projection = Projection::default();
        let mut next_sequence = 1_u64;
        for record in records {
            if record.sequence != next_sequence || record.payload.len() > MAX_RECORD_BYTES {
                return Err(ControlDeliveryError::CorruptJournal);
            }
            let persisted: PersistedEvent = serde_json::from_slice(&record.payload)
                .map_err(|_| ControlDeliveryError::CorruptJournal)?;
            if persisted.schema_version != EVENT_SCHEMA_VERSION {
                return Err(ControlDeliveryError::CorruptJournal);
            }
            apply_event(
                &mut projection,
                record.sequence,
                &persisted.event,
                &binding,
                &node_id,
            )?;
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or(ControlDeliveryError::SequenceOverflow)?;
        }
        let mut inbox = Self {
            journal,
            binding,
            node_id,
            next_sequence,
            projection,
        };
        if inbox.next_sequence == 1 {
            inbox.append(Event::Root {
                binding: inbox.binding.clone(),
                node_id: inbox.node_id.clone(),
            })?;
        }
        Ok(inbox)
    }

    #[must_use]
    pub const fn binding(&self) -> &AccountDeliveryBinding {
        &self.binding
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    #[must_use]
    pub const fn is_failed_closed(&self) -> bool {
        self.projection.failed_closed
    }

    pub fn accept_claim(
        &mut self,
        claim: AccountDeliveryClaim,
        received_ms: u64,
    ) -> Result<ClaimAcceptance, ControlDeliveryError> {
        self.require_open()?;
        claim
            .validate()
            .map_err(|_| ControlDeliveryError::InvalidClaim)?;
        if claim.lease.binding != self.binding || claim.lease.node_id != self.node_id {
            return self.fail_value(FailureReason::Binding, received_ms, &claim);
        }
        validate_lease_time(&claim.lease, received_ms)?;
        let digest = digest_serialized(&claim)?;
        let delivery_id = claim.lease.delivery_id.clone();
        let epoch = claim.lease.lease_epoch;
        if let Some(existing) = self
            .projection
            .deliveries
            .get(&delivery_id)
            .and_then(|delivery| delivery.claims.get(&epoch))
        {
            if existing.claim != claim || existing.durable_inbox_digest != digest {
                return self.fail_value(FailureReason::ClaimIdentity, received_ms, &claim);
            }
            return acceptance(existing, DurableStoreResult::Existing);
        }
        if validate_next_claim(&self.projection, &claim, received_ms).is_err() {
            return self.fail_value(FailureReason::ClaimSequence, received_ms, &claim);
        }
        let ack = if claim.lease.purpose == AccountDeliveryPurpose::Install {
            Some(AccountDeliveryAck {
                schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                lease: claim.lease.clone(),
                acknowledged_ms: received_ms,
                durable_inbox_digest: digest,
            })
        } else {
            None
        };
        let sequence = self.append(Event::ClaimAccepted {
            claim: Box::new(claim.clone()),
            durable_inbox_digest: digest,
            ack: ack.clone(),
        })?;
        acceptance(
            &AcceptedClaim {
                claim,
                durable_inbox_digest: digest,
                ack,
                durable_sequence: sequence,
            },
            DurableStoreResult::Stored,
        )
    }

    /// Must be called only after the transport reports that Control stored the exact ACK.
    pub fn confirm_acknowledgement(
        &mut self,
        ack: &AccountDeliveryAck,
        confirmed_ms: u64,
    ) -> Result<DurableStoreResult, ControlDeliveryError> {
        self.require_open()?;
        ack.validate()
            .map_err(|_| ControlDeliveryError::InvalidAck)?;
        if confirmed_ms < ack.acknowledged_ms {
            return Err(ControlDeliveryError::InvalidAck);
        }
        let delivery = self
            .projection
            .deliveries
            .get(&ack.lease.delivery_id)
            .ok_or(ControlDeliveryError::AckConflict)?;
        let accepted = delivery
            .claims
            .get(&ack.lease.lease_epoch)
            .ok_or(ControlDeliveryError::AckConflict)?;
        if accepted.ack.as_ref() != Some(ack) {
            return self.fail_value(FailureReason::Ack, confirmed_ms, ack);
        }
        if let Some(existing) = delivery.ack_confirmed.get(&ack.lease.lease_epoch) {
            return if existing == ack {
                Ok(DurableStoreResult::Existing)
            } else {
                self.fail_value(FailureReason::Ack, confirmed_ms, ack)
            };
        }
        self.append(Event::AckConfirmed {
            ack: ack.clone(),
            confirmed_ms,
        })?;
        Ok(DurableStoreResult::Stored)
    }

    pub fn actor_turn(
        &self,
        delivery_id: &str,
        now_ms: u64,
    ) -> Result<Option<ActorDeliveryTurn>, ControlDeliveryError> {
        self.require_open()?;
        let Some(delivery) = self.projection.deliveries.get(delivery_id) else {
            return Ok(None);
        };
        let Some((&epoch, accepted)) = delivery.claims.last_key_value() else {
            return Ok(None);
        };
        if accepted.claim.lease.purpose != AccountDeliveryPurpose::Install
            || !delivery.ack_confirmed.contains_key(&epoch)
            || delivery.receipts.contains_key(&epoch)
        {
            return Ok(None);
        }
        validate_lease_time(&accepted.claim.lease, now_ms)?;
        Ok(Some(ActorDeliveryTurn {
            claim: accepted.claim.clone(),
            durable_inbox_digest: accepted.durable_inbox_digest,
        }))
    }

    pub fn reconciliation_turn(
        &self,
        delivery_id: &str,
        now_ms: u64,
    ) -> Result<Option<ReconciliationTurn>, ControlDeliveryError> {
        self.require_open()?;
        let Some(delivery) = self.projection.deliveries.get(delivery_id) else {
            return Ok(None);
        };
        let Some((&epoch, accepted)) = delivery.claims.last_key_value() else {
            return Ok(None);
        };
        if accepted.claim.lease.purpose != AccountDeliveryPurpose::ReconcileOnly
            || delivery.receipts.contains_key(&epoch)
        {
            return Ok(None);
        }
        validate_lease_time(&accepted.claim.lease, now_ms)?;
        Ok(Some(ReconciliationTurn {
            claim: accepted.claim.clone(),
            durable_inbox_digest: accepted.durable_inbox_digest,
        }))
    }

    pub fn record_actor_completion(
        &mut self,
        completion: ActorDeliveryCompletion,
    ) -> Result<DurableDeliveryOutput<AccountDeliveryReceipt>, ControlDeliveryError> {
        if completion.claim.lease.purpose != AccountDeliveryPurpose::Install {
            return Err(ControlDeliveryError::InvalidCompletion);
        }
        let receipt = make_receipt(
            &completion.claim.lease,
            completion.durable_inbox_digest,
            completion.state,
            completion.observed_ms,
            completion.account_fact_digest,
            completion.detail,
        )?;
        self.record_completion(
            &completion.claim,
            completion.durable_inbox_digest,
            receipt,
            true,
        )
    }

    pub fn record_reconciliation(
        &mut self,
        completion: ReconciliationCompletion,
    ) -> Result<DurableDeliveryOutput<AccountDeliveryReceipt>, ControlDeliveryError> {
        if completion.claim.lease.purpose != AccountDeliveryPurpose::ReconcileOnly {
            return Err(ControlDeliveryError::InvalidCompletion);
        }
        let receipt = make_receipt(
            &completion.claim.lease,
            completion.durable_inbox_digest,
            AccountDeliveryReceiptState::Reconciled,
            completion.observed_ms,
            completion.account_fact_digest,
            completion.detail,
        )?;
        self.record_completion(
            &completion.claim,
            completion.durable_inbox_digest,
            receipt,
            false,
        )
    }

    pub fn confirm_receipt(
        &mut self,
        receipt: &AccountDeliveryReceipt,
        confirmed_ms: u64,
    ) -> Result<DurableStoreResult, ControlDeliveryError> {
        self.require_open()?;
        receipt
            .validate()
            .map_err(|_| ControlDeliveryError::InvalidReceipt)?;
        if confirmed_ms < receipt.observed_ms {
            return Err(ControlDeliveryError::InvalidReceipt);
        }
        let delivery = self
            .projection
            .deliveries
            .get(&receipt.lease.delivery_id)
            .ok_or(ControlDeliveryError::ReceiptConflict)?;
        let Some((expected, _)) = delivery.receipts.get(&receipt.lease.lease_epoch) else {
            return Err(ControlDeliveryError::ReceiptConflict);
        };
        if expected != receipt {
            return self.fail_value(FailureReason::Receipt, confirmed_ms, receipt);
        }
        if let Some(existing) = delivery.receipt_confirmed.get(&receipt.lease.lease_epoch) {
            return if existing == receipt {
                Ok(DurableStoreResult::Existing)
            } else {
                self.fail_value(FailureReason::Receipt, confirmed_ms, receipt)
            };
        }
        self.append(Event::ReceiptConfirmed {
            receipt: receipt.clone(),
            confirmed_ms,
        })?;
        Ok(DurableStoreResult::Stored)
    }

    #[must_use]
    pub fn pending_acknowledgements(&self, now_ms: u64) -> Vec<AccountDeliveryAck> {
        self.projection
            .deliveries
            .values()
            .filter_map(|delivery| {
                let (&epoch, accepted) = delivery.claims.last_key_value()?;
                let ack = accepted.ack.as_ref()?;
                (!delivery.ack_confirmed.contains_key(&epoch)
                    && now_ms >= ack.lease.leased_at_ms
                    && now_ms < ack.lease.expires_at_ms)
                    .then(|| ack.clone())
            })
            .collect()
    }

    #[must_use]
    pub fn pending_receipts(&self) -> Vec<AccountDeliveryReceipt> {
        self.projection
            .deliveries
            .values()
            .filter_map(|delivery| {
                let (&epoch, (receipt, _)) = delivery.receipts.last_key_value()?;
                (!delivery.receipt_confirmed.contains_key(&epoch)).then(|| receipt.clone())
            })
            .collect()
    }

    fn record_completion(
        &mut self,
        claim: &AccountDeliveryClaim,
        durable_inbox_digest: [u8; 32],
        receipt: AccountDeliveryReceipt,
        require_ack: bool,
    ) -> Result<DurableDeliveryOutput<AccountDeliveryReceipt>, ControlDeliveryError> {
        self.require_open()?;
        let delivery = self
            .projection
            .deliveries
            .get(&claim.lease.delivery_id)
            .ok_or(ControlDeliveryError::CompletionConflict)?;
        let epoch = claim.lease.lease_epoch;
        let accepted = delivery
            .claims
            .get(&epoch)
            .ok_or(ControlDeliveryError::CompletionConflict)?;
        if accepted.claim != *claim || accepted.durable_inbox_digest != durable_inbox_digest {
            return self.fail_value(FailureReason::Completion, receipt.observed_ms, &receipt);
        }
        if delivery.claims.last_key_value().map(|item| *item.0) != Some(epoch)
            || (require_ack && !delivery.ack_confirmed.contains_key(&epoch))
        {
            return Err(ControlDeliveryError::CompletionConflict);
        }
        if let Some((existing, sequence)) = delivery.receipts.get(&epoch) {
            return if existing == &receipt {
                Ok(DurableDeliveryOutput {
                    value: existing.clone(),
                    store_result: DurableStoreResult::Existing,
                    durable_sequence: *sequence,
                })
            } else {
                self.fail_value(FailureReason::Completion, receipt.observed_ms, &receipt)
            };
        }
        let sequence = self.append(Event::CompletionRecorded {
            durable_inbox_digest,
            receipt: receipt.clone(),
        })?;
        Ok(DurableDeliveryOutput {
            value: receipt,
            store_result: DurableStoreResult::Stored,
            durable_sequence: sequence,
        })
    }

    fn require_open(&self) -> Result<(), ControlDeliveryError> {
        if self.projection.failed_closed {
            Err(ControlDeliveryError::FailedClosed)
        } else {
            Ok(())
        }
    }

    fn fail_value<T, V: Serialize + ?Sized>(
        &mut self,
        reason: FailureReason,
        observed_ms: u64,
        value: &V,
    ) -> Result<T, ControlDeliveryError> {
        let conflicting_sha256 = digest_serialized(value)?;
        self.projection.failed_closed = true;
        self.append(Event::FailedClosed {
            reason,
            observed_ms,
            conflicting_sha256,
        })?;
        Err(ControlDeliveryError::FailedClosed)
    }

    fn append(&mut self, event: Event) -> Result<u64, ControlDeliveryError> {
        let persisted = PersistedEvent {
            schema_version: EVENT_SCHEMA_VERSION,
            event,
        };
        let payload = serde_json::to_vec(&persisted)?;
        if payload.len() > MAX_RECORD_BYTES {
            return Err(ControlDeliveryError::RecordTooLarge);
        }
        let sequence = self.journal.append(self.next_sequence, &payload)?;
        if sequence != self.next_sequence {
            self.projection.failed_closed = true;
            return Err(ControlDeliveryError::JournalSequence);
        }
        apply_event(
            &mut self.projection,
            sequence,
            &persisted.event,
            &self.binding,
            &self.node_id,
        )?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(ControlDeliveryError::SequenceOverflow)?;
        Ok(sequence)
    }
}

fn apply_event(
    projection: &mut Projection,
    sequence: u64,
    event: &Event,
    binding: &AccountDeliveryBinding,
    node_id: &str,
) -> Result<(), ControlDeliveryError> {
    match event {
        Event::Root {
            binding: durable_binding,
            node_id: durable_node,
        } => {
            if sequence != 1 || durable_binding != binding || durable_node != node_id {
                return Err(ControlDeliveryError::JournalRoot);
            }
        }
        Event::ClaimAccepted {
            claim,
            durable_inbox_digest,
            ack,
        } => {
            claim
                .validate()
                .map_err(|_| ControlDeliveryError::CorruptJournal)?;
            if sequence == 1
                || claim.lease.binding != *binding
                || claim.lease.node_id != node_id
                || digest_serialized(claim)? != *durable_inbox_digest
                || !valid_ack_shape(claim, *durable_inbox_digest, ack.as_ref())
                || validate_next_claim(projection, claim, claim.lease.leased_at_ms).is_err()
            {
                return Err(ControlDeliveryError::CorruptJournal);
            }
            let delivery = projection
                .deliveries
                .entry(claim.lease.delivery_id.clone())
                .or_default();
            if delivery
                .claims
                .insert(
                    claim.lease.lease_epoch,
                    AcceptedClaim {
                        claim: claim.as_ref().clone(),
                        durable_inbox_digest: *durable_inbox_digest,
                        ack: ack.clone(),
                        durable_sequence: sequence,
                    },
                )
                .is_some()
            {
                return Err(ControlDeliveryError::CorruptJournal);
            }
        }
        Event::AckConfirmed { ack, confirmed_ms } => {
            let delivery = projection
                .deliveries
                .get_mut(&ack.lease.delivery_id)
                .ok_or(ControlDeliveryError::CorruptJournal)?;
            let accepted = delivery
                .claims
                .get(&ack.lease.lease_epoch)
                .ok_or(ControlDeliveryError::CorruptJournal)?;
            if accepted.ack.as_ref() != Some(ack)
                || *confirmed_ms < ack.acknowledged_ms
                || delivery
                    .ack_confirmed
                    .insert(ack.lease.lease_epoch, ack.clone())
                    .is_some()
            {
                return Err(ControlDeliveryError::CorruptJournal);
            }
        }
        Event::CompletionRecorded {
            durable_inbox_digest,
            receipt,
        } => {
            receipt
                .validate()
                .map_err(|_| ControlDeliveryError::CorruptJournal)?;
            let delivery = projection
                .deliveries
                .get_mut(&receipt.lease.delivery_id)
                .ok_or(ControlDeliveryError::CorruptJournal)?;
            let accepted = delivery
                .claims
                .get(&receipt.lease.lease_epoch)
                .ok_or(ControlDeliveryError::CorruptJournal)?;
            if accepted.claim.lease != receipt.lease
                || accepted.durable_inbox_digest != *durable_inbox_digest
                || (receipt.lease.purpose == AccountDeliveryPurpose::Install
                    && !delivery
                        .ack_confirmed
                        .contains_key(&receipt.lease.lease_epoch))
                || delivery
                    .receipts
                    .insert(receipt.lease.lease_epoch, (receipt.clone(), sequence))
                    .is_some()
            {
                return Err(ControlDeliveryError::CorruptJournal);
            }
        }
        Event::ReceiptConfirmed {
            receipt,
            confirmed_ms,
        } => {
            let delivery = projection
                .deliveries
                .get_mut(&receipt.lease.delivery_id)
                .ok_or(ControlDeliveryError::CorruptJournal)?;
            if delivery
                .receipts
                .get(&receipt.lease.lease_epoch)
                .map(|item| &item.0)
                != Some(receipt)
                || *confirmed_ms < receipt.observed_ms
                || delivery
                    .receipt_confirmed
                    .insert(receipt.lease.lease_epoch, receipt.clone())
                    .is_some()
            {
                return Err(ControlDeliveryError::CorruptJournal);
            }
        }
        Event::FailedClosed { .. } => projection.failed_closed = true,
    }
    Ok(())
}

fn validate_next_claim(
    projection: &Projection,
    claim: &AccountDeliveryClaim,
    received_ms: u64,
) -> Result<(), ()> {
    let Some(delivery) = projection.deliveries.get(&claim.lease.delivery_id) else {
        return (claim.lease.purpose == AccountDeliveryPurpose::Install
            && claim.lease.lease_epoch == 1)
            .then_some(())
            .ok_or(());
    };
    let Some((&previous_epoch, previous)) = delivery.claims.last_key_value() else {
        return Err(());
    };
    if previous_epoch.checked_add(1) != Some(claim.lease.lease_epoch)
        || claim.lease.leased_at_ms < previous.claim.lease.expires_at_ms
        || received_ms < previous.claim.lease.expires_at_ms
        || claim.payload != previous.claim.payload
    {
        return Err(());
    }
    if delivery
        .receipt_confirmed
        .get(&previous_epoch)
        .is_some_and(|terminal| terminal.state != AccountDeliveryReceiptState::Unknown)
    {
        return Err(());
    }
    match claim.lease.purpose {
        AccountDeliveryPurpose::ReconcileOnly => Ok(()),
        AccountDeliveryPurpose::Install => (previous.claim.lease.purpose
            == AccountDeliveryPurpose::Install
            && !delivery.ack_confirmed.contains_key(&previous_epoch)
            && !delivery.receipts.contains_key(&previous_epoch))
        .then_some(())
        .ok_or(()),
    }
}

fn acceptance(
    accepted: &AcceptedClaim,
    store_result: DurableStoreResult,
) -> Result<ClaimAcceptance, ControlDeliveryError> {
    match (&accepted.ack, accepted.claim.lease.purpose) {
        (Some(ack), AccountDeliveryPurpose::Install) => {
            Ok(ClaimAcceptance::Install(DurableDeliveryOutput {
                value: ack.clone(),
                store_result,
                durable_sequence: accepted.durable_sequence,
            }))
        }
        (None, AccountDeliveryPurpose::ReconcileOnly) => {
            Ok(ClaimAcceptance::Reconcile(ReconciliationTurn {
                claim: accepted.claim.clone(),
                durable_inbox_digest: accepted.durable_inbox_digest,
            }))
        }
        _ => Err(ControlDeliveryError::CorruptJournal),
    }
}

fn valid_ack_shape(
    claim: &AccountDeliveryClaim,
    digest: [u8; 32],
    ack: Option<&AccountDeliveryAck>,
) -> bool {
    match (claim.lease.purpose, ack) {
        (AccountDeliveryPurpose::Install, Some(ack)) => {
            ack.validate().is_ok() && ack.lease == claim.lease && ack.durable_inbox_digest == digest
        }
        (AccountDeliveryPurpose::ReconcileOnly, None) => true,
        _ => false,
    }
}

fn make_receipt(
    lease: &AccountDeliveryLease,
    durable_inbox_digest: [u8; 32],
    state: AccountDeliveryReceiptState,
    observed_ms: u64,
    account_fact_digest: [u8; 32],
    detail: String,
) -> Result<AccountDeliveryReceipt, ControlDeliveryError> {
    validate_lease_time(lease, observed_ms)?;
    validate_detail(state, &detail)?;
    let digest = encode_hex(&durable_inbox_digest);
    let state_name = match state {
        AccountDeliveryReceiptState::Applied => "applied",
        AccountDeliveryReceiptState::Rejected => "rejected",
        AccountDeliveryReceiptState::Unknown => "unknown",
        AccountDeliveryReceiptState::Reconciled => "reconciled",
    };
    let receipt = AccountDeliveryReceipt {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: lease.clone(),
        receipt_id: format!(
            "node-delivery-{}-{}-{state_name}",
            lease.lease_epoch,
            &digest[..24]
        ),
        state,
        observed_ms,
        account_fact_digest,
        detail,
    };
    receipt
        .validate()
        .map_err(|_| ControlDeliveryError::InvalidReceipt)?;
    Ok(receipt)
}

fn validate_lease_time(
    lease: &AccountDeliveryLease,
    observed_ms: u64,
) -> Result<(), ControlDeliveryError> {
    if observed_ms < lease.leased_at_ms || observed_ms >= lease.expires_at_ms {
        Err(ControlDeliveryError::LeaseExpired)
    } else {
        Ok(())
    }
}

fn validate_detail(
    state: AccountDeliveryReceiptState,
    detail: &str,
) -> Result<(), ControlDeliveryError> {
    if detail.len() > MAX_DETAIL_BYTES
        || (matches!(
            state,
            AccountDeliveryReceiptState::Rejected | AccountDeliveryReceiptState::Unknown
        ) && detail.trim().is_empty())
    {
        Err(ControlDeliveryError::InvalidCompletion)
    } else {
        Ok(())
    }
}

fn digest_serialized<T: Serialize + ?Sized>(value: &T) -> Result<[u8; 32], ControlDeliveryError> {
    let encoded = serde_json::to_vec(value)?;
    if encoded.len() > MAX_RECORD_BYTES {
        return Err(ControlDeliveryError::RecordTooLarge);
    }
    Ok(Sha256::digest(encoded).into())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Debug, thiserror::Error)]
pub enum ControlDeliveryError {
    #[error("control delivery binding is invalid or conflicts with the durable root")]
    Binding,
    #[error("control delivery node identity is invalid")]
    NodeIdentity,
    #[error("control delivery claim is invalid")]
    InvalidClaim,
    #[error("control delivery lease is expired or not yet active")]
    LeaseExpired,
    #[error("control delivery ACK is invalid")]
    InvalidAck,
    #[error("control delivery ACK conflicts with durable state")]
    AckConflict,
    #[error("control delivery completion is invalid")]
    InvalidCompletion,
    #[error("control delivery completion conflicts with durable state")]
    CompletionConflict,
    #[error("control delivery receipt is invalid")]
    InvalidReceipt,
    #[error("control delivery receipt conflicts with durable state")]
    ReceiptConflict,
    #[error("control delivery inbox is durably failed closed")]
    FailedClosed,
    #[error("control delivery journal root does not match this node")]
    JournalRoot,
    #[error("control delivery journal contains invalid semantic records")]
    CorruptJournal,
    #[error("control delivery record exceeds the 2 MiB bound")]
    RecordTooLarge,
    #[error("control delivery sequence is exhausted")]
    SequenceOverflow,
    #[error("shared journal returned a non-fencing sequence")]
    JournalSequence,
    #[error(transparent)]
    Journal(#[from] ControlDeliveryJournalError),
    #[error("control delivery JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
}
