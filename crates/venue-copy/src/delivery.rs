use serde::{Deserialize, Serialize};
use thiserror::Error;
use venue_domain::domain::{InstrumentIdentity, MarketKind, is_canonical_trading_account_id};

use crate::{CopyId, CopyIdentitySet, identity::derive_commitment};

pub const MAX_DELIVERY_TTL_MS: u64 = 5 * 60 * 1_000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeliveryBinding {
    pub leader_id: CopyId,
    pub follower_id: CopyId,
    pub follower_binding_id: CopyId,
    pub account_id: String,
    pub instrument: InstrumentIdentity,
    pub policy_id: CopyId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FollowerDeliveryManifest {
    pub identities: CopyIdentitySet,
    pub binding: DeliveryBinding,
    pub plan_digest: [u8; 32],
    pub snapshot_generation: u64,
    pub instrument_generation: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

impl FollowerDeliveryManifest {
    pub fn validate(&self, now_ms: u64) -> Result<(), DeliveryError> {
        if !is_canonical_trading_account_id(&self.binding.account_id)
            || self.binding.leader_id.is_nil()
            || self.binding.follower_id.is_nil()
            || self.binding.follower_binding_id.is_nil()
            || self.binding.policy_id.is_nil()
            || self.identities.job_id.is_nil()
            || self.identities.planning_snapshot_id.is_nil()
            || self.identities.child_order_id.is_nil()
            || self.identities.idempotency_key.is_zero()
        {
            return Err(DeliveryError::Binding);
        }
        if matches!(self.binding.instrument.market, MarketKind::LinearPerpetual)
            && self
                .binding
                .instrument
                .settlement_asset
                .as_ref()
                .is_none_or(|asset| asset.as_str() != self.binding.instrument.symbol.quote())
        {
            return Err(DeliveryError::Instrument);
        }
        if self.plan_digest == [0; 32] {
            return Err(DeliveryError::Digest);
        }
        if self.snapshot_generation == 0 || self.instrument_generation == 0 {
            return Err(DeliveryError::Generation);
        }
        let ttl = self
            .expires_at_ms
            .checked_sub(self.issued_at_ms)
            .ok_or(DeliveryError::Window)?;
        if self.issued_at_ms == 0
            || ttl == 0
            || ttl > MAX_DELIVERY_TTL_MS
            || now_ms < self.issued_at_ms
            || now_ms >= self.expires_at_ms
        {
            return Err(DeliveryError::Window);
        }
        Ok(())
    }

    #[must_use]
    pub fn delivery_digest(&self) -> [u8; 32] {
        let snapshot_generation = self.snapshot_generation.to_be_bytes();
        let instrument_generation = self.instrument_generation.to_be_bytes();
        let issued_at = self.issued_at_ms.to_be_bytes();
        let expires_at = self.expires_at_ms.to_be_bytes();
        let market = match self.binding.instrument.market {
            MarketKind::Spot => b"SPOT".as_slice(),
            MarketKind::LinearPerpetual => b"LINEAR_PERPETUAL".as_slice(),
        };
        let settlement = self
            .binding
            .instrument
            .settlement_asset
            .as_ref()
            .map_or("", |asset| asset.as_str());
        derive_commitment(
            b"copy-delivery-v1",
            &[
                self.identities.job_id.as_bytes(),
                self.identities.planning_snapshot_id.as_bytes(),
                self.identities.child_order_id.as_bytes(),
                self.identities.idempotency_key.as_bytes(),
                self.binding.leader_id.as_bytes(),
                self.binding.follower_id.as_bytes(),
                self.binding.follower_binding_id.as_bytes(),
                self.binding.account_id.as_bytes(),
                self.binding.instrument.symbol.base().as_bytes(),
                self.binding.instrument.symbol.quote().as_bytes(),
                market,
                settlement.as_bytes(),
                self.binding.policy_id.as_bytes(),
                &self.plan_digest,
                &snapshot_generation,
                &instrument_generation,
                &issued_at,
                &expires_at,
            ],
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DeliveryReceiptStatus {
    Applied,
    Unknown,
    Reconciled,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistedDeliveryReceipt {
    pub delivery_digest: [u8; 32],
    pub binding: DeliveryBinding,
    pub plan_digest: [u8; 32],
    pub snapshot_generation: u64,
    pub instrument_generation: u64,
    pub receipt_sequence: u64,
    pub status: DeliveryReceiptStatus,
    pub persisted_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryState {
    Pending,
    Applied(PersistedDeliveryReceipt),
    Unknown(PersistedDeliveryReceipt),
    Reconciled(PersistedDeliveryReceipt),
    Rejected(PersistedDeliveryReceipt),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptApply {
    Advanced,
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryTracker {
    manifest: FollowerDeliveryManifest,
    state: DeliveryState,
}

impl DeliveryTracker {
    pub fn new(manifest: FollowerDeliveryManifest, now_ms: u64) -> Result<Self, DeliveryError> {
        manifest.validate(now_ms)?;
        Ok(Self {
            manifest,
            state: DeliveryState::Pending,
        })
    }

    #[must_use]
    pub const fn manifest(&self) -> &FollowerDeliveryManifest {
        &self.manifest
    }

    #[must_use]
    pub const fn state(&self) -> &DeliveryState {
        &self.state
    }

    #[must_use]
    pub fn may_deliver(&self, now_ms: u64) -> bool {
        matches!(self.state, DeliveryState::Pending) && self.manifest.validate(now_ms).is_ok()
    }

    #[must_use]
    pub const fn requires_reconciliation(&self) -> bool {
        matches!(self.state, DeliveryState::Unknown(_))
    }

    pub fn apply_persisted_receipt(
        &mut self,
        receipt: PersistedDeliveryReceipt,
    ) -> Result<ReceiptApply, DeliveryError> {
        self.validate_receipt(&receipt)?;
        match &self.state {
            DeliveryState::Pending => {
                if matches!(receipt.status, DeliveryReceiptStatus::Reconciled) {
                    return Err(DeliveryError::Transition);
                }
                self.state = state_from_receipt(receipt);
                Ok(ReceiptApply::Advanced)
            }
            DeliveryState::Unknown(previous) => {
                if &receipt == previous {
                    return Ok(ReceiptApply::NoOp);
                }
                let expected_sequence = previous
                    .receipt_sequence
                    .checked_add(1)
                    .ok_or(DeliveryError::Transition)?;
                if receipt.status != DeliveryReceiptStatus::Reconciled
                    || receipt.receipt_sequence != expected_sequence
                {
                    return Err(DeliveryError::Transition);
                }
                self.state = DeliveryState::Reconciled(receipt);
                Ok(ReceiptApply::Advanced)
            }
            DeliveryState::Applied(previous)
            | DeliveryState::Reconciled(previous)
            | DeliveryState::Rejected(previous) => {
                if &receipt == previous {
                    Ok(ReceiptApply::NoOp)
                } else {
                    Err(DeliveryError::Transition)
                }
            }
        }
    }

    fn validate_receipt(&self, receipt: &PersistedDeliveryReceipt) -> Result<(), DeliveryError> {
        if receipt.delivery_digest != self.manifest.delivery_digest()
            || receipt.binding != self.manifest.binding
            || receipt.plan_digest != self.manifest.plan_digest
        {
            return Err(DeliveryError::ReceiptBinding);
        }
        if receipt.snapshot_generation != self.manifest.snapshot_generation
            || receipt.instrument_generation != self.manifest.instrument_generation
            || receipt.receipt_sequence == 0
        {
            return Err(DeliveryError::ReceiptGeneration);
        }
        if receipt.persisted_at_ms < self.manifest.issued_at_ms {
            return Err(DeliveryError::ReceiptPersistence);
        }
        Ok(())
    }
}

fn state_from_receipt(receipt: PersistedDeliveryReceipt) -> DeliveryState {
    match receipt.status {
        DeliveryReceiptStatus::Applied => DeliveryState::Applied(receipt),
        DeliveryReceiptStatus::Unknown => DeliveryState::Unknown(receipt),
        DeliveryReceiptStatus::Reconciled => DeliveryState::Reconciled(receipt),
        DeliveryReceiptStatus::Rejected => DeliveryState::Rejected(receipt),
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DeliveryError {
    #[error("delivery account binding is not canonical")]
    Binding,
    #[error("delivery linear instrument settlement must exactly match its quote asset")]
    Instrument,
    #[error("delivery plan digest must be non-zero")]
    Digest,
    #[error("delivery generations must be positive")]
    Generation,
    #[error("delivery authorization window is malformed, stale, future-dated, or too long")]
    Window,
    #[error("receipt does not bind the exact delivery, account, instrument, policy, and plan")]
    ReceiptBinding,
    #[error("receipt does not bind the exact positive generations and sequence")]
    ReceiptGeneration,
    #[error("receipt was not persisted after delivery authorization was issued")]
    ReceiptPersistence,
    #[error("receipt transition conflicts with terminal or unknown delivery state")]
    Transition,
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{Asset, MarketKind, Symbol};

    use super::*;
    use crate::{CopyAction, CopyIdentityInput, derive_copy_identities};

    fn identities(seed: u8) -> Result<CopyIdentitySet, crate::CopyIdentityError> {
        derive_copy_identities(&CopyIdentityInput {
            event_id: [seed; 16],
            source_event_id: [seed + 1; 16],
            follower_account_id: [seed + 2; 16],
            follower_binding_id: [seed + 3; 16],
            leader_order_id: [seed + 4; 16],
            revision: 1,
            action: CopyAction::New,
        })
    }

    fn manifest() -> Result<FollowerDeliveryManifest, Box<dyn std::error::Error>> {
        let primary = identities(1)?;
        let related = identities(20)?;
        Ok(FollowerDeliveryManifest {
            identities: primary,
            binding: DeliveryBinding {
                leader_id: related.job_id,
                follower_id: related.planning_snapshot_id,
                follower_binding_id: related.child_order_id,
                account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                instrument: InstrumentIdentity {
                    symbol: "BTC/USDT".parse::<Symbol>()?,
                    market: MarketKind::LinearPerpetual,
                    settlement_asset: Some(Asset::new("USDT")?),
                },
                policy_id: identities(40)?.job_id,
            },
            plan_digest: [7; 32],
            snapshot_generation: 8,
            instrument_generation: 9,
            issued_at_ms: 100,
            expires_at_ms: 1_000,
        })
    }

    fn receipt(
        manifest: &FollowerDeliveryManifest,
        status: DeliveryReceiptStatus,
        sequence: u64,
    ) -> PersistedDeliveryReceipt {
        PersistedDeliveryReceipt {
            delivery_digest: manifest.delivery_digest(),
            binding: manifest.binding.clone(),
            plan_digest: manifest.plan_digest,
            snapshot_generation: manifest.snapshot_generation,
            instrument_generation: manifest.instrument_generation,
            receipt_sequence: sequence,
            status,
            persisted_at_ms: 500,
        }
    }

    #[test]
    fn manifest_binds_all_identity_and_freshness_inputs() -> Result<(), Box<dyn std::error::Error>>
    {
        let baseline = manifest()?;
        baseline.validate(500)?;
        let digest = baseline.delivery_digest();
        let mut changed = baseline.clone();
        changed.identities.child_order_id = identities(60)?.child_order_id;
        assert_ne!(changed.delivery_digest(), digest);
        changed = baseline.clone();
        changed.plan_digest[0] ^= 1;
        assert_ne!(changed.delivery_digest(), digest);
        changed = baseline.clone();
        changed.instrument_generation += 1;
        assert_ne!(changed.delivery_digest(), digest);
        Ok(())
    }

    #[test]
    fn malformed_or_unbounded_authorization_fails_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut value = manifest()?;
        value.account_id_mut_for_test("native-account");
        assert_eq!(value.validate(500), Err(DeliveryError::Binding));
        value = manifest()?;
        value.plan_digest = [0; 32];
        assert_eq!(value.validate(500), Err(DeliveryError::Digest));
        value = manifest()?;
        value.binding.instrument.settlement_asset = Some(Asset::new("USD")?);
        assert_eq!(value.validate(500), Err(DeliveryError::Instrument));
        value = manifest()?;
        value.expires_at_ms = value.issued_at_ms + MAX_DELIVERY_TTL_MS + 1;
        assert_eq!(value.validate(500), Err(DeliveryError::Window));
        value = manifest()?;
        assert_eq!(
            value.validate(value.expires_at_ms),
            Err(DeliveryError::Window)
        );
        Ok(())
    }

    #[test]
    fn unknown_fences_redelivery_until_exact_reconciliation()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = manifest()?;
        let mut tracker = DeliveryTracker::new(value.clone(), 500)?;
        let unknown = receipt(&value, DeliveryReceiptStatus::Unknown, 4);
        assert_eq!(
            tracker.apply_persisted_receipt(unknown.clone()),
            Ok(ReceiptApply::Advanced)
        );
        assert!(!tracker.may_deliver(600));
        assert!(tracker.requires_reconciliation());
        assert_eq!(
            tracker.apply_persisted_receipt(unknown),
            Ok(ReceiptApply::NoOp)
        );
        assert_eq!(
            tracker.apply_persisted_receipt(receipt(&value, DeliveryReceiptStatus::Applied, 5)),
            Err(DeliveryError::Transition)
        );
        assert_eq!(
            tracker.apply_persisted_receipt(receipt(&value, DeliveryReceiptStatus::Reconciled, 5)),
            Ok(ReceiptApply::Advanced)
        );
        assert!(matches!(tracker.state(), DeliveryState::Reconciled(_)));
        Ok(())
    }

    #[test]
    fn receipt_requires_exact_digest_binding_and_generations()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = manifest()?;
        let mut tracker = DeliveryTracker::new(value.clone(), 500)?;
        let mut wrong = receipt(&value, DeliveryReceiptStatus::Applied, 1);
        wrong.plan_digest[0] ^= 1;
        assert_eq!(
            tracker.apply_persisted_receipt(wrong),
            Err(DeliveryError::ReceiptBinding)
        );
        let mut wrong = receipt(&value, DeliveryReceiptStatus::Applied, 1);
        wrong.snapshot_generation += 1;
        assert_eq!(
            tracker.apply_persisted_receipt(wrong),
            Err(DeliveryError::ReceiptGeneration)
        );
        let applied = receipt(&value, DeliveryReceiptStatus::Applied, 1);
        assert_eq!(
            tracker.apply_persisted_receipt(applied.clone()),
            Ok(ReceiptApply::Advanced)
        );
        assert_eq!(
            tracker.apply_persisted_receipt(applied),
            Ok(ReceiptApply::NoOp)
        );
        Ok(())
    }

    impl FollowerDeliveryManifest {
        fn account_id_mut_for_test(&mut self, value: &str) {
            self.binding.account_id = value.to_owned();
        }
    }
}
