use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use venue_domain::domain::Amount;

use crate::{CopyId, DeliveryBinding};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LedgerAttribution {
    Copy,
    External,
    Manual,
}

/// One persisted private-fact projection. It is evidence for a reducer, not an execution result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerEntry {
    pub sequence: u64,
    pub generation: u64,
    pub binding: DeliveryBinding,
    pub attribution: LedgerAttribution,
    pub source_id: CopyId,
    pub fact_digest: [u8; 32],
    pub managed_exposure: Amount,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerApply {
    Advanced,
    NoOp,
}

/// Pure idempotent projection of already-persisted account facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyLedger {
    binding: DeliveryBinding,
    entries: BTreeMap<u64, LedgerEntry>,
    generation: u64,
    managed_exposure: Option<Amount>,
}

impl CopyLedger {
    #[must_use]
    pub fn new(binding: DeliveryBinding) -> Self {
        Self {
            binding,
            entries: BTreeMap::new(),
            generation: 0,
            managed_exposure: None,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn managed_exposure(&self) -> Option<&Amount> {
        self.managed_exposure.as_ref()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn apply(&mut self, entry: LedgerEntry) -> Result<LedgerApply, LedgerError> {
        validate_entry(&entry)?;
        if entry.binding != self.binding {
            return Err(LedgerError::Binding);
        }

        if let Some(existing) = self.entries.get(&entry.sequence) {
            return if existing == &entry {
                Ok(LedgerApply::NoOp)
            } else {
                Err(LedgerError::Conflict)
            };
        }

        let expected_sequence = self.entries.len() as u64 + 1;
        if entry.sequence < expected_sequence {
            return Err(LedgerError::Rollback);
        }
        if entry.sequence > expected_sequence {
            return Err(LedgerError::SequenceGap);
        }
        if !self.entries.is_empty() && entry.generation < self.generation {
            return Err(LedgerError::GenerationRollback);
        }
        if let Some(current) = &self.managed_exposure
            && current.asset != entry.managed_exposure.asset
        {
            return Err(LedgerError::Asset);
        }

        self.generation = entry.generation;
        self.managed_exposure = Some(entry.managed_exposure.clone());
        self.entries.insert(entry.sequence, entry);
        Ok(LedgerApply::Advanced)
    }
}

fn validate_entry(entry: &LedgerEntry) -> Result<(), LedgerError> {
    if entry.sequence == 0 || entry.generation == 0 {
        return Err(LedgerError::Generation);
    }
    if entry.fact_digest == [0; 32] {
        return Err(LedgerError::Digest);
    }
    if entry.managed_exposure.value == Decimal::MAX || entry.managed_exposure.value == Decimal::MIN
    {
        return Err(LedgerError::Exposure);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LedgerError {
    #[error("ledger sequence and generation must be positive")]
    Generation,
    #[error("ledger fact digest must be non-zero")]
    Digest,
    #[error("ledger entry does not match the exact follower binding")]
    Binding,
    #[error("an existing ledger sequence has conflicting facts")]
    Conflict,
    #[error("ledger sequence attempted to roll back")]
    Rollback,
    #[error("ledger sequence skipped an unapplied fact")]
    SequenceGap,
    #[error("ledger generation attempted to roll back")]
    GenerationRollback,
    #[error("ledger valuation asset changed across generations")]
    Asset,
    #[error("ledger exposure is outside the supported decimal range")]
    Exposure,
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{Asset, InstrumentIdentity, MarketKind, Symbol};

    use super::*;
    use crate::{CopyAction, CopyIdentityInput, CopyIdentitySet, derive_copy_identities};

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

    fn binding() -> Result<DeliveryBinding, Box<dyn std::error::Error>> {
        let ids = identities(1)?;
        Ok(DeliveryBinding {
            relation: crate::RelationCommitment {
                relation_id: identities(60)?.job_id,
                revision: 1,
                policy_digest: [6; 32],
            },
            leader_id: ids.job_id,
            follower_id: ids.planning_snapshot_id,
            follower_binding_id: ids.child_order_id,
            follower_instance_id: "copy-follower".to_owned(),
            account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instrument: InstrumentIdentity {
                symbol: "BTC/USDT".parse::<Symbol>()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
            },
            policy_id: identities(20)?.job_id,
        })
    }

    fn entry(
        sequence: u64,
        attribution: LedgerAttribution,
    ) -> Result<LedgerEntry, Box<dyn std::error::Error>> {
        Ok(LedgerEntry {
            sequence,
            generation: sequence,
            binding: binding()?,
            attribution,
            source_id: identities(40 + sequence as u8)?.job_id,
            fact_digest: [sequence as u8; 32],
            managed_exposure: Amount::new(Asset::new("USDT")?, Decimal::from(sequence * 10)),
        })
    }

    #[test]
    fn exact_replay_is_noop_and_all_attributions_advance() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut ledger = CopyLedger::new(binding()?);
        let first = entry(1, LedgerAttribution::Copy)?;
        assert_eq!(ledger.apply(first.clone()), Ok(LedgerApply::Advanced));
        assert_eq!(ledger.apply(first), Ok(LedgerApply::NoOp));
        assert_eq!(
            ledger.apply(entry(2, LedgerAttribution::External)?),
            Ok(LedgerApply::Advanced)
        );
        assert_eq!(
            ledger.apply(entry(3, LedgerAttribution::Manual)?),
            Ok(LedgerApply::Advanced)
        );
        assert_eq!(ledger.generation(), 3);
        assert_eq!(ledger.len(), 3);
        assert_eq!(
            ledger.managed_exposure().map(|value| value.value),
            Some(Decimal::from(30))
        );
        Ok(())
    }

    #[test]
    fn conflicting_replay_rollback_and_gaps_fail_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut ledger = CopyLedger::new(binding()?);
        ledger.apply(entry(1, LedgerAttribution::Copy)?)?;
        let mut conflict = entry(1, LedgerAttribution::Copy)?;
        conflict.fact_digest = [9; 32];
        assert_eq!(ledger.apply(conflict), Err(LedgerError::Conflict));
        assert_eq!(
            ledger.apply(entry(3, LedgerAttribution::Copy)?),
            Err(LedgerError::SequenceGap)
        );
        let mut generation_gap = entry(2, LedgerAttribution::Copy)?;
        generation_gap.generation = 3;
        assert_eq!(ledger.apply(generation_gap), Ok(LedgerApply::Advanced));
        let mut same_generation = entry(3, LedgerAttribution::Copy)?;
        same_generation.generation = 3;
        assert_eq!(ledger.apply(same_generation), Ok(LedgerApply::Advanced));
        let mut generation_rollback = entry(4, LedgerAttribution::Copy)?;
        generation_rollback.generation = 2;
        assert_eq!(
            ledger.apply(generation_rollback),
            Err(LedgerError::GenerationRollback)
        );
        Ok(())
    }

    #[test]
    fn cross_binding_and_asset_changes_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut ledger = CopyLedger::new(binding()?);
        let mut wrong_binding = entry(1, LedgerAttribution::Copy)?;
        wrong_binding.binding.account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        assert_eq!(ledger.apply(wrong_binding), Err(LedgerError::Binding));
        ledger.apply(entry(1, LedgerAttribution::Copy)?)?;
        let mut wrong_asset = entry(2, LedgerAttribution::Copy)?;
        wrong_asset.managed_exposure.asset = Asset::new("USD")?;
        assert_eq!(ledger.apply(wrong_asset), Err(LedgerError::Asset));
        Ok(())
    }
}
