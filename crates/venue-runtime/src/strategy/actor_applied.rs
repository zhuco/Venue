use std::{collections::BTreeSet, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_storage::{
    ActorAppliedAnchor, ActorAppliedCommit, ActorAppliedError, ActorAppliedGenerations,
    ActorAppliedReceipt, ActorAppliedReplayState, ActorAppliedScope, ActorAppliedStore,
    DurableWalHead, RecoveredActorApplied,
};

use crate::domain::{StrategyBinding, StrategyKind, StrategyTurnToken};

const RUNTIME_REPLAY_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Deserialize, Serialize)]
struct RuntimeReplayEnvelope {
    schema_version: u16,
    config_digest: String,
    applied_private_deliveries: BTreeSet<AppliedPrivateDelivery>,
    actor_checkpoint: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct AppliedPrivateDelivery {
    pub(crate) evidence_sequence: u64,
    pub(crate) fact_index: u32,
}

/// Runtime-owned adapter for the one Actor applied journal/checkpoint pair. Construction and
/// commit are crate-private so strategy, Control and database values cannot mint authority.
#[derive(Debug)]
pub(crate) struct ActorAppliedTurnStore {
    binding: StrategyBinding,
    store: ActorAppliedStore,
}

impl ActorAppliedTurnStore {
    pub(crate) fn create_new(
        binding: StrategyBinding,
        journal_path: impl Into<PathBuf>,
        checkpoint_path: impl Into<PathBuf>,
    ) -> Result<Self, ActorAppliedError> {
        Ok(Self {
            binding,
            store: ActorAppliedStore::create_new(journal_path, checkpoint_path)?,
        })
    }

    pub(crate) fn open_existing(
        binding: StrategyBinding,
        journal_path: impl Into<PathBuf>,
        checkpoint_path: impl Into<PathBuf>,
        anchor: ActorAppliedAnchor,
    ) -> Result<Self, ActorAppliedError> {
        let store = ActorAppliedStore::open_existing(journal_path, checkpoint_path, anchor)?;
        let recovered = store
            .recover()?
            .ok_or(ActorAppliedError::MissingArtifacts)?;
        verify_recovered_binding(&binding, &recovered)?;
        Ok(Self { binding, store })
    }

    #[must_use]
    pub(crate) const fn binding(&self) -> &StrategyBinding {
        &self.binding
    }

    pub(crate) fn recover(&self) -> Result<Option<RecoveredActorApplied>, ActorAppliedError> {
        let recovered = self.store.recover()?;
        if let Some(recovered) = &recovered {
            verify_recovered_binding(&self.binding, recovered)?;
        }
        Ok(recovered)
    }

    pub(crate) fn recovered_private_deliveries(
        &self,
    ) -> Result<BTreeSet<AppliedPrivateDelivery>, ActorAppliedError> {
        let Some(recovered) = self.recover()? else {
            return Ok(BTreeSet::new());
        };
        let envelope = replay_envelope(&recovered)?;
        Ok(envelope.applied_private_deliveries)
    }

    pub(crate) fn refresh_binding(
        &mut self,
        binding: StrategyBinding,
    ) -> Result<(), ActorAppliedError> {
        (canonical_scope(&binding)? == canonical_scope(&self.binding)?
            && binding.key == self.binding.key
            && binding.run_id == self.binding.run_id)
            .then_some(())
            .ok_or(ActorAppliedError::ScopeDrift)?;
        self.binding = binding;
        Ok(())
    }

    pub(crate) fn commit(
        &mut self,
        token: &StrategyTurnToken,
        wal: DurableWalHead,
        applied_private_sequence: u64,
        private_delivery: Option<AppliedPrivateDelivery>,
        replay_state: Vec<u8>,
    ) -> Result<ActorAppliedReceipt, ActorAppliedError> {
        self.require_token_binding(token)?;
        let recovered = self.store.recover()?;
        let replay_revision = recovered
            .as_ref()
            .map_or(Some(1), |recovered| {
                recovered.receipt().replay_revision().checked_add(1)
            })
            .ok_or(ActorAppliedError::SequenceExhausted)?;
        let mut applied_private_deliveries = recovered
            .as_ref()
            .map(replay_envelope)
            .transpose()?
            .map_or_else(BTreeSet::new, |envelope| {
                envelope.applied_private_deliveries
            });
        let previously_committed_cursor = recovered
            .as_ref()
            .map_or(0, |state| state.receipt().applied_private_sequence());
        applied_private_deliveries
            .retain(|delivery| delivery.evidence_sequence > previously_committed_cursor);
        if let Some(private_delivery) = private_delivery {
            applied_private_deliveries.insert(private_delivery);
        }
        let replay_state = serde_json::to_vec(&RuntimeReplayEnvelope {
            schema_version: RUNTIME_REPLAY_SCHEMA_VERSION,
            config_digest: self.binding.config_digest.clone(),
            applied_private_deliveries,
            actor_checkpoint: replay_state,
        })
        .map_err(ActorAppliedError::Encode)?;
        self.store.commit(ActorAppliedCommit::new(
            canonical_scope(&self.binding)?,
            ActorAppliedGenerations::new(
                token.config_epoch(),
                token.connection_generation(),
                token.private_generation(),
            )?,
            token.turn_sequence(),
            wal,
            ActorAppliedReplayState::new(replay_revision, applied_private_sequence, replay_state)?,
        )?)
    }

    pub(crate) fn verify_current(
        &self,
        token: &StrategyTurnToken,
        wal: DurableWalHead,
        applied_private_sequence: u64,
        receipt: &ActorAppliedReceipt,
    ) -> Result<(), ActorAppliedError> {
        self.require_token_binding(token)?;
        if receipt.scope() != canonical_scope(&self.binding)?
            || receipt.generations()
                != ActorAppliedGenerations::new(
                    token.config_epoch(),
                    token.connection_generation(),
                    token.private_generation(),
                )?
            || receipt.turn_sequence() != token.turn_sequence()
            || receipt.wal() != wal
            || receipt.applied_private_sequence() != applied_private_sequence
        {
            return Err(ActorAppliedError::StaleReceipt);
        }
        self.store.verify_current(receipt)
    }

    fn require_token_binding(&self, token: &StrategyTurnToken) -> Result<(), ActorAppliedError> {
        (token.target() == &self.binding.key && token.config_digest() == self.binding.config_digest)
            .then_some(())
            .ok_or(ActorAppliedError::InvalidScope)
    }
}

fn verify_recovered_binding(
    binding: &StrategyBinding,
    recovered: &RecoveredActorApplied,
) -> Result<(), ActorAppliedError> {
    if recovered.receipt().scope() != canonical_scope(binding)? {
        return Err(ActorAppliedError::ScopeDrift);
    }
    let envelope = replay_envelope(recovered)?;
    if envelope.schema_version != RUNTIME_REPLAY_SCHEMA_VERSION
        || envelope.config_digest != binding.config_digest
        || envelope.actor_checkpoint.is_empty()
    {
        return Err(ActorAppliedError::CheckpointDrift);
    }
    Ok(())
}

fn replay_envelope(
    recovered: &RecoveredActorApplied,
) -> Result<RuntimeReplayEnvelope, ActorAppliedError> {
    serde_json::from_slice(recovered.replay_state()).map_err(ActorAppliedError::Decode)
}

fn canonical_scope(binding: &StrategyBinding) -> Result<ActorAppliedScope, ActorAppliedError> {
    ActorAppliedScope::new(actor_commitment(binding), owner_commitment(binding))
}

fn actor_commitment(binding: &StrategyBinding) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_field(&mut digest, b"venue.runtime.actor.v1");
    commit_identity(&mut digest, binding, true);
    digest.finalize().into()
}

fn owner_commitment(binding: &StrategyBinding) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_field(&mut digest, b"venue.runtime.owner.v1");
    commit_identity(&mut digest, binding, false);
    digest.finalize().into()
}

fn commit_identity(digest: &mut Sha256, binding: &StrategyBinding, include_kind: bool) {
    commit_field(digest, binding.key.account.exchange.as_str().as_bytes());
    commit_field(digest, binding.key.account.account.as_bytes());
    if include_kind {
        commit_field(
            digest,
            match binding.key.strategy_kind {
                StrategyKind::HedgedGrid => b"hedged_grid",
                StrategyKind::Scalping => b"scalping",
                StrategyKind::Copy => b"copy",
            },
        );
    }
    commit_field(digest, binding.key.instance_id.as_bytes());
    commit_field(digest, binding.run_id.as_bytes());
    commit_field(digest, binding.key.symbol.base().as_bytes());
    commit_field(digest, binding.key.symbol.quote().as_bytes());
}

fn commit_field(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{AccountKey, ExchangeId, StrategyInstanceKey};

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        let account = AccountKey::new(ExchangeId::Binance, "00000000-0000-4000-8000-000000000001")?;
        let key = StrategyInstanceKey::new(
            account,
            StrategyKind::HedgedGrid,
            "grid_a",
            "BTC/USDT".parse()?,
        )?;
        Ok(StrategyBinding::new(key, "run_1", "config_1")?)
    }

    fn token(
        binding: &StrategyBinding,
        config_epoch: u64,
        connection_generation: u64,
        private_generation: u64,
        turn_sequence: u64,
    ) -> Result<StrategyTurnToken, Box<dyn std::error::Error>> {
        Ok(StrategyTurnToken::issue(
            binding.key.clone(),
            connection_generation,
            private_generation,
            binding.config_digest.clone(),
            config_epoch,
            turn_sequence,
        )?)
    }

    #[test]
    fn recovered_store_rejects_another_actor_or_owner() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let journal = directory.path().join("applied.jsonl");
        let checkpoint = directory.path().join("checkpoint.json");
        let binding = binding()?;
        let token = StrategyTurnToken::issue(
            binding.key.clone(),
            1,
            1,
            binding.config_digest.clone(),
            1,
            1,
        )?;
        let mut store = ActorAppliedTurnStore::create_new(binding.clone(), &journal, &checkpoint)?;
        let receipt = store.commit(
            &token,
            DurableWalHead::new([7; 32], 0, 0)?,
            0,
            None,
            b"checkpoint-1".to_vec(),
        )?;

        let mut foreign = binding;
        foreign.run_id = "run_2".to_owned();
        assert!(matches!(
            ActorAppliedTurnStore::open_existing(foreign, journal, checkpoint, receipt.anchor()),
            Err(ActorAppliedError::ScopeDrift)
        ));
        Ok(())
    }

    #[test]
    fn crash_after_applied_commit_recovers_once_and_stale_receipt_loses_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let journal = directory.path().join("applied.jsonl");
        let checkpoint = directory.path().join("checkpoint.json");
        let binding = binding()?;
        let wal = DurableWalHead::new([7; 32], 0, 0)?;
        let first_token = token(&binding, 1, 1, 1, 1)?;
        let first = {
            let mut store =
                ActorAppliedTurnStore::create_new(binding.clone(), &journal, &checkpoint)?;
            assert!(store.recover()?.is_none());
            store.commit(
                &first_token,
                wal,
                1,
                Some(AppliedPrivateDelivery {
                    evidence_sequence: 1,
                    fact_index: 0,
                }),
                b"actor-state-1".to_vec(),
            )?
        };

        let mut reopened = ActorAppliedTurnStore::open_existing(
            binding.clone(),
            &journal,
            &checkpoint,
            first.anchor(),
        )?;
        let recovered = reopened.recover()?.ok_or("applied turn missing")?;
        assert_eq!(recovered.receipt().turn_sequence(), 1);
        assert_eq!(recovered.receipt().applied_private_sequence(), 1);
        let envelope: RuntimeReplayEnvelope = serde_json::from_slice(recovered.replay_state())?;
        assert_eq!(envelope.actor_checkpoint, b"actor-state-1");
        assert_eq!(
            envelope.applied_private_deliveries,
            BTreeSet::from([AppliedPrivateDelivery {
                evidence_sequence: 1,
                fact_index: 0,
            }])
        );

        assert!(matches!(
            reopened.commit(&first_token, wal, 1, None, b"duplicate".to_vec()),
            Err(ActorAppliedError::StaleTurn)
        ));
        let second_token = token(&binding, 1, 1, 1, 2)?;
        let second = reopened.commit(&second_token, wal, 1, None, b"actor-state-2".to_vec())?;
        assert!(matches!(
            reopened.verify_current(&first_token, wal, 1, &first),
            Err(ActorAppliedError::StaleReceipt)
        ));
        reopened.verify_current(&second_token, wal, 1, &second)?;
        Ok(())
    }

    #[test]
    fn generation_turn_and_wal_rollback_or_equivocation_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let mut store = ActorAppliedTurnStore::create_new(
            binding.clone(),
            directory.path().join("applied.jsonl"),
            directory.path().join("checkpoint.json"),
        )?;
        let first_wal = DurableWalHead::new([7; 32], 2, 2)?;
        store.commit(
            &token(&binding, 2, 2, 2, 1)?,
            first_wal,
            1,
            None,
            b"state-1".to_vec(),
        )?;

        assert!(matches!(
            store.commit(
                &token(&binding, 1, 2, 2, 2)?,
                first_wal,
                1,
                None,
                b"stale-generation".to_vec(),
            ),
            Err(ActorAppliedError::StaleGeneration)
        ));
        assert!(matches!(
            store.commit(
                &token(&binding, 2, 2, 2, 3)?,
                first_wal,
                1,
                None,
                b"skipped-turn".to_vec(),
            ),
            Err(ActorAppliedError::StaleTurn)
        ));
        assert!(matches!(
            store.commit(
                &token(&binding, 2, 2, 2, 2)?,
                DurableWalHead::new([8; 32], 2, 2)?,
                1,
                None,
                b"equivocated-wal".to_vec(),
            ),
            Err(ActorAppliedError::WalDrift)
        ));
        assert!(matches!(
            store.commit(
                &token(&binding, 2, 2, 2, 2)?,
                DurableWalHead::new([9; 32], 1, 1)?,
                1,
                None,
                b"rollback-wal".to_vec(),
            ),
            Err(ActorAppliedError::WalDrift)
        ));
        Ok(())
    }
}
