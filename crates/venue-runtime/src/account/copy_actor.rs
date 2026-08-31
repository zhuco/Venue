use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_storage::{ActorAppliedAnchor, ActorAppliedReceipt, DurableWalHead};

use super::{AccountHealth, AccountRuntime, AccountRuntimeError, StrategyBinding, StrategyKind};
use crate::{
    StrategyTurnToken,
    execution::{
        AccountExecutionIntent, AccountLanePriority, CommandIdentityReceipt,
        DurableCommandIdentityAllocation,
    },
    strategy::ActorAppliedTurnStore,
};
use venue_domain::domain::ExecutionCommand;

const COPY_REPLAY_SCHEMA_VERSION: u16 = 1;

/// Immutable Node-inbox facts which a Copy actor must retain in its own durable checkpoint.
/// This value is semantic data, never a writer lease or physical dispatch permit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyActorCommitment {
    delivery_digest: [u8; 32],
    durable_inbox_digest: [u8; 32],
    durable_inbox_sequence: u64,
    durable_inbox_root_digest: [u8; 32],
}

impl CopyActorCommitment {
    pub fn new(
        delivery_digest: [u8; 32],
        durable_inbox_digest: [u8; 32],
        durable_inbox_sequence: u64,
        durable_inbox_root_digest: [u8; 32],
    ) -> Result<Self, AccountRuntimeError> {
        if is_zero(&delivery_digest)
            || is_zero(&durable_inbox_digest)
            || durable_inbox_sequence == 0
            || is_zero(&durable_inbox_root_digest)
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        Ok(Self {
            delivery_digest,
            durable_inbox_digest,
            durable_inbox_sequence,
            durable_inbox_root_digest,
        })
    }

    #[must_use]
    pub const fn delivery_digest(&self) -> [u8; 32] {
        self.delivery_digest
    }

    #[must_use]
    pub const fn durable_inbox_digest(&self) -> [u8; 32] {
        self.durable_inbox_digest
    }

    #[must_use]
    pub const fn durable_inbox_sequence(&self) -> u64 {
        self.durable_inbox_sequence
    }

    #[must_use]
    pub const fn durable_inbox_root_digest(&self) -> [u8; 32] {
        self.durable_inbox_root_digest
    }
}

/// Actor-applied files are configured separately from Control. Existing files always need the
/// last persisted anchor, so an old-but-consistent pair cannot be silently substituted at restart.
#[derive(Clone, Debug)]
pub struct CopyActorAppliedArtifacts {
    journal_path: PathBuf,
    checkpoint_path: PathBuf,
    anchor: Option<ActorAppliedAnchor>,
}

impl CopyActorAppliedArtifacts {
    #[must_use]
    pub fn create_new(
        journal_path: impl Into<PathBuf>,
        checkpoint_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            journal_path: journal_path.into(),
            checkpoint_path: checkpoint_path.into(),
            anchor: None,
        }
    }

    #[must_use]
    pub fn open_existing(
        journal_path: impl Into<PathBuf>,
        checkpoint_path: impl Into<PathBuf>,
        anchor: ActorAppliedAnchor,
    ) -> Self {
        Self {
            journal_path: journal_path.into(),
            checkpoint_path: checkpoint_path.into(),
            anchor: Some(anchor),
        }
    }
}

/// Durable result which can be mapped to a Control `Applied` receipt only after its exact inbox
/// commitment is compared by the Node. It cannot express a physical execution result.
#[derive(Clone, Debug)]
pub struct CopyActorAppliedReceipt {
    commitment: CopyActorCommitment,
    actor_applied: ActorAppliedReceipt,
}

impl CopyActorAppliedReceipt {
    #[must_use]
    pub const fn commitment(&self) -> &CopyActorCommitment {
        &self.commitment
    }

    #[must_use]
    pub const fn actor_applied(&self) -> &ActorAppliedReceipt {
        &self.actor_applied
    }

    #[must_use]
    pub fn account_fact_digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"venue.copy.actor-applied.receipt.v1");
        digest.update(self.commitment.delivery_digest);
        digest.update(self.commitment.durable_inbox_digest);
        digest.update(self.commitment.durable_inbox_sequence.to_le_bytes());
        digest.update(self.commitment.durable_inbox_root_digest);
        digest.update(self.actor_applied.journal_root_sha256());
        digest.update(self.actor_applied.journal_tail_sequence().to_le_bytes());
        digest.finalize().into()
    }
}

impl AccountRuntime {
    /// Recovers a semantic receipt only from the already opened, verified Actor checkpoint.
    /// This does not mint a current turn token or restore any dispatch authority.
    pub fn recover_copy_actor_applied(
        &self,
        binding: &StrategyBinding,
        commitment: CopyActorCommitment,
    ) -> Result<Option<CopyActorAppliedReceipt>, AccountRuntimeError> {
        if binding.key.strategy_kind != StrategyKind::Copy
            || binding.key.account != self.account
            || !self.durable_recovery_complete
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registered| registered.binding != *binding)
        {
            return Err(AccountRuntimeError::ActorAppliedUnavailable);
        }
        let store = self
            .actor_applied_stores
            .get(&binding.key)
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?;
        let Some((actor_applied, checkpoint)) = store.recovered_actor_checkpoint()? else {
            return Ok(None);
        };
        // A later control turn may legitimately replace the latest Copy checkpoint. It cannot
        // stand in for the requested delivery's Applied receipt.
        let Ok(replay) = serde_json::from_slice::<CopyActorReplay>(&checkpoint) else {
            return Ok(None);
        };
        if replay.schema_version != COPY_REPLAY_SCHEMA_VERSION || replay.commitment != commitment {
            return Ok(None);
        }
        Ok(Some(CopyActorAppliedReceipt {
            commitment,
            actor_applied,
        }))
    }

    pub(crate) fn current_copy_actor_turn(
        &self,
        binding: &StrategyBinding,
        applied: &CopyActorAppliedReceipt,
    ) -> Result<crate::AppliedStrategyTurnReceipt, AccountRuntimeError> {
        if binding.key.strategy_kind != StrategyKind::Copy
            || binding.key.account != self.account
            || self.last_applied_durable.get(&binding.key) != Some(applied.actor_applied())
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        let token = self
            .last_applied_turns
            .get(&binding.key)
            .cloned()
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        Ok(crate::AppliedStrategyTurnReceipt::persisted(
            token,
            applied.actor_applied().clone(),
        ))
    }

    /// Installs the one durable Copy actor store for an already-registered exact Copy binding.
    /// The mutation WAL head is deliberately absent from this API; it can only arrive from the
    /// account's recovered durable state when `apply_copy_actor_turn` runs.
    pub fn install_copy_actor_applied_artifacts(
        &mut self,
        binding: &StrategyBinding,
        artifacts: CopyActorAppliedArtifacts,
    ) -> Result<(), AccountRuntimeError> {
        if binding.key.strategy_kind != StrategyKind::Copy
            || binding.key.account != self.account
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registered| registered.binding != *binding)
        {
            return Err(AccountRuntimeError::ActorAppliedStore);
        }
        let store = match artifacts.anchor {
            Some(anchor) => ActorAppliedTurnStore::open_existing(
                binding.clone(),
                artifacts.journal_path,
                artifacts.checkpoint_path,
                anchor,
            )?,
            None => ActorAppliedTurnStore::create_new(
                binding.clone(),
                artifacts.journal_path,
                artifacts.checkpoint_path,
            )?,
        };
        self.install_actor_applied_store(store)
    }

    /// Commits a Copy semantic turn using the exact recovered WAL head held by this runtime.
    /// It never queues or returns an `ExecutionCommand`; physical mutation remains behind the
    /// existing account lane, risk checks, WAL and one-time dispatch permit.
    pub fn apply_copy_actor_turn(
        &mut self,
        binding: &StrategyBinding,
        commitment: CopyActorCommitment,
    ) -> Result<CopyActorAppliedReceipt, AccountRuntimeError> {
        if binding.key.strategy_kind != StrategyKind::Copy
            || binding.key.account != self.account
            || self.health != AccountHealth::Ready
            || !self.durable_recovery_complete
            || self.active_turns.contains_key(&binding.key)
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registered| registered.binding != *binding)
        {
            return Err(AccountRuntimeError::AccountUnavailable);
        }
        let wal = self
            .actor_applied_wal_head
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?;
        let private_generation = self.last_reconciliation_generation;
        if private_generation == 0 {
            return Err(AccountRuntimeError::ActorAppliedUnavailable);
        }
        let turn_sequence = self
            .turn_sequences
            .get(&binding.key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        let config_epoch = self
            .registry
            .registration(&binding.key)
            .ok_or(AccountRuntimeError::ActorMissing)?
            .config_epoch;
        let token = StrategyTurnToken::issue(
            binding.key.clone(),
            self.connection_generation(),
            private_generation,
            binding.config_digest.clone(),
            config_epoch,
            turn_sequence,
        )
        .map_err(|_| AccountRuntimeError::StrategyTurnAuthority)?;
        let replay = serde_json::to_vec(&CopyActorReplay {
            schema_version: COPY_REPLAY_SCHEMA_VERSION,
            commitment,
        })
        .map_err(venue_storage::ActorAppliedError::Encode)?;
        let applied_private_sequence = self.applied_private_sequence();
        let durable = self
            .actor_applied_stores
            .get_mut(&binding.key)
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?
            .commit(&token, wal, applied_private_sequence, None, replay)?;
        self.turn_sequences
            .insert(binding.key.clone(), turn_sequence);
        self.last_applied_turns.insert(binding.key.clone(), token);
        self.last_applied_durable
            .insert(binding.key.clone(), durable.clone());
        Ok(CopyActorAppliedReceipt {
            commitment,
            actor_applied: durable,
        })
    }

    /// Admits one Node-derived physical command only when it is bound to the exact durable Copy
    /// Actor turn. Node may translate Copy's semantic exposure using fresh rules, but it cannot
    /// construct a lane request, dispatch permit, or alternate WAL path.
    pub(crate) fn admit_copy_actor_command(
        &mut self,
        binding: &StrategyBinding,
        applied: &CopyActorAppliedReceipt,
        priority: AccountLanePriority,
        command: ExecutionCommand,
        allocation: DurableCommandIdentityAllocation,
    ) -> Result<(), AccountRuntimeError> {
        if binding.key.strategy_kind != StrategyKind::Copy
            || binding.key.account != self.account
            || self.health != AccountHealth::Ready
            || !self.durable_recovery_complete
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registered| registered.binding != *binding)
            || self.last_applied_durable.get(&binding.key) != Some(applied.actor_applied())
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        let token = self
            .last_applied_turns
            .get(&binding.key)
            .cloned()
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        let durable = self
            .last_applied_durable
            .get(&binding.key)
            .cloned()
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        let turn = crate::AppliedStrategyTurnReceipt::persisted(token, durable);
        let identity =
            CommandIdentityReceipt::from_durable_allocation(&turn, &command, allocation)?;
        let intent = AccountExecutionIntent::from_applied_turn(&turn, priority, command, identity)?;
        self.enqueue_execution(intent)
    }

    #[must_use]
    pub const fn recovered_wal_head_for_copy(&self) -> Option<DurableWalHead> {
        self.actor_applied_wal_head
    }
}

#[derive(Deserialize, Serialize)]
struct CopyActorReplay {
    schema_version: u16,
    commitment: CopyActorCommitment,
}

fn is_zero(digest: &[u8; 32]) -> bool {
    digest.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountKey, ExchangeId, StrategyInstanceKey};

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        let account = AccountKey::new(ExchangeId::Binance, "00000000-0000-4000-8000-000000000001")?;
        let key =
            StrategyInstanceKey::new(account, StrategyKind::Copy, "copy-a", "BTC/USDT".parse()?)?;
        Ok(StrategyBinding::new(
            key,
            "follower-binding",
            "copy-plan-digest",
        )?)
    }

    #[test]
    fn copy_actor_cannot_apply_without_recovered_runtime_wal_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let mut runtime = AccountRuntime::new(binding.key.account.clone());
        runtime.register_strategy(binding.clone())?;
        let directory = tempfile::tempdir()?;
        runtime.install_copy_actor_applied_artifacts(
            &binding,
            CopyActorAppliedArtifacts::create_new(
                directory.path().join("copy-applied.jsonl"),
                directory.path().join("copy-checkpoint.json"),
            ),
        )?;
        let commitment = CopyActorCommitment::new([1; 32], [2; 32], 1, [3; 32])?;
        assert!(matches!(
            runtime.apply_copy_actor_turn(&binding, commitment),
            Err(AccountRuntimeError::AccountUnavailable)
        ));
        assert!(runtime.recovered_wal_head_for_copy().is_none());
        Ok(())
    }
}
