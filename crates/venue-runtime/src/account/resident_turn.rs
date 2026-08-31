use std::path::PathBuf;

use venue_storage::ActorAppliedAnchor;

use super::{AccountHealth, AccountRuntime, AccountRuntimeError, StrategyBinding, StrategyKind};
use crate::{
    AppliedStrategyTurnReceipt, StrategyTurnToken,
    strategy::{ActorAppliedTurnStore, StrategyInput, StrategyTurn},
};

/// Files owned by one resident actor.  The persisted anchor is required when reopening so an
/// internally consistent but older journal/checkpoint pair cannot be substituted after restart.
#[derive(Clone, Debug)]
pub struct ResidentActorAppliedArtifacts {
    journal_path: PathBuf,
    checkpoint_path: PathBuf,
    anchor: Option<ActorAppliedAnchor>,
}

impl ResidentActorAppliedArtifacts {
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

    pub(crate) fn open_store(
        self,
        binding: StrategyBinding,
    ) -> Result<ActorAppliedTurnStore, AccountRuntimeError> {
        match self.anchor {
            Some(anchor) => ActorAppliedTurnStore::open_existing(
                binding,
                self.journal_path,
                self.checkpoint_path,
                anchor,
            )
            .map_err(AccountRuntimeError::ActorApplied),
            None => {
                ActorAppliedTurnStore::create_new(binding, self.journal_path, self.checkpoint_path)
                    .map_err(AccountRuntimeError::ActorApplied)
            }
        }
    }
}

impl AccountRuntime {
    /// Returns the opaque actor checkpoint already verified by the runtime-owned Actor Applied
    /// store. Node may deserialize only its own strategy state; it cannot access the store or
    /// manufacture an anchor.
    pub fn resident_actor_checkpoint(
        &self,
        binding: &StrategyBinding,
    ) -> Result<Option<Vec<u8>>, AccountRuntimeError> {
        if binding.key.account != self.account
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registration| registration.binding != *binding)
        {
            return Err(AccountRuntimeError::ActorMissing);
        }
        self.actor_applied_stores
            .get(&binding.key)
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?
            .recovered_actor_checkpoint()
            .map(|checkpoint| checkpoint.map(|(_, bytes)| bytes))
            .map_err(AccountRuntimeError::ActorApplied)
    }

    /// Returns the opaque manual-control portion of the existing actor checkpoint. This shares
    /// the actor's journal, receipt and anchor; callers cannot install a separate checkpoint.
    pub fn resident_manual_checkpoint(
        &self,
        binding: &StrategyBinding,
    ) -> Result<Option<Vec<u8>>, AccountRuntimeError> {
        if binding.key.account != self.account
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registration| registration.binding != *binding)
        {
            return Err(AccountRuntimeError::ActorMissing);
        }
        self.actor_applied_stores
            .get(&binding.key)
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?
            .recovered_manual_checkpoint()
            .map_err(AccountRuntimeError::ActorApplied)
    }

    /// Begins the next lossless private delivery for a resident actor. It exposes no execution
    /// permit; callers must commit it through [`persist_private_strategy_turn`] before physical
    /// preparation can bind to the resulting durable receipt.
    pub fn begin_private_strategy_turn(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<Option<StrategyTurn>, AccountRuntimeError> {
        if binding.key.account != self.account {
            return Err(AccountRuntimeError::ActorMissing);
        }
        let turn = self.pop_strategy_input(&binding.key)?;
        if turn
            .as_ref()
            .is_some_and(|turn| !matches!(turn.input(), StrategyInput::Private(_)))
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        Ok(turn)
    }

    /// Commits only the active private turn created by [`begin_private_strategy_turn`]. This
    /// preserves the exact private delivery cursor in Actor Applied and cannot acknowledge a
    /// control, market, or caller-constructed turn.
    pub fn persist_private_strategy_turn(
        &mut self,
        binding: &StrategyBinding,
        replay_state: Vec<u8>,
    ) -> Result<AppliedStrategyTurnReceipt, AccountRuntimeError> {
        if binding.key.account != self.account
            || !matches!(
                self.active_turns.get(&binding.key).map(|turn| &turn.input),
                Some(StrategyInput::Private(_))
            )
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        self.persist_and_acknowledge_strategy_turn(&binding.key, replay_state)
    }

    /// Commits a manual-control replay without replacing the registered strategy's opaque
    /// checkpoint. Copy has a specialized recovery receipt and deliberately remains excluded.
    pub fn persist_resident_manual_turn(
        &mut self,
        binding: &StrategyBinding,
        manual_checkpoint: Vec<u8>,
        permits_risk_increase: bool,
    ) -> Result<AppliedStrategyTurnReceipt, AccountRuntimeError> {
        if manual_checkpoint.is_empty()
            || binding.key.strategy_kind == StrategyKind::Copy
            || binding.key.account != self.account
            || !self.durable_recovery_complete
            || self.active_turns.contains_key(&binding.key)
            || self.has_pending_private_delivery(&binding.key)
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registration| registration.binding != *binding)
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        if permits_risk_increase
            && (self.health != AccountHealth::Ready
                || self.production_new_risk_fenced()
                || !self
                    .registry
                    .registration(&binding.key)
                    .is_some_and(|registration| registration.lifecycle.accepts_new_risk()))
        {
            return Err(AccountRuntimeError::RiskFenced);
        }
        self.persist_resident_manual_checkpoint(binding, manual_checkpoint, None)
    }

    /// Atomically acknowledges the active private delivery as manual-owned while retaining the
    /// strategy replay. This prevents a manual fill from reaching the Grid reducer.
    pub fn persist_manual_private_strategy_turn(
        &mut self,
        binding: &StrategyBinding,
        manual_checkpoint: Vec<u8>,
    ) -> Result<AppliedStrategyTurnReceipt, AccountRuntimeError> {
        if manual_checkpoint.is_empty()
            || binding.key.strategy_kind == StrategyKind::Copy
            || binding.key.account != self.account
            || !matches!(
                self.active_turns.get(&binding.key).map(|turn| &turn.input),
                Some(StrategyInput::Private(_))
            )
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        self.persist_resident_manual_checkpoint(binding, manual_checkpoint, Some(()))
    }

    /// Installs the one recovered Actor-applied store for any registered resident strategy.
    /// This is intentionally narrower than exposing the store itself to Node or Control.
    pub fn install_resident_actor_applied_artifacts(
        &mut self,
        binding: &StrategyBinding,
        artifacts: ResidentActorAppliedArtifacts,
    ) -> Result<(), AccountRuntimeError> {
        if binding.key.account != self.account
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registration| registration.binding != *binding)
        {
            return Err(AccountRuntimeError::ActorAppliedStore);
        }
        let store = artifacts.open_store(binding.clone())?;
        self.install_actor_applied_store(store)
    }

    /// Promotes a newly registered resident actor only after the Host-installed signed bootstrap
    /// and its own durable checkpoint are present.  Existing orders, positions, or UNKNOWN keep
    /// the account-wide production fence in place, so this cannot manufacture entry authority.
    pub fn activate_resident_strategy(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), AccountRuntimeError> {
        if binding.key.account != self.account
            || self.health != AccountHealth::Ready
            || !self.durable_recovery_complete
            || !self.actor_applied_stores.contains_key(&binding.key)
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registration| registration.binding != *binding)
        {
            return Err(AccountRuntimeError::AccountUnavailable);
        }
        if self.production_new_risk_fenced() {
            // A complete signed bootstrap with inventory or UNKNOWN remains a valid control and
            // reduction host.  Keep this actor paused rather than rejecting startup or granting
            // it entry authority; the account-wide Host fence still rejects new risk.
            self.registry.pause(&binding.key)?;
        } else {
            self.registry.mark_running(&binding.key)?;
        }
        Ok(())
    }

    /// The resident asks Runtime, rather than constructing a token, to durably apply a semantic
    /// turn.  The bytes are strategy checkpoint/replay data; this API neither accepts a command
    /// nor exposes an execution permit.  A later Host preparation binds any resulting command to
    /// this exact receipt and the single account WAL.
    pub fn persist_resident_semantic_turn(
        &mut self,
        binding: &StrategyBinding,
        replay_state: Vec<u8>,
    ) -> Result<AppliedStrategyTurnReceipt, AccountRuntimeError> {
        if replay_state.is_empty()
            || binding.key.account != self.account
            || self.health != AccountHealth::Ready
            || !self.durable_recovery_complete
            || self.active_turns.contains_key(&binding.key)
            || self.has_pending_private_delivery(&binding.key)
            || self
                .registry
                .registration(&binding.key)
                .is_none_or(|registration| registration.binding != *binding)
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
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
        let applied_private_sequence = self.applied_private_sequence();
        let durable = self
            .actor_applied_stores
            .get_mut(&binding.key)
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?
            .commit(&token, wal, applied_private_sequence, None, replay_state)?;
        let receipt = AppliedStrategyTurnReceipt::persisted(token.clone(), durable.clone());
        self.turn_sequences
            .insert(binding.key.clone(), turn_sequence);
        self.last_applied_turns.insert(binding.key.clone(), token);
        self.last_applied_durable
            .insert(binding.key.clone(), durable);
        Ok(receipt)
    }

    fn persist_resident_manual_checkpoint(
        &mut self,
        binding: &StrategyBinding,
        manual_checkpoint: Vec<u8>,
        private_turn: Option<()>,
    ) -> Result<AppliedStrategyTurnReceipt, AccountRuntimeError> {
        if private_turn.is_some() {
            return self
                .persist_and_acknowledge_manual_strategy_turn(&binding.key, manual_checkpoint);
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
        let applied_private_sequence = self.applied_private_sequence();
        let durable = self
            .actor_applied_stores
            .get_mut(&binding.key)
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?
            .commit_manual(
                &token,
                wal,
                applied_private_sequence,
                None,
                manual_checkpoint,
            )?;
        let receipt = AppliedStrategyTurnReceipt::persisted(token.clone(), durable.clone());
        self.turn_sequences
            .insert(binding.key.clone(), turn_sequence);
        self.last_applied_turns.insert(binding.key.clone(), token);
        self.last_applied_durable
            .insert(binding.key.clone(), durable);
        Ok(receipt)
    }

    /// Called only by the resident Host after it has durably advanced the shared command WAL.
    /// A stale or regressive head is rejected, preventing a later turn from binding itself to an
    /// old command history after a dispatch outcome or restart.
    pub(crate) fn advance_resident_wal_head(
        &mut self,
        next: venue_storage::DurableWalHead,
    ) -> Result<(), AccountRuntimeError> {
        let previous = self
            .actor_applied_wal_head
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?;
        if !self.durable_recovery_complete
            || next.tail_sequence() < previous.tail_sequence()
            || next.record_count() < previous.record_count()
        {
            return Err(AccountRuntimeError::ResidentWalHead);
        }
        self.actor_applied_wal_head = Some(next);
        Ok(())
    }
}
