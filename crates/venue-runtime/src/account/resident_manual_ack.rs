use super::*;

impl AccountRuntime {
    /// Commits a manual-owned private fact through the registered actor's existing checkpoint.
    /// Visibility stops at `account`: sibling `resident_turn` may invoke this narrow bridge but
    /// no caller outside Account Runtime can reach the private turn/WAL state it uses.
    pub(in super::super) fn persist_and_acknowledge_manual_strategy_turn(
        &mut self,
        key: &StrategyInstanceKey,
        manual_checkpoint: Vec<u8>,
    ) -> Result<AppliedStrategyTurnReceipt, AccountRuntimeError> {
        self.reject_drifted_physical_authority()?;
        let active = self
            .active_turns
            .get(key)
            .cloned()
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        let applied_private_sequence =
            self.prospective_applied_private_sequence(key, &active.input)?;
        let wal = self
            .actor_applied_wal_head
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?;
        let private_delivery = match &active.input {
            StrategyInput::Private(fact) => Some(AppliedPrivateDelivery {
                evidence_sequence: fact.evidence().sequence(),
                fact_index: fact.fact_index(),
            }),
            _ => None,
        };
        let durable = self
            .actor_applied_stores
            .get_mut(key)
            .ok_or(AccountRuntimeError::ActorAppliedUnavailable)?
            .commit_manual(
                &active.token,
                wal,
                applied_private_sequence,
                private_delivery,
                manual_checkpoint,
            )?;
        let receipt = AppliedStrategyTurnReceipt::persisted(active.token, durable);
        self.acknowledge_durable_strategy_turn(receipt.clone())?;
        Ok(receipt)
    }
}
