use venue_execution::{RuntimeBootstrapReceipt, SignedAccountPositionMode, SignedUnknownResult};
use venue_gateway_api::GatewayMode;

use super::{AccountHealth, AccountPositionMode, AccountRuntime, AccountRuntimeError};

impl AccountRuntime {
    /// Host-issued, fsynced checkpoint from the shared account WAL plus a complete signed read.
    /// The opaque receipt cannot be assembled by Node, Control, or a strategy.
    pub fn install_production_signed_bootstrap(
        &mut self,
        bootstrap: &RuntimeBootstrapReceipt,
    ) -> Result<(), AccountRuntimeError> {
        let snapshot = bootstrap.snapshot();
        let binding = snapshot.binding();
        if self.durable_recovery_complete
            || binding.mode != GatewayMode::Live
            || binding.venue.as_str() != self.account.exchange.as_str()
            || binding.trading_account_id != self.account.account
            || snapshot.connection_generation() == 0
            || snapshot.private_generation() == 0
            || snapshot.rules_generation() == 0
        {
            return Err(AccountRuntimeError::RecoveryStateMismatch);
        }
        let mut router = self.private_router.clone();
        router.activate_generation(
            snapshot.connection_generation(),
            self.last_applied_private_sequence,
        )?;
        self.durable_recovery_complete = true;
        self.recovered_gateway_mode = Some(GatewayMode::Live);
        self.recovered_position_mode = Some(match snapshot.position_mode() {
            SignedAccountPositionMode::Net => AccountPositionMode::Net,
            SignedAccountPositionMode::Hedge => AccountPositionMode::Hedge,
        });
        self.connection_generation = snapshot.connection_generation();
        self.private_router = router;
        self.last_reconciliation_generation = snapshot.private_generation();
        self.actor_applied_wal_head = Some(bootstrap.wal_head());
        self.physical_private_generation_floor = snapshot.private_generation();
        self.production_signed_bootstrap = true;
        self.production_risk_fenced = bootstrap.risk_fenced();
        self.production_rules_generation = snapshot.rules_generation();
        self.health = AccountHealth::Ready;
        self.fault = None;
        Ok(())
    }

    /// Refreshes only Host-persisted signed risk facts. Generation and mode are revalidated
    /// against the already-running authority; this never resumes a paused operator lifecycle or
    /// rewrites an existing Actor-applied receipt.
    pub(crate) fn refresh_production_signed_snapshot(
        &mut self,
        refresh: &RuntimeBootstrapReceipt,
    ) -> Result<venue_execution::SignedAccountSnapshot, AccountRuntimeError> {
        let snapshot = refresh.snapshot();
        let binding = snapshot.binding();
        let position_mode = match snapshot.position_mode() {
            SignedAccountPositionMode::Net => AccountPositionMode::Net,
            SignedAccountPositionMode::Hedge => AccountPositionMode::Hedge,
        };
        if !self.production_signed_bootstrap
            || !self.durable_recovery_complete
            || self.health != AccountHealth::Ready
            || binding.mode != GatewayMode::Live
            || binding.venue.as_str() != self.account.exchange.as_str()
            || binding.trading_account_id != self.account.account
            || snapshot.connection_generation() != self.connection_generation
            || snapshot.private_generation() < self.last_reconciliation_generation
            || snapshot.rules_generation() != self.production_rules_generation
            || self.recovered_position_mode != Some(position_mode)
        {
            return Err(AccountRuntimeError::RecoveryStateMismatch);
        }
        self.advance_resident_wal_head(refresh.wal_head())?;
        for fact in snapshot.unknown_results() {
            if !matches!(fact.result, SignedUnknownResult::Unknown) {
                self.execution_lane
                    .settle_host_signed_unknown(&fact.command_id)?;
            }
        }
        if snapshot.private_generation() > self.last_reconciliation_generation {
            let next_dispatch_revision = self.next_dispatch_revision()?;
            self.last_reconciliation_generation = snapshot.private_generation();
            self.physical_private_generation_floor = snapshot.private_generation();
            self.active_turns.clear();
            self.last_applied_turns.clear();
            self.last_applied_durable.clear();
            let _discarded = self.execution_lane.discard_all_queued();
            self.install_dispatch_revision(next_dispatch_revision);
        }
        self.production_risk_fenced = refresh.risk_fenced();
        Ok(snapshot.clone())
    }
}
