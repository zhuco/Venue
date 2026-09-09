use venue_domain::Symbol;

use super::{NoopReason, SupportMartingaleStore, SupportMartingaleStoreError};

impl NoopReason {
    pub(crate) const fn waiting_status(self) -> &'static str {
        match self {
            Self::InvalidInput => "waiting_invalid_input",
            Self::StaleAccount => "waiting_account_freshness",
            Self::MissingReference => "waiting_reference",
            Self::BasisTooWide => "waiting_basis",
            Self::EnvironmentBlocked => "waiting_symbol_environment",
            Self::BtcDown => "waiting_btc_down",
            Self::BtcWarmup => "waiting_btc_warmup",
            Self::BtcNeutral => "waiting_btc_neutral",
            Self::NoSupport => "waiting_support",
            Self::CallbackNotConfirmed => "waiting_callback",
            Self::SupportConsumed => "waiting_unused_support",
            Self::BudgetExhausted => "waiting_budget",
            Self::PositionLimit => "waiting_position_limit",
            Self::QuantityTooSmall => "waiting_amount",
            Self::ExitOnly => "waiting_exit_only",
            Self::TakeProfitBlocked => "waiting_take_profit",
            Self::WaitingForCancel => "waiting_cancel",
        }
    }
}

impl SupportMartingaleStore {
    pub(crate) async fn record_waiting(
        &self,
        owner: &str,
        instance_id: &str,
        symbol: &Symbol,
        reason: NoopReason,
        now: u64,
    ) -> Result<(), SupportMartingaleStoreError> {
        let now = i64::try_from(now).map_err(|_| SupportMartingaleStoreError::Invalid)?;
        // A diagnostic cannot replace pending, protective, cooldown or holding state.
        sqlx::query("UPDATE venue_support_martingale_symbol_states s SET status=$1,updated_ms=$2 FROM venue_support_martingale_instances i WHERE s.instance_id=i.instance_id AND i.owner_user_id=$3 AND i.instance_id=$4 AND s.symbol=$5 AND i.lifecycle='running' AND s.quantity=0 AND s.pending_command_id IS NULL AND s.health_reason IS NULL AND (s.status='idle' OR left(s.status,8)='waiting_') AND s.status<>$1")
            .bind(reason.waiting_status()).bind(now).bind(owner).bind(instance_id)
            .bind(symbol.to_string()).execute(&self.pool).await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        Ok(())
    }
}
