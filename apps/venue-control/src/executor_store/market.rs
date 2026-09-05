use super::*;
use crate::executor_exchange::{MarketBaseline, MarketSettlement};

impl PgExecutorStore {
    pub async fn persist_market_baseline(
        &self,
        command_id: &str,
        baseline: &MarketBaseline,
    ) -> Result<(), BinanceCommandLedgerError> {
        let value =
            serde_json::to_value(baseline).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let affected = sqlx::query("UPDATE venue_binance_commands c SET market_baseline=$2 WHERE command_id=$1 AND order_kind='market' AND command_state='sending' AND market_baseline IS NULL AND (command_origin<>'copy' OR EXISTS (SELECT 1 FROM venue_kol_copy_targets t WHERE t.relation_id=c.relation_id AND t.symbol=c.symbol AND t.position_side=c.position_side AND t.observed_quantity::numeric=$3::numeric))")
            .bind(command_id).bind(value).bind(baseline.before_quantity.to_string()).execute(&self.pool).await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?.rows_affected();
        (affected == 1)
            .then_some(())
            .ok_or(BinanceCommandLedgerError::Conflict)
    }

    pub async fn market_baseline(
        &self,
        command_id: &str,
    ) -> Result<Option<MarketBaseline>, BinanceCommandLedgerError> {
        let value: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT market_baseline FROM venue_binance_commands WHERE command_id=$1",
        )
        .bind(command_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| BinanceCommandLedgerError::Conflict)
    }

    pub async fn reconcile_with_execution(
        &self,
        command_id: &str,
        now_ms: u64,
        native_order_id: Option<&str>,
        execution: Option<&MarketSettlement>,
    ) -> Result<(), BinanceCommandLedgerError> {
        BinanceCommandLedger::new(self.pool.clone())
            .settle_with_execution(
                command_id,
                ExecutorCommandState::Reconciled,
                now_ms,
                None,
                native_order_id,
                execution,
            )
            .await
    }
}
