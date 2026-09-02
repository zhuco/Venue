//! PostgreSQL facts owned by the singleton executor; no local journal is created.

use serde_json::json;
use sqlx::{PgPool, Row};

use crate::kol_executor::{
    BinanceCommandLedger, BinanceCommandLedgerError, ClaimedBinanceCommand, KolSourceFill,
};
use venue_control_protocol::kol::ExecutorCommandState;

#[derive(Clone)]
pub struct PgExecutorStore {
    pool: PgPool,
}

impl PgExecutorStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Durable native trade identity makes repeated WS frames and restart replay idempotent.
    pub async fn record_source_fill(
        &self,
        kol_user_id: &str,
        fill: &KolSourceFill,
    ) -> Result<bool, BinanceCommandLedgerError> {
        let inserted = sqlx::query("INSERT INTO venue_kol_source_fills (kol_trading_account_id,kol_user_id,native_symbol,native_trade_id,symbol,order_side,position_side,quantity,price,occurred_ms,observed_ms,payload_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING")
            .bind(&fill.leader_trading_account_id).bind(kol_user_id).bind(&fill.native_symbol).bind(&fill.native_trade_id)
            .bind(&fill.symbol).bind(order_side(fill.order_side)).bind(position_side(fill.position_side))
            .bind(fill.quantity.to_string()).bind(fill.price.to_string()).bind(ms(fill.occurred_ms)?).bind(ms(fill.observed_ms)?).bind(fill.payload_digest.as_slice())
            .execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        Ok(inserted.rows_affected() == 1)
    }

    /// Restart recovery only returns identities to read back. It deliberately does not make a
    /// Sending or ReconcileRequired command eligible for another POST.
    pub async fn recover_nonterminal(
        &self,
    ) -> Result<Vec<ClaimedBinanceCommand>, BinanceCommandLedgerError> {
        let rows = sqlx::query("SELECT command_id,trading_account_id,credential_id,client_order_id,command_state FROM venue_binance_commands WHERE command_state IN ('pending','sending','accepted','reconcile_required') ORDER BY created_ms,command_id")
            .fetch_all(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        rows.into_iter().map(recovery_row).collect()
    }

    /// Atomically changes one committed Pending command to Sending. The underlying PostgreSQL
    /// predicate fences a second claimant and later commands behind uncertain work.
    pub async fn claim_next_command(
        &self,
        trading_account_id: &str,
        now_ms: u64,
    ) -> Result<Option<ClaimedBinanceCommand>, BinanceCommandLedgerError> {
        BinanceCommandLedger::new(self.pool.clone())
            .claim_next(trading_account_id, now_ms)
            .await
    }

    /// Exposes only the command ledger's forward-only transition table. In particular, callers
    /// cannot turn Sending back into Pending after a timeout.
    pub async fn transition_command(
        &self,
        command_id: &str,
        next: ExecutorCommandState,
        now_ms: u64,
        sanitized_error_code: Option<&str>,
    ) -> Result<(), BinanceCommandLedgerError> {
        BinanceCommandLedger::new(self.pool.clone())
            .settle(command_id, next, now_ms, sanitized_error_code)
            .await
    }

    /// A successful, independently signed baseline is the only path that promotes a requested
    /// relation. The singleton owns the slot allocation transaction.
    pub async fn complete_activation(
        &self,
        relation_id: &str,
        revision: u64,
        baseline_ms: u64,
    ) -> Result<(), BinanceCommandLedgerError> {
        let revision = i64::try_from(revision).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let now = ms(baseline_ms)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let slot: Option<i16> = sqlx::query_scalar("SELECT s::smallint FROM generate_series(1,200) s WHERE NOT EXISTS (SELECT 1 FROM venue_kol_follow_relations r WHERE r.active_slot=s) LIMIT 1")
            .fetch_optional(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let slot = slot.ok_or(BinanceCommandLedgerError::Conflict)?;
        let changed = sqlx::query("UPDATE venue_kol_follow_relations r SET relation_state='active',active_slot=$1,baseline_json=$2,attention_code=NULL,updated_ms=$3 FROM venue_kol_activation_requests a WHERE r.relation_id=$4 AND a.relation_id=r.relation_id AND a.request_state='pending' AND a.relation_revision=$5 AND r.relation_state='paused' AND r.revision=$5")
            .bind(slot).bind(json!({"baseline_ms": baseline_ms})).bind(now).bind(relation_id).bind(revision)
            .execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if changed.rows_affected() != 1 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        sqlx::query("UPDATE venue_kol_activation_requests SET request_state='completed',updated_ms=$1 WHERE relation_id=$2 AND request_state='pending'")
            .bind(now).bind(relation_id).execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        tx.commit()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)
    }

    pub async fn reject_activation(
        &self,
        relation_id: &str,
        now_ms: u64,
        reason: &str,
    ) -> Result<(), BinanceCommandLedgerError> {
        if reason.is_empty() || reason.len() > 64 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let changed = sqlx::query("UPDATE venue_kol_activation_requests SET request_state='rejected',sanitized_reason=$1,updated_ms=$2 WHERE relation_id=$3 AND request_state='pending'")
            .bind(reason).bind(ms(now_ms)?).bind(relation_id).execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        (changed.rows_affected() == 1)
            .then_some(())
            .ok_or(BinanceCommandLedgerError::Conflict)
    }
}

fn recovery_row(
    row: sqlx::postgres::PgRow,
) -> Result<ClaimedBinanceCommand, BinanceCommandLedgerError> {
    let state = match row
        .try_get::<String, _>("command_state")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .as_str()
    {
        "pending" => venue_control_protocol::kol::ExecutorCommandState::Pending,
        "sending" => venue_control_protocol::kol::ExecutorCommandState::Sending,
        "accepted" => venue_control_protocol::kol::ExecutorCommandState::Accepted,
        "reconcile_required" => {
            venue_control_protocol::kol::ExecutorCommandState::ReconcileRequired
        }
        _ => return Err(BinanceCommandLedgerError::Unavailable),
    };
    Ok(ClaimedBinanceCommand {
        command_id: row
            .try_get("command_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        trading_account_id: row
            .try_get("trading_account_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        credential_id: row
            .try_get("credential_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        client_order_id: row
            .try_get("client_order_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        state,
    })
}
fn ms(value: u64) -> Result<i64, BinanceCommandLedgerError> {
    i64::try_from(value).map_err(|_| BinanceCommandLedgerError::Conflict)
}
fn order_side(side: venue_domain::domain::OrderSide) -> &'static str {
    match side {
        venue_domain::domain::OrderSide::Buy => "buy",
        venue_domain::domain::OrderSide::Sell => "sell",
    }
}
fn position_side(side: venue_domain::domain::PositionSide) -> &'static str {
    match side {
        venue_domain::domain::PositionSide::Long => "long",
        venue_domain::domain::PositionSide::Short => "short",
        venue_domain::domain::PositionSide::Net => "net",
    }
}
