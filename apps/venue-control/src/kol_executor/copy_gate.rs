use super::BinanceCommandLedgerError;
use sqlx::{PgConnection, Row};

pub(super) async fn settle_copy_target(
    connection: &mut PgConnection,
    command: &sqlx::postgres::PgRow,
    execution: Option<&crate::executor_exchange::MarketSettlement>,
    now: i64,
) -> Result<(), BinanceCommandLedgerError> {
    let origin: String = command
        .try_get("command_origin")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    if origin != "copy" {
        return Ok(());
    }
    let execution = execution.ok_or(BinanceCommandLedgerError::Conflict)?;
    if command
        .try_get::<Option<String>, _>("native_order_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .is_none()
    {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let baseline = command
        .try_get::<Option<serde_json::Value>, _>("market_baseline")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    let baseline: crate::executor_exchange::MarketBaseline =
        serde_json::from_value(baseline).map_err(|_| BinanceCommandLedgerError::Conflict)?;
    let phase: String = command
        .try_get("command_phase")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let expected = if phase == "close" {
        baseline
            .before_quantity
            .checked_sub(execution.executed_quantity)
    } else {
        baseline
            .before_quantity
            .checked_add(execution.executed_quantity)
    };
    if execution.executed_quantity <= rust_decimal::Decimal::ZERO
        || execution.executed_quantity > baseline.order_quantity
        || expected != Some(execution.position_quantity)
        || execution.position_quantity < rust_decimal::Decimal::ZERO
        || execution.observed_ms <= baseline.observed_ms
    {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let relation: String = command
        .try_get("relation_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let symbol: String = command
        .try_get("symbol")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let side: String = command
        .try_get("position_side")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let changed = sqlx::query("UPDATE venue_kol_copy_targets SET observed_quantity=$4,target_revision=target_revision+1,dirty=(target_quantity::numeric<>$4::numeric),updated_ms=$5 WHERE relation_id=$1 AND symbol=$2 AND position_side=$3 AND observed_quantity::numeric=$6::numeric")
        .bind(relation).bind(symbol).bind(side).bind(execution.position_quantity.to_string()).bind(now)
        .bind(baseline.before_quantity.to_string()).execute(connection).await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?.rows_affected();
    (changed == 1)
        .then_some(())
        .ok_or(BinanceCommandLedgerError::Conflict)
}

/// Relation -> credential -> command is also the lifecycle/producer lock order. A pause that
/// wins the relation lock cancels Pending work; a claim that wins first is already Sending and
/// is left for exact reconciliation, never reinterpreted under the new relation revision.
pub(super) async fn lock_account_claim(
    connection: &mut PgConnection,
    trading_account_id: &str,
    now: i64,
) -> Result<(), BinanceCommandLedgerError> {
    sqlx::query("SELECT relation_id FROM venue_kol_follow_relations WHERE follower_trading_account_id=$1 ORDER BY relation_id FOR SHARE")
        .bind(trading_account_id)
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let credentials = sqlx::query("SELECT credential_id FROM venue_api_credentials WHERE trading_account_id=$1 AND deleted_ms IS NULL ORDER BY credential_id FOR UPDATE")
        .bind(trading_account_id)
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    if credentials.is_empty() {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    sqlx::query(
        "UPDATE venue_binance_commands c SET command_state='cancelled',terminal_ms=$2,updated_ms=$2,\
         sanitized_error_code='copy_revision_retired' \
         WHERE c.trading_account_id=$1 AND c.command_origin='copy' AND c.command_state='pending' \
         AND NOT EXISTS (SELECT 1 FROM venue_kol_follow_relations r \
           JOIN venue_kol_profiles p ON p.kol_user_id=r.kol_user_id \
           WHERE r.relation_id=c.relation_id AND r.relation_state='active' \
             AND p.profile_state='enabled' AND r.revision=c.relation_revision \
             AND ((r.baseline_json->>'target_model'='1' AND c.mirror_order_id IS NULL) OR (r.baseline_json->>'target_model'='2' AND c.mirror_order_id IS NOT NULL)) \
             AND r.follower_trading_account_id=c.trading_account_id \
             AND r.credential_id=c.credential_id \
             AND r.allowed_symbols @> jsonb_build_array(c.symbol)) \
         AND NOT (c.command_phase='cancel' AND c.mirror_order_id IS NOT NULL AND EXISTS(SELECT 1 FROM venue_order_mirrors m JOIN venue_kol_follow_relations r ON r.relation_id=m.relation_id WHERE m.mirror_id=c.mirror_order_id AND m.child_native_order_id=c.selected_native_order_id AND r.follower_trading_account_id=c.trading_account_id AND r.credential_id=c.credential_id))",
    )
    .bind(trading_account_id)
    .bind(now)
    .execute(&mut *connection)
    .await
    .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    Ok(())
}

pub(crate) async fn cancel_pending_copy_commands(
    connection: &mut PgConnection,
    relation_id: &str,
    now: i64,
    reason: &str,
) -> Result<(), sqlx::Error> {
    // The caller owns the relation row lock. In-flight commands and their target evidence survive
    // a pause; deleting or cancelling them would lose the only durable physical-order identity.
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$2,updated_ms=$2,\
         sanitized_error_code=$3 WHERE relation_id=$1 AND command_origin='copy' \
         AND command_state='pending' AND NOT (mirror_order_id IS NOT NULL AND command_phase='cancel')",
    )
    .bind(relation_id)
    .bind(now)
    .bind(reason)
    .execute(&mut *connection)
    .await?;
    Ok(())
}
