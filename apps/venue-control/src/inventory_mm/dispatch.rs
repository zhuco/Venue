use crate::BinanceCommandLedgerError;
use sqlx::{PgConnection, PgPool};

/// Called with the shared credential/account queue lock held. Unsent stale quotes are
/// cancelled locally, never relabelled/replayed; sent work remains in reconciliation.
pub(crate) async fn retire_pending(
    connection: &mut PgConnection,
    account: &str,
    now: i64,
) -> Result<(), BinanceCommandLedgerError> {
    sqlx::query("UPDATE venue_binance_commands c SET command_state='cancelled',terminal_ms=$2,updated_ms=$2,sanitized_error_code='inventory_mm_gate_retired' WHERE c.trading_account_id=$1 AND c.command_origin='inventory_mm' AND c.command_state='pending' AND c.command_phase<>'cancel' AND (c.created_ms<$2-5000 OR NOT EXISTS(SELECT 1 FROM venue_inventory_mm_instances i JOIN venue_api_credentials k ON k.credential_id=i.credential_id AND k.user_id=i.owner_user_id AND k.trading_account_id=i.trading_account_id WHERE i.instance_id=c.inventory_mm_instance_id AND i.instance_state='running' AND i.owner_user_id=c.owner_user_id AND i.trading_account_id=c.trading_account_id AND i.credential_id=c.credential_id AND i.symbol=c.symbol AND k.deleted_ms IS NULL AND k.venue='binance' AND k.verification_json->>'verification'='verified'))")
        .bind(account).bind(now).execute(connection).await.map_err(|_|BinanceCommandLedgerError::Unavailable)?;
    Ok(())
}

/// Rechecks the durable lifecycle immediately before physical dispatch. Cancels carry no
/// inventory increase and remain allowed after a halt, but only for this strategy's own ID.
pub(crate) async fn permits(
    pool: &PgPool,
    command_id: &str,
    now: u64,
) -> Result<bool, BinanceCommandLedgerError> {
    let now = i64::try_from(now).map_err(|_| BinanceCommandLedgerError::Conflict)?;
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands c JOIN venue_inventory_mm_instances i ON i.instance_id=c.inventory_mm_instance_id AND i.owner_user_id=c.owner_user_id AND i.trading_account_id=c.trading_account_id AND i.credential_id=c.credential_id AND i.symbol=c.symbol JOIN venue_api_credentials k ON k.credential_id=i.credential_id AND k.user_id=i.owner_user_id AND k.trading_account_id=i.trading_account_id WHERE c.command_id=$1 AND c.command_origin='inventory_mm' AND c.command_state='sending' AND k.deleted_ms IS NULL AND k.venue='binance' AND k.verification_json->>'verification'='verified' AND (c.command_phase='cancel' AND EXISTS(SELECT 1 FROM venue_binance_commands owned WHERE owned.inventory_mm_instance_id=i.instance_id AND owned.client_order_id=c.target_client_order_id AND owned.command_phase IN ('open','close') AND owned.command_state='reconciled') OR (c.command_phase IN ('open','close') AND i.instance_state='running' AND c.created_ms>=$2-5000 AND c.created_ms<=$2 AND EXISTS(SELECT 1 FROM venue_binance_account_projections p WHERE p.credential_id=i.credential_id AND p.private_generation=c.inventory_mm_private_generation AND p.observed_ms=c.inventory_mm_observed_ms AND p.observed_ms>=$2-5000 AND p.observed_ms<=$2 AND COALESCE((p.projection_json->>'stream_healthy')::boolean,false)))) AND NOT EXISTS(SELECT 1 FROM venue_control_strategy_scopes s WHERE s.trading_account_id=i.trading_account_id AND s.venue='binance' AND s.mode='LIVE') AND NOT EXISTS(SELECT 1 FROM venue_binance_commands u WHERE u.trading_account_id=i.trading_account_id AND u.command_id<>c.command_id AND u.command_state IN ('sending','accepted','reconcile_required')))")
        .bind(command_id).bind(now).fetch_one(pool).await.map_err(|_|BinanceCommandLedgerError::Unavailable)
}
