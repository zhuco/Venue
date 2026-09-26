use super::*;

pub(super) async fn check_current_custody(
    connection: &mut sqlx::PgConnection,
    account: &str,
    verified_ms: Option<u64>,
) -> Result<(), AccountError> {
    // Caller holds command-admission credential locks. A post-probe send can invalidate
    // a flat signed readback even when that command has already settled.
    let blocked: bool = sqlx::query_scalar(
        "SELECT
        EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE trading_account_id=$1 AND venue='binance' AND mode='LIVE')
        OR EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND (command_state IN ('pending','sending','accepted','reconcile_required') OR sending_ms >= $2))
        OR EXISTS(SELECT 1 FROM venue_binance_grid_instances WHERE trading_account_id=$1 AND instance_state NOT IN ('draft','stopped'))
        OR EXISTS(SELECT 1 FROM venue_inventory_mm_instances WHERE trading_account_id=$1 AND instance_state<>'stopped')
        OR EXISTS(SELECT 1 FROM venue_leader_bots WHERE trading_account_id=$1 AND bot_state<>'stopped')
        OR EXISTS(SELECT 1 FROM venue_kol_follow_relations r WHERE (follower_trading_account_id=$1 OR leader_trading_account_id=$1) AND (relation_state NOT IN ('paused','disabled')
            OR EXISTS(SELECT 1 FROM venue_kol_activation_requests a WHERE a.relation_id=r.relation_id AND a.request_state='pending')
            OR EXISTS(SELECT 1 FROM venue_order_mirrors m WHERE m.relation_id=r.relation_id AND m.mirror_state NOT IN ('terminal','blocked'))))",
    ).bind(account).bind(ms(verified_ms.ok_or(error(Code::VerificationRequired))?)?)
        .fetch_one(connection).await.map_err(database_error)?;
    if blocked {
        return Err(error(Code::AccountInUse));
    }
    Ok(())
}
