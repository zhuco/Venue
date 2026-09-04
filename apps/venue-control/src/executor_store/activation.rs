use super::*;
use crate::executor_exchange::AccountBaseline;
use venue_execution::SignedAccountPositionMode;

const MAX_ACTIVATION_AGE_MS: u64 = 30_000;

impl PgExecutorStore {
    pub async fn complete_activation(
        &self,
        activation: &PendingActivation,
        leader: &AccountBaseline,
        follower: &AccountBaseline,
        now_ms: u64,
    ) -> Result<(), BinanceCommandLedgerError> {
        validate_baseline(leader, &activation.leader_trading_account_id, false, now_ms)?;
        validate_baseline(
            follower,
            &activation.follower_trading_account_id,
            true,
            now_ms,
        )?;
        let now = ms(now_ms)?;
        let revision = ms(activation.revision)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        // Draining followers still consume a private-stream slot until exact settlement.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':kol-activation-capacity',0))")
            .execute(&mut *tx).await.map_err(unavailable)?;
        // Source ingestion takes this profile lock in shared mode before looking at relations.
        // It cannot observe half of the activation boundary or replay pre-activation targets.
        sqlx::query("SELECT kol_user_id FROM venue_kol_profiles WHERE kol_user_id=$1 AND leader_trading_account_id=$2 AND profile_state='enabled' FOR UPDATE")
            .bind(&activation.leader_user_id).bind(&activation.leader_trading_account_id)
            .fetch_optional(&mut *tx).await.map_err(unavailable)?
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        sqlx::query("SELECT r.relation_id FROM venue_kol_follow_relations r JOIN venue_kol_activation_requests a USING (relation_id) WHERE r.relation_id=$1 AND r.revision=$2 AND r.relation_state='paused' AND a.relation_revision=$2 AND a.request_id=$3 AND a.request_state='pending' AND r.kol_user_id=$4 AND r.leader_trading_account_id=$5 AND r.follower_user_id=$6 AND r.follower_trading_account_id=$7 AND r.credential_id=$8 FOR UPDATE OF r")
            .bind(&activation.relation_id).bind(revision).bind(&activation.request_id)
            .bind(&activation.leader_user_id).bind(&activation.leader_trading_account_id)
            .bind(&activation.follower_user_id).bind(&activation.follower_trading_account_id)
            .bind(&activation.follower_credential_id)
            .fetch_optional(&mut *tx).await.map_err(unavailable)?
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        let depth = lock_account_command_queue(
            &mut tx,
            &activation.follower_user_id,
            &activation.follower_trading_account_id,
            &activation.follower_credential_id,
        )
        .await?;
        if depth != 0 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let outstanding: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_order_mirrors WHERE relation_id=$1 AND mirror_state NOT IN ('terminal','blocked'))")
            .bind(&activation.relation_id).fetch_one(&mut *tx).await.map_err(unavailable)?;
        let occupied: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_kol_follow_relations r WHERE r.relation_state='active' OR EXISTS(SELECT 1 FROM venue_order_mirrors m WHERE m.relation_id=r.relation_id AND m.mirror_state NOT IN ('terminal','blocked'))")
            .fetch_one(&mut *tx).await.map_err(unavailable)?;
        if outstanding || occupied >= 200 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        for (account, user, credential, baseline) in [
            (
                &activation.leader_trading_account_id,
                &activation.leader_user_id,
                &activation.leader_credential_id,
                leader,
            ),
            (
                &activation.follower_trading_account_id,
                &activation.follower_user_id,
                &activation.follower_credential_id,
                follower,
            ),
        ] {
            let identity: Option<Vec<u8>> = sqlx::query_scalar("SELECT a.exchange_identity_hash FROM venue_user_trading_accounts a JOIN venue_api_credentials c ON c.trading_account_id=a.trading_account_id AND c.user_id=a.user_id WHERE a.trading_account_id=$1 AND a.user_id=$2 AND c.credential_id=$3 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified'")
                .bind(account).bind(user).bind(credential).fetch_optional(&mut *tx).await.map_err(unavailable)?;
            if identity.as_deref() != Some(baseline.account_identity_hash.as_slice()) {
                return Err(BinanceCommandLedgerError::Conflict);
            }
        }
        let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM venue_control_strategy_scopes WHERE venue='binance' AND mode='LIVE' AND trading_account_id=ANY($1)) OR EXISTS (SELECT 1 FROM venue_binance_grid_instances WHERE trading_account_id=$2 AND instance_state<>'stopped')")
            .bind(vec![&activation.leader_trading_account_id, &activation.follower_trading_account_id])
            .bind(&activation.follower_trading_account_id).fetch_one(&mut *tx).await.map_err(unavailable)?;
        if blocked {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let slot: Option<i16> = sqlx::query_scalar("SELECT s::smallint FROM generate_series(1,200) s WHERE NOT EXISTS (SELECT 1 FROM venue_kol_follow_relations r WHERE r.active_slot=s) LIMIT 1")
            .fetch_optional(&mut *tx).await.map_err(unavailable)?;
        let slot = slot.ok_or(BinanceCommandLedgerError::Conflict)?;
        let baseline = json!({
            "target_model": 2, "baseline_ms": now_ms,
            "leader_observed_ms": leader.snapshot.observed_at_ms(),
            "leader_positions": leader.snapshot.positions(),
            "leader_fills_cursor": leader.snapshot.fills_cursor(),
            "follower_observed_ms": follower.snapshot.observed_at_ms(),
            "follower_positions": follower.snapshot.positions(),
            "follower_fills_cursor": follower.snapshot.fills_cursor(),
        });
        let leader_enabled:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_leader_bots b JOIN venue_leader_bot_permissions g ON g.kol_user_id=b.owner_user_id WHERE b.owner_user_id=$1 AND b.trading_account_id=$2 AND b.credential_id=$3 AND b.bot_state='running' AND g.enabled AND b.permission_revision=g.revision)")
            .bind(&activation.leader_user_id).bind(&activation.leader_trading_account_id).bind(&activation.leader_credential_id).fetch_one(&mut *tx).await.map_err(unavailable)?;
        if !leader_enabled {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        // Resuming starts from a fresh flat follower. Keep command history and monotonic target
        // identities, but do not let a previous session's desired quantity create a catch-up order.
        sqlx::query("UPDATE venue_kol_copy_targets SET copyable_quantity='0',target_quantity='0',observed_quantity='0',dirty=false,target_revision=target_revision+1,updated_ms=$2 WHERE relation_id=$1")
            .bind(&activation.relation_id).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='active',active_slot=$2,baseline_json=$3,attention_code=NULL,updated_ms=$4 WHERE relation_id=$1")
            .bind(&activation.relation_id).bind(slot).bind(baseline).bind(now)
            .execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("UPDATE venue_kol_activation_requests SET request_state='completed',sanitized_reason=NULL,updated_ms=$2 WHERE relation_id=$1 AND request_id=$3")
            .bind(&activation.relation_id).bind(now).bind(&activation.request_id)
            .execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)
    }
}

fn validate_baseline(
    baseline: &AccountBaseline,
    account: &str,
    require_empty: bool,
    now_ms: u64,
) -> Result<(), BinanceCommandLedgerError> {
    let snapshot = &baseline.snapshot;
    let valid = snapshot.binding().trading_account_id == account
        && snapshot.binding().venue == venue_gateway_binance::VenueId::Binance
        && snapshot.binding().mode == venue_gateway_binance::GatewayMode::Live
        && snapshot.position_mode() == SignedAccountPositionMode::Hedge
        && snapshot.unknown_results().is_empty()
        && snapshot.observed_at_ms() <= now_ms
        && now_ms - snapshot.observed_at_ms() <= MAX_ACTIVATION_AGE_MS
        && (!require_empty
            || (snapshot.open_orders().is_empty()
                && snapshot
                    .positions()
                    .iter()
                    .all(|position| position.quantity == Decimal::ZERO)));
    valid
        .then_some(())
        .ok_or(BinanceCommandLedgerError::Conflict)
}

fn unavailable(_: sqlx::Error) -> BinanceCommandLedgerError {
    BinanceCommandLedgerError::Unavailable
}
