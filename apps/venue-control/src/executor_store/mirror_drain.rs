use super::*;
use venue_gateway_binance::BinanceAbsentLimitOrder;

// Only an explicitly stopping bot, a never-acknowledged ordinary opening limit, and a single
// unresolved account command qualify. Known native identity or any accounted fill keeps the fence.
const CANDIDATE: &str = "SELECT c.created_ms,b.revision AS drain_revision,c.relation_id,m.bot_id,m.mirror_id FROM venue_binance_commands c JOIN venue_order_mirrors m ON m.mirror_id=c.mirror_order_id JOIN venue_leader_bots b ON b.bot_id=m.bot_id JOIN venue_kol_follow_relations r ON r.relation_id=c.relation_id WHERE c.command_id=$1 AND c.command_state='reconcile_required' AND c.command_origin='copy' AND c.command_phase='open' AND c.order_kind IN ('limit_gtc','limit_post_only') AND c.native_order_id IS NULL AND c.accepted_ms IS NULL AND c.sending_ms IS NOT NULL AND c.sending_ms<=$2::bigint-60000 AND c.created_ms>=$2::bigint-172680000 AND c.created_ms>60000 AND c.signed_settlement IS NULL AND m.source_kind='limit' AND m.child_client_order_id=c.client_order_id AND m.child_native_order_id IS NULL AND m.filled_quantity::numeric=0 AND m.mirror_state='pending' AND b.bot_state='draining' AND r.follower_user_id=c.owner_user_id AND r.follower_trading_account_id=c.trading_account_id AND r.credential_id=c.credential_id AND r.kol_user_id=b.owner_user_id AND NOT EXISTS(SELECT 1 FROM venue_binance_commands other WHERE other.trading_account_id=c.trading_account_id AND other.command_id<>c.command_id AND other.command_state IN ('pending','sending','accepted','reconcile_required'))";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MirrorDrainCandidate {
    pub created_ms: u64,
    drain_revision: i64,
    relation_id: String,
    bot_id: String,
    mirror_id: String,
}

fn candidate(
    row: sqlx::postgres::PgRow,
) -> Result<MirrorDrainCandidate, BinanceCommandLedgerError> {
    let unavailable = |_| BinanceCommandLedgerError::Unavailable;
    Ok(MirrorDrainCandidate {
        created_ms: u64::try_from(row.try_get::<i64, _>("created_ms").map_err(unavailable)?)
            .map_err(|_| BinanceCommandLedgerError::Conflict)?,
        drain_revision: row.try_get("drain_revision").map_err(unavailable)?,
        relation_id: row.try_get("relation_id").map_err(unavailable)?,
        bot_id: row.try_get("bot_id").map_err(unavailable)?,
        mirror_id: row.try_get("mirror_id").map_err(unavailable)?,
    })
}

fn valid_absence(
    command: &ClaimedBinanceCommand,
    expected: &MirrorDrainCandidate,
    fact: &BinanceAbsentLimitOrder,
    now: u64,
) -> bool {
    fact.trading_account_id == command.trading_account_id
        && fact.symbol == command.symbol
        && fact.client_order_id == command.client_order_id
        && fact.history_start_ms.checked_add(60_000) == Some(expected.created_ms)
        && fact.history_start_ms < fact.snapshot_observed_ms
        && fact.snapshot_observed_ms <= fact.history_end_ms
        && fact.history_end_ms <= fact.observed_ms
        && fact.observed_ms <= now
        && now.saturating_sub(fact.snapshot_observed_ms) <= 3_000
        && fact.observed_ms.saturating_sub(fact.history_start_ms) <= 48 * 60 * 60 * 1000
}

impl PgExecutorStore {
    pub(crate) async fn mirror_drain_candidate(
        &self,
        command: &ClaimedBinanceCommand,
        now: u64,
    ) -> Result<Option<MirrorDrainCandidate>, BinanceCommandLedgerError> {
        if command.state != ExecutorCommandState::ReconcileRequired
            || command.origin != venue_control_protocol::kol::ExecutorCommandOrigin::Copy
            || command.native_order_id.is_some()
            || !matches!(
                command.order,
                ClaimedBinanceOrder::Limit {
                    reducing: false,
                    ..
                }
            )
        {
            return Ok(None);
        }
        sqlx::query(CANDIDATE)
            .bind(&command.command_id)
            .bind(i64::try_from(now).map_err(|_| BinanceCommandLedgerError::Conflict)?)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?
            .map(candidate)
            .transpose()
    }

    pub(crate) async fn settle_absent_mirror_drain(
        &self,
        command: &ClaimedBinanceCommand,
        expected: &MirrorDrainCandidate,
        fact: &BinanceAbsentLimitOrder,
        now: u64,
    ) -> Result<bool, BinanceCommandLedgerError> {
        if !valid_absence(command, expected, fact, now) {
            return Ok(false);
        }
        let stamp = i64::try_from(now).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let unavailable = |_| BinanceCommandLedgerError::Unavailable;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        // Match the planner's profile -> bot/relation -> account -> mirror/command lock order.
        sqlx::query("SELECT p.kol_user_id FROM venue_kol_profiles p JOIN venue_kol_follow_relations r ON r.kol_user_id=p.kol_user_id WHERE r.relation_id=$1 FOR SHARE OF p")
            .bind(&expected.relation_id).fetch_optional(&mut *tx).await.map_err(unavailable)?
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        sqlx::query("SELECT r.relation_id FROM venue_kol_follow_relations r JOIN venue_leader_bots b ON b.owner_user_id=r.kol_user_id WHERE r.relation_id=$1 AND b.bot_id=$2 FOR SHARE OF b FOR UPDATE OF r")
            .bind(&expected.relation_id).bind(&expected.bot_id).fetch_optional(&mut *tx).await.map_err(unavailable)?
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        if lock_account_command_queue(
            &mut tx,
            &command.owner_user_id,
            &command.trading_account_id,
            &command.credential_id,
        )
        .await?
            != 1
        {
            return Ok(false);
        }
        let current = sqlx::query(&format!("{CANDIDATE} AND c.client_order_id=$3 AND c.trading_account_id=$4 AND c.credential_id=$5 AND c.owner_user_id=$6 AND c.symbol=$7 FOR UPDATE OF m,c"))
            .bind(&command.command_id)
            .bind(stamp)
            .bind(&command.client_order_id)
            .bind(&command.trading_account_id)
            .bind(&command.credential_id)
            .bind(&command.owner_user_id)
            .bind(command.symbol.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?
            .map(candidate)
            .transpose()?;
        if current.as_ref() != Some(expected) {
            return Ok(false);
        }
        let stamp: i64 =
            sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
        if !valid_absence(
            command,
            expected,
            fact,
            u64::try_from(stamp).map_err(|_| BinanceCommandLedgerError::Conflict)?,
        ) {
            return Ok(false);
        }
        let settlement = serde_json::json!({"schema_version":1,"kind":"mirror_stop_confirmed_absent",
            "exact_absence_reads":2,"historical_orders":0,"historical_fills":0,
            "open_orders":0,"conditional_orders":0,"positions":0,"fact":fact});
        sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$2,updated_ms=$2,next_reconcile_ms=NULL,sanitized_error_code='mirror_stop_confirmed_absent',mirror_stop_readback=$3 WHERE command_id=$1")
            .bind(&command.command_id).bind(stamp).bind(settlement).execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("UPDATE venue_order_mirrors SET mirror_state='terminal',attention_code=NULL,updated_ms=$2 WHERE mirror_id=$1")
            .bind(&expected.mirror_id).bind(stamp).execute(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(true)
    }
}
