use super::{
    planner::same_terms,
    store::{number, projection, stamp, text, unavailable},
};
use crate::{
    executor_exchange::{ExecutionOrderKind, ExecutionOutcome, ExecutionRequest},
    executor_store::PgExecutorStore,
    kol_executor::{BinanceCommandLedgerError as Error, ClaimedBinanceCommand},
};
use rust_decimal::Decimal;
use sqlx::{PgConnection, Row};
use venue_control_protocol::kol::{ExecutorCommandOrigin, TerminalOpenOrder};

pub(super) fn now_ms() -> Result<u64, Error> {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_millis(),
    )
    .map_err(|_| Error::Unavailable)
}

pub(crate) async fn mirror_send_allowed(
    store: &PgExecutorStore,
    command: &ClaimedBinanceCommand,
    now: u64,
) -> Result<bool, Error> {
    if command.origin != ExecutorCommandOrigin::Copy {
        return Ok(true);
    }
    let row=sqlx::query("SELECT c.mirror_order_id,c.command_phase,c.selected_native_order_id,c.credential_id,c.trading_account_id,m.child_native_order_id,m.source_order_json,m.bot_revision,m.permission_revision,m.relation_revision,m.mirror_state,b.bot_state,b.revision AS current_bot_revision,b.permission_revision AS bot_grant,r.revision AS current_relation_revision,r.relation_state,p.profile_state,g.enabled,g.revision AS current_permission_revision,b.trading_account_id AS leader_account,EXISTS(SELECT 1 FROM venue_api_credentials lc WHERE lc.credential_id=b.credential_id AND lc.user_id=b.owner_user_id AND lc.deleted_ms IS NULL AND lc.verification_json->>'verification'='verified') AS leader_verified,EXISTS(SELECT 1 FROM venue_api_credentials fc WHERE fc.credential_id=c.credential_id AND fc.user_id=c.owner_user_id AND fc.trading_account_id=c.trading_account_id AND fc.deleted_ms IS NULL AND fc.verification_json->>'verification'='verified') AS follower_verified,lp.projection_json FROM venue_binance_commands c LEFT JOIN venue_order_mirrors m ON m.mirror_id=c.mirror_order_id LEFT JOIN venue_leader_bots b ON b.bot_id=m.bot_id LEFT JOIN venue_kol_follow_relations r ON r.relation_id=m.relation_id LEFT JOIN venue_kol_profiles p ON p.kol_user_id=b.owner_user_id LEFT JOIN venue_leader_bot_permissions g ON g.kol_user_id=b.owner_user_id LEFT JOIN venue_binance_account_projections lp ON lp.credential_id=b.credential_id WHERE c.command_id=$1")
        .bind(&command.command_id).fetch_one(store.mirror_pool()).await.map_err(unavailable)?;
    if row
        .try_get::<Option<String>, _>("mirror_order_id")
        .map_err(unavailable)?
        .is_none()
    {
        return Ok(false);
    }
    if text(&row, "command_phase")? == "cancel" {
        return Ok(row
            .try_get::<Option<String>, _>("selected_native_order_id")
            .map_err(unavailable)?
            == row
                .try_get::<Option<String>, _>("child_native_order_id")
                .map_err(unavailable)?
            && row
                .try_get::<Option<String>, _>("child_native_order_id")
                .map_err(unavailable)?
                .is_some());
    }
    if !row
        .try_get::<bool, _>("leader_verified")
        .map_err(unavailable)?
        || !row
            .try_get::<bool, _>("follower_verified")
            .map_err(unavailable)?
        || text(&row, "relation_state")? != "active"
        || text(&row, "bot_state")? != "running"
        || text(&row, "profile_state")? != "enabled"
        || !row
            .try_get::<Option<bool>, _>("enabled")
            .map_err(unavailable)?
            .unwrap_or(false)
        || number(&row, "bot_revision")? != number(&row, "current_bot_revision")?
        || number(&row, "permission_revision")? != number(&row, "current_permission_revision")?
        || number(&row, "relation_revision")? != number(&row, "current_relation_revision")?
    {
        return Ok(false);
    }
    let original: TerminalOpenOrder =
        serde_json::from_value(row.try_get("source_order_json").map_err(unavailable)?)
            .map_err(|_| Error::Conflict)?;
    let current = projection(
        row.try_get("projection_json").map_err(unavailable)?,
        &text(&row, "leader_account")?,
        now,
    )?;
    Ok(current.is_some_and(|p| {
        p.open_orders
            .iter()
            .any(|o| super::planner::eligible(o, 0) && same_terms(o, &original))
    }))
}

// Return true for mirror commands so the old market-target settlement cannot consume them.
pub(crate) async fn settle_mirror_command(
    connection: &mut PgConnection,
    command: &sqlx::postgres::PgRow,
    state: &str,
    now: i64,
) -> Result<bool, Error> {
    let Some(mirror) = command
        .try_get::<Option<String>, _>("mirror_order_id")
        .map_err(unavailable)?
    else {
        return Ok(false);
    };
    let cancel = text(command, "command_phase")? == "cancel";
    if matches!(state, "rejected" | "cancelled") && !cancel {
        sqlx::query("UPDATE venue_order_mirrors SET mirror_state='blocked',attention_code='child_not_placed',updated_ms=$1 WHERE mirror_id=$2 AND mirror_state<>'terminal'")
            .bind(now).bind(mirror).execute(connection).await.map_err(unavailable)?;
    } else if matches!(state, "rejected" | "cancelled") && cancel {
        sqlx::query("UPDATE venue_order_mirrors SET attention_code='cancel_requires_attention',updated_ms=$1 WHERE mirror_id=$2 AND mirror_state<>'terminal'")
            .bind(now).bind(&mirror).execute(&mut *connection).await.map_err(unavailable)?;
    } else if state == "reconciled" {
        let native: Option<String> = command.try_get("native_order_id").map_err(unavailable)?;
        if native.is_none() {
            return Err(Error::Conflict);
        }
        let updated=sqlx::query("UPDATE venue_order_mirrors SET child_native_order_id=COALESCE(child_native_order_id,$1),mirror_state=CASE WHEN mirror_state='terminal' THEN 'terminal' WHEN $2 THEN mirror_state ELSE 'live' END,updated_ms=$3 WHERE mirror_id=$4 AND (child_native_order_id IS NULL OR child_native_order_id=$1)")
            .bind(native).bind(cancel).bind(now).bind(mirror).execute(connection).await.map_err(unavailable)?.rows_affected();
        if updated != 1 {
            return Err(Error::Conflict);
        }
    }
    Ok(true)
}

impl PgExecutorStore {
    pub(crate) async fn prepare_mirror_request(
        &self,
        command: &ClaimedBinanceCommand,
        request: &mut ExecutionRequest,
    ) -> Result<(), Error> {
        if command.origin != ExecutorCommandOrigin::Copy {
            return Ok(());
        }
        let target:Option<String>=sqlx::query_scalar("SELECT m.child_client_order_id FROM venue_order_mirrors m JOIN venue_binance_commands c ON c.mirror_order_id=m.mirror_id WHERE c.command_id=$1 AND c.command_phase='cancel'")
            .bind(&command.command_id).fetch_optional(self.mirror_pool()).await.map_err(unavailable)?;
        if let (
            Some(target),
            ExecutionOrderKind::CancelExact {
                target_client_order_id,
                ..
            },
        ) = (target, &mut request.order_kind)
        {
            *target_client_order_id = Some(target);
        }
        Ok(())
    }

    pub(crate) async fn record_mirror_order_fact(
        &self,
        command: &ClaimedBinanceCommand,
        result: &ExecutionOutcome,
        now: u64,
    ) -> Result<(), Error> {
        if command.origin != ExecutorCommandOrigin::Copy {
            return Ok(());
        }
        let Some(fact) = result.order_fact.as_ref() else {
            return Ok(());
        };
        let native = result.native_order_id.as_ref().ok_or(Error::Conflict)?;
        if fact.quantity <= Decimal::ZERO
            || fact.filled_quantity < Decimal::ZERO
            || fact.filled_quantity > fact.quantity
        {
            return Err(Error::Conflict);
        }
        let updated=sqlx::query("UPDATE venue_order_mirrors m SET child_native_order_id=$1,filled_quantity=$2,mirror_state=CASE WHEN $3 THEN 'terminal' ELSE m.mirror_state END,updated_ms=$4 FROM venue_binance_commands c WHERE c.command_id=$5 AND c.mirror_order_id=m.mirror_id AND (m.child_native_order_id IS NULL OR m.child_native_order_id=$1) AND m.filled_quantity::numeric<=$2::numeric AND $6::numeric<=m.child_quantity::numeric")
            .bind(native).bind(fact.filled_quantity.to_string()).bind(fact.terminal).bind(stamp(now)?).bind(&command.command_id).bind(fact.quantity.to_string()).execute(self.mirror_pool()).await.map_err(unavailable)?.rows_affected();
        if updated != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
