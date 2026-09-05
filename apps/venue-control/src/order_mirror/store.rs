use super::{MAX_MIRROR_ORDERS_PER_RELATION, planner::*};
use crate::{executor_runtime::CommandWake, kol_executor::BinanceCommandLedgerError as Error};
use futures_util::{StreamExt, stream};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool, Row, postgres::PgRow};
use std::collections::BTreeMap;
use std::str::FromStr;
use venue_control_protocol::kol::{TerminalAccountProjection, TerminalOpenOrder};

pub(super) fn unavailable(_: sqlx::Error) -> Error {
    Error::Unavailable
}
pub(super) fn text(row: &PgRow, key: &str) -> Result<String, Error> {
    row.try_get(key).map_err(unavailable)
}
pub(super) fn number(row: &PgRow, key: &str) -> Result<i64, Error> {
    row.try_get(key).map_err(unavailable)
}
pub(super) fn decimal(row: &PgRow, key: &str) -> Result<Decimal, Error> {
    Decimal::from_str(&text(row, key)?).map_err(|_| Error::Conflict)
}
pub(super) fn stamp(now: u64) -> Result<i64, Error> {
    i64::try_from(now).map_err(|_| Error::Conflict)
}
pub(super) fn identity(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part.len().to_be_bytes());
        hash.update(part.as_bytes());
    }
    let bytes = hash.finalize();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-8{:01x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6] & 15,
        bytes[7],
        bytes[8] & 15,
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

pub(super) fn projection(
    value: Option<serde_json::Value>,
    account: &str,
    now: u64,
) -> Result<Option<TerminalAccountProjection>, Error> {
    let Some(value) = value else { return Ok(None) };
    if value
        .get("stream_healthy")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Ok(None);
    }
    let Some(raw) = value.get("projection") else {
        return Err(Error::Conflict);
    };
    let projection: TerminalAccountProjection =
        serde_json::from_value(raw.clone()).map_err(|_| Error::Conflict)?;
    projection.validate().map_err(|_| Error::Conflict)?;
    if projection.trading_account_id != account
        || projection.observed_ms > now
        || now - projection.observed_ms > 3_000
    {
        return Ok(None);
    }
    Ok(Some(projection))
}

pub async fn run_order_mirror(
    pool: PgPool,
    wake: CommandWake,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Error> {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed=shutdown.changed()=>{if changed.is_err() || *shutdown.borrow(){return Ok(())}},
            _=interval.tick()=>{}
        }
        if *shutdown.borrow() {
            return Ok(());
        }
        let now = super::settlement::now_ms()?;
        let relations:Vec<String>=match sqlx::query_scalar("SELECT r.relation_id FROM venue_kol_follow_relations r JOIN venue_leader_bots b ON b.owner_user_id=r.kol_user_id WHERE r.relation_state='active' OR EXISTS(SELECT 1 FROM venue_order_mirrors m WHERE m.relation_id=r.relation_id AND m.mirror_state NOT IN ('terminal','blocked')) ORDER BY r.relation_id")
            .fetch_all(&pool).await {
                Ok(relations)=>relations,
                Err(_)=>{tracing::warn!("Order mirror discovery unavailable; preserving pending reconciliation");interval.reset_after(std::time::Duration::from_secs(1));continue;}
            };
        let mut turns = stream::iter(relations.into_iter().map(|relation| {
            let pool = pool.clone();
            async move { plan_relation(&pool, &relation, now).await }
        }))
        .buffer_unordered(8);
        while let Some(result) = turns.next().await {
            match result {
                Ok(true) => wake.wake(),
                Ok(false) => {}
                Err(_) => tracing::warn!(
                    "Order mirror turn failed; retained mappings and commands for reconciliation"
                ),
            }
        }
        if sqlx::query("UPDATE venue_leader_bots b SET bot_state='needs_attention',attention_code='cancel_retry_exhausted',updated_ms=$1 WHERE b.bot_state<>'needs_attention' AND EXISTS(SELECT 1 FROM venue_order_mirrors m WHERE m.bot_id=b.bot_id AND m.mirror_state='cancelling' AND m.cancel_attempts>=8 AND m.attention_code='cancel_retry_exhausted')")
            .bind(stamp(now)?).execute(&pool).await.is_err(){tracing::warn!("Order mirror attention state unavailable");}
        if sqlx::query("UPDATE venue_leader_bots b SET bot_state='stopped',updated_ms=$1 WHERE b.bot_state='draining' AND NOT EXISTS(SELECT 1 FROM venue_order_mirrors m WHERE m.bot_id=b.bot_id AND m.mirror_state NOT IN ('terminal','blocked')) AND NOT EXISTS(SELECT 1 FROM venue_binance_commands c JOIN venue_order_mirrors m ON m.mirror_id=c.mirror_order_id WHERE m.bot_id=b.bot_id AND c.command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(stamp(now)?).execute(&pool).await.is_err() {
                tracing::warn!("Order mirror drain settlement unavailable; preserving draining state");
                interval.reset_after(std::time::Duration::from_secs(1));
            }
    }
}

pub(super) async fn plan_relation(pool: &PgPool, relation: &str, now: u64) -> Result<bool, Error> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    sqlx::query("SELECT p.kol_user_id FROM venue_kol_profiles p JOIN venue_kol_follow_relations r ON r.kol_user_id=p.kol_user_id WHERE r.relation_id=$1 FOR SHARE OF p")
        .bind(relation).fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or(Error::Conflict)?;
    let row=sqlx::query("SELECT r.*,b.bot_id,b.bot_state,b.revision AS bot_revision,b.permission_revision,b.started_ms,b.credential_id AS leader_credential_id,p.strategy_capital,p.profile_state,EXISTS(SELECT 1 FROM venue_api_credentials lc WHERE lc.credential_id=b.credential_id AND lc.user_id=b.owner_user_id AND lc.deleted_ms IS NULL AND lc.verification_json->>'verification'='verified') AS leader_verified,EXISTS(SELECT 1 FROM venue_api_credentials fc WHERE fc.credential_id=r.credential_id AND fc.user_id=r.follower_user_id AND fc.trading_account_id=r.follower_trading_account_id AND fc.deleted_ms IS NULL AND fc.verification_json->>'verification'='verified') AS follower_verified,COALESCE(g.enabled,false) AS granted,COALESCE(g.revision,0) AS grant_revision,lp.projection_json AS leader_projection,fp.projection_json AS follower_projection FROM venue_kol_follow_relations r JOIN venue_kol_profiles p ON p.kol_user_id=r.kol_user_id JOIN venue_leader_bots b ON b.owner_user_id=r.kol_user_id LEFT JOIN venue_leader_bot_permissions g ON g.kol_user_id=p.kol_user_id LEFT JOIN venue_binance_account_projections lp ON lp.credential_id=b.credential_id LEFT JOIN venue_binance_account_projections fp ON fp.credential_id=r.credential_id WHERE r.relation_id=$1 FOR SHARE OF b FOR UPDATE OF r")
        .bind(relation).fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or(Error::Conflict)?;
    let owner = text(&row, "follower_user_id")?;
    let account = text(&row, "follower_trading_account_id")?;
    let credential = text(&row, "credential_id")?;
    let mut depth =
        crate::executor_store::lock_account_command_queue(&mut tx, &owner, &account, &credential)
            .await?;
    let baseline: Option<serde_json::Value> = row.try_get("baseline_json").map_err(unavailable)?;
    let active = text(&row, "relation_state")? == "active"
        && text(&row, "bot_state")? == "running"
        && row
            .try_get::<bool, _>("leader_verified")
            .map_err(unavailable)?
        && row
            .try_get::<bool, _>("follower_verified")
            .map_err(unavailable)?
        && text(&row, "profile_state")? == "enabled"
        && row.try_get::<bool, _>("granted").map_err(unavailable)?
        && number(&row, "permission_revision")? == number(&row, "grant_revision")?
        && baseline
            .as_ref()
            .and_then(|v| v.get("target_model"))
            .and_then(serde_json::Value::as_u64)
            == Some(2);
    let source = projection(
        row.try_get("leader_projection").map_err(unavailable)?,
        &text(&row, "leader_trading_account_id")?,
        now,
    )?;
    let follower = projection(
        row.try_get("follower_projection").map_err(unavailable)?,
        &account,
        now,
    )?;
    let cutoff = baseline
        .as_ref()
        .and_then(|v| v.get("baseline_ms"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(u64::MAX)
        .max(
            row.try_get::<Option<i64>, _>("started_ms")
                .map_err(unavailable)?
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or(u64::MAX),
        );
    let allowed: Vec<String> =
        serde_json::from_value(row.try_get("allowed_symbols").map_err(unavailable)?)
            .map_err(|_| Error::Conflict)?;
    let desired: BTreeMap<(String, String), TerminalOpenOrder> = source
        .as_ref()
        .into_iter()
        .flat_map(|p| p.open_orders.iter())
        .filter(|order| {
            active && allowed.contains(&order.symbol.to_string()) && eligible(order, cutoff)
        })
        .filter_map(|order| {
            order
                .native_order_id
                .as_ref()
                .map(|id| ((order.symbol.to_string(), id.clone()), order.clone()))
        })
        .collect();
    let mirrors=sqlx::query("SELECT m.*,c.command_state AS place_state FROM venue_order_mirrors m LEFT JOIN venue_binance_commands c ON c.command_id=m.child_client_order_id WHERE m.relation_id=$1 ORDER BY m.symbol,m.source_order_id,m.child_sequence FOR UPDATE OF m")
        .bind(relation).fetch_all(&mut *tx).await.map_err(unavailable)?;
    let mut latest = BTreeMap::new();
    let mut filled = BTreeMap::<(String, String), Decimal>::new();
    let mut has_work = false;
    let mut live_count = 0;
    for mirror in mirrors {
        let key = (text(&mirror, "symbol")?, text(&mirror, "source_order_id")?);
        let mut cumulative = decimal(&mirror, "filled_quantity")?;
        let client = text(&mirror, "child_client_order_id")?;
        let native: Option<String> = mirror
            .try_get("child_native_order_id")
            .map_err(unavailable)?;
        let child = follower.as_ref().and_then(|p| {
            p.open_orders.iter().find(|o| {
                o.client_order_id == client
                    && o.symbol.to_string() == key.0
                    && native.is_some()
                    && o.native_order_id == native
            })
        });
        if let Some(child) = child {
            if let Some(value) = child.filled_quantity {
                if value < cumulative || value > child.quantity {
                    return Err(Error::Conflict);
                }
                cumulative = value;
                sqlx::query("UPDATE venue_order_mirrors SET filled_quantity=$1,updated_ms=$2 WHERE mirror_id=$3 AND filled_quantity::numeric<$1::numeric")
                    .bind(value.to_string()).bind(stamp(now)?).bind(text(&mirror,"mirror_id")?).execute(&mut *tx).await.map_err(unavailable)?;
            }
        }
        if number(&mirror, "relation_revision")? == number(&row, "revision")?
            && number(&mirror, "bot_revision")? == number(&row, "bot_revision")?
        {
            let sum = filled.entry(key.clone()).or_default();
            *sum = sum.checked_add(cumulative).ok_or(Error::Conflict)?;
        }
        let state = text(&mirror, "mirror_state")?;
        if state == "cancelling" {
            let failed:Option<String>=sqlx::query_scalar("SELECT command_state FROM venue_binance_commands WHERE mirror_order_id=$1 AND command_phase='cancel' ORDER BY created_ms DESC,command_id DESC LIMIT 1")
                .bind(text(&mirror,"mirror_id")?).fetch_optional(&mut *tx).await.map_err(unavailable)?;
            if matches!(failed.as_deref(), Some("rejected" | "cancelled")) {
                let attempts: i32 = mirror.try_get("cancel_attempts").map_err(unavailable)?;
                if attempts >= 8 {
                    sqlx::query("UPDATE venue_order_mirrors SET attention_code='cancel_retry_exhausted' WHERE mirror_id=$1")
                        .bind(text(&mirror,"mirror_id")?).execute(&mut *tx).await.map_err(unavailable)?;
                } else if stamp(now)?.saturating_sub(number(&mirror, "updated_ms")?) >= 30_000
                    && follower.is_some()
                {
                    sqlx::query(
                        "UPDATE venue_order_mirrors SET mirror_state='live' WHERE mirror_id=$1",
                    )
                    .bind(text(&mirror, "mirror_id")?)
                    .execute(&mut *tx)
                    .await
                    .map_err(unavailable)?;
                }
            }
        }
        if state == "pending"
            && matches!(
                mirror
                    .try_get::<Option<String>, _>("place_state")
                    .map_err(unavailable)?
                    .as_deref(),
                Some("cancelled" | "rejected")
            )
        {
            sqlx::query("UPDATE venue_order_mirrors SET mirror_state='blocked',attention_code='child_not_placed',updated_ms=$1 WHERE mirror_id=$2")
                .bind(stamp(now)?).bind(text(&mirror,"mirror_id")?).execute(&mut *tx).await.map_err(unavailable)?;
            latest.insert(key, mirror);
            continue;
        }
        if !matches!(state.as_str(), "terminal" | "blocked") {
            live_count += 1;
        }
        let original: TerminalOpenOrder =
            serde_json::from_value(mirror.try_get("source_order_json").map_err(unavailable)?)
                .map_err(|_| Error::Conflict)?;
        let obsolete = !active
            || number(&mirror, "relation_revision")? != number(&row, "revision")?
            || number(&mirror, "bot_revision")? != number(&row, "bot_revision")?
            || (source.is_some()
                && desired
                    .get(&key)
                    .is_none_or(|order| !same_terms(order, &original)))
            || (state == "live" && follower.is_some() && child.is_none());
        if obsolete
            && state == "pending"
            && mirror
                .try_get::<Option<String>, _>("place_state")
                .map_err(unavailable)?
                .as_deref()
                == Some("pending")
        {
            sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$1,updated_ms=$1,sanitized_error_code='mirror_target_retired' WHERE command_id=$2 AND command_state='pending'")
                .bind(stamp(now)?).bind(&client).execute(&mut *tx).await.map_err(unavailable)?;
            sqlx::query("UPDATE venue_order_mirrors SET mirror_state='terminal',updated_ms=$1 WHERE mirror_id=$2")
                .bind(stamp(now)?).bind(text(&mirror,"mirror_id")?).execute(&mut *tx).await.map_err(unavailable)?;
            depth = depth.saturating_sub(1);
        } else if obsolete
            && state == "live"
            && depth < crate::kol_executor::MAX_ACCOUNT_QUEUE_DEPTH
        {
            if enqueue_cancel(&mut tx, &row, &mirror, now).await? {
                has_work = true;
                depth += 1;
            }
        }
        latest.insert(key, mirror);
    }
    if active && source.is_some() && follower.is_some() {
        for (key, order) in desired {
            if depth >= crate::kol_executor::MAX_ACCOUNT_QUEUE_DEPTH
                || live_count >= MAX_MIRROR_ORDERS_PER_RELATION
            {
                break;
            }
            let previous = latest.get(&key);
            if let Some(previous) = previous {
                let original: TerminalOpenOrder = serde_json::from_value(
                    previous.try_get("source_order_json").map_err(unavailable)?,
                )
                .map_err(|_| Error::Conflict)?;
                if !matches!(
                    text(previous, "mirror_state")?.as_str(),
                    "terminal" | "blocked"
                ) || same_terms(&original, &order)
                {
                    continue;
                }
            }
            let mut quantity = replacement_quantity(
                &order,
                decimal(&row, "allocated_capital")?,
                decimal(&row, "strategy_capital")?,
                decimal(&row, "multiplier")?,
                serde_json::from_value(row.try_get("sizing_json").map_err(unavailable)?)
                    .map_err(|_| Error::Conflict)?,
                filled.get(&key).copied().unwrap_or_default(),
            )?;
            if reducing(&order) {
                let p = follower.as_ref().ok_or(Error::Conflict)?;
                let position = p
                    .positions
                    .iter()
                    .find(|p| p.symbol == order.symbol && p.position_side == order.position_side)
                    .map(|p| p.quantity)
                    .unwrap_or_default();
                let reserved = p
                    .open_orders
                    .iter()
                    .filter(|o| {
                        o.symbol == order.symbol
                            && o.position_side == order.position_side
                            && reducing(o)
                    })
                    .try_fold(Decimal::ZERO, |sum, o| {
                        sum.checked_add(o.quantity - o.filled_quantity.unwrap_or_default())
                            .ok_or(Error::Conflict)
                    })?;
                quantity = quantity.min((position - reserved).max(Decimal::ZERO));
            }
            if quantity <= Decimal::ZERO {
                continue;
            }
            let sequence = previous
                .map(|p| number(p, "child_sequence"))
                .transpose()?
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(Error::Conflict)?;
            enqueue_place(&mut tx, &row, &order, sequence, quantity, now).await?;
            has_work = true;
            depth += 1;
            live_count += 1;
        }
    }
    tx.commit().await.map_err(unavailable)?;
    Ok(has_work)
}

async fn enqueue_place(
    connection: &mut PgConnection,
    row: &PgRow,
    order: &TerminalOpenOrder,
    sequence: i64,
    quantity: Decimal,
    now: u64,
) -> Result<(), Error> {
    let relation = text(row, "relation_id")?;
    let revision = number(row, "revision")?;
    let native = order.native_order_id.as_deref().ok_or(Error::Conflict)?;
    let bot = text(row, "bot_id")?;
    let mirror = identity(&[
        &bot,
        &number(row, "bot_revision")?.to_string(),
        &relation,
        &revision.to_string(),
        &order.symbol.to_string(),
        native,
        &sequence.to_string(),
    ]);
    let client = identity(&[&mirror, "place"]);
    let source = serde_json::to_value(order).map_err(|_| Error::Conflict)?;
    let command_revision:i64=sqlx::query_scalar("INSERT INTO venue_order_mirrors (mirror_id,bot_id,bot_revision,permission_revision,relation_id,relation_revision,source_order_id,source_client_order_id,symbol,source_order_json,child_sequence,child_client_order_id,child_quantity,mirror_state,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending',$14,$14) RETURNING command_revision")
        .bind(&mirror).bind(bot).bind(number(row,"bot_revision")?).bind(number(row,"permission_revision")?).bind(&relation).bind(revision).bind(native).bind(&order.client_order_id).bind(order.symbol.to_string()).bind(source).bind(sequence).bind(&client).bind(quantity.to_string()).bind(stamp(now)?)
        .fetch_one(&mut *connection).await.map_err(unavailable)?;
    let risk = serde_json::json!({"max_order_notional":text(row,"max_order_notional")?,"max_total_notional":text(row,"max_total_notional")?,"max_deviation_bps":row.try_get::<i32,_>("max_deviation_bps").map_err(unavailable)?,"source_price":order.limit_price.ok_or(Error::Conflict)?.to_string(),"source_occurred_ms":order.created_ms.ok_or(Error::Conflict)?});
    sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,limit_price,rule_version,client_order_id,command_state,created_ms,updated_ms,copy_risk,mirror_order_id) VALUES ($1,'copy',$2,$3,$4,$5,$6,$7,$8,$9,$10,$17,$11,$12,$12,$13,'binance-pm-um-v1',$1,'pending',$14,$14,$15,$16)")
        .bind(client).bind(relation).bind(revision).bind(command_revision).bind(text(row,"follower_user_id")?).bind(text(row,"follower_trading_account_id")?).bind(text(row,"credential_id")?).bind(order.symbol.to_string())
        .bind(if order.position_side==venue_domain::PositionSide::Long{"long"}else{"short"}).bind(if reducing(order){"close"}else{"open"}).bind(if order.order_side==venue_domain::OrderSide::Buy{"buy"}else{"sell"})
        .bind(quantity.to_string()).bind(order.limit_price.ok_or(Error::Conflict)?.to_string()).bind(stamp(now)?).bind(risk).bind(mirror).bind(if order.post_only {"limit_post_only"}else{"limit_gtc"}).execute(connection).await.map_err(unavailable)?;
    Ok(())
}

async fn enqueue_cancel(
    connection: &mut PgConnection,
    row: &PgRow,
    mirror: &PgRow,
    now: u64,
) -> Result<bool, Error> {
    let Some(native) = mirror
        .try_get::<Option<String>, _>("child_native_order_id")
        .map_err(unavailable)?
    else {
        return Ok(false);
    };
    let mirror_id = text(mirror, "mirror_id")?;
    let revision: i64 = sqlx::query_scalar(
        "SELECT nextval(pg_get_serial_sequence('venue_order_mirrors','command_revision'))",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(unavailable)?;
    let command = identity(&[&mirror_id, "cancel", &revision.to_string()]);
    let inserted=sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,command_phase,order_kind,selected_native_order_id,rule_version,client_order_id,command_state,created_ms,updated_ms,copy_risk,mirror_order_id) SELECT $1,'copy',$2,$3,$4,$5,$6,$7,$8,'cancel','cancel_exact',$9,'binance-pm-um-v1',$1,'pending',$10,$10,c.copy_risk,$11 FROM venue_binance_commands c WHERE c.command_id=$12 ON CONFLICT DO NOTHING")
        .bind(command).bind(text(mirror,"relation_id")?).bind(number(mirror,"relation_revision")?).bind(revision).bind(text(row,"follower_user_id")?).bind(text(row,"follower_trading_account_id")?).bind(text(row,"credential_id")?).bind(text(mirror,"symbol")?).bind(native).bind(stamp(now)?).bind(&mirror_id).bind(text(mirror,"child_client_order_id")?).execute(&mut *connection).await.map_err(unavailable)?.rows_affected()>0;
    if inserted {
        sqlx::query("UPDATE venue_order_mirrors SET mirror_state='cancelling',cancel_attempts=cancel_attempts+1,updated_ms=$1 WHERE mirror_id=$2").bind(stamp(now)?).bind(mirror_id).execute(connection).await.map_err(unavailable)?;
    }
    Ok(inserted)
}
