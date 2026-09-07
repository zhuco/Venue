use super::{MAX_MIRROR_ORDERS_PER_RELATION, planner, store::*};
use crate::kol_executor::BinanceCommandLedgerError as Error;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Row, postgres::PgRow};
use std::collections::BTreeMap;
use venue_control_protocol::{
    follow_sizing::FollowSizing,
    kol::{TerminalAccountProjection, TerminalConditionalOrder},
};
use venue_domain::{OrderSide, PositionSide, Symbol};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExtendedSource {
    Market {
        native_order_id: String,
        client_order_id: String,
        symbol: Symbol,
        order_side: OrderSide,
        position_side: PositionSide,
        #[serde(with = "rust_decimal::serde::str")]
        quantity: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        reference_price: Decimal,
        occurred_ms: u64,
    },
    Stop {
        order: TerminalConditionalOrder,
    },
}

pub(super) fn source_send_allowed(
    kind: &str,
    value: serde_json::Value,
    current: Option<&TerminalAccountProjection>,
    market_source_exists: bool,
) -> Result<bool, Error> {
    let source: ExtendedSource = serde_json::from_value(value).map_err(|_| Error::Conflict)?;
    if source.kind() != kind {
        return Err(Error::Conflict);
    }
    Ok(match source {
        ExtendedSource::Market { .. } => market_source_exists && current.is_some(),
        ExtendedSource::Stop { order } => current.is_some_and(|projection| {
            projection
                .conditional_orders
                .iter()
                .any(|current| current == &order)
        }),
    })
}

impl ExtendedSource {
    fn kind(&self) -> &'static str {
        match self {
            Self::Market { .. } => "market",
            Self::Stop { .. } => "stop",
        }
    }
    fn native_id(&self) -> &str {
        match self {
            Self::Market {
                native_order_id, ..
            } => native_order_id,
            Self::Stop { order } => &order.native_order_id,
        }
    }
    fn client_id(&self) -> &str {
        match self {
            Self::Market {
                client_order_id, ..
            } => client_order_id,
            Self::Stop { order } => &order.client_order_id,
        }
    }
    fn symbol(&self) -> &Symbol {
        match self {
            Self::Market { symbol, .. } => symbol,
            Self::Stop { order } => &order.symbol,
        }
    }
    fn side(&self) -> OrderSide {
        match self {
            Self::Market { order_side, .. } => *order_side,
            Self::Stop { order } => order.order_side,
        }
    }
    fn position_side(&self) -> PositionSide {
        match self {
            Self::Market { position_side, .. } => *position_side,
            Self::Stop { order } => order.position_side,
        }
    }
    fn quantity(&self) -> Decimal {
        match self {
            Self::Market { quantity, .. } => *quantity,
            Self::Stop { order } => order.quantity,
        }
    }
    fn price(&self) -> Decimal {
        match self {
            Self::Market {
                reference_price, ..
            } => *reference_price,
            Self::Stop { order } => order.trigger_price,
        }
    }
    fn occurred_ms(&self) -> u64 {
        match self {
            Self::Market { occurred_ms, .. } => *occurred_ms,
            Self::Stop { order } => order.created_ms.unwrap_or_default(),
        }
    }
    fn reducing(&self) -> bool {
        matches!(
            (self.position_side(), self.side()),
            (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
        )
    }
}

pub(super) async fn plan_relation(pool: &PgPool, relation: &str, now: u64) -> Result<bool, Error> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let row=sqlx::query("SELECT r.*,b.bot_id,b.bot_state,b.revision AS bot_revision,b.permission_revision,b.started_ms,b.credential_id AS leader_credential_id,b.strategy_capital,p.profile_state,EXISTS(SELECT 1 FROM venue_api_credentials lc WHERE lc.credential_id=b.credential_id AND lc.user_id=b.owner_user_id AND lc.deleted_ms IS NULL AND lc.verification_json->>'verification'='verified') AS leader_verified,EXISTS(SELECT 1 FROM venue_api_credentials fc WHERE fc.credential_id=r.credential_id AND fc.user_id=r.follower_user_id AND fc.trading_account_id=r.follower_trading_account_id AND fc.deleted_ms IS NULL AND fc.verification_json->>'verification'='verified') AS follower_verified,COALESCE(g.enabled,false) AS granted,COALESCE(g.revision,0) AS grant_revision,lp.projection_json AS leader_projection,fp.projection_json AS follower_projection FROM venue_kol_follow_relations r JOIN venue_kol_profiles p ON p.kol_user_id=r.kol_user_id JOIN venue_leader_bots b ON b.owner_user_id=r.kol_user_id AND b.bot_state<>'stopped' LEFT JOIN venue_leader_bot_permissions g ON g.kol_user_id=p.kol_user_id LEFT JOIN venue_binance_account_projections lp ON lp.credential_id=b.credential_id LEFT JOIN venue_binance_account_projections fp ON fp.credential_id=r.credential_id WHERE r.relation_id=$1 FOR SHARE OF b FOR UPDATE OF r")
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
    let cutoff = baseline
        .as_ref()
        .and_then(|v| v.get("baseline_ms"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(u64::MAX)
        .max(
            row.try_get::<Option<i64>, _>("started_ms")
                .map_err(unavailable)?
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(u64::MAX),
        );
    let allowed: Vec<String> =
        serde_json::from_value(row.try_get("allowed_symbols").map_err(unavailable)?)
            .map_err(|_| Error::Conflict)?;
    let leader_account = text(&row, "leader_trading_account_id")?;
    let source = projection(
        row.try_get("leader_projection").map_err(unavailable)?,
        &leader_account,
        now,
    )?;
    let follower = projection(
        row.try_get("follower_projection").map_err(unavailable)?,
        &account,
        now,
    )?;
    let mut desired = BTreeMap::<(String, String, String), ExtendedSource>::new();
    if active {
        if let Some(source) = source.as_ref() {
            for order in &source.conditional_orders {
                if order.created_ms.is_some_and(|time| time > cutoff)
                    && (allowed.is_empty() || allowed.contains(&order.symbol.to_string()))
                    && matches!(
                        order.position_side,
                        PositionSide::Long | PositionSide::Short
                    )
                {
                    let value = ExtendedSource::Stop {
                        order: order.clone(),
                    };
                    desired.insert(
                        (
                            "stop".into(),
                            order.symbol.to_string(),
                            order.native_order_id.clone(),
                        ),
                        value,
                    );
                }
            }
        }
        let markets=sqlx::query("SELECT native_order_id,client_order_id,symbol,order_side,position_side,original_quantity,reference_price,occurred_ms FROM venue_kol_source_market_orders WHERE leader_trading_account_id=$1 AND kol_user_id=$2 AND occurred_ms>$3 ORDER BY occurred_ms,native_order_id")
            .bind(&leader_account).bind(text(&row,"kol_user_id")?).bind(stamp(cutoff)?).fetch_all(&mut *tx).await.map_err(unavailable)?;
        for market in markets {
            let symbol: Symbol = text(&market, "symbol")?
                .parse()
                .map_err(|_| Error::Conflict)?;
            if !allowed.is_empty() && !allowed.contains(&symbol.to_string()) {
                continue;
            }
            let position_side = match text(&market, "position_side")?.as_str() {
                "long" => PositionSide::Long,
                "short" => PositionSide::Short,
                _ => return Err(Error::Conflict),
            };
            let order_side = match text(&market, "order_side")?.as_str() {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                _ => return Err(Error::Conflict),
            };
            let value = ExtendedSource::Market {
                native_order_id: text(&market, "native_order_id")?,
                client_order_id: text(&market, "client_order_id")?,
                symbol: symbol.clone(),
                order_side,
                position_side,
                quantity: decimal(&market, "original_quantity")?,
                reference_price: decimal(&market, "reference_price")?,
                occurred_ms: u64::try_from(number(&market, "occurred_ms")?)
                    .map_err(|_| Error::Conflict)?,
            };
            desired.insert(
                (
                    "market".into(),
                    symbol.to_string(),
                    value.native_id().to_owned(),
                ),
                value,
            );
        }
    }
    let mirrors=sqlx::query("SELECT m.*,c.command_state AS place_state FROM venue_order_mirrors m LEFT JOIN venue_binance_commands c ON c.command_id=m.child_client_order_id WHERE m.relation_id=$1 AND m.source_kind IN ('market','stop') ORDER BY m.source_kind,m.symbol,m.source_order_id,m.child_sequence FOR UPDATE OF m")
        .bind(relation).fetch_all(&mut *tx).await.map_err(unavailable)?;
    let mut latest = BTreeMap::new();
    let mut has_work = false;
    let mut live_count = mirrors
        .iter()
        .filter(|m| {
            text(m, "mirror_state").is_ok_and(|s| !matches!(s.as_str(), "terminal" | "blocked"))
        })
        .count();
    for mirror in mirrors {
        let key = (
            text(&mirror, "source_kind")?,
            text(&mirror, "symbol")?,
            text(&mirror, "source_order_id")?,
        );
        let original: ExtendedSource =
            serde_json::from_value(mirror.try_get("source_order_json").map_err(unavailable)?)
                .map_err(|_| Error::Conflict)?;
        let state = text(&mirror, "mirror_state")?;
        if state == "pending"
            && matches!(
                mirror
                    .try_get::<Option<String>, _>("place_state")
                    .map_err(unavailable)?
                    .as_deref(),
                Some("cancelled" | "rejected")
            )
        {
            sqlx::query("UPDATE venue_order_mirrors SET mirror_state='blocked',attention_code='child_not_placed',updated_ms=$1 WHERE mirror_id=$2").bind(stamp(now)?).bind(text(&mirror,"mirror_id")?).execute(&mut *tx).await.map_err(unavailable)?;
            latest.insert(key, mirror);
            continue;
        }
        // Exact Algo readback can confirm a stop before the follower projection contains it.
        // Keep that identity until a source/lifecycle change requires cancellation.
        let obsolete = !active
            || number(&mirror, "relation_revision")? != number(&row, "revision")?
            || number(&mirror, "bot_revision")? != number(&row, "bot_revision")?
            || (source.is_some() && desired.get(&key).is_none_or(|current| current != &original));
        if obsolete
            && state == "pending"
            && mirror
                .try_get::<Option<String>, _>("place_state")
                .map_err(unavailable)?
                .as_deref()
                == Some("pending")
        {
            sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$1,updated_ms=$1,sanitized_error_code='mirror_target_retired' WHERE command_id=$2 AND command_state='pending'").bind(stamp(now)?).bind(text(&mirror,"child_client_order_id")?).execute(&mut *tx).await.map_err(unavailable)?;
            sqlx::query("UPDATE venue_order_mirrors SET mirror_state='terminal',updated_ms=$1 WHERE mirror_id=$2").bind(stamp(now)?).bind(text(&mirror,"mirror_id")?).execute(&mut *tx).await.map_err(unavailable)?;
            depth = depth.saturating_sub(1);
            live_count = live_count.saturating_sub(1);
        } else if obsolete
            && state == "live"
            && matches!(original, ExtendedSource::Stop { .. })
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
                let original: ExtendedSource = serde_json::from_value(
                    previous.try_get("source_order_json").map_err(unavailable)?,
                )
                .map_err(|_| Error::Conflict)?;
                if !matches!(
                    text(previous, "mirror_state")?.as_str(),
                    "terminal" | "blocked"
                ) || original == order
                {
                    continue;
                }
            }
            let mut quantity = scaled_quantity(&order, &row)?;
            if order.reducing() {
                quantity = quantity.min(available_close(
                    follower.as_ref().ok_or(Error::Conflict)?,
                    &order,
                )?);
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

fn scaled_quantity(order: &ExtendedSource, row: &PgRow) -> Result<Decimal, Error> {
    let sizing: FollowSizing =
        serde_json::from_value(row.try_get("sizing_json").map_err(unavailable)?)
            .map_err(|_| Error::Conflict)?;
    match sizing {
        FollowSizing::Proportional => crate::kol_executor::scaled_copy_quantity(
            order.quantity(),
            decimal(row, "allocated_capital")?,
            decimal(row, "strategy_capital")?,
            decimal(row, "multiplier")?,
        ),
        FollowSizing::FixedNotional { notional } => notional
            .checked_div(order.price())
            .filter(|v| *v > Decimal::ZERO)
            .ok_or(Error::Conflict),
    }
}

fn available_close(
    projection: &TerminalAccountProjection,
    order: &ExtendedSource,
) -> Result<Decimal, Error> {
    let position = projection
        .positions
        .iter()
        .find(|p| p.symbol == *order.symbol() && p.position_side == order.position_side())
        .map(|p| p.quantity)
        .unwrap_or_default();
    let regular = projection
        .open_orders
        .iter()
        .filter(|o| {
            o.symbol == *order.symbol()
                && o.position_side == order.position_side()
                && planner::reducing(o)
        })
        .try_fold(Decimal::ZERO, |sum, o| {
            sum.checked_add(o.quantity - o.filled_quantity.unwrap_or_default())
                .ok_or(Error::Conflict)
        })?;
    let conditional = projection
        .conditional_orders
        .iter()
        .filter(|o| {
            o.symbol == *order.symbol()
                && o.position_side == order.position_side()
                && matches!(
                    (o.position_side, o.order_side),
                    (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
                )
        })
        .try_fold(Decimal::ZERO, |sum, o| {
            sum.checked_add(o.quantity).ok_or(Error::Conflict)
        })?;
    Ok((position - regular - conditional).max(Decimal::ZERO))
}

async fn enqueue_place(
    connection: &mut PgConnection,
    row: &PgRow,
    order: &ExtendedSource,
    sequence: i64,
    quantity: Decimal,
    now: u64,
) -> Result<(), Error> {
    let relation = text(row, "relation_id")?;
    let revision = number(row, "revision")?;
    let bot = text(row, "bot_id")?;
    let mirror = identity(&[
        &bot,
        &number(row, "bot_revision")?.to_string(),
        &relation,
        &revision.to_string(),
        order.kind(),
        &order.symbol().to_string(),
        order.native_id(),
        &sequence.to_string(),
    ]);
    let client = identity(&[&mirror, "place"]);
    let source = serde_json::to_value(order).map_err(|_| Error::Conflict)?;
    let command_revision:i64=sqlx::query_scalar("INSERT INTO venue_order_mirrors (mirror_id,bot_id,bot_revision,permission_revision,relation_id,relation_revision,source_order_id,source_client_order_id,symbol,source_order_json,source_kind,child_sequence,child_client_order_id,child_quantity,mirror_state,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'pending',$15,$15) RETURNING command_revision")
        .bind(&mirror).bind(bot).bind(number(row,"bot_revision")?).bind(number(row,"permission_revision")?).bind(&relation).bind(revision).bind(order.native_id()).bind(order.client_id()).bind(order.symbol().to_string()).bind(source).bind(order.kind()).bind(sequence).bind(&client).bind(quantity.to_string()).bind(stamp(now)?).fetch_one(&mut *connection).await.map_err(unavailable)?;
    let mut risk = serde_json::json!({"round_open_quantity_up":!order.reducing(),"max_order_notional":text(row,"max_order_notional")?,"max_total_notional":text(row,"max_total_notional")?,"max_deviation_bps":row.try_get::<i32,_>("max_deviation_bps").map_err(unavailable)?,"source_price":order.price().to_string(),"source_occurred_ms":order.occurred_ms()});
    if !order.reducing() {
        risk["open_quantity_rounding"] =
            serde_json::Value::String("minimum_up_otherwise_down".into());
    }
    let (kind, trigger, working) = match order {
        ExtendedSource::Market { .. } => ("market", None, None),
        ExtendedSource::Stop { order } => (
            "stop_market",
            Some(order.trigger_price.to_string()),
            Some(order.working_type.clone()),
        ),
    };
    sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,trigger_price,working_type,rule_version,client_order_id,command_state,created_ms,updated_ms,copy_risk,mirror_order_id) VALUES ($1,'copy',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13,$14,$15,'binance-pm-um-v1',$1,'pending',$16,$16,$17,$18)")
        .bind(client).bind(relation).bind(revision).bind(command_revision).bind(text(row,"follower_user_id")?).bind(text(row,"follower_trading_account_id")?).bind(text(row,"credential_id")?).bind(order.symbol().to_string()).bind(if order.position_side()==PositionSide::Long{"long"}else{"short"}).bind(if order.reducing(){"close"}else{"open"}).bind(kind).bind(if order.side()==OrderSide::Buy{"buy"}else{"sell"}).bind(quantity.to_string()).bind(trigger).bind(working).bind(stamp(now)?).bind(risk).bind(mirror).execute(connection).await.map_err(unavailable)?;
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
    let inserted=sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,command_phase,order_kind,selected_native_order_id,rule_version,client_order_id,command_state,created_ms,updated_ms,copy_risk,mirror_order_id) SELECT $1,'copy',$2,$3,$4,$5,$6,$7,$8,'cancel','cancel_algo_exact',$9,'binance-pm-um-v1',$1,'pending',$10,$10,c.copy_risk,$11 FROM venue_binance_commands c WHERE c.command_id=$12 ON CONFLICT DO NOTHING")
        .bind(command).bind(text(mirror,"relation_id")?).bind(number(mirror,"relation_revision")?).bind(revision).bind(text(row,"follower_user_id")?).bind(text(row,"follower_trading_account_id")?).bind(text(row,"credential_id")?).bind(text(mirror,"symbol")?).bind(native).bind(stamp(now)?).bind(&mirror_id).bind(text(mirror,"child_client_order_id")?).execute(&mut *connection).await.map_err(unavailable)?.rows_affected()>0;
    if inserted {
        sqlx::query("UPDATE venue_order_mirrors SET mirror_state='cancelling',cancel_attempts=cancel_attempts+1,updated_ms=$1 WHERE mirror_id=$2").bind(stamp(now)?).bind(mirror_id).execute(connection).await.map_err(unavailable)?;
    }
    Ok(inserted)
}
