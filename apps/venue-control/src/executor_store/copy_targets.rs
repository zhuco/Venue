use super::*;
use std::collections::{BTreeMap, BTreeSet};
use venue_domain::domain::PositionSide;

#[derive(serde::Serialize)]
struct TargetWrite {
    relation_id: String,
    owner_user_id: String,
    trading_account_id: String,
    credential_id: String,
    relation_revision: i64,
    target_revision: i64,
    copyable_quantity: String,
    target_quantity: String,
    observed_quantity: String,
    command_id: Option<String>,
    command_phase: &'static str,
    order_side: &'static str,
    requested_quantity: String,
    dirty: bool,
    copy_risk: crate::executor_exchange::CopyRiskContext,
}

pub(super) async fn record_source_fill_and_plan(
    store: &PgExecutorStore,
    kol_user_id: &str,
    fill: &KolSourceFill,
    now_ms: u64,
) -> Result<Vec<PlannedCopyCommand>, BinanceCommandLedgerError> {
    if fill.quantity <= Decimal::ZERO
        || fill.price <= Decimal::ZERO
        || !matches!(fill.position_side, PositionSide::Long | PositionSide::Short)
        || now_ms < fill.observed_ms
        || fill.observed_ms < fill.occurred_ms
    {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let now = ms(now_ms)?;
    let mut tx = store.pool.begin().await.map_err(unavailable)?;
    sqlx::query("SELECT kol_user_id FROM venue_kol_profiles WHERE kol_user_id=$1 AND leader_trading_account_id=$2 FOR SHARE")
        .bind(kol_user_id).bind(&fill.leader_trading_account_id)
        .fetch_one(&mut *tx).await.map_err(unavailable)?;
    let inserted = sqlx::query("INSERT INTO venue_kol_source_fills (kol_trading_account_id,kol_user_id,native_symbol,native_trade_id,symbol,order_side,position_side,quantity,price,occurred_ms,observed_ms,payload_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING")
        .bind(&fill.leader_trading_account_id).bind(kol_user_id).bind(&fill.native_symbol).bind(&fill.native_trade_id)
        .bind(&fill.symbol).bind(order_side(fill.order_side)).bind(position_side(fill.position_side))
        .bind(fill.quantity.to_string()).bind(fill.price.to_string()).bind(ms(fill.occurred_ms)?)
        .bind(ms(fill.observed_ms)?).bind(fill.payload_digest.as_slice())
        .execute(&mut *tx).await.map_err(unavailable)?;
    if inserted.rows_affected() == 0 {
        let original = sqlx::query("SELECT kol_user_id,symbol,order_side,position_side,quantity,price,occurred_ms FROM venue_kol_source_fills WHERE kol_trading_account_id=$1 AND native_symbol=$2 AND native_trade_id=$3")
            .bind(&fill.leader_trading_account_id).bind(&fill.native_symbol).bind(&fill.native_trade_id)
            .fetch_one(&mut *tx).await.map_err(unavailable)?;
        if text(&original, "kol_user_id")? != kol_user_id
            || text(&original, "symbol")? != fill.symbol
            || text(&original, "order_side")? != order_side(fill.order_side)
            || text(&original, "position_side")? != position_side(fill.position_side)
            || decimal(&original, "quantity")? != fill.quantity
            || decimal(&original, "price")? != fill.price
            || unsigned_ms(&original, "occurred_ms")? != fill.occurred_ms
        {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        tx.commit().await.map_err(unavailable)?;
        return Ok(Vec::new());
    }
    // The relation lock serializes target updates and lifecycle changes, including targets which
    // do not exist yet. All follower rows are read at once; no SQL round trip per follower.
    let relations = sqlx::query(
        "SELECT r.relation_id,r.follower_user_id,r.follower_trading_account_id,r.credential_id,\
         r.revision,r.allocated_capital,r.multiplier,r.baseline_json,p.strategy_capital,\
         r.max_order_notional,r.max_total_notional,r.max_deviation_bps,\
         COALESCE(t.copyable_quantity,'0') AS copyable_quantity,\
         COALESCE(t.observed_quantity,'0') AS observed_quantity,\
         COALESCE(t.target_revision,0) AS target_revision \
         FROM venue_kol_follow_relations r JOIN venue_kol_profiles p ON p.kol_user_id=r.kol_user_id \
         LEFT JOIN venue_kol_copy_targets t ON t.relation_id=r.relation_id AND t.symbol=$3 AND t.position_side=$4 \
         WHERE r.kol_user_id=$1 AND r.leader_trading_account_id=$2 AND r.relation_state='active' \
           AND p.profile_state='enabled' AND r.baseline_json->>'target_model'='1' AND r.allowed_symbols @> jsonb_build_array($3::text) \
         ORDER BY r.relation_id FOR UPDATE OF r",
    )
    .bind(kol_user_id).bind(&fill.leader_trading_account_id).bind(&fill.symbol)
    .bind(position_side(fill.position_side)).fetch_all(&mut *tx).await.map_err(unavailable)?;
    let accounts = relations
        .iter()
        .map(|row| text(row, "follower_trading_account_id"))
        .collect::<Result<Vec<_>, _>>()?;
    let credentials = sqlx::query("SELECT credential_id FROM venue_api_credentials WHERE trading_account_id=ANY($1) AND deleted_ms IS NULL ORDER BY credential_id FOR UPDATE")
        .bind(&accounts).fetch_all(&mut *tx).await.map_err(unavailable)?;
    let live_credentials = credentials
        .iter()
        .map(|row| text(row, "credential_id"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let busy: BTreeMap<String, i64> = sqlx::query_as("SELECT trading_account_id,count(*) FROM venue_binance_commands WHERE trading_account_id=ANY($1) AND command_state IN ('pending','sending','accepted','reconcile_required') GROUP BY trading_account_id")
        .bind(&accounts).fetch_all(&mut *tx).await.map_err(unavailable)?.into_iter().collect();
    let mut writes = Vec::with_capacity(relations.len());
    for row in relations {
        let baseline: Option<serde_json::Value> =
            row.try_get("baseline_json").map_err(unavailable)?;
        if baseline
            .as_ref()
            .and_then(|value| value.get("target_model"))
            .and_then(serde_json::Value::as_u64)
            != Some(1)
        {
            let relation = text(&row, "relation_id")?;
            sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='needs_attention',active_slot=NULL,attention_code='activation_baseline_required',revision=revision+1,updated_ms=$2 WHERE relation_id=$1")
                .bind(&relation).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
            crate::kol_executor::cancel_pending_copy_commands(
                &mut tx,
                &relation,
                now,
                "activation_baseline_required",
            )
            .await
            .map_err(unavailable)?;
            continue;
        }
        let Some(cutoff) = baseline
            .as_ref()
            .and_then(|value| value.get("baseline_ms"))
            .and_then(serde_json::Value::as_u64)
        else {
            // A missing activation boundary cannot be treated as permission to replay history.
            continue;
        };
        if fill.occurred_ms <= cutoff {
            continue;
        }
        let prior = decimal(&row, "copyable_quantity")?;
        let copyable = advance_copyable(prior, fill)?;
        let target = if copyable == Decimal::ZERO {
            Decimal::ZERO
        } else {
            scaled_copy_quantity(
                copyable,
                decimal(&row, "allocated_capital")?,
                decimal(&row, "strategy_capital")?,
                decimal(&row, "multiplier")?,
            )?
        };
        let observed = decimal(&row, "observed_quantity")?;
        if observed < Decimal::ZERO {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let revision = row
            .try_get::<i64, _>("target_revision")
            .map_err(unavailable)?
            .checked_add(1)
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        let relation_id = text(&row, "relation_id")?;
        let account = text(&row, "follower_trading_account_id")?;
        let credential = text(&row, "credential_id")?;
        let opening = target > observed;
        let phase = if opening { "open" } else { "close" };
        let changed = target != observed;
        let dispatchable = changed
            && busy.get(&account).copied().unwrap_or(0) == 0
            && live_credentials.contains(&credential);
        let command_id = dispatchable.then(|| {
            deterministic_id(
                &relation_id,
                &fill.symbol,
                fill.position_side,
                revision,
                phase,
            )
        });
        let copy_risk = crate::executor_exchange::CopyRiskContext {
            max_order_notional: decimal(&row, "max_order_notional")?,
            max_total_notional: decimal(&row, "max_total_notional")?,
            max_deviation_bps: u32::try_from(
                row.try_get::<i32, _>("max_deviation_bps")
                    .map_err(unavailable)?,
            )
            .map_err(|_| BinanceCommandLedgerError::Conflict)?,
            source_price: fill.price,
            source_occurred_ms: fill.occurred_ms,
        };
        copy_risk
            .validate()
            .map_err(|_| BinanceCommandLedgerError::Conflict)?;
        writes.push(TargetWrite {
            relation_id,
            owner_user_id: text(&row, "follower_user_id")?,
            trading_account_id: account,
            credential_id: credential,
            relation_revision: row.try_get("revision").map_err(unavailable)?,
            target_revision: revision,
            copyable_quantity: copyable.to_string(),
            target_quantity: target.to_string(),
            observed_quantity: observed.to_string(),
            command_id,
            command_phase: phase,
            order_side: order_for(fill.position_side, opening),
            requested_quantity: (target - observed).abs().to_string(),
            dirty: changed && !dispatchable,
            copy_risk,
        });
    }
    let values = serde_json::to_value(writes).map_err(|_| BinanceCommandLedgerError::Conflict)?;
    let rows = sqlx::query(
        "WITH input AS (SELECT * FROM jsonb_to_recordset($1) AS x(\
           relation_id text,owner_user_id text,trading_account_id text,credential_id text,\
           relation_revision bigint,target_revision bigint,copyable_quantity text,target_quantity text,\
           observed_quantity text,command_id text,command_phase text,order_side text,\
           requested_quantity text,dirty boolean,copy_risk jsonb)), \
         targets AS (INSERT INTO venue_kol_copy_targets (relation_id,symbol,position_side,\
           copyable_quantity,target_quantity,observed_quantity,target_revision,last_native_symbol,\
           last_native_trade_id,dirty,updated_ms) \
           SELECT relation_id,$2,$3,copyable_quantity,target_quantity,observed_quantity,target_revision,\
             $4,$5,dirty,$6 FROM input \
           ON CONFLICT (relation_id,symbol,position_side) DO UPDATE SET \
             copyable_quantity=EXCLUDED.copyable_quantity,target_quantity=EXCLUDED.target_quantity,\
             target_revision=EXCLUDED.target_revision,last_native_symbol=EXCLUDED.last_native_symbol,\
             last_native_trade_id=EXCLUDED.last_native_trade_id,dirty=EXCLUDED.dirty,updated_ms=EXCLUDED.updated_ms \
           RETURNING relation_id) \
         INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,\
           target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,\
           command_phase,order_kind,order_side,requested_quantity,target_quantity,rule_version,\
           client_order_id,command_state,source_digest,created_ms,updated_ms,copy_risk) \
         SELECT i.command_id,'copy',i.relation_id,i.relation_revision,i.target_revision,i.owner_user_id,\
           i.trading_account_id,i.credential_id,$2,$3,i.command_phase,'market',i.order_side,\
           i.requested_quantity,i.target_quantity,'binance-pm-um-v1',i.command_id,'pending',$7,$6,$6,i.copy_risk \
         FROM input i JOIN targets t USING (relation_id) WHERE i.command_id IS NOT NULL \
         ON CONFLICT DO NOTHING RETURNING command_id,relation_id,trading_account_id,target_revision",
    )
    .bind(values).bind(&fill.symbol).bind(position_side(fill.position_side))
    .bind(&fill.native_symbol).bind(&fill.native_trade_id).bind(now).bind(fill.payload_digest.as_slice())
    .fetch_all(&mut *tx).await.map_err(unavailable)?;
    let planned = rows
        .into_iter()
        .map(|row| {
            Ok(PlannedCopyCommand {
                client_order_id: text(&row, "command_id")?,
                command_id: text(&row, "command_id")?,
                relation_id: text(&row, "relation_id")?,
                trading_account_id: text(&row, "trading_account_id")?,
                target_revision: unsigned_ms(&row, "target_revision")?,
            })
        })
        .collect::<Result<Vec<_>, BinanceCommandLedgerError>>()?;
    tx.commit().await.map_err(unavailable)?;
    Ok(planned)
}

fn advance_copyable(
    prior: Decimal,
    fill: &KolSourceFill,
) -> Result<Decimal, BinanceCommandLedgerError> {
    if prior < Decimal::ZERO {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let increasing = matches!(
        (fill.position_side, fill.order_side),
        (PositionSide::Long, venue_domain::domain::OrderSide::Buy)
            | (PositionSide::Short, venue_domain::domain::OrderSide::Sell)
    );
    if increasing {
        prior
            .checked_add(fill.quantity)
            .ok_or(BinanceCommandLedgerError::Conflict)
    } else {
        Ok((prior - fill.quantity.min(prior)).max(Decimal::ZERO))
    }
}

fn text(row: &sqlx::postgres::PgRow, field: &str) -> Result<String, BinanceCommandLedgerError> {
    row.try_get(field).map_err(unavailable)
}

fn unavailable(_: sqlx::Error) -> BinanceCommandLedgerError {
    BinanceCommandLedgerError::Unavailable
}
