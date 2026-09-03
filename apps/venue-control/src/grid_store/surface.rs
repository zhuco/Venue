use sqlx::{Postgres, Row, Transaction};
use venue_control_protocol::grid::{GridAnchor, GridInstanceState, GridOrderSemanticKey};

use super::{
    BinanceGridStore, GridDesiredOrder, GridDesiredSurface, GridInstanceSummary, GridStoreError,
    bytes_digest, corrupt_row, database_error, decimal, decimal_text, decode_position_side,
    decode_role, decode_state, integer, ms, order_side_name, position_side_name, role_name,
    unsigned, validate_desired_orders, validate_ids,
};

impl BinanceGridStore {
    /// Atomically commits the rolling anchor, complete desired surface, and plan metadata.
    /// Every expected revision participates in the CAS so a lifecycle transition fences stale
    /// runtime work before any part of the new plan becomes durable.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_plan_surface(
        &self,
        instance_id: &str,
        expected_instance_revision: u64,
        expected_config_revision: u64,
        expected_plan_revision: u64,
        next_plan_revision: u64,
        anchor: Option<&GridAnchor>,
        desired_digest: [u8; 32],
        orders: &[GridDesiredOrder],
        last_facts_ms: u64,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        validate_surface_input(
            instance_id,
            expected_plan_revision,
            next_plan_revision,
            orders,
            last_facts_ms,
            now_ms,
        )?;
        if let Some(anchor) = anchor {
            anchor.validate().map_err(|_| GridStoreError::Invalid)?;
            if anchor.observed_ms > last_facts_ms {
                return Err(GridStoreError::Invalid);
            }
        }
        if expected_instance_revision == 0 || expected_config_revision == 0 {
            return Err(GridStoreError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query(
            "SELECT owner_user_id,symbol,revision,current_config_revision,plan_revision,\
             instance_state,convergence_started_ms FROM venue_binance_grid_instances \
             WHERE instance_id=$1 FOR UPDATE",
        )
        .bind(instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Forbidden)?;
        let owner_user_id: String = row.try_get("owner_user_id").map_err(corrupt_row)?;
        let symbol: String = row.try_get("symbol").map_err(corrupt_row)?;
        let revision = unsigned(row.try_get("revision").map_err(corrupt_row)?)?;
        let config_revision = unsigned(
            row.try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let plan_revision = unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)?;
        let state = decode_state(row.try_get("instance_state").map_err(corrupt_row)?)?;
        if revision != expected_instance_revision
            || config_revision != expected_config_revision
            || plan_revision != expected_plan_revision
            || !plan_state_allows_commit(state)
        {
            return Err(GridStoreError::Conflict);
        }
        if let Some(anchor) = anchor {
            write_anchor(&mut tx, instance_id, config_revision, anchor, now_ms).await?;
        }
        replace_desired_rows(
            &mut tx,
            instance_id,
            &symbol,
            config_revision,
            next_plan_revision,
            desired_digest,
            orders,
            now_ms,
        )
        .await?;
        let previous_started = row
            .try_get::<Option<i64>, _>("convergence_started_ms")
            .map_err(corrupt_row)?;
        let convergence_started = if next_plan_revision != expected_plan_revision {
            ms(now_ms)?
        } else {
            previous_started.unwrap_or(ms(now_ms)?)
        };
        let updated = sqlx::query(
            "UPDATE venue_binance_grid_instances SET plan_revision=$1,desired_digest=$2,dirty=TRUE,\
             convergence_started_ms=$3,last_facts_ms=$4,revision=revision+1,updated_ms=$5 \
             WHERE instance_id=$6 AND revision=$7 AND current_config_revision=$8 \
               AND plan_revision=$9 AND instance_state IN ('start_pending','running')",
        )
        .bind(integer(next_plan_revision)?)
        .bind(desired_digest.as_slice())
        .bind(convergence_started)
        .bind(ms(last_facts_ms)?)
        .bind(ms(now_ms)?)
        .bind(instance_id)
        .bind(integer(expected_instance_revision)?)
        .bind(integer(expected_config_revision)?)
        .bind(integer(expected_plan_revision)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        if updated.rows_affected() != 1 {
            return Err(GridStoreError::Conflict);
        }
        tx.commit().await.map_err(database_error)?;
        self.load_owned(&owner_user_id, instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }

    pub async fn load_desired_orders(
        &self,
        instance_id: &str,
    ) -> Result<Option<GridDesiredSurface>, GridStoreError> {
        validate_ids(&[instance_id])?;
        let instance = sqlx::query(
            "SELECT symbol,current_config_revision,plan_revision,desired_digest \
             FROM venue_binance_grid_instances WHERE instance_id=$1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        let Some(instance) = instance else {
            return Ok(None);
        };
        let desired_digest: Option<Vec<u8>> =
            instance.try_get("desired_digest").map_err(corrupt_row)?;
        let Some(desired_digest) = desired_digest else {
            return Ok(None);
        };
        let desired_digest = bytes_digest(desired_digest)?;
        let config_revision = unsigned(
            instance
                .try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let plan_revision = unsigned(instance.try_get("plan_revision").map_err(corrupt_row)?)?;
        let symbol = instance
            .try_get::<String, _>("symbol")
            .map_err(corrupt_row)?
            .parse()
            .map_err(|_| GridStoreError::Corrupt)?;
        let rows = sqlx::query(
            "SELECT config_revision,plan_revision,desired_digest,semantic_key,client_order_id,\
             position_side,order_role,grid_level,order_sequence,quantity,limit_price \
             FROM venue_binance_grid_desired_orders WHERE instance_id=$1 ORDER BY semantic_key",
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let mut orders = Vec::with_capacity(rows.len());
        for row in rows {
            if unsigned(row.try_get("config_revision").map_err(corrupt_row)?)? != config_revision
                || unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)? != plan_revision
                || bytes_digest(row.try_get("desired_digest").map_err(corrupt_row)?)?
                    != desired_digest
            {
                return Err(GridStoreError::Corrupt);
            }
            let key = GridOrderSemanticKey {
                position_side: decode_position_side(
                    row.try_get("position_side").map_err(corrupt_row)?,
                )?,
                role: decode_role(row.try_get("order_role").map_err(corrupt_row)?)?,
                level: u16::try_from(row.try_get::<i16, _>("grid_level").map_err(corrupt_row)?)
                    .map_err(|_| GridStoreError::Corrupt)?,
                sequence: unsigned(row.try_get("order_sequence").map_err(corrupt_row)?)?,
            };
            if row
                .try_get::<String, _>("semantic_key")
                .map_err(corrupt_row)?
                != key.encoded()
            {
                return Err(GridStoreError::Corrupt);
            }
            orders.push(GridDesiredOrder {
                key,
                client_order_id: row.try_get("client_order_id").map_err(corrupt_row)?,
                quantity: decimal(row.try_get("quantity").map_err(corrupt_row)?)?,
                limit_price: decimal(row.try_get("limit_price").map_err(corrupt_row)?)?,
            });
        }
        validate_desired_orders(&orders).map_err(|_| GridStoreError::Corrupt)?;
        Ok(Some(GridDesiredSurface {
            instance_id: instance_id.to_owned(),
            symbol,
            config_revision,
            plan_revision,
            desired_digest,
            orders,
        }))
    }
}

pub(super) fn validate_surface_input(
    instance_id: &str,
    expected_plan_revision: u64,
    next_plan_revision: u64,
    orders: &[GridDesiredOrder],
    last_facts_ms: u64,
    now_ms: u64,
) -> Result<(), GridStoreError> {
    validate_ids(&[instance_id])?;
    validate_desired_orders(orders)?;
    if expected_plan_revision == 0
        || next_plan_revision < expected_plan_revision
        || next_plan_revision > expected_plan_revision.saturating_add(1)
        || last_facts_ms == 0
        || last_facts_ms > now_ms
    {
        return Err(GridStoreError::Invalid);
    }
    Ok(())
}

pub(super) async fn write_anchor(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: &str,
    config_revision: u64,
    anchor: &GridAnchor,
    now_ms: u64,
) -> Result<(), GridStoreError> {
    let existing = sqlx::query(
        "SELECT config_revision,anchor_revision,instrument_generation,anchor_price,price_step,\
         grid_quantity,source_native_trade_id,observed_ms \
         FROM venue_binance_grid_anchors WHERE instance_id=$1 FOR UPDATE",
    )
    .bind(instance_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)?;
    if let Some(row) = existing {
        let stored_revision = unsigned(row.try_get("anchor_revision").map_err(corrupt_row)?)?;
        if stored_revision == anchor.revision {
            let exact = unsigned(row.try_get("config_revision").map_err(corrupt_row)?)?
                == config_revision
                && unsigned(row.try_get("instrument_generation").map_err(corrupt_row)?)?
                    == anchor.instrument_generation
                && decimal(row.try_get("anchor_price").map_err(corrupt_row)?)? == anchor.price
                && decimal(row.try_get("price_step").map_err(corrupt_row)?)? == anchor.price_step
                && decimal(row.try_get("grid_quantity").map_err(corrupt_row)?)?
                    == anchor.grid_quantity
                && row
                    .try_get::<Option<String>, _>("source_native_trade_id")
                    .map_err(corrupt_row)?
                    == anchor.source_native_trade_id
                && unsigned(row.try_get("observed_ms").map_err(corrupt_row)?)?
                    == anchor.observed_ms;
            if !exact {
                return Err(GridStoreError::Conflict);
            }
            return Ok(());
        }
        if stored_revision > anchor.revision {
            return Err(GridStoreError::Conflict);
        }
        sqlx::query(
            "UPDATE venue_binance_grid_anchors SET config_revision=$1,anchor_revision=$2,\
             instrument_generation=$3,anchor_price=$4,price_step=$5,grid_quantity=$6,\
             source_native_trade_id=$7,observed_ms=$8,updated_ms=$9 WHERE instance_id=$10",
        )
        .bind(integer(config_revision)?)
        .bind(integer(anchor.revision)?)
        .bind(integer(anchor.instrument_generation)?)
        .bind(decimal_text(anchor.price))
        .bind(decimal_text(anchor.price_step))
        .bind(decimal_text(anchor.grid_quantity))
        .bind(&anchor.source_native_trade_id)
        .bind(ms(anchor.observed_ms)?)
        .bind(ms(now_ms)?)
        .bind(instance_id)
        .execute(&mut **tx)
        .await
        .map_err(database_error)?;
    } else {
        sqlx::query(
            "INSERT INTO venue_binance_grid_anchors \
             (instance_id,config_revision,anchor_revision,instrument_generation,anchor_price,\
              price_step,grid_quantity,source_native_trade_id,observed_ms,updated_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(instance_id)
        .bind(integer(config_revision)?)
        .bind(integer(anchor.revision)?)
        .bind(integer(anchor.instrument_generation)?)
        .bind(decimal_text(anchor.price))
        .bind(decimal_text(anchor.price_step))
        .bind(decimal_text(anchor.grid_quantity))
        .bind(&anchor.source_native_trade_id)
        .bind(ms(anchor.observed_ms)?)
        .bind(ms(now_ms)?)
        .execute(&mut **tx)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn replace_desired_rows(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: &str,
    symbol: &str,
    config_revision: u64,
    plan_revision: u64,
    desired_digest: [u8; 32],
    orders: &[GridDesiredOrder],
    now_ms: u64,
) -> Result<(), GridStoreError> {
    sqlx::query("DELETE FROM venue_binance_grid_desired_orders WHERE instance_id=$1")
        .bind(instance_id)
        .execute(&mut **tx)
        .await
        .map_err(database_error)?;
    for order in orders {
        sqlx::query(
            "INSERT INTO venue_binance_grid_desired_orders \
             (instance_id,config_revision,plan_revision,desired_digest,semantic_key,\
              client_order_id,symbol,position_side,order_role,grid_level,order_sequence,\
              order_side,quantity,limit_price,updated_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        )
        .bind(instance_id)
        .bind(integer(config_revision)?)
        .bind(integer(plan_revision)?)
        .bind(desired_digest.as_slice())
        .bind(order.key.encoded())
        .bind(&order.client_order_id)
        .bind(symbol)
        .bind(position_side_name(order.key.position_side))
        .bind(role_name(order.key.role))
        .bind(i16::try_from(order.key.level).map_err(|_| GridStoreError::Invalid)?)
        .bind(integer(order.key.sequence)?)
        .bind(order_side_name(order.key.order_side()))
        .bind(decimal_text(order.quantity))
        .bind(decimal_text(order.limit_price))
        .bind(ms(now_ms)?)
        .execute(&mut **tx)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

const fn plan_state_allows_commit(state: GridInstanceState) -> bool {
    matches!(
        state,
        GridInstanceState::StartPending | GridInstanceState::Running
    )
}
