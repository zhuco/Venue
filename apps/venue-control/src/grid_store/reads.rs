use std::collections::BTreeMap;

use rust_decimal::Decimal;
use sqlx::Row;
use venue_control_protocol::kol::{ExecutorCommandOrigin, TerminalFill};
use venue_domain::Symbol;

use super::{
    BinanceGridStore, GridCommandStatus, GridFillAllocation, GridReduceReservation, GridStoreError,
    corrupt_row, database_error, decimal, decode_command_state, decode_grid_command_status,
    decode_position_side, decode_role, integer, ms, positive, unsigned, validate_ids,
};

impl BinanceGridStore {
    pub async fn has_nonterminal_account_commands(
        &self,
        trading_account_id: &str,
    ) -> Result<bool, GridStoreError> {
        validate_ids(&[trading_account_id])?;
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 \
             AND command_state IN ('pending','sending','accepted','reconcile_required'))",
        )
        .bind(trading_account_id)
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)
    }

    pub async fn load_grid_commands(
        &self,
        instance_id: &str,
        config_revision: u64,
        plan_revision: u64,
    ) -> Result<Vec<GridCommandStatus>, GridStoreError> {
        validate_ids(&[instance_id])?;
        if config_revision == 0 || plan_revision == 0 {
            return Err(GridStoreError::Invalid);
        }
        let rows = sqlx::query(
            "SELECT command_id,client_order_id,grid_semantic_key,command_phase,order_kind,\
             command_state,native_order_id,selected_native_order_id,target_client_order_id,\
             sanitized_error_code,updated_ms FROM venue_binance_commands \
             WHERE command_origin='grid' AND grid_instance_id=$1 AND grid_config_revision=$2 \
               AND grid_plan_revision=$3 \
             ORDER BY created_ms,command_id",
        )
        .bind(instance_id)
        .bind(integer(config_revision)?)
        .bind(integer(plan_revision)?)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.iter().map(decode_grid_command_status).collect()
    }

    pub async fn has_nonterminal_grid_mutations(
        &self,
        instance_id: &str,
        plan_revision: Option<u64>,
    ) -> Result<bool, GridStoreError> {
        validate_ids(&[instance_id])?;
        if plan_revision == Some(0) {
            return Err(GridStoreError::Invalid);
        }
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE command_origin='grid' \
             AND grid_instance_id=$1 AND ($2::bigint IS NULL OR grid_plan_revision=$2) \
             AND command_state IN ('pending','sending','accepted','reconcile_required'))",
        )
        .bind(instance_id)
        .bind(plan_revision.map(integer).transpose()?)
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)
    }

    pub async fn latest_grid_command_updated_ms(
        &self,
        instance_id: &str,
    ) -> Result<Option<u64>, GridStoreError> {
        validate_ids(&[instance_id])?;
        let value = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(updated_ms) FROM venue_binance_commands WHERE command_origin='grid' \
             AND grid_instance_id=$1",
        )
        .bind(instance_id)
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)?;
        value.map(unsigned).transpose()
    }

    /// Returns in-flight closes plus newly reconciled closes that may be newer than projection.
    /// Callers exclude their own instance and client IDs visible in signed open orders.
    pub async fn load_reduce_reservations(
        &self,
        trading_account_id: &str,
        symbol: &Symbol,
    ) -> Result<Vec<GridReduceReservation>, GridStoreError> {
        validate_ids(&[trading_account_id])?;
        let rows = sqlx::query(
            "SELECT command_id,command_origin,grid_instance_id,client_order_id,position_side,\
             requested_quantity,command_state,updated_ms FROM venue_binance_commands \
             WHERE trading_account_id=$1 AND symbol=$2 AND command_phase='close' \
               AND command_state IN (\
                   'pending','sending','accepted','reconcile_required','reconciled') \
             ORDER BY created_ms,command_id",
        )
        .bind(trading_account_id)
        .bind(symbol.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.iter()
            .map(|row| {
                let quantity = decimal(row.try_get("requested_quantity").map_err(corrupt_row)?)?;
                let position_side =
                    decode_position_side(row.try_get("position_side").map_err(corrupt_row)?)?;
                let origin = decode_origin(row.try_get("command_origin").map_err(corrupt_row)?)?;
                let grid_instance_id: Option<String> =
                    row.try_get("grid_instance_id").map_err(corrupt_row)?;
                if !positive(quantity) {
                    return Err(GridStoreError::Corrupt);
                }
                if (origin == ExecutorCommandOrigin::Grid) != grid_instance_id.is_some() {
                    return Err(GridStoreError::Corrupt);
                }
                Ok(GridReduceReservation {
                    command_id: row.try_get("command_id").map_err(corrupt_row)?,
                    origin,
                    grid_instance_id,
                    client_order_id: row.try_get("client_order_id").map_err(corrupt_row)?,
                    position_side,
                    quantity,
                    state: decode_command_state(
                        row.try_get("command_state").map_err(corrupt_row)?,
                    )?,
                    updated_ms: unsigned(row.try_get("updated_ms").map_err(corrupt_row)?)?,
                })
            })
            .collect()
    }

    pub async fn load_unallocated_fills(
        &self,
        instance_id: &str,
        after_observed_ms: u64,
        limit: u16,
    ) -> Result<Vec<GridFillAllocation>, GridStoreError> {
        validate_ids(&[instance_id])?;
        if limit == 0 || limit > 1_000 {
            return Err(GridStoreError::Invalid);
        }
        ensure_resolvable_owners(self, instance_id).await?;
        let rows = sqlx::query(
            "SELECT f.fill_json,f.observed_ms,o.trading_account_id,o.client_order_id,\
             o.config_revision,o.position_side,o.order_role \
             FROM venue_binance_grid_order_owners o \
             JOIN venue_binance_commands c ON c.command_id=o.place_command_id \
              AND c.command_origin='grid' AND c.grid_instance_id=o.instance_id \
              AND c.trading_account_id=o.trading_account_id \
              AND c.client_order_id=o.client_order_id AND c.symbol=o.symbol \
             JOIN venue_binance_account_fills f ON f.trading_account_id=o.trading_account_id \
              AND f.symbol=o.symbol AND f.fill_json->>'native_order_id'=COALESCE(\
                  o.native_order_id,c.native_order_id,c.selected_native_order_id) \
             LEFT JOIN venue_binance_grid_fill_allocations a \
              ON a.trading_account_id=f.trading_account_id AND a.symbol=f.symbol \
              AND a.native_trade_id=f.native_trade_id \
             WHERE o.instance_id=$1 AND f.observed_ms>$2 AND a.native_trade_id IS NULL \
             ORDER BY f.observed_ms,f.native_trade_id LIMIT $3",
        )
        .bind(instance_id)
        .bind(ms(after_observed_ms)?)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.iter()
            .map(|row| {
                let fill: TerminalFill =
                    serde_json::from_value(row.try_get("fill_json").map_err(corrupt_row)?)
                        .map_err(|_| GridStoreError::Corrupt)?;
                let position_side =
                    decode_position_side(row.try_get("position_side").map_err(corrupt_row)?)?;
                if fill.position_side != position_side {
                    return Err(GridStoreError::Corrupt);
                }
                Ok(GridFillAllocation {
                    instance_id: instance_id.to_owned(),
                    trading_account_id: row.try_get("trading_account_id").map_err(corrupt_row)?,
                    config_revision: unsigned(
                        row.try_get("config_revision").map_err(corrupt_row)?,
                    )?,
                    client_order_id: row.try_get("client_order_id").map_err(corrupt_row)?,
                    native_trade_id: fill.native_trade_id,
                    symbol: fill.symbol,
                    position_side,
                    role: decode_role(row.try_get("order_role").map_err(corrupt_row)?)?,
                    quantity: fill.quantity,
                    price: fill.price,
                    maker: fill.maker,
                    occurred_ms: fill.occurred_ms,
                    observed_ms: unsigned(row.try_get("observed_ms").map_err(corrupt_row)?)?,
                })
            })
            .collect()
    }

    /// Signed cumulative fill quantity by owned client order. Unlike allocation reads, this
    /// includes every persisted fill so crash recovery never double-counts a partial batch.
    pub async fn load_grid_fill_totals(
        &self,
        instance_id: &str,
    ) -> Result<BTreeMap<String, Decimal>, GridStoreError> {
        validate_ids(&[instance_id])?;
        ensure_resolvable_owners(self, instance_id).await?;
        let rows = sqlx::query(
            "SELECT o.client_order_id,o.position_side,f.fill_json \
             FROM venue_binance_grid_order_owners o \
             JOIN venue_binance_commands c ON c.command_id=o.place_command_id \
              AND c.command_origin='grid' AND c.grid_instance_id=o.instance_id \
              AND c.trading_account_id=o.trading_account_id \
              AND c.client_order_id=o.client_order_id AND c.symbol=o.symbol \
             JOIN venue_binance_account_fills f ON f.trading_account_id=o.trading_account_id \
              AND f.symbol=o.symbol AND f.fill_json->>'native_order_id'=COALESCE(\
                  o.native_order_id,c.native_order_id,c.selected_native_order_id) \
             WHERE o.instance_id=$1 ORDER BY o.client_order_id,f.native_trade_id",
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let mut totals = BTreeMap::new();
        for row in rows {
            let fill: TerminalFill =
                serde_json::from_value(row.try_get("fill_json").map_err(corrupt_row)?)
                    .map_err(|_| GridStoreError::Corrupt)?;
            let position_side =
                decode_position_side(row.try_get("position_side").map_err(corrupt_row)?)?;
            if fill.position_side != position_side || !positive(fill.quantity) {
                return Err(GridStoreError::Corrupt);
            }
            let client_order_id: String = row.try_get("client_order_id").map_err(corrupt_row)?;
            let total = totals.entry(client_order_id).or_insert(Decimal::ZERO);
            *total = total
                .checked_add(fill.quantity)
                .ok_or(GridStoreError::Corrupt)?;
        }
        Ok(totals)
    }
}

fn decode_origin(value: String) -> Result<ExecutorCommandOrigin, GridStoreError> {
    match value.as_str() {
        "copy" => Ok(ExecutorCommandOrigin::Copy),
        "terminal" => Ok(ExecutorCommandOrigin::Terminal),
        "grid" => Ok(ExecutorCommandOrigin::Grid),
        _ => Err(GridStoreError::Corrupt),
    }
}

async fn ensure_resolvable_owners(
    store: &BinanceGridStore,
    instance_id: &str,
) -> Result<(), GridStoreError> {
    let mut tx = store.pool.begin().await.map_err(database_error)?;
    sqlx::query(
        "UPDATE venue_binance_grid_order_owners o SET native_order_id=COALESCE(\
             c.native_order_id,c.selected_native_order_id) FROM venue_binance_commands c \
         WHERE o.instance_id=$1 AND o.native_order_id IS NULL \
          AND c.command_id=o.place_command_id AND c.command_origin='grid' \
          AND c.grid_instance_id=o.instance_id AND c.trading_account_id=o.trading_account_id \
          AND c.client_order_id=o.client_order_id AND c.symbol=o.symbol \
          AND c.command_state='reconciled' \
          AND COALESCE(c.native_order_id,c.selected_native_order_id) IS NOT NULL",
    )
    .bind(instance_id)
    .execute(&mut *tx)
    .await
    .map_err(database_error)?;
    let unresolved: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM venue_binance_grid_order_owners o \
         JOIN venue_binance_commands c ON c.command_id=o.place_command_id \
          AND c.command_origin='grid' AND c.grid_instance_id=o.instance_id \
          AND c.trading_account_id=o.trading_account_id \
          AND c.client_order_id=o.client_order_id AND c.symbol=o.symbol \
         WHERE o.instance_id=$1 AND c.command_state='reconciled' \
          AND COALESCE(o.native_order_id,c.native_order_id,c.selected_native_order_id) IS NULL)",
    )
    .bind(instance_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(database_error)?;
    if unresolved {
        return Err(GridStoreError::Corrupt);
    }
    tx.commit().await.map_err(database_error)
}
