use serde::Deserialize;
use sqlx::Row;

use super::{StrategyGridConfig, StrategyGridStore, store::lock_account};
use crate::multi_venue_store::MultiVenueStoreError as Error;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridDepthUpdate {
    pub expected_revision: u64,
    pub grid_count: u8,
}

impl StrategyGridStore {
    pub async fn update_depth(
        &self,
        owner: &str,
        id: &str,
        request: GridDepthUpdate,
        now: u64,
    ) -> Result<u64, Error> {
        if request.expected_revision == 0 || now == 0 {
            return Err(Error::Invalid);
        }
        let record = self.get(owner, id).await?;
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        lock_account(&mut tx, &record.trading_account_id).await?;
        let row = sqlx::query("SELECT lifecycle,revision,config FROM venue_strategy_grids WHERE instance_id=$1 AND owner_user_id=$2 FOR UPDATE")
            .bind(id).bind(owner).fetch_one(&mut *tx).await.map_err(|_| Error::Conflict)?;
        let revision: i64 = row.try_get("revision").map_err(|_| Error::Conflict)?;
        let revision = u64::try_from(revision).map_err(|_| Error::Conflict)?;
        let mut config: StrategyGridConfig =
            serde_json::from_value(row.try_get("config").map_err(|_| Error::Conflict)?)
                .map_err(|_| Error::Conflict)?;
        let next = request
            .expected_revision
            .checked_add(1)
            .ok_or(Error::Invalid)?;
        if revision == next && config.planner.grid_count == request.grid_count {
            return Ok(revision);
        }
        if revision != request.expected_revision
            || row
                .try_get::<String, _>("lifecycle")
                .map_err(|_| Error::Conflict)?
                != "stopped"
        {
            return Err(Error::Conflict);
        }
        let settled: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM venue_strategy_grid_orders WHERE instance_id=$1 AND NOT terminal) AND NOT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$2 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(id).bind(&record.trading_account_id).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if !settled {
            return Err(Error::Conflict);
        }
        apply_depth(&mut config, request.grid_count, next)?;
        let now = i64::try_from(now).map_err(|_| Error::Invalid)?;
        sqlx::query("UPDATE venue_strategy_grids SET config=$1,revision=$2,rolling_anchor=NULL,desired_orders='[]'::jsonb,updated_ms=$3 WHERE instance_id=$4")
            .bind(serde_json::to_value(&config).map_err(|_| Error::Invalid)?)
            .bind(i64::try_from(next).map_err(|_| Error::Invalid)?).bind(now).bind(id)
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(next)
    }
}

fn apply_depth(config: &mut StrategyGridConfig, count: u8, revision: u64) -> Result<(), Error> {
    let max = if config.net_direction.is_some() { 8 } else { 4 };
    let legs = if config.net_direction.is_some() { 1 } else { 2 };
    if count == 0
        || count > max
        || config
            .planner
            .order_notional
            .value
            .checked_mul(rust_decimal::Decimal::from(count * legs))
            .is_none_or(|value| value > config.planner.maximum_grid_notional.value)
    {
        return Err(Error::Invalid);
    }
    config.planner.grid_count = count;
    config.planner.revision = revision;
    config.planner.validate().map_err(|_| Error::Invalid)
}

#[test]
fn depth_change_keeps_amount_limits_and_rejects_excess_depth()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config: StrategyGridConfig =
        serde_json::from_str(include_str!("../../tests/fixtures/multi_venue_grid.json"))?;
    let amount = config.planner.order_notional.clone();
    let limit = config.planner.maximum_grid_notional.clone();
    apply_depth(&mut config, 3, 2)?;
    assert_eq!(config.planner.order_notional, amount);
    assert_eq!(config.planner.maximum_grid_notional, limit);
    assert!(apply_depth(&mut config, 5, 3).is_err());
    config.planner.maximum_grid_notional.value = amount.value;
    assert!(apply_depth(&mut config, 3, 3).is_err());
    Ok(())
}
