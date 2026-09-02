//! Secret-free Binance private-account projection shared by the singleton Executor and Control.

use std::collections::{BTreeMap, BTreeSet};

use sqlx::{PgPool, Row};
use venue_control_protocol::kol::{
    TERMINAL_PROJECTION_SCHEMA_VERSION, TerminalAccountProjection, TerminalAsset, TerminalFill,
    TerminalOpenOrder, TerminalOrderState, TerminalPosition, TerminalPositionHistoryEntry,
    TerminalPositionMode,
};
use venue_domain::domain::{FieldState, LimitTimeInForce, OrderState, PositionSide, Symbol};
use venue_execution::SignedAccountSnapshot;

const PROJECTION_SUBSCRIPTION_MS: u64 = 45_000;
const HISTORY_LIMIT: i64 = 500;
const MAX_ACTIVE_PROJECTION_WORKERS: i64 = 32;
pub const MIGRATION_0019: &str = include_str!("../migrations/0019_binance_account_projection.sql");
pub const MIGRATION_0020: &str = include_str!("../migrations/0020_binance_post_only_terminal.sql");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveProjectionSource {
    pub owner_user_id: String,
    pub credential_id: String,
    pub trading_account_id: String,
    pub symbols: BTreeSet<Symbol>,
    pub previous_fills_cursor: Option<String>,
}

#[derive(Clone)]
pub struct BinancePrivateProjectionStore {
    pool: PgPool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PrivateProjectionError {
    #[error("private projection request is invalid")]
    Invalid,
    #[error("private projection owner was rejected")]
    Forbidden,
    #[error("private projection is unavailable")]
    Unavailable,
}

impl BinancePrivateProjectionStore {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn subscribe(
        &self,
        owner_user_id: &str,
        credential_id: &str,
        symbols: &[Symbol],
        now_ms: u64,
    ) -> Result<(), PrivateProjectionError> {
        if owner_user_id.is_empty() || symbols.is_empty() || now_ms == 0 {
            return Err(PrivateProjectionError::Invalid);
        }
        let row = sqlx::query("SELECT trading_account_id,verification_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL")
            .bind(credential_id)
            .bind(owner_user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?
            .ok_or(PrivateProjectionError::Forbidden)?;
        let trading_account_id: Option<String> = row
            .try_get("trading_account_id")
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let verification: serde_json::Value = row
            .try_get("verification_json")
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let trading_account_id = trading_account_id.ok_or(PrivateProjectionError::Forbidden)?;
        if verification
            .get("verification")
            .and_then(serde_json::Value::as_str)
            != Some("verified")
        {
            return Err(PrivateProjectionError::Forbidden);
        }
        let symbols = serde_json::to_value(symbols).map_err(|_| PrivateProjectionError::Invalid)?;
        let expires_ms = now_ms
            .checked_add(PROJECTION_SUBSCRIPTION_MS)
            .ok_or(PrivateProjectionError::Invalid)?;
        sqlx::query("INSERT INTO venue_binance_projection_subscriptions (credential_id,owner_user_id,trading_account_id,symbols,requested_ms,expires_ms) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (credential_id) DO UPDATE SET owner_user_id=EXCLUDED.owner_user_id,trading_account_id=EXCLUDED.trading_account_id,symbols=EXCLUDED.symbols,requested_ms=EXCLUDED.requested_ms,expires_ms=EXCLUDED.expires_ms")
            .bind(credential_id)
            .bind(owner_user_id)
            .bind(trading_account_id)
            .bind(symbols)
            .bind(ms(now_ms)?)
            .bind(ms(expires_ms)?)
            .execute(&self.pool)
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        Ok(())
    }

    pub async fn active_sources(
        &self,
        now_ms: u64,
    ) -> Result<Vec<ActiveProjectionSource>, PrivateProjectionError> {
        let rows = sqlx::query("SELECT s.owner_user_id,s.credential_id,s.trading_account_id,s.symbols,p.projection_json FROM venue_binance_projection_subscriptions s JOIN venue_api_credentials c ON c.credential_id=s.credential_id AND c.user_id=s.owner_user_id AND c.trading_account_id=s.trading_account_id LEFT JOIN venue_binance_account_projections p ON p.credential_id=s.credential_id WHERE s.expires_ms>$1 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified' ORDER BY s.requested_ms DESC,s.credential_id LIMIT $2")
            .bind(ms(now_ms)?)
            .bind(MAX_ACTIVE_PROJECTION_WORKERS)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        rows.into_iter()
            .map(|row| {
                let symbols: serde_json::Value = row
                    .try_get("symbols")
                    .map_err(|_| PrivateProjectionError::Unavailable)?;
                let symbols = serde_json::from_value::<Vec<Symbol>>(symbols)
                    .map_err(|_| PrivateProjectionError::Unavailable)?
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                if symbols.is_empty() {
                    return Err(PrivateProjectionError::Unavailable);
                }
                let projection: Option<serde_json::Value> = row
                    .try_get("projection_json")
                    .map_err(|_| PrivateProjectionError::Unavailable)?;
                let previous_fills_cursor = projection
                    .and_then(|value| value.get("fills_cursor").cloned())
                    .and_then(|value| value.as_str().map(ToOwned::to_owned));
                Ok(ActiveProjectionSource {
                    owner_user_id: row
                        .try_get("owner_user_id")
                        .map_err(|_| PrivateProjectionError::Unavailable)?,
                    credential_id: row
                        .try_get("credential_id")
                        .map_err(|_| PrivateProjectionError::Unavailable)?,
                    trading_account_id: row
                        .try_get("trading_account_id")
                        .map_err(|_| PrivateProjectionError::Unavailable)?,
                    symbols,
                    previous_fills_cursor,
                })
            })
            .collect()
    }

    pub async fn persist(
        &self,
        source: &ActiveProjectionSource,
        snapshot: &SignedAccountSnapshot,
        now_ms: u64,
    ) -> Result<TerminalAccountProjection, PrivateProjectionError> {
        if snapshot.binding().trading_account_id != source.trading_account_id {
            return Err(PrivateProjectionError::Invalid);
        }
        let persisted_ms = now_ms.max(snapshot.observed_at_ms());
        let mut projection = project(source, snapshot, persisted_ms)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let previous: Option<serde_json::Value> = sqlx::query_scalar("SELECT projection_json FROM venue_binance_account_projections WHERE credential_id=$1 FOR UPDATE")
            .bind(&source.credential_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let previous = previous
            .map(serde_json::from_value::<StoredProjection>)
            .transpose()
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        if previous
            .as_ref()
            .is_some_and(|stored| stored.projection.observed_ms > projection.observed_ms)
        {
            return Err(PrivateProjectionError::Invalid);
        }
        persist_position_changes(&mut tx, source, previous.as_ref(), &projection).await?;
        for fill in &projection.fills {
            let payload =
                serde_json::to_value(fill).map_err(|_| PrivateProjectionError::Invalid)?;
            sqlx::query("INSERT INTO venue_binance_account_fills (trading_account_id,owner_user_id,native_trade_id,symbol,occurred_ms,observed_ms,fill_json) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (trading_account_id,symbol,native_trade_id) DO NOTHING")
                .bind(&source.trading_account_id).bind(&source.owner_user_id).bind(&fill.native_trade_id)
                .bind(fill.symbol.to_string()).bind(fill.occurred_ms.map(ms).transpose()?).bind(ms(projection.observed_ms)?).bind(payload)
                .execute(&mut *tx).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        }
        for order in &projection.open_orders {
            let payload =
                serde_json::to_value(order).map_err(|_| PrivateProjectionError::Invalid)?;
            sqlx::query("INSERT INTO venue_binance_order_observations (trading_account_id,owner_user_id,client_order_id,observed_ms,order_json) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
                .bind(&source.trading_account_id).bind(&source.owner_user_id).bind(&order.client_order_id).bind(ms(projection.observed_ms)?).bind(payload)
                .execute(&mut *tx).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        }
        let stored = StoredProjection {
            fills_cursor: snapshot.fills_cursor().to_owned(),
            projection: projection.clone(),
        };
        let payload = serde_json::to_value(stored).map_err(|_| PrivateProjectionError::Invalid)?;
        sqlx::query("INSERT INTO venue_binance_account_projections (credential_id,owner_user_id,trading_account_id,observed_ms,persisted_ms,private_generation,projection_json) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (credential_id) DO UPDATE SET observed_ms=EXCLUDED.observed_ms,persisted_ms=EXCLUDED.persisted_ms,private_generation=EXCLUDED.private_generation,projection_json=EXCLUDED.projection_json WHERE venue_binance_account_projections.observed_ms<=EXCLUDED.observed_ms")
            .bind(&source.credential_id).bind(&source.owner_user_id).bind(&source.trading_account_id)
            .bind(ms(projection.observed_ms)?).bind(ms(projection.persisted_ms)?).bind(i64::try_from(projection.private_generation).map_err(|_| PrivateProjectionError::Invalid)?).bind(payload)
            .execute(&mut *tx).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        trim_history(&mut tx, source).await?;
        tx.commit()
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        projection.fills = self.load_fills(source).await?;
        projection.position_history = self.load_position_history(source).await?;
        Ok(projection)
    }

    pub async fn load_owned(
        &self,
        owner_user_id: &str,
        credential_id: &str,
    ) -> Result<Option<TerminalAccountProjection>, PrivateProjectionError> {
        let row: Option<(String, serde_json::Value)> = sqlx::query_as("SELECT p.trading_account_id,p.projection_json FROM venue_binance_account_projections p JOIN venue_api_credentials c ON c.credential_id=p.credential_id AND c.user_id=p.owner_user_id AND c.trading_account_id=p.trading_account_id WHERE p.credential_id=$1 AND p.owner_user_id=$2 AND c.deleted_ms IS NULL")
            .bind(credential_id).bind(owner_user_id).fetch_optional(&self.pool).await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let Some((trading_account_id, payload)) = row else {
            return Ok(None);
        };
        let mut stored: StoredProjection =
            serde_json::from_value(payload).map_err(|_| PrivateProjectionError::Unavailable)?;
        let source = ActiveProjectionSource {
            owner_user_id: owner_user_id.to_owned(),
            credential_id: credential_id.to_owned(),
            trading_account_id,
            symbols: BTreeSet::new(),
            previous_fills_cursor: None,
        };
        stored.projection.fills = self.load_fills(&source).await?;
        stored.projection.position_history = self.load_position_history(&source).await?;
        stored
            .projection
            .validate()
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        Ok(Some(stored.projection))
    }

    async fn load_fills(
        &self,
        source: &ActiveProjectionSource,
    ) -> Result<Vec<TerminalFill>, PrivateProjectionError> {
        let values: Vec<serde_json::Value> = sqlx::query_scalar("SELECT fill_json FROM venue_binance_account_fills WHERE trading_account_id=$1 AND owner_user_id=$2 ORDER BY observed_ms DESC,native_trade_id DESC LIMIT 500")
            .bind(&source.trading_account_id).bind(&source.owner_user_id).fetch_all(&self.pool).await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        values
            .into_iter()
            .map(|value| {
                serde_json::from_value(value).map_err(|_| PrivateProjectionError::Unavailable)
            })
            .collect()
    }

    async fn load_position_history(
        &self,
        source: &ActiveProjectionSource,
    ) -> Result<Vec<TerminalPositionHistoryEntry>, PrivateProjectionError> {
        let rows = sqlx::query("SELECT observed_ms,position_json FROM venue_binance_position_history WHERE trading_account_id=$1 AND owner_user_id=$2 ORDER BY observed_ms DESC,symbol,position_side LIMIT 500")
            .bind(&source.trading_account_id).bind(&source.owner_user_id).fetch_all(&self.pool).await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        rows.into_iter()
            .map(|row| {
                Ok(TerminalPositionHistoryEntry {
                    observed_ms: unsigned(
                        row.try_get("observed_ms")
                            .map_err(|_| PrivateProjectionError::Unavailable)?,
                    )?,
                    position: serde_json::from_value(
                        row.try_get("position_json")
                            .map_err(|_| PrivateProjectionError::Unavailable)?,
                    )
                    .map_err(|_| PrivateProjectionError::Unavailable)?,
                })
            })
            .collect()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct StoredProjection {
    fills_cursor: String,
    projection: TerminalAccountProjection,
}

fn project(
    source: &ActiveProjectionSource,
    snapshot: &SignedAccountSnapshot,
    persisted_ms: u64,
) -> Result<TerminalAccountProjection, PrivateProjectionError> {
    let positions = snapshot
        .positions()
        .iter()
        .map(|position| TerminalPosition {
            symbol: position.symbol.clone(),
            position_side: position.position_side,
            quantity: position.quantity,
            entry_price: position.entry_price,
            mark_price: position.mark_price,
        })
        .collect();
    let open_orders = snapshot
        .open_orders()
        .iter()
        .map(|order| {
            let state = match order.state.unwrap_or(OrderState::New) {
                OrderState::New => TerminalOrderState::New,
                OrderState::PartiallyFilled => TerminalOrderState::PartiallyFilled,
                _ => return Err(PrivateProjectionError::Invalid),
            };
            Ok(TerminalOpenOrder {
                client_order_id: order.client_order_id.clone(),
                native_order_id: order.venue_order_id.clone(),
                symbol: order.symbol.clone(),
                order_side: order.side,
                position_side: order.position_side,
                quantity: order.quantity,
                filled_quantity: order.filled_quantity,
                limit_price: order.limit_price,
                post_only: order.time_in_force == Some(LimitTimeInForce::PostOnly),
                reduce_only: order.reduce_only,
                state,
                created_ms: order.created_at_ms,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let fills = snapshot
        .fills()
        .iter()
        .map(|fill| {
            let position_side = match fill.position_side {
                FieldState::Known(side) if side != PositionSide::Net => side,
                _ => return Err(PrivateProjectionError::Invalid),
            };
            let maker = match fill.maker {
                FieldState::Known(value) => Some(value),
                _ => None,
            };
            Ok(TerminalFill {
                native_trade_id: fill.fill_id.clone(),
                native_order_id: fill.order_id.clone(),
                symbol: fill.symbol.clone(),
                order_side: fill.side,
                position_side,
                quantity: fill.quantity,
                price: fill.price.value(),
                maker,
                occurred_ms: fill.exchange_time_ms,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let assets = snapshot
        .balances()
        .iter()
        .map(|balance| TerminalAsset {
            asset: balance.asset.as_str().to_owned(),
            equity: balance.equity,
            available_margin: balance.available_margin,
        })
        .collect();
    let projection = TerminalAccountProjection {
        schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
        credential_id: source.credential_id.clone(),
        trading_account_id: source.trading_account_id.clone(),
        observed_ms: snapshot.observed_at_ms(),
        persisted_ms,
        private_generation: snapshot.private_generation(),
        position_mode: TerminalPositionMode::Hedge,
        positions,
        position_history: Vec::new(),
        open_orders,
        fills,
        assets,
    };
    projection
        .validate()
        .map_err(|_| PrivateProjectionError::Invalid)?;
    Ok(projection)
}

async fn persist_position_changes(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &ActiveProjectionSource,
    previous: Option<&StoredProjection>,
    current: &TerminalAccountProjection,
) -> Result<(), PrivateProjectionError> {
    let old = previous
        .map(|stored| &stored.projection.positions)
        .into_iter()
        .flatten()
        .map(|position| {
            (
                (position.symbol.to_string(), position.position_side),
                position.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let new = current
        .positions
        .iter()
        .map(|position| {
            (
                (position.symbol.to_string(), position.position_side),
                position.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (key, prior) in &old {
        if !new.contains_key(key) {
            let mut closed = prior.clone();
            closed.quantity = rust_decimal::Decimal::ZERO;
            insert_position_history(tx, source, current.observed_ms, &closed).await?;
        }
    }
    for (key, position) in &new {
        if old.get(key) != Some(position) {
            insert_position_history(tx, source, current.observed_ms, position).await?;
        }
    }
    Ok(())
}

async fn insert_position_history(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &ActiveProjectionSource,
    observed_ms: u64,
    position: &TerminalPosition,
) -> Result<(), PrivateProjectionError> {
    let side = match position.position_side {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
        PositionSide::Net => return Err(PrivateProjectionError::Invalid),
    };
    let payload = serde_json::to_value(position).map_err(|_| PrivateProjectionError::Invalid)?;
    sqlx::query("INSERT INTO venue_binance_position_history (trading_account_id,owner_user_id,symbol,position_side,observed_ms,position_json) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING")
        .bind(&source.trading_account_id).bind(&source.owner_user_id).bind(position.symbol.to_string()).bind(side).bind(ms(observed_ms)?).bind(payload)
        .execute(&mut **tx).await.map_err(|_| PrivateProjectionError::Unavailable)?;
    Ok(())
}

async fn trim_history(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &ActiveProjectionSource,
) -> Result<(), PrivateProjectionError> {
    for statement in [
        "DELETE FROM venue_binance_account_fills WHERE trading_account_id=$1 AND owner_user_id=$2 AND (trading_account_id,symbol,native_trade_id) IN (SELECT trading_account_id,symbol,native_trade_id FROM venue_binance_account_fills WHERE trading_account_id=$1 AND owner_user_id=$2 ORDER BY observed_ms DESC OFFSET $3)",
        "DELETE FROM venue_binance_position_history WHERE trading_account_id=$1 AND owner_user_id=$2 AND (trading_account_id,symbol,position_side,observed_ms) IN (SELECT trading_account_id,symbol,position_side,observed_ms FROM venue_binance_position_history WHERE trading_account_id=$1 AND owner_user_id=$2 ORDER BY observed_ms DESC OFFSET $3)",
        "DELETE FROM venue_binance_order_observations WHERE trading_account_id=$1 AND owner_user_id=$2 AND (trading_account_id,client_order_id,observed_ms) IN (SELECT trading_account_id,client_order_id,observed_ms FROM venue_binance_order_observations WHERE trading_account_id=$1 AND owner_user_id=$2 ORDER BY observed_ms DESC OFFSET $3)",
    ] {
        sqlx::query(statement)
            .bind(&source.trading_account_id)
            .bind(&source.owner_user_id)
            .bind(HISTORY_LIMIT)
            .execute(&mut **tx)
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
    }
    Ok(())
}

fn ms(value: u64) -> Result<i64, PrivateProjectionError> {
    i64::try_from(value).map_err(|_| PrivateProjectionError::Invalid)
}

fn unsigned(value: i64) -> Result<u64, PrivateProjectionError> {
    u64::try_from(value).map_err(|_| PrivateProjectionError::Unavailable)
}
