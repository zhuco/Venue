//! Secret-free Binance private-account projection shared by the singleton Executor and Control.

mod terminal_positions;

use std::collections::{BTreeMap, BTreeSet};

use sqlx::{PgPool, Row};
use venue_control_protocol::kol::{
    TERMINAL_PROJECTION_SCHEMA_VERSION, TerminalAccountProjection, TerminalAsset, TerminalFill,
    TerminalOpenOrder, TerminalOrderState, TerminalPosition, TerminalPositionHistoryEntry,
    TerminalPositionMode,
};
use venue_domain::domain::{FieldState, Fill, LimitTimeInForce, OrderState, PositionSide, Symbol};
use venue_execution::SignedAccountSnapshot;
use venue_gateway_binance::BinancePrivateFillEvent;

const PROJECTION_SUBSCRIPTION_MS: u64 = 45_000;
const HISTORY_LIMIT: i64 = 500;
pub const PRIVATE_STREAM_FILL_BATCH_LIMIT: usize = 5;
pub const MIGRATION_0019: &str = include_str!("../migrations/0019_binance_account_projection.sql");
pub const MIGRATION_0020: &str = include_str!("../migrations/0020_binance_post_only_terminal.sql");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveProjectionSource {
    /// Set only for an enabled KOL leader. Its signed fill suffix must be committed to the copy
    /// ledger before this source's durable account-projection cursor may advance.
    pub kol_user_id: Option<String>,
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
    pub async fn invalidate_stream(
        &self,
        credential_id: &str,
    ) -> Result<(), PrivateProjectionError> {
        sqlx::query("UPDATE venue_binance_account_projections SET projection_json=jsonb_set(projection_json,'{stream_healthy}','false'::jsonb) WHERE credential_id=$1")
            .bind(credential_id).execute(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        Ok(())
    }

    pub async fn load_healthy_owned(
        &self,
        owner: &str,
        credential: &str,
    ) -> Result<Option<TerminalAccountProjection>, PrivateProjectionError> {
        let healthy: Option<bool> = sqlx::query_scalar("SELECT COALESCE((projection_json->>'stream_healthy')::boolean,false) FROM venue_binance_account_projections WHERE credential_id=$1 AND owner_user_id=$2")
            .bind(credential).bind(owner).fetch_optional(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        if healthy != Some(true) {
            return Ok(None);
        }
        self.load_owned(owner, credential).await
    }
    /// A live projection must not replace the continuation cache while REST RESULT and user
    /// stream order acknowledgements are still crossing. This is local ledger validation only.
    /// None means commands are in flight, not evidence of a broken user stream.
    pub async fn stream_surface_settled(
        &self,
        source: &ActiveProjectionSource,
        snapshot: &SignedAccountSnapshot,
    ) -> Result<Option<bool>, PrivateProjectionError> {
        let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(&source.trading_account_id).fetch_one(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        if pending {
            return Ok(None);
        }
        let rows = sqlx::query("SELECT d.client_order_id,d.quantity,d.limit_price FROM venue_binance_grid_desired_orders d JOIN venue_binance_grid_instances i ON i.instance_id=d.instance_id WHERE i.trading_account_id=$1 AND i.owner_user_id=$2 AND i.instance_state='running'")
            .bind(&source.trading_account_id).bind(&source.owner_user_id).fetch_all(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        for row in rows {
            let client: String = row
                .try_get("client_order_id")
                .map_err(|_| PrivateProjectionError::Unavailable)?;
            let quantity: String = row
                .try_get("quantity")
                .map_err(|_| PrivateProjectionError::Unavailable)?;
            let price: String = row
                .try_get("limit_price")
                .map_err(|_| PrivateProjectionError::Unavailable)?;
            let quantity: rust_decimal::Decimal = quantity
                .parse()
                .map_err(|_| PrivateProjectionError::Invalid)?;
            let price: rust_decimal::Decimal =
                price.parse().map_err(|_| PrivateProjectionError::Invalid)?;
            if !snapshot.open_orders().iter().any(|order| {
                order.client_order_id == client
                    && order
                        .filled_quantity
                        .and_then(|filled| order.quantity.checked_sub(filled))
                        .is_some_and(|remaining| {
                            remaining > rust_decimal::Decimal::ZERO && remaining <= quantity
                        })
                    && order.limit_price == Some(price)
            }) {
                return Ok(Some(false));
            }
        }
        let retired: Vec<String> = sqlx::query_scalar("SELECT o.client_order_id FROM venue_binance_grid_order_owners o JOIN venue_binance_grid_instances i ON i.instance_id=o.instance_id LEFT JOIN venue_binance_grid_desired_orders d ON d.client_order_id=o.client_order_id WHERE i.trading_account_id=$1 AND i.owner_user_id=$2 AND i.instance_state='running' AND d.client_order_id IS NULL")
            .bind(&source.trading_account_id).bind(&source.owner_user_id).fetch_all(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        if snapshot
            .open_orders()
            .iter()
            .any(|order| retired.contains(&order.client_order_id))
        {
            return Ok(Some(false));
        }
        Ok(Some(true))
    }
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
        let worker_limit = i64::try_from(crate::kol_executor::MAX_ACTIVE_EXECUTOR_ACCOUNTS)
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let kol_rows = sqlx::query(
            "WITH active_kol AS (\
               SELECT p.kol_user_id,p.leader_trading_account_id,\
                 jsonb_agg(DISTINCT symbols.value ORDER BY symbols.value) AS symbols \
               FROM venue_kol_profiles p \
               JOIN venue_kol_follow_relations r ON r.kol_user_id=p.kol_user_id \
                 AND r.leader_trading_account_id=p.leader_trading_account_id \
                 AND r.relation_state='active' \
               CROSS JOIN LATERAL jsonb_array_elements_text(r.allowed_symbols) AS symbols(value) \
               WHERE p.profile_state='enabled' \
               GROUP BY p.kol_user_id,p.leader_trading_account_id\
             ) \
             SELECT k.kol_user_id,k.kol_user_id AS owner_user_id,\
               credentials.credential_id,k.leader_trading_account_id AS trading_account_id,\
               k.symbols,p.projection_json,credentials.credential_count \
             FROM active_kol k \
             CROSS JOIN LATERAL (\
               SELECT min(c.credential_id) AS credential_id,count(*) AS credential_count \
               FROM venue_api_credentials c \
               WHERE c.user_id=k.kol_user_id \
                 AND c.trading_account_id=k.leader_trading_account_id AND c.credential_id=COALESCE((SELECT b.credential_id FROM venue_leader_bots b WHERE b.owner_user_id=k.kol_user_id),c.credential_id) \
                 AND c.deleted_ms IS NULL \
                 AND c.verification_json->>'verification'='verified'\
             ) credentials \
             LEFT JOIN venue_binance_account_projections p \
               ON p.credential_id=credentials.credential_id \
             ORDER BY k.kol_user_id LIMIT $1",
        )
        .bind(crate::kol_executor::MAX_ENABLED_KOLS as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| PrivateProjectionError::Unavailable)?;
        let grid_rows = sqlx::query(
            "SELECT i.owner_user_id,i.credential_id,i.trading_account_id,\
             jsonb_agg(DISTINCT i.symbol ORDER BY i.symbol) AS symbols,p.projection_json \
             FROM venue_binance_grid_instances i \
             JOIN venue_api_credentials c ON c.credential_id=i.credential_id \
               AND c.user_id=i.owner_user_id AND c.trading_account_id=i.trading_account_id \
             LEFT JOIN venue_binance_account_projections p ON p.credential_id=i.credential_id \
             WHERE i.instance_state IN ('start_pending','running','paused','stop_pending',\
               'blocked','reset_required','needs_attention') \
               AND c.deleted_ms IS NULL \
               AND c.verification_json->>'verification'='verified' \
             GROUP BY i.owner_user_id,i.credential_id,i.trading_account_id,p.projection_json \
             ORDER BY i.credential_id LIMIT $1",
        )
        .bind(worker_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| PrivateProjectionError::Unavailable)?;
        let ui_rows = sqlx::query("SELECT s.owner_user_id,s.credential_id,s.trading_account_id,s.symbols,p.projection_json FROM venue_binance_projection_subscriptions s JOIN venue_api_credentials c ON c.credential_id=s.credential_id AND c.user_id=s.owner_user_id AND c.trading_account_id=s.trading_account_id LEFT JOIN venue_binance_account_projections p ON p.credential_id=s.credential_id WHERE s.expires_ms>$1 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified' ORDER BY s.requested_ms DESC,s.credential_id LIMIT $2")
            .bind(ms(now_ms)?)
            .bind(worker_limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let follower_rows=sqlx::query("SELECT r.follower_user_id AS owner_user_id,r.credential_id,r.follower_trading_account_id AS trading_account_id,r.allowed_symbols AS symbols,p.projection_json FROM venue_kol_follow_relations r JOIN venue_api_credentials c ON c.credential_id=r.credential_id AND c.user_id=r.follower_user_id AND c.trading_account_id=r.follower_trading_account_id LEFT JOIN venue_binance_account_projections p ON p.credential_id=r.credential_id WHERE c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified' AND ((r.relation_state='active' AND r.baseline_json->>'target_model'='2') OR EXISTS(SELECT 1 FROM venue_order_mirrors m WHERE m.relation_id=r.relation_id AND m.mirror_state NOT IN ('terminal','blocked'))) ORDER BY r.relation_id LIMIT 200")
            .fetch_all(&self.pool).await.map_err(|_|PrivateProjectionError::Unavailable)?;
        let mut by_credential = BTreeMap::<String, ActiveProjectionSource>::new();
        let mut priority = Vec::new();
        for row in kol_rows {
            let credential_count: i64 = row
                .try_get("credential_count")
                .map_err(|_| PrivateProjectionError::Unavailable)?;
            if credential_count != 1 {
                return Err(PrivateProjectionError::Unavailable);
            }
            let kol_user_id: String = row
                .try_get("kol_user_id")
                .map_err(|_| PrivateProjectionError::Unavailable)?;
            let source = projection_source(&row, Some(kol_user_id))?;
            merge_projection_source(&mut by_credential, &mut priority, source)?;
        }
        for row in follower_rows.into_iter().chain(grid_rows).chain(ui_rows) {
            let source = projection_source(&row, None)?;
            merge_projection_source(&mut by_credential, &mut priority, source)?;
        }
        for source in by_credential.values_mut() {
            let rows = sqlx::query("SELECT symbol,MIN(occurred_ms) AS replay_from FROM venue_binance_account_fills WHERE trading_account_id=$1 AND owner_user_id=$2 AND observed_ms<occurred_ms GROUP BY symbol")
                .bind(&source.trading_account_id).bind(&source.owner_user_id)
                .fetch_all(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
            let mut from = BTreeMap::new();
            for row in rows {
                let symbol: String = row
                    .try_get("symbol")
                    .map_err(|_| PrivateProjectionError::Unavailable)?;
                let symbol: Symbol = symbol
                    .parse()
                    .map_err(|_| PrivateProjectionError::Invalid)?;
                let time: i64 = row
                    .try_get("replay_from")
                    .map_err(|_| PrivateProjectionError::Unavailable)?;
                source.symbols.insert(symbol.clone());
                from.insert(
                    symbol,
                    u64::try_from(time).map_err(|_| PrivateProjectionError::Invalid)?,
                );
            }
            source.previous_fills_cursor =
                venue_gateway_binance::BinanceAccountGateway::replay_projection_fills_from(
                    source.previous_fills_cursor.as_deref(),
                    &from,
                )
                .map_err(|_| PrivateProjectionError::Invalid)?;
        }
        let max_workers =
            usize::try_from(worker_limit).map_err(|_| PrivateProjectionError::Unavailable)?;
        priority
            .into_iter()
            .take(max_workers)
            .map(|credential_id| {
                by_credential
                    .remove(&credential_id)
                    .ok_or(PrivateProjectionError::Unavailable)
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
            // Account/order freshness remains the snapshot start; a later REST fill is first
            // observed only after collection completes. Never freshen the entire snapshot.
            if fill.occurred_ms.is_some_and(|time| time > persisted_ms) {
                return Err(PrivateProjectionError::Invalid);
            }
            let payload =
                serde_json::to_value(fill).map_err(|_| PrivateProjectionError::Invalid)?;
            sqlx::query("INSERT INTO venue_binance_account_fills (trading_account_id,owner_user_id,native_trade_id,symbol,occurred_ms,observed_ms,fill_json) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (trading_account_id,symbol,native_trade_id) DO UPDATE SET observed_ms=EXCLUDED.observed_ms WHERE venue_binance_account_fills.observed_ms<venue_binance_account_fills.occurred_ms AND venue_binance_account_fills.owner_user_id=EXCLUDED.owner_user_id AND (venue_binance_account_fills.fill_json - 'price' - 'quantity')=(EXCLUDED.fill_json - 'price' - 'quantity') AND (venue_binance_account_fills.fill_json->>'price')::numeric=(EXCLUDED.fill_json->>'price')::numeric AND (venue_binance_account_fills.fill_json->>'quantity')::numeric=(EXCLUDED.fill_json->>'quantity')::numeric")
                .bind(&source.trading_account_id).bind(&source.owner_user_id).bind(&fill.native_trade_id)
                .bind(fill.symbol.to_string()).bind(fill.occurred_ms.map(ms).transpose()?).bind(ms(persisted_ms)?).bind(payload)
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
            stream_healthy: true,
            balance_observed_ms: Some(snapshot.balance_observed_at_ms()),
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

    /// Persists one authenticated stream fill without advancing the signed projection cursor.
    /// The current signed generation is locked and checked first, so a reconnect or a racing
    /// signed refresh turns this into a harmless fallback instead of applying an event to the
    /// wrong baseline. Restart and genuine stream gaps use a new signed REST bootstrap.
    pub async fn persist_stream_fill(
        &self,
        source: &ActiveProjectionSource,
        event: &BinancePrivateFillEvent,
    ) -> Result<(), PrivateProjectionError> {
        self.persist_stream_fills(source, std::slice::from_ref(event))
            .await
    }

    /// Commits one private-stream micro-burst atomically. The signed projection baseline is
    /// locked once for the whole burst; a mixed generation or conflicting native trade identity
    /// rejects the complete transaction so Grid never observes only a prefix of the burst.
    pub async fn persist_stream_fills(
        &self,
        source: &ActiveProjectionSource,
        events: &[BinancePrivateFillEvent],
    ) -> Result<(), PrivateProjectionError> {
        let prepared = prepare_stream_fill_batch(source, events)?;
        let batch_generation = events
            .first()
            .map(|event| event.private_generation)
            .ok_or(PrivateProjectionError::Invalid)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let baseline = sqlx::query(
            "SELECT owner_user_id,trading_account_id,observed_ms,private_generation \
             FROM venue_binance_account_projections WHERE credential_id=$1 FOR UPDATE",
        )
        .bind(&source.credential_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| PrivateProjectionError::Unavailable)?
        .ok_or(PrivateProjectionError::Unavailable)?;
        let owner_user_id: String = baseline
            .try_get("owner_user_id")
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let trading_account_id: String = baseline
            .try_get("trading_account_id")
            .map_err(|_| PrivateProjectionError::Unavailable)?;
        let observed_ms = unsigned(
            baseline
                .try_get("observed_ms")
                .map_err(|_| PrivateProjectionError::Unavailable)?,
        )?;
        let private_generation = unsigned(
            baseline
                .try_get("private_generation")
                .map_err(|_| PrivateProjectionError::Unavailable)?,
        )?;
        if owner_user_id != source.owner_user_id
            || trading_account_id != source.trading_account_id
            || private_generation != batch_generation
            || events
                .iter()
                .any(|event| observed_ms >= event.received_at_ms)
        {
            tracing::warn!(target: "venue_control::grid_hot_path", generation_matches = private_generation == batch_generation,
                event_after_baseline = events.iter().all(|event| observed_ms < event.received_at_ms),
                "Authenticated fill baseline comparison failed");
            return Err(PrivateProjectionError::Invalid);
        }

        // REST and authenticated streams may spell the same Decimal with different trailing
        // zeros. Compare only numeric fields numerically; identity, side and time remain exact.
        for fill in prepared {
            let changed = sqlx::query(
                "INSERT INTO venue_binance_account_fills \
                 (trading_account_id,owner_user_id,native_trade_id,symbol,occurred_ms,observed_ms,fill_json,\
                  stream_private_generation,baseline_private_generation,original_quantity,\
                  cumulative_filled_quantity,order_state,client_order_id) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
                 ON CONFLICT (trading_account_id,symbol,native_trade_id) DO UPDATE SET \
                  observed_ms=CASE WHEN venue_binance_account_fills.observed_ms<venue_binance_account_fills.occurred_ms THEN EXCLUDED.observed_ms ELSE venue_binance_account_fills.observed_ms END,\
                  stream_private_generation=COALESCE(venue_binance_account_fills.stream_private_generation,EXCLUDED.stream_private_generation),\
                  baseline_private_generation=COALESCE(venue_binance_account_fills.baseline_private_generation,EXCLUDED.baseline_private_generation),\
                  original_quantity=COALESCE(venue_binance_account_fills.original_quantity,EXCLUDED.original_quantity),\
                  cumulative_filled_quantity=COALESCE(venue_binance_account_fills.cumulative_filled_quantity,EXCLUDED.cumulative_filled_quantity),\
                  order_state=COALESCE(venue_binance_account_fills.order_state,EXCLUDED.order_state),\
                  client_order_id=COALESCE(venue_binance_account_fills.client_order_id,EXCLUDED.client_order_id) \
                 WHERE venue_binance_account_fills.owner_user_id=EXCLUDED.owner_user_id \
                   AND (venue_binance_account_fills.fill_json - 'price' - 'quantity')=(EXCLUDED.fill_json - 'price' - 'quantity') \
                   AND (venue_binance_account_fills.fill_json->>'price')::numeric=(EXCLUDED.fill_json->>'price')::numeric \
                   AND (venue_binance_account_fills.fill_json->>'quantity')::numeric=(EXCLUDED.fill_json->>'quantity')::numeric \
                   AND (venue_binance_account_fills.stream_private_generation IS NULL OR (\
                     venue_binance_account_fills.stream_private_generation=EXCLUDED.stream_private_generation \
                     AND venue_binance_account_fills.baseline_private_generation=EXCLUDED.baseline_private_generation \
                     AND venue_binance_account_fills.original_quantity::numeric=EXCLUDED.original_quantity::numeric \
                     AND venue_binance_account_fills.cumulative_filled_quantity::numeric=EXCLUDED.cumulative_filled_quantity::numeric \
                     AND venue_binance_account_fills.order_state=EXCLUDED.order_state \
                     AND venue_binance_account_fills.client_order_id=EXCLUDED.client_order_id))",
            )
            .bind(&source.trading_account_id)
            .bind(&source.owner_user_id)
            .bind(&fill.native_trade_id)
            .bind(&fill.symbol)
            .bind(fill.occurred_ms)
            .bind(fill.observed_ms)
            .bind(&fill.payload)
            .bind(fill.stream_generation)
            .bind(fill.baseline_generation)
            .bind(fill.original)
            .bind(fill.cumulative)
            .bind(fill.state)
            .bind(fill.client)
            .execute(&mut *tx)
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)?;
            if changed.rows_affected() != 1 {
                let prior: Option<serde_json::Value> = sqlx::query_scalar(
                    "SELECT fill_json FROM venue_binance_account_fills WHERE trading_account_id=$1 AND symbol=$2 AND native_trade_id=$3",
                ).bind(&source.trading_account_id).bind(&fill.symbol).bind(&fill.native_trade_id)
                    .fetch_optional(&mut *tx).await.map_err(|_| PrivateProjectionError::Unavailable)?;
                let mismatched_fields: Vec<&str> = [
                    "native_order_id",
                    "order_side",
                    "position_side",
                    "occurred_ms",
                    "maker",
                    "price",
                    "quantity",
                ]
                .into_iter()
                .filter(|key| {
                    prior.as_ref().and_then(|value| value.get(key)) != fill.payload.get(key)
                })
                .collect();
                tracing::warn!(target: "venue_control::grid_hot_path", ?mismatched_fields, "Authenticated fill conflicts with an existing durable fill");
                return Err(PrivateProjectionError::Invalid);
            }
        }
        tx.commit()
            .await
            .map_err(|_| PrivateProjectionError::Unavailable)
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
            kol_user_id: None,
            owner_user_id: owner_user_id.to_owned(),
            credential_id: credential_id.to_owned(),
            trading_account_id,
            symbols: BTreeSet::new(),
            previous_fills_cursor: None,
        };
        stored.projection.fills = self.load_fills(&source).await?;
        stored.projection.position_history = self.load_position_history(&source).await?;
        self.apply_terminal_position_refresh(owner_user_id, &mut stored.projection)
            .await?;
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
        let mut latest_inventory = BTreeMap::new();
        let mut history = Vec::new();
        for row in rows {
            let entry = TerminalPositionHistoryEntry {
                observed_ms: unsigned(
                    row.try_get("observed_ms")
                        .map_err(|_| PrivateProjectionError::Unavailable)?,
                )?,
                position: serde_json::from_value(
                    row.try_get("position_json")
                        .map_err(|_| PrivateProjectionError::Unavailable)?,
                )
                .map_err(|_| PrivateProjectionError::Unavailable)?,
            };
            let key = (
                entry.position.symbol.to_string(),
                entry.position.position_side,
            );
            if latest_inventory
                .get(&key)
                .is_some_and(|position| same_position_inventory(position, &entry.position))
            {
                continue;
            }
            latest_inventory.insert(key, entry.position.clone());
            history.push(entry);
        }
        Ok(history)
    }
}

fn projection_source(
    row: &sqlx::postgres::PgRow,
    kol_user_id: Option<String>,
) -> Result<ActiveProjectionSource, PrivateProjectionError> {
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
        kol_user_id,
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
}

fn merge_projection_source(
    by_credential: &mut BTreeMap<String, ActiveProjectionSource>,
    priority: &mut Vec<String>,
    source: ActiveProjectionSource,
) -> Result<(), PrivateProjectionError> {
    if !priority.iter().any(|value| value == &source.credential_id) {
        priority.push(source.credential_id.clone());
    }
    match by_credential.get_mut(&source.credential_id) {
        Some(current) => {
            if current.owner_user_id != source.owner_user_id
                || current.trading_account_id != source.trading_account_id
                || (current.kol_user_id.is_some()
                    && source.kol_user_id.is_some()
                    && current.kol_user_id != source.kol_user_id)
            {
                return Err(PrivateProjectionError::Unavailable);
            }
            current.symbols.extend(source.symbols);
            if current.kol_user_id.is_none() {
                current.kol_user_id = source.kol_user_id;
            }
            if current.previous_fills_cursor.is_none() {
                current.previous_fills_cursor = source.previous_fills_cursor;
            }
        }
        None => {
            by_credential.insert(source.credential_id.clone(), source);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct StoredProjection {
    #[serde(default)]
    balance_observed_ms: Option<u64>,
    #[serde(default)]
    stream_healthy: bool,
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
                time_in_force: if order.family == venue_domain::NativeOrderFamily::UmOrder {
                    order.time_in_force
                } else {
                    None
                },
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
        .map(terminal_fill)
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

fn terminal_fill(fill: &Fill) -> Result<TerminalFill, PrivateProjectionError> {
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
        quantity: fill.quantity.normalize(),
        price: fill.price.value().normalize(),
        maker,
        occurred_ms: fill.exchange_time_ms,
    })
}

struct PreparedStreamFill {
    native_trade_id: String,
    symbol: String,
    occurred_ms: Option<i64>,
    observed_ms: i64,
    payload: serde_json::Value,
    stream_generation: Option<i64>,
    baseline_generation: Option<i64>,
    original: Option<String>,
    cumulative: Option<String>,
    state: Option<&'static str>,
    client: Option<String>,
}

fn prepare_stream_fill_batch(
    source: &ActiveProjectionSource,
    events: &[BinancePrivateFillEvent],
) -> Result<Vec<PreparedStreamFill>, PrivateProjectionError> {
    if events.is_empty() || events.len() > PRIVATE_STREAM_FILL_BATCH_LIMIT {
        return Err(PrivateProjectionError::Invalid);
    }
    let generation = events
        .first()
        .map(|event| (event.stream_private_generation, event.private_generation))
        .ok_or(PrivateProjectionError::Invalid)?;
    let mut identities = BTreeMap::<(String, String), &BinancePrivateFillEvent>::new();
    let mut prepared = Vec::with_capacity(events.len());
    for event in events {
        if event.stream_private_generation == 0
            || event.private_generation < event.stream_private_generation
            || (event.stream_private_generation, event.private_generation) != generation
            || event.received_at_ms == 0
            || !source.symbols.contains(&event.fill.symbol)
            || event
                .fill
                .exchange_time_ms
                .is_none_or(|occurred| occurred == 0 || event.received_at_ms < occurred)
        {
            return Err(PrivateProjectionError::Invalid);
        }
        let key = (event.fill.symbol.to_string(), event.fill.fill_id.clone());
        if identities
            .insert(key, event)
            .is_some_and(|prior| !same_stream_fill_identity(prior, event))
        {
            return Err(PrivateProjectionError::Invalid);
        }
        let fill = terminal_fill(&event.fill)?;
        let payload = serde_json::to_value(&fill).map_err(|_| PrivateProjectionError::Invalid)?;
        let context = stream_fill_context(event)?;
        let (stream_generation, baseline_generation, original, cumulative, state, client) =
            match context {
                Some((original, cumulative, state, client)) => (
                    Some(
                        i64::try_from(event.stream_private_generation)
                            .map_err(|_| PrivateProjectionError::Invalid)?,
                    ),
                    Some(
                        i64::try_from(event.private_generation)
                            .map_err(|_| PrivateProjectionError::Invalid)?,
                    ),
                    Some(original.to_string()),
                    Some(cumulative.to_string()),
                    Some(state),
                    Some(client),
                ),
                None => (None, None, None, None, None, None),
            };
        prepared.push(PreparedStreamFill {
            native_trade_id: fill.native_trade_id,
            symbol: fill.symbol.to_string(),
            occurred_ms: fill.occurred_ms.map(ms).transpose()?,
            observed_ms: ms(event.received_at_ms)?,
            payload,
            stream_generation,
            baseline_generation,
            original,
            cumulative,
            state,
            client,
        });
    }
    Ok(prepared)
}

fn same_stream_fill_identity(
    left: &BinancePrivateFillEvent,
    right: &BinancePrivateFillEvent,
) -> bool {
    left.stream_private_generation == right.stream_private_generation
        && left.private_generation == right.private_generation
        && left.fill == right.fill
        && left.client_order_id == right.client_order_id
        && left.original_quantity == right.original_quantity
        && left.cumulative_filled_quantity == right.cumulative_filled_quantity
        && left.order_state == right.order_state
}

fn stream_fill_context(
    event: &BinancePrivateFillEvent,
) -> Result<
    Option<(
        rust_decimal::Decimal,
        rust_decimal::Decimal,
        &'static str,
        String,
    )>,
    PrivateProjectionError,
> {
    let Some((original, cumulative, state)) = event.complete_order_progress() else {
        return Ok(None);
    };
    let client = match &event.client_order_id {
        FieldState::Known(value)
            if !value.trim().is_empty()
                && value.len() <= 36
                && !value.chars().any(char::is_whitespace) =>
        {
            value.clone()
        }
        _ => return Ok(None),
    };
    let state = match state {
        OrderState::PartiallyFilled => "partially_filled",
        OrderState::Filled => "filled",
        _ => return Err(PrivateProjectionError::Invalid),
    };
    Ok(Some((original, cumulative, state, client)))
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
        if old
            .get(key)
            .is_none_or(|prior| !same_position_inventory(prior, position))
        {
            insert_position_history(tx, source, current.observed_ms, position).await?;
        }
    }
    Ok(())
}

fn same_position_inventory(left: &TerminalPosition, right: &TerminalPosition) -> bool {
    left.symbol == right.symbol
        && left.position_side == right.position_side
        && left.quantity == right.quantity
        && left.entry_price == right.entry_price
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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_domain::domain::{OrderSide, Price};

    fn source() -> Result<ActiveProjectionSource, Box<dyn std::error::Error>> {
        Ok(ActiveProjectionSource {
            kol_user_id: None,
            owner_user_id: "owner".to_owned(),
            credential_id: "credential".to_owned(),
            trading_account_id: "account".to_owned(),
            symbols: ["BTC/USDT".parse()?].into_iter().collect(),
            previous_fills_cursor: None,
        })
    }

    fn stream_fill(
        fill_id: &str,
        cumulative: Decimal,
        state: OrderState,
    ) -> Result<BinancePrivateFillEvent, Box<dyn std::error::Error>> {
        Ok(BinancePrivateFillEvent {
            stream_private_generation: 3,
            private_generation: 3,
            received_at_ms: 200,
            fill: Fill {
                fill_id: fill_id.to_owned(),
                execution_sequence: FieldState::Known(7),
                order_id: "order-1".to_owned(),
                symbol: "BTC/USDT".parse()?,
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: Decimal::new(1, 3),
                price: Price::new(Decimal::new(100_000, 0))?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(true),
                exchange_time_ms: Some(199),
            },
            client_order_id: FieldState::Known("client-1".to_owned()),
            original_quantity: FieldState::Known(Decimal::new(2, 3)),
            cumulative_filled_quantity: FieldState::Known(cumulative),
            order_state: FieldState::Known(state),
        })
    }

    #[test]
    fn one_stream_fill_reuses_the_batch_contract() -> Result<(), Box<dyn std::error::Error>> {
        let event = stream_fill("trade-1", Decimal::new(1, 3), OrderState::PartiallyFilled)?;
        assert_eq!(prepare_stream_fill_batch(&source()?, &[event])?.len(), 1);
        Ok(())
    }

    #[test]
    fn partial_and_full_executions_from_one_order_share_a_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        let partial = stream_fill("trade-1", Decimal::new(1, 3), OrderState::PartiallyFilled)?;
        let mut full = stream_fill("trade-2", Decimal::new(2, 3), OrderState::Filled)?;
        full.fill.execution_sequence = FieldState::Known(8);
        full.received_at_ms = 201;
        full.fill.exchange_time_ms = Some(200);
        assert_eq!(
            prepare_stream_fill_batch(&source()?, &[partial, full])?.len(),
            2
        );
        Ok(())
    }

    #[test]
    fn mixed_generation_or_conflicting_trade_identity_rejects_the_whole_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        let partial = stream_fill("trade-1", Decimal::new(1, 3), OrderState::PartiallyFilled)?;
        let mut mixed_generation = stream_fill("trade-2", Decimal::new(2, 3), OrderState::Filled)?;
        mixed_generation.private_generation = 4;
        assert_eq!(
            prepare_stream_fill_batch(&source()?, &[partial.clone(), mixed_generation]).err(),
            Some(PrivateProjectionError::Invalid)
        );

        let mut conflicting_identity = partial.clone();
        conflicting_identity.fill.quantity = Decimal::new(2, 3);
        assert_eq!(
            prepare_stream_fill_batch(&source()?, &[partial, conflicting_identity]).err(),
            Some(PrivateProjectionError::Invalid)
        );
        Ok(())
    }
}
