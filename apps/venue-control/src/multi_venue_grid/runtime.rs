use super::StrategyGridStore;
use crate::multi_venue_credentials::StrategyCredentialStore;
use crate::multi_venue_store::MultiVenueStoreError as Error;
use sqlx::PgPool;

#[derive(Clone)]
pub struct StrategyGridRuntime {
    store: StrategyGridStore,
    credentials: StrategyCredentialStore,
}
impl StrategyGridRuntime {
    pub(crate) async fn send_fence(
        &self,
        command: &venue_domain::ExecutionCommand,
    ) -> Result<Vec<(super::store::GridOrderRow, bool)>, Error> {
        let venue_domain::ExecutionCommand::PlaceLimit(order) = command else {
            return Ok(vec![]);
        };
        let id: Option<String> = sqlx::query_scalar(
            "SELECT instance_id FROM venue_strategy_grid_orders WHERE client_order_id=$1",
        )
        .bind(order.client_order_id.as_str())
        .fetch_optional(&self.store.pool)
        .await
        .map_err(|_| Error::Unavailable)?;
        let Some(id) = id else {
            return Ok(vec![]);
        };
        let cancelled:Vec<String>=sqlx::query_scalar("SELECT DISTINCT c.target_client_order_id FROM venue_binance_commands c JOIN venue_strategy_grid_orders o ON o.client_order_id=c.target_client_order_id WHERE c.trading_account_id=$1 AND c.command_origin='strategy' AND c.command_phase='cancel' AND c.command_state='reconciled' AND o.instance_id=$2 AND NOT o.terminal")
            .bind(&order.owner.account).bind(&id).fetch_all(&self.store.pool).await.map_err(|_| Error::Unavailable)?;
        Ok(self
            .store
            .orders(&id)
            .await?
            .into_iter()
            .filter(|row| row.native_id.is_some())
            .map(|row| {
                let expected_cancel = row
                    .command
                    .native_client_id()
                    .is_some_and(|client| cancelled.iter().any(|id| id == client.as_str()));
                (row, expected_cancel)
            })
            .collect())
    }

    pub(crate) async fn invalidate_pending(
        &self,
        command: &venue_domain::ExecutionCommand,
        now: u64,
    ) -> Result<(), Error> {
        let mut tx = self
            .store
            .pool
            .begin()
            .await
            .map_err(|_| Error::Unavailable)?;
        super::store::lock_account(&mut tx, &command.mutation_owner().account).await?;
        sqlx::query("UPDATE venue_binance_commands c SET command_state='cancelled',terminal_ms=$1,updated_ms=$1 FROM venue_strategy_grid_orders o WHERE c.client_order_id=o.client_order_id AND o.instance_id=$2 AND c.trading_account_id=$3 AND c.command_state='pending'")
            .bind(i64::try_from(now).map_err(|_| Error::Invalid)?).bind(&command.mutation_owner().strategy_instance_id).bind(&command.mutation_owner().account)
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)
    }

    pub fn new(pool: PgPool, credentials: StrategyCredentialStore) -> Self {
        Self {
            store: StrategyGridStore::new(pool),
            credentials,
        }
    }
    pub async fn account_turn(&self, account: &str) -> Result<(), Error> {
        let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(account).fetch_one(&self.store.pool).await.map_err(|_| Error::Unavailable)?;
        if pending {
            let now = crate::multi_venue_runtime::now_ms().map_err(|_| Error::Unavailable)?;
            self.store.note_account_pending(account, now).await?;
            return Ok(());
        }
        for mut record in self.store.active(account).await? {
            let now = crate::multi_venue_runtime::now_ms().map_err(|_| Error::Unavailable)?;
            let (counted_failure, paused) = self.store.note_new_rejections(&record, now).await?;
            if paused {
                continue;
            }
            if counted_failure {
                record = self
                    .store
                    .get(&record.owner_user_id, &record.instance_id)
                    .await?;
            }
            let orders = self.store.orders(&record.instance_id).await?;
            let queries = orders
                .iter()
                .filter(|r| {
                    r.native_id.is_some()
                        || !matches!(r.ledger_state.as_str(), "rejected" | "cancelled")
                })
                .map(|r| r.command.clone())
                .collect();
            let facts = self
                .credentials
                .grid_facts(
                    &record.owner_user_id,
                    &record.credential_id,
                    record.symbol.clone(),
                    queries,
                )
                .await;
            let now = crate::multi_venue_runtime::now_ms().map_err(|_| Error::Unavailable)?;
            let (snapshot, market, observations) = match facts {
                Ok(facts) => facts,
                Err(_) => {
                    self.store
                        .note_failure(&record, "signed_facts_unavailable", now)
                        .await?;
                    continue;
                }
            };
            match super::planner::plan(&record, &orders, &snapshot, &market, &observations, now) {
                Ok(work) => {
                    let enqueues_commands = !work.commands.is_empty();
                    if !counted_failure && work.lifecycle.as_deref() == Some("resetting") {
                        let paused = self
                            .store
                            .note_failure(&record, "planner_reset_required", now)
                            .await?;
                        if paused {
                            continue;
                        }
                        record = self
                            .store
                            .get(&record.owner_user_id, &record.instance_id)
                            .await?;
                    }
                    self.store.apply(&record, work, now).await?;
                    if enqueues_commands {
                        return Ok(());
                    }
                }
                Err(_) => {
                    let _ = self
                        .store
                        .note_failure(&record, "planner_requires_fresh_consistent_facts", now)
                        .await?;
                }
            }
        }
        Ok(())
    }
}
