//! Account-serialized strategy dispatch inside the existing singleton process.
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;
use venue_control_protocol::kol::ExecutorCommandState;
use venue_execution::{AccountGatewayResult, validate_signed_durable_command};
use venue_gateway_api::{GatewayBinding, GatewayMode};

use crate::multi_venue_credentials::{StrategyCredentialStore, identity_hash};
use crate::multi_venue_exchange::{StrategyExchangeError, StrategyGateway};
use crate::multi_venue_store::{MultiVenueStore, MultiVenueStoreError, StrategyClaim};

/// Shared across Binance and the other five venues. It bounds account network turns, not the
/// number of exchange orders inside an already admitted Binance Grid microbatch.
pub static ACCOUNT_NETWORK_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(32);

pub fn now_ms() -> Result<u64, StrategyExchangeError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .filter(|n| *n > 0)
        .ok_or(StrategyExchangeError)
}

#[derive(Clone)]
pub struct MultiVenueExecutor {
    pool: PgPool,
    store: MultiVenueStore,
    credentials: StrategyCredentialStore,
    grids: crate::multi_venue_grid::StrategyGridRuntime,
    support: crate::support_martingale::SupportMartingaleRuntime,
}

impl MultiVenueExecutor {
    pub fn new(
        pool: PgPool,
        credentials: StrategyCredentialStore,
    ) -> Result<Self, MultiVenueStoreError> {
        Ok(Self {
            grids: crate::multi_venue_grid::StrategyGridRuntime::new(
                pool.clone(),
                credentials.clone(),
            ),
            support: crate::support_martingale::SupportMartingaleRuntime::new(
                pool.clone(),
                credentials.clone(),
            )?,
            store: MultiVenueStore::new(pool.clone()),
            pool,
            credentials,
        })
    }

    pub async fn run_until_shutdown(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut tasks = tokio::task::JoinSet::new();
        let mut active = BTreeSet::new();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                _ = tick.tick() => {
                    // Ordering by the oldest update rotates failed accounts behind useful work.
                    let accounts = sqlx::query("SELECT trading_account_id,MIN(updated_ms) AS oldest FROM (SELECT trading_account_id,updated_ms FROM venue_binance_commands WHERE strategy_command IS NOT NULL AND command_state IN ('pending','sending','accepted','reconcile_required') UNION ALL SELECT trading_account_id,updated_ms FROM venue_strategy_grids WHERE lifecycle IN ('running','pausing','stopping','resetting') UNION ALL SELECT trading_account_id,updated_ms FROM venue_support_martingale_instances WHERE lifecycle IN ('running','entry_paused','increase_paused','draining')) work GROUP BY trading_account_id ORDER BY oldest,trading_account_id LIMIT 232")
                        .fetch_all(&self.pool).await;
                    let Ok(accounts) = accounts else {
                        tracing::warn!("strategy discovery unavailable; durable commands retained");
                        continue;
                    };
                    for row in accounts {
                        if active.len() >= 32 { break; }
                        let Ok(account) = row.try_get::<String,_>("trading_account_id") else { continue; };
                        if !active.insert(account.clone()) { continue; }
                        let executor = self.clone();
                        tasks.spawn(async move {
                            if executor.account_turn(&account).await.is_err() {
                                tracing::warn!("strategy account turn failed; original command retained");
                            }
                            account
                        });
                    }
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Ok(account)) = joined { active.remove(&account); }
                    // A panicked worker cannot free its account for another send in this process.
                }
            }
        }
        // Blocking HTTP workers cannot be aborted. Retain the singleton until they have all left
        // transport; a new process must never overlap an old in-flight account worker.
        while tasks.join_next().await.is_some() {}
    }

    async fn account_turn(&self, account: &str) -> Result<(), MultiVenueStoreError> {
        // Planning and transport share the same in-process account worker. Planner failure must
        // not prevent an existing command from being reconciled below.
        if self.support.account_turn(account).await.is_err() {
            tracing::warn!(
                "support martingale planning deferred; durable account commands retained"
            );
        }
        if self.grids.account_turn(account).await.is_err() {
            tracing::warn!("strategy grid planning deferred; durable account commands retained");
        }
        let _slot = ACCOUNT_NETWORK_SLOTS
            .acquire()
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let now = now_ms().map_err(|_| MultiVenueStoreError::Unavailable)?;
        let Some(claim) = self.store.claim(account, now).await? else {
            return Ok(());
        };
        let result = self.perform(&claim).await;
        let stale_grid_plan = matches!(&result, Ok(AccountGatewayResult::Rejected {reason}) if reason=="strategy_grid_plan_changed");
        let now = now_ms().map_err(|_| MultiVenueStoreError::Unavailable)?;
        let (state, native) = match result {
            Ok(AccountGatewayResult::Accepted { venue_order_id }) => {
                (ExecutorCommandState::Reconciled, Some(venue_order_id))
            }
            Ok(AccountGatewayResult::Rejected { .. }) => (ExecutorCommandState::Rejected, None),
            Ok(AccountGatewayResult::Unknown) | Err(_) => {
                (ExecutorCommandState::ReconcileRequired, None)
            }
        };
        self.store
            .finish(&claim, state, now, native.as_deref())
            .await?;
        if stale_grid_plan {
            self.grids.invalidate_pending(&claim.command, now).await?;
        }
        if state == ExecutorCommandState::ReconcileRequired {
            let mut recovery = claim;
            recovery.reconcile_only = true;
            let attempts: i32 = sqlx::query_scalar(
                "SELECT reconcile_attempts FROM venue_binance_commands WHERE command_id=$1",
            )
            .bind(recovery.command.command_id().as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
            let shift =
                u32::try_from(attempts.clamp(0, 4)).map_err(|_| MultiVenueStoreError::Conflict)?;
            let delay = 500_u64 << shift;
            self.store
                .backoff(&recovery, now.saturating_add(delay), now)
                .await?;
        }
        Ok(())
    }

    async fn perform(
        &self,
        claim: &StrategyClaim,
    ) -> Result<AccountGatewayResult, StrategyExchangeError> {
        let account = &claim.command.mutation_owner().account;
        let prepared = async {
            let (credentials, expected_identity) = self
                .credentials
                .load(
                    &claim.owner_user_id,
                    &claim.credential_id,
                    account,
                    claim.reconcile_only,
                )
                .await?;
            let binding = GatewayBinding::new(
                claim.venue,
                GatewayMode::Live,
                account,
                claim.command.mutation_owner().symbol.clone(),
            )
            .map_err(|_| StrategyExchangeError)?;
            let context = self
                .store
                .execution_context(claim)
                .await
                .map_err(|_| StrategyExchangeError)?;
            let limits = if claim.reconcile_only {
                None
            } else {
                self.credentials.limits(&claim.credential_id).await?
            };
            let grid_fence = if claim.reconcile_only {
                vec![]
            } else {
                self.grids
                    .send_fence(&claim.command)
                    .await
                    .map_err(|_| StrategyExchangeError)?
            };
            let support_fence = if claim.reconcile_only {
                None
            } else {
                self.support
                    .send_fence(&claim.command, now_ms()?)
                    .await
                    .map_err(|_| StrategyExchangeError)?
            };
            Ok::<_, StrategyExchangeError>((
                credentials,
                expected_identity,
                binding,
                context,
                limits,
                grid_fence,
                support_fence,
            ))
        }
        .await;
        let (credentials, expected_identity, binding, context, limits, grid_fence, support_fence) =
            match prepared {
                Ok(prepared) => prepared,
                Err(_) => return Ok(pre_send_failure(claim.reconcile_only)),
            };
        let claim = claim.clone();
        let credential_id = claim.credential_id.clone();
        let (result, snapshot) = tokio::task::spawn_blocking(move || {
            let mut gateway =
                match StrategyGateway::connect(binding.clone(), credentials, claim.nonce) {
                    Ok(gateway) => gateway,
                    Err(_) => return Ok((pre_send_failure(claim.reconcile_only), None)),
                };
            match gateway.identity() {
                Ok(identity) if identity_hash(binding.venue, &identity) == expected_identity => {}
                _ => return Ok((pre_send_failure(claim.reconcile_only), None)),
            };
            if claim.reconcile_only {
                let result = gateway.reconcile(&claim.command, &context);
                let snapshot = gateway.snapshot(&binding).ok();
                return Ok((result, snapshot));
            }
            // Planned replacements are invalid after any newly observed execution, including
            // a fill racing the preceding cancel. Read originals before the final fresh account
            // snapshot; the consumed cursor is changed only by a later atomic planning cycle.
            let mut checked_grid_orders = Vec::new();
            for (row, expected_cancel) in grid_fence {
                let observed = match gateway.order_observation(&row.command) {
                    Ok(Some(observed)) => observed,
                    _ => {
                        return Ok((
                            AccountGatewayResult::Rejected {
                                reason: "strategy_grid_plan_changed".into(),
                            },
                            None,
                        ));
                    }
                };
                if !grid_fill_fence(row.native_id.as_deref(), row.observed_filled, &observed)
                    || !grid_expected_state(expected_cancel, observed.state)
                {
                    return Ok((
                        AccountGatewayResult::Rejected {
                            reason: "strategy_grid_plan_changed".into(),
                        },
                        None,
                    ));
                }
                checked_grid_orders.push((observed, expected_cancel));
            }
            let snapshot = match gateway.snapshot(&binding) {
                Ok(snapshot) => snapshot,
                Err(_) => return Ok((pre_send_failure(false), None)),
            };
            if support_fence.is_some_and(|fence| !fence.validates(&claim.command, &snapshot)) {
                return Ok((
                    AccountGatewayResult::Rejected {
                        reason: "strategy_support_plan_changed".into(),
                    },
                    Some(snapshot),
                ));
            }
            for (observed, expected_cancel) in checked_grid_orders {
                let fact = snapshot.open_orders().iter().find(|fact| {
                    fact.venue_order_id.as_deref() == Some(observed.native_order_id.as_str())
                });
                let consistent = if expected_cancel {
                    fact.is_none()
                } else {
                    fact.is_some_and(|fact| fact.filled_quantity == Some(observed.filled_quantity))
                };
                if !consistent {
                    return Ok((
                        AccountGatewayResult::Rejected {
                            reason: "strategy_grid_plan_changed".into(),
                        },
                        Some(snapshot),
                    ));
                }
            }
            if !validate_signed_durable_command(&binding, &claim.command, &snapshot, now_ms()?) {
                return Ok((
                    AccountGatewayResult::Rejected {
                        reason: "strategy_signed_preflight".into(),
                    },
                    Some(snapshot),
                ));
            }
            if !matches!(claim.command, venue_domain::ExecutionCommand::Cancel(_)) {
                let market = match gateway.market_facts() {
                    Ok(market) => market,
                    Err(_) => return Ok((pre_send_failure(false), Some(snapshot))),
                };
                if !crate::multi_venue_risk::market_guard(
                    &claim.command,
                    &snapshot,
                    &market,
                    limits.as_ref(),
                    now_ms()?,
                ) {
                    return Ok((
                        AccountGatewayResult::Rejected {
                            reason: "strategy_market_limits".into(),
                        },
                        Some(snapshot),
                    ));
                }
            }
            match gateway.submit(&claim.command, &context) {
                AccountGatewayResult::Rejected { reason } => {
                    Ok((AccountGatewayResult::Rejected { reason }, Some(snapshot)))
                }
                AccountGatewayResult::Unknown => Ok((AccountGatewayResult::Unknown, None)),
                AccountGatewayResult::Accepted { .. } => {
                    // An HTTP ACK alone never completes a durable strategy command.
                    let result = gateway.reconcile(&claim.command, &context);
                    let snapshot = gateway.snapshot(&binding).ok();
                    Ok((result, snapshot))
                }
            }
        })
        .await
        .map_err(|_| StrategyExchangeError)??;
        if let Some(snapshot) = snapshot {
            sqlx::query(
                "UPDATE venue_api_credentials SET strategy_snapshot=$1 WHERE credential_id=$2",
            )
            .bind(serde_json::to_value(snapshot).map_err(|_| StrategyExchangeError)?)
            .bind(credential_id)
            .execute(&self.pool)
            .await
            .map_err(|_| StrategyExchangeError)?;
        }
        Ok(result)
    }
}

fn pre_send_failure(reconcile_only: bool) -> AccountGatewayResult {
    // A fresh claim has not reached a mutation transport. Recovery claims retain their
    // original identity and uncertainty even when credentials or read-only APIs fail.
    if reconcile_only {
        AccountGatewayResult::Unknown
    } else {
        AccountGatewayResult::Rejected {
            reason: "strategy_pre_send_unavailable".into(),
        }
    }
}

fn grid_fill_fence(
    native: Option<&str>,
    consumed: rust_decimal::Decimal,
    observed: &venue_execution::DurableOrderObservation,
) -> bool {
    native == Some(observed.native_order_id.as_str())
        && consumed == observed.filled_quantity
        && observed.state != venue_domain::OrderState::Unknown
}

fn grid_expected_state(cancelled: bool, state: venue_domain::OrderState) -> bool {
    use venue_domain::OrderState;
    if cancelled {
        matches!(state, OrderState::Cancelled | OrderState::Expired)
    } else {
        matches!(state, OrderState::New | OrderState::PartiallyFilled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_send_failure_never_rejects_a_recovery_claim() {
        assert!(matches!(
            pre_send_failure(false),
            AccountGatewayResult::Rejected { .. }
        ));
        assert!(matches!(
            pre_send_failure(true),
            AccountGatewayResult::Unknown
        ));
    }

    #[test]
    fn cancellation_fill_race_invalidates_unsent_grid_replacement() {
        use rust_decimal::Decimal;
        let mut observation = venue_execution::DurableOrderObservation {
            client_order_id: "original".into(),
            native_order_id: "native".into(),
            state: venue_domain::OrderState::Cancelled,
            filled_quantity: Decimal::ZERO,
            average_price: venue_domain::domain::FieldState::Missing,
            cumulative_fee: venue_domain::domain::FieldState::Missing,
        };
        assert!(grid_fill_fence(Some("native"), Decimal::ZERO, &observation));
        observation.filled_quantity = Decimal::ONE;
        assert!(!grid_fill_fence(
            Some("native"),
            Decimal::ZERO,
            &observation
        ));
        assert!(!grid_fill_fence(Some("other"), Decimal::ONE, &observation));
        observation.state = venue_domain::OrderState::Unknown;
        assert!(!grid_fill_fence(Some("native"), Decimal::ONE, &observation));
        assert!(grid_expected_state(
            true,
            venue_domain::OrderState::Cancelled
        ));
        assert!(!grid_expected_state(true, venue_domain::OrderState::Filled));
        assert!(!grid_expected_state(
            false,
            venue_domain::OrderState::Cancelled
        ));
    }
}
