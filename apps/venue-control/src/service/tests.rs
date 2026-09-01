use std::{
    collections::BTreeMap,
    future::{Future, ready},
    sync::{Arc, Mutex, MutexGuard},
};

use rust_decimal::Decimal;
use venue_control_protocol::{
    AccountSummary, CONTROL_SCHEMA_VERSION, CommandReceipt, CommandState, ConnectionState,
    ControlAction, ControlCommandRequest, ControlSnapshot, GatewayMode, HealthState, StrategyKind,
    StrategyLifecycle, StrategySummary, TradeIntent, TradingAction, TradingOrderType,
    TradingTimeInForce, UiAccountScope, UiEventEnvelope, UiEventKind, UiEventNotification, VenueId,
};

use super::*;

#[derive(Clone, Default)]
struct MemoryRepository {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Default)]
struct MemoryState {
    snapshot: Option<ControlSnapshot>,
    events: Vec<UiEventEnvelope>,
    commands: BTreeMap<String, MemoryCommand>,
}

struct MemoryCommand {
    command: ControlCommandRequest,
    receipt: CommandReceipt,
    delivery: MemoryDelivery,
}

enum MemoryDelivery {
    Pending,
    Claimed {
        consumer_id: String,
        claimed_ms: u64,
    },
    Settled,
}

impl MemoryRepository {
    fn lock(&self) -> Result<MutexGuard<'_, MemoryState>, RepositoryError> {
        self.state.lock().map_err(|_| RepositoryError::Database)
    }
}

impl ControlRepository for MemoryRepository {
    fn load_snapshot(
        &self,
    ) -> impl Future<Output = Result<Option<ControlSnapshot>, RepositoryError>> + Send {
        ready(self.lock().map(|state| state.snapshot.clone()))
    }

    fn store_snapshot(
        &self,
        snapshot: &ControlSnapshot,
    ) -> impl Future<Output = Result<SnapshotStoreResult, RepositoryError>> + Send {
        let result = self.lock().and_then(|mut state| {
            if let Some(current) = &state.snapshot {
                if current == snapshot {
                    return Ok(SnapshotStoreResult::Unchanged);
                }
                if snapshot.generated_ms <= current.generated_ms {
                    return Err(RepositoryError::SnapshotConflict);
                }
            }
            state.snapshot = Some(snapshot.clone());
            push_event(
                &mut state.events,
                UiEventKind::Snapshot,
                snapshot_scope(snapshot),
            )?;
            Ok(SnapshotStoreResult::Inserted {
                event_sequence: state.events.len() as i64,
            })
        });
        ready(result)
    }

    fn enqueue_command(
        &self,
        command: &ControlCommandRequest,
        accepted: &CommandReceipt,
    ) -> impl Future<Output = Result<CommandEnqueueResult, RepositoryError>> + Send {
        let result = self.lock().and_then(|mut state| {
            if !has_strategy_scope(state.snapshot.as_ref(), command) {
                return Err(RepositoryError::StaleScope);
            }
            if let Some(existing) = state.commands.get(&command.request_id) {
                if existing.command != *command {
                    return Err(RepositoryError::ReplayConflict);
                }
                return Ok(CommandEnqueueResult::Existing(existing.receipt.clone()));
            }
            state.commands.insert(
                command.request_id.clone(),
                MemoryCommand {
                    command: command.clone(),
                    receipt: accepted.clone(),
                    delivery: MemoryDelivery::Pending,
                },
            );
            push_event(
                &mut state.events,
                UiEventKind::Command,
                command_scope(command),
            )?;
            Ok(CommandEnqueueResult::Inserted(accepted.clone()))
        });
        ready(result)
    }

    fn claim_commands(
        &self,
        binding: &AccountNodeBinding,
        consumer_id: &str,
        claimed_ms: u64,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<ClaimedCommand>, RepositoryError>> + Send {
        let result = self.lock().map(|mut state| {
            let current_snapshot = state.snapshot.clone();
            let mut claimed = Vec::new();
            for entry in state.commands.values_mut() {
                if claimed.len() >= limit as usize
                    || !matches!(entry.delivery, MemoryDelivery::Pending)
                    || entry.command.venue != binding.venue
                    || entry.command.mode != binding.mode
                    || entry.command.trading_account_id != binding.trading_account_id
                    || !has_strategy_scope(current_snapshot.as_ref(), &entry.command)
                {
                    continue;
                }
                entry.delivery = MemoryDelivery::Claimed {
                    consumer_id: consumer_id.to_owned(),
                    claimed_ms,
                };
                claimed.push(ClaimedCommand {
                    command: entry.command.clone(),
                    consumer_id: consumer_id.to_owned(),
                    claimed_ms,
                });
            }
            claimed
        });
        ready(result)
    }

    fn settle_command(
        &self,
        scoped: &ScopedCommandReceipt,
    ) -> impl Future<Output = Result<CommandSettleResult, RepositoryError>> + Send {
        let result = self.lock().and_then(|mut state| {
            let entry = state
                .commands
                .get_mut(&scoped.command.request_id)
                .ok_or(RepositoryError::DeliveryConflict)?;
            if entry.command != scoped.command {
                return Err(RepositoryError::DeliveryConflict);
            }
            if entry.receipt.state != CommandState::Accepted {
                return if entry.receipt == scoped.receipt {
                    Ok(CommandSettleResult::Existing(entry.receipt.clone()))
                } else {
                    Err(RepositoryError::DeliveryConflict)
                };
            }
            match &entry.delivery {
                MemoryDelivery::Claimed {
                    consumer_id,
                    claimed_ms,
                } if consumer_id == &scoped.consumer_id
                    && scoped.receipt.observed_ms >= *claimed_ms
                    && scoped.receipt.observed_ms >= entry.receipt.observed_ms => {}
                MemoryDelivery::Pending
                | MemoryDelivery::Claimed { .. }
                | MemoryDelivery::Settled => return Err(RepositoryError::DeliveryConflict),
            }
            entry.receipt = scoped.receipt.clone();
            entry.delivery = MemoryDelivery::Settled;
            let receipt = entry.receipt.clone();
            push_event(
                &mut state.events,
                UiEventKind::Command,
                command_scope(&scoped.command),
            )?;
            Ok(CommandSettleResult::Stored(receipt))
        });
        ready(result)
    }

    fn list_events(
        &self,
        scope: &UiAccountScope,
        after_sequence: i64,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<StoredEvent>, RepositoryError>> + Send {
        let result = self.lock().map(|state| {
            state
                .events
                .iter()
                .enumerate()
                .filter_map(|(index, event)| {
                    let sequence = index as i64 + 1;
                    (sequence > after_sequence && event.scope == *scope).then_some(StoredEvent {
                        sequence,
                        event: event.clone(),
                    })
                })
                .take(limit as usize)
                .collect()
        });
        ready(result)
    }

    fn has_current_strategy_scope(
        &self,
        command: &ControlCommandRequest,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send {
        ready(
            self.lock()
                .map(|state| has_strategy_scope(state.snapshot.as_ref(), command)),
        )
    }

    fn has_current_account_scope(
        &self,
        venue: VenueId,
        mode: GatewayMode,
        trading_account_id: &str,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send {
        ready(self.lock().map(|state| {
            state.snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.accounts.iter().any(|account| {
                    account.venue == venue
                        && account.mode == mode
                        && account.trading_account_id == trading_account_id
                })
            })
        }))
    }
}

fn has_strategy_scope(snapshot: Option<&ControlSnapshot>, command: &ControlCommandRequest) -> bool {
    snapshot.is_some_and(|snapshot| {
        snapshot.strategies.iter().any(|strategy| {
            strategy.venue == command.venue
                && strategy.mode == command.mode
                && strategy.trading_account_id == command.trading_account_id
                && strategy.symbol == command.symbol
                && strategy.instance_id == command.instance_id
                && strategy.config_epoch == command.expected_config_epoch
        })
    })
}

#[tokio::test]
async fn snapshot_and_events_are_validated_and_monotonic() -> Result<(), Box<dyn std::error::Error>>
{
    let service = service_with_snapshot().await?;
    assert_eq!(service.snapshot().await?, snapshot(100, 7)?);
    assert_eq!(service.events(&scope(), 0, 10).await?.len(), 1);
    assert_eq!(
        service.publish_snapshot(&snapshot(100, 7)?).await?,
        SnapshotStoreResult::Unchanged
    );
    assert_eq!(
        service.publish_snapshot(&snapshot(99, 7)?).await,
        Err(ServiceError::Repository(RepositoryError::SnapshotConflict))
    );
    let inserted = service.publish_snapshot(&snapshot(101, 8)?).await?;
    assert_eq!(
        inserted,
        SnapshotStoreResult::Inserted { event_sequence: 2 }
    );
    assert_eq!(service.events(&scope(), 1, 10).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn submission_fails_closed_for_stale_scope_and_bad_confirmation()
-> Result<(), Box<dyn std::error::Error>> {
    let service = service_with_snapshot().await?;

    let mut stale = command(ControlAction::Pause)?;
    stale.expected_config_epoch += 1;
    assert_eq!(
        service.submit_command(&stale, 101).await,
        Err(ServiceError::StaleOrMismatchedScope)
    );

    let stop = command(ControlAction::Stop)?;
    assert!(matches!(
        service.submit_command(&stop, 101).await,
        Err(ServiceError::Protocol(_))
    ));
    Ok(())
}

#[tokio::test]
async fn trade_submission_requires_exact_manual_strategy_kind()
-> Result<(), Box<dyn std::error::Error>> {
    let service = service_with_snapshot().await?;
    let mut trade = command(ControlAction::Trade)?;
    trade.trade = Some(TradeIntent {
        action: TradingAction::CancelAllOrders,
        quote_asset: "USDT".to_owned(),
        order_type: TradingOrderType::Limit,
        time_in_force: TradingTimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        selected_price: None,
        quote_notional: None,
        close_quantity_cap: None,
        selected_order_id: None,
    });
    assert_eq!(
        service.submit_command(&trade, 101).await,
        Err(ServiceError::InvalidDelivery(
            "Trade commands require an exact Manual strategy binding"
        ))
    );

    let mut manual = snapshot(102, 7)?;
    manual.strategies[0].kind = StrategyKind::Manual;
    service.publish_snapshot(&manual).await?;
    assert_eq!(
        service.submit_command(&trade, 103).await?.state,
        CommandState::Accepted
    );
    Ok(())
}

#[tokio::test]
async fn exact_replay_is_idempotent_but_payload_variant_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let service = service_with_snapshot().await?;
    let pause = command(ControlAction::Pause)?;
    let first = service.submit_command(&pause, 101).await?;
    let replay = service.submit_command(&pause, 102).await?;
    assert_eq!(replay, first);

    let mut changed = pause;
    changed.action = ControlAction::Resume;
    assert_eq!(
        service.submit_command(&changed, 103).await,
        Err(ServiceError::Repository(RepositoryError::ReplayConflict))
    );
    Ok(())
}

#[tokio::test]
async fn claim_is_mode_bound_one_shot_and_unknown_is_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let service = service_with_snapshot().await?;
    let pause = command(ControlAction::Pause)?;
    service.submit_command(&pause, 101).await?;

    let binding = AccountNodeBinding {
        venue: pause.venue,
        mode: pause.mode,
        trading_account_id: pause.trading_account_id.clone(),
    };
    let claimed = service
        .claim_commands(&binding, "live-node", 102, 10)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert!(
        service
            .claim_commands(&binding, "live-node", 103, 10)
            .await?
            .is_empty()
    );

    let unknown = CommandReceipt {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: pause.request_id.clone(),
        state: CommandState::Unknown,
        receipt_id: "node-unknown:request-1".to_owned(),
        observed_ms: 104,
        detail: "dispatch result requires account reconciliation".to_owned(),
    };
    let scoped = ScopedCommandReceipt {
        command: pause.clone(),
        consumer_id: "live-node".to_owned(),
        receipt: unknown.clone(),
    };
    let mut wrong_consumer = scoped.clone();
    wrong_consumer.consumer_id = "other-node".to_owned();
    assert_eq!(
        service.record_receipt(&wrong_consumer).await,
        Err(ServiceError::Repository(RepositoryError::DeliveryConflict))
    );
    assert_eq!(service.record_receipt(&scoped).await?, unknown);
    assert_eq!(service.submit_command(&pause, 105).await?, unknown);
    assert!(
        service
            .claim_commands(&binding, "live-node", 106, 10)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn stale_pending_command_is_not_claimed_after_epoch_change()
-> Result<(), Box<dyn std::error::Error>> {
    let service = service_with_snapshot().await?;
    let pause = command(ControlAction::Pause)?;
    service.submit_command(&pause, 101).await?;
    service.publish_snapshot(&snapshot(102, 8)?).await?;
    assert_eq!(
        service.submit_command(&pause, 103).await,
        Err(ServiceError::StaleOrMismatchedScope)
    );

    let binding = AccountNodeBinding {
        venue: pause.venue,
        mode: pause.mode,
        trading_account_id: pause.trading_account_id,
    };
    assert!(
        service
            .claim_commands(&binding, "live-node", 104, 10)
            .await?
            .is_empty()
    );
    Ok(())
}

async fn service_with_snapshot()
-> Result<ControlService<MemoryRepository>, Box<dyn std::error::Error>> {
    let service = ControlService::new(MemoryRepository::default());
    service.publish_snapshot(&snapshot(100, 7)?).await?;
    Ok(service)
}

fn command(action: ControlAction) -> Result<ControlCommandRequest, Box<dyn std::error::Error>> {
    Ok(ControlCommandRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: "request-1".to_owned(),
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        instance_id: "grid-btc".to_owned(),
        symbol: "BTC/USDT".parse()?,
        action,
        trade: None,
        expected_config_epoch: 7,
        confirmation: None,
    })
}

fn scope() -> UiAccountScope {
    UiAccountScope {
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
    }
}

fn snapshot_scope(snapshot: &ControlSnapshot) -> UiAccountScope {
    snapshot
        .accounts
        .first()
        .map(|account| UiAccountScope {
            venue: account.venue,
            mode: account.mode,
            trading_account_id: account.trading_account_id.clone(),
        })
        .unwrap_or_else(scope)
}

fn command_scope(command: &ControlCommandRequest) -> UiAccountScope {
    UiAccountScope {
        venue: command.venue,
        mode: command.mode,
        trading_account_id: command.trading_account_id.clone(),
    }
}

fn push_event(
    events: &mut Vec<UiEventEnvelope>,
    event_type: UiEventKind,
    scope: UiAccountScope,
) -> Result<(), RepositoryError> {
    let cursor = events.len() as u64 + 1;
    let previous_cursor = events
        .iter()
        .rev()
        .find(|event| event.scope == scope)
        .map_or(0, |event| event.cursor);
    events.push(
        UiEventEnvelope::from_notification(
            UiEventNotification {
                schema_version: CONTROL_SCHEMA_VERSION,
                event_type,
                scope,
                observed_ms: cursor,
            },
            cursor,
            previous_cursor,
        )
        .map_err(|_| RepositoryError::CorruptData)?,
    );
    Ok(())
}

fn snapshot(
    generated_ms: u64,
    config_epoch: u64,
) -> Result<ControlSnapshot, Box<dyn std::error::Error>> {
    let trading_account_id = "00000000-0000-4000-8000-000000000001".to_owned();
    Ok(ControlSnapshot {
        schema_version: CONTROL_SCHEMA_VERSION,
        generated_ms,
        connection: ConnectionState::Live,
        accounts: vec![AccountSummary {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: trading_account_id.clone(),
            health: HealthState::Healthy,
            equity: Some(Decimal::new(10_000, 0)),
            available_margin: Some(Decimal::new(8_000, 0)),
            unrealized_pnl: Some(Decimal::ZERO),
            balances: Vec::new(),
            private_generation: 2,
            writer_generation: 1,
            last_reconciled_ms: generated_ms - 1,
        }],
        strategies: vec![StrategySummary {
            instance_id: "grid-btc".to_owned(),
            kind: StrategyKind::Grid,
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id,
            symbol: "BTC/USDT".parse()?,
            lifecycle: StrategyLifecycle::Running,
            config_epoch,
            open_orders: 4,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
            realized_pnl: Some(Decimal::ZERO),
            unrealized_pnl: Some(Decimal::ZERO),
            last_receipt_ms: generated_ms - 1,
            attention: None,
        }],
        copy_relations: Vec::new(),
        markets: Vec::new(),
        ledger: Vec::new(),
    })
}
