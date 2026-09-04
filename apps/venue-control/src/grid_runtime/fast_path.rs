//! Authenticated private-stream wake path for ordinary Grid rolling.
//!
//! Stream facts are only a process-local acceleration over one still-current signed projection.
//! PostgreSQL projection CAS, fill allocation, desired surface and command insertion remain the
//! durable boundary; any ambiguity requests a new signed projection instead of mutating.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};
use venue_control_protocol::{
    grid::{GridAnchor, GridInstanceState, GridOrderRole as ProtocolOrderRole},
    kol::{TerminalAccountProjection, TerminalPositionMode},
};
use venue_domain::domain::{FieldState, PositionSide, Price};
use venue_gateway_binance::{BinanceGridReferenceFacts, BinancePrivateFillEvent};
use venue_strategies::hedged_grid::{
    GridCloseReservations, GridConvergenceFacts, GridInstrumentLimits, GridMakerFill,
    GridOrderIntent, GridOrderKey, GridPlanDirective, GridPlanner, GridPlannerControl,
    GridPlannerInput, GridReferencePrice,
};

use super::{
    ActualSurface, BinanceGridRuntime, BinanceGridRuntimeError, GridDesiredSurface,
    GridFillAllocation, GridOrderOwnership, GridOwnedOrderState, GridRuntimeRecord, MAX_FILL_BATCH,
    actual_matches_desired, bind_plan_batch_identity, desired_diff, desired_digest, desired_orders,
    desired_valid_for_market, is_close_order, now_ms, planner_anchor, planner_config,
    prepare_mutation_batch, private_facts, remaining_quantity, strategy_position, strategy_role,
    validate_owned_order,
};
use crate::{
    executor_runtime::CommandWake, grid_store::GridPlanMutationBatch,
    private_projection::ActiveProjectionSource,
};

pub const GRID_PRIVATE_STREAM_CHANNEL_CAPACITY: usize = 128;
const GRID_FILL_COALESCE_WINDOW: std::time::Duration = std::time::Duration::from_millis(1);
const MAX_HOT_FILL_EVENTS: usize = 5;
const MAX_CACHED_STREAM_FILLS: usize = 64;

#[derive(Debug)]
pub enum GridPrivateStreamSignal {
    Fill {
        source: ActiveProjectionSource,
        event: BinancePrivateFillEvent,
    },
    /// A private-stream burst whose fills are already durably committed together. The producer
    /// must not plan lower-priority KOL work for the same account until Grid has settled this
    /// batch, otherwise those commands can win the shared account queue.
    FillBatch {
        source: ActiveProjectionSource,
        events: Vec<BinancePrivateFillEvent>,
        completion: oneshot::Sender<bool>,
    },
    Invalidate {
        credential_id: String,
    },
}

#[derive(Clone)]
struct GridStreamCache {
    record_revision: u64,
    signed_observed_ms: u64,
    private_generation: u64,
    stream_private_generation: u64,
    last_execution_sequence: u64,
    last_received_ms: u64,
    last_occurred_ms: u64,
    projection: TerminalAccountProjection,
    owners: Vec<GridOrderOwnership>,
    desired: GridDesiredSurface,
    provisional_clients: BTreeSet<String>,
    pending_cancel_targets: BTreeSet<String>,
    tail_batch_id: Option<String>,
    seen: BTreeMap<String, BinancePrivateFillEvent>,
}

pub(super) struct GridHotPathState {
    receiver: Option<mpsc::Receiver<GridPrivateStreamSignal>>,
    recovery_requests: Option<mpsc::Sender<String>>,
    command_wake: CommandWake,
    dispatch_cache: crate::GridHotDispatchCache,
    records: BTreeMap<String, GridRuntimeRecord>,
    markets: BTreeMap<String, BinanceGridReferenceFacts>,
    streams: BTreeMap<String, GridStreamCache>,
}

impl GridHotPathState {
    pub(super) fn recent_stream(&self, instance_id: &str, now: u64, max_age: u64) -> bool {
        self.streams.get(instance_id).is_some_and(|cache| {
            cache.last_received_ms <= now && now - cache.last_received_ms <= max_age
        })
    }
    pub(super) fn new(
        receiver: Option<mpsc::Receiver<GridPrivateStreamSignal>>,
        recovery_requests: Option<mpsc::Sender<String>>,
        command_wake: CommandWake,
        dispatch_cache: crate::GridHotDispatchCache,
    ) -> Self {
        Self {
            receiver,
            recovery_requests,
            command_wake,
            dispatch_cache,
            records: BTreeMap::new(),
            markets: BTreeMap::new(),
            streams: BTreeMap::new(),
        }
    }

    pub(super) fn take_receiver(&mut self) -> Option<mpsc::Receiver<GridPrivateStreamSignal>> {
        self.receiver.take()
    }

    pub(super) fn replace_records(&mut self, records: &[GridRuntimeRecord]) {
        self.records = records
            .iter()
            .cloned()
            .map(|record| (record.instance.instance_id.clone(), record))
            .collect();
        self.streams.retain(|instance_id, cached| {
            self.records.get(instance_id).is_some_and(|record| {
                record.instance.state == GridInstanceState::Running
                    && record.instance.revision == cached.record_revision
                    && record.tail_batch_id == cached.tail_batch_id
            })
        });
    }

    pub(super) fn cache_market(&mut self, instance_id: String, facts: BinanceGridReferenceFacts) {
        let changed_credential = self
            .markets
            .get(&instance_id)
            .filter(|prior| prior.rules.instrument.generation != facts.rules.instrument.generation)
            .and_then(|_| self.records.get(&instance_id))
            .map(|record| record.instance.credential_id.clone());
        self.markets.insert(instance_id, facts);
        if let Some(credential_id) = changed_credential {
            self.dispatch_cache.invalidate_credential(&credential_id);
        }
    }

    pub(super) fn wake_commands(&self) {
        self.command_wake.wake();
    }

    fn invalidate_credential(&mut self, credential_id: &str) {
        self.dispatch_cache.invalidate_credential(credential_id);
        let affected = self
            .records
            .values()
            .filter(|record| record.instance.credential_id == credential_id)
            .map(|record| record.instance.instance_id.clone())
            .collect::<Vec<_>>();
        for instance_id in affected {
            self.streams.remove(&instance_id);
        }
    }

    fn request_recovery(&self, credential_id: &str) {
        if let Some(sender) = &self.recovery_requests {
            let _ = sender.try_send(credential_id.to_owned());
        }
    }
}

pub(super) async fn receive_private_signal(
    receiver: &mut Option<mpsc::Receiver<GridPrivateStreamSignal>>,
) -> Option<GridPrivateStreamSignal> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

impl BinanceGridRuntime {
    pub(super) async fn handle_private_signal(
        &mut self,
        signal: GridPrivateStreamSignal,
        receiver: &mut Option<mpsc::Receiver<GridPrivateStreamSignal>>,
        deferred: &mut VecDeque<GridPrivateStreamSignal>,
    ) {
        let (source, mut events) = match signal {
            GridPrivateStreamSignal::Fill { source, event } => (source, vec![event]),
            GridPrivateStreamSignal::FillBatch {
                source,
                events,
                completion,
            } => {
                let settled = self.settle_private_events(&source, events).await;
                let _ = completion.send(settled);
                return;
            }
            GridPrivateStreamSignal::Invalidate { credential_id } => {
                self.hot_path.invalidate_credential(&credential_id);
                return;
            }
        };
        let deadline = tokio::time::Instant::now() + GRID_FILL_COALESCE_WINDOW;
        let mut invalidated = false;
        while events.len() < MAX_HOT_FILL_EVENTS {
            let Some(stream) = receiver.as_mut() else {
                break;
            };
            let next = match tokio::time::timeout_at(deadline, stream.recv()).await {
                Ok(Some(next)) => next,
                Ok(None) => {
                    *receiver = None;
                    break;
                }
                Err(_) => break,
            };
            match next {
                GridPrivateStreamSignal::Fill {
                    source: next_source,
                    event,
                } if same_stream_batch(&source, &events[0], &next_source, &event) => {
                    events.push(event);
                }
                GridPrivateStreamSignal::Invalidate { credential_id }
                    if credential_id == source.credential_id =>
                {
                    self.hot_path.invalidate_credential(&credential_id);
                    invalidated = true;
                }
                other => deferred.push_back(other),
            }
        }
        if invalidated {
            return;
        }
        self.settle_private_events(&source, events).await;
    }

    async fn settle_private_events(
        &mut self,
        source: &ActiveProjectionSource,
        events: Vec<BinancePrivateFillEvent>,
    ) -> bool {
        if let Err(error) = self.process_private_events(source, events).await {
            tracing::warn!(target: "venue_control::grid_hot_path", error = %error,
                "Grid stream planning requires signed recovery");
            self.hot_path.invalidate_credential(&source.credential_id);
            self.hot_path.request_recovery(&source.credential_id);
            false
        } else {
            true
        }
    }

    async fn process_private_events(
        &mut self,
        source: &ActiveProjectionSource,
        events: Vec<BinancePrivateFillEvent>,
    ) -> Result<(), BinanceGridRuntimeError> {
        if events.is_empty()
            || events.len() > MAX_HOT_FILL_EVENTS
            || events
                .iter()
                .any(|event| !source.symbols.contains(&event.fill.symbol))
        {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let symbol = events[0].fill.symbol.clone();
        if events.iter().any(|event| event.fill.symbol != symbol) {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let candidates = self
            .hot_path
            .records
            .values()
            .filter(|record| {
                record.owner_user_id == source.owner_user_id
                    && record.instance.credential_id == source.credential_id
                    && record.instance.trading_account_id == source.trading_account_id
                    && record.instance.symbol == symbol
            })
            .cloned()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(());
        }

        let mut owner_sets = BTreeMap::new();
        for record in &candidates {
            let owners = self
                .hot_path
                .streams
                .get(&record.instance.instance_id)
                .filter(|cache| cache.record_revision == record.instance.revision)
                .map(|cache| cache.owners.clone());
            let owners = match owners {
                Some(owners) => owners,
                None => {
                    self.store
                        .load_owned_orders(&record.instance.instance_id)
                        .await?
                }
            };
            owner_sets.insert(record.instance.instance_id.clone(), owners);
        }

        let mut grouped = BTreeMap::<String, Vec<BinancePrivateFillEvent>>::new();
        let mut saw_grid_identity = false;
        for event in events {
            let client = match &event.client_order_id {
                FieldState::Known(client) => client,
                _ => return Err(BinanceGridRuntimeError::Facts),
            };
            if candidates.iter().any(|record| {
                self.hot_path
                    .streams
                    .get(&record.instance.instance_id)
                    .is_some_and(|cache| {
                        cache.provisional_clients.contains(client)
                            || cache.pending_cancel_targets.contains(client)
                    })
            }) {
                return Err(BinanceGridRuntimeError::SurfaceConflict);
            }
            let matches = candidates
                .iter()
                .filter_map(|record| {
                    owner_sets
                        .get(&record.instance.instance_id)?
                        .iter()
                        .find(|owner| owner.client_order_id == *client)
                        .map(|owner| (record, owner))
                })
                .collect::<Vec<_>>();
            if matches.is_empty() {
                continue;
            }
            saw_grid_identity = true;
            if matches.len() != 1
                || matches[0].1.native_order_id.as_deref() != Some(event.native_order_id())
            {
                return Err(BinanceGridRuntimeError::SurfaceConflict);
            }
            grouped
                .entry(matches[0].0.instance.instance_id.clone())
                .or_default()
                .push(event);
        }
        if !saw_grid_identity {
            return Ok(());
        }
        for (instance_id, events) in grouped {
            let record = candidates
                .iter()
                .find(|record| record.instance.instance_id == instance_id)
                .cloned()
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let owners = owner_sets
                .remove(&instance_id)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            self.process_instance_events(record, owners, events).await?;
        }
        Ok(())
    }

    async fn process_instance_events(
        &mut self,
        record: GridRuntimeRecord,
        owners: Vec<GridOrderOwnership>,
        events: Vec<BinancePrivateFillEvent>,
    ) -> Result<(), BinanceGridRuntimeError> {
        let cached = self
            .hot_path
            .streams
            .get(&record.instance.instance_id)
            .filter(|cache| {
                cache.record_revision == record.instance.revision
                    && cache.tail_batch_id == record.tail_batch_id
            })
            .cloned();
        if !ordinary_stream_record_eligible(&record, cached.is_some()) {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let now = now_ms()?;
        let market = self
            .hot_path
            .markets
            .get(&record.instance.instance_id)
            .cloned()
            .ok_or(BinanceGridRuntimeError::Market)?;
        let reference_event = events
            .iter()
            .max_by_key(|event| event.fill.exchange_time_ms)
            .ok_or(BinanceGridRuntimeError::Facts)?;
        let reference_price = GridReferencePrice {
            price: reference_event.fill.price,
            observed_at_ms: reference_event
                .fill
                .exchange_time_ms
                .ok_or(BinanceGridRuntimeError::Facts)?,
        };

        let (
            baseline,
            owners,
            prior,
            mut seen,
            continuation,
            mut provisional_clients,
            mut pending_cancel_targets,
            projected_continuation,
        ) = if let Some(cache) = cached {
            if cache.private_generation != events[0].private_generation
                || cache.stream_private_generation != events[0].stream_private_generation
                || cache.owners != owners
            {
                return Err(BinanceGridRuntimeError::Facts);
            }
            validate_continuation(&cache, &events)?;
            (
                cache.projection,
                cache.owners,
                cache.desired,
                cache.seen,
                Some((
                    cache.signed_observed_ms,
                    cache.private_generation,
                    cache.stream_private_generation,
                )),
                cache.provisional_clients,
                cache.pending_cancel_targets,
                true,
            )
        } else {
            if record.instance.dirty {
                return Err(BinanceGridRuntimeError::Facts);
            }
            let projection = self
                .projections
                .load_healthy_owned(&record.owner_user_id, &record.instance.credential_id)
                .await?
                .ok_or(BinanceGridRuntimeError::PrivateProjection)?;
            let desired = self
                .store
                .load_desired_orders(&record.instance.instance_id)
                .await?
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let continuation = Some((
                projection.observed_ms,
                projection.private_generation,
                events[0].stream_private_generation,
            ));
            (
                projection,
                owners,
                desired,
                BTreeMap::new(),
                continuation,
                BTreeSet::new(),
                BTreeSet::new(),
                false,
            )
        };
        let (signed_observed_ms, private_generation, stream_generation) =
            continuation.ok_or(BinanceGridRuntimeError::Facts)?;
        if baseline.observed_ms != signed_observed_ms
            || baseline.private_generation != private_generation
            || baseline.position_mode != TerminalPositionMode::Hedge
            || baseline.observed_ms > now
            || events.iter().any(|event| {
                event.received_at_ms > now
                    || now.saturating_sub(event.received_at_ms)
                        > record.instance.config.reset_policy.stale_private_ms
            })
            || !desired_valid_for_market(&record, &prior, &market)
        {
            return Err(BinanceGridRuntimeError::Facts);
        }

        let mut fresh_events = Vec::new();
        for event in events {
            match seen.get(&event.fill.fill_id) {
                Some(previous) if previous == &event => continue,
                Some(_) => return Err(BinanceGridRuntimeError::Facts),
                None => fresh_events.push(event),
            }
        }
        if fresh_events.is_empty() {
            return Ok(());
        }
        let first_received_ms = fresh_events
            .iter()
            .map(|event| event.received_at_ms)
            .min()
            .ok_or(BinanceGridRuntimeError::Facts)?;

        let baseline_actual = actual_surface(&record, &baseline, &owners)?;
        if !projected_continuation && !surface_is_exact(&prior, &baseline_actual)? {
            return Err(BinanceGridRuntimeError::SurfaceConflict);
        }
        let overlay = if projected_continuation {
            super::stream_overlay::apply_stream_continuation(
                &record,
                &baseline,
                &owners,
                &fresh_events,
            )
        } else {
            super::stream_overlay::apply_stream_overlay(&record, &baseline, &owners, &fresh_events)
        };
        let mut overlaid = overlay.map_err(|error| {
            tracing::warn!(target: "venue_control::grid_hot_path",
                instance_id = %record.instance.instance_id,
                overlay_error = ?error, projected_continuation,
                "Grid authenticated fill overlay rejected");
            BinanceGridRuntimeError::Facts
        })?;
        for position in overlaid
            .projection
            .positions
            .iter_mut()
            .filter(|position| position.symbol == record.instance.symbol)
        {
            position.mark_price = Some(reference_price.price.value());
        }
        if overlaid.latest_event_ms > now {
            return Err(BinanceGridRuntimeError::Clock);
        }
        let mut actual = actual_surface(&record, &overlaid.projection, &overlaid.owners)?;
        self.add_command_reservations(&record, &overlaid.projection, &mut actual)
            .await?;
        let mut private = private_facts(&record, &overlaid.projection, &actual)?;
        private.inventory.private_observed_at_ms = overlaid.latest_event_ms;
        // Settle execution-driven rolling first. Profit reduction is evaluated by the cold
        // supervisor against a fresh PM equity read; it must not gate ordinary replenishment.
        let mut config = planner_config(&record)?;
        config.profit_reduction = None;
        let unallocated = self
            .store
            .load_unallocated_fills(&record.instance.instance_id, 0, MAX_FILL_BATCH)
            .await?;
        if unallocated.len() == usize::from(MAX_FILL_BATCH)
            || !same_fill_allocations(&unallocated, &overlaid.fills)
        {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let maker_fills = overlaid
            .fills
            .iter()
            .map(|applied| stream_maker_fill(applied, &actual.ownership))
            .collect::<Result<Vec<_>, _>>()?;
        let (projected_orders, projected_clients) = if projected_continuation {
            projected_after_fills(&prior, &overlaid.fills, &actual.ownership)?
        } else {
            (
                actual.intents.clone(),
                actual.orders.keys().cloned().collect::<BTreeSet<_>>(),
            )
        };
        let plan = GridPlanner::plan(&GridPlannerInput {
            config,
            instrument: market
                .rules
                .metadata()
                .map_err(|_| BinanceGridRuntimeError::Facts)?,
            instrument_limits: GridInstrumentLimits {
                minimum_quantity: market.rules.minimum_quantity,
                maximum_quantity: market.rules.maximum_quantity,
                minimum_price: Price::new(market.rules.minimum_price)
                    .map_err(|_| BinanceGridRuntimeError::Facts)?,
                maximum_price: Price::new(market.rules.maximum_price)
                    .map_err(|_| BinanceGridRuntimeError::Facts)?,
            },
            book: None,
            reference_price: Some(reference_price),
            inventory: private.inventory,
            owned_orders: projected_orders,
            maker_fills,
            pending_place_keys: pending_place_keys(&prior, &provisional_clients)?,
            other_close_reservations: actual.other_close_reservations.clone(),
            rolling_anchor: record
                .instance
                .anchor
                .as_ref()
                .map(|anchor| planner_anchor(anchor, record.instance.config_revision))
                .transpose()?,
            convergence: GridConvergenceFacts {
                pending_since_ms: None,
                consecutive_failures: 0,
            },
            risk: None,
            control: GridPlannerControl::Run,
            now_ms: now,
        })
        .map_err(|_| BinanceGridRuntimeError::Planner)?;
        let GridPlanDirective::Converge {
            rolling_anchor,
            desired_orders: intents,
        } = plan.directive
        else {
            return Err(BinanceGridRuntimeError::Planner);
        };

        let next_revision = record
            .instance
            .plan_revision
            .checked_add(1)
            .ok_or(BinanceGridRuntimeError::Facts)?;
        let next_orders = desired_orders(
            &record.instance.instance_id,
            record.instance.config_revision,
            &intents,
            Some(&prior),
            next_revision,
        )?;
        let digest = desired_digest(&rolling_anchor, &next_orders);
        let unchanged = prior.desired_digest == digest && prior.orders == next_orders;
        let plan_revision = if unchanged {
            record.instance.plan_revision
        } else {
            next_revision
        };
        let desired = GridDesiredSurface {
            instance_id: record.instance.instance_id.clone(),
            symbol: record.instance.symbol.clone(),
            config_revision: record.instance.config_revision,
            plan_revision,
            desired_digest: digest,
            orders: next_orders,
        };
        let latest_complete = overlaid
            .fills
            .iter()
            .filter(|fill| fill.complete)
            .max_by_key(|fill| {
                (
                    fill.allocation.observed_ms,
                    fill.allocation.native_trade_id.as_str(),
                )
            });
        let durable_anchor = if unchanged {
            record
                .instance
                .anchor
                .clone()
                .ok_or(BinanceGridRuntimeError::Facts)?
        } else {
            GridAnchor {
                revision: plan_revision,
                instrument_generation: rolling_anchor.instrument_generation,
                price: rolling_anchor.anchor_price.value(),
                price_step: rolling_anchor.step.value(),
                grid_quantity: rolling_anchor.grid_quantity,
                source_native_trade_id: latest_complete
                    .map(|fill| fill.allocation.native_trade_id.clone())
                    .or_else(|| {
                        record
                            .instance
                            .anchor
                            .as_ref()
                            .and_then(|anchor| anchor.source_native_trade_id.clone())
                    }),
                observed_ms: overlaid.latest_event_ms,
            }
        };
        let (placements, cancellations) = if projected_continuation {
            desired_diff_projected(&desired, &projected_clients)
        } else {
            desired_diff(&desired, &actual)
        };
        if cancellations
            .iter()
            .any(|target| provisional_clients.contains(*target))
        {
            return Err(BinanceGridRuntimeError::SurfaceConflict);
        }
        let mut mutation = prepare_mutation_batch(
            &record,
            &desired,
            placements,
            cancellations,
            0,
            rolling_anchor.instrument_generation,
            now,
        )?;
        let native_trade_ids = overlaid
            .fills
            .iter()
            .map(|fill| fill.allocation.native_trade_id.clone())
            .collect::<Vec<_>>();
        bind_plan_batch_identity(&mut mutation, &record, &desired, &native_trade_ids);
        let placed_clients = mutation
            .placements
            .iter()
            .map(|placement| placement.command.client_order_id.clone())
            .collect::<Vec<_>>();
        let cancelled_clients = mutation
            .cancellations
            .iter()
            .filter_map(|command| match &command.intent {
                super::GridCommandIntent::Cancel {
                    target_client_order_id,
                } => Some(target_client_order_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let desired_for_cache = desired.clone();
        let allocations = overlaid
            .fills
            .iter()
            .map(|fill| fill.allocation.clone())
            .collect::<Vec<_>>();
        let committed = self
            .store
            .commit_plan_mutation_batch(
                &GridPlanMutationBatch {
                    mutation,
                    expected_plan_revision: record.instance.plan_revision,
                    expected_desired_digest: Some(prior.desired_digest),
                    predecessor_batch_id: record.tail_batch_id.clone(),
                    expected_private_generation: private_generation,
                    expected_private_observed_ms: signed_observed_ms,
                    source_event_received_ms: Some(first_received_ms),
                    require_empty_account_queue: false,
                    anchor: durable_anchor,
                    desired_orders: desired.orders,
                    fill_allocations: allocations,
                    last_facts_ms: overlaid.latest_event_ms,
                },
                now,
            )
            .await?;
        if committed.receipt.command_count != 0 {
            // The durable batch already exists.  Token publication is only an acceleration, so
            // no local clock arithmetic may skip the command wake after commit.
            let valid_until_ms = first_received_ms
                .saturating_add(record.instance.config.reset_policy.stale_private_ms);
            let _ = self
                .hot_path
                .dispatch_cache
                .publish(crate::GridHotDispatchToken {
                    batch_id: committed.receipt.batch_id.clone(),
                    batch_digest: committed.receipt.batch_digest,
                    owner_user_id: record.owner_user_id.clone(),
                    trading_account_id: record.instance.trading_account_id.clone(),
                    credential_id: record.instance.credential_id.clone(),
                    symbol: record.instance.symbol.clone(),
                    private_generation,
                    private_observed_ms: signed_observed_ms,
                    source_event_received_ms: first_received_ms,
                    valid_until_ms,
                    rules: market.rules.clone(),
                });
        }
        let next_record = GridRuntimeRecord {
            owner_user_id: record.owner_user_id,
            instance: committed.instance.clone(),
            tail_batch_id: Some(committed.receipt.batch_id.clone()),
        };
        self.hot_path.records.insert(
            next_record.instance.instance_id.clone(),
            next_record.clone(),
        );
        if committed.receipt.command_count != 0 {
            self.hot_path.wake_commands();
        }
        if let Ok(committed_ms) = now_ms()
            && let Some(event_to_commit_ms) = committed_ms.checked_sub(first_received_ms)
        {
            tracing::info!(
                target: "venue_control::grid_hot_path",
                batch_id = %committed.receipt.batch_id,
                fill_count = fresh_events.len(),
                command_count = committed.receipt.command_count,
                event_to_commit_and_wake_ms = event_to_commit_ms,
                within_target = event_to_commit_ms <= 10,
                "Binance Grid authenticated-fill hot-path timing"
            );
        }
        for event in &fresh_events {
            seen.insert(event.fill.fill_id.clone(), event.clone());
        }
        while seen.len() > MAX_CACHED_STREAM_FILLS {
            let oldest = seen
                .iter()
                .min_by_key(|(_, event)| event.received_at_ms)
                .map(|(id, _)| id.clone())
                .ok_or(BinanceGridRuntimeError::Facts)?;
            seen.remove(&oldest);
        }
        provisional_clients.extend(placed_clients);
        pending_cancel_targets.extend(cancelled_clients);
        let last = fresh_events.last().ok_or(BinanceGridRuntimeError::Facts)?;
        let sequence = event_sequence(last)?;
        let occurred = event_occurred_ms(last)?;
        self.hot_path.streams.insert(
            next_record.instance.instance_id.clone(),
            GridStreamCache {
                record_revision: next_record.instance.revision,
                signed_observed_ms,
                private_generation,
                stream_private_generation: stream_generation,
                last_execution_sequence: sequence,
                last_received_ms: last.received_at_ms,
                last_occurred_ms: occurred,
                projection: overlaid.projection,
                owners: overlaid.owners,
                desired: desired_for_cache,
                provisional_clients,
                pending_cancel_targets,
                tail_batch_id: next_record.tail_batch_id.clone(),
                seen,
            },
        );
        Ok(())
    }
}

pub(super) fn ordinary_stream_record_eligible(
    record: &GridRuntimeRecord,
    has_continuation: bool,
) -> bool {
    record.instance.state == GridInstanceState::Running
        && (!record.instance.dirty || has_continuation)
}

fn same_stream_batch(
    source: &ActiveProjectionSource,
    first: &BinancePrivateFillEvent,
    next_source: &ActiveProjectionSource,
    next: &BinancePrivateFillEvent,
) -> bool {
    source.owner_user_id == next_source.owner_user_id
        && source.credential_id == next_source.credential_id
        && source.trading_account_id == next_source.trading_account_id
        && first.fill.symbol == next.fill.symbol
        && first.private_generation == next.private_generation
        && first.stream_private_generation == next.stream_private_generation
}

fn actual_surface(
    record: &GridRuntimeRecord,
    projection: &TerminalAccountProjection,
    owners: &[GridOrderOwnership],
) -> Result<ActualSurface, BinanceGridRuntimeError> {
    let ownership = owners
        .iter()
        .filter(|owner| owner.config_revision == record.instance.config_revision)
        .cloned()
        .map(|owner| (owner.client_order_id.clone(), owner))
        .collect::<BTreeMap<_, _>>();
    let mut orders = BTreeMap::new();
    let mut intents = Vec::new();
    let mut reservations = GridCloseReservations::default();
    let mut seen = BTreeSet::new();
    for order in projection
        .open_orders
        .iter()
        .filter(|order| order.symbol == record.instance.symbol)
    {
        if !seen.insert(order.client_order_id.clone()) {
            return Err(BinanceGridRuntimeError::SurfaceConflict);
        }
        let remaining = remaining_quantity(order)?;
        if let Some(owner) = ownership.get(&order.client_order_id) {
            validate_owned_order(record, owner, order)?;
            if owner.state != GridOwnedOrderState::Working
                || owner.filled_quantity != order.filled_quantity.unwrap_or(Decimal::ZERO)
            {
                return Err(BinanceGridRuntimeError::SurfaceConflict);
            }
            orders.insert(order.client_order_id.clone(), order.clone());
            if remaining > Decimal::ZERO {
                intents.push(GridOrderIntent {
                    key: GridOrderKey {
                        epoch: owner.config_revision,
                        position: strategy_position(owner.key.position_side)?,
                        role: strategy_role(owner.key.role),
                        level: owner.key.sequence,
                    },
                    side: order.order_side,
                    price: Price::new(order.limit_price.ok_or(BinanceGridRuntimeError::Facts)?)
                        .map_err(|_| BinanceGridRuntimeError::Facts)?,
                    quantity: remaining,
                    reduce_only: owner.key.role == ProtocolOrderRole::Close,
                });
            }
        } else if is_close_order(order) {
            match order.position_side {
                PositionSide::Long => {
                    reservations.long_quantity = reservations
                        .long_quantity
                        .checked_add(remaining)
                        .ok_or(BinanceGridRuntimeError::Facts)?;
                }
                PositionSide::Short => {
                    reservations.short_quantity = reservations
                        .short_quantity
                        .checked_add(remaining)
                        .ok_or(BinanceGridRuntimeError::Facts)?;
                }
                PositionSide::Net => return Err(BinanceGridRuntimeError::Facts),
            }
        }
    }
    for owner in ownership.values() {
        match owner.state {
            GridOwnedOrderState::Working if !orders.contains_key(&owner.client_order_id) => {
                return Err(BinanceGridRuntimeError::SurfaceConflict);
            }
            GridOwnedOrderState::Terminal if orders.contains_key(&owner.client_order_id) => {
                return Err(BinanceGridRuntimeError::SurfaceConflict);
            }
            _ => {}
        }
    }
    intents.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(ActualSurface {
        ownership,
        orders,
        intents,
        other_close_reservations: reservations,
    })
}

fn desired_intents(
    desired: &GridDesiredSurface,
) -> Result<Vec<GridOrderIntent>, BinanceGridRuntimeError> {
    let mut intents = desired
        .orders
        .iter()
        .map(|order| {
            Ok::<GridOrderIntent, BinanceGridRuntimeError>(GridOrderIntent {
                key: GridOrderKey {
                    epoch: desired.config_revision,
                    position: strategy_position(order.key.position_side)?,
                    role: strategy_role(order.key.role),
                    level: order.key.sequence,
                },
                side: order.key.order_side(),
                price: Price::new(order.limit_price).map_err(|_| BinanceGridRuntimeError::Facts)?,
                quantity: order.quantity,
                reduce_only: order.key.role == ProtocolOrderRole::Close,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    intents.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(intents)
}

fn pending_place_keys(
    desired: &GridDesiredSurface,
    provisional_clients: &BTreeSet<String>,
) -> Result<BTreeSet<GridOrderKey>, BinanceGridRuntimeError> {
    let keys = desired
        .orders
        .iter()
        .filter(|order| provisional_clients.contains(&order.client_order_id))
        .map(|order| {
            Ok(GridOrderKey {
                epoch: desired.config_revision,
                position: strategy_position(order.key.position_side)?,
                role: strategy_role(order.key.role),
                level: order.key.sequence,
            })
        })
        .collect::<Result<BTreeSet<_>, BinanceGridRuntimeError>>()?;
    if keys.len() != provisional_clients.len() {
        return Err(BinanceGridRuntimeError::SurfaceConflict);
    }
    Ok(keys)
}

fn projected_after_fills(
    prior: &GridDesiredSurface,
    applied: &[super::stream_overlay::AppliedGridStreamFill],
    owners: &BTreeMap<String, GridOrderOwnership>,
) -> Result<(Vec<GridOrderIntent>, BTreeSet<String>), BinanceGridRuntimeError> {
    let mut intents = desired_intents(prior)?;
    let mut clients = prior
        .orders
        .iter()
        .map(|order| order.client_order_id.clone())
        .collect::<BTreeSet<_>>();
    for fill in applied {
        let client = &fill.allocation.client_order_id;
        let desired = prior
            .orders
            .iter()
            .find(|order| &order.client_order_id == client)
            .ok_or(BinanceGridRuntimeError::SurfaceConflict)?;
        let key = GridOrderKey {
            epoch: prior.config_revision,
            position: strategy_position(desired.key.position_side)?,
            role: strategy_role(desired.key.role),
            level: desired.key.sequence,
        };
        if fill.complete {
            intents.retain(|intent| intent.key != key);
            clients.remove(client);
            continue;
        }
        let owner = owners
            .get(client)
            .ok_or(BinanceGridRuntimeError::SurfaceConflict)?;
        let remaining = owner
            .quantity
            .checked_sub(owner.filled_quantity)
            .filter(|quantity| *quantity > Decimal::ZERO)
            .ok_or(BinanceGridRuntimeError::SurfaceConflict)?;
        let intent = intents
            .iter_mut()
            .find(|intent| intent.key == key)
            .ok_or(BinanceGridRuntimeError::SurfaceConflict)?;
        intent.quantity = remaining;
    }
    intents.sort_by(|left, right| left.key.cmp(&right.key));
    Ok((intents, clients))
}

fn desired_diff_projected<'a>(
    desired: &'a GridDesiredSurface,
    projected_clients: &'a BTreeSet<String>,
) -> (Vec<&'a super::GridDesiredOrder>, Vec<&'a str>) {
    let desired_clients = desired
        .orders
        .iter()
        .map(|order| order.client_order_id.as_str())
        .collect::<BTreeSet<_>>();
    let placements = desired
        .orders
        .iter()
        .filter(|order| !projected_clients.contains(&order.client_order_id))
        .collect();
    let cancellations = projected_clients
        .iter()
        .map(String::as_str)
        .filter(|client| !desired_clients.contains(client))
        .collect();
    (placements, cancellations)
}

fn surface_is_exact(
    desired: &GridDesiredSurface,
    actual: &ActualSurface,
) -> Result<bool, BinanceGridRuntimeError> {
    if desired.orders.len() != actual.orders.len() {
        return Ok(false);
    }
    for order in &desired.orders {
        let Some(actual) = actual.orders.get(&order.client_order_id) else {
            return Ok(false);
        };
        if !matches!(
            actual_matches_desired(actual, order)?,
            super::DesiredOrderMatch::Exact
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn same_fill_allocations(
    durable: &[GridFillAllocation],
    applied: &[super::stream_overlay::AppliedGridStreamFill],
) -> bool {
    if durable.len() != applied.len() {
        return false;
    }
    let durable = durable
        .iter()
        .map(|fill| (fill.native_trade_id.as_str(), fill))
        .collect::<BTreeMap<_, _>>();
    applied.iter().all(|fill| {
        durable
            .get(fill.allocation.native_trade_id.as_str())
            .is_some_and(|stored| *stored == &fill.allocation)
    })
}

fn stream_maker_fill(
    applied: &super::stream_overlay::AppliedGridStreamFill,
    owners: &BTreeMap<String, GridOrderOwnership>,
) -> Result<GridMakerFill, BinanceGridRuntimeError> {
    let owner = owners
        .get(&applied.allocation.client_order_id)
        .ok_or(BinanceGridRuntimeError::Facts)?;
    Ok(GridMakerFill {
        fill_id: applied.allocation.native_trade_id.clone(),
        source_order: GridOrderIntent {
            key: GridOrderKey {
                epoch: owner.config_revision,
                position: strategy_position(owner.key.position_side)?,
                role: strategy_role(owner.key.role),
                level: owner.key.sequence,
            },
            side: owner.key.order_side(),
            price: Price::new(owner.limit_price).map_err(|_| BinanceGridRuntimeError::Facts)?,
            quantity: owner.quantity,
            reduce_only: owner.key.role == ProtocolOrderRole::Close,
        },
        complete: applied.complete,
        maker: true,
    })
}

fn validate_continuation(
    cache: &GridStreamCache,
    events: &[BinancePrivateFillEvent],
) -> Result<(), BinanceGridRuntimeError> {
    let first = events.first().ok_or(BinanceGridRuntimeError::Facts)?;
    if cache.seen.get(&first.fill.fill_id) == Some(first) {
        return Ok(());
    }
    let sequence = event_sequence(first)?;
    let occurred = event_occurred_ms(first)?;
    if sequence <= cache.last_execution_sequence
        || first.received_at_ms < cache.last_received_ms
        || occurred < cache.last_occurred_ms
    {
        return Err(BinanceGridRuntimeError::Facts);
    }
    Ok(())
}

fn event_sequence(event: &BinancePrivateFillEvent) -> Result<u64, BinanceGridRuntimeError> {
    match event.fill.execution_sequence {
        FieldState::Known(value) if value > 0 => Ok(value),
        _ => Err(BinanceGridRuntimeError::Facts),
    }
}

fn event_occurred_ms(event: &BinancePrivateFillEvent) -> Result<u64, BinanceGridRuntimeError> {
    event
        .fill
        .exchange_time_ms
        .filter(|value| *value > 0 && *value <= event.received_at_ms)
        .ok_or(BinanceGridRuntimeError::Facts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::domain::{Fill, OrderSide};

    #[test]
    fn coalescing_requires_the_same_account_symbol_and_generations()
    -> Result<(), Box<dyn std::error::Error>> {
        let baseline_source = source("credential-a", "account-a")?;
        let first = event("trade-a", 7, 11)?;
        let mut next = event("trade-b", 8, 12)?;
        assert!(same_stream_batch(
            &baseline_source,
            &first,
            &baseline_source,
            &next
        ));
        next.private_generation += 1;
        assert!(!same_stream_batch(
            &baseline_source,
            &first,
            &baseline_source,
            &next
        ));
        let other = source("credential-a", "account-b")?;
        assert!(!same_stream_batch(&baseline_source, &first, &other, &first));
        Ok(())
    }

    fn source(
        credential: &str,
        account: &str,
    ) -> Result<ActiveProjectionSource, Box<dyn std::error::Error>> {
        Ok(ActiveProjectionSource {
            kol_user_id: None,
            owner_user_id: "owner".into(),
            credential_id: credential.into(),
            trading_account_id: account.into(),
            symbols: BTreeSet::from(["BTC/USDT".parse()?]),
            previous_fills_cursor: None,
        })
    }

    fn event(
        fill_id: &str,
        sequence: u64,
        received_at_ms: u64,
    ) -> Result<BinancePrivateFillEvent, Box<dyn std::error::Error>> {
        Ok(BinancePrivateFillEvent {
            stream_private_generation: 3,
            private_generation: 5,
            received_at_ms,
            fill: Fill {
                fill_id: fill_id.into(),
                execution_sequence: FieldState::Known(sequence),
                order_id: "native".into(),
                symbol: "BTC/USDT".parse()?,
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: Decimal::ONE,
                price: Price::new(Decimal::from(100))?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(true),
                exchange_time_ms: Some(received_at_ms - 1),
            },
            client_order_id: FieldState::Known("client".into()),
            original_quantity: FieldState::Known(Decimal::ONE),
            cumulative_filled_quantity: FieldState::Known(Decimal::ONE),
            order_state: FieldState::Known(venue_domain::domain::OrderState::Filled),
        })
    }
}
