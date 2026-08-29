use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender, TryRecvError},
    },
    thread,
};

use tracing::{info, warn};

use crate::{
    domain::Instrument,
    exchange::binance::{PrivateError, PrivateRest},
    execution::{CommandJournal, CommandState, DispatchGuard, WriterLeaseAuthority, WriterSession},
    storage::ProjectionStore,
    strategy::hedged_grid::{
        GridAction, GridPhase, GridTransaction, HedgedGridBinding, HedgedGridState, OwnedGridFill,
    },
};

use super::{
    BinancePrivateFactsTransport,
    hedged_grid_live::{
        GRID_REJECTED_RESET_DELAY_MS, GridMutation, HedgedGridLiveError,
        log_grid_transaction_result, parse_grid_client_order_id, save_state, settle_mutation,
        unsettled_transaction_mutations,
    },
    private_facts_worker::DurableStreamFullFill,
};

struct GridNetworkBatch {
    batch_id: String,
    mutations: Vec<GridMutation>,
}

enum GridNetworkMessage {
    Submit(GridNetworkBatch),
    Shutdown,
}

enum GridNetworkBatchOutcome {
    Completed(Vec<(GridMutation, Result<String, PrivateError>)>),
    DispatcherFailed,
}

struct GridNetworkCompletion {
    batch_id: String,
    outcome: GridNetworkBatchOutcome,
}

struct InFlightGridTransaction {
    transaction: GridTransaction,
    mutations: Vec<GridMutation>,
}

#[derive(Default)]
pub(super) struct HedgedGridHotPath {
    dispatcher: Option<GridHotDispatcher>,
    in_flight: BTreeMap<String, InFlightGridTransaction>,
}

impl HedgedGridHotPath {
    pub(super) fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    pub(super) fn release_dispatcher_if_idle(&mut self) {
        if self.in_flight.is_empty() {
            self.dispatcher = None;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn queue_durable_stream_fill(
        &mut self,
        state: &mut HedgedGridState,
        store: &ProjectionStore,
        commands: &mut CommandJournal,
        transport: &BinancePrivateFactsTransport,
        authority: &WriterLeaseAuthority,
        writer: &WriterSession,
        binding: &HedgedGridBinding,
        instrument: &Instrument,
        fill: DurableStreamFullFill,
        now_ms: u64,
    ) -> Result<bool, HedgedGridLiveError> {
        let mut next = state.clone();
        if next.phase != GridPhase::Running {
            info!(
                event = "grid_stream_fill_deferred",
                fill_id = %fill.fill_id,
                client_order_id = %fill.client_order_id,
                phase = ?next.phase,
                reason = "grid_reconciliation_in_progress",
                "网格重置期间延后成交滚动"
            );
            return Ok(false);
        }
        let key = match parse_grid_client_order_id(&fill.client_order_id) {
            Ok(key) => key,
            Err(_) => {
                info!(
                    event = "grid_stream_fill_ignored",
                    fill_id = %fill.fill_id,
                    client_order_id = %fill.client_order_id,
                    reason = "not_owned_grid_identity",
                    "忽略非本网格成交"
                );
                return Ok(false);
            }
        };
        if next
            .epoch
            .as_ref()
            .is_none_or(|epoch| key.epoch != epoch.epoch)
        {
            info!(
                event = "grid_stream_fill_ignored",
                fill_id = %fill.fill_id,
                client_order_id = %fill.client_order_id,
                reason = "historical_epoch",
                "忽略历史网格成交"
            );
            return Ok(false);
        }
        info!(
            event = "grid_stream_fill_received",
            fill_id = %fill.fill_id,
            client_order_id = %fill.client_order_id,
            event_time_ms = fill.event_time_ms,
            received_at_ms = fill.received_at_ms,
            durable_to_plan_ms = now_ms.saturating_sub(fill.received_at_ms),
            exchange_to_plan_ms = now_ms.saturating_sub(fill.event_time_ms),
            "用户流完整成交进入逐笔直驱热路径"
        );
        let private_generation = stream_fill_private_generation(&next, &fill)?;
        let application = super::hedged_grid::apply_owned_grid_fill(
            &mut next,
            OwnedGridFill {
                fill_id: fill.fill_id.clone(),
                private_generation,
                source_order: key,
                fill_price: fill.fill_price,
                complete: true,
                maker: fill.maker,
            },
            super::hedged_grid::GridFillProjection::ProjectStreamInventory,
        )?;
        let transactions = match application {
            super::hedged_grid::GridFillApplication::Rolling(actions) => actions
                .into_iter()
                .filter_map(|action| match action {
                    GridAction::Dispatch(transaction) => Some(transaction),
                    GridAction::Reset { .. }
                    | GridAction::Place(_)
                    | GridAction::Replenish(_)
                    | GridAction::ReanchorAtFill { .. } => None,
                })
                .collect::<Vec<_>>(),
            super::hedged_grid::GridFillApplication::ReanchorPending => {
                save_state(store, &next)?;
                *state = next;
                return Ok(true);
            }
            super::hedged_grid::GridFillApplication::Noop => Vec::new(),
            super::hedged_grid::GridFillApplication::TakerInventoryOnly => {
                save_state(store, &next)?;
                *state = next;
                return Err(HedgedGridLiveError::PostOnlyFillBecameTaker);
            }
            super::hedged_grid::GridFillApplication::AwaitLiquidityEvidence => {
                save_state(store, &next)?;
                *state = next;
                return Err(HedgedGridLiveError::FillLiquidityUnknown);
            }
        };
        save_state(store, &next)?;
        *state = next;
        if transactions.is_empty() {
            return Ok(false);
        }
        let mut reconciliation_needed = false;
        for transaction in transactions {
            reconciliation_needed |= self.queue_transaction(
                state,
                store,
                commands,
                transport,
                authority,
                writer,
                binding,
                instrument,
                transaction,
                now_ms,
            )?;
        }
        Ok(reconciliation_needed)
    }

    #[allow(clippy::too_many_arguments)]
    fn queue_transaction(
        &mut self,
        state: &mut HedgedGridState,
        store: &ProjectionStore,
        commands: &mut CommandJournal,
        transport: &BinancePrivateFactsTransport,
        authority: &WriterLeaseAuthority,
        writer: &WriterSession,
        binding: &HedgedGridBinding,
        instrument: &Instrument,
        transaction: GridTransaction,
        now_ms: u64,
    ) -> Result<bool, HedgedGridLiveError> {
        if self.in_flight.contains_key(&transaction.id)
            || has_unmanaged_unresolved(commands, &self.in_flight)
        {
            return Err(HedgedGridLiveError::Unresolved);
        }
        if self.dispatcher.is_none() {
            let guard = authority.persistent_dispatch_guard(writer)?;
            self.dispatcher = Some(GridHotDispatcher::start(
                transport.private_rest_handle(),
                guard,
            ));
        }
        info!(
            event = "grid_fill_transaction",
            transaction_id = %transaction.id,
            fill_id = %transaction.source_fill_id,
            source = ?transaction.source_order,
            place_1 = ?transaction.places[0].key,
            place_1_quantity = %transaction.places[0].quantity,
            place_2 = ?transaction.places[1].key,
            place_2_quantity = %transaction.places[1].quantity,
            cancel = ?transaction.cancel,
            "成交逐笔生成补2撤1事务"
        );
        let mutations = unsettled_transaction_mutations(
            commands,
            binding,
            instrument,
            std::slice::from_ref(&transaction),
        )?;
        for mutation in &mutations {
            mutation.prepare(commands)?;
        }
        for mutation in &mutations {
            commands.transition(mutation.command_id(), CommandState::Submitted)?;
        }
        let submitted_at_ms = transport.authoritative_now_ms()?;
        info!(
            event = "grid_fill_transaction_submitted",
            transaction_id = %transaction.id,
            fill_id = %transaction.source_fill_id,
            child_count = mutations.len(),
            plan_to_submit_ms = submitted_at_ms.saturating_sub(now_ms),
            "补2撤1已交给并发网络执行器"
        );
        let batch_id = transaction.id.clone();
        let tracked = InFlightGridTransaction {
            transaction,
            mutations: mutations.clone(),
        };
        let batch = GridNetworkBatch {
            batch_id: batch_id.clone(),
            mutations,
        };
        let Some(dispatcher) = self.dispatcher.as_ref() else {
            return Err(HedgedGridLiveError::Dispatch);
        };
        if dispatcher.submit(batch).is_ok() {
            self.in_flight.insert(batch_id, tracked);
            return Ok(false);
        }

        let result = settle_failed_network_batch(commands, &tracked.mutations);
        log_grid_transaction_result(commands, binding, instrument, &tracked.transaction, &result);
        state.settle_transaction(&tracked.transaction.id, false)?;
        state.defer_blocked_reconciliation_until(
            now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS),
        )?;
        save_state(store, state)?;
        self.dispatcher = None;
        Ok(true)
    }

    pub(super) fn drain_completions(
        &mut self,
        state: &mut HedgedGridState,
        store: &ProjectionStore,
        commands: &mut CommandJournal,
        binding: &HedgedGridBinding,
        instrument: &Instrument,
        now_ms: u64,
    ) -> Result<bool, HedgedGridLiveError> {
        let mut reconciliation_needed = false;
        let Some(dispatcher) = self.dispatcher.as_ref() else {
            return Ok(false);
        };
        while let Some(completion) = dispatcher.try_completion()? {
            let tracked = self
                .in_flight
                .remove(&completion.batch_id)
                .ok_or(HedgedGridLiveError::Dispatch)?;
            let result = match completion.outcome {
                GridNetworkBatchOutcome::Completed(outcomes)
                    if matching_mutation_outcomes(&tracked.mutations, &outcomes) =>
                {
                    settle_network_outcomes(commands, outcomes)
                }
                GridNetworkBatchOutcome::Completed(_)
                | GridNetworkBatchOutcome::DispatcherFailed => {
                    settle_failed_network_batch(commands, &tracked.mutations)
                }
            };
            log_grid_transaction_result(
                commands,
                binding,
                instrument,
                &tracked.transaction,
                &result,
            );
            state.settle_transaction(&tracked.transaction.id, result.is_ok())?;
            if result.is_err() {
                state.defer_blocked_reconciliation_until(
                    now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS),
                )?;
                warn!(
                    event = "grid_rejected_reset_deferred",
                    transaction_id = %tracked.transaction.id,
                    not_before_ms = now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS),
                    delay_ms = GRID_REJECTED_RESET_DELAY_MS,
                    "补撤请求未全部成功，30秒后仍未收敛才重置网格"
                );
                reconciliation_needed = true;
            }
            save_state(store, state)?;
        }
        Ok(reconciliation_needed)
    }
}

fn stream_fill_private_generation(
    state: &HedgedGridState,
    fill: &DurableStreamFullFill,
) -> Result<u64, HedgedGridLiveError> {
    if fill.private_generation == 0 || fill.received_at_ms == 0 {
        return Err(HedgedGridLiveError::Snapshot);
    }
    let Some(inventory) = state.inventory.as_ref() else {
        return Ok(fill.private_generation);
    };
    if fill.received_at_ms <= inventory.private_observed_at_ms {
        return Ok(inventory.private_generation);
    }
    let next = inventory
        .private_generation
        .checked_add(1)
        .ok_or(HedgedGridLiveError::Clock)?;
    Ok(next.max(fill.private_generation))
}

fn has_unmanaged_unresolved(
    commands: &CommandJournal,
    in_flight: &BTreeMap<String, InFlightGridTransaction>,
) -> bool {
    let managed = in_flight
        .values()
        .flat_map(|batch| {
            batch
                .mutations
                .iter()
                .map(|mutation| mutation.command_id().clone())
        })
        .collect::<BTreeSet<_>>();
    commands
        .unresolved_command_ids()
        .into_iter()
        .any(|command_id| !managed.contains(&command_id))
}

fn matching_mutation_outcomes(
    expected: &[GridMutation],
    outcomes: &[(GridMutation, Result<String, PrivateError>)],
) -> bool {
    let expected = expected
        .iter()
        .map(|mutation| mutation.command_id().clone())
        .collect::<BTreeSet<_>>();
    let observed = outcomes
        .iter()
        .map(|(mutation, _)| mutation.command_id().clone())
        .collect::<BTreeSet<_>>();
    expected.len() == outcomes.len() && expected == observed
}

fn settle_network_outcomes(
    commands: &mut CommandJournal,
    outcomes: Vec<(GridMutation, Result<String, PrivateError>)>,
) -> Result<(), HedgedGridLiveError> {
    let mut result = Ok(());
    for (mutation, outcome) in outcomes {
        if let Err(error) = settle_mutation(commands, mutation, outcome) {
            if result.is_ok() {
                result = Err(error);
            }
        }
    }
    result
}

fn settle_failed_network_batch(
    commands: &mut CommandJournal,
    mutations: &[GridMutation],
) -> Result<(), HedgedGridLiveError> {
    let outcomes = mutations
        .iter()
        .cloned()
        .map(|mutation| (mutation, Err(PrivateError::Http)))
        .collect();
    settle_network_outcomes(commands, outcomes)
}

struct GridHotDispatcher {
    sender: Sender<GridNetworkMessage>,
    completions: Receiver<GridNetworkCompletion>,
    worker: Option<thread::JoinHandle<()>>,
}

impl GridHotDispatcher {
    fn start(private: Arc<PrivateRest>, guard: DispatchGuard) -> Self {
        let (sender, jobs) = mpsc::channel();
        let (completed, completions) = mpsc::channel();
        let worker = thread::spawn(move || {
            let dispatch_guard = guard;
            let mut batch_workers = Vec::new();
            while let Ok(message) = jobs.recv() {
                match message {
                    GridNetworkMessage::Submit(batch) => {
                        reap_finished_batch_workers(&mut batch_workers);
                        let private = Arc::clone(&private);
                        let completed = completed.clone();
                        batch_workers.push(thread::spawn(move || {
                            let batch_id = batch.batch_id;
                            let outcome = thread::scope(|scope| {
                                let handles = batch
                                    .mutations
                                    .into_iter()
                                    .map(|mutation| {
                                        let private = Arc::clone(&private);
                                        scope.spawn(move || {
                                            let outcome = mutation.submit(private.as_ref());
                                            (mutation, outcome)
                                        })
                                    })
                                    .collect::<Vec<_>>();
                                handles
                                    .into_iter()
                                    .map(|handle| handle.join().map_err(|_| ()))
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .map_or(
                                GridNetworkBatchOutcome::DispatcherFailed,
                                GridNetworkBatchOutcome::Completed,
                            );
                            let _ = completed.send(GridNetworkCompletion { batch_id, outcome });
                        }));
                    }
                    GridNetworkMessage::Shutdown => break,
                }
            }
            for worker in batch_workers {
                let _ = worker.join();
            }
            drop(dispatch_guard);
        });
        Self {
            sender,
            completions,
            worker: Some(worker),
        }
    }

    fn submit(&self, batch: GridNetworkBatch) -> Result<(), HedgedGridLiveError> {
        self.sender
            .send(GridNetworkMessage::Submit(batch))
            .map_err(|_| HedgedGridLiveError::Dispatch)
    }

    fn try_completion(&self) -> Result<Option<GridNetworkCompletion>, HedgedGridLiveError> {
        match self.completions.try_recv() {
            Ok(completion) => Ok(Some(completion)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(HedgedGridLiveError::Dispatch),
        }
    }
}

impl Drop for GridHotDispatcher {
    fn drop(&mut self) {
        let _ = self.sender.send(GridNetworkMessage::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn reap_finished_batch_workers(workers: &mut Vec<thread::JoinHandle<()>>) {
    let mut index = 0;
    while index < workers.len() {
        if workers[index].is_finished() {
            let worker = workers.swap_remove(index);
            let _ = worker.join();
        } else {
            index += 1;
        }
    }
}

#[cfg(test)]
mod generation_tests {
    use rust_decimal::Decimal;

    use crate::{
        domain::{FieldState, Price},
        strategy::hedged_grid::{GridInventory, HedgedGridParams, HedgedGridState},
    };

    use super::*;

    #[test]
    fn same_connection_fill_after_inventory_uses_a_later_strategy_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = HedgedGridState::new_with_params(
            super::super::hedged_grid_live::phase_one_binding()?,
            HedgedGridParams::phase_one(10)?,
        )?;
        state.inventory = Some(GridInventory {
            private_generation: 41,
            private_observed_at_ms: 1_000,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        });
        let mut fill = DurableStreamFullFill {
            fill_id: "fill-1".to_owned(),
            private_generation: 7,
            client_order_id: "owned".to_owned(),
            event_time_ms: 1_001,
            received_at_ms: 1_001,
            fill_price: Price::new(Decimal::new(100, 0))?,
            maker: FieldState::Known(true),
        };
        assert_eq!(stream_fill_private_generation(&state, &fill)?, 42);

        fill.received_at_ms = 1_000;
        assert_eq!(stream_fill_private_generation(&state, &fill)?, 41);
        Ok(())
    }
}
