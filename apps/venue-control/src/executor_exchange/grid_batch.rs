use std::{collections::BTreeMap, time::Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use rust_decimal::Decimal;
use venue_domain::domain::PositionSide;
use venue_gateway_binance::{
    BinanceCancelIntent, BinanceCredentials, BinanceGridDispatchFence, BinanceHttpTransport,
    BinanceMarketIntent, BinanceMutationAck, BinancePlaceIntent, BinancePreparedDispatch,
    BinancePreparedMutation, BinancePrivateReadScope, BinanceTimeInForce, BinanceTransportError,
    prepare_cancel, prepare_place_limit, prepare_place_market,
};

use super::{
    BinanceExecutionError, BinanceHttpExecution, ExecutionOrderKind, ExecutionOutcome,
    ExecutionReadback, ExecutionRequest, GridBatchCommandOutcome, GridBatchExecutionOutcome,
    GridBatchSubmitTiming, PlaceReadbackDecision, cancel_target, check_minimum_notional,
    dispatch_failed, dispatch_unknown, exact_place_matches, normalize_quantity, now_ms,
    opening_minimum_notional_required, outcome, place_readback_decision, place_shape,
    position_quantity, reserved_close_quantity, terminal_order_state, validate_request_binding,
};

impl BinanceHttpExecution {
    pub(super) async fn submit_grid_batch_request(
        &mut self,
        requests: &[ExecutionRequest],
        credentials: BinanceCredentials,
    ) -> Result<GridBatchExecutionOutcome, BinanceExecutionError> {
        self.submit_grid_batch_request_started(requests, credentials, Instant::now())
            .await
    }

    pub(super) async fn submit_grid_batch_request_started(
        &mut self,
        requests: &[ExecutionRequest],
        credentials: BinanceCredentials,
        executor_started: Instant,
    ) -> Result<GridBatchExecutionOutcome, BinanceExecutionError> {
        validate_grid_batch_requests(&self.transport, requests)?;
        let (before, rules) = self.snapshot_with_rules(&requests[0], &credentials).await?;
        let prepared = prepare_grid_batch(requests, &rules, &before)?;
        self.dispatch_prepared_grid_batch(
            requests,
            &credentials,
            before.scope(),
            &rules,
            prepared,
            executor_started,
        )
        .await
    }

    /// Uses only the one-shot facts from a just-committed Grid plan. Every error returned by this
    /// method occurs before a physical send; once the burst starts, ambiguity is represented per
    /// command and the caller must never fall back to a second mutation attempt.
    pub(super) async fn submit_grid_batch_hot_request(
        &mut self,
        requests: &[ExecutionRequest],
        credentials: &BinanceCredentials,
        token: &crate::GridHotDispatchToken,
        executor_started: Instant,
    ) -> Result<GridBatchExecutionOutcome, BinanceExecutionError> {
        validate_grid_batch_shape(requests)?;
        let first = requests.first().ok_or(BinanceExecutionError::Invalid)?;
        let now = now_ms()?;
        if !token.valid()
            || token.credential_id != first.credential_id
            || token.trading_account_id != first.trading_account_id
            || token.symbol != first.symbol
            || token.rules.instrument.generation == 0
            || token.private_generation == 0
            || token.source_event_received_ms > now
            || now > token.valid_until_ms
        {
            return Err(BinanceExecutionError::Invalid);
        }
        self.transport
            .rebind_generations(token.rules.instrument.generation, token.private_generation)
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        validate_grid_batch_requests(&self.transport, requests)?;
        let requested_at_ms = self
            .transport
            .signing_timestamp_ms()
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BinanceExecutionError::Unavailable)?;
        let fence = BinanceGridDispatchFence::new(
            self.transport.config(),
            token.rules.clone(),
            token.private_generation,
            attempt_id,
            requested_at_ms,
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        let prepared = prepare_hot_grid_batch(requests, &fence)?;
        self.dispatch_prepared_grid_batch(
            requests,
            credentials,
            fence.scope(),
            fence.rules(),
            prepared,
            executor_started,
        )
        .await
    }

    async fn dispatch_prepared_grid_batch(
        &mut self,
        requests: &[ExecutionRequest],
        credentials: &BinanceCredentials,
        scope: &BinancePrivateReadScope,
        rules: &venue_gateway_binance::BinanceInstrumentRules,
        prepared: Vec<PreparedGridCommand>,
        executor_started: Instant,
    ) -> Result<GridBatchExecutionOutcome, BinanceExecutionError> {
        let mut outcomes = vec![None; requests.len()];
        let mut acknowledgements = vec![None; requests.len()];
        let mut native_ids = vec![None; requests.len()];
        let mut place_dispatches = Vec::new();
        let mut cancel_dispatches = Vec::new();
        for (index, child) in prepared.into_iter().enumerate() {
            native_ids[index] = child.native_id;
            if let Some(immediate) = child.immediate {
                outcomes[index] = Some(GridBatchCommandOutcome::Submitted(immediate));
                continue;
            }
            let mutation = child.mutation.ok_or(BinanceExecutionError::Invalid)?;
            let dispatch = self
                .transport
                .prepare_dispatch(
                    credentials,
                    scope,
                    &mutation,
                    self.transport
                        .signing_timestamp_ms()
                        .map_err(|_| BinanceExecutionError::Unavailable)?,
                )
                .map_err(prepare_dispatch_error)?;
            match &requests[index].order_kind {
                ExecutionOrderKind::Market { .. } | ExecutionOrderKind::LimitPostOnly { .. } => {
                    place_dispatches.push((index, dispatch));
                }
                ExecutionOrderKind::CancelExact { .. } => {
                    cancel_dispatches.push((index, dispatch));
                }
            }
        }
        let mut first_submit_us = None;
        let mut last_submit_us = None;
        let mut outbound_attempts = 0_u16;
        let burst = dispatch_grid_burst(
            &self.transport,
            place_dispatches,
            cancel_dispatches,
            executor_started,
        )
        .await;
        record_phase_timing(
            burst.send_entries_us,
            &mut first_submit_us,
            &mut last_submit_us,
            &mut outbound_attempts,
        );
        record_phase_results(
            burst.results,
            &native_ids,
            &mut outcomes,
            &mut acknowledgements,
        );
        for index in burst.not_dispatched_cancels {
            outcomes[index] = Some(GridBatchCommandOutcome::NotDispatched(
                BinanceExecutionError::Unavailable,
            ));
        }
        for (index, ack) in acknowledgements.into_iter().enumerate() {
            let Some(ack) = ack else { continue };
            outcomes[index] = Some(GridBatchCommandOutcome::Submitted(
                self.grid_batch_signed_readback(&requests[index], credentials, scope, rules, &ack)
                    .await,
            ));
        }
        let commands = outcomes
            .into_iter()
            .enumerate()
            .map(|(index, child)| match child {
                Some(child) => child,
                None => GridBatchCommandOutcome::Submitted(outcome(
                    ExecutionReadback::Unknown,
                    native_ids[index].clone(),
                )),
            })
            .collect();
        Ok(GridBatchExecutionOutcome {
            commands,
            timing: GridBatchSubmitTiming {
                executor_start_to_first_submit_us: first_submit_us,
                executor_start_to_last_submit_us: last_submit_us,
                first_to_last_submit_us: first_submit_us
                    .zip(last_submit_us)
                    .map(|(first, last)| last.saturating_sub(first)),
                outbound_attempts,
            },
        })
    }

    async fn grid_batch_signed_readback(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
        scope: &BinancePrivateReadScope,
        rules: &venue_gateway_binance::BinanceInstrumentRules,
        ack: &BinanceMutationAck,
    ) -> ExecutionOutcome {
        let client_order_id = match &request.order_kind {
            ExecutionOrderKind::CancelExact {
                target_client_order_id,
                ..
            } => target_client_order_id
                .as_deref()
                .unwrap_or(request.client_order_id.as_str()),
            ExecutionOrderKind::Market { .. } | ExecutionOrderKind::LimitPostOnly { .. } => {
                request.client_order_id.as_str()
            }
        };
        let order = match self
            .exact_order_for_client_in_scope(request, credentials, client_order_id, scope)
            .await
        {
            Ok(order) => order,
            Err(_) => return outcome(ExecutionReadback::Unknown, Some(ack.order_id.clone())),
        };
        if order.order_id != ack.order_id {
            return outcome(ExecutionReadback::Unknown, Some(ack.order_id.clone()));
        }
        let state = match &request.order_kind {
            ExecutionOrderKind::CancelExact { .. } => {
                if terminal_order_state(order.state) {
                    ExecutionReadback::Reconciled
                } else {
                    ExecutionReadback::Accepted
                }
            }
            ExecutionOrderKind::LimitPostOnly { .. }
                if exact_place_matches(request, &order, rules) == Ok(true) =>
            {
                match place_readback_decision(order.state, order.filled_quantity) {
                    PlaceReadbackDecision::Accepted => ExecutionReadback::Accepted,
                    PlaceReadbackDecision::Rejected => ExecutionReadback::Rejected,
                    PlaceReadbackDecision::VerifyTerminal | PlaceReadbackDecision::Unknown => {
                        ExecutionReadback::Unknown
                    }
                }
            }
            ExecutionOrderKind::Market { .. } | ExecutionOrderKind::LimitPostOnly { .. } => {
                ExecutionReadback::Unknown
            }
        };
        outcome(state, Some(ack.order_id.clone()))
    }
}

struct GridDispatchBurst {
    results: Vec<(usize, Result<BinanceMutationAck, BinanceTransportError>)>,
    send_entries_us: Vec<u64>,
    not_dispatched_cancels: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GridDispatchPhase {
    Place,
    Cancel,
}

struct PlaceSendBarrier {
    expected: usize,
    observed: std::collections::BTreeSet<usize>,
    failed: bool,
}

impl PlaceSendBarrier {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            observed: std::collections::BTreeSet::new(),
            failed: false,
        }
    }

    fn observe(&mut self, index: usize) -> bool {
        self.observed.insert(index);
        !self.failed && self.observed.len() == self.expected
    }

    fn complete(&mut self, index: usize) {
        if !self.observed.contains(&index) {
            self.failed = true;
        }
    }
}

async fn dispatch_grid_burst(
    transport: &BinanceHttpTransport,
    place_dispatches: Vec<(usize, BinancePreparedDispatch)>,
    cancel_dispatches: Vec<(usize, BinancePreparedDispatch)>,
    executor_started: Instant,
) -> GridDispatchBurst {
    dispatch_grid_burst_with(
        place_dispatches,
        cancel_dispatches,
        executor_started,
        |index, phase, dispatch, started, send_entry| {
            dispatch_grid_child(transport, index, phase, dispatch, started, send_entry)
        },
    )
    .await
}

async fn dispatch_grid_burst_with<T, F, Fut>(
    place_dispatches: Vec<(usize, T)>,
    cancel_dispatches: Vec<(usize, T)>,
    executor_started: Instant,
    dispatch_child: F,
) -> GridDispatchBurst
where
    F: Fn(
        usize,
        GridDispatchPhase,
        T,
        Instant,
        tokio::sync::mpsc::UnboundedSender<(GridDispatchPhase, usize, u64)>,
    ) -> Fut,
    Fut: std::future::Future<
            Output = (
                GridDispatchPhase,
                usize,
                Result<BinanceMutationAck, BinanceTransportError>,
            ),
        >,
{
    let (send_entry, mut send_entries) = tokio::sync::mpsc::unbounded_channel();
    let mut inflight = FuturesUnordered::new();
    let place_count = place_dispatches.len();
    for (index, dispatch) in place_dispatches {
        inflight.push(dispatch_child(
            index,
            GridDispatchPhase::Place,
            dispatch,
            executor_started,
            send_entry.clone(),
        ));
    }
    let mut pending_cancels = Some(cancel_dispatches);
    let mut place_barrier = PlaceSendBarrier::new(place_count);
    let mut cancels_started = false;
    let mut results = Vec::new();
    let mut observed = Vec::new();
    if place_count == 0 {
        if let Some(cancels) = pending_cancels.take() {
            for (index, dispatch) in cancels {
                inflight.push(dispatch_child(
                    index,
                    GridDispatchPhase::Cancel,
                    dispatch,
                    executor_started,
                    send_entry.clone(),
                ));
            }
        }
        cancels_started = true;
    }
    while !inflight.is_empty() {
        tokio::select! {
            biased;
            Some((phase, index, elapsed_us)) = send_entries.recv() => {
                observed.push(elapsed_us);
                if phase == GridDispatchPhase::Place {
                    if !cancels_started
                        && place_barrier.observe(index)
                    {
                        if let Some(cancels) = pending_cancels.take() {
                            for (cancel_index, dispatch) in cancels {
                                inflight.push(dispatch_child(
                                    cancel_index,
                                    GridDispatchPhase::Cancel,
                                    dispatch,
                                    executor_started,
                                    send_entry.clone(),
                                ));
                            }
                        }
                        cancels_started = true;
                    }
                }
            }
            Some((phase, index, result)) = inflight.next() => {
                let barrier_reached = drain_send_entries(
                    &mut send_entries,
                    &mut observed,
                    &mut place_barrier,
                );
                if !cancels_started && barrier_reached {
                    if let Some(cancels) = pending_cancels.take() {
                        for (cancel_index, dispatch) in cancels {
                            inflight.push(dispatch_child(
                                cancel_index,
                                GridDispatchPhase::Cancel,
                                dispatch,
                                executor_started,
                                send_entry.clone(),
                            ));
                        }
                    }
                    cancels_started = true;
                }
                if phase == GridDispatchPhase::Place {
                    place_barrier.complete(index);
                }
                results.push((index, result));
            }
        }
    }
    drop(send_entry);
    let _ = drain_send_entries(&mut send_entries, &mut observed, &mut place_barrier);
    let not_dispatched_cancels = match pending_cancels {
        Some(cancels) => cancels.into_iter().map(|(index, _)| index).collect(),
        None => Vec::new(),
    };
    GridDispatchBurst {
        results,
        send_entries_us: observed,
        not_dispatched_cancels,
    }
}

fn drain_send_entries(
    send_entries: &mut tokio::sync::mpsc::UnboundedReceiver<(GridDispatchPhase, usize, u64)>,
    observed: &mut Vec<u64>,
    place_barrier: &mut PlaceSendBarrier,
) -> bool {
    let mut barrier_reached = false;
    while let Ok((phase, index, elapsed_us)) = send_entries.try_recv() {
        observed.push(elapsed_us);
        if phase == GridDispatchPhase::Place && place_barrier.observe(index) {
            barrier_reached = true;
        }
    }
    barrier_reached
}

async fn dispatch_grid_child(
    transport: &BinanceHttpTransport,
    index: usize,
    phase: GridDispatchPhase,
    dispatch: BinancePreparedDispatch,
    executor_started: Instant,
    send_entry: tokio::sync::mpsc::UnboundedSender<(GridDispatchPhase, usize, u64)>,
) -> (
    GridDispatchPhase,
    usize,
    Result<BinanceMutationAck, BinanceTransportError>,
) {
    let result = transport
        .dispatch_prepared_once_observed(dispatch, move || {
            let _ = send_entry.send((phase, index, elapsed_us(executor_started)));
        })
        .await;
    (phase, index, result)
}

fn record_phase_timing(
    mut entries: Vec<u64>,
    first: &mut Option<u64>,
    last: &mut Option<u64>,
    attempts: &mut u16,
) {
    entries.sort_unstable();
    for elapsed_us in entries {
        record_outbound_timing(elapsed_us, first, last, attempts);
    }
}

fn record_phase_results(
    results: Vec<(usize, Result<BinanceMutationAck, BinanceTransportError>)>,
    native_ids: &[Option<String>],
    outcomes: &mut [Option<GridBatchCommandOutcome>],
    acknowledgements: &mut [Option<BinanceMutationAck>],
) {
    for (index, result) in results {
        match result {
            Ok(ack) => acknowledgements[index] = Some(ack),
            Err(error) => {
                outcomes[index] = Some(
                    match dispatch_error_outcome(error, native_ids[index].clone()) {
                        Ok(outcome) => GridBatchCommandOutcome::Submitted(outcome),
                        Err(error) => GridBatchCommandOutcome::NotDispatched(error),
                    },
                );
            }
        }
    }
}

pub(super) struct PreparedGridCommand {
    pub(super) mutation: Option<BinancePreparedMutation>,
    pub(super) immediate: Option<ExecutionOutcome>,
    pub(super) native_id: Option<String>,
}

pub(super) fn validate_grid_batch_requests(
    transport: &BinanceHttpTransport,
    requests: &[ExecutionRequest],
) -> Result<(), BinanceExecutionError> {
    validate_grid_batch_shape(requests)?;
    for request in requests {
        validate_request_binding(transport, request)?;
    }
    Ok(())
}

pub(super) fn validate_grid_batch_shape(
    requests: &[ExecutionRequest],
) -> Result<(), BinanceExecutionError> {
    if !(1..=16).contains(&requests.len()) {
        return Err(BinanceExecutionError::Invalid);
    }
    let first = &requests[0];
    let mut command_ids = std::collections::BTreeSet::new();
    let mut client_ids = std::collections::BTreeSet::new();
    let mut cancel_targets = std::collections::BTreeSet::new();
    let mut cancellation_seen = false;
    for request in requests {
        if request.command_id.is_empty()
            || request.client_order_id.is_empty()
            || request.credential_id.is_empty()
            || request.trading_account_id.is_empty()
            || request.credential_id != first.credential_id
            || request.trading_account_id != first.trading_account_id
            || request.symbol != first.symbol
            || !command_ids.insert(request.command_id.as_str())
            || !client_ids.insert(request.client_order_id.as_str())
        {
            return Err(BinanceExecutionError::Invalid);
        }
        match &request.order_kind {
            ExecutionOrderKind::LimitPostOnly { .. } if !cancellation_seen => {}
            ExecutionOrderKind::Market { .. } if requests.len() == 1 && !cancellation_seen => {}
            ExecutionOrderKind::CancelExact {
                native_order_id,
                target_client_order_id,
            } => {
                cancellation_seen = true;
                let target = format!(
                    "{}:{}",
                    native_order_id.as_deref().unwrap_or_default(),
                    target_client_order_id.as_deref().unwrap_or_default()
                );
                if !cancel_targets.insert(target) {
                    return Err(BinanceExecutionError::Invalid);
                }
            }
            // Multi-command market dispatch would make several position mutations one
            // acknowledgement group. Replenishment and profit reduction remain single commands.
            ExecutionOrderKind::Market { .. } | ExecutionOrderKind::LimitPostOnly { .. } => {
                return Err(BinanceExecutionError::Invalid);
            }
        }
    }
    Ok(())
}

fn prepare_hot_grid_batch(
    requests: &[ExecutionRequest],
    fence: &BinanceGridDispatchFence,
) -> Result<Vec<PreparedGridCommand>, BinanceExecutionError> {
    let mut prepared = Vec::with_capacity(requests.len());
    for request in requests {
        match &request.order_kind {
            ExecutionOrderKind::LimitPostOnly {
                side,
                position_side,
                quantity,
                price,
                reducing,
            } => {
                let mutation = fence
                    .prepare_place_limit(&BinancePlaceIntent {
                        client_order_id: request.client_order_id.clone(),
                        side: *side,
                        position_side: *position_side,
                        quantity: *quantity,
                        limit_price: venue_domain::domain::Price::new(*price)
                            .map_err(|_| BinanceExecutionError::Invalid)?,
                        time_in_force: BinanceTimeInForce::PostOnly,
                        reduce_only: *reducing,
                    })
                    .map_err(|_| BinanceExecutionError::Invalid)?;
                prepared.push(PreparedGridCommand {
                    mutation: Some(mutation),
                    immediate: None,
                    native_id: None,
                });
            }
            ExecutionOrderKind::CancelExact {
                native_order_id,
                target_client_order_id,
            } => {
                let native_id = native_order_id
                    .clone()
                    .or_else(|| request.known_native_order_id.clone())
                    .filter(|value| !value.is_empty())
                    .ok_or(BinanceExecutionError::Invalid)?;
                let target = target_client_order_id
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .ok_or(BinanceExecutionError::Invalid)?;
                let mutation = fence
                    .prepare_cancel(&BinanceCancelIntent {
                        client_order_id: target.clone(),
                    })
                    .map_err(|_| BinanceExecutionError::Invalid)?;
                prepared.push(PreparedGridCommand {
                    mutation: Some(mutation),
                    immediate: None,
                    native_id: Some(native_id),
                });
            }
            ExecutionOrderKind::Market { .. } => return Err(BinanceExecutionError::Invalid),
        }
    }
    Ok(prepared)
}

pub(super) fn prepare_grid_batch(
    requests: &[ExecutionRequest],
    rules: &venue_gateway_binance::BinanceInstrumentRules,
    before: &venue_gateway_binance::BinancePrivateReadbackCandidate,
) -> Result<Vec<PreparedGridCommand>, BinanceExecutionError> {
    let mut batch_close_reservations = BTreeMap::<PositionSide, Decimal>::new();
    let mut prepared = Vec::with_capacity(requests.len());
    for request in requests {
        match &request.order_kind {
            ExecutionOrderKind::CancelExact {
                native_order_id,
                target_client_order_id,
            } => {
                let Some((native_id, target_client_id)) = cancel_target(
                    before,
                    native_order_id.as_deref(),
                    target_client_order_id.as_deref(),
                )?
                else {
                    prepared.push(PreparedGridCommand {
                        mutation: None,
                        immediate: Some(outcome(
                            ExecutionReadback::Reconciled,
                            native_order_id.clone(),
                        )),
                        native_id: native_order_id.clone(),
                    });
                    continue;
                };
                prepared.push(PreparedGridCommand {
                    mutation: Some(
                        prepare_cancel(
                            rules,
                            before,
                            &BinanceCancelIntent {
                                client_order_id: target_client_id,
                            },
                        )
                        .map_err(|_| BinanceExecutionError::Invalid)?,
                    ),
                    immediate: None,
                    native_id: Some(native_id),
                });
            }
            ExecutionOrderKind::Market { .. } | ExecutionOrderKind::LimitPostOnly { .. } => {
                let (side, position_side, requested, reducing) = place_shape(request)?;
                let before_position = position_quantity(before, position_side)?;
                let requested = if reducing {
                    let signed_and_persisted =
                        reserved_close_quantity(before, request, position_side, side)?;
                    let same_batch = batch_close_reservations
                        .get(&position_side)
                        .copied()
                        .unwrap_or(Decimal::ZERO);
                    requested.min(
                        before_position
                            .checked_sub(signed_and_persisted)
                            .and_then(|value| value.checked_sub(same_batch))
                            .ok_or(BinanceExecutionError::Invalid)?
                            .max(Decimal::ZERO),
                    )
                } else {
                    requested
                };
                let quantity = normalize_quantity(requested, rules)?;
                if reducing {
                    let prior = batch_close_reservations
                        .get(&position_side)
                        .copied()
                        .unwrap_or(Decimal::ZERO);
                    batch_close_reservations.insert(
                        position_side,
                        prior
                            .checked_add(quantity)
                            .ok_or(BinanceExecutionError::Invalid)?,
                    );
                }
                if opening_minimum_notional_required(reducing) {
                    check_minimum_notional(before, position_side, quantity, rules)?;
                }
                let mutation = match &request.order_kind {
                    ExecutionOrderKind::LimitPostOnly { price, .. } => prepare_place_limit(
                        rules,
                        before,
                        &BinancePlaceIntent {
                            client_order_id: request.client_order_id.clone(),
                            side,
                            position_side,
                            quantity,
                            limit_price: venue_domain::domain::Price::new(*price)
                                .map_err(|_| BinanceExecutionError::Invalid)?,
                            time_in_force: BinanceTimeInForce::PostOnly,
                            reduce_only: reducing,
                        },
                    ),
                    ExecutionOrderKind::Market { .. } => prepare_place_market(
                        rules,
                        before,
                        &BinanceMarketIntent {
                            client_order_id: request.client_order_id.clone(),
                            side,
                            position_side,
                            quantity,
                            reduce_only: reducing,
                        },
                    ),
                    ExecutionOrderKind::CancelExact { .. } => {
                        return Err(BinanceExecutionError::Invalid);
                    }
                }
                .map_err(|_| BinanceExecutionError::Invalid)?;
                prepared.push(PreparedGridCommand {
                    mutation: Some(mutation),
                    immediate: None,
                    native_id: None,
                });
            }
        }
    }
    Ok(prepared)
}

pub(super) fn dispatch_error_outcome(
    error: BinanceTransportError,
    native_id: Option<String>,
) -> Result<ExecutionOutcome, BinanceExecutionError> {
    if error.is_unknown_dispatch() {
        Ok(dispatch_unknown(error, native_id))
    } else {
        dispatch_failed(error, native_id)
    }
}

const fn prepare_dispatch_error(error: BinanceTransportError) -> BinanceExecutionError {
    match error {
        BinanceTransportError::Binding => BinanceExecutionError::Invalid,
        BinanceTransportError::Limits
        | BinanceTransportError::Signing
        | BinanceTransportError::Http
        | BinanceTransportError::Payload
        | BinanceTransportError::Protocol
        | BinanceTransportError::EndOfStream
        | BinanceTransportError::Timeout
        | BinanceTransportError::Disconnected
        | BinanceTransportError::AmbiguousStatus(_)
        | BinanceTransportError::HttpStatus(_)
        | BinanceTransportError::BodyTooLarge
        | BinanceTransportError::Ack
        | BinanceTransportError::TimestampRejected
        | BinanceTransportError::Clock => BinanceExecutionError::Unavailable,
    }
}

pub(super) fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

pub(super) fn record_outbound_timing(
    elapsed_us: u64,
    first: &mut Option<u64>,
    last: &mut Option<u64>,
    attempts: &mut u16,
) {
    if first.is_none() {
        *first = Some(elapsed_us);
    }
    *last = Some(elapsed_us);
    *attempts = attempts.saturating_add(1);
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Clone, Copy)]
    struct FakeDispatch {
        enter_send: bool,
        wait_for_release: bool,
    }

    #[derive(Default)]
    struct FakeObservations {
        place_entries: AtomicUsize,
        cancel_entries: AtomicUsize,
        changed: tokio::sync::Notify,
    }

    async fn fake_dispatch_child(
        index: usize,
        phase: GridDispatchPhase,
        dispatch: FakeDispatch,
        started: Instant,
        send_entry: tokio::sync::mpsc::UnboundedSender<(GridDispatchPhase, usize, u64)>,
        observations: Arc<FakeObservations>,
        mut release: tokio::sync::watch::Receiver<bool>,
    ) -> (
        GridDispatchPhase,
        usize,
        Result<BinanceMutationAck, BinanceTransportError>,
    ) {
        if dispatch.enter_send {
            let _ = send_entry.send((phase, index, elapsed_us(started)));
            match phase {
                GridDispatchPhase::Place => {
                    observations.place_entries.fetch_add(1, Ordering::SeqCst);
                }
                GridDispatchPhase::Cancel => {
                    observations.cancel_entries.fetch_add(1, Ordering::SeqCst);
                }
            }
            observations.changed.notify_one();
        }
        while dispatch.wait_for_release && !*release.borrow() {
            if release.changed().await.is_err() {
                break;
            }
        }
        let error = if dispatch.enter_send {
            BinanceTransportError::Timeout
        } else {
            BinanceTransportError::Binding
        };
        (phase, index, Err(error))
    }

    async fn wait_for_entries(
        observations: &FakeObservations,
        counter: &AtomicUsize,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(std::time::Duration::from_millis(100), async {
            while counter.load(Ordering::SeqCst) < expected {
                observations.changed.notified().await;
            }
        })
        .await?;
        Ok(())
    }

    #[test]
    fn cancel_phase_opens_only_after_every_unique_place_reaches_send_entry() {
        let mut barrier = PlaceSendBarrier::new(4);
        assert!(!barrier.observe(2));
        assert!(!barrier.observe(0));
        assert!(
            !barrier.observe(2),
            "a duplicate cannot satisfy the barrier"
        );
        assert!(!barrier.observe(3));
        assert!(barrier.observe(1));
    }

    #[test]
    fn presend_place_failure_permanently_keeps_cancel_phase_closed() {
        let mut barrier = PlaceSendBarrier::new(2);
        assert!(!barrier.observe(0));
        barrier.complete(1);
        assert!(!barrier.observe(1));
    }

    #[test]
    fn same_poll_send_callback_is_drained_before_place_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let (send_entry, mut send_entries) = tokio::sync::mpsc::unbounded_channel();
        send_entry.send((GridDispatchPhase::Place, 0, 11))?;
        let mut observed = Vec::new();
        let mut barrier = PlaceSendBarrier::new(1);

        let release_cancel = drain_send_entries(&mut send_entries, &mut observed, &mut barrier);
        barrier.complete(0);

        assert!(release_cancel);
        assert_eq!(observed, vec![11]);
        assert!(!barrier.failed);
        Ok(())
    }

    #[test]
    fn send_entry_timing_uses_earliest_and_latest_observer_samples() {
        let mut first = None;
        let mut last = None;
        let mut attempts = 0;
        record_phase_timing(vec![19, 7, 12], &mut first, &mut last, &mut attempts);
        assert_eq!(first, Some(7));
        assert_eq!(last, Some(19));
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn cancel_reaches_send_entry_before_blocked_place_responses_are_released()
    -> Result<(), Box<dyn std::error::Error>> {
        let observations = Arc::new(FakeObservations::default());
        let driver_observations = Arc::clone(&observations);
        let (release, release_rx) = tokio::sync::watch::channel(false);
        let driver = tokio::spawn(async move {
            dispatch_grid_burst_with(
                vec![
                    (
                        0,
                        FakeDispatch {
                            enter_send: true,
                            wait_for_release: true,
                        },
                    ),
                    (
                        1,
                        FakeDispatch {
                            enter_send: true,
                            wait_for_release: true,
                        },
                    ),
                ],
                vec![(
                    2,
                    FakeDispatch {
                        enter_send: true,
                        wait_for_release: true,
                    },
                )],
                Instant::now(),
                move |index, phase, dispatch, started, send_entry| {
                    fake_dispatch_child(
                        index,
                        phase,
                        dispatch,
                        started,
                        send_entry,
                        Arc::clone(&driver_observations),
                        release_rx.clone(),
                    )
                },
            )
            .await
        });

        wait_for_entries(&observations, &observations.place_entries, 2).await?;
        wait_for_entries(&observations, &observations.cancel_entries, 1).await?;
        assert_eq!(observations.place_entries.load(Ordering::SeqCst), 2);
        assert_eq!(observations.cancel_entries.load(Ordering::SeqCst), 1);
        release.send(true)?;
        let burst = driver.await?;
        assert_eq!(burst.results.len(), 3);
        assert!(burst.not_dispatched_cancels.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn presend_place_failure_never_starts_cancel_transport()
    -> Result<(), Box<dyn std::error::Error>> {
        let observations = Arc::new(FakeObservations::default());
        let driver_observations = Arc::clone(&observations);
        let (_, release_rx) = tokio::sync::watch::channel(true);
        let burst = dispatch_grid_burst_with(
            vec![
                (
                    0,
                    FakeDispatch {
                        enter_send: true,
                        wait_for_release: false,
                    },
                ),
                (
                    1,
                    FakeDispatch {
                        enter_send: false,
                        wait_for_release: false,
                    },
                ),
            ],
            vec![(
                2,
                FakeDispatch {
                    enter_send: true,
                    wait_for_release: false,
                },
            )],
            Instant::now(),
            move |index, phase, dispatch, started, send_entry| {
                fake_dispatch_child(
                    index,
                    phase,
                    dispatch,
                    started,
                    send_entry,
                    Arc::clone(&driver_observations),
                    release_rx.clone(),
                )
            },
        )
        .await;
        assert_eq!(observations.cancel_entries.load(Ordering::SeqCst), 0);
        assert_eq!(burst.not_dispatched_cancels, vec![2]);
        Ok(())
    }

    #[tokio::test]
    async fn post_send_timeout_still_releases_cancel_after_place_barrier()
    -> Result<(), Box<dyn std::error::Error>> {
        let observations = Arc::new(FakeObservations::default());
        let driver_observations = Arc::clone(&observations);
        let (_, release_rx) = tokio::sync::watch::channel(true);
        let entered = FakeDispatch {
            enter_send: true,
            wait_for_release: false,
        };
        let burst = dispatch_grid_burst_with(
            vec![(0, entered), (1, entered)],
            vec![(2, entered)],
            Instant::now(),
            move |index, phase, dispatch, started, send_entry| {
                fake_dispatch_child(
                    index,
                    phase,
                    dispatch,
                    started,
                    send_entry,
                    Arc::clone(&driver_observations),
                    release_rx.clone(),
                )
            },
        )
        .await;
        assert_eq!(observations.place_entries.load(Ordering::SeqCst), 2);
        assert_eq!(observations.cancel_entries.load(Ordering::SeqCst), 1);
        assert_eq!(burst.send_entries_us.len(), 3);
        assert!(burst.not_dispatched_cancels.is_empty());
        Ok(())
    }
}
