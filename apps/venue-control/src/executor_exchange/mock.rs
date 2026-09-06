use super::*;

#[derive(Clone, Default)]
pub struct MockBinanceExecution {
    pub(super) orders: BTreeMap<String, ExecutionOutcome>,
    pub(super) baselines: BTreeMap<String, AccountBaseline>,
    pub(super) grid_batch_failure: Option<GridBatchSubmitError>,
    pub(super) grid_batch_dispatch_started: Arc<AtomicBool>,
    pub(super) market_positions: Arc<Mutex<BTreeMap<String, Decimal>>>,
}

impl MockBinanceExecution {
    pub fn set_rejection(&mut self, client_order_id: String, code: i64) {
        let mut result = outcome(ExecutionReadback::Rejected, None);
        result.exchange_error_code = Some(code);
        self.orders.insert(client_order_id, result);
    }

    pub fn set_readback(&mut self, client_order_id: String, state: ExecutionReadback) {
        let native_order_id = matches!(
            state,
            ExecutionReadback::Accepted | ExecutionReadback::Reconciled
        )
        .then(|| format!("mock-{client_order_id}"));
        self.orders
            .insert(client_order_id, outcome(state, native_order_id));
    }

    pub fn set_baseline(&mut self, trading_account_id: String, baseline: AccountBaseline) {
        self.baselines.insert(trading_account_id, baseline);
    }

    pub fn set_grid_batch_failure(&mut self, failure: GridBatchSubmitError) {
        self.grid_batch_failure = Some(failure);
    }

    #[must_use]
    pub fn grid_batch_dispatch_started(&self) -> bool {
        self.grid_batch_dispatch_started.load(Ordering::Acquire)
    }
}

impl BinanceExecution for MockBinanceExecution {
    fn prepare_market<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _: &'a BinanceCredentials,
    ) -> MarketPreparationFuture<'a> {
        Box::pin(async move { self.mock_market_baseline(request) })
    }

    fn submit<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async move {
            if request.client_order_id.is_empty()
                || request.command_id.is_empty()
                || request.trading_account_id.is_empty()
            {
                return Err(BinanceExecutionError::Invalid);
            }
            let default_native = mock_native_order_id(request);
            let result = self
                .orders
                .entry(request.client_order_id.clone())
                .or_insert_with(|| outcome(ExecutionReadback::Accepted, Some(default_native)))
                .clone();
            self.mock_market_settlement(request, result)
        })
    }

    fn readback<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async move {
            let result = self
                .orders
                .get(&request.client_order_id)
                .cloned()
                .ok_or(BinanceExecutionError::Unavailable)?;
            self.mock_market_settlement(request, result)
        })
    }

    fn submit_grid_batch<'a>(
        &'a mut self,
        _context: &'a GridBatchExecutionContext,
        requests: &'a [ExecutionRequest],
        _credentials: BinanceCredentials,
    ) -> BinanceGridBatchFuture<'a> {
        Box::pin(async move {
            validate_grid_batch_shape(requests)
                .map_err(GridBatchSubmitError::DefinitelyNotDispatched)?;
            if let Some(failure) = self.grid_batch_failure.take() {
                if failure == GridBatchSubmitError::DispatchUncertain {
                    self.grid_batch_dispatch_started
                        .store(true, Ordering::Release);
                }
                return Err(failure);
            }
            if requests.iter().any(|request| {
                request.command_id.is_empty()
                    || request.client_order_id.is_empty()
                    || request.trading_account_id.is_empty()
            }) {
                return Err(GridBatchSubmitError::DefinitelyNotDispatched(
                    BinanceExecutionError::Invalid,
                ));
            }
            self.grid_batch_dispatch_started
                .store(true, Ordering::Release);
            let started = Instant::now();
            let mut commands = Vec::with_capacity(requests.len());
            let mut first = None;
            let mut last = None;
            let mut attempts = 0_u16;
            for request in requests {
                let submit_us = elapsed_us(started);
                let native = mock_native_order_id(request);
                let result = self
                    .orders
                    .entry(request.client_order_id.clone())
                    .or_insert_with(|| outcome(ExecutionReadback::Accepted, Some(native)))
                    .clone();
                record_outbound_timing(submit_us, &mut first, &mut last, &mut attempts);
                commands.push(GridBatchCommandOutcome::Submitted(result));
            }
            Ok(GridBatchExecutionOutcome {
                commands,
                timing: GridBatchSubmitTiming {
                    executor_start_to_first_submit_us: first,
                    executor_start_to_last_submit_us: last,
                    first_to_last_submit_us: first
                        .zip(last)
                        .map(|(first, last)| last.saturating_sub(first)),
                    outbound_attempts: attempts,
                },
            })
        })
    }
}

fn mock_native_order_id(request: &ExecutionRequest) -> String {
    match &request.order_kind {
        ExecutionOrderKind::CancelExact {
            native_order_id, ..
        }
        | ExecutionOrderKind::CancelAlgoExact {
            native_order_id, ..
        } => native_order_id
            .clone()
            .or_else(|| request.known_native_order_id.clone())
            .unwrap_or_else(|| format!("mock-target-{}", request.client_order_id)),
        ExecutionOrderKind::Market { .. }
        | ExecutionOrderKind::Limit { .. }
        | ExecutionOrderKind::StopMarket { .. } => format!("mock-{}", request.client_order_id),
    }
}

impl BinanceActivationBaseline for MockBinanceExecution {
    async fn activation_baseline(
        &mut self,
        trading_account_id: &str,
        _symbols: &std::collections::BTreeSet<Symbol>,
        _credentials: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        if trading_account_id.is_empty() {
            return Err(BinanceExecutionError::Invalid);
        }
        self.baselines
            .get(trading_account_id)
            .cloned()
            .ok_or(BinanceExecutionError::Unavailable)
    }
}
