use super::*;
use crate::OkxHttpResponse;
use crate::algo_execution::{
    OkxAlgoState, build_algo_cancel_request, build_algo_lookup_request, build_algo_place_request,
    build_exact_regular_cancel_request, parse_algo_cancel_ack, parse_algo_detail,
    parse_algo_place_ack, parse_exact_regular_cancel_ack,
};
use crate::execution::{OkxExecutionScope, OkxPrivateRequest};

impl OkxAccountGateway {
    pub fn durable_order_observation(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<Option<venue_execution::DurableOrderObservation>, OkxAccountGatewayError> {
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Ok(None);
        };
        if !venue_execution::validate_durable_command(self.config.gateway_binding(), command) {
            return Ok(None);
        }
        self.refresh_instrument()?;
        let submitted = build_place_request(
            &self.config,
            &self.instrument,
            &self.profile,
            self.trade_mode,
            OkxPlaceIntent::Limit(order),
        )
        .map_err(|_| OkxAccountGatewayError::Readback)?;
        let request = crate::execution::build_unknown_order_readback_request(
            &self.config,
            &self.instrument,
            &self.profile,
            &submitted,
        )
        .map_err(|_| OkxAccountGatewayError::Readback)?;
        let timestamp =
            okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
        let response = self
            .runtime
            .block_on(
                self.transport
                    .execute(&self.credentials, &request, &timestamp),
            )
            .map_err(|_| OkxAccountGatewayError::Readback)?;
        let observed = crate::execution::parse_unknown_order_readback(response, &request)
            .map_err(|_| OkxAccountGatewayError::Readback)?
            .order;
        if !matches!(
            reconcile_okx_order(command, &observed),
            AccountGatewayResult::Accepted { .. }
        ) {
            return Ok(None);
        }
        Ok(Some(venue_execution::DurableOrderObservation {
            client_order_id: order.client_order_id.as_str().to_owned(),
            native_order_id: observed.order.order_id,
            state: observed.order.state,
            filled_quantity: observed.order.filled_quantity,
        }))
    }
    /// Returns a fresh public reference and the exact parsed instrument generation.  Bounds are
    /// omitted when OKX does not publish one rather than being guessed by the adapter.
    pub fn durable_market_facts(
        &mut self,
    ) -> Result<venue_execution::DurableMarketFacts, OkxAccountGatewayError> {
        self.refresh_instrument()?;
        let bbo = self.current_market_bbo()?;
        let reference_price =
            Price::new((bbo.bid.value() + bbo.ask.value()) / rust_decimal::Decimal::from(2))
                .map_err(|_| OkxAccountGatewayError::Instrument)?;
        let metadata = venue_domain::domain::InstrumentMetadata::new(
            self.instrument.instrument().clone(),
            venue_domain::domain::Precision::new(
                self.instrument.instrument().price_tick.value(),
                self.instrument.instrument().price_tick.value(),
            )
            .map_err(|_| OkxAccountGatewayError::Instrument)?,
            venue_domain::domain::Precision::new(
                self.instrument.instrument().quantity_step,
                self.instrument.minimum_base_quantity(),
            )
            .map_err(|_| OkxAccountGatewayError::Instrument)?,
            None,
            true,
        )
        .map_err(|_| OkxAccountGatewayError::Instrument)?;
        Ok(venue_execution::DurableMarketFacts {
            binding: self.config.gateway_binding().clone(),
            metadata,
            reference_price,
            observed_at_ms: unix_ms()?,
            maximum_quantity: self
                .instrument
                .maximum_limit_contracts()
                .and_then(|lots| lots.checked_mul(self.instrument.base_quantity_per_contract())),
            maximum_price: None,
        })
    }
}

impl OkxAccountGateway {
    /// The legacy profile keeps the one-position canary admission guard. Durable commands use
    /// the same native builders and permissions while allowing an already-active strategy.
    pub(super) fn execute_command(
        &mut self,
        command: &ExecutionCommand,
        legacy_profile: bool,
    ) -> AccountGatewayResult {
        if self.refresh_instrument().is_err() || self.refresh_private().is_err() {
            return rejected("okx_preflight_failed");
        }
        if !self.profile.can_read() || !self.profile.can_trade() || self.profile.can_withdraw() {
            return rejected("okx_permissions");
        }
        let timestamp = match okx_timestamp(SystemTime::now()) {
            Ok(value) => value,
            Err(_) => return rejected("okx_clock"),
        };
        match command {
            ExecutionCommand::PlaceLimit(command) => {
                if !legacy_profile
                    && command.reduce_only
                    && validate_limit_reduce_position(command, &self.positions).is_err()
                {
                    return rejected("okx_limit_reduce_position");
                }
                if legacy_profile
                    && !command.reduce_only
                    && self
                        .positions
                        .iter()
                        .any(|position| !position.position.quantity.is_zero())
                {
                    return rejected("okx_existing_position");
                }
                let request = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::Limit(command),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_place_ack(response.clone(), &request) {
                        Ok(accepted) => self.settle_accepted_place(&accepted, "okx_limit_rejected"),
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::PlaceMarket(command) => {
                let request = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::Market(command),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_market_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_place_ack(response.clone(), &request) {
                        Ok(accepted) => {
                            self.settle_accepted_place(&accepted, "okx_market_rejected")
                        }
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::Cancel(command) => {
                let request = match build_host_cancel_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    command,
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_cancel_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_host_cancel_ack(response.clone(), &request) {
                        Ok(venue_order_id) => AccountGatewayResult::Accepted { venue_order_id },
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::MarketReduce(command) => {
                if validate_market_reduce_position(command, &self.positions).is_err() {
                    return rejected("okx_market_reduce_position");
                }
                let request = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::MarketReduce(command),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_market_reduce_rules"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_place_ack(response.clone(), &request) {
                        Ok(accepted) => {
                            self.settle_accepted_place(&accepted, "okx_market_reduce_rejected")
                        }
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::StopMarketFullPosition(command) => {
                if validate_stop_position(command, &self.positions).is_err() {
                    return rejected("okx_stop_position");
                }
                let request = match build_algo_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    command,
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_stop_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_algo_place_ack(response.clone(), &request) {
                        Ok(venue_order_id) => AccountGatewayResult::Accepted { venue_order_id },
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::StopMarketCloseAll(_) => {
                rejected("okx_initial_profile_unsupported_command")
            }
        }
    }

    pub(super) fn reconcile_order(&mut self, command: &ExecutionCommand) -> AccountGatewayResult {
        // Reconciliation is a read-only historical path. It must remain usable after a key is
        // revoked for trading, and a successful reduction may have already made the position
        // zero; neither current permission nor inventory is evidence about the original order.
        if self.refresh_instrument().is_err() {
            return AccountGatewayResult::Unknown;
        }
        match command {
            ExecutionCommand::PlaceLimit(order) => {
                let submitted = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::Limit(order),
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let request = match crate::execution::build_unknown_order_readback_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    &submitted,
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let timestamp = match okx_timestamp(SystemTime::now()) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let response = match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let order = match crate::execution::parse_unknown_order_readback(response, &request)
                {
                    Ok(value) => value.order,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                reconcile_okx_order(command, &order)
            }
            ExecutionCommand::MarketReduce(order) => {
                let submitted = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::MarketReduce(order),
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let request = match crate::execution::build_unknown_order_readback_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    &submitted,
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let timestamp = match okx_timestamp(SystemTime::now()) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let response = match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let order = match crate::execution::parse_unknown_order_readback(response, &request)
                {
                    Ok(value) => value.order,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                reconcile_okx_order(command, &order)
            }
            ExecutionCommand::PlaceMarket(order) => {
                let submitted = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::Market(order),
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let request = match crate::execution::build_unknown_order_readback_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    &submitted,
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let timestamp = match okx_timestamp(SystemTime::now()) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let response = match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let order = match crate::execution::parse_unknown_order_readback(response, &request)
                {
                    Ok(value) => value.order,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                reconcile_okx_order(command, &order)
            }
            ExecutionCommand::Cancel(cancel) => {
                let request = match build_host_order_lookup_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    cancel.target_client_order_id.as_str(),
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let timestamp = match okx_timestamp(SystemTime::now()) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let response = match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let order =
                    match crate::execution::parse_host_order_lookup_detail(response, &request) {
                        Ok(Some(value)) => value,
                        Ok(None) => return AccountGatewayResult::Unknown,
                        Err(_) => return AccountGatewayResult::Unknown,
                    };
                reconcile_okx_order(command, &order)
            }
            ExecutionCommand::StopMarketFullPosition(order) => {
                let submitted = match build_algo_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    order,
                ) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let request = match build_algo_lookup_request(&submitted, None) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let timestamp = match okx_timestamp(SystemTime::now()) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let response = match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                match parse_algo_detail(response, &request) {
                    Ok(Some(detail)) => settle_algo_detail(detail),
                    _ => AccountGatewayResult::Unknown,
                }
            }
            _ => AccountGatewayResult::Unknown,
        }
    }
}

fn validate_stop_position(
    command: &venue_domain::domain::StopMarketFullPositionCommand,
    positions: &[OkxTimedPosition],
) -> Result<(), OkxError> {
    command.validate().map_err(|_| OkxError::Payload)?;
    let position = positions
        .iter()
        .find(|candidate| {
            candidate.position.symbol == command.owner.symbol
                && candidate.position.side == command.position_side
        })
        .ok_or(OkxError::PositionMode)?;
    command
        .validate_with_authoritative_position(&position.position)
        .map_err(|_| OkxError::PositionMode)?;
    if command.quantity > position.position.quantity {
        return Err(OkxError::PositionMode);
    }
    let mark = position.position.mark_price.ok_or(OkxError::Payload)?;
    let valid = match (command.owner.purpose, command.position_side) {
        (venue_domain::domain::OrderPurpose::Protection, PositionSide::Long)
        | (venue_domain::domain::OrderPurpose::TakeProfit, PositionSide::Short) => {
            command.trigger_price < mark
        }
        (venue_domain::domain::OrderPurpose::Protection, PositionSide::Short)
        | (venue_domain::domain::OrderPurpose::TakeProfit, PositionSide::Long) => {
            command.trigger_price > mark
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(OkxError::Payload)
    }
}

fn settle_algo_detail(detail: crate::algo_execution::OkxAlgoDetail) -> AccountGatewayResult {
    match detail.state {
        OkxAlgoState::Working | OkxAlgoState::Triggered { .. } | OkxAlgoState::Cancelled => {
            AccountGatewayResult::Accepted {
                venue_order_id: detail.algo_id,
            }
        }
        OkxAlgoState::Failed => rejected("okx_algo_rejected"),
    }
}

impl OkxAccountGateway {
    fn exact_cancel_target<'a>(
        &self,
        context: &'a venue_execution::DurableExecutionContext,
    ) -> Option<(&'a ExecutionCommand, &'a str)> {
        let target = context.target_command.as_ref()?;
        if !venue_execution::validate_durable_command(self.config.gateway_binding(), target)
            || matches!(
                target,
                ExecutionCommand::Cancel(_) | ExecutionCommand::StopMarketCloseAll(_)
            )
        {
            return None;
        }
        let native_id = context.target_native_order_id.as_deref()?.trim();
        if native_id.is_empty() {
            return None;
        }
        Some((target, native_id))
    }

    pub(super) fn execute_exact_cancel(
        &mut self,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if self.refresh_instrument().is_err()
            || !self.profile.can_read()
            || !self.profile.can_trade()
            || self.profile.can_withdraw()
        {
            return rejected("okx_cancel_preflight");
        }
        let (target, native_id) = match self.exact_cancel_target(context) {
            Some((target, native_id)) => (target.clone(), native_id.to_owned()),
            None => return rejected("okx_cancel_target_context"),
        };
        match &target {
            ExecutionCommand::StopMarketFullPosition(stop) => {
                self.execute_exact_algo_cancel(stop, &native_id)
            }
            _ => self.execute_exact_regular_cancel(&target, &native_id),
        }
    }

    pub(super) fn reconcile_exact_cancel(
        &mut self,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if self.refresh_instrument().is_err() {
            return AccountGatewayResult::Unknown;
        }
        let (target, native_id) = match self.exact_cancel_target(context) {
            Some((target, native_id)) => (target.clone(), native_id.to_owned()),
            None => return AccountGatewayResult::Unknown,
        };
        match &target {
            ExecutionCommand::StopMarketFullPosition(stop) => {
                let request = match build_algo_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    stop,
                )
                .and_then(|submitted| build_algo_lookup_request(&submitted, Some(&native_id)))
                {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let response = match self.execute_private_read(&request) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                match parse_algo_detail(response, &request) {
                    Ok(Some(detail))
                        if matches!(
                            detail.state,
                            OkxAlgoState::Triggered { .. }
                                | OkxAlgoState::Cancelled
                                | OkxAlgoState::Failed
                        ) =>
                    {
                        AccountGatewayResult::Accepted {
                            venue_order_id: native_id,
                        }
                    }
                    _ => AccountGatewayResult::Unknown,
                }
            }
            _ => match self.read_exact_regular_target(&target, &native_id) {
                Ok(order) if is_terminal(order.order.state) => AccountGatewayResult::Accepted {
                    venue_order_id: native_id,
                },
                _ => AccountGatewayResult::Unknown,
            },
        }
    }

    fn execute_exact_algo_cancel(
        &mut self,
        stop: &venue_domain::domain::StopMarketFullPositionCommand,
        native_id: &str,
    ) -> AccountGatewayResult {
        let submitted = match build_algo_place_request(
            &self.config,
            &self.instrument,
            &self.profile,
            self.trade_mode,
            stop,
        ) {
            Ok(value) => value,
            Err(_) => return rejected("okx_algo_cancel_target"),
        };
        let lookup = match build_algo_lookup_request(&submitted, Some(native_id)) {
            Ok(value) => value,
            Err(_) => return rejected("okx_algo_cancel_identity"),
        };
        let response = match self.execute_private_read(&lookup) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let detail = match parse_algo_detail(response, &lookup) {
            Ok(Some(value)) => value,
            _ => return AccountGatewayResult::Unknown,
        };
        match detail.state {
            OkxAlgoState::Triggered { .. } | OkxAlgoState::Cancelled | OkxAlgoState::Failed => {
                return AccountGatewayResult::Accepted {
                    venue_order_id: native_id.to_owned(),
                };
            }
            OkxAlgoState::Working => {}
        }
        let request = match build_algo_cancel_request(&lookup, native_id) {
            Ok(value) => value,
            Err(_) => return rejected("okx_algo_cancel_intent"),
        };
        self.dispatch_exact_cancel(&request, |response| {
            parse_algo_cancel_ack(response, &request)
        })
    }

    fn execute_exact_regular_cancel(
        &mut self,
        target: &ExecutionCommand,
        native_id: &str,
    ) -> AccountGatewayResult {
        let order = match self.read_exact_regular_target(target, native_id) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        if is_terminal(order.order.state) {
            return AccountGatewayResult::Accepted {
                venue_order_id: native_id.to_owned(),
            };
        }
        let scope = match OkxExecutionScope::new(
            &self.config,
            &self.instrument,
            &self.profile,
            self.trade_mode,
        ) {
            Ok(value) => value,
            Err(_) => return rejected("okx_cancel_scope"),
        };
        let request = match build_exact_regular_cancel_request(&scope, native_id) {
            Ok(value) => value,
            Err(_) => return rejected("okx_cancel_identity"),
        };
        self.dispatch_exact_cancel(&request, |response| {
            parse_exact_regular_cancel_ack(response, &request)
        })
    }

    fn read_exact_regular_target(
        &mut self,
        target: &ExecutionCommand,
        native_id: &str,
    ) -> Result<OkxTimedOrder, OkxAccountGatewayError> {
        let client_id = target_client_identity(target).ok_or(OkxAccountGatewayError::Readback)?;
        let request = build_host_order_lookup_request(
            &self.config,
            &self.instrument,
            &self.profile,
            self.trade_mode,
            client_id,
        )
        .map_err(|_| OkxAccountGatewayError::Readback)?;
        let response = self.execute_private_read(&request)?;
        let order = crate::execution::parse_host_order_lookup_detail(response, &request)
            .map_err(|_| OkxAccountGatewayError::Readback)?
            .ok_or(OkxAccountGatewayError::Readback)?;
        if order.order.order_id != native_id || !regular_target_matches(target, &order) {
            return Err(OkxAccountGatewayError::Readback);
        }
        Ok(order)
    }

    fn execute_private_read<R: OkxPrivateRequest>(
        &mut self,
        request: &R,
    ) -> Result<OkxHttpResponse, OkxAccountGatewayError> {
        let timestamp =
            okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
        self.runtime
            .block_on(
                self.transport
                    .execute(&self.credentials, request, &timestamp),
            )
            .map_err(OkxAccountGatewayError::Transport)
    }

    fn dispatch_exact_cancel<R: OkxPrivateRequest>(
        &mut self,
        request: &R,
        parse: impl FnOnce(OkxHttpResponse) -> Result<String, OkxError>,
    ) -> AccountGatewayResult {
        let timestamp = match okx_timestamp(SystemTime::now()) {
            Ok(value) => value,
            Err(_) => return rejected("okx_clock"),
        };
        match self.runtime.block_on(
            self.transport
                .execute(&self.credentials, request, &timestamp),
        ) {
            Ok(response) => match parse(response.clone()) {
                Ok(venue_order_id) => AccountGatewayResult::Accepted { venue_order_id },
                Err(OkxError::Rejected) => rejected_response(&response.body),
                Err(_) => AccountGatewayResult::Unknown,
            },
            Err(error) => map_transport_dispatch(error),
        }
    }
}

fn target_client_identity(command: &ExecutionCommand) -> Option<&str> {
    match command {
        ExecutionCommand::PlaceLimit(value) => Some(value.client_order_id.as_str()),
        ExecutionCommand::PlaceMarket(value) => Some(value.client_order_id.as_str()),
        ExecutionCommand::MarketReduce(value) => Some(value.client_order_id.as_str()),
        _ => None,
    }
}

fn is_terminal(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    )
}

fn regular_target_matches(command: &ExecutionCommand, observed: &OkxTimedOrder) -> bool {
    let order = &observed.order;
    match command {
        ExecutionCommand::PlaceLimit(command) => {
            order.client_order_id == FieldState::Known(command.client_order_id.as_str().to_owned())
                && order.symbol == command.owner.symbol
                && order.side == command.side
                && order.position_side == FieldState::Known(command.position_side)
                && order.quantity == command.quantity
                && order.limit_price == Some(command.limit_price)
                && order.time_in_force == FieldState::Known(command.time_in_force)
                && order.reduce_only == command.reduce_only
        }
        ExecutionCommand::PlaceMarket(command) => {
            order.client_order_id == FieldState::Known(command.client_order_id.as_str().to_owned())
                && order.symbol == command.owner.symbol
                && order.side == command.side
                && order.position_side == FieldState::Known(command.position_side)
                && order.quantity == command.quantity
                && order.limit_price.is_none()
                && !order.reduce_only
        }
        ExecutionCommand::MarketReduce(command) => {
            order.client_order_id == FieldState::Known(command.client_order_id.as_str().to_owned())
                && order.symbol == command.owner.symbol
                && order.side == command.side
                && order.position_side == FieldState::Known(command.position_side)
                && order.quantity == command.quantity
                && order.limit_price.is_none()
                && !order.reduce_only
        }
        _ => false,
    }
}

pub(super) fn reconcile_okx_order(
    command: &ExecutionCommand,
    observed: &OkxTimedOrder,
) -> AccountGatewayResult {
    let order = &observed.order;
    let matches = match command {
        ExecutionCommand::PlaceLimit(command) => {
            order.client_order_id == FieldState::Known(command.client_order_id.as_str().to_owned())
                && order.symbol == command.owner.symbol
                && order.side == command.side
                && order.position_side == FieldState::Known(command.position_side)
                && order.quantity == command.quantity
                && order.limit_price == Some(command.limit_price)
                && order.time_in_force == FieldState::Known(command.time_in_force)
                && order.reduce_only == command.reduce_only
        }
        ExecutionCommand::MarketReduce(command) => {
            order.client_order_id == FieldState::Known(command.client_order_id.as_str().to_owned())
                && order.symbol == command.owner.symbol
                && order.side == command.side
                && order.position_side == FieldState::Known(command.position_side)
                && order.quantity == command.quantity
                && order.limit_price.is_none()
                && order.reduce_only
        }
        ExecutionCommand::PlaceMarket(command) => {
            order.client_order_id == FieldState::Known(command.client_order_id.as_str().to_owned())
                && order.symbol == command.owner.symbol
                && order.side == command.side
                && order.position_side == FieldState::Known(command.position_side)
                && order.quantity == command.quantity
                && order.limit_price.is_none()
                && !order.reduce_only
        }
        ExecutionCommand::Cancel(command) => {
            order.client_order_id
                == FieldState::Known(command.target_client_order_id.as_str().to_owned())
                && order.symbol == command.owner.symbol
        }
        _ => false,
    };
    if !matches {
        return AccountGatewayResult::Unknown;
    }
    match command {
        ExecutionCommand::MarketReduce(_) => match order.state {
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired => {
                AccountGatewayResult::Accepted {
                    venue_order_id: order.order_id.clone(),
                }
            }
            OrderState::Rejected => rejected("okx_order_rejected"),
            OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown => {
                AccountGatewayResult::Unknown
            }
        },
        ExecutionCommand::Cancel(_) => match order.state {
            // A cancel racing an exchange fill has converged to the requested terminal state.
            OrderState::Cancelled | OrderState::Filled | OrderState::Expired => {
                AccountGatewayResult::Accepted {
                    venue_order_id: order.order_id.clone(),
                }
            }
            OrderState::Rejected => rejected("okx_target_terminal_without_cancel"),
            OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown => {
                AccountGatewayResult::Unknown
            }
        },
        ExecutionCommand::PlaceLimit(_) => match order.state {
            OrderState::Rejected => rejected("okx_order_rejected"),
            OrderState::New
            | OrderState::PartiallyFilled
            | OrderState::Filled
            | OrderState::Cancelled
            | OrderState::Expired => AccountGatewayResult::Accepted {
                venue_order_id: order.order_id.clone(),
            },
            OrderState::Unknown => AccountGatewayResult::Unknown,
        },
        _ => AccountGatewayResult::Unknown,
    }
}
