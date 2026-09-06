use super::*;

use venue_domain::domain::{Amount, Instrument, InstrumentMetadata, Precision, Symbol};
use venue_execution::{
    DurableExecutionContext, DurableMarketFacts, DurableOrderObservation, validate_durable_context,
};

use crate::action::{
    HyperliquidTriggerMarketOrder, build_ioc_market_request, build_trigger_market_request,
};

const TRIGGER_MARKET_SLIPPAGE_BPS: u64 = 1_000;

fn ioc_entry_price(
    side: OrderSide,
    bbo: &HyperliquidBbo,
    meta: &HyperliquidPerpMeta,
) -> Result<Decimal, ()> {
    // A market entry is bounded by the same contemporaneous BBO guard as reductions; the IOC
    // price crosses the spread by a fixed amount and is rounded in the executable direction.
    ioc_reduce_price(side, bbo, meta)
}

impl HyperliquidAccountGateway {
    /// Reads one exact client identity and returns its cumulative fill observation. Absence is
    /// deliberately inconclusive: callers retain the durable command for later reconciliation.
    pub fn durable_order_observation(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<Option<DurableOrderObservation>, HyperliquidAccountGatewayError> {
        let expected_client_order_id = match command {
            ExecutionCommand::PlaceLimit(order) => order.client_order_id.as_str(),
            ExecutionCommand::PlaceMarket(order) => order.client_order_id.as_str(),
            ExecutionCommand::MarketReduce(order) => order.client_order_id.as_str(),
            _ => return Ok(None),
        };
        if !validate_durable_command(self.binding.gateway().gateway_binding(), command) {
            return Err(HyperliquidAccountGatewayError::Binding);
        }
        let lookup =
            HyperliquidOrderLookup::client_order_id(command_cloid(expected_client_order_id))
                .map_err(|_| HyperliquidAccountGatewayError::Readback)?;
        let status = self.order_status(&lookup)?;
        let HyperliquidOrderStatus::Known {
            order_id,
            client_order_id: observed_client_order_id,
            side,
            limit_price,
            original_quantity,
            remaining_quantity,
            reduce_only,
            native_order_type,
            time_in_force,
            state,
            trigger_price,
            is_position_tpsl,
            ..
        } = status
        else {
            return Ok(None);
        };
        let common_matches = matches!(observed_client_order_id, FieldState::Known(ref value) if value.eq_ignore_ascii_case(&command_cloid(expected_client_order_id)))
            && native_order_type == "Limit"
            && trigger_price.is_none()
            && !is_position_tpsl;
        let command_matches = match command {
            ExecutionCommand::PlaceLimit(order) => {
                let expected_policy = match order.time_in_force {
                    LimitTimeInForce::PostOnly => "Alo",
                    LimitTimeInForce::Gtc => "Gtc",
                };
                side == order.side
                    && limit_price == order.limit_price
                    && original_quantity == order.quantity
                    && reduce_only == order.reduce_only
                    && time_in_force.as_deref() == Some(expected_policy)
            }
            ExecutionCommand::PlaceMarket(order) => {
                side == order.side
                    && original_quantity == order.quantity
                    && !reduce_only
                    && time_in_force.as_deref() == Some("Ioc")
            }
            ExecutionCommand::MarketReduce(order) => {
                side == order.side
                    && original_quantity == order.quantity
                    && reduce_only
                    && time_in_force.as_deref() == Some("Ioc")
            }
            _ => false,
        };
        if !common_matches || !command_matches {
            return Ok(None);
        }
        let filled_quantity = original_quantity
            .checked_sub(remaining_quantity)
            .ok_or(HyperliquidAccountGatewayError::Readback)?;
        Ok(Some(DurableOrderObservation {
            client_order_id: expected_client_order_id.to_owned(),
            native_order_id: order_id.to_string(),
            state,
            filled_quantity,
            average_price: FieldState::Missing,
            cumulative_fee: FieldState::Missing,
        }))
    }

    /// Reads public native metadata and a timestamped book, then exposes only rules the venue
    /// actually publishes. Hyperliquid has no fixed price tick: this snapshot uses the coarsest
    /// tick implied by the current price's five-significant-figure and perp decimal constraints.
    pub fn durable_market_facts(
        &mut self,
    ) -> Result<DurableMarketFacts, HyperliquidAccountGatewayError> {
        self.refresh_meta()?;
        let bbo = self.current_bbo()?;
        durable_market_facts_from_observation(&self.meta, &bbo)
    }

    /// The legacy profile keeps the empty-account canary guard. Durable commands retain native
    /// validation and signed reduction checks while allowing an already-active strategy.
    pub(super) fn execute_command(
        &mut self,
        command: &ExecutionCommand,
        legacy_profile: bool,
        context: Option<&DurableExecutionContext>,
    ) -> AccountGatewayResult {
        if self.refresh_meta().is_err() || self.refresh().is_err() {
            return rejected("hyperliquid_preflight_failed");
        }
        if self.verify_account_scope().is_err() {
            return rejected("hyperliquid_account_scope");
        }
        let now_ms = match unix_ms() {
            Ok(value) => value,
            Err(_) => return rejected("hyperliquid_clock"),
        };
        let expires_after_ms = match now_ms.checked_add(ACTION_EXPIRY_MS) {
            Some(value) => Some(value),
            None => return rejected("hyperliquid_clock"),
        };
        let request = match command {
            ExecutionCommand::PlaceLimit(command) => {
                if !legacy_profile {
                    let position = match self.account_safety.position.as_ref() {
                        Some(value) => match position_for_command(value, command.position_side) {
                            Ok(value) => value,
                            Err(_) => return rejected("hyperliquid_limit_position"),
                        },
                        None if command.position_side == PositionSide::Net => {
                            flat_net_position(&command.owner.symbol)
                        }
                        None => return rejected("hyperliquid_limit_position"),
                    };
                    if command
                        .validate_with_authoritative_position(&position)
                        .is_err()
                    {
                        return rejected("hyperliquid_limit_position");
                    }
                }
                if legacy_profile
                    && !command.reduce_only
                    && (self.account_safety.has_position || self.account_safety.has_open_orders)
                {
                    return rejected("hyperliquid_existing_account_risk");
                }
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let cloid = command_cloid(command.client_order_id.as_str());
                match command.time_in_force {
                    LimitTimeInForce::PostOnly => {
                        let order = match HyperliquidAloOrder::new(
                            &self.meta,
                            command.side,
                            command.limit_price.value(),
                            command.quantity,
                            command.reduce_only,
                            cloid,
                        ) {
                            Ok(value) => value,
                            Err(_) => return rejected("hyperliquid_intent_rejected"),
                        };
                        build_alo_place_request(&self.credentials, nonce, order, expires_after_ms)
                    }
                    LimitTimeInForce::Gtc => {
                        let order = match HyperliquidGtcOrder::new(
                            &self.meta,
                            command.side,
                            command.limit_price.value(),
                            command.quantity,
                            command.reduce_only,
                            cloid,
                        ) {
                            Ok(value) => value,
                            Err(_) => return rejected("hyperliquid_intent_rejected"),
                        };
                        build_gtc_place_request(&self.credentials, nonce, order, expires_after_ms)
                    }
                }
            }
            ExecutionCommand::Cancel(command) => {
                let target = if legacy_profile {
                    self.legacy_cancel_target(command)
                } else {
                    self.cancel_target(command, context)
                };
                let order_id = match target {
                    Ok(CancelTarget::Open(order_id)) => order_id,
                    Ok(CancelTarget::Terminal(order_id)) => {
                        return AccountGatewayResult::Accepted {
                            venue_order_id: order_id.to_string(),
                        };
                    }
                    Err(CancelTargetError::Context) => {
                        return rejected("hyperliquid_cancel_context");
                    }
                    Err(CancelTargetError::Unresolved) => return AccountGatewayResult::Unknown,
                };
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let cancel = match HyperliquidCancel::new(&self.meta, order_id) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_cancel_identity"),
                };
                build_cancel_request(&self.credentials, nonce, cancel, expires_after_ms)
            }
            ExecutionCommand::MarketReduce(command) => {
                let position = match self.account_safety.position.as_ref() {
                    Some(value) => value,
                    None => return rejected("hyperliquid_market_reduce_position"),
                };
                let position = match position_for_command(position, command.position_side) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_reduce_position"),
                };
                if validate_market_reduce_position(command, &position).is_err() {
                    return rejected("hyperliquid_market_reduce_position");
                }
                let bbo = match self.current_bbo() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_reduce_rules"),
                };
                let price = match ioc_reduce_price(command.side, &bbo, &self.meta) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_reduce_rules"),
                };
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let order = match HyperliquidIocReduceOnlyOrder::new(
                    &self.meta,
                    command.side,
                    price,
                    command.quantity,
                    command_cloid(command.client_order_id.as_str()),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_reduce_rules"),
                };
                build_ioc_reduce_only_request(&self.credentials, nonce, order, expires_after_ms)
            }
            ExecutionCommand::PlaceMarket(command) => {
                let position = match self.account_safety.position.as_ref() {
                    Some(value) => value.clone(),
                    None if command.position_side == PositionSide::Net => {
                        flat_net_position(&command.owner.symbol)
                    }
                    None => return rejected("hyperliquid_market_entry_position"),
                };
                let position = match position_for_command(&position, command.position_side) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_entry_position"),
                };
                if command
                    .validate_with_authoritative_position(&position)
                    .is_err()
                {
                    return rejected("hyperliquid_market_entry_position");
                }
                let bbo = match self.current_bbo() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_entry_rules"),
                };
                let price = match ioc_entry_price(command.side, &bbo, &self.meta) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_entry_rules"),
                };
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let order = match HyperliquidIocReduceOnlyOrder::new_market(
                    &self.meta,
                    command.side,
                    price,
                    command.quantity,
                    command_cloid(command.client_order_id.as_str()),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_entry_rules"),
                };
                build_ioc_market_request(&self.credentials, nonce, order, expires_after_ms)
            }
            ExecutionCommand::StopMarketFullPosition(command) => {
                let position = match self.account_safety.position.as_ref() {
                    Some(value) => value,
                    None => return rejected("hyperliquid_trigger_position"),
                };
                let position = match position_for_command(position, command.position_side) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_trigger_position"),
                };
                if command
                    .validate_with_authoritative_position(&position)
                    .is_err()
                {
                    return rejected("hyperliquid_trigger_position");
                }
                if !valid_trigger_direction(command, position.mark_price) {
                    return rejected("hyperliquid_trigger_direction");
                }
                let limit_price = match trigger_market_limit_price(
                    command.side,
                    command.trigger_price.value(),
                    &self.meta,
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_trigger_rules"),
                };
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let tpsl =
                    if command.owner.purpose == venue_domain::domain::OrderPurpose::TakeProfit {
                        "tp"
                    } else {
                        "sl"
                    };
                let order = match HyperliquidTriggerMarketOrder::new(
                    &self.meta,
                    command.side,
                    limit_price,
                    command.trigger_price.value(),
                    command.quantity,
                    tpsl,
                    command_cloid(command.client_algo_id.as_str()),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_trigger_rules"),
                };
                build_trigger_market_request(&self.credentials, nonce, order, expires_after_ms)
            }
            ExecutionCommand::StopMarketCloseAll(_) => {
                return rejected("hyperliquid_trigger_legacy_unsupported");
            }
        };
        let request = match request {
            Ok(value) => value,
            Err(_) => return rejected("hyperliquid_signing_rejected"),
        };
        match self
            .runtime
            .block_on(self.transport.post_exchange(request.binding(), &request))
        {
            Ok(response) => match parse_exchange_ack(&response.body, &request) {
                Ok(outcome @ HyperliquidExchangeOutcome::Resting { .. })
                | Ok(outcome @ HyperliquidExchangeOutcome::Filled { .. })
                | Ok(outcome @ HyperliquidExchangeOutcome::Cancelled { .. }) => {
                    self.confirm_exchange_readback(&request, &outcome)
                }
                Ok(HyperliquidExchangeOutcome::Rejected { reason }) => {
                    AccountGatewayResult::Rejected { reason }
                }
                Err(_) => AccountGatewayResult::Unknown,
            },
            Err(error) => map_transport_dispatch(error),
        }
    }

    pub(super) fn reconcile_command(
        &mut self,
        command: &ExecutionCommand,
        context: Option<&DurableExecutionContext>,
    ) -> AccountGatewayResult {
        if self.refresh_meta().is_err() || self.refresh().is_err() {
            return AccountGatewayResult::Unknown;
        }
        if self.verify_account_scope().is_err() {
            return AccountGatewayResult::Unknown;
        }
        let lookup = match order_lookup(command, context) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let status = match self.order_status(&lookup) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        match command {
            ExecutionCommand::Cancel(_) => {
                let Some(target) = context.and_then(|value| value.target_command.as_ref()) else {
                    return AccountGatewayResult::Unknown;
                };
                reconcile_cancel_status(target, &status, self.binding.gateway().gateway_binding())
            }
            _ => reconcile_hyperliquid_status(
                command,
                &status,
                self.binding.gateway().gateway_binding(),
            ),
        }
    }

    fn cancel_target(
        &mut self,
        command: &venue_domain::domain::CancelCommand,
        context: Option<&DurableExecutionContext>,
    ) -> Result<CancelTarget, CancelTargetError> {
        let execution = ExecutionCommand::Cancel(command.clone());
        let context = context.filter(|value| validate_durable_context(&execution, value));
        let target = context
            .and_then(|value| value.target_command.as_ref())
            .ok_or(CancelTargetError::Context)?;
        let lookup = order_lookup(&execution, context).map_err(|_| CancelTargetError::Context)?;
        let status = self
            .order_status(&lookup)
            .map_err(|_| CancelTargetError::Unresolved)?;
        let HyperliquidOrderStatus::Known {
            order_id, state, ..
        } = &status
        else {
            return Err(CancelTargetError::Unresolved);
        };
        if !hyperliquid_status_matches_command(
            target,
            &status,
            self.binding.gateway().gateway_binding(),
        ) {
            return Err(CancelTargetError::Unresolved);
        }
        match state {
            OrderState::New | OrderState::PartiallyFilled => Ok(CancelTarget::Open(*order_id)),
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired => {
                Ok(CancelTarget::Terminal(*order_id))
            }
            OrderState::Rejected | OrderState::Unknown => Err(CancelTargetError::Unresolved),
        }
    }

    fn legacy_cancel_target(
        &mut self,
        command: &venue_domain::domain::CancelCommand,
    ) -> Result<CancelTarget, CancelTargetError> {
        let lookup = HyperliquidOrderLookup::client_order_id(command_cloid(
            command.target_client_order_id.as_str(),
        ))
        .map_err(|_| CancelTargetError::Context)?;
        let status = self
            .order_status(&lookup)
            .map_err(|_| CancelTargetError::Unresolved)?;
        let HyperliquidOrderStatus::Known {
            order_id, state, ..
        } = status
        else {
            return Err(CancelTargetError::Unresolved);
        };
        match state {
            OrderState::New | OrderState::PartiallyFilled => Ok(CancelTarget::Open(order_id)),
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired => {
                Ok(CancelTarget::Terminal(order_id))
            }
            OrderState::Rejected | OrderState::Unknown => Err(CancelTargetError::Unresolved),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancelTarget {
    Open(u64),
    Terminal(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancelTargetError {
    Context,
    Unresolved,
}

pub(super) fn reconcile_hyperliquid_status(
    command: &ExecutionCommand,
    status: &HyperliquidOrderStatus,
    binding: &GatewayBinding,
) -> AccountGatewayResult {
    let HyperliquidOrderStatus::Known {
        scope,
        order_id,
        state,
        ..
    } = status
    else {
        return AccountGatewayResult::Unknown;
    };
    if scope.binding().gateway().gateway_binding() != binding
        || !hyperliquid_status_matches_command(command, status, binding)
    {
        return AccountGatewayResult::Unknown;
    }
    match command {
        ExecutionCommand::PlaceLimit(_) => match state {
            OrderState::Rejected => rejected("hyperliquid_order_rejected"),
            OrderState::New
            | OrderState::PartiallyFilled
            | OrderState::Filled
            | OrderState::Cancelled
            | OrderState::Expired => AccountGatewayResult::Accepted {
                venue_order_id: order_id.to_string(),
            },
            OrderState::Unknown => AccountGatewayResult::Unknown,
        },
        ExecutionCommand::MarketReduce(_) | ExecutionCommand::PlaceMarket(_) => match state {
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired => {
                AccountGatewayResult::Accepted {
                    venue_order_id: order_id.to_string(),
                }
            }
            OrderState::Rejected => rejected("hyperliquid_order_rejected"),
            OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown => {
                AccountGatewayResult::Unknown
            }
        },
        ExecutionCommand::StopMarketFullPosition(_) => match state {
            OrderState::New
            | OrderState::PartiallyFilled
            | OrderState::Filled
            | OrderState::Cancelled
            | OrderState::Expired => AccountGatewayResult::Accepted {
                venue_order_id: order_id.to_string(),
            },
            OrderState::Rejected => rejected("hyperliquid_order_rejected"),
            OrderState::Unknown => AccountGatewayResult::Unknown,
        },
        ExecutionCommand::Cancel(_) => match state {
            OrderState::Cancelled | OrderState::Filled | OrderState::Expired => {
                AccountGatewayResult::Accepted {
                    venue_order_id: order_id.to_string(),
                }
            }
            OrderState::Rejected => rejected("hyperliquid_target_terminal_without_cancel"),
            OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown => {
                AccountGatewayResult::Unknown
            }
        },
        _ => AccountGatewayResult::Unknown,
    }
}

fn hyperliquid_status_matches_command(
    command: &ExecutionCommand,
    status: &HyperliquidOrderStatus,
    binding: &GatewayBinding,
) -> bool {
    let HyperliquidOrderStatus::Known {
        scope,
        client_order_id,
        side,
        limit_price,
        original_quantity,
        reduce_only,
        native_order_type,
        time_in_force,
        trigger_price,
        is_position_tpsl,
        ..
    } = status
    else {
        return false;
    };
    if scope.binding().gateway().gateway_binding() != binding {
        return false;
    }
    let Some(expected_client) = command.native_client_id() else {
        return false;
    };
    let expected_client = command_cloid(expected_client.as_str());
    let identity_matches = matches!(
        client_order_id,
        FieldState::Known(actual) if actual.eq_ignore_ascii_case(&expected_client)
    );
    match command {
        ExecutionCommand::PlaceLimit(command) => {
            identity_matches
                && *side == command.side
                && *limit_price == command.limit_price
                && *original_quantity == command.quantity
                && *reduce_only == command.reduce_only
                && native_order_type == "Limit"
                && time_in_force.as_deref()
                    == Some(match command.time_in_force {
                        LimitTimeInForce::PostOnly => "Alo",
                        LimitTimeInForce::Gtc => "Gtc",
                    })
                && trigger_price.is_none()
                && !is_position_tpsl
        }
        ExecutionCommand::MarketReduce(command) => {
            identity_matches
                && *side == command.side
                && *original_quantity == command.quantity
                && *reduce_only
                && market_status_shape(native_order_type, time_in_force.as_deref())
                && trigger_price.is_none()
                && !is_position_tpsl
        }
        ExecutionCommand::PlaceMarket(command) => {
            identity_matches
                && *side == command.side
                && *original_quantity == command.quantity
                && !*reduce_only
                && market_status_shape(native_order_type, time_in_force.as_deref())
                && trigger_price.is_none()
                && !is_position_tpsl
        }
        ExecutionCommand::StopMarketFullPosition(command) => {
            identity_matches
                && *side == command.side
                && *original_quantity == command.quantity
                && *reduce_only
                && native_order_type
                    == if command.owner.purpose == venue_domain::domain::OrderPurpose::TakeProfit {
                        "Take Profit Market"
                    } else {
                        "Stop Market"
                    }
                && time_in_force.is_none()
                && *trigger_price == Some(command.trigger_price)
                && *is_position_tpsl
        }
        ExecutionCommand::Cancel(_) | ExecutionCommand::StopMarketCloseAll(_) => false,
    }
}

fn market_status_shape(native_order_type: &str, time_in_force: Option<&str>) -> bool {
    matches!(
        (native_order_type, time_in_force),
        ("Limit", Some("Ioc")) | ("Market", Some("FrontendMarket"))
    )
}

fn reconcile_cancel_status(
    target: &ExecutionCommand,
    status: &HyperliquidOrderStatus,
    binding: &GatewayBinding,
) -> AccountGatewayResult {
    if !hyperliquid_status_matches_command(target, status, binding) {
        return AccountGatewayResult::Unknown;
    }
    let HyperliquidOrderStatus::Known {
        order_id, state, ..
    } = status
    else {
        return AccountGatewayResult::Unknown;
    };
    match state {
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired => {
            AccountGatewayResult::Accepted {
                venue_order_id: order_id.to_string(),
            }
        }
        OrderState::Rejected => rejected("hyperliquid_target_terminal_without_cancel"),
        OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown => {
            AccountGatewayResult::Unknown
        }
    }
}

fn order_lookup(
    command: &ExecutionCommand,
    context: Option<&DurableExecutionContext>,
) -> Result<HyperliquidOrderLookup, ()> {
    if matches!(command, ExecutionCommand::Cancel(_)) {
        let context = context
            .filter(|value| validate_durable_context(command, value))
            .ok_or(())?;
        if let Some(order_id) = context.target_native_order_id.as_deref() {
            let order_id = order_id.parse::<u64>().map_err(|_| ())?;
            return HyperliquidOrderLookup::order_id(order_id).map_err(|_| ());
        }
        let target = context.target_command.as_ref().ok_or(())?;
        let client_order_id = target.native_client_id().ok_or(())?;
        return HyperliquidOrderLookup::client_order_id(command_cloid(client_order_id.as_str()))
            .map_err(|_| ());
    }
    let client_order_id = command.native_client_id().ok_or(())?;
    HyperliquidOrderLookup::client_order_id(command_cloid(client_order_id.as_str())).map_err(|_| ())
}

fn position_for_command(position: &Position, position_side: PositionSide) -> Result<Position, ()> {
    if position_side != PositionSide::Net {
        return Ok(position.clone());
    }
    let quantity = match position.side {
        PositionSide::Long => position.quantity,
        PositionSide::Short => position
            .quantity
            .checked_mul(Decimal::NEGATIVE_ONE)
            .ok_or(())?,
        PositionSide::Net => position.quantity,
    };
    Ok(Position {
        symbol: position.symbol.clone(),
        side: PositionSide::Net,
        quantity,
        entry_price: position.entry_price,
        mark_price: position.mark_price,
    })
}

fn flat_net_position(symbol: &Symbol) -> Position {
    Position {
        symbol: symbol.clone(),
        side: PositionSide::Net,
        quantity: Decimal::ZERO,
        entry_price: None,
        mark_price: None,
    }
}

fn valid_trigger_direction(
    command: &venue_domain::domain::StopMarketFullPositionCommand,
    mark_price: Option<Price>,
) -> bool {
    let Some(mark_price) = mark_price else {
        return false;
    };
    match (command.owner.purpose, command.side) {
        (venue_domain::domain::OrderPurpose::Protection, OrderSide::Sell)
        | (venue_domain::domain::OrderPurpose::TakeProfit, OrderSide::Buy) => {
            command.trigger_price < mark_price
        }
        (venue_domain::domain::OrderPurpose::Protection, OrderSide::Buy)
        | (venue_domain::domain::OrderPurpose::TakeProfit, OrderSide::Sell) => {
            command.trigger_price > mark_price
        }
        _ => false,
    }
}

fn durable_market_facts_from_observation(
    meta: &HyperliquidPerpMeta,
    bbo: &HyperliquidBbo,
) -> Result<DurableMarketFacts, HyperliquidAccountGatewayError> {
    if meta.scope != bbo.scope || bbo.exchange_time_ms == 0 {
        return Err(HyperliquidAccountGatewayError::Instrument);
    }
    let reference = bbo
        .bid
        .price
        .value()
        .checked_add(bbo.ask.price.value())
        .and_then(|value| value.checked_div(Decimal::from(2)))
        .ok_or(HyperliquidAccountGatewayError::Instrument)?;
    let reference_price =
        Price::new(reference).map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    let price_tick = dynamic_price_tick(reference, meta.size_decimals)
        .ok_or(HyperliquidAccountGatewayError::Instrument)?;
    let quantity_step = decimal_power_of_ten(
        -(i32::try_from(meta.size_decimals)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?),
    )
    .ok_or(HyperliquidAccountGatewayError::Instrument)?;
    let quote = Asset::new(meta.scope.symbol().quote())
        .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    let instrument = Instrument {
        symbol: meta.scope.symbol().clone(),
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(quote.clone()),
        generation: bbo.exchange_time_ms,
        price_tick: Price::new(price_tick)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?,
        quantity_step,
        // Hyperliquid's metadata response does not publish a universal minimum notional and the
        // venue gives reducing orders different treatment, so zero records no shared lower bound.
        minimum_notional: Amount::new(quote, Decimal::ZERO),
    };
    let metadata = InstrumentMetadata::new(
        instrument,
        Precision::new(price_tick, price_tick)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?,
        Precision::new(quantity_step, quantity_step)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?,
        None,
        meta.trading_enabled,
    )
    .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    Ok(DurableMarketFacts {
        binding: meta.scope.binding().gateway().gateway_binding().clone(),
        metadata,
        reference_price,
        observed_at_ms: bbo.exchange_time_ms,
        maximum_quantity: None,
        maximum_price: None,
    })
}

fn dynamic_price_tick(reference: Decimal, size_decimals: u32) -> Option<Decimal> {
    if reference <= Decimal::ZERO || size_decimals > 6 {
        return None;
    }
    let scientific_exponent = decimal_scientific_exponent(reference)?;
    let significant_tick_exponent = scientific_exponent.checked_sub(4)?;
    let decimal_tick_exponent = i32::try_from(6_u32.checked_sub(size_decimals)?)
        .ok()?
        .checked_neg()?;
    decimal_power_of_ten(significant_tick_exponent.max(decimal_tick_exponent))
}

fn decimal_scientific_exponent(value: Decimal) -> Option<i32> {
    let normalized = value.normalize();
    if normalized <= Decimal::ZERO {
        return None;
    }
    let digits = normalized.mantissa().unsigned_abs().to_string().len();
    i32::try_from(digits)
        .ok()?
        .checked_sub(i32::try_from(normalized.scale()).ok()?)?
        .checked_sub(1)
}

fn decimal_power_of_ten(exponent: i32) -> Option<Decimal> {
    if exponent >= 0 {
        let exponent = u32::try_from(exponent).ok()?;
        let mantissa = 10_i128.checked_pow(exponent)?;
        Some(Decimal::from_i128_with_scale(mantissa, 0))
    } else {
        let scale = exponent
            .checked_neg()
            .and_then(|value| u32::try_from(value).ok())?;
        (scale <= 28).then(|| Decimal::from_i128_with_scale(1, scale))
    }
}

fn trigger_market_limit_price(
    side: OrderSide,
    trigger_price: Decimal,
    meta: &HyperliquidPerpMeta,
) -> Result<Decimal, ()> {
    let bps = Decimal::from(TRIGGER_MARKET_SLIPPAGE_BPS);
    let denominator = Decimal::from(BPS_DENOMINATOR);
    let factor = match side {
        OrderSide::Buy => Decimal::ONE.checked_add(bps.checked_div(denominator).ok_or(())?),
        OrderSide::Sell => Decimal::ONE.checked_sub(bps.checked_div(denominator).ok_or(())?),
    }
    .ok_or(())?;
    let raw = trigger_price.checked_mul(factor).ok_or(())?;
    executable_price(raw, side, meta.size_decimals)
}

fn executable_price(raw: Decimal, side: OrderSide, size_decimals: u32) -> Result<Decimal, ()> {
    let max_scale = 6_u32.checked_sub(size_decimals).ok_or(())?;
    for scale in (0..=max_scale).rev() {
        let factor = Decimal::from(10_u64.checked_pow(scale).ok_or(())?);
        let scaled = raw.checked_mul(factor).ok_or(())?;
        let floor = scaled.floor();
        let units = match side {
            OrderSide::Sell => floor,
            OrderSide::Buy if floor == scaled => floor,
            OrderSide::Buy => floor.checked_add(Decimal::ONE).ok_or(())?,
        };
        let candidate = units.checked_div(factor).ok_or(())?;
        let significant_digits = candidate
            .normalize()
            .mantissa()
            .unsigned_abs()
            .to_string()
            .len();
        if candidate > Decimal::ZERO
            && (candidate.normalize().scale() == 0 || significant_digits <= 5)
        {
            return Ok(candidate);
        }
    }
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate_frontend_open_orders_snapshot;
    use venue_domain::domain::{
        CancelCommand, CommandId, OrderOwner, OrderPurpose, StopMarketFullPositionCommand,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    const META: &[u8] = include_bytes!("../fixtures/perp-meta.json");
    const BOOK: &[u8] = include_bytes!("../fixtures/l2-book.json");
    const POSITION_TPSL: &[u8] = include_bytes!("../fixtures/order-status-position-tpsl.json");
    const USER: &str = "0x0000000000000000000000000000000000000001";
    const AGENT: &str = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a";
    const AGENT_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn market_facts() -> Result<(HyperliquidPerpMeta, HyperliquidBbo), Box<dyn std::error::Error>> {
        let gateway = HyperliquidGatewayBinding::new(GatewayBinding::new(
            VenueId::Hyperliquid,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)?;
        let binding = HyperliquidReadBinding::new(gateway, USER)?;
        let meta = parse_perp_meta(META, &binding)?;
        let bbo = parse_l2_book_bbo(BOOK, &meta)?;
        Ok((meta, bbo))
    }

    fn stop(
        purpose: OrderPurpose,
        side: OrderSide,
        trigger: i64,
    ) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::StopMarketFullPosition(
            StopMarketFullPositionCommand {
                command_id: CommandId::new("hl_stop")?,
                client_algo_id: CommandId::new("hl_stop_client")?,
                owner: OrderOwner {
                    strategy_instance_id: "grid1".into(),
                    run_id: "run1".into(),
                    exchange: "hyperliquid".into(),
                    account: "00000000-0000-4000-8000-000000000001".into(),
                    symbol: "BTC/USDC".parse()?,
                    purpose,
                },
                side,
                position_side: PositionSide::Net,
                quantity: Decimal::new(4, 1),
                trigger_price: Price::new(Decimal::from(trigger))?,
                position_generation: 9,
            },
        ))
    }

    fn cancel(target: &ExecutionCommand) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        let owner = target.mutation_owner().clone();
        let target_client_order_id = target
            .native_client_id()
            .ok_or_else(|| std::io::Error::other("missing target identity"))?
            .clone();
        Ok(ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("hl_cancel")?,
            owner,
            target_client_order_id,
        }))
    }

    fn position_tpsl_payload(
        command: &ExecutionCommand,
        state: &str,
        remaining: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut payload: serde_json::Value = serde_json::from_slice(POSITION_TPSL)?;
        payload["order"]["order"]["cloid"] = serde_json::json!(command_cloid(
            command
                .native_client_id()
                .ok_or_else(|| std::io::Error::other("missing client identity"))?
                .as_str()
        ));
        payload["order"]["order"]["sz"] = serde_json::json!(remaining);
        payload["order"]["status"] = serde_json::json!(state);
        Ok(serde_json::to_vec(&payload)?)
    }

    #[test]
    fn durable_market_facts_use_native_base_units_and_dynamic_price_rules()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, bbo) = market_facts()?;
        let facts = durable_market_facts_from_observation(&meta, &bbo)?;
        assert_eq!(
            &facts.binding,
            meta.scope.binding().gateway().gateway_binding()
        );
        assert_eq!(facts.reference_price.value(), Decimal::from(113_387));
        assert_eq!(facts.observed_at_ms, 1_754_450_974_231);
        assert_eq!(facts.metadata.instrument.price_tick.value(), Decimal::TEN);
        assert_eq!(facts.metadata.price.step, Decimal::TEN);
        assert_eq!(facts.metadata.quantity.step, Decimal::new(1, 5));
        assert_eq!(facts.metadata.instrument.quantity_step, Decimal::new(1, 5));
        assert!(facts.metadata.contract.is_none());
        assert_eq!(
            facts.metadata.instrument.minimum_notional.value,
            Decimal::ZERO
        );
        assert!(facts.maximum_quantity.is_none());
        assert!(facts.maximum_price.is_none());

        assert_eq!(
            dynamic_price_tick(Decimal::new(1234, 6), 1),
            Some(Decimal::new(1, 5))
        );
        assert_eq!(
            dynamic_price_tick(Decimal::from(3_500), 4),
            Some(Decimal::new(1, 1))
        );
        assert_eq!(
            dynamic_price_tick(Decimal::from(99_999), 5),
            Some(Decimal::ONE)
        );
        assert_eq!(
            dynamic_price_tick(Decimal::from(100_000), 5),
            Some(Decimal::TEN)
        );
        assert_eq!(dynamic_price_tick(Decimal::ONE, 7), None);
        Ok(())
    }

    #[test]
    fn position_tpsl_wire_and_frontend_readback_match_exact_native_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let fixture: serde_json::Value = serde_json::from_slice(POSITION_TPSL)?;
        let frontend = serde_json::to_vec(&serde_json::json!([fixture["order"]["order"].clone()]))?;
        let frontend = parse_frontend_open_orders_snapshot(&frontend, &meta, 1_700_000_000_003)?;
        assert_eq!(frontend.orders.len(), 1);
        assert!(frontend.orders[0].is_position_tpsl);
        validate_frontend_open_orders_snapshot(&frontend, &meta)?;

        let command = stop(OrderPurpose::Protection, OrderSide::Sell, 64_000)?;
        let order = HyperliquidTriggerMarketOrder::new(
            &meta,
            OrderSide::Sell,
            Decimal::from(57_600),
            Decimal::from(64_000),
            Decimal::new(4, 1),
            "sl",
            command_cloid("hl_stop_client"),
        )?;
        let credentials = HyperliquidCredentials::from_values(USER, None, AGENT, AGENT_KEY)?;
        let request = build_trigger_market_request(
            &credentials,
            crate::PersistedNonce::from_committed(AGENT, 1_700_000_000_000)?,
            order,
            Some(1_700_000_001_000),
        )?;
        let body: serde_json::Value = serde_json::from_slice(request.body())?;
        assert_eq!(body["action"]["grouping"], "positionTpsl");
        assert_eq!(body["action"]["orders"][0]["p"], "57600");
        assert_eq!(body["action"]["orders"][0]["s"], "0.4");
        assert_eq!(body["action"]["orders"][0]["r"], true);
        assert_eq!(
            body["action"]["orders"][0]["t"]["trigger"],
            serde_json::json!({"isMarket":true,"triggerPx":"64000","tpsl":"sl"})
        );

        let acknowledgement = parse_exchange_ack(
            br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77}}]}}}"#,
            &request,
        )?;
        let private = HyperliquidPrivateStreamBinding::new(&meta, 4)?;
        let plan = begin_exchange_readback(&request, Some(&acknowledgement), &private)?;
        let status = parse_order_status(
            &position_tpsl_payload(&command, "open", "0.4")?,
            &meta,
            plan.lookup(),
        )?;
        assert!(matches!(
            plan.reconcile(Some(&status))?,
            HyperliquidExchangeConvergence::Confirmed {
                order_id: 77,
                state: OrderState::New,
                ..
            }
        ));
        assert!(matches!(
            reconcile_hyperliquid_status(
                &command,
                &status,
                meta.scope.binding().gateway().gateway_binding()
            ),
            AccountGatewayResult::Accepted { .. }
        ));

        let mut wrong: serde_json::Value =
            serde_json::from_slice(&position_tpsl_payload(&command, "open", "0.4")?)?;
        wrong["order"]["order"]["isPositionTpsl"] = serde_json::json!(false);
        let wrong = parse_order_status(&serde_json::to_vec(&wrong)?, &meta, plan.lookup())?;
        assert_eq!(
            plan.reconcile(Some(&wrong)),
            Err(HyperliquidError::Readback)
        );
        assert!(matches!(
            reconcile_hyperliquid_status(
                &command,
                &wrong,
                meta.scope.binding().gateway().gateway_binding()
            ),
            AccountGatewayResult::Unknown
        ));
        let mut wrong_type: serde_json::Value =
            serde_json::from_slice(&position_tpsl_payload(&command, "open", "0.4")?)?;
        wrong_type["order"]["order"]["orderType"] = serde_json::json!("Take Profit Market");
        let wrong_type =
            parse_order_status(&serde_json::to_vec(&wrong_type)?, &meta, plan.lookup())?;
        assert_eq!(
            plan.reconcile(Some(&wrong_type)),
            Err(HyperliquidError::Readback)
        );
        Ok(())
    }

    #[test]
    fn terminal_position_tpsl_remains_reconcilable_after_position_is_flat()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let command = stop(OrderPurpose::Protection, OrderSide::Sell, 64_000)?;
        command.validate_persisted_shape()?;
        let lookup = HyperliquidOrderLookup::client_order_id(command_cloid("hl_stop_client"))?;
        let status = parse_order_status(
            &position_tpsl_payload(&command, "filled", "0")?,
            &meta,
            &lookup,
        )?;
        assert!(matches!(
            reconcile_hyperliquid_status(
                &command,
                &status,
                meta.scope.binding().gateway().gateway_binding()
            ),
            AccountGatewayResult::Accepted { .. }
        ));
        Ok(())
    }

    #[test]
    fn exact_cancel_context_rejects_conflicts_and_unknown_never_confirms()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let target = stop(OrderPurpose::Protection, OrderSide::Sell, 64_000)?;
        let cancel = cancel(&target)?;
        let context = DurableExecutionContext {
            target_command: Some(target.clone()),
            target_native_order_id: Some("77".into()),
        };
        assert!(validate_durable_context(&cancel, &context));
        assert_eq!(
            order_lookup(&cancel, Some(&context))
                .map_err(|_| std::io::Error::other("cancel lookup"))?,
            HyperliquidOrderLookup::OrderId(77)
        );

        let bad_native = DurableExecutionContext {
            target_command: Some(target.clone()),
            target_native_order_id: Some("another-order".into()),
        };
        assert!(order_lookup(&cancel, Some(&bad_native)).is_err());
        let conflicting = DurableExecutionContext {
            target_command: Some(stop(OrderPurpose::TakeProfit, OrderSide::Sell, 70_000)?),
            target_native_order_id: Some("77".into()),
        };
        assert!(!validate_durable_context(&cancel, &conflicting));
        assert!(order_lookup(&cancel, Some(&conflicting)).is_err());

        let lookup = HyperliquidOrderLookup::order_id(77)?;
        let open = parse_order_status(
            &position_tpsl_payload(&target, "open", "0.4")?,
            &meta,
            &lookup,
        )?;
        assert!(matches!(
            reconcile_cancel_status(
                &target,
                &open,
                meta.scope.binding().gateway().gateway_binding()
            ),
            AccountGatewayResult::Unknown
        ));
        let cancelled = parse_order_status(
            &position_tpsl_payload(&target, "canceled", "0.4")?,
            &meta,
            &lookup,
        )?;
        assert!(matches!(
            reconcile_cancel_status(
                &target,
                &cancelled,
                meta.scope.binding().gateway().gateway_binding()
            ),
            AccountGatewayResult::Accepted { .. }
        ));
        let unknown = parse_order_status(br#"{"status":"unknownOid"}"#, &meta, &lookup)?;
        assert!(matches!(
            reconcile_cancel_status(
                &target,
                &unknown,
                meta.scope.binding().gateway().gateway_binding()
            ),
            AccountGatewayResult::Unknown
        ));
        Ok(())
    }

    #[test]
    fn trigger_direction_uses_mark_price_for_stop_and_take_profit()
    -> Result<(), Box<dyn std::error::Error>> {
        let mark = Some(Price::new(Decimal::from(65_000))?);
        let ExecutionCommand::StopMarketFullPosition(stop_loss) =
            stop(OrderPurpose::Protection, OrderSide::Sell, 64_000)?
        else {
            return Err("wrong command".into());
        };
        let ExecutionCommand::StopMarketFullPosition(take_profit) =
            stop(OrderPurpose::TakeProfit, OrderSide::Sell, 70_000)?
        else {
            return Err("wrong command".into());
        };
        assert!(valid_trigger_direction(&stop_loss, mark));
        assert!(valid_trigger_direction(&take_profit, mark));
        assert!(!valid_trigger_direction(&stop_loss, None));
        let mut wrong = stop_loss;
        wrong.trigger_price = Price::new(Decimal::from(70_000))?;
        assert!(!valid_trigger_direction(&wrong, mark));
        Ok(())
    }
}
