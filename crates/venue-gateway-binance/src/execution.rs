use rust_decimal::Decimal;
mod terminal_open;
use serde_json::Value;
pub use terminal_open::prepare_terminal_open_limit;
use venue_domain::domain::{
    ExecutionCommand, FieldState, LimitTimeInForce, Order, OrderSide, OrderState, PositionSide,
    Price,
};
use venue_gateway_api::GatewayBinding;

use crate::readback::{
    BinancePositionMode, BinancePrivateReadRequest, BinancePrivateReadScope,
    BinancePrivateReadbackCandidate, BinanceRawPrivatePage, BinanceReadbackError,
    build_exact_order_request, validate_client_order_id,
};
use crate::{
    BinanceHttpMethod, BinanceInstrumentRules, BinancePrivateSurface, endpoints, native_symbol,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceTimeInForce {
    GoodTillCancelled,
    ImmediateOrCancel,
    PostOnly,
}

impl BinanceTimeInForce {
    const fn wire(self) -> &'static str {
        match self {
            Self::GoodTillCancelled => "GTC",
            Self::ImmediateOrCancel => "IOC",
            Self::PostOnly => "GTX",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePlaceIntent {
    pub client_order_id: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub limit_price: Price,
    pub time_in_force: BinanceTimeInForce,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceMarketIntent {
    pub client_order_id: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceCancelIntent {
    pub client_order_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceReduceOnceIntent {
    pub client_order_id: String,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub private_generation: u64,
}

/// Same-generation, Hedge-Mode-only preparation boundary for a Grid batch which has already
/// passed the host's signed-projection CAS. It carries no dispatch capability or credentials:
/// the singleton executor must still claim durable commands and sign each mutation exactly once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceGridDispatchFence {
    scope: BinancePrivateReadScope,
    rules: BinanceInstrumentRules,
}

impl BinanceGridDispatchFence {
    pub fn new(
        config: &crate::BinanceConfig,
        rules: BinanceInstrumentRules,
        private_generation: u64,
        attempt_id: u64,
        requested_at_ms: u64,
    ) -> Result<Self, BinanceExecutionError> {
        if config.account_binding() != crate::BinanceAccountBinding::PortfolioMarginUm {
            return Err(BinanceExecutionError::Binding);
        }
        let scope = BinancePrivateReadScope::new(
            config,
            &rules,
            private_generation,
            attempt_id,
            requested_at_ms,
        )
        .map_err(|_| BinanceExecutionError::Binding)?;
        Ok(Self { scope, rules })
    }

    #[must_use]
    pub const fn scope(&self) -> &BinancePrivateReadScope {
        &self.scope
    }

    #[must_use]
    pub const fn rules(&self) -> &BinanceInstrumentRules {
        &self.rules
    }

    /// Prepares only Binance GTX orders with an explicit LONG/SHORT direction. The committed
    /// Grid plan is responsible for balance, inventory and reservation authority.
    pub fn prepare_place_limit(
        &self,
        intent: &BinancePlaceIntent,
    ) -> Result<BinancePreparedMutation, BinanceExecutionError> {
        validate_grid_common(
            &self.rules,
            &self.scope,
            &intent.client_order_id,
            intent.quantity,
        )?;
        validate_price_and_notional(&self.rules, intent.quantity, intent.limit_price)?;
        if intent.time_in_force != BinanceTimeInForce::PostOnly {
            return Err(BinanceExecutionError::Intent);
        }
        validate_place_direction(BinancePositionMode::Hedge, intent)?;
        prepared_for_scope(
            &self.rules,
            &self.scope,
            BinanceMutationKind::PlaceLimit,
            place_limit_parameters(&self.rules, intent, BinancePositionMode::Hedge),
            intent.client_order_id.clone(),
        )
    }

    /// Prepares one exact client-order-id cancellation under the same Grid plan fence.
    pub fn prepare_cancel(
        &self,
        intent: &BinanceCancelIntent,
    ) -> Result<BinancePreparedMutation, BinanceExecutionError> {
        validate_grid_binding(&self.rules, &self.scope, &intent.client_order_id)?;
        prepared_for_scope(
            &self.rules,
            &self.scope,
            BinanceMutationKind::Cancel,
            vec![
                ("symbol".to_owned(), self.rules.native_symbol.clone()),
                (
                    "origClientOrderId".to_owned(),
                    intent.client_order_id.clone(),
                ),
            ],
            intent.client_order_id.clone(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceMutationKind {
    PlaceLimit,
    PlaceMarket,
    Cancel,
    ReduceOnce,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePreparedMutation {
    binding: GatewayBinding,
    instrument_generation: u64,
    private_generation: u64,
    kind: BinanceMutationKind,
    parameters: Vec<(String, String)>,
    client_order_id: String,
}

impl BinancePreparedMutation {
    pub fn validate(&self, scope: &BinancePrivateReadScope) -> Result<(), BinanceExecutionError> {
        if &self.binding != scope.binding()
            || self.instrument_generation == 0
            || self.instrument_generation != scope.instrument_generation()
            || self.private_generation == 0
            || self.private_generation != scope.private_generation()
            || self.parameters.is_empty()
            || self.client_order_id.is_empty()
        {
            return Err(BinanceExecutionError::Binding);
        }
        validate_client_order_id(&self.client_order_id).map_err(|_| BinanceExecutionError::Intent)
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn instrument_generation(&self) -> u64 {
        self.instrument_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn kind(&self) -> BinanceMutationKind {
        self.kind
    }

    #[must_use]
    pub const fn method(&self) -> BinanceHttpMethod {
        match self.kind {
            BinanceMutationKind::PlaceLimit
            | BinanceMutationKind::PlaceMarket
            | BinanceMutationKind::ReduceOnce => BinanceHttpMethod::Post,
            BinanceMutationKind::Cancel => BinanceHttpMethod::Delete,
        }
    }

    #[must_use]
    pub const fn path(&self) -> &'static str {
        endpoints::ORDER
    }

    #[must_use]
    pub fn parameters(&self) -> &[(String, String)] {
        &self.parameters
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    pub fn exact_readback_request(
        &self,
        scope: &BinancePrivateReadScope,
    ) -> Result<BinancePrivateReadRequest, BinanceExecutionError> {
        self.validate(scope)?;
        build_exact_order_request(scope, &self.client_order_id)
            .map_err(|_| BinanceExecutionError::Readback)
    }
}

pub fn prepare_place_limit(
    rules: &BinanceInstrumentRules,
    readback: &BinancePrivateReadbackCandidate,
    intent: &BinancePlaceIntent,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    validate_common(rules, readback, &intent.client_order_id, intent.quantity)?;
    validate_price_and_notional(rules, intent.quantity, intent.limit_price)?;
    validate_place_direction(readback.position_mode, intent)?;
    let parameters = place_limit_parameters(rules, intent, readback.position_mode);
    prepared(
        rules,
        readback,
        BinanceMutationKind::PlaceLimit,
        parameters,
        intent.client_order_id.clone(),
    )
}

/// Prepares an explicit market order. The caller must have already applied current-price and
/// notional guards; Hedge Mode deliberately omits native `reduceOnly`.
pub fn prepare_place_market(
    rules: &BinanceInstrumentRules,
    readback: &BinancePrivateReadbackCandidate,
    intent: &BinanceMarketIntent,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    validate_common(rules, readback, &intent.client_order_id, intent.quantity)?;
    validate_direction(
        readback.position_mode,
        intent.position_side,
        intent.side,
        intent.reduce_only,
    )?;
    let mut parameters = vec![
        ("symbol".to_owned(), rules.native_symbol.clone()),
        ("side".to_owned(), side_wire(intent.side).to_owned()),
        ("type".to_owned(), "MARKET".to_owned()),
        ("quantity".to_owned(), decimal_wire(intent.quantity)),
        (
            "positionSide".to_owned(),
            position_side_wire(intent.position_side).to_owned(),
        ),
        ("newOrderRespType".to_owned(), "RESULT".to_owned()),
        (
            "newClientOrderId".to_owned(),
            intent.client_order_id.clone(),
        ),
    ];
    if readback.position_mode == BinancePositionMode::Net {
        parameters.push(("reduceOnly".to_owned(), intent.reduce_only.to_string()));
    }
    prepared(
        rules,
        readback,
        BinanceMutationKind::PlaceMarket,
        parameters,
        intent.client_order_id.clone(),
    )
}

pub fn prepare_cancel(
    rules: &BinanceInstrumentRules,
    readback: &BinancePrivateReadbackCandidate,
    intent: &BinanceCancelIntent,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    validate_common(
        rules,
        readback,
        &intent.client_order_id,
        rules.minimum_quantity,
    )?;
    prepared(
        rules,
        readback,
        BinanceMutationKind::Cancel,
        vec![
            ("symbol".to_owned(), rules.native_symbol.clone()),
            (
                "origClientOrderId".to_owned(),
                intent.client_order_id.clone(),
            ),
        ],
        intent.client_order_id.clone(),
    )
}

/// Prepares one market reduction against the exact same-generation signed position. The request
/// is deliberately not clone-and-retry authority: a host must WAL-wrap and dispatch it once.
pub fn prepare_reduce_once(
    rules: &BinanceInstrumentRules,
    readback: &BinancePrivateReadbackCandidate,
    intent: &BinanceReduceOnceIntent,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    validate_common(rules, readback, &intent.client_order_id, intent.quantity)?;
    if intent.private_generation != readback.scope().private_generation()
        || intent.position_side == PositionSide::Net
            && readback.position_mode != BinancePositionMode::Net
        || intent.position_side != PositionSide::Net
            && readback.position_mode != BinancePositionMode::Hedge
    {
        return Err(BinanceExecutionError::Binding);
    }
    let position = readback
        .positions
        .iter()
        .find(|position| position.side == intent.position_side)
        .ok_or(BinanceExecutionError::Position)?;
    let (side, available) = match intent.position_side {
        PositionSide::Long => (OrderSide::Sell, position.quantity),
        PositionSide::Short => (OrderSide::Buy, position.quantity),
        PositionSide::Net if position.quantity.is_sign_positive() => {
            (OrderSide::Sell, position.quantity)
        }
        PositionSide::Net if position.quantity.is_sign_negative() => {
            (OrderSide::Buy, position.quantity.abs())
        }
        PositionSide::Net => return Err(BinanceExecutionError::Position),
    };
    if available <= Decimal::ZERO || intent.quantity > available {
        return Err(BinanceExecutionError::Position);
    }
    let mut parameters = vec![
        ("symbol".to_owned(), rules.native_symbol.clone()),
        ("side".to_owned(), side_wire(side).to_owned()),
        ("type".to_owned(), "MARKET".to_owned()),
        ("quantity".to_owned(), decimal_wire(intent.quantity)),
        (
            "positionSide".to_owned(),
            position_side_wire(intent.position_side).to_owned(),
        ),
        ("newOrderRespType".to_owned(), "RESULT".to_owned()),
        (
            "newClientOrderId".to_owned(),
            intent.client_order_id.clone(),
        ),
    ];
    if readback.position_mode == BinancePositionMode::Net {
        parameters.push(("reduceOnly".to_owned(), "true".to_owned()));
    }
    prepared(
        rules,
        readback,
        BinanceMutationKind::ReduceOnce,
        parameters,
        intent.client_order_id.clone(),
    )
}

/// Translates the account-node's canonical command without weakening either command validation or
/// the adapter's same-generation mutation checks. Unsupported native families stay closed until
/// their exact Binance surfaces are represented by this adapter.
pub fn prepare_execution_command(
    rules: &BinanceInstrumentRules,
    readback: &BinancePrivateReadbackCandidate,
    command: &ExecutionCommand,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    command
        .validate()
        .map_err(|_| BinanceExecutionError::Intent)?;
    match command {
        ExecutionCommand::PlaceLimit(command) => prepare_place_limit(
            rules,
            readback,
            &BinancePlaceIntent {
                client_order_id: command.client_order_id.as_str().to_owned(),
                side: command.side,
                position_side: command.position_side,
                quantity: command.quantity,
                limit_price: command.limit_price,
                time_in_force: match command.time_in_force {
                    LimitTimeInForce::PostOnly => BinanceTimeInForce::PostOnly,
                    LimitTimeInForce::Gtc => BinanceTimeInForce::GoodTillCancelled,
                },
                reduce_only: command.reduce_only,
            },
        ),
        ExecutionCommand::MarketReduce(command) => prepare_reduce_once(
            rules,
            readback,
            &BinanceReduceOnceIntent {
                client_order_id: command.client_order_id.as_str().to_owned(),
                position_side: command.position_side,
                quantity: command.quantity,
                private_generation: command.position_generation,
            },
        ),
        ExecutionCommand::Cancel(command) => prepare_cancel(
            rules,
            readback,
            &BinanceCancelIntent {
                client_order_id: command.target_client_order_id.as_str().to_owned(),
            },
        ),
        ExecutionCommand::PlaceMarket(command) => prepare_place_market(
            rules,
            readback,
            &BinanceMarketIntent {
                client_order_id: command.client_order_id.as_str().to_owned(),
                position_side: command.position_side,
                side: command.side,
                quantity: command.quantity,
                reduce_only: command.reduce_only,
            },
        ),
        ExecutionCommand::StopMarketCloseAll(_) | ExecutionCommand::StopMarketFullPosition(_) => {
            Err(BinanceExecutionError::UnsupportedCommand)
        }
    }
}

fn prepared(
    rules: &BinanceInstrumentRules,
    readback: &BinancePrivateReadbackCandidate,
    kind: BinanceMutationKind,
    parameters: Vec<(String, String)>,
    client_order_id: String,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    prepared_for_scope(rules, readback.scope(), kind, parameters, client_order_id)
}

fn prepared_for_scope(
    rules: &BinanceInstrumentRules,
    scope: &BinancePrivateReadScope,
    kind: BinanceMutationKind,
    parameters: Vec<(String, String)>,
    client_order_id: String,
) -> Result<BinancePreparedMutation, BinanceExecutionError> {
    let request = BinancePreparedMutation {
        binding: scope.binding().clone(),
        instrument_generation: rules.instrument.generation,
        private_generation: scope.private_generation(),
        kind,
        parameters,
        client_order_id,
    };
    request.validate(scope)?;
    Ok(request)
}

fn place_limit_parameters(
    rules: &BinanceInstrumentRules,
    intent: &BinancePlaceIntent,
    position_mode: BinancePositionMode,
) -> Vec<(String, String)> {
    let mut parameters = vec![
        ("symbol".to_owned(), rules.native_symbol.clone()),
        ("side".to_owned(), side_wire(intent.side).to_owned()),
        ("type".to_owned(), "LIMIT".to_owned()),
        (
            "timeInForce".to_owned(),
            intent.time_in_force.wire().to_owned(),
        ),
        ("quantity".to_owned(), decimal_wire(intent.quantity)),
        ("price".to_owned(), decimal_wire(intent.limit_price.value())),
        (
            "positionSide".to_owned(),
            position_side_wire(intent.position_side).to_owned(),
        ),
        ("newOrderRespType".to_owned(), "RESULT".to_owned()),
        (
            "newClientOrderId".to_owned(),
            intent.client_order_id.clone(),
        ),
    ];
    if position_mode == BinancePositionMode::Net {
        parameters.push(("reduceOnly".to_owned(), intent.reduce_only.to_string()));
    }
    parameters
}

fn validate_grid_binding(
    rules: &BinanceInstrumentRules,
    scope: &BinancePrivateReadScope,
    client_order_id: &str,
) -> Result<(), BinanceExecutionError> {
    validate_client_order_id(client_order_id).map_err(|_| BinanceExecutionError::Intent)?;
    if rules.instrument.generation == 0
        || rules.instrument.generation != scope.instrument_generation()
        || rules.instrument.symbol != scope.binding().symbol
        || rules.native_symbol != native_symbol(&rules.instrument.symbol)
    {
        return Err(BinanceExecutionError::Binding);
    }
    Ok(())
}

fn validate_grid_common(
    rules: &BinanceInstrumentRules,
    scope: &BinancePrivateReadScope,
    client_order_id: &str,
    quantity: Decimal,
) -> Result<(), BinanceExecutionError> {
    validate_grid_binding(rules, scope, client_order_id)?;
    if quantity < rules.minimum_quantity
        || quantity > rules.maximum_quantity
        || quantity <= Decimal::ZERO
        || quantity % rules.instrument.quantity_step != Decimal::ZERO
    {
        return Err(BinanceExecutionError::Rules);
    }
    Ok(())
}

fn validate_common(
    rules: &BinanceInstrumentRules,
    readback: &BinancePrivateReadbackCandidate,
    client_order_id: &str,
    quantity: Decimal,
) -> Result<(), BinanceExecutionError> {
    validate_client_order_id(client_order_id).map_err(|_| BinanceExecutionError::Intent)?;
    if !readback.capabilities.can_trade
        || rules.instrument.generation == 0
        || rules.instrument.generation != readback.scope().instrument_generation()
        || rules.instrument.symbol != readback.scope().binding().symbol
        || rules.native_symbol != native_symbol(&rules.instrument.symbol)
        || quantity < rules.minimum_quantity
        || quantity > rules.maximum_quantity
        || quantity <= Decimal::ZERO
        || quantity % rules.instrument.quantity_step != Decimal::ZERO
    {
        return Err(BinanceExecutionError::Rules);
    }
    Ok(())
}

fn validate_price_and_notional(
    rules: &BinanceInstrumentRules,
    quantity: Decimal,
    price: Price,
) -> Result<(), BinanceExecutionError> {
    if price.value() % rules.instrument.price_tick.value() != Decimal::ZERO
        || price.value() < rules.minimum_price
        || price.value() > rules.maximum_price
        || quantity
            .checked_mul(price.value())
            .ok_or(BinanceExecutionError::Rules)?
            < rules.instrument.minimum_notional.value
    {
        return Err(BinanceExecutionError::Rules);
    }
    Ok(())
}

fn validate_place_direction(
    mode: BinancePositionMode,
    intent: &BinancePlaceIntent,
) -> Result<(), BinanceExecutionError> {
    validate_direction(mode, intent.position_side, intent.side, intent.reduce_only)
}

fn validate_direction(
    mode: BinancePositionMode,
    position_side: PositionSide,
    side: OrderSide,
    reduce_only: bool,
) -> Result<(), BinanceExecutionError> {
    let valid = match mode {
        BinancePositionMode::Net => position_side == PositionSide::Net,
        BinancePositionMode::Hedge => matches!(
            (position_side, side, reduce_only),
            (PositionSide::Long, OrderSide::Buy, false)
                | (PositionSide::Long, OrderSide::Sell, true)
                | (PositionSide::Short, OrderSide::Sell, false)
                | (PositionSide::Short, OrderSide::Buy, true)
        ),
    };
    if valid {
        Ok(())
    } else {
        Err(BinanceExecutionError::Intent)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceMutationAck {
    pub binding: GatewayBinding,
    pub instrument_generation: u64,
    pub private_generation: u64,
    pub kind: BinanceMutationKind,
    pub order_id: String,
    pub client_order_id: String,
    time_in_force: Option<LimitTimeInForce>,
    pub accepted_at_ms: u64,
    pub received_at_ms: u64,
    /// Full RESULT response, when present. Identity-only ACKs never imply an order state.
    pub order: Option<Order>,
}

pub fn parse_mutation_ack(
    request: &BinancePreparedMutation,
    scope: &BinancePrivateReadScope,
    payload: &[u8],
    received_at_ms: u64,
) -> Result<BinanceMutationAck, BinanceExecutionError> {
    request.validate(scope)?;
    if received_at_ms == 0 {
        return Err(BinanceExecutionError::Payload);
    }
    let value: Value =
        serde_json::from_slice(payload).map_err(|_| BinanceExecutionError::Payload)?;
    if let Some(code) = value.get("code").and_then(Value::as_i64)
        && code != 0
    {
        return Err(BinanceExecutionError::VenueRejected);
    }
    let order_id = identifier(value.get("orderId"))?;
    let client_order_id = value
        .get("clientOrderId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(BinanceExecutionError::Payload)?;
    if client_order_id != request.client_order_id {
        return Err(BinanceExecutionError::Binding);
    }
    let accepted_at_ms = value
        .get("updateTime")
        .or_else(|| value.get("transactTime"))
        .and_then(Value::as_u64)
        .filter(|value| *value > 0 && *value <= received_at_ms)
        .ok_or(BinanceExecutionError::Payload)?;
    Ok(BinanceMutationAck {
        binding: request.binding.clone(),
        instrument_generation: request.instrument_generation,
        private_generation: request.private_generation,
        kind: request.kind,
        order_id: order_id.clone(),
        client_order_id: client_order_id.to_owned(),
        time_in_force: request.limit_time_in_force()?,
        accepted_at_ms,
        received_at_ms,
        order: std::str::from_utf8(payload)
            .ok()
            .and_then(|payload| crate::private::parse_order(payload, &request.binding.symbol).ok())
            .filter(|order| {
                order.order_id == order_id
                    && order.client_order_id == FieldState::Known(client_order_id.to_owned())
            }),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceExactOrderReadback {
    pub binding: GatewayBinding,
    pub instrument_generation: u64,
    pub private_generation: u64,
    pub requested_at_ms: u64,
    pub received_at_ms: u64,
    pub order: Order,
    pub raw_payload: Vec<u8>,
}

pub fn parse_exact_order_readback(
    ack: &BinanceMutationAck,
    request: &BinancePrivateReadRequest,
    page: &BinanceRawPrivatePage,
) -> Result<BinanceExactOrderReadback, BinanceExecutionError> {
    if request.surface() != BinancePrivateSurface::ExactOrder
        || page.surface != BinancePrivateSurface::ExactOrder
        || page.scope != *request.scope()
        || ack.binding != *request.scope().binding()
        || ack.instrument_generation != request.scope().instrument_generation()
        || ack.private_generation != request.scope().private_generation()
        || page.requested_at_ms < ack.received_at_ms
        || page.received_at_ms < page.requested_at_ms
    {
        return Err(BinanceExecutionError::Binding);
    }
    let payload = std::str::from_utf8(&page.payload).map_err(|_| BinanceExecutionError::Payload)?;
    let order = crate::private::parse_order(payload, &ack.binding.symbol)
        .map_err(|_| BinanceExecutionError::Readback)?;
    let client_matches = matches!(
        &order.client_order_id,
        FieldState::Known(value) if value == &ack.client_order_id
    );
    if order.order_id != ack.order_id
        || !client_matches
        || ack.time_in_force.is_some_and(|expected| {
            !matches!(order.time_in_force, FieldState::Known(actual) if actual == expected)
        })
    {
        return Err(BinanceExecutionError::Readback);
    }
    Ok(BinanceExactOrderReadback {
        binding: ack.binding.clone(),
        instrument_generation: ack.instrument_generation,
        private_generation: ack.private_generation,
        requested_at_ms: page.requested_at_ms,
        received_at_ms: page.received_at_ms,
        order,
        raw_payload: page.payload.to_vec(),
    })
}

impl BinancePreparedMutation {
    fn limit_time_in_force(&self) -> Result<Option<LimitTimeInForce>, BinanceExecutionError> {
        if self.kind != BinanceMutationKind::PlaceLimit {
            return Ok(None);
        }
        match self
            .parameters
            .iter()
            .find_map(|(name, value)| (name == "timeInForce").then_some(value.as_str()))
        {
            Some("GTX") => Ok(Some(LimitTimeInForce::PostOnly)),
            Some("GTC") => Ok(Some(LimitTimeInForce::Gtc)),
            _ => Err(BinanceExecutionError::Intent),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceOrderFinality {
    Working,
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceOrderSettlement {
    pub order: Order,
    pub finality: BinanceOrderFinality,
}

pub fn settle_mutation_ack(
    ack: &BinanceMutationAck,
    readback: BinanceExactOrderReadback,
) -> Result<BinanceOrderSettlement, BinanceExecutionError> {
    if readback.binding != ack.binding
        || readback.instrument_generation != ack.instrument_generation
        || readback.private_generation != ack.private_generation
        || readback.requested_at_ms < ack.received_at_ms
        || readback.order.order_id != ack.order_id
    {
        return Err(BinanceExecutionError::Binding);
    }
    let finality = if matches!(
        readback.order.state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    ) {
        BinanceOrderFinality::Terminal
    } else {
        BinanceOrderFinality::Working
    };
    Ok(BinanceOrderSettlement {
        order: readback.order,
        finality,
    })
}

fn identifier(value: Option<&Value>) -> Result<String, BinanceExecutionError> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(BinanceExecutionError::Payload),
    }
}

const fn side_wire(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

const fn position_side_wire(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Net => "BOTH",
        PositionSide::Long => "LONG",
        PositionSide::Short => "SHORT",
    }
}

fn decimal_wire(value: Decimal) -> String {
    value.normalize().to_string()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceExecutionError {
    #[error("Binance mutation does not match the fixed binding or generation")]
    Binding,
    #[error("Binance mutation intent is invalid or ambiguous")]
    Intent,
    #[error("Binance mutation violates current instrument rules")]
    Rules,
    #[error("Binance reduction exceeds the exact signed position")]
    Position,
    #[error("Binance mutation payload is invalid or incomplete")]
    Payload,
    #[error("Binance rejected the mutation request")]
    VenueRejected,
    #[error("Binance ACK did not settle through an exact signed readback")]
    Readback,
    #[error("canonical command has no closed Binance physical mutation surface")]
    UnsupportedCommand,
}

impl From<BinanceReadbackError> for BinanceExecutionError {
    fn from(_: BinanceReadbackError) -> Self {
        Self::Readback
    }
}

#[cfg(test)]
pub(crate) fn prepared_for_transport_test(
    scope: &BinancePrivateReadScope,
    kind: BinanceMutationKind,
    client_order_id: &str,
) -> BinancePreparedMutation {
    let parameters = match kind {
        BinanceMutationKind::PlaceLimit => vec![
            ("symbol".to_owned(), native_symbol(&scope.binding().symbol)),
            ("side".to_owned(), "BUY".to_owned()),
            ("type".to_owned(), "LIMIT".to_owned()),
            ("timeInForce".to_owned(), "GTX".to_owned()),
            ("quantity".to_owned(), "0.002".to_owned()),
            ("price".to_owned(), "50000".to_owned()),
            ("positionSide".to_owned(), "LONG".to_owned()),
            ("newOrderRespType".to_owned(), "RESULT".to_owned()),
            ("newClientOrderId".to_owned(), client_order_id.to_owned()),
        ],
        BinanceMutationKind::PlaceMarket => vec![
            ("symbol".to_owned(), native_symbol(&scope.binding().symbol)),
            ("side".to_owned(), "BUY".to_owned()),
            ("type".to_owned(), "MARKET".to_owned()),
            ("quantity".to_owned(), "0.002".to_owned()),
            ("positionSide".to_owned(), "LONG".to_owned()),
            ("newOrderRespType".to_owned(), "RESULT".to_owned()),
            ("newClientOrderId".to_owned(), client_order_id.to_owned()),
        ],
        BinanceMutationKind::Cancel => vec![
            ("symbol".to_owned(), native_symbol(&scope.binding().symbol)),
            ("origClientOrderId".to_owned(), client_order_id.to_owned()),
        ],
        BinanceMutationKind::ReduceOnce => vec![
            ("symbol".to_owned(), native_symbol(&scope.binding().symbol)),
            ("side".to_owned(), "SELL".to_owned()),
            ("type".to_owned(), "MARKET".to_owned()),
            ("quantity".to_owned(), "0.001".to_owned()),
            ("positionSide".to_owned(), "LONG".to_owned()),
            ("newOrderRespType".to_owned(), "RESULT".to_owned()),
            ("newClientOrderId".to_owned(), client_order_id.to_owned()),
        ],
    };
    BinancePreparedMutation {
        binding: scope.binding().clone(),
        instrument_generation: scope.instrument_generation(),
        private_generation: scope.private_generation(),
        kind,
        parameters,
        client_order_id: client_order_id.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::private::RecentFillsCursor;
    use crate::{
        BinanceAccountBinding, BinanceConfig, BinancePrivateReadScope,
        build_account_config_request, build_account_request, build_algo_orders_request,
        build_fills_request, build_position_mode_request, build_positions_request,
        build_regular_orders_request, complete_private_readback, parse_instrument_rules,
    };
    use bytes::Bytes;
    use venue_domain::domain::{CommandId, OrderCommand, OrderOwner, OrderPurpose};
    use venue_gateway_api::{GatewayMode, VenueId};

    const EXCHANGE_INFO: &str = include_str!("../tests/fixtures/exchange_info_btcusdt.json");
    const ACCOUNT: &[u8] = include_bytes!("../fixtures/portfolio-account.json");
    const ACCOUNT_CONFIG: &[u8] = include_bytes!("../fixtures/account-config.json");
    const POSITION_MODE: &[u8] = include_bytes!("../fixtures/position-mode-hedge.json");
    const POSITIONS: &[u8] = include_bytes!("../fixtures/positions-hedge-long-only.json");
    const REGULAR: &[u8] = include_bytes!("../fixtures/open-orders.json");
    const ALGO: &[u8] = include_bytes!("../fixtures/open-algo-orders.json");
    const FILLS: &[u8] = include_bytes!("../fixtures/user-trades-page.json");
    const ACK: &[u8] = include_bytes!("../fixtures/place-order-ack.json");
    const EXACT: &[u8] = include_bytes!("../fixtures/exact-order-readback.json");

    struct Facts {
        rules: BinanceInstrumentRules,
        readback: BinancePrivateReadbackCandidate,
    }

    fn facts(account: &str) -> Result<Facts, Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            account,
            "BTC/USDT".parse()?,
        )?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let rules = parse_instrument_rules(EXCHANGE_INFO, binding.symbol.clone(), 7)?;
        let scope = BinancePrivateReadScope::new(&config, &rules, 17, 11, 900)?;
        let inputs = [
            (build_account_request(&scope)?, ACCOUNT),
            (build_account_config_request(&scope)?, ACCOUNT_CONFIG),
            (build_position_mode_request(&scope)?, POSITION_MODE),
            (build_positions_request(&scope)?, POSITIONS),
            (build_regular_orders_request(&scope)?, REGULAR),
            (build_algo_orders_request(&scope)?, ALGO),
            (
                build_fills_request(
                    &scope,
                    1,
                    RecentFillsCursor {
                        observed_through_ms: 1_000,
                        last_trade_id: None,
                        last_event_time_ms: None,
                    },
                    1_000,
                    2_000,
                )?,
                FILLS,
            ),
        ];
        let pages = inputs
            .into_iter()
            .map(|(request, payload)| {
                BinanceRawPrivatePage::new(&request, 1_000, 2_000, Bytes::copy_from_slice(payload))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let readback = complete_private_readback(
            &config,
            &rules,
            &scope,
            RecentFillsCursor {
                observed_through_ms: 1_000,
                last_trade_id: None,
                last_event_time_ms: None,
            },
            2_000,
            pages,
        )?;
        Ok(Facts { rules, readback })
    }

    fn place_intent() -> Result<BinancePlaceIntent, Box<dyn std::error::Error>> {
        Ok(BinancePlaceIntent {
            client_order_id: "venue_place_1".to_owned(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 3),
            limit_price: Price::new(Decimal::new(50_000, 0))?,
            time_in_force: BinanceTimeInForce::PostOnly,
            reduce_only: false,
        })
    }

    fn grid_fence() -> Result<BinanceGridDispatchFence, Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let rules = parse_instrument_rules(EXCHANGE_INFO, binding.symbol.clone(), 7)?;
        Ok(BinanceGridDispatchFence::new(&config, rules, 17, 12, 901)?)
    }

    fn gtc_command() -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new("gtc-command")?,
            client_order_id: CommandId::new("venue_place_1")?,
            owner: OrderOwner {
                strategy_instance_id: "manual".to_owned(),
                run_id: "gtc".to_owned(),
                exchange: "binance".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 3),
            limit_price: Price::new(Decimal::new(50_000, 0))?,
            time_in_force: LimitTimeInForce::Gtc,
            reduce_only: false,
        }))
    }

    #[test]
    fn place_cancel_and_reduce_are_generation_bound_and_exactly_scoped()
    -> Result<(), Box<dyn std::error::Error>> {
        let facts = facts("00000000-0000-4000-8000-000000000001")?;
        let place = prepare_place_limit(&facts.rules, &facts.readback, &place_intent()?)?;
        assert_eq!(place.kind(), BinanceMutationKind::PlaceLimit);
        assert!(
            place
                .parameters()
                .iter()
                .any(|pair| pair == &("timeInForce".to_owned(), "GTX".to_owned()))
        );
        assert!(
            place
                .parameters()
                .iter()
                .all(|(key, _)| key != "reduceOnly")
        );

        let cancel = prepare_cancel(
            &facts.rules,
            &facts.readback,
            &BinanceCancelIntent {
                client_order_id: "venue_regular_1".to_owned(),
            },
        )?;
        assert_eq!(cancel.method(), BinanceHttpMethod::Delete);

        let reduce = prepare_reduce_once(
            &facts.rules,
            &facts.readback,
            &BinanceReduceOnceIntent {
                client_order_id: "venue_reduce_1".to_owned(),
                position_side: PositionSide::Long,
                quantity: Decimal::new(5, 3),
                private_generation: 17,
            },
        )?;
        assert_eq!(reduce.kind(), BinanceMutationKind::ReduceOnce);
        assert!(
            reduce
                .parameters()
                .iter()
                .any(|pair| pair == &("side".to_owned(), "SELL".to_owned()))
        );
        assert!(
            reduce
                .parameters()
                .iter()
                .all(|(key, _)| key != "reduceOnly")
        );

        assert_eq!(
            prepare_reduce_once(
                &facts.rules,
                &facts.readback,
                &BinanceReduceOnceIntent {
                    client_order_id: "venue_reduce_2".to_owned(),
                    position_side: PositionSide::Long,
                    quantity: Decimal::new(11, 3),
                    private_generation: 17,
                },
            ),
            Err(BinanceExecutionError::Position)
        );
        Ok(())
    }

    #[test]
    fn committed_grid_fence_prepares_only_exact_hedge_post_only_mutations()
    -> Result<(), Box<dyn std::error::Error>> {
        let fence = grid_fence()?;
        let place = fence.prepare_place_limit(&place_intent()?)?;
        assert_eq!(place.kind(), BinanceMutationKind::PlaceLimit);
        assert_eq!(place.private_generation(), 17);
        assert_eq!(place.instrument_generation(), 7);
        assert!(
            place
                .parameters()
                .contains(&("timeInForce".to_owned(), "GTX".to_owned()))
        );
        assert!(
            place
                .parameters()
                .iter()
                .all(|(key, _)| key != "reduceOnly")
        );

        let cancel = fence.prepare_cancel(&BinanceCancelIntent {
            client_order_id: "venue_regular_1".to_owned(),
        })?;
        assert_eq!(cancel.kind(), BinanceMutationKind::Cancel);

        let mut gtc = place_intent()?;
        gtc.time_in_force = BinanceTimeInForce::GoodTillCancelled;
        assert_eq!(
            fence.prepare_place_limit(&gtc),
            Err(BinanceExecutionError::Intent)
        );
        let mut wrong_direction = place_intent()?;
        wrong_direction.side = OrderSide::Sell;
        assert_eq!(
            fence.prepare_place_limit(&wrong_direction),
            Err(BinanceExecutionError::Intent)
        );
        Ok(())
    }

    #[test]
    fn net_reduce_once_uses_both_and_wire_reduce_only_without_guessing_direction()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut facts = facts("00000000-0000-4000-8000-000000000001")?;
        facts.readback.position_mode = BinancePositionMode::Net;
        facts.readback.capabilities.one_way_position = true;
        facts.readback.capabilities.hedge_position = false;
        facts.readback.positions = vec![venue_domain::domain::Position {
            symbol: "BTC/USDT".parse()?,
            side: PositionSide::Net,
            quantity: Decimal::new(-10, 3),
            entry_price: Some(Price::new(Decimal::new(50_000, 0))?),
            mark_price: Some(Price::new(Decimal::new(49_000, 0))?),
        }];
        let reduce = prepare_reduce_once(
            &facts.rules,
            &facts.readback,
            &BinanceReduceOnceIntent {
                client_order_id: "venue_net_reduce_1".to_owned(),
                position_side: PositionSide::Net,
                quantity: Decimal::new(2, 3),
                private_generation: 17,
            },
        )?;

        assert!(
            reduce
                .parameters()
                .iter()
                .any(|pair| pair == &("side".to_owned(), "BUY".to_owned()))
        );
        assert!(
            reduce
                .parameters()
                .iter()
                .any(|pair| pair == &("positionSide".to_owned(), "BOTH".to_owned()))
        );
        assert!(
            reduce
                .parameters()
                .iter()
                .any(|pair| pair == &("reduceOnly".to_owned(), "true".to_owned()))
        );
        Ok(())
    }

    #[test]
    fn ack_is_only_accepted_after_newer_exact_signed_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let facts = facts("00000000-0000-4000-8000-000000000001")?;
        let request = prepare_place_limit(&facts.rules, &facts.readback, &place_intent()?)?;
        let ack = parse_mutation_ack(&request, facts.readback.scope(), ACK, 2_000)?;
        assert!(ack.order.is_some());
        let mut identity_only: serde_json::Value = serde_json::from_slice(ACK)?;
        identity_only["status"] = serde_json::Value::Null;
        let identity_ack = parse_mutation_ack(
            &request,
            facts.readback.scope(),
            &serde_json::to_vec(&identity_only)?,
            2_000,
        )?;
        assert!(
            identity_ack.order.is_none(),
            "identity-only ACK is not a full RESULT"
        );
        let exact_request = request.exact_readback_request(facts.readback.scope())?;
        let exact_page =
            BinanceRawPrivatePage::new(&exact_request, 2_100, 2_200, Bytes::from_static(EXACT))?;
        let exact = parse_exact_order_readback(&ack, &exact_request, &exact_page)?;
        let settled = settle_mutation_ack(&ack, exact)?;
        assert_eq!(settled.finality, BinanceOrderFinality::Working);
        assert_eq!(settled.order.order_id, "401");

        let stale_page =
            BinanceRawPrivatePage::new(&exact_request, 1_900, 2_100, Bytes::from_static(EXACT))?;
        assert_eq!(
            parse_exact_order_readback(&ack, &exact_request, &stale_page),
            Err(BinanceExecutionError::Binding)
        );
        Ok(())
    }

    #[test]
    fn gtc_wire_and_exact_readback_policy_are_bound() -> Result<(), Box<dyn std::error::Error>> {
        let facts = facts("00000000-0000-4000-8000-000000000001")?;
        let request = prepare_execution_command(&facts.rules, &facts.readback, &gtc_command()?)?;
        assert!(
            request
                .parameters()
                .contains(&("timeInForce".to_owned(), "GTC".to_owned()))
        );
        let ack = parse_mutation_ack(&request, facts.readback.scope(), ACK, 2_000)?;
        let exact_request = request.exact_readback_request(facts.readback.scope())?;
        let gtc = std::str::from_utf8(EXACT)?.replace("\"GTX\"", "\"GTC\"");
        let exact =
            BinanceRawPrivatePage::new(&exact_request, 2_100, 2_200, Bytes::from(gtc.clone()))?;
        assert!(parse_exact_order_readback(&ack, &exact_request, &exact).is_ok());
        let post_only =
            BinanceRawPrivatePage::new(&exact_request, 2_100, 2_200, Bytes::from_static(EXACT))?;
        assert_eq!(
            parse_exact_order_readback(&ack, &exact_request, &post_only),
            Err(BinanceExecutionError::Readback)
        );
        let missing = BinanceRawPrivatePage::new(
            &exact_request,
            2_100,
            2_200,
            Bytes::from(gtc.replace("\"timeInForce\":\"GTC\",", "")),
        )?;
        assert_eq!(
            parse_exact_order_readback(&ack, &exact_request, &missing),
            Err(BinanceExecutionError::Readback)
        );
        Ok(())
    }

    #[test]
    fn wrong_account_candidate_cannot_validate_a_prepared_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = facts("00000000-0000-4000-8000-000000000001")?;
        let second = facts("00000000-0000-4000-8000-000000000002")?;
        let request = prepare_place_limit(&first.rules, &first.readback, &place_intent()?)?;
        assert_eq!(
            request.validate(second.readback.scope()),
            Err(BinanceExecutionError::Binding)
        );
        Ok(())
    }
}
