use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use venue_domain::domain::{
    CancelCommand, FieldState, LimitTimeInForce, MarketOrderCommand, MarketReduceCommand, Order,
    OrderCommand, OrderPurpose, OrderSide, OrderState, PositionSide, Price,
    StopMarketFullPositionCommand,
};
use venue_gateway_api::GatewayBinding;

use crate::{
    GateContractRules, GateCredentials, GateGatewayBinding, GateOrderPayloadError,
    GateProtocolError, GateRestSignedHeaders, endpoints, parse_regular_order, sign_rest,
};

const MAX_CLIENT_ORDER_SUFFIX_BYTES: usize = 28;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateMutationKind {
    PlacePostOnly,
    PlaceMarket,
    StopMarketFullPosition,
    Cancel,
    CancelPrice,
    ReduceOnce,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpectedOrder {
    order_id: Option<String>,
    client_order_id: String,
    side: Option<OrderSide>,
    position_side: Option<PositionSide>,
    quantity: Option<Decimal>,
    limit_price: Option<Price>,
    time_in_force: Option<LimitTimeInForce>,
    reduce_only: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExpectedPriceOrder {
    order_id: Option<String>,
    client_order_id: String,
    side: OrderSide,
    position_side: PositionSide,
    quantity: Decimal,
    trigger_price: Price,
    purpose: OrderPurpose,
}

/// A prepared request is intentionally not `Clone`. The async transport consumes it and returns
/// either an ACK-bound readback or an UNKNOWN-bound readback, so it cannot implement a retry loop.
#[derive(Debug, Eq, PartialEq)]
pub struct GatePreparedMutation {
    binding: GatewayBinding,
    generation: u64,
    origin: &'static str,
    method: &'static str,
    endpoint: String,
    body: Vec<u8>,
    kind: GateMutationKind,
    expected: ExpectedOrder,
    expected_price: Option<ExpectedPriceOrder>,
    reduce_episode: Option<(String, u64)>,
}

impl GatePreparedMutation {
    #[must_use]
    pub const fn kind(&self) -> GateMutationKind {
        self.kind
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn reduce_episode_id(&self) -> Option<&str> {
        self.reduce_episode
            .as_ref()
            .map(|(episode, _)| episode.as_str())
    }

    #[must_use]
    pub fn position_generation(&self) -> Option<u64> {
        self.reduce_episode
            .as_ref()
            .map(|(_, generation)| *generation)
    }

    pub(crate) fn validate(
        &self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
    ) -> Result<(), GateExecutionError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| GateExecutionError::Binding)?;
        validate_scope(binding, rules, self.generation)?;
        if self.origin != binding.config().rest_origin()
            || !matches!(self.method, "POST" | "DELETE")
            || self.body.is_empty() && self.method == "POST"
            || !self.body.is_empty() && self.method == "DELETE"
            || self.endpoint.is_empty()
        {
            return Err(GateExecutionError::Binding);
        }
        Ok(())
    }

    pub(crate) fn sign(
        &self,
        credentials: &GateCredentials,
        timestamp_sec: i64,
    ) -> Result<GateRestSignedHeaders, GateExecutionError> {
        sign_rest(
            credentials,
            timestamp_sec,
            self.method,
            &self.endpoint,
            "",
            &self.body,
        )
        .map_err(|_| GateExecutionError::Signing)
    }
}

pub fn prepare_limit(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    command: &OrderCommand,
) -> Result<GatePreparedMutation, GateExecutionError> {
    command.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &command.owner.exchange,
        &command.owner.account,
        &command.owner.symbol,
    )?;
    validate_step(
        command.limit_price.value(),
        rules.instrument.price_tick.value(),
    )?;
    let contracts = rules
        .native_order_contracts_checked(command.quantity)
        .map_err(|_| GateExecutionError::Rules)?;
    let signed_contracts = signed_contracts(contracts, command.side);
    let notional = command
        .quantity
        .checked_mul(command.limit_price.value())
        .ok_or(GateExecutionError::Rules)?;
    if notional < rules.instrument.minimum_notional.value {
        return Err(GateExecutionError::Rules);
    }
    let client_order_id = command.client_order_id.as_str().to_owned();
    let body = PlaceBody {
        contract: &rules.native_symbol,
        size: decimal_wire(signed_contracts),
        price: decimal_wire(command.limit_price.value()),
        tif: limit_time_in_force_wire(command.time_in_force),
        reduce_only: command.reduce_only,
        text: native_client_id(&client_order_id)?,
    };
    prepared_place(
        binding,
        rules,
        GateMutationKind::PlacePostOnly,
        body,
        ExpectedOrder {
            order_id: None,
            client_order_id,
            side: Some(command.side),
            position_side: Some(command.position_side),
            quantity: Some(command.quantity),
            limit_price: Some(command.limit_price),
            time_in_force: Some(command.time_in_force),
            reduce_only: Some(command.reduce_only),
        },
        None,
    )
}

/// Builds an exposure increasing futures market order with an explicit IOC execution policy.
pub fn prepare_market(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    command: &MarketOrderCommand,
) -> Result<GatePreparedMutation, GateExecutionError> {
    command.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &command.owner.exchange,
        &command.owner.account,
        &command.owner.symbol,
    )?;
    let contracts = rules
        .native_order_contracts_checked(command.quantity)
        .map_err(|_| GateExecutionError::Rules)?;
    let client_order_id = command.client_order_id.as_str().to_owned();
    let body = PlaceBody {
        contract: &rules.native_symbol,
        size: decimal_wire(signed_contracts(contracts, command.side)),
        price: "0".to_owned(),
        tif: "ioc",
        reduce_only: false,
        text: native_client_id(&client_order_id)?,
    };
    prepared_place(
        binding,
        rules,
        GateMutationKind::PlaceMarket,
        body,
        ExpectedOrder {
            order_id: None,
            client_order_id,
            side: Some(command.side),
            position_side: Some(command.position_side),
            quantity: Some(command.quantity),
            limit_price: None,
            time_in_force: None,
            reduce_only: Some(false),
        },
        None,
    )
}

/// Creates Gate's native futures price-triggered close order. Gate encodes the hedge leg in
/// `order_type`; the trigger child is intrinsically reduce-only and quantity bounded.
pub fn prepare_stop_market(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    command: &StopMarketFullPositionCommand,
) -> Result<GatePreparedMutation, GateExecutionError> {
    command.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &command.owner.exchange,
        &command.owner.account,
        &command.owner.symbol,
    )?;
    validate_step(
        command.trigger_price.value(),
        rules.instrument.price_tick.value(),
    )?;
    let contracts = rules
        .native_order_contracts_checked(command.quantity)
        .map_err(|_| GateExecutionError::Rules)?;
    let trigger_rule = match (command.position_side, command.owner.purpose) {
        (PositionSide::Long, OrderPurpose::Protection)
        | (PositionSide::Short, OrderPurpose::TakeProfit) => 2,
        (PositionSide::Long, OrderPurpose::TakeProfit)
        | (PositionSide::Short, OrderPurpose::Protection) => 1,
        _ => return Err(GateExecutionError::Intent),
    };
    let body = TriggerBody {
        initial: TriggerInitial {
            contract: &rules.native_symbol,
            amount: decimal_wire(signed_contracts(contracts, command.side)),
            price: "0".to_owned(),
            tif: "ioc",
            text: native_client_id(command.client_algo_id.as_str())?,
            reduce_only: true,
        },
        trigger: TriggerSpec {
            strategy_type: 0,
            price_type: 1,
            price: decimal_wire(command.trigger_price.value()),
            rule: trigger_rule,
            expiration: 0,
        },
        order_type: if command.position_side == PositionSide::Long {
            "plan-close-long-position"
        } else {
            "plan-close-short-position"
        },
    };
    Ok(GatePreparedMutation {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        method: "POST",
        endpoint: endpoints::FUTURES_PRICE_ORDERS.to_owned(),
        body: serde_json::to_vec(&body).map_err(|_| GateExecutionError::Payload)?,
        kind: GateMutationKind::StopMarketFullPosition,
        expected: ExpectedOrder {
            order_id: None,
            client_order_id: command.client_algo_id.as_str().to_owned(),
            side: Some(command.side),
            position_side: Some(command.position_side),
            quantity: Some(command.quantity),
            limit_price: Some(command.trigger_price),
            time_in_force: None,
            reduce_only: Some(true),
        },
        expected_price: Some(ExpectedPriceOrder {
            order_id: None,
            client_order_id: command.client_algo_id.as_str().to_owned(),
            side: command.side,
            position_side: command.position_side,
            quantity: command.quantity,
            trigger_price: command.trigger_price,
            purpose: command.owner.purpose,
        }),
        reduce_episode: Some((
            command.client_algo_id.as_str().to_owned(),
            command.position_generation,
        )),
    })
}

pub fn prepare_reduce_once(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    command: &MarketReduceCommand,
) -> Result<GatePreparedMutation, GateExecutionError> {
    command.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &command.owner.exchange,
        &command.owner.account,
        &command.owner.symbol,
    )?;
    let contracts = rules
        .native_order_contracts_checked(command.quantity)
        .map_err(|_| GateExecutionError::Rules)?;
    let client_order_id = command.client_order_id.as_str().to_owned();
    let body = PlaceBody {
        contract: &rules.native_symbol,
        size: decimal_wire(signed_contracts(contracts, command.side)),
        price: "0".to_owned(),
        tif: "ioc",
        reduce_only: true,
        text: native_client_id(&client_order_id)?,
    };
    prepared_place(
        binding,
        rules,
        GateMutationKind::ReduceOnce,
        body,
        ExpectedOrder {
            order_id: None,
            client_order_id,
            side: Some(command.side),
            position_side: Some(command.position_side),
            quantity: Some(command.quantity),
            limit_price: None,
            time_in_force: None,
            reduce_only: Some(true),
        },
        Some((
            command.risk_episode_id.as_str().to_owned(),
            command.position_generation,
        )),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateCancelIntent {
    pub command: CancelCommand,
    pub venue_order_id: String,
}

pub fn prepare_cancel(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    intent: &GateCancelIntent,
) -> Result<GatePreparedMutation, GateExecutionError> {
    intent
        .command
        .validate()
        .map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &intent.command.owner.exchange,
        &intent.command.owner.account,
        &intent.command.owner.symbol,
    )?;
    if !valid_native_order_id(&intent.venue_order_id) {
        return Err(GateExecutionError::Intent);
    }
    let client_order_id = intent.command.target_client_order_id.as_str().to_owned();
    native_client_id(&client_order_id)?;
    Ok(GatePreparedMutation {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        method: "DELETE",
        endpoint: format!("{}/{}", endpoints::FUTURES_ORDER, intent.venue_order_id),
        body: Vec::new(),
        kind: GateMutationKind::Cancel,
        expected: ExpectedOrder {
            order_id: Some(intent.venue_order_id.clone()),
            client_order_id,
            side: None,
            position_side: None,
            quantity: None,
            limit_price: None,
            time_in_force: None,
            reduce_only: None,
        },
        expected_price: None,
        reduce_episode: None,
    })
}

fn prepared_place(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    kind: GateMutationKind,
    body: PlaceBody<'_>,
    expected: ExpectedOrder,
    reduce_episode: Option<(String, u64)>,
) -> Result<GatePreparedMutation, GateExecutionError> {
    Ok(GatePreparedMutation {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        method: "POST",
        endpoint: endpoints::FUTURES_ORDER.to_owned(),
        body: serde_json::to_vec(&body).map_err(|_| GateExecutionError::Payload)?,
        kind,
        expected,
        expected_price: None,
        reduce_episode,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateExactReadbackRequest {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub endpoint: String,
    pub not_before_ms: u64,
    pub mutation_kind: GateMutationKind,
    expected: ExpectedOrder,
    expected_price: Option<ExpectedPriceOrder>,
    ack_order: Option<Order>,
}

impl GateExactReadbackRequest {
    pub(crate) fn price_lookup_client(&self) -> Option<&str> {
        self.expected_price
            .as_ref()
            .filter(|value| value.order_id.is_none())
            .map(|value| value.client_order_id.as_str())
    }

    pub(crate) fn select_price_lookup(
        &self,
        payloads: &[String],
    ) -> Result<String, GateExecutionError> {
        let expected = native_client_id(
            self.price_lookup_client()
                .ok_or(GateExecutionError::Readback)?,
        )?;
        let mut found = None;
        for payload in payloads {
            let rows: Vec<Value> =
                serde_json::from_str(payload).map_err(|_| GateExecutionError::Readback)?;
            for row in rows {
                if row
                    .get("initial")
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("text"))
                    .and_then(Value::as_str)
                    == Some(expected.as_str())
                {
                    if found.is_some() {
                        return Err(GateExecutionError::Readback);
                    }
                    found = Some(
                        serde_json::to_string(&row).map_err(|_| GateExecutionError::Readback)?,
                    );
                }
            }
        }
        found.ok_or(GateExecutionError::Readback)
    }

    pub(crate) fn validate(
        &self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
    ) -> Result<(), GateExecutionError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| GateExecutionError::Binding)?;
        validate_scope(binding, rules, self.generation)?;
        if self.not_before_ms == 0
            || (!self.endpoint.starts_with(endpoints::FUTURES_ORDER)
                && !self.endpoint.starts_with(endpoints::FUTURES_PRICE_ORDERS))
        {
            return Err(GateExecutionError::Binding);
        }
        Ok(())
    }

    pub(crate) fn sign(
        &self,
        credentials: &GateCredentials,
        timestamp_sec: i64,
    ) -> Result<GateRestSignedHeaders, GateExecutionError> {
        sign_rest(credentials, timestamp_sec, "GET", &self.endpoint, "", &[])
            .map_err(|_| GateExecutionError::Signing)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateAcceptedMutation {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub kind: GateMutationKind,
    pub accepted_at_ms: u64,
    pub order: Order,
    pub readback: GateExactReadbackRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateDispatchUnknown {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub kind: GateMutationKind,
    pub unknown_at_ms: u64,
    pub readback: GateExactReadbackRequest,
}

pub(crate) fn parse_mutation_ack(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    request: GatePreparedMutation,
    payload: &[u8],
    received_at_ms: u64,
) -> Result<GateAcceptedMutation, GateExecutionError> {
    request.validate(binding, rules)?;
    if received_at_ms == 0 {
        return Err(GateExecutionError::Payload);
    }
    let value: Value = serde_json::from_slice(payload).map_err(|_| GateExecutionError::Payload)?;
    let order = if let Some(expected) = request.expected_price.as_ref() {
        if request.kind == GateMutationKind::StopMarketFullPosition {
            price_order_from_create_ack(&value, &rules.instrument.symbol, expected)?
        } else {
            parse_price_order(&value, &rules.instrument.symbol, rules, expected)?
        }
    } else {
        parse_regular_order(&value, &rules.instrument.symbol, rules)
            .map_err(GateExecutionError::Order)?
    };
    if !matches_expected(&order, &request.expected) {
        return Err(GateExecutionError::Binding);
    }
    let mut expected = request.expected;
    expected.order_id = Some(order.order_id.clone());
    expected.side = Some(order.side);
    expected.position_side = known_position_side(&order);
    expected.quantity = Some(order.quantity);
    expected.limit_price = order.limit_price;
    expected.time_in_force = known_time_in_force(&order);
    expected.reduce_only = Some(order.reduce_only);
    let mut expected_price = request.expected_price;
    if let Some(expected_price) = &mut expected_price {
        expected_price.order_id = Some(order.order_id.clone());
    }
    let readback = exact_readback(
        &request.binding,
        request.generation,
        request.kind,
        expected,
        expected_price,
        Some(order.clone()),
        received_at_ms,
    )?;
    Ok(GateAcceptedMutation {
        binding: request.binding,
        generation: request.generation,
        kind: request.kind,
        accepted_at_ms: received_at_ms,
        order,
        readback,
    })
}

pub(crate) fn mutation_unknown(
    request: GatePreparedMutation,
    unknown_at_ms: u64,
) -> Result<GateDispatchUnknown, GateExecutionError> {
    let readback = exact_readback(
        &request.binding,
        request.generation,
        request.kind,
        request.expected,
        request.expected_price,
        None,
        unknown_at_ms,
    )?;
    Ok(GateDispatchUnknown {
        binding: request.binding,
        generation: request.generation,
        kind: request.kind,
        unknown_at_ms,
        readback,
    })
}

fn exact_readback(
    binding: &GatewayBinding,
    generation: u64,
    mutation_kind: GateMutationKind,
    expected: ExpectedOrder,
    expected_price: Option<ExpectedPriceOrder>,
    ack_order: Option<Order>,
    not_before_ms: u64,
) -> Result<GateExactReadbackRequest, GateExecutionError> {
    if not_before_ms == 0 {
        return Err(GateExecutionError::Payload);
    }
    let identity = expected
        .order_id
        .clone()
        .unwrap_or(native_client_id(&expected.client_order_id)?);
    let family_endpoint = if expected_price.is_some() {
        endpoints::FUTURES_PRICE_ORDERS
    } else {
        endpoints::FUTURES_ORDER
    };
    Ok(GateExactReadbackRequest {
        binding: binding.clone(),
        generation,
        endpoint: format!("{family_endpoint}/{identity}"),
        not_before_ms,
        mutation_kind,
        expected,
        expected_price,
        ack_order,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateExactOrderReadback {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub requested_at_ms: u64,
    pub received_at_ms: u64,
    pub raw_payload: String,
    pub order: Order,
}

/// Builds an exact signed lookup for a durable client identity during WAL recovery. It grants no
/// mutation capability and deliberately carries no reconstructed place/cancel request.
pub fn prepare_exact_readback_by_client_id(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    client_order_id: &str,
) -> Result<GateExactReadbackRequest, GateExecutionError> {
    validate_scope(binding, rules, rules.instrument.generation)?;
    let native_client_id = native_client_id(client_order_id)?;
    exact_readback(
        binding.gateway_binding(),
        rules.instrument.generation,
        GateMutationKind::PlacePostOnly,
        ExpectedOrder {
            order_id: None,
            // The endpoint proves Gate's exact `t-{canonical}` wire identity; parsed orders
            // expose their canonical client id, which must still match the WAL command.
            client_order_id: client_order_id.to_owned(),
            side: None,
            position_side: None,
            quantity: None,
            limit_price: None,
            time_in_force: None,
            reduce_only: None,
        },
        None,
        None,
        1,
    )
    .map(|mut request| {
        request.endpoint = format!("{}/{}", endpoints::FUTURES_ORDER, native_client_id);
        request
    })
}

pub fn prepare_price_readback_by_client_id(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    command: &StopMarketFullPositionCommand,
) -> Result<GateExactReadbackRequest, GateExecutionError> {
    command.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &command.owner.exchange,
        &command.owner.account,
        &command.owner.symbol,
    )?;
    let expected_price = ExpectedPriceOrder {
        order_id: None,
        client_order_id: command.client_algo_id.as_str().to_owned(),
        side: command.side,
        position_side: command.position_side,
        quantity: command.quantity,
        trigger_price: command.trigger_price,
        purpose: command.owner.purpose,
    };
    exact_readback(
        binding.gateway_binding(),
        rules.instrument.generation,
        GateMutationKind::StopMarketFullPosition,
        ExpectedOrder {
            order_id: None,
            client_order_id: command.client_algo_id.as_str().to_owned(),
            side: Some(command.side),
            position_side: Some(command.position_side),
            quantity: Some(command.quantity),
            limit_price: Some(command.trigger_price),
            time_in_force: None,
            reduce_only: Some(true),
        },
        Some(expected_price),
        None,
        1,
    )
}

impl GateExactOrderReadback {
    pub fn from_response(
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        request: &GateExactReadbackRequest,
        requested_at_ms: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, GateExecutionError> {
        request.validate(binding, rules)?;
        if requested_at_ms < request.not_before_ms
            || received_at_ms < requested_at_ms
            || payload.is_empty()
        {
            return Err(GateExecutionError::Readback);
        }
        let value: Value =
            serde_json::from_str(&payload).map_err(|_| GateExecutionError::Readback)?;
        let order = if let Some(expected) = request.expected_price.as_ref() {
            parse_price_order(&value, &rules.instrument.symbol, rules, expected)?
        } else {
            parse_regular_order(&value, &rules.instrument.symbol, rules)
                .map_err(|_| GateExecutionError::Readback)?
        };
        if !matches_expected(&order, &request.expected)
            || request
                .ack_order
                .as_ref()
                .is_some_and(|ack| !same_order_semantics(ack, &order))
        {
            return Err(GateExecutionError::Readback);
        }
        Ok(Self {
            binding: request.binding.clone(),
            generation: request.generation,
            requested_at_ms,
            received_at_ms,
            raw_payload: payload,
            order,
        })
    }
}

fn price_order_from_create_ack(
    value: &Value,
    symbol: &venue_domain::domain::Symbol,
    expected: &ExpectedPriceOrder,
) -> Result<Order, GateExecutionError> {
    let object = value.as_object().ok_or(GateExecutionError::Payload)?;
    let order_id = object
        .get("id_string")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            object
                .get("id")
                .and_then(Value::as_i64)
                .map(|value| value.to_string())
        })
        .filter(|value| valid_native_order_id(value))
        .ok_or(GateExecutionError::Payload)?;
    Ok(expected_order(symbol, expected, order_id, OrderState::New))
}

fn parse_price_order(
    value: &Value,
    symbol: &venue_domain::domain::Symbol,
    rules: &GateContractRules,
    expected: &ExpectedPriceOrder,
) -> Result<Order, GateExecutionError> {
    let item = value.as_object().ok_or(GateExecutionError::Readback)?;
    let initial = item
        .get("initial")
        .and_then(Value::as_object)
        .ok_or(GateExecutionError::Readback)?;
    let trigger = item
        .get("trigger")
        .and_then(Value::as_object)
        .ok_or(GateExecutionError::Readback)?;
    let order_id = item
        .get("id_string")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            item.get("id")
                .and_then(Value::as_i64)
                .map(|value| value.to_string())
        })
        .filter(|value| valid_native_order_id(value))
        .ok_or(GateExecutionError::Readback)?;
    let native_text = initial
        .get("text")
        .and_then(Value::as_str)
        .ok_or(GateExecutionError::Readback)?;
    let client_id =
        canonical_client_id_from_native(native_text).ok_or(GateExecutionError::Readback)?;
    let contracts = value_decimal(initial.get("amount").or_else(|| initial.get("size")))?;
    let quantity = contracts
        .abs()
        .checked_mul(rules.quanto_multiplier)
        .ok_or(GateExecutionError::Readback)?;
    let trigger_price = Price::new(value_decimal(trigger.get("price"))?)
        .map_err(|_| GateExecutionError::Readback)?;
    let reduce_only = initial
        .get("is_reduce_only")
        .or_else(|| initial.get("reduce_only"))
        .and_then(Value::as_bool)
        .ok_or(GateExecutionError::Readback)?;
    let expected_order_type = if expected.position_side == PositionSide::Long {
        "plan-close-long-position"
    } else {
        "plan-close-short-position"
    };
    let expected_rule = match (expected.position_side, expected.purpose) {
        (PositionSide::Long, OrderPurpose::Protection)
        | (PositionSide::Short, OrderPurpose::TakeProfit) => 2,
        (PositionSide::Long, OrderPurpose::TakeProfit)
        | (PositionSide::Short, OrderPurpose::Protection) => 1,
        _ => return Err(GateExecutionError::Readback),
    };
    if expected
        .order_id
        .as_ref()
        .is_some_and(|value| value != &order_id)
        || initial.get("contract").and_then(Value::as_str) != Some(rules.native_symbol.as_str())
        || client_id != expected.client_order_id
        || initial.get("price").and_then(Value::as_str) != Some("0")
        || initial.get("tif").and_then(Value::as_str) != Some("ioc")
        || !reduce_only
        || quantity != expected.quantity
        || trigger_price != expected.trigger_price
        || item.get("order_type").and_then(Value::as_str) != Some(expected_order_type)
        || trigger.get("strategy_type").and_then(Value::as_u64) != Some(0)
        || trigger.get("price_type").and_then(Value::as_u64) != Some(1)
        || trigger.get("rule").and_then(Value::as_u64) != Some(expected_rule)
        || (contracts.is_sign_positive() && expected.side != OrderSide::Buy)
        || (contracts.is_sign_negative() && expected.side != OrderSide::Sell)
    {
        return Err(GateExecutionError::Readback);
    }
    let state = match item.get("status").and_then(Value::as_str) {
        Some("open") => OrderState::New,
        Some("finished") => match item.get("finish_as").and_then(Value::as_str) {
            Some("succeeded") => OrderState::Filled,
            Some("cancelled" | "canceled") => OrderState::Cancelled,
            Some("expired") => OrderState::Expired,
            Some("failed") => OrderState::Rejected,
            _ => return Err(GateExecutionError::Readback),
        },
        _ => return Err(GateExecutionError::Readback),
    };
    let order = expected_order(symbol, expected, order_id, state);
    order.validate().map_err(|_| GateExecutionError::Readback)?;
    Ok(order)
}

fn expected_order(
    symbol: &venue_domain::domain::Symbol,
    expected: &ExpectedPriceOrder,
    order_id: String,
    state: OrderState,
) -> Order {
    Order {
        order_id,
        client_order_id: FieldState::Known(expected.client_order_id.clone()),
        symbol: symbol.clone(),
        side: expected.side,
        position_side: FieldState::Known(expected.position_side),
        purpose: FieldState::Known(expected.purpose),
        state,
        quantity: expected.quantity,
        filled_quantity: if state == OrderState::Filled {
            expected.quantity
        } else {
            Decimal::ZERO
        },
        limit_price: Some(expected.trigger_price),
        time_in_force: FieldState::NotApplicable,
        average_price: FieldState::Missing,
        reduce_only: true,
    }
}

impl GateExactReadbackRequest {
    pub(crate) fn finished_trigger_trade_id(
        &self,
        payload: &str,
    ) -> Result<Option<String>, GateExecutionError> {
        if self.expected_price.is_none() {
            return Ok(None);
        }
        let value: Value =
            serde_json::from_str(payload).map_err(|_| GateExecutionError::Readback)?;
        if value.get("status").and_then(Value::as_str) == Some("finished")
            && value.get("finish_as").and_then(Value::as_str) == Some("succeeded")
        {
            return value
                .get("trade_id")
                .and_then(|value| match value {
                    Value::String(value) => Some(value.clone()),
                    Value::Number(value) => Some(value.to_string()),
                    _ => None,
                })
                .filter(|value| valid_native_order_id(value))
                .map(Some)
                .ok_or(GateExecutionError::Readback);
        }
        Ok(None)
    }

    pub(crate) fn validate_trigger_child(
        &self,
        rules: &GateContractRules,
        payload: &str,
    ) -> Result<(), GateExecutionError> {
        let expected = self
            .expected_price
            .as_ref()
            .ok_or(GateExecutionError::Readback)?;
        let value: Value =
            serde_json::from_str(payload).map_err(|_| GateExecutionError::Readback)?;
        let order = parse_regular_order(&value, &rules.instrument.symbol, rules)
            .map_err(|_| GateExecutionError::Readback)?;
        if order.side != expected.side
            || known_position_side(&order) != Some(expected.position_side)
            || order.quantity != expected.quantity
            || !order.reduce_only
        {
            return Err(GateExecutionError::Readback);
        }
        Ok(())
    }
}

fn value_decimal(value: Option<&Value>) -> Result<Decimal, GateExecutionError> {
    match value {
        Some(Value::String(value)) => value.parse().map_err(|_| GateExecutionError::Readback),
        Some(Value::Number(value)) => value
            .to_string()
            .parse()
            .map_err(|_| GateExecutionError::Readback),
        _ => Err(GateExecutionError::Readback),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateSettlementFinality {
    Working,
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateMutationSettlement {
    pub kind: GateMutationKind,
    pub order: Order,
    pub finality: GateSettlementFinality,
    pub settled_at_ms: u64,
}

pub fn settle_exact_readback(
    request: &GateExactReadbackRequest,
    readback: &GateExactOrderReadback,
) -> Result<GateMutationSettlement, GateExecutionError> {
    if readback.binding != request.binding
        || readback.generation != request.generation
        || readback.requested_at_ms < request.not_before_ms
        || !matches_expected(&readback.order, &request.expected)
    {
        return Err(GateExecutionError::Binding);
    }
    let finality = if matches!(
        readback.order.state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    ) {
        GateSettlementFinality::Terminal
    } else {
        GateSettlementFinality::Working
    };
    if matches!(
        request.mutation_kind,
        GateMutationKind::Cancel | GateMutationKind::CancelPrice
    ) && finality != GateSettlementFinality::Terminal
    {
        return Err(GateExecutionError::Unsettled);
    }
    Ok(GateMutationSettlement {
        kind: request.mutation_kind,
        order: readback.order.clone(),
        finality,
        settled_at_ms: readback.received_at_ms,
    })
}

fn validate_scope(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    generation: u64,
) -> Result<(), GateExecutionError> {
    if binding.gateway_binding().validate().is_err()
        || generation == 0
        || generation != rules.instrument.generation
        || rules.instrument.validate().is_err()
        || rules.native_symbol.trim().is_empty()
        || rules.quanto_multiplier <= Decimal::ZERO
    {
        return Err(GateExecutionError::Binding);
    }
    Ok(())
}

fn validate_owner(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    exchange: &str,
    account: &str,
    symbol: &venue_domain::domain::Symbol,
) -> Result<(), GateExecutionError> {
    validate_scope(binding, rules, rules.instrument.generation)?;
    if exchange != "gate"
        || account != binding.gateway_binding().trading_account_id.as_str()
        || symbol != &rules.instrument.symbol
    {
        return Err(GateExecutionError::Binding);
    }
    Ok(())
}

fn matches_expected(order: &Order, expected: &ExpectedOrder) -> bool {
    expected
        .order_id
        .as_ref()
        .is_none_or(|value| value == &order.order_id)
        && matches!(
            &order.client_order_id,
            FieldState::Known(actual) if actual == &expected.client_order_id
        )
        && expected.side.is_none_or(|value| value == order.side)
        && expected
            .position_side
            .is_none_or(|value| Some(value) == known_position_side(order))
        && expected
            .quantity
            .is_none_or(|value| value == order.quantity)
        && expected
            .limit_price
            .is_none_or(|value| Some(value) == order.limit_price)
        && expected
            .time_in_force
            .is_none_or(|value| Some(value) == known_time_in_force(order))
        && expected
            .reduce_only
            .is_none_or(|value| value == order.reduce_only)
}

fn same_order_semantics(left: &Order, right: &Order) -> bool {
    left.order_id == right.order_id
        && left.client_order_id == right.client_order_id
        && left.symbol == right.symbol
        && left.side == right.side
        && left.position_side == right.position_side
        && left.quantity == right.quantity
        && left.limit_price == right.limit_price
        && left.time_in_force == right.time_in_force
        && left.reduce_only == right.reduce_only
}

const fn limit_time_in_force_wire(value: LimitTimeInForce) -> &'static str {
    match value {
        LimitTimeInForce::PostOnly => "poc",
        LimitTimeInForce::Gtc => "gtc",
    }
}

fn known_time_in_force(order: &Order) -> Option<LimitTimeInForce> {
    match order.time_in_force {
        FieldState::Known(value) => Some(value),
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => None,
    }
}

fn known_position_side(order: &Order) -> Option<PositionSide> {
    match order.position_side {
        FieldState::Known(value) => Some(value),
        _ => None,
    }
}

fn validate_step(value: Decimal, step: Decimal) -> Result<(), GateExecutionError> {
    if value > Decimal::ZERO && step > Decimal::ZERO && value % step == Decimal::ZERO {
        Ok(())
    } else {
        Err(GateExecutionError::Rules)
    }
}

fn signed_contracts(contracts: Decimal, side: OrderSide) -> Decimal {
    match side {
        OrderSide::Buy => contracts,
        OrderSide::Sell => -contracts,
    }
}

pub(crate) fn native_client_id(client_order_id: &str) -> Result<String, GateExecutionError> {
    if !(1..=MAX_CLIENT_ORDER_SUFFIX_BYTES).contains(&client_order_id.len())
        || !client_order_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(GateExecutionError::ClientOrderId);
    }
    Ok(format!("t-{client_order_id}"))
}

/// Returns a canonical WAL client id only for the exact native form emitted by this adapter.
/// Other `text` values remain external identities; a prefix alone never proves ownership.
pub(crate) fn canonical_client_id_from_native(native: &str) -> Option<String> {
    let canonical = native.strip_prefix("t-")?;
    native_client_id(canonical)
        .ok()
        .filter(|encoded| encoded == native)
        .map(|_| canonical.to_owned())
}

fn valid_native_order_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn decimal_wire(value: Decimal) -> String {
    value.normalize().to_string()
}

#[derive(Serialize)]
struct PlaceBody<'a> {
    contract: &'a str,
    size: String,
    price: String,
    tif: &'static str,
    reduce_only: bool,
    text: String,
}

pub fn prepare_price_cancel(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    cancel: &CancelCommand,
    target: &StopMarketFullPositionCommand,
    venue_order_id: &str,
) -> Result<GatePreparedMutation, GateExecutionError> {
    cancel.validate().map_err(|_| GateExecutionError::Intent)?;
    target.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &cancel.owner.exchange,
        &cancel.owner.account,
        &cancel.owner.symbol,
    )?;
    validate_owner(
        binding,
        rules,
        &target.owner.exchange,
        &target.owner.account,
        &target.owner.symbol,
    )?;
    if cancel.target_client_order_id.as_str() != target.client_algo_id.as_str()
        || !valid_native_order_id(venue_order_id)
    {
        return Err(GateExecutionError::Intent);
    }
    let expected_price = ExpectedPriceOrder {
        order_id: Some(venue_order_id.to_owned()),
        client_order_id: target.client_algo_id.as_str().to_owned(),
        side: target.side,
        position_side: target.position_side,
        quantity: target.quantity,
        trigger_price: target.trigger_price,
        purpose: target.owner.purpose,
    };
    Ok(GatePreparedMutation {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        method: "DELETE",
        endpoint: format!("{}/{}", endpoints::FUTURES_PRICE_ORDERS, venue_order_id),
        body: Vec::new(),
        kind: GateMutationKind::CancelPrice,
        expected: ExpectedOrder {
            order_id: Some(venue_order_id.to_owned()),
            client_order_id: target.client_algo_id.as_str().to_owned(),
            side: Some(target.side),
            position_side: Some(target.position_side),
            quantity: Some(target.quantity),
            limit_price: Some(target.trigger_price),
            time_in_force: None,
            reduce_only: Some(true),
        },
        expected_price: Some(expected_price),
        reduce_episode: None,
    })
}

#[derive(Serialize)]
struct TriggerBody<'a> {
    initial: TriggerInitial<'a>,
    trigger: TriggerSpec,
    order_type: &'static str,
}
#[derive(Serialize)]
struct TriggerInitial<'a> {
    contract: &'a str,
    amount: String,
    price: String,
    tif: &'static str,
    text: String,
    reduce_only: bool,
}
#[derive(Serialize)]
struct TriggerSpec {
    strategy_type: u8,
    price_type: u8,
    price: String,
    rule: u8,
    expiration: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateExecutionError {
    #[error("Gate mutation does not match the exact gateway binding, owner, or generation")]
    Binding,
    #[error("Gate mutation intent is invalid or ambiguous")]
    Intent,
    #[error("Gate mutation violates current contract quantity, tick, or notional rules")]
    Rules,
    #[error("Gate client order identity must be 1..=28 safe ASCII bytes")]
    ClientOrderId,
    #[error("Gate mutation payload or acknowledgement is invalid")]
    Payload,
    #[error("Gate rejected the mutation request")]
    VenueRejected,
    #[error("Gate mutation or exact readback could not be signed")]
    Signing,
    #[error("Gate exact signed readback is missing, conflicting, or stale")]
    Readback,
    #[error("Gate mutation remains UNKNOWN and must not be retried")]
    Unsettled,
    #[error(transparent)]
    Order(#[from] GateOrderPayloadError),
}

impl From<GateProtocolError> for GateExecutionError {
    fn from(_: GateProtocolError) -> Self {
        Self::Signing
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{
        Amount, CommandId, Instrument, MarketKind, OrderOwner, OrderPurpose,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    fn facts() -> Result<(GateGatewayBinding, GateContractRules), Box<dyn std::error::Error>> {
        let binding = GateGatewayBinding::new(GatewayBinding::new(
            VenueId::Gate,
            GatewayMode::Live,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?)?;
        let rules = GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                symbol: "DOGE/USDT".parse()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some("USDT".parse()?),
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            maximum_contracts: Some(Decimal::from(1000)),
            decimal_contracts: false,
        };
        Ok((binding, rules))
    }

    fn owner(purpose: OrderPurpose) -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "grid".to_owned(),
            run_id: "run".to_owned(),
            exchange: "gate".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose,
        })
    }

    fn limit() -> Result<OrderCommand, Box<dyn std::error::Error>> {
        Ok(OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("command")?,
            client_order_id: CommandId::new("grid_long_1")?,
            owner: owner(OrderPurpose::Entry)?,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::new(1, 1))?,
            reduce_only: false,
        })
    }

    fn stop() -> Result<StopMarketFullPositionCommand, Box<dyn std::error::Error>> {
        Ok(StopMarketFullPositionCommand {
            command_id: CommandId::new("stop-command")?,
            client_algo_id: CommandId::new("protect1")?,
            owner: owner(OrderPurpose::Protection)?,
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            trigger_price: Price::new(Decimal::new(9, 2))?,
            position_generation: 9,
        })
    }

    fn ack_payload(status: &str, finish_as: &str) -> String {
        format!(
            r#"{{"id":"9001","contract":"DOGE_USDT","size":"10","left":"10","is_reduce_only":false,"tif":"poc","status":"{status}","finish_as":"{finish_as}","price":"0.1","fill_price":"0","text":"t-grid_long_1"}}"#
        )
    }

    #[test]
    fn post_only_preserves_contract_count_poc_reduce_and_client_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let request = prepare_limit(&binding, &rules, &limit()?)?;
        assert_eq!(request.kind(), GateMutationKind::PlacePostOnly);
        assert_eq!(
            std::str::from_utf8(request.body())?,
            r#"{"contract":"DOGE_USDT","size":"10","price":"0.1","tif":"poc","reduce_only":false,"text":"t-grid_long_1"}"#
        );
        Ok(())
    }

    #[test]
    fn recovery_lookup_binds_canonical_id_to_exact_native_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let request = prepare_exact_readback_by_client_id(&binding, &rules, "grid_long_1")?;
        assert_eq!(request.endpoint, "/futures/usdt/orders/t-grid_long_1");
        let readback = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &request,
            1_000,
            1_001,
            ack_payload("open", ""),
        )?;
        assert_eq!(
            readback.order.client_order_id,
            FieldState::Known("grid_long_1".to_owned())
        );
        assert_eq!(
            canonical_client_id_from_native("t-grid_long_1"),
            Some("grid_long_1".to_owned())
        );
        assert_eq!(canonical_client_id_from_native("t-bad space"), None);
        Ok(())
    }

    #[test]
    fn gtc_wire_and_exact_readback_policy_are_bound() -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let mut command = limit()?;
        command.time_in_force = LimitTimeInForce::Gtc;
        let request = prepare_limit(&binding, &rules, &command)?;
        assert_eq!(
            std::str::from_utf8(request.body())?,
            r#"{"contract":"DOGE_USDT","size":"10","price":"0.1","tif":"gtc","reduce_only":false,"text":"t-grid_long_1"}"#
        );
        let accepted = parse_mutation_ack(
            &binding,
            &rules,
            request,
            ack_payload("open", "")
                .replace("\"poc\"", "\"gtc\"")
                .as_bytes(),
            1_000,
        )?;
        let exact = ack_payload("open", "").replace("\"poc\"", "\"gtc\"");
        let readback = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &accepted.readback,
            1_001,
            1_002,
            exact.clone(),
        )?;
        assert_eq!(
            settle_exact_readback(&accepted.readback, &readback)?.finality,
            GateSettlementFinality::Working
        );
        let mismatched = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &accepted.readback,
            1_003,
            1_004,
            ack_payload("open", ""),
        );
        assert_eq!(mismatched, Err(GateExecutionError::Readback));
        let missing = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &accepted.readback,
            1_003,
            1_004,
            exact.replace("\"tif\":\"gtc\",", ""),
        );
        assert_eq!(missing, Err(GateExecutionError::Readback));
        Ok(())
    }

    #[test]
    fn ack_is_only_accepted_until_a_later_exact_signed_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let accepted = parse_mutation_ack(
            &binding,
            &rules,
            prepare_limit(&binding, &rules, &limit()?)?,
            ack_payload("open", "").as_bytes(),
            1_000,
        )?;
        assert_eq!(accepted.readback.endpoint, "/futures/usdt/orders/9001");
        let readback = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &accepted.readback,
            1_001,
            1_002,
            ack_payload("open", ""),
        )?;
        let settled = settle_exact_readback(&accepted.readback, &readback)?;
        assert_eq!(settled.finality, GateSettlementFinality::Working);
        Ok(())
    }

    #[test]
    fn timeout_plan_uses_exact_client_readback_and_never_reconstructs_a_place()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let unknown = mutation_unknown(prepare_limit(&binding, &rules, &limit()?)?, 1_000)?;
        assert_eq!(
            unknown.readback.endpoint,
            "/futures/usdt/orders/t-grid_long_1"
        );
        assert_eq!(unknown.kind, GateMutationKind::PlacePostOnly);
        Ok(())
    }

    #[test]
    fn quantity_direction_and_client_id_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let mut command = limit()?;
        command.quantity = Decimal::new(15, 2);
        assert_eq!(
            prepare_limit(&binding, &rules, &command),
            Err(GateExecutionError::Rules)
        );
        let mut command = limit()?;
        command.client_order_id = CommandId::new("12345678901234567890123456789")?;
        assert_eq!(
            prepare_limit(&binding, &rules, &command),
            Err(GateExecutionError::ClientOrderId)
        );
        Ok(())
    }

    #[test]
    fn reduce_once_and_cancel_preserve_exact_native_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let reduce = MarketReduceCommand {
            command_id: CommandId::new("reduce")?,
            client_order_id: CommandId::new("ord-etp-l-0000000000000001")?,
            owner: owner(OrderPurpose::ExposureTakeProfit)?,
            position_side: PositionSide::Long,
            side: OrderSide::Sell,
            quantity: Decimal::ONE,
            risk_episode_id: CommandId::new("episode")?,
            position_generation: 9,
        };
        let request = prepare_reduce_once(&binding, &rules, &reduce)?;
        assert_eq!(request.kind(), GateMutationKind::ReduceOnce);
        assert_eq!(request.reduce_episode_id(), Some("episode"));
        assert_eq!(request.position_generation(), Some(9));
        assert_eq!(
            std::str::from_utf8(request.body())?,
            r#"{"contract":"DOGE_USDT","size":"-10","price":"0","tif":"ioc","reduce_only":true,"text":"t-ord-etp-l-0000000000000001"}"#
        );

        let cancel = GateCancelIntent {
            command: CancelCommand {
                command_id: CommandId::new("cancel")?,
                owner: owner(OrderPurpose::Entry)?,
                target_client_order_id: CommandId::new("grid_long_1")?,
            },
            venue_order_id: "9001".to_owned(),
        };
        let cancel = prepare_cancel(&binding, &rules, &cancel)?;
        assert_eq!(cancel.kind(), GateMutationKind::Cancel);
        assert_eq!(cancel.endpoint(), "/futures/usdt/orders/9001");
        assert!(cancel.body().is_empty());
        let accepted = parse_mutation_ack(
            &binding,
            &rules,
            cancel,
            ack_payload("finished", "cancelled").as_bytes(),
            2_000,
        )?;
        let readback = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &accepted.readback,
            2_001,
            2_002,
            ack_payload("finished", "cancelled"),
        )?;
        assert_eq!(
            settle_exact_readback(&accepted.readback, &readback)?.finality,
            GateSettlementFinality::Terminal
        );
        Ok(())
    }

    #[test]
    fn unknown_price_create_scans_history_and_requires_matching_trigger_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let command = stop()?;
        let prepared = prepare_stop_market(&binding, &rules, &command)?;
        assert_eq!(prepared.endpoint(), "/futures/usdt/price_orders");
        let wire: Value = serde_json::from_slice(prepared.body())?;
        assert_eq!(wire["initial"]["amount"], "-10");
        assert_eq!(wire["initial"]["reduce_only"], true);
        assert_eq!(wire["initial"]["text"], "t-protect1");
        assert_eq!(wire["trigger"]["price_type"], 1);
        assert_eq!(wire["trigger"]["rule"], 2);
        assert_eq!(wire["order_type"], "plan-close-long-position");

        let request = prepare_price_readback_by_client_id(&binding, &rules, &command)?;
        let selected = request.select_price_lookup(&[
            "[]".to_owned(),
            include_str!("../tests/fixtures/gate_price_orders_finished.json").to_owned(),
        ])?;
        assert_eq!(
            request.finished_trigger_trade_id(&selected)?.as_deref(),
            Some("9002")
        );
        request.validate_trigger_child(
            &rules,
            include_str!("../tests/fixtures/gate_trigger_child_filled.json"),
        )?;
        let wrong_child = include_str!("../tests/fixtures/gate_trigger_child_filled.json")
            .replace("\"is_reduce_only\": true", "\"is_reduce_only\": false");
        assert_eq!(
            request.validate_trigger_child(&rules, &wrong_child),
            Err(GateExecutionError::Readback)
        );

        let mut missing_trade: Value = serde_json::from_str(&selected)?;
        missing_trade["trade_id"] = Value::Null;
        assert_eq!(
            request.finished_trigger_trade_id(&serde_json::to_string(&missing_trade)?),
            Err(GateExecutionError::Readback)
        );
        Ok(())
    }
}
