//! Request-bound UTA v3 normal-order mutations and exact post-dispatch readback.

use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    FieldState, Order, OrderSide, OrderState, Position, PositionSide, Price,
};
use venue_gateway_api::{GatewayBinding, GatewayMode};

use crate::{
    BitgetAccountBinding, BitgetConfig, BitgetCredentials, SignInput, SignedHeaders, endpoints,
    instrument::BitgetInstrumentRules, private::parse_regular_order, sign,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetTimeInForce {
    GoodTillCancelled,
    ImmediateOrCancel,
    FillOrKill,
    PostOnly,
}

impl BitgetTimeInForce {
    const fn wire(self) -> &'static str {
        match self {
            Self::GoodTillCancelled => "gtc",
            Self::ImmediateOrCancel => "ioc",
            Self::FillOrKill => "fok",
            Self::PostOnly => "post_only",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetPlaceIntent {
    pub client_order_id: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub limit_price: Price,
    pub time_in_force: BitgetTimeInForce,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetReduceOnceIntent {
    pub client_order_id: String,
    pub position_side: PositionSide,
    pub quantity: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetCancelIntent {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetMutationKind {
    Place,
    Cancel,
    ReduceOnce,
}

/// A prepared request is intentionally non-Clone and is consumed by the one-shot transport.
#[derive(Debug, Eq, PartialEq)]
pub struct BitgetPreparedMutation {
    pub binding: GatewayBinding,
    pub attempt_id: u64,
    pub generation: u64,
    pub kind: BitgetMutationKind,
    pub(crate) path: &'static str,
    pub(crate) body: Vec<u8>,
    pub(crate) expected_order_id: Option<String>,
    pub(crate) expected_client_order_id: Option<String>,
}

impl BitgetPreparedMutation {
    pub(crate) fn validate(&self, config: &BitgetConfig) -> Result<(), BitgetExecutionError> {
        validate_binding(&self.binding, config)?;
        let expected_path = match self.kind {
            BitgetMutationKind::Place | BitgetMutationKind::ReduceOnce => endpoints::PLACE_ORDER,
            BitgetMutationKind::Cancel => endpoints::CANCEL_ORDER,
        };
        if self.attempt_id == 0
            || self.generation == 0
            || self.path != expected_path
            || self.body.is_empty()
        {
            return Err(BitgetExecutionError::Binding);
        }
        Ok(())
    }

    #[must_use]
    pub fn client_order_id(&self) -> Option<&str> {
        self.expected_client_order_id.as_deref()
    }
}

pub fn prepare_place_request(
    binding: &GatewayBinding,
    config: &BitgetConfig,
    rules: &BitgetInstrumentRules,
    attempt_id: u64,
    intent: &BitgetPlaceIntent,
    now_ms: u64,
) -> Result<BitgetPreparedMutation, BitgetExecutionError> {
    validate_scope(binding, config, rules, attempt_id, now_ms)?;
    validate_client_order_id(&intent.client_order_id)?;
    validate_hedge_direction(intent.position_side, intent.side, intent.reduce_only)?;
    validate_quantity(rules, intent.quantity, false)?;
    if !rules
        .snapshot
        .metadata
        .price
        .accepts(intent.limit_price.value())
        .map_err(|_| BitgetExecutionError::Rules)?
    {
        return Err(BitgetExecutionError::Rules);
    }
    let notional = intent
        .quantity
        .checked_mul(intent.limit_price.value())
        .ok_or(BitgetExecutionError::Rules)?;
    if !intent.reduce_only && notional < rules.snapshot.metadata.instrument.minimum_notional.value {
        return Err(BitgetExecutionError::Rules);
    }
    let body = PlaceWire {
        category: crate::private::BITGET_UTA_FUTURES_CATEGORY,
        symbol: rules.native_symbol(),
        client_oid: &intent.client_order_id,
        side: side_wire(intent.side),
        pos_side: position_side_wire(intent.position_side)?,
        order_type: "limit",
        qty: decimal_wire(intent.quantity),
        price: Some(decimal_wire(intent.limit_price.value())),
        time_in_force: Some(intent.time_in_force.wire()),
    };
    prepared_place(
        binding,
        attempt_id,
        rules.snapshot.metadata.instrument.generation,
        BitgetMutationKind::Place,
        &intent.client_order_id,
        body,
    )
}

/// Builds one market reduction request bound to a signed Hedge leg. It cannot increase exposure.
pub fn prepare_reduce_once_request(
    binding: &GatewayBinding,
    config: &BitgetConfig,
    rules: &BitgetInstrumentRules,
    attempt_id: u64,
    intent: &BitgetReduceOnceIntent,
    signed_position: &Position,
    now_ms: u64,
) -> Result<BitgetPreparedMutation, BitgetExecutionError> {
    validate_scope(binding, config, rules, attempt_id, now_ms)?;
    validate_client_order_id(&intent.client_order_id)?;
    if intent.position_side == PositionSide::Net
        || signed_position.symbol != binding.symbol
        || signed_position.side != intent.position_side
        || signed_position.quantity <= Decimal::ZERO
        || intent.quantity > signed_position.quantity
    {
        return Err(BitgetExecutionError::Position);
    }
    validate_quantity(rules, intent.quantity, true)?;
    let side = close_side(intent.position_side)?;
    validate_hedge_direction(intent.position_side, side, true)?;
    let body = PlaceWire {
        category: crate::private::BITGET_UTA_FUTURES_CATEGORY,
        symbol: rules.native_symbol(),
        client_oid: &intent.client_order_id,
        side: side_wire(side),
        pos_side: position_side_wire(intent.position_side)?,
        order_type: "market",
        qty: decimal_wire(intent.quantity),
        price: None,
        time_in_force: None,
    };
    prepared_place(
        binding,
        attempt_id,
        rules.snapshot.metadata.instrument.generation,
        BitgetMutationKind::ReduceOnce,
        &intent.client_order_id,
        body,
    )
}

pub fn prepare_cancel_request(
    binding: &GatewayBinding,
    config: &BitgetConfig,
    generation: u64,
    attempt_id: u64,
    intent: &BitgetCancelIntent,
) -> Result<BitgetPreparedMutation, BitgetExecutionError> {
    validate_binding(binding, config)?;
    if generation == 0 || attempt_id == 0 {
        return Err(BitgetExecutionError::Binding);
    }
    let (order_id, client_order_id) = match (&intent.order_id, &intent.client_order_id) {
        (Some(order_id), None) if valid_native_id(order_id) => (Some(order_id.as_str()), None),
        (None, Some(client_order_id)) => {
            validate_client_order_id(client_order_id)?;
            (None, Some(client_order_id.as_str()))
        }
        _ => return Err(BitgetExecutionError::Identity),
    };
    let body = serde_json::to_vec(&CancelWire {
        category: crate::private::BITGET_UTA_FUTURES_CATEGORY,
        order_id,
        client_oid: client_order_id,
    })
    .map_err(|_| BitgetExecutionError::Payload)?;
    Ok(BitgetPreparedMutation {
        binding: binding.clone(),
        attempt_id,
        generation,
        kind: BitgetMutationKind::Cancel,
        path: endpoints::CANCEL_ORDER,
        body,
        expected_order_id: intent.order_id.clone(),
        expected_client_order_id: intent.client_order_id.clone(),
    })
}

fn prepared_place(
    binding: &GatewayBinding,
    attempt_id: u64,
    generation: u64,
    kind: BitgetMutationKind,
    client_order_id: &str,
    body: PlaceWire<'_>,
) -> Result<BitgetPreparedMutation, BitgetExecutionError> {
    Ok(BitgetPreparedMutation {
        binding: binding.clone(),
        attempt_id,
        generation,
        kind,
        path: endpoints::PLACE_ORDER,
        body: serde_json::to_vec(&body).map_err(|_| BitgetExecutionError::Payload)?,
        expected_order_id: None,
        expected_client_order_id: Some(client_order_id.to_owned()),
    })
}

pub fn sign_prepared_mutation(
    credentials: &BitgetCredentials,
    config: &BitgetConfig,
    request: &BitgetPreparedMutation,
    timestamp_ms: u64,
) -> Result<SignedHeaders, BitgetExecutionError> {
    request.validate(config)?;
    sign(
        credentials,
        config,
        &SignInput {
            timestamp_ms,
            method: "POST",
            request_path: request.path,
            query: "",
            body: &request.body,
        },
    )
    .map_err(|_| BitgetExecutionError::Signing)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetAckStatus {
    AcceptedOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetMutationAck {
    pub binding: GatewayBinding,
    pub attempt_id: u64,
    pub generation: u64,
    pub kind: BitgetMutationKind,
    pub order_id: String,
    pub client_order_id: String,
    pub accepted_at_ms: u64,
    pub received_at_ms: u64,
    pub status: BitgetAckStatus,
    pub payload_sha256: String,
    pub raw_payload: Vec<u8>,
}

pub fn parse_mutation_ack(
    config: &BitgetConfig,
    request: &BitgetPreparedMutation,
    payload: &[u8],
    received_at_ms: u64,
) -> Result<BitgetMutationAck, BitgetExecutionError> {
    request.validate(config)?;
    let root: Value = serde_json::from_slice(payload).map_err(|_| BitgetExecutionError::Payload)?;
    let object = root.as_object().ok_or(BitgetExecutionError::Payload)?;
    if object.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(BitgetExecutionError::VenueRejected);
    }
    let accepted_at_ms = unsigned(object.get("requestTime"))?;
    if accepted_at_ms == 0 || received_at_ms < accepted_at_ms {
        return Err(BitgetExecutionError::Clock);
    }
    let data = object
        .get("data")
        .and_then(Value::as_object)
        .ok_or(BitgetExecutionError::Payload)?;
    let order_id = identifier(data.get("orderId"))?;
    let client_order_id = identifier(data.get("clientOid"))?;
    if request
        .expected_order_id
        .as_ref()
        .is_some_and(|expected| expected != &order_id)
        || request
            .expected_client_order_id
            .as_ref()
            .is_some_and(|expected| expected != &client_order_id)
    {
        return Err(BitgetExecutionError::Identity);
    }
    Ok(BitgetMutationAck {
        binding: request.binding.clone(),
        attempt_id: request.attempt_id,
        generation: request.generation,
        kind: request.kind,
        order_id,
        client_order_id,
        accepted_at_ms,
        received_at_ms,
        status: BitgetAckStatus::AcceptedOnly,
        payload_sha256: payload_digest(payload),
        raw_payload: payload.to_vec(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetUnknownReason {
    Timeout,
    Disconnected,
    AmbiguousResponse,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetUnknownMutation {
    pub binding: GatewayBinding,
    pub attempt_id: u64,
    pub generation: u64,
    pub kind: BitgetMutationKind,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub dispatched_at_ms: u64,
    pub reason: BitgetUnknownReason,
}

pub(crate) fn into_unknown(
    request: BitgetPreparedMutation,
    dispatched_at_ms: u64,
    reason: BitgetUnknownReason,
) -> BitgetUnknownMutation {
    BitgetUnknownMutation {
        binding: request.binding,
        attempt_id: request.attempt_id,
        generation: request.generation,
        kind: request.kind,
        order_id: request.expected_order_id,
        client_order_id: request.expected_client_order_id,
        dispatched_at_ms,
        reason,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitgetMutationOutcome {
    Acknowledged(BitgetMutationAck),
    Rejected,
    Unknown(BitgetUnknownMutation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitgetOrderLookup {
    OrderId(String),
    ClientOrderId(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetExactReadbackRequest {
    pub binding: GatewayBinding,
    pub attempt_id: u64,
    pub generation: u64,
    pub lookup: BitgetOrderLookup,
    pub not_before_ms: u64,
    pub expected_kind: BitgetMutationKind,
    pub(crate) query: String,
}

pub fn build_ack_readback_request(
    ack: &BitgetMutationAck,
) -> Result<BitgetExactReadbackRequest, BitgetExecutionError> {
    build_readback(
        &ack.binding,
        ack.attempt_id,
        ack.generation,
        BitgetOrderLookup::OrderId(ack.order_id.clone()),
        ack.received_at_ms,
        ack.kind,
    )
}

pub fn build_unknown_readback_request(
    unknown: &BitgetUnknownMutation,
) -> Result<BitgetExactReadbackRequest, BitgetExecutionError> {
    let lookup = match (&unknown.order_id, &unknown.client_order_id) {
        (Some(order_id), _) => BitgetOrderLookup::OrderId(order_id.clone()),
        (None, Some(client_order_id)) => BitgetOrderLookup::ClientOrderId(client_order_id.clone()),
        (None, None) => return Err(BitgetExecutionError::Identity),
    };
    build_readback(
        &unknown.binding,
        unknown.attempt_id,
        unknown.generation,
        lookup,
        unknown.dispatched_at_ms,
        unknown.kind,
    )
}

fn build_readback(
    binding: &GatewayBinding,
    attempt_id: u64,
    generation: u64,
    lookup: BitgetOrderLookup,
    not_before_ms: u64,
    expected_kind: BitgetMutationKind,
) -> Result<BitgetExactReadbackRequest, BitgetExecutionError> {
    if attempt_id == 0 || generation == 0 || not_before_ms == 0 {
        return Err(BitgetExecutionError::Binding);
    }
    let query = match &lookup {
        BitgetOrderLookup::OrderId(value) if valid_native_id(value) => {
            format!("orderId={}", encode_query(value))
        }
        BitgetOrderLookup::ClientOrderId(value) => {
            validate_client_order_id(value)?;
            format!("clientOid={}", encode_query(value))
        }
        _ => return Err(BitgetExecutionError::Identity),
    };
    Ok(BitgetExactReadbackRequest {
        binding: binding.clone(),
        attempt_id,
        generation,
        lookup,
        not_before_ms,
        expected_kind,
        query,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetExactOrderReadback {
    pub request: BitgetExactReadbackRequest,
    pub requested_at_ms: u64,
    pub received_at_ms: u64,
    pub order: Option<Order>,
    pub payload_sha256: String,
    pub raw_payload: Vec<u8>,
}

pub fn parse_exact_order_readback(
    config: &BitgetConfig,
    request: BitgetExactReadbackRequest,
    requested_at_ms: u64,
    received_at_ms: u64,
    payload: Vec<u8>,
) -> Result<BitgetExactOrderReadback, BitgetExecutionError> {
    validate_binding(&request.binding, config)?;
    if requested_at_ms < request.not_before_ms || received_at_ms < requested_at_ms {
        return Err(BitgetExecutionError::Clock);
    }
    let root: Value =
        serde_json::from_slice(&payload).map_err(|_| BitgetExecutionError::Payload)?;
    let object = root.as_object().ok_or(BitgetExecutionError::Payload)?;
    if object.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(BitgetExecutionError::VenueRejected);
    }
    let order = match object.get("data") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            parse_regular_order(value, &request.binding.symbol)
                .map_err(|_| BitgetExecutionError::Payload)?,
        ),
    };
    if order
        .as_ref()
        .is_some_and(|order| !lookup_matches(&request.lookup, order))
    {
        return Err(BitgetExecutionError::Identity);
    }
    Ok(BitgetExactOrderReadback {
        request,
        requested_at_ms,
        received_at_ms,
        order,
        payload_sha256: payload_digest(&payload),
        raw_payload: payload,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetReadbackFinality {
    Working,
    Terminal,
    AbsentAtReadback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetMutationSettlement {
    pub order: Option<Order>,
    pub finality: BitgetReadbackFinality,
}

pub fn settle_ack_readback(
    ack: &BitgetMutationAck,
    readback: &BitgetExactOrderReadback,
) -> Result<BitgetMutationSettlement, BitgetExecutionError> {
    if ack.binding != readback.request.binding
        || ack.attempt_id != readback.request.attempt_id
        || ack.generation != readback.request.generation
        || ack.kind != readback.request.expected_kind
        || readback.request.not_before_ms != ack.received_at_ms
        || readback
            .order
            .as_ref()
            .is_some_and(|order| order.order_id != ack.order_id)
    {
        return Err(BitgetExecutionError::Readback);
    }
    let Some(order) = readback.order.clone() else {
        return Err(BitgetExecutionError::Unsettled);
    };
    let terminal = matches!(
        order.state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    );
    if ack.kind == BitgetMutationKind::Cancel && !terminal {
        return Err(BitgetExecutionError::Unsettled);
    }
    Ok(BitgetMutationSettlement {
        order: Some(order),
        finality: if terminal {
            BitgetReadbackFinality::Terminal
        } else {
            BitgetReadbackFinality::Working
        },
    })
}

pub fn settle_unknown_readback(
    unknown: &BitgetUnknownMutation,
    readback: &BitgetExactOrderReadback,
) -> Result<BitgetMutationSettlement, BitgetExecutionError> {
    if unknown.binding != readback.request.binding
        || unknown.attempt_id != readback.request.attempt_id
        || unknown.generation != readback.request.generation
        || unknown.kind != readback.request.expected_kind
        || readback.request.not_before_ms != unknown.dispatched_at_ms
    {
        return Err(BitgetExecutionError::Readback);
    }
    let Some(order) = readback.order.clone() else {
        return Ok(BitgetMutationSettlement {
            order: None,
            finality: BitgetReadbackFinality::AbsentAtReadback,
        });
    };
    let finality = if matches!(
        order.state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    ) {
        BitgetReadbackFinality::Terminal
    } else {
        BitgetReadbackFinality::Working
    };
    Ok(BitgetMutationSettlement {
        order: Some(order),
        finality,
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceWire<'a> {
    category: &'static str,
    symbol: &'a str,
    client_oid: &'a str,
    side: &'static str,
    pos_side: &'static str,
    order_type: &'static str,
    qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelWire<'a> {
    category: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_oid: Option<&'a str>,
}

fn validate_scope(
    binding: &GatewayBinding,
    config: &BitgetConfig,
    rules: &BitgetInstrumentRules,
    attempt_id: u64,
    now_ms: u64,
) -> Result<(), BitgetExecutionError> {
    validate_binding(binding, config)?;
    rules
        .raw
        .validate()
        .map_err(|_| BitgetExecutionError::Rules)?;
    if attempt_id == 0
        || rules.raw.binding != *binding
        || rules.snapshot.metadata.instrument.generation == 0
        || rules.snapshot.metadata.instrument.generation != rules.raw.generation
        || now_ms < rules.raw.observed_at_ms
        || now_ms >= rules.raw.expires_at_ms
    {
        return Err(BitgetExecutionError::Rules);
    }
    Ok(())
}

fn validate_binding(
    binding: &GatewayBinding,
    config: &BitgetConfig,
) -> Result<(), BitgetExecutionError> {
    BitgetAccountBinding::UtaUsdtFuturesHedge
        .validate_gateway_binding(binding)
        .map_err(|_| BitgetExecutionError::Binding)?;
    if binding.mode != config.mode()
        || (matches!(binding.mode, GatewayMode::Test) != config.paper_trading())
    {
        return Err(BitgetExecutionError::Binding);
    }
    Ok(())
}

fn validate_quantity(
    rules: &BitgetInstrumentRules,
    quantity: Decimal,
    market: bool,
) -> Result<(), BitgetExecutionError> {
    if quantity <= Decimal::ZERO
        || !rules
            .snapshot
            .metadata
            .quantity
            .accepts(quantity)
            .map_err(|_| BitgetExecutionError::Rules)?
        || rules
            .maximum_order_quantity
            .is_some_and(|maximum| !market && quantity > maximum)
        || rules
            .maximum_market_order_quantity
            .is_some_and(|maximum| market && quantity > maximum)
    {
        return Err(BitgetExecutionError::Rules);
    }
    Ok(())
}

fn validate_hedge_direction(
    position_side: PositionSide,
    side: OrderSide,
    reduce_only: bool,
) -> Result<(), BitgetExecutionError> {
    let close = match (position_side, side) {
        (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy) => true,
        (PositionSide::Long, OrderSide::Buy) | (PositionSide::Short, OrderSide::Sell) => false,
        (PositionSide::Net, _) => return Err(BitgetExecutionError::Position),
    };
    if close != reduce_only {
        return Err(BitgetExecutionError::Position);
    }
    Ok(())
}

const fn close_side(position_side: PositionSide) -> Result<OrderSide, BitgetExecutionError> {
    match position_side {
        PositionSide::Long => Ok(OrderSide::Sell),
        PositionSide::Short => Ok(OrderSide::Buy),
        PositionSide::Net => Err(BitgetExecutionError::Position),
    }
}

const fn side_wire(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

const fn position_side_wire(side: PositionSide) -> Result<&'static str, BitgetExecutionError> {
    match side {
        PositionSide::Long => Ok("long"),
        PositionSide::Short => Ok("short"),
        PositionSide::Net => Err(BitgetExecutionError::Position),
    }
}

fn validate_client_order_id(value: &str) -> Result<(), BitgetExecutionError> {
    if value.is_empty()
        || value.len() > 32
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b':' | b'/' | b'_' | b'-')
        })
    {
        return Err(BitgetExecutionError::Identity);
    }
    Ok(())
}

fn valid_native_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64 && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn decimal_wire(value: Decimal) -> String {
    value.normalize().to_string()
}

fn unsigned(value: Option<&Value>) -> Result<u64, BitgetExecutionError> {
    match value {
        Some(Value::String(value)) => value.parse().map_err(|_| BitgetExecutionError::Clock),
        Some(Value::Number(value)) => value
            .to_string()
            .parse()
            .map_err(|_| BitgetExecutionError::Clock),
        _ => Err(BitgetExecutionError::Clock),
    }
}

fn identifier(value: Option<&Value>) -> Result<String, BitgetExecutionError> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(BitgetExecutionError::Identity),
    }
}

fn lookup_matches(lookup: &BitgetOrderLookup, order: &Order) -> bool {
    match lookup {
        BitgetOrderLookup::OrderId(expected) => &order.order_id == expected,
        BitgetOrderLookup::ClientOrderId(expected) => {
            order.client_order_id == FieldState::Known(expected.clone())
        }
    }
}

fn encode_query(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn payload_digest(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetExecutionError {
    #[error("Bitget mutation binding, attempt, or generation is invalid")]
    Binding,
    #[error("Bitget mutation identity is invalid or ambiguous")]
    Identity,
    #[error("Bitget Hedge direction or signed position is invalid")]
    Position,
    #[error("Bitget instrument rules reject the mutation")]
    Rules,
    #[error("Bitget mutation signing failed")]
    Signing,
    #[error("Bitget mutation payload is invalid or incomplete")]
    Payload,
    #[error("Bitget explicitly rejected the mutation")]
    VenueRejected,
    #[error("Bitget mutation or readback timestamp is invalid")]
    Clock,
    #[error("Bitget exact readback does not match its mutation")]
    Readback,
    #[error("Bitget ACK has not been settled by exact readback")]
    Unsettled,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;
    use crate::instrument::{BitgetRawInstrumentPayload, parse_instrument_rules};

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn rules(mode: GatewayMode) -> Result<BitgetInstrumentRules, Box<dyn std::error::Error>> {
        let raw = BitgetRawInstrumentPayload::new(
            binding(mode)?,
            7,
            50,
            1_000,
            include_str!("../tests/fixtures/bitget_uta_btcusdt_instrument.json").to_owned(),
        )?;
        Ok(parse_instrument_rules(raw, 60)?)
    }

    #[test]
    fn place_and_reduce_once_encode_only_consistent_hedge_directions()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Test)?;
        let config = BitgetConfig::for_mode(GatewayMode::Test);
        let rules = rules(GatewayMode::Test)?;
        let place = prepare_place_request(
            &binding,
            &config,
            &rules,
            9,
            &BitgetPlaceIntent {
                client_order_id: "venue_open_long".to_owned(),
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quantity: Decimal::new(1, 3),
                limit_price: Price::new(Decimal::from(50_000))?,
                time_in_force: BitgetTimeInForce::PostOnly,
                reduce_only: false,
            },
            60,
        )?;
        let place_wire: Value = serde_json::from_slice(&place.body)?;
        assert_eq!(place_wire["timeInForce"], "post_only");
        assert_eq!(place_wire["posSide"], "long");
        assert_eq!(place_wire["side"], "buy");
        assert_eq!(place_wire.get("reduceOnly"), None);

        let signed_position = Position {
            symbol: binding.symbol.clone(),
            side: PositionSide::Long,
            quantity: Decimal::new(1, 2),
            entry_price: None,
            mark_price: None,
        };
        let reduce = prepare_reduce_once_request(
            &binding,
            &config,
            &rules,
            10,
            &BitgetReduceOnceIntent {
                client_order_id: "venue_reduce_long".to_owned(),
                position_side: PositionSide::Long,
                quantity: Decimal::new(1, 3),
            },
            &signed_position,
            60,
        )?;
        let reduce_wire: Value = serde_json::from_slice(&reduce.body)?;
        assert_eq!(reduce.kind, BitgetMutationKind::ReduceOnce);
        assert_eq!(reduce_wire["orderType"], "market");
        assert_eq!(reduce_wire["side"], "sell");
        assert_eq!(reduce_wire["posSide"], "long");
        assert_eq!(reduce_wire.get("price"), None);
        assert!(
            prepare_reduce_once_request(
                &binding,
                &config,
                &rules,
                11,
                &BitgetReduceOnceIntent {
                    client_order_id: "venue_reduce_too_much".to_owned(),
                    position_side: PositionSide::Long,
                    quantity: Decimal::ONE,
                },
                &signed_position,
                60,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn cancel_identity_is_exact_and_mode_bound() -> Result<(), Box<dyn std::error::Error>> {
        let live = binding(GatewayMode::Live)?;
        let request = prepare_cancel_request(
            &live,
            &BitgetConfig::for_mode(GatewayMode::Live),
            7,
            9,
            &BitgetCancelIntent {
                order_id: None,
                client_order_id: Some("venue_1".to_owned()),
            },
        )?;
        assert_eq!(request.kind, BitgetMutationKind::Cancel);
        assert!(
            prepare_cancel_request(
                &live,
                &BitgetConfig::for_mode(GatewayMode::Test),
                7,
                9,
                &BitgetCancelIntent {
                    order_id: Some("1".to_owned()),
                    client_order_id: Some("venue_1".to_owned()),
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn ack_is_only_accepted_until_same_generation_exact_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let live = binding(GatewayMode::Live)?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let request = prepare_cancel_request(
            &live,
            &config,
            7,
            9,
            &BitgetCancelIntent {
                order_id: Some("123".to_owned()),
                client_order_id: None,
            },
        )?;
        let ack_payload = json!({
            "code":"00000", "requestTime":100,
            "data":{"orderId":"123", "clientOid":"venue_1"}
        })
        .to_string();
        let ack = parse_mutation_ack(&config, &request, ack_payload.as_bytes(), 101)?;
        assert_eq!(ack.status, BitgetAckStatus::AcceptedOnly);
        let detail = json!({
            "code":"00000",
            "data":{
                "orderId":"123", "clientOid":"venue_1", "category":"USDT-FUTURES",
                "symbol":"BTCUSDT", "orderStatus":"cancelled", "side":"sell",
                "posSide":"long", "holdMode":"hedge_mode", "tradeSide":"close_long",
                "qty":"1", "cumExecQty":"0", "price":"1", "avgPrice":"0",
                "delegateType":"normal"
            }
        })
        .to_string()
        .into_bytes();
        let readback = parse_exact_order_readback(
            &config,
            build_ack_readback_request(&ack)?,
            102,
            103,
            detail,
        )?;
        assert_eq!(
            settle_ack_readback(&ack, &readback)?.finality,
            BitgetReadbackFinality::Terminal
        );
        let mut wrong_generation = readback;
        wrong_generation.request.generation = 8;
        assert_eq!(
            settle_ack_readback(&ack, &wrong_generation),
            Err(BitgetExecutionError::Readback)
        );
        Ok(())
    }

    #[test]
    fn unknown_can_only_build_exact_readback_and_absence_is_not_rejection()
    -> Result<(), Box<dyn std::error::Error>> {
        let live = binding(GatewayMode::Live)?;
        let unknown = BitgetUnknownMutation {
            binding: live,
            attempt_id: 9,
            generation: 7,
            kind: BitgetMutationKind::Place,
            order_id: None,
            client_order_id: Some("venue_1".to_owned()),
            dispatched_at_ms: 100,
            reason: BitgetUnknownReason::Timeout,
        };
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let readback = parse_exact_order_readback(
            &config,
            build_unknown_readback_request(&unknown)?,
            101,
            102,
            br#"{"code":"00000","data":null}"#.to_vec(),
        )?;
        assert_eq!(
            settle_unknown_readback(&unknown, &readback)?.finality,
            BitgetReadbackFinality::AbsentAtReadback
        );
        Ok(())
    }
}
