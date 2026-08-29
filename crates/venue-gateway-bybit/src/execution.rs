use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{FieldState, OrderSide, OrderState, PositionSide, Price};
use venue_gateway_api::GatewayBinding;

use crate::{
    BybitAccountIdentity, BybitAccountMode, BybitCredentials, BybitError, BybitGatewayBinding,
    BybitLinearInstrumentRules, BybitOpenOrder, BybitOpenOrderPage, BybitOrderEvidence,
    BybitOrderEvidencePage, BybitPageClosure, BybitPublicSource, BybitRestBbo, SignedHeaders,
    endpoints, linear_native_symbol, sign,
};

const MARKET_BBO_MAX_AGE_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitOrderKind {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitTimeInForce {
    GoodTillCancelled,
    ImmediateOrCancel,
    FillOrKill,
    PostOnly,
}

impl BybitTimeInForce {
    const fn wire(self) -> &'static str {
        match self {
            Self::GoodTillCancelled => "GTC",
            Self::ImmediateOrCancel => "IOC",
            Self::FillOrKill => "FOK",
            Self::PostOnly => "PostOnly",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPlaceIntent {
    pub client_order_id: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub kind: BybitOrderKind,
    pub quantity: Decimal,
    pub limit_price: Option<Price>,
    pub time_in_force: BybitTimeInForce,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitCancelIntent {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitRequestKind {
    Place,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPreparedRequest {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub origin: &'static str,
    pub path: &'static str,
    pub kind: BybitRequestKind,
    pub body: Vec<u8>,
    expected_order_id: Option<String>,
    expected_client_order_id: Option<String>,
}

impl BybitPreparedRequest {
    fn validate(&self, binding: &BybitGatewayBinding) -> Result<(), BybitExecutionError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| BybitExecutionError::Binding)?;
        let expected_path = match self.kind {
            BybitRequestKind::Place => endpoints::PLACE_ORDER,
            BybitRequestKind::Cancel => endpoints::CANCEL_ORDER,
        };
        if self.origin != binding.config().rest_origin()
            || self.path != expected_path
            || self.generation == 0
            || self.body.is_empty()
        {
            return Err(BybitExecutionError::Binding);
        }
        Ok(())
    }
}

pub fn prepare_place_request(
    binding: &BybitGatewayBinding,
    identity: &BybitAccountIdentity,
    rules: &BybitLinearInstrumentRules,
    intent: &BybitPlaceIntent,
    now_ms: u64,
    market_bbo: Option<&BybitRestBbo>,
) -> Result<BybitPreparedRequest, BybitExecutionError> {
    validate_scope(binding, identity, rules)?;
    validate_client_order_id(&intent.client_order_id)?;
    let position_idx = position_idx(intent.position_side);
    validate_direction(intent)?;
    let price = match (intent.kind, intent.limit_price) {
        (BybitOrderKind::Limit, Some(price)) => Some(price),
        (BybitOrderKind::Limit, None) | (BybitOrderKind::Market, Some(_)) => {
            return Err(BybitExecutionError::Intent);
        }
        (BybitOrderKind::Market, None) => None,
    };
    if intent.kind == BybitOrderKind::Market
        && intent.time_in_force != BybitTimeInForce::ImmediateOrCancel
    {
        return Err(BybitExecutionError::Intent);
    }
    validate_step(intent.quantity, rules.instrument.quantity_step)?;
    if intent.quantity < rules.minimum_quantity {
        return Err(BybitExecutionError::Rules);
    }
    let maximum = match intent.kind {
        BybitOrderKind::Limit => rules.maximum_limit_quantity,
        BybitOrderKind::Market => rules.maximum_market_quantity,
    };
    if intent.quantity > maximum {
        return Err(BybitExecutionError::Rules);
    }
    let notional_price = match price {
        Some(price) => {
            if price < rules.minimum_price || price > rules.maximum_price {
                return Err(BybitExecutionError::Rules);
            }
            validate_step(price.value(), rules.instrument.price_tick.value())?;
            price
        }
        None => market_reference_price(binding, rules, intent.side, now_ms, market_bbo)?,
    };
    let notional = intent
        .quantity
        .checked_mul(notional_price.value())
        .ok_or(BybitExecutionError::Rules)?;
    if notional < rules.instrument.minimum_notional.value {
        return Err(BybitExecutionError::Rules);
    }
    let body = PlaceBody {
        category: "linear",
        symbol: &rules.native_symbol,
        side: side_wire(intent.side),
        order_type: match intent.kind {
            BybitOrderKind::Limit => "Limit",
            BybitOrderKind::Market => "Market",
        },
        qty: decimal_wire(intent.quantity),
        price: price.map(|value| decimal_wire(value.value())),
        time_in_force: intent.time_in_force.wire(),
        position_idx,
        order_link_id: &intent.client_order_id,
        reduce_only: intent.reduce_only,
    };
    Ok(BybitPreparedRequest {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        path: endpoints::PLACE_ORDER,
        kind: BybitRequestKind::Place,
        body: serde_json::to_vec(&body).map_err(|_| BybitExecutionError::Payload)?,
        expected_order_id: None,
        expected_client_order_id: Some(intent.client_order_id.clone()),
    })
}

pub fn prepare_cancel_request(
    binding: &BybitGatewayBinding,
    identity: &BybitAccountIdentity,
    rules: &BybitLinearInstrumentRules,
    intent: &BybitCancelIntent,
) -> Result<BybitPreparedRequest, BybitExecutionError> {
    validate_scope(binding, identity, rules)?;
    let (order_id, client_order_id) = match (&intent.order_id, &intent.client_order_id) {
        (Some(order_id), None) if valid_native_id(order_id) => (Some(order_id.as_str()), None),
        (None, Some(client_order_id)) => {
            validate_client_order_id(client_order_id)?;
            (None, Some(client_order_id.as_str()))
        }
        _ => return Err(BybitExecutionError::Intent),
    };
    let body = CancelBody {
        category: "linear",
        symbol: &rules.native_symbol,
        order_id,
        order_link_id: client_order_id,
    };
    Ok(BybitPreparedRequest {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        path: endpoints::CANCEL_ORDER,
        kind: BybitRequestKind::Cancel,
        body: serde_json::to_vec(&body).map_err(|_| BybitExecutionError::Payload)?,
        expected_order_id: intent.order_id.clone(),
        expected_client_order_id: intent.client_order_id.clone(),
    })
}

pub fn sign_prepared_request(
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    request: &BybitPreparedRequest,
    timestamp_ms: u64,
) -> Result<SignedHeaders, BybitExecutionError> {
    request.validate(binding)?;
    sign(
        credentials,
        binding,
        &request.binding,
        timestamp_ms,
        &request.body,
    )
    .map_err(|_| BybitExecutionError::Signing)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitAckStatus {
    AcceptedOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderAck {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub request_kind: BybitRequestKind,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub accepted_at_ms: u64,
    pub received_at_ms: u64,
    pub status: BybitAckStatus,
}

pub fn parse_order_ack(
    binding: &BybitGatewayBinding,
    request: &BybitPreparedRequest,
    payload: &[u8],
    received_at_ms: u64,
) -> Result<BybitOrderAck, BybitExecutionError> {
    request.validate(binding)?;
    if received_at_ms == 0 {
        return Err(BybitExecutionError::Payload);
    }
    let envelope: AckEnvelope =
        serde_json::from_slice(payload).map_err(|_| BybitExecutionError::Payload)?;
    if envelope.ret_code != 0 {
        return Err(BybitExecutionError::VenueRejected);
    }
    let order_id = nonempty(envelope.result.order_id);
    let client_order_id = nonempty(envelope.result.order_link_id);
    if order_id.is_none() && client_order_id.is_none() {
        return Err(BybitExecutionError::Payload);
    }
    if request
        .expected_order_id
        .as_ref()
        .is_some_and(|expected| order_id.as_ref() != Some(expected))
        || request
            .expected_client_order_id
            .as_ref()
            .is_some_and(|expected| client_order_id.as_ref() != Some(expected))
    {
        return Err(BybitExecutionError::Binding);
    }
    let accepted_at_ms = positive_u64(envelope.time)?;
    if accepted_at_ms > received_at_ms {
        return Err(BybitExecutionError::Payload);
    }
    Ok(BybitOrderAck {
        binding: request.binding.clone(),
        generation: request.generation,
        request_kind: request.kind,
        order_id,
        client_order_id,
        accepted_at_ms,
        received_at_ms,
        status: BybitAckStatus::AcceptedOnly,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitClosedOrderReadback {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub received_at_ms: u64,
    pub open_orders: Vec<BybitOpenOrder>,
    pub history: Vec<BybitOrderEvidence>,
}

impl BybitClosedOrderReadback {
    pub fn from_pages(
        binding: &BybitGatewayBinding,
        generation: u64,
        open_pages: &[BybitOpenOrderPage],
        history_pages: &[BybitOrderEvidencePage],
    ) -> Result<Self, BybitExecutionError> {
        if generation == 0 || open_pages.is_empty() || history_pages.is_empty() {
            return Err(BybitExecutionError::Readback);
        }
        let mut open_closure = BybitPageClosure::default();
        let mut history_closure = BybitPageClosure::default();
        let mut open_orders = Vec::new();
        let mut history = Vec::new();
        let mut received_at_ms = u64::MAX;
        for page in open_pages {
            validate_page_scope(binding, generation, &page.binding, page.generation)?;
            received_at_ms = received_at_ms.min(page.received_at_ms);
            open_closure
                .accept(&page.meta)
                .map_err(|_| BybitExecutionError::Readback)?;
            open_orders.extend(page.orders.iter().cloned());
        }
        for page in history_pages {
            validate_page_scope(binding, generation, &page.binding, page.generation)?;
            received_at_ms = received_at_ms.min(page.received_at_ms);
            history_closure
                .accept(&page.meta)
                .map_err(|_| BybitExecutionError::Readback)?;
            history.extend(page.orders.iter().cloned());
        }
        if !open_closure.is_closed() || !history_closure.is_closed() {
            return Err(BybitExecutionError::Readback);
        }
        Ok(Self {
            binding: binding.gateway_binding().clone(),
            generation,
            received_at_ms,
            open_orders,
            history,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitSettlementFinality {
    Working,
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderSettlement {
    pub order_id: String,
    pub client_order_id: FieldState<String>,
    pub state: OrderState,
    pub finality: BybitSettlementFinality,
    pub updated_at_ms: u64,
}

pub fn settle_order_ack(
    binding: &BybitGatewayBinding,
    ack: &BybitOrderAck,
    readback: &BybitClosedOrderReadback,
) -> Result<BybitOrderSettlement, BybitExecutionError> {
    if &ack.binding != binding.gateway_binding()
        || &readback.binding != binding.gateway_binding()
        || ack.generation != readback.generation
        || readback.received_at_ms < ack.received_at_ms
        || ack.status != BybitAckStatus::AcceptedOnly
    {
        return Err(BybitExecutionError::Binding);
    }
    let open = readback
        .open_orders
        .iter()
        .filter(|item| matches_open(item, ack))
        .collect::<Vec<_>>();
    let history = readback
        .history
        .iter()
        .filter(|item| matches_history(item, ack))
        .collect::<Vec<_>>();
    if open.len() > 1 || history.len() > 1 || (!open.is_empty() && !history.is_empty()) {
        return Err(BybitExecutionError::Readback);
    }
    if let Some(item) = open.first() {
        return Ok(BybitOrderSettlement {
            order_id: item.order.order_id.clone(),
            client_order_id: item.order.client_order_id.clone(),
            state: item.order.state,
            finality: BybitSettlementFinality::Working,
            updated_at_ms: item.updated_at_ms,
        });
    }
    let item = history.first().ok_or(BybitExecutionError::Unsettled)?;
    let finality = if matches!(
        item.state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    ) {
        BybitSettlementFinality::Terminal
    } else {
        BybitSettlementFinality::Working
    };
    Ok(BybitOrderSettlement {
        order_id: item.order_id.clone(),
        client_order_id: item.client_order_id.clone(),
        state: item.state,
        finality,
        updated_at_ms: item.updated_at_ms,
    })
}

fn validate_scope(
    binding: &BybitGatewayBinding,
    identity: &BybitAccountIdentity,
    rules: &BybitLinearInstrumentRules,
) -> Result<(), BybitExecutionError> {
    rules
        .raw
        .validate(binding, BybitPublicSource::LinearInstrument)
        .map_err(|_| BybitExecutionError::Binding)?;
    if identity.binding != *binding.gateway_binding()
        || rules.raw.binding != *binding.gateway_binding()
        || identity.generation != rules.instrument.generation
        || rules.raw.generation != rules.instrument.generation
        || !matches!(
            identity.mode,
            BybitAccountMode::Uta2 | BybitAccountMode::Uta2Pro
        )
        || rules.native_symbol
            != linear_native_symbol(&binding.gateway_binding().symbol)
                .map_err(|_| BybitExecutionError::Binding)?
    {
        return Err(BybitExecutionError::Binding);
    }
    Ok(())
}

fn market_reference_price(
    binding: &BybitGatewayBinding,
    rules: &BybitLinearInstrumentRules,
    side: OrderSide,
    now_ms: u64,
    bbo: Option<&BybitRestBbo>,
) -> Result<Price, BybitExecutionError> {
    let bbo = bbo.ok_or(BybitExecutionError::Rules)?;
    if bbo.raw.binding != *binding.gateway_binding()
        || bbo.raw.generation != rules.instrument.generation
        || bbo.snapshot.generation != rules.instrument.generation
        || now_ms < bbo.raw.received_at_ms
        || now_ms.saturating_sub(bbo.raw.received_at_ms) > MARKET_BBO_MAX_AGE_MS
    {
        return Err(BybitExecutionError::Binding);
    }
    match side {
        OrderSide::Buy => bbo.snapshot.asks.first(),
        OrderSide::Sell => bbo.snapshot.bids.first(),
    }
    .map(|level| level.price)
    .ok_or(BybitExecutionError::Rules)
}

fn validate_direction(intent: &BybitPlaceIntent) -> Result<(), BybitExecutionError> {
    let valid = matches!(
        (intent.position_side, intent.side, intent.reduce_only),
        (PositionSide::Net, _, _)
            | (PositionSide::Long, OrderSide::Buy, false)
            | (PositionSide::Long, OrderSide::Sell, true)
            | (PositionSide::Short, OrderSide::Sell, false)
            | (PositionSide::Short, OrderSide::Buy, true)
    );
    if valid {
        Ok(())
    } else {
        Err(BybitExecutionError::Intent)
    }
}

fn validate_step(value: Decimal, step: Decimal) -> Result<(), BybitExecutionError> {
    if value > Decimal::ZERO && step > Decimal::ZERO && value % step == Decimal::ZERO {
        Ok(())
    } else {
        Err(BybitExecutionError::Rules)
    }
}

fn validate_client_order_id(value: &str) -> Result<(), BybitExecutionError> {
    if (1..=36).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        Ok(())
    } else {
        Err(BybitExecutionError::Intent)
    }
}

fn valid_native_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn validate_page_scope(
    binding: &BybitGatewayBinding,
    generation: u64,
    page_binding: &GatewayBinding,
    page_generation: u64,
) -> Result<(), BybitExecutionError> {
    if page_binding == binding.gateway_binding() && page_generation == generation {
        Ok(())
    } else {
        Err(BybitExecutionError::Binding)
    }
}

fn matches_open(order: &BybitOpenOrder, ack: &BybitOrderAck) -> bool {
    let order_id_matches = ack
        .order_id
        .as_ref()
        .is_none_or(|value| value == &order.order.order_id);
    let client_id_matches = match (&ack.client_order_id, &order.order.client_order_id) {
        (None, _) => true,
        (Some(expected), FieldState::Known(actual)) => expected == actual,
        (Some(_), _) => false,
    };
    order_id_matches && client_id_matches
}

fn matches_history(order: &BybitOrderEvidence, ack: &BybitOrderAck) -> bool {
    let order_id_matches = ack
        .order_id
        .as_ref()
        .is_none_or(|value| value == &order.order_id);
    let client_id_matches = match (&ack.client_order_id, &order.client_order_id) {
        (None, _) => true,
        (Some(expected), FieldState::Known(actual)) => expected == actual,
        (Some(_), _) => false,
    };
    order_id_matches && client_id_matches
}

fn side_wire(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

const fn position_idx(side: PositionSide) -> u8 {
    match side {
        PositionSide::Net => 0,
        PositionSide::Long => 1,
        PositionSide::Short => 2,
    }
}

fn decimal_wire(value: Decimal) -> String {
    value.normalize().to_string()
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn positive_u64(value: u64) -> Result<u64, BybitExecutionError> {
    if value == 0 {
        Err(BybitExecutionError::Payload)
    } else {
        Ok(value)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceBody<'a> {
    category: &'static str,
    symbol: &'a str,
    side: &'static str,
    order_type: &'static str,
    qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    time_in_force: &'static str,
    position_idx: u8,
    order_link_id: &'a str,
    reduce_only: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelBody<'a> {
    category: &'static str,
    symbol: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_link_id: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AckEnvelope {
    ret_code: i64,
    result: AckResult,
    time: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AckResult {
    order_id: String,
    order_link_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitExecutionError {
    #[error("Bybit execution input does not match the fixed gateway binding")]
    Binding,
    #[error("Bybit canonical order intent is invalid or ambiguous")]
    Intent,
    #[error("Bybit order violates the current linear instrument rules")]
    Rules,
    #[error("Bybit execution payload is invalid or incomplete")]
    Payload,
    #[error("Bybit rejected the execution request")]
    VenueRejected,
    #[error("Bybit execution request could not be signed")]
    Signing,
    #[error("Bybit order readback is incomplete, conflicting, or unclosed")]
    Readback,
    #[error("Bybit accepted the request but no exact order readback settles it")]
    Unsettled,
}

impl From<BybitError> for BybitExecutionError {
    fn from(_: BybitError) -> Self {
        Self::Payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BybitPrivateSource, BybitPublicSource, BybitRawPrivatePayload, BybitRawPublicPayload,
        parse_account_identity, parse_linear_instrument, parse_open_order_page,
        parse_order_history_page, parse_rest_bbo,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
    const ACCOUNT: &[u8] = include_bytes!("../fixtures/account-info-uta2.json");
    const INSTRUMENT: &str = include_str!("../fixtures/instruments-linear.json");
    const BBO: &str = include_str!("../fixtures/orderbook-linear-bbo.json");
    const OPEN: &[u8] = include_bytes!("../fixtures/open-orders-linear.json");
    const CANCEL_HISTORY: &[u8] = include_bytes!("../fixtures/cancel-order-history-linear.json");
    const PLACE_ACK: &[u8] = include_bytes!("../fixtures/place-order-ack.json");
    const CANCEL_ACK: &[u8] = include_bytes!("../fixtures/cancel-order-ack.json");
    const EMPTY_ORDERS: &[u8] = br#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"","list":[]},"time":2002}"#;

    struct Facts {
        binding: BybitGatewayBinding,
        identity: BybitAccountIdentity,
        rules: BybitLinearInstrumentRules,
        bbo: BybitRestBbo,
    }

    fn facts(mode: GatewayMode) -> Result<Facts, Box<dyn std::error::Error>> {
        let binding = BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            mode,
            ACCOUNT_ID,
            "BTC/USDT".parse()?,
        )?)?;
        let account_raw = BybitRawPrivatePayload::new(
            &binding,
            BybitPrivateSource::AccountInfo,
            7,
            1_716_863_719_400,
            ACCOUNT.to_vec(),
        )?;
        let identity = parse_account_identity(&binding, &account_raw)?;
        let instrument_raw = BybitRawPublicPayload::new(
            &binding,
            BybitPublicSource::LinearInstrument,
            7,
            1_716_863_719_400,
            INSTRUMENT.to_owned(),
        )?;
        let rules = parse_linear_instrument(&binding, instrument_raw)?;
        let bbo_raw = BybitRawPublicPayload::new(
            &binding,
            BybitPublicSource::RestOrderBook,
            7,
            1_716_863_719_400,
            BBO.to_owned(),
        )?;
        let bbo = parse_rest_bbo(&binding, bbo_raw)?;
        Ok(Facts {
            binding,
            identity,
            rules,
            bbo,
        })
    }

    fn limit_intent() -> Result<BybitPlaceIntent, Box<dyn std::error::Error>> {
        Ok(BybitPlaceIntent {
            client_order_id: "MANAGED_CLIENT_ID".to_owned(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            kind: BybitOrderKind::Limit,
            quantity: Decimal::new(1, 3),
            limit_price: Some(Price::new(Decimal::new(60_000, 0))?),
            time_in_force: BybitTimeInForce::GoodTillCancelled,
            reduce_only: false,
        })
    }

    fn private_raw(
        binding: &BybitGatewayBinding,
        source: BybitPrivateSource,
        payload: &[u8],
    ) -> Result<BybitRawPrivatePayload, BybitError> {
        BybitRawPrivatePayload::new(binding, source, 7, 2_100, payload.to_vec())
    }

    #[test]
    fn limit_request_has_exact_linear_body_and_signature_preimage()
    -> Result<(), Box<dyn std::error::Error>> {
        let facts = facts(GatewayMode::Test)?;
        let request = prepare_place_request(
            &facts.binding,
            &facts.identity,
            &facts.rules,
            &limit_intent()?,
            1_716_863_719_500,
            None,
        )?;
        assert_eq!(request.path, endpoints::PLACE_ORDER);
        assert_eq!(request.origin, "https://api-testnet.bybit.com");
        assert_eq!(
            std::str::from_utf8(&request.body)?,
            r#"{"category":"linear","symbol":"BTCUSDT","side":"Buy","orderType":"Limit","qty":"0.001","price":"60000","timeInForce":"GTC","positionIdx":1,"orderLinkId":"MANAGED_CLIENT_ID","reduceOnly":false}"#
        );
        let credentials = BybitCredentials::from_values("test", "secret")?;
        let headers =
            sign_prepared_request(&credentials, &facts.binding, &request, 1_670_000_000_000)?;
        assert_eq!(
            headers.get("X-BAPI-SIGN"),
            Some("c49749595d3ac0677f0ce48ad9c114251a7e23c929520347898cf75a0e89cf0b")
        );
        Ok(())
    }

    #[test]
    fn market_request_requires_ioc_and_fresh_same_generation_bbo()
    -> Result<(), Box<dyn std::error::Error>> {
        let facts = facts(GatewayMode::Live)?;
        let mut intent = limit_intent()?;
        intent.kind = BybitOrderKind::Market;
        intent.limit_price = None;
        assert_eq!(
            prepare_place_request(
                &facts.binding,
                &facts.identity,
                &facts.rules,
                &intent,
                1_716_863_719_500,
                Some(&facts.bbo)
            ),
            Err(BybitExecutionError::Intent)
        );
        intent.time_in_force = BybitTimeInForce::ImmediateOrCancel;
        assert!(
            prepare_place_request(
                &facts.binding,
                &facts.identity,
                &facts.rules,
                &intent,
                1_716_863_719_500,
                Some(&facts.bbo)
            )
            .is_ok()
        );
        assert_eq!(
            prepare_place_request(
                &facts.binding,
                &facts.identity,
                &facts.rules,
                &intent,
                1_716_863_721_000,
                Some(&facts.bbo)
            ),
            Err(BybitExecutionError::Binding)
        );
        Ok(())
    }

    #[test]
    fn rules_and_position_direction_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let facts = facts(GatewayMode::Live)?;
        let mut intent = limit_intent()?;
        intent.quantity = Decimal::new(15, 4);
        assert_eq!(
            prepare_place_request(
                &facts.binding,
                &facts.identity,
                &facts.rules,
                &intent,
                1_716_863_719_500,
                None
            ),
            Err(BybitExecutionError::Rules)
        );
        intent.quantity = Decimal::new(1, 3);
        intent.side = OrderSide::Sell;
        assert_eq!(
            prepare_place_request(
                &facts.binding,
                &facts.identity,
                &facts.rules,
                &intent,
                1_716_863_719_500,
                None
            ),
            Err(BybitExecutionError::Intent)
        );
        Ok(())
    }

    #[test]
    fn ack_is_accepted_only_until_closed_readback_settles() -> Result<(), Box<dyn std::error::Error>>
    {
        let facts = facts(GatewayMode::Live)?;
        let request = prepare_place_request(
            &facts.binding,
            &facts.identity,
            &facts.rules,
            &limit_intent()?,
            1_716_863_719_500,
            None,
        )?;
        let ack = parse_order_ack(&facts.binding, &request, PLACE_ACK, 2_002)?;
        assert_eq!(ack.status, BybitAckStatus::AcceptedOnly);
        let open = parse_open_order_page(
            &facts.binding,
            &private_raw(&facts.binding, BybitPrivateSource::OpenOrders, OPEN)?,
            None,
        )?;
        let history = parse_order_history_page(
            &facts.binding,
            &private_raw(
                &facts.binding,
                BybitPrivateSource::OrderHistory,
                EMPTY_ORDERS,
            )?,
            None,
        )?;
        let readback =
            BybitClosedOrderReadback::from_pages(&facts.binding, 7, &[open], &[history])?;
        let settlement = settle_order_ack(&facts.binding, &ack, &readback)?;
        assert_eq!(settlement.state, OrderState::New);
        assert_eq!(settlement.finality, BybitSettlementFinality::Working);
        let mut stale_generation = readback.clone();
        stale_generation.generation = 8;
        assert_eq!(
            settle_order_ack(&facts.binding, &ack, &stale_generation),
            Err(BybitExecutionError::Binding)
        );
        let mut pre_ack = readback;
        pre_ack.received_at_ms = ack.received_at_ms - 1;
        assert_eq!(
            settle_order_ack(&facts.binding, &ack, &pre_ack),
            Err(BybitExecutionError::Binding)
        );
        assert_eq!(
            parse_order_ack(&facts.binding, &request, PLACE_ACK, 2_000),
            Err(BybitExecutionError::Payload)
        );
        Ok(())
    }

    #[test]
    fn cancel_ack_needs_terminal_history_and_unclosed_pages_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let facts = facts(GatewayMode::Live)?;
        let request = prepare_cancel_request(
            &facts.binding,
            &facts.identity,
            &facts.rules,
            &BybitCancelIntent {
                order_id: Some("23".to_owned()),
                client_order_id: None,
            },
        )?;
        let ack = parse_order_ack(&facts.binding, &request, CANCEL_ACK, 2_003)?;
        let open = parse_open_order_page(
            &facts.binding,
            &private_raw(&facts.binding, BybitPrivateSource::OpenOrders, EMPTY_ORDERS)?,
            None,
        )?;
        let history = parse_order_history_page(
            &facts.binding,
            &private_raw(
                &facts.binding,
                BybitPrivateSource::OrderHistory,
                CANCEL_HISTORY,
            )?,
            None,
        )?;
        let mut unclosed = open.clone();
        unclosed.meta.next_cursor = Some("next".to_owned());
        assert_eq!(
            BybitClosedOrderReadback::from_pages(
                &facts.binding,
                7,
                &[unclosed],
                std::slice::from_ref(&history)
            ),
            Err(BybitExecutionError::Readback)
        );
        let readback =
            BybitClosedOrderReadback::from_pages(&facts.binding, 7, &[open], &[history])?;
        let settlement = settle_order_ack(&facts.binding, &ack, &readback)?;
        assert_eq!(settlement.state, OrderState::Cancelled);
        assert_eq!(settlement.finality, BybitSettlementFinality::Terminal);
        Ok(())
    }

    #[test]
    fn cross_mode_signing_and_ambiguous_cancel_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let live = facts(GatewayMode::Live)?;
        let test = facts(GatewayMode::Test)?;
        let request = prepare_place_request(
            &live.binding,
            &live.identity,
            &live.rules,
            &limit_intent()?,
            1_716_863_719_500,
            None,
        )?;
        let credentials = BybitCredentials::from_values("test", "secret")?;
        assert_eq!(
            sign_prepared_request(&credentials, &test.binding, &request, 1_670_000_000_000).err(),
            Some(BybitExecutionError::Binding)
        );
        assert_eq!(
            prepare_cancel_request(
                &live.binding,
                &live.identity,
                &live.rules,
                &BybitCancelIntent {
                    order_id: Some("22".to_owned()),
                    client_order_id: Some("foreign".to_owned())
                }
            ),
            Err(BybitExecutionError::Intent)
        );
        Ok(())
    }
}
