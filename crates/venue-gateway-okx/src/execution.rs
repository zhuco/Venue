use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    CancelCommand, FieldState, MarketOrderCommand, MarketReduceCommand, Order, OrderCommand,
    OrderPurpose, OrderSide, OrderState, PositionSide, Price,
};
use venue_gateway_api::GatewayBinding;

use crate::private::{OkxAccountLevel, OkxAccountProfile, OkxTimedOrder};
use crate::public::{decimal, decode_success, positive_decimal, positive_u64};
use crate::{
    OkxConfig, OkxCredentials, OkxError, OkxInstrument, OkxPositionMode, SignedHeaders, endpoints,
    sign,
};

const POST: &str = "POST";
const GET: &str = "GET";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxTradeMode {
    Cross,
    Isolated,
}

impl OkxTradeMode {
    const fn wire_value(self) -> &'static str {
        match self {
            Self::Cross => "cross",
            Self::Isolated => "isolated",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxExecutionScope {
    gateway_binding: GatewayBinding,
    native_instrument_id: String,
    instrument_generation: u64,
    uid: String,
    main_uid: String,
    account_level: OkxAccountLevel,
    position_mode: OkxPositionMode,
    trade_mode: OkxTradeMode,
    base_quantity_per_contract: Decimal,
}

impl OkxExecutionScope {
    fn new(
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        trade_mode: OkxTradeMode,
    ) -> Result<Self, OkxError> {
        instrument.validate_scope(config)?;
        if profile.uid().is_empty() || profile.main_uid().is_empty() {
            return Err(OkxError::Binding);
        }
        Ok(Self {
            gateway_binding: config.gateway_binding().clone(),
            native_instrument_id: instrument.native_id().to_owned(),
            instrument_generation: instrument.instrument().generation,
            uid: profile.uid().to_owned(),
            main_uid: profile.main_uid().to_owned(),
            account_level: profile.level(),
            position_mode: profile.position_mode(),
            trade_mode,
            base_quantity_per_contract: instrument.base_quantity_per_contract(),
        })
    }

    fn validate(
        &self,
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
    ) -> Result<(), OkxError> {
        let candidate = Self::new(config, instrument, profile, self.trade_mode)?;
        if &candidate != self {
            return Err(OkxError::Binding);
        }
        Ok(())
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.gateway_binding
    }

    #[must_use]
    pub fn native_instrument_id(&self) -> &str {
        &self.native_instrument_id
    }

    #[must_use]
    pub const fn position_mode(&self) -> OkxPositionMode {
        self.position_mode
    }

    #[must_use]
    pub const fn trade_mode(&self) -> OkxTradeMode {
        self.trade_mode
    }
}

/// The existing canonical commands are the only admitted place intents. Adapter-specific order
/// variants are deliberately not invented here.
#[derive(Clone, Copy, Debug)]
pub enum OkxPlaceIntent<'a> {
    Limit(&'a OrderCommand),
    Market(&'a MarketOrderCommand),
    MarketReduce(&'a MarketReduceCommand),
}

impl OkxPlaceIntent<'_> {
    fn fields(&self) -> Result<PlaceFields<'_>, OkxError> {
        match self {
            Self::Limit(command) => {
                command.validate().map_err(|_| OkxError::Payload)?;
                Ok(PlaceFields {
                    owner: &command.owner,
                    client_order_id: command.client_order_id.as_str(),
                    side: command.side,
                    position_side: command.position_side,
                    quantity: command.quantity,
                    limit_price: Some(command.limit_price),
                    purpose: command.owner.purpose,
                    reduce_only: command.reduce_only,
                    order_type: "limit",
                })
            }
            Self::Market(command) => {
                command.validate().map_err(|_| OkxError::Payload)?;
                Ok(PlaceFields {
                    owner: &command.owner,
                    client_order_id: command.client_order_id.as_str(),
                    side: command.side,
                    position_side: command.position_side,
                    quantity: command.quantity,
                    limit_price: None,
                    purpose: command.owner.purpose,
                    reduce_only: false,
                    order_type: "market",
                })
            }
            Self::MarketReduce(command) => {
                command.validate().map_err(|_| OkxError::Payload)?;
                Ok(PlaceFields {
                    owner: &command.owner,
                    client_order_id: command.client_order_id.as_str(),
                    side: command.side,
                    position_side: command.position_side,
                    quantity: command.quantity,
                    limit_price: None,
                    purpose: command.owner.purpose,
                    reduce_only: true,
                    order_type: "market",
                })
            }
        }
    }
}

struct PlaceFields<'a> {
    owner: &'a venue_domain::domain::OrderOwner,
    client_order_id: &'a str,
    side: OrderSide,
    position_side: PositionSide,
    quantity: Decimal,
    limit_price: Option<Price>,
    purpose: OrderPurpose,
    reduce_only: bool,
    order_type: &'static str,
}

pub trait OkxPrivateRequest {
    fn scope(&self) -> &OkxExecutionScope;
    fn method(&self) -> &'static str;
    fn request_path(&self) -> &str;
    fn body(&self) -> &[u8];

    fn signed_headers(
        &self,
        credentials: &OkxCredentials,
        config: &OkxConfig,
        timestamp: &str,
    ) -> Result<SignedHeaders, OkxError> {
        if self.scope().gateway_binding() != config.gateway_binding() {
            return Err(OkxError::Binding);
        }
        sign(
            credentials,
            config,
            timestamp,
            self.method(),
            self.request_path(),
            self.body(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPlaceRequest {
    scope: OkxExecutionScope,
    body: Vec<u8>,
    client_order_id: String,
    side: OrderSide,
    position_side: PositionSide,
    quantity: Decimal,
    contracts: Decimal,
    limit_price: Option<Price>,
    purpose: OrderPurpose,
    reduce_only: bool,
    order_type: &'static str,
}

impl OkxPrivateRequest for OkxPlaceRequest {
    fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }
    fn method(&self) -> &'static str {
        POST
    }
    fn request_path(&self) -> &str {
        endpoints::PLACE_ORDER
    }
    fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceWire<'a> {
    inst_id: &'a str,
    td_mode: &'static str,
    cl_ord_id: &'a str,
    side: &'static str,
    pos_side: &'static str,
    ord_type: &'static str,
    sz: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    px: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reduce_only: Option<bool>,
}

pub fn build_place_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    trade_mode: OkxTradeMode,
    intent: OkxPlaceIntent<'_>,
) -> Result<OkxPlaceRequest, OkxError> {
    let scope = OkxExecutionScope::new(config, instrument, profile, trade_mode)?;
    let fields = intent.fields()?;
    validate_owner(fields.owner, config)?;
    validate_client_order_id(fields.client_order_id)?;
    if profile.position_mode() != OkxPositionMode::LongShort {
        // Current canonical commands describe an explicit LONG/SHORT leg. Silently translating
        // them to OKX net mode would change their meaning.
        return Err(OkxError::PositionMode);
    }
    let derived_reduce = match (fields.position_side, fields.side) {
        (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy) => true,
        (PositionSide::Long, OrderSide::Buy) | (PositionSide::Short, OrderSide::Sell) => false,
        (PositionSide::Net, _) => return Err(OkxError::PositionMode),
    };
    if derived_reduce != fields.reduce_only {
        return Err(OkxError::PositionMode);
    }
    let contracts = instrument.base_to_contracts(fields.quantity)?;
    if let Some(price) = fields.limit_price
        && price.value() % instrument.instrument().price_tick.value() != Decimal::ZERO
    {
        return Err(OkxError::Precision);
    }
    let wire = PlaceWire {
        inst_id: instrument.native_id(),
        td_mode: trade_mode.wire_value(),
        cl_ord_id: fields.client_order_id,
        side: side_text(fields.side),
        pos_side: position_side_text(fields.position_side)?,
        ord_type: fields.order_type,
        sz: contracts.normalize().to_string(),
        px: fields
            .limit_price
            .map(|price| price.value().normalize().to_string()),
        // OKX V5 only permits reduceOnly for FUTURES/SWAP in net mode. In long/short mode the
        // reducing meaning is carried by the checked side + posSide pair.
        reduce_only: None,
    };
    let body = serde_json::to_vec(&wire).map_err(|_| OkxError::Payload)?;
    Ok(OkxPlaceRequest {
        scope,
        body,
        client_order_id: fields.client_order_id.to_owned(),
        side: fields.side,
        position_side: fields.position_side,
        quantity: fields.quantity,
        contracts,
        limit_price: fields.limit_price,
        purpose: fields.purpose,
        reduce_only: fields.reduce_only,
        order_type: fields.order_type,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxAcceptedOrder {
    scope: OkxExecutionScope,
    order_id: String,
    client_order_id: String,
    accepted_at_ms: u64,
    side: OrderSide,
    position_side: PositionSide,
    quantity: Decimal,
    contracts: Decimal,
    limit_price: Option<Price>,
    purpose: OrderPurpose,
    reduce_only: bool,
    order_type: &'static str,
}

impl OkxAcceptedOrder {
    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }
    #[must_use]
    pub const fn accepted_at_ms(&self) -> u64 {
        self.accepted_at_ms
    }
    #[must_use]
    pub const fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AckRow {
    ord_id: String,
    cl_ord_id: String,
    ts: String,
    s_code: String,
    #[serde(default)]
    s_msg: String,
}

pub fn parse_place_ack(
    payload: &[u8],
    request: &OkxPlaceRequest,
) -> Result<OkxAcceptedOrder, OkxError> {
    let row = one_ack(payload)?;
    if row.cl_ord_id != request.client_order_id {
        return Err(OkxError::Identity);
    }
    validate_order_id(&row.ord_id)?;
    Ok(OkxAcceptedOrder {
        scope: request.scope.clone(),
        order_id: row.ord_id,
        client_order_id: row.cl_ord_id,
        accepted_at_ms: positive_u64(&row.ts)?,
        side: request.side,
        position_side: request.position_side,
        quantity: request.quantity,
        contracts: request.contracts,
        limit_price: request.limit_price,
        purpose: request.purpose,
        reduce_only: request.reduce_only,
        order_type: request.order_type,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxCancelRequest {
    scope: OkxExecutionScope,
    body: Vec<u8>,
    order_id: String,
    client_order_id: String,
}

impl OkxPrivateRequest for OkxCancelRequest {
    fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }
    fn method(&self) -> &'static str {
        POST
    }
    fn request_path(&self) -> &str {
        endpoints::CANCEL_ORDER
    }
    fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelWire<'a> {
    inst_id: &'a str,
    ord_id: &'a str,
}

pub fn build_cancel_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    command: &CancelCommand,
    accepted: &OkxAcceptedOrder,
) -> Result<OkxCancelRequest, OkxError> {
    command.validate().map_err(|_| OkxError::Payload)?;
    validate_owner(&command.owner, config)?;
    accepted.scope.validate(config, instrument, profile)?;
    if command.target_client_order_id.as_str() != accepted.client_order_id {
        return Err(OkxError::Identity);
    }
    let wire = CancelWire {
        inst_id: instrument.native_id(),
        // ordId is globally unambiguous for this instrument; historical clOrdId lookup is not.
        ord_id: &accepted.order_id,
    };
    Ok(OkxCancelRequest {
        scope: accepted.scope.clone(),
        body: serde_json::to_vec(&wire).map_err(|_| OkxError::Payload)?,
        order_id: accepted.order_id.clone(),
        client_order_id: accepted.client_order_id.clone(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxAcceptedCancel {
    scope: OkxExecutionScope,
    order_id: String,
    client_order_id: String,
    accepted_at_ms: u64,
}

impl OkxAcceptedCancel {
    #[must_use]
    pub const fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }
    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }
    #[must_use]
    pub const fn accepted_at_ms(&self) -> u64 {
        self.accepted_at_ms
    }
}

pub fn parse_cancel_ack(
    payload: &[u8],
    request: &OkxCancelRequest,
) -> Result<OkxAcceptedCancel, OkxError> {
    let row = one_ack(payload)?;
    if row.ord_id != request.order_id || row.cl_ord_id != request.client_order_id {
        return Err(OkxError::Identity);
    }
    Ok(OkxAcceptedCancel {
        scope: request.scope.clone(),
        order_id: row.ord_id,
        client_order_id: row.cl_ord_id,
        accepted_at_ms: positive_u64(&row.ts)?,
    })
}

fn one_ack(payload: &[u8]) -> Result<AckRow, OkxError> {
    let envelope = decode_success::<AckRow>(payload)?;
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    if row.s_code != "0" || !row.s_msg.is_empty() {
        return Err(OkxError::Rejected);
    }
    Ok(row.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxOrderReadbackRequest {
    scope: OkxExecutionScope,
    request_path: String,
    expected: OkxAcceptedOrder,
}

impl OkxPrivateRequest for OkxOrderReadbackRequest {
    fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }
    fn method(&self) -> &'static str {
        GET
    }
    fn request_path(&self) -> &str {
        &self.request_path
    }
    fn body(&self) -> &[u8] {
        &[]
    }
}

pub fn build_order_readback_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    accepted: &OkxAcceptedOrder,
) -> Result<OkxOrderReadbackRequest, OkxError> {
    accepted.scope.validate(config, instrument, profile)?;
    validate_order_id(&accepted.order_id)?;
    Ok(OkxOrderReadbackRequest {
        scope: accepted.scope.clone(),
        request_path: format!(
            "{}?instId={}&ordId={}",
            endpoints::PLACE_ORDER,
            instrument.native_id(),
            accepted.order_id
        ),
        expected: accepted.clone(),
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DetailRow {
    inst_type: String,
    inst_id: String,
    td_mode: String,
    ord_type: String,
    ord_id: String,
    cl_ord_id: String,
    side: String,
    pos_side: String,
    sz: String,
    acc_fill_sz: String,
    px: String,
    avg_px: String,
    reduce_only: String,
    state: String,
    u_time: String,
}

pub fn parse_order_detail(
    payload: &[u8],
    request: &OkxOrderReadbackRequest,
) -> Result<OkxTimedOrder, OkxError> {
    let envelope = decode_success::<DetailRow>(payload)?;
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    let expected = &request.expected;
    if row.inst_type != "SWAP"
        || row.inst_id != request.scope.native_instrument_id
        || row.td_mode != request.scope.trade_mode.wire_value()
        || row.ord_id != expected.order_id
        || row.cl_ord_id != expected.client_order_id
        || row.side != side_text(expected.side)
        || row.pos_side != position_side_text(expected.position_side)?
        || row.ord_type != expected.order_type
        || positive_decimal(&row.sz)? != expected.contracts
        || parse_optional_price(&row.px)? != expected.limit_price
    {
        return Err(OkxError::Binding);
    }
    let raw_reduce_only = parse_boolean(&row.reduce_only)?;
    if request.scope.position_mode != OkxPositionMode::LongShort || raw_reduce_only {
        return Err(OkxError::PositionMode);
    }
    let filled_contracts = decimal(&row.acc_fill_sz)?;
    if filled_contracts.is_sign_negative() || filled_contracts > expected.contracts {
        return Err(OkxError::Payload);
    }
    let state = match row.state.as_str() {
        "live" => OrderState::New,
        "partially_filled" => OrderState::PartiallyFilled,
        "filled" => OrderState::Filled,
        "canceled" | "mmp_canceled" => OrderState::Cancelled,
        "rejected" => OrderState::Rejected,
        "expired" => OrderState::Expired,
        _ => return Err(OkxError::Payload),
    };
    let order = Order {
        order_id: row.ord_id.clone(),
        client_order_id: FieldState::Known(row.cl_ord_id.clone()),
        symbol: request.scope.gateway_binding.symbol.clone(),
        side: expected.side,
        position_side: FieldState::Known(expected.position_side),
        purpose: FieldState::Known(expected.purpose),
        state,
        quantity: expected.quantity,
        filled_quantity: filled_contracts
            .checked_mul(request.scope.base_quantity_per_contract)
            .ok_or(OkxError::Payload)?,
        limit_price: expected.limit_price,
        average_price: parse_optional_price(&row.avg_px)?
            .map(FieldState::Known)
            .unwrap_or(FieldState::Missing),
        reduce_only: expected.reduce_only,
    };
    order.validate().map_err(|_| OkxError::Payload)?;
    Ok(OkxTimedOrder {
        order,
        update_time_ms: positive_u64(&row.u_time)?,
    })
}

fn validate_owner(
    owner: &venue_domain::domain::OrderOwner,
    config: &OkxConfig,
) -> Result<(), OkxError> {
    owner.validate().map_err(|_| OkxError::Binding)?;
    if owner.exchange != "okx"
        || owner.account != config.gateway_binding().trading_account_id
        || owner.symbol != config.gateway_binding().symbol
    {
        return Err(OkxError::Binding);
    }
    Ok(())
}

fn validate_client_order_id(value: &str) -> Result<(), OkxError> {
    if !(1..=32).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(OkxError::Identity);
    }
    Ok(())
}

fn validate_order_id(value: &str) -> Result<(), OkxError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(OkxError::Identity);
    }
    Ok(())
}

const fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn position_side_text(side: PositionSide) -> Result<&'static str, OkxError> {
    match side {
        PositionSide::Long => Ok("long"),
        PositionSide::Short => Ok("short"),
        PositionSide::Net => Err(OkxError::PositionMode),
    }
}

fn parse_boolean(value: &str) -> Result<bool, OkxError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(OkxError::Payload),
    }
}

fn parse_optional_price(value: &str) -> Result<Option<Price>, OkxError> {
    if value.is_empty() || value == "0" {
        Ok(None)
    } else {
        Price::new(positive_decimal(value)?)
            .map(Some)
            .map_err(|_| OkxError::Payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::domain::{CommandId, OrderOwner};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const PROFILE: &[u8] = include_bytes!("../fixtures/account-config.json");
    const PLACE_ACK: &[u8] = include_bytes!("../fixtures/execution-place-ack.json");
    const CANCEL_ACK: &[u8] = include_bytes!("../fixtures/execution-cancel-ack.json");
    const ORDER_DETAIL: &[u8] = include_bytes!("../fixtures/execution-order-detail.json");

    fn scope(
        mode: GatewayMode,
    ) -> Result<(OkxConfig, OkxInstrument, OkxAccountProfile), Box<dyn std::error::Error>> {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = crate::parse_instrument(INSTRUMENT, &config, 7)?;
        let profile = crate::parse_account_profile(PROFILE, OkxPositionMode::LongShort)?;
        Ok((config, instrument, profile))
    }

    fn owner(purpose: OrderPurpose) -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "grid1".to_owned(),
            run_id: "run1".to_owned(),
            exchange: "okx".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            purpose,
        })
    }

    fn limit() -> Result<OrderCommand, Box<dyn std::error::Error>> {
        Ok(OrderCommand {
            command_id: CommandId::new("place3")?,
            client_order_id: CommandId::new("00000000000000000000000000000003")?,
            owner: owner(OrderPurpose::Reduce)?,
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 1),
            limit_price: Price::new(Decimal::new(60_000, 0))?,
            reduce_only: true,
        })
    }

    #[test]
    fn place_cancel_and_detail_form_one_bound_signed_flow() -> Result<(), Box<dyn std::error::Error>>
    {
        let (config, instrument, profile) = scope(GatewayMode::Test)?;
        let command = limit()?;
        let place = build_place_request(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            OkxPlaceIntent::Limit(&command),
        )?;
        assert_eq!(
            std::str::from_utf8(place.body())?,
            r#"{"instId":"BTC-USDT-SWAP","tdMode":"cross","clOrdId":"00000000000000000000000000000003","side":"sell","posSide":"long","ordType":"limit","sz":"2","px":"60000"}"#
        );
        assert_eq!(place.scope().gateway_binding().mode, GatewayMode::Test);
        let headers = place.signed_headers(
            &OkxCredentials::from_values("key", "secret", "pass")?,
            &config,
            "2026-08-29T01:02:03.000Z",
        )?;
        assert_eq!(headers.get("x-simulated-trading"), Some("1"));

        // sCode=0 is acceptance only; no terminal state is inferred here.
        let accepted = parse_place_ack(PLACE_ACK, &place)?;
        assert_eq!(accepted.order_id(), "7003");
        let cancel_command = CancelCommand {
            command_id: CommandId::new("cancel3")?,
            owner: owner(OrderPurpose::Reduce)?,
            target_client_order_id: CommandId::new(accepted.client_order_id())?,
        };
        let cancel =
            build_cancel_request(&config, &instrument, &profile, &cancel_command, &accepted)?;
        assert_eq!(
            std::str::from_utf8(cancel.body())?,
            r#"{"instId":"BTC-USDT-SWAP","ordId":"7003"}"#
        );
        let cancel_accepted = parse_cancel_ack(CANCEL_ACK, &cancel)?;
        assert_eq!(cancel_accepted.order_id(), "7003");

        let readback = build_order_readback_request(&config, &instrument, &profile, &accepted)?;
        assert_eq!(
            readback.request_path(),
            "/api/v5/trade/order?instId=BTC-USDT-SWAP&ordId=7003"
        );
        let readback_headers = readback.signed_headers(
            &OkxCredentials::from_values("key", "secret", "pass")?,
            &config,
            "2026-08-29T01:02:04.000Z",
        )?;
        assert_eq!(readback_headers.get("x-simulated-trading"), Some("1"));
        let order = parse_order_detail(ORDER_DETAIL, &readback)?;
        assert_eq!(order.order.state, OrderState::Cancelled);
        assert_eq!(order.order.quantity, Decimal::new(2, 1));
        assert!(order.order.reduce_only);
        Ok(())
    }

    #[test]
    fn precision_mode_identity_and_environment_mismatches_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (live, instrument, profile) = scope(GatewayMode::Live)?;
        let mut command = limit()?;
        command.quantity = Decimal::new(25, 2);
        assert_eq!(
            build_place_request(
                &live,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                OkxPlaceIntent::Limit(&command)
            ),
            Err(OkxError::Precision)
        );
        command = limit()?;
        command.limit_price = Price::new(Decimal::new(6_000_005, 2))?;
        assert_eq!(
            build_place_request(
                &live,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                OkxPlaceIntent::Limit(&command)
            ),
            Err(OkxError::Precision)
        );
        command = limit()?;
        command.client_order_id = CommandId::new("client_id")?;
        assert_eq!(
            build_place_request(
                &live,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                OkxPlaceIntent::Limit(&command)
            ),
            Err(OkxError::Identity)
        );

        let place = build_place_request(
            &live,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            OkxPlaceIntent::Limit(&limit()?),
        )?;
        assert_eq!(
            parse_place_ack(
                br#"{"code":"0","msg":"","data":[{"ordId":"7003","clOrdId":"wrong","ts":"1","sCode":"0","sMsg":""}]}"#,
                &place
            ),
            Err(OkxError::Identity)
        );
        let (test, _, _) = scope(GatewayMode::Test)?;
        assert_eq!(
            place
                .signed_headers(
                    &OkxCredentials::from_values("key", "secret", "pass")?,
                    &test,
                    "2026-08-29T01:02:03.000Z"
                )
                .err(),
            Some(OkxError::Binding)
        );
        Ok(())
    }
}
