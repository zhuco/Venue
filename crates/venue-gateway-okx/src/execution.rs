use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    CancelCommand, FieldState, LimitTimeInForce, MarketOrderCommand, MarketReduceCommand, Order,
    OrderCommand, OrderPurpose, OrderSide, OrderState, PositionSide, Price,
};
use venue_gateway_api::GatewayBinding;

use crate::private::{
    OkxAccountLevel, OkxAccountProfile, OkxTimedOrder, time_in_force_from_order_type,
};
use crate::public::{decimal, decode_success, positive_decimal, positive_u64};
use crate::{
    OkxConfig, OkxCredentials, OkxError, OkxHttpResponse, OkxInstrument, OkxPositionMode,
    OkxTradeMode, SignedHeaders, endpoints, sign,
};

const POST: &str = "POST";
const GET: &str = "GET";

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
    pub(crate) fn new(
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        trade_mode: OkxTradeMode,
    ) -> Result<Self, OkxError> {
        instrument.validate_scope(config)?;
        if profile.uid().is_empty()
            || profile.main_uid().is_empty()
            || !profile.supports_trade_mode(trade_mode)
        {
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
    pub const fn instrument_generation(&self) -> u64 {
        self.instrument_generation
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

/// Narrow host-only cancel request keyed by the durable client identity. The type is crate-private
/// so no caller can reach the transport without an account-host dispatch permit.
pub(crate) struct OkxHostCancelRequest {
    scope: OkxExecutionScope,
    body: Vec<u8>,
    client_order_id: String,
}

impl OkxPrivateRequest for OkxHostCancelRequest {
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
struct HostCancelWire<'a> {
    inst_id: &'a str,
    cl_ord_id: &'a str,
}

pub(crate) fn build_host_cancel_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    trade_mode: OkxTradeMode,
    command: &CancelCommand,
) -> Result<OkxHostCancelRequest, OkxError> {
    command.validate().map_err(|_| OkxError::Payload)?;
    validate_owner(&command.owner, config)?;
    validate_client_order_id(command.target_client_order_id.as_str())?;
    let scope = OkxExecutionScope::new(config, instrument, profile, trade_mode)?;
    let wire = HostCancelWire {
        inst_id: instrument.native_id(),
        cl_ord_id: command.target_client_order_id.as_str(),
    };
    Ok(OkxHostCancelRequest {
        scope,
        body: serde_json::to_vec(&wire).map_err(|_| OkxError::Payload)?,
        client_order_id: command.target_client_order_id.as_str().to_owned(),
    })
}

pub(crate) fn parse_host_cancel_ack(
    response: OkxHttpResponse,
    request: &OkxHostCancelRequest,
) -> Result<String, OkxError> {
    validate_http_response(&response, &request.scope)?;
    let row = one_ack(&response.body)?;
    if row.cl_ord_id != request.client_order_id {
        return Err(OkxError::Identity);
    }
    validate_order_id(&row.ord_id)?;
    Ok(row.ord_id)
}

pub(crate) struct OkxHostOrderLookupRequest {
    scope: OkxExecutionScope,
    request_path: String,
    client_order_id: String,
}

impl OkxPrivateRequest for OkxHostOrderLookupRequest {
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

pub(crate) fn build_host_order_lookup_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    trade_mode: OkxTradeMode,
    client_order_id: &str,
) -> Result<OkxHostOrderLookupRequest, OkxError> {
    validate_client_order_id(client_order_id)?;
    let scope = OkxExecutionScope::new(config, instrument, profile, trade_mode)?;
    Ok(OkxHostOrderLookupRequest {
        scope,
        request_path: format!(
            "{}?instId={}&clOrdId={client_order_id}",
            endpoints::PLACE_ORDER,
            instrument.native_id()
        ),
        client_order_id: client_order_id.to_owned(),
    })
}

pub(crate) fn parse_host_order_lookup(
    response: OkxHttpResponse,
    request: &OkxHostOrderLookupRequest,
) -> Result<Option<(String, OrderState, Option<LimitTimeInForce>)>, OkxError> {
    validate_http_response(&response, &request.scope)?;
    let envelope = decode_success::<DetailRow>(&response.body)?;
    if envelope.data.is_empty() {
        return Ok(None);
    }
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Identity);
    };
    if row.inst_type != "SWAP"
        || row.inst_id != request.scope.native_instrument_id
        || row.td_mode != request.scope.trade_mode.wire_value()
        || row.cl_ord_id != request.client_order_id
    {
        return Err(OkxError::Binding);
    }
    validate_order_id(&row.ord_id)?;
    let state = parse_order_state(&row.state)?;
    let time_in_force = match time_in_force_from_order_type(&row.ord_type) {
        FieldState::Known(value) => Some(value),
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => None,
    };
    Ok(Some((row.ord_id.clone(), state, time_in_force)))
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
                    order_type: limit_order_type(command.time_in_force),
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

const fn limit_order_type(value: LimitTimeInForce) -> &'static str {
    match value {
        LimitTimeInForce::PostOnly => "post_only",
        LimitTimeInForce::Gtc => "limit",
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

pub(crate) trait OkxPrivateRequest {
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
    ack_received_at_ms: u64,
    ack_payload_sha256: String,
    raw_ack_payload: Vec<u8>,
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
    pub const fn ack_received_at_ms(&self) -> u64 {
        self.ack_received_at_ms
    }
    #[must_use]
    pub const fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }

    #[must_use]
    pub fn ack_payload_sha256(&self) -> &str {
        &self.ack_payload_sha256
    }

    #[must_use]
    pub fn raw_ack_payload(&self) -> &[u8] {
        &self.raw_ack_payload
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AckRow {
    ord_id: String,
    cl_ord_id: String,
    ts: String,
    s_code: String,
    #[serde(default, rename = "sMsg")]
    _s_msg: String,
}

pub fn parse_place_ack(
    response: OkxHttpResponse,
    request: &OkxPlaceRequest,
) -> Result<OkxAcceptedOrder, OkxError> {
    validate_http_response(&response, &request.scope)?;
    let row = one_ack(&response.body)?;
    if row.cl_ord_id != request.client_order_id {
        return Err(OkxError::Identity);
    }
    validate_order_id(&row.ord_id)?;
    let accepted_at_ms = positive_u64(&row.ts)?;
    if accepted_at_ms > response.received_at_ms {
        return Err(OkxError::Sequence);
    }
    Ok(OkxAcceptedOrder {
        scope: request.scope.clone(),
        order_id: row.ord_id,
        client_order_id: row.cl_ord_id,
        accepted_at_ms,
        ack_received_at_ms: response.received_at_ms,
        ack_payload_sha256: crate::readback::payload_digest(&response.body),
        raw_ack_payload: response.body.to_vec(),
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
    ack_received_at_ms: u64,
    ack_payload_sha256: String,
    raw_ack_payload: Vec<u8>,
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

    #[must_use]
    pub const fn ack_received_at_ms(&self) -> u64 {
        self.ack_received_at_ms
    }

    #[must_use]
    pub fn ack_payload_sha256(&self) -> &str {
        &self.ack_payload_sha256
    }

    #[must_use]
    pub fn raw_ack_payload(&self) -> &[u8] {
        &self.raw_ack_payload
    }
}

pub fn parse_cancel_ack(
    response: OkxHttpResponse,
    request: &OkxCancelRequest,
) -> Result<OkxAcceptedCancel, OkxError> {
    validate_http_response(&response, &request.scope)?;
    let row = one_ack(&response.body)?;
    if row.ord_id != request.order_id || row.cl_ord_id != request.client_order_id {
        return Err(OkxError::Identity);
    }
    let accepted_at_ms = positive_u64(&row.ts)?;
    if accepted_at_ms > response.received_at_ms {
        return Err(OkxError::Sequence);
    }
    Ok(OkxAcceptedCancel {
        scope: request.scope.clone(),
        order_id: row.ord_id,
        client_order_id: row.cl_ord_id,
        accepted_at_ms,
        ack_received_at_ms: response.received_at_ms,
        ack_payload_sha256: crate::readback::payload_digest(&response.body),
        raw_ack_payload: response.body.to_vec(),
    })
}

fn one_ack(payload: &[u8]) -> Result<AckRow, OkxError> {
    let envelope = decode_success::<AckRow>(payload)?;
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    if row.s_code != "0" {
        return Err(OkxError::Rejected);
    }
    Ok(row.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxOrderReadbackRequest {
    scope: OkxExecutionScope,
    request_path: String,
    expected: OkxAcceptedOrder,
    anchor: OkxOrderReadbackAnchor,
    not_before_ms: u64,
}

fn validate_http_response(
    response: &OkxHttpResponse,
    scope: &OkxExecutionScope,
) -> Result<(), OkxError> {
    if response.binding != *scope.gateway_binding()
        || response.instrument_generation != scope.instrument_generation()
        || response.received_at_ms == 0
        || response.body.is_empty()
    {
        return Err(OkxError::Binding);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxOrderReadbackAnchor {
    PlaceAck,
    CancelAck,
    CancelDispatch,
}

impl OkxCancelRequest {
    fn matches_accepted(&self, accepted: &OkxAcceptedOrder) -> bool {
        self.scope == accepted.scope
            && self.order_id == accepted.order_id
            && self.client_order_id == accepted.client_order_id
    }
}

impl OkxPlaceRequest {
    #[must_use]
    pub const fn is_reduce_once(&self) -> bool {
        self.reduce_only && matches!(self.purpose, OrderPurpose::ExposureTakeProfit)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxOrderReadback {
    pub scope: OkxExecutionScope,
    pub anchor: OkxOrderReadbackAnchor,
    pub request_path: String,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub raw_payload: Vec<u8>,
    pub order: OkxTimedOrder,
}

/// Exact post-dispatch readback keyed by the original client identity. It is usable when the
/// transport outcome is UNKNOWN and therefore no accepted venue order ID exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxUnknownOrderReadbackRequest {
    scope: OkxExecutionScope,
    request_path: String,
    expected: OkxPlaceRequest,
    not_before_ms: u64,
}

impl OkxPrivateRequest for OkxUnknownOrderReadbackRequest {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxUnknownOrderReadback {
    pub scope: OkxExecutionScope,
    pub request_path: String,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub raw_payload: Vec<u8>,
    pub order: OkxTimedOrder,
}

/// Read-only convergence request retained when a cancel dispatch has no trustworthy ACK.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxUnknownCancelReadbackRequest {
    scope: OkxExecutionScope,
    request_path: String,
    expected: OkxAcceptedOrder,
    not_before_ms: u64,
}

impl OkxPrivateRequest for OkxUnknownCancelReadbackRequest {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxUnknownCancelResolution {
    Terminal(OkxOrderReadback),
    StillUnknown(OkxOrderReadback),
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
        anchor: OkxOrderReadbackAnchor::PlaceAck,
        not_before_ms: accepted.ack_received_at_ms,
    })
}

pub fn build_cancel_order_readback_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    accepted_order: &OkxAcceptedOrder,
    accepted_cancel: &OkxAcceptedCancel,
) -> Result<OkxOrderReadbackRequest, OkxError> {
    accepted_order.scope.validate(config, instrument, profile)?;
    if accepted_cancel.scope != accepted_order.scope
        || accepted_cancel.order_id != accepted_order.order_id
        || accepted_cancel.client_order_id != accepted_order.client_order_id
        || accepted_cancel.accepted_at_ms < accepted_order.accepted_at_ms
        || accepted_cancel.ack_received_at_ms < accepted_order.ack_received_at_ms
    {
        return Err(OkxError::Identity);
    }
    validate_order_id(&accepted_order.order_id)?;
    Ok(OkxOrderReadbackRequest {
        scope: accepted_order.scope.clone(),
        request_path: format!(
            "{}?instId={}&ordId={}",
            endpoints::PLACE_ORDER,
            instrument.native_id(),
            accepted_order.order_id
        ),
        expected: accepted_order.clone(),
        anchor: OkxOrderReadbackAnchor::CancelAck,
        not_before_ms: accepted_cancel.ack_received_at_ms,
    })
}

pub fn build_unknown_order_readback_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    submitted: &OkxPlaceRequest,
) -> Result<OkxUnknownOrderReadbackRequest, OkxError> {
    build_unknown_order_readback_request_after(config, instrument, profile, submitted, 0)
}

pub fn build_unknown_order_readback_request_after(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    submitted: &OkxPlaceRequest,
    not_before_ms: u64,
) -> Result<OkxUnknownOrderReadbackRequest, OkxError> {
    submitted.scope.validate(config, instrument, profile)?;
    validate_client_order_id(&submitted.client_order_id)?;
    Ok(OkxUnknownOrderReadbackRequest {
        scope: submitted.scope.clone(),
        request_path: format!(
            "{}?instId={}&clOrdId={}",
            endpoints::PLACE_ORDER,
            instrument.native_id(),
            submitted.client_order_id
        ),
        expected: submitted.clone(),
        not_before_ms,
    })
}

pub fn build_unknown_cancel_readback_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    submitted: &OkxCancelRequest,
    accepted: &OkxAcceptedOrder,
    not_before_ms: u64,
) -> Result<OkxUnknownCancelReadbackRequest, OkxError> {
    accepted.scope.validate(config, instrument, profile)?;
    if !submitted.matches_accepted(accepted) {
        return Err(OkxError::Identity);
    }
    validate_order_id(&accepted.order_id)?;
    if not_before_ms == 0 {
        return Err(OkxError::Sequence);
    }
    Ok(OkxUnknownCancelReadbackRequest {
        scope: accepted.scope.clone(),
        request_path: format!(
            "{}?instId={}&ordId={}",
            endpoints::PLACE_ORDER,
            instrument.native_id(),
            accepted.order_id
        ),
        expected: accepted.clone(),
        not_before_ms,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DetailRow {
    inst_type: String,
    inst_id: String,
    td_mode: String,
    #[serde(default)]
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
    response: OkxHttpResponse,
    request: &OkxOrderReadbackRequest,
) -> Result<OkxOrderReadback, OkxError> {
    validate_http_response(&response, &request.scope)?;
    if response.received_at_ms < request.not_before_ms {
        return Err(OkxError::Sequence);
    }
    let order = parse_bound_order_detail(
        &response.body,
        &request.scope,
        Some(&request.expected.order_id),
        &request.expected.client_order_id,
        request.expected.side,
        request.expected.position_side,
        request.expected.quantity,
        request.expected.contracts,
        request.expected.limit_price,
        request.expected.purpose,
        request.expected.reduce_only,
        request.expected.order_type,
    )?;
    if order.update_time_ms > response.received_at_ms {
        return Err(OkxError::Sequence);
    }
    Ok(OkxOrderReadback {
        scope: request.scope.clone(),
        anchor: request.anchor,
        request_path: request.request_path.clone(),
        received_at_ms: response.received_at_ms,
        payload_sha256: crate::readback::payload_digest(&response.body),
        raw_payload: response.body.to_vec(),
        order,
    })
}

pub fn parse_unknown_order_readback(
    response: OkxHttpResponse,
    request: &OkxUnknownOrderReadbackRequest,
) -> Result<OkxUnknownOrderReadback, OkxError> {
    validate_http_response(&response, &request.scope)?;
    if response.received_at_ms < request.not_before_ms {
        return Err(OkxError::Sequence);
    }
    let order = parse_bound_order_detail(
        &response.body,
        &request.scope,
        None,
        &request.expected.client_order_id,
        request.expected.side,
        request.expected.position_side,
        request.expected.quantity,
        request.expected.contracts,
        request.expected.limit_price,
        request.expected.purpose,
        request.expected.reduce_only,
        request.expected.order_type,
    )?;
    if order.update_time_ms > response.received_at_ms {
        return Err(OkxError::Sequence);
    }
    Ok(OkxUnknownOrderReadback {
        scope: request.scope.clone(),
        request_path: request.request_path.clone(),
        received_at_ms: response.received_at_ms,
        payload_sha256: crate::readback::payload_digest(&response.body),
        raw_payload: response.body.to_vec(),
        order,
    })
}

pub fn parse_unknown_cancel_readback(
    response: OkxHttpResponse,
    request: &OkxUnknownCancelReadbackRequest,
) -> Result<OkxUnknownCancelResolution, OkxError> {
    validate_http_response(&response, &request.scope)?;
    if response.received_at_ms < request.not_before_ms {
        return Err(OkxError::Sequence);
    }
    let order = parse_bound_order_detail(
        &response.body,
        &request.scope,
        Some(&request.expected.order_id),
        &request.expected.client_order_id,
        request.expected.side,
        request.expected.position_side,
        request.expected.quantity,
        request.expected.contracts,
        request.expected.limit_price,
        request.expected.purpose,
        request.expected.reduce_only,
        request.expected.order_type,
    )?;
    if order.update_time_ms > response.received_at_ms {
        return Err(OkxError::Sequence);
    }
    let readback = OkxOrderReadback {
        scope: request.scope.clone(),
        anchor: OkxOrderReadbackAnchor::CancelDispatch,
        request_path: request.request_path.clone(),
        received_at_ms: response.received_at_ms,
        payload_sha256: crate::readback::payload_digest(&response.body),
        raw_payload: response.body.to_vec(),
        order,
    };
    if matches!(
        readback.order.order.state,
        OrderState::Cancelled | OrderState::Filled | OrderState::Expired | OrderState::Rejected
    ) {
        Ok(OkxUnknownCancelResolution::Terminal(readback))
    } else {
        Ok(OkxUnknownCancelResolution::StillUnknown(readback))
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_bound_order_detail(
    payload: &[u8],
    scope: &OkxExecutionScope,
    expected_order_id: Option<&str>,
    expected_client_order_id: &str,
    expected_side: OrderSide,
    expected_position_side: PositionSide,
    expected_quantity: Decimal,
    expected_contracts: Decimal,
    expected_limit_price: Option<Price>,
    expected_purpose: OrderPurpose,
    expected_reduce_only: bool,
    expected_order_type: &'static str,
) -> Result<OkxTimedOrder, OkxError> {
    let envelope = decode_success::<DetailRow>(payload)?;
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    if row.inst_type != "SWAP"
        || row.inst_id != scope.native_instrument_id
        || row.td_mode != scope.trade_mode.wire_value()
        || expected_order_id.is_some_and(|expected| row.ord_id != expected)
        || row.cl_ord_id != expected_client_order_id
        || row.side != side_text(expected_side)
        || row.pos_side != position_side_text(expected_position_side)?
        || row.ord_type != expected_order_type
        || positive_decimal(&row.sz)? != expected_contracts
        || parse_optional_price(&row.px)? != expected_limit_price
    {
        return Err(OkxError::Binding);
    }
    validate_order_id(&row.ord_id)?;
    let raw_reduce_only = parse_boolean(&row.reduce_only)?;
    if scope.position_mode != OkxPositionMode::LongShort || raw_reduce_only {
        return Err(OkxError::PositionMode);
    }
    let filled_contracts = decimal(&row.acc_fill_sz)?;
    if filled_contracts.is_sign_negative() || filled_contracts > expected_contracts {
        return Err(OkxError::Payload);
    }
    let state = parse_order_state(&row.state)?;
    let order = Order {
        order_id: row.ord_id.clone(),
        client_order_id: FieldState::Known(row.cl_ord_id.clone()),
        symbol: scope.gateway_binding.symbol.clone(),
        side: expected_side,
        position_side: FieldState::Known(expected_position_side),
        purpose: FieldState::Known(expected_purpose),
        state,
        quantity: expected_quantity,
        filled_quantity: filled_contracts
            .checked_mul(scope.base_quantity_per_contract)
            .ok_or(OkxError::Payload)?,
        limit_price: expected_limit_price,
        time_in_force: time_in_force_from_order_type(&row.ord_type),
        average_price: parse_optional_price(&row.avg_px)?
            .map(FieldState::Known)
            .unwrap_or(FieldState::Missing),
        reduce_only: expected_reduce_only,
    };
    order.validate().map_err(|_| OkxError::Payload)?;
    Ok(OkxTimedOrder {
        order,
        update_time_ms: positive_u64(&row.u_time)?,
    })
}

fn parse_order_state(value: &str) -> Result<OrderState, OkxError> {
    Ok(match value {
        "live" => OrderState::New,
        "partially_filled" => OrderState::PartiallyFilled,
        "filled" => OrderState::Filled,
        "canceled" | "mmp_canceled" => OrderState::Cancelled,
        "rejected" => OrderState::Rejected,
        "expired" => OrderState::Expired,
        _ => return Err(OkxError::Payload),
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
    const MARKET_REDUCE_REQUEST: &str = include_str!("../fixtures/market-reduce-request.json");

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
            time_in_force: LimitTimeInForce::Gtc,
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
    fn limit_policy_maps_to_distinct_okx_wire_types() -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope(GatewayMode::Live)?;
        let mut command = limit()?;
        command.time_in_force = LimitTimeInForce::PostOnly;
        let post_only = build_place_request(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            OkxPlaceIntent::Limit(&command),
        )?;
        let body: serde_json::Value = serde_json::from_slice(post_only.body())?;
        assert_eq!(body["ordType"], "post_only");

        command.time_in_force = LimitTimeInForce::Gtc;
        let gtc = build_place_request(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            OkxPlaceIntent::Limit(&command),
        )?;
        let body: serde_json::Value = serde_json::from_slice(gtc.body())?;
        assert_eq!(body["ordType"], "limit");
        Ok(())
    }

    #[test]
    fn post_only_unknown_readback_does_not_accept_gtc_or_missing_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope(GatewayMode::Live)?;
        let mut command = limit()?;
        command.time_in_force = LimitTimeInForce::PostOnly;
        let request = build_place_request(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            OkxPlaceIntent::Limit(&command),
        )?;
        let lookup =
            build_unknown_order_readback_request(&config, &instrument, &profile, &request)?;
        assert_eq!(
            parse_unknown_order_readback(
                response(&config, &instrument, 1_900_000_000_000, ORDER_DETAIL),
                &lookup,
            ),
            Err(OkxError::Binding)
        );
        let missing_policy =
            String::from_utf8(ORDER_DETAIL.to_vec())?.replace("    \"ordType\": \"limit\",\n", "");
        assert_eq!(
            parse_unknown_order_readback(
                OkxHttpResponse {
                    binding: config.gateway_binding().clone(),
                    instrument_generation: instrument.instrument().generation,
                    received_at_ms: 1_900_000_000_000,
                    body: bytes::Bytes::from(missing_policy),
                },
                &lookup,
            ),
            Err(OkxError::Binding)
        );
        Ok(())
    }

    fn market_reduce() -> Result<MarketReduceCommand, Box<dyn std::error::Error>> {
        Ok(MarketReduceCommand {
            command_id: CommandId::new("reduce4")?,
            client_order_id: CommandId::new("00000000000000000000000000000004")?,
            owner: owner(OrderPurpose::ExposureTakeProfit)?,
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 1),
            risk_episode_id: CommandId::new("episode4")?,
            position_generation: 4,
        })
    }

    #[test]
    fn market_reduce_converts_exact_contracts_and_uses_checked_close_direction()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope(GatewayMode::Live)?;
        let command = market_reduce()?;
        let request = build_place_request(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            OkxPlaceIntent::MarketReduce(&command),
        )?;
        assert_eq!(
            std::str::from_utf8(request.body())?,
            MARKET_REDUCE_REQUEST.trim()
        );
        let mut wrong_side = command.clone();
        wrong_side.side = OrderSide::Buy;
        assert_eq!(
            build_place_request(
                &config,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                OkxPlaceIntent::MarketReduce(&wrong_side),
            ),
            Err(OkxError::Payload)
        );
        let mut off_contract = command;
        off_contract.quantity = Decimal::new(15, 2);
        assert_eq!(
            build_place_request(
                &config,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                OkxPlaceIntent::MarketReduce(&off_contract),
            ),
            Err(OkxError::Precision)
        );
        Ok(())
    }

    fn response(
        config: &OkxConfig,
        instrument: &OkxInstrument,
        received_at_ms: u64,
        payload: &'static [u8],
    ) -> OkxHttpResponse {
        OkxHttpResponse {
            binding: config.gateway_binding().clone(),
            instrument_generation: instrument.instrument().generation,
            received_at_ms,
            body: bytes::Bytes::from_static(payload),
        }
    }

    #[test]
    fn place_cancel_and_detail_form_one_bound_signed_flow() -> Result<(), Box<dyn std::error::Error>>
    {
        let (config, instrument, profile) = scope(GatewayMode::Live)?;
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
        assert_eq!(place.scope().gateway_binding().mode, GatewayMode::Live);
        let headers = place.signed_headers(
            &OkxCredentials::from_values("key", "secret", "pass")?,
            &config,
            "2026-08-29T01:02:03.000Z",
        )?;
        assert_eq!(headers.get("x-simulated-trading"), None);

        // sCode=0 is acceptance only; no terminal state is inferred here.
        let accepted = parse_place_ack(
            response(&config, &instrument, 1_787_911_200_350, PLACE_ACK),
            &place,
        )?;
        assert_eq!(accepted.order_id(), "7003");
        assert_eq!(accepted.ack_payload_sha256().len(), 64);
        assert_eq!(
            parse_place_ack(
                response(
                    &config,
                    &instrument,
                    1_787_911_200_350,
                    br#"{"code":"0","msg":"","data":[{"ordId":"7003","clOrdId":"00000000000000000000000000000003","ts":"1787911200300","sCode":"0","sMsg":"Order placed"}]}"#,
                ),
                &place,
            )?
            .order_id(),
            "7003"
        );
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
        let cancel_accepted = parse_cancel_ack(
            response(&config, &instrument, 1_787_911_200_450, CANCEL_ACK),
            &cancel,
        )?;
        assert_eq!(cancel_accepted.order_id(), "7003");

        let readback = build_cancel_order_readback_request(
            &config,
            &instrument,
            &profile,
            &accepted,
            &cancel_accepted,
        )?;
        assert_eq!(
            readback.request_path(),
            "/api/v5/trade/order?instId=BTC-USDT-SWAP&ordId=7003"
        );
        let readback_headers = readback.signed_headers(
            &OkxCredentials::from_values("key", "secret", "pass")?,
            &config,
            "2026-08-29T01:02:04.000Z",
        )?;
        assert_eq!(readback_headers.get("x-simulated-trading"), None);
        let order = parse_order_detail(
            response(&config, &instrument, 1_787_911_200_600, ORDER_DETAIL),
            &readback,
        )?;
        assert_eq!(order.anchor, OkxOrderReadbackAnchor::CancelAck);
        assert_eq!(order.order.order.state, OrderState::Cancelled);
        assert_eq!(order.order.order.quantity, Decimal::new(2, 1));
        assert!(order.order.order.reduce_only);
        assert_eq!(order.payload_sha256.len(), 64);
        assert_eq!(
            parse_order_detail(
                response(&config, &instrument, 1_787_911_200_430, ORDER_DETAIL),
                &readback,
            ),
            Err(OkxError::Sequence)
        );

        let unknown = build_unknown_order_readback_request(&config, &instrument, &profile, &place)?;
        assert_eq!(
            unknown.request_path(),
            "/api/v5/trade/order?instId=BTC-USDT-SWAP&clOrdId=00000000000000000000000000000003"
        );
        let unknown = parse_unknown_order_readback(
            OkxHttpResponse {
                binding: config.gateway_binding().clone(),
                instrument_generation: instrument.instrument().generation,
                received_at_ms: 1_900_000_000_000,
                body: bytes::Bytes::copy_from_slice(ORDER_DETAIL),
            },
            &unknown,
        )?;
        assert_eq!(unknown.order.order.order_id, "7003");
        assert_eq!(unknown.raw_payload, ORDER_DETAIL);
        assert_eq!(unknown.payload_sha256.len(), 64);

        let cancel_unknown = build_unknown_cancel_readback_request(
            &config,
            &instrument,
            &profile,
            &cancel,
            &accepted,
            1_787_911_200_450,
        )?;
        assert_eq!(cancel_unknown.method(), "GET");
        let resolved = parse_unknown_cancel_readback(
            response(&config, &instrument, 1_787_911_200_600, ORDER_DETAIL),
            &cancel_unknown,
        )?;
        assert!(matches!(
            resolved,
            OkxUnknownCancelResolution::Terminal(ref value)
                if value.anchor == OkxOrderReadbackAnchor::CancelDispatch
                    && value.order.order.state == OrderState::Cancelled
        ));
        assert_eq!(
            parse_unknown_cancel_readback(
                response(&config, &instrument, 1_787_911_200_440, ORDER_DETAIL),
                &cancel_unknown,
            ),
            Err(OkxError::Sequence)
        );
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
            build_place_request(
                &live,
                &instrument,
                &profile,
                OkxTradeMode::Isolated,
                OkxPlaceIntent::Limit(&limit()?),
            ),
            Err(OkxError::Binding)
        );
        let net_profile = crate::parse_account_profile(
            br#"{"code":"0","msg":"","data":[{"uid":"fixture-sub-account","mainUid":"fixture-main-account","acctLv":"3","posMode":"net_mode","perm":"read_only,trade"}]}"#,
            OkxPositionMode::Net,
        )?;
        assert_eq!(
            build_place_request(
                &live,
                &instrument,
                &net_profile,
                OkxTradeMode::Cross,
                OkxPlaceIntent::Limit(&limit()?),
            ),
            Err(OkxError::PositionMode)
        );
        assert_eq!(
            parse_place_ack(
                OkxHttpResponse {
                    binding: live.gateway_binding().clone(),
                    instrument_generation: instrument.instrument().generation,
                    received_at_ms: 2,
                    body: bytes::Bytes::from_static(
                        br#"{"code":"0","msg":"","data":[{"ordId":"7003","clOrdId":"wrong","ts":"1","sCode":"0","sMsg":""}]}"#,
                    ),
                },
                &place,
            ),
            Err(OkxError::Identity)
        );
        assert_eq!(
            parse_place_ack(
                OkxHttpResponse {
                    binding: live.gateway_binding().clone(),
                    instrument_generation: instrument.instrument().generation + 1,
                    received_at_ms: 1_787_911_200_350,
                    body: bytes::Bytes::from_static(PLACE_ACK),
                },
                &place,
            ),
            Err(OkxError::Binding)
        );
        let wrong = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "ETH/USDT".parse()?,
        )?)?;
        assert_eq!(
            place
                .signed_headers(
                    &OkxCredentials::from_values("key", "secret", "pass")?,
                    &wrong,
                    "2026-08-29T01:02:03.000Z"
                )
                .err(),
            Some(OkxError::Binding)
        );
        Ok(())
    }
}
