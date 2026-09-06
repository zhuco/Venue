use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    OrderPurpose, OrderSide, PositionSide, Price, StopMarketFullPositionCommand,
};

use crate::execution::{
    OkxExecutionScope, OkxPrivateRequest, position_side_text, side_text, validate_client_order_id,
    validate_order_id, validate_owner,
};
use crate::models::Envelope;
use crate::public::{decode_success, positive_decimal, positive_u64};
use crate::{
    OkxAccountProfile, OkxConfig, OkxError, OkxHttpResponse, OkxInstrument, OkxPositionMode,
    OkxTradeMode, endpoints,
};

const POST: &str = "POST";
const GET: &str = "GET";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxAlgoPlaceRequest {
    scope: OkxExecutionScope,
    body: Vec<u8>,
    client_id: String,
    side: OrderSide,
    position_side: PositionSide,
    quantity: Decimal,
    contracts: Decimal,
    trigger_price: Price,
    purpose: OrderPurpose,
}

impl OkxPrivateRequest for OkxAlgoPlaceRequest {
    fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }
    fn method(&self) -> &'static str {
        POST
    }
    fn request_path(&self) -> &str {
        endpoints::ALGO_ORDER
    }
    fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AlgoPlaceWire<'a> {
    inst_id: &'a str,
    td_mode: &'static str,
    algo_cl_ord_id: &'a str,
    side: &'static str,
    pos_side: &'static str,
    ord_type: &'static str,
    sz: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sl_trigger_px: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sl_ord_px: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sl_trigger_px_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tp_trigger_px: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tp_ord_px: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tp_trigger_px_type: Option<&'static str>,
}

pub(crate) fn build_algo_place_request(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    trade_mode: OkxTradeMode,
    command: &StopMarketFullPositionCommand,
) -> Result<OkxAlgoPlaceRequest, OkxError> {
    command.validate().map_err(|_| OkxError::Payload)?;
    validate_owner(&command.owner, config)?;
    validate_client_order_id(command.client_algo_id.as_str())?;
    if profile.position_mode() != OkxPositionMode::LongShort
        || command.trigger_price.value() % instrument.instrument().price_tick.value()
            != Decimal::ZERO
    {
        return Err(OkxError::PositionMode);
    }
    let scope = OkxExecutionScope::new(config, instrument, profile, trade_mode)?;
    let contracts = instrument.base_to_contracts(command.quantity)?;
    let trigger = command.trigger_price.value().normalize().to_string();
    let (
        sl_trigger_px,
        sl_ord_px,
        sl_trigger_px_type,
        tp_trigger_px,
        tp_ord_px,
        tp_trigger_px_type,
    ) = match command.owner.purpose {
        OrderPurpose::Protection => (Some(trigger), Some("-1"), Some("mark"), None, None, None),
        OrderPurpose::TakeProfit => (None, None, None, Some(trigger), Some("-1"), Some("mark")),
        _ => return Err(OkxError::Payload),
    };
    let wire = AlgoPlaceWire {
        inst_id: instrument.native_id(),
        td_mode: trade_mode.wire_value(),
        algo_cl_ord_id: command.client_algo_id.as_str(),
        side: side_text(command.side),
        pos_side: position_side_text(command.position_side)?,
        ord_type: "conditional",
        sz: contracts.normalize().to_string(),
        sl_trigger_px,
        sl_ord_px,
        sl_trigger_px_type,
        tp_trigger_px,
        tp_ord_px,
        tp_trigger_px_type,
    };
    Ok(OkxAlgoPlaceRequest {
        scope,
        body: serde_json::to_vec(&wire).map_err(|_| OkxError::Payload)?,
        client_id: command.client_algo_id.as_str().to_owned(),
        side: command.side,
        position_side: command.position_side,
        quantity: command.quantity,
        contracts,
        trigger_price: command.trigger_price,
        purpose: command.owner.purpose,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlgoAckRow {
    algo_id: String,
    #[serde(default)]
    algo_cl_ord_id: String,
    s_code: String,
}

pub(crate) fn parse_algo_place_ack(
    response: OkxHttpResponse,
    request: &OkxAlgoPlaceRequest,
) -> Result<String, OkxError> {
    validate_response(&response, &request.scope)?;
    let row = one_algo_ack(&response.body)?;
    if row.algo_cl_ord_id != request.client_id {
        return Err(OkxError::Identity);
    }
    validate_order_id(&row.algo_id)?;
    Ok(row.algo_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxAlgoLookupRequest {
    scope: OkxExecutionScope,
    request_path: String,
    expected: OkxAlgoPlaceRequest,
    expected_algo_id: Option<String>,
}

impl OkxPrivateRequest for OkxAlgoLookupRequest {
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

pub(crate) fn build_algo_lookup_request(
    request: &OkxAlgoPlaceRequest,
    native_id: Option<&str>,
) -> Result<OkxAlgoLookupRequest, OkxError> {
    let request_path = match native_id {
        Some(id) => {
            validate_order_id(id)?;
            format!("{}?algoId={id}", endpoints::ALGO_ORDER)
        }
        None => format!(
            "{}?algoClOrdId={}",
            endpoints::ALGO_ORDER,
            request.client_id
        ),
    };
    Ok(OkxAlgoLookupRequest {
        scope: request.scope.clone(),
        request_path,
        expected: request.clone(),
        expected_algo_id: native_id.map(str::to_owned),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OkxAlgoState {
    Working,
    Triggered { child_order_ids: Vec<String> },
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxAlgoDetail {
    pub algo_id: String,
    pub state: OkxAlgoState,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlgoDetailRow {
    inst_type: String,
    inst_id: String,
    td_mode: String,
    ord_type: String,
    algo_id: String,
    algo_cl_ord_id: String,
    side: String,
    pos_side: String,
    sz: String,
    #[serde(default)]
    reduce_only: String,
    state: String,
    #[serde(default)]
    sl_trigger_px: String,
    #[serde(default)]
    sl_ord_px: String,
    #[serde(default)]
    sl_trigger_px_type: String,
    #[serde(default)]
    tp_trigger_px: String,
    #[serde(default)]
    tp_ord_px: String,
    #[serde(default)]
    tp_trigger_px_type: String,
    #[serde(default)]
    close_fraction: String,
    #[serde(default)]
    actual_side: String,
    #[serde(default)]
    ord_id_list: Vec<String>,
    c_time: String,
    u_time: String,
}

pub(crate) fn parse_algo_detail(
    response: OkxHttpResponse,
    request: &OkxAlgoLookupRequest,
) -> Result<Option<OkxAlgoDetail>, OkxError> {
    validate_response(&response, &request.scope)?;
    let envelope = decode_success::<AlgoDetailRow>(&response.body)?;
    let Some(row) = envelope.data.first() else {
        return Ok(None);
    };
    if envelope.data.len() != 1
        || row.inst_type != "SWAP"
        || row.inst_id != request.scope.native_instrument_id()
        || row.td_mode != request.scope.trade_mode().wire_value()
        || row.ord_type != "conditional"
        || row.algo_cl_ord_id != request.expected.client_id
        || row.side != side_text(request.expected.side)
        || row.pos_side != position_side_text(request.expected.position_side)?
        || positive_decimal(&row.sz)? != request.expected.contracts
        || request
            .expected_algo_id
            .as_deref()
            .is_some_and(|id| id != row.algo_id)
        || !matches!(row.reduce_only.as_str(), "" | "false")
        || !row.close_fraction.is_empty()
    {
        return Err(OkxError::Binding);
    }
    validate_order_id(&row.algo_id)?;
    validate_trigger(row, &request.expected)?;
    let created = positive_u64(&row.c_time)?;
    let updated = positive_u64(&row.u_time)?;
    if created > updated || updated > response.received_at_ms {
        return Err(OkxError::Sequence);
    }
    let state = match row.state.as_str() {
        "live" | "pause" => {
            if !row.actual_side.is_empty() || !row.ord_id_list.is_empty() {
                return Err(OkxError::Binding);
            }
            OkxAlgoState::Working
        }
        "effective" | "partially_effective" => {
            let actual_side = match request.expected.purpose {
                OrderPurpose::Protection => "sl",
                OrderPurpose::TakeProfit => "tp",
                _ => return Err(OkxError::Binding),
            };
            if row.actual_side != actual_side || row.ord_id_list.is_empty() {
                return Err(OkxError::Binding);
            }
            for id in &row.ord_id_list {
                validate_order_id(id)?;
            }
            OkxAlgoState::Triggered {
                child_order_ids: row.ord_id_list.clone(),
            }
        }
        "canceled" => OkxAlgoState::Cancelled,
        "order_failed" | "partially_failed" => OkxAlgoState::Failed,
        _ => return Err(OkxError::Payload),
    };
    Ok(Some(OkxAlgoDetail {
        algo_id: row.algo_id.clone(),
        state,
    }))
}

fn validate_trigger(row: &AlgoDetailRow, expected: &OkxAlgoPlaceRequest) -> Result<(), OkxError> {
    let expected_trigger = expected.trigger_price.value();
    let matches = match expected.purpose {
        OrderPurpose::Protection => {
            positive_decimal(&row.sl_trigger_px)? == expected_trigger
                && row.sl_ord_px == "-1"
                && row.sl_trigger_px_type == "mark"
                && row.tp_trigger_px.is_empty()
                && row.tp_ord_px.is_empty()
                && row.tp_trigger_px_type.is_empty()
        }
        OrderPurpose::TakeProfit => {
            positive_decimal(&row.tp_trigger_px)? == expected_trigger
                && row.tp_ord_px == "-1"
                && row.tp_trigger_px_type == "mark"
                && row.sl_trigger_px.is_empty()
                && row.sl_ord_px.is_empty()
                && row.sl_trigger_px_type.is_empty()
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(OkxError::Binding)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxAlgoCancelRequest {
    scope: OkxExecutionScope,
    body: Vec<u8>,
    algo_id: String,
}

impl OkxPrivateRequest for OkxAlgoCancelRequest {
    fn scope(&self) -> &OkxExecutionScope {
        &self.scope
    }
    fn method(&self) -> &'static str {
        POST
    }
    fn request_path(&self) -> &str {
        endpoints::CANCEL_ALGO_ORDERS
    }
    fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AlgoCancelWire<'a> {
    algo_id: &'a str,
    inst_id: &'a str,
}

pub(crate) fn build_algo_cancel_request(
    detail_request: &OkxAlgoLookupRequest,
    algo_id: &str,
) -> Result<OkxAlgoCancelRequest, OkxError> {
    validate_order_id(algo_id)?;
    let body = serde_json::to_vec(&[AlgoCancelWire {
        algo_id,
        inst_id: detail_request.scope.native_instrument_id(),
    }])
    .map_err(|_| OkxError::Payload)?;
    Ok(OkxAlgoCancelRequest {
        scope: detail_request.scope.clone(),
        body,
        algo_id: algo_id.to_owned(),
    })
}

pub(crate) fn parse_algo_cancel_ack(
    response: OkxHttpResponse,
    request: &OkxAlgoCancelRequest,
) -> Result<String, OkxError> {
    validate_response(&response, &request.scope)?;
    let row = one_algo_ack(&response.body)?;
    if row.algo_id != request.algo_id {
        return Err(OkxError::Identity);
    }
    Ok(row.algo_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxExactRegularCancelRequest {
    scope: OkxExecutionScope,
    body: Vec<u8>,
    order_id: String,
}

impl OkxPrivateRequest for OkxExactRegularCancelRequest {
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
struct ExactRegularCancelWire<'a> {
    inst_id: &'a str,
    ord_id: &'a str,
}

pub(crate) fn build_exact_regular_cancel_request(
    scope: &OkxExecutionScope,
    order_id: &str,
) -> Result<OkxExactRegularCancelRequest, OkxError> {
    validate_order_id(order_id)?;
    Ok(OkxExactRegularCancelRequest {
        scope: scope.clone(),
        body: serde_json::to_vec(&ExactRegularCancelWire {
            inst_id: scope.native_instrument_id(),
            ord_id: order_id,
        })
        .map_err(|_| OkxError::Payload)?,
        order_id: order_id.to_owned(),
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegularCancelAckRow {
    ord_id: String,
    s_code: String,
}

pub(crate) fn parse_exact_regular_cancel_ack(
    response: OkxHttpResponse,
    request: &OkxExactRegularCancelRequest,
) -> Result<String, OkxError> {
    validate_response(&response, &request.scope)?;
    let envelope = decode_success::<RegularCancelAckRow>(&response.body)?;
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    if row.s_code != "0" {
        return Err(OkxError::Rejected);
    }
    if row.ord_id != request.order_id {
        return Err(OkxError::Identity);
    }
    Ok(row.ord_id.clone())
}

fn one_algo_ack(payload: &[u8]) -> Result<AlgoAckRow, OkxError> {
    let envelope: Envelope<AlgoAckRow> =
        serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if envelope.code != "0" {
        return Err(OkxError::Rejected);
    }
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    if row.s_code != "0" {
        return Err(OkxError::Rejected);
    }
    Ok(AlgoAckRow {
        algo_id: row.algo_id.clone(),
        algo_cl_ord_id: row.algo_cl_ord_id.clone(),
        s_code: row.s_code.clone(),
    })
}

fn validate_response(
    response: &OkxHttpResponse,
    scope: &OkxExecutionScope,
) -> Result<(), OkxError> {
    if response.binding != *scope.gateway_binding()
        || response.instrument_generation != scope.instrument_generation()
        || response.received_at_ms == 0
        || response.body.is_empty()
    {
        Err(OkxError::Binding)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use venue_domain::domain::{CommandId, OrderOwner};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const PROFILE: &[u8] = include_bytes!("../fixtures/account-config.json");

    fn scope() -> Result<(OkxConfig, OkxInstrument, OkxAccountProfile), Box<dyn std::error::Error>>
    {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = crate::parse_instrument(INSTRUMENT, &config, 7)?;
        let profile = crate::parse_account_profile(PROFILE, OkxPositionMode::LongShort)?;
        Ok((config, instrument, profile))
    }

    fn command(
        purpose: OrderPurpose,
        client: &str,
        trigger: i64,
    ) -> Result<StopMarketFullPositionCommand, Box<dyn std::error::Error>> {
        Ok(StopMarketFullPositionCommand {
            command_id: CommandId::new(match purpose {
                OrderPurpose::Protection => "stopcommand1",
                OrderPurpose::TakeProfit => "stopcommand2",
                _ => "stopcommand3",
            })?,
            client_algo_id: CommandId::new(client)?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "okx".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 1),
            trigger_price: Price::new(Decimal::from(trigger))?,
            position_generation: 9,
        })
    }

    fn response(config: &OkxConfig, body: &'static [u8]) -> OkxHttpResponse {
        OkxHttpResponse {
            binding: config.gateway_binding().clone(),
            instrument_generation: 7,
            received_at_ms: 1_720_000_001_000,
            body: Bytes::from_static(body),
        }
    }

    #[test]
    fn algorithm_endpoints_are_distinct_from_regular_orders() {
        assert_eq!(endpoints::ALGO_ORDER, "/api/v5/trade/order-algo");
        assert_eq!(endpoints::CANCEL_ALGO_ORDERS, "/api/v5/trade/cancel-algos");
        assert_ne!(endpoints::ALGO_ORDER, endpoints::PLACE_ORDER);
    }

    #[test]
    fn sl_fixture_round_trips_create_detail_and_exact_cancel()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope()?;
        let command = command(
            OrderPurpose::Protection,
            "okxalgo000000000000000000000001",
            59_000,
        )?;
        let placed = build_algo_place_request(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            &command,
        )?;
        let body: serde_json::Value = serde_json::from_slice(placed.body())?;
        assert_eq!(body["ordType"], "conditional");
        assert_eq!(body["slTriggerPx"], "59000");
        assert_eq!(body["slOrdPx"], "-1");
        assert_eq!(body["slTriggerPxType"], "mark");
        assert!(body.get("tpTriggerPx").is_none());
        assert!(body.get("reduceOnly").is_none());
        let id = parse_algo_place_ack(
            response(&config, include_bytes!("../fixtures/algo-place-ack.json")),
            &placed,
        )?;
        let lookup = build_algo_lookup_request(&placed, Some(&id))?;
        let detail = parse_algo_detail(
            response(
                &config,
                include_bytes!("../fixtures/algo-detail-live-sl.json"),
            ),
            &lookup,
        )?
        .ok_or(OkxError::Payload)?;
        assert_eq!(detail.state, OkxAlgoState::Working);
        let cancel = build_algo_cancel_request(&lookup, &id)?;
        let cancel_body: serde_json::Value = serde_json::from_slice(cancel.body())?;
        assert_eq!(
            cancel_body,
            serde_json::json!([{"algoId":id,"instId":"BTC-USDT-SWAP"}])
        );
        assert_eq!(
            parse_algo_cancel_ack(
                response(&config, include_bytes!("../fixtures/algo-cancel-ack.json")),
                &cancel
            )?,
            id
        );
        Ok(())
    }

    #[test]
    fn triggered_tp_child_is_terminal_and_conflicting_semantics_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope()?;
        let command = command(
            OrderPurpose::TakeProfit,
            "okxalgo000000000000000000000002",
            61_000,
        )?;
        let placed = build_algo_place_request(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            &command,
        )?;
        let lookup = build_algo_lookup_request(&placed, Some("312269865356374017"))?;
        let fixture = include_bytes!("../fixtures/algo-detail-effective-tp.json");
        let detail =
            parse_algo_detail(response(&config, fixture), &lookup)?.ok_or(OkxError::Payload)?;
        assert_eq!(
            detail.state,
            OkxAlgoState::Triggered {
                child_order_ids: vec!["312269865356374018".to_owned()]
            }
        );

        let mut conflicting: serde_json::Value = serde_json::from_slice(fixture)?;
        conflicting["data"][0]["tpTriggerPx"] = serde_json::json!("61000.1");
        let bytes = serde_json::to_vec(&conflicting)?;
        let bad = OkxHttpResponse {
            body: Bytes::from(bytes),
            ..response(&config, fixture)
        };
        assert!(matches!(
            parse_algo_detail(bad, &lookup),
            Err(OkxError::Binding)
        ));
        Ok(())
    }
}
