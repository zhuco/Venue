use super::*;
use crate::{
    HyperliquidGatewayBinding, HyperliquidNonceStore, NonceCheckpoint, reserve_next_nonce,
};
use venue_gateway_api::{GatewayBinding, VenueId};

const USER: &str = "0x0000000000000000000000000000000000000001";
const AGENT: &str = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a";
const AGENT_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

#[derive(Default)]
struct MemoryNonceStore {
    checkpoint: Option<NonceCheckpoint>,
}

impl HyperliquidNonceStore for MemoryNonceStore {
    fn load(&mut self, _agent_address: &str) -> Result<Option<NonceCheckpoint>, HyperliquidError> {
        Ok(self.checkpoint.clone())
    }

    fn persist(&mut self, checkpoint: &NonceCheckpoint) -> Result<(), HyperliquidError> {
        self.checkpoint = Some(checkpoint.clone());
        Ok(())
    }
}

#[derive(Serialize)]
struct OfficialOrderAction {
    #[serde(rename = "type")]
    kind: &'static str,
    orders: Vec<OfficialOrder>,
    grouping: &'static str,
}

#[derive(Serialize)]
struct OfficialOrder {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    price: &'static str,
    #[serde(rename = "s")]
    size: &'static str,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: LimitOrderType,
}

#[derive(Serialize)]
struct OfficialDummyAction {
    #[serde(rename = "type")]
    kind: &'static str,
    num: u64,
}

#[test]
fn official_python_sdk_order_signature_vector_matches() -> Result<(), HyperliquidError> {
    let action = OfficialOrderAction {
        kind: "order",
        orders: vec![OfficialOrder {
            asset: 1,
            is_buy: true,
            price: "100",
            size: "100",
            reduce_only: false,
            order_type: LimitOrderType {
                limit: LimitTif { tif: "Gtc" },
            },
        }],
        grouping: "na",
    };
    let key = SigningKey::from_slice(&hex_key(
        "0123456789012345678901234567890123456789012345678901234567890123",
    )?)
    .map_err(|_| HyperliquidError::Signing)?;
    let connection_id = action_hash(&action, None, 0, None)?;
    let signature = sign_agent(&key, HyperliquidSource::Live, connection_id)?;
    assert_eq!(
        signature.r,
        "0xd65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e"
    );
    assert_eq!(
        signature.s,
        "0x2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e"
    );
    assert_eq!(signature.v, 28);
    Ok(())
}

#[test]
fn official_python_sdk_source_and_vault_signature_vectors_match() -> Result<(), HyperliquidError> {
    let action = OfficialDummyAction {
        kind: "dummy",
        num: 100_000_000_000,
    };
    let key = SigningKey::from_slice(&hex_key(
        "0123456789012345678901234567890123456789012345678901234567890123",
    )?)
    .map_err(|_| HyperliquidError::Signing)?;
    let no_vault = action_hash(&action, None, 0, None)?;
    let live = sign_agent(&key, HyperliquidSource::Live, no_vault)?;
    assert_eq!(
        live.r,
        // The official Python vector omits this scalar's leading zero nibble; the wire uses
        // the equivalent fixed-width bytes32 representation.
        "0x053749d5b30552aeb2fca34b530185976545bb22d0b3ce6f62e31be961a59298"
    );
    assert_eq!(
        live.s,
        "0x755c40ba9bf05223521753995abb2f73ab3229be8ec921f350cb447e384d8ed8"
    );
    assert_eq!(live.v, 27);
    let vault = "0x1719884eb866cb12b2287399b15f7db5e7d775ea";
    let vault_hash = action_hash(&action, Some(vault), 0, None)?;
    let live_vault = sign_agent(&key, HyperliquidSource::Live, vault_hash)?;
    assert_eq!(
        live_vault.r,
        "0x003c548db75e479f8012acf3000ca3a6b05606bc2ec0c29c50c515066a326239"
    );
    assert_eq!(
        live_vault.s,
        "0x4d402be7396ce74fbba3795769cda45aec00dc3125a984f2a9f23177b190da2c"
    );
    assert_eq!(live_vault.v, 28);
    Ok(())
}

#[test]
fn narrow_action_wires_are_bound_signed_and_strict() -> Result<(), Box<dyn std::error::Error>> {
    let meta = meta(GatewayMode::Live, USER)?;
    let credentials = HyperliquidCredentials::from_values(USER, None, AGENT, AGENT_KEY)?;
    let mut nonce_store = MemoryNonceStore::default();
    let nonce = reserve_next_nonce(&mut nonce_store, AGENT, 1_700_000_000_000)?;
    let alo = HyperliquidAloOrder::new(
        &meta,
        OrderSide::Buy,
        Decimal::new(6_500_500, 3),
        Decimal::new(4, 1),
        false,
        "0x00000000000000000000000000000001",
    )?;
    let request = build_alo_place_request(&credentials, nonce, alo, Some(1_700_000_001_000))?;
    assert_eq!(request.source(), HyperliquidSource::Live);
    assert_eq!(request.endpoint(), "/exchange");
    let body: serde_json::Value = serde_json::from_slice(request.body())?;
    assert_eq!(
        body["action"],
        serde_json::json!({
            "type":"order",
            "orders":[{
                "a":0,
                "b":true,
                "p":"6500.5",
                "s":"0.4",
                "r":false,
                "t":{"limit":{"tif":"Alo"}},
                "c":"0x00000000000000000000000000000001"
            }],
            "grouping":"na"
        })
    );
    assert!(body["vaultAddress"].is_null());
    assert_eq!(body["expiresAfter"], 1_700_000_001_000_u64);
    assert!(body["signature"]["v"].as_u64().is_some());
    assert!(matches!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77}}]}}}"#,
                &request,
            )?,
            HyperliquidExchangeOutcome::Resting { order_id: 77 }
        ));
    assert_eq!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.4","avgPx":"6500","oid":77}}]}}}"#,
                &request,
            ),
            Err(HyperliquidError::Response)
        );
    assert_eq!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":["waitingForFill"]}},"unexpected":true}"#,
                &request,
            ),
            Err(HyperliquidError::Response)
        );
    assert!(matches!(
        HyperliquidAloOrder::new(
            &meta,
            OrderSide::Buy,
            Decimal::new(65_000_500, 3),
            Decimal::new(4, 1),
            false,
            "0x00000000000000000000000000000003",
        ),
        Err(HyperliquidError::Action)
    ));
    Ok(())
}

#[test]
fn gtc_wire_and_readback_policy_must_match() -> Result<(), Box<dyn std::error::Error>> {
    let meta = meta(GatewayMode::Live, USER)?;
    let credentials = HyperliquidCredentials::from_values(USER, None, AGENT, AGENT_KEY)?;
    let mut store = MemoryNonceStore::default();
    let nonce = reserve_next_nonce(&mut store, AGENT, 1_700_000_000_000)?;
    let request = build_gtc_place_request(
        &credentials,
        nonce,
        HyperliquidGtcOrder::new(
            &meta,
            OrderSide::Buy,
            Decimal::new(6_500_500, 3),
            Decimal::new(4, 1),
            false,
            "0x00000000000000000000000000000001",
        )?,
        None,
    )?;
    let body: serde_json::Value = serde_json::from_slice(request.body())?;
    assert_eq!(body["action"]["orders"][0]["t"]["limit"]["tif"], "Gtc");
    let acknowledgement = parse_exchange_ack(
            br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77}}]}}}"#,
            &request,
        )?;
    let private = HyperliquidPrivateStreamBinding::new(&meta, 9)?;
    let plan = begin_exchange_readback(&request, Some(&acknowledgement), &private)?;
    let status_payload = |tif: Option<&str>| {
        let mut order = serde_json::json!({
            "children":[], "coin":"BTC", "isPositionTpsl":false,
            "isTrigger":false, "side":"B", "limitPx":"6500.5", "sz":"0.4",
            "oid":77, "timestamp":1_700_000_000_001_u64, "reduceOnly":false,
            "orderType":"Limit", "origSz":"0.4",
            "triggerCondition":"N/A", "triggerPx":"0.0",
            "cloid":"0x00000000000000000000000000000001"
        });
        if let Some(value) = tif {
            order["tif"] = serde_json::json!(value);
        }
        serde_json::to_vec(&serde_json::json!({
            "status":"order", "order":{"order":order, "status":"open",
            "statusTimestamp":1_700_000_000_002_u64}
        }))
    };
    let matching = crate::parse_order_status(&status_payload(Some("Gtc"))?, &meta, plan.lookup())?;
    assert!(matches!(
        plan.reconcile(Some(&matching))?,
        HyperliquidExchangeConvergence::Confirmed { order_id: 77, .. }
    ));
    let filled_ack = parse_exchange_ack(
            br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.4","avgPx":"6500.5","oid":77}}]}}}"#,
            &request,
        )?;
    let filled_plan = begin_exchange_readback(&request, Some(&filled_ack), &private)?;
    let mut filled_payload: serde_json::Value =
        serde_json::from_slice(&status_payload(Some("Gtc"))?)?;
    filled_payload["order"]["order"]["sz"] = serde_json::json!("0");
    filled_payload["order"]["status"] = serde_json::json!("filled");
    let filled_status = crate::parse_order_status(
        &serde_json::to_vec(&filled_payload)?,
        &meta,
        filled_plan.lookup(),
    )?;
    assert!(matches!(
        filled_plan.reconcile(Some(&filled_status))?,
        HyperliquidExchangeConvergence::Confirmed {
            order_id: 77,
            state: OrderState::Filled,
            ..
        }
    ));
    assert_eq!(
            parse_exchange_ack(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.5","avgPx":"6500.5","oid":77}}]}}}"#,
                &request,
            ),
            Err(HyperliquidError::Response)
        );
    let mismatched =
        crate::parse_order_status(&status_payload(Some("Alo"))?, &meta, plan.lookup())?;
    assert_eq!(
        plan.reconcile(Some(&mismatched)),
        Err(HyperliquidError::Readback)
    );
    assert!(crate::parse_order_status(&status_payload(None)?, &meta, plan.lookup()).is_err());
    Ok(())
}

#[test]
fn acknowledged_and_unknown_actions_converge_only_through_bound_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let meta = meta(GatewayMode::Live, USER)?;
    let credentials = HyperliquidCredentials::from_values(USER, None, AGENT, AGENT_KEY)?;
    let mut store = MemoryNonceStore::default();
    let nonce = reserve_next_nonce(&mut store, AGENT, 1_700_000_000_000)?;
    let request = build_alo_place_request(
        &credentials,
        nonce,
        HyperliquidAloOrder::new(
            &meta,
            OrderSide::Buy,
            Decimal::new(6_500_500, 3),
            Decimal::new(4, 1),
            false,
            "0x00000000000000000000000000000001",
        )?,
        None,
    )?;
    let acknowledgement = parse_exchange_ack(
            br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77}}]}}}"#,
            &request,
        )?;
    let private = HyperliquidPrivateStreamBinding::new(&meta, 9)?;
    let plan = begin_exchange_readback(&request, Some(&acknowledgement), &private)?;
    assert_eq!(plan.binding().generation(), 9);
    assert_eq!(plan.nonce(), 1_700_000_000_000);
    assert_eq!(plan.kind(), HyperliquidActionKind::AloPlace);
    assert_eq!(
        plan.reconcile(None)?,
        HyperliquidExchangeConvergence::PendingUnknown
    );
    let status_payload = serde_json::to_vec(&serde_json::json!({
        "status":"order",
        "order":{
            "order":{
                "children":[], "coin":"BTC", "isPositionTpsl":false,
                "isTrigger":false, "side":"B", "limitPx":"6500.5", "sz":"0.4",
                "oid":77, "timestamp":1_700_000_000_001_u64, "reduceOnly":false,
                "orderType":"Limit", "origSz":"0.4", "tif":"Alo",
                "triggerCondition":"N/A", "triggerPx":"0.0",
                "cloid":"0x00000000000000000000000000000001"
            },
            "status":"open", "statusTimestamp":1_700_000_000_002_u64
        }
    }))?;
    let status = crate::parse_order_status(&status_payload, &meta, plan.lookup())?;
    assert_eq!(
        plan.reconcile(Some(&status))?,
        HyperliquidExchangeConvergence::Confirmed {
            order_id: 77,
            state: OrderState::New,
            exchange_time_ms: 1_700_000_000_002,
        }
    );

    let unknown = begin_exchange_readback(&request, None, &private)?;
    assert!(matches!(
        unknown.lookup(),
        HyperliquidOrderLookup::ClientOrderId(_)
    ));
    let unknown_status =
        crate::parse_order_status(br#"{"status":"unknownOid"}"#, &meta, unknown.lookup())?;
    assert_eq!(
        unknown.reconcile(Some(&unknown_status))?,
        HyperliquidExchangeConvergence::PendingUnknown
    );
    assert_eq!(
        begin_exchange_readback(
            &request,
            Some(&acknowledgement),
            &HyperliquidPrivateStreamBinding::new(&meta, 10)?
        )?
        .binding()
        .generation(),
        10
    );

    let mut wrong = serde_json::from_slice::<serde_json::Value>(&status_payload)?;
    wrong["order"]["order"]["reduceOnly"] = serde_json::json!(true);
    let wrong_status =
        crate::parse_order_status(&serde_json::to_vec(&wrong)?, &meta, plan.lookup())?;
    assert_eq!(
        plan.reconcile(Some(&wrong_status)),
        Err(HyperliquidError::Readback)
    );
    Ok(())
}

#[test]
fn vault_ioc_and_cancel_keep_exact_scope_and_response_shape()
-> Result<(), Box<dyn std::error::Error>> {
    const VAULT: &str = "0x0000000000000000000000000000000000000002";
    let meta = meta(GatewayMode::Live, VAULT)?;
    let credentials =
        HyperliquidCredentials::from_values(USER, Some(VAULT.to_owned()), AGENT, AGENT_KEY)?;
    let mut store = MemoryNonceStore::default();
    let ioc_nonce = reserve_next_nonce(&mut store, AGENT, 1_700_000_000_000)?;
    let ioc = HyperliquidIocReduceOnlyOrder::new(
        &meta,
        OrderSide::Sell,
        Decimal::new(64_000, 0),
        Decimal::new(3, 1),
        "0x00000000000000000000000000000002",
    )?;
    let ioc_request = build_ioc_reduce_only_request(&credentials, ioc_nonce, ioc, None)?;
    assert_eq!(ioc_request.source(), HyperliquidSource::Live);
    let body: serde_json::Value = serde_json::from_slice(ioc_request.body())?;
    assert_eq!(body["vaultAddress"], VAULT);
    assert_eq!(body["action"]["orders"][0]["r"], true);
    assert_eq!(body["action"]["orders"][0]["t"]["limit"]["tif"], "Ioc");
    let ioc_ack = parse_exchange_ack(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.2","avgPx":"63999.5","oid":88}}]}}}"#,
                &ioc_request,
            )?;
    assert!(matches!(
        &ioc_ack,
        HyperliquidExchangeOutcome::Filled { order_id: 88, .. }
    ));
    assert!(matches!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"IocCancel"}]}}}"#,
                &ioc_request,
            )?,
            HyperliquidExchangeOutcome::Rejected { .. }
        ));

    let cancel_nonce = reserve_next_nonce(&mut store, AGENT, 1_700_000_000_001)?;
    let cancel_request = build_cancel_request(
        &credentials,
        cancel_nonce,
        HyperliquidCancel::new(&meta, 88)?,
        None,
    )?;
    let cancel_ack = parse_exchange_ack(
        br#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}"#,
        &cancel_request,
    )?;
    assert!(matches!(
        &cancel_ack,
        HyperliquidExchangeOutcome::Cancelled { order_id: 88 }
    ));

    let private = HyperliquidPrivateStreamBinding::new(&meta, 22)?;
    let ioc_plan = begin_exchange_readback(&ioc_request, Some(&ioc_ack), &private)?;
    let ioc_status_payload = serde_json::to_vec(&serde_json::json!({
        "status":"order",
        "order":{
            "order":{
                "children":[], "coin":"BTC", "isPositionTpsl":false,
                "isTrigger":false, "side":"A", "limitPx":"64000", "sz":"0",
                "oid":88, "timestamp":1_700_000_000_001_u64, "reduceOnly":true,
                "orderType":"Market", "origSz":"0.3", "tif":"FrontendMarket",
                "triggerCondition":"N/A", "triggerPx":"0.0",
                "cloid":"0x00000000000000000000000000000002"
            },
            "status":"filled", "statusTimestamp":1_700_000_000_002_u64
        }
    }))?;
    let ioc_status = crate::parse_order_status(&ioc_status_payload, &meta, ioc_plan.lookup())?;
    assert!(matches!(
        ioc_plan.reconcile(Some(&ioc_status))?,
        HyperliquidExchangeConvergence::Confirmed {
            order_id: 88,
            state: OrderState::Filled,
            ..
        }
    ));

    let cancel_plan = begin_exchange_readback(&cancel_request, Some(&cancel_ack), &private)?;
    let cancel_status =
        crate::parse_order_status(&ioc_status_payload, &meta, cancel_plan.lookup())?;
    assert!(matches!(
        cancel_plan.reconcile(Some(&cancel_status))?,
        HyperliquidExchangeConvergence::Confirmed {
            order_id: 88,
            state: OrderState::Filled,
            ..
        }
    ));

    let unknown_cancel = begin_exchange_readback(&cancel_request, None, &private)?;
    let mut still_open = serde_json::from_slice::<serde_json::Value>(&ioc_status_payload)?;
    still_open["order"]["order"]["sz"] = serde_json::json!("0.3");
    still_open["order"]["order"]["orderType"] = serde_json::json!("Limit");
    still_open["order"]["order"]["tif"] = serde_json::json!("Alo");
    still_open["order"]["status"] = serde_json::json!("open");
    let still_open = crate::parse_order_status(
        &serde_json::to_vec(&still_open)?,
        &meta,
        unknown_cancel.lookup(),
    )?;
    assert_eq!(
        unknown_cancel.reconcile(Some(&still_open))?,
        HyperliquidExchangeConvergence::PendingUnknown
    );
    Ok(())
}

fn meta(mode: GatewayMode, user: &str) -> Result<HyperliquidPerpMeta, Box<dyn std::error::Error>> {
    let gateway = HyperliquidGatewayBinding::new(GatewayBinding::new(
        VenueId::Hyperliquid,
        mode,
        "00000000-0000-4000-8000-000000000001",
        "BTC/USDC".parse()?,
    )?)?;
    let read = HyperliquidReadBinding::new(gateway, user)?;
    Ok(crate::parse_perp_meta(
        br#"{"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":50}]}"#,
        &read,
    )?)
}

fn hex_key(value: &str) -> Result<[u8; 32], HyperliquidError> {
    if value.len() != 64 {
        return Err(HyperliquidError::Signing);
    }
    let mut output = [0_u8; 32];
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(HyperliquidError::Signing);
    }
    for (index, pair) in pairs.iter().enumerate() {
        let high = hex_nibble(pair[0]).ok_or(HyperliquidError::Signing)?;
        let low = hex_nibble(pair[1]).ok_or(HyperliquidError::Signing)?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
