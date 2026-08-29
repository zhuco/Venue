use std::{collections::BTreeMap, future::Future, pin::Pin};

use bytes::Bytes;
use rust_decimal::Decimal;
use venue_domain::domain::{FieldState, OrderSide};
use venue_gateway_api::{
    CapabilityFlags, GatewayApiError, GatewayBinding, GatewayMode, MutationCapability, VenueId,
};

use crate::physical::HyperliquidExchangeDispatch;
use crate::*;

const META: &[u8] = include_bytes!("../fixtures/perp-meta.json");
const CLEARINGHOUSE: &[u8] = include_bytes!("../fixtures/clearinghouse-state.json");
const ORDERS: &[u8] = include_bytes!("../fixtures/frontend-open-orders-family.json");
const FILLS: &[u8] = include_bytes!("../fixtures/fills-page.json");
const PRIVATE_EVENTS: &[u8] = include_bytes!("../fixtures/private-account-events.json");
const USER: &str = "0x0000000000000000000000000000000000000001";
const AGENT: &str = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a";
const AGENT_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

#[derive(Default)]
struct MemoryNonceStore {
    states: BTreeMap<String, NonceCheckpoint>,
}

impl HyperliquidNonceStore for MemoryNonceStore {
    fn load(&mut self, agent_address: &str) -> Result<Option<NonceCheckpoint>, HyperliquidError> {
        Ok(self
            .states
            .get(&agent_address.to_ascii_lowercase())
            .cloned())
    }

    fn persist(&mut self, checkpoint: &NonceCheckpoint) -> Result<(), HyperliquidError> {
        self.states.insert(
            checkpoint.agent_address.to_ascii_lowercase(),
            checkpoint.clone(),
        );
        Ok(())
    }
}

enum MockReply {
    Body(Bytes),
    Disconnect,
}

struct MockDispatch {
    reply: MockReply,
    calls: usize,
}

impl HyperliquidExchangeDispatch for MockDispatch {
    fn post_exchange<'a>(
        &'a mut self,
        expected_binding: &'a HyperliquidReadBinding,
        _request: &'a HyperliquidExchangeRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HyperliquidHttpResponse, HyperliquidTransportError>>
                + Send
                + 'a,
        >,
    > {
        self.calls += 1;
        let result = match &self.reply {
            MockReply::Body(body) => Ok(HyperliquidHttpResponse {
                binding: expected_binding.clone(),
                body: body.clone(),
                received_at_ms: 1_700_000_000_100,
            }),
            MockReply::Disconnect => Err(HyperliquidTransportError::Http),
        };
        Box::pin(async move { result })
    }
}

fn meta() -> Result<HyperliquidPerpMeta, Box<dyn std::error::Error>> {
    let gateway = GatewayBinding::new(
        VenueId::Hyperliquid,
        GatewayMode::Test,
        ACCOUNT,
        "BTC/USDC".parse()?,
    )?;
    let read = HyperliquidReadBinding::new(HyperliquidGatewayBinding::new(gateway)?, USER)?;
    Ok(parse_perp_meta(META, &read)?)
}

fn credentials() -> Result<HyperliquidCredentials, HyperliquidError> {
    HyperliquidCredentials::from_values(USER, USER, None, "venue-agent", AGENT, AGENT_KEY)
}

#[test]
fn nonce_crash_after_durable_reservation_never_reuses_the_signed_nonce()
-> Result<(), Box<dyn std::error::Error>> {
    let mut before_crash = MemoryNonceStore::default();
    let reserved = reserve_next_nonce(&mut before_crash, AGENT, 1_700_000_000_000)?;
    let consumed = reserved.value();
    let persisted = before_crash.load(AGENT)?.ok_or("checkpoint missing")?;
    drop(reserved);

    let mut after_restart = MemoryNonceStore::default();
    after_restart.persist(&persisted)?;
    let next = reserve_next_nonce(&mut after_restart, AGENT, 1_699_999_000_000)?;
    assert_eq!(next.value(), consumed + 1);
    Ok(())
}

#[tokio::test]
async fn ack_disconnect_becomes_read_only_unknown_and_never_resubmits()
-> Result<(), Box<dyn std::error::Error>> {
    let selected = meta()?;
    let private = HyperliquidPrivateStreamBinding::new(&selected, 9)?;
    let mut store = MemoryNonceStore::default();
    let request = build_alo_place_request(
        &credentials()?,
        reserve_next_nonce(&mut store, AGENT, 1_700_000_000_000)?,
        HyperliquidAloOrder::new(
            &selected,
            OrderSide::Buy,
            Decimal::new(6_500_500, 3),
            Decimal::new(4, 1),
            false,
            "0x00000000000000000000000000000001",
        )?,
        None,
    )?;
    let mut transport = MockDispatch {
        reply: MockReply::Disconnect,
        calls: 0,
    };
    let HyperliquidPhysicalDispatchResult::PendingReadback(pending) =
        HyperliquidPhysicalDispatch::new(request, &private)?
            .dispatch_once_for_test(&mut transport)
            .await
    else {
        return Err("disconnect was not fenced UNKNOWN".into());
    };
    assert_eq!(transport.calls, 1);
    let unknown = parse_order_status(
        br#"{"status":"unknownOid"}"#,
        &selected,
        pending.plan().lookup(),
    )?;
    let HyperliquidPhysicalReadbackResult::PendingUnknown(read_only) =
        pending.reconcile(Some(&unknown))?
    else {
        return Err("unknownOid became terminal".into());
    };
    assert_eq!(transport.calls, 1);
    assert_eq!(read_only.plan().binding().generation(), 9);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            read_only.order_status_request(&selected)?.body()
        )?["type"],
        "orderStatus"
    );
    Ok(())
}

#[test]
fn fill_window_deduplicates_exact_private_overlap_and_rejects_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let selected = meta()?;
    let private = HyperliquidPrivateStreamBinding::new(&selected, 11)?;
    let parsed = parse_private_user_fills(PRIVATE_EVENTS, &selected)?;
    let fill = parsed.fills.first().ok_or("fixture fill missing")?.clone();
    let time_ms = fill.fill.exchange_time_ms.ok_or("fill time missing")?;
    let query = HyperliquidFillQuery::new(&selected, time_ms, time_ms, 10, None)?;
    let page = HyperliquidFillPage {
        scope: selected.scope.clone(),
        fills: vec![fill.clone()],
        next_cursor: None,
        complete: true,
        coverage: HyperliquidFillCoverage::VenueVisibleWindowExhausted {
            maximum_retained_fills: HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT,
        },
    };
    let update = HyperliquidFillUpdate {
        binding: private.clone(),
        stream: HyperliquidFillStream::UserFills,
        snapshot: FieldState::Known(false),
        fill: fill.clone(),
    };
    let mut exact = HyperliquidFillWindowProbe::new(&private, time_ms, time_ms)?;
    exact.ingest_page(&query, &page)?;
    exact.ingest_private(&update)?;
    let evidence = exact.finish()?;
    assert_eq!(evidence.fill_count(), 1);

    let mut conflicting_fill = fill;
    conflicting_fill.fill.quantity += Decimal::ONE;
    let mut conflict = HyperliquidFillWindowProbe::new(&private, time_ms, time_ms)?;
    conflict.ingest_page(&query, &page)?;
    assert_eq!(
        conflict.ingest_private(&HyperliquidFillUpdate {
            binding: private,
            stream: HyperliquidFillStream::UserEvents,
            snapshot: FieldState::NotApplicable,
            fill: conflicting_fill,
        }),
        Err(HyperliquidError::CapabilityProbe)
    );
    Ok(())
}

#[tokio::test]
async fn complete_fresh_probe_yields_only_a_non_withdrawal_candidate_and_tamper_fails()
-> Result<(), Box<dyn std::error::Error>> {
    let selected = meta()?;
    let credential = credentials()?;
    let private = HyperliquidPrivateStreamBinding::new(&selected, 21)?;
    let account = parse_clearinghouse_snapshot(CLEARINGHOUSE, &selected)?;
    let orders = parse_frontend_open_orders_snapshot(ORDERS, &selected, 1_700_000_000_010)?;
    let query =
        HyperliquidFillQuery::new(&selected, 1_700_000_000_001, 1_700_000_000_002, 10, None)?;
    let page = parse_user_fills_page(FILLS, &selected, &query)?;
    let mut fill_probe =
        HyperliquidFillWindowProbe::new(&private, query.begin_ms(), query.end_ms())?;
    fill_probe.ingest_page(&query, &page)?;
    let fill_window = fill_probe.finish()?;
    let private_stream =
        HyperliquidPrivateStreamProbeEvidence::from_connected(&private, 1_700_000_000_020)?;

    let mut nonces = MemoryNonceStore::default();
    let alo = action_receipt(
        &selected,
        &private,
        &credential,
        reserve_next_nonce(&mut nonces, AGENT, 1_700_000_000_100)?,
        ActionFixture::Alo,
    )
    .await
    .map_err(|error| format!("ALO probe failed: {error}"))?;
    let cancel = action_receipt(
        &selected,
        &private,
        &credential,
        reserve_next_nonce(&mut nonces, AGENT, 1_700_000_000_101)?,
        ActionFixture::Cancel,
    )
    .await
    .map_err(|error| format!("cancel probe failed: {error}"))?;
    let ioc = action_receipt(
        &selected,
        &private,
        &credential,
        reserve_next_nonce(&mut nonces, AGENT, 1_700_000_000_102)?,
        ActionFixture::Ioc,
    )
    .await
    .map_err(|error| format!("IOC probe failed: {error}"))?;
    let observed_ms = 1_800_000_000_000;
    let evidence = HyperliquidCapabilityProbeEvidence::issue(
        &selected,
        &credential,
        &account,
        &orders,
        fill_window,
        private_stream,
        31,
        7,
        observed_ms,
        observed_ms + 30_000,
        [ioc, alo, cancel],
    )?;
    evidence.verify()?;
    let candidate = evidence.candidate_capability_snapshot(
        selected.scope.binding().gateway().gateway_binding(),
        observed_ms + 1,
    )?;
    assert!(candidate.flags.contains(CapabilityFlags::PLACE_LIMIT));
    assert!(candidate.flags.contains(CapabilityFlags::CANCEL));
    assert!(!candidate.flags.contains(CapabilityFlags::PLACE_MARKET));
    assert!(!candidate.flags.contains(CapabilityFlags::WITHDRAW));
    candidate.authorize(
        &candidate.binding,
        candidate.version,
        observed_ms + 1,
        MutationCapability::PlaceLimit,
    )?;
    assert_eq!(
        candidate.authorize(
            &candidate.binding,
            candidate.version,
            observed_ms + 1,
            MutationCapability::PlaceMarket,
        ),
        Err(GatewayApiError::CapabilityDenied)
    );
    assert_eq!(capabilities(), CapabilityFlags::empty());
    assert_eq!(
        evidence.candidate_capability_snapshot(
            selected.scope.binding().gateway().gateway_binding(),
            observed_ms + 30_000,
        ),
        Err(HyperliquidError::CapabilityProbe)
    );

    let persisted = serde_json::to_vec(&evidence)?;
    let restored = HyperliquidNodeCandidate::from_persisted_slice(
        selected.scope.binding().gateway().gateway_binding(),
        &persisted,
        observed_ms + 1,
    )?;
    assert_eq!(restored.candidate_capability_snapshot(), &candidate);
    assert_eq!(capabilities(), CapabilityFlags::empty());

    let mut action_nonces = MemoryNonceStore::default();
    drop(restored.prepare_alo(
        &mut action_nonces,
        &selected,
        &private,
        &credential,
        1_800_000_000_100,
        HyperliquidAloOrder::new(
            &selected,
            OrderSide::Buy,
            Decimal::new(6_500_500, 3),
            Decimal::new(4, 1),
            false,
            "0x00000000000000000000000000000011",
        )?,
        None,
    )?);
    drop(restored.prepare_cancel(
        &mut action_nonces,
        &selected,
        &private,
        &credential,
        1_800_000_000_101,
        HyperliquidCancel::new(&selected, 77)?,
        None,
    )?);
    drop(restored.prepare_ioc_reduce_only(
        &mut action_nonces,
        &selected,
        &private,
        &credential,
        1_800_000_000_102,
        HyperliquidIocReduceOnlyOrder::new(
            &selected,
            OrderSide::Sell,
            Decimal::new(6_400_000, 3),
            Decimal::new(2, 1),
            "0x00000000000000000000000000000012",
        )?,
        None,
    )?);
    assert_eq!(
        action_nonces
            .load(AGENT)?
            .ok_or("candidate nonce checkpoint missing")?
            .last_nonce_ms,
        1_800_000_000_102
    );

    let newer_private = HyperliquidPrivateStreamBinding::new(&selected, 22)?;
    assert!(matches!(
        restored.prepare_cancel(
            &mut action_nonces,
            &selected,
            &newer_private,
            &credential,
            1_800_000_000_103,
            HyperliquidCancel::new(&selected, 77)?,
            None,
        ),
        Err(HyperliquidError::CapabilityProbe)
    ));

    let mut tampered = serde_json::to_value(&evidence)?;
    tampered["payload"]["vault_address"] =
        serde_json::json!("0x0000000000000000000000000000000000000002");
    let tampered: HyperliquidCapabilityProbeEvidence = serde_json::from_value(tampered)?;
    assert_eq!(tampered.verify(), Err(HyperliquidError::CapabilityProbe));
    let tampered = serde_json::to_vec(&tampered)?;
    assert!(matches!(
        HyperliquidNodeCandidate::from_persisted_slice(
            selected.scope.binding().gateway().gateway_binding(),
            &tampered,
            observed_ms + 1,
        ),
        Err(HyperliquidError::CapabilityProbe)
    ));

    let live_binding = GatewayBinding::new(
        VenueId::Hyperliquid,
        GatewayMode::Live,
        ACCOUNT,
        "BTC/USDC".parse()?,
    )?;
    assert!(matches!(
        HyperliquidNodeCandidate::from_persisted_slice(&live_binding, &persisted, observed_ms + 1,),
        Err(HyperliquidError::CapabilityProbe)
    ));
    Ok(())
}

#[derive(Clone, Copy)]
enum ActionFixture {
    Alo,
    Cancel,
    Ioc,
}

async fn action_receipt(
    meta: &HyperliquidPerpMeta,
    private: &HyperliquidPrivateStreamBinding,
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    fixture: ActionFixture,
) -> Result<HyperliquidProbeActionReceipt, Box<dyn std::error::Error>> {
    let (request, acknowledgement, status) = match fixture {
        ActionFixture::Alo => (
            build_alo_place_request(
                credentials,
                nonce,
                HyperliquidAloOrder::new(
                    meta,
                    OrderSide::Buy,
                    Decimal::new(6_500_500, 3),
                    Decimal::new(4, 1),
                    false,
                    "0x00000000000000000000000000000001",
                )?,
                None,
            )?,
            Bytes::from_static(br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77}}]}}}"#),
            status_payload(77, "B", "6500.5", "0.4", false, "Alo", "open", "0x00000000000000000000000000000001")?,
        ),
        ActionFixture::Cancel => (
            build_cancel_request(credentials, nonce, HyperliquidCancel::new(meta, 77)?, None)?,
            Bytes::from_static(br#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}"#),
            status_payload(77, "B", "6500.5", "0.4", false, "Alo", "canceled", "0x00000000000000000000000000000001")?,
        ),
        ActionFixture::Ioc => (
            build_ioc_reduce_only_request(
                credentials,
                nonce,
                HyperliquidIocReduceOnlyOrder::new(
                    meta,
                    OrderSide::Sell,
                    Decimal::new(6_400_000, 3),
                    Decimal::new(2, 1),
                    "0x00000000000000000000000000000002",
                )?,
                None,
            )?,
            Bytes::from_static(br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.2","avgPx":"6400","oid":88}}]}}}"#),
            status_payload(88, "A", "6400", "0", true, "Ioc", "filled", "0x00000000000000000000000000000002")?,
        ),
    };
    let mut dispatch = MockDispatch {
        reply: MockReply::Body(acknowledgement),
        calls: 0,
    };
    let HyperliquidPhysicalDispatchResult::PendingReadback(pending) =
        HyperliquidPhysicalDispatch::new(request, private)?
            .dispatch_once_for_test(&mut dispatch)
            .await
    else {
        return Err("probe action rejected".into());
    };
    assert_eq!(dispatch.calls, 1);
    let status = parse_order_status(&status, meta, pending.plan().lookup())?;
    let HyperliquidPhysicalReadbackResult::Confirmed(receipt) = pending.reconcile(Some(&status))?
    else {
        return Err("probe action did not converge".into());
    };
    Ok(receipt)
}

#[allow(clippy::too_many_arguments)]
fn status_payload(
    order_id: u64,
    side: &str,
    price: &str,
    remaining_size: &str,
    reduce_only: bool,
    tif: &str,
    status: &str,
    client_order_id: &str,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&serde_json::json!({
        "status":"order",
        "order":{
            "order":{
                "children":[], "coin":"BTC", "isPositionTpsl":false,
                "isTrigger":false, "side":side, "limitPx":price, "sz":remaining_size,
                "oid":order_id, "timestamp":1_700_000_000_110_u64,
                "reduceOnly":reduce_only,
                "orderType":if tif == "Ioc" {"Market"} else {"Limit"},
                "origSz":if tif == "Ioc" {"0.2"} else {"0.4"},
                "tif":if tif == "Ioc" {"FrontendMarket"} else {tif},
                "triggerCondition":"N/A", "triggerPx":"0.0",
                "cloid":client_order_id
            },
            "status":status, "statusTimestamp":1_700_000_000_120_u64
        }
    }))
}
