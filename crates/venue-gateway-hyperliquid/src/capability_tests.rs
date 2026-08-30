use std::{collections::BTreeMap, future::Future, pin::Pin};

use rust_decimal::Decimal;
use venue_domain::domain::{FieldState, OrderSide};
use venue_gateway_api::{CapabilityFlags, GatewayBinding, GatewayMode, VenueId};

use crate::action::build_alo_place_request;
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
    Disconnect,
}

struct MockDispatch {
    reply: MockReply,
    calls: usize,
}

impl HyperliquidExchangeDispatch for MockDispatch {
    fn post_exchange<'a>(
        &'a mut self,
        _expected_binding: &'a HyperliquidReadBinding,
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
            MockReply::Disconnect => Err(HyperliquidTransportError::Http),
        };
        Box::pin(async move { result })
    }
}

fn meta() -> Result<HyperliquidPerpMeta, Box<dyn std::error::Error>> {
    let gateway = GatewayBinding::new(
        VenueId::Hyperliquid,
        GatewayMode::Live,
        ACCOUNT,
        "BTC/USDC".parse()?,
    )?;
    let read = HyperliquidReadBinding::new(HyperliquidGatewayBinding::new(gateway)?, USER)?;
    Ok(parse_perp_meta(META, &read)?)
}

fn credentials() -> Result<HyperliquidCredentials, HyperliquidError> {
    HyperliquidCredentials::from_values(USER, None, AGENT, AGENT_KEY)
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

#[test]
fn complete_fresh_probe_yields_only_a_non_withdrawal_candidate_and_tamper_fails()
-> Result<(), Box<dyn std::error::Error>> {
    let selected = meta()?;
    let credential = credentials()?;
    let private = HyperliquidPrivateStreamBinding::new(&selected, 21)?;
    let orders = parse_frontend_open_orders_snapshot(ORDERS, &selected, 1_700_000_000_010)?;
    let query =
        HyperliquidFillQuery::new(&selected, 1_700_000_000_001, 1_700_000_000_002, 10, None)?;
    let started_ms = 1_708_622_398_623;
    let private_stream =
        HyperliquidPrivateStreamProbeEvidence::from_connected(&private, started_ms + 20)?;
    let owners = owner_snapshot(&orders)?;
    let unknowns = HyperliquidUnknownSnapshot::new(vec![HyperliquidUnresolvedOrder::new(
        HyperliquidOrderFamily::Regular,
        HyperliquidOrderLookup::order_id(999)?,
        "ack_disconnect",
    )?])?;
    let roots = HyperliquidProbeAuthorityRoots::for_snapshots(&owners, &unknowns, [2; 32])?;
    let observed_ms = started_ms + 150;
    let scope = HyperliquidProbeCollectionScope::new(
        &selected,
        &credential,
        &private,
        "config_7",
        7,
        vec![selected.scope.symbol().clone(), "ETH/USDC".parse()?],
        41,
        31,
        20,
        roots,
        started_ms,
        started_ms + 180,
        started_ms + 30_000,
    )?;
    let expected_scope = scope.clone();
    let mut collector = HyperliquidFreshProbeCollector::start(
        scope,
        &selected,
        META,
        &private,
        owners,
        unknowns,
        query.begin_ms(),
        query.end_ms(),
    )?;
    collector.ingest_account(CLEARINGHOUSE, started_ms + 30)?;
    collector.ingest_orders(ORDERS, started_ms + 40)?;
    collector.ingest_fill_page(&query, FILLS, started_ms + 50)?;
    collector.ingest_unknown_order_status(
        &HyperliquidOrderLookup::order_id(999)?,
        br#"{"status":"unknownOid"}"#,
        started_ms + 60,
    )?;

    let evidence = collector.finish(&credential, private_stream, 7, observed_ms)?;
    evidence.verify(&credential)?;
    assert_eq!(evidence.recovery_faces().len(), 6);
    let order_face = evidence
        .recovery_faces()
        .iter()
        .find(|face| face.surface() == HyperliquidRecoverySurface::UmOrder)
        .ok_or("regular order face missing")?;
    assert!(matches!(
        order_face.coverage(),
        HyperliquidRecoveryCoverage::BlockedUnknown {
            visible_record_count: 1,
            unresolved_count: 1,
            ..
        }
    ));
    assert_eq!(evidence.unknown_orders().len(), 1);
    assert_eq!(evidence.unknown_orders()[0].native_identity(), "999");
    assert_eq!(
        evidence.unknown_orders()[0].unresolved_reason(),
        "ack_disconnect"
    );
    assert_eq!(
        evidence.unknown_orders()[0].reason(),
        HyperliquidOrderStatusUnknownReason::UnknownOid
    );
    let candidate = evidence.candidate_capability_snapshot(
        selected.scope.binding().gateway().gateway_binding(),
        &credential,
        observed_ms + 1,
    )?;
    assert!(candidate.flags.contains(CapabilityFlags::READ_ACCOUNT));
    assert!(!candidate.flags.contains(CapabilityFlags::TRADE));
    assert!(!candidate.flags.contains(CapabilityFlags::PLACE_LIMIT));
    assert!(!candidate.flags.contains(CapabilityFlags::CANCEL));
    assert!(!candidate.flags.contains(CapabilityFlags::PLACE_MARKET));
    assert!(!candidate.flags.contains(CapabilityFlags::WITHDRAW));
    assert_eq!(capabilities(), CapabilityFlags::empty());
    assert_eq!(
        evidence.candidate_capability_snapshot(
            selected.scope.binding().gateway().gateway_binding(),
            &credential,
            observed_ms + 30_000,
        ),
        Err(HyperliquidError::CapabilityProbe)
    );

    let persisted = serde_json::to_vec(&evidence)?;
    let restored = HyperliquidNodeCandidate::from_persisted_slice(
        &expected_scope,
        &credential,
        &persisted,
        observed_ms + 1,
    )?;
    assert_eq!(restored.candidate_capability_snapshot(), &candidate);
    assert_eq!(capabilities(), CapabilityFlags::empty());

    let mut tampered = serde_json::to_value(&evidence)?;
    tampered["payload"]["vault_address"] =
        serde_json::json!("0x0000000000000000000000000000000000000002");
    let tampered: HyperliquidCapabilityProbeEvidence = serde_json::from_value(tampered)?;
    assert_eq!(
        tampered.verify(&credential),
        Err(HyperliquidError::CapabilityProbe)
    );
    let tampered = serde_json::to_vec(&tampered)?;
    assert!(matches!(
        HyperliquidNodeCandidate::from_persisted_slice(
            &expected_scope,
            &credential,
            &tampered,
            observed_ms + 1,
        ),
        Err(HyperliquidError::CapabilityProbe)
    ));

    let mut wrong_scope = serde_json::to_value(&expected_scope)?;
    wrong_scope["config_epoch"] = serde_json::json!(8);
    let wrong_scope: HyperliquidProbeCollectionScope = serde_json::from_value(wrong_scope)?;
    assert!(matches!(
        HyperliquidNodeCandidate::from_persisted_slice(
            &wrong_scope,
            &credential,
            &persisted,
            observed_ms + 1,
        ),
        Err(HyperliquidError::CapabilityProbe)
    ));

    for (field, replacement) in [
        ("attempt_id", serde_json::json!(42)),
        ("private_generation", serde_json::json!(22)),
    ] {
        let mut relabeled = serde_json::to_value(&evidence)?;
        relabeled["payload"]["collection_scope"][field] = replacement;
        let encoded = serde_json::to_vec(&relabeled)?;
        assert!(matches!(
            HyperliquidNodeCandidate::from_persisted_slice(
                &expected_scope,
                &credential,
                &encoded,
                observed_ms + 1,
            ),
            Err(HyperliquidError::CapabilityProbe)
        ));
    }

    let mut root_relabel = serde_json::to_value(&evidence)?;
    root_relabel["payload"]["collection_scope"]["authority_roots"]["wal_keccak256"] =
        serde_json::json!("44".repeat(32));
    let root_relabel = serde_json::to_vec(&root_relabel)?;
    assert!(matches!(
        HyperliquidNodeCandidate::from_persisted_slice(
            &expected_scope,
            &credential,
            &root_relabel,
            observed_ms + 1,
        ),
        Err(HyperliquidError::CapabilityProbe)
    ));

    let mut raw_replacement = serde_json::to_value(&evidence)?;
    raw_replacement["payload"]["account_raw_payload"][0] = serde_json::json!(b'[');
    let raw_replacement = serde_json::to_vec(&raw_replacement)?;
    assert!(matches!(
        HyperliquidNodeCandidate::from_persisted_slice(
            &expected_scope,
            &credential,
            &raw_replacement,
            observed_ms + 1,
        ),
        Err(HyperliquidError::CapabilityProbe)
    ));
    Ok(())
}

fn owner_snapshot(
    orders: &HyperliquidOpenOrdersSnapshot,
) -> Result<HyperliquidOwnerSnapshot, Box<dyn std::error::Error>> {
    let routes = orders
        .orders
        .iter()
        .map(|order| {
            let client_order_id = match &order.order.client_order_id {
                FieldState::Known(value) => Some(value.clone()),
                FieldState::Missing => None,
                _ => return Err(HyperliquidError::CapabilityProbe),
            };
            HyperliquidOwnerRoute::new(
                order.family,
                order.order.symbol.clone(),
                order
                    .order
                    .order_id
                    .parse()
                    .map_err(|_| HyperliquidError::CapabilityProbe)?,
                client_order_id,
                "grid_btc",
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(HyperliquidOwnerSnapshot::new(routes)?)
}

#[test]
fn fresh_collector_rejects_owner_omission_deadline_and_cross_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let selected = meta()?;
    let credential = credentials()?;
    let private = HyperliquidPrivateStreamBinding::new(&selected, 21)?;
    let owners = HyperliquidOwnerSnapshot::new(Vec::new())?;
    let unknowns = HyperliquidUnknownSnapshot::new(Vec::new())?;
    let roots = HyperliquidProbeAuthorityRoots::for_snapshots(&owners, &unknowns, [2; 32])?;
    let started_ms = 1_708_622_398_623;
    let scope = HyperliquidProbeCollectionScope::new(
        &selected,
        &credential,
        &private,
        "config_7",
        7,
        vec![selected.scope.symbol().clone(), "ETH/USDC".parse()?],
        41,
        31,
        20,
        roots,
        started_ms,
        started_ms + 180,
        started_ms + 30_000,
    )?;
    let query =
        HyperliquidFillQuery::new(&selected, 1_700_000_000_001, 1_700_000_000_002, 10, None)?;

    let mut owner_collector = HyperliquidFreshProbeCollector::start(
        scope.clone(),
        &selected,
        META,
        &private,
        owners.clone(),
        unknowns.clone(),
        query.begin_ms(),
        query.end_ms(),
    )?;
    assert_eq!(
        owner_collector.ingest_orders(ORDERS, started_ms + 40),
        Err(HyperliquidError::CapabilityProbe)
    );

    let mut deadline_collector = HyperliquidFreshProbeCollector::start(
        scope.clone(),
        &selected,
        META,
        &private,
        owners.clone(),
        unknowns.clone(),
        query.begin_ms(),
        query.end_ms(),
    )?;
    assert_eq!(
        deadline_collector.ingest_account(CLEARINGHOUSE, started_ms + 181),
        Err(HyperliquidError::CapabilityProbe)
    );

    let newer_private = HyperliquidPrivateStreamBinding::new(&selected, 22)?;
    assert!(matches!(
        HyperliquidFreshProbeCollector::start(
            scope,
            &selected,
            META,
            &newer_private,
            owners,
            unknowns,
            query.begin_ms(),
            query.end_ms(),
        ),
        Err(HyperliquidError::CapabilityProbe)
    ));
    Ok(())
}
