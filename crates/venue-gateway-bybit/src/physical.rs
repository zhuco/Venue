use rust_decimal::Decimal;
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{OrderSide, PositionSide};
use venue_gateway_api::{CapabilitySnapshot, GatewayBinding, MutationCapability};

use crate::{
    BybitAccountIdentity, BybitCancelIntent, BybitCapabilityProbeEvidence,
    BybitClosedOrderReadback, BybitCredentials, BybitExecutionError, BybitGatewayBinding,
    BybitHistoryWindow, BybitHttpTransport, BybitLinearInstrumentRules, BybitOrderAck,
    BybitOrderKind, BybitOrderLookup, BybitOrderSettlement, BybitPlaceIntent,
    BybitPositionReadback, BybitPreparedPrivateRequest, BybitPreparedRequest, BybitPrivateSource,
    BybitRawPublicPayload, BybitRequestKind, BybitRestBbo, BybitTimeInForce, BybitTransportError,
    BybitTransportLimits, parse_linear_instrument, prepare_cancel_request, prepare_place_request,
    prepare_private_request, settle_order_ack,
};

/// A verified adapter session with real Bybit HTTP transport. It owns no writer, WAL, Owner map,
/// retry loop, or live admission; those authorities remain the responsibility of a future host.
pub struct BybitPhysicalSession {
    binding: BybitGatewayBinding,
    credentials: BybitCredentials,
    transport: BybitHttpTransport,
    identity: BybitAccountIdentity,
    positions: BybitPositionReadback,
    rules: BybitLinearInstrumentRules,
    capability: CapabilitySnapshot,
    probe_sha256: String,
}

impl BybitPhysicalSession {
    /// Replays a durable HMAC-bound probe and its exact public instrument payload before creating
    /// a physical session. This remains an adapter candidate: callers must supply independent
    /// Owner/WAL/writer/Control/Canary authority before loading credentials or invoking it.
    pub fn from_persisted_probe(
        binding: BybitGatewayBinding,
        credentials: BybitCredentials,
        probe_payload: &[u8],
        instrument_payload: BybitRawPublicPayload,
        limits: BybitTransportLimits,
        now_ms: u64,
    ) -> Result<Self, BybitPhysicalError> {
        let (probe, capability) = BybitCapabilityProbeEvidence::from_json_verified(
            probe_payload,
            &binding,
            &credentials,
            now_ms,
        )
        .map_err(|_| BybitPhysicalError::Capability)?;
        let rules = parse_linear_instrument(&binding, instrument_payload)
            .map_err(|_| BybitPhysicalError::Intent)?;
        if rules.raw.generation != capability.version
            || rules.instrument.generation != capability.version
        {
            return Err(BybitPhysicalError::Scope);
        }
        Self::from_probe(binding, credentials, rules, &probe, limits, now_ms)
    }

    pub fn from_probe(
        binding: BybitGatewayBinding,
        credentials: BybitCredentials,
        rules: BybitLinearInstrumentRules,
        probe: &BybitCapabilityProbeEvidence,
        limits: BybitTransportLimits,
        now_ms: u64,
    ) -> Result<Self, BybitPhysicalError> {
        let (capability, candidate) = probe
            .verify_candidate(&binding, &credentials, now_ms)
            .map_err(|_| BybitPhysicalError::Capability)?;
        let identity = candidate.account.identity.clone();
        if identity.binding != *binding.gateway_binding()
            || rules.raw.binding != *binding.gateway_binding()
            || identity.generation != capability.version
            || rules.instrument.generation != capability.version
            || rules.raw.generation != capability.version
        {
            return Err(BybitPhysicalError::Scope);
        }
        let transport = BybitHttpTransport::new(&binding, capability.version, limits)
            .map_err(|_| BybitPhysicalError::Transport)?;
        Ok(Self {
            binding,
            credentials,
            transport,
            identity,
            positions: candidate.positions,
            rules,
            capability,
            probe_sha256: probe.evidence_sha256().to_owned(),
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    #[must_use]
    pub fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.capability.clone()
    }

    pub fn prepare_place_once(
        &self,
        intent: &BybitPlaceIntent,
        now_ms: u64,
        market_bbo: Option<&BybitRestBbo>,
    ) -> Result<BybitOneShotMutation, BybitPhysicalError> {
        let mutation = match intent.kind {
            BybitOrderKind::Limit => MutationCapability::PlaceLimit,
            BybitOrderKind::Market => MutationCapability::PlaceMarket,
        };
        self.authorize(now_ms, mutation)?;
        let request = prepare_place_request(
            &self.binding,
            &self.identity,
            &self.rules,
            intent,
            now_ms,
            market_bbo,
        )?;
        self.one_shot(request, mutation, now_ms)
    }

    pub fn prepare_cancel_once(
        &self,
        intent: &BybitCancelIntent,
        now_ms: u64,
    ) -> Result<BybitOneShotMutation, BybitPhysicalError> {
        self.authorize(now_ms, MutationCapability::Cancel)?;
        let request = prepare_cancel_request(&self.binding, &self.identity, &self.rules, intent)?;
        self.one_shot(request, MutationCapability::Cancel, now_ms)
    }

    /// Builds exactly one reduce-only IOC market request for a direction-specific Hedge leg.
    pub fn prepare_reduce_once(
        &self,
        client_order_id: impl Into<String>,
        position_side: PositionSide,
        quantity: Decimal,
        now_ms: u64,
        market_bbo: &BybitRestBbo,
    ) -> Result<BybitOneShotMutation, BybitPhysicalError> {
        let side = match position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
            PositionSide::Net => return Err(BybitPhysicalError::Intent),
        };
        let signed_quantity = self
            .positions
            .positions
            .iter()
            .find(|position| position.position.side == position_side)
            .map(|position| position.position.quantity)
            .ok_or(BybitPhysicalError::Scope)?;
        if quantity <= Decimal::ZERO || quantity > signed_quantity {
            return Err(BybitPhysicalError::Intent);
        }
        let intent = BybitPlaceIntent {
            client_order_id: client_order_id.into(),
            side,
            position_side,
            kind: BybitOrderKind::Market,
            quantity,
            limit_price: None,
            time_in_force: BybitTimeInForce::ImmediateOrCancel,
            reduce_only: true,
        };
        self.prepare_place_once(&intent, now_ms, Some(market_bbo))
    }

    pub async fn dispatch_once(
        &self,
        mutation: BybitOneShotMutation,
        timestamp_ms: u64,
    ) -> Result<BybitDispatchOnceResult, BybitPhysicalError> {
        if timestamp_ms == 0
            || timestamp_ms < mutation.submitted_at_ms
            || mutation.binding != *self.binding.gateway_binding()
            || mutation.generation != self.capability.version
            || mutation.probe_sha256 != self.probe_sha256
        {
            return Err(BybitPhysicalError::Scope);
        }
        self.authorize(timestamp_ms, mutation.capability)?;
        let pending = mutation.pending();
        match self
            .transport
            .execute_order(
                &self.binding,
                &self.credentials,
                &mutation.request,
                timestamp_ms,
            )
            .await
        {
            Ok(ack) => Ok(BybitDispatchOnceResult::AwaitingReadback(
                pending.with_ack(ack),
            )),
            Err(BybitTransportError::Rejected) => Ok(BybitDispatchOnceResult::Rejected),
            Err(
                BybitTransportError::Binding
                | BybitTransportError::Signing
                | BybitTransportError::BodyTooLarge
                | BybitTransportError::Limits,
            ) => Err(BybitPhysicalError::Transport),
            Err(_) => Ok(BybitDispatchOnceResult::Unknown(pending)),
        }
    }

    fn authorize(
        &self,
        now_ms: u64,
        mutation: MutationCapability,
    ) -> Result<(), BybitPhysicalError> {
        self.capability
            .authorize(
                self.binding.gateway_binding(),
                self.capability.version,
                now_ms,
                mutation,
            )
            .map_err(|_| BybitPhysicalError::Capability)
    }

    fn one_shot(
        &self,
        request: BybitPreparedRequest,
        capability: MutationCapability,
        submitted_at_ms: u64,
    ) -> Result<BybitOneShotMutation, BybitPhysicalError> {
        let lookup = request.exact_lookup()?;
        let request_sha256 = request.body_sha256();
        Ok(BybitOneShotMutation {
            binding: self.binding.gateway_binding().clone(),
            generation: self.capability.version,
            capability,
            request_kind: request.kind,
            lookup,
            submitted_at_ms,
            request_sha256,
            probe_sha256: self.probe_sha256.clone(),
            request,
        })
    }
}

/// Local synchronous shell for the account node's synchronous `PhysicalGateway` boundary. The
/// runtime is created only when a caller has already supplied the persisted inputs and credentials;
/// constructing the fixed node's secret-free candidate does not allocate it or touch the network.
pub struct BybitSynchronousPhysicalSession {
    runtime: Runtime,
    session: BybitPhysicalSession,
}

impl BybitSynchronousPhysicalSession {
    pub fn from_persisted_probe(
        binding: BybitGatewayBinding,
        credentials: BybitCredentials,
        probe_payload: &[u8],
        instrument_payload: BybitRawPublicPayload,
        limits: BybitTransportLimits,
        now_ms: u64,
    ) -> Result<Self, BybitPhysicalError> {
        let session = BybitPhysicalSession::from_persisted_probe(
            binding,
            credentials,
            probe_payload,
            instrument_payload,
            limits,
            now_ms,
        )?;
        Self::from_session(session)
    }

    fn from_session(session: BybitPhysicalSession) -> Result<Self, BybitPhysicalError> {
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| BybitPhysicalError::Runtime)?;
        Ok(Self { runtime, session })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        self.session.binding()
    }

    #[must_use]
    pub fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.session.capability_snapshot()
    }

    pub fn prepare_place_once(
        &self,
        intent: &BybitPlaceIntent,
        now_ms: u64,
        market_bbo: Option<&BybitRestBbo>,
    ) -> Result<BybitOneShotMutation, BybitPhysicalError> {
        self.session.prepare_place_once(intent, now_ms, market_bbo)
    }

    pub fn prepare_cancel_once(
        &self,
        intent: &BybitCancelIntent,
        now_ms: u64,
    ) -> Result<BybitOneShotMutation, BybitPhysicalError> {
        self.session.prepare_cancel_once(intent, now_ms)
    }

    pub fn prepare_reduce_once(
        &self,
        client_order_id: impl Into<String>,
        position_side: PositionSide,
        quantity: Decimal,
        now_ms: u64,
        market_bbo: &BybitRestBbo,
    ) -> Result<BybitOneShotMutation, BybitPhysicalError> {
        self.session.prepare_reduce_once(
            client_order_id,
            position_side,
            quantity,
            now_ms,
            market_bbo,
        )
    }

    pub fn dispatch_once(
        &mut self,
        mutation: BybitOneShotMutation,
        timestamp_ms: u64,
    ) -> Result<BybitDispatchOnceResult, BybitPhysicalError> {
        self.runtime
            .block_on(self.session.dispatch_once(mutation, timestamp_ms))
    }
}

/// Linear dispatch value. It deliberately does not implement `Clone` and is consumed by
/// `dispatch_once`; timeout/disconnect results contain only readback state, never a retry request.
pub struct BybitOneShotMutation {
    binding: GatewayBinding,
    generation: u64,
    capability: MutationCapability,
    request_kind: BybitRequestKind,
    lookup: BybitOrderLookup,
    submitted_at_ms: u64,
    request_sha256: String,
    probe_sha256: String,
    request: BybitPreparedRequest,
}

impl BybitOneShotMutation {
    fn pending(&self) -> BybitPendingMutation {
        BybitPendingMutation {
            binding: self.binding.clone(),
            generation: self.generation,
            request_kind: self.request_kind,
            lookup: self.lookup.clone(),
            submitted_at_ms: self.submitted_at_ms,
            request_sha256: self.request_sha256.clone(),
            ack: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPendingMutation {
    binding: GatewayBinding,
    generation: u64,
    request_kind: BybitRequestKind,
    lookup: BybitOrderLookup,
    submitted_at_ms: u64,
    request_sha256: String,
    ack: Option<BybitOrderAck>,
}

impl BybitPendingMutation {
    fn with_ack(mut self, ack: BybitOrderAck) -> Self {
        self.ack = Some(ack);
        self
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn request_kind(&self) -> BybitRequestKind {
        self.request_kind
    }

    #[must_use]
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }

    /// Returns the two exact first-page reads. Any returned cursor must be exhausted by the caller
    /// before constructing `BybitClosedOrderReadback`.
    pub fn exact_readback_requests(
        &self,
        attempt_id: u64,
        history_window: BybitHistoryWindow,
    ) -> Result<[BybitPreparedPrivateRequest; 2], BybitPhysicalError> {
        let binding = BybitGatewayBinding::new(self.binding.clone())
            .map_err(|_| BybitPhysicalError::Scope)?;
        Ok([
            prepare_private_request(
                &binding,
                self.generation,
                attempt_id,
                0,
                BybitPrivateSource::OpenOrders(venue_domain::domain::NativeOrderFamily::UmOrder),
                None,
                None,
                Some(self.lookup.clone()),
            )?,
            prepare_private_request(
                &binding,
                self.generation,
                attempt_id,
                0,
                BybitPrivateSource::OrderHistory(venue_domain::domain::NativeOrderFamily::UmOrder),
                None,
                Some(history_window),
                Some(self.lookup.clone()),
            )?,
        ])
    }

    pub fn converge(
        &self,
        binding: &BybitGatewayBinding,
        readback: &BybitClosedOrderReadback,
    ) -> Result<BybitReadbackConvergence, BybitPhysicalError> {
        readback.validate_pending_scope(
            binding,
            self.generation,
            self.submitted_at_ms,
            &self.lookup,
        )?;
        if let Some(ack) = &self.ack {
            return match settle_order_ack(binding, ack, readback) {
                Ok(settlement) => Ok(BybitReadbackConvergence::Settled(settlement)),
                Err(BybitExecutionError::Unsettled) => Ok(BybitReadbackConvergence::StillUnknown),
                Err(error) => Err(error.into()),
            };
        }
        Ok(match readback.exact_settlement()? {
            Some(settlement) => BybitReadbackConvergence::Settled(settlement),
            None => BybitReadbackConvergence::StillUnknown,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BybitDispatchOnceResult {
    AwaitingReadback(BybitPendingMutation),
    Rejected,
    Unknown(BybitPendingMutation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BybitReadbackConvergence {
    Settled(BybitOrderSettlement),
    StillUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitPhysicalError {
    #[error("Bybit physical capability probe is invalid, incomplete, or stale")]
    Capability,
    #[error("Bybit physical binding or generation does not match")]
    Scope,
    #[error("Bybit physical mutation intent is invalid")]
    Intent,
    #[error("Bybit physical transport could not safely prepare or dispatch")]
    Transport,
    #[error("Bybit physical exact readback is incomplete or conflicting")]
    Readback,
    #[error("Bybit synchronous physical runtime could not be created")]
    Runtime,
}

impl From<BybitExecutionError> for BybitPhysicalError {
    fn from(error: BybitExecutionError) -> Self {
        match error {
            BybitExecutionError::Binding => Self::Scope,
            BybitExecutionError::Intent | BybitExecutionError::Rules => Self::Intent,
            BybitExecutionError::Readback | BybitExecutionError::Unsettled => Self::Readback,
            BybitExecutionError::Payload
            | BybitExecutionError::VenueRejected
            | BybitExecutionError::Signing => Self::Transport,
        }
    }
}

impl From<crate::BybitError> for BybitPhysicalError {
    fn from(_: crate::BybitError) -> Self {
        Self::Readback
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Read},
        net::TcpListener as StdTcpListener,
        thread,
        time::Duration,
    };

    use tokio::net::TcpListener;
    use venue_domain::domain::Price;
    use venue_gateway_api::{CapabilityFlags, GatewayMode, VenueId};

    use super::*;
    use crate::{
        BybitPublicSource, BybitRawPrivatePayload, BybitRawPublicPayload, complete_position_pages,
        parse_account_identity, parse_linear_instrument, parse_open_order_page, parse_order_ack,
        parse_order_history_page, parse_position_page,
    };

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
    const ACCOUNT: &[u8] = include_bytes!("../fixtures/account-info-uta2.json");
    const POSITIONS: &[u8] = include_bytes!("../fixtures/positions-linear.json");
    const INSTRUMENT: &str = include_str!("../fixtures/instruments-linear.json");
    const BBO: &str = include_str!("../fixtures/orderbook-linear-bbo.json");
    const PLACE_ACK: &[u8] = include_bytes!("../fixtures/place-order-ack.json");
    const OPEN: &[u8] = include_bytes!("../fixtures/exact-open-order-linear.json");
    const EMPTY: &[u8] = br#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"","list":[]},"time":2300}"#;
    const NOW_MS: u64 = 1_716_863_719_500;

    type TestError = Box<dyn std::error::Error + Send + Sync>;

    fn session(
        mode: GatewayMode,
        endpoint: String,
        timeout: Duration,
    ) -> Result<(BybitPhysicalSession, BybitRestBbo), TestError> {
        let binding = BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            mode,
            ACCOUNT_ID,
            "BTC/USDT".parse()?,
        )?)?;
        let account_request = prepare_private_request(
            &binding,
            7,
            11,
            0,
            BybitPrivateSource::AccountInfo,
            None,
            None,
            None,
        )?;
        let account = BybitRawPrivatePayload::from_response(
            &binding,
            &account_request,
            NOW_MS - 200,
            NOW_MS - 100,
            ACCOUNT.to_vec(),
        )?;
        let identity = parse_account_identity(&binding, &account)?;
        let position_request = prepare_private_request(
            &binding,
            7,
            11,
            0,
            BybitPrivateSource::Positions,
            None,
            None,
            None,
        )?;
        let position_raw = BybitRawPrivatePayload::from_response(
            &binding,
            &position_request,
            NOW_MS - 200,
            NOW_MS - 100,
            POSITIONS.to_vec(),
        )?;
        let positions =
            complete_position_pages(&binding, &[parse_position_page(&binding, &position_raw)?])?;
        let instrument = BybitRawPublicPayload::new(
            &binding,
            BybitPublicSource::LinearInstrument,
            7,
            NOW_MS,
            INSTRUMENT.to_owned(),
        )?;
        let rules = parse_linear_instrument(&binding, instrument)?;
        let bbo = crate::parse_rest_bbo(
            &binding,
            BybitRawPublicPayload::new(
                &binding,
                BybitPublicSource::RestOrderBook,
                7,
                NOW_MS,
                BBO.to_owned(),
            )?,
        )?;
        let limits = BybitTransportLimits::new(timeout, 16 * 1_024)?;
        let transport = BybitHttpTransport::with_endpoint(&binding, 7, endpoint, limits)?;
        let capability = CapabilitySnapshot {
            binding: binding.gateway_binding().clone(),
            version: 7,
            observed_ms: NOW_MS - 1_000,
            expires_ms: NOW_MS + 10_000,
            flags: CapabilityFlags::READ_ACCOUNT
                | CapabilityFlags::READ_ORDERS
                | CapabilityFlags::READ_FILLS
                | CapabilityFlags::PRIVATE_STREAM
                | CapabilityFlags::TRADE
                | CapabilityFlags::PLACE_LIMIT
                | CapabilityFlags::PLACE_MARKET
                | CapabilityFlags::CANCEL
                | CapabilityFlags::HEDGE_POSITION,
        };
        Ok((
            BybitPhysicalSession {
                binding,
                credentials: BybitCredentials::from_values("test", "secret")?,
                transport,
                identity,
                positions,
                rules,
                capability,
                probe_sha256: "a".repeat(64),
            },
            bbo,
        ))
    }

    fn place_intent() -> Result<BybitPlaceIntent, TestError> {
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

    #[test]
    fn live_fixture_prepares_bound_place_cancel_and_reduce_once() -> Result<(), TestError> {
        let (session, bbo) = session(
            GatewayMode::Live,
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
        )?;
        let place = session.prepare_place_once(&place_intent()?, NOW_MS, None)?;
        assert_eq!(place.request.path, crate::endpoints::PLACE_ORDER);
        assert_eq!(place.request.binding.mode, GatewayMode::Live);
        let cancel = session.prepare_cancel_once(
            &BybitCancelIntent {
                order_id: None,
                client_order_id: Some("MANAGED_CLIENT_ID".to_owned()),
            },
            NOW_MS,
        )?;
        assert_eq!(cancel.request.path, crate::endpoints::CANCEL_ORDER);
        let reduce = session.prepare_reduce_once(
            "reduce-short-once",
            PositionSide::Short,
            Decimal::new(1, 3),
            NOW_MS,
            &bbo,
        )?;
        let body: serde_json::Value = serde_json::from_slice(&reduce.request.body)?;
        assert_eq!(body["side"], "Buy");
        assert_eq!(body["positionIdx"], 2);
        assert_eq!(body["orderType"], "Market");
        assert_eq!(body["timeInForce"], "IOC");
        assert_eq!(body["reduceOnly"], true);
        Ok(())
    }

    #[test]
    fn stale_or_cross_generation_session_fails_before_mutation() -> Result<(), TestError> {
        let (mut session, _) = session(
            GatewayMode::Live,
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
        )?;
        session.capability.expires_ms = NOW_MS;
        assert!(matches!(
            session.prepare_place_once(&place_intent()?, NOW_MS, None),
            Err(BybitPhysicalError::Capability)
        ));
        session.capability.expires_ms = NOW_MS + 10_000;
        session.identity.generation = 8;
        assert!(matches!(
            session.prepare_place_once(&place_intent()?, NOW_MS, None),
            Err(BybitPhysicalError::Scope)
        ));
        Ok(())
    }

    async fn failure_endpoint(
        delay: Duration,
    ) -> Result<(String, tokio::task::JoinHandle<Result<bool, io::Error>>), TestError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            stream.readable().await?;
            let mut buffer = [0_u8; 4_096];
            let _ = stream.try_read(&mut buffer);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            drop(stream);
            Ok(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_ok(),
            )
        });
        Ok((endpoint, server))
    }

    #[tokio::test]
    async fn disconnect_and_timeout_become_unknown_without_resubmission() -> Result<(), TestError> {
        for (server_delay, timeout) in [
            (Duration::ZERO, Duration::from_secs(1)),
            (Duration::from_millis(200), Duration::from_millis(40)),
        ] {
            let (endpoint, server) = failure_endpoint(server_delay).await?;
            let (session, _) = session(GatewayMode::Live, endpoint, timeout)?;
            let mutation = session.prepare_place_once(&place_intent()?, NOW_MS, None)?;
            assert!(matches!(
                session.dispatch_once(mutation, NOW_MS).await?,
                BybitDispatchOnceResult::Unknown(_)
            ));
            assert!(!server.await??);
        }
        Ok(())
    }

    #[test]
    fn synchronous_shell_consumes_one_request_and_preserves_unknown() -> Result<(), TestError> {
        let listener = StdTcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = thread::spawn(move || -> Result<(), io::Error> {
            let (mut stream, _) = listener.accept()?;
            let mut buffer = [0_u8; 4_096];
            let _ = stream.read(&mut buffer)?;
            Ok(())
        });
        let (session, _) = session(GatewayMode::Live, endpoint, Duration::from_secs(1))?;
        let mut synchronous = BybitSynchronousPhysicalSession::from_session(session)?;
        let mutation = synchronous.prepare_place_once(&place_intent()?, NOW_MS, None)?;
        assert!(matches!(
            synchronous.dispatch_once(mutation, NOW_MS)?,
            BybitDispatchOnceResult::Unknown(_)
        ));
        server
            .join()
            .map_err(|_| io::Error::other("Bybit test server thread panicked"))??;
        Ok(())
    }

    #[test]
    fn ack_remains_unknown_until_new_exact_readback_settles() -> Result<(), TestError> {
        let (session, _) = session(
            GatewayMode::Live,
            "http://127.0.0.1:1".to_owned(),
            Duration::from_secs(1),
        )?;
        let mutation = session.prepare_place_once(&place_intent()?, NOW_MS, None)?;
        let ack = parse_order_ack(&session.binding, &mutation.request, PLACE_ACK, NOW_MS + 100)?;
        let pending = mutation.pending().with_ack(ack);
        let lookup = BybitOrderLookup::by_client_order_id("MANAGED_CLIENT_ID")?;
        let open_request = prepare_private_request(
            &session.binding,
            7,
            12,
            0,
            BybitPrivateSource::OpenOrders(venue_domain::domain::NativeOrderFamily::UmOrder),
            None,
            None,
            Some(lookup.clone()),
        )?;
        let history_request = prepare_private_request(
            &session.binding,
            7,
            12,
            0,
            BybitPrivateSource::OrderHistory(venue_domain::domain::NativeOrderFamily::UmOrder),
            None,
            Some(BybitHistoryWindow::new(NOW_MS - 1_000, NOW_MS + 300)?),
            Some(lookup),
        )?;
        let open_raw = BybitRawPrivatePayload::from_response(
            &session.binding,
            &open_request,
            NOW_MS + 200,
            NOW_MS + 250,
            OPEN.to_vec(),
        )?;
        let history_raw = BybitRawPrivatePayload::from_response(
            &session.binding,
            &history_request,
            NOW_MS + 200,
            NOW_MS + 250,
            EMPTY.to_vec(),
        )?;
        let readback = BybitClosedOrderReadback::from_pages(
            &session.binding,
            7,
            &[parse_open_order_page(&session.binding, &open_raw)?],
            &[parse_order_history_page(&session.binding, &history_raw)?],
        )?;
        assert!(matches!(
            pending.converge(&session.binding, &readback)?,
            BybitReadbackConvergence::Settled(BybitOrderSettlement {
                finality: crate::BybitSettlementFinality::Working,
                ..
            })
        ));
        Ok(())
    }
}
