//! Durable, replayable OKX capability-probe evidence.
//!
//! A validated value remains a candidate only. It does not implement the account-node gateway,
//! acquire a writer, or turn the crate-level static capability set on.

use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

#[cfg(test)]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use venue_domain::domain::{
    CancelCommand, MarketReduceCommand, NativeOrderFamily, OrderCommand, PositionSide,
};
#[cfg(test)]
use venue_domain::domain::{FieldState, OrderState};
use venue_gateway_api::{CapabilityFlags, GatewayBinding, VenueId};

use crate::{
    OkxConfig, OkxError, OkxHttpResponse, OkxInstrument, OkxPositionMode, OkxPrivateReadScope,
    OkxPrivateReadbackCandidate, OkxRawPrivatePage, OkxReceivedPrivateFrame, OkxTradeMode,
    complete_private_readback,
};
#[cfg(test)]
use crate::{
    OkxPlaceIntent, build_cancel_order_readback_request, build_cancel_request,
    build_order_readback_request, build_place_request, parse_cancel_ack, parse_order_detail,
    parse_place_ack,
};

pub const OKX_CAPABILITY_PROBE_SCHEMA_VERSION: u16 = 1;
/// Stable label for schema-1 probe files. They are legacy, caller-assembled capability evidence;
/// they are not fresh recovery collection and never authorize startup or mutation.
pub const OKX_LEGACY_CAPABILITY_PROBE_EVIDENCE_CLASS: &str =
    "legacy_non_authoritative_capability_probe";
const MAX_PERSISTED_PROBE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxCapabilityProbeScope {
    pub binding: GatewayBinding,
    pub native_instrument_id: String,
    pub instrument_generation: u64,
    pub read_attempt_id: u64,
    pub private_generation: u64,
    pub capability_version: u64,
    pub position_mode: OkxPositionMode,
    pub trade_mode: OkxTradeMode,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxProbeHttpResponse {
    pub binding: GatewayBinding,
    pub instrument_generation: u64,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: Vec<u8>,
}

impl OkxProbeHttpResponse {
    pub fn capture(response: OkxHttpResponse) -> Result<Self, OkxError> {
        let captured = Self {
            binding: response.binding,
            instrument_generation: response.instrument_generation,
            received_at_ms: response.received_at_ms,
            payload_sha256: crate::readback::payload_digest(&response.body),
            payload: response.body.to_vec(),
        };
        captured.validate()?;
        Ok(captured)
    }

    fn validate(&self) -> Result<(), OkxError> {
        if self.binding.venue != VenueId::Okx
            || self.instrument_generation == 0
            || self.received_at_ms == 0
            || self.payload.is_empty()
            || self.payload_sha256 != crate::readback::payload_digest(&self.payload)
        {
            return Err(OkxError::Capability);
        }
        Ok(())
    }

    #[cfg(test)]
    fn response(&self) -> Result<OkxHttpResponse, OkxError> {
        self.validate()?;
        Ok(OkxHttpResponse {
            binding: self.binding.clone(),
            instrument_generation: self.instrument_generation,
            received_at_ms: self.received_at_ms,
            body: Bytes::copy_from_slice(&self.payload),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxPrivateStreamProbeFrame {
    pub binding: GatewayBinding,
    pub native_instrument_id: String,
    pub instrument_generation: u64,
    pub private_generation: u64,
    pub connection_id: String,
    pub subscription_request_id: String,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: Vec<u8>,
}

impl OkxPrivateStreamProbeFrame {
    pub fn capture(frame: OkxReceivedPrivateFrame) -> Result<Self, OkxError> {
        if frame.binding != *frame.scope.gateway_binding()
            || frame.instrument_generation != frame.scope.instrument_generation()
            || frame.private_generation != frame.scope.private_generation()
            || frame.connection_id.is_empty()
            || frame.subscription_request_id.is_empty()
            || frame.received_at_ms == 0
            || frame.payload.is_empty()
        {
            return Err(OkxError::Capability);
        }
        let payload = frame.payload.to_vec();
        Ok(Self {
            binding: frame.binding,
            native_instrument_id: frame.scope.native_instrument_id().to_owned(),
            instrument_generation: frame.instrument_generation,
            private_generation: frame.private_generation,
            connection_id: frame.connection_id,
            subscription_request_id: frame.subscription_request_id,
            received_at_ms: frame.received_at_ms,
            payload_sha256: crate::readback::payload_digest(&payload),
            payload,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxPrivateStreamProbeEvidence {
    pub connected_at_ms: u64,
    pub frames: Vec<OkxPrivateStreamProbeFrame>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxMutationProbeEvidence {
    pub place: OrderCommand,
    pub place_ack: OkxProbeHttpResponse,
    pub place_detail: OkxProbeHttpResponse,
    pub cancel: CancelCommand,
    pub cancel_ack: OkxProbeHttpResponse,
    pub cancel_detail: OkxProbeHttpResponse,
    pub reduce: MarketReduceCommand,
    pub reduce_ack: OkxProbeHttpResponse,
    pub reduce_detail: OkxProbeHttpResponse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxCapabilityProbeEvidence {
    pub schema_version: u16,
    pub scope: OkxCapabilityProbeScope,
    pub private_pages: Vec<OkxRawPrivatePage>,
    pub private_stream: OkxPrivateStreamProbeEvidence,
    pub mutations: OkxMutationProbeEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedOkxCapabilityProbe {
    evidence: OkxCapabilityProbeEvidence,
    evidence_sha256: String,
}

impl PersistedOkxCapabilityProbe {
    #[must_use]
    pub const fn evidence(&self) -> &OkxCapabilityProbeEvidence {
        &self.evidence
    }

    #[must_use]
    pub fn evidence_sha256(&self) -> &str {
        &self.evidence_sha256
    }
}

/// Validated, immutable view of legacy probe evidence.
///
/// Mutation and scope fields are deliberately not writable by callers:
///
/// ```compile_fail
/// use venue_gateway_okx::OkxCapabilityCandidate;
///
/// fn rewrite_flags(candidate: &mut OkxCapabilityCandidate) {
///     candidate.candidate_flags = candidate.candidate_flags();
/// }
/// ```
///
/// ```compile_fail
/// use venue_gateway_okx::OkxCapabilityCandidate;
///
/// fn rewrite_generation(candidate: &mut OkxCapabilityCandidate) {
///     candidate.scope.private_generation += 1;
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxCapabilityCandidate {
    scope: OkxCapabilityProbeScope,
    evidence_sha256: String,
    candidate_flags: CapabilityFlags,
    readback: OkxPrivateReadbackCandidate,
}

impl OkxCapabilityCandidate {
    #[must_use]
    pub const fn scope(&self) -> &OkxCapabilityProbeScope {
        &self.scope
    }

    #[must_use]
    pub fn evidence_sha256(&self) -> &str {
        &self.evidence_sha256
    }

    #[must_use]
    pub const fn candidate_flags(&self) -> CapabilityFlags {
        self.candidate_flags
    }

    #[must_use]
    pub const fn readback(&self) -> &OkxPrivateReadbackCandidate {
        &self.readback
    }
}

#[derive(Serialize, Deserialize)]
struct ProbeEnvelope {
    schema_version: u16,
    evidence_sha256: String,
    evidence_json: Vec<u8>,
}

pub fn persist_capability_probe(
    path: &Path,
    evidence: &OkxCapabilityProbeEvidence,
) -> Result<PersistedOkxCapabilityProbe, OkxError> {
    if evidence.schema_version != OKX_CAPABILITY_PROBE_SCHEMA_VERSION {
        return Err(OkxError::Capability);
    }
    let evidence_json = serde_json::to_vec(evidence).map_err(|_| OkxError::Persistence)?;
    let envelope = ProbeEnvelope {
        schema_version: OKX_CAPABILITY_PROBE_SCHEMA_VERSION,
        evidence_sha256: crate::readback::payload_digest(&evidence_json),
        evidence_json,
    };
    let encoded = serde_json::to_vec(&envelope).map_err(|_| OkxError::Persistence)?;
    if u64::try_from(encoded.len()).map_err(|_| OkxError::Persistence)? > MAX_PERSISTED_PROBE_BYTES
    {
        return Err(OkxError::Persistence);
    }
    if write_immutable(path, &encoded).is_err() {
        return Err(OkxError::Persistence);
    }
    load_capability_probe(path)
}

pub fn load_capability_probe(path: &Path) -> Result<PersistedOkxCapabilityProbe, OkxError> {
    let metadata = fs::metadata(path).map_err(|_| OkxError::Persistence)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_PERSISTED_PROBE_BYTES {
        return Err(OkxError::Persistence);
    }
    let encoded = fs::read(path).map_err(|_| OkxError::Persistence)?;
    let envelope: ProbeEnvelope =
        serde_json::from_slice(&encoded).map_err(|_| OkxError::Persistence)?;
    if envelope.schema_version != OKX_CAPABILITY_PROBE_SCHEMA_VERSION
        || envelope.evidence_sha256 != crate::readback::payload_digest(&envelope.evidence_json)
    {
        return Err(OkxError::Persistence);
    }
    let evidence: OkxCapabilityProbeEvidence =
        serde_json::from_slice(&envelope.evidence_json).map_err(|_| OkxError::Persistence)?;
    if evidence.schema_version != OKX_CAPABILITY_PROBE_SCHEMA_VERSION {
        return Err(OkxError::Persistence);
    }
    Ok(PersistedOkxCapabilityProbe {
        evidence,
        evidence_sha256: envelope.evidence_sha256,
    })
}

fn write_immutable(path: &Path, encoded: &[u8]) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(encoded)?;
    file.sync_all()?;
    #[cfg(unix)]
    OpenOptions::new()
        .read(true)
        .open(
            path.parent()
                .filter(|value| !value.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?
        .sync_all()?;
    Ok(())
}

/// Legacy schema-1 probes can establish read-side consistency only. Mutation fields remain in the
/// frozen wire schema, but this production validator never returns mutation capability flags.
pub fn validate_capability_candidate(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    persisted: &PersistedOkxCapabilityProbe,
    validated_at_ms: u64,
) -> Result<OkxCapabilityCandidate, OkxError> {
    validate_read_capability_candidate(config, instrument, persisted, validated_at_ms)
}

/// Test-only replay of the frozen schema-1 mutation fixture. This cannot be enabled by a Cargo
/// feature and is not present in normal library builds.
#[cfg(test)]
pub(crate) fn validate_mutation_capability_fixture(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    persisted: &PersistedOkxCapabilityProbe,
    validated_at_ms: u64,
) -> Result<OkxCapabilityCandidate, OkxError> {
    let (evidence, readback, stream_observed) =
        validate_read_candidate_parts(config, instrument, persisted, validated_at_ms)?;
    let scope = &evidence.scope;
    if scope.position_mode != OkxPositionMode::LongShort {
        // Shared canonical mutation commands carry explicit LONG/SHORT meaning. Net mutation
        // remains rejected even though a Net account may produce complete read-side evidence.
        return Err(OkxError::Capability);
    }
    let mutation_observed = validate_mutations(config, instrument, &readback, &evidence.mutations)?;
    let observed_at_ms = readback
        .observed_at_ms
        .max(stream_observed)
        .max(mutation_observed);
    if observed_at_ms != scope.observed_at_ms {
        return Err(OkxError::Capability);
    }
    let candidate_flags = read_flags(scope.position_mode)
        | CapabilityFlags::TRADE
        | CapabilityFlags::PLACE_LIMIT
        | CapabilityFlags::PLACE_MARKET
        | CapabilityFlags::CANCEL;
    if candidate_flags.contains(CapabilityFlags::WITHDRAW) {
        return Err(OkxError::Capability);
    }
    Ok(OkxCapabilityCandidate {
        scope: scope.clone(),
        evidence_sha256: persisted.evidence_sha256().to_owned(),
        candidate_flags,
        readback,
    })
}

/// Validates the durable account/order/fill/private-stream portion for either OKX Net or
/// Long/Short mode. This deliberately omits TRADE and all mutation flags: callers cannot turn a
/// read-only candidate into authority for canonical LONG/SHORT commands.
pub fn validate_read_capability_candidate(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    persisted: &PersistedOkxCapabilityProbe,
    validated_at_ms: u64,
) -> Result<OkxCapabilityCandidate, OkxError> {
    let (evidence, readback, stream_observed) =
        validate_read_candidate_parts(config, instrument, persisted, validated_at_ms)?;
    let scope = &evidence.scope;
    if readback.observed_at_ms.max(stream_observed) != scope.observed_at_ms {
        return Err(OkxError::Capability);
    }
    Ok(OkxCapabilityCandidate {
        scope: scope.clone(),
        evidence_sha256: persisted.evidence_sha256().to_owned(),
        candidate_flags: read_flags(scope.position_mode),
        readback,
    })
}

fn validate_read_candidate_parts<'a>(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    persisted: &'a PersistedOkxCapabilityProbe,
    validated_at_ms: u64,
) -> Result<
    (
        &'a OkxCapabilityProbeEvidence,
        OkxPrivateReadbackCandidate,
        u64,
    ),
    OkxError,
> {
    let evidence = persisted.evidence();
    let scope = &evidence.scope;
    instrument.validate_scope(config)?;
    if evidence.schema_version != OKX_CAPABILITY_PROBE_SCHEMA_VERSION
        || scope.binding != *config.gateway_binding()
        || scope.native_instrument_id != instrument.native_id()
        || scope.instrument_generation != instrument.instrument().generation
        || scope.read_attempt_id == 0
        || scope.private_generation == 0
        || scope.capability_version == 0
        || scope.observed_at_ms == 0
        || scope.expires_at_ms <= scope.observed_at_ms
        || validated_at_ms < scope.observed_at_ms
        || validated_at_ms >= scope.expires_at_ms
    {
        return Err(OkxError::Capability);
    }
    let read_scope = OkxPrivateReadScope::new(
        config,
        instrument,
        scope.position_mode,
        scope.trade_mode,
        scope.read_attempt_id,
    )?;
    let readback =
        complete_private_readback(&read_scope, instrument, evidence.private_pages.clone())?;
    if readback.scope() != &read_scope
        || !readback.profile.can_read()
        || !readback.profile.can_trade()
        || readback.profile.can_withdraw()
    {
        return Err(OkxError::Capability);
    }
    validate_positions(&readback, scope.position_mode)?;
    validate_order_families(&readback)?;
    let stream_observed = validate_private_stream(scope, &readback, &evidence.private_stream)?;
    Ok((evidence, readback, stream_observed))
}

fn read_flags(position_mode: OkxPositionMode) -> CapabilityFlags {
    let mut flags = CapabilityFlags::READ_ACCOUNT
        | CapabilityFlags::READ_ORDERS
        | CapabilityFlags::READ_FILLS
        | CapabilityFlags::PRIVATE_STREAM;
    if position_mode == OkxPositionMode::LongShort {
        flags |= CapabilityFlags::HEDGE_POSITION;
    }
    flags
}

fn validate_positions(
    readback: &OkxPrivateReadbackCandidate,
    mode: OkxPositionMode,
) -> Result<(), OkxError> {
    let actual = readback
        .positions
        .iter()
        .map(|fact| fact.position.side)
        .collect::<BTreeSet<_>>();
    let expected = match mode {
        OkxPositionMode::Net => BTreeSet::from([PositionSide::Net]),
        OkxPositionMode::LongShort => BTreeSet::from([PositionSide::Long, PositionSide::Short]),
    };
    if actual != expected || actual.len() != readback.positions.len() {
        return Err(OkxError::Capability);
    }
    Ok(())
}

fn validate_order_families(readback: &OkxPrivateReadbackCandidate) -> Result<(), OkxError> {
    let expected = BTreeSet::from([
        NativeOrderFamily::UmOrder,
        NativeOrderFamily::UmConditional,
        NativeOrderFamily::UmAlgo,
    ]);
    let actual = readback
        .order_families
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    if actual != expected
        || readback
            .order_families
            .iter()
            .any(|(family, value)| *family != value.family || value.raw_pages.is_empty())
    {
        return Err(OkxError::Capability);
    }
    Ok(())
}

fn validate_private_stream(
    scope: &OkxCapabilityProbeScope,
    readback: &OkxPrivateReadbackCandidate,
    stream: &OkxPrivateStreamProbeEvidence,
) -> Result<u64, OkxError> {
    if stream.connected_at_ms == 0 || stream.frames.len() != 3 {
        return Err(OkxError::Capability);
    }
    let mut channels = BTreeSet::new();
    let mut connection_id = None;
    let mut subscription_request_id = None;
    let mut observed = 0;
    for frame in &stream.frames {
        if frame.binding != scope.binding
            || frame.native_instrument_id != scope.native_instrument_id
            || frame.instrument_generation != scope.instrument_generation
            || frame.private_generation != scope.private_generation
            || frame.connection_id.is_empty()
            || frame.subscription_request_id.is_empty()
            || frame.received_at_ms < stream.connected_at_ms
            || frame.payload.is_empty()
            || frame.payload_sha256 != crate::readback::payload_digest(&frame.payload)
        {
            return Err(OkxError::Capability);
        }
        if connection_id.get_or_insert_with(|| frame.connection_id.clone()) != &frame.connection_id
            || subscription_request_id.get_or_insert_with(|| frame.subscription_request_id.clone())
                != &frame.subscription_request_id
        {
            return Err(OkxError::Capability);
        }
        let value: Value =
            serde_json::from_slice(&frame.payload).map_err(|_| OkxError::Capability)?;
        let arg = value
            .get("arg")
            .and_then(Value::as_object)
            .ok_or(OkxError::Capability)?;
        let channel = arg
            .get("channel")
            .and_then(Value::as_str)
            .ok_or(OkxError::Capability)?;
        let uid = arg
            .get("uid")
            .and_then(Value::as_str)
            .ok_or(OkxError::Capability)?;
        if uid != readback.profile.uid() || !channels.insert(channel.to_owned()) {
            return Err(OkxError::Capability);
        }
        let valid_scope = match channel {
            "orders" | "positions" => {
                arg.get("instType").and_then(Value::as_str) == Some("SWAP")
                    && arg.get("instId").and_then(Value::as_str)
                        == Some(scope.native_instrument_id.as_str())
            }
            "account" => {
                arg.get("ccy").and_then(Value::as_str) == Some(scope.binding.symbol.quote())
            }
            _ => false,
        };
        if !valid_scope || !value.get("data").is_some_and(Value::is_array) {
            return Err(OkxError::Capability);
        }
        observed = observed.max(frame.received_at_ms);
    }
    if channels
        != BTreeSet::from([
            "account".to_owned(),
            "orders".to_owned(),
            "positions".to_owned(),
        ])
    {
        return Err(OkxError::Capability);
    }
    Ok(observed)
}

#[cfg(test)]
fn validate_mutations(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    readback: &OkxPrivateReadbackCandidate,
    mutations: &OkxMutationProbeEvidence,
) -> Result<u64, OkxError> {
    if readback.profile.position_mode() != OkxPositionMode::LongShort {
        return Err(OkxError::Capability);
    }
    let place_request = build_place_request(
        config,
        instrument,
        &readback.profile,
        readback.scope().trade_mode(),
        OkxPlaceIntent::Limit(&mutations.place),
    )?;
    let accepted_place = parse_place_ack(mutations.place_ack.response()?, &place_request)?;
    let place_detail_request =
        build_order_readback_request(config, instrument, &readback.profile, &accepted_place)?;
    let place_detail =
        parse_order_detail(mutations.place_detail.response()?, &place_detail_request)?;
    if !matches!(
        place_detail.order.order.state,
        OrderState::New | OrderState::PartiallyFilled
    ) {
        return Err(OkxError::Capability);
    }
    let cancel_request = build_cancel_request(
        config,
        instrument,
        &readback.profile,
        &mutations.cancel,
        &accepted_place,
    )?;
    let accepted_cancel = parse_cancel_ack(mutations.cancel_ack.response()?, &cancel_request)?;
    let cancel_detail_request = build_cancel_order_readback_request(
        config,
        instrument,
        &readback.profile,
        &accepted_place,
        &accepted_cancel,
    )?;
    let cancel_detail =
        parse_order_detail(mutations.cancel_detail.response()?, &cancel_detail_request)?;
    if cancel_detail.order.order.state != OrderState::Cancelled {
        return Err(OkxError::Capability);
    }
    let reduce_request = build_place_request(
        config,
        instrument,
        &readback.profile,
        readback.scope().trade_mode(),
        OkxPlaceIntent::MarketReduce(&mutations.reduce),
    )?;
    if !reduce_request.is_reduce_once() {
        return Err(OkxError::Capability);
    }
    let accepted_reduce = parse_place_ack(mutations.reduce_ack.response()?, &reduce_request)?;
    let reduce_detail_request =
        build_order_readback_request(config, instrument, &readback.profile, &accepted_reduce)?;
    let reduce_detail =
        parse_order_detail(mutations.reduce_detail.response()?, &reduce_detail_request)?;
    let matching_fills = readback
        .fills
        .iter()
        .filter(|fill| {
            fill.fill.order_id == accepted_reduce.order_id()
                && matches!(
                    &fill.client_order_id,
                    FieldState::Known(value) if value == accepted_reduce.client_order_id()
                )
        })
        .collect::<Vec<_>>();
    let filled_quantity = matching_fills
        .iter()
        .try_fold(rust_decimal::Decimal::ZERO, |quantity, fill| {
            quantity.checked_add(fill.fill.quantity)
        })
        .ok_or(OkxError::Capability)?;
    if reduce_detail.order.order.state != OrderState::Filled
        || matching_fills.is_empty()
        || matching_fills.iter().any(|fill| {
            fill.fill.side != mutations.reduce.side
                || fill.fill.position_side != FieldState::Known(mutations.reduce.position_side)
        })
        || filled_quantity != mutations.reduce.quantity
        || accepted_place.order_id() == accepted_reduce.order_id()
        || accepted_place.client_order_id() == accepted_reduce.client_order_id()
    {
        return Err(OkxError::Capability);
    }
    let times = [
        mutations.place_ack.received_at_ms,
        mutations.place_detail.received_at_ms,
        mutations.cancel_ack.received_at_ms,
        mutations.cancel_detail.received_at_ms,
        mutations.reduce_ack.received_at_ms,
        mutations.reduce_detail.received_at_ms,
    ];
    if times.windows(2).any(|window| window[0] > window[1]) || readback.observed_at_ms < times[5] {
        return Err(OkxError::Capability);
    }
    Ok(times[5])
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use rust_decimal::Decimal;
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use venue_domain::domain::{
        CommandId, ExecutionCommand, OrderOwner, OrderPurpose, OrderSide, Price,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;
    use crate::{
        OkxAlgoOrderKind, OkxCredentials, OkxPrivateReadRequest, build_account_config_request,
        build_algo_orders_request, build_balance_request, build_fills_request,
        build_positions_request, build_regular_orders_request, capabilities, parse_instrument,
    };

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const BASE_MS: u64 = 1_787_911_200_000;
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    type Fixture = (OkxConfig, OkxInstrument, OkxCapabilityProbeEvidence);

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Okx,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn owner(config: &OkxConfig, purpose: OrderPurpose) -> OrderOwner {
        OrderOwner {
            strategy_instance_id: "probe1".to_owned(),
            run_id: "run1".to_owned(),
            exchange: "okx".to_owned(),
            account: config.gateway_binding().trading_account_id.clone(),
            symbol: config.gateway_binding().symbol.clone(),
            purpose,
        }
    }

    fn private_page(
        request: &OkxPrivateReadRequest,
        payload: Value,
    ) -> Result<OkxRawPrivatePage, OkxError> {
        OkxRawPrivatePage::new(
            request,
            BASE_MS + 700,
            serde_json::to_vec(&payload).map_err(|_| OkxError::Payload)?,
        )
    }

    fn probe_response(
        binding: &GatewayBinding,
        payload: Value,
        received_at_ms: u64,
    ) -> Result<OkxProbeHttpResponse, OkxError> {
        let payload = serde_json::to_vec(&payload).map_err(|_| OkxError::Payload)?;
        Ok(OkxProbeHttpResponse {
            binding: binding.clone(),
            instrument_generation: 7,
            received_at_ms,
            payload_sha256: crate::readback::payload_digest(&payload),
            payload,
        })
    }

    fn ack(order_id: &str, client_id: &str, time_ms: u64) -> Value {
        json!({"code":"0","msg":"","data":[{
            "ordId":order_id,"clOrdId":client_id,"ts":time_ms.to_string(),"sCode":"0","sMsg":""
        }]})
    }

    #[allow(clippy::too_many_arguments)]
    fn detail(
        order_id: &str,
        client_id: &str,
        order_type: &str,
        side: &str,
        size: &str,
        filled: &str,
        price: &str,
        average: &str,
        state: &str,
        update_time_ms: u64,
    ) -> Value {
        json!({"code":"0","msg":"","data":[{
            "instType":"SWAP","instId":"BTC-USDT-SWAP","tdMode":"cross",
            "ordType":order_type,"ordId":order_id,"clOrdId":client_id,
            "side":side,"posSide":"long","sz":size,"accFillSz":filled,
            "px":price,"avgPx":average,"reduceOnly":"false","state":state,
            "uTime":update_time_ms.to_string()
        }]})
    }

    fn stream_frame(
        binding: &GatewayBinding,
        channel: &str,
        received_at_ms: u64,
    ) -> Result<OkxPrivateStreamProbeFrame, OkxError> {
        let arg = match channel {
            "account" => json!({"channel":"account","uid":"fixture-sub-account","ccy":"USDT"}),
            _ => {
                json!({"channel":channel,"uid":"fixture-sub-account","instType":"SWAP","instId":"BTC-USDT-SWAP"})
            }
        };
        let payload =
            serde_json::to_vec(&json!({"arg":arg,"data":[]})).map_err(|_| OkxError::Payload)?;
        Ok(OkxPrivateStreamProbeFrame {
            binding: binding.clone(),
            native_instrument_id: "BTC-USDT-SWAP".to_owned(),
            instrument_generation: 7,
            private_generation: 17,
            connection_id: "connection17".to_owned(),
            subscription_request_id: "probe17".to_owned(),
            received_at_ms,
            payload_sha256: crate::readback::payload_digest(&payload),
            payload,
        })
    }

    fn fixture(mode: GatewayMode) -> Result<Fixture, Box<dyn std::error::Error>> {
        let config = OkxConfig::for_binding(binding(mode)?)?;
        let instrument = parse_instrument(INSTRUMENT, &config, 7)?;
        let read_scope = OkxPrivateReadScope::new(
            &config,
            &instrument,
            OkxPositionMode::LongShort,
            OkxTradeMode::Cross,
            11,
        )?;
        let empty = json!({"code":"0","msg":"","data":[]});
        let mut private_pages = vec![
            private_page(
                &build_account_config_request(&read_scope)?,
                json!({"code":"0","msg":"","data":[{
                    "uid":"fixture-sub-account","mainUid":"fixture-main-account",
                    "acctLv":"3","posMode":"long_short_mode","perm":"read_only,trade"
                }]}),
            )?,
            private_page(
                &build_balance_request(&read_scope)?,
                json!({"code":"0","msg":"","data":[{
                    "uTime":(BASE_MS+600).to_string(),"details":[{
                        "ccy":"USDT","eq":"20000","availBal":"18000","imr":"4000",
                        "mmr":"1200","uTime":(BASE_MS+590).to_string()
                    }]
                }]}),
            )?,
            private_page(
                &build_positions_request(&read_scope)?,
                json!({"code":"0","msg":"","data":[
                    {"instType":"SWAP","instId":"BTC-USDT-SWAP","mgnMode":"cross",
                     "posSide":"long","pos":"0","avgPx":"","markPx":"","uTime":(BASE_MS+600).to_string()},
                    {"instType":"SWAP","instId":"BTC-USDT-SWAP","mgnMode":"cross",
                     "posSide":"short","pos":"0","avgPx":"","markPx":"","uTime":(BASE_MS+600).to_string()}
                ]}),
            )?,
            private_page(
                &build_regular_orders_request(&read_scope, 0, None)?,
                empty.clone(),
            )?,
        ];
        for kind in [
            OkxAlgoOrderKind::ConditionalOco,
            OkxAlgoOrderKind::Trigger,
            OkxAlgoOrderKind::MoveOrderStop,
            OkxAlgoOrderKind::Chase,
            OkxAlgoOrderKind::Iceberg,
            OkxAlgoOrderKind::Twap,
            OkxAlgoOrderKind::SmartIceberg,
        ] {
            private_pages.push(private_page(
                &build_algo_orders_request(&read_scope, kind, 0, None)?,
                empty.clone(),
            )?);
        }
        let reduce_client_id = "00000000000000000000000000000004";
        private_pages.push(private_page(
            &build_fills_request(&read_scope, 0, None)?,
            json!({"code":"0","msg":"","data":[{
                "instType":"SWAP","instId":"BTC-USDT-SWAP","billId":"9004",
                "ordId":"7004","clOrdId":reduce_client_id,"fillPx":"60100","fillSz":"1",
                "side":"sell","posSide":"long","feeCcy":"USDT","fee":"-0.1",
                "ts":(BASE_MS+570).to_string(),"fillTime":(BASE_MS+565).to_string(),"execType":"T"
            }]}),
        )?);

        let place_client_id = "00000000000000000000000000000003";
        let place = OrderCommand {
            command_id: CommandId::new("probeplace")?,
            client_order_id: CommandId::new(place_client_id)?,
            owner: owner(&config, OrderPurpose::Entry),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 1),
            limit_price: Price::new(Decimal::new(60_000, 0))?,
            reduce_only: false,
        };
        let cancel = CancelCommand {
            command_id: CommandId::new("probecancel")?,
            owner: owner(&config, OrderPurpose::Entry),
            target_client_order_id: place.client_order_id.clone(),
        };
        let reduce = MarketReduceCommand {
            command_id: CommandId::new("probereduce")?,
            client_order_id: CommandId::new(reduce_client_id)?,
            owner: owner(&config, OrderPurpose::ExposureTakeProfit),
            position_side: PositionSide::Long,
            side: OrderSide::Sell,
            quantity: Decimal::new(1, 1),
            risk_episode_id: CommandId::new("episode1")?,
            position_generation: 9,
        };
        let mutations = OkxMutationProbeEvidence {
            place,
            place_ack: probe_response(
                config.gateway_binding(),
                ack("7003", place_client_id, BASE_MS + 300),
                BASE_MS + 350,
            )?,
            place_detail: probe_response(
                config.gateway_binding(),
                detail(
                    "7003",
                    place_client_id,
                    "limit",
                    "buy",
                    "2",
                    "0",
                    "60000",
                    "",
                    "live",
                    BASE_MS + 370,
                ),
                BASE_MS + 380,
            )?,
            cancel,
            cancel_ack: probe_response(
                config.gateway_binding(),
                ack("7003", place_client_id, BASE_MS + 400),
                BASE_MS + 450,
            )?,
            cancel_detail: probe_response(
                config.gateway_binding(),
                detail(
                    "7003",
                    place_client_id,
                    "limit",
                    "buy",
                    "2",
                    "0",
                    "60000",
                    "",
                    "canceled",
                    BASE_MS + 470,
                ),
                BASE_MS + 480,
            )?,
            reduce,
            reduce_ack: probe_response(
                config.gateway_binding(),
                ack("7004", reduce_client_id, BASE_MS + 500),
                BASE_MS + 550,
            )?,
            reduce_detail: probe_response(
                config.gateway_binding(),
                detail(
                    "7004",
                    reduce_client_id,
                    "market",
                    "sell",
                    "1",
                    "1",
                    "",
                    "60100",
                    "filled",
                    BASE_MS + 570,
                ),
                BASE_MS + 580,
            )?,
        };
        let evidence = OkxCapabilityProbeEvidence {
            schema_version: OKX_CAPABILITY_PROBE_SCHEMA_VERSION,
            scope: OkxCapabilityProbeScope {
                binding: config.gateway_binding().clone(),
                native_instrument_id: instrument.native_id().to_owned(),
                instrument_generation: 7,
                read_attempt_id: 11,
                private_generation: 17,
                capability_version: 3,
                position_mode: OkxPositionMode::LongShort,
                trade_mode: OkxTradeMode::Cross,
                observed_at_ms: BASE_MS + 700,
                expires_at_ms: BASE_MS + 1_700,
            },
            private_pages,
            private_stream: OkxPrivateStreamProbeEvidence {
                connected_at_ms: BASE_MS + 600,
                frames: vec![
                    stream_frame(config.gateway_binding(), "orders", BASE_MS + 610)?,
                    stream_frame(config.gateway_binding(), "account", BASE_MS + 620)?,
                    stream_frame(config.gateway_binding(), "positions", BASE_MS + 630)?,
                ],
            },
            mutations,
        };
        Ok((config, instrument, evidence))
    }

    fn test_path(name: &str) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("venue-okx-capability-{}-{id}", std::process::id()));
        fs::create_dir(&directory)?;
        Ok((directory.join(name), directory))
    }

    fn persist_fixture(
        name: &str,
        evidence: &OkxCapabilityProbeEvidence,
    ) -> Result<(PersistedOkxCapabilityProbe, PathBuf), Box<dyn std::error::Error>> {
        let (path, directory) = test_path(name)?;
        let persisted = persist_capability_probe(&path, evidence)?;
        Ok((persisted, directory))
    }

    #[test]
    fn complete_durable_probe_forms_only_a_non_authoritative_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, evidence) = fixture(GatewayMode::Live)?;
        let (persisted, directory) = persist_fixture("probe.json", &evidence)?;
        let candidate =
            validate_capability_candidate(&config, &instrument, &persisted, BASE_MS + 1_000)?;
        assert!(
            candidate
                .candidate_flags()
                .contains(CapabilityFlags::PRIVATE_STREAM)
        );
        assert!(
            candidate
                .candidate_flags()
                .contains(CapabilityFlags::HEDGE_POSITION)
        );
        assert!(!candidate.candidate_flags().contains(CapabilityFlags::TRADE));
        assert!(
            !candidate
                .candidate_flags()
                .contains(CapabilityFlags::PLACE_LIMIT)
        );
        assert!(
            !candidate
                .candidate_flags()
                .contains(CapabilityFlags::PLACE_MARKET)
        );
        assert!(
            !candidate
                .candidate_flags()
                .contains(CapabilityFlags::CANCEL)
        );
        let fixture_candidate = validate_mutation_capability_fixture(
            &config,
            &instrument,
            &persisted,
            BASE_MS + 1_000,
        )?;
        assert!(
            fixture_candidate
                .candidate_flags()
                .contains(CapabilityFlags::PLACE_LIMIT)
        );
        assert!(
            !candidate
                .candidate_flags()
                .contains(CapabilityFlags::WITHDRAW)
        );
        assert_eq!(
            validate_capability_candidate(
                &config,
                &instrument,
                &persisted,
                evidence.scope.expires_at_ms,
            ),
            Err(OkxError::Capability)
        );
        assert_eq!(
            persist_capability_probe(&directory.join("probe.json"), &evidence),
            Err(OkxError::Persistence)
        );
        assert_eq!(capabilities(), CapabilityFlags::empty());
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn legacy_probe_cannot_create_a_production_physical_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, evidence) = fixture(GatewayMode::Live)?;
        let (persisted, directory) = persist_fixture("legacy-physical.json", &evidence)?;
        assert!(matches!(
            crate::OkxPhysicalCandidate::from_probe(
                config,
                instrument,
                &persisted,
                BASE_MS + 1_000
            ),
            Err(crate::OkxPhysicalError::LegacyProbeUnavailable)
        ));
        assert_eq!(crate::capabilities(), CapabilityFlags::empty());
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn net_mode_is_complete_read_side_only_and_cannot_authorize_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, mut evidence) = fixture(GatewayMode::Live)?;
        evidence.scope.position_mode = OkxPositionMode::Net;
        let net_scope = OkxPrivateReadScope::new(
            &config,
            &instrument,
            OkxPositionMode::Net,
            evidence.scope.trade_mode,
            evidence.scope.read_attempt_id,
        )?;
        for page in &mut evidence.private_pages {
            page.scope = net_scope.clone();
            if page.surface == crate::OkxPrivateSurface::AccountConfig {
                let mut payload: Value = serde_json::from_slice(&page.payload)?;
                payload["data"][0]["posMode"] = Value::String("net_mode".to_owned());
                page.payload = serde_json::to_vec(&payload)?;
                page.payload_sha256 = crate::readback::payload_digest(&page.payload);
            } else if page.surface == crate::OkxPrivateSurface::Positions {
                let payload = json!({"code":"0","msg":"","data":[{
                    "instType":"SWAP","instId":"BTC-USDT-SWAP","mgnMode":"cross",
                    "posSide":"net","pos":"0","avgPx":"","markPx":"",
                    "uTime":(BASE_MS+600).to_string()
                }]});
                page.payload = serde_json::to_vec(&payload)?;
                page.payload_sha256 = crate::readback::payload_digest(&page.payload);
            } else if page.surface == crate::OkxPrivateSurface::Fills {
                let mut payload: Value = serde_json::from_slice(&page.payload)?;
                payload["data"][0]["posSide"] = Value::String("net".to_owned());
                page.payload = serde_json::to_vec(&payload)?;
                page.payload_sha256 = crate::readback::payload_digest(&page.payload);
            }
        }
        let (persisted, directory) = persist_fixture("net-read.json", &evidence)?;
        let candidate =
            validate_read_capability_candidate(&config, &instrument, &persisted, BASE_MS + 1_000)?;
        assert_eq!(candidate.scope().position_mode, OkxPositionMode::Net);
        assert!(
            candidate
                .candidate_flags()
                .contains(CapabilityFlags::READ_ACCOUNT)
        );
        assert!(
            candidate
                .candidate_flags()
                .contains(CapabilityFlags::READ_ORDERS)
        );
        assert!(
            candidate
                .candidate_flags()
                .contains(CapabilityFlags::READ_FILLS)
        );
        assert!(
            candidate
                .candidate_flags()
                .contains(CapabilityFlags::PRIVATE_STREAM)
        );
        assert!(!candidate.candidate_flags().contains(CapabilityFlags::TRADE));
        assert!(
            !candidate
                .candidate_flags()
                .contains(CapabilityFlags::HEDGE_POSITION)
        );
        let public_candidate =
            validate_capability_candidate(&config, &instrument, &persisted, BASE_MS + 1_000)?;
        assert_eq!(
            public_candidate.candidate_flags(),
            candidate.candidate_flags()
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn physical_candidate_prepares_live_place_cancel_and_reduce_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, mut evidence) = fixture(GatewayMode::Live)?;
        let position_page = evidence
            .private_pages
            .iter_mut()
            .find(|page| page.surface == crate::OkxPrivateSurface::Positions)
            .ok_or("missing position page")?;
        let mut positions: Value = serde_json::from_slice(&position_page.payload)?;
        positions["data"][0]["pos"] = Value::String("1".to_owned());
        positions["data"][0]["avgPx"] = Value::String("59000".to_owned());
        positions["data"][0]["markPx"] = Value::String("60000".to_owned());
        position_page.payload = serde_json::to_vec(&positions)?;
        position_page.payload_sha256 = crate::readback::payload_digest(&position_page.payload);
        let (persisted, directory) = persist_fixture("physical.json", &evidence)?;
        let candidate = crate::OkxPhysicalCandidate::from_probe_fixture(
            config.clone(),
            instrument.clone(),
            &persisted,
            BASE_MS + 1_000,
        )?;
        assert_eq!(candidate.binding().mode, GatewayMode::Live);
        assert!(
            candidate
                .prepare_place_once(
                    &ExecutionCommand::PlaceLimit(evidence.mutations.place.clone()),
                    BASE_MS + 1_000,
                )
                .is_ok()
        );
        assert!(
            candidate
                .prepare_place_once(
                    &ExecutionCommand::MarketReduce(evidence.mutations.reduce.clone()),
                    BASE_MS + 1_000,
                )
                .is_ok()
        );
        let place_request = build_place_request(
            &config,
            &instrument,
            &candidate.readback().profile,
            evidence.scope.trade_mode,
            OkxPlaceIntent::Limit(&evidence.mutations.place),
        )?;
        let accepted = parse_place_ack(evidence.mutations.place_ack.response()?, &place_request)?;
        assert!(
            candidate
                .prepare_cancel_once(&evidence.mutations.cancel, &accepted, BASE_MS + 1_000,)
                .is_ok()
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    async fn response_server(
        responses: Vec<Option<Vec<u8>>>,
    ) -> Result<
        (
            String,
            tokio::task::JoinHandle<Result<bool, std::io::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            for response in responses {
                let (mut stream, _) = listener.accept().await?;
                let mut request = [0_u8; 8_192];
                let _ = stream.read(&mut request).await?;
                if let Some(body) = response {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(headers.as_bytes()).await?;
                    stream.write_all(&body).await?;
                    stream.shutdown().await?;
                }
            }
            Ok(
                tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                    .await
                    .is_ok(),
            )
        });
        Ok((origin, server))
    }

    #[tokio::test]
    async fn physical_ack_and_disconnect_both_require_exact_readback_without_resubmission()
    -> Result<(), Box<dyn std::error::Error>> {
        for disconnect_first in [false, true] {
            let (config, instrument, evidence) = fixture(GatewayMode::Live)?;
            let (persisted, directory) = persist_fixture("dispatch.json", &evidence)?;
            let candidate = crate::OkxPhysicalCandidate::from_probe_fixture(
                config,
                instrument,
                &persisted,
                BASE_MS + 1_000,
            )?;
            let first = (!disconnect_first).then(|| evidence.mutations.place_ack.payload.clone());
            let (origin, server) = response_server(vec![
                first,
                Some(evidence.mutations.place_detail.payload.clone()),
            ])
            .await?;
            let session = candidate.into_session_with_origin(
                OkxCredentials::from_values("key", "secret", "passphrase")?,
                &origin,
                std::time::Duration::from_secs(1),
                16 * 1_024,
            )?;
            let mutation = session.candidate().prepare_place_once(
                &ExecutionCommand::PlaceLimit(evidence.mutations.place.clone()),
                BASE_MS + 1_000,
            )?;
            let crate::OkxDispatchOnceResult::PendingReadback(pending) = session
                .dispatch_once(mutation, "2026-08-30T00:00:00.000Z", BASE_MS + 1_000)
                .await?;
            let settled = session
                .readback_pending(pending, "2026-08-30T00:00:00.100Z")
                .await?;
            if disconnect_first {
                assert!(matches!(
                    settled,
                    crate::OkxPhysicalReadbackResult::ConfirmedUnknown(_)
                ));
            } else {
                assert!(matches!(
                    settled,
                    crate::OkxPhysicalReadbackResult::Confirmed(_)
                ));
            }
            assert!(!server.await??);
            fs::remove_dir_all(directory)?;
        }
        Ok(())
    }

    #[test]
    fn mode_generation_disconnect_scope_and_withdrawal_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (live, live_instrument, evidence) = fixture(GatewayMode::Live)?;
        let (persisted, directory) = persist_fixture("live.json", &evidence)?;
        let wrong = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "ETH/USDT".parse()?,
        )?)?;
        assert!(
            validate_capability_candidate(&wrong, &live_instrument, &persisted, BASE_MS + 1_000)
                .is_err()
        );
        fs::remove_dir_all(directory)?;

        let mut cross_generation = evidence.clone();
        cross_generation.private_stream.frames[0].private_generation += 1;
        let (persisted, directory) = persist_fixture("generation.json", &cross_generation)?;
        assert!(
            validate_capability_candidate(&live, &live_instrument, &persisted, BASE_MS + 1_000)
                .is_err()
        );
        fs::remove_dir_all(directory)?;

        let mut disconnected = evidence.clone();
        disconnected.private_stream.frames.clear();
        let (persisted, directory) = persist_fixture("disconnect.json", &disconnected)?;
        assert!(
            validate_capability_candidate(&live, &live_instrument, &persisted, BASE_MS + 1_000)
                .is_err()
        );
        fs::remove_dir_all(directory)?;

        let mut wrong_scope = evidence.clone();
        wrong_scope.private_stream.frames[0]
            .binding
            .trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        let (persisted, directory) = persist_fixture("scope.json", &wrong_scope)?;
        assert!(
            validate_capability_candidate(&live, &live_instrument, &persisted, BASE_MS + 1_000)
                .is_err()
        );
        fs::remove_dir_all(directory)?;

        let mut withdrawal = evidence;
        let account_page = withdrawal
            .private_pages
            .iter_mut()
            .find(|page| page.surface == crate::OkxPrivateSurface::AccountConfig)
            .ok_or("missing account page")?;
        let mut payload: Value = serde_json::from_slice(&account_page.payload)?;
        payload["data"][0]["perm"] = Value::String("read_only,trade,withdraw".to_owned());
        account_page.payload = serde_json::to_vec(&payload)?;
        account_page.payload_sha256 = crate::readback::payload_digest(&account_page.payload);
        let (persisted, directory) = persist_fixture("withdraw.json", &withdrawal)?;
        assert!(
            validate_capability_candidate(&live, &live_instrument, &persisted, BASE_MS + 1_000)
                .is_err()
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn stale_mutation_or_file_tampering_cannot_become_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, mut evidence) = fixture(GatewayMode::Live)?;
        evidence.mutations.reduce_detail.payload[0] ^= 1;
        let (persisted, directory) = persist_fixture("probe-payload.json", &evidence)?;
        assert!(
            validate_mutation_capability_fixture(&config, &instrument, &persisted, BASE_MS + 1_000)
                .is_err()
        );
        fs::remove_dir_all(directory)?;

        let (_, _, evidence) = fixture(GatewayMode::Live)?;
        let (path, directory) = test_path("probe-file.json")?;
        persist_capability_probe(&path, &evidence)?;
        let mut encoded = fs::read(&path)?;
        let index = encoded
            .len()
            .checked_div(2)
            .ok_or("invalid encoded evidence")?;
        encoded[index] ^= 1;
        fs::write(&path, encoded)?;
        assert_eq!(load_capability_probe(&path), Err(OkxError::Persistence));
        fs::remove_dir_all(directory)?;
        Ok(())
    }
}
