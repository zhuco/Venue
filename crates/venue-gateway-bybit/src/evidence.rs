use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_domain::domain::NativeOrderFamily;
use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding};

use crate::{
    BYBIT_LINEAR_ORDER_PROFILE_VERSION, BybitAccountReadback, BybitApiKeyEvidence,
    BybitCredentials, BybitError, BybitExecutionPage, BybitFillReadback, BybitGatewayBinding,
    BybitOpenOrderPage, BybitOpenOrdersReadback, BybitOrderEvidence, BybitOrderEvidencePage,
    BybitOrderHistoryReadback, BybitPositionPage, BybitPositionReadback, BybitPrivateSource,
    BybitRawPrivatePayload, complete_account_readback, complete_execution_pages,
    complete_open_order_pages, complete_order_history_pages, complete_position_pages,
    parse_api_key_evidence, parse_execution_page, parse_open_order_page, parse_order_history_page,
    parse_position_page,
};

pub const BYBIT_CAPABILITY_PROBE_SCHEMA_VERSION: u16 = 1;

const EXECUTABLE_FLAGS: CapabilityFlags = CapabilityFlags::from_bits_retain(
    CapabilityFlags::READ_ACCOUNT.bits()
        | CapabilityFlags::READ_ORDERS.bits()
        | CapabilityFlags::READ_FILLS.bits()
        | CapabilityFlags::PRIVATE_STREAM.bits()
        | CapabilityFlags::TRADE.bits()
        | CapabilityFlags::PLACE_LIMIT.bits()
        | CapabilityFlags::PLACE_MARKET.bits()
        | CapabilityFlags::CANCEL.bits()
        | CapabilityFlags::HEDGE_POSITION.bits(),
);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BybitOrderFamilyScope {
    pub binding: GatewayBinding,
    pub profile_version: u64,
    pub attempt_id: u64,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitUnsupportedOrderFamilyEvidence {
    pub binding: GatewayBinding,
    pub family: NativeOrderFamily,
    pub profile_version: u64,
}

impl BybitUnsupportedOrderFamilyEvidence {
    #[must_use]
    pub fn algo(binding: GatewayBinding, profile_version: u64) -> Self {
        Self {
            binding,
            family: NativeOrderFamily::UmAlgo,
            profile_version,
        }
    }

    #[must_use]
    pub const fn reason(&self) -> &'static str {
        "Bybit V5 linear exposes signed Order and StopOrder namespaces but no distinct algo namespace or admitted algo mutation surface"
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitCompleteOrderFamilyEvidence {
    pub open_orders: BybitOpenOrdersReadback,
    pub order_history: BybitOrderHistoryReadback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BybitOrderFamilyEvidence {
    Complete(Box<BybitCompleteOrderFamilyEvidence>),
    Unsupported(BybitUnsupportedOrderFamilyEvidence),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderFamilyCandidate {
    scope: BybitOrderFamilyScope,
    regular: BybitCompleteOrderFamilyEvidence,
    conditional: BybitCompleteOrderFamilyEvidence,
    algo: BybitUnsupportedOrderFamilyEvidence,
    raw_payload_digest: [u8; 32],
}

impl BybitOrderFamilyCandidate {
    #[must_use]
    pub const fn scope(&self) -> &BybitOrderFamilyScope {
        &self.scope
    }

    #[must_use]
    pub const fn regular(&self) -> &BybitCompleteOrderFamilyEvidence {
        &self.regular
    }

    #[must_use]
    pub const fn conditional(&self) -> &BybitCompleteOrderFamilyEvidence {
        &self.conditional
    }

    #[must_use]
    pub const fn algo(&self) -> &BybitUnsupportedOrderFamilyEvidence {
        &self.algo
    }

    #[must_use]
    pub const fn raw_payload_digest(&self) -> [u8; 32] {
        self.raw_payload_digest
    }

    fn order_details(&self) -> Result<Vec<BybitOrderEvidence>, BybitError> {
        let mut native_ids = BTreeSet::new();
        self.regular
            .order_history
            .orders
            .iter()
            .chain(self.conditional.order_history.orders.iter())
            .map(|order| {
                if !native_ids.insert(order.order.order_id.clone()) {
                    return Err(BybitError::OrderFamily);
                }
                Ok(order.clone())
            })
            .collect()
    }
}

pub fn validate_order_family_candidate<I>(
    scope: BybitOrderFamilyScope,
    validated_at_ms: u64,
    evidence: I,
) -> Result<BybitOrderFamilyCandidate, BybitError>
where
    I: IntoIterator<Item = BybitOrderFamilyEvidence>,
{
    let binding = BybitGatewayBinding::new(scope.binding.clone())?;
    if scope.profile_version != BYBIT_LINEAR_ORDER_PROFILE_VERSION
        || scope.attempt_id == 0
        || scope.generation == 0
        || scope.observed_at_ms == 0
        || scope.expires_at_ms <= scope.observed_at_ms
        || validated_at_ms < scope.observed_at_ms
        || validated_at_ms >= scope.expires_at_ms
    {
        return Err(BybitError::Capability);
    }
    let mut regular = None;
    let mut conditional = None;
    let mut algo = None;
    for item in evidence {
        match item {
            BybitOrderFamilyEvidence::Complete(value) => {
                let value = *value;
                let family = value.open_orders.family;
                if value.order_history.family != family {
                    return Err(BybitError::OrderFamily);
                }
                let slot = match family {
                    NativeOrderFamily::UmOrder => &mut regular,
                    NativeOrderFamily::UmConditional => &mut conditional,
                    NativeOrderFamily::UmAlgo => return Err(BybitError::OrderFamily),
                };
                if slot.replace(value).is_some() {
                    return Err(BybitError::OrderFamily);
                }
            }
            BybitOrderFamilyEvidence::Unsupported(value) => {
                if value.family != NativeOrderFamily::UmAlgo
                    || value.binding != scope.binding
                    || value.profile_version != scope.profile_version
                    || algo.replace(value).is_some()
                {
                    return Err(BybitError::OrderFamily);
                }
            }
        }
    }
    let regular = regular.ok_or(BybitError::OrderFamily)?;
    let conditional = conditional.ok_or(BybitError::OrderFamily)?;
    let algo = algo.ok_or(BybitError::OrderFamily)?;
    validate_complete_family(&binding, &scope, NativeOrderFamily::UmOrder, &regular)?;
    validate_complete_family(
        &binding,
        &scope,
        NativeOrderFamily::UmConditional,
        &conditional,
    )?;
    let regular_window = history_window(&regular.order_history)?;
    let conditional_window = history_window(&conditional.order_history)?;
    if regular_window != conditional_window {
        return Err(BybitError::Clock);
    }
    let raw_payload_digest = digest_raw_pages(
        regular
            .open_orders
            .raw_pages
            .iter()
            .chain(regular.order_history.raw_pages.iter())
            .chain(conditional.open_orders.raw_pages.iter())
            .chain(conditional.order_history.raw_pages.iter()),
    );
    Ok(BybitOrderFamilyCandidate {
        scope,
        regular,
        conditional,
        algo,
        raw_payload_digest,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitCapabilityCandidate {
    pub scope: BybitOrderFamilyScope,
    pub api_key: BybitApiKeyEvidence,
    pub account: BybitAccountReadback,
    pub positions: BybitPositionReadback,
    pub order_families: BybitOrderFamilyCandidate,
    pub fills: BybitFillReadback,
    pub candidate_flags: CapabilityFlags,
}

/// Secret-free proof emitted only after private WebSocket authentication and subscription have
/// completed for the exact binding and generation. The connection id is hashed before persistence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BybitPrivateStreamProbeEvidence {
    binding: GatewayBinding,
    connection_generation: u64,
    private_generation: u64,
    authenticated_at_ms: u64,
    observed_at_ms: u64,
    expires_at_ms: u64,
    connection_id_sha256: String,
}

impl BybitPrivateStreamProbeEvidence {
    pub(crate) fn authenticated(
        binding: GatewayBinding,
        generation: u64,
        authenticated_at_ms: u64,
        observed_at_ms: u64,
        expires_at_ms: u64,
        connection_id: &str,
    ) -> Result<Self, BybitError> {
        if generation == 0
            || authenticated_at_ms == 0
            || observed_at_ms < authenticated_at_ms
            || expires_at_ms <= observed_at_ms
            || connection_id.is_empty()
        {
            return Err(BybitError::Capability);
        }
        BybitGatewayBinding::new(binding.clone())?;
        Ok(Self {
            binding,
            connection_generation: generation,
            private_generation: generation,
            authenticated_at_ms,
            observed_at_ms,
            expires_at_ms,
            connection_id_sha256: sha256_hex(connection_id.as_bytes()),
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.private_generation
    }

    fn validate(
        &self,
        binding: &GatewayBinding,
        generation: u64,
        now_ms: u64,
    ) -> Result<(), BybitError> {
        if &self.binding != binding
            || self.connection_generation != generation
            || self.private_generation != generation
            || self.authenticated_at_ms == 0
            || self.observed_at_ms < self.authenticated_at_ms
            || self.expires_at_ms <= self.observed_at_ms
            || now_ms < self.observed_at_ms
            || now_ms >= self.expires_at_ms
            || !is_sha256(&self.connection_id_sha256)
        {
            return Err(BybitError::Capability);
        }
        Ok(())
    }
}

/// Durable, secret-free probe artifact. Raw signed-read response bodies are retained so loading the
/// artifact replays account, both Hedge legs, all three canonical order families, order details,
/// and fills instead of trusting a serialized flag set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BybitCapabilityProbeEvidence {
    schema_version: u16,
    scope: BybitOrderFamilyScope,
    private_stream: BybitPrivateStreamProbeEvidence,
    raw_payloads: Vec<BybitRawPrivatePayload>,
    flags: CapabilityFlags,
    evidence_sha256: String,
}

impl BybitCapabilityProbeEvidence {
    #[must_use]
    pub const fn scope(&self) -> &BybitOrderFamilyScope {
        &self.scope
    }

    #[must_use]
    pub fn evidence_sha256(&self) -> &str {
        &self.evidence_sha256
    }

    pub fn to_json(&self) -> Result<Vec<u8>, BybitError> {
        serde_json::to_vec(self).map_err(|_| BybitError::Capability)
    }

    pub fn from_json_verified(
        payload: &[u8],
        expected_binding: &BybitGatewayBinding,
        credentials: &BybitCredentials,
        now_ms: u64,
    ) -> Result<(Self, CapabilitySnapshot), BybitError> {
        let evidence: Self = serde_json::from_slice(payload).map_err(|_| BybitError::Capability)?;
        let snapshot = evidence.verify(expected_binding, credentials, now_ms)?;
        Ok((evidence, snapshot))
    }

    pub fn verify(
        &self,
        expected_binding: &BybitGatewayBinding,
        credentials: &BybitCredentials,
        now_ms: u64,
    ) -> Result<CapabilitySnapshot, BybitError> {
        self.verify_candidate(expected_binding, credentials, now_ms)
            .map(|(snapshot, _)| snapshot)
    }

    pub(crate) fn verify_candidate(
        &self,
        expected_binding: &BybitGatewayBinding,
        credentials: &BybitCredentials,
        now_ms: u64,
    ) -> Result<(CapabilitySnapshot, BybitCapabilityCandidate), BybitError> {
        expected_binding.validate_request_binding(&self.scope.binding)?;
        if self.schema_version != BYBIT_CAPABILITY_PROBE_SCHEMA_VERSION
            || self.scope.profile_version != BYBIT_LINEAR_ORDER_PROFILE_VERSION
            || self.scope.attempt_id == 0
            || self.scope.generation == 0
            || self.flags != EXECUTABLE_FLAGS
            || self.evidence_sha256 != self.compute_digest()?
        {
            return Err(BybitError::Capability);
        }
        self.private_stream
            .validate(&self.scope.binding, self.scope.generation, now_ms)?;
        let candidate = replay_capability_candidate(
            expected_binding,
            credentials,
            self.scope.clone(),
            now_ms,
            &self.raw_payloads,
        )?;
        if candidate
            .candidate_flags
            .contains(CapabilityFlags::WITHDRAW)
            || !candidate
                .candidate_flags
                .contains(EXECUTABLE_FLAGS - CapabilityFlags::PRIVATE_STREAM)
        {
            return Err(BybitError::Capability);
        }
        let observed_ms = self
            .scope
            .observed_at_ms
            .max(self.private_stream.observed_at_ms);
        let expires_ms = self
            .scope
            .expires_at_ms
            .min(self.private_stream.expires_at_ms);
        if observed_ms == 0
            || expires_ms <= observed_ms
            || now_ms < observed_ms
            || now_ms >= expires_ms
        {
            return Err(BybitError::Capability);
        }
        Ok((
            CapabilitySnapshot {
                binding: self.scope.binding.clone(),
                version: self.scope.generation,
                observed_ms,
                expires_ms,
                flags: self.flags,
            },
            candidate,
        ))
    }

    fn compute_digest(&self) -> Result<String, BybitError> {
        #[derive(Serialize)]
        struct Unsigned<'a> {
            schema_version: u16,
            scope: &'a BybitOrderFamilyScope,
            private_stream: &'a BybitPrivateStreamProbeEvidence,
            raw_payloads: &'a [BybitRawPrivatePayload],
            flags: CapabilityFlags,
        }
        let bytes = serde_json::to_vec(&Unsigned {
            schema_version: self.schema_version,
            scope: &self.scope,
            private_stream: &self.private_stream,
            raw_payloads: &self.raw_payloads,
            flags: self.flags,
        })
        .map_err(|_| BybitError::Capability)?;
        Ok(sha256_hex(&bytes))
    }
}

pub fn finalize_capability_probe(
    candidate: BybitCapabilityCandidate,
    private_stream: BybitPrivateStreamProbeEvidence,
    credentials: &BybitCredentials,
    validated_at_ms: u64,
) -> Result<(BybitCapabilityProbeEvidence, CapabilitySnapshot), BybitError> {
    let raw_payloads = collect_probe_payloads(&candidate);
    let mut evidence = BybitCapabilityProbeEvidence {
        schema_version: BYBIT_CAPABILITY_PROBE_SCHEMA_VERSION,
        scope: candidate.scope,
        private_stream,
        raw_payloads,
        flags: EXECUTABLE_FLAGS,
        evidence_sha256: String::new(),
    };
    evidence.evidence_sha256 = evidence.compute_digest()?;
    let binding = BybitGatewayBinding::new(evidence.scope.binding.clone())?;
    let snapshot = evidence.verify(&binding, credentials, validated_at_ms)?;
    Ok((evidence, snapshot))
}

#[allow(clippy::too_many_arguments)]
pub fn validate_capability_candidate(
    scope: BybitOrderFamilyScope,
    validated_at_ms: u64,
    api_key: BybitApiKeyEvidence,
    account: BybitAccountReadback,
    positions: BybitPositionReadback,
    order_families: BybitOrderFamilyCandidate,
    fills: BybitFillReadback,
) -> Result<BybitCapabilityCandidate, BybitError> {
    let binding = BybitGatewayBinding::new(scope.binding.clone())?;
    let observed_at_ms = [
        api_key.observed_at_ms,
        account.observed_at_ms,
        positions.observed_at_ms,
        order_families.scope.observed_at_ms,
        fills.observed_at_ms,
    ]
    .into_iter()
    .max()
    .ok_or(BybitError::Capability)?;
    if order_families.scope != scope
        || api_key.raw.binding != scope.binding
        || api_key.binding != scope.binding
        || account.identity.binding != scope.binding
        || positions.binding != scope.binding
        || fills.binding != scope.binding
        || api_key.attempt_id != scope.attempt_id
        || account.attempt_id != scope.attempt_id
        || positions.attempt_id != scope.attempt_id
        || fills.attempt_id != scope.attempt_id
        || api_key.generation != scope.generation
        || api_key.raw.generation != scope.generation
        || account.identity.generation != scope.generation
        || positions.generation != scope.generation
        || fills.generation != scope.generation
        || observed_at_ms != scope.observed_at_ms
        || validated_at_ms < scope.observed_at_ms
        || validated_at_ms >= scope.expires_at_ms
        || api_key.withdraw
        || !api_key.contract_order
        || !api_key.contract_position
        || !positions.hedge_mode
    {
        return Err(BybitError::Capability);
    }
    let order_details = order_families.order_details()?;
    let replayed_fills = replay_fills(&binding, &fills, &order_details)?;
    if replayed_fills != fills {
        return Err(BybitError::Projection);
    }
    let mut candidate_flags = CapabilityFlags::READ_ACCOUNT
        | CapabilityFlags::READ_ORDERS
        | CapabilityFlags::READ_FILLS
        | CapabilityFlags::HEDGE_POSITION;
    if !api_key.read_only && api_key.derivatives_trade {
        candidate_flags |= CapabilityFlags::TRADE
            | CapabilityFlags::PLACE_LIMIT
            | CapabilityFlags::PLACE_MARKET
            | CapabilityFlags::CANCEL
            | CapabilityFlags::AMEND;
    }
    Ok(BybitCapabilityCandidate {
        scope,
        api_key,
        account,
        positions,
        order_families,
        fills,
        candidate_flags,
    })
}

fn collect_probe_payloads(candidate: &BybitCapabilityCandidate) -> Vec<BybitRawPrivatePayload> {
    let mut payloads = vec![candidate.api_key.raw.clone()];
    payloads.extend(candidate.account.raw_payloads.iter().cloned());
    payloads.extend(candidate.positions.raw_pages.iter().cloned());
    payloads.extend(
        candidate
            .order_families
            .regular
            .open_orders
            .raw_pages
            .iter()
            .cloned(),
    );
    payloads.extend(
        candidate
            .order_families
            .regular
            .order_history
            .raw_pages
            .iter()
            .cloned(),
    );
    payloads.extend(
        candidate
            .order_families
            .conditional
            .open_orders
            .raw_pages
            .iter()
            .cloned(),
    );
    payloads.extend(
        candidate
            .order_families
            .conditional
            .order_history
            .raw_pages
            .iter()
            .cloned(),
    );
    payloads.extend(candidate.fills.raw_pages.iter().cloned());
    payloads
}

fn replay_capability_candidate(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    scope: BybitOrderFamilyScope,
    validated_at_ms: u64,
    payloads: &[BybitRawPrivatePayload],
) -> Result<BybitCapabilityCandidate, BybitError> {
    if payloads.is_empty()
        || payloads.iter().any(|raw| {
            raw.binding != scope.binding
                || raw.generation != scope.generation
                || raw.attempt_id != scope.attempt_id
        })
    {
        return Err(BybitError::Capability);
    }
    let api_raw = exact_payload(payloads, BybitPrivateSource::ApiKeyInfo)?;
    let account_raw = exact_payload(payloads, BybitPrivateSource::AccountInfo)?;
    let wallet_raw = exact_payload(payloads, BybitPrivateSource::WalletBalance)?;
    let position_raws = matching_payloads(payloads, BybitPrivateSource::Positions);
    let regular_open_raws = matching_payloads(
        payloads,
        BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
    );
    let regular_history_raws = matching_payloads(
        payloads,
        BybitPrivateSource::OrderHistory(NativeOrderFamily::UmOrder),
    );
    let conditional_open_raws = matching_payloads(
        payloads,
        BybitPrivateSource::OpenOrders(NativeOrderFamily::UmConditional),
    );
    let conditional_history_raws = matching_payloads(
        payloads,
        BybitPrivateSource::OrderHistory(NativeOrderFamily::UmConditional),
    );
    let execution_raws = matching_payloads(payloads, BybitPrivateSource::Executions);
    let accounted = 3_usize
        .checked_add(position_raws.len())
        .and_then(|count| count.checked_add(regular_open_raws.len()))
        .and_then(|count| count.checked_add(regular_history_raws.len()))
        .and_then(|count| count.checked_add(conditional_open_raws.len()))
        .and_then(|count| count.checked_add(conditional_history_raws.len()))
        .and_then(|count| count.checked_add(execution_raws.len()))
        .ok_or(BybitError::Capability)?;
    if accounted != payloads.len() {
        return Err(BybitError::Capability);
    }

    let api_key = parse_api_key_evidence(binding, credentials, api_raw)?;
    let account = complete_account_readback(binding, account_raw.clone(), wallet_raw.clone())?;
    let position_pages = position_raws
        .iter()
        .map(|raw| parse_position_page(binding, raw))
        .collect::<Result<Vec<BybitPositionPage>, _>>()?;
    let positions = complete_position_pages(binding, &position_pages)?;
    let regular_open = parse_open_pages(binding, &regular_open_raws)?;
    let regular_history = parse_history_pages(binding, &regular_history_raws)?;
    let conditional_open = parse_open_pages(binding, &conditional_open_raws)?;
    let conditional_history = parse_history_pages(binding, &conditional_history_raws)?;
    let families = validate_order_family_candidate(
        scope.clone(),
        validated_at_ms,
        [
            BybitOrderFamilyEvidence::Complete(Box::new(BybitCompleteOrderFamilyEvidence {
                open_orders: complete_open_order_pages(
                    binding,
                    NativeOrderFamily::UmOrder,
                    &regular_open,
                )?,
                order_history: complete_order_history_pages(
                    binding,
                    NativeOrderFamily::UmOrder,
                    &regular_history,
                )?,
            })),
            BybitOrderFamilyEvidence::Complete(Box::new(BybitCompleteOrderFamilyEvidence {
                open_orders: complete_open_order_pages(
                    binding,
                    NativeOrderFamily::UmConditional,
                    &conditional_open,
                )?,
                order_history: complete_order_history_pages(
                    binding,
                    NativeOrderFamily::UmConditional,
                    &conditional_history,
                )?,
            })),
            BybitOrderFamilyEvidence::Unsupported(BybitUnsupportedOrderFamilyEvidence::algo(
                scope.binding.clone(),
                scope.profile_version,
            )),
        ],
    )?;
    let order_details = families.order_details()?;
    let execution_pages = execution_raws
        .iter()
        .map(|raw| parse_execution_page(binding, raw, &order_details))
        .collect::<Result<Vec<BybitExecutionPage>, _>>()?;
    let fills = complete_execution_pages(binding, &execution_pages, &order_details)?;
    validate_capability_candidate(
        scope,
        validated_at_ms,
        api_key,
        account,
        positions,
        families,
        fills,
    )
}

fn exact_payload(
    payloads: &[BybitRawPrivatePayload],
    source: BybitPrivateSource,
) -> Result<&BybitRawPrivatePayload, BybitError> {
    let mut matches = payloads.iter().filter(|raw| raw.source == source);
    let value = matches.next().ok_or(BybitError::Capability)?;
    if matches.next().is_some() {
        return Err(BybitError::Capability);
    }
    Ok(value)
}

fn matching_payloads(
    payloads: &[BybitRawPrivatePayload],
    source: BybitPrivateSource,
) -> Vec<BybitRawPrivatePayload> {
    payloads
        .iter()
        .filter(|raw| raw.source == source)
        .cloned()
        .collect()
}

fn parse_open_pages(
    binding: &BybitGatewayBinding,
    raws: &[BybitRawPrivatePayload],
) -> Result<Vec<BybitOpenOrderPage>, BybitError> {
    raws.iter()
        .map(|raw| parse_open_order_page(binding, raw))
        .collect()
}

fn parse_history_pages(
    binding: &BybitGatewayBinding,
    raws: &[BybitRawPrivatePayload],
) -> Result<Vec<BybitOrderEvidencePage>, BybitError> {
    raws.iter()
        .map(|raw| parse_order_history_page(binding, raw))
        .collect()
}

fn validate_complete_family(
    binding: &BybitGatewayBinding,
    scope: &BybitOrderFamilyScope,
    family: NativeOrderFamily,
    evidence: &BybitCompleteOrderFamilyEvidence,
) -> Result<(), BybitError> {
    for (binding_value, generation, attempt_id, observed_at_ms) in [
        (
            &evidence.open_orders.binding,
            evidence.open_orders.generation,
            evidence.open_orders.attempt_id,
            evidence.open_orders.observed_at_ms,
        ),
        (
            &evidence.order_history.binding,
            evidence.order_history.generation,
            evidence.order_history.attempt_id,
            evidence.order_history.observed_at_ms,
        ),
    ] {
        if binding_value != &scope.binding
            || generation != scope.generation
            || attempt_id != scope.attempt_id
            || observed_at_ms > scope.observed_at_ms
        {
            return Err(BybitError::Binding);
        }
    }
    if evidence.open_orders.family != family || evidence.order_history.family != family {
        return Err(BybitError::OrderFamily);
    }
    let open_pages = evidence
        .open_orders
        .raw_pages
        .iter()
        .map(|raw| parse_open_order_page(binding, raw))
        .collect::<Result<Vec<BybitOpenOrderPage>, _>>()?;
    let history_pages = evidence
        .order_history
        .raw_pages
        .iter()
        .map(|raw| parse_order_history_page(binding, raw))
        .collect::<Result<Vec<BybitOrderEvidencePage>, _>>()?;
    if complete_open_order_pages(binding, family, &open_pages)? != evidence.open_orders
        || complete_order_history_pages(binding, family, &history_pages)? != evidence.order_history
    {
        return Err(BybitError::Projection);
    }
    Ok(())
}

fn replay_fills(
    binding: &BybitGatewayBinding,
    fills: &BybitFillReadback,
    order_details: &[BybitOrderEvidence],
) -> Result<BybitFillReadback, BybitError> {
    let pages = fills
        .raw_pages
        .iter()
        .map(|raw| parse_execution_page(binding, raw, order_details))
        .collect::<Result<Vec<BybitExecutionPage>, _>>()?;
    complete_execution_pages(binding, &pages, order_details)
}

fn history_window(
    history: &BybitOrderHistoryReadback,
) -> Result<Option<crate::BybitHistoryWindow>, BybitError> {
    let first = history.raw_pages.first().ok_or(BybitError::Pagination)?;
    Ok(first.history_window.clone())
}

fn digest_raw_pages<'a>(pages: impl Iterator<Item = &'a BybitRawPrivatePayload>) -> [u8; 32] {
    let mut digest = Sha256::new();
    for page in pages {
        digest.update(
            u64::try_from(page.payload.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        digest.update(&page.payload);
    }
    digest.finalize().into()
}

fn sha256_hex(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
