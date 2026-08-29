use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use venue_domain::domain::NativeOrderFamily;
use venue_gateway_api::{CapabilityFlags, GatewayBinding};

use crate::{
    BYBIT_LINEAR_ORDER_PROFILE_VERSION, BybitAccountReadback, BybitApiKeyEvidence, BybitError,
    BybitExecutionPage, BybitFillReadback, BybitGatewayBinding, BybitOpenOrderPage,
    BybitOpenOrdersReadback, BybitOrderEvidence, BybitOrderEvidencePage, BybitOrderHistoryReadback,
    BybitPositionReadback, BybitRawPrivatePayload, complete_execution_pages,
    complete_open_order_pages, complete_order_history_pages, parse_execution_page,
    parse_open_order_page, parse_order_history_page,
};

#[derive(Clone, Debug, Eq, PartialEq)]
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
        || api_key.binding != scope.binding
        || account.identity.binding != scope.binding
        || positions.binding != scope.binding
        || fills.binding != scope.binding
        || api_key.attempt_id != scope.attempt_id
        || account.attempt_id != scope.attempt_id
        || positions.attempt_id != scope.attempt_id
        || fills.attempt_id != scope.attempt_id
        || api_key.generation != scope.generation
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
