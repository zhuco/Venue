//! Request-bound OKX private REST evidence and closed readback collection.
//!
//! This module performs no network I/O and grants no mutation authority. Each response is bound to
//! the exact signed request that produced it; a complete candidate is returned only after every
//! account face, canonical order family, and fill-history cursor chain has closed in one attempt.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    FieldState, NativeOrderFamily, Order, OrderPurpose, OrderSide, OrderState, Position,
    PositionSide,
};
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::models::{Envelope, FillRow, OrderRow, PositionRow};
use crate::private::{
    OkxAccountProfile, OkxTimedBalance, OkxTimedOrder, normalize_fill, normalize_order_row,
    normalize_position_row, order_side, position_side,
};
use crate::public::{decode_success, positive_decimal, positive_u64};
use crate::{
    OkxConfig, OkxCredentials, OkxError, OkxFill, OkxHttpResponse, OkxInstrument, OkxPositionMode,
    OkxTradeMode, SignedHeaders, endpoints, sign,
};

pub const OKX_PRIVATE_READBACK_SCHEMA_VERSION: u16 = 1;
pub const OKX_PRIVATE_PAGE_LIMIT: u16 = 100;
pub const OKX_PRIVATE_MAX_PAGES: usize = 1_000;

const GET: &str = "GET";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OkxAlgoOrderKind {
    ConditionalOco,
    Trigger,
    MoveOrderStop,
    Chase,
    Iceberg,
    Twap,
    SmartIceberg,
}

impl OkxAlgoOrderKind {
    const ALL: [Self; 7] = [
        Self::ConditionalOco,
        Self::Trigger,
        Self::MoveOrderStop,
        Self::Chase,
        Self::Iceberg,
        Self::Twap,
        Self::SmartIceberg,
    ];

    const fn query_value(self) -> &'static str {
        match self {
            Self::ConditionalOco => "conditional,oco",
            Self::Trigger => "trigger",
            Self::MoveOrderStop => "move_order_stop",
            Self::Chase => "chase",
            Self::Iceberg => "iceberg",
            Self::Twap => "twap",
            Self::SmartIceberg => "smart_iceberg",
        }
    }

    const fn family(self) -> NativeOrderFamily {
        match self {
            Self::ConditionalOco => NativeOrderFamily::UmConditional,
            Self::Trigger
            | Self::MoveOrderStop
            | Self::Chase
            | Self::Iceberg
            | Self::Twap
            | Self::SmartIceberg => NativeOrderFamily::UmAlgo,
        }
    }

    fn accepts(self, order_type: &str) -> bool {
        match self {
            Self::ConditionalOco => matches!(order_type, "conditional" | "oco"),
            _ => order_type == self.query_value(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "algo_kind")]
pub enum OkxPrivateSurface {
    AccountConfig,
    Balance,
    Positions,
    RegularOrders,
    AlgoOrders(OkxAlgoOrderKind),
    Fills,
}

impl OkxPrivateSurface {
    const fn paginated(self) -> bool {
        matches!(
            self,
            Self::RegularOrders | Self::AlgoOrders(_) | Self::Fills
        )
    }
}

/// Immutable identity for one complete authenticated readback attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxPrivateReadScope {
    binding: GatewayBinding,
    native_instrument_id: String,
    instrument_generation: u64,
    expected_position_mode: OkxPositionMode,
    trade_mode: OkxTradeMode,
    attempt_id: u64,
}

impl OkxPrivateReadScope {
    pub fn new(
        config: &OkxConfig,
        instrument: &OkxInstrument,
        expected_position_mode: OkxPositionMode,
        trade_mode: OkxTradeMode,
        attempt_id: u64,
    ) -> Result<Self, OkxError> {
        instrument.validate_scope(config)?;
        if attempt_id == 0 {
            return Err(OkxError::Identity);
        }
        Ok(Self {
            binding: config.gateway_binding().clone(),
            native_instrument_id: instrument.native_id().to_owned(),
            instrument_generation: instrument.instrument().generation,
            expected_position_mode,
            trade_mode,
            attempt_id,
        })
    }

    pub(crate) fn validate_instrument(&self, instrument: &OkxInstrument) -> Result<(), OkxError> {
        if self.binding.venue != VenueId::Okx
            || self.binding.symbol != instrument.instrument().symbol
            || self.native_instrument_id != instrument.native_id()
            || self.instrument_generation != instrument.instrument().generation
            || self.instrument_generation == 0
            || self.attempt_id == 0
        {
            return Err(OkxError::Binding);
        }
        Ok(())
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.binding
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
    pub const fn expected_position_mode(&self) -> OkxPositionMode {
        self.expected_position_mode
    }

    #[must_use]
    pub const fn trade_mode(&self) -> OkxTradeMode {
        self.trade_mode
    }

    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPrivateReadRequest {
    scope: OkxPrivateReadScope,
    surface: OkxPrivateSurface,
    page_index: u32,
    request_after: Option<String>,
    request_path: String,
}

impl OkxPrivateReadRequest {
    fn new(
        scope: &OkxPrivateReadScope,
        surface: OkxPrivateSurface,
        page_index: u32,
        request_after: Option<&str>,
    ) -> Result<Self, OkxError> {
        validate_page_request(surface, page_index, request_after)?;
        let request_path = request_path(scope, surface, request_after)?;
        Ok(Self {
            scope: scope.clone(),
            surface,
            page_index,
            request_after: request_after.map(str::to_owned),
            request_path,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &OkxPrivateReadScope {
        &self.scope
    }

    #[must_use]
    pub const fn surface(&self) -> OkxPrivateSurface {
        self.surface
    }

    #[must_use]
    pub const fn page_index(&self) -> u32 {
        self.page_index
    }

    #[must_use]
    pub fn request_after(&self) -> Option<&str> {
        self.request_after.as_deref()
    }

    #[must_use]
    pub fn request_path(&self) -> &str {
        &self.request_path
    }

    #[must_use]
    pub const fn method(&self) -> &'static str {
        GET
    }

    pub fn signed_headers(
        &self,
        credentials: &OkxCredentials,
        config: &OkxConfig,
        timestamp: &str,
    ) -> Result<SignedHeaders, OkxError> {
        if self.scope.gateway_binding() != config.gateway_binding() {
            return Err(OkxError::Binding);
        }
        sign(
            credentials,
            config,
            timestamp,
            self.method(),
            self.request_path(),
            &[],
        )
    }
}

pub fn build_account_config_request(
    scope: &OkxPrivateReadScope,
) -> Result<OkxPrivateReadRequest, OkxError> {
    OkxPrivateReadRequest::new(scope, OkxPrivateSurface::AccountConfig, 0, None)
}

pub fn build_balance_request(
    scope: &OkxPrivateReadScope,
) -> Result<OkxPrivateReadRequest, OkxError> {
    OkxPrivateReadRequest::new(scope, OkxPrivateSurface::Balance, 0, None)
}

pub fn build_positions_request(
    scope: &OkxPrivateReadScope,
) -> Result<OkxPrivateReadRequest, OkxError> {
    OkxPrivateReadRequest::new(scope, OkxPrivateSurface::Positions, 0, None)
}

pub fn build_regular_orders_request(
    scope: &OkxPrivateReadScope,
    page_index: u32,
    after: Option<&str>,
) -> Result<OkxPrivateReadRequest, OkxError> {
    OkxPrivateReadRequest::new(scope, OkxPrivateSurface::RegularOrders, page_index, after)
}

pub fn build_algo_orders_request(
    scope: &OkxPrivateReadScope,
    kind: OkxAlgoOrderKind,
    page_index: u32,
    after: Option<&str>,
) -> Result<OkxPrivateReadRequest, OkxError> {
    OkxPrivateReadRequest::new(
        scope,
        OkxPrivateSurface::AlgoOrders(kind),
        page_index,
        after,
    )
}

pub fn build_fills_request(
    scope: &OkxPrivateReadScope,
    page_index: u32,
    after: Option<&str>,
) -> Result<OkxPrivateReadRequest, OkxError> {
    OkxPrivateReadRequest::new(scope, OkxPrivateSurface::Fills, page_index, after)
}

fn request_path(
    scope: &OkxPrivateReadScope,
    surface: OkxPrivateSurface,
    after: Option<&str>,
) -> Result<String, OkxError> {
    let native = scope.native_instrument_id();
    let path = match surface {
        OkxPrivateSurface::AccountConfig => endpoints::ACCOUNT_CONFIG.to_owned(),
        OkxPrivateSurface::Balance => format!(
            "{}?ccy={}",
            endpoints::BALANCES,
            scope.gateway_binding().symbol.quote()
        ),
        OkxPrivateSurface::Positions => {
            format!("{}?instType=SWAP&instId={native}", endpoints::POSITIONS)
        }
        OkxPrivateSurface::RegularOrders => paged_path(
            endpoints::OPEN_ORDERS,
            &format!("instType=SWAP&instId={native}"),
            after,
        )?,
        OkxPrivateSurface::AlgoOrders(kind) => paged_path(
            endpoints::OPEN_ALGO_ORDERS,
            &format!(
                "ordType={}&instType=SWAP&instId={native}",
                kind.query_value()
            ),
            after,
        )?,
        OkxPrivateSurface::Fills => paged_path(
            endpoints::FILLS_HISTORY,
            &format!("instType=SWAP&instId={native}"),
            after,
        )?,
    };
    Ok(path)
}

fn paged_path(path: &str, query: &str, after: Option<&str>) -> Result<String, OkxError> {
    let mut value = format!("{path}?{query}");
    if let Some(after) = after {
        validate_numeric_id(after)?;
        value.push_str("&after=");
        value.push_str(after);
    }
    value.push_str("&limit=100");
    Ok(value)
}

fn validate_page_request(
    surface: OkxPrivateSurface,
    page_index: u32,
    request_after: Option<&str>,
) -> Result<(), OkxError> {
    if surface.paginated() {
        if (page_index == 0) != request_after.is_none() {
            return Err(OkxError::Pagination);
        }
        if let Some(after) = request_after {
            validate_numeric_id(after)?;
        }
    } else if page_index != 0 || request_after.is_some() {
        return Err(OkxError::Pagination);
    }
    Ok(())
}

/// Exact raw response plus the signed request metadata needed to replay it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OkxRawPrivatePage {
    pub parser_schema_version: u16,
    pub scope: OkxPrivateReadScope,
    pub surface: OkxPrivateSurface,
    pub page_index: u32,
    pub request_after: Option<String>,
    pub request_path: String,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: Vec<u8>,
}

impl OkxRawPrivatePage {
    pub fn new(
        request: &OkxPrivateReadRequest,
        received_at_ms: u64,
        payload: Vec<u8>,
    ) -> Result<Self, OkxError> {
        let page = Self {
            parser_schema_version: OKX_PRIVATE_READBACK_SCHEMA_VERSION,
            scope: request.scope.clone(),
            surface: request.surface,
            page_index: request.page_index,
            request_after: request.request_after.clone(),
            request_path: request.request_path.clone(),
            received_at_ms,
            payload_sha256: payload_digest(&payload),
            payload,
        };
        page.validate()?;
        Ok(page)
    }

    pub fn from_http_response(
        request: &OkxPrivateReadRequest,
        response: OkxHttpResponse,
    ) -> Result<Self, OkxError> {
        if response.binding != *request.scope.gateway_binding()
            || response.instrument_generation != request.scope.instrument_generation()
        {
            return Err(OkxError::Binding);
        }
        Self::new(request, response.received_at_ms, response.body.to_vec())
    }

    pub fn validate(&self) -> Result<(), OkxError> {
        if self.parser_schema_version != OKX_PRIVATE_READBACK_SCHEMA_VERSION
            || self.scope.binding.venue != VenueId::Okx
            || self.scope.instrument_generation == 0
            || self.scope.attempt_id == 0
            || self.received_at_ms == 0
            || self.payload.is_empty()
            || self.payload_sha256 != payload_digest(&self.payload)
        {
            return Err(OkxError::Binding);
        }
        validate_page_request(self.surface, self.page_index, self.request_after.as_deref())?;
        if self.request_path
            != request_path(&self.scope, self.surface, self.request_after.as_deref())?
        {
            return Err(OkxError::Binding);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxPrivatePageAdvance {
    Closed,
    More(OkxPrivateReadRequest),
}

/// Derives the only admissible next signed request from a captured response. A full 100-row page
/// is not closed; even when it is the last populated page, an additional empty response is needed
/// to prove completion.
pub fn advance_private_page(page: &OkxRawPrivatePage) -> Result<OkxPrivatePageAdvance, OkxError> {
    page.validate()?;
    if !page.surface.paginated() {
        return Err(OkxError::Pagination);
    }
    let ids = private_page_ids(page)?;
    let previous_after = page.request_after.as_deref().map(numeric_id).transpose()?;
    validate_page_ids(&ids, previous_after)?;
    if ids.len() < usize::from(OKX_PRIVATE_PAGE_LIMIT) {
        return Ok(OkxPrivatePageAdvance::Closed);
    }
    let next_index = page.page_index.checked_add(1).ok_or(OkxError::Pagination)?;
    if usize::try_from(next_index).map_err(|_| OkxError::Pagination)? >= OKX_PRIVATE_MAX_PAGES {
        return Err(OkxError::Pagination);
    }
    let after = ids.last().ok_or(OkxError::Pagination)?;
    Ok(OkxPrivatePageAdvance::More(OkxPrivateReadRequest::new(
        &page.scope,
        page.surface,
        next_index,
        Some(after),
    )?))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxPositionFactSource {
    Reported,
    AbsentFromExactInstrumentQuery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPositionFact {
    pub position: Position,
    pub source: OkxPositionFactSource,
    pub update_time_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxCanonicalOrder {
    pub family: NativeOrderFamily,
    pub native_order_type: String,
    pub trade_mode: OkxTradeMode,
    pub order: Order,
    pub update_time_ms: u64,
    /// Exact native semantic row. The containing raw page remains the signed source of truth.
    pub native_semantics: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxOrderFamilyReadback {
    pub family: NativeOrderFamily,
    pub orders: Vec<OkxCanonicalOrder>,
    pub raw_pages: Vec<OkxRawPrivatePage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPrivateReadbackCandidate {
    scope: OkxPrivateReadScope,
    pub profile: OkxAccountProfile,
    pub balance: OkxTimedBalance,
    pub positions: Vec<OkxPositionFact>,
    pub regular_orders: Vec<OkxTimedOrder>,
    pub order_families: BTreeMap<NativeOrderFamily, OkxOrderFamilyReadback>,
    pub fills: Vec<OkxFill>,
    pub raw_pages: Vec<OkxRawPrivatePage>,
    pub observed_at_ms: u64,
}

impl OkxPrivateReadbackCandidate {
    #[must_use]
    pub const fn scope(&self) -> &OkxPrivateReadScope {
        &self.scope
    }

    #[must_use]
    pub fn order_family(&self, family: NativeOrderFamily) -> Option<&OkxOrderFamilyReadback> {
        self.order_families.get(&family)
    }

    #[must_use]
    pub fn has_open_orders(&self) -> bool {
        self.order_families
            .values()
            .any(|family| !family.orders.is_empty())
    }

    #[must_use]
    pub fn is_flat(&self) -> bool {
        self.positions
            .iter()
            .all(|fact| fact.position.quantity.is_zero())
    }
}

/// Atomically validates one account/profile/balance/position/order/fill collection attempt.
pub fn complete_private_readback(
    scope: &OkxPrivateReadScope,
    instrument: &OkxInstrument,
    raw_pages: Vec<OkxRawPrivatePage>,
) -> Result<OkxPrivateReadbackCandidate, OkxError> {
    scope.validate_instrument(instrument)?;
    if raw_pages.is_empty() {
        return Err(OkxError::Pagination);
    }
    let mut grouped = BTreeMap::<OkxPrivateSurface, Vec<OkxRawPrivatePage>>::new();
    let mut observed_at_ms = 0;
    for page in &raw_pages {
        page.validate()?;
        if &page.scope != scope {
            return Err(OkxError::Binding);
        }
        observed_at_ms = observed_at_ms.max(page.received_at_ms);
        grouped.entry(page.surface).or_default().push(page.clone());
    }

    let account_raw = one_unpaged(&mut grouped, OkxPrivateSurface::AccountConfig)?;
    let profile = crate::parse_account_profile(&account_raw.payload, scope.expected_position_mode)?;
    if !profile.supports_trade_mode(scope.trade_mode) {
        return Err(OkxError::Binding);
    }
    let balance_raw = one_unpaged(&mut grouped, OkxPrivateSurface::Balance)?;
    let balance = crate::parse_balance(&balance_raw.payload, &config_from_scope(scope)?, &profile)?;
    if balance.update_time_ms > balance_raw.received_at_ms {
        return Err(OkxError::Sequence);
    }
    let positions_raw = one_unpaged(&mut grouped, OkxPrivateSurface::Positions)?;
    let positions = parse_strict_positions(&positions_raw, instrument, &profile)?;

    let regular_raw = grouped
        .remove(&OkxPrivateSurface::RegularOrders)
        .ok_or(OkxError::Pagination)?;
    let (regular_orders, regular_canonical, regular_raw) =
        complete_regular_pages(regular_raw, instrument, &profile, scope.trade_mode)?;

    let mut conditional_orders = Vec::new();
    let mut conditional_raw = Vec::new();
    let mut algo_orders = Vec::new();
    let mut algo_raw = Vec::new();
    for kind in OkxAlgoOrderKind::ALL {
        let surface = OkxPrivateSurface::AlgoOrders(kind);
        let pages = grouped.remove(&surface).ok_or(OkxError::Pagination)?;
        let (orders, pages) =
            complete_algo_pages(pages, instrument, &profile, scope.trade_mode, kind)?;
        if kind.family() == NativeOrderFamily::UmConditional {
            conditional_orders.extend(orders);
            conditional_raw.extend(pages);
        } else {
            algo_orders.extend(orders);
            algo_raw.extend(pages);
        }
    }
    reject_duplicate_canonical_ids(&conditional_orders)?;
    reject_duplicate_canonical_ids(&algo_orders)?;

    let fill_raw = grouped
        .remove(&OkxPrivateSurface::Fills)
        .ok_or(OkxError::Pagination)?;
    let fills = complete_fill_pages(fill_raw, instrument, &profile)?.0;
    if !grouped.is_empty() {
        return Err(OkxError::Pagination);
    }

    let regular_family = OkxOrderFamilyReadback {
        family: NativeOrderFamily::UmOrder,
        orders: regular_canonical,
        raw_pages: regular_raw,
    };
    if regular_family
        .orders
        .iter()
        .map(|item| &item.order)
        .ne(regular_orders.iter().map(|item| &item.order))
    {
        return Err(OkxError::Binding);
    }
    let conditional_family = OkxOrderFamilyReadback {
        family: NativeOrderFamily::UmConditional,
        orders: conditional_orders,
        raw_pages: conditional_raw,
    };
    let algo_family = OkxOrderFamilyReadback {
        family: NativeOrderFamily::UmAlgo,
        orders: algo_orders,
        raw_pages: algo_raw,
    };
    let order_families = BTreeMap::from([
        (NativeOrderFamily::UmOrder, regular_family),
        (NativeOrderFamily::UmConditional, conditional_family),
        (NativeOrderFamily::UmAlgo, algo_family),
    ]);
    Ok(OkxPrivateReadbackCandidate {
        scope: scope.clone(),
        profile,
        balance,
        positions,
        regular_orders,
        order_families,
        fills,
        raw_pages,
        observed_at_ms,
    })
}

fn config_from_scope(scope: &OkxPrivateReadScope) -> Result<OkxConfig, OkxError> {
    OkxConfig::for_binding(scope.binding.clone()).map_err(|_| OkxError::Binding)
}

fn one_unpaged(
    grouped: &mut BTreeMap<OkxPrivateSurface, Vec<OkxRawPrivatePage>>,
    surface: OkxPrivateSurface,
) -> Result<OkxRawPrivatePage, OkxError> {
    let pages = grouped.remove(&surface).ok_or(OkxError::Pagination)?;
    let [page] = pages.as_slice() else {
        return Err(OkxError::Pagination);
    };
    Ok(page.clone())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrictPositionRow {
    inst_type: String,
    inst_id: String,
    mgn_mode: String,
    pos_side: String,
    pos: String,
    avg_px: String,
    mark_px: String,
    u_time: String,
}

fn parse_strict_positions(
    raw: &OkxRawPrivatePage,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
) -> Result<Vec<OkxPositionFact>, OkxError> {
    let envelope: Envelope<StrictPositionRow> = decode_success(&raw.payload)?;
    let mut seen = BTreeSet::new();
    let mut positions = Vec::new();
    for row in envelope.data {
        if row.mgn_mode != raw.scope.trade_mode.wire_value() {
            return Err(OkxError::Binding);
        }
        let normalized = normalize_position_row(
            PositionRow {
                inst_type: row.inst_type,
                inst_id: row.inst_id,
                pos_side: row.pos_side,
                pos: row.pos,
                avg_px: row.avg_px,
                mark_px: row.mark_px,
                u_time: row.u_time,
            },
            instrument,
            profile,
            true,
        )?
        .ok_or(OkxError::Payload)?;
        if normalized.update_time_ms > raw.received_at_ms || !seen.insert(normalized.position.side)
        {
            return Err(OkxError::Sequence);
        }
        positions.push(OkxPositionFact {
            position: normalized.position,
            source: OkxPositionFactSource::Reported,
            update_time_ms: Some(normalized.update_time_ms),
        });
    }
    for side in required_position_sides(profile.position_mode()) {
        if !seen.contains(side) {
            positions.push(OkxPositionFact {
                position: Position {
                    symbol: instrument.instrument().symbol.clone(),
                    side: *side,
                    quantity: Decimal::ZERO,
                    entry_price: None,
                    mark_price: None,
                },
                source: OkxPositionFactSource::AbsentFromExactInstrumentQuery,
                update_time_ms: None,
            });
        }
    }
    positions.sort_by_key(|fact| fact.position.side);
    Ok(positions)
}

fn required_position_sides(mode: OkxPositionMode) -> &'static [PositionSide] {
    match mode {
        OkxPositionMode::Net => &[PositionSide::Net],
        OkxPositionMode::LongShort => &[PositionSide::Long, PositionSide::Short],
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StrictOrderRow {
    inst_type: String,
    inst_id: String,
    td_mode: String,
    category: String,
    ord_type: String,
    ord_id: String,
    #[serde(default)]
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

type CompletedRegularPages = (
    Vec<OkxTimedOrder>,
    Vec<OkxCanonicalOrder>,
    Vec<OkxRawPrivatePage>,
);

fn complete_regular_pages(
    pages: Vec<OkxRawPrivatePage>,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    trade_mode: OkxTradeMode,
) -> Result<CompletedRegularPages, OkxError> {
    let pages = closed_pages(pages, |page| {
        let envelope: Envelope<StrictOrderRow> = decode_success(&page.payload)?;
        let ids = envelope
            .data
            .iter()
            .map(|row| row.ord_id.clone())
            .collect::<Vec<_>>();
        Ok((envelope.data, ids))
    })?;
    let mut timed = Vec::new();
    let mut canonical = Vec::new();
    for (raw, rows) in &pages {
        for row in rows {
            if row.td_mode != trade_mode.wire_value()
                || row.category != "normal"
                || !regular_order_type(&row.ord_type)
            {
                return Err(OkxError::Binding);
            }
            let native_semantics = serde_json::to_value(row).map_err(|_| OkxError::Payload)?;
            let side = order_side(&row.side)?;
            let leg = position_side(profile.position_mode(), &row.pos_side, Decimal::ONE)?;
            let semantic_reduce =
                semantic_reduce(profile.position_mode(), leg, side, &row.reduce_only)?;
            let mut item = normalize_order_row(
                OrderRow {
                    inst_type: row.inst_type.clone(),
                    inst_id: row.inst_id.clone(),
                    ord_id: row.ord_id.clone(),
                    cl_ord_id: row.cl_ord_id.clone(),
                    side: row.side.clone(),
                    pos_side: row.pos_side.clone(),
                    sz: row.sz.clone(),
                    acc_fill_sz: row.acc_fill_sz.clone(),
                    px: row.px.clone(),
                    avg_px: row.avg_px.clone(),
                    reduce_only: row.reduce_only.clone(),
                    state: row.state.clone(),
                    u_time: row.u_time.clone(),
                },
                instrument,
                profile,
                false,
            )?;
            item.order.reduce_only = semantic_reduce;
            item.order.purpose = FieldState::Known(if semantic_reduce {
                OrderPurpose::Reduce
            } else {
                OrderPurpose::Entry
            });
            item.order.validate().map_err(|_| OkxError::Payload)?;
            if item.update_time_ms > raw.received_at_ms {
                return Err(OkxError::Sequence);
            }
            canonical.push(OkxCanonicalOrder {
                family: NativeOrderFamily::UmOrder,
                native_order_type: row.ord_type.clone(),
                trade_mode,
                order: item.order.clone(),
                update_time_ms: item.update_time_ms,
                native_semantics,
            });
            timed.push(item);
        }
    }
    reject_duplicate_canonical_ids(&canonical)?;
    Ok((
        timed,
        canonical,
        pages.into_iter().map(|(raw, _)| raw).collect(),
    ))
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StrictAlgoRow {
    inst_type: String,
    inst_id: String,
    td_mode: String,
    algo_id: String,
    #[serde(default)]
    algo_cl_ord_id: String,
    side: String,
    pos_side: String,
    sz: String,
    ord_type: String,
    reduce_only: String,
    state: String,
    #[serde(default)]
    u_time: String,
    c_time: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

fn complete_algo_pages(
    pages: Vec<OkxRawPrivatePage>,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    trade_mode: OkxTradeMode,
    kind: OkxAlgoOrderKind,
) -> Result<(Vec<OkxCanonicalOrder>, Vec<OkxRawPrivatePage>), OkxError> {
    let pages = closed_pages(pages, |page| {
        let envelope: Envelope<StrictAlgoRow> = decode_success(&page.payload)?;
        let ids = envelope
            .data
            .iter()
            .map(|row| row.algo_id.clone())
            .collect::<Vec<_>>();
        Ok((envelope.data, ids))
    })?;
    let mut orders = Vec::new();
    for (raw, rows) in &pages {
        for row in rows {
            if row.inst_type != "SWAP"
                || row.inst_id != instrument.native_id()
                || row.td_mode != trade_mode.wire_value()
                || !kind.accepts(&row.ord_type)
                || !matches!(row.state.as_str(), "live" | "effective")
            {
                return Err(OkxError::Binding);
            }
            validate_numeric_id(&row.algo_id)?;
            validate_client_id_or_empty(&row.algo_cl_ord_id)?;
            let side = order_side(&row.side)?;
            let leg = position_side(profile.position_mode(), &row.pos_side, Decimal::ONE)?;
            let semantic_reduce =
                semantic_reduce(profile.position_mode(), leg, side, &row.reduce_only)?;
            let quantity = instrument.contracts_to_base(positive_decimal(&row.sz)?)?;
            let update_time_ms = if row.u_time.is_empty() {
                positive_u64(&row.c_time)?
            } else {
                positive_u64(&row.u_time)?
            };
            if update_time_ms > raw.received_at_ms {
                return Err(OkxError::Sequence);
            }
            let order = Order {
                order_id: row.algo_id.clone(),
                client_order_id: if row.algo_cl_ord_id.is_empty() {
                    FieldState::Missing
                } else {
                    FieldState::Known(row.algo_cl_ord_id.clone())
                },
                symbol: instrument.instrument().symbol.clone(),
                side,
                position_side: FieldState::Known(leg),
                purpose: FieldState::Known(if semantic_reduce {
                    OrderPurpose::Reduce
                } else {
                    OrderPurpose::Entry
                }),
                state: OrderState::New,
                quantity,
                filled_quantity: Decimal::ZERO,
                limit_price: None,
                average_price: FieldState::Missing,
                reduce_only: semantic_reduce,
            };
            order.validate().map_err(|_| OkxError::Payload)?;
            orders.push(OkxCanonicalOrder {
                family: kind.family(),
                native_order_type: row.ord_type.clone(),
                trade_mode,
                order,
                update_time_ms,
                native_semantics: serde_json::to_value(row).map_err(|_| OkxError::Payload)?,
            });
        }
    }
    reject_duplicate_canonical_ids(&orders)?;
    Ok((orders, pages.into_iter().map(|(raw, _)| raw).collect()))
}

fn complete_fill_pages(
    pages: Vec<OkxRawPrivatePage>,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
) -> Result<(Vec<OkxFill>, Vec<OkxRawPrivatePage>), OkxError> {
    let pages = closed_pages(pages, |page| {
        let envelope: Envelope<FillRow> = decode_success(&page.payload)?;
        let ids = envelope
            .data
            .iter()
            .map(|row| row.bill_id.clone())
            .collect::<Vec<_>>();
        Ok((envelope.data, ids))
    })?;
    let mut seen = BTreeSet::new();
    let mut fills = Vec::new();
    for (raw, rows) in &pages {
        for row in rows {
            let fill = normalize_fill(row.clone(), instrument, profile)?;
            if fill
                .fill
                .exchange_time_ms
                .is_some_and(|time| time > raw.received_at_ms)
                || !seen.insert(fill.fill.fill_id.clone())
            {
                return Err(OkxError::Sequence);
            }
            fills.push(fill);
        }
    }
    Ok((fills, pages.into_iter().map(|(raw, _)| raw).collect()))
}

fn closed_pages<T, F>(
    mut pages: Vec<OkxRawPrivatePage>,
    mut parse: F,
) -> Result<Vec<(OkxRawPrivatePage, Vec<T>)>, OkxError>
where
    F: FnMut(&OkxRawPrivatePage) -> Result<(Vec<T>, Vec<String>), OkxError>,
{
    if pages.is_empty() || pages.len() > OKX_PRIVATE_MAX_PAGES {
        return Err(OkxError::Pagination);
    }
    pages.sort_by_key(|page| page.page_index);
    let surface = pages[0].surface;
    let scope = pages[0].scope.clone();
    let mut expected_after = None::<String>;
    let mut all_ids = BTreeSet::new();
    let mut result = Vec::new();
    let page_count = pages.len();
    for (index, page) in pages.into_iter().enumerate() {
        if page.surface != surface
            || page.scope != scope
            || usize::try_from(page.page_index).ok() != Some(index)
            || page.request_after != expected_after
        {
            return Err(OkxError::Pagination);
        }
        let (items, ids) = parse(&page)?;
        if ids.len() > usize::from(OKX_PRIVATE_PAGE_LIMIT)
            || ids.iter().any(|id| !all_ids.insert(id.clone()))
        {
            return Err(OkxError::Pagination);
        }
        let previous_after = expected_after.as_deref().map(numeric_id).transpose()?;
        validate_page_ids(&ids, previous_after)?;
        let closed = ids.len() < usize::from(OKX_PRIVATE_PAGE_LIMIT);
        if closed != (index + 1 == page_count) {
            return Err(OkxError::Pagination);
        }
        expected_after = if closed { None } else { ids.last().cloned() };
        result.push((page, items));
    }
    if expected_after.is_some() {
        return Err(OkxError::Pagination);
    }
    Ok(result)
}

fn private_page_ids(page: &OkxRawPrivatePage) -> Result<Vec<String>, OkxError> {
    match page.surface {
        OkxPrivateSurface::RegularOrders => {
            let envelope: Envelope<StrictOrderRow> = decode_success(&page.payload)?;
            Ok(envelope.data.into_iter().map(|row| row.ord_id).collect())
        }
        OkxPrivateSurface::AlgoOrders(_) => {
            let envelope: Envelope<StrictAlgoRow> = decode_success(&page.payload)?;
            Ok(envelope.data.into_iter().map(|row| row.algo_id).collect())
        }
        OkxPrivateSurface::Fills => {
            let envelope: Envelope<FillRow> = decode_success(&page.payload)?;
            Ok(envelope.data.into_iter().map(|row| row.bill_id).collect())
        }
        _ => Err(OkxError::Pagination),
    }
}

fn validate_page_ids(ids: &[String], previous_after: Option<u128>) -> Result<(), OkxError> {
    if ids.len() > usize::from(OKX_PRIVATE_PAGE_LIMIT) {
        return Err(OkxError::Pagination);
    }
    let parsed_ids = ids
        .iter()
        .map(|id| numeric_id(id))
        .collect::<Result<Vec<_>, _>>()?;
    if parsed_ids.windows(2).any(|pair| pair[0] <= pair[1])
        || previous_after
            .is_some_and(|after| parsed_ids.first().is_some_and(|first| *first >= after))
    {
        return Err(OkxError::Sequence);
    }
    Ok(())
}

fn semantic_reduce(
    mode: OkxPositionMode,
    leg: PositionSide,
    side: OrderSide,
    wire_reduce_only: &str,
) -> Result<bool, OkxError> {
    let raw_reduce = match wire_reduce_only {
        "true" => true,
        "false" => false,
        _ => return Err(OkxError::Payload),
    };
    match mode {
        OkxPositionMode::Net if leg == PositionSide::Net => Ok(raw_reduce),
        OkxPositionMode::LongShort if leg != PositionSide::Net && !raw_reduce => Ok(matches!(
            (leg, side),
            (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
        )),
        _ => Err(OkxError::PositionMode),
    }
}

fn regular_order_type(value: &str) -> bool {
    matches!(
        value,
        "market" | "limit" | "post_only" | "fok" | "ioc" | "optimal_limit_ioc" | "rpi" | "elp"
    )
}

fn reject_duplicate_canonical_ids(orders: &[OkxCanonicalOrder]) -> Result<(), OkxError> {
    let mut venue_ids = BTreeSet::new();
    let mut client_ids = BTreeSet::new();
    for item in orders {
        if !venue_ids.insert(item.order.order_id.clone()) {
            return Err(OkxError::Identity);
        }
        if let FieldState::Known(client_id) = &item.order.client_order_id
            && !client_ids.insert(client_id.clone())
        {
            return Err(OkxError::Identity);
        }
    }
    Ok(())
}

fn validate_client_id_or_empty(value: &str) -> Result<(), OkxError> {
    if value.is_empty()
        || ((1..=32).contains(&value.len())
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric()))
    {
        Ok(())
    } else {
        Err(OkxError::Identity)
    }
}

fn validate_numeric_id(value: &str) -> Result<(), OkxError> {
    numeric_id(value).map(|_| ())
}

fn numeric_id(value: &str) -> Result<u128, OkxError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(OkxError::Pagination);
    }
    value.parse::<u128>().map_err(|_| OkxError::Pagination)
}

pub(crate) fn payload_digest(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const PROFILE: &str = include_str!("../fixtures/account-config.json");

    fn setup() -> Result<(OkxConfig, OkxInstrument, OkxPrivateReadScope), Box<dyn std::error::Error>>
    {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Test,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = crate::parse_instrument(INSTRUMENT, &config, 7)?;
        let scope = OkxPrivateReadScope::new(
            &config,
            &instrument,
            OkxPositionMode::LongShort,
            OkxTradeMode::Cross,
            11,
        )?;
        Ok((config, instrument, scope))
    }

    fn page(
        request: OkxPrivateReadRequest,
        payload: impl Into<Vec<u8>>,
    ) -> Result<OkxRawPrivatePage, OkxError> {
        OkxRawPrivatePage::new(&request, 1_900_000_000_000, payload.into())
    }

    fn empty() -> &'static str {
        r#"{"code":"0","msg":"","data":[]}"#
    }

    fn all_empty_pages(scope: &OkxPrivateReadScope) -> Result<Vec<OkxRawPrivatePage>, OkxError> {
        let mut pages = vec![
            page(build_account_config_request(scope)?, PROFILE)?,
            page(
                build_balance_request(scope)?,
                r#"{"code":"0","msg":"","data":[{"uTime":"1899999999000","details":[{"ccy":"USDT","eq":"1000","availBal":"900","imr":"50","mmr":"10","uTime":"1899999998000"}]}]}"#,
            )?,
            page(build_positions_request(scope)?, empty())?,
            page(build_regular_orders_request(scope, 0, None)?, empty())?,
            page(build_fills_request(scope, 0, None)?, empty())?,
        ];
        for kind in OkxAlgoOrderKind::ALL {
            pages.push(page(
                build_algo_orders_request(scope, kind, 0, None)?,
                empty(),
            )?);
        }
        Ok(pages)
    }

    #[test]
    fn every_private_request_is_exactly_scoped_and_signed() -> Result<(), Box<dyn std::error::Error>>
    {
        let (config, _, scope) = setup()?;
        assert_eq!(
            build_account_config_request(&scope)?.request_path(),
            "/api/v5/account/config"
        );
        assert_eq!(
            build_balance_request(&scope)?.request_path(),
            "/api/v5/account/balance?ccy=USDT"
        );
        assert_eq!(
            build_positions_request(&scope)?.request_path(),
            "/api/v5/account/positions?instType=SWAP&instId=BTC-USDT-SWAP"
        );
        let regular = build_regular_orders_request(&scope, 0, None)?;
        assert_eq!(
            regular.request_path(),
            "/api/v5/trade/orders-pending?instType=SWAP&instId=BTC-USDT-SWAP&limit=100"
        );
        let conditional =
            build_algo_orders_request(&scope, OkxAlgoOrderKind::ConditionalOco, 0, None)?;
        assert_eq!(
            conditional.request_path(),
            "/api/v5/trade/orders-algo-pending?ordType=conditional,oco&instType=SWAP&instId=BTC-USDT-SWAP&limit=100"
        );
        assert_eq!(
            build_fills_request(&scope, 1, Some("9001"))?.request_path(),
            "/api/v5/trade/fills-history?instType=SWAP&instId=BTC-USDT-SWAP&after=9001&limit=100"
        );
        let headers = conditional.signed_headers(
            &OkxCredentials::from_values("key", "secret", "pass")?,
            &config,
            "2026-08-30T01:02:03.000Z",
        )?;
        assert_eq!(headers.get("x-simulated-trading"), Some("1"));
        assert_eq!(
            build_regular_orders_request(&scope, 1, None),
            Err(OkxError::Pagination)
        );
        assert_eq!(
            build_fills_request(&scope, 1, Some("not-numeric")),
            Err(OkxError::Pagination)
        );
        Ok(())
    }

    #[test]
    fn empty_exact_snapshot_proves_all_families_and_both_zero_hedge_legs()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, instrument, scope) = setup()?;
        let candidate = complete_private_readback(&scope, &instrument, all_empty_pages(&scope)?)?;
        assert_eq!(candidate.order_families.len(), 3);
        assert!(
            candidate
                .order_families
                .values()
                .all(|family| family.orders.is_empty())
        );
        assert_eq!(candidate.positions.len(), 2);
        assert!(candidate.positions.iter().all(|fact| {
            fact.source == OkxPositionFactSource::AbsentFromExactInstrumentQuery
                && fact.position.quantity.is_zero()
        }));
        assert!(!candidate.has_open_orders());
        assert!(candidate.is_flat());

        let mut missing_algo_surface = all_empty_pages(&scope)?;
        missing_algo_surface.retain(|page| {
            page.surface != OkxPrivateSurface::AlgoOrders(OkxAlgoOrderKind::SmartIceberg)
        });
        assert_eq!(
            complete_private_readback(&scope, &instrument, missing_algo_surface),
            Err(OkxError::Pagination)
        );
        Ok(())
    }

    #[test]
    fn regular_conditional_and_algo_rows_keep_raw_semantics_and_exact_td_mode()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, instrument, scope) = setup()?;
        let mut pages = all_empty_pages(&scope)?;
        let regular = r#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","tdMode":"cross","category":"normal","ordType":"post_only","ordId":"9003","clOrdId":"regular3","side":"sell","posSide":"long","sz":"2","accFillSz":"0","px":"60000","avgPx":"","reduceOnly":"false","state":"live","uTime":"1899999999000"}]}"#;
        let conditional = r#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","tdMode":"cross","algoId":"8003","algoClOrdId":"conditional3","side":"sell","posSide":"long","sz":"2","ordType":"conditional","reduceOnly":"false","state":"live","uTime":"1899999999000","cTime":"1899999998000","slTriggerPx":"59000","slOrdPx":"-1"}]}"#;
        let trigger = r#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","tdMode":"cross","algoId":"7003","algoClOrdId":"trigger3","side":"buy","posSide":"short","sz":"2","ordType":"trigger","reduceOnly":"false","state":"live","uTime":"1899999999000","cTime":"1899999998000","triggerPx":"61000","orderPx":"-1"}]}"#;
        for raw in &mut pages {
            raw.payload = match raw.surface {
                OkxPrivateSurface::RegularOrders => regular.as_bytes().to_vec(),
                OkxPrivateSurface::AlgoOrders(OkxAlgoOrderKind::ConditionalOco) => {
                    conditional.as_bytes().to_vec()
                }
                OkxPrivateSurface::AlgoOrders(OkxAlgoOrderKind::Trigger) => {
                    trigger.as_bytes().to_vec()
                }
                _ => continue,
            };
            raw.payload_sha256 = payload_digest(&raw.payload);
        }
        let candidate = complete_private_readback(&scope, &instrument, pages)?;
        assert_eq!(candidate.regular_orders.len(), 1);
        assert!(candidate.regular_orders[0].order.reduce_only);
        let conditional = candidate
            .order_family(NativeOrderFamily::UmConditional)
            .ok_or("missing conditional")?;
        assert_eq!(conditional.orders.len(), 1);
        assert_eq!(conditional.orders[0].native_order_type, "conditional");
        assert!(
            conditional.orders[0]
                .native_semantics
                .get("slTriggerPx")
                .is_some()
        );
        let algo = candidate
            .order_family(NativeOrderFamily::UmAlgo)
            .ok_or("missing algo")?;
        assert_eq!(algo.orders.len(), 1);
        assert!(algo.orders[0].order.reduce_only);
        Ok(())
    }

    #[test]
    fn mixed_attempt_tampered_raw_wrong_td_mode_and_unclosed_pages_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, instrument, scope) = setup()?;
        let mut pages = all_empty_pages(&scope)?;
        pages[0].scope.attempt_id = 12;
        assert_eq!(
            complete_private_readback(&scope, &instrument, pages),
            Err(OkxError::Binding)
        );

        let mut pages = all_empty_pages(&scope)?;
        pages[0].payload.push(b' ');
        assert_eq!(
            complete_private_readback(&scope, &instrument, pages),
            Err(OkxError::Binding)
        );

        let mut pages = all_empty_pages(&scope)?;
        let wrong = r#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","tdMode":"isolated","category":"normal","ordType":"limit","ordId":"9","clOrdId":"a","side":"buy","posSide":"long","sz":"1","accFillSz":"0","px":"60000","avgPx":"","reduceOnly":"false","state":"live","uTime":"1"}]}"#;
        let regular = pages
            .iter_mut()
            .find(|page| page.surface == OkxPrivateSurface::RegularOrders)
            .ok_or("missing regular")?;
        regular.payload = wrong.as_bytes().to_vec();
        regular.payload_sha256 = payload_digest(&regular.payload);
        assert_eq!(
            complete_private_readback(&scope, &instrument, pages),
            Err(OkxError::Binding)
        );

        let full = format!(
            "{{\"code\":\"0\",\"msg\":\"\",\"data\":[{}]}}",
            (1..=100)
                .rev()
                .map(|id| format!(
                    "{{\"instType\":\"SWAP\",\"instId\":\"BTC-USDT-SWAP\",\"billId\":\"{id}\",\"ordId\":\"{id}\",\"clOrdId\":\"c{id}\",\"fillPx\":\"60000\",\"fillSz\":\"1\",\"side\":\"buy\",\"posSide\":\"long\",\"feeCcy\":\"USDT\",\"fee\":\"-1\",\"ts\":\"100\",\"fillTime\":\"100\",\"execType\":\"M\"}}"
                ))
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut pages = all_empty_pages(&scope)?;
        let fill = pages
            .iter_mut()
            .find(|page| page.surface == OkxPrivateSurface::Fills)
            .ok_or("missing fills")?;
        fill.payload = full.into_bytes();
        fill.payload_sha256 = payload_digest(&fill.payload);
        let next = match advance_private_page(fill)? {
            OkxPrivatePageAdvance::More(request) => request,
            OkxPrivatePageAdvance::Closed => return Err("full page closed early".into()),
        };
        assert_eq!(next.page_index(), 1);
        assert_eq!(next.request_after(), Some("1"));
        assert_eq!(
            complete_private_readback(&scope, &instrument, pages.clone()),
            Err(OkxError::Pagination)
        );
        pages.push(page(next, empty())?);
        let candidate = complete_private_readback(&scope, &instrument, pages)?;
        assert_eq!(candidate.fills.len(), 100);
        Ok(())
    }

    #[test]
    fn net_short_sign_is_preserved_and_account_level_rejects_isolated_mode()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, _) = setup()?;
        let net_scope = OkxPrivateReadScope::new(
            &config,
            &instrument,
            OkxPositionMode::Net,
            OkxTradeMode::Cross,
            12,
        )?;
        let mut pages = all_empty_pages(&net_scope)?;
        for raw in &mut pages {
            raw.payload = match raw.surface {
                OkxPrivateSurface::AccountConfig => br#"{"code":"0","msg":"","data":[{"uid":"fixture-sub-account","mainUid":"fixture-main-account","acctLv":"3","posMode":"net_mode","perm":"read_only,trade"}]}"#.to_vec(),
                OkxPrivateSurface::Positions => br#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"net","pos":"-2","avgPx":"60000","markPx":"59000","uTime":"1899999999000"}]}"#.to_vec(),
                _ => continue,
            };
            raw.payload_sha256 = payload_digest(&raw.payload);
        }
        let candidate = complete_private_readback(&net_scope, &instrument, pages)?;
        assert_eq!(candidate.positions.len(), 1);
        assert_eq!(candidate.positions[0].position.side, PositionSide::Net);
        assert_eq!(
            candidate.positions[0].position.quantity,
            Decimal::new(-2, 1)
        );

        let isolated_scope = OkxPrivateReadScope::new(
            &config,
            &instrument,
            OkxPositionMode::LongShort,
            OkxTradeMode::Isolated,
            13,
        )?;
        assert_eq!(
            complete_private_readback(
                &isolated_scope,
                &instrument,
                all_empty_pages(&isolated_scope)?,
            ),
            Err(OkxError::Binding)
        );
        Ok(())
    }

    #[test]
    fn next_cursor_is_not_derived_from_a_page_outside_its_requested_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, scope) = setup()?;
        let request = build_fills_request(&scope, 1, Some("200"))?;
        let payload = br#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","billId":"200","ordId":"200","clOrdId":"c200","fillPx":"60000","fillSz":"1","side":"buy","posSide":"long","feeCcy":"USDT","fee":"-1","ts":"100","fillTime":"100","execType":"M"}]}"#;
        let raw = page(request, payload.as_slice())?;
        assert_eq!(advance_private_page(&raw), Err(OkxError::Sequence));
        Ok(())
    }
}
