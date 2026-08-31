use std::{
    cmp::{Ordering, Reverse},
    collections::BTreeMap,
    str::FromStr,
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    AccountBalance, Asset, FieldState, Fill, MarketLevel, Order, OrderSide, OrderState, Position,
    PositionSide, Price, Symbol, UnknownReason,
};
use venue_gateway_api::GatewayMode;

use crate::{
    HyperliquidConfig, HyperliquidError, HyperliquidReadBinding, endpoints,
    models::{
        BboData, BookData, BookLevel, ClearinghouseState, EventEnvelope, FrontendOrderRow,
        OrderStatusEnvelope, PerpMetaResponse, UserFillRow, UserFillsData, UserTwapSliceFillRow,
    },
};

pub(crate) mod account;

const MAX_BOOK_LEVELS: usize = 20;
pub const HYPERLIQUID_FILL_RESPONSE_LIMIT: usize = 2_000;
pub const HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT: usize = 10_000;
const MAX_FRONTEND_ORDERS: usize = 2_000;
const MAX_FRONTEND_CHILDREN: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidPayloadScope {
    binding: HyperliquidReadBinding,
    native_coin: String,
}

impl HyperliquidPayloadScope {
    #[must_use]
    pub const fn binding(&self) -> &HyperliquidReadBinding {
        &self.binding
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.binding.gateway().gateway_binding().symbol
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.binding.gateway().gateway_binding().mode
    }

    #[must_use]
    pub fn user_address(&self) -> &str {
        self.binding.user_address()
    }

    #[must_use]
    pub fn native_coin(&self) -> &str {
        &self.native_coin
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidPerpMeta {
    pub scope: HyperliquidPayloadScope,
    pub asset_index: u32,
    pub size_decimals: u32,
    pub max_leverage: u32,
    pub trading_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidBbo {
    pub scope: HyperliquidPayloadScope,
    pub exchange_time_ms: u64,
    pub bid: MarketLevel,
    pub ask: MarketLevel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidAccountSnapshot {
    pub scope: HyperliquidPayloadScope,
    pub exchange_time_ms: u64,
    pub balance: AccountBalance,
    pub position: Option<Position>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidOpenOrder {
    pub order: Order,
    pub exchange_time_ms: u64,
    pub family: HyperliquidOrderFamily,
    pub native_order_type: String,
    pub time_in_force: Option<String>,
    pub trigger_price: Option<Price>,
    pub trigger_condition: String,
    pub is_position_tpsl: bool,
    pub child_order_ids: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HyperliquidOrderFamily {
    Regular,
    Conditional,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidOrderFamilyCoverage {
    CompleteFrontendSnapshot,
    NotCoveredByFrontendOpenOrders,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidOpenOrdersSnapshot {
    pub scope: HyperliquidPayloadScope,
    pub observed_at_ms: u64,
    pub orders: Vec<HyperliquidOpenOrder>,
    pub raw_payload: Vec<u8>,
    pub regular_coverage: HyperliquidOrderFamilyCoverage,
    pub conditional_coverage: HyperliquidOrderFamilyCoverage,
    /// TWAP and other algorithmic parent state require a separate endpoint and remain unproven.
    pub algo_coverage: HyperliquidOrderFamilyCoverage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidUserFills {
    pub scope: HyperliquidPayloadScope,
    pub is_snapshot: bool,
    pub fills: Vec<HyperliquidFill>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidTwapSliceFill {
    pub twap_id: u64,
    pub fill: HyperliquidFill,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFillCursor {
    scope: HyperliquidPayloadScope,
    time_ms: u64,
    page_coin: String,
    trade_id: u64,
}

impl HyperliquidFillCursor {
    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.scope
    }

    #[must_use]
    pub const fn time_ms(&self) -> u64 {
        self.time_ms
    }

    #[must_use]
    pub fn page_coin(&self) -> &str {
        &self.page_coin
    }

    #[must_use]
    pub const fn trade_id(&self) -> u64 {
        self.trade_id
    }

    fn key(&self) -> (u64, &str, u64) {
        (self.time_ms, &self.page_coin, self.trade_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFillQuery {
    scope: HyperliquidPayloadScope,
    begin_ms: u64,
    end_ms: u64,
    limit: usize,
    after: Option<HyperliquidFillCursor>,
}

impl HyperliquidFillQuery {
    pub fn new(
        meta: &HyperliquidPerpMeta,
        begin_ms: u64,
        end_ms: u64,
        limit: usize,
        after: Option<HyperliquidFillCursor>,
    ) -> Result<Self, HyperliquidError> {
        let value = Self {
            scope: meta.scope.clone(),
            begin_ms,
            end_ms,
            limit,
            after,
        };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.scope
    }

    #[must_use]
    pub const fn begin_ms(&self) -> u64 {
        self.begin_ms
    }

    #[must_use]
    pub const fn end_ms(&self) -> u64 {
        self.end_ms
    }

    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }

    #[must_use]
    pub const fn after(&self) -> Option<&HyperliquidFillCursor> {
        self.after.as_ref()
    }

    fn validate(&self) -> Result<(), HyperliquidError> {
        if self.begin_ms == 0
            || self.end_ms < self.begin_ms
            || !(1..=HYPERLIQUID_FILL_RESPONSE_LIMIT).contains(&self.limit)
            || self.after.as_ref().is_some_and(|cursor| {
                cursor.scope != self.scope
                    || cursor.page_coin.is_empty()
                    || cursor.trade_id == 0
                    || cursor.time_ms < self.begin_ms
                    || cursor.time_ms > self.end_ms
            })
        {
            return Err(HyperliquidError::Payload);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidInfoRequest {
    binding: HyperliquidReadBinding,
    mode: GatewayMode,
    rest_origin: &'static str,
    body: Vec<u8>,
}

impl HyperliquidInfoRequest {
    #[must_use]
    pub const fn binding(&self) -> &HyperliquidReadBinding {
        &self.binding
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.mode
    }

    #[must_use]
    pub const fn rest_origin(&self) -> &'static str {
        self.rest_origin
    }

    #[must_use]
    pub const fn endpoint(&self) -> &'static str {
        endpoints::INFO
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HyperliquidOrderLookup {
    OrderId(u64),
    ClientOrderId(String),
}

impl HyperliquidOrderLookup {
    #[must_use]
    pub fn native_identity(&self) -> String {
        match self {
            Self::OrderId(value) => value.to_string(),
            Self::ClientOrderId(value) => value.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HyperliquidOrderStatusUnknownReason {
    UnknownOid,
}

impl HyperliquidOrderLookup {
    pub fn order_id(value: u64) -> Result<Self, HyperliquidError> {
        if value == 0 {
            Err(HyperliquidError::Payload)
        } else {
            Ok(Self::OrderId(value))
        }
    }

    pub fn client_order_id(value: impl Into<String>) -> Result<Self, HyperliquidError> {
        let value = value.into();
        let raw = value.strip_prefix("0x").ok_or(HyperliquidError::Payload)?;
        if raw.len() != 32 || !raw.as_bytes().iter().all(u8::is_ascii_hexdigit) {
            return Err(HyperliquidError::Payload);
        }
        Ok(Self::ClientOrderId(format!(
            "0x{}",
            raw.to_ascii_lowercase()
        )))
    }

    fn validate(&self) -> Result<(), HyperliquidError> {
        match self {
            Self::OrderId(value) => Self::order_id(*value).map(|_| ()),
            Self::ClientOrderId(value) => Self::client_order_id(value.clone()).map(|_| ()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HyperliquidOrderStatus {
    /// The venue did not resolve the exact queried identity. This is not proof of absence or a
    /// terminal outcome and must never authorize resubmission of an UNKNOWN command.
    Unknown {
        scope: HyperliquidPayloadScope,
        lookup: HyperliquidOrderLookup,
        native_identity: String,
        reason: HyperliquidOrderStatusUnknownReason,
    },
    Known {
        scope: HyperliquidPayloadScope,
        order_id: u64,
        client_order_id: FieldState<String>,
        side: OrderSide,
        limit_price: Price,
        original_quantity: Decimal,
        remaining_quantity: Decimal,
        reduce_only: bool,
        native_order_type: String,
        time_in_force: Option<String>,
        state: OrderState,
        exchange_time_ms: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidFillCoverage {
    MorePages,
    /// The requested window is exhausted within the venue-visible recent-fill history. The venue
    /// only exposes its most recent 10,000 fills, so this is never proof about older history.
    VenueVisibleWindowExhausted {
        maximum_retained_fills: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFillPage {
    pub scope: HyperliquidPayloadScope,
    pub fills: Vec<HyperliquidFill>,
    pub next_cursor: Option<HyperliquidFillCursor>,
    /// True only when the response was below the per-response cap and the local limit did not
    /// truncate it. This exhausts the venue-visible window, not history older than its retention
    /// limit; callers must inspect `coverage` and must not interpret it as all-time completeness.
    pub complete: bool,
    pub coverage: HyperliquidFillCoverage,
}

pub fn build_meta_request(
    binding: &HyperliquidReadBinding,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    info_request(binding, serde_json::json!({"type": "meta"}))
}

pub fn build_l2_book_request(
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    info_request(
        meta.scope.binding(),
        serde_json::json!({"type": "l2Book", "coin": meta.scope.native_coin()}),
    )
}

pub fn build_clearinghouse_state_request(
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    bound_user_request("clearinghouseState", &meta.scope)
}

pub fn build_open_orders_request(
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    build_frontend_open_orders_request(meta)
}

pub fn build_frontend_open_orders_request(
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    bound_user_request("frontendOpenOrders", &meta.scope)
}

pub fn build_user_fills_by_time_request(
    query: &HyperliquidFillQuery,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    query.validate()?;
    let start_time = query
        .after
        .as_ref()
        .map_or(query.begin_ms, |cursor| cursor.time_ms);
    info_request(
        query.scope.binding(),
        serde_json::json!({
            "type": "userFillsByTime",
            "user": query.scope.user_address(),
            "startTime": start_time,
            "endTime": query.end_ms,
            "aggregateByTime": false,
        }),
    )
}

/// TWAP slice history has no continuation token.  A cap-sized response is therefore rejected by
/// its parser instead of being mistaken for a complete view of a live parent.
pub fn build_user_twap_slice_fills_request(
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    bound_user_request("userTwapSliceFills", &meta.scope)
}

pub fn build_order_status_request(
    meta: &HyperliquidPerpMeta,
    lookup: &HyperliquidOrderLookup,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    lookup.validate()?;
    let oid = match lookup {
        HyperliquidOrderLookup::OrderId(value) if *value > 0 => serde_json::Value::from(*value),
        HyperliquidOrderLookup::ClientOrderId(value) => {
            HyperliquidOrderLookup::client_order_id(value.clone())?;
            serde_json::Value::String(value.clone())
        }
        HyperliquidOrderLookup::OrderId(_) => return Err(HyperliquidError::Payload),
    };
    info_request(
        meta.scope.binding(),
        serde_json::json!({
            "type": "orderStatus",
            "user": meta.scope.user_address(),
            "oid": oid,
        }),
    )
}

pub fn parse_order_status(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
    lookup: &HyperliquidOrderLookup,
) -> Result<HyperliquidOrderStatus, HyperliquidError> {
    lookup.validate()?;
    let envelope: OrderStatusEnvelope =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    match (envelope.status.as_str(), envelope.order) {
        ("unknownOid", None) => Ok(HyperliquidOrderStatus::Unknown {
            scope: meta.scope.clone(),
            lookup: lookup.clone(),
            native_identity: lookup.native_identity(),
            reason: HyperliquidOrderStatusUnknownReason::UnknownOid,
        }),
        ("order", Some(body)) => {
            if body.order.coin != meta.scope.native_coin {
                return Err(HyperliquidError::Binding);
            }
            validate_status_order(&body.order)?;
            if body.status_timestamp < body.order.timestamp {
                return Err(HyperliquidError::Payload);
            }
            match lookup {
                HyperliquidOrderLookup::OrderId(expected) if body.order.oid != *expected => {
                    return Err(HyperliquidError::Binding);
                }
                HyperliquidOrderLookup::ClientOrderId(expected)
                    if body
                        .order
                        .cloid
                        .as_deref()
                        .is_none_or(|actual| !actual.eq_ignore_ascii_case(expected)) =>
                {
                    return Err(HyperliquidError::Binding);
                }
                HyperliquidOrderLookup::OrderId(_) | HyperliquidOrderLookup::ClientOrderId(_) => {}
            }
            let state = normalized_order_status(&body.status)?;
            let original_quantity = decimal(&body.order.orig_sz)?;
            let remaining_quantity = decimal(&body.order.sz)?;
            if (state == OrderState::Filled && !remaining_quantity.is_zero())
                || (matches!(state, OrderState::New | OrderState::PartiallyFilled)
                    && remaining_quantity.is_zero())
            {
                return Err(HyperliquidError::Payload);
            }
            Ok(HyperliquidOrderStatus::Known {
                scope: meta.scope.clone(),
                order_id: body.order.oid,
                client_order_id: body
                    .order
                    .cloid
                    .map(canonical_cloid)
                    .transpose()?
                    .map(FieldState::Known)
                    .unwrap_or(FieldState::Missing),
                side: side(&body.order.side)?,
                limit_price: Price::new(decimal(&body.order.limit_px)?)
                    .map_err(|_| HyperliquidError::Payload)?,
                original_quantity,
                remaining_quantity,
                reduce_only: body.order.reduce_only,
                native_order_type: body.order.order_type,
                time_in_force: body.order.tif,
                state,
                exchange_time_ms: body.status_timestamp,
            })
        }
        _ => Err(HyperliquidError::Payload),
    }
}

pub fn parse_perp_meta(
    payload: &[u8],
    binding: &HyperliquidReadBinding,
) -> Result<HyperliquidPerpMeta, HyperliquidError> {
    let response: PerpMetaResponse =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    let base = binding.gateway().gateway_binding().symbol.base();
    let mut matches = response
        .universe
        .into_iter()
        .enumerate()
        .filter(|(_, row)| row.name == base);
    let (index, row) = matches.next().ok_or(HyperliquidError::Binding)?;
    if matches.next().is_some()
        || row.name.contains([':', '/', '@'])
        || row.max_leverage == 0
        || row.sz_decimals > 18
    {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidPerpMeta {
        scope: HyperliquidPayloadScope {
            binding: binding.clone(),
            native_coin: row.name,
        },
        asset_index: u32::try_from(index).map_err(|_| HyperliquidError::Payload)?,
        size_decimals: row.sz_decimals,
        max_leverage: row.max_leverage,
        trading_enabled: !row.is_delisted,
    })
}

pub fn parse_l2_book_bbo(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidBbo, HyperliquidError> {
    let data: BookData = serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    validate_coin_time(&data.coin, data.time, &meta.scope)?;
    if data.levels.iter().any(|side| {
        side.is_empty() || side.len() > MAX_BOOK_LEVELS || side.iter().any(|level| level.n == 0)
    }) {
        return Err(HyperliquidError::Payload);
    }
    let mut bids = data.levels[0]
        .iter()
        .map(normalize_level)
        .collect::<Result<Vec<_>, _>>()?;
    let mut asks = data.levels[1]
        .iter()
        .map(normalize_level)
        .collect::<Result<Vec<_>, _>>()?;
    bids.sort_by_key(|level| Reverse(level.price));
    asks.sort_by_key(|level| level.price);
    bbo(meta, data.time, bids.remove(0), asks.remove(0))
}

pub fn parse_ws_bbo(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidBbo, HyperliquidError> {
    let envelope: EventEnvelope =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if envelope.channel != "bbo" {
        return Err(HyperliquidError::Payload);
    }
    let data: BboData =
        serde_json::from_value(envelope.data).map_err(|_| HyperliquidError::Payload)?;
    validate_coin_time(&data.coin, data.time, &meta.scope)?;
    let bid = normalize_level(data.bbo[0].as_ref().ok_or(HyperliquidError::Payload)?)?;
    let ask = normalize_level(data.bbo[1].as_ref().ok_or(HyperliquidError::Payload)?)?;
    bbo(meta, data.time, bid, ask)
}

pub fn parse_clearinghouse_snapshot(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidAccountSnapshot, HyperliquidError> {
    let state: ClearinghouseState =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if state.time == 0 {
        return Err(HyperliquidError::Payload);
    }
    let balance = AccountBalance {
        asset: Asset::new("USDC").map_err(|_| HyperliquidError::Payload)?,
        wallet_balance: decimal(&state.margin_summary.account_value)?,
        available_balance: decimal(&state.withdrawable)?,
        initial_margin: decimal(&state.margin_summary.total_margin_used)?,
        maintenance_margin: decimal(&state.cross_maintenance_margin_used)?,
    };
    balance.validate().map_err(|_| HyperliquidError::Payload)?;
    if balance.available_balance > balance.wallet_balance {
        return Err(HyperliquidError::Payload);
    }
    let mut matching = state
        .asset_positions
        .into_iter()
        .filter(|row| row.position.coin == meta.scope.native_coin);
    let position = matching
        .next()
        .map(|row| normalize_position(row, meta))
        .transpose()?
        .flatten();
    if matching.next().is_some() {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidAccountSnapshot {
        scope: meta.scope.clone(),
        exchange_time_ms: state.time,
        balance,
        position,
    })
}

pub fn parse_open_orders_snapshot(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
    observed_at_ms: u64,
) -> Result<HyperliquidOpenOrdersSnapshot, HyperliquidError> {
    parse_frontend_open_orders_snapshot(payload, meta, observed_at_ms)
}

pub fn parse_frontend_open_orders_snapshot(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
    observed_at_ms: u64,
) -> Result<HyperliquidOpenOrdersSnapshot, HyperliquidError> {
    if observed_at_ms == 0 {
        return Err(HyperliquidError::Payload);
    }
    let rows: Vec<FrontendOrderRow> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    let rows = flatten_frontend_rows(rows)?;
    let orders = rows
        .into_iter()
        .filter(|row| row.coin == meta.scope.native_coin)
        .map(|row| normalize_open_order(row, meta.scope.symbol()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut order_ids = orders
        .iter()
        .map(|item| item.order.order_id.as_str())
        .collect::<Vec<_>>();
    order_ids.sort_unstable();
    if order_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(HyperliquidError::Payload);
    }
    let mut client_order_ids = orders
        .iter()
        .filter_map(|item| match &item.order.client_order_id {
            FieldState::Known(value) => Some(value.as_str()),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        })
        .collect::<Vec<_>>();
    client_order_ids.sort_unstable();
    if client_order_ids
        .windows(2)
        .any(|pair| pair[0].eq_ignore_ascii_case(pair[1]))
    {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidOpenOrdersSnapshot {
        scope: meta.scope.clone(),
        observed_at_ms,
        orders,
        raw_payload: payload.to_vec(),
        regular_coverage: HyperliquidOrderFamilyCoverage::CompleteFrontendSnapshot,
        conditional_coverage: HyperliquidOrderFamilyCoverage::CompleteFrontendSnapshot,
        algo_coverage: HyperliquidOrderFamilyCoverage::NotCoveredByFrontendOpenOrders,
    })
}

pub fn validate_frontend_open_orders_snapshot(
    snapshot: &HyperliquidOpenOrdersSnapshot,
    meta: &HyperliquidPerpMeta,
) -> Result<(), HyperliquidError> {
    let replayed =
        parse_frontend_open_orders_snapshot(&snapshot.raw_payload, meta, snapshot.observed_at_ms)?;
    if &replayed == snapshot {
        Ok(())
    } else {
        Err(HyperliquidError::OrderFamily)
    }
}

pub fn parse_private_user_fills(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
) -> Result<HyperliquidUserFills, HyperliquidError> {
    let events: Vec<EventEnvelope> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    let mut matching = events
        .into_iter()
        .filter(|event| event.channel == "userFills");
    let event = matching.next().ok_or(HyperliquidError::Payload)?;
    if matching.next().is_some() {
        return Err(HyperliquidError::Payload);
    }
    let data: UserFillsData =
        serde_json::from_value(event.data).map_err(|_| HyperliquidError::Payload)?;
    if !data.user.eq_ignore_ascii_case(meta.scope.user_address()) {
        return Err(HyperliquidError::Binding);
    }
    let fills = data
        .fills
        .into_iter()
        .filter(|row| row.coin == meta.scope.native_coin)
        .map(|row| normalize_fill(row, &meta.scope))
        .collect::<Result<Vec<_>, _>>()?;
    reject_duplicate_fills(&fills)?;
    Ok(HyperliquidUserFills {
        scope: meta.scope.clone(),
        is_snapshot: data.is_snapshot.ok_or(HyperliquidError::Payload)?,
        fills,
    })
}

pub fn parse_user_fills_page(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
    query: &HyperliquidFillQuery,
) -> Result<HyperliquidFillPage, HyperliquidError> {
    parse_user_fills_page_scoped(payload, meta, query, None)
}

fn parse_user_fills_page_scoped(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
    query: &HyperliquidFillQuery,
    universe: Option<&BTreeMap<String, HyperliquidPerpMeta>>,
) -> Result<HyperliquidFillPage, HyperliquidError> {
    query.validate()?;
    if query.scope != meta.scope {
        return Err(HyperliquidError::Binding);
    }
    let rows: Vec<UserFillRow> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if rows.len() > HYPERLIQUID_FILL_RESPONSE_LIMIT {
        return Err(HyperliquidError::Payload);
    }
    let capped = rows.len() == HYPERLIQUID_FILL_RESPONSE_LIMIT;
    let mut rows = rows
        .into_iter()
        .map(|row| {
            if row.time < query.begin_ms || row.time > query.end_ms {
                return Err(HyperliquidError::Payload);
            }
            let cursor = fill_cursor(&row, &meta.scope)?;
            Ok((cursor, row))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| left.0.key().cmp(&right.0.key()));
    if rows
        .windows(2)
        .any(|pair| pair[0].0.key() == pair[1].0.key())
    {
        return Err(HyperliquidError::Payload);
    }
    let mut fills = Vec::new();
    let mut last_consumed = None;
    let mut truncated = false;
    for (cursor, row) in rows.into_iter().filter(|(cursor, _)| {
        query
            .after
            .as_ref()
            .is_none_or(|after| cursor.key() > after.key())
    }) {
        last_consumed = Some(cursor.clone());
        let scope = if let Some(universe) = universe {
            // The account endpoint also returns spot trades. They advance its shared cursor,
            // but are not perpetual facts. An unknown perpetual coin must never be dropped.
            match account::perp_scope(&row.coin, universe)? {
                Some(scope) => scope,
                None => continue,
            }
        } else if row.coin == meta.scope.native_coin {
            &meta.scope
        } else {
            continue;
        };
        fills.push(normalize_fill(row, scope)?);
        if fills.len() == query.limit {
            truncated = true;
            break;
        }
    }
    let complete = !capped && !truncated;
    let next_cursor = (!complete).then_some(last_consumed).flatten();
    if !complete && next_cursor.is_none() {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidFillPage {
        scope: meta.scope.clone(),
        fills,
        next_cursor,
        complete,
        coverage: if complete {
            HyperliquidFillCoverage::VenueVisibleWindowExhausted {
                maximum_retained_fills: HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT,
            }
        } else {
            HyperliquidFillCoverage::MorePages
        },
    })
}

pub fn parse_user_twap_slice_fills(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
) -> Result<Vec<HyperliquidTwapSliceFill>, HyperliquidError> {
    let rows: Vec<UserTwapSliceFillRow> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if rows.len() >= HYPERLIQUID_FILL_RESPONSE_LIMIT {
        return Err(HyperliquidError::Payload);
    }
    let mut fill_ids = BTreeMap::new();
    let mut fills = Vec::with_capacity(rows.len());
    for row in rows {
        if row.twap_id == 0 {
            return Err(HyperliquidError::Payload);
        }
        if row.fill.coin != meta.scope.native_coin {
            continue;
        }
        let fill = normalize_fill(row.fill, &meta.scope)?;
        if fill_ids.insert(fill.fill.fill_id.clone(), ()).is_some() {
            return Err(HyperliquidError::Payload);
        }
        fills.push(HyperliquidTwapSliceFill {
            twap_id: row.twap_id,
            fill,
        });
    }
    Ok(fills)
}

fn normalize_position(
    row: crate::models::AssetPositionRow,
    meta: &HyperliquidPerpMeta,
) -> Result<Option<Position>, HyperliquidError> {
    if row.kind != "oneWay" {
        return Err(HyperliquidError::Payload);
    }
    let signed = decimal(&row.position.szi)?;
    if signed.is_zero() {
        return Ok(None);
    }
    let entry_price = row
        .position
        .entry_px
        .as_deref()
        .map(decimal)
        .transpose()?
        .map(Price::new)
        .transpose()
        .map_err(|_| HyperliquidError::Payload)?;
    let mark_price = row
        .position
        .position_value
        .as_deref()
        .map(decimal)
        .transpose()?
        .map(|value| {
            value
                .abs()
                .checked_div(signed.abs())
                .ok_or(HyperliquidError::Payload)
        })
        .transpose()?
        .map(Price::new)
        .transpose()
        .map_err(|_| HyperliquidError::Payload)?;
    Ok(Some(Position {
        symbol: meta.scope.symbol().clone(),
        side: if signed.is_sign_negative() {
            PositionSide::Short
        } else {
            PositionSide::Long
        },
        quantity: signed.abs(),
        entry_price,
        mark_price,
    }))
}

fn flatten_frontend_rows(
    rows: Vec<FrontendOrderRow>,
) -> Result<Vec<FrontendOrderRow>, HyperliquidError> {
    if rows.len() > MAX_FRONTEND_ORDERS {
        return Err(HyperliquidError::OrderFamily);
    }
    let mut indexes = BTreeMap::new();
    let mut flattened = Vec::new();
    for row in rows {
        validate_frontend_row(&row, false)?;
        let children = row.children.clone();
        insert_frontend_row(&mut flattened, &mut indexes, row)?;
        for child in children {
            insert_frontend_row(&mut flattened, &mut indexes, child)?;
        }
    }
    if flattened.len() > MAX_FRONTEND_ORDERS {
        return Err(HyperliquidError::OrderFamily);
    }
    Ok(flattened)
}

fn insert_frontend_row(
    rows: &mut Vec<FrontendOrderRow>,
    indexes: &mut BTreeMap<u64, usize>,
    row: FrontendOrderRow,
) -> Result<(), HyperliquidError> {
    if let Some(index) = indexes.get(&row.oid) {
        if rows.get(*index) == Some(&row) {
            return Ok(());
        }
        return Err(HyperliquidError::OrderFamily);
    }
    indexes.insert(row.oid, rows.len());
    rows.push(row);
    Ok(())
}

fn validate_frontend_row(
    row: &FrontendOrderRow,
    child: bool,
) -> Result<HyperliquidOrderFamily, HyperliquidError> {
    if row.oid == 0
        || row.timestamp == 0
        || row.coin.trim().is_empty()
        || row.children.len() > MAX_FRONTEND_CHILDREN
    {
        return Err(HyperliquidError::OrderFamily);
    }
    let original = decimal(&row.orig_sz).map_err(|_| HyperliquidError::OrderFamily)?;
    let remaining = decimal(&row.sz).map_err(|_| HyperliquidError::OrderFamily)?;
    let limit_price = decimal(&row.limit_px).map_err(|_| HyperliquidError::OrderFamily)?;
    if original <= Decimal::ZERO
        || remaining <= Decimal::ZERO
        || remaining > original
        || limit_price <= Decimal::ZERO
    {
        return Err(HyperliquidError::OrderFamily);
    }
    if let Some(cloid) = &row.cloid {
        canonical_cloid(cloid.clone()).map_err(|_| HyperliquidError::OrderFamily)?;
    }

    let trigger_price = decimal(&row.trigger_px).map_err(|_| HyperliquidError::OrderFamily)?;
    let family = if row.is_trigger {
        if !matches!(
            row.order_type.as_str(),
            "Take Profit Market" | "Take Profit Limit" | "Stop Market" | "Stop Limit"
        ) || row.tif.is_some()
            || trigger_price <= Decimal::ZERO
            || !trigger_condition_matches(&row.trigger_condition, trigger_price)
            || !row.children.is_empty()
            || (row.is_position_tpsl && !row.reduce_only)
        {
            return Err(HyperliquidError::OrderFamily);
        }
        HyperliquidOrderFamily::Conditional
    } else {
        if row.is_position_tpsl
            || row.order_type != "Limit"
            || !matches!(row.tif.as_deref(), Some("Alo" | "Gtc"))
            || trigger_price != Decimal::ZERO
            || row.trigger_condition != "N/A"
        {
            return Err(HyperliquidError::OrderFamily);
        }
        HyperliquidOrderFamily::Regular
    };
    if child && family != HyperliquidOrderFamily::Conditional {
        return Err(HyperliquidError::OrderFamily);
    }
    let mut child_ids = BTreeMap::new();
    for nested in &row.children {
        if nested.coin != row.coin
            || validate_frontend_row(nested, true)? != HyperliquidOrderFamily::Conditional
            || child_ids.insert(nested.oid, ()).is_some()
        {
            return Err(HyperliquidError::OrderFamily);
        }
    }
    Ok(family)
}

fn trigger_condition_matches(value: &str, trigger_price: Decimal) -> bool {
    value
        .strip_prefix("Price above ")
        .or_else(|| value.strip_prefix("Price below "))
        .and_then(|raw| Decimal::from_str(raw).ok())
        .is_some_and(|condition_price| condition_price == trigger_price)
}

fn validate_status_order(row: &FrontendOrderRow) -> Result<(), HyperliquidError> {
    if row.oid == 0
        || row.timestamp == 0
        || row.coin.trim().is_empty()
        || row.children.len() > MAX_FRONTEND_CHILDREN
    {
        return Err(HyperliquidError::Payload);
    }
    let original = decimal(&row.orig_sz)?;
    let remaining = decimal(&row.sz)?;
    if original <= Decimal::ZERO
        || remaining < Decimal::ZERO
        || remaining > original
        || decimal(&row.limit_px)? <= Decimal::ZERO
    {
        return Err(HyperliquidError::Payload);
    }
    if let Some(cloid) = &row.cloid {
        canonical_cloid(cloid.clone())?;
    }
    if row.order_type == "Market" {
        if row.is_trigger
            || row.is_position_tpsl
            || row.tif.as_deref() != Some("FrontendMarket")
            || decimal(&row.trigger_px)? != Decimal::ZERO
            || row.trigger_condition != "N/A"
            || !row.children.is_empty()
        {
            return Err(HyperliquidError::Payload);
        }
        return Ok(());
    }
    let mut open_row = row.clone();
    if remaining.is_zero() {
        open_row.sz = original.to_string();
    }
    validate_frontend_row(&open_row, false).map_err(|_| HyperliquidError::Payload)?;
    Ok(())
}

pub(crate) fn canonical_cloid(value: String) -> Result<String, HyperliquidError> {
    match HyperliquidOrderLookup::client_order_id(value)? {
        HyperliquidOrderLookup::ClientOrderId(value) => Ok(value),
        HyperliquidOrderLookup::OrderId(_) => Err(HyperliquidError::Payload),
    }
}

fn normalize_open_order(
    row: FrontendOrderRow,
    symbol: &Symbol,
) -> Result<HyperliquidOpenOrder, HyperliquidError> {
    let family = validate_frontend_row(&row, false)?;
    let quantity = decimal(&row.orig_sz)?;
    let remaining = decimal(&row.sz)?;
    if quantity <= Decimal::ZERO || remaining <= Decimal::ZERO || remaining > quantity {
        return Err(HyperliquidError::Payload);
    }
    let filled_quantity = quantity
        .checked_sub(remaining)
        .ok_or(HyperliquidError::Payload)?;
    let trigger_price = match family {
        HyperliquidOrderFamily::Regular => None,
        HyperliquidOrderFamily::Conditional => {
            Some(Price::new(decimal(&row.trigger_px)?).map_err(|_| HyperliquidError::OrderFamily)?)
        }
    };
    let client_order_id = row.cloid.map(canonical_cloid).transpose()?;
    let child_order_ids = row.children.iter().map(|child| child.oid).collect();
    let order = Order {
        order_id: row.oid.to_string(),
        client_order_id: client_order_id
            .map(FieldState::Known)
            .unwrap_or(FieldState::Missing),
        symbol: symbol.clone(),
        side: side(&row.side)?,
        position_side: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        purpose: FieldState::Missing,
        state: if filled_quantity.is_zero() {
            OrderState::New
        } else {
            OrderState::PartiallyFilled
        },
        quantity,
        filled_quantity,
        limit_price: Some(
            Price::new(decimal(&row.limit_px)?).map_err(|_| HyperliquidError::Payload)?,
        ),
        average_price: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        reduce_only: row.reduce_only,
    };
    order.validate().map_err(|_| HyperliquidError::Payload)?;
    Ok(HyperliquidOpenOrder {
        order,
        exchange_time_ms: row.timestamp,
        family,
        native_order_type: row.order_type,
        time_in_force: row.tif,
        trigger_price,
        trigger_condition: row.trigger_condition,
        is_position_tpsl: row.is_position_tpsl,
        child_order_ids,
    })
}

pub(crate) fn normalize_fill(
    row: UserFillRow,
    scope: &HyperliquidPayloadScope,
) -> Result<HyperliquidFill, HyperliquidError> {
    if row.coin != scope.native_coin {
        return Err(HyperliquidError::Binding);
    }
    if row.oid == 0 || row.tid == 0 || row.time == 0 {
        return Err(HyperliquidError::Payload);
    }
    let quantity = decimal(&row.sz)?;
    if quantity <= Decimal::ZERO {
        return Err(HyperliquidError::Payload);
    }
    let fee_asset = Asset::new(&row.fee_token).map_err(|_| HyperliquidError::Payload)?;
    let fill = Fill {
        fill_id: format!(
            "hl:{}:{}:{}:{}",
            scope.user_address(),
            row.time,
            scope.native_coin,
            row.tid
        ),
        // Hyperliquid documents tid as a 50-bit hash, not a monotonic execution sequence.
        execution_sequence: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        order_id: row.oid.to_string(),
        symbol: scope.symbol().clone(),
        side: side(&row.side)?,
        position_side: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        quantity,
        price: Price::new(decimal(&row.px)?).map_err(|_| HyperliquidError::Payload)?,
        fee: FieldState::Known(venue_domain::domain::Amount::new(
            fee_asset.clone(),
            decimal(&row.fee)?.abs(),
        )),
        realized_pnl: FieldState::Known(venue_domain::domain::Amount::new(
            fee_asset,
            decimal(&row.closed_pnl)?,
        )),
        maker: FieldState::Known(!row.crossed),
        exchange_time_ms: Some(row.time),
    };
    fill.validate().map_err(|_| HyperliquidError::Payload)?;
    Ok(HyperliquidFill {
        fill,
        client_order_id: row
            .cloid
            .map(canonical_cloid)
            .transpose()?
            .map(FieldState::Known)
            .unwrap_or(FieldState::Missing),
    })
}

fn fill_cursor(
    row: &UserFillRow,
    scope: &HyperliquidPayloadScope,
) -> Result<HyperliquidFillCursor, HyperliquidError> {
    if row.coin.is_empty() || row.time == 0 || row.tid == 0 {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidFillCursor {
        scope: scope.clone(),
        time_ms: row.time,
        page_coin: row.coin.clone(),
        trade_id: row.tid,
    })
}

fn reject_duplicate_fills(fills: &[HyperliquidFill]) -> Result<(), HyperliquidError> {
    let mut ids = fills
        .iter()
        .map(|fill| fill.fill.fill_id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        Err(HyperliquidError::Payload)
    } else {
        Ok(())
    }
}

fn validate_coin_time(
    coin: &str,
    time: u64,
    scope: &HyperliquidPayloadScope,
) -> Result<(), HyperliquidError> {
    if coin != scope.native_coin {
        return Err(HyperliquidError::Binding);
    }
    if time == 0 {
        return Err(HyperliquidError::Payload);
    }
    Ok(())
}

fn normalize_level(level: &BookLevel) -> Result<MarketLevel, HyperliquidError> {
    let quantity = decimal(&level.sz)?;
    if quantity <= Decimal::ZERO {
        return Err(HyperliquidError::Payload);
    }
    Ok(MarketLevel {
        price: Price::new(decimal(&level.px)?).map_err(|_| HyperliquidError::Payload)?,
        quantity,
    })
}

fn bbo(
    meta: &HyperliquidPerpMeta,
    exchange_time_ms: u64,
    bid: MarketLevel,
    ask: MarketLevel,
) -> Result<HyperliquidBbo, HyperliquidError> {
    if bid.price.cmp(&ask.price) != Ordering::Less {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidBbo {
        scope: meta.scope.clone(),
        exchange_time_ms,
        bid,
        ask,
    })
}

pub(crate) fn side(value: &str) -> Result<OrderSide, HyperliquidError> {
    match value {
        "B" => Ok(OrderSide::Buy),
        "A" => Ok(OrderSide::Sell),
        _ => Err(HyperliquidError::Payload),
    }
}

pub(crate) fn decimal(value: &str) -> Result<Decimal, HyperliquidError> {
    Decimal::from_str(value).map_err(|_| HyperliquidError::Payload)
}

fn bound_user_request(
    kind: &str,
    scope: &HyperliquidPayloadScope,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    info_request(
        scope.binding(),
        serde_json::json!({"type": kind, "user": scope.user_address()}),
    )
}

fn info_request(
    binding: &HyperliquidReadBinding,
    body: serde_json::Value,
) -> Result<HyperliquidInfoRequest, HyperliquidError> {
    let config = HyperliquidConfig::for_binding(binding.gateway());
    Ok(HyperliquidInfoRequest {
        binding: binding.clone(),
        mode: config.mode(),
        rest_origin: config.rest_origin(),
        body: serde_json::to_vec(&body).map_err(|_| HyperliquidError::Payload)?,
    })
}

pub(crate) fn normalized_order_status(value: &str) -> Result<OrderState, HyperliquidError> {
    match value {
        "open" | "triggered" => Ok(OrderState::New),
        "filled" => Ok(OrderState::Filled),
        "canceled"
        | "marginCanceled"
        | "vaultWithdrawalCanceled"
        | "openInterestCapCanceled"
        | "selfTradeCanceled"
        | "reduceOnlyCanceled"
        | "siblingFilledCanceled"
        | "delistedCanceled"
        | "liquidatedCanceled"
        | "scheduledCancel" => Ok(OrderState::Cancelled),
        "rejected"
        | "tickRejected"
        | "minTradeNtlRejected"
        | "perpMarginRejected"
        | "reduceOnlyRejected"
        | "badAloPxRejected"
        | "iocCancelRejected"
        | "badTriggerPxRejected"
        | "marketOrderNoLiquidityRejected"
        | "positionIncreaseAtOpenInterestCapRejected"
        | "positionFlipAtOpenInterestCapRejected"
        | "tooAggressiveAtOpenInterestCapRejected"
        | "openInterestIncreaseRejected"
        | "insufficientSpotBalanceRejected"
        | "oracleRejected"
        | "perpMaxPositionRejected" => Ok(OrderState::Rejected),
        _ => Err(HyperliquidError::Payload),
    }
}
