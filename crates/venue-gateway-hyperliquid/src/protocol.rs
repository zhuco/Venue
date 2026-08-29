use std::{
    cmp::{Ordering, Reverse},
    str::FromStr,
};

use rust_decimal::Decimal;
use venue_domain::domain::{
    AccountBalance, Asset, FieldState, Fill, MarketLevel, Order, OrderSide, OrderState, Position,
    PositionSide, Price, Symbol, UnknownReason,
};
use venue_gateway_api::GatewayMode;

use crate::{
    HyperliquidConfig, HyperliquidError, HyperliquidReadBinding, endpoints,
    models::{
        BboData, BookData, BookLevel, ClearinghouseState, EventEnvelope, OpenOrderRow,
        OrderStatusEnvelope, PerpMetaResponse, UserFillRow, UserFillsData,
    },
};

const MAX_BOOK_LEVELS: usize = 20;
const MAX_FILL_PAGE: usize = 2_000;

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidOpenOrdersSnapshot {
    pub scope: HyperliquidPayloadScope,
    pub observed_at_ms: u64,
    pub orders: Vec<HyperliquidOpenOrder>,
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
            || !(1..=MAX_FILL_PAGE).contains(&self.limit)
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HyperliquidOrderLookup {
    OrderId(u64),
    ClientOrderId(String),
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
    Unknown {
        scope: HyperliquidPayloadScope,
    },
    Known {
        scope: HyperliquidPayloadScope,
        order_id: u64,
        client_order_id: FieldState<String>,
        state: OrderState,
        exchange_time_ms: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFillPage {
    pub scope: HyperliquidPayloadScope,
    pub fills: Vec<HyperliquidFill>,
    pub next_cursor: Option<HyperliquidFillCursor>,
    /// True only when the response was below the venue cap and the local limit did not truncate it.
    pub complete: bool,
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
    bound_user_request("openOrders", &meta.scope)
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
        }),
        ("order", Some(body)) => {
            if body.order.coin != meta.scope.native_coin {
                return Err(HyperliquidError::Binding);
            }
            if body.status_timestamp == 0 || body.order.oid == 0 {
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
            Ok(HyperliquidOrderStatus::Known {
                scope: meta.scope.clone(),
                order_id: body.order.oid,
                client_order_id: body
                    .order
                    .cloid
                    .filter(|value| !value.is_empty())
                    .map(FieldState::Known)
                    .unwrap_or(FieldState::Missing),
                state: normalized_order_status(&body.status)?,
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
    if observed_at_ms == 0 {
        return Err(HyperliquidError::Payload);
    }
    let rows: Vec<OpenOrderRow> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
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
    Ok(HyperliquidOpenOrdersSnapshot {
        scope: meta.scope.clone(),
        observed_at_ms,
        orders,
    })
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
        is_snapshot: data.is_snapshot,
        fills,
    })
}

pub fn parse_user_fills_page(
    payload: &[u8],
    meta: &HyperliquidPerpMeta,
    query: &HyperliquidFillQuery,
) -> Result<HyperliquidFillPage, HyperliquidError> {
    query.validate()?;
    if query.scope != meta.scope {
        return Err(HyperliquidError::Binding);
    }
    let rows: Vec<UserFillRow> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if rows.len() > MAX_FILL_PAGE {
        return Err(HyperliquidError::Payload);
    }
    let capped = rows.len() == MAX_FILL_PAGE;
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
        if row.coin != meta.scope.native_coin {
            continue;
        }
        fills.push(normalize_fill(row, &meta.scope)?);
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
    })
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
    Ok(Some(Position {
        symbol: meta.scope.symbol().clone(),
        side: if signed.is_sign_negative() {
            PositionSide::Short
        } else {
            PositionSide::Long
        },
        quantity: signed.abs(),
        entry_price,
        mark_price: None,
    }))
}

fn normalize_open_order(
    row: OpenOrderRow,
    symbol: &Symbol,
) -> Result<HyperliquidOpenOrder, HyperliquidError> {
    if row.timestamp == 0 {
        return Err(HyperliquidError::Payload);
    }
    let quantity = decimal(&row.orig_sz)?;
    let remaining = decimal(&row.sz)?;
    if quantity <= Decimal::ZERO || remaining <= Decimal::ZERO || remaining > quantity {
        return Err(HyperliquidError::Payload);
    }
    let filled_quantity = quantity
        .checked_sub(remaining)
        .ok_or(HyperliquidError::Payload)?;
    let order_id = match row.oid {
        serde_json::Value::Number(value) => value.as_u64().map(|value| value.to_string()),
        serde_json::Value::String(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
    .ok_or(HyperliquidError::Payload)?;
    let order = Order {
        order_id,
        client_order_id: row
            .cloid
            .filter(|value| !value.is_empty())
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
    })
}

fn normalize_fill(
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
            .filter(|value| !value.is_empty())
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

fn side(value: &str) -> Result<OrderSide, HyperliquidError> {
    match value {
        "B" => Ok(OrderSide::Buy),
        "A" => Ok(OrderSide::Sell),
        _ => Err(HyperliquidError::Payload),
    }
}

fn decimal(value: &str) -> Result<Decimal, HyperliquidError> {
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

fn normalized_order_status(value: &str) -> Result<OrderState, HyperliquidError> {
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
