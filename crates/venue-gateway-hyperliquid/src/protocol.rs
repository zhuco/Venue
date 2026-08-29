use std::{cmp::Ordering, str::FromStr};

use rust_decimal::Decimal;
use venue_domain::domain::{
    AccountBalance, Asset, FieldState, Fill, MarketLevel, Order, OrderSide, OrderState, Position,
    PositionSide, Price, Symbol, UnknownReason,
};
use venue_gateway_api::GatewayMode;

use crate::{
    HyperliquidError, HyperliquidReadBinding,
    models::{
        BboData, BookData, BookLevel, ClearinghouseState, EventEnvelope, OpenOrderRow,
        PerpMetaResponse, UserFillRow, UserFillsData,
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

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct HyperliquidFillCursor {
    pub time_ms: u64,
    pub native_coin: String,
    pub trade_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFillQuery {
    pub begin_ms: u64,
    pub end_ms: u64,
    pub limit: usize,
    pub after: Option<HyperliquidFillCursor>,
}

impl HyperliquidFillQuery {
    pub fn validate(&self) -> Result<(), HyperliquidError> {
        if self.begin_ms == 0
            || self.end_ms < self.begin_ms
            || !(1..=MAX_FILL_PAGE).contains(&self.limit)
            || self.after.as_ref().is_some_and(|cursor| {
                cursor.time_ms < self.begin_ms || cursor.time_ms > self.end_ms
            })
        {
            return Err(HyperliquidError::Payload);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFillPage {
    pub scope: HyperliquidPayloadScope,
    pub fills: Vec<HyperliquidFill>,
    pub next_cursor: Option<HyperliquidFillCursor>,
    /// True only when the response was below the venue cap and the local limit did not truncate it.
    pub complete: bool,
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
    bids.sort_by(|left, right| right.price.cmp(&left.price));
    asks.sort_by(|left, right| left.price.cmp(&right.price));
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
            let cursor = fill_cursor(&row)?;
            Ok((cursor, row))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    if rows.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(HyperliquidError::Payload);
    }
    let mut fills = Vec::new();
    let mut last_consumed = None;
    let mut truncated = false;
    for (cursor, row) in rows
        .into_iter()
        .filter(|(cursor, _)| query.after.as_ref().is_none_or(|after| cursor > after))
    {
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

fn fill_cursor(row: &UserFillRow) -> Result<HyperliquidFillCursor, HyperliquidError> {
    if row.coin.is_empty() || row.time == 0 || row.tid == 0 {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidFillCursor {
        time_ms: row.time,
        native_coin: row.coin.clone(),
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
