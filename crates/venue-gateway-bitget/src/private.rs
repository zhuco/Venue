//! Pure Bitget UTA private order and fill protocol parsing.
//!
//! This module owns no credentials, transport, evidence journal, capability, or mutation writer.
//! Callers bind each exact signed response to an attempt and request cursor before parsing, then
//! persist the [`BitgetRawPrivatePage`] as evidence outside this crate.

use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    AccountBalance, Amount, Asset, FieldState, Fill, Order, OrderSide, OrderState, Position,
    PositionSide, Price, Symbol, UnknownReason,
};
use venue_gateway_api::GatewayBinding;

use crate::{BitgetAccountBinding, account, public};

pub const BITGET_PRIVATE_PARSER_SCHEMA_VERSION: u16 = 1;
pub const BITGET_UTA_FUTURES_CATEGORY: &str = "USDT-FUTURES";
pub const BITGET_PRIVATE_PAGE_SIZE: usize = 100;
pub const BITGET_MAX_PRIVATE_PAGES: usize = 900;
pub const BITGET_MAX_FILL_HISTORY_WINDOW_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitgetPrivateSurface {
    Account,
    Settings,
    Positions,
    RegularOrders,
    Fills,
}

/// Request-bound raw evidence for one signed private REST page.
///
/// `attempt_id` prevents successful faces from different reconciliation turns being relabelled as
/// one generation. `request_cursor` and `page_index` bind the payload to its exact pagination
/// request; [`complete_regular_order_pages`] and [`complete_fill_pages`] verify the full chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BitgetRawPrivatePage {
    pub parser_schema_version: u16,
    pub surface: BitgetPrivateSurface,
    pub binding: GatewayBinding,
    pub native_symbol: String,
    pub attempt_id: u64,
    pub generation: u64,
    pub page_index: u32,
    pub request_cursor: Option<String>,
    /// Effective signed `startTime` for fill history after window clamping. Other surfaces must
    /// leave this absent; every page in one fill face must carry the same value.
    pub fill_history_start_ms: Option<u64>,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: String,
}

impl BitgetRawPrivatePage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        surface: BitgetPrivateSurface,
        binding: GatewayBinding,
        attempt_id: u64,
        page_index: u32,
        request_cursor: Option<String>,
        fill_history_start_ms: Option<u64>,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, BitgetPrivateError> {
        Self::new_with_generation(
            surface,
            binding,
            attempt_id,
            attempt_id,
            page_index,
            request_cursor,
            fill_history_start_ms,
            received_at_ms,
            payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_generation(
        surface: BitgetPrivateSurface,
        binding: GatewayBinding,
        attempt_id: u64,
        generation: u64,
        page_index: u32,
        request_cursor: Option<String>,
        fill_history_start_ms: Option<u64>,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, BitgetPrivateError> {
        validate_binding(&binding)?;
        let native_symbol = native_symbol(&binding.symbol)?;
        let payload_sha256 = payload_digest(&payload);
        let page = Self {
            parser_schema_version: BITGET_PRIVATE_PARSER_SCHEMA_VERSION,
            surface,
            binding,
            native_symbol,
            attempt_id,
            generation,
            page_index,
            request_cursor,
            fill_history_start_ms,
            received_at_ms,
            payload_sha256,
            payload,
        };
        page.validate()?;
        Ok(page)
    }

    pub fn validate(&self) -> Result<(), BitgetPrivateError> {
        if self.parser_schema_version != BITGET_PRIVATE_PARSER_SCHEMA_VERSION
            || self.attempt_id == 0
            || self.generation == 0
            || self.received_at_ms == 0
            || self.payload.is_empty()
            || self
                .request_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.is_empty())
            || self.fill_history_start_ms == Some(0)
            || (self.surface != BitgetPrivateSurface::Fills && self.fill_history_start_ms.is_some())
            || validate_binding(&self.binding).is_err()
            || self.native_symbol != native_symbol(&self.binding.symbol)?
            || self.payload_sha256 != payload_digest(&self.payload)
        {
            return Err(BitgetPrivateError::Metadata);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRegularOrderPage {
    pub raw: BitgetRawPrivatePage,
    pub orders: Vec<Order>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetFillPage {
    pub raw: BitgetRawPrivatePage,
    pub fills: Vec<BitgetFill>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetAccountFace {
    pub raw: BitgetRawPrivatePage,
    pub balance: AccountBalance,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetSettingsFace {
    pub raw: BitgetRawPrivatePage,
    pub hedge_mode: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetPositionsFace {
    pub raw: BitgetRawPrivatePage,
    pub positions: Vec<Position>,
}

/// A successfully parsed private surface. The turn aggregator still requires exactly one of each
/// variant and reparses every raw response before producing a generation candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitgetPrivateFace {
    Account(BitgetAccountFace),
    Settings(BitgetSettingsFace),
    Positions(BitgetPositionsFace),
    RegularOrders(Vec<BitgetRegularOrderPage>),
    Fills(Vec<BitgetFillPage>),
}

/// Complete normalized facts from one five-surface signed read attempt.
///
/// This is deliberately named a candidate: it is not capability evidence, a private-generation
/// receipt, mutation authority, or proof that the surrounding transport actually persisted the
/// raw pages. The runtime must durably record and admit it before consuming these facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetPrivateGenerationCandidate {
    pub binding: GatewayBinding,
    pub attempt_id: u64,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub raw_pages: Vec<BitgetRawPrivatePage>,
    pub balance: AccountBalance,
    pub hedge_mode: bool,
    pub positions: Vec<Position>,
    pub orders: Vec<Order>,
    pub fills: Vec<BitgetFill>,
}

pub fn parse_account_face(
    raw: BitgetRawPrivatePage,
) -> Result<BitgetAccountFace, BitgetPrivateError> {
    require_single_surface(&raw, BitgetPrivateSurface::Account)?;
    let data = successful_data(&raw.payload)?;
    let balance = account::parse_balance(&data).map_err(|_| BitgetPrivateError::Payload)?;
    Ok(BitgetAccountFace { raw, balance })
}

pub fn parse_settings_face(
    raw: BitgetRawPrivatePage,
) -> Result<BitgetSettingsFace, BitgetPrivateError> {
    require_single_surface(&raw, BitgetPrivateSurface::Settings)?;
    let data = successful_data(&raw.payload)?;
    let hedge_mode = account::is_hedge_mode(&data).map_err(|_| BitgetPrivateError::Payload)?;
    Ok(BitgetSettingsFace { raw, hedge_mode })
}

pub fn parse_positions_face(
    raw: BitgetRawPrivatePage,
) -> Result<BitgetPositionsFace, BitgetPrivateError> {
    require_single_surface(&raw, BitgetPrivateSurface::Positions)?;
    let data = successful_data(&raw.payload)?;
    let (rows, cursor) = list_and_cursor(&data)?;
    if cursor.is_some() {
        return Err(BitgetPrivateError::Cursor);
    }
    let mut sides = BTreeSet::new();
    let mut positions = rows
        .iter()
        .filter(|row| row.get("symbol").and_then(Value::as_str) == Some(raw.native_symbol.as_str()))
        .map(|row| {
            let position = account::parse_position(row, &raw.binding.symbol)
                .map_err(|_| BitgetPrivateError::Payload)?;
            if !sides.insert(position.side) {
                return Err(BitgetPrivateError::DuplicateFact);
            }
            Ok(position)
        })
        .collect::<Result<Vec<_>, BitgetPrivateError>>()?;
    for side in [PositionSide::Long, PositionSide::Short] {
        if !sides.contains(&side) {
            positions.push(Position {
                symbol: raw.binding.symbol.clone(),
                side,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: None,
            });
        }
    }
    positions.sort_by_key(|position| match position.side {
        PositionSide::Long => 0,
        PositionSide::Short => 1,
        PositionSide::Net => 2,
    });
    Ok(BitgetPositionsFace { raw, positions })
}

/// Atomically validates all five private read surfaces from one exact signed attempt.
/// Missing, duplicate, failed, cross-attempt, cross-binding, or incomplete paginated faces reject
/// the entire turn; no successful subset is returned for reuse by another attempt.
pub fn complete_private_turn(
    faces: Vec<BitgetPrivateFace>,
) -> Result<BitgetPrivateGenerationCandidate, BitgetPrivateError> {
    let mut account_face = None;
    let mut settings_face = None;
    let mut positions_face = None;
    let mut order_pages = None;
    let mut fill_pages = None;
    for face in faces {
        match face {
            BitgetPrivateFace::Account(face) => {
                install_face(&mut account_face, face, BitgetPrivateSurface::Account)?;
            }
            BitgetPrivateFace::Settings(face) => {
                install_face(&mut settings_face, face, BitgetPrivateSurface::Settings)?;
            }
            BitgetPrivateFace::Positions(face) => {
                install_face(&mut positions_face, face, BitgetPrivateSurface::Positions)?;
            }
            BitgetPrivateFace::RegularOrders(pages) => {
                install_face(&mut order_pages, pages, BitgetPrivateSurface::RegularOrders)?;
            }
            BitgetPrivateFace::Fills(pages) => {
                install_face(&mut fill_pages, pages, BitgetPrivateSurface::Fills)?;
            }
        }
    }

    let account_face = account_face.ok_or(BitgetPrivateError::MissingFace(
        BitgetPrivateSurface::Account,
    ))?;
    let settings_face = settings_face.ok_or(BitgetPrivateError::MissingFace(
        BitgetPrivateSurface::Settings,
    ))?;
    let positions_face = positions_face.ok_or(BitgetPrivateError::MissingFace(
        BitgetPrivateSurface::Positions,
    ))?;
    let order_pages = order_pages.ok_or(BitgetPrivateError::MissingFace(
        BitgetPrivateSurface::RegularOrders,
    ))?;
    let fill_pages =
        fill_pages.ok_or(BitgetPrivateError::MissingFace(BitgetPrivateSurface::Fills))?;

    let reparsed_account = parse_account_face(account_face.raw.clone())?;
    let reparsed_settings = parse_settings_face(settings_face.raw.clone())?;
    let reparsed_positions = parse_positions_face(positions_face.raw.clone())?;
    if reparsed_account != account_face
        || reparsed_settings != settings_face
        || reparsed_positions != positions_face
    {
        return Err(BitgetPrivateError::Projection);
    }
    if !settings_face.hedge_mode {
        return Err(BitgetPrivateError::PositionMode);
    }

    let orders = complete_regular_order_pages(&order_pages)?;
    let fills = complete_fill_pages(&fill_pages)?;
    let binding = account_face.raw.binding.clone();
    let attempt_id = account_face.raw.attempt_id;
    let generation = account_face.raw.generation;
    let mut raw_pages = vec![account_face.raw, settings_face.raw, positions_face.raw];
    raw_pages.extend(order_pages.into_iter().map(|page| page.raw));
    raw_pages.extend(fill_pages.into_iter().map(|page| page.raw));
    if raw_pages.iter().any(|raw| {
        raw.binding != binding
            || raw.attempt_id != attempt_id
            || raw.generation != generation
            || raw.validate().is_err()
    }) {
        return Err(BitgetPrivateError::MixedAttempt);
    }
    let observed_at_ms = raw_pages.iter().map(|raw| raw.received_at_ms).max().ok_or(
        BitgetPrivateError::MissingFace(BitgetPrivateSurface::Account),
    )?;
    Ok(BitgetPrivateGenerationCandidate {
        binding,
        attempt_id,
        generation,
        observed_at_ms,
        raw_pages,
        balance: account_face.balance,
        hedge_mode: settings_face.hedge_mode,
        positions: positions_face.positions,
        orders,
        fills,
    })
}

/// Parses one signed UTA `unfilled-orders` page. That endpoint includes every active delegate
/// type; this regular-only mutation profile accepts only `delegateType=normal`, so any strategy
/// row fails the whole candidate instead of being silently omitted from a claimed empty family.
pub fn parse_regular_order_page(
    raw: BitgetRawPrivatePage,
) -> Result<BitgetRegularOrderPage, BitgetPrivateError> {
    require_surface(&raw, BitgetPrivateSurface::RegularOrders)?;
    let data = successful_data(&raw.payload)?;
    let (rows, next_cursor) = list_and_cursor(&data)?;
    let orders = rows
        .iter()
        .map(|row| parse_regular_order(row, &raw.binding.symbol))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BitgetRegularOrderPage {
        raw,
        orders,
        next_cursor,
    })
}

/// Parses exactly one signed fill-history page. Client identity is retained separately because it
/// is not part of the canonical [`Fill`] but is required for deterministic Owner attribution.
pub fn parse_fill_page(raw: BitgetRawPrivatePage) -> Result<BitgetFillPage, BitgetPrivateError> {
    require_surface(&raw, BitgetPrivateSurface::Fills)?;
    let data = successful_data(&raw.payload)?;
    let (rows, next_cursor) = list_and_cursor(&data)?;
    let fills = rows
        .iter()
        .map(|row| {
            Ok(BitgetFill {
                fill: parse_fill(row, &raw.binding.symbol)?,
                client_order_id: client_order_id(row.get("clientOid")),
            })
        })
        .collect::<Result<Vec<_>, BitgetPrivateError>>()?;
    Ok(BitgetFillPage {
        raw,
        fills,
        next_cursor,
    })
}

/// Proves a closed, single-attempt regular-order pagination chain and rejects duplicate venue IDs.
pub fn complete_regular_order_pages(
    pages: &[BitgetRegularOrderPage],
) -> Result<Vec<Order>, BitgetPrivateError> {
    for page in pages {
        let reparsed = parse_regular_order_page(page.raw.clone())?;
        if reparsed.orders != page.orders || reparsed.next_cursor != page.next_cursor {
            return Err(BitgetPrivateError::Pagination);
        }
    }
    validate_page_chain(
        pages.iter().map(|page| (&page.raw, &page.next_cursor)),
        BitgetPrivateSurface::RegularOrders,
    )?;
    let mut seen = BTreeSet::new();
    let mut orders = Vec::new();
    for order in pages.iter().flat_map(|page| &page.orders) {
        if !seen.insert(order.order_id.clone()) {
            return Err(BitgetPrivateError::Pagination);
        }
        orders.push(order.clone());
    }
    Ok(orders)
}

/// Proves a closed, single-attempt fill pagination chain and rejects duplicate execution IDs.
pub fn complete_fill_pages(
    pages: &[BitgetFillPage],
) -> Result<Vec<BitgetFill>, BitgetPrivateError> {
    for page in pages {
        let reparsed = parse_fill_page(page.raw.clone())?;
        if reparsed.fills != page.fills || reparsed.next_cursor != page.next_cursor {
            return Err(BitgetPrivateError::Pagination);
        }
    }
    validate_page_chain(
        pages.iter().map(|page| (&page.raw, &page.next_cursor)),
        BitgetPrivateSurface::Fills,
    )?;
    let mut seen = BTreeSet::new();
    let mut fills = Vec::new();
    for fill in pages.iter().flat_map(|page| &page.fills) {
        if !seen.insert(fill.fill.fill_id.clone()) {
            return Err(BitgetPrivateError::Pagination);
        }
        fills.push(fill.clone());
    }
    Ok(fills)
}

/// Builds the signed regular-order query for a known canonical symbol. It performs no I/O.
pub fn regular_orders_query(
    symbol: &Symbol,
    cursor: Option<&str>,
) -> Result<String, BitgetPrivateError> {
    let native = native_symbol(symbol)?;
    let mut query = format!(
        "category={BITGET_UTA_FUTURES_CATEGORY}&symbol={native}&limit={BITGET_PRIVATE_PAGE_SIZE}"
    );
    push_cursor(&mut query, cursor)?;
    Ok(query)
}

/// Builds a bounded signed fill-history query using a caller-supplied server-time observation.
pub fn fill_history_query(
    requested_start_ms: Option<u64>,
    cursor: Option<&str>,
    server_now_ms: u64,
) -> Result<String, BitgetPrivateError> {
    if server_now_ms == 0 {
        return Err(BitgetPrivateError::Clock);
    }
    let mut query =
        format!("category={BITGET_UTA_FUTURES_CATEGORY}&limit={BITGET_PRIVATE_PAGE_SIZE}");
    if let Some(requested_start_ms) = requested_start_ms {
        let earliest = server_now_ms.saturating_sub(BITGET_MAX_FILL_HISTORY_WINDOW_MS);
        query.push_str("&startTime=");
        query.push_str(&requested_start_ms.max(earliest).to_string());
    }
    push_cursor(&mut query, cursor)?;
    Ok(query)
}

pub fn parse_regular_order(value: &Value, symbol: &Symbol) -> Result<Order, BitgetPrivateError> {
    if object(value)?.get("delegateType").and_then(Value::as_str) != Some("normal") {
        return Err(BitgetPrivateError::OrderFamily);
    }
    parse_order(value, symbol)
}

pub fn parse_fill(value: &Value, symbol: &Symbol) -> Result<Fill, BitgetPrivateError> {
    let object = object(value)?;
    validate_symbol_category(object, symbol)?;
    let fee = object
        .get("feeDetail")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
        .map(parse_fee)
        .transpose()?
        .unwrap_or(FieldState::Missing);
    let fill_id = identifier(object.get("execId"))?;
    let fill = Fill {
        execution_sequence: fill_id.parse::<u64>().map(FieldState::Known).unwrap_or(
            FieldState::Unavailable {
                reason: UnknownReason::ParseFailure,
            },
        ),
        fill_id,
        order_id: identifier(object.get("orderId"))?,
        symbol: symbol.clone(),
        side: parse_side(text(object, "side")?)?,
        position_side: FieldState::Known(parse_position_side(
            text(object, "holdSide").or_else(|_| text(object, "posSide"))?,
        )?),
        quantity: decimal(object, "execQty")?,
        price: Price::new(decimal(object, "execPrice")?)
            .map_err(|_| BitgetPrivateError::Payload)?,
        fee,
        realized_pnl: amount_state(object.get("execPnl"))?,
        maker: match object.get("tradeScope").and_then(Value::as_str) {
            Some("maker") => FieldState::Known(true),
            Some("taker") => FieldState::Known(false),
            Some(_) => FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
            None => FieldState::Missing,
        },
        exchange_time_ms: optional_timestamp_ms(
            object
                .get("execTime")
                .or_else(|| object.get("updatedTime"))
                .or_else(|| object.get("createdTime")),
        )?,
    };
    fill.validate().map_err(|_| BitgetPrivateError::Payload)?;
    Ok(fill)
}

fn parse_order(value: &Value, symbol: &Symbol) -> Result<Order, BitgetPrivateError> {
    let object = object(value)?;
    validate_symbol_category(object, symbol)?;
    let position_side = parse_position_side(text(object, "posSide")?)?;
    let side = parse_side(text(object, "side")?)?;
    let time_in_force = match object.get("timeInForce") {
        Some(Value::String(value)) => match value.as_str() {
            "post_only" => FieldState::Known(venue_domain::domain::LimitTimeInForce::PostOnly),
            "gtc" => FieldState::Known(venue_domain::domain::LimitTimeInForce::Gtc),
            _ => FieldState::Unavailable {
                reason: UnknownReason::Ambiguous,
            },
        },
        Some(Value::Null) => FieldState::Null,
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
        None => FieldState::Missing,
    };
    let order = Order {
        order_id: identifier(object.get("orderId"))?,
        client_order_id: client_order_id(object.get("clientOid")),
        symbol: symbol.clone(),
        side,
        position_side: FieldState::Known(position_side),
        purpose: FieldState::Missing,
        state: parse_order_state(text(object, "orderStatus")?)?,
        quantity: decimal(object, "qty")?,
        filled_quantity: account::optional_decimal(object.get("cumExecQty"))
            .map_err(|_| BitgetPrivateError::Payload)?,
        limit_price: account::optional_price(object.get("price"))
            .map_err(|_| BitgetPrivateError::Payload)?,
        time_in_force,
        average_price: optional_price_state(object.get("avgPrice"))?,
        reduce_only: parse_reduce_only(object, position_side, side)?,
    };
    order.validate().map_err(|_| BitgetPrivateError::Payload)?;
    Ok(order)
}

fn validate_page_chain<'a>(
    pages: impl Iterator<Item = (&'a BitgetRawPrivatePage, &'a Option<String>)>,
    surface: BitgetPrivateSurface,
) -> Result<(), BitgetPrivateError> {
    let pages = pages.collect::<Vec<_>>();
    let Some((first, _)) = pages.first() else {
        return Err(BitgetPrivateError::Pagination);
    };
    if pages.len() > BITGET_MAX_PRIVATE_PAGES {
        return Err(BitgetPrivateError::Pagination);
    }
    let attempt_id = first.attempt_id;
    let generation = first.generation;
    let binding = &first.binding;
    let fill_history_start_ms = first.fill_history_start_ms;
    let mut expected_cursor: Option<&str> = None;
    let mut seen_response_cursors = BTreeSet::new();
    for (index, (raw, next_cursor)) in pages.iter().enumerate() {
        raw.validate()?;
        if raw.surface != surface
            || raw.attempt_id != attempt_id
            || raw.generation != generation
            || &raw.binding != binding
            || raw.fill_history_start_ms != fill_history_start_ms
            || usize::try_from(raw.page_index).ok() != Some(index)
            || raw.request_cursor.as_deref() != expected_cursor
        {
            return Err(BitgetPrivateError::Pagination);
        }
        if let Some(cursor) = next_cursor
            && (cursor.is_empty() || !seen_response_cursors.insert(cursor.as_str()))
        {
            return Err(BitgetPrivateError::Pagination);
        }
        expected_cursor = next_cursor.as_deref();
    }
    if expected_cursor.is_some() {
        return Err(BitgetPrivateError::Pagination);
    }
    Ok(())
}

fn require_surface(
    raw: &BitgetRawPrivatePage,
    expected: BitgetPrivateSurface,
) -> Result<(), BitgetPrivateError> {
    raw.validate()?;
    if raw.surface != expected {
        return Err(BitgetPrivateError::Surface);
    }
    Ok(())
}

fn require_single_surface(
    raw: &BitgetRawPrivatePage,
    expected: BitgetPrivateSurface,
) -> Result<(), BitgetPrivateError> {
    require_surface(raw, expected)?;
    if raw.page_index != 0 || raw.request_cursor.is_some() {
        return Err(BitgetPrivateError::Pagination);
    }
    Ok(())
}

fn install_face<T>(
    slot: &mut Option<T>,
    value: T,
    surface: BitgetPrivateSurface,
) -> Result<(), BitgetPrivateError> {
    if slot.replace(value).is_some() {
        return Err(BitgetPrivateError::DuplicateFace(surface));
    }
    Ok(())
}

fn successful_data(payload: &str) -> Result<Value, BitgetPrivateError> {
    let value = serde_json::from_str::<Value>(payload).map_err(|_| BitgetPrivateError::Payload)?;
    let object = value.as_object().ok_or(BitgetPrivateError::Payload)?;
    if object.get("code").and_then(Value::as_str) != Some("00000") {
        return Err(BitgetPrivateError::Rejected);
    }
    object
        .get("data")
        .cloned()
        .ok_or(BitgetPrivateError::Payload)
}

fn list_and_cursor(value: &Value) -> Result<(&[Value], Option<String>), BitgetPrivateError> {
    match value {
        Value::Array(values) => Ok((values, None)),
        Value::Object(object) => {
            let rows = match object.get("list") {
                Some(Value::Array(values)) => values.as_slice(),
                Some(Value::Null) => &[],
                _ => return Err(BitgetPrivateError::Payload),
            };
            let cursor = match object.get("cursor") {
                None | Some(Value::Null) => None,
                Some(Value::String(cursor)) if !cursor.is_empty() => Some(cursor.clone()),
                _ => return Err(BitgetPrivateError::Cursor),
            };
            Ok((rows, cursor))
        }
        _ => Err(BitgetPrivateError::Payload),
    }
}

fn validate_symbol_category(
    object: &Map<String, Value>,
    symbol: &Symbol,
) -> Result<(), BitgetPrivateError> {
    if text(object, "symbol")? != native_symbol(symbol)?
        || !text(object, "category")?.eq_ignore_ascii_case(BITGET_UTA_FUTURES_CATEGORY)
    {
        return Err(BitgetPrivateError::Symbol);
    }
    Ok(())
}

fn parse_reduce_only(
    object: &Map<String, Value>,
    position_side: PositionSide,
    side: OrderSide,
) -> Result<bool, BitgetPrivateError> {
    if object.get("holdMode").and_then(Value::as_str) == Some("hedge_mode") {
        let inferred = is_close(position_side, side)?;
        let directional_side = position_side_text(position_side)?;
        return match object.get("tradeSide").and_then(Value::as_str) {
            Some("close") if inferred => Ok(true),
            Some(value)
                if inferred
                    && value
                        .strip_prefix("close_")
                        .is_some_and(|value| value == directional_side) =>
            {
                Ok(true)
            }
            Some("open") if !inferred => Ok(false),
            Some(value)
                if !inferred
                    && value
                        .strip_prefix("open_")
                        .is_some_and(|value| value == directional_side) =>
            {
                Ok(false)
            }
            Some(_) => Err(BitgetPrivateError::DirectionalOrder),
            None => Ok(inferred),
        };
    }
    match object.get("tradeSide").and_then(Value::as_str) {
        Some("open") => Ok(false),
        Some("close") => Ok(true),
        Some(_) => Err(BitgetPrivateError::DirectionalOrder),
        None => match object.get("reduceOnly").and_then(Value::as_str) {
            Some(value) if value.eq_ignore_ascii_case("yes") => Ok(true),
            Some(value) if value.eq_ignore_ascii_case("no") => Ok(false),
            _ => Err(BitgetPrivateError::DirectionalOrder),
        },
    }
}

fn is_close(position_side: PositionSide, side: OrderSide) -> Result<bool, BitgetPrivateError> {
    match (position_side, side) {
        (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy) => Ok(true),
        (PositionSide::Long, OrderSide::Buy) | (PositionSide::Short, OrderSide::Sell) => Ok(false),
        (PositionSide::Net, _) => Err(BitgetPrivateError::DirectionalOrder),
    }
}

fn position_side_text(value: PositionSide) -> Result<&'static str, BitgetPrivateError> {
    match value {
        PositionSide::Long => Ok("long"),
        PositionSide::Short => Ok("short"),
        PositionSide::Net => Err(BitgetPrivateError::DirectionalOrder),
    }
}

fn parse_fee(value: &Value) -> Result<FieldState<Amount>, BitgetPrivateError> {
    let object = object(value)?;
    Ok(FieldState::Known(Amount::new(
        Asset::new(text(object, "feeCoin")?).map_err(|_| BitgetPrivateError::Payload)?,
        decimal(object, "fee")?.abs(),
    )))
}

fn optional_price_state(value: Option<&Value>) -> Result<FieldState<Price>, BitgetPrivateError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value == "0" || value.is_empty() => {
            Ok(FieldState::Unavailable {
                reason: UnknownReason::VenueUnavailable,
            })
        }
        value => Price::new(decimal_value(value)?)
            .map(FieldState::Known)
            .map_err(|_| BitgetPrivateError::Payload),
    }
}

fn amount_state(value: Option<&Value>) -> Result<FieldState<Amount>, BitgetPrivateError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value.is_empty() => Ok(FieldState::Unavailable {
            reason: UnknownReason::VenueUnavailable,
        }),
        value => Ok(FieldState::Known(Amount::new(
            Asset::new("USDT").map_err(|_| BitgetPrivateError::Payload)?,
            decimal_value(value)?,
        ))),
    }
}

fn optional_timestamp_ms(value: Option<&Value>) -> Result<Option<u64>, BitgetPrivateError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => value
            .parse()
            .map(Some)
            .map_err(|_| BitgetPrivateError::Clock),
        Some(Value::Number(value)) => value
            .to_string()
            .parse()
            .map(Some)
            .map_err(|_| BitgetPrivateError::Clock),
        _ => Err(BitgetPrivateError::Clock),
    }
}

fn push_cursor(query: &mut String, cursor: Option<&str>) -> Result<(), BitgetPrivateError> {
    if let Some(cursor) = cursor {
        if cursor.is_empty() {
            return Err(BitgetPrivateError::Cursor);
        }
        query.push_str("&cursor=");
        query.push_str(&encode_query_component(cursor));
    }
    Ok(())
}

fn encode_query_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn native_symbol(symbol: &Symbol) -> Result<String, BitgetPrivateError> {
    public::native_symbol(symbol).map_err(|_| BitgetPrivateError::Symbol)
}

fn validate_binding(binding: &GatewayBinding) -> Result<(), BitgetPrivateError> {
    BitgetAccountBinding::UtaUsdtFuturesHedge
        .validate_gateway_binding(binding)
        .map_err(|_| BitgetPrivateError::Metadata)
}

fn object(value: &Value) -> Result<&Map<String, Value>, BitgetPrivateError> {
    value.as_object().ok_or(BitgetPrivateError::Payload)
}

fn text<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, BitgetPrivateError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(BitgetPrivateError::Payload)
}

fn identifier(value: Option<&Value>) -> Result<String, BitgetPrivateError> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(BitgetPrivateError::Payload),
    }
}

fn decimal(object: &Map<String, Value>, field: &str) -> Result<Decimal, BitgetPrivateError> {
    decimal_value(object.get(field))
}

fn decimal_value(value: Option<&Value>) -> Result<Decimal, BitgetPrivateError> {
    crate::risk::decimal_value(value).map_err(|_| BitgetPrivateError::Payload)
}

fn parse_side(value: &str) -> Result<OrderSide, BitgetPrivateError> {
    match value {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(BitgetPrivateError::Payload),
    }
}

fn parse_position_side(value: &str) -> Result<PositionSide, BitgetPrivateError> {
    crate::risk::parse_position_side(value).map_err(|_| BitgetPrivateError::Payload)
}

fn parse_order_state(value: &str) -> Result<OrderState, BitgetPrivateError> {
    match value {
        "live" | "new" => Ok(OrderState::New),
        "partially_filled" => Ok(OrderState::PartiallyFilled),
        "filled" => Ok(OrderState::Filled),
        "cancelled" => Ok(OrderState::Cancelled),
        "rejected" => Ok(OrderState::Rejected),
        _ => Err(BitgetPrivateError::Payload),
    }
}

fn client_order_id(value: Option<&Value>) -> FieldState<String> {
    match value.and_then(Value::as_str) {
        Some(value) if !value.is_empty() => FieldState::Known(value.to_owned()),
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::Ambiguous,
        },
        None => FieldState::Missing,
    }
}

fn payload_digest(payload: &str) -> String {
    Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetPrivateError {
    #[error("Bitget private raw-page metadata is invalid")]
    Metadata,
    #[error("Bitget private page was parsed as the wrong surface")]
    Surface,
    #[error("Bitget rejected the signed private request")]
    Rejected,
    #[error("Bitget private payload is invalid or incomplete")]
    Payload,
    #[error("Bitget private symbol is outside the USDT perpetual binding")]
    Symbol,
    #[error("Bitget regular snapshot contains another delegate order family")]
    OrderFamily,
    #[error("Bitget Hedge order has contradictory side, position side, or trade side")]
    DirectionalOrder,
    #[error("Bitget private response cursor is invalid")]
    Cursor,
    #[error("Bitget private pagination is incomplete, mixed, or non-unique")]
    Pagination,
    #[error("Bitget private turn is missing the {0:?} surface")]
    MissingFace(BitgetPrivateSurface),
    #[error("Bitget private turn contains the {0:?} surface more than once")]
    DuplicateFace(BitgetPrivateSurface),
    #[error("Bitget private turn mixes attempts, bindings, accounts, modes, or symbols")]
    MixedAttempt,
    #[error("Bitget private normalized projection does not match its raw response")]
    Projection,
    #[error("Bitget private position snapshot repeats one Hedge leg")]
    DuplicateFact,
    #[error("Bitget private settings do not prove Hedge position mode")]
    PositionMode,
    #[error("Bitget private timestamp is invalid")]
    Clock,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    fn symbol() -> Result<Symbol, Box<dyn std::error::Error>> {
        Ok("DOGE/USDT".parse()?)
    }

    fn binding(raw_symbol: &str) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            raw_symbol.parse()?,
        )?)
    }

    fn order_row(order_id: &str) -> Value {
        json!({
            "orderId": order_id,
            "clientOid": format!("hgo_{order_id}"),
            "category": "USDT-FUTURES",
            "symbol": "DOGEUSDT",
            "orderStatus": "live",
            "side": "sell",
            "posSide": "long",
            "holdMode": "hedge_mode",
            "tradeSide": "close_long",
            "qty": "50",
            "cumExecQty": "0",
            "price": "0.1",
            "avgPrice": "0",
            "delegateType": "normal"
        })
    }

    fn fill_row(fill_id: &str) -> Value {
        json!({
            "execId": fill_id,
            "orderId": "101",
            "clientOid": "hgo_101",
            "category": "USDT-FUTURES",
            "symbol": "DOGEUSDT",
            "side": "sell",
            "holdSide": "long",
            "execQty": "5",
            "execPrice": "0.10001",
            "feeDetail": [{"feeCoin":"USDT", "fee":"-0.002"}],
            "execPnl": "0.5",
            "tradeScope": "maker",
            "execTime": "11",
            "updatedTime": "12"
        })
    }

    fn raw(
        surface: BitgetPrivateSurface,
        page_index: u32,
        request_cursor: Option<&str>,
        list: Vec<Value>,
        next_cursor: Option<&str>,
    ) -> Result<BitgetRawPrivatePage, BitgetPrivateError> {
        let payload = json!({
            "code":"00000",
            "data":{"list":list, "cursor":next_cursor}
        })
        .to_string();
        BitgetRawPrivatePage::new(
            surface,
            binding("DOGE/USDT").map_err(|_| BitgetPrivateError::Metadata)?,
            7,
            page_index,
            request_cursor.map(str::to_owned),
            matches!(surface, BitgetPrivateSurface::Fills).then_some(10),
            100 + u64::from(page_index),
            payload,
        )
    }

    fn single_raw(
        surface: BitgetPrivateSurface,
        attempt_id: u64,
        binding: GatewayBinding,
        data: Value,
    ) -> Result<BitgetRawPrivatePage, BitgetPrivateError> {
        BitgetRawPrivatePage::new(
            surface,
            binding,
            attempt_id,
            0,
            None,
            None,
            90,
            json!({"code":"00000", "data":data}).to_string(),
        )
    }

    fn complete_faces() -> Result<Vec<BitgetPrivateFace>, Box<dyn std::error::Error>> {
        let binding = binding("DOGE/USDT")?;
        let account = parse_account_face(single_raw(
            BitgetPrivateSurface::Account,
            7,
            binding.clone(),
            json!({
                "imr":"2.5", "mmr":"1",
                "assets":[{"coin":"USDT", "balance":"20", "available":"17.5"}]
            }),
        )?)?;
        let settings = parse_settings_face(single_raw(
            BitgetPrivateSurface::Settings,
            7,
            binding.clone(),
            json!({"holdMode":"hedge_mode"}),
        )?)?;
        let positions = parse_positions_face(single_raw(
            BitgetPrivateSurface::Positions,
            7,
            binding,
            json!({"list":[{
                "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"hedge_mode",
                "posSide":"long", "total":"12", "avgPrice":"0.09", "markPrice":"0.1"
            }]}),
        )?)?;
        let orders = parse_regular_order_page(raw(
            BitgetPrivateSurface::RegularOrders,
            0,
            None,
            vec![order_row("1")],
            None,
        )?)?;
        let fills = parse_fill_page(raw(
            BitgetPrivateSurface::Fills,
            0,
            None,
            vec![fill_row("10")],
            None,
        )?)?;
        Ok(vec![
            BitgetPrivateFace::Account(account),
            BitgetPrivateFace::Settings(settings),
            BitgetPrivateFace::Positions(positions),
            BitgetPrivateFace::RegularOrders(vec![orders]),
            BitgetPrivateFace::Fills(vec![fills]),
        ])
    }

    #[test]
    fn five_faces_form_only_a_same_attempt_generation_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let candidate = complete_private_turn(complete_faces()?)?;
        assert_eq!(candidate.attempt_id, 7);
        assert_eq!(candidate.generation, 7);
        assert_eq!(candidate.binding, binding("DOGE/USDT")?);
        assert_eq!(candidate.raw_pages.len(), 5);
        assert_eq!(candidate.positions.len(), 2);
        assert_eq!(candidate.positions[1].side, PositionSide::Short);
        assert_eq!(candidate.positions[1].quantity, Decimal::ZERO);
        assert_eq!(candidate.orders.len(), 1);
        assert_eq!(candidate.fills.len(), 1);
        assert!(candidate.hedge_mode);
        assert_eq!(candidate.observed_at_ms, 100);
        assert_eq!(candidate.balance.wallet_balance, Decimal::from(20));
        Ok(())
    }

    #[test]
    fn missing_or_duplicate_face_discards_the_whole_turn() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut missing = complete_faces()?;
        missing.retain(|face| !matches!(face, BitgetPrivateFace::Fills(_)));
        assert_eq!(
            complete_private_turn(missing),
            Err(BitgetPrivateError::MissingFace(BitgetPrivateSurface::Fills))
        );

        let mut duplicate = complete_faces()?;
        let account = duplicate
            .iter()
            .find_map(|face| match face {
                BitgetPrivateFace::Account(face) => Some(face.clone()),
                _ => None,
            })
            .ok_or("account face missing")?;
        duplicate.push(BitgetPrivateFace::Account(account));
        assert_eq!(
            complete_private_turn(duplicate),
            Err(BitgetPrivateError::DuplicateFace(
                BitgetPrivateSurface::Account
            ))
        );
        Ok(())
    }

    #[test]
    fn cross_attempt_or_gateway_binding_cannot_be_spliced() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut cross_attempt = complete_faces()?;
        if let Some(BitgetPrivateFace::Settings(face)) = cross_attempt
            .iter_mut()
            .find(|face| matches!(face, BitgetPrivateFace::Settings(_)))
        {
            face.raw.attempt_id = 8;
        }
        assert_eq!(
            complete_private_turn(cross_attempt),
            Err(BitgetPrivateError::MixedAttempt)
        );

        let mut cross_generation = complete_faces()?;
        if let Some(BitgetPrivateFace::Settings(face)) = cross_generation
            .iter_mut()
            .find(|face| matches!(face, BitgetPrivateFace::Settings(_)))
        {
            face.raw.generation = 8;
        }
        assert_eq!(
            complete_private_turn(cross_generation),
            Err(BitgetPrivateError::MixedAttempt)
        );

        let mut cross_binding = complete_faces()?;
        if let Some(BitgetPrivateFace::Settings(face)) = cross_binding
            .iter_mut()
            .find(|face| matches!(face, BitgetPrivateFace::Settings(_)))
        {
            face.raw.binding = GatewayBinding::new(
                VenueId::Bitget,
                GatewayMode::Live,
                "00000000-0000-4000-8000-000000000002",
                "DOGE/USDT".parse()?,
            )?;
        }
        assert_eq!(
            complete_private_turn(cross_binding),
            Err(BitgetPrivateError::MixedAttempt)
        );
        Ok(())
    }

    #[test]
    fn settings_failure_duplicate_leg_and_projection_tamper_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut one_way = complete_faces()?;
        if let Some(BitgetPrivateFace::Settings(face)) = one_way
            .iter_mut()
            .find(|face| matches!(face, BitgetPrivateFace::Settings(_)))
        {
            *face = parse_settings_face(single_raw(
                BitgetPrivateSurface::Settings,
                7,
                binding("DOGE/USDT")?,
                json!({"holdMode":"one_way_mode"}),
            )?)?;
        }
        assert_eq!(
            complete_private_turn(one_way),
            Err(BitgetPrivateError::PositionMode)
        );

        let position = json!({
            "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"hedge_mode",
            "posSide":"long", "total":"12", "avgPrice":"0.09", "markPrice":"0.1"
        });
        assert_eq!(
            parse_positions_face(single_raw(
                BitgetPrivateSurface::Positions,
                7,
                binding("DOGE/USDT")?,
                json!({"list":[position.clone(), position]}),
            )?),
            Err(BitgetPrivateError::DuplicateFact)
        );

        let mut tampered = complete_faces()?;
        if let Some(BitgetPrivateFace::Account(face)) = tampered
            .iter_mut()
            .find(|face| matches!(face, BitgetPrivateFace::Account(_)))
        {
            face.balance.wallet_balance = Decimal::from(999);
        }
        assert_eq!(
            complete_private_turn(tampered),
            Err(BitgetPrivateError::Projection)
        );
        Ok(())
    }

    #[test]
    fn singleton_faces_reject_pagination_request_identity() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut raw = single_raw(
            BitgetPrivateSurface::Account,
            7,
            binding("DOGE/USDT")?,
            json!({"assets":[]}),
        )?;
        raw.page_index = 1;
        raw.request_cursor = Some("cursor".to_owned());
        assert_eq!(parse_account_face(raw), Err(BitgetPrivateError::Pagination));
        Ok(())
    }

    #[test]
    fn rejected_account_surface_cannot_enter_a_turn() -> Result<(), Box<dyn std::error::Error>> {
        let raw = BitgetRawPrivatePage::new(
            BitgetPrivateSurface::Account,
            binding("DOGE/USDT")?,
            7,
            0,
            None,
            None,
            90,
            json!({"code":"40001", "data":null}).to_string(),
        )?;
        assert_eq!(parse_account_face(raw), Err(BitgetPrivateError::Rejected));
        Ok(())
    }

    #[test]
    fn regular_pages_require_normal_family_and_close_a_cursor_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = parse_regular_order_page(raw(
            BitgetPrivateSurface::RegularOrders,
            0,
            None,
            vec![order_row("1")],
            Some("next cursor&x=1"),
        )?)?;
        let second = parse_regular_order_page(raw(
            BitgetPrivateSurface::RegularOrders,
            1,
            Some("next cursor&x=1"),
            vec![order_row("2")],
            None,
        )?)?;
        let orders = complete_regular_order_pages(&[first, second])?;
        assert_eq!(orders.len(), 2);
        assert!(orders.iter().all(|order| order.reduce_only));
        assert_eq!(
            orders[0].position_side,
            FieldState::Known(PositionSide::Long)
        );
        Ok(())
    }

    #[test]
    fn regular_page_rejects_other_delegate_families() -> Result<(), BitgetPrivateError> {
        let mut row = order_row("1");
        row["delegateType"] = json!("market");
        let result = parse_regular_order_page(raw(
            BitgetPrivateSurface::RegularOrders,
            0,
            None,
            vec![row],
            None,
        )?);
        assert!(matches!(result, Err(BitgetPrivateError::OrderFamily)));
        Ok(())
    }

    #[test]
    fn directional_trade_side_must_match_side_and_position_side()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut row = order_row("1");
        row["tradeSide"] = json!("close_short");
        assert_eq!(
            parse_regular_order(&row, &symbol()?),
            Err(BitgetPrivateError::DirectionalOrder)
        );
        row["tradeSide"] = Value::Null;
        assert!(parse_regular_order(&row, &symbol()?)?.reduce_only);
        Ok(())
    }

    #[test]
    fn fill_preserves_identity_fee_role_and_prefers_exec_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let page = parse_fill_page(raw(
            BitgetPrivateSurface::Fills,
            0,
            None,
            vec![fill_row("10")],
            None,
        )?)?;
        let fills = complete_fill_pages(&[page])?;
        let fill = &fills[0];
        assert_eq!(fill.fill.execution_sequence, FieldState::Known(10));
        assert_eq!(fill.fill.exchange_time_ms, Some(11));
        assert_eq!(fill.fill.maker, FieldState::Known(true));
        assert_eq!(
            fill.client_order_id,
            FieldState::Known("hgo_101".to_owned())
        );
        assert_eq!(
            fill.fill.fee,
            FieldState::Known(Amount::new("USDT".parse()?, Decimal::new(2, 3)))
        );
        Ok(())
    }

    #[test]
    fn opaque_fill_sequence_and_role_remain_explicitly_unknown()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut row = fill_row("opaque");
        row["tradeScope"] = json!("liquidation");
        let fill = parse_fill(&row, &symbol()?)?;
        assert_eq!(
            fill.execution_sequence,
            FieldState::Unavailable {
                reason: UnknownReason::ParseFailure
            }
        );
        assert_eq!(
            fill.maker,
            FieldState::Unavailable {
                reason: UnknownReason::Ambiguous
            }
        );
        Ok(())
    }

    #[test]
    fn pagination_rejects_mixed_attempts_open_tail_and_duplicate_identities()
    -> Result<(), BitgetPrivateError> {
        let first = parse_fill_page(raw(
            BitgetPrivateSurface::Fills,
            0,
            None,
            vec![fill_row("1")],
            Some("next"),
        )?)?;
        assert_eq!(
            complete_fill_pages(std::slice::from_ref(&first)),
            Err(BitgetPrivateError::Pagination)
        );

        let mut second = parse_fill_page(raw(
            BitgetPrivateSurface::Fills,
            1,
            Some("next"),
            vec![fill_row("1")],
            None,
        )?)?;
        second.raw.fill_history_start_ms = Some(11);
        assert_eq!(
            complete_fill_pages(&[first.clone(), second.clone()]),
            Err(BitgetPrivateError::Pagination)
        );
        second.raw.fill_history_start_ms = Some(10);
        second.raw.attempt_id = 8;
        assert_eq!(
            complete_fill_pages(&[first.clone(), second.clone()]),
            Err(BitgetPrivateError::Pagination)
        );
        second.raw.attempt_id = 7;
        assert_eq!(
            complete_fill_pages(&[first, second]),
            Err(BitgetPrivateError::Pagination)
        );
        Ok(())
    }

    #[test]
    fn raw_evidence_detects_payload_or_binding_tampering() -> Result<(), BitgetPrivateError> {
        let mut page = raw(BitgetPrivateSurface::RegularOrders, 0, None, vec![], None)?;
        page.payload.push(' ');
        assert_eq!(page.validate(), Err(BitgetPrivateError::Metadata));
        Ok(())
    }

    #[test]
    fn completed_pages_reparse_raw_evidence_before_accepting_projection()
    -> Result<(), BitgetPrivateError> {
        let mut page = parse_fill_page(raw(
            BitgetPrivateSurface::Fills,
            0,
            None,
            vec![fill_row("10")],
            None,
        )?)?;
        page.fills[0].fill.quantity = Decimal::from(99);
        assert_eq!(
            complete_fill_pages(&[page]),
            Err(BitgetPrivateError::Pagination)
        );
        Ok(())
    }

    #[test]
    fn queries_escape_cursor_and_clamp_fill_window() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            regular_orders_query(&"SOL/USDT".parse()?, Some("next cursor&x=1"))?,
            "category=USDT-FUTURES&symbol=SOLUSDT&limit=100&cursor=next%20cursor%26x%3D1"
        );
        let now = BITGET_MAX_FILL_HISTORY_WINDOW_MS + 10;
        assert_eq!(
            fill_history_query(Some(1), None, now)?,
            "category=USDT-FUTURES&limit=100&startTime=10"
        );
        assert_eq!(
            fill_history_query(None, Some("next cursor&x=1"), now)?,
            "category=USDT-FUTURES&limit=100&cursor=next%20cursor%26x%3D1"
        );
        Ok(())
    }

    #[test]
    fn wrong_surface_rejection_and_empty_cursor_are_fail_closed() -> Result<(), BitgetPrivateError>
    {
        let page = raw(BitgetPrivateSurface::Fills, 0, None, vec![], None)?;
        assert_eq!(
            parse_regular_order_page(page),
            Err(BitgetPrivateError::Surface)
        );
        assert_eq!(
            fill_history_query(None, Some(""), 1),
            Err(BitgetPrivateError::Cursor)
        );
        Ok(())
    }
}
