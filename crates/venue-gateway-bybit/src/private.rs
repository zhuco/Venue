use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use rust_decimal::Decimal;
use serde::Deserialize;
use venue_domain::domain::{
    Amount, Asset, FieldState, Fill, Order, OrderPurpose, OrderSide, OrderState, Position,
    PositionSide, Price,
};
use venue_gateway_api::GatewayBinding;

use crate::{BybitError, BybitGatewayBinding, linear_native_symbol};

const LINEAR: &str = "linear";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitPrivateSource {
    AccountInfo,
    WalletBalance,
    Positions,
    OpenOrders,
    OrderHistory,
    Executions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitRawPrivatePayload {
    pub binding: GatewayBinding,
    pub source: BybitPrivateSource,
    pub native_symbol: String,
    pub generation: u64,
    pub received_at_ms: u64,
    pub payload: Vec<u8>,
}

impl BybitRawPrivatePayload {
    pub fn new(
        binding: &BybitGatewayBinding,
        source: BybitPrivateSource,
        generation: u64,
        received_at_ms: u64,
        payload: Vec<u8>,
    ) -> Result<Self, BybitError> {
        if generation == 0 || received_at_ms == 0 || payload.is_empty() {
            return Err(BybitError::Payload);
        }
        Ok(Self {
            binding: binding.gateway_binding().clone(),
            source,
            native_symbol: linear_native_symbol(&binding.gateway_binding().symbol)
                .map_err(|_| BybitError::Binding)?,
            generation,
            received_at_ms,
            payload,
        })
    }

    fn validate(
        &self,
        binding: &BybitGatewayBinding,
        source: BybitPrivateSource,
    ) -> Result<(), BybitError> {
        binding.validate_request_binding(&self.binding)?;
        if self.source != source
            || self.native_symbol
                != linear_native_symbol(&self.binding.symbol).map_err(|_| BybitError::Binding)?
            || self.generation == 0
            || self.received_at_ms == 0
            || self.payload.is_empty()
        {
            return Err(BybitError::Binding);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitAccountMode {
    Classic,
    Uta1,
    Uta1Pro,
    Uta2,
    Uta2Pro,
}

impl BybitAccountMode {
    fn parse(value: i64) -> Result<Self, BybitError> {
        match value {
            1 => Ok(Self::Classic),
            3 => Ok(Self::Uta1),
            4 => Ok(Self::Uta1Pro),
            5 => Ok(Self::Uta2),
            6 => Ok(Self::Uta2Pro),
            _ => Err(BybitError::Payload),
        }
    }

    #[must_use]
    pub const fn supports_unified_wallet(self) -> bool {
        matches!(self, Self::Uta2 | Self::Uta2Pro)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitMarginMode {
    Isolated,
    Cross,
    Portfolio,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitAccountIdentity {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub mode: BybitAccountMode,
    pub margin_mode: BybitMarginMode,
    pub updated_at_ms: u64,
}

pub fn parse_account_identity(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
) -> Result<BybitAccountIdentity, BybitError> {
    raw.validate(binding, BybitPrivateSource::AccountInfo)?;
    let envelope: Envelope<AccountInfo> = decode(&raw.payload)?;
    accepted(&envelope)?;
    let margin_mode = match envelope.result.margin_mode.as_str() {
        "ISOLATED_MARGIN" => BybitMarginMode::Isolated,
        "REGULAR_MARGIN" | "CROSS_MARGIN" => BybitMarginMode::Cross,
        "PORTFOLIO_MARGIN" => BybitMarginMode::Portfolio,
        _ => return Err(BybitError::Payload),
    };
    Ok(BybitAccountIdentity {
        binding: raw.binding.clone(),
        generation: raw.generation,
        mode: BybitAccountMode::parse(envelope.result.unified_margin_status)?,
        margin_mode,
        updated_at_ms: positive_u64(&envelope.result.updated_time)?,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitCoinBalance {
    pub asset: Asset,
    pub equity: Decimal,
    pub wallet_balance: Decimal,
    pub locked: Decimal,
    pub borrow_amount: Decimal,
    pub initial_margin: Decimal,
    pub maintenance_margin: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitUnifiedWallet {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub total_equity_usd: Decimal,
    pub total_wallet_balance_usd: Decimal,
    pub total_available_balance_usd: Decimal,
    pub total_initial_margin_usd: Decimal,
    pub total_maintenance_margin_usd: Decimal,
    pub coins: Vec<BybitCoinBalance>,
}

pub fn parse_unified_wallet(
    binding: &BybitGatewayBinding,
    identity: &BybitAccountIdentity,
    raw: &BybitRawPrivatePayload,
) -> Result<BybitUnifiedWallet, BybitError> {
    raw.validate(binding, BybitPrivateSource::WalletBalance)?;
    if identity.binding != raw.binding
        || identity.generation != raw.generation
        || !identity.mode.supports_unified_wallet()
    {
        return Err(BybitError::Binding);
    }
    let envelope: Envelope<WalletResult> = decode(&raw.payload)?;
    accepted(&envelope)?;
    if envelope.result.list.len() != 1 {
        return Err(BybitError::Payload);
    }
    let account = envelope
        .result
        .list
        .into_iter()
        .next()
        .ok_or(BybitError::Payload)?;
    if account.account_type != "UNIFIED" || account.coin.is_empty() {
        return Err(BybitError::Payload);
    }
    let mut assets = BTreeSet::new();
    let coins = account
        .coin
        .into_iter()
        .map(|coin| {
            let asset = Asset::new(&coin.coin).map_err(|_| BybitError::Payload)?;
            if !assets.insert(asset.clone()) {
                return Err(BybitError::Payload);
            }
            Ok(BybitCoinBalance {
                asset,
                equity: non_negative_decimal(&coin.equity)?,
                wallet_balance: non_negative_decimal(&coin.wallet_balance)?,
                locked: non_negative_decimal(&coin.locked)?,
                borrow_amount: non_negative_decimal(&coin.borrow_amount)?,
                initial_margin: non_negative_decimal(&coin.total_position_im)?,
                maintenance_margin: non_negative_decimal(&coin.total_position_mm)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitUnifiedWallet {
        binding: raw.binding.clone(),
        generation: raw.generation,
        total_equity_usd: non_negative_decimal(&account.total_equity)?,
        total_wallet_balance_usd: non_negative_decimal(&account.total_wallet_balance)?,
        total_available_balance_usd: non_negative_decimal(&account.total_available_balance)?,
        total_initial_margin_usd: non_negative_decimal(&account.total_initial_margin)?,
        total_maintenance_margin_usd: non_negative_decimal(&account.total_maintenance_margin)?,
        coins,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPageMeta {
    pub requested_cursor: Option<String>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BybitPageState {
    Continue(String),
    Closed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BybitPageClosure {
    expected_cursor: Option<String>,
    seen: BTreeSet<String>,
    closed: bool,
}

impl BybitPageClosure {
    pub fn accept(&mut self, page: &BybitPageMeta) -> Result<BybitPageState, BybitError> {
        if self.closed || page.requested_cursor != self.expected_cursor {
            return Err(BybitError::Payload);
        }
        match &page.next_cursor {
            Some(next)
                if next.is_empty()
                    || page.requested_cursor.as_ref() == Some(next)
                    || !self.seen.insert(next.clone()) =>
            {
                Err(BybitError::Payload)
            }
            Some(next) => {
                self.expected_cursor = Some(next.clone());
                Ok(BybitPageState::Continue(next.clone()))
            }
            None => {
                self.closed = true;
                self.expected_cursor = None;
                Ok(BybitPageState::Closed)
            }
        }
    }

    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPosition {
    pub position: Position,
    pub position_idx: u8,
    pub liquidation_price: Option<Price>,
    pub unrealized_pnl: Decimal,
    pub native_sequence: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPositionPage {
    pub meta: BybitPageMeta,
    pub positions: Vec<BybitPosition>,
}

pub fn parse_position_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
    requested_cursor: Option<&str>,
) -> Result<BybitPositionPage, BybitError> {
    raw.validate(binding, BybitPrivateSource::Positions)?;
    let envelope: Envelope<Page<PositionRow>> = decode(&raw.payload)?;
    accepted(&envelope)?;
    validate_page(&envelope.result, &raw.native_symbol, 200)?;
    envelope.result.validate_symbols(&raw.native_symbol)?;
    let mut sides = BTreeSet::new();
    let positions = envelope
        .result
        .list
        .into_iter()
        .map(|row| {
            let side = position_side(row.position_idx)?;
            if !sides.insert(side) {
                return Err(BybitError::Payload);
            }
            let quantity = non_negative_decimal(&row.size)?;
            match (row.position_idx, row.side.as_str(), quantity.is_zero()) {
                (0, "Buy" | "Sell", _)
                | (0, "", true)
                | (1, "Buy", _)
                | (1, "", true)
                | (2, "Sell", _)
                | (2, "", true) => {}
                _ => return Err(BybitError::Payload),
            }
            let position = Position {
                symbol: raw.binding.symbol.clone(),
                side,
                quantity,
                entry_price: optional_price(&row.avg_price)?,
                mark_price: optional_price(&row.mark_price)?,
            };
            Ok(BybitPosition {
                position,
                position_idx: row.position_idx,
                liquidation_price: optional_price(&row.liq_price)?,
                unrealized_pnl: decimal(&row.unrealised_pnl)?,
                native_sequence: positive_u64(&row.seq)?,
                updated_at_ms: positive_u64(&row.updated_time)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let has_net = sides.contains(&PositionSide::Net);
    let has_hedge = sides.contains(&PositionSide::Long) || sides.contains(&PositionSide::Short);
    if has_net && has_hedge {
        return Err(BybitError::Payload);
    }
    Ok(BybitPositionPage {
        meta: page_meta(requested_cursor, envelope.result.next_page_cursor),
        positions,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOpenOrder {
    pub order: Order,
    pub position_idx: u8,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOpenOrderPage {
    pub meta: BybitPageMeta,
    pub orders: Vec<BybitOpenOrder>,
}

pub fn parse_open_order_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
    requested_cursor: Option<&str>,
) -> Result<BybitOpenOrderPage, BybitError> {
    raw.validate(binding, BybitPrivateSource::OpenOrders)?;
    let envelope: Envelope<Page<OrderRow>> = decode(&raw.payload)?;
    accepted(&envelope)?;
    validate_page(&envelope.result, &raw.native_symbol, 50)?;
    envelope.result.validate_symbols(&raw.native_symbol)?;
    let mut ids = BTreeSet::new();
    let orders = envelope
        .result
        .list
        .into_iter()
        .map(|row| {
            if !ids.insert(row.order_id.clone()) {
                return Err(BybitError::Payload);
            }
            normalize_order(raw, row, true)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitOpenOrderPage {
        meta: page_meta(requested_cursor, envelope.result.next_page_cursor),
        orders,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderEvidence {
    pub order_id: String,
    pub client_order_id: FieldState<String>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub position_idx: u8,
    pub reduce_only: bool,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderEvidencePage {
    pub meta: BybitPageMeta,
    pub orders: Vec<BybitOrderEvidence>,
}

pub fn parse_order_history_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
    requested_cursor: Option<&str>,
) -> Result<BybitOrderEvidencePage, BybitError> {
    raw.validate(binding, BybitPrivateSource::OrderHistory)?;
    let envelope: Envelope<Page<OrderRow>> = decode(&raw.payload)?;
    accepted(&envelope)?;
    validate_page(&envelope.result, &raw.native_symbol, 50)?;
    envelope.result.validate_symbols(&raw.native_symbol)?;
    let mut ids = BTreeSet::new();
    let orders = envelope
        .result
        .list
        .into_iter()
        .map(|row| {
            if row.order_id.is_empty() || !ids.insert(row.order_id.clone()) {
                return Err(BybitError::Payload);
            }
            let side = order_side(&row.side)?;
            Ok(BybitOrderEvidence {
                order_id: row.order_id,
                client_order_id: field_text(row.order_link_id),
                side,
                position_side: position_side(row.position_idx)?,
                position_idx: row.position_idx,
                reduce_only: row.reduce_only,
                updated_at_ms: positive_u64(&row.updated_time)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitOrderEvidencePage {
        meta: page_meta(requested_cursor, envelope.result.next_page_cursor),
        orders,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
    pub closed_size: Decimal,
    pub native_order_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitExecutionPage {
    pub meta: BybitPageMeta,
    pub fills: Vec<BybitFill>,
}

pub fn parse_execution_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
    requested_cursor: Option<&str>,
    order_evidence: &[BybitOrderEvidence],
) -> Result<BybitExecutionPage, BybitError> {
    raw.validate(binding, BybitPrivateSource::Executions)?;
    let envelope: Envelope<Page<ExecutionRow>> = decode(&raw.payload)?;
    accepted(&envelope)?;
    validate_page(&envelope.result, &raw.native_symbol, 100)?;
    envelope.result.validate_symbols(&raw.native_symbol)?;
    let evidence = order_evidence
        .iter()
        .map(|item| (item.order_id.as_str(), item))
        .collect::<BTreeMap<_, _>>();
    if evidence.len() != order_evidence.len() {
        return Err(BybitError::Payload);
    }
    let mut ids = BTreeSet::new();
    let mut previous_time = None;
    let fills = envelope
        .result
        .list
        .into_iter()
        .map(|row| {
            if row.exec_type != "Trade"
                || row.exec_id.is_empty()
                || !ids.insert(row.exec_id.clone())
            {
                return Err(BybitError::Payload);
            }
            let item = evidence
                .get(row.order_id.as_str())
                .ok_or(BybitError::Payload)?;
            let side = order_side(&row.side)?;
            if side != item.side || field_text(row.order_link_id.clone()) != item.client_order_id {
                return Err(BybitError::Binding);
            }
            let time = positive_u64(&row.exec_time)?;
            if previous_time.is_some_and(|prior| time > prior) {
                return Err(BybitError::Payload);
            }
            previous_time = Some(time);
            let quantity = positive_decimal(&row.exec_qty)?;
            let closed_size = non_negative_decimal(&row.closed_size)?;
            if closed_size > quantity {
                return Err(BybitError::Payload);
            }
            let fee = if row.fee_currency.is_empty() {
                FieldState::Missing
            } else {
                FieldState::Known(Amount::new(
                    Asset::new(&row.fee_currency).map_err(|_| BybitError::Payload)?,
                    decimal(&row.exec_fee)?,
                ))
            };
            let realized_pnl = match (row.exec_pnl.as_deref(), row.fee_currency.as_str()) {
                (Some(value), asset) if !asset.is_empty() => FieldState::Known(Amount::new(
                    Asset::new(asset).map_err(|_| BybitError::Payload)?,
                    decimal(value)?,
                )),
                (None, _) => FieldState::Missing,
                _ => return Err(BybitError::Payload),
            };
            let seq = positive_u64(&row.seq)?;
            let fill = Fill {
                fill_id: row.exec_id,
                execution_sequence: FieldState::Known(seq),
                order_id: row.order_id,
                symbol: raw.binding.symbol.clone(),
                side,
                position_side: FieldState::Known(item.position_side),
                quantity,
                price: Price::new(decimal(&row.exec_price)?).map_err(|_| BybitError::Payload)?,
                fee,
                realized_pnl,
                maker: FieldState::Known(row.is_maker),
                exchange_time_ms: Some(time),
            };
            fill.validate().map_err(|_| BybitError::Payload)?;
            Ok(BybitFill {
                fill,
                client_order_id: item.client_order_id.clone(),
                closed_size,
                native_order_sequence: seq,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitExecutionPage {
        meta: page_meta(requested_cursor, envelope.result.next_page_cursor),
        fills,
    })
}

fn normalize_order(
    raw: &BybitRawPrivatePayload,
    row: OrderRow,
    open_only: bool,
) -> Result<BybitOpenOrder, BybitError> {
    if row.order_id.is_empty() {
        return Err(BybitError::Payload);
    }
    let state = match row.order_status.as_str() {
        "New" => OrderState::New,
        "PartiallyFilled" => OrderState::PartiallyFilled,
        "Filled" => OrderState::Filled,
        "Cancelled" | "Deactivated" => OrderState::Cancelled,
        "Rejected" => OrderState::Rejected,
        _ => return Err(BybitError::Payload),
    };
    if open_only && !matches!(state, OrderState::New | OrderState::PartiallyFilled) {
        return Err(BybitError::Payload);
    }
    let reduce_only = row.reduce_only;
    let order = Order {
        order_id: row.order_id,
        client_order_id: field_text(row.order_link_id),
        symbol: raw.binding.symbol.clone(),
        side: order_side(&row.side)?,
        position_side: FieldState::Known(position_side(row.position_idx)?),
        purpose: FieldState::Known(if reduce_only {
            OrderPurpose::Reduce
        } else {
            OrderPurpose::Entry
        }),
        state,
        quantity: positive_decimal(&row.qty)?,
        filled_quantity: non_negative_decimal(&row.cum_exec_qty)?,
        limit_price: optional_price(&row.price)?,
        average_price: optional_field_price(&row.avg_price)?,
        reduce_only,
    };
    order.validate().map_err(|_| BybitError::Payload)?;
    Ok(BybitOpenOrder {
        order,
        position_idx: row.position_idx,
        created_at_ms: positive_u64(&row.created_time)?,
        updated_at_ms: positive_u64(&row.updated_time)?,
    })
}

fn validate_page<T>(page: &Page<T>, native_symbol: &str, limit: usize) -> Result<(), BybitError> {
    if page.category != LINEAR || page.list.len() > limit || native_symbol.is_empty() {
        Err(BybitError::Binding)
    } else {
        Ok(())
    }
}

fn page_meta(requested: Option<&str>, next: String) -> BybitPageMeta {
    BybitPageMeta {
        requested_cursor: requested.map(str::to_owned),
        next_cursor: (!next.is_empty()).then_some(next),
    }
}

fn position_side(value: u8) -> Result<PositionSide, BybitError> {
    match value {
        0 => Ok(PositionSide::Net),
        1 => Ok(PositionSide::Long),
        2 => Ok(PositionSide::Short),
        _ => Err(BybitError::Payload),
    }
}

fn order_side(value: &str) -> Result<OrderSide, BybitError> {
    match value {
        "Buy" => Ok(OrderSide::Buy),
        "Sell" => Ok(OrderSide::Sell),
        _ => Err(BybitError::Payload),
    }
}

fn field_text(value: String) -> FieldState<String> {
    if value.is_empty() {
        FieldState::Missing
    } else {
        FieldState::Known(value)
    }
}
fn decimal(value: &str) -> Result<Decimal, BybitError> {
    Decimal::from_str(value).map_err(|_| BybitError::Payload)
}
fn non_negative_decimal(value: &str) -> Result<Decimal, BybitError> {
    let value = decimal(value)?;
    if value.is_sign_negative() {
        Err(BybitError::Payload)
    } else {
        Ok(value)
    }
}
fn positive_decimal(value: &str) -> Result<Decimal, BybitError> {
    let value = decimal(value)?;
    if value.is_sign_positive() && !value.is_zero() {
        Ok(value)
    } else {
        Err(BybitError::Payload)
    }
}
fn positive_u64(value: &str) -> Result<u64, BybitError> {
    let value = u64::from_str(value).map_err(|_| BybitError::Payload)?;
    if value == 0 {
        Err(BybitError::Payload)
    } else {
        Ok(value)
    }
}
fn optional_price(value: &str) -> Result<Option<Price>, BybitError> {
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            Price::new(decimal(value)?).map_err(|_| BybitError::Payload)?,
        ))
    }
}
fn optional_field_price(value: &str) -> Result<FieldState<Price>, BybitError> {
    optional_price(value).map(|price| price.map_or(FieldState::Missing, FieldState::Known))
}
fn decode<'a, T: Deserialize<'a>>(payload: &'a [u8]) -> Result<T, BybitError> {
    serde_json::from_slice(payload).map_err(|_| BybitError::Payload)
}
fn accepted<T>(envelope: &Envelope<T>) -> Result<(), BybitError> {
    if envelope.ret_code == 0 {
        Ok(())
    } else {
        Err(BybitError::Rejected)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<T> {
    ret_code: i64,
    result: T,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountInfo {
    margin_mode: String,
    unified_margin_status: i64,
    updated_time: String,
}

#[derive(Deserialize)]
struct WalletResult {
    list: Vec<WalletAccount>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WalletAccount {
    account_type: String,
    total_equity: String,
    total_wallet_balance: String,
    total_available_balance: String,
    total_initial_margin: String,
    total_maintenance_margin: String,
    coin: Vec<CoinRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CoinRow {
    coin: String,
    equity: String,
    wallet_balance: String,
    locked: String,
    borrow_amount: String,
    #[serde(rename = "totalPositionIM")]
    total_position_im: String,
    #[serde(rename = "totalPositionMM")]
    total_position_mm: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page<T> {
    category: String,
    next_page_cursor: String,
    list: Vec<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    symbol: String,
    side: String,
    size: String,
    avg_price: String,
    mark_price: String,
    liq_price: String,
    unrealised_pnl: String,
    position_idx: u8,
    updated_time: String,
    seq: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderRow {
    symbol: String,
    order_id: String,
    order_link_id: String,
    side: String,
    position_idx: u8,
    order_status: String,
    qty: String,
    cum_exec_qty: String,
    price: String,
    avg_price: String,
    reduce_only: bool,
    created_time: String,
    updated_time: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionRow {
    symbol: String,
    order_id: String,
    order_link_id: String,
    side: String,
    exec_id: String,
    exec_price: String,
    exec_qty: String,
    exec_fee: String,
    exec_time: String,
    fee_currency: String,
    closed_size: String,
    exec_type: String,
    is_maker: bool,
    seq: String,
    #[serde(default)]
    exec_pnl: Option<String>,
}

impl<T> Page<T> {
    fn validate_symbols(&self, native_symbol: &str) -> Result<(), BybitError>
    where
        T: HasSymbol,
    {
        if self.list.iter().all(|row| row.symbol() == native_symbol) {
            Ok(())
        } else {
            Err(BybitError::Binding)
        }
    }
}

trait HasSymbol {
    fn symbol(&self) -> &str;
}
impl HasSymbol for PositionRow {
    fn symbol(&self) -> &str {
        &self.symbol
    }
}
impl HasSymbol for OrderRow {
    fn symbol(&self) -> &str {
        &self.symbol
    }
}
impl HasSymbol for ExecutionRow {
    fn symbol(&self) -> &str {
        &self.symbol
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    const ACCOUNT: &[u8] = include_bytes!("../fixtures/account-info-uta2.json");
    const WALLET: &[u8] = include_bytes!("../fixtures/wallet-balance-unified.json");
    const POSITIONS: &[u8] = include_bytes!("../fixtures/positions-linear.json");
    const ORDERS: &[u8] = include_bytes!("../fixtures/open-orders-linear.json");
    const HISTORY: &[u8] = include_bytes!("../fixtures/order-history-linear.json");
    const EXECUTIONS: &[u8] = include_bytes!("../fixtures/execution-trade-page.json");

    fn binding(mode: GatewayMode) -> Result<BybitGatewayBinding, Box<dyn std::error::Error>> {
        Ok(BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?)
    }
    fn raw(
        binding: &BybitGatewayBinding,
        source: BybitPrivateSource,
        bytes: &[u8],
    ) -> Result<BybitRawPrivatePayload, BybitError> {
        BybitRawPrivatePayload::new(binding, source, 7, 2_000, bytes.to_vec())
    }

    #[test]
    fn uta2_identity_and_unified_wallet_are_one_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Test)?;
        let identity = parse_account_identity(
            &binding,
            &raw(&binding, BybitPrivateSource::AccountInfo, ACCOUNT)?,
        )?;
        assert_eq!(identity.mode, BybitAccountMode::Uta2);
        let wallet = parse_unified_wallet(
            &binding,
            &identity,
            &raw(&binding, BybitPrivateSource::WalletBalance, WALLET)?,
        )?;
        assert_eq!(wallet.coins[0].asset.as_str(), "USDT");
        assert_eq!(wallet.total_available_balance_usd, Decimal::new(900, 0));
        let mut stale = identity.clone();
        stale.generation = 6;
        assert_eq!(
            parse_unified_wallet(
                &binding,
                &stale,
                &raw(&binding, BybitPrivateSource::WalletBalance, WALLET)?
            ),
            Err(BybitError::Binding)
        );
        Ok(())
    }

    #[test]
    fn positions_and_open_orders_preserve_position_idx_and_times()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Live)?;
        let positions = parse_position_page(
            &binding,
            &raw(&binding, BybitPrivateSource::Positions, POSITIONS)?,
            None,
        )?;
        assert_eq!(positions.positions.len(), 2);
        assert_eq!(positions.positions[0].position.side, PositionSide::Long);
        assert_eq!(positions.positions[1].position_idx, 2);
        let orders = parse_open_order_page(
            &binding,
            &raw(&binding, BybitPrivateSource::OpenOrders, ORDERS)?,
            None,
        )?;
        assert_eq!(orders.orders[0].order.order_id, "20");
        assert_eq!(orders.orders[0].updated_at_ms, 1_900);
        Ok(())
    }

    #[test]
    fn execution_requires_exact_order_evidence_and_preserves_native_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Live)?;
        let history = parse_order_history_page(
            &binding,
            &raw(&binding, BybitPrivateSource::OrderHistory, HISTORY)?,
            None,
        )?;
        let page = parse_execution_page(
            &binding,
            &raw(&binding, BybitPrivateSource::Executions, EXECUTIONS)?,
            None,
            &history.orders,
        )?;
        assert_eq!(page.fills.len(), 3);
        assert_eq!(page.fills[0].fill.fill_id, "c");
        assert_eq!(
            page.fills[0].fill.execution_sequence,
            FieldState::Known(103)
        );
        assert_eq!(
            page.fills[0].fill.position_side,
            FieldState::Known(PositionSide::Short)
        );
        assert_eq!(
            page.fills[2].client_order_id,
            FieldState::Known("MANAGED_CLIENT_ID".to_owned())
        );
        assert!(
            parse_execution_page(
                &binding,
                &raw(&binding, BybitPrivateSource::Executions, EXECUTIONS)?,
                None,
                &history.orders[..2]
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn cursor_must_reach_an_explicit_empty_terminal_page() -> Result<(), BybitError> {
        let mut closure = BybitPageClosure::default();
        let first = BybitPageMeta {
            requested_cursor: None,
            next_cursor: Some("next".to_owned()),
        };
        assert_eq!(
            closure.accept(&first)?,
            BybitPageState::Continue("next".to_owned())
        );
        assert!(!closure.is_closed());
        let terminal = BybitPageMeta {
            requested_cursor: Some("next".to_owned()),
            next_cursor: None,
        };
        assert_eq!(closure.accept(&terminal)?, BybitPageState::Closed);
        assert!(closure.is_closed());
        assert!(closure.accept(&terminal).is_err());
        Ok(())
    }

    #[test]
    fn cross_mode_or_wrong_symbol_payload_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let live = binding(GatewayMode::Live)?;
        let test = binding(GatewayMode::Test)?;
        let test_raw = raw(&test, BybitPrivateSource::Positions, POSITIONS)?;
        assert_eq!(
            parse_position_page(&live, &test_raw, None),
            Err(BybitError::Binding)
        );
        let wrong = String::from_utf8(POSITIONS.to_vec())?.replace("BTCUSDT", "ETHUSDT");
        let raw = raw(&live, BybitPrivateSource::Positions, wrong.as_bytes())?;
        assert_eq!(
            parse_position_page(&live, &raw, None),
            Err(BybitError::Binding)
        );
        Ok(())
    }
}
