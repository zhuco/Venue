use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use rust_decimal::Decimal;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    Amount, Asset, FieldState, Fill, LimitTimeInForce, NativeOrderFamily, Order, OrderPurpose,
    OrderSide, OrderState, Position, PositionSide, Price, UnknownReason,
};
use venue_gateway_api::GatewayBinding;

use crate::{
    BybitCredentials, BybitError, BybitGatewayBinding, SignedHeaders, endpoints,
    linear_native_symbol, sign,
};

const LINEAR: &str = "linear";
pub const BYBIT_PRIVATE_PARSER_SCHEMA_VERSION: u16 = 1;
pub const BYBIT_PRIVATE_MAX_PAGES: usize = 1_000;
pub const BYBIT_HISTORY_WINDOW_MAX_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
pub const BYBIT_LINEAR_ORDER_PROFILE_VERSION: u64 = 1;

const POSITION_PAGE_LIMIT: usize = 200;
const ORDER_PAGE_LIMIT: usize = 50;
const EXECUTION_PAGE_LIMIT: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BybitPrivateSource {
    ApiKeyInfo,
    AccountInfo,
    WalletBalance,
    Positions,
    AccountWidePositions,
    OpenOrders(NativeOrderFamily),
    AccountWideOpenOrders(NativeOrderFamily),
    OrderHistory(NativeOrderFamily),
    Executions,
    AccountWideExecutions,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BybitHistoryWindow {
    pub start_ms: u64,
    pub end_ms: u64,
}

impl BybitHistoryWindow {
    pub fn new(start_ms: u64, end_ms: u64) -> Result<Self, BybitError> {
        if start_ms == 0
            || end_ms <= start_ms
            || end_ms.saturating_sub(start_ms) > BYBIT_HISTORY_WINDOW_MAX_MS
        {
            return Err(BybitError::Clock);
        }
        Ok(Self { start_ms, end_ms })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BybitOrderLookup {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
}

impl BybitOrderLookup {
    pub fn by_order_id(order_id: impl Into<String>) -> Result<Self, BybitError> {
        let order_id = order_id.into();
        validate_lookup_id(&order_id)?;
        Ok(Self {
            order_id: Some(order_id),
            client_order_id: None,
        })
    }

    pub fn by_client_order_id(client_order_id: impl Into<String>) -> Result<Self, BybitError> {
        let client_order_id = client_order_id.into();
        validate_lookup_id(&client_order_id)?;
        Ok(Self {
            order_id: None,
            client_order_id: Some(client_order_id),
        })
    }

    fn validate(&self) -> Result<(), BybitError> {
        match (&self.order_id, &self.client_order_id) {
            (Some(value), None) | (None, Some(value)) => validate_lookup_id(value),
            _ => Err(BybitError::Binding),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPreparedPrivateRequest {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt_id: u64,
    pub page_index: u32,
    pub origin: &'static str,
    pub path: &'static str,
    pub source: BybitPrivateSource,
    pub query: String,
    pub request_cursor: Option<String>,
    pub history_window: Option<BybitHistoryWindow>,
    pub lookup: Option<BybitOrderLookup>,
}

impl BybitPreparedPrivateRequest {
    pub(crate) fn validate(&self, binding: &BybitGatewayBinding) -> Result<(), BybitError> {
        binding.validate_request_binding(&self.binding)?;
        if self.generation == 0
            || self.attempt_id == 0
            || usize::try_from(self.page_index).map_err(|_| BybitError::Pagination)?
                >= BYBIT_PRIVATE_MAX_PAGES
            || self.origin != binding.config().rest_origin()
            || self
                .request_cursor
                .as_deref()
                .is_some_and(|cursor| validate_cursor(cursor).is_err())
            || self
                .lookup
                .as_ref()
                .is_some_and(|value| value.validate().is_err())
        {
            return Err(BybitError::Binding);
        }
        if (self.page_index == 0) != self.request_cursor.is_none() {
            return Err(BybitError::Pagination);
        }
        let (expected_path, expected_query) = private_request_parts(
            binding,
            self.source,
            self.request_cursor.as_deref(),
            self.history_window.as_ref(),
            self.lookup.as_ref(),
        )?;
        if self.path != expected_path || self.query != expected_query {
            return Err(BybitError::Binding);
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_private_request(
    binding: &BybitGatewayBinding,
    generation: u64,
    attempt_id: u64,
    page_index: u32,
    source: BybitPrivateSource,
    request_cursor: Option<&str>,
    history_window: Option<BybitHistoryWindow>,
    lookup: Option<BybitOrderLookup>,
) -> Result<BybitPreparedPrivateRequest, BybitError> {
    let (path, query) = private_request_parts(
        binding,
        source,
        request_cursor,
        history_window.as_ref(),
        lookup.as_ref(),
    )?;
    let request = BybitPreparedPrivateRequest {
        binding: binding.gateway_binding().clone(),
        generation,
        attempt_id,
        page_index,
        origin: binding.config().rest_origin(),
        path,
        source,
        query,
        request_cursor: request_cursor.map(str::to_owned),
        history_window,
        lookup,
    };
    request.validate(binding)?;
    Ok(request)
}

pub fn sign_private_request(
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    request: &BybitPreparedPrivateRequest,
    timestamp_ms: u64,
) -> Result<SignedHeaders, BybitError> {
    request.validate(binding)?;
    sign(
        credentials,
        binding,
        &request.binding,
        timestamp_ms,
        request.query.as_bytes(),
    )
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BybitRawPrivatePayload {
    pub parser_schema_version: u16,
    pub binding: GatewayBinding,
    pub source: BybitPrivateSource,
    pub native_symbol: String,
    pub generation: u64,
    pub attempt_id: u64,
    pub page_index: u32,
    pub request_cursor: Option<String>,
    pub history_window: Option<BybitHistoryWindow>,
    pub lookup: Option<BybitOrderLookup>,
    pub request_path: String,
    pub request_query: String,
    pub request_timestamp_ms: u64,
    pub received_at_ms: u64,
    pub payload_sha256: String,
    pub payload: Vec<u8>,
}

impl BybitRawPrivatePayload {
    pub fn from_response(
        binding: &BybitGatewayBinding,
        request: &BybitPreparedPrivateRequest,
        request_timestamp_ms: u64,
        received_at_ms: u64,
        payload: Vec<u8>,
    ) -> Result<Self, BybitError> {
        request.validate(binding)?;
        if request_timestamp_ms == 0 || received_at_ms < request_timestamp_ms || payload.is_empty()
        {
            return Err(BybitError::Payload);
        }
        let raw = Self {
            parser_schema_version: BYBIT_PRIVATE_PARSER_SCHEMA_VERSION,
            binding: request.binding.clone(),
            source: request.source,
            native_symbol: linear_native_symbol(&binding.gateway_binding().symbol)
                .map_err(|_| BybitError::Binding)?,
            generation: request.generation,
            attempt_id: request.attempt_id,
            page_index: request.page_index,
            request_cursor: request.request_cursor.clone(),
            history_window: request.history_window.clone(),
            lookup: request.lookup.clone(),
            request_path: request.path.to_owned(),
            request_query: request.query.clone(),
            request_timestamp_ms,
            received_at_ms,
            payload_sha256: payload_digest(&payload),
            payload,
        };
        raw.validate(binding, request.source)?;
        Ok(raw)
    }

    fn validate(
        &self,
        binding: &BybitGatewayBinding,
        source: BybitPrivateSource,
    ) -> Result<(), BybitError> {
        binding.validate_request_binding(&self.binding)?;
        let request = prepare_private_request(
            binding,
            self.generation,
            self.attempt_id,
            self.page_index,
            self.source,
            self.request_cursor.as_deref(),
            self.history_window.clone(),
            self.lookup.clone(),
        )?;
        if self.parser_schema_version != BYBIT_PRIVATE_PARSER_SCHEMA_VERSION
            || self.source != source
            || self.native_symbol
                != linear_native_symbol(&self.binding.symbol).map_err(|_| BybitError::Binding)?
            || self.request_timestamp_ms == 0
            || self.received_at_ms < self.request_timestamp_ms
            || self.payload.is_empty()
            || self.payload_sha256 != payload_digest(&self.payload)
            || self.request_path != request.path
            || self.request_query != request.query
            || request.validate(binding).is_err()
        {
            return Err(BybitError::Binding);
        }
        Ok(())
    }
}

impl fmt::Debug for BybitRawPrivatePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BybitRawPrivatePayload")
            .field("source", &self.source)
            .field("binding", &self.binding)
            .field("generation", &self.generation)
            .field("attempt_id", &self.attempt_id)
            .field("page_index", &self.page_index)
            .field("request_cursor", &self.request_cursor)
            .field("request_path", &self.request_path)
            .field("request_query", &self.request_query)
            .field("request_timestamp_ms", &self.request_timestamp_ms)
            .field("received_at_ms", &self.received_at_ms)
            .field("payload_sha256", &self.payload_sha256)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

fn private_request_parts(
    binding: &BybitGatewayBinding,
    source: BybitPrivateSource,
    cursor: Option<&str>,
    history_window: Option<&BybitHistoryWindow>,
    lookup: Option<&BybitOrderLookup>,
) -> Result<(&'static str, String), BybitError> {
    let native_symbol =
        linear_native_symbol(&binding.gateway_binding().symbol).map_err(|_| BybitError::Binding)?;
    if let Some(cursor) = cursor {
        validate_cursor(cursor)?;
    }
    if let Some(lookup) = lookup {
        lookup.validate()?;
    }
    let (path, mut query) = match source {
        BybitPrivateSource::ApiKeyInfo => {
            require_unpaged(cursor, history_window, lookup)?;
            (endpoints::API_INFO, String::new())
        }
        BybitPrivateSource::AccountInfo => {
            require_unpaged(cursor, history_window, lookup)?;
            (endpoints::ACCOUNT_INFO, String::new())
        }
        BybitPrivateSource::WalletBalance => {
            require_unpaged(cursor, history_window, lookup)?;
            (
                endpoints::BALANCES,
                format!(
                    "accountType=UNIFIED&coin={}",
                    binding.gateway_binding().symbol.quote()
                ),
            )
        }
        BybitPrivateSource::Positions => {
            if history_window.is_some() || lookup.is_some() {
                return Err(BybitError::Binding);
            }
            (
                endpoints::POSITIONS,
                format!("category={LINEAR}&symbol={native_symbol}&limit={POSITION_PAGE_LIMIT}"),
            )
        }
        BybitPrivateSource::AccountWidePositions => {
            if history_window.is_some() || lookup.is_some() {
                return Err(BybitError::Binding);
            }
            (
                endpoints::POSITIONS,
                format!("category={LINEAR}&limit={POSITION_PAGE_LIMIT}"),
            )
        }
        BybitPrivateSource::OpenOrders(family) => {
            if history_window.is_some() {
                return Err(BybitError::Binding);
            }
            let filter = order_filter(family)?;
            (
                endpoints::OPEN_ORDERS,
                format!(
                    "category={LINEAR}&symbol={native_symbol}&openOnly=0&orderFilter={filter}&limit={ORDER_PAGE_LIMIT}"
                ),
            )
        }
        BybitPrivateSource::AccountWideOpenOrders(family) => {
            if history_window.is_some() || lookup.is_some() {
                return Err(BybitError::Binding);
            }
            let filter = order_filter(family)?;
            (
                endpoints::OPEN_ORDERS,
                format!(
                    "category={LINEAR}&openOnly=0&orderFilter={filter}&limit={ORDER_PAGE_LIMIT}"
                ),
            )
        }
        BybitPrivateSource::OrderHistory(family) => {
            let window = history_window.ok_or(BybitError::Clock)?;
            let filter = order_filter(family)?;
            (
                endpoints::ORDER_HISTORY,
                format!(
                    "category={LINEAR}&symbol={native_symbol}&orderFilter={filter}&startTime={}&endTime={}&limit={ORDER_PAGE_LIMIT}",
                    window.start_ms, window.end_ms
                ),
            )
        }
        BybitPrivateSource::Executions => {
            let window = history_window.ok_or(BybitError::Clock)?;
            (
                endpoints::EXECUTIONS,
                format!(
                    "category={LINEAR}&symbol={native_symbol}&startTime={}&endTime={}&execType=Trade&limit={EXECUTION_PAGE_LIMIT}",
                    window.start_ms, window.end_ms
                ),
            )
        }
        BybitPrivateSource::AccountWideExecutions => {
            let window = history_window.ok_or(BybitError::Clock)?;
            if lookup.is_some() {
                return Err(BybitError::Binding);
            }
            (
                endpoints::EXECUTIONS,
                format!(
                    "category={LINEAR}&startTime={}&endTime={}&execType=Trade&limit={EXECUTION_PAGE_LIMIT}",
                    window.start_ms, window.end_ms
                ),
            )
        }
    };
    if let Some(lookup) = lookup {
        match (&lookup.order_id, &lookup.client_order_id) {
            (Some(value), None) => push_query(&mut query, "orderId", value),
            (None, Some(value)) => push_query(&mut query, "orderLinkId", value),
            _ => return Err(BybitError::Binding),
        }
    }
    if let Some(cursor) = cursor {
        push_query(&mut query, "cursor", cursor);
    }
    Ok((path, query))
}

fn require_unpaged(
    cursor: Option<&str>,
    history_window: Option<&BybitHistoryWindow>,
    lookup: Option<&BybitOrderLookup>,
) -> Result<(), BybitError> {
    if cursor.is_some() || history_window.is_some() || lookup.is_some() {
        Err(BybitError::Binding)
    } else {
        Ok(())
    }
}

fn order_filter(family: NativeOrderFamily) -> Result<&'static str, BybitError> {
    match family {
        NativeOrderFamily::UmOrder => Ok("Order"),
        NativeOrderFamily::UmConditional => Ok("StopOrder"),
        NativeOrderFamily::UmAlgo => Err(BybitError::OrderFamily),
    }
}

fn push_query(query: &mut String, name: &str, value: &str) {
    if !query.is_empty() {
        query.push('&');
    }
    query.push_str(name);
    query.push('=');
    query.push_str(value);
}

fn validate_cursor(value: &str) -> Result<(), BybitError> {
    if (1..=2_048).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'&' | b'?' | b'#'))
    {
        Ok(())
    } else {
        Err(BybitError::Pagination)
    }
}

fn validate_lookup_id(value: &str) -> Result<(), BybitError> {
    if (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(BybitError::Binding)
    }
}

fn payload_digest(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
    pub raw: BybitRawPrivatePayload,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub meta: BybitPageMeta,
    pub positions: Vec<BybitPosition>,
}

pub fn parse_position_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
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
            let empty_position = quantity.is_zero() && row.side.is_empty();
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
                unrealized_pnl: position_unrealized_pnl(&row.unrealised_pnl, empty_position)?,
                native_sequence: position_sequence(&row.seq, empty_position)?,
                updated_at_ms: position_updated_at(&row.updated_time, empty_position)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if sides != BTreeSet::from([PositionSide::Long, PositionSide::Short]) {
        return Err(BybitError::Payload);
    }
    Ok(BybitPositionPage {
        raw: raw.clone(),
        binding: raw.binding.clone(),
        generation: raw.generation,
        meta: page_meta(
            raw.request_cursor.as_deref(),
            envelope.result.next_page_cursor,
        ),
        positions,
    })
}

pub(crate) fn diagnose_position_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
) -> &'static str {
    if raw
        .validate(binding, BybitPrivateSource::Positions)
        .is_err()
    {
        return "raw_binding";
    }
    let envelope = match decode::<Envelope<Page<PositionRow>>>(&raw.payload) {
        Ok(envelope) => envelope,
        Err(_) => {
            let detail = serde_json::from_slice::<Envelope<Page<PositionRow>>>(&raw.payload)
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default();
            for (field, marker) in [
                ("symbol", "decode_symbol"),
                ("side", "decode_side"),
                ("size", "decode_size"),
                ("avgPrice", "decode_avg_price"),
                ("markPrice", "decode_mark_price"),
                ("liqPrice", "decode_liq_price"),
                ("unrealisedPnl", "decode_pnl"),
                ("positionIdx", "decode_position_index"),
                ("updatedTime", "decode_updated"),
                ("seq", "decode_sequence"),
                ("nextPageCursor", "decode_cursor"),
                ("category", "decode_category"),
            ] {
                if detail.contains(field) {
                    return marker;
                }
            }
            return "decode_other";
        }
    };
    if accepted(&envelope).is_err() {
        return "venue_rejected";
    }
    if validate_page(&envelope.result, &raw.native_symbol, 200).is_err() {
        return "page";
    }
    if envelope
        .result
        .validate_symbols(&raw.native_symbol)
        .is_err()
    {
        return "symbol";
    }
    let mut sides = BTreeSet::new();
    for row in envelope.result.list {
        let Ok(side) = position_side(row.position_idx) else {
            return "position_index";
        };
        if !sides.insert(side) {
            return "duplicate_side";
        }
        let Ok(quantity) = non_negative_decimal(&row.size) else {
            return "quantity";
        };
        let empty = quantity.is_zero() && row.side.is_empty();
        if !matches!(
            (row.position_idx, row.side.as_str(), quantity.is_zero()),
            (0, "Buy" | "Sell", _)
                | (0, "", true)
                | (1, "Buy", _)
                | (1, "", true)
                | (2, "Sell", _)
                | (2, "", true)
        ) {
            return "direction";
        }
        if optional_price(&row.avg_price).is_err()
            || optional_price(&row.mark_price).is_err()
            || optional_price(&row.liq_price).is_err()
        {
            return "price";
        }
        if position_unrealized_pnl(&row.unrealised_pnl, empty).is_err() {
            return "pnl";
        }
        if position_sequence(&row.seq, empty).is_err() {
            return "sequence";
        }
        if position_updated_at(&row.updated_time, empty).is_err() {
            return "updated";
        }
    }
    if sides != BTreeSet::from([PositionSide::Long, PositionSide::Short]) {
        return "hedge_sides";
    }
    "unknown"
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitApiKeyEvidence {
    pub raw: BybitRawPrivatePayload,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt_id: u64,
    pub observed_at_ms: u64,
    pub payload_sha256: String,
    pub read_only: bool,
    pub contract_order: bool,
    pub contract_position: bool,
    pub derivatives_trade: bool,
    pub withdraw: bool,
}

pub fn parse_api_key_evidence(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    raw: &BybitRawPrivatePayload,
) -> Result<BybitApiKeyEvidence, BybitError> {
    raw.validate(binding, BybitPrivateSource::ApiKeyInfo)?;
    let envelope: Envelope<ApiKeyInfo> = decode(&raw.payload)?;
    accepted(&envelope)?;
    let result = envelope.result;
    if result.api_key != credentials.api_key.expose_secret()
        || !result.secret.is_empty()
        || result.uta != 1
        || !matches!(result.read_only, 0 | 1)
    {
        return Err(BybitError::Capability);
    }
    validate_permissions(&result.permissions)?;
    Ok(BybitApiKeyEvidence {
        raw: raw.clone(),
        binding: raw.binding.clone(),
        generation: raw.generation,
        attempt_id: raw.attempt_id,
        observed_at_ms: raw.received_at_ms,
        payload_sha256: raw.payload_sha256.clone(),
        read_only: result.read_only == 1,
        contract_order: result
            .permissions
            .contract_trade
            .iter()
            .any(|value| value == "Order"),
        contract_position: result
            .permissions
            .contract_trade
            .iter()
            .any(|value| value == "Position"),
        derivatives_trade: result
            .permissions
            .derivatives
            .iter()
            .any(|value| value == "DerivativesTrade"),
        withdraw: result
            .permissions
            .wallet
            .iter()
            .any(|value| value == "Withdraw"),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOpenOrder {
    pub order: Order,
    pub family: NativeOrderFamily,
    pub native_order_type: String,
    pub native_time_in_force: String,
    pub position_idx: u8,
    pub stop_order_type: Option<String>,
    pub trigger_price: Option<Price>,
    pub close_on_trigger: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOpenOrderPage {
    pub raw: BybitRawPrivatePayload,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub family: NativeOrderFamily,
    pub received_at_ms: u64,
    pub meta: BybitPageMeta,
    pub orders: Vec<BybitOpenOrder>,
}

pub fn parse_open_order_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
) -> Result<BybitOpenOrderPage, BybitError> {
    let family = match raw.source {
        BybitPrivateSource::OpenOrders(family) => family,
        _ => return Err(BybitError::Binding),
    };
    raw.validate(binding, BybitPrivateSource::OpenOrders(family))?;
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
            normalize_order(raw, row, family, true)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitOpenOrderPage {
        raw: raw.clone(),
        binding: raw.binding.clone(),
        generation: raw.generation,
        family,
        received_at_ms: raw.received_at_ms,
        meta: page_meta(
            raw.request_cursor.as_deref(),
            envelope.result.next_page_cursor,
        ),
        orders,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderEvidence {
    pub order: Order,
    pub family: NativeOrderFamily,
    pub native_order_type: String,
    pub native_time_in_force: String,
    pub position_idx: u8,
    pub stop_order_type: Option<String>,
    pub trigger_price: Option<Price>,
    pub close_on_trigger: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderEvidencePage {
    pub raw: BybitRawPrivatePayload,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub family: NativeOrderFamily,
    pub received_at_ms: u64,
    pub meta: BybitPageMeta,
    pub orders: Vec<BybitOrderEvidence>,
}

pub fn parse_order_history_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
) -> Result<BybitOrderEvidencePage, BybitError> {
    let family = match raw.source {
        BybitPrivateSource::OrderHistory(family) => family,
        _ => return Err(BybitError::Binding),
    };
    raw.validate(binding, BybitPrivateSource::OrderHistory(family))?;
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
            let normalized = normalize_order(raw, row, family, false)?;
            Ok(BybitOrderEvidence {
                order: normalized.order,
                family,
                native_order_type: normalized.native_order_type,
                native_time_in_force: normalized.native_time_in_force,
                position_idx: normalized.position_idx,
                stop_order_type: normalized.stop_order_type,
                trigger_price: normalized.trigger_price,
                close_on_trigger: normalized.close_on_trigger,
                created_at_ms: normalized.created_at_ms,
                updated_at_ms: normalized.updated_at_ms,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitOrderEvidencePage {
        raw: raw.clone(),
        binding: raw.binding.clone(),
        generation: raw.generation,
        family,
        received_at_ms: raw.received_at_ms,
        meta: page_meta(
            raw.request_cursor.as_deref(),
            envelope.result.next_page_cursor,
        ),
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
    pub raw: BybitRawPrivatePayload,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub received_at_ms: u64,
    pub meta: BybitPageMeta,
    pub fills: Vec<BybitFill>,
}

pub fn parse_execution_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
    order_evidence: &[BybitOrderEvidence],
) -> Result<BybitExecutionPage, BybitError> {
    raw.validate(binding, BybitPrivateSource::Executions)?;
    let envelope: Envelope<Page<ExecutionRow>> = decode(&raw.payload)?;
    accepted(&envelope)?;
    validate_page(&envelope.result, &raw.native_symbol, 100)?;
    envelope.result.validate_symbols(&raw.native_symbol)?;
    let evidence = order_evidence
        .iter()
        .map(|item| (item.order.order_id.as_str(), item))
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
            if side != item.order.side
                || field_text(row.order_link_id.clone()) != item.order.client_order_id
            {
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
                position_side: item.order.position_side.clone(),
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
                client_order_id: item.order.client_order_id.clone(),
                closed_size,
                native_order_sequence: seq,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitExecutionPage {
        raw: raw.clone(),
        binding: raw.binding.clone(),
        generation: raw.generation,
        received_at_ms: raw.received_at_ms,
        meta: page_meta(
            raw.request_cursor.as_deref(),
            envelope.result.next_page_cursor,
        ),
        fills,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitAccountReadback {
    pub raw_payloads: Vec<BybitRawPrivatePayload>,
    pub identity: BybitAccountIdentity,
    pub wallet: BybitUnifiedWallet,
    pub attempt_id: u64,
    pub observed_at_ms: u64,
}

pub fn complete_account_readback(
    binding: &BybitGatewayBinding,
    account_raw: BybitRawPrivatePayload,
    wallet_raw: BybitRawPrivatePayload,
) -> Result<BybitAccountReadback, BybitError> {
    let identity = parse_account_identity(binding, &account_raw)?;
    let wallet = parse_unified_wallet(binding, &identity, &wallet_raw)?;
    validate_same_attempt(&account_raw, &wallet_raw)?;
    let observed_at_ms = account_raw.received_at_ms.max(wallet_raw.received_at_ms);
    Ok(BybitAccountReadback {
        attempt_id: account_raw.attempt_id,
        raw_payloads: vec![account_raw, wallet_raw],
        identity,
        wallet,
        observed_at_ms,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitPositionReadback {
    pub raw_pages: Vec<BybitRawPrivatePayload>,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt_id: u64,
    pub observed_at_ms: u64,
    pub hedge_mode: bool,
    pub positions: Vec<BybitPosition>,
}

pub fn complete_position_pages(
    binding: &BybitGatewayBinding,
    pages: &[BybitPositionPage],
) -> Result<BybitPositionReadback, BybitError> {
    validate_page_chain(
        binding,
        pages.iter().map(|page| (&page.raw, &page.meta)),
        BybitPrivateSource::Positions,
    )?;
    let mut sides = BTreeSet::new();
    let mut positions = Vec::new();
    for page in pages {
        if parse_position_page(binding, &page.raw)? != *page
            || (page.positions.is_empty() && page.meta.next_cursor.is_some())
        {
            return Err(BybitError::Projection);
        }
        for position in &page.positions {
            if !sides.insert(position.position.side) {
                return Err(BybitError::Pagination);
            }
            positions.push(position.clone());
        }
    }
    let first = pages.first().ok_or(BybitError::Pagination)?;
    if sides != BTreeSet::from([PositionSide::Long, PositionSide::Short]) {
        return Err(BybitError::Payload);
    }
    Ok(BybitPositionReadback {
        raw_pages: pages.iter().map(|page| page.raw.clone()).collect(),
        binding: first.binding.clone(),
        generation: first.generation,
        attempt_id: first.raw.attempt_id,
        observed_at_ms: pages
            .iter()
            .map(|page| page.raw.received_at_ms)
            .max()
            .ok_or(BybitError::Pagination)?,
        hedge_mode: true,
        positions,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOpenOrdersReadback {
    pub raw_pages: Vec<BybitRawPrivatePayload>,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt_id: u64,
    pub observed_at_ms: u64,
    pub family: NativeOrderFamily,
    pub orders: Vec<BybitOpenOrder>,
}

pub fn complete_open_order_pages(
    binding: &BybitGatewayBinding,
    family: NativeOrderFamily,
    pages: &[BybitOpenOrderPage],
) -> Result<BybitOpenOrdersReadback, BybitError> {
    let source = BybitPrivateSource::OpenOrders(family);
    validate_page_chain(
        binding,
        pages.iter().map(|page| (&page.raw, &page.meta)),
        source,
    )?;
    let mut native_ids = BTreeSet::new();
    let mut client_ids = BTreeSet::new();
    let mut orders = Vec::new();
    for page in pages {
        if page.family != family
            || parse_open_order_page(binding, &page.raw)? != *page
            || (page.orders.is_empty() && page.meta.next_cursor.is_some())
        {
            return Err(BybitError::Projection);
        }
        for order in &page.orders {
            if !native_ids.insert(order.order.order_id.clone())
                || matches!(
                    &order.order.client_order_id,
                    FieldState::Known(value) if !client_ids.insert(value.clone())
                )
            {
                return Err(BybitError::Pagination);
            }
            orders.push(order.clone());
        }
    }
    let first = pages.first().ok_or(BybitError::Pagination)?;
    Ok(BybitOpenOrdersReadback {
        raw_pages: pages.iter().map(|page| page.raw.clone()).collect(),
        binding: first.binding.clone(),
        generation: first.generation,
        attempt_id: first.raw.attempt_id,
        observed_at_ms: pages
            .iter()
            .map(|page| page.received_at_ms)
            .max()
            .ok_or(BybitError::Pagination)?,
        family,
        orders,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitOrderHistoryReadback {
    pub raw_pages: Vec<BybitRawPrivatePayload>,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt_id: u64,
    pub observed_at_ms: u64,
    pub family: NativeOrderFamily,
    pub orders: Vec<BybitOrderEvidence>,
}

pub fn complete_order_history_pages(
    binding: &BybitGatewayBinding,
    family: NativeOrderFamily,
    pages: &[BybitOrderEvidencePage],
) -> Result<BybitOrderHistoryReadback, BybitError> {
    let source = BybitPrivateSource::OrderHistory(family);
    validate_page_chain(
        binding,
        pages.iter().map(|page| (&page.raw, &page.meta)),
        source,
    )?;
    let mut ids = BTreeSet::new();
    let mut orders = Vec::new();
    for page in pages {
        if page.family != family
            || parse_order_history_page(binding, &page.raw)? != *page
            || (page.orders.is_empty() && page.meta.next_cursor.is_some())
        {
            return Err(BybitError::Projection);
        }
        for order in &page.orders {
            if !ids.insert(order.order.order_id.clone()) {
                return Err(BybitError::Pagination);
            }
            orders.push(order.clone());
        }
    }
    let first = pages.first().ok_or(BybitError::Pagination)?;
    Ok(BybitOrderHistoryReadback {
        raw_pages: pages.iter().map(|page| page.raw.clone()).collect(),
        binding: first.binding.clone(),
        generation: first.generation,
        attempt_id: first.raw.attempt_id,
        observed_at_ms: pages
            .iter()
            .map(|page| page.received_at_ms)
            .max()
            .ok_or(BybitError::Pagination)?,
        family,
        orders,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitFillReadback {
    pub raw_pages: Vec<BybitRawPrivatePayload>,
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt_id: u64,
    pub observed_at_ms: u64,
    pub fills: Vec<BybitFill>,
}

pub fn complete_execution_pages(
    binding: &BybitGatewayBinding,
    pages: &[BybitExecutionPage],
    order_evidence: &[BybitOrderEvidence],
) -> Result<BybitFillReadback, BybitError> {
    validate_page_chain(
        binding,
        pages.iter().map(|page| (&page.raw, &page.meta)),
        BybitPrivateSource::Executions,
    )?;
    let mut ids = BTreeSet::new();
    let mut previous_time = None;
    let mut fills = Vec::new();
    for page in pages {
        if parse_execution_page(binding, &page.raw, order_evidence)? != *page
            || (page.fills.is_empty() && page.meta.next_cursor.is_some())
        {
            return Err(BybitError::Projection);
        }
        for fill in &page.fills {
            let time = fill.fill.exchange_time_ms.ok_or(BybitError::Payload)?;
            if !ids.insert(fill.fill.fill_id.clone())
                || previous_time.is_some_and(|prior| time > prior)
            {
                return Err(BybitError::Pagination);
            }
            previous_time = Some(time);
            fills.push(fill.clone());
        }
    }
    let first = pages.first().ok_or(BybitError::Pagination)?;
    Ok(BybitFillReadback {
        raw_pages: pages.iter().map(|page| page.raw.clone()).collect(),
        binding: first.binding.clone(),
        generation: first.generation,
        attempt_id: first.raw.attempt_id,
        observed_at_ms: pages
            .iter()
            .map(|page| page.received_at_ms)
            .max()
            .ok_or(BybitError::Pagination)?,
        fills,
    })
}

fn validate_page_chain<'a>(
    binding: &BybitGatewayBinding,
    pages: impl Iterator<Item = (&'a BybitRawPrivatePayload, &'a BybitPageMeta)>,
    source: BybitPrivateSource,
) -> Result<(), BybitError> {
    let pages = pages.collect::<Vec<_>>();
    if pages.is_empty() || pages.len() > BYBIT_PRIVATE_MAX_PAGES {
        return Err(BybitError::Pagination);
    }
    let first = pages.first().ok_or(BybitError::Pagination)?.0;
    let mut closure = BybitPageClosure::default();
    let mut previous_request_timestamp_ms = None;
    let mut previous_received_at_ms = None;
    for (index, (raw, meta)) in pages.iter().enumerate() {
        raw.validate(binding, source)?;
        if raw.binding != first.binding
            || raw.generation != first.generation
            || raw.attempt_id != first.attempt_id
            || raw.history_window != first.history_window
            || raw.lookup != first.lookup
            || usize::try_from(raw.page_index).map_err(|_| BybitError::Pagination)? != index
            || meta.requested_cursor != raw.request_cursor
            || previous_request_timestamp_ms
                .is_some_and(|previous| raw.request_timestamp_ms < previous)
            || previous_received_at_ms.is_some_and(|previous| raw.received_at_ms < previous)
        {
            return Err(BybitError::Pagination);
        }
        previous_request_timestamp_ms = Some(raw.request_timestamp_ms);
        previous_received_at_ms = Some(raw.received_at_ms);
        closure.accept(meta)?;
    }
    if closure.is_closed() {
        Ok(())
    } else {
        Err(BybitError::Pagination)
    }
}

fn validate_same_attempt(
    left: &BybitRawPrivatePayload,
    right: &BybitRawPrivatePayload,
) -> Result<(), BybitError> {
    if left.binding == right.binding
        && left.generation == right.generation
        && left.attempt_id == right.attempt_id
    {
        Ok(())
    } else {
        Err(BybitError::Binding)
    }
}

struct OrderFamilyFields {
    stop_order_type: Option<String>,
    trigger_price: Option<Price>,
}

fn validate_order_family(
    row: &OrderRow,
    family: NativeOrderFamily,
) -> Result<OrderFamilyFields, BybitError> {
    let stop_order_type = match row.stop_order_type.as_str() {
        "" | "UNKNOWN" => None,
        value if value.len() <= 64 && value.bytes().all(|byte| byte.is_ascii_alphanumeric()) => {
            Some(value.to_owned())
        }
        _ => return Err(BybitError::Payload),
    };
    let trigger_price = optional_zero_price(&row.trigger_price)?;
    match family {
        NativeOrderFamily::UmOrder
            if stop_order_type.is_none() && trigger_price.is_none() && !row.close_on_trigger => {}
        NativeOrderFamily::UmConditional
            if stop_order_type.is_some() && trigger_price.is_some() => {}
        _ => return Err(BybitError::OrderFamily),
    }
    Ok(OrderFamilyFields {
        stop_order_type,
        trigger_price,
    })
}

fn optional_zero_price(value: &str) -> Result<Option<Price>, BybitError> {
    if value.is_empty() {
        return Ok(None);
    }
    let value = decimal(value)?;
    if value.is_zero() {
        Ok(None)
    } else {
        Price::new(value).map(Some).map_err(|_| BybitError::Payload)
    }
}

fn normalize_order(
    raw: &BybitRawPrivatePayload,
    row: OrderRow,
    family: NativeOrderFamily,
    open_only: bool,
) -> Result<BybitOpenOrder, BybitError> {
    if row.order_id.is_empty() {
        return Err(BybitError::Payload);
    }
    let state = order_state(&row.order_status)?;
    if open_only && !matches!(state, OrderState::New | OrderState::PartiallyFilled) {
        return Err(BybitError::Payload);
    }
    let family_fields = validate_order_family(&row, family)?;
    let native_order_type = validate_native_order_type(&row.order_type)?.to_owned();
    let native_time_in_force = validate_native_time_in_force(&row.time_in_force)?.to_owned();
    let time_in_force = canonical_limit_time_in_force(&native_order_type, &native_time_in_force);
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
        time_in_force,
        average_price: optional_field_price(&row.avg_price)?,
        reduce_only,
    };
    order.validate().map_err(|_| BybitError::Payload)?;
    Ok(BybitOpenOrder {
        order,
        family,
        native_order_type,
        native_time_in_force,
        position_idx: row.position_idx,
        stop_order_type: family_fields.stop_order_type,
        trigger_price: family_fields.trigger_price,
        close_on_trigger: row.close_on_trigger,
        created_at_ms: positive_u64(&row.created_time)?,
        updated_at_ms: positive_u64(&row.updated_time)?,
    })
}

fn canonical_limit_time_in_force(order_type: &str, value: &str) -> FieldState<LimitTimeInForce> {
    if order_type != "Limit" {
        return FieldState::NotApplicable;
    }
    match value {
        "PostOnly" => FieldState::Known(LimitTimeInForce::PostOnly),
        "GTC" => FieldState::Known(LimitTimeInForce::Gtc),
        _ => FieldState::Unavailable {
            reason: UnknownReason::Ambiguous,
        },
    }
}

fn validate_native_order_type(value: &str) -> Result<&str, BybitError> {
    match value {
        "Limit" | "Market" => Ok(value),
        _ => Err(BybitError::Payload),
    }
}

fn validate_native_time_in_force(value: &str) -> Result<&str, BybitError> {
    match value {
        "GTC" | "IOC" | "FOK" | "PostOnly" | "RPI" => Ok(value),
        _ => Err(BybitError::Payload),
    }
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

fn order_state(value: &str) -> Result<OrderState, BybitError> {
    match value {
        "Created" | "New" | "Untriggered" | "Active" => Ok(OrderState::New),
        "PartiallyFilled" => Ok(OrderState::PartiallyFilled),
        "Filled" => Ok(OrderState::Filled),
        "Cancelled" | "Deactivated" | "PartiallyFilledCanceled" | "PartiallyFilledCancelled" => {
            Ok(OrderState::Cancelled)
        }
        "Rejected" => Ok(OrderState::Rejected),
        "PendingCancel" | "Triggered" => Ok(OrderState::Unknown),
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
fn position_sequence(value: &str, empty_position: bool) -> Result<u64, BybitError> {
    if empty_position && matches!(value, "" | "-1" | "0") {
        Ok(0)
    } else {
        positive_u64(value)
    }
}
fn position_updated_at(value: &str, empty_position: bool) -> Result<u64, BybitError> {
    if empty_position && matches!(value, "" | "0") {
        Ok(0)
    } else {
        positive_u64(value)
    }
}
fn position_unrealized_pnl(value: &str, empty_position: bool) -> Result<Decimal, BybitError> {
    if empty_position && value.is_empty() {
        Ok(Decimal::ZERO)
    } else {
        decimal(value)
    }
}

#[cfg(test)]
mod empty_position_sentinel_tests {
    use super::*;

    #[test]
    fn never_traded_sentinels_are_only_valid_for_empty_positions() {
        assert_eq!(position_sequence("-1", true), Ok(0));
        assert_eq!(position_updated_at("0", true), Ok(0));
        assert_eq!(position_unrealized_pnl("", true), Ok(Decimal::ZERO));
        assert_eq!(position_sequence("7", false), Ok(7));
        assert!(position_sequence("-1", false).is_err());
        assert!(position_updated_at("0", false).is_err());
        assert!(position_unrealized_pnl("", false).is_err());
    }

    #[test]
    fn native_limit_policy_is_projected_without_inventing_unsupported_values() {
        assert_eq!(
            canonical_limit_time_in_force("Limit", "PostOnly"),
            FieldState::Known(LimitTimeInForce::PostOnly)
        );
        assert_eq!(
            canonical_limit_time_in_force("Limit", "GTC"),
            FieldState::Known(LimitTimeInForce::Gtc)
        );
        assert!(matches!(
            canonical_limit_time_in_force("Limit", "IOC"),
            FieldState::Unavailable {
                reason: UnknownReason::Ambiguous
            }
        ));
    }
}
fn optional_price(value: &str) -> Result<Option<Price>, BybitError> {
    if value.is_empty() {
        Ok(None)
    } else {
        let value = decimal(value)?;
        if value.is_zero() {
            Ok(None)
        } else {
            Ok(Some(Price::new(value).map_err(|_| BybitError::Payload)?))
        }
    }
}
fn optional_field_price(value: &str) -> Result<FieldState<Price>, BybitError> {
    optional_price(value).map(|price| price.map_or(FieldState::Missing, FieldState::Known))
}
fn decode<'a, T: Deserialize<'a>>(payload: &'a [u8]) -> Result<T, BybitError> {
    serde_json::from_slice(payload).map_err(|_| BybitError::Payload)
}
fn nullable_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Option::unwrap_or_default)
}
fn string_or_integer<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Text(String),
        Signed(i64),
        Unsigned(u64),
        Missing(Option<()>),
    }
    match Value::deserialize(deserializer)? {
        Value::Text(value) => Ok(value),
        Value::Signed(value) => Ok(value.to_string()),
        Value::Unsigned(value) => Ok(value.to_string()),
        Value::Missing(None | Some(())) => Ok(String::new()),
    }
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
#[serde(rename_all = "camelCase")]
struct ApiKeyInfo {
    api_key: String,
    read_only: u8,
    secret: String,
    permissions: ApiPermissions,
    uta: u8,
}

#[derive(Deserialize)]
struct ApiPermissions {
    #[serde(rename = "ContractTrade", default)]
    contract_trade: Vec<String>,
    #[serde(rename = "Wallet", default)]
    wallet: Vec<String>,
    #[serde(rename = "Derivatives", default)]
    derivatives: Vec<String>,
}

fn validate_permissions(permissions: &ApiPermissions) -> Result<(), BybitError> {
    for values in [
        &permissions.contract_trade,
        &permissions.wallet,
        &permissions.derivatives,
    ] {
        let mut seen = BTreeSet::new();
        if values
            .iter()
            .any(|value| value.is_empty() || !seen.insert(value))
        {
            return Err(BybitError::Capability);
        }
    }
    Ok(())
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
    #[serde(default, deserialize_with = "nullable_string")]
    next_page_cursor: String,
    list: Vec<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    symbol: String,
    #[serde(default, deserialize_with = "nullable_string")]
    side: String,
    size: String,
    avg_price: String,
    mark_price: String,
    liq_price: String,
    unrealised_pnl: String,
    position_idx: u8,
    updated_time: String,
    #[serde(default, deserialize_with = "string_or_integer")]
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
    order_type: String,
    time_in_force: String,
    qty: String,
    cum_exec_qty: String,
    price: String,
    avg_price: String,
    reduce_only: bool,
    #[serde(default)]
    stop_order_type: String,
    #[serde(default)]
    trigger_price: String,
    #[serde(default)]
    close_on_trigger: bool,
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
