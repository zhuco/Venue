use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    AccountBalance, Fill, NativeOrderFamily, Order, Position, PositionSide, Price,
};
use venue_gateway_api::GatewayBinding;

use crate::private::{
    AlgoOrderReadback, PrivateAccountCapabilities, RecentFillsCursor, RecentFillsPaginationError,
    paginate_recent_fills, parse_fills, parse_open_algo_order_facts, parse_open_algo_orders,
    parse_orders,
};
use crate::{
    BinanceConfig, BinanceHttpMethod, BinanceInstrumentRules, endpoints, native_symbol, portfolio,
};

pub const BINANCE_PRIVATE_READBACK_SCHEMA_VERSION: u16 = 1;
pub const BINANCE_EXECUTION_PROFILE_VERSION: u64 = 1;
pub const BINANCE_PRIVATE_MAX_PAGES: usize = 10_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BinancePrivateSurface {
    Account,
    AccountConfig,
    PositionMode,
    Positions,
    RegularOrders,
    AlgoOrders,
    Fills,
    ExactOrder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinancePositionMode {
    Net,
    Hedge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePrivateReadScope {
    binding: GatewayBinding,
    instrument_generation: u64,
    private_generation: u64,
    attempt_id: u64,
    requested_at_ms: u64,
}

impl BinancePrivateReadScope {
    pub fn new(
        config: &BinanceConfig,
        rules: &BinanceInstrumentRules,
        private_generation: u64,
        attempt_id: u64,
        requested_at_ms: u64,
    ) -> Result<Self, BinanceReadbackError> {
        let scope = Self {
            binding: config.gateway_binding().clone(),
            instrument_generation: rules.instrument.generation,
            private_generation,
            attempt_id,
            requested_at_ms,
        };
        scope.validate(config, rules)?;
        Ok(scope)
    }

    pub fn validate(
        &self,
        config: &BinanceConfig,
        rules: &BinanceInstrumentRules,
    ) -> Result<(), BinanceReadbackError> {
        if &self.binding != config.gateway_binding()
            || self.binding.symbol != rules.instrument.symbol
            || self.instrument_generation == 0
            || self.instrument_generation != rules.instrument.generation
            || self.private_generation == 0
            || self.attempt_id == 0
            || self.requested_at_ms == 0
            || rules.native_symbol != native_symbol(&self.binding.symbol)
        {
            return Err(BinanceReadbackError::Binding);
        }
        Ok(())
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn instrument_generation(&self) -> u64 {
        self.instrument_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub const fn requested_at_ms(&self) -> u64 {
        self.requested_at_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePrivateReadRequest {
    scope: BinancePrivateReadScope,
    surface: BinancePrivateSurface,
    page_index: u32,
    parameters: Vec<(String, String)>,
}

impl BinancePrivateReadRequest {
    fn new(
        scope: &BinancePrivateReadScope,
        surface: BinancePrivateSurface,
        page_index: u32,
        parameters: Vec<(String, String)>,
    ) -> Result<Self, BinanceReadbackError> {
        if page_index == 0
            || parameters
                .iter()
                .any(|(key, value)| key.is_empty() || value.is_empty())
        {
            return Err(BinanceReadbackError::Request);
        }
        Ok(Self {
            scope: scope.clone(),
            surface,
            page_index,
            parameters,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &BinancePrivateReadScope {
        &self.scope
    }

    #[must_use]
    pub const fn surface(&self) -> BinancePrivateSurface {
        self.surface
    }

    #[must_use]
    pub const fn page_index(&self) -> u32 {
        self.page_index
    }

    #[must_use]
    pub const fn method(&self) -> BinanceHttpMethod {
        BinanceHttpMethod::Get
    }

    #[must_use]
    pub const fn path(&self) -> &'static str {
        match self.surface {
            BinancePrivateSurface::Account => endpoints::ACCOUNT,
            BinancePrivateSurface::AccountConfig => endpoints::ACCOUNT_CONFIG,
            BinancePrivateSurface::PositionMode => endpoints::POSITION_MODE,
            BinancePrivateSurface::Positions => endpoints::POSITIONS,
            BinancePrivateSurface::RegularOrders => endpoints::OPEN_ORDERS,
            BinancePrivateSurface::AlgoOrders => endpoints::OPEN_ALGO_ORDERS,
            BinancePrivateSurface::Fills => endpoints::USER_TRADES,
            BinancePrivateSurface::ExactOrder => endpoints::ORDER,
        }
    }

    #[must_use]
    pub fn parameters(&self) -> &[(String, String)] {
        &self.parameters
    }
}

pub fn build_account_request(
    scope: &BinancePrivateReadScope,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    BinancePrivateReadRequest::new(scope, BinancePrivateSurface::Account, 1, Vec::new())
}

pub fn build_account_config_request(
    scope: &BinancePrivateReadScope,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    BinancePrivateReadRequest::new(scope, BinancePrivateSurface::AccountConfig, 1, Vec::new())
}

pub fn build_position_mode_request(
    scope: &BinancePrivateReadScope,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    BinancePrivateReadRequest::new(scope, BinancePrivateSurface::PositionMode, 1, Vec::new())
}

fn symbol_parameters(scope: &BinancePrivateReadScope) -> Vec<(String, String)> {
    vec![("symbol".to_owned(), native_symbol(&scope.binding.symbol))]
}

pub fn build_positions_request(
    scope: &BinancePrivateReadScope,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    BinancePrivateReadRequest::new(
        scope,
        BinancePrivateSurface::Positions,
        1,
        symbol_parameters(scope),
    )
}

pub fn build_regular_orders_request(
    scope: &BinancePrivateReadScope,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    BinancePrivateReadRequest::new(
        scope,
        BinancePrivateSurface::RegularOrders,
        1,
        symbol_parameters(scope),
    )
}

pub fn build_algo_orders_request(
    scope: &BinancePrivateReadScope,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    BinancePrivateReadRequest::new(
        scope,
        BinancePrivateSurface::AlgoOrders,
        1,
        vec![
            ("algoType".to_owned(), "CONDITIONAL".to_owned()),
            ("symbol".to_owned(), native_symbol(&scope.binding.symbol)),
        ],
    )
}

pub fn build_fills_request(
    scope: &BinancePrivateReadScope,
    page_index: u32,
    cursor: RecentFillsCursor,
    start_time_ms: u64,
    end_time_ms: u64,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    if start_time_ms == 0
        || end_time_ms < start_time_ms
        || cursor.last_trade_id.is_some() != cursor.last_event_time_ms.is_some()
    {
        return Err(BinanceReadbackError::Request);
    }
    let mut parameters = symbol_parameters(scope);
    parameters.push(("limit".to_owned(), "1000".to_owned()));
    match cursor.last_trade_id {
        Some(last_id) => parameters.push((
            "fromId".to_owned(),
            last_id
                .checked_add(1)
                .ok_or(BinanceReadbackError::Pagination)?
                .to_string(),
        )),
        None => {
            parameters.push(("startTime".to_owned(), start_time_ms.to_string()));
            parameters.push(("endTime".to_owned(), end_time_ms.to_string()));
        }
    }
    BinancePrivateReadRequest::new(scope, BinancePrivateSurface::Fills, page_index, parameters)
}

pub fn build_exact_order_request(
    scope: &BinancePrivateReadScope,
    client_order_id: &str,
) -> Result<BinancePrivateReadRequest, BinanceReadbackError> {
    validate_client_order_id(client_order_id)?;
    BinancePrivateReadRequest::new(
        scope,
        BinancePrivateSurface::ExactOrder,
        1,
        vec![
            ("symbol".to_owned(), native_symbol(&scope.binding.symbol)),
            ("origClientOrderId".to_owned(), client_order_id.to_owned()),
        ],
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRawPrivatePage {
    pub scope: BinancePrivateReadScope,
    pub surface: BinancePrivateSurface,
    pub page_index: u32,
    pub requested_at_ms: u64,
    pub received_at_ms: u64,
    pub payload: Bytes,
    request_parameters: Vec<(String, String)>,
}

impl BinanceRawPrivatePage {
    pub fn new(
        request: &BinancePrivateReadRequest,
        requested_at_ms: u64,
        received_at_ms: u64,
        payload: impl Into<Bytes>,
    ) -> Result<Self, BinanceReadbackError> {
        let value = Self {
            scope: request.scope.clone(),
            surface: request.surface,
            page_index: request.page_index,
            requested_at_ms,
            received_at_ms,
            payload: payload.into(),
            request_parameters: request.parameters.clone(),
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), BinanceReadbackError> {
        if self.page_index == 0
            || self.requested_at_ms < self.scope.requested_at_ms
            || self.received_at_ms < self.requested_at_ms
            || self.payload.is_empty()
        {
            return Err(BinanceReadbackError::Page);
        }
        Ok(())
    }

    fn payload_str(&self) -> Result<&str, BinanceReadbackError> {
        std::str::from_utf8(&self.payload).map_err(|_| BinanceReadbackError::Payload)
    }

    #[must_use]
    pub fn request_parameters(&self) -> &[(String, String)] {
        &self.request_parameters
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceUnsupportedOrderFamilyEvidence {
    pub binding: GatewayBinding,
    pub private_generation: u64,
    pub profile_version: u64,
    pub family: NativeOrderFamily,
    pub reason: &'static str,
}

impl BinanceUnsupportedOrderFamilyEvidence {
    fn conditional(scope: &BinancePrivateReadScope) -> Self {
        Self {
            binding: scope.binding.clone(),
            private_generation: scope.private_generation,
            profile_version: BINANCE_EXECUTION_PROFILE_VERSION,
            family: NativeOrderFamily::UmConditional,
            reason: "retired PAPI UM conditional endpoints are not an admissible order surface",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceOrderFamilyReadback {
    pub family: NativeOrderFamily,
    pub orders: Vec<Order>,
    pub raw_pages: Vec<BinanceRawPrivatePage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRegularOrderSemantics {
    pub order_id: String,
    pub client_order_id: String,
    pub order_type: String,
    pub time_in_force: String,
    pub position_side: PositionSide,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePrivateReadbackCandidate {
    scope: BinancePrivateReadScope,
    pub(crate) position_mode: BinancePositionMode,
    pub(crate) capabilities: PrivateAccountCapabilities,
    pub(crate) balances: Vec<AccountBalance>,
    pub(crate) positions: Vec<Position>,
    pub(crate) regular: BinanceOrderFamilyReadback,
    pub(crate) regular_semantics: Vec<BinanceRegularOrderSemantics>,
    pub(crate) conditional: BinanceUnsupportedOrderFamilyEvidence,
    pub(crate) algo: BinanceOrderFamilyReadback,
    pub(crate) algo_custody: Vec<AlgoOrderReadback>,
    pub(crate) fills: Vec<Fill>,
    pub(crate) fills_cursor: RecentFillsCursor,
    pub(crate) raw_payload_digest: [u8; 32],
}

impl BinancePrivateReadbackCandidate {
    #[must_use]
    pub const fn scope(&self) -> &BinancePrivateReadScope {
        &self.scope
    }

    #[must_use]
    pub const fn position_mode(&self) -> BinancePositionMode {
        self.position_mode
    }

    #[must_use]
    pub const fn capabilities(&self) -> PrivateAccountCapabilities {
        self.capabilities
    }

    #[must_use]
    pub fn balances(&self) -> &[AccountBalance] {
        &self.balances
    }

    #[must_use]
    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    #[must_use]
    pub const fn regular(&self) -> &BinanceOrderFamilyReadback {
        &self.regular
    }

    #[must_use]
    pub fn regular_semantics(&self) -> &[BinanceRegularOrderSemantics] {
        &self.regular_semantics
    }

    #[must_use]
    pub const fn conditional(&self) -> &BinanceUnsupportedOrderFamilyEvidence {
        &self.conditional
    }

    #[must_use]
    pub const fn algo(&self) -> &BinanceOrderFamilyReadback {
        &self.algo
    }

    #[must_use]
    pub fn algo_custody(&self) -> &[AlgoOrderReadback] {
        &self.algo_custody
    }

    #[must_use]
    pub fn fills(&self) -> &[Fill] {
        &self.fills
    }

    #[must_use]
    pub const fn fills_cursor(&self) -> RecentFillsCursor {
        self.fills_cursor
    }

    #[must_use]
    pub const fn raw_payload_digest(&self) -> [u8; 32] {
        self.raw_payload_digest
    }

    #[must_use]
    pub fn order_family(&self, family: NativeOrderFamily) -> Option<&BinanceOrderFamilyReadback> {
        match family {
            NativeOrderFamily::UmOrder => Some(&self.regular),
            NativeOrderFamily::UmConditional => None,
            NativeOrderFamily::UmAlgo => Some(&self.algo),
        }
    }
}

pub fn complete_private_readback(
    config: &BinanceConfig,
    rules: &BinanceInstrumentRules,
    scope: &BinancePrivateReadScope,
    initial_fills_cursor: RecentFillsCursor,
    fills_target_through_ms: u64,
    pages: Vec<BinanceRawPrivatePage>,
) -> Result<BinancePrivateReadbackCandidate, BinanceReadbackError> {
    scope.validate(config, rules)?;
    if pages.is_empty() || pages.len() > BINANCE_PRIVATE_MAX_PAGES {
        return Err(BinanceReadbackError::Pagination);
    }
    let mut grouped = BTreeMap::<BinancePrivateSurface, Vec<BinanceRawPrivatePage>>::new();
    for page in &pages {
        page.validate()?;
        if &page.scope != scope || page.surface == BinancePrivateSurface::ExactOrder {
            return Err(BinanceReadbackError::Binding);
        }
        grouped.entry(page.surface).or_default().push(page.clone());
    }
    let account = one(&mut grouped, BinancePrivateSurface::Account)?;
    let account_config = one(&mut grouped, BinancePrivateSurface::AccountConfig)?;
    let position_mode = one(&mut grouped, BinancePrivateSurface::PositionMode)?;
    let positions = one(&mut grouped, BinancePrivateSurface::Positions)?;
    let regular = one(&mut grouped, BinancePrivateSurface::RegularOrders)?;
    let algo = one(&mut grouped, BinancePrivateSurface::AlgoOrders)?;
    let fills = grouped
        .remove(&BinancePrivateSurface::Fills)
        .ok_or(BinanceReadbackError::OrderFamily)?;
    if !grouped.is_empty() {
        return Err(BinanceReadbackError::Page);
    }

    let capabilities =
        portfolio::capabilities(account_config.payload_str()?, position_mode.payload_str()?)
            .map_err(|_| BinanceReadbackError::Payload)?;
    let mode = if capabilities.hedge_position && !capabilities.one_way_position {
        BinancePositionMode::Hedge
    } else if capabilities.one_way_position && !capabilities.hedge_position {
        BinancePositionMode::Net
    } else {
        return Err(BinanceReadbackError::Position);
    };
    let balances = vec![
        portfolio::parse_account_balance(account.payload_str()?)
            .map_err(|_| BinanceReadbackError::Payload)?,
    ];
    let positions = parse_complete_positions(positions.payload_str()?, &scope.binding, mode)?;
    let regular_orders = parse_orders(regular.payload_str()?, &scope.binding.symbol)
        .map_err(|_| BinanceReadbackError::OrderFamily)?;
    let regular_semantics = parse_regular_semantics(
        regular.payload_str()?,
        &scope.binding,
        mode,
        &regular_orders,
    )?;
    let algo_orders = parse_open_algo_order_facts(algo.payload_str()?, &scope.binding.symbol)
        .map_err(|_| BinanceReadbackError::OrderFamily)?;
    let algo_custody = parse_open_algo_orders(algo.payload_str()?, &scope.binding.symbol)
        .map_err(|_| BinanceReadbackError::OrderFamily)?;
    let (fills, fills_cursor) = complete_fill_pages(
        initial_fills_cursor,
        fills_target_through_ms,
        fills,
        &scope.binding,
    )?;

    Ok(BinancePrivateReadbackCandidate {
        scope: scope.clone(),
        position_mode: mode,
        capabilities,
        balances,
        positions,
        regular: BinanceOrderFamilyReadback {
            family: NativeOrderFamily::UmOrder,
            orders: regular_orders,
            raw_pages: vec![regular],
        },
        regular_semantics,
        conditional: BinanceUnsupportedOrderFamilyEvidence::conditional(scope),
        algo: BinanceOrderFamilyReadback {
            family: NativeOrderFamily::UmAlgo,
            orders: algo_orders,
            raw_pages: vec![algo],
        },
        algo_custody,
        fills,
        fills_cursor,
        raw_payload_digest: digest_pages(&pages),
    })
}

fn parse_regular_semantics(
    payload: &str,
    binding: &GatewayBinding,
    mode: BinancePositionMode,
    orders: &[Order],
) -> Result<Vec<BinanceRegularOrderSemantics>, BinanceReadbackError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| BinanceReadbackError::Payload)?;
    let rows = value.as_array().ok_or(BinanceReadbackError::OrderFamily)?;
    if rows.len() != orders.len() {
        return Err(BinanceReadbackError::OrderFamily);
    }
    let mut seen = BTreeSet::new();
    let mut semantics = Vec::with_capacity(rows.len());
    for (row, order) in rows.iter().zip(orders) {
        let row = row.as_object().ok_or(BinanceReadbackError::OrderFamily)?;
        let order_id = json_identifier(row.get("orderId"))?;
        let client_order_id = row
            .get("clientOrderId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or(BinanceReadbackError::OrderFamily)?;
        let position_side = match row.get("positionSide").and_then(Value::as_str) {
            Some("BOTH") if mode == BinancePositionMode::Net => PositionSide::Net,
            Some("LONG") if mode == BinancePositionMode::Hedge => PositionSide::Long,
            Some("SHORT") if mode == BinancePositionMode::Hedge => PositionSide::Short,
            _ => return Err(BinanceReadbackError::OrderFamily),
        };
        let order_type = row
            .get("type")
            .and_then(Value::as_str)
            .filter(|value| matches!(*value, "LIMIT" | "MARKET"))
            .ok_or(BinanceReadbackError::OrderFamily)?;
        let time_in_force = row
            .get("timeInForce")
            .and_then(Value::as_str)
            .filter(|value| matches!(*value, "GTC" | "GTX" | "IOC" | "FOK"))
            .ok_or(BinanceReadbackError::OrderFamily)?;
        let reduce_only = row
            .get("reduceOnly")
            .and_then(Value::as_bool)
            .ok_or(BinanceReadbackError::OrderFamily)?;
        if order.order_id != order_id
            || !matches!(&order.client_order_id, venue_domain::domain::FieldState::Known(value) if value == client_order_id)
            || !matches!(
                order.state,
                venue_domain::domain::OrderState::New
                    | venue_domain::domain::OrderState::PartiallyFilled
            )
            || !seen.insert(order_id.clone())
            || binding.symbol != order.symbol
            || reduce_only != order.reduce_only
        {
            return Err(BinanceReadbackError::OrderFamily);
        }
        semantics.push(BinanceRegularOrderSemantics {
            order_id,
            client_order_id: client_order_id.to_owned(),
            order_type: order_type.to_owned(),
            time_in_force: time_in_force.to_owned(),
            position_side,
            reduce_only,
        });
    }
    Ok(semantics)
}

fn json_identifier(value: Option<&Value>) -> Result<String, BinanceReadbackError> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) if value.as_u64().is_some() => Ok(value.to_string()),
        _ => Err(BinanceReadbackError::OrderFamily),
    }
}

fn one(
    grouped: &mut BTreeMap<BinancePrivateSurface, Vec<BinanceRawPrivatePage>>,
    surface: BinancePrivateSurface,
) -> Result<BinanceRawPrivatePage, BinanceReadbackError> {
    let mut pages = grouped
        .remove(&surface)
        .ok_or(BinanceReadbackError::OrderFamily)?;
    if pages.len() != 1 || pages[0].page_index != 1 {
        return Err(BinanceReadbackError::Pagination);
    }
    pages.pop().ok_or(BinanceReadbackError::Page)
}

fn parse_complete_positions(
    payload: &str,
    binding: &GatewayBinding,
    mode: BinancePositionMode,
) -> Result<Vec<Position>, BinanceReadbackError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| BinanceReadbackError::Payload)?;
    let rows = value.as_array().ok_or(BinanceReadbackError::Payload)?;
    let expected_symbol = native_symbol(&binding.symbol);
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for row in rows {
        let row = row.as_object().ok_or(BinanceReadbackError::Payload)?;
        if row.get("symbol").and_then(Value::as_str) != Some(expected_symbol.as_str()) {
            return Err(BinanceReadbackError::Binding);
        }
        let raw_side = row
            .get("positionSide")
            .and_then(Value::as_str)
            .ok_or(BinanceReadbackError::Position)?;
        let side = match (mode, raw_side) {
            (BinancePositionMode::Net, "BOTH") => PositionSide::Net,
            (BinancePositionMode::Hedge, "LONG") => PositionSide::Long,
            (BinancePositionMode::Hedge, "SHORT") => PositionSide::Short,
            _ => return Err(BinanceReadbackError::Position),
        };
        if !seen.insert(side) {
            return Err(BinanceReadbackError::Position);
        }
        let raw_quantity = decimal(row.get("positionAmt"))?;
        let quantity = match mode {
            BinancePositionMode::Net => raw_quantity,
            BinancePositionMode::Hedge => raw_quantity.abs(),
        };
        result.push(Position {
            symbol: binding.symbol.clone(),
            side,
            quantity,
            entry_price: optional_positive_price(row.get("entryPrice"))?,
            mark_price: optional_positive_price(row.get("markPrice"))?,
        });
    }
    let required: &[PositionSide] = match mode {
        BinancePositionMode::Net => &[PositionSide::Net],
        BinancePositionMode::Hedge => &[PositionSide::Long, PositionSide::Short],
    };
    for side in required {
        if !seen.contains(side) {
            result.push(Position {
                symbol: binding.symbol.clone(),
                side: *side,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: None,
            });
        }
    }
    result.sort_by_key(|position| position.side);
    Ok(result)
}

fn complete_fill_pages(
    initial: RecentFillsCursor,
    target: u64,
    mut pages: Vec<BinanceRawPrivatePage>,
    binding: &GatewayBinding,
) -> Result<(Vec<Fill>, RecentFillsCursor), BinanceReadbackError> {
    pages.sort_by_key(|page| page.page_index);
    if pages
        .iter()
        .enumerate()
        .any(|(index, page)| page.page_index != u32::try_from(index + 1).unwrap_or(u32::MAX))
    {
        return Err(BinanceReadbackError::Pagination);
    }
    let mut queue = VecDeque::from(pages);
    let readback = paginate_recent_fills(initial, target, |request| {
        let page = queue.pop_front().ok_or(BinanceReadbackError::Pagination)?;
        if !fill_request_matches(&page, request, binding) {
            return Err(BinanceReadbackError::Pagination);
        }
        page.payload_str().map(str::to_owned)
    })
    .map_err(|_| BinanceReadbackError::Pagination)?;
    if !queue.is_empty() {
        return Err(BinanceReadbackError::Pagination);
    }
    let fills = parse_fills(&readback.payload, &binding.symbol)
        .map_err(|_| BinanceReadbackError::Payload)?;
    Ok((fills, readback.cursor))
}

fn fill_request_matches(
    page: &BinanceRawPrivatePage,
    request: crate::private::RecentFillsPageRequest,
    binding: &GatewayBinding,
) -> bool {
    let expected_symbol = native_symbol(&binding.symbol);
    let has = |key: &str, value: &str| {
        page.request_parameters
            .iter()
            .any(|(actual_key, actual_value)| actual_key == key && actual_value == value)
    };
    if !has("symbol", &expected_symbol) || !has("limit", &request.limit.to_string()) {
        return false;
    }
    match request.from_id {
        Some(from_id) => {
            has("fromId", &from_id.to_string())
                && page
                    .request_parameters
                    .iter()
                    .all(|(key, _)| !matches!(key.as_str(), "startTime" | "endTime"))
        }
        None => {
            has("startTime", &request.start_time_ms.to_string())
                && has("endTime", &request.end_time_ms.to_string())
                && page
                    .request_parameters
                    .iter()
                    .all(|(key, _)| key != "fromId")
        }
    }
}

fn decimal(value: Option<&Value>) -> Result<Decimal, BinanceReadbackError> {
    let raw = value
        .and_then(Value::as_str)
        .ok_or(BinanceReadbackError::Payload)?;
    raw.parse().map_err(|_| BinanceReadbackError::Payload)
}

fn optional_positive_price(value: Option<&Value>) -> Result<Option<Price>, BinanceReadbackError> {
    let value = decimal(value)?;
    if value <= Decimal::ZERO {
        Ok(None)
    } else {
        Price::new(value)
            .map(Some)
            .map_err(|_| BinanceReadbackError::Payload)
    }
}

fn digest_pages(pages: &[BinanceRawPrivatePage]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for page in pages {
        hasher.update([page.surface as u8]);
        hasher.update(page.page_index.to_be_bytes());
        hasher.update(page.payload.as_ref());
    }
    hasher.finalize().into()
}

pub(crate) fn validate_client_order_id(value: &str) -> Result<(), BinanceReadbackError> {
    if (1..=36).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
    {
        Ok(())
    } else {
        Err(BinanceReadbackError::Request)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceReadbackError {
    #[error("Binance private readback does not match the fixed binding or generation")]
    Binding,
    #[error("Binance private read request is invalid or ambiguous")]
    Request,
    #[error("Binance private response page is invalid or time-regressed")]
    Page,
    #[error("Binance private pagination is incomplete, regressed, or unbounded")]
    Pagination,
    #[error("Binance private payload is invalid or incomplete")]
    Payload,
    #[error("Binance position mode or complete position legs are invalid")]
    Position,
    #[error("Binance regular, conditional, or Algo order-family evidence is incomplete")]
    OrderFamily,
}

impl From<RecentFillsPaginationError> for BinanceReadbackError {
    fn from(_: RecentFillsPaginationError) -> Self {
        Self::Pagination
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinanceAccountBinding, parse_instrument_rules};
    use venue_gateway_api::{GatewayMode, VenueId};

    const EXCHANGE_INFO: &str = include_str!("../tests/fixtures/exchange_info_btcusdt.json");
    const ACCOUNT: &[u8] = include_bytes!("../fixtures/portfolio-account.json");
    const ACCOUNT_CONFIG: &[u8] = include_bytes!("../fixtures/account-config.json");
    const POSITION_MODE: &[u8] = include_bytes!("../fixtures/position-mode-hedge.json");
    const POSITIONS: &[u8] = include_bytes!("../fixtures/positions-hedge-long-only.json");
    const REGULAR: &[u8] = include_bytes!("../fixtures/open-orders.json");
    const ALGO: &[u8] = include_bytes!("../fixtures/open-algo-orders.json");
    const FILLS: &[u8] = include_bytes!("../fixtures/user-trades-page.json");

    fn facts(
        mode: GatewayMode,
        account: &str,
        generation: u64,
        private_generation: u64,
    ) -> Result<
        (
            BinanceConfig,
            BinanceInstrumentRules,
            BinancePrivateReadScope,
        ),
        Box<dyn std::error::Error>,
    > {
        let binding = GatewayBinding::new(VenueId::Binance, mode, account, "BTC/USDT".parse()?)?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let rules = parse_instrument_rules(EXCHANGE_INFO, binding.symbol.clone(), generation)?;
        let scope = BinancePrivateReadScope::new(&config, &rules, private_generation, 11, 900)?;
        Ok((config, rules, scope))
    }

    fn page(
        request: BinancePrivateReadRequest,
        payload: &'static [u8],
    ) -> Result<BinanceRawPrivatePage, BinanceReadbackError> {
        BinanceRawPrivatePage::new(&request, 1_000, 2_000, Bytes::from_static(payload))
    }

    fn pages(
        scope: &BinancePrivateReadScope,
    ) -> Result<Vec<BinanceRawPrivatePage>, BinanceReadbackError> {
        Ok(vec![
            page(build_account_request(scope)?, ACCOUNT)?,
            page(build_account_config_request(scope)?, ACCOUNT_CONFIG)?,
            page(build_position_mode_request(scope)?, POSITION_MODE)?,
            page(build_positions_request(scope)?, POSITIONS)?,
            page(build_regular_orders_request(scope)?, REGULAR)?,
            page(build_algo_orders_request(scope)?, ALGO)?,
            page(
                build_fills_request(
                    scope,
                    1,
                    RecentFillsCursor {
                        observed_through_ms: 1_000,
                        last_trade_id: None,
                        last_event_time_ms: None,
                    },
                    1_000,
                    2_000,
                )?,
                FILLS,
            )?,
        ])
    }

    #[test]
    fn complete_readback_closes_hedge_legs_families_and_fill_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, rules, scope) = facts(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            7,
            17,
        )?;
        let candidate = complete_private_readback(
            &config,
            &rules,
            &scope,
            RecentFillsCursor {
                observed_through_ms: 1_000,
                last_trade_id: None,
                last_event_time_ms: None,
            },
            2_000,
            pages(&scope)?,
        )?;

        assert_eq!(candidate.position_mode, BinancePositionMode::Hedge);
        assert_eq!(candidate.positions.len(), 2);
        assert!(candidate.positions.iter().any(|position| {
            position.side == PositionSide::Short && position.quantity == Decimal::ZERO
        }));
        assert_eq!(candidate.regular.orders.len(), 1);
        assert_eq!(candidate.algo.orders.len(), 1);
        assert_eq!(candidate.algo_custody[0].client_algo_id, "venue_algo_1");
        assert_eq!(
            candidate.conditional.family,
            NativeOrderFamily::UmConditional
        );
        assert_eq!(candidate.conditional.profile_version, 1);
        assert_eq!(candidate.fills.len(), 1);
        assert_eq!(candidate.fills_cursor.last_trade_id, Some(301));
        assert_eq!(candidate.fills_cursor.observed_through_ms, 2_000);
        assert_ne!(candidate.raw_payload_digest, [0; 32]);
        assert_eq!(config.mode(), GatewayMode::Live);
        Ok(())
    }

    #[test]
    fn net_mode_is_one_complete_signed_leg_and_preserves_directional_quantity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, rules, scope) = facts(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            7,
            17,
        )?;
        let mut raw_pages = pages(&scope)?;
        raw_pages.retain(|page| {
            !matches!(
                page.surface,
                BinancePrivateSurface::PositionMode
                    | BinancePrivateSurface::Positions
                    | BinancePrivateSurface::RegularOrders
                    | BinancePrivateSurface::AlgoOrders
            )
        });
        raw_pages.push(page(
            build_position_mode_request(&scope)?,
            br#"{"dualSidePosition":false}"#,
        )?);
        raw_pages.push(page(
            build_positions_request(&scope)?,
            br#"[{"symbol":"BTCUSDT","positionAmt":"-0.010","positionSide":"BOTH","entryPrice":"50000","markPrice":"49000"}]"#,
        )?);
        raw_pages.push(page(build_regular_orders_request(&scope)?, br#"[]"#)?);
        raw_pages.push(page(build_algo_orders_request(&scope)?, br#"[]"#)?);
        let candidate = complete_private_readback(
            &config,
            &rules,
            &scope,
            RecentFillsCursor {
                observed_through_ms: 1_000,
                last_trade_id: None,
                last_event_time_ms: None,
            },
            2_000,
            raw_pages,
        )?;

        assert_eq!(candidate.position_mode(), BinancePositionMode::Net);
        assert_eq!(candidate.positions().len(), 1);
        assert_eq!(candidate.positions()[0].side, PositionSide::Net);
        assert_eq!(candidate.positions()[0].quantity, Decimal::new(-10, 3));
        Ok(())
    }

    #[test]
    fn wrong_binding_generation_and_missing_family_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, rules, scope) = facts(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            7,
            17,
        )?;
        let (_, _, other_scope) = facts(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000002",
            7,
            17,
        )?;
        let mut mixed = pages(&scope)?;
        mixed[0] = page(build_account_request(&other_scope)?, ACCOUNT)?;
        assert_eq!(
            complete_private_readback(
                &config,
                &rules,
                &scope,
                RecentFillsCursor {
                    observed_through_ms: 1_000,
                    last_trade_id: None,
                    last_event_time_ms: None,
                },
                2_000,
                mixed,
            ),
            Err(BinanceReadbackError::Binding)
        );

        let mut missing_algo = pages(&scope)?;
        missing_algo.retain(|page| page.surface != BinancePrivateSurface::AlgoOrders);
        assert_eq!(
            complete_private_readback(
                &config,
                &rules,
                &scope,
                RecentFillsCursor {
                    observed_through_ms: 1_000,
                    last_trade_id: None,
                    last_event_time_ms: None,
                },
                2_000,
                missing_algo,
            ),
            Err(BinanceReadbackError::OrderFamily)
        );
        Ok(())
    }

    #[test]
    fn fills_use_bounded_pages_and_never_mix_from_id_with_time_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, scope) = facts(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            7,
            17,
        )?;
        let with_time = build_fills_request(
            &scope,
            1,
            RecentFillsCursor {
                observed_through_ms: 1_000,
                last_trade_id: None,
                last_event_time_ms: None,
            },
            1_000,
            2_000,
        )?;
        assert!(
            with_time
                .parameters()
                .iter()
                .any(|(key, _)| key == "startTime")
        );
        assert!(
            !with_time
                .parameters()
                .iter()
                .any(|(key, _)| key == "fromId")
        );

        let with_id = build_fills_request(
            &scope,
            2,
            RecentFillsCursor {
                observed_through_ms: 1_000,
                last_trade_id: Some(300),
                last_event_time_ms: Some(900),
            },
            1_000,
            2_000,
        )?;
        assert!(
            with_id
                .parameters()
                .iter()
                .any(|pair| pair == &("fromId".to_owned(), "301".to_owned()))
        );
        assert!(
            !with_id
                .parameters()
                .iter()
                .any(|(key, _)| key == "startTime")
        );
        assert_eq!(with_id.page_index(), 2);
        Ok(())
    }
}
