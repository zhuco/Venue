use rust_decimal::Decimal;
use venue_domain::domain::{
    AccountBalance, Amount, Asset, FieldState, Fill, Order, OrderPurpose, OrderSide, OrderState,
    Position, PositionSide, Price,
};

use crate::models::{AccountConfigRow, BalanceRow, Envelope, FillRow, OrderRow, PositionRow};
use crate::public::{decimal, decode_success, positive_decimal, positive_u64};
use crate::{OkxConfig, OkxError, OkxFill, OkxInstrument, OkxPositionMode};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxAccountLevel {
    Futures,
    MultiCurrencyMargin,
    PortfolioMargin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxAccountProfile {
    uid: String,
    main_uid: String,
    level: OkxAccountLevel,
    position_mode: OkxPositionMode,
}

impl OkxAccountProfile {
    #[must_use]
    pub fn uid(&self) -> &str {
        &self.uid
    }

    #[must_use]
    pub fn main_uid(&self) -> &str {
        &self.main_uid
    }

    #[must_use]
    pub const fn level(&self) -> OkxAccountLevel {
        self.level
    }

    #[must_use]
    pub const fn position_mode(&self) -> OkxPositionMode {
        self.position_mode
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxTimedBalance {
    pub balance: AccountBalance,
    pub update_time_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxTimedPosition {
    pub position: Position,
    pub update_time_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxTimedOrder {
    pub order: Order,
    pub update_time_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxPageState {
    Closed,
    More { after: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPage<T> {
    pub items: Vec<T>,
    pub state: OkxPageState,
}

impl<T> OkxPage<T> {
    pub fn require_closed(self) -> Result<Vec<T>, OkxError> {
        match self.state {
            OkxPageState::Closed => Ok(self.items),
            OkxPageState::More { .. } => Err(OkxError::Pagination),
        }
    }
}

pub fn parse_account_profile(
    payload: &[u8],
    expected_position_mode: OkxPositionMode,
) -> Result<OkxAccountProfile, OkxError> {
    let envelope: Envelope<AccountConfigRow> = decode_success(payload)?;
    let [row] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    if !account_uid(&row.uid) || !account_uid(&row.main_uid) {
        return Err(OkxError::Payload);
    }
    let level = match row.acct_lv.as_str() {
        "2" => OkxAccountLevel::Futures,
        "3" => OkxAccountLevel::MultiCurrencyMargin,
        "4" => OkxAccountLevel::PortfolioMargin,
        _ => return Err(OkxError::PositionMode),
    };
    let position_mode = match row.pos_mode.as_str() {
        "net_mode" => OkxPositionMode::Net,
        "long_short_mode" => OkxPositionMode::LongShort,
        _ => return Err(OkxError::PositionMode),
    };
    if position_mode != expected_position_mode
        || (level == OkxAccountLevel::PortfolioMargin && position_mode != OkxPositionMode::Net)
    {
        return Err(OkxError::PositionMode);
    }
    Ok(OkxAccountProfile {
        uid: row.uid.clone(),
        main_uid: row.main_uid.clone(),
        level,
        position_mode,
    })
}

fn account_uid(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

pub fn parse_balance(
    payload: &[u8],
    config: &OkxConfig,
    _profile: &OkxAccountProfile,
) -> Result<OkxTimedBalance, OkxError> {
    let envelope: Envelope<BalanceRow> = decode_success(payload)?;
    let [account] = envelope.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    normalize_balance_row(account, config)
}

pub(crate) fn normalize_balance_row(
    account: &BalanceRow,
    config: &OkxConfig,
) -> Result<OkxTimedBalance, OkxError> {
    let quote = config.gateway_binding().symbol.quote();
    let mut details = account.details.iter().filter(|detail| detail.ccy == quote);
    let detail = details.next().ok_or(OkxError::Binding)?;
    if details.next().is_some() {
        return Err(OkxError::Binding);
    }
    let account_time = positive_u64(&account.u_time)?;
    let detail_time = positive_u64(&detail.u_time)?;
    if detail_time > account_time {
        return Err(OkxError::Sequence);
    }
    let balance = AccountBalance {
        asset: Asset::new(&detail.ccy).map_err(|_| OkxError::Payload)?,
        wallet_balance: decimal(&detail.eq)?,
        available_balance: decimal(&detail.avail_bal)?,
        initial_margin: decimal(&detail.imr)?,
        maintenance_margin: decimal(&detail.mmr)?,
    };
    balance.validate().map_err(|_| OkxError::Payload)?;
    Ok(OkxTimedBalance {
        balance,
        update_time_ms: account_time,
    })
}

pub fn parse_positions(
    payload: &[u8],
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
) -> Result<Vec<OkxTimedPosition>, OkxError> {
    instrument.validate_scope(config)?;
    let envelope: Envelope<PositionRow> = decode_success(payload)?;
    envelope
        .data
        .into_iter()
        .filter_map(
            |row| match normalize_position_row(row, instrument, profile, false) {
                Ok(Some(position)) => Some(Ok(position)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            },
        )
        .collect()
}

pub fn parse_orders_page(
    payload: &[u8],
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    limit: u16,
    previous_after: Option<&str>,
) -> Result<OkxPage<OkxTimedOrder>, OkxError> {
    instrument.validate_scope(config)?;
    let envelope: Envelope<OrderRow> = decode_success(payload)?;
    let ids = envelope
        .data
        .iter()
        .map(|row| row.ord_id.as_str())
        .collect::<Vec<_>>();
    let state = page_state(&ids, limit, previous_after)?;
    let items = envelope
        .data
        .into_iter()
        .map(|row| normalize_order_row(row, instrument, profile, false))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(OkxPage { items, state })
}

pub fn parse_fills_page(
    payload: &[u8],
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    limit: u16,
    previous_after: Option<&str>,
) -> Result<OkxPage<OkxFill>, OkxError> {
    instrument.validate_scope(config)?;
    let envelope: Envelope<FillRow> = decode_success(payload)?;
    let ids = envelope
        .data
        .iter()
        .map(|row| row.bill_id.as_str())
        .collect::<Vec<_>>();
    let state = page_state(&ids, limit, previous_after)?;
    let items = envelope
        .data
        .into_iter()
        .map(|row| normalize_fill(row, instrument, profile))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(OkxPage { items, state })
}

pub(crate) fn normalize_position_row(
    row: PositionRow,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    retain_zero: bool,
) -> Result<Option<OkxTimedPosition>, OkxError> {
    validate_instrument_row(&row.inst_type, &row.inst_id, instrument)?;
    let raw_quantity = decimal(&row.pos)?;
    let side = position_side(profile.position_mode, &row.pos_side, raw_quantity)?;
    if raw_quantity.is_zero() && !retain_zero {
        return Ok(None);
    }
    let quantity = instrument.contracts_to_base(raw_quantity.abs())?;
    let position = Position {
        symbol: instrument.instrument().symbol.clone(),
        side,
        quantity,
        entry_price: optional_price(&row.avg_px)?,
        mark_price: optional_price(&row.mark_px)?,
    };
    Ok(Some(OkxTimedPosition {
        position,
        update_time_ms: positive_u64(&row.u_time)?,
    }))
}

pub(crate) fn normalize_order_row(
    row: OrderRow,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    allow_terminal: bool,
) -> Result<OkxTimedOrder, OkxError> {
    validate_instrument_row(&row.inst_type, &row.inst_id, instrument)?;
    nonempty_id(&row.ord_id)?;
    let side = order_side(&row.side)?;
    let position_side = position_side(profile.position_mode, &row.pos_side, Decimal::ONE)?;
    let quantity = instrument.contracts_to_base(positive_decimal(&row.sz)?)?;
    let filled_contracts = decimal(&row.acc_fill_sz)?;
    if filled_contracts.is_sign_negative() {
        return Err(OkxError::Payload);
    }
    let filled_quantity = instrument.contracts_to_base(filled_contracts)?;
    let reduce_only = boolean(&row.reduce_only)?;
    let state = match row.state.as_str() {
        "live" => OrderState::New,
        "partially_filled" => OrderState::PartiallyFilled,
        "filled" if allow_terminal => OrderState::Filled,
        "canceled" | "mmp_canceled" if allow_terminal => OrderState::Cancelled,
        "rejected" if allow_terminal => OrderState::Rejected,
        "expired" if allow_terminal => OrderState::Expired,
        _ => return Err(OkxError::Payload),
    };
    let order = Order {
        order_id: row.ord_id,
        client_order_id: optional_text(row.cl_ord_id),
        symbol: instrument.instrument().symbol.clone(),
        side,
        position_side: FieldState::Known(position_side),
        purpose: if reduce_only {
            FieldState::Known(OrderPurpose::Reduce)
        } else {
            FieldState::Missing
        },
        state,
        quantity,
        filled_quantity,
        limit_price: optional_price(&row.px)?,
        average_price: optional_price_state(&row.avg_px)?,
        reduce_only,
    };
    order.validate().map_err(|_| OkxError::Payload)?;
    Ok(OkxTimedOrder {
        order,
        update_time_ms: positive_u64(&row.u_time)?,
    })
}

pub(crate) fn normalize_fill(
    row: FillRow,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
) -> Result<OkxFill, OkxError> {
    validate_instrument_row(&row.inst_type, &row.inst_id, instrument)?;
    id(&row.bill_id)?;
    nonempty_id(&row.ord_id)?;
    positive_u64(&row.ts)?;
    let side = order_side(&row.side)?;
    let position_side = position_side(profile.position_mode, &row.pos_side, Decimal::ONE)?;
    let maker = match row.exec_type.as_str() {
        "M" => true,
        "T" => false,
        _ => return Err(OkxError::Payload),
    };
    let fill = Fill {
        fill_id: row.bill_id,
        execution_sequence: FieldState::Missing,
        order_id: row.ord_id,
        symbol: instrument.instrument().symbol.clone(),
        side,
        position_side: FieldState::Known(position_side),
        quantity: instrument.contracts_to_base(positive_decimal(&row.fill_sz)?)?,
        price: Price::new(positive_decimal(&row.fill_px)?).map_err(|_| OkxError::Payload)?,
        fee: FieldState::Known(Amount::new(
            Asset::new(&row.fee_ccy).map_err(|_| OkxError::Payload)?,
            decimal(&row.fee)?.abs(),
        )),
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(maker),
        exchange_time_ms: Some(positive_u64(&row.fill_time)?),
    };
    fill.validate().map_err(|_| OkxError::Payload)?;
    Ok(OkxFill {
        fill,
        client_order_id: optional_text(row.cl_ord_id),
    })
}

fn validate_instrument_row(
    instrument_type: &str,
    native_id: &str,
    instrument: &OkxInstrument,
) -> Result<(), OkxError> {
    if instrument_type != "SWAP" || native_id != instrument.native_id() {
        Err(OkxError::Binding)
    } else {
        Ok(())
    }
}

pub(crate) fn position_side(
    mode: OkxPositionMode,
    raw_side: &str,
    raw_quantity: Decimal,
) -> Result<PositionSide, OkxError> {
    match (mode, raw_side) {
        (OkxPositionMode::Net, "net") => Ok(PositionSide::Net),
        (OkxPositionMode::LongShort, "long") if !raw_quantity.is_sign_negative() => {
            Ok(PositionSide::Long)
        }
        (OkxPositionMode::LongShort, "short") if !raw_quantity.is_sign_negative() => {
            Ok(PositionSide::Short)
        }
        _ => Err(OkxError::PositionMode),
    }
}

pub(crate) fn order_side(value: &str) -> Result<OrderSide, OkxError> {
    match value {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(OkxError::Payload),
    }
}

pub(crate) fn boolean(value: &str) -> Result<bool, OkxError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(OkxError::Payload),
    }
}

fn optional_price(value: &str) -> Result<Option<Price>, OkxError> {
    if value.is_empty() || value == "0" {
        Ok(None)
    } else {
        Price::new(positive_decimal(value)?)
            .map(Some)
            .map_err(|_| OkxError::Payload)
    }
}

fn optional_price_state(value: &str) -> Result<FieldState<Price>, OkxError> {
    optional_price(value).map(|value| value.map(FieldState::Known).unwrap_or(FieldState::Missing))
}

fn optional_text(value: String) -> FieldState<String> {
    if value.is_empty() {
        FieldState::Missing
    } else {
        FieldState::Known(value)
    }
}

fn id(value: &str) -> Result<u128, OkxError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(OkxError::Pagination);
    }
    value.parse::<u128>().map_err(|_| OkxError::Pagination)
}

fn nonempty_id(value: &str) -> Result<(), OkxError> {
    if value.trim().is_empty() {
        Err(OkxError::Payload)
    } else {
        Ok(())
    }
}

fn page_state(
    ids: &[&str],
    limit: u16,
    previous_after: Option<&str>,
) -> Result<OkxPageState, OkxError> {
    if limit == 0 || limit > 100 || ids.len() > usize::from(limit) {
        return Err(OkxError::Pagination);
    }
    let parsed = ids
        .iter()
        .map(|value| id(value))
        .collect::<Result<Vec<_>, _>>()?;
    if parsed.windows(2).any(|window| window[0] <= window[1]) {
        return Err(OkxError::Sequence);
    }
    if let (Some(previous), Some(first)) = (previous_after, parsed.first())
        && *first >= id(previous)?
    {
        return Err(OkxError::Sequence);
    }
    if ids.len() == usize::from(limit) {
        Ok(OkxPageState::More {
            after: ids.last().ok_or(OkxError::Pagination)?.to_string(),
        })
    } else {
        Ok(OkxPageState::Closed)
    }
}

#[cfg(test)]
mod tests {
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    use super::*;
    use crate::parse_instrument;

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const CONFIG: &[u8] = include_bytes!("../fixtures/account-config.json");
    const BALANCE: &[u8] = include_bytes!("../fixtures/balance.json");
    const POSITIONS: &[u8] = include_bytes!("../fixtures/positions.json");
    const ORDERS: &[u8] = include_bytes!("../fixtures/pending-orders.json");
    const FILLS: &[u8] = include_bytes!("../fixtures/fills-history-page.json");

    fn scope() -> Result<(OkxConfig, OkxInstrument, OkxAccountProfile), Box<dyn std::error::Error>>
    {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = parse_instrument(INSTRUMENT, &config, 1)?;
        let profile = parse_account_profile(CONFIG, OkxPositionMode::LongShort)?;
        Ok((config, instrument, profile))
    }

    #[test]
    fn private_snapshot_preserves_mode_balances_positions_orders_and_fills()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope()?;
        assert_eq!(profile.level(), OkxAccountLevel::MultiCurrencyMargin);
        let balance = parse_balance(BALANCE, &config, &profile)?;
        assert_eq!(balance.balance.wallet_balance, Decimal::new(20_000, 0));
        assert_eq!(balance.update_time_ms, 1_787_911_200_300);

        let positions = parse_positions(POSITIONS, &config, &instrument, &profile)?;
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].position.side, PositionSide::Short);
        assert_eq!(positions[0].position.quantity, Decimal::new(2, 1));

        let orders = parse_orders_page(ORDERS, &config, &instrument, &profile, 100, None)?;
        assert_eq!(orders.state, OkxPageState::Closed);
        assert_eq!(orders.items[0].order.state, OrderState::PartiallyFilled);
        assert_eq!(orders.items[0].order.quantity, Decimal::new(2, 1));

        let fills = parse_fills_page(FILLS, &config, &instrument, &profile, 100, None)?;
        assert_eq!(fills.state, OkxPageState::Closed);
        assert_eq!(fills.items.len(), 2);
        assert_eq!(fills.items[0].fill.fill_id, "9002");
        assert_eq!(fills.items[0].fill.order_id, "order-9002");
        assert_eq!(fills.items[0].fill.quantity, Decimal::new(2, 1));
        assert_eq!(
            fills.items[0].client_order_id,
            FieldState::Known("0123456789abcdef0123456789abcdef".to_owned())
        );
        assert_eq!(
            fills.items[0].fill.exchange_time_ms,
            Some(1_787_911_201_400)
        );
        assert_eq!(fills.items[0].fill.maker, FieldState::Known(true));
        Ok(())
    }

    #[test]
    fn wrong_mode_out_of_order_and_unclosed_pages_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            parse_account_profile(CONFIG, OkxPositionMode::Net),
            Err(OkxError::PositionMode)
        );
        let (config, instrument, profile) = scope()?;
        let unclosed = parse_fills_page(FILLS, &config, &instrument, &profile, 2, None)?;
        assert_eq!(
            unclosed.state,
            OkxPageState::More {
                after: "9001".to_owned()
            }
        );
        assert_eq!(unclosed.require_closed(), Err(OkxError::Pagination));

        let reversed = br#"{"code":"0","msg":"","data":[
            {"instType":"SWAP","instId":"BTC-USDT-SWAP","ordId":"1","clOrdId":"a","side":"buy","posSide":"long","sz":"1","accFillSz":"0","px":"60000","avgPx":"","reduceOnly":"false","state":"live","uTime":"1787911200200"},
            {"instType":"SWAP","instId":"BTC-USDT-SWAP","ordId":"2","clOrdId":"b","side":"buy","posSide":"long","sz":"1","accFillSz":"0","px":"60000","avgPx":"","reduceOnly":"false","state":"live","uTime":"1787911200100"}
        ]}"#;
        assert_eq!(
            parse_orders_page(reversed, &config, &instrument, &profile, 100, None),
            Err(OkxError::Sequence)
        );
        assert_eq!(
            parse_balance(
                br#"{"code":"0","msg":"","data":[{"uTime":"1","details":[{"ccy":"USDT"}]}]}"#,
                &config,
                &profile
            ),
            Err(OkxError::Payload)
        );
        assert_eq!(
            parse_positions(
                br#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"ETH-USDT-SWAP","posSide":"short","pos":"1","avgPx":"1","markPx":"1","uTime":"1"}]}"#,
                &config,
                &instrument,
                &profile
            ),
            Err(OkxError::Binding)
        );
        Ok(())
    }
}
