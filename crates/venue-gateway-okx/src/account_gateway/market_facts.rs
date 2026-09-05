use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use venue_domain::domain::Price;

use super::{OkxAccountGatewayError, OkxConfig, OkxInstrument};

const LIMIT_BBO_MAX_AGE_MS: u64 = 1_000;
const LIMIT_BBO_MAX_CLOCK_SKEW_MS: u64 = 250;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OkxLimitBbo {
    pub(super) bid: Price,
    pub(super) ask: Price,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LimitBboFailure {
    ResponseTime,
    Decode,
    VenueCode,
    RowCount,
    Scope,
    ExchangeTime,
    Book,
}

impl LimitBboFailure {
    pub(super) const fn code(self) -> &'static str {
        match self {
            Self::ResponseTime => "strategy_okx_bbo_response_time",
            Self::Decode => "strategy_okx_bbo_decode",
            Self::VenueCode => "strategy_okx_bbo_venue_code",
            Self::RowCount => "strategy_okx_bbo_row_count",
            Self::Scope => "strategy_okx_bbo_scope",
            Self::ExchangeTime => "strategy_okx_bbo_exchange_time",
            Self::Book => "strategy_okx_bbo_book",
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxLimitBboEnvelope {
    code: String,
    data: Vec<OkxLimitBboRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxLimitBboRow {
    inst_id: String,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    ts: String,
}

pub(super) fn parse_limit_bbo_detailed(
    response: &crate::OkxHttpResponse,
    config: &OkxConfig,
    instrument: &OkxInstrument,
    now_ms: u64,
) -> Result<OkxLimitBbo, LimitBboFailure> {
    if response.binding != *config.gateway_binding()
        || response.instrument_generation != instrument.instrument().generation
        || response.received_at_ms == 0
        || now_ms < response.received_at_ms
        || now_ms.saturating_sub(response.received_at_ms) > LIMIT_BBO_MAX_AGE_MS
    {
        return Err(LimitBboFailure::ResponseTime);
    }
    let envelope: OkxLimitBboEnvelope =
        serde_json::from_slice(&response.body).map_err(|_| LimitBboFailure::Decode)?;
    if envelope.code != "0" {
        return Err(LimitBboFailure::VenueCode);
    }
    let [row] = envelope.data.as_slice() else {
        return Err(LimitBboFailure::RowCount);
    };
    if row.inst_id != instrument.native_id() {
        return Err(LimitBboFailure::Scope);
    }
    let exchange_time_ms = row
        .ts
        .parse::<u64>()
        .map_err(|_| LimitBboFailure::ExchangeTime)?;
    if exchange_time_ms == 0
        || exchange_time_ms > now_ms.saturating_add(LIMIT_BBO_MAX_CLOCK_SKEW_MS)
        || now_ms.saturating_sub(exchange_time_ms) > LIMIT_BBO_MAX_AGE_MS
    {
        return Err(LimitBboFailure::ExchangeTime);
    }
    let bid = bbo_level_price(&row.bids)?;
    let ask = bbo_level_price(&row.asks)?;
    if bid >= ask {
        return Err(LimitBboFailure::Book);
    }
    Ok(OkxLimitBbo { bid, ask })
}

pub(super) fn parse_limit_bbo(
    response: &crate::OkxHttpResponse,
    config: &OkxConfig,
    instrument: &OkxInstrument,
    now_ms: u64,
) -> Result<OkxLimitBbo, OkxAccountGatewayError> {
    parse_limit_bbo_detailed(response, config, instrument, now_ms)
        .map_err(|_| OkxAccountGatewayError::Instrument)
}

fn bbo_level_price(levels: &[Vec<String>]) -> Result<Price, LimitBboFailure> {
    let [price, ..] = levels.first().ok_or(LimitBboFailure::Book)?.as_slice() else {
        return Err(LimitBboFailure::Book);
    };
    Price::new(Decimal::from_str(price).map_err(|_| LimitBboFailure::Book)?)
        .map_err(|_| LimitBboFailure::Book)
}
