use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use venue_domain::domain::{
    AggressorSide, Amount, Asset, FieldState, Instrument, MarketKind, Price, PublicBar,
    PublicTicker, PublicTrade, PublicTradeId, PublicTradeOrdering, UnknownReason,
};

use crate::models::{BookPush, Envelope, InstrumentRow};
use crate::{OkxConfig, OkxError};

const ONE_MINUTE_MS: u64 = 60_000;
const MAX_TRADES_PER_PUSH: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxInstrument {
    native_id: String,
    instrument: Instrument,
    base_quantity_per_contract: Decimal,
    minimum_base_quantity: Decimal,
    maximum_limit_contracts: Option<Decimal>,
}

/// Executable OKX contract size derived from a quote-notional ceiling. Quantities exposed to the
/// shared runtime stay in base units while the wire request uses `contracts`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxNotionalSize {
    contracts: Decimal,
    base_quantity: Decimal,
    quote_notional: Amount,
}

impl OkxNotionalSize {
    #[must_use]
    pub const fn contracts(&self) -> Decimal {
        self.contracts
    }

    #[must_use]
    pub const fn base_quantity(&self) -> Decimal {
        self.base_quantity
    }

    #[must_use]
    pub const fn quote_notional(&self) -> &Amount {
        &self.quote_notional
    }
}

impl OkxInstrument {
    #[must_use]
    pub fn native_id(&self) -> &str {
        &self.native_id
    }

    #[must_use]
    pub const fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    #[must_use]
    pub const fn base_quantity_per_contract(&self) -> Decimal {
        self.base_quantity_per_contract
    }

    #[must_use]
    pub const fn minimum_base_quantity(&self) -> Decimal {
        self.minimum_base_quantity
    }

    #[must_use]
    pub const fn maximum_limit_contracts(&self) -> Option<Decimal> {
        self.maximum_limit_contracts
    }

    pub(crate) fn validate_scope(&self, config: &OkxConfig) -> Result<(), OkxError> {
        if self.instrument.symbol != config.gateway_binding().symbol
            || self.instrument.generation == 0
            || self.base_quantity_per_contract <= Decimal::ZERO
        {
            return Err(OkxError::Binding);
        }
        Ok(())
    }

    pub fn contracts_to_base(&self, contracts: Decimal) -> Result<Decimal, OkxError> {
        contracts
            .checked_mul(self.base_quantity_per_contract)
            .ok_or(OkxError::Payload)
    }

    pub fn base_to_contracts(&self, quantity: Decimal) -> Result<Decimal, OkxError> {
        if quantity < self.minimum_base_quantity
            || quantity <= Decimal::ZERO
            || quantity % self.instrument.quantity_step != Decimal::ZERO
        {
            return Err(OkxError::Precision);
        }
        let contracts = quantity
            .checked_div(self.base_quantity_per_contract)
            .ok_or(OkxError::Precision)?;
        if contracts <= Decimal::ZERO
            || contracts
                .checked_mul(self.base_quantity_per_contract)
                .filter(|round_trip| *round_trip == quantity)
                .is_none()
        {
            return Err(OkxError::Precision);
        }
        Ok(contracts)
    }

    /// Floors an entry size to the venue contract step without exceeding the supplied quote cap.
    /// A cap below `minSz` is rejected rather than rounded up through the risk boundary.
    pub fn size_for_quote_notional(
        &self,
        cap: &Amount,
        price: Price,
    ) -> Result<OkxNotionalSize, OkxError> {
        self.instrument.validate().map_err(|_| OkxError::Payload)?;
        if cap.asset.as_str() != self.instrument.symbol.quote()
            || cap.value <= Decimal::ZERO
            || price.value() <= Decimal::ZERO
        {
            return Err(OkxError::Precision);
        }
        let quote_per_contract = self
            .base_quantity_per_contract
            .checked_mul(price.value())
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(OkxError::Precision)?;
        let contract_step = self
            .instrument
            .quantity_step
            .checked_div(self.base_quantity_per_contract)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(OkxError::Precision)?;
        let minimum_contracts = self
            .minimum_base_quantity
            .checked_div(self.base_quantity_per_contract)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(OkxError::Precision)?;
        let raw_contracts = cap
            .value
            .checked_div(quote_per_contract)
            .ok_or(OkxError::Precision)?;
        let contracts = raw_contracts - raw_contracts % contract_step;
        if contracts < minimum_contracts || contracts <= Decimal::ZERO {
            return Err(OkxError::Precision);
        }
        let base_quantity = self.contracts_to_base(contracts)?;
        let quote_value = base_quantity
            .checked_mul(price.value())
            .filter(|value| *value <= cap.value)
            .ok_or(OkxError::Precision)?;
        Ok(OkxNotionalSize {
            contracts,
            base_quantity,
            quote_notional: Amount::new(cap.asset.clone(), quote_value),
        })
    }
}

/// Parses the exact bound linear USDT perpetual from an OKX V5 instruments response.
pub fn parse_instrument(
    payload: &[u8],
    config: &OkxConfig,
    generation: u64,
) -> Result<OkxInstrument, OkxError> {
    if generation == 0 {
        return Err(OkxError::Payload);
    }
    let envelope: Envelope<InstrumentRow> = decode_success(payload)?;
    let symbol = &config.gateway_binding().symbol;
    let expected_native = format!("{}-{}-SWAP", symbol.base(), symbol.quote());
    let mut rows = envelope
        .data
        .into_iter()
        .filter(|row| row.inst_id == expected_native);
    let row = rows.next().ok_or(OkxError::Binding)?;
    if rows.next().is_some()
        || row.inst_type != "SWAP"
        || row.ct_type != "linear"
        || row.ct_val_ccy != symbol.base()
        || row.settle_ccy != symbol.quote()
        || row.state != "live"
    {
        return Err(OkxError::Binding);
    }
    let contract_value = positive_decimal(&row.ct_val)?;
    let contract_multiplier = positive_decimal(&row.ct_mult)?;
    let base_quantity_per_contract = contract_value
        .checked_mul(contract_multiplier)
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(OkxError::Payload)?;
    let lot_size = positive_decimal(&row.lot_sz)?;
    let minimum_contracts = positive_decimal(&row.min_sz)?;
    let maximum_limit_contracts = row
        .max_lmt_sz
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(positive_decimal)
        .transpose()?;
    let quantity_step = lot_size
        .checked_mul(base_quantity_per_contract)
        .ok_or(OkxError::Payload)?;
    let minimum_base_quantity = minimum_contracts
        .checked_mul(base_quantity_per_contract)
        .ok_or(OkxError::Payload)?;
    let quote = Asset::new(symbol.quote()).map_err(|_| OkxError::Payload)?;
    let instrument = Instrument {
        symbol: symbol.clone(),
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(quote.clone()),
        generation,
        price_tick: Price::new(positive_decimal(&row.tick_sz)?).map_err(|_| OkxError::Payload)?,
        quantity_step,
        // OKX exposes minSz in contracts, not a stable quote-notional floor.
        minimum_notional: Amount::new(quote, Decimal::ZERO),
    };
    instrument.validate().map_err(|_| OkxError::Payload)?;
    Ok(OkxInstrument {
        native_id: expected_native,
        instrument,
        base_quantity_per_contract,
        minimum_base_quantity,
        maximum_limit_contracts,
    })
}

/// Parses one `bbo-tbt` snapshot and rejects a non-increasing venue sequence.
pub fn parse_bbo(
    payload: &[u8],
    config: &OkxConfig,
    instrument: &OkxInstrument,
    received_at_ms: u64,
    previous_sequence: Option<u64>,
) -> Result<PublicTicker, OkxError> {
    instrument.validate_scope(config)?;
    if received_at_ms == 0 {
        return Err(OkxError::Payload);
    }
    let push: BookPush = serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if push.arg.channel != "bbo-tbt" || push.arg.inst_id != instrument.native_id {
        return Err(OkxError::Binding);
    }
    let [row] = push.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    if row.seq_id == 0 || previous_sequence.is_some_and(|previous| row.seq_id <= previous) {
        return Err(OkxError::Sequence);
    }
    let (bid_price, bid_contracts) = one_level(&row.bids)?;
    let (ask_price, ask_contracts) = one_level(&row.asks)?;
    if ask_price.value() <= bid_price.value() {
        return Err(OkxError::Payload);
    }
    let exchange_time_ms = positive_u64(&row.ts)?;
    if exchange_time_ms > received_at_ms {
        return Err(OkxError::Sequence);
    }
    let bid_quantity = instrument.contracts_to_base(bid_contracts)?;
    let ask_quantity = instrument.contracts_to_base(ask_contracts)?;
    if bid_quantity <= Decimal::ZERO || ask_quantity <= Decimal::ZERO {
        return Err(OkxError::Payload);
    }
    Ok(PublicTicker {
        symbol: instrument.instrument.symbol.clone(),
        generation: instrument.instrument.generation,
        received_at_ms,
        exchange_time_ms,
        transaction_time_ms: exchange_time_ms,
        update_id: row.seq_id,
        bid_price,
        bid_quantity,
        ask_price,
        ask_quantity,
    })
}

/// Parses the public OKX `trades` aggregation. The documented `tradeId` is the last native ID
/// in an exactly contiguous aggregation described by `count`; `seqId` may repeat and is therefore
/// deliberately not promoted as a trade identity.
pub fn parse_trades(
    payload: &[u8],
    instrument: &OkxInstrument,
    received_at_ms: u64,
) -> Result<Vec<PublicTrade>, OkxError> {
    if received_at_ms == 0 {
        return Err(OkxError::Payload);
    }
    let push: TradesPush = serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if push.arg.channel != "trades" || push.arg.inst_id != instrument.native_id {
        return Err(OkxError::Binding);
    }
    if push.data.is_empty() || push.data.len() > MAX_TRADES_PER_PUSH {
        return Err(OkxError::Payload);
    }
    push.data
        .into_iter()
        .map(|row| normalize_trade(row, instrument, received_at_ms))
        .collect()
}

/// Parses one closed public OKX `candle1m` record. Its opening timestamp is a stable candle
/// bucket used for deduplication; it is not represented as a native exchange sequence.
pub fn parse_closed_1m_candle(
    payload: &[u8],
    instrument: &OkxInstrument,
    received_at_ms: u64,
) -> Result<PublicBar, OkxError> {
    if received_at_ms == 0 {
        return Err(OkxError::Payload);
    }
    let push: CandlePush = serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if push.arg.channel != "candle1m" || push.arg.inst_id != instrument.native_id {
        return Err(OkxError::Binding);
    }
    let [row] = push.data.as_slice() else {
        return Err(OkxError::Payload);
    };
    let [
        open_time,
        open,
        high,
        low,
        close,
        contracts,
        base_volume,
        quote_volume,
        confirm,
    ] = row.as_slice()
    else {
        return Err(OkxError::Payload);
    };
    if confirm != "1" {
        return Err(OkxError::Sequence);
    }
    let open_time_ms = positive_u64(open_time)?;
    let close_time_ms = open_time_ms
        .checked_add(ONE_MINUTE_MS - 1)
        .ok_or(OkxError::Sequence)?;
    if open_time_ms % ONE_MINUTE_MS != 0 {
        return Err(OkxError::Sequence);
    }
    // Parse `vol` too: it must be a non-negative contract count, and its converted base volume
    // must agree exactly with the source-provided derivatives `volCcy` field.
    let contracts = non_negative_decimal(contracts)?;
    let derived_base_volume = instrument.contracts_to_base(contracts)?;
    let base_volume = non_negative_decimal(base_volume)?;
    if base_volume != derived_base_volume {
        return Err(OkxError::Payload);
    }
    let quote_volume = non_negative_decimal(quote_volume)?;
    let sequence = open_time_ms
        .checked_div(ONE_MINUTE_MS)
        .and_then(|value| value.checked_add(1))
        .ok_or(OkxError::Sequence)?;
    let fact = PublicBar {
        symbol: instrument.instrument.symbol.clone(),
        generation: instrument.instrument.generation,
        received_at_ms,
        sequence,
        open_time_ms,
        close_time_ms,
        interval_ms: ONE_MINUTE_MS,
        open: Price::new(positive_decimal(open)?).map_err(|_| OkxError::Payload)?,
        high: Price::new(positive_decimal(high)?).map_err(|_| OkxError::Payload)?,
        low: Price::new(positive_decimal(low)?).map_err(|_| OkxError::Payload)?,
        close: Price::new(positive_decimal(close)?).map_err(|_| OkxError::Payload)?,
        base_volume: FieldState::Known(base_volume),
        quote_volume: FieldState::Known(quote_volume),
        trade_count: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        taker_buy_base_volume: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        taker_buy_quote_volume: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
    };
    fact.is_valid().then_some(fact).ok_or(OkxError::Payload)
}

fn normalize_trade(
    row: TradeRow,
    instrument: &OkxInstrument,
    received_at_ms: u64,
) -> Result<PublicTrade, OkxError> {
    if row.inst_id != instrument.native_id {
        return Err(OkxError::Binding);
    }
    let last_trade_id = positive_u64(&row.trade_id)?;
    let count = positive_u64(&row.count)?;
    let first_trade_id = last_trade_id
        .checked_sub(count.saturating_sub(1))
        .filter(|value| *value > 0)
        .ok_or(OkxError::Sequence)?;
    let price = Price::new(positive_decimal(&row.px)?).map_err(|_| OkxError::Payload)?;
    let contracts = positive_decimal(&row.sz)?;
    let quantity = instrument.contracts_to_base(contracts)?;
    let quote_quantity = quantity
        .checked_mul(price.value())
        .ok_or(OkxError::Payload)?;
    let aggressor = match row.side.as_str() {
        "buy" => AggressorSide::Buy,
        "sell" => AggressorSide::Sell,
        _ => return Err(OkxError::Payload),
    };
    let transaction_time_ms = positive_u64(&row.ts)?;
    Ok(PublicTrade {
        symbol: instrument.instrument.symbol.clone(),
        generation: instrument.instrument.generation,
        received_at_ms,
        exchange_time_ms: transaction_time_ms,
        transaction_time_ms,
        aggregate_trade_id: PublicTradeId::Numeric(last_trade_id),
        first_trade_id: Some(first_trade_id),
        last_trade_id: Some(last_trade_id),
        ordering: PublicTradeOrdering::Unsequenced,
        price,
        quantity,
        quote_quantity,
        aggressor: FieldState::Known(aggressor),
    })
}

fn one_level(levels: &[Vec<String>]) -> Result<(Price, Decimal), OkxError> {
    let [level] = levels else {
        return Err(OkxError::Payload);
    };
    if level.len() < 2 {
        return Err(OkxError::Payload);
    }
    let price = Price::new(positive_decimal(&level[0])?).map_err(|_| OkxError::Payload)?;
    let quantity = positive_decimal(&level[1])?;
    Ok((price, quantity))
}

pub(crate) fn decode_success<T: serde::de::DeserializeOwned>(
    payload: &[u8],
) -> Result<Envelope<T>, OkxError> {
    let envelope: Envelope<T> = serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if envelope.code != "0" || !envelope.msg.is_empty() {
        return Err(OkxError::Rejected);
    }
    Ok(envelope)
}

pub(crate) fn decimal(value: &str) -> Result<Decimal, OkxError> {
    Decimal::from_str(value).map_err(|_| OkxError::Payload)
}

pub(crate) fn positive_decimal(value: &str) -> Result<Decimal, OkxError> {
    decimal(value).and_then(|value| {
        if value > Decimal::ZERO {
            Ok(value)
        } else {
            Err(OkxError::Payload)
        }
    })
}

fn non_negative_decimal(value: &str) -> Result<Decimal, OkxError> {
    decimal(value).and_then(|value| {
        (!value.is_sign_negative())
            .then_some(value)
            .ok_or(OkxError::Payload)
    })
}

pub(crate) fn positive_u64(value: &str) -> Result<u64, OkxError> {
    u64::from_str(value)
        .map_err(|_| OkxError::Payload)
        .and_then(|value| (value > 0).then_some(value).ok_or(OkxError::Payload))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradesPush {
    arg: PublicStreamArg,
    data: Vec<TradeRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeRow {
    inst_id: String,
    trade_id: String,
    px: String,
    sz: String,
    side: String,
    ts: String,
    count: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CandlePush {
    arg: PublicStreamArg,
    data: Vec<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublicStreamArg {
    channel: String,
    inst_id: String,
}

#[cfg(test)]
mod tests {
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    use super::*;

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const BBO: &[u8] = include_bytes!("../fixtures/bbo-tbt.json");
    const TRADES: &[u8] = include_bytes!("../fixtures/public-ws-trades.json");
    const CLOSED_CANDLE: &[u8] = include_bytes!("../fixtures/business-ws-candle-1m-closed.json");

    fn config() -> Result<OkxConfig, Box<dyn std::error::Error>> {
        Ok(OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?)
    }

    #[test]
    fn instrument_and_bbo_preserve_native_identity_and_event_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = config()?;
        let instrument = parse_instrument(INSTRUMENT, &config, 7)?;
        assert_eq!(instrument.native_id(), "BTC-USDT-SWAP");
        assert_eq!(instrument.instrument().symbol.to_string(), "BTC/USDT");
        assert_eq!(instrument.base_quantity_per_contract(), Decimal::new(1, 1));
        assert_eq!(instrument.instrument().quantity_step, Decimal::new(1, 1));

        let ticker = parse_bbo(BBO, &config, &instrument, 1_787_911_200_600, None)?;
        assert_eq!(ticker.exchange_time_ms, 1_787_911_200_500);
        assert_eq!(ticker.update_id, 8_001);
        assert_eq!(ticker.bid_quantity, Decimal::new(20, 1));
        assert_eq!(ticker.ask_quantity, Decimal::new(15, 1));
        assert_eq!(
            parse_bbo(BBO, &config, &instrument, 1_787_911_200_600, Some(8_001)),
            Err(OkxError::Sequence)
        );
        Ok(())
    }

    #[test]
    fn wrong_symbol_or_missing_instrument_field_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let wrong = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "ETH/USDT".parse()?,
        )?)?;
        assert_eq!(
            parse_instrument(INSTRUMENT, &wrong, 1),
            Err(OkxError::Binding)
        );
        assert_eq!(
            parse_instrument(
                br#"{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP"}]}"#,
                &config()?,
                1
            ),
            Err(OkxError::Payload)
        );
        Ok(())
    }

    #[test]
    fn doge_contract_size_floors_ten_usdt_cap_to_point_eleven_contracts()
    -> Result<(), Box<dyn std::error::Error>> {
        const DOGE: &[u8] = br#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"DOGE-USDT-SWAP","ctType":"linear","ctVal":"1000","ctMult":"1","ctValCcy":"DOGE","settleCcy":"USDT","state":"live","tickSz":"0.00001","lotSz":"0.01","minSz":"0.01"}]}"#;
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDT".parse()?,
        )?)?;
        let instrument = parse_instrument(DOGE, &config, 1)?;
        let cap = Amount::new("USDT".parse()?, Decimal::TEN);
        let price = Price::new(Decimal::new(8_503, 5))?;
        let size = instrument.size_for_quote_notional(&cap, price)?;

        assert_eq!(instrument.minimum_base_quantity(), Decimal::TEN);
        assert_eq!(size.contracts(), Decimal::new(11, 2));
        assert_eq!(size.base_quantity(), Decimal::new(110, 0));
        assert_eq!(size.quote_notional().value, Decimal::new(93_533, 4));
        assert!(size.quote_notional().value <= cap.value);
        Ok(())
    }

    #[test]
    fn locked_or_future_bbo_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let config = config()?;
        let instrument = parse_instrument(INSTRUMENT, &config, 1)?;
        let locked = br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["60000","1"]],"bids":[["60000","1"]],"ts":"1787911200500","seqId":8001}]}"#;
        assert_eq!(
            parse_bbo(locked, &config, &instrument, 1_787_911_200_600, None),
            Err(OkxError::Payload)
        );
        assert_eq!(
            parse_bbo(BBO, &config, &instrument, 1_787_911_200_499, None),
            Err(OkxError::Sequence)
        );
        Ok(())
    }

    #[test]
    fn trades_restore_documented_contiguous_native_ids_and_base_quantity()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = config()?;
        let instrument = parse_instrument(INSTRUMENT, &config, 7)?;
        let trades = parse_trades(TRADES, &instrument, 1_630_048_898_000)?;
        let [trade] = trades.as_slice() else {
            return Err("expected one aggregation".into());
        };
        assert_eq!(
            trade.aggregate_trade_id,
            PublicTradeId::Numeric(130_639_474)
        );
        assert_eq!(trade.first_trade_id, Some(130_639_472));
        assert_eq!(trade.last_trade_id, Some(130_639_474));
        assert_eq!(trade.ordering, PublicTradeOrdering::Unsequenced);
        assert_eq!(trade.quantity, Decimal::new(12_060_306, 9));
        assert_eq!(trade.quote_quantity, Decimal::new(5_091_849_132_894, 10));
        assert_eq!(trade.aggressor, FieldState::Known(AggressorSide::Buy));
        let invalid =
            String::from_utf8_lossy(TRADES).replace("\"count\":\"3\"", "\"count\":\"130639475\"");
        assert_eq!(
            parse_trades(invalid.as_bytes(), &instrument, 1_630_048_898_000),
            Err(OkxError::Sequence)
        );
        Ok(())
    }

    #[test]
    fn closed_one_minute_candle_uses_documented_base_and_quote_volume()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = config()?;
        let instrument = parse_instrument(INSTRUMENT, &config, 7)?;
        let candle = parse_closed_1m_candle(CLOSED_CANDLE, &instrument, 1_630_048_860_000)?;
        assert_eq!(candle.sequence, 27_167_481);
        assert_eq!(candle.base_volume, FieldState::Known(Decimal::new(15, 1)));
        assert_eq!(
            candle.quote_volume,
            FieldState::Known(Decimal::new(90_030, 0))
        );
        assert!(matches!(candle.trade_count, FieldState::Unavailable { .. }));
        assert!(candle.is_valid());
        let forming = String::from_utf8_lossy(CLOSED_CANDLE).replace("\"1\"]]}", "\"0\"]]}");
        assert_eq!(
            parse_closed_1m_candle(forming.as_bytes(), &instrument, 1_630_048_860_000),
            Err(OkxError::Sequence)
        );
        Ok(())
    }
}
