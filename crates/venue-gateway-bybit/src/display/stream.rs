//! Account-free display decoding reuses the V5 book sequencer and trade normalization.
use super::*;
use crate::public_ws::BybitBookBridge;
use venue_domain::{FieldState, MarkFunding, MarketEvent, OpenInterestSample, OpenInterestUnit,
    Price, PublicTrade, UnknownReason};

pub const ENDPOINT: &str = "wss://stream.bybit.com/v5/public/linear";
pub const PING: &str = r#"{"op":"ping"}"#;

pub enum Frame {
    Book(Book),
    Trades(Vec<PublicTrade>),
    Bar {
        bar: PublicBar,
        closed: bool,
        event_time_ms: u64,
    },
    BaseMinuteBar { bar: PublicBar, closed: bool },
    SessionDayBar { bar: PublicBar, closed: bool },
    Derivatives { funding: Option<MarkFunding>, interest: Option<OpenInterestSample> },
}

pub struct Decoder {
    instrument: Instrument,
    interval_ms: u64,
    generation: u64,
    native: String,
    book: BybitBookBridge,
    ticker: Option<serde_json::Map<String, Value>>,
    last_derivatives_ms: u64,
    derivatives_enabled: bool,
    source_minutes: bool,
    source_days: bool,
}

impl Decoder {
    pub fn new(instrument: Instrument, interval_ms: u64, generation: u64) -> Result<Self> {
        Self::new_with_derivatives(instrument, interval_ms, generation, true)
    }

    pub fn new_with_derivatives(instrument: Instrument, interval_ms: u64,
        generation: u64, derivatives_enabled: bool) -> Result<Self> {
        Self::new_with_sources(instrument, interval_ms, generation,
            derivatives_enabled, derivatives_enabled, derivatives_enabled)
    }

    pub fn new_with_sources(instrument: Instrument, interval_ms: u64,
        generation: u64, derivatives_enabled: bool, source_minutes: bool,
        source_days: bool) -> Result<Self> {
        super::interval(interval_ms)?;
        if !matches!(instrument.symbol.quote(), "USDT" | "USDC") {
            return Err("unsupported Bybit display product".into());
        }
        let native = format!("{}{}", instrument.symbol.base(), instrument.symbol.quote());
        let book =
            BybitBookBridge::for_display(instrument.symbol.clone(), native.clone(), generation)
                .map_err(|e| e.to_string())?;
        Ok(Self {
            instrument,
            interval_ms,
            generation,
            native,
            book,
            ticker: None,
            last_derivatives_ms: 0,
            derivatives_enabled,
            source_minutes,
            source_days,
        })
    }

    pub fn subscription(&self) -> Result<String> {
        let mut args = vec![format!("orderbook.50.{}", self.native),
            format!("publicTrade.{}", self.native),
            format!("kline.{}.{}", super::interval(self.interval_ms)?, self.native)];
        if self.derivatives_enabled {
            args.push(format!("tickers.{}", self.native));
        }
        for (interval, needed) in [(60_000, self.source_minutes), (86_400_000, self.source_days)] {
            if !needed { continue; }
            let topic = format!("kline.{}.{}", super::interval(interval)?, self.native);
            if !args.contains(&topic) { args.push(topic); }
        }
        Ok(serde_json::json!({"op":"subscribe","args":args}).to_string())
    }

    pub fn parse(&mut self, payload: &str, now: u64) -> Result<Vec<Frame>> {
        if payload.len() > 1024 * 1024 {
            return Err("Bybit display frame too large".into());
        }
        let value: Value =
            serde_json::from_str(payload).map_err(|_| "invalid Bybit display JSON")?;
        if let Some(op) = value["op"].as_str() {
            if matches!(op, "ping" | "pong") || (op == "subscribe" && value["success"] == true) {
                return Ok(vec![]);
            }
            return Err("Bybit display subscription rejected".into());
        }
        let time = stamp(&value["ts"])?;
        if time > now || now.saturating_sub(time) > 15_000 {
            return Err("stale Bybit display frame".into());
        }
        let topic = string(&value["topic"])?;
        if topic == format!("orderbook.50.{}", self.native) {
            let root = value.as_object().ok_or("invalid Bybit display object")?;
            let (_, event) = self.book.accept(root, now).map_err(|e| e.to_string())?;
            let MarketEvent::Snapshot(book) = event else {
                return Err("Bybit book is not a complete snapshot".into());
            };
            let levels = |rows: Vec<venue_domain::MarketLevel>| {
                rows.into_iter()
                    .take(20)
                    .map(|row| (row.price.value(), row.quantity))
                    .collect()
            };
            return Ok(vec![Frame::Book(Book {
                bids: levels(book.bids),
                asks: levels(book.asks),
                time_ms: time,
            })]);
        }
        if topic == format!("publicTrade.{}", self.native) {
            return crate::public::parse_display_trades(
                payload,
                &self.instrument.symbol,
                self.generation,
                now,
            )
            .map(|trades| vec![Frame::Trades(trades)])
            .map_err(|e| e.to_string());
        }
        if self.derivatives_enabled && topic == format!("tickers.{}", self.native) {
            let row = value["data"].as_object().ok_or("invalid Bybit ticker")?;
            if row.get("symbol").and_then(Value::as_str).is_some_and(|symbol| symbol != self.native)
                || (value["type"] == "snapshot" && row.get("symbol").and_then(Value::as_str) != Some(self.native.as_str())) {
                return Err("Bybit ticker symbol mismatch".into());
            }
            match value["type"].as_str() {
                Some("snapshot") => self.ticker = Some(row.clone()),
                Some("delta") => {
                    let Some(current) = self.ticker.as_mut() else { return Ok(vec![]); };
                    current.extend(row.clone());
                }
                _ => return Err("invalid Bybit ticker update".into()),
            }
            if time.saturating_sub(self.last_derivatives_ms) < 15_000 { return Ok(vec![]); }
            let Some(current) = self.ticker.as_ref() else { return Ok(vec![]); };
            let field = |name: &str| current.get(name).filter(|value| !value.is_null()
                && value.as_str().is_none_or(|text| !text.is_empty()));
            let funding = (|| -> Result<MarkFunding> {
                let rate = number(field("fundingRate").ok_or("missing Bybit funding")?)?;
                let next = stamp(field("nextFundingTime").ok_or("missing Bybit funding time")?)?;
                let mark = Price::new(number(field("markPrice").ok_or("missing Bybit mark")?)?)
                    .map_err(|_| "invalid Bybit mark")?;
                let index = Price::new(number(field("indexPrice").ok_or("missing Bybit index")?)?)
                    .map_err(|_| "invalid Bybit index")?;
                if next <= time { return Err("expired Bybit funding time".into()); }
                Ok(MarkFunding { symbol: self.instrument.symbol.clone(), generation: self.generation,
                    received_at_ms: now, exchange_time_ms: time, time_source: venue_domain::MarketTimeSource::Exchange, next_funding_time_ms: Some(next),
                    mark_price: FieldState::Known(mark), index_price: FieldState::Known(index), funding_rate: rate,
                    estimated_settle_price: FieldState::Unavailable { reason: UnknownReason::SourceOmitted },
                    predicted_funding_rate: FieldState::Unavailable { reason: UnknownReason::SourceOmitted },
                    unknown_reason: None })
            })().ok();
            let interest = (|| -> Result<OpenInterestSample> {
                let quantity = number(field("openInterest").ok_or("missing Bybit OI")?)?;
                if quantity < Decimal::ZERO { return Err("invalid Bybit OI".into()); }
                let notional = field("openInterestValue").and_then(|value| number(value).ok())
                    .map_or(FieldState::Unavailable { reason: UnknownReason::SourceOmitted }, FieldState::Known);
                let sample = OpenInterestSample { symbol: self.instrument.symbol.clone(),
                    generation: self.generation, received_at_ms: now, exchange_time_ms: time, time_source: venue_domain::MarketTimeSource::Exchange,
                    sampling_interval_ms: None, native_quantity: quantity,
                    native_unit: OpenInterestUnit::BaseAsset, base_quantity: FieldState::Known(quantity),
                    quote_notional: notional, quote_asset: Some(self.instrument.symbol.quote().to_owned()) };
                if !sample.is_valid() { return Err("invalid Bybit OI".into()); }
                Ok(sample)
            })().ok();
            if funding.is_none() && interest.is_none() { return Ok(vec![]); }
            self.last_derivatives_ms = time;
            return Ok(vec![Frame::Derivatives { funding, interest }]);
        }
        let source_interval = [self.interval_ms, 60_000, 86_400_000].into_iter()
            .find(|interval| topic == format!("kline.{}.{}", super::interval(*interval).unwrap_or(""), self.native))
            .ok_or("Bybit display symbol or interval mismatch")?;
        if source_interval != self.interval_ms
            && !(source_interval == 60_000 && self.source_minutes)
            && !(source_interval == 86_400_000 && self.source_days) {
            return Err("unexpected Bybit shared interval".into());
        }
        let mut frames = Vec::new();
        for row in array(&value["data"])? {
            if string(&row["interval"])? != super::interval(source_interval)? {
                return Err("Bybit candle interval mismatch".into());
            }
            let candle = bar(
                &self.instrument.symbol,
                self.generation,
                now,
                source_interval,
                stamp(&row["start"])?,
                [
                    number(&row["open"])?,
                    number(&row["high"])?,
                    number(&row["low"])?,
                    number(&row["close"])?,
                ],
                number(&row["volume"])?,
                Some(number(&row["turnover"])?),
            )?;
            let closed = row["confirm"]
                .as_bool()
                .ok_or("missing Bybit candle confirmation")?;
            if stamp(&row["end"])? != candle.close_time_ms || (closed && candle.close_time_ms > now)
            {
                return Err("invalid Bybit candle boundary".into());
            }
            if source_interval == self.interval_ms {
                frames.push(Frame::Bar { bar: candle.clone(), closed, event_time_ms: time });
            }
            if self.source_minutes && source_interval == 60_000 {
                frames.push(Frame::BaseMinuteBar { bar: candle.clone(), closed });
            }
            if self.source_days && source_interval == 86_400_000 {
                frames.push(Frame::SessionDayBar { bar: candle, closed });
            }
        }
        Ok(frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn decoder() -> Result<Decoder> {
        Decoder::new(
            Instrument {
                symbol: "DOGE/USDT".parse().map_err(|_| "symbol")?,
                native_symbol: "DOGEUSDT".into(),
                price_tick: Some(Decimal::new(1, 5)),
                price_scale: 5,
                quantity_scale: 0,
                contract_size: Decimal::ONE,
            },
            60_000,
            1,
        )
    }
    #[test]
    fn sequence_reset_delta_deletion_and_crossed_symbol_remain_checked() -> Result<()> {
        let snapshot = r#"{"topic":"orderbook.50.DOGEUSDT","type":"snapshot","ts":1001,"cts":1000,"data":{"s":"DOGEUSDT","u":10,"seq":20,"b":[["0.10","50"],["0.09","30"]],"a":[["0.11","40"]]}}"#;
        let delta = r#"{"topic":"orderbook.50.DOGEUSDT","type":"delta","ts":1002,"cts":1001,"data":{"s":"DOGEUSDT","u":11,"seq":21,"b":[["0.10","0"]],"a":[]}}"#;
        let mut d = decoder()?;
        assert!(d.parse(delta, 1003).is_err());
        assert_eq!(d.parse(snapshot, 1003)?.len(), 1);
        let frames = d.parse(delta, 1003)?;
        assert!(
            matches!(&frames[0],Frame::Book(book) if book.bids.len()==1 && book.bids[0].0==Decimal::new(9,2))
        );
        assert!(d.parse(delta, 1003).is_err());
        assert!(
            d.parse(&snapshot.replace("DOGEUSDT", "BTCUSDT"), 1003)
                .is_err()
        );
        assert_eq!(
            d.parse(&snapshot.replace("\"u\":10", "\"u\":1"), 1003)?
                .len(),
            1
        );
        Ok(())
    }
    #[test]
    fn subscription_omits_unconsumed_shared_intervals() -> Result<()> {
        let source = decoder()?;
        let no_sources = Decoder::new_with_sources(source.instrument.clone(), 300_000,
            1, true, false, false)?;
        let topics = no_sources.subscription()?;
        assert!(topics.contains("kline.5.DOGEUSDT"));
        assert!(topics.contains("tickers.DOGEUSDT"));
        assert!(!topics.contains("kline.1.DOGEUSDT"));
        assert!(!topics.contains("kline.D.DOGEUSDT"));
        let minute_only = Decoder::new_with_sources(source.instrument.clone(), 300_000,
            1, true, true, false)?;
        let topics = minute_only.subscription()?;
        assert!(topics.contains("kline.1.DOGEUSDT"));
        assert!(!topics.contains("kline.D.DOGEUSDT"));
        Ok(())
    }
    #[test]
    fn ticker_snapshot_and_delta_keep_exact_venue_symbol_and_native_units() -> Result<()> {
        let mut decoder = decoder()?;
        assert!(decoder.subscription()?.contains("tickers.DOGEUSDT"));
        let no_derivatives = Decoder::new_with_derivatives(decoder.instrument.clone(), 60_000, 1, false)?;
        assert!(!no_derivatives.subscription()?.contains("tickers.DOGEUSDT"));
        let snapshot = serde_json::json!({"topic":"tickers.DOGEUSDT","type":"snapshot",
            "ts":1_000_000,"data":{"symbol":"DOGEUSDT","markPrice":"0.10",
            "indexPrice":"0.101","fundingRate":"-0.0001","nextFundingTime":"1200000",
            "openInterest":"1000","openInterestValue":"100"}}).to_string();
        let frames = decoder.parse(&snapshot, 1_000_001)?;
        assert!(matches!(&frames[..], [Frame::Derivatives { funding: Some(funding),
            interest: Some(interest) }] if funding.funding_rate == Decimal::new(-1, 4)
            && interest.base_quantity == FieldState::Known(Decimal::from(1000))));
        let delta = serde_json::json!({"topic":"tickers.DOGEUSDT","type":"delta",
            "ts":1_016_000,"data":{"openInterest":"1100"}}).to_string();
        let frames = decoder.parse(&delta, 1_016_001)?;
        assert!(matches!(&frames[..], [Frame::Derivatives { interest: Some(interest), .. }]
            if interest.base_quantity == FieldState::Known(Decimal::from(1100))));
        assert!(decoder.parse(&snapshot.replace("DOGEUSDT", "DOGEUSDC"), 1_016_001).is_err());
        Ok(())
    }
}
