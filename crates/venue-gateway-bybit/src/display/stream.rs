//! Account-free display decoding reuses the V5 book sequencer and trade normalization.
use super::*;
use crate::public_ws::BybitBookBridge;
use venue_domain::{MarketEvent, PublicTrade};

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
}

pub struct Decoder {
    instrument: Instrument,
    interval_ms: u64,
    generation: u64,
    native: String,
    book: BybitBookBridge,
}

impl Decoder {
    pub fn new(instrument: Instrument, interval_ms: u64, generation: u64) -> Result<Self> {
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
        })
    }

    pub fn subscription(&self) -> Result<String> {
        Ok(serde_json::json!({"op":"subscribe","args":[format!("orderbook.50.{}", self.native),format!("publicTrade.{}",self.native),format!("kline.{}.{}",super::interval(self.interval_ms)?,self.native)]}).to_string())
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
        if topic
            != format!(
                "kline.{}.{}",
                super::interval(self.interval_ms)?,
                self.native
            )
        {
            return Err("Bybit display symbol or interval mismatch".into());
        }
        let mut frames = Vec::new();
        for row in array(&value["data"])? {
            if string(&row["interval"])? != super::interval(self.interval_ms)? {
                return Err("Bybit candle interval mismatch".into());
            }
            let candle = bar(
                &self.instrument.symbol,
                self.generation,
                now,
                self.interval_ms,
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
            frames.push(Frame::Bar {
                bar: candle,
                closed,
                event_time_ms: time,
            });
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
}
