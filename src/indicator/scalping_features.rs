use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroUsize,
};

use rust_decimal::{
    Decimal,
    prelude::{Signed, ToPrimitive},
};

use crate::{
    domain::{Price, PublicBar, PublicTrade, Symbol},
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
        TRADES_SOURCE,
    },
    market::OrderBook,
};

const BOOK_FEATURE_VERSION: &str = "pulse-mas-depth-ofi-v2";
const TRADE_FEATURE_VERSION: &str = "pulse-orderflow-v1";
const BAR_FEATURE_VERSION: &str = "pulse-ta-v1";
const TOXICITY_FEATURE_VERSION: &str = "pulse-mas-flow-toxicity-v2";
const SHORT_RETURN_PERIOD: usize = 2;
const TREND_PERIOD: usize = 16;
const BANDWIDTH_PERIOD: usize = 20;
const BANDWIDTH_MULTIPLIER: Decimal = Decimal::from_parts(2, 0, 0, false, 0);
const NATR_PERIOD: usize = 14;
const BOOK_DEPTH_LEVELS: usize = 10;
const TRADE_WINDOW: usize = 64;
const DECIMAL_SCALE: u32 = 8;

/// Builds generation-scoped book/trade features plus immutable contiguous closed-bar history.
/// Transport generations fence live book and flow; completed one-minute OHLC survives a reconnect
/// only when the next bar proves exact time continuity.
#[derive(Clone, Debug)]
pub struct ScalpingFeatureBuilder {
    profile: String,
    profile_digest: String,
    max_data_age_ms: u64,
    maximum_trades: NonZeroUsize,
    generation: Option<u64>,
    state: FeatureState,
    cursors: BTreeMap<String, SourceCursor>,
    book: Option<BookSample>,
    bars: VecDeque<BarSample>,
    trades: VecDeque<TradeSample>,
    previous_top: Option<(Price, Decimal, Price, Decimal)>,
    previous_bar_close: Option<Price>,
    natr_seed_sum: Decimal,
    natr_value: Option<Decimal>,
    natr_samples: usize,
}

#[derive(Clone, Debug)]
struct BookSample {
    symbol: Symbol,
    generation: u64,
    mid_price: Price,
    fair_price: Price,
    spread_bps: Decimal,
    depth_quote: Decimal,
    book_imbalance: Option<Decimal>,
}

#[derive(Clone, Debug)]
struct TradeSample {
    generation: u64,
    sequence: u64,
    signed_quantity: Decimal,
}

#[derive(Clone, Debug)]
struct BarSample {
    generation: u64,
    close_time_ms: u64,
    close: Price,
}

impl ScalpingFeatureBuilder {
    pub fn new(
        profile: impl Into<String>,
        profile_digest: impl Into<String>,
        trade_window_ms: u64,
        maximum_trades: NonZeroUsize,
    ) -> Result<Self, FeatureBuildError> {
        let profile = profile.into();
        let profile_digest = profile_digest.into();
        if profile.trim().is_empty()
            || profile_digest.len() != 64
            || !profile_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || trade_window_ms == 0
        {
            return Err(FeatureBuildError::Parameters);
        }
        Ok(Self {
            profile,
            profile_digest,
            max_data_age_ms: trade_window_ms,
            maximum_trades,
            generation: None,
            state: FeatureState::Warmup,
            cursors: BTreeMap::new(),
            book: None,
            bars: VecDeque::new(),
            trades: VecDeque::new(),
            previous_top: None,
            previous_bar_close: None,
            natr_seed_sum: Decimal::ZERO,
            natr_value: None,
            natr_samples: 0,
        })
    }

    pub fn ingest_book(
        &mut self,
        book: &OrderBook,
        received_at_ms: u64,
    ) -> Result<(), FeatureBuildError> {
        let symbol = book.symbol().cloned().ok_or(FeatureBuildError::Book)?;
        let generation = book.generation().ok_or(FeatureBuildError::Book)?;
        let sequence = book.sequence().ok_or(FeatureBuildError::Book)?;
        let bids = book.bids();
        let asks = book.asks();
        let best_bid = bids.first().ok_or(FeatureBuildError::Book)?;
        let best_ask = asks.first().ok_or(FeatureBuildError::Book)?;
        if best_bid.quantity <= Decimal::ZERO || best_ask.quantity <= Decimal::ZERO {
            return Err(FeatureBuildError::Book);
        }
        // OrderBook already proves Binance Futures continuity with `pu`; its update IDs are
        // monotonic but aggregated ranges are not required to be numerically adjacent.
        self.observe(BOOK_SOURCE, generation, sequence, received_at_ms, false)?;
        let denominator = best_bid.quantity + best_ask.quantity;
        let mid_price =
            Price::new((best_bid.price.value() + best_ask.price.value()) / Decimal::TWO)
                .map_err(|_| FeatureBuildError::Book)?;
        // Legacy VenuePulse `WeightedMid` is same-side weighted. It is intentionally distinct
        // from the cross-size microprice; RangeFade consumes this fair-price identity.
        let fair_price = Price::new(
            (best_bid.price.value() * best_bid.quantity
                + best_ask.price.value() * best_ask.quantity)
                / denominator,
        )
        .map_err(|_| FeatureBuildError::Book)?;
        let depth_quote = bids
            .iter()
            .take(BOOK_DEPTH_LEVELS)
            .chain(asks.iter().take(BOOK_DEPTH_LEVELS))
            .map(|level| level.price.value() * level.quantity)
            .sum();
        let book_imbalance = self.previous_top.map(|previous| {
            let (
                previous_bid_price,
                previous_bid_quantity,
                previous_ask_price,
                previous_ask_quantity,
            ) = previous;
            let bid_event = if best_bid.price > previous_bid_price {
                best_bid.quantity
            } else if best_bid.price == previous_bid_price {
                best_bid.quantity - previous_bid_quantity
            } else {
                -previous_bid_quantity
            };
            let ask_event = if best_ask.price < previous_ask_price {
                -best_ask.quantity
            } else if best_ask.price == previous_ask_price {
                previous_ask_quantity - best_ask.quantity
            } else {
                previous_ask_quantity
            };
            let available_base_depth = bids
                .iter()
                .take(BOOK_DEPTH_LEVELS)
                .chain(asks.iter().take(BOOK_DEPTH_LEVELS))
                .map(|level| level.quantity)
                .sum::<Decimal>();
            if available_base_depth.is_zero() {
                Decimal::ZERO
            } else {
                ((bid_event + ask_event) / available_base_depth)
                    .max(-Decimal::ONE)
                    .min(Decimal::ONE)
            }
        });
        let spread_bps = (best_ask.price.value() - best_bid.price.value()) / mid_price.value()
            * Decimal::new(10_000, 0);
        self.previous_top = Some((
            best_bid.price,
            best_bid.quantity,
            best_ask.price,
            best_ask.quantity,
        ));
        self.book = Some(BookSample {
            symbol,
            generation,
            mid_price,
            fair_price,
            spread_bps,
            depth_quote,
            book_imbalance,
        });
        self.trades.retain(|trade| trade.generation == generation);
        self.state = FeatureState::Warmup;
        Ok(())
    }

    pub fn ingest_bar(&mut self, bar: &PublicBar) -> Result<(), FeatureBuildError> {
        let book = self.book.as_ref().ok_or(FeatureBuildError::Book)?;
        if bar.symbol != book.symbol
            || bar.generation != book.generation
            || bar.interval_ms != 60_000
            || bar.close_time_ms <= bar.open_time_ms
            || bar.high < bar.open.max(bar.close)
            || bar.low > bar.open.min(bar.close)
            || bar.high < bar.low
        {
            return Err(FeatureBuildError::Bar);
        }
        if self.bars.back().is_some_and(|previous| {
            bar.open_time_ms != previous.close_time_ms
                && bar.open_time_ms != previous.close_time_ms.saturating_add(1)
        }) {
            self.clear_bar_history();
        }
        self.observe(
            BARS_SOURCE,
            bar.generation,
            bar.sequence,
            bar.close_time_ms,
            true,
        )?;
        let true_range =
            self.previous_bar_close
                .map_or(bar.high.value() - bar.low.value(), |previous_close| {
                    (bar.high.value() - bar.low.value())
                        .max((bar.high.value() - previous_close.value()).abs())
                        .max((bar.low.value() - previous_close.value()).abs())
                });
        self.update_natr(true_range);
        self.previous_bar_close = Some(bar.close);
        self.bars.push_back(BarSample {
            generation: bar.generation,
            close_time_ms: bar.close_time_ms,
            close: bar.close,
        });
        while self.bars.len() > self.maximum_trades.get().max(BANDWIDTH_PERIOD + 1) {
            let _ = self.bars.pop_front();
        }
        Ok(())
    }

    pub fn ingest_trade(&mut self, trade: &PublicTrade) -> Result<(), FeatureBuildError> {
        let book = self.book.as_ref().ok_or(FeatureBuildError::Book)?;
        if trade.symbol != book.symbol || trade.generation != book.generation {
            return Err(FeatureBuildError::Generation);
        }
        self.observe(
            TRADES_SOURCE,
            trade.generation,
            trade.aggregate_trade_id,
            trade.received_at_ms,
            true,
        )?;
        let signed_quantity = match trade.aggressor {
            crate::domain::FieldState::Known(crate::domain::AggressorSide::Buy) => trade.quantity,
            crate::domain::FieldState::Known(crate::domain::AggressorSide::Sell) => -trade.quantity,
            _ => return Err(FeatureBuildError::Trade),
        };
        self.trades.push_back(TradeSample {
            generation: trade.generation,
            sequence: trade.aggregate_trade_id,
            signed_quantity,
        });
        while self.trades.len() > self.maximum_trades.get().max(TRADE_WINDOW) {
            let _ = self.trades.pop_front();
        }
        Ok(())
    }

    pub fn frame(&mut self, now_ms: u64) -> Result<FeatureFrame, FeatureBuildError> {
        let book = self.book.as_ref().ok_or(FeatureBuildError::Book)?;
        let generation_trades: Vec<_> = self
            .trades
            .iter()
            .filter(|trade| trade.generation == book.generation)
            .rev()
            .take(TRADE_WINDOW)
            .collect();
        let gross_quantity: Decimal = generation_trades
            .iter()
            .map(|trade| trade.signed_quantity.abs())
            .sum();
        let signed_quantity: Decimal = generation_trades
            .iter()
            .map(|trade| trade.signed_quantity)
            .sum();
        let trade_imbalance = if gross_quantity.is_zero() {
            Decimal::ZERO
        } else {
            signed_quantity / gross_quantity
        };
        let toxicity = book.book_imbalance.map_or(Decimal::ZERO, |book_imbalance| {
            flow_toxicity(book_imbalance, trade_imbalance)
        });
        let trade_sequence = generation_trades
            .iter()
            .map(|trade| trade.sequence)
            .max()
            .unwrap_or(0);
        let bar_samples: Vec<_> = self
            .bars
            .iter()
            .filter(|sample| sample.generation == book.generation)
            .collect();
        let bar_features = derive_bar_features(&bar_samples, self.natr_value)?;
        let complete = trade_sequence > 0
            && generation_trades.len() == TRADE_WINDOW
            && book.book_imbalance.is_some()
            && bar_features
                .as_ref()
                .is_some_and(|features| features.expected_move_bps.is_some());
        let watermark_ms = self
            .cursors
            .values()
            .map(|cursor| cursor.event_time_ms)
            .max()
            .ok_or(FeatureBuildError::Cursor)?;
        let stale = complete
            && (now_ms.saturating_sub(watermark_ms) > self.max_data_age_ms
                || self.cursors.values().any(|cursor| {
                    watermark_ms.saturating_sub(cursor.event_time_ms) > self.max_data_age_ms
                }));
        let state = if self.state == FeatureState::DataGap {
            FeatureState::DataGap
        } else if complete {
            if stale {
                FeatureState::Stale
            } else {
                FeatureState::Ready
            }
        } else {
            FeatureState::Warmup
        };
        self.state = state;
        let bar_features = bar_features.unwrap_or_default();
        Ok(FeatureFrame {
            symbol: book.symbol.clone(),
            schema_version: 1,
            generation: book.generation,
            watermark_ms,
            state,
            cursors: self.cursors.clone(),
            feature_versions: BTreeMap::from([
                (BOOK_SOURCE.to_owned(), BOOK_FEATURE_VERSION.to_owned()),
                (TRADES_SOURCE.to_owned(), TRADE_FEATURE_VERSION.to_owned()),
                (BARS_SOURCE.to_owned(), BAR_FEATURE_VERSION.to_owned()),
                ("toxicity".to_owned(), TOXICITY_FEATURE_VERSION.to_owned()),
                ("_feature_profile".to_owned(), self.profile.clone()),
                (
                    "_feature_profile_digest".to_owned(),
                    self.profile_digest.clone(),
                ),
            ]),
            values: FeatureValues {
                mid_price: Price::new(book.mid_price.value().round_dp(DECIMAL_SCALE))
                    .map_err(|_| FeatureBuildError::Book)?,
                fair_price: Price::new(book.fair_price.value().round_dp(DECIMAL_SCALE))
                    .map_err(|_| FeatureBuildError::Book)?,
                spread_bps: book.spread_bps.round_dp(DECIMAL_SCALE),
                depth_quote: book.depth_quote.round_dp(DECIMAL_SCALE),
                book_imbalance: book
                    .book_imbalance
                    .unwrap_or(Decimal::ZERO)
                    .round_dp(DECIMAL_SCALE),
                trade_imbalance: trade_imbalance.round_dp(DECIMAL_SCALE),
                short_return_bps: bar_features.short_return_bps.round_dp(DECIMAL_SCALE),
                trend_efficiency: bar_features.trend_efficiency.round_dp(DECIMAL_SCALE),
                bandwidth_expansion: bar_features.bandwidth_expansion.round_dp(DECIMAL_SCALE),
                expected_move_bps: bar_features
                    .expected_move_bps
                    .unwrap_or(Decimal::ZERO)
                    .round_dp(DECIMAL_SCALE),
                toxicity: toxicity.round_dp(DECIMAL_SCALE),
            },
            breakout: None,
        })
    }

    fn update_natr(&mut self, true_range: Decimal) {
        self.natr_samples = self.natr_samples.saturating_add(1);
        if self.natr_samples <= NATR_PERIOD {
            self.natr_seed_sum += true_range;
            if self.natr_samples == NATR_PERIOD {
                self.natr_value = Some(self.natr_seed_sum / Decimal::from(NATR_PERIOD as u32));
            }
            return;
        }
        if let Some(previous) = self.natr_value {
            self.natr_value = Some(
                (previous * Decimal::from((NATR_PERIOD - 1) as u32) + true_range)
                    / Decimal::from(NATR_PERIOD as u32),
            );
        }
    }

    fn observe(
        &mut self,
        source: &'static str,
        generation: u64,
        sequence: u64,
        event_time_ms: u64,
        require_contiguous_sequence: bool,
    ) -> Result<(), FeatureBuildError> {
        if generation == 0 || sequence == 0 || event_time_ms == 0 {
            return Err(FeatureBuildError::Cursor);
        }
        match self.generation {
            None => self.reset_generation(generation),
            Some(current) if generation > current => self.reset_generation(generation),
            Some(current) if generation < current => return Err(FeatureBuildError::Generation),
            Some(_) => {}
        }
        if let Some(previous) = self.cursors.get(source) {
            if sequence <= previous.sequence || event_time_ms < previous.event_time_ms {
                return Err(FeatureBuildError::Cursor);
            }
            if require_contiguous_sequence && sequence != previous.sequence.saturating_add(1) {
                self.state = FeatureState::DataGap;
                return Err(FeatureBuildError::DataGap);
            }
        }
        self.cursors.insert(
            source.to_owned(),
            SourceCursor {
                generation,
                sequence,
                event_time_ms,
                fresh: true,
            },
        );
        Ok(())
    }

    fn reset_generation(&mut self, generation: u64) {
        self.generation = Some(generation);
        self.state = FeatureState::Rebuilding;
        self.cursors.clear();
        self.book = None;
        for bar in &mut self.bars {
            bar.generation = generation;
        }
        self.trades.clear();
        self.previous_top = None;
    }

    fn clear_bar_history(&mut self) {
        self.bars.clear();
        self.previous_bar_close = None;
        self.natr_seed_sum = Decimal::ZERO;
        self.natr_value = None;
        self.natr_samples = 0;
    }
}

fn flow_toxicity(book_imbalance: Decimal, trade_imbalance: Decimal) -> Decimal {
    ((book_imbalance - trade_imbalance).abs() / Decimal::TWO)
        .max(Decimal::ZERO)
        .min(Decimal::ONE)
}

#[derive(Clone, Copy, Debug, Default)]
struct BarFeatures {
    short_return_bps: Decimal,
    trend_efficiency: Decimal,
    bandwidth_expansion: Decimal,
    expected_move_bps: Option<Decimal>,
}

fn derive_bar_features(
    samples: &[&BarSample],
    natr_value: Option<Decimal>,
) -> Result<Option<BarFeatures>, FeatureBuildError> {
    if samples.len() < BANDWIDTH_PERIOD + 1 {
        return Ok(None);
    }
    let current = samples.last().ok_or(FeatureBuildError::Bar)?.close;
    if current.value() <= Decimal::ZERO {
        return Err(FeatureBuildError::Book);
    }
    let short_oldest = samples
        .get(samples.len().saturating_sub(SHORT_RETURN_PERIOD + 1))
        .ok_or(FeatureBuildError::Bar)?
        .close;
    if short_oldest.value() <= Decimal::ZERO {
        return Err(FeatureBuildError::Book);
    }
    let short_return_bps =
        (current.value() / short_oldest.value() - Decimal::ONE) * Decimal::new(10_000, 0);
    let trend = samples
        .get(samples.len().saturating_sub(TREND_PERIOD + 1)..)
        .ok_or(FeatureBuildError::Book)?;
    let trend_oldest = trend.first().ok_or(FeatureBuildError::Bar)?.close.value();
    let trend_change = (current.value() - trend_oldest).abs();
    let trend_path: Decimal = trend
        .windows(2)
        .map(|pair| (pair[1].close.value() - pair[0].close.value()).abs())
        .sum();
    let trend_efficiency = if trend_path.is_zero() {
        Decimal::ZERO
    } else {
        (trend_change / trend_path) * short_return_bps.signum()
    };
    let current_bandwidth = bandwidth(&samples[samples.len() - BANDWIDTH_PERIOD..])?;
    let previous_bandwidth =
        bandwidth(&samples[samples.len() - BANDWIDTH_PERIOD - 1..samples.len() - 1])?;
    let bandwidth_expansion = if previous_bandwidth.is_zero() {
        Decimal::ZERO
    } else {
        current_bandwidth / previous_bandwidth - Decimal::ONE
    };
    Ok(Some(BarFeatures {
        short_return_bps,
        trend_efficiency,
        bandwidth_expansion,
        expected_move_bps: natr_value.map(|atr| atr / current.value() * Decimal::new(10_000, 0)),
    }))
}

fn bandwidth(samples: &[&BarSample]) -> Result<Decimal, FeatureBuildError> {
    let mean = samples
        .iter()
        .map(|sample| sample.close.value())
        .sum::<Decimal>()
        / Decimal::from(samples.len() as u32);
    if mean <= Decimal::ZERO {
        return Err(FeatureBuildError::Book);
    }
    let variance = samples
        .iter()
        .map(|sample| {
            let delta = sample.close.value() - mean;
            delta * delta
        })
        .sum::<Decimal>()
        / Decimal::from(samples.len() as u32);
    let standard_deviation = variance.to_f64().ok_or(FeatureBuildError::Book)?.sqrt();
    let standard_deviation =
        Decimal::from_f64_retain(standard_deviation).ok_or(FeatureBuildError::Book)?;
    Ok(BANDWIDTH_MULTIPLIER * standard_deviation * Decimal::TWO / mean)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FeatureBuildError {
    #[error("scalping feature builder parameters are invalid")]
    Parameters,
    #[error("a synchronized non-empty order book is required")]
    Book,
    #[error("trade does not match the active book symbol and generation")]
    Generation,
    #[error("trade aggressor is unknown and cannot be used for signed flow")]
    Trade,
    #[error("a completed, generation-matched one-minute OHLC bar is required")]
    Bar,
    #[error("feature source cursor is invalid or regressed")]
    Cursor,
    #[error("feature source sequence gap requires a new generation")]
    DataGap,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use rust_decimal::Decimal;

    use crate::{
        domain::{
            AggressorSide, FieldState, MarketLevel, MarketSnapshot, Price, PublicBar, PublicTrade,
        },
        indicator::FeatureState,
        market::OrderBook,
    };

    use super::{BarSample, ScalpingFeatureBuilder, derive_bar_features, flow_toxicity};

    fn builder() -> Result<ScalpingFeatureBuilder, Box<dyn std::error::Error>> {
        let capacity = NonZeroUsize::new(32).ok_or("non-zero capacity")?;
        Ok(ScalpingFeatureBuilder::new(
            "scalping-shadow-v1",
            "0".repeat(64),
            1_000,
            capacity,
        )?)
    }

    fn book() -> Result<OrderBook, Box<dyn std::error::Error>> {
        let mut book = OrderBook::default();
        book.apply_snapshot(MarketSnapshot {
            symbol: "BTC/USDT".parse()?,
            generation: 3,
            sequence: 9,
            exchange_time_ms: Some(100),
            bids: vec![MarketLevel {
                price: Price::new(Decimal::new(99, 0))?,
                quantity: Decimal::new(4, 0),
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::new(101, 0))?,
                quantity: Decimal::ONE,
            }],
        });
        Ok(book)
    }

    #[test]
    fn expected_move_fails_closed_without_ohlc_bars() -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = builder()?;
        let book = book()?;
        builder.ingest_book(&book, 100)?;
        assert_eq!(builder.frame(100)?.state, FeatureState::Warmup);
        builder.ingest_trade(&PublicTrade {
            symbol: "BTC/USDT".parse()?,
            generation: 3,
            received_at_ms: 100,
            exchange_time_ms: 100,
            transaction_time_ms: 100,
            aggregate_trade_id: 7,
            first_trade_id: 7,
            last_trade_id: 7,
            price: Price::new(Decimal::new(100, 0))?,
            quantity: Decimal::ONE,
            quote_quantity: Decimal::new(100, 0),
            aggressor: FieldState::Known(AggressorSide::Buy),
        })?;
        let frame = builder.frame(100)?;
        assert_eq!(frame.state, FeatureState::Warmup);
        assert_eq!(frame.values.expected_move_bps, Decimal::ZERO);
        assert!(frame.values.trade_imbalance > Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn aggregate_trade_cursor_allows_one_event_to_cover_multiple_trade_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = builder()?;
        builder.ingest_book(&book()?, 100)?;
        let first = PublicTrade {
            symbol: "BTC/USDT".parse()?,
            generation: 3,
            received_at_ms: 100,
            exchange_time_ms: 100,
            transaction_time_ms: 100,
            aggregate_trade_id: 7,
            first_trade_id: 70,
            last_trade_id: 70,
            price: Price::new(Decimal::new(100, 0))?,
            quantity: Decimal::ONE,
            quote_quantity: Decimal::new(100, 0),
            aggressor: FieldState::Known(AggressorSide::Buy),
        };
        builder.ingest_trade(&first)?;
        builder.ingest_trade(&PublicTrade {
            received_at_ms: 101,
            exchange_time_ms: 101,
            transaction_time_ms: 101,
            aggregate_trade_id: 8,
            first_trade_id: 71,
            last_trade_id: 75,
            ..first
        })?;
        Ok(())
    }

    #[test]
    fn legacy_close_features_use_fixed_periods_and_signed_efficiency()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut samples = Vec::new();
        for value in 100..=121 {
            samples.push(BarSample {
                generation: 1,
                close_time_ms: value as u64,
                close: Price::new(Decimal::from(value))?,
            });
        }
        let refs: Vec<_> = samples.iter().collect();
        let features = derive_bar_features(&refs, Some(Decimal::ONE))?.ok_or("feature warmup")?;
        assert_eq!(
            features.short_return_bps,
            (Decimal::from(121) / Decimal::from(119) - Decimal::ONE) * Decimal::new(10_000, 0)
        );
        assert_eq!(features.trend_efficiency, Decimal::ONE);
        assert!(features.bandwidth_expansion < Decimal::ZERO);
        assert!(features.expected_move_bps.is_some());
        Ok(())
    }

    #[test]
    fn sparse_futures_book_ids_are_accepted_after_orderbook_continuity_proof()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = builder()?;
        let mut book = book()?;
        builder.ingest_book(&book, 100)?;
        book.apply_snapshot(MarketSnapshot {
            symbol: "BTC/USDT".parse()?,
            generation: 3,
            sequence: 11,
            exchange_time_ms: Some(101),
            bids: vec![MarketLevel {
                price: Price::new(Decimal::new(99, 0))?,
                quantity: Decimal::new(4, 0),
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::new(101, 0))?,
                quantity: Decimal::ONE,
            }],
        });
        builder.ingest_book(&book, 101)?;
        assert_eq!(builder.frame(101)?.state, FeatureState::Warmup);
        Ok(())
    }

    #[test]
    fn book_ofi_and_toxicity_match_legacy_normalized_domain()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = builder()?;
        let mut book = book()?;
        builder.ingest_book(&book, 100)?;
        book.apply_snapshot(MarketSnapshot {
            symbol: "BTC/USDT".parse()?,
            generation: 3,
            sequence: 10,
            exchange_time_ms: Some(101),
            bids: vec![MarketLevel {
                price: Price::new(Decimal::new(99, 0))?,
                quantity: Decimal::new(3, 0),
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::new(101, 0))?,
                quantity: Decimal::ONE,
            }],
        });
        builder.ingest_book(&book, 101)?;
        let frame = builder.frame(101)?;
        assert_eq!(frame.values.fair_price.value(), Decimal::new(995, 1));
        assert_eq!(frame.values.book_imbalance, -Decimal::new(25, 2));
        assert_eq!(frame.values.toxicity, Decimal::new(125, 3));
        assert_eq!(
            flow_toxicity(Decimal::new(8, 1), -Decimal::new(8, 1)),
            Decimal::new(8, 1)
        );
        Ok(())
    }

    #[test]
    fn wilder_natr_uses_completed_ohlc_bars() -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = builder()?;
        builder.ingest_book(&book()?, 100)?;
        for sequence in 1..=21 {
            builder.ingest_bar(&PublicBar {
                symbol: "BTC/USDT".parse()?,
                generation: 3,
                received_at_ms: sequence * 60_000,
                sequence,
                open_time_ms: (sequence - 1) * 60_000,
                close_time_ms: sequence * 60_000,
                interval_ms: 60_000,
                open: Price::new(Decimal::new(100, 0))?,
                high: Price::new(Decimal::new(101, 0))?,
                low: Price::new(Decimal::new(99, 0))?,
                close: Price::new(Decimal::new(100, 0))?,
            })?;
        }
        assert_eq!(builder.natr_value, Some(Decimal::new(2, 0)));
        let frame = builder.frame(1_260_000)?;
        assert_eq!(frame.values.expected_move_bps, Decimal::new(200, 0));
        assert_eq!(frame.state, FeatureState::Warmup);
        Ok(())
    }

    #[test]
    fn contiguous_closed_bars_survive_a_transport_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = builder()?;
        builder.ingest_book(&book()?, 100)?;
        for sequence in 1..=13 {
            builder.ingest_bar(&PublicBar {
                symbol: "BTC/USDT".parse()?,
                generation: 3,
                received_at_ms: sequence * 60_000,
                sequence,
                open_time_ms: (sequence - 1) * 60_000,
                close_time_ms: sequence * 60_000 - 1,
                interval_ms: 60_000,
                open: Price::new(Decimal::new(100, 0))?,
                high: Price::new(Decimal::new(101, 0))?,
                low: Price::new(Decimal::new(99, 0))?,
                close: Price::new(Decimal::new(100, 0))?,
            })?;
        }
        let mut next_book = OrderBook::default();
        next_book.apply_snapshot(MarketSnapshot {
            symbol: "BTC/USDT".parse()?,
            generation: 4,
            sequence: 1,
            exchange_time_ms: Some(780_000),
            bids: vec![MarketLevel {
                price: Price::new(Decimal::new(99, 0))?,
                quantity: Decimal::ONE,
            }],
            asks: vec![MarketLevel {
                price: Price::new(Decimal::new(101, 0))?,
                quantity: Decimal::ONE,
            }],
        });
        builder.ingest_book(&next_book, 780_000)?;
        builder.ingest_bar(&PublicBar {
            symbol: "BTC/USDT".parse()?,
            generation: 4,
            received_at_ms: 840_000,
            sequence: 1,
            open_time_ms: 780_000,
            close_time_ms: 839_999,
            interval_ms: 60_000,
            open: Price::new(Decimal::new(100, 0))?,
            high: Price::new(Decimal::new(101, 0))?,
            low: Price::new(Decimal::new(99, 0))?,
            close: Price::new(Decimal::new(100, 0))?,
        })?;
        assert_eq!(builder.natr_samples, 14);
        assert!(builder.bars.iter().all(|bar| bar.generation == 4));
        assert!(builder.natr_value.is_some());
        Ok(())
    }
}
