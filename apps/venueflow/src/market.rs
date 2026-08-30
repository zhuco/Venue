use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use venue_control_protocol::{UiBar, UiBookLevel, UiTrade};
use venue_domain::{FieldState, PublicBar};
use venue_gateway_api::PublicMarketBinding;
use venue_indicators::chart::{
    ChartIndicatorError, ChartStudyConfig, ChartStudyEngine, ChartStudyValues,
};

use crate::chart::{ChartInterval, ChartStudyPoint};

pub const MAX_BARS: usize = 2_000;
pub const MAX_TRADES: usize = 200;
pub const MAX_BOOK_LEVELS: usize = 20;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MarketSelection {
    pub binding: PublicMarketBinding,
    pub interval: ChartInterval,
}

impl MarketSelection {
    pub fn binance_usd_m(symbol: &str, interval: ChartInterval) -> Result<Self, LocalMarketError> {
        let symbol = symbol
            .parse()
            .map_err(|_| LocalMarketError::InvalidSymbol)?;
        let binding = PublicMarketBinding::binance_usds_m(symbol)
            .map_err(|_| LocalMarketError::InvalidBinding)?;
        Ok(Self { binding, interval })
    }

    pub fn validate(&self) -> Result<(), LocalMarketError> {
        self.binding
            .validate()
            .map_err(|_| LocalMarketError::InvalidBinding)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketStatus {
    LoadingHistory,
    Connecting,
    Live,
    Stale,
    Resyncing,
    Offline,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MarketPayload {
    RestHistory {
        bars: Vec<PublicBar>,
    },
    WsBar {
        bar: UiBar,
        study_bar: Box<PublicBar>,
        closed: bool,
    },
    BookSnapshot {
        bids: Vec<UiBookLevel>,
        asks: Vec<UiBookLevel>,
    },
    Bbo {
        bid: Decimal,
        ask: Decimal,
    },
    Trade(UiTrade),
    Status {
        status: MarketStatus,
        detail: Option<String>,
    },
}

/// One result from an external await, bound to the exact subscription that started it.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketEnvelope {
    pub generation: u64,
    pub selection: MarketSelection,
    pub event_time_ms: u64,
    pub received_ms: u64,
    pub payload: MarketPayload,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LocalMarketView {
    pub generation: u64,
    pub selection: MarketSelection,
    pub status: MarketStatus,
    pub status_detail: Option<String>,
    pub bars: Vec<UiBar>,
    pub studies: Vec<ChartStudyPoint>,
    pub bids: Vec<UiBookLevel>,
    pub asks: Vec<UiBookLevel>,
    pub trades: Vec<UiTrade>,
    pub last: Option<Decimal>,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub last_event_ms: Option<u64>,
    pub last_received_ms: Option<u64>,
    pub latency_ms: Option<u64>,
}

impl LocalMarketView {
    fn empty(generation: u64, selection: MarketSelection) -> Self {
        Self {
            generation,
            selection,
            status: MarketStatus::LoadingHistory,
            status_detail: None,
            bars: Vec::new(),
            studies: Vec::new(),
            bids: Vec::new(),
            asks: Vec::new(),
            trades: Vec::new(),
            last: None,
            bid: None,
            ask: None,
            last_event_ms: None,
            last_received_ms: None,
            latency_ms: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReduceOutcome {
    Applied,
    IgnoredOldGeneration,
}

#[derive(Clone, Debug)]
pub struct LocalMarketReducer {
    view: LocalMarketView,
    closed_bars: BTreeSet<u64>,
    closed_facts: BTreeMap<u64, PublicBar>,
    studies: ChartStudyEngine,
    study_config: ChartStudyConfig,
}

impl LocalMarketReducer {
    #[cfg(test)]
    pub fn new(selection: MarketSelection) -> Result<Self, LocalMarketError> {
        Self::new_at_generation(selection, 1, ChartStudyConfig::default())
    }

    fn new_at_generation(
        selection: MarketSelection,
        generation: u64,
        study_config: ChartStudyConfig,
    ) -> Result<Self, LocalMarketError> {
        selection.validate()?;
        if generation == 0 {
            return Err(LocalMarketError::InvalidGeneration);
        }
        Ok(Self {
            view: LocalMarketView::empty(generation, selection),
            closed_bars: BTreeSet::new(),
            closed_facts: BTreeMap::new(),
            studies: ChartStudyEngine::with_config(&study_config)
                .map_err(LocalMarketError::Indicator)?,
            study_config,
        })
    }

    pub fn view(&self) -> &LocalMarketView {
        &self.view
    }

    /// Starts a fresh subscription. Results from the previous generation can no longer mutate the
    /// view, even if their network request completes after this call.
    #[cfg(test)]
    pub fn select(&mut self, selection: MarketSelection) -> Result<u64, LocalMarketError> {
        selection.validate()?;
        let generation = self
            .view
            .generation
            .checked_add(1)
            .ok_or(LocalMarketError::GenerationExhausted)?;
        self.view = LocalMarketView::empty(generation, selection);
        self.closed_bars.clear();
        self.closed_facts.clear();
        self.studies.reset();
        Ok(generation)
    }

    pub fn apply(&mut self, envelope: MarketEnvelope) -> Result<ReduceOutcome, LocalMarketError> {
        if envelope.generation < self.view.generation {
            return Ok(ReduceOutcome::IgnoredOldGeneration);
        }
        if envelope.generation > self.view.generation {
            return Err(LocalMarketError::FutureGeneration);
        }
        if envelope.selection != self.view.selection {
            return Err(LocalMarketError::ScopeMismatch);
        }
        if envelope.event_time_ms > envelope.received_ms {
            return Err(LocalMarketError::EventFromFuture);
        }

        match envelope.payload {
            MarketPayload::RestHistory { bars } => self.apply_history(bars)?,
            MarketPayload::WsBar {
                bar,
                study_bar,
                closed,
            } => self.apply_bar(bar, *study_bar, closed)?,
            MarketPayload::BookSnapshot { bids, asks } => self.apply_book(bids, asks)?,
            MarketPayload::Bbo { bid, ask } => self.apply_bbo(bid, ask)?,
            MarketPayload::Trade(trade) => self.apply_trade(trade)?,
            MarketPayload::Status { status, detail } => {
                self.view.status = status;
                self.view.status_detail = detail;
            }
        }

        self.view.last_event_ms = Some(envelope.event_time_ms);
        self.view.last_received_ms = Some(envelope.received_ms);
        self.view.latency_ms = Some(envelope.received_ms - envelope.event_time_ms);
        Ok(ReduceOutcome::Applied)
    }

    pub fn refresh_staleness(&mut self, now_ms: u64, stale_after_ms: u64) {
        if self.view.status != MarketStatus::Live {
            return;
        }
        let Some(last_received_ms) = self.view.last_received_ms else {
            self.view.status = MarketStatus::Stale;
            self.view.status_detail = Some("no market event received".to_owned());
            return;
        };
        if now_ms.saturating_sub(last_received_ms) > stale_after_ms {
            self.view.status = MarketStatus::Stale;
            self.view.status_detail = Some("market event timeout".to_owned());
        }
    }

    fn apply_history(&mut self, mut bars: Vec<PublicBar>) -> Result<(), LocalMarketError> {
        bars.sort_by_key(|bar| bar.open_time_ms);
        self.view.bars.clear();
        self.view.studies.clear();
        self.closed_bars.clear();
        self.closed_facts.clear();
        self.studies.reset();
        for bar in bars {
            validate_study_bar(&bar, &self.view.selection)?;
            let ui_bar = ui_bar_from_public(&bar)?;
            let values = self
                .studies
                .ingest_closed(&bar)
                .map_err(LocalMarketError::Indicator)?;
            self.closed_bars.insert(bar.open_time_ms);
            self.closed_facts.insert(bar.open_time_ms, bar);
            upsert_bar(&mut self.view.bars, ui_bar.clone());
            upsert_study(
                &mut self.view.studies,
                ChartStudyPoint {
                    open_time_ms: ui_bar.open_time_ms,
                    confirmed: true,
                    ..study_point(values)
                },
            );
        }
        trim_bars(&mut self.view.bars, &mut self.closed_bars);
        trim_studies(&mut self.view.studies);
        self.view.last = self.view.bars.last().map(|bar| bar.close);
        Ok(())
    }

    fn apply_bar(
        &mut self,
        bar: UiBar,
        study_bar: PublicBar,
        closed: bool,
    ) -> Result<(), LocalMarketError> {
        validate_bar(&bar, self.view.selection.interval)?;
        validate_study_bar(&study_bar, &self.view.selection)?;
        if bar.open_time_ms != study_bar.open_time_ms {
            return Err(LocalMarketError::ScopeMismatch);
        }
        if self.closed_bars.contains(&bar.open_time_ms) && !closed {
            return Ok(());
        }
        if closed {
            if let Some(existing) = self.closed_facts.get(&bar.open_time_ms) {
                if existing != &study_bar {
                    self.closed_facts.insert(bar.open_time_ms, study_bar);
                    return self.rebuild_studies_and_bars();
                }
            } else {
                let values = self
                    .studies
                    .ingest_closed(&study_bar)
                    .map_err(LocalMarketError::Indicator)?;
                upsert_study(
                    &mut self.view.studies,
                    ChartStudyPoint {
                        open_time_ms: bar.open_time_ms,
                        confirmed: true,
                        ..study_point(values)
                    },
                );
                self.closed_facts.insert(bar.open_time_ms, study_bar);
            }
            self.closed_bars.insert(bar.open_time_ms);
        } else {
            let values = self
                .studies
                .preview(&study_bar)
                .map_err(LocalMarketError::Indicator)?;
            upsert_study(
                &mut self.view.studies,
                ChartStudyPoint {
                    open_time_ms: bar.open_time_ms,
                    confirmed: false,
                    ..study_point(values)
                },
            );
        }
        self.view.last = Some(bar.close);
        upsert_bar(&mut self.view.bars, bar);
        trim_bars(&mut self.view.bars, &mut self.closed_bars);
        trim_studies(&mut self.view.studies);
        Ok(())
    }

    fn rebuild_studies_and_bars(&mut self) -> Result<(), LocalMarketError> {
        self.studies = ChartStudyEngine::with_config(&self.study_config)
            .map_err(LocalMarketError::Indicator)?;
        self.view.studies.clear();
        self.view.bars.clear();
        self.closed_bars.clear();
        for bar in self.closed_facts.values() {
            let values = self
                .studies
                .ingest_closed(bar)
                .map_err(LocalMarketError::Indicator)?;
            let ui_bar = ui_bar_from_public(bar)?;
            self.closed_bars.insert(bar.open_time_ms);
            self.view.bars.push(ui_bar);
            self.view.studies.push(ChartStudyPoint {
                open_time_ms: bar.open_time_ms,
                confirmed: true,
                ..study_point(values)
            });
        }
        self.view.last = self.view.bars.last().map(|bar| bar.close);
        trim_bars(&mut self.view.bars, &mut self.closed_bars);
        trim_studies(&mut self.view.studies);
        Ok(())
    }

    fn reconfigure_studies(
        &mut self,
        study_config: ChartStudyConfig,
    ) -> Result<(), LocalMarketError> {
        if self.study_config == study_config {
            return Ok(());
        }
        study_config
            .validate()
            .map_err(LocalMarketError::Indicator)?;
        self.study_config = study_config;
        self.rebuild_studies_and_bars()
    }

    fn apply_bbo(&mut self, bid: Decimal, ask: Decimal) -> Result<(), LocalMarketError> {
        if bid <= Decimal::ZERO || ask <= Decimal::ZERO || bid >= ask {
            return Err(LocalMarketError::InvalidBbo);
        }
        self.view.bid = Some(bid);
        self.view.ask = Some(ask);
        Ok(())
    }

    fn apply_book(
        &mut self,
        bids: Vec<UiBookLevel>,
        asks: Vec<UiBookLevel>,
    ) -> Result<(), LocalMarketError> {
        let bids = normalize_book_side(bids, true)?;
        let asks = normalize_book_side(asks, false)?;
        if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first())
            && best_bid.price >= best_ask.price
        {
            return Err(LocalMarketError::CrossedBook);
        }
        self.view.bid = bids.first().map(|level| level.price);
        self.view.ask = asks.first().map(|level| level.price);
        self.view.bids = bids;
        self.view.asks = asks;
        Ok(())
    }

    fn apply_trade(&mut self, trade: UiTrade) -> Result<(), LocalMarketError> {
        if trade.trade_id.trim().is_empty()
            || trade.occurred_ms == 0
            || trade.price <= Decimal::ZERO
            || trade.quantity <= Decimal::ZERO
        {
            return Err(LocalMarketError::InvalidTrade);
        }
        if self
            .view
            .trades
            .iter()
            .any(|existing| existing.trade_id == trade.trade_id)
        {
            return Ok(());
        }
        self.view.last = Some(trade.price);
        self.view.trades.push(trade);
        self.view.trades.sort_by(|left, right| {
            left.occurred_ms
                .cmp(&right.occurred_ms)
                .then_with(|| left.trade_id.cmp(&right.trade_id))
        });
        if self.view.trades.len() > MAX_TRADES {
            self.view
                .trades
                .drain(..self.view.trades.len() - MAX_TRADES);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalMarketStore {
    generation: u64,
    reducers: BTreeMap<MarketSelection, LocalMarketReducer>,
    study_config: ChartStudyConfig,
}

impl LocalMarketStore {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn selections(&self) -> impl Iterator<Item = &MarketSelection> {
        self.reducers.keys()
    }

    pub fn replace(
        &mut self,
        selections: impl IntoIterator<Item = MarketSelection>,
    ) -> Result<Option<u64>, LocalMarketError> {
        let unique = selections.into_iter().collect::<BTreeSet<_>>();
        if unique.iter().eq(self.reducers.keys()) {
            return Ok(None);
        }
        let generation = self
            .generation
            .checked_add(1)
            .ok_or(LocalMarketError::GenerationExhausted)?;
        let mut reducers = BTreeMap::new();
        for selection in unique {
            let reducer = LocalMarketReducer::new_at_generation(
                selection.clone(),
                generation,
                self.study_config.clone(),
            )?;
            reducers.insert(selection, reducer);
        }
        self.generation = generation;
        self.reducers = reducers;
        Ok(Some(generation))
    }

    pub fn apply(&mut self, envelope: MarketEnvelope) -> Result<ReduceOutcome, LocalMarketError> {
        if envelope.generation < self.generation {
            return Ok(ReduceOutcome::IgnoredOldGeneration);
        }
        if envelope.generation > self.generation {
            return Err(LocalMarketError::FutureGeneration);
        }
        let reducer = self
            .reducers
            .get_mut(&envelope.selection)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        reducer.apply(envelope)
    }

    pub fn view(&self, selection: &MarketSelection) -> Option<&LocalMarketView> {
        self.reducers.get(selection).map(LocalMarketReducer::view)
    }

    pub fn view_for_symbol(&self, symbol: &str) -> Option<&LocalMarketView> {
        self.reducers
            .values()
            .map(LocalMarketReducer::view)
            .find(|view| view.selection.binding.symbol.to_string() == symbol)
    }

    pub fn refresh_staleness(&mut self, now_ms: u64, stale_after_ms: u64) {
        for reducer in self.reducers.values_mut() {
            reducer.refresh_staleness(now_ms, stale_after_ms);
        }
    }

    pub fn reconfigure_studies(
        &mut self,
        study_config: ChartStudyConfig,
    ) -> Result<(), LocalMarketError> {
        study_config
            .validate()
            .map_err(LocalMarketError::Indicator)?;
        if self.study_config == study_config {
            return Ok(());
        }
        for reducer in self.reducers.values_mut() {
            reducer.reconfigure_studies(study_config.clone())?;
        }
        self.study_config = study_config;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum LocalMarketError {
    #[error("symbol must be canonical BASE/USDT or BASE/USDC")]
    InvalidSymbol,
    #[error("public market binding is outside the approved Binance LIVE USD-M scope")]
    InvalidBinding,
    #[error("market generation must be positive")]
    InvalidGeneration,
    #[error("market generation is exhausted")]
    GenerationExhausted,
    #[error("market result belongs to a generation that has not been selected")]
    FutureGeneration,
    #[error("market result does not match the active selection and interval")]
    ScopeMismatch,
    #[error("exchange event time is later than local receive time")]
    EventFromFuture,
    #[error("bar is invalid or not aligned to the selected interval")]
    InvalidBar,
    #[error("book level contains a non-positive price or quantity")]
    InvalidBookLevel,
    #[error("best bid must be below best ask")]
    CrossedBook,
    #[error("best bid and ask must be positive and uncrossed")]
    InvalidBbo,
    #[error("trade identity, time, price, or quantity is invalid")]
    InvalidTrade,
    #[error("local chart indicator rejected market data: {0}")]
    Indicator(#[from] ChartIndicatorError),
}

fn validate_study_bar(
    bar: &PublicBar,
    selection: &MarketSelection,
) -> Result<(), LocalMarketError> {
    if bar.symbol != selection.binding.symbol
        || bar.generation == 0
        || bar.interval_ms != selection.interval.duration_ms()
        || !bar.is_valid()
    {
        return Err(LocalMarketError::InvalidBar);
    }
    Ok(())
}

fn ui_bar_from_public(bar: &PublicBar) -> Result<UiBar, LocalMarketError> {
    let FieldState::Known(volume) = bar.base_volume else {
        return Err(LocalMarketError::InvalidBar);
    };
    Ok(UiBar {
        open_time_ms: bar.open_time_ms,
        open: bar.open.value(),
        high: bar.high.value(),
        low: bar.low.value(),
        close: bar.close.value(),
        volume,
    })
}

fn study_point(values: ChartStudyValues) -> ChartStudyPoint {
    let (bollinger_upper, bollinger_middle, bollinger_lower) =
        values.bollinger.map_or((None, None, None), |value| {
            (Some(value.upper), Some(value.middle), Some(value.lower))
        });
    let (macd, macd_signal, macd_histogram) = values.macd.map_or((None, None, None), |value| {
        (Some(value.macd), Some(value.signal), Some(value.histogram))
    });
    ChartStudyPoint {
        sma: values.sma,
        ema: values.ema,
        bollinger_upper,
        bollinger_middle,
        bollinger_lower,
        vwap: values.vwap,
        rsi: values.rsi,
        macd,
        macd_signal,
        macd_histogram,
        atr: values.atr,
        ..ChartStudyPoint::default()
    }
}

fn validate_bar(bar: &UiBar, interval: ChartInterval) -> Result<(), LocalMarketError> {
    let positive = bar.open > Decimal::ZERO
        && bar.high > Decimal::ZERO
        && bar.low > Decimal::ZERO
        && bar.close > Decimal::ZERO
        && bar.volume >= Decimal::ZERO;
    let bounds = bar.low <= bar.open
        && bar.low <= bar.close
        && bar.high >= bar.open
        && bar.high >= bar.close
        && bar.low <= bar.high;
    if bar.open_time_ms == 0
        || !bar.open_time_ms.is_multiple_of(interval.duration_ms())
        || !positive
        || !bounds
    {
        return Err(LocalMarketError::InvalidBar);
    }
    Ok(())
}

fn upsert_bar(bars: &mut Vec<UiBar>, bar: UiBar) {
    match bars.binary_search_by_key(&bar.open_time_ms, |existing| existing.open_time_ms) {
        Ok(index) => bars[index] = bar,
        Err(index) => bars.insert(index, bar),
    }
}

fn upsert_study(studies: &mut Vec<ChartStudyPoint>, point: ChartStudyPoint) {
    match studies.binary_search_by_key(&point.open_time_ms, |existing| existing.open_time_ms) {
        Ok(index) => studies[index] = point,
        Err(index) => studies.insert(index, point),
    }
}

fn trim_studies(studies: &mut Vec<ChartStudyPoint>) {
    if studies.len() > MAX_BARS {
        studies.drain(..studies.len() - MAX_BARS);
    }
}

fn trim_bars(bars: &mut Vec<UiBar>, closed_bars: &mut BTreeSet<u64>) {
    if bars.len() > MAX_BARS {
        bars.drain(..bars.len() - MAX_BARS);
    }
    let first_retained = bars.first().map(|bar| bar.open_time_ms);
    if let Some(first_retained) = first_retained {
        closed_bars.retain(|open_time_ms| *open_time_ms >= first_retained);
    } else {
        closed_bars.clear();
    }
}

fn normalize_book_side(
    levels: Vec<UiBookLevel>,
    descending: bool,
) -> Result<Vec<UiBookLevel>, LocalMarketError> {
    let mut by_price = BTreeMap::new();
    for level in levels {
        if level.price <= Decimal::ZERO || level.quantity <= Decimal::ZERO {
            return Err(LocalMarketError::InvalidBookLevel);
        }
        by_price.insert(level.price, level.quantity);
    }
    let mut normalized: Vec<_> = by_price
        .into_iter()
        .map(|(price, quantity)| UiBookLevel { price, quantity })
        .collect();
    if descending {
        normalized.reverse();
    }
    normalized.truncate(MAX_BOOK_LEVELS);
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::AggressorSide;
    use venue_domain::Price;

    fn selection(symbol: &str) -> Result<MarketSelection, LocalMarketError> {
        MarketSelection::binance_usd_m(symbol, ChartInterval::OneMinute)
    }

    fn bar(open_time_ms: u64, close: i64) -> UiBar {
        UiBar {
            open_time_ms,
            open: Decimal::new(close - 1, 0),
            high: Decimal::new(close + 1, 0),
            low: Decimal::new(close - 2, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
        }
    }

    fn study_bar(open_time_ms: u64, close: i64) -> Result<PublicBar, LocalMarketError> {
        let ui = bar(open_time_ms, close);
        let price = |value| Price::new(value).map_err(|_| LocalMarketError::InvalidBar);
        Ok(PublicBar {
            symbol: "BTC/USDT"
                .parse()
                .map_err(|_| LocalMarketError::InvalidSymbol)?,
            generation: 1,
            received_at_ms: open_time_ms + 60_000,
            sequence: open_time_ms / 60_000,
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            interval_ms: 60_000,
            open: price(ui.open)?,
            high: price(ui.high)?,
            low: price(ui.low)?,
            close: price(ui.close)?,
            base_volume: FieldState::Known(ui.volume),
            quote_volume: FieldState::Known(ui.volume * ui.close),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        })
    }

    fn envelope(
        reducer: &LocalMarketReducer,
        event_time_ms: u64,
        payload: MarketPayload,
    ) -> MarketEnvelope {
        MarketEnvelope {
            generation: reducer.view().generation,
            selection: reducer.view().selection.clone(),
            event_time_ms,
            received_ms: event_time_ms + 7,
            payload,
        }
    }

    #[test]
    fn selection_is_fixed_to_canonical_binance_usd_m() -> Result<(), LocalMarketError> {
        assert_eq!(
            selection("BTC/USDT")?.binding.symbol.to_string(),
            "BTC/USDT"
        );
        assert_eq!(
            selection("ETH/USDC")?.binding.symbol.to_string(),
            "ETH/USDC"
        );
        assert_eq!(selection("btcusdt"), Err(LocalMarketError::InvalidSymbol));
        assert_eq!(selection("BTC/USD"), Err(LocalMarketError::InvalidBinding));
        Ok(())
    }

    #[test]
    fn switching_generation_clears_state_and_ignores_old_results() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let old = envelope(
            &reducer,
            120_000,
            MarketPayload::RestHistory {
                bars: vec![study_bar(60_000, 10)?],
            },
        );
        reducer.apply(old.clone())?;
        let generation = reducer.select(MarketSelection::binance_usd_m(
            "ETH/USDT",
            ChartInterval::FiveMinutes,
        )?)?;

        assert_eq!(generation, 2);
        assert!(reducer.view().bars.is_empty());
        assert_eq!(reducer.apply(old)?, ReduceOutcome::IgnoredOldGeneration);
        assert_eq!(
            reducer.view().selection.binding.symbol.to_string(),
            "ETH/USDT"
        );
        Ok(())
    }

    #[test]
    fn rejects_current_generation_with_wrong_scope() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let mut result = envelope(
            &reducer,
            120_000,
            MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        );
        result.selection = selection("ETH/USDT")?;
        assert_eq!(reducer.apply(result), Err(LocalMarketError::ScopeMismatch));
        Ok(())
    }

    #[test]
    fn history_and_tail_upsert_are_sorted_and_closed_never_regresses()
    -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let history = envelope(
            &reducer,
            180_000,
            MarketPayload::RestHistory {
                bars: vec![study_bar(120_000, 12)?, study_bar(60_000, 11)?],
            },
        );
        reducer.apply(history)?;
        let closed_update = envelope(
            &reducer,
            181_000,
            MarketPayload::WsBar {
                bar: bar(120_000, 13),
                study_bar: Box::new(study_bar(120_000, 13)?),
                closed: true,
            },
        );
        reducer.apply(closed_update)?;
        let regressing_update = envelope(
            &reducer,
            182_000,
            MarketPayload::WsBar {
                bar: bar(120_000, 99),
                study_bar: Box::new(study_bar(120_000, 99)?),
                closed: false,
            },
        );
        reducer.apply(regressing_update)?;

        assert_eq!(reducer.view().bars[0].open_time_ms, 60_000);
        assert_eq!(reducer.view().bars[1].close, Decimal::new(13, 0));
        assert_eq!(reducer.view().last, Some(Decimal::new(13, 0)));
        Ok(())
    }

    #[test]
    fn indicator_reconfiguration_rebuilds_closed_history_without_resubscribing()
    -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let bars = (1_u64..=5)
            .map(|index| study_bar(index * 60_000, 100 + index as i64))
            .collect::<Result<Vec<_>, _>>()?;
        let history = envelope(&reducer, 360_000, MarketPayload::RestHistory { bars });
        reducer.apply(history)?;
        let generation = reducer.view().generation;
        assert!(
            reducer
                .view()
                .studies
                .last()
                .is_some_and(|point| point.sma.is_none())
        );

        let configuration = ChartStudyConfig {
            sma_period: 2,
            ..ChartStudyConfig::default()
        };
        reducer.reconfigure_studies(configuration)?;

        assert_eq!(reducer.view().generation, generation);
        assert_eq!(reducer.view().bars.len(), 5);
        assert!(
            reducer
                .view()
                .studies
                .last()
                .is_some_and(|point| point.sma.is_some())
        );
        Ok(())
    }

    #[test]
    fn bars_are_bounded_to_newest_two_thousand() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let bars = (1_u64..=2_010)
            .map(|index| study_bar(index * 60_000, 10))
            .collect::<Result<Vec<_>, _>>()?;
        let result = envelope(
            &reducer,
            2_011 * 60_000,
            MarketPayload::RestHistory { bars },
        );
        reducer.apply(result)?;
        assert_eq!(reducer.view().bars.len(), MAX_BARS);
        assert_eq!(reducer.view().bars[0].open_time_ms, 11 * 60_000);
        Ok(())
    }

    #[test]
    fn book_is_deduplicated_sorted_and_bounded() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let bids = (1_i64..=25)
            .map(|price| UiBookLevel {
                price: Decimal::new(price, 0),
                quantity: Decimal::new(price, 0),
            })
            .chain(std::iter::once(UiBookLevel {
                price: Decimal::new(25, 0),
                quantity: Decimal::new(99, 0),
            }))
            .collect();
        let asks = (30_i64..=55)
            .map(|price| UiBookLevel {
                price: Decimal::new(price, 0),
                quantity: Decimal::new(1, 0),
            })
            .collect();
        let result = envelope(&reducer, 60_000, MarketPayload::BookSnapshot { bids, asks });
        reducer.apply(result)?;
        assert_eq!(reducer.view().bids.len(), MAX_BOOK_LEVELS);
        assert_eq!(reducer.view().asks.len(), MAX_BOOK_LEVELS);
        assert_eq!(reducer.view().bids[0].price, Decimal::new(25, 0));
        assert_eq!(reducer.view().bids[0].quantity, Decimal::new(99, 0));
        assert_eq!(reducer.view().asks[0].price, Decimal::new(30, 0));
        assert_eq!(reducer.view().bid, Some(Decimal::new(25, 0)));
        assert_eq!(reducer.view().ask, Some(Decimal::new(30, 0)));
        Ok(())
    }

    #[test]
    fn trades_are_deduplicated_sorted_and_bounded() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        for index in (1_u64..=205).rev() {
            let result = envelope(
                &reducer,
                index,
                MarketPayload::Trade(UiTrade {
                    trade_id: format!("trade-{index:03}"),
                    occurred_ms: index,
                    price: Decimal::new(10, 0),
                    quantity: Decimal::new(1, 0),
                    aggressor: AggressorSide::Buy,
                }),
            );
            reducer.apply(result)?;
        }
        let duplicate = envelope(
            &reducer,
            205,
            MarketPayload::Trade(reducer.view().trades[0].clone()),
        );
        reducer.apply(duplicate)?;

        assert_eq!(reducer.view().trades.len(), MAX_TRADES);
        assert_eq!(reducer.view().trades[0].occurred_ms, 6);
        assert_eq!(reducer.view().trades[199].occurred_ms, 205);
        Ok(())
    }

    #[test]
    fn tracks_latency_and_marks_live_feed_stale() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let live = envelope(
            &reducer,
            1_000,
            MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        );
        reducer.apply(live)?;
        assert_eq!(reducer.view().latency_ms, Some(7));
        reducer.refresh_staleness(6_008, 5_000);
        assert_eq!(reducer.view().status, MarketStatus::Stale);
        assert_eq!(
            reducer.view().status_detail.as_deref(),
            Some("market event timeout")
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_market_values() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let crossed = envelope(
            &reducer,
            60_000,
            MarketPayload::BookSnapshot {
                bids: vec![UiBookLevel {
                    price: Decimal::new(11, 0),
                    quantity: Decimal::ONE,
                }],
                asks: vec![UiBookLevel {
                    price: Decimal::new(10, 0),
                    quantity: Decimal::ONE,
                }],
            },
        );
        assert_eq!(reducer.apply(crossed), Err(LocalMarketError::CrossedBook));
        Ok(())
    }

    #[test]
    fn store_reuses_identical_subscriptions_and_fences_replaced_sets()
    -> Result<(), LocalMarketError> {
        let btc = selection("BTC/USDT")?;
        let eth = selection("ETH/USDT")?;
        let mut store = LocalMarketStore::default();
        assert_eq!(store.replace([btc.clone(), eth.clone()])?, Some(1));
        assert_eq!(store.replace([eth.clone(), btc.clone()])?, None);

        let old = MarketEnvelope {
            generation: 1,
            selection: btc.clone(),
            event_time_ms: 60_000,
            received_ms: 60_007,
            payload: MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        };
        assert_eq!(store.apply(old.clone())?, ReduceOutcome::Applied);
        assert_eq!(store.replace([eth])?, Some(2));
        assert_eq!(store.apply(old)?, ReduceOutcome::IgnoredOldGeneration);
        assert!(store.view(&btc).is_none());
        Ok(())
    }
}
