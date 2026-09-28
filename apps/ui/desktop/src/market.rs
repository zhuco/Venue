use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use venue_control_protocol::{UiBar, UiBookLevel, UiTrade};
use venue_domain::{FieldState, MarkFunding, OpenInterestSample, PublicBar};
use venue_gateway_api::PublicMarketBinding;
use venue_indicators::chart::{
    ChartIndicatorError, ChartStudyConfig, ChartStudyEngine, ChartStudyValues,
};

use crate::chart::{BaseMinuteStudy, ChartInterval, ChartStudyPoint};

pub const MAX_BARS: usize = 10_000;
// A 200k-bar 1m source can retain about four months of deliberately requested history.
// It is separate from the displayed-candle limit and uses compact studies.
pub const MAX_BASE_MINUTE_BARS: usize = 200_000;
// Leave room within the 256 MiB indicator budget for chart results and heatmap workers.
const MAX_BASE_MINUTE_CACHE_BYTES: usize = 124 * 1024 * 1024;
const MAX_SESSION_DAY_CACHE_BINDINGS: usize = 8;
pub const MAX_TRADES: usize = 200;
pub const MAX_BOOK_LEVELS: usize = 20;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MarketSelection {
    pub binding: PublicMarketBinding,
    pub interval: ChartInterval,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistoryRequest {
    pub generation: u64,
    pub selection: MarketSelection,
    pub before: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SharedHistoryRequest {
    pub request_id: u64,
    pub generation: u64,
    pub binding: PublicMarketBinding,
    pub interval: ChartInterval,
    pub before: u64,
    /// The last known bar before an interior gap. Prefix requests leave this empty.
    pub gap_after: Option<u64>,
}

pub(crate) fn history_page_covers_gap(bars: &[PublicBar], before: u64, gap_after: Option<u64>) -> bool {
    gap_after.is_none_or(|left| bars.iter()
        .any(|bar| bar.open_time_ms > left && bar.open_time_ms < before))
}

impl MarketSelection {
    pub fn for_server(
        server: crate::model::MarketServer,
        symbol: &str,
        interval: ChartInterval,
    ) -> Result<Self, LocalMarketError> {
        let mut selection = Self::binance_usd_m(symbol, interval)?;
        selection.binding.venue = server.venue();
        selection.validate()?;
        Ok(selection)
    }
    pub fn binance_usd_m(symbol: &str, interval: ChartInterval) -> Result<Self, LocalMarketError> {
        let symbol = symbol
            .parse()
            .map_err(|_| LocalMarketError::InvalidSymbol)?;
        let binding = PublicMarketBinding::binance_usds_m(symbol)
            .map_err(|_| LocalMarketError::InvalidBinding)?;
        Ok(Self { binding, interval })
    }

    pub fn validate(&self) -> Result<(), LocalMarketError> {
        // The Binance parser retains its narrower binding contract. Display subscriptions may
        // use another adapter, while keeping the same LIVE linear stablecoin product boundary.
        let mut binding = self.binding.clone();
        binding.venue = venue_gateway_api::VenueId::Binance;
        binding
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
    ConnectionRtt {
        millis: u64,
        measured_at: std::time::Instant,
    },
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
    Funding(MarkFunding),
    OpenInterestCurrent(OpenInterestSample),
    OpenInterestHistory(Vec<OpenInterestSample>),
    OpenInterestUnavailable(String),
    OpenInterestHistoryUnavailable(String),
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
    pub revision: u64,
    pub bar_revision: u64,
    history_started_ms: u64,
    pub history_loading: bool,
    pub history_exhausted: bool,
    pub history_error: Option<String>,
    pub generation: u64,
    pub selection: MarketSelection,
    pub status: MarketStatus,
    pub status_detail: Option<String>,
    pub bars: Vec<UiBar>,
    pub studies: Vec<ChartStudyPoint>,
    pub study_error: Option<String>,
    pub bids: Vec<UiBookLevel>,
    pub asks: Vec<UiBookLevel>,
    pub trades: Vec<UiTrade>,
    pub funding: Option<MarkFunding>,
    pub open_interest_current: Option<OpenInterestSample>,
    pub open_interest_history: Vec<OpenInterestSample>,
    pub open_interest_error: Option<String>,
    pub open_interest_history_error: Option<String>,
    pub last: Option<Decimal>,
    pub last_price_event_ms: Option<u64>,
    pub last_price_received_ms: Option<u64>,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub book_event_ms: Option<u64>,
    pub book_received_ms: Option<u64>,
    pub depth_event_ms: Option<u64>,
    pub depth_received_ms: Option<u64>,
    pub last_event_ms: Option<u64>,
    pub last_received_ms: Option<u64>,
    pub latency_ms: Option<u64>,
    pub connection_rtt_ms: Option<u64>,
    connection_rtt_at: Option<std::time::Instant>,
}

impl LocalMarketView {
    pub fn recent_rtt_ms(&self) -> Option<u64> {
        self.connection_rtt_at
            .filter(|at| at.elapsed() <= std::time::Duration::from_secs(45))
            .and(self.connection_rtt_ms)
    }

    fn empty(generation: u64, selection: MarketSelection) -> Self {
        Self {
            revision: 0,
            bar_revision: 0,
            history_started_ms: 0,
            history_loading: false,
            history_exhausted: false,
            history_error: None,
            generation,
            selection,
            status: MarketStatus::LoadingHistory,
            status_detail: None,
            bars: Vec::new(),
            studies: Vec::new(),
            study_error: None,
            bids: Vec::new(),
            asks: Vec::new(),
            trades: Vec::new(),
            funding: None,
            open_interest_current: None,
            open_interest_history: Vec::new(),
            open_interest_error: None,
            open_interest_history_error: None,
            last: None,
            last_price_event_ms: None,
            last_price_received_ms: None,
            bid: None,
            ask: None,
            book_event_ms: None,
            book_received_ms: None,
            depth_event_ms: None,
            depth_received_ms: None,
            last_event_ms: None,
            last_received_ms: None,
            latency_ms: None,
            connection_rtt_ms: None,
            connection_rtt_at: None,
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
    forming_bar: Option<PublicBar>,
    last_price_event_ms: u64,
    last_bar_event_ms: u64,
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
            forming_bar: None,
            last_price_event_ms: 0,
            last_bar_event_ms: 0,
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
        self.forming_bar = None;
        self.last_price_event_ms = 0;
        self.last_bar_event_ms = 0;
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
        if let MarketPayload::ConnectionRtt {
            millis,
            measured_at,
        } = &envelope.payload
        {
            self.view.revision = self.view.revision.wrapping_add(1);
            self.view.connection_rtt_ms = Some(*millis);
            self.view.connection_rtt_at = Some(*measured_at);
            return Ok(ReduceOutcome::Applied);
        }
        if envelope.event_time_ms > envelope.received_ms {
            return Err(LocalMarketError::EventFromFuture);
        }

        let exchange_event = !matches!(
            &envelope.payload,
            MarketPayload::RestHistory { .. } | MarketPayload::Status { .. }
                | MarketPayload::Funding(_) | MarketPayload::OpenInterestCurrent(_)
                | MarketPayload::OpenInterestHistory(_) | MarketPayload::OpenInterestUnavailable(_)
                | MarketPayload::OpenInterestHistoryUnavailable(_)
        );
        let previous_price_event_ms = self.last_price_event_ms;
        match envelope.payload {
            MarketPayload::ConnectionRtt { .. } => {}
            MarketPayload::RestHistory { bars } => self.apply_history(bars)?,
            MarketPayload::WsBar {
                bar,
                study_bar,
                closed,
            } => self.apply_bar(bar, *study_bar, closed, envelope.event_time_ms)?,
            MarketPayload::BookSnapshot { bids, asks } => {
                if self
                    .view
                    .depth_event_ms
                    .is_some_and(|time| time > envelope.event_time_ms)
                {
                    return Ok(ReduceOutcome::Applied);
                }
                self.apply_book(bids, asks)?;
                self.view.depth_event_ms = Some(envelope.event_time_ms);
                self.view.depth_received_ms = Some(envelope.received_ms);
                self.apply_best_prices(
                    self.view.bids.first().map(|level| level.price),
                    self.view.asks.first().map(|level| level.price),
                    envelope.event_time_ms,
                    envelope.received_ms,
                )?;
            }
            MarketPayload::Bbo { bid, ask } => {
                self.apply_best_prices(
                    Some(bid),
                    Some(ask),
                    envelope.event_time_ms,
                    envelope.received_ms,
                )?;
            }
            MarketPayload::Trade(trade) => self.apply_trade(trade)?,
            MarketPayload::Funding(funding) => {
                if funding.symbol != self.view.selection.binding.symbol
                    || funding.generation != envelope.generation
                    || funding.exchange_time_ms != envelope.event_time_ms
                    || funding.received_at_ms != envelope.received_ms
                {
                    return Err(LocalMarketError::ScopeMismatch);
                }
                if self.view.funding.as_ref().is_none_or(|previous| {
                    previous.exchange_time_ms <= funding.exchange_time_ms
                }) {
                    self.view.funding = Some(funding);
                }
            }
            MarketPayload::OpenInterestCurrent(sample) => {
                if !sample.is_valid()
                    || sample.symbol != self.view.selection.binding.symbol
                    || sample.generation != envelope.generation
                    || sample.sampling_interval_ms.is_some()
                {
                    return Err(LocalMarketError::ScopeMismatch);
                }
                if self.view.open_interest_current.as_ref().is_none_or(|previous| {
                    previous.exchange_time_ms <= sample.exchange_time_ms
                }) {
                    self.view.open_interest_current = Some(sample);
                    self.view.open_interest_error = None;
                }
            }
            MarketPayload::OpenInterestHistory(samples) => {
                if samples.len() > 500 || samples.iter().any(|sample| {
                    !sample.is_valid()
                        || sample.symbol != self.view.selection.binding.symbol
                        || sample.generation != envelope.generation
                        || sample.sampling_interval_ms != Some(300_000)
                }) || samples.windows(2).any(|pair| pair[0].exchange_time_ms >= pair[1].exchange_time_ms) {
                    return Err(LocalMarketError::ScopeMismatch);
                }
                if self.view.open_interest_history.last().is_none_or(|previous| {
                    samples.last().is_some_and(|latest| latest.exchange_time_ms >= previous.exchange_time_ms)
                }) {
                    self.view.open_interest_history = samples;
                    self.view.open_interest_history_error = None;
                }
            }
            MarketPayload::OpenInterestUnavailable(detail) => {
                self.view.open_interest_error = Some(detail);
            }
            MarketPayload::OpenInterestHistoryUnavailable(detail) => {
                self.view.open_interest_history_error = Some(detail);
            }
            MarketPayload::Status { status, detail } => {
                if status != MarketStatus::Live {
                    self.view.connection_rtt_ms = None;
                    self.view.connection_rtt_at = None;
                }
                self.view.status = status;
                self.view.status_detail = detail;
            }
        }

        if exchange_event
            && self.view.status == MarketStatus::Stale
            && envelope.received_ms.saturating_sub(envelope.event_time_ms) <= 5_000
        {
            self.view.status = MarketStatus::Live;
            self.view.status_detail = None;
        }

        // Book updates and status heartbeats cannot make an old traded price fresh.
        if exchange_event && self.last_price_event_ms > previous_price_event_ms {
            self.view.last_price_event_ms = Some(self.last_price_event_ms);
            self.view.last_price_received_ms = Some(envelope.received_ms);
        }
        self.view.revision = self.view.revision.wrapping_add(1);
        self.view.last_event_ms = Some(envelope.event_time_ms);
        if exchange_event || self.view.last_received_ms.is_none() {
            self.view.last_received_ms = Some(envelope.received_ms);
        }
        if exchange_event {
            self.view.latency_ms = Some(envelope.received_ms - envelope.event_time_ms);
        }
        Ok(ReduceOutcome::Applied)
    }

    pub fn refresh_staleness(&mut self, now_ms: u64, stale_after_ms: u64) {
        if self.view.status != MarketStatus::Live {
            return;
        }
        let Some(last_received_ms) = self.view.last_received_ms else {
            self.view.revision = self.view.revision.wrapping_add(1);
            self.view.status = MarketStatus::Stale;
            self.view.status_detail = Some("no market event received".to_owned());
            return;
        };
        if now_ms.saturating_sub(last_received_ms) > stale_after_ms {
            self.view.revision = self.view.revision.wrapping_add(1);
            self.view.status = MarketStatus::Stale;
            self.view.status_detail = Some("market event timeout".to_owned());
        }
    }

    fn apply_history(&mut self, mut bars: Vec<PublicBar>) -> Result<(), LocalMarketError> {
        bars.sort_by_key(|bar| bar.open_time_ms);
        if bars.windows(2).any(|pair| pair[0].open_time_ms == pair[1].open_time_ms) {
            return Err(LocalMarketError::InvalidBar);
        }
        for bar in &bars { validate_study_bar(bar, &self.view.selection, self.view.generation)?; }
        let same_values = |old: &PublicBar, next: &PublicBar| {
            old.symbol == next.symbol && old.open_time_ms == next.open_time_ms
                && old.close_time_ms == next.close_time_ms && old.interval_ms == next.interval_ms
                && old.open == next.open && old.high == next.high && old.low == next.low
                && old.close == next.close && old.base_volume == next.base_volume
                && old.quote_volume == next.quote_volume && old.trade_count == next.trade_count
                && old.taker_buy_base_volume == next.taker_buy_base_volume
                && old.taker_buy_quote_volume == next.taker_buy_quote_volume
        };
        if !bars.is_empty() && bars.iter().all(|bar| self.closed_facts.get(&bar.open_time_ms)
            .is_some_and(|old| same_values(old, bar))) { return Ok(()); }
        if let (Some((old_first, _)), Some((old_last, _)), Some(new_first), Some(new_last)) =
            (self.closed_facts.first_key_value(), self.closed_facts.last_key_value(),
                bars.first(), bars.last()) {
            let step = self.view.selection.interval.duration_ms();
            if new_first.open_time_ms <= old_last.saturating_add(step)
                && *old_first <= new_last.open_time_ms.saturating_add(step) {
                let mut merged = self.closed_facts.clone();
                for bar in bars { merged.insert(bar.open_time_ms, bar); }
                bars = merged.into_values().rev().take(MAX_BARS).collect::<Vec<_>>()
                    .into_iter().rev().collect();
            }
        }
        self.forming_bar = None;
        self.view.bars.clear();
        self.view.studies.clear();
        self.closed_bars.clear();
        self.closed_facts.clear();
        self.studies.reset();
        for bar in bars {
            let ui_bar = ui_bar_from_public(&bar)?;
            let point = self.study_closed_or_empty(&bar)?;
            self.closed_bars.insert(bar.open_time_ms);
            self.closed_facts.insert(bar.open_time_ms, bar);
            upsert_bar(&mut self.view.bars, ui_bar.clone());
            upsert_study(
                &mut self.view.studies,
                ChartStudyPoint {
                    open_time_ms: ui_bar.open_time_ms,
                    confirmed: true,
                    ..point
                },
            );
        }
        trim_bars(&mut self.view.bars, &mut self.closed_bars);
        trim_studies(&mut self.view.studies);
        while self.closed_facts.len() > MAX_BARS {
            self.closed_facts.pop_first();
        }
        self.view.last = self.view.bars.last().map(|bar| bar.close);
        self.view.last_price_event_ms = None;
        self.view.last_price_received_ms = None;
        self.last_price_event_ms = self
            .closed_facts
            .last_key_value()
            .map_or(0, |(_, bar)| bar.close_time_ms);
        self.last_bar_event_ms = 0;
        self.view.bar_revision = self.view.bar_revision.wrapping_add(1);
        Ok(())
    }

    fn apply_bar(
        &mut self,
        mut bar: UiBar,
        mut study_bar: PublicBar,
        closed: bool,
        event_time_ms: u64,
    ) -> Result<(), LocalMarketError> {
        validate_bar(&bar, self.view.selection.interval)?;
        validate_study_bar(&study_bar, &self.view.selection, self.view.generation)?;
        if bar.open_time_ms != study_bar.open_time_ms {
            return Err(LocalMarketError::ScopeMismatch);
        }
        if self.closed_bars.contains(&bar.open_time_ms) && !closed {
            return Ok(());
        }
        if !closed && event_time_ms < self.last_bar_event_ms {
            return Ok(());
        }
        if !closed {
            self.last_bar_event_ms = event_time_ms;
            // Kline and aggregate trades arrive independently. Preserve newer prints in
            // the forming preview without counting their volume a second time.
            for trade in &self.view.trades {
                if trade.occurred_ms > event_time_ms
                    && trade.occurred_ms >= bar.open_time_ms
                    && trade.occurred_ms
                        < bar
                            .open_time_ms
                            .saturating_add(self.view.selection.interval.duration_ms())
                {
                    apply_print_to_bar(&mut study_bar, trade.price)?;
                }
            }
            bar = ui_bar_from_public(&study_bar)?;
        }
        if event_time_ms >= self.last_price_event_ms
            && self
                .view
                .bars
                .last()
                .is_none_or(|latest| bar.open_time_ms >= latest.open_time_ms)
        {
            self.view.last = Some(bar.close);
            self.last_price_event_ms = event_time_ms;
        }
        if closed {
            if self
                .forming_bar
                .as_ref()
                .is_some_and(|forming| forming.open_time_ms <= study_bar.open_time_ms)
            {
                self.forming_bar = None;
            }
            if let Some(existing) = self.closed_facts.get(&bar.open_time_ms) {
                if existing != &study_bar {
                    self.closed_facts.insert(bar.open_time_ms, study_bar);
                    return self.rebuild_studies_and_bars();
                }
            } else {
                let point = self.study_closed_or_empty(&study_bar)?;
                upsert_study(
                    &mut self.view.studies,
                    ChartStudyPoint {
                        open_time_ms: bar.open_time_ms,
                        confirmed: true,
                        ..point
                    },
                );
                self.closed_facts.insert(bar.open_time_ms, study_bar);
            }
            self.closed_bars.insert(bar.open_time_ms);
        } else {
            self.forming_bar = Some(study_bar.clone());
            let point = self.study_preview_or_empty(&study_bar)?;
            upsert_study(
                &mut self.view.studies,
                ChartStudyPoint {
                    open_time_ms: bar.open_time_ms,
                    confirmed: false,
                    ..point
                },
            );
        }
        upsert_bar(&mut self.view.bars, bar);
        if closed { self.view.bar_revision = self.view.bar_revision.wrapping_add(1); }
        while self.closed_facts.len() > MAX_BARS {
            self.closed_facts.pop_first();
        }
        trim_bars(&mut self.view.bars, &mut self.closed_bars);
        trim_studies(&mut self.view.studies);
        // A late close changes the base of the next candle's preview, including its signals.
        if closed && let Some(forming) = self.forming_bar.clone() {
            self.apply_bar(
                ui_bar_from_public(&forming)?,
                forming,
                false,
                self.last_bar_event_ms,
            )?;
        }
        Ok(())
    }

    fn rebuild_studies_and_bars(&mut self) -> Result<(), LocalMarketError> {
        self.view.bar_revision = self.view.bar_revision.wrapping_add(1);
        let live_price = self.view.last;
        self.studies = ChartStudyEngine::with_config(&self.study_config)
            .map_err(LocalMarketError::Indicator)?;
        self.view.studies.clear();
        self.view.bars.clear();
        self.closed_bars.clear();
        for bar in self.closed_facts.values().cloned().collect::<Vec<_>>() {
            let point = self.study_closed_or_empty(&bar)?;
            let ui_bar = ui_bar_from_public(&bar)?;
            self.closed_bars.insert(bar.open_time_ms);
            self.view.bars.push(ui_bar);
            self.view.studies.push(ChartStudyPoint {
                open_time_ms: bar.open_time_ms,
                confirmed: true,
                ..point
            });
        }
        self.view.last = self.view.bars.last().map(|bar| bar.close);
        trim_bars(&mut self.view.bars, &mut self.closed_bars);
        trim_studies(&mut self.view.studies);
        if let Some(forming) = self.forming_bar.clone() {
            self.apply_bar(
                ui_bar_from_public(&forming)?,
                forming,
                false,
                self.last_bar_event_ms,
            )?;
        }
        self.view.last = live_price.or(self.view.last);
        Ok(())
    }

    fn study_closed_or_empty(&mut self, bar: &PublicBar) -> Result<ChartStudyPoint, LocalMarketError> {
        match self.studies.ingest_closed(bar) {
            Ok(values) => {
                self.view.study_error = None;
                Ok(study_point(values))
            }
            Err(ChartIndicatorError::DiscontinuousBar) => Err(LocalMarketError::Indicator(ChartIndicatorError::DiscontinuousBar)),
            Err(error) => {
                self.view.study_error = Some(error.to_string());
                self.studies = ChartStudyEngine::with_config(&self.study_config)
                    .map_err(LocalMarketError::Indicator)?;
                Ok(ChartStudyPoint::default())
            }
        }
    }

    fn study_preview_or_empty(&mut self, bar: &PublicBar) -> Result<ChartStudyPoint, LocalMarketError> {
        match self.studies.preview(bar) {
            Ok(values) => {
                self.view.study_error = None;
                Ok(study_point(values))
            }
            Err(ChartIndicatorError::DiscontinuousBar) => Err(LocalMarketError::Indicator(ChartIndicatorError::DiscontinuousBar)),
            Err(error) => {
                self.view.study_error = Some(error.to_string());
                Ok(ChartStudyPoint::default())
            }
        }
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
        self.view.revision = self.view.revision.wrapping_add(1);
        self.rebuild_studies_and_bars()
    }

    fn apply_best_prices(
        &mut self,
        bid: Option<Decimal>,
        ask: Option<Decimal>,
        event: u64,
        received: u64,
    ) -> Result<(), LocalMarketError> {
        if bid.is_some_and(|price| price <= Decimal::ZERO)
            || ask.is_some_and(|price| price <= Decimal::ZERO)
            || bid.zip(ask).is_some_and(|(bid, ask)| bid >= ask)
        {
            return Err(LocalMarketError::InvalidBbo);
        }
        if self
            .view
            .book_event_ms
            .is_none_or(|previous| event >= previous)
        {
            self.view.bid = bid;
            self.view.ask = ask;
            self.view.book_event_ms = Some(event);
            self.view.book_received_ms = Some(received);
        }
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
        if trade.occurred_ms >= self.last_price_event_ms {
            if let Some(mut forming) = self.forming_bar.clone()
                && trade.occurred_ms >= forming.open_time_ms
                && trade.occurred_ms <= forming.close_time_ms
            {
                apply_print_to_bar(&mut forming, trade.price)?;
                let point = self.study_preview_or_empty(&forming)?;
                upsert_bar(&mut self.view.bars, ui_bar_from_public(&forming)?);
                upsert_study(
                    &mut self.view.studies,
                    ChartStudyPoint {
                        open_time_ms: forming.open_time_ms,
                        confirmed: false,
                        ..point
                    },
                );
                self.forming_bar = Some(forming);
            }
            self.view.last = Some(trade.price);
            self.last_price_event_ms = trade.occurred_ms;
        }
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
    chart_reducers: Vec<LocalMarketReducer>,
    chart_bindings: BTreeMap<String, (MarketSelection, ChartStudyConfig)>,
    chart_previews: std::collections::VecDeque<(MarketSelection, Vec<UiBar>)>,
    base_minutes: BTreeMap<PublicMarketBinding, BaseMinuteSeries>,
    base_minute_tick: u64,
    session_days: BTreeMap<PublicMarketBinding, SessionDaySeries>,
    session_day_tick: u64,
    shared_history_pending: BTreeMap<(PublicMarketBinding, ChartInterval), u64>,
    next_shared_history_request_id: u64,
    shared_history_retry_after: BTreeMap<(PublicMarketBinding, ChartInterval), u64>,
    shared_history_exhausted_at: BTreeSet<(PublicMarketBinding, ChartInterval, u64)>,
}

#[derive(Clone, Debug, Default)]
struct SessionDaySeries {
    bars: Vec<PublicBar>,
    confirmed: BTreeSet<u64>,
    last_used_tick: u64,
}

#[derive(Clone, Debug, Default)]
struct BaseMinuteSeries {
    bars: Vec<UiBar>,
    studies: Vec<BaseMinuteStudy>,
    facts: Vec<PublicBar>,
    gap_right_edges: BTreeSet<u64>,
    closed_revision: u64,
    incarnation: u64,
    last_used_tick: u64,
}

impl BaseMinuteSeries {
    fn memory_bytes(&self) -> usize {
        self.bars.capacity() * std::mem::size_of::<UiBar>()
            + self.studies.capacity() * std::mem::size_of::<BaseMinuteStudy>()
            + self.facts.capacity() * std::mem::size_of::<PublicBar>()
            + self.facts.len() * 64 // Conservative allowance for symbol strings.
            + self.gap_right_edges.len() * 48
    }

    fn prepare(bar: PublicBar, confirmed: bool)
    -> Result<(PublicBar, UiBar, BaseMinuteStudy), LocalMarketError> {
        let ui_bar = ui_bar_from_public(&bar)?;
        let point = BaseMinuteStudy {
            confirmed,
            bar_vwap: venue_indicators::chart::bar_vwap(&bar),
        };
        Ok((bar, ui_bar, point))
    }

    fn insert(&mut self, bar: PublicBar, confirmed: bool) -> Result<(), LocalMarketError> {
        let (bar, ui_bar, point) = Self::prepare(bar, confirmed)?;
        self.insert_prepared(bar, ui_bar, point);
        Ok(())
    }

    fn insert_prepared(&mut self, bar: PublicBar, ui_bar: UiBar, point: BaseMinuteStudy) {
        let open_time_ms = bar.open_time_ms;
        let confirmed = point.confirmed;
        let mut closed_changed = confirmed;
        match self.bars.binary_search_by_key(&open_time_ms, |item| item.open_time_ms) {
            Ok(index) if self.studies[index].confirmed && !confirmed => return,
            Ok(index) => {
                closed_changed = confirmed && (self.bars[index] != ui_bar
                    || self.studies[index].bar_vwap != point.bar_vwap
                    || !self.studies[index].confirmed
                    || self.facts[index].base_volume != bar.base_volume
                    || self.facts[index].quote_volume != bar.quote_volume
                    || self.facts[index].taker_buy_base_volume != bar.taker_buy_base_volume
                    || self.facts[index].taker_buy_quote_volume != bar.taker_buy_quote_volume);
                self.bars[index] = ui_bar;
                self.studies[index] = point;
                self.facts[index] = bar;
            }
            Err(index) => {
                let previous = index.checked_sub(1).and_then(|previous| self.facts.get(previous))
                    .map(|previous| previous.open_time_ms);
                let next = self.facts.get(index).map(|next| next.open_time_ms);
                if let Some(next) = next { self.gap_right_edges.remove(&next); }
                if previous.is_some_and(|previous| previous.saturating_add(60_000) < open_time_ms) {
                    self.gap_right_edges.insert(open_time_ms);
                }
                if let Some(next) = next
                    && open_time_ms.saturating_add(60_000) < next {
                    self.gap_right_edges.insert(next);
                }
                self.bars.insert(index, ui_bar);
                self.studies.insert(index, point);
                self.facts.insert(index, bar);
            }
        }
        if closed_changed { self.closed_revision = self.closed_revision.saturating_add(1); }
        if self.bars.len() > MAX_BASE_MINUTE_BARS {
            let extra = self.bars.len() - MAX_BASE_MINUTE_BARS;
            self.bars.drain(..extra);
            self.studies.drain(..extra);
            self.facts.drain(..extra);
            if let Some(first) = self.facts.first() {
                self.gap_right_edges.retain(|right| *right > first.open_time_ms);
            }
        }
    }
}

impl LocalMarketStore {
    pub(crate) fn retained_study_source_bytes(&self) -> usize {
        let minute = self.base_minutes.values().fold(0_usize, |total, series|
            total.saturating_add(series.memory_bytes()));
        self.session_days.values().fold(minute, |total, series| {
            total.saturating_add(series.bars.capacity() * std::mem::size_of::<PublicBar>())
                .saturating_add(series.bars.len() * 64)
                .saturating_add(series.confirmed.len() * 48)
        })
    }

    pub fn chart_preview(&self, selection: &MarketSelection) -> Option<&[UiBar]> {
        self.chart_previews
            .iter()
            .find(|(key, _)| key == selection)
            .map(|(_, bars)| bars.as_slice())
    }
    pub fn begin_history(
        &mut self,
        selection: &MarketSelection,
        retry: bool,
    ) -> Option<HistoryRequest> {
        let view = &mut self.reducers.get_mut(selection)?.view;
        let now = crate::account_center::now_ms();
        if view.history_loading && now.saturating_sub(view.history_started_ms) > 15_000 {
            view.history_loading = false;
            view.history_error = Some("History timed out; retry manually".into());
        }
        if view.history_loading
            || view.history_exhausted
            || (!retry && view.history_error.is_some())
            || view.bars.len() >= MAX_BARS
        {
            return None;
        }
        let before = view.bars.first()?.open_time_ms;
        if before == 0 {
            return None;
        }
        view.revision = view.revision.wrapping_add(1);
        view.history_loading = true;
        view.history_started_ms = now;
        view.history_error = None;
        Some(HistoryRequest {
            generation: self.generation,
            selection: selection.clone(),
            before,
        })
    }

    pub fn finish_history(
        &mut self,
        request: &HistoryRequest,
        result: Result<Vec<PublicBar>, String>,
    ) -> Result<usize, LocalMarketError> {
        if request.generation != self.generation {
            return Ok(0);
        }
        let Some(base) = self.reducers.get_mut(&request.selection) else {
            return Ok(0);
        };
        base.view.revision = base.view.revision.wrapping_add(1);
        base.view.history_loading = false;
        let bars = match result {
            Ok(bars) => bars,
            Err(error) => {
                base.view.history_error = Some(error);
                return Ok(0);
            }
        };
        if base
            .view
            .bars
            .first()
            .is_none_or(|bar| bar.open_time_ms != request.before)
        {
            return Ok(0);
        }
        if bars.is_empty() {
            base.view.history_exhausted = true;
            return Ok(0);
        }
        let mut candidate = base.clone();
        let mut added = 0;
        for bar in bars {
            validate_study_bar(&bar, &request.selection, request.generation)?;
            if bar.open_time_ms >= request.before {
                return Err(LocalMarketError::InvalidBar);
            }
            if candidate
                .closed_facts
                .insert(bar.open_time_ms, bar)
                .is_none()
            {
                added += 1;
            }
        }
        if candidate.closed_facts.len() + usize::from(candidate.forming_bar.is_some()) > MAX_BARS {
            base.view.history_exhausted = true;
            return Ok(0);
        }
        candidate.rebuild_studies_and_bars()?;
        // Rebuild all chart configurations from the same validated history, then commit together.
        let mut charts = Vec::new();
        for (key, chart) in self
            .chart_reducers
            .iter()
            .enumerate()
            .filter(|(_, chart)| chart.view.selection == request.selection)
        {
            let mut replacement = candidate.clone();
            replacement.reconfigure_studies(chart.study_config.clone())?;
            charts.push((key, replacement));
        }
        *base = candidate;
        for (key, replacement) in charts {
            self.chart_reducers[key] = replacement;
        }
        Ok(added)
    }
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
        if reducers.is_empty() {
            self.chart_previews.clear();
        } else {
            for (selection, reducer) in &self.reducers {
                if reducer.view.bars.is_empty() {
                    continue;
                }
                self.chart_previews.retain(|(key, _)| key != selection);
                let bars = &reducer.view.bars;
                self.chart_previews.push_back((
                    selection.clone(),
                    bars[bars.len().saturating_sub(1000)..].to_vec(),
                ));
            }
            while self.chart_previews.len() > 8 {
                self.chart_previews.pop_front();
            }
        }
        self.generation = generation;
        self.reducers = reducers;
        self.reattach_shared_sources();
        self.shared_history_pending.clear();
        self.shared_history_retry_after.clear();
        self.shared_history_exhausted_at.clear();
        self.chart_reducers.clear();
        self.chart_bindings.clear();
        Ok(Some(generation))
    }

    fn reattach_shared_sources(&mut self) {
        let active = self.reducers.keys().map(|selection| selection.binding.clone())
            .collect::<BTreeSet<_>>();
        if active.is_empty() {
            self.base_minutes.clear();
            self.session_days.clear();
            return;
        }
        for (binding, series) in &mut self.base_minutes {
            if !active.contains(binding) { continue; }
            if series.studies.last().is_some_and(|point| !point.confirmed) {
                if let Some(forming) = series.facts.pop() {
                    series.gap_right_edges.remove(&forming.open_time_ms);
                }
                series.bars.pop();
                series.studies.pop();
            }
            for bar in &mut series.facts { bar.generation = self.generation; }
            self.base_minute_tick = self.base_minute_tick.saturating_add(1);
            series.incarnation = self.base_minute_tick;
            series.last_used_tick = self.base_minute_tick;
        }
        self.base_minutes.retain(|_, series| !series.facts.is_empty());
        for (binding, series) in &mut self.session_days {
            if !active.contains(binding) { continue; }
            series.bars.retain(|bar| series.confirmed.contains(&bar.open_time_ms));
            for bar in &mut series.bars { bar.generation = self.generation; }
            self.session_day_tick = self.session_day_tick.saturating_add(1);
            series.last_used_tick = self.session_day_tick;
        }
        self.session_days.retain(|_, series| !series.bars.is_empty());
        while self.session_days.len() > active.len() + MAX_SESSION_DAY_CACHE_BINDINGS {
            let victim = self.session_days.iter().filter(|(binding, _)| !active.contains(*binding))
                .min_by_key(|(_, series)| series.last_used_tick)
                .map(|(binding, _)| binding.clone());
            let Some(victim) = victim else { break; };
            self.session_days.remove(&victim);
        }
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
        let outcome = reducer.apply(envelope.clone())?;
        for chart in self
            .chart_reducers
            .iter_mut()
            .filter(|chart| chart.view.selection == envelope.selection)
        {
            chart.apply(envelope.clone())?;
        }
        Ok(outcome)
    }

    pub fn configure_chart(
        &mut self,
        key: &str,
        selection: &MarketSelection,
        config: ChartStudyConfig,
    ) -> Result<(), LocalMarketError> {
        config.validate().map_err(LocalMarketError::Indicator)?;
        let Some(base) = self.reducers.get(selection) else {
            self.chart_bindings.remove(key);
            self.prune_chart_reducers();
            return Ok(());
        };
        if base.study_config != config
            && !self
                .chart_reducers
                .iter()
                .any(|chart| &chart.view.selection == selection && chart.study_config == config)
        {
            let mut candidate = base.clone();
            candidate.reconfigure_studies(config.clone())?;
            self.chart_reducers.push(candidate);
        }
        self.chart_bindings
            .insert(key.to_owned(), (selection.clone(), config));
        self.prune_chart_reducers();
        Ok(())
    }

    pub fn retain_chart_keys(&mut self, keys: &BTreeSet<String>) {
        self.chart_bindings.retain(|key, _| keys.contains(key));
        self.prune_chart_reducers();
    }

    fn prune_chart_reducers(&mut self) {
        self.chart_reducers.retain(|chart| {
            self.chart_bindings.values().any(|(selection, config)| {
                selection == &chart.view.selection && config == &chart.study_config
            }) && self
                .reducers
                .get(&chart.view.selection)
                .is_none_or(|base| base.study_config != chart.study_config)
        });
    }

    pub fn chart_view(&self, key: &str) -> Option<&LocalMarketView> {
        let (selection, config) = self.chart_bindings.get(key)?;
        self.reducers
            .get(selection)
            .filter(|base| &base.study_config == config)
            .or_else(|| {
                self.chart_reducers.iter().find(|chart| {
                    &chart.view.selection == selection && &chart.study_config == config
                })
            })
            .map(LocalMarketReducer::view)
    }

    pub fn view(&self, selection: &MarketSelection) -> Option<&LocalMarketView> {
        self.reducers.get(selection).map(LocalMarketReducer::view)
    }

    pub(crate) fn base_minutes(&self, binding: &PublicMarketBinding) -> Option<(&[UiBar], &[BaseMinuteStudy], (u64, u64))> {
        if !self.reducers.keys().any(|selection| &selection.binding == binding) { return None; }
        self.base_minutes.get(binding).map(|series| (series.bars.as_slice(), series.studies.as_slice(), (series.incarnation, series.closed_revision)))
    }

    pub fn base_minute_facts(&self, binding: &PublicMarketBinding) -> Option<&[PublicBar]> {
        if !self.reducers.keys().any(|selection| &selection.binding == binding) { return None; }
        self.base_minutes.get(binding).map(|series| {
            let end = series.facts.len().saturating_sub(usize::from(series.studies.last().is_some_and(|point| !point.confirmed)));
            &series.facts[..end]
        })
    }

    pub fn base_minute_forming_fact(&self, binding: &PublicMarketBinding) -> Option<&PublicBar> {
        if !self.reducers.keys().any(|selection| &selection.binding == binding) { return None; }
        self.base_minutes.get(binding).and_then(|series| {
            series.studies.last().filter(|point| !point.confirmed)?;
            series.facts.last()
        })
    }

    pub fn session_days(&self, binding: &PublicMarketBinding) -> Option<&[PublicBar]> {
        if !self.reducers.keys().any(|selection| &selection.binding == binding) { return None; }
        self.session_days.get(binding).map(|series| series.bars.as_slice())
    }

    pub fn begin_shared_history(&mut self, binding: &PublicMarketBinding,
        interval: ChartInterval, desired_start_ms: u64, desired_end_ms: u64) -> Option<SharedHistoryRequest> {
        if !matches!(interval, ChartInterval::OneMinute | ChartInterval::OneDay)
            || !self.reducers.keys().any(|selection| &selection.binding == binding) { return None; }
        let first = match interval {
            ChartInterval::OneMinute => {
                let series = self.base_minutes.get(binding)?;
                series.facts.first()?.open_time_ms
            }
            ChartInterval::OneDay => {
                let series = self.session_days.get(binding)?;
                if series.bars.len() >= 500 { return None; }
                series.bars.first()?.open_time_ms
            }
            _ => return None,
        };
        let key = (binding.clone(), interval);
        if self.shared_history_pending.contains_key(&key)
            || self.shared_history_retry_after.get(&key).is_some_and(|until| *until > crate::account_center::now_ms())
        { return None; }
        let gap = if interval == ChartInterval::OneMinute && desired_start_ms < desired_end_ms {
            self.base_minutes.get(binding).and_then(|series| {
                series.gap_right_edges.range(desired_start_ms.saturating_add(1)..=desired_end_ms)
                    .find_map(|right| {
                        if self.shared_history_exhausted_at.contains(&(binding.clone(), interval, *right)) { return None; }
                        let index = series.facts.binary_search_by_key(right, |bar| bar.open_time_ms).ok()?;
                        let left = series.facts.get(index.checked_sub(1)?)?.open_time_ms;
                        Some((*right, left))
                    })
            })
        } else { None };
        let (before, gap_after) = if let Some((right, left)) = gap { (right, Some(left)) }
            else if desired_start_ms < first && first > 0
                && (interval != ChartInterval::OneMinute
                    || self.base_minutes.get(binding).is_some_and(|series| series.facts.len() < MAX_BASE_MINUTE_BARS))
                && !self.shared_history_exhausted_at.contains(&(binding.clone(), interval, first))
            { (first, None) } else { return None; };
        self.next_shared_history_request_id = self.next_shared_history_request_id
            .wrapping_add(1).max(1);
        let request_id = self.next_shared_history_request_id;
        self.shared_history_pending.insert(key, request_id);
        Some(SharedHistoryRequest { request_id, generation: self.generation,
            binding: binding.clone(), interval, before, gap_after })
    }

    pub fn cancel_shared_history(&mut self, request: &SharedHistoryRequest) {
        let key = (request.binding.clone(), request.interval);
        if request.generation == self.generation
            && self.shared_history_pending.get(&key) == Some(&request.request_id) {
            self.shared_history_pending.remove(&key);
        }
    }

    pub fn retain_shared_history_demands(
        &mut self, demanded: &BTreeSet<(PublicMarketBinding, ChartInterval)>) {
        self.shared_history_pending.retain(|key, _| demanded.contains(key));
        self.shared_history_retry_after.retain(|key, _| demanded.contains(key));
    }

    pub fn finish_shared_history(&mut self, request: &SharedHistoryRequest,
        result: Result<Vec<PublicBar>, String>) -> Result<usize, LocalMarketError> {
        if request.generation != self.generation { return Ok(0); }
        let key = (request.binding.clone(), request.interval);
        if self.shared_history_pending.get(&key) != Some(&request.request_id) { return Ok(0); }
        self.shared_history_pending.remove(&key);
        let bars = match result {
            Ok(bars) => { self.shared_history_retry_after.remove(&key); bars }
            Err(_) => {
                self.shared_history_retry_after.insert(key, crate::account_center::now_ms().saturating_add(30_000));
                return Ok(0);
            }
        };
        if bars.len() > 500 || bars.iter().any(|bar| bar.open_time_ms >= request.before) {
            self.shared_history_retry_after.insert(key,
                crate::account_center::now_ms().saturating_add(30_000));
            return Err(LocalMarketError::ScopeMismatch);
        }
        if bars.is_empty() {
            self.shared_history_exhausted_at.insert((request.binding.clone(), request.interval, request.before));
            return Ok(0);
        }
        if !history_page_covers_gap(&bars, request.before, request.gap_after) {
            self.shared_history_exhausted_at.insert((request.binding.clone(), request.interval, request.before));
            return Ok(0);
        }
        self.shared_history_exhausted_at.remove(&(request.binding.clone(), request.interval, request.before));
        let added = bars.len();
        let applied = match request.interval {
            ChartInterval::OneMinute => self.apply_base_history(request.generation, request.binding.clone(), bars, None).map(|_| ()),
            ChartInterval::OneDay => self.apply_session_history(request.generation, request.binding.clone(), bars, None).map(|_| ()),
            _ => Err(LocalMarketError::ScopeMismatch),
        };
        if applied.is_err() {
            self.shared_history_retry_after.insert(key,
                crate::account_center::now_ms().saturating_add(30_000));
        }
        applied.map(|()| added)
    }

    pub fn apply_session_day(&mut self, generation: u64, binding: PublicMarketBinding,
        bar: PublicBar, confirmed: bool) -> Result<ReduceOutcome, LocalMarketError> {
        if generation < self.generation { return Ok(ReduceOutcome::IgnoredOldGeneration) }
        if generation > self.generation { return Err(LocalMarketError::FutureGeneration) }
        if !self.valid_session_scope(&binding, &bar) { return Err(LocalMarketError::ScopeMismatch) }
        self.session_day_tick = self.session_day_tick.saturating_add(1);
        let series = self.session_days.entry(binding).or_default();
        series.last_used_tick = self.session_day_tick;
        match series.bars.binary_search_by_key(&bar.open_time_ms, |day| day.open_time_ms) {
            Ok(_index) if series.confirmed.contains(&bar.open_time_ms) && !confirmed => {}
            Ok(index) if !confirmed && series.bars[index].received_at_ms > bar.received_at_ms => {}
            Ok(index) => series.bars[index] = bar.clone(),
            Err(index) => series.bars.insert(index, bar.clone()),
        }
        if confirmed { series.confirmed.insert(bar.open_time_ms); }
        if series.bars.len() > 500 {
            let expired = series.bars.drain(..series.bars.len()-500).map(|bar| bar.open_time_ms).collect::<Vec<_>>();
            for time in expired { series.confirmed.remove(&time); }
        }
        Ok(ReduceOutcome::Applied)
    }

    pub fn apply_session_history(&mut self, generation: u64, binding: PublicMarketBinding,
        bars: Vec<PublicBar>, forming: Option<PublicBar>) -> Result<ReduceOutcome, LocalMarketError> {
        if generation < self.generation { return Ok(ReduceOutcome::IgnoredOldGeneration) }
        if generation > self.generation { return Err(LocalMarketError::FutureGeneration) }
        if bars.len() > 500 || bars.windows(2).any(|pair| pair[0].open_time_ms >= pair[1].open_time_ms)
            || bars.iter().any(|bar| !self.valid_session_scope(&binding, bar))
            || forming.as_ref().is_some_and(|bar| !self.valid_session_scope(&binding, bar))
        { return Err(LocalMarketError::ScopeMismatch) }
        for bar in bars { self.apply_session_day(generation, binding.clone(), bar, true)?; }
        if let Some(bar) = forming { self.apply_session_day(generation, binding, bar, false)?; }
        Ok(ReduceOutcome::Applied)
    }

    fn valid_session_scope(&self, binding: &PublicMarketBinding, bar: &PublicBar) -> bool {
        self.reducers.keys().any(|selection| &selection.binding == binding)
            && bar.symbol == binding.symbol && bar.generation == self.generation
            && bar.interval_ms == 86_400_000 && bar.is_valid()
    }

    pub fn apply_base_minute(&mut self, generation: u64, binding: PublicMarketBinding,
        bar: PublicBar, confirmed: bool) -> Result<ReduceOutcome, LocalMarketError> {
        if generation < self.generation { return Ok(ReduceOutcome::IgnoredOldGeneration) }
        if generation > self.generation { return Err(LocalMarketError::FutureGeneration) }
        if !self.valid_base_scope(generation, &binding, &bar) {
            return Err(LocalMarketError::ScopeMismatch);
        }
        self.base_minute_tick = self.base_minute_tick.saturating_add(1);
        let tick = self.base_minute_tick;
        let series = self.base_minutes.entry(binding.clone()).or_default();
        if series.incarnation == 0 { series.incarnation = tick; }
        let newly_narrowed_gap = series.facts.binary_search_by_key(&bar.open_time_ms,
            |known| known.open_time_ms).err().and_then(|index| {
                let left = series.facts.get(index.checked_sub(1)?)?.open_time_ms;
                let right = series.facts.get(index)?.open_time_ms;
                (left.saturating_add(60_000) < right).then_some(right)
            });
        series.insert(bar, confirmed)?;
        series.last_used_tick = tick;
        if let Some(right) = newly_narrowed_gap {
            self.shared_history_exhausted_at.remove(&(binding.clone(), ChartInterval::OneMinute, right));
        }
        self.trim_base_minute_cache(&binding, MAX_BASE_MINUTE_CACHE_BYTES);
        Ok(ReduceOutcome::Applied)
    }

    pub fn apply_base_history(&mut self, generation: u64, binding: PublicMarketBinding,
        bars: Vec<PublicBar>, forming: Option<PublicBar>) -> Result<ReduceOutcome, LocalMarketError> {
        if generation < self.generation { return Ok(ReduceOutcome::IgnoredOldGeneration) }
        if generation > self.generation { return Err(LocalMarketError::FutureGeneration) }
        if bars.len() > 1_500 || bars.windows(2).any(|pair| pair[0].open_time_ms >= pair[1].open_time_ms)
            || bars.iter().any(|bar| !self.valid_base_scope(generation, &binding, bar))
            || forming.as_ref().is_some_and(|bar| !self.valid_base_scope(generation, &binding, bar))
        {
            return Err(LocalMarketError::ScopeMismatch);
        }
        let prepared = bars.into_iter().map(|bar| BaseMinuteSeries::prepare(bar, true))
            .collect::<Result<Vec<_>, _>>()?;
        let prepared_forming = forming.map(|bar| BaseMinuteSeries::prepare(bar, false)).transpose()?;
        self.base_minute_tick = self.base_minute_tick.saturating_add(1);
        let tick = self.base_minute_tick;
        let prefix_page = prepared_forming.is_none() && !prepared.is_empty() && self.base_minutes.get(&binding)
            .and_then(|series| series.facts.first())
            .is_some_and(|first| prepared.last().is_some_and(|(last, _, _)| last.open_time_ms < first.open_time_ms));
        if prefix_page {
            let mut prefix = BaseMinuteSeries::default();
            for (bar, ui_bar, point) in prepared { prefix.insert_prepared(bar, ui_bar, point); }
            // Validate the new page before taking ownership of the cached tail.
            // Repeated 1m paging must not clone the entire growing series.
            let mut candidate = self.base_minutes.remove(&binding).unwrap_or_default();
            prefix.bars.append(&mut candidate.bars);
            prefix.studies.append(&mut candidate.studies);
            if let (Some(left), Some(right)) = (prefix.facts.last(), candidate.facts.first()) {
                if left.open_time_ms.saturating_add(60_000) < right.open_time_ms {
                    prefix.gap_right_edges.insert(right.open_time_ms);
                }
            }
            prefix.gap_right_edges.append(&mut candidate.gap_right_edges);
            prefix.facts.append(&mut candidate.facts);
            prefix.closed_revision = candidate.closed_revision.saturating_add(1);
            prefix.incarnation = if candidate.incarnation == 0 { tick } else { candidate.incarnation };
            prefix.last_used_tick = tick;
            if prefix.facts.len() > MAX_BASE_MINUTE_BARS {
                let extra = prefix.facts.len() - MAX_BASE_MINUTE_BARS;
                prefix.bars.drain(..extra);
                prefix.studies.drain(..extra);
                prefix.facts.drain(..extra);
                if let Some(first) = prefix.facts.first() {
                    prefix.gap_right_edges.retain(|right| *right > first.open_time_ms);
                }
            }
            self.base_minutes.insert(binding.clone(), prefix);
        } else {
            let candidate = self.base_minutes.entry(binding.clone()).or_default();
            if candidate.incarnation == 0 { candidate.incarnation = tick; }
            for (bar, ui_bar, point) in prepared { candidate.insert_prepared(bar, ui_bar, point); }
            if let Some((bar, ui_bar, point)) = prepared_forming {
                candidate.insert_prepared(bar, ui_bar, point);
            }
            candidate.last_used_tick = tick;
        }
        self.trim_base_minute_cache(&binding, MAX_BASE_MINUTE_CACHE_BYTES);
        Ok(ReduceOutcome::Applied)
    }

    fn trim_base_minute_cache(&mut self, protected: &PublicMarketBinding, budget: usize) {
        let mut used = self.base_minutes.values().map(BaseMinuteSeries::memory_bytes).sum::<usize>();
        if used > budget {
            if let Some(series) = self.base_minutes.get_mut(protected) {
                let reclaimable = (series.bars.capacity() - series.bars.len())
                    * std::mem::size_of::<UiBar>()
                    + (series.studies.capacity() - series.studies.len())
                        * std::mem::size_of::<BaseMinuteStudy>()
                    + (series.facts.capacity() - series.facts.len())
                        * std::mem::size_of::<PublicBar>();
                if used.saturating_sub(reclaimable) <= budget {
                    let before = series.memory_bytes();
                    series.bars.shrink_to_fit();
                    series.studies.shrink_to_fit();
                    series.facts.shrink_to_fit();
                    used = used.saturating_sub(before).saturating_add(series.memory_bytes());
                }
            }
        }
        while used > budget {
            let victim = self.base_minutes.iter()
                .filter(|(binding, _)| *binding != protected)
                .min_by_key(|(_, series)| series.last_used_tick)
                .map(|(binding, _)| binding.clone());
            let Some(victim) = victim else {
                // Keep the active source intact; only release unused vector capacity.
                if let Some(series) = self.base_minutes.get_mut(protected) {
                    series.bars.shrink_to_fit();
                    series.studies.shrink_to_fit();
                    series.facts.shrink_to_fit();
                }
                break;
            };
            if let Some(removed) = self.base_minutes.remove(&victim) {
                used = used.saturating_sub(removed.memory_bytes());
                self.shared_history_pending.remove(&(victim.clone(), ChartInterval::OneMinute));
                self.shared_history_retry_after.remove(&(victim.clone(), ChartInterval::OneMinute));
                self.shared_history_exhausted_at.retain(|(binding, interval, _)|
                    binding != &victim || *interval != ChartInterval::OneMinute);
            }
        }
    }

    fn valid_base_scope(&self, generation: u64, binding: &PublicMarketBinding, bar: &PublicBar) -> bool {
        self.reducers.keys().any(|selection| &selection.binding == binding)
            && bar.symbol == binding.symbol && bar.generation == generation
            && bar.interval_ms == 60_000 && bar.is_valid()
    }

    pub fn view_for_symbol(&self, symbol: &str) -> Option<&LocalMarketView> {
        let (base, quote) = symbol.split_once('/')?;
        self.reducers
            .values()
            .map(LocalMarketReducer::view)
            .find(|view| {
                view.selection.binding.symbol.base() == base
                    && view.selection.binding.symbol.quote() == quote
            })
    }

    pub(crate) fn views_for_market(
        &self,
        venue: venue_gateway_api::VenueId,
        symbol: &str,
    ) -> impl Iterator<Item = &LocalMarketView> {
        let parts = symbol.split_once('/');
        self.reducers
            .values()
            .map(LocalMarketReducer::view)
            .filter(move |view| {
                view.selection.binding.venue == venue
                    && parts.is_some_and(|(base, quote)| {
                        view.selection.binding.symbol.base() == base
                            && view.selection.binding.symbol.quote() == quote
                    })
            })
    }

    pub fn refresh_staleness(&mut self, now_ms: u64, stale_after_ms: u64) {
        for reducer in self.reducers.values_mut() {
            reducer.refresh_staleness(now_ms, stale_after_ms);
        }
        for reducer in &mut self.chart_reducers {
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
        // A chart keeps its explicit configuration when the default engine changes.
        for reducer in self.reducers.values() {
            if self.chart_bindings.values().any(|(selection, config)| {
                selection == &reducer.view.selection && config == &reducer.study_config
            }) {
                self.chart_reducers.push(reducer.clone());
            }
        }
        for reducer in self.reducers.values_mut() {
            reducer.reconfigure_studies(study_config.clone())?;
        }
        self.study_config = study_config;
        self.prune_chart_reducers();
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum LocalMarketError {
    #[error("symbol must be canonical BASE/USDT or BASE/USDC")]
    InvalidSymbol,
    #[error("public market binding must use LIVE linear stablecoin perpetuals")]
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
    generation: u64,
) -> Result<(), LocalMarketError> {
    if bar.symbol != selection.binding.symbol
        || bar.generation != generation
        || bar.interval_ms != selection.interval.duration_ms()
        || !bar.is_valid()
    {
        return Err(LocalMarketError::InvalidBar);
    }
    Ok(())
}

fn apply_print_to_bar(bar: &mut PublicBar, price: Decimal) -> Result<(), LocalMarketError> {
    let price = venue_domain::Price::new(price).map_err(|_| LocalMarketError::InvalidTrade)?;
    bar.close = price;
    bar.high = bar.high.max(price);
    bar.low = bar.low.min(price);
    Ok(())
}

fn ui_bar_from_public(bar: &PublicBar) -> Result<UiBar, LocalMarketError> {
    Ok(UiBar {
        open_time_ms: bar.open_time_ms,
        open: bar.open.value(),
        high: bar.high.value(),
        low: bar.low.value(),
        close: bar.close.value(),
        volume: match bar.base_volume {
            FieldState::Known(volume) => Some(volume),
            FieldState::Unavailable { .. } => None,
            _ => return Err(LocalMarketError::InvalidBar),
        },
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
    let common = values.common;
    let (sar, sar_rising) = common
        .sar
        .map_or((None, false), |value| (Some(value.value), value.rising));
    let (supertrend, supertrend_rising) = common
        .supertrend
        .map_or((None, false), |value| (Some(value.value), value.rising));
    let (kdj_k, kdj_d, kdj_j) = common.kdj.map_or((None, None, None), |value| {
        (Some(value.k), Some(value.d), Some(value.j))
    });
    let (stoch_rsi_k, stoch_rsi_d) = common.stoch_rsi.map_or((None, None), |value| {
        (Some(value.first), Some(value.second))
    });
    let (dmi_plus, dmi_minus, dmi_adx) = common.dmi.map_or((None, None, None), |value| {
        (Some(value.plus_di), Some(value.minus_di), Some(value.adx))
    });
    ChartStudyPoint {
        custom_ema_adx: values.custom_ema_adx,
        custom_scripts: values.custom_scripts,
        order_flow: values.order_flow,
        bar_vwap: values.bar_vwap,
        sma: values.sma,
        sma_second: common.sma_extra.second,
        sma_third: common.sma_extra.third,
        ema: values.ema,
        ema_second: common.ema_extra.second,
        ema_third: common.ema_extra.third,
        wma: common.wma.first,
        wma_second: common.wma.second,
        wma_third: common.wma.third,
        bollinger_upper,
        bollinger_middle,
        bollinger_lower,
        vwap: values.vwap,
        avl: common.avl,
        trix: common.trix,
        sar,
        sar_rising,
        supertrend,
        supertrend_rising,
        rsi: values.rsi,
        macd,
        macd_signal,
        macd_histogram,
        atr: values.atr,
        mfi: common.mfi,
        kdj_k,
        kdj_d,
        kdj_j,
        obv: common.obv,
        cci: common.cci,
        stoch_rsi_k,
        stoch_rsi_d,
        williams_r: common.williams_r,
        dmi_plus,
        dmi_minus,
        dmi_adx,
        momentum: common.momentum,
        emv: common.emv,
        ..ChartStudyPoint::default()
    }
}

fn validate_bar(bar: &UiBar, interval: ChartInterval) -> Result<(), LocalMarketError> {
    let positive = bar.open > Decimal::ZERO
        && bar.high > Decimal::ZERO
        && bar.low > Decimal::ZERO
        && bar.close > Decimal::ZERO
        && bar.volume.is_none_or(|volume| volume >= Decimal::ZERO);
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
#[path = "market/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "market/sharing_tests.rs"]
mod sharing_tests;
