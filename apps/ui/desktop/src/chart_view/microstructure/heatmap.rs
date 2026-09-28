use std::sync::{Arc, OnceLock, atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::hash::{Hash, Hasher};

use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect};
use venue_control_protocol::UiBar;
use venue_indicators::chart::liquidation_scenario::LiquidationScenario;

use super::{Settings, label};
use crate::{chart::PriceRange, i18n::Language, model::decimal_to_f64, theme};

const ROWS: usize = 96;
const COLUMNS: usize = 160;
const ASYNC_MINUTE_THRESHOLD: usize = 4_000;
const MAX_RETAINED_INPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_PENDING_INPUT_BYTES: usize = 64 * 1024 * 1024;
static PENDING_INPUT_BYTES: AtomicUsize = AtomicUsize::new(0);

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn pending_input_bytes() -> usize {
    PENDING_INPUT_BYTES.load(Ordering::Relaxed)
}

struct InputReservation(usize);

impl InputReservation {
    fn try_new(bytes: usize) -> Option<Self> {
        PENDING_INPUT_BYTES.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current|
            current.checked_add(bytes).filter(|total| *total <= MAX_PENDING_INPUT_BYTES))
            .ok().map(|_| Self(bytes))
    }
}

impl Drop for InputReservation {
    fn drop(&mut self) {
        PENDING_INPUT_BYTES.fetch_sub(self.0, Ordering::AcqRel);
    }
}

fn estimated_async_input_bytes(source_bars: usize, display_bars: usize) -> usize {
    source_bars.saturating_mul(std::mem::size_of::<UiBar>()
        + std::mem::size_of::<crate::chart::BaseMinuteStudy>()
        + std::mem::size_of::<rust_decimal::Decimal>())
        .saturating_add(display_bars.saturating_mul(std::mem::size_of::<UiBar>()))
        // Raster buffers and one bounded lookback model are temporary worker allocations.
        .saturating_add(4 * 1024 * 1024)
}

type HeatmapJob = Box<dyn FnOnce() + Send + 'static>;
static HEATMAP_WORKERS: OnceLock<Option<crossbeam_channel::Sender<HeatmapJob>>> = OnceLock::new();

fn heatmap_sender() -> &'static Option<crossbeam_channel::Sender<HeatmapJob>> {
    HEATMAP_WORKERS.get_or_init(|| {
        let (sender, receiver) = crossbeam_channel::bounded::<HeatmapJob>(1);
        let mut started = 0;
        for index in 0..2 {
            let receiver = receiver.clone();
            if std::thread::Builder::new().name(format!("venueflow-heatmap-{index}"))
                .spawn(move || while let Ok(job) = receiver.recv() { job(); }).is_ok() {
                started += 1;
            }
        }
        (started > 0).then_some(sender)
    })
}

fn dispatch_heatmap(job: HeatmapJob) -> bool {
    heatmap_sender().as_ref().is_some_and(|sender| sender.try_send(job).is_ok())
}

fn heatmap_worker_available() -> bool {
    heatmap_sender().as_ref().is_some_and(|sender| !sender.is_full())
}

#[derive(Clone, Copy, PartialEq)]
struct SourceStamp {
    revision: Option<(u64, u64)>,
    first: Option<u64>,
    last: Option<u64>,
    len: usize,
}

impl SourceStamp {
    fn new(bars: &[UiBar], revision: Option<(u64, u64)>) -> Self {
        Self { revision, first: bars.first().map(|bar| bar.open_time_ms),
            last: bars.last().map(|bar| bar.open_time_ms), len: bars.len() }
    }
    fn matches(self, other: Self, snapshot: &[UiBar], bars: &[UiBar]) -> bool {
        self == other && (self.revision.is_some() || snapshot == bars)
    }
    fn snapshot(self, bars: &[UiBar]) -> Vec<UiBar> {
        if self.revision.is_some() { Vec::new() } else { bars.to_vec() }
    }
}

#[derive(Clone)]
struct Cache {
    scope: Option<venue_gateway_api::PublicMarketBinding>,
    live_extrema: Option<(u64, rust_decimal::Decimal, rust_decimal::Decimal)>,
    source: SourceStamp,
    bars: Vec<UiBar>,
    source_snapshot: Option<Arc<Vec<UiBar>>>,
    entries_snapshot: Option<Arc<Vec<rust_decimal::Decimal>>>,
    display_times: Vec<u64>,
    entries: Vec<rust_decimal::Decimal>,
    interval_ms: u64,
    settings: Settings,
    offset: usize,
    start: usize,
    slots: usize,
    low: f64,
    high: f64,
    price_tick: Option<rust_decimal::Decimal>,
    cells: Vec<f64>,
    column_hashes: Vec<u64>,
    column_maxima: Vec<f64>,
    maximum: f64,
    approximated: usize,
    complete: bool,
    render_minute: u64,
}

struct ChunkRaster {
    cells: Vec<f64>,
    maximum: f64,
    hashes: Vec<u64>,
    maxima: Vec<f64>,
    reused_columns: usize,
}

#[derive(Clone)]
struct TextureCache {
    raster: Arc<Cache>,
    texture: egui::TextureHandle,
}

#[derive(Clone)]
struct ModelCache {
    scope: Option<venue_gateway_api::PublicMarketBinding>,
    source: SourceStamp,
    bars: Vec<UiBar>,
    entries: Vec<rust_decimal::Decimal>,
    leverage: [bool; 4],
    maintenance_bps: u16,
    margin_cost_bps: u16,
    tick: Option<rust_decimal::Decimal>,
    model: LiquidationScenario,
}

struct AsyncInput {
    source: Arc<Vec<UiBar>>,
    studies: Option<Vec<crate::chart::BaseMinuteStudy>>,
    entries: Option<Arc<Vec<rust_decimal::Decimal>>>,
    approximated: Option<usize>,
    display: Vec<UiBar>,
    display_times: Vec<u64>,
    scope: Option<venue_gateway_api::PublicMarketBinding>,
    source_stamp: SourceStamp,
    live_extrema: Option<(u64, rust_decimal::Decimal, rust_decimal::Decimal)>,
    interval_ms: u64,
    settings: Settings,
    offset: usize,
    start: usize,
    slots: usize,
    range: PriceRange,
    price_tick: Option<rust_decimal::Decimal>,
    display_start: u64,
    display_end: u64,
    now_ms: u64,
    render_minute: u64,
    reuse: Option<Arc<Cache>>,
    cancel: Arc<AtomicBool>,
}

#[derive(Clone, PartialEq)]
struct Demand {
    scope: Option<venue_gateway_api::PublicMarketBinding>,
    source: SourceStamp,
    display_times: Vec<u64>,
    live_extrema: Option<(u64, rust_decimal::Decimal, rust_decimal::Decimal)>,
    interval_ms: u64,
    settings: Settings,
    offset: usize,
    start: usize,
    slots: usize,
    low: f64,
    high: f64,
    price_tick: Option<rust_decimal::Decimal>,
    render_minute: u64,
}

#[derive(Clone)]
struct Pending {
    demand: Demand,
    token: u64,
    running: bool,
    cancel: Option<Arc<AtomicBool>>,
}

impl Pending {
    fn latest(previous: Option<Self>, demand: Demand) -> Self {
        match previous {
            Some(previous) if previous.demand == demand => previous,
            Some(mut previous) => {
                previous.invalidate();
                Self { demand, token: previous.token, running: false, cancel: None }
            }
            None => Self { demand, token: 1, running: false, cancel: None },
        }
    }

    fn accepts(&self, token: u64, demand: &Demand) -> bool {
        self.token == token && self.demand == *demand
    }

    fn invalidate(&mut self) {
        if let Some(cancel) = self.cancel.take() { cancel.store(true, Ordering::Relaxed); }
        self.running = false;
        self.token = self.token.wrapping_add(1);
    }
}

pub(super) fn cancel(context: &egui::Context, id: egui::Id) {
    let key = id.with("liquidation-scenario-pending");
    context.data_mut(|data| {
        if let Some(mut pending) = data.get_temp::<Pending>(key)
            && (pending.running || pending.cancel.is_some()) {
            pending.invalidate();
            data.insert_temp(key, pending);
        }
    });
    release_result(context, id);
}

pub(super) fn release_result(context: &egui::Context, id: egui::Id) {
    context.data_mut(|data| {
        data.remove::<Arc<Cache>>(id.with("liquidation-scenario-raster"));
        data.remove::<Arc<ModelCache>>(id.with("liquidation-scenario-model"));
        data.remove::<Arc<TextureCache>>(id.with("liquidation-scenario-texture"));
    });
}

fn cache_retained_bytes(cache: &Cache) -> usize {
    std::mem::size_of::<Cache>()
        .saturating_add(cache.bars.capacity() * std::mem::size_of::<UiBar>())
        .saturating_add(cache.source_snapshot.as_ref().map_or(0, |bars|
            bars.capacity() * std::mem::size_of::<UiBar>()))
        .saturating_add(cache.entries_snapshot.as_ref().map_or(0, |entries|
            entries.capacity() * std::mem::size_of::<rust_decimal::Decimal>()))
        .saturating_add(cache.display_times.capacity() * std::mem::size_of::<u64>())
        .saturating_add(cache.entries.capacity() * std::mem::size_of::<rust_decimal::Decimal>())
        .saturating_add(cache.cells.capacity() * std::mem::size_of::<f64>())
        .saturating_add(cache.column_hashes.capacity() * std::mem::size_of::<u64>())
        .saturating_add(cache.column_maxima.capacity() * std::mem::size_of::<f64>())
}

pub(super) fn retained_bytes(context: &egui::Context, id: egui::Id) -> usize {
    let (raster, model, texture) = context.data(|data| (
        data.get_temp::<Arc<Cache>>(id.with("liquidation-scenario-raster")),
        data.get_temp::<Arc<ModelCache>>(id.with("liquidation-scenario-model")),
        data.get_temp::<Arc<TextureCache>>(id.with("liquidation-scenario-texture")),
    ));
    let mut bytes = raster.as_ref().map_or(0, |cache| cache_retained_bytes(cache));
    if let Some(model) = model {
        bytes = bytes.saturating_add(std::mem::size_of::<ModelCache>())
            .saturating_add(model.bars.capacity() * std::mem::size_of::<UiBar>())
            .saturating_add(model.entries.capacity() * std::mem::size_of::<rust_decimal::Decimal>())
            .saturating_add(model.model.estimated_retained_bytes());
    }
    if let Some(texture) = texture {
        if raster.as_ref().is_none_or(|cache| !Arc::ptr_eq(cache, &texture.raster)) {
            bytes = bytes.saturating_add(cache_retained_bytes(&texture.raster));
        }
        // The CPU image and the uploaded texture are both retained by the renderer.
        bytes = bytes.saturating_add(std::mem::size_of::<TextureCache>())
            .saturating_add(2 * ROWS * COLUMNS * std::mem::size_of::<egui::Color32>());
    }
    bytes
}

fn compute_async_cache(input: AsyncInput) -> Arc<Cache> {
    #[cfg(not(target_arch = "wasm32"))]
    let started = std::time::Instant::now();
    let (entries, approximated) = if let Some(entries) = input.entries {
        (entries, input.approximated.unwrap_or_default())
    } else {
        let mut approximated = 0;
        let entries = input.source.iter().enumerate().map(|(index, bar)| {
            input.studies.as_ref().and_then(|studies| studies.get(index))
                .and_then(|point| point.bar_vwap).unwrap_or_else(|| {
                approximated += 1;
                bar.close
            })
        }).collect::<Vec<_>>();
        (Arc::new(entries), approximated)
    };
    let raster = raster_chunked_cancellable(&input.source, &entries, &input.display,
        input.slots, input.range, &input.settings, input.interval_ms,
        input.now_ms, input.live_extrema, input.price_tick, input.reuse.as_deref(),
        Some(&input.cancel));
    #[cfg(not(target_arch = "wasm32"))]
    tracing::info!(target: "venueflow::indicator_performance", study = "heatmap",
        phase = "async_compute", source_bars = input.source.len(),
        reused_columns = raster.reused_columns, total_columns = COLUMNS,
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "indicator computation completed");
    let required_start = input.display_start.saturating_sub(
        u64::from(input.settings.lookback_hours) * 3_600_000);
    let required_end = input.display_end.min(input.now_ms.saturating_sub(input.now_ms % 60_000));
    let complete = input.source.first().is_some_and(|first| first.open_time_ms <= required_start)
        && input.source.last().is_some_and(|last| last.open_time_ms.saturating_add(60_000) >= required_end)
        && !input.source.windows(2).any(|pair| pair[0].open_time_ms.saturating_add(60_000) != pair[1].open_time_ms)
        && input.source.iter().all(|bar| bar.volume.is_some());
    let retained_input_bytes = input.source.capacity() * std::mem::size_of::<UiBar>()
        + entries.capacity() * std::mem::size_of::<rust_decimal::Decimal>();
    let retain_input = retained_input_bytes <= MAX_RETAINED_INPUT_BYTES;
    Arc::new(Cache {
        scope: input.scope,
        live_extrema: input.live_extrema,
        source: input.source_stamp,
        bars: Vec::new(),
        source_snapshot: retain_input.then_some(input.source),
        entries_snapshot: retain_input.then_some(entries),
        display_times: input.display_times,
        entries: Vec::new(),
        interval_ms: input.interval_ms,
        settings: input.settings,
        offset: input.offset,
        start: input.start,
        slots: input.slots,
        low: input.range.low,
        high: input.range.high,
        price_tick: input.price_tick,
        cells: raster.cells,
        column_hashes: raster.hashes,
        column_maxima: raster.maxima,
        maximum: raster.maximum,
        approximated,
        complete,
        render_minute: input.render_minute,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn draw(
    ui: &egui::Ui,
    painter: &egui::Painter,
    id: egui::Id,
    rect: Rect,
    bars: &[UiBar],
    studies: &[crate::chart::ChartStudyPoint],
    interval_ms: u64,
    visible: std::ops::Range<usize>,
    slots: usize,
    range: PriceRange,
    settings: &Settings,
    language: Language,
    market_scope: Option<&venue_gateway_api::PublicMarketBinding>,
    minute_source: Option<(&[UiBar], &[crate::chart::BaseMinuteStudy], (u64, u64))>,
    price_tick: Option<rust_decimal::Decimal>,
) {
    let (source_bars, base_studies, source_revision) = if let Some((source_bars, source_studies, revision)) = minute_source {
        (source_bars, Some(source_studies), Some(revision))
    } else if interval_ms == 60_000 {
        (bars, None, None)
    } else {
        painter.text(rect.left_bottom() + egui::vec2(6.0, -5.0), Align2::LEFT_BOTTOM,
            label(language, "清算估算：等待同源1m行情", "Liquidation estimate: waiting for same-market 1m bars"),
            FontId::proportional(10.0), theme::WARNING);
        return;
    };
    let display_start = bars.get(visible.start).map_or(0, |bar| bar.open_time_ms);
    let display_end = visible.end.checked_sub(1).and_then(|index| bars.get(index))
        .map_or(display_start, |bar| bar.open_time_ms.saturating_add(interval_ms));
    let display_times = bars.get(visible.start.min(bars.len())..visible.end.min(bars.len()))
        .unwrap_or_default().iter().map(|bar| bar.open_time_ms).collect::<Vec<_>>();
    // Only an explicitly unconfirmed latest bar is a preview; historical panning uses
    // the visible time range rather than anchoring every scenario to today's last bar.
    let latest_confirmed = match base_studies {
        Some(points) => points.last().is_some_and(|point| point.confirmed),
        None => studies.last().is_some_and(|point| point.confirmed),
    };
    let live_extrema = (!latest_confirmed)
        .then(|| source_bars.last().filter(|bar| bar.open_time_ms < display_end)
            .map(|bar| (bar.open_time_ms, bar.high, bar.low))).flatten();
    let source_range = history_range(source_bars, display_start..display_end, settings.lookback_hours, latest_confirmed);
    let offset = source_range.start;
    let source = &source_bars[source_range];
    let stamp = SourceStamp::new(source, source_revision);
    let read_entries = || {
        let mut approximated = 0;
        let entries = source.iter().enumerate().map(|(index, bar)| {
            let vwap = match base_studies {
                Some(points) => points.get(offset + index).and_then(|point| point.bar_vwap),
                None => studies.get(offset + index).and_then(|point| point.bar_vwap),
            };
            vwap.unwrap_or_else(|| { approximated += 1; bar.close })
        }).collect::<Vec<_>>();
        (entries, approximated)
    };
    // The display-only fallback has no source revision, so compare its derived values.
    let fallback_entries = stamp.revision.is_none().then(&read_entries);
    let now_ms = crate::account_center::now_ms();
    let render_minute = if live_extrema.is_some() { now_ms / 60_000 } else { 0 };
    let key = id.with("liquidation-scenario-raster");
    let previous = ui.ctx().data(|data| data.get_temp::<Arc<Cache>>(key));
    let previous = if previous.as_ref().is_some_and(|cache| cache.scope.as_ref() != market_scope) {
        cancel(ui.ctx(), id);
        None
    } else { previous };
    let cached = previous.as_ref()
        .filter(|c| {
            c.scope.as_ref() == market_scope
                && c.live_extrema == live_extrema
                && stamp.matches(c.source, &c.bars, source)
                && c.display_times == display_times
                && fallback_entries.as_ref().is_none_or(|(entries, _)| c.entries == *entries)
                && c.interval_ms == interval_ms
                && c.settings == *settings
                && c.offset == offset
                && c.start == visible.start
                && c.slots == slots
                && c.low == range.low
                && c.high == range.high
                && c.price_tick == price_tick
                && c.render_minute == render_minute
        }).cloned();
    let pending_key = id.with("liquidation-scenario-pending");
    let old_pending = ui.ctx().data(|data| data.get_temp::<Pending>(pending_key));
    let mut pending = Pending::latest(old_pending.clone(), Demand {
        scope: market_scope.cloned(), source: stamp, display_times: display_times.clone(),
        live_extrema, interval_ms, settings: settings.clone(), offset,
        start: visible.start, slots, low: range.low, high: range.high,
        price_tick, render_minute,
    });
    if old_pending.as_ref().is_none_or(|old| old.token != pending.token) {
        ui.ctx().data_mut(|data| data.insert_temp(pending_key, pending.clone()));
    }
    let mut updating = false;
    let mut prefix_preview = None;
    if cached.is_none() && source.len() > ASYNC_MINUTE_THRESHOLD
        && let Some(points) = base_studies
        && points.len() >= offset.saturating_add(source.len()) {
        if !pending.running && heatmap_worker_available()
            && let Some(reservation) = InputReservation::try_new(estimated_async_input_bytes(
                source.len(), visible.end.saturating_sub(visible.start))) {
            let reuse = previous.as_ref().filter(|cache| {
                cache.scope.as_ref() == market_scope
                    && cache.interval_ms == interval_ms
                    && cache.settings == *settings
                    && cache.low == range.low && cache.high == range.high
                    && cache.price_tick == price_tick
                    && cache.cells.len() == ROWS * COLUMNS
                    && cache.column_hashes.len() == COLUMNS
                    && cache.column_maxima.len() == COLUMNS
            }).cloned();
            let source_snapshot = previous.as_ref().filter(|cache| {
                cache.scope.as_ref() == market_scope && stamp.matches(cache.source, &cache.bars, source)
            });
            let entries_snapshot = source_snapshot.and_then(|cache| cache.entries_snapshot.clone());
            let input = AsyncInput {
                source: source_snapshot.and_then(|cache| cache.source_snapshot.clone())
                    .unwrap_or_else(|| Arc::new(source.to_vec())),
                studies: entries_snapshot.is_none()
                    .then(|| points[offset..offset + source.len()].to_vec()),
                entries: entries_snapshot,
                approximated: source_snapshot.map(|cache| cache.approximated),
                display: bars.get(visible.start.min(bars.len())..visible.end.min(bars.len()))
                    .unwrap_or_default().to_vec(),
                display_times: display_times.clone(),
                scope: market_scope.cloned(),
                source_stamp: stamp,
                live_extrema,
                interval_ms,
                settings: settings.clone(),
                offset,
                start: visible.start,
                slots,
                range,
                price_tick,
                display_start,
                display_end,
                now_ms,
                render_minute,
                reuse,
                cancel: Arc::new(AtomicBool::new(false)),
            };
            let context = ui.ctx().clone();
            pending.running = true;
            let token = pending.token;
            let expected = pending.demand.clone();
            pending.cancel = Some(input.cancel.clone());
            context.data_mut(|data| data.insert_temp(pending_key, pending));
            let queued = dispatch_heatmap(Box::new(move || {
                let _reservation = reservation;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || compute_async_cache(input)));
                context.data_mut(|data| {
                    let Some(mut pending) = data.get_temp::<Pending>(pending_key) else { return; };
                    if !pending.accepts(token, &expected) { return; }
                    if let Ok(cache) = result { data.insert_temp(key, cache); }
                    pending.running = false;
                    pending.cancel = None;
                    data.insert_temp(pending_key, pending);
                });
                context.request_repaint();
            }));
            if !queued {
                ui.ctx().data_mut(|data| {
                    if let Some(mut pending) = data.get_temp::<Pending>(pending_key)
                        && pending.token == token {
                        pending.running = false;
                        pending.cancel = None;
                        data.insert_temp(pending_key, pending);
                    }
                });
            }
        }
        prefix_preview = previous.filter(|cache| {
            cache.scope.as_ref() == market_scope
                && cache.source.last == stamp.last
                && cache.source.first.zip(stamp.first).is_some_and(|(old, new)| new <= old)
                && cache.source.len <= stamp.len
                && cache.display_times == display_times
                && cache.interval_ms == interval_ms
                && cache.settings == *settings
                && cache.start == visible.start
                && cache.slots == slots
                && cache.low == range.low
                && cache.high == range.high
                && cache.price_tick == price_tick
                && cache.render_minute == render_minute
        });
        if prefix_preview.is_none() {
            painter.text(rect.left_bottom() + egui::vec2(6.0, -5.0), Align2::LEFT_BOTTOM,
                label(language, "清算估算：正在计算同源历史", "Liquidation estimate: computing same-market history"),
                FontId::proportional(10.0), theme::WARNING);
            return;
        }
        updating = true;
    }
    let cache = cached.or(prefix_preview).unwrap_or_else(|| {
            #[cfg(not(target_arch = "wasm32"))]
            let started = std::time::Instant::now();
            let (entries, approximated) = fallback_entries.unwrap_or_else(read_entries);
            let model_key = id.with("liquidation-scenario-model");
            let model = ui
                .ctx()
                .data(|data| data.get_temp::<Arc<ModelCache>>(model_key))
                .filter(|c| {
                    c.scope.as_ref() == market_scope
                        && stamp.matches(c.source, &c.bars, source)
                        && (stamp.revision.is_some() || c.entries == entries)
                        && c.leverage == settings.leverage
                        && c.maintenance_bps == settings.maintenance_bps
                        && c.margin_cost_bps == settings.margin_cost_bps
                        && c.tick == price_tick
                })
                .unwrap_or_else(|| {
                    let cache = Arc::new(ModelCache {
                        scope: market_scope.cloned(),
                        source: stamp,
                        bars: stamp.snapshot(source),
                        entries: if stamp.revision.is_some() { Vec::new() } else { entries.clone() },
                        leverage: settings.leverage,
                        maintenance_bps: settings.maintenance_bps,
                        margin_cost_bps: settings.margin_cost_bps,
                        tick: price_tick,
                        model: build_model(source, &entries, 60_000, settings, price_tick),
                    });
                    ui.ctx()
                        .data_mut(|data| data.insert_temp(model_key, cache.clone()));
                    cache
                });
            let cells = raster(
                &model.model,
                bars,
                visible.start,
                slots,
                range,
                settings,
                interval_ms,
                source_bars,
                now_ms,
                live_extrema,
            );
            // The reference is a property of this model input, not the visible display interval.
            let maximum = model.model.bands().iter().map(|band| decimal_to_f64(band.weight))
                .fold(0.0_f64, f64::max);
            let required_start = display_start.saturating_sub(u64::from(settings.lookback_hours) * 3_600_000);
            let required_end = display_end.min(now_ms.saturating_sub(now_ms % 60_000));
            let complete = source.first().is_some_and(|first| first.open_time_ms <= required_start)
                && source.last().is_some_and(|last| last.open_time_ms.saturating_add(60_000) >= required_end)
                && !source.windows(2).any(|pair| pair[0].open_time_ms.saturating_add(60_000) != pair[1].open_time_ms)
                && source.iter().all(|bar| bar.volume.is_some());
            let cache = Arc::new(Cache {
                scope: market_scope.cloned(),
                live_extrema,
                source: stamp,
                bars: stamp.snapshot(source),
                source_snapshot: None,
                entries_snapshot: None,
                display_times,
                entries: if stamp.revision.is_some() { Vec::new() } else { entries.clone() },
                interval_ms,
                settings: settings.clone(),
                offset,
                start: visible.start,
                slots,
                low: range.low,
                high: range.high,
                price_tick,
                cells,
                column_hashes: Vec::new(),
                column_maxima: Vec::new(),
                maximum,
                approximated,
                complete,
                render_minute,
            });
            ui.ctx()
                .data_mut(|data| data.insert_temp(key, cache.clone()));
            #[cfg(not(target_arch = "wasm32"))]
            tracing::info!(target: "venueflow::indicator_performance", study = "heatmap",
                phase = "sync_cache_miss", source_bars = source.len(),
                elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
                "indicator computation completed");
            cache
        });
    if cache.maximum > 0.0 {
        let texture_key = id.with("liquidation-scenario-texture");
        let painted = ui.ctx().data(|data| data.get_temp::<Arc<TextureCache>>(texture_key));
        let painted = painted.filter(|painted| Arc::ptr_eq(&painted.raster, &cache))
            .unwrap_or_else(|| {
                let texture = ui.ctx().load_texture("venueflow-liquidation-heatmap",
                    raster_image(&cache), egui::TextureOptions::NEAREST);
                let painted = Arc::new(TextureCache { texture, raster: cache.clone() });
                ui.ctx().data_mut(|data| data.insert_temp(texture_key, painted.clone()));
                painted
            });
        painter.image(painted.texture.id(), rect,
            Rect::from_min_max(Pos2::ZERO, Pos2::new(1.0, 1.0)), Color32::WHITE);
    }
    let title = if source.is_empty() {
        label(language,
            "清算估算：所选历史暂无同源1m覆盖",
            "Liquidation estimate: no same-market 1m coverage for this history").to_owned()
    } else if language == Language::SimplifiedChinese {
        format!("清算估算 · 相对强度 · 1m {}根 · 前置{}h · 收盘近似{}根{}",
            source.len(), settings.lookback_hours, cache.approximated,
            if updating { " · 更新中" } else if cache.complete { "" } else { " · 历史覆盖不足" })
    } else {
        format!("Liquidation scenarios · relative · {} 1m bars · {}h warm-up · {} close proxies{}",
            source.len(), settings.lookback_hours, cache.approximated,
            if updating { " · updating" } else if cache.complete { "" } else { " · partial history" })
    };
    painter.text(
        rect.left_bottom() + egui::vec2(6.0, -5.0),
        Align2::LEFT_BOTTOM,
        title,
        FontId::proportional(10.0),
        theme::WARNING,
    );
}

fn raster_image(cache: &Cache) -> egui::ColorImage {
    let mut image = egui::ColorImage::filled([COLUMNS, ROWS], Color32::TRANSPARENT);
    for (index, &value) in cache.cells.iter().enumerate() {
        if value <= 0.0 { continue; }
        let strength = ((1.0 + 20.0 * value / cache.maximum).ln() / 21.0_f64.ln()).clamp(0.0, 1.0);
        let color = Color32::from_rgba_unmultiplied(
            (25.0 + 230.0 * strength) as u8,
            (60.0 + 170.0 * strength) as u8,
            (145.0 - 90.0 * strength) as u8,
            (f64::from(cache.settings.opacity.clamp(10, 70)) * 2.55 * strength.sqrt()) as u8,
        );
        image.pixels[index] = color;
    }
    image
}

fn history_range(bars: &[UiBar], visible_time: std::ops::Range<u64>, hours: u16, latest_confirmed: bool) -> std::ops::Range<usize> {
    let end = bars.partition_point(|bar| bar.open_time_ms < visible_time.end)
        .min(if latest_confirmed { bars.len() } else { bars.len().saturating_sub(1) });
    let cutoff = visible_time.start.saturating_sub(u64::from(hours) * 3_600_000);
    let offset = bars[..end].partition_point(|bar| bar.open_time_ms < cutoff);
    offset..end
}

fn build_model(
    bars: &[UiBar],
    entries: &[rust_decimal::Decimal],
    interval_ms: u64,
    settings: &Settings,
    price_tick: Option<rust_decimal::Decimal>,
) -> LiquidationScenario {
    // Market scenarios are independent of the account's leverage and legacy UI tier selection.
    let tiers = [10, 25, 50, 100];
    let mut model = LiquidationScenario::default();
    for (bar, entry) in bars.iter().zip(entries) {
        let end = bar.open_time_ms.saturating_add(interval_ms);
        if let Some(volume) = bar.volume {
            model.observe_with_tick(
                bar.open_time_ms,
                end,
                bar.high,
                bar.low,
                *entry,
                volume,
                &tiers,
                settings.maintenance_bps,
                settings.margin_cost_bps,
                price_tick,
            );
        } else {
            model.retire(end, bar.high, bar.low);
        }
    }
    model
}

fn raster(
    model: &LiquidationScenario,
    bars: &[UiBar],
    start: usize,
    slots: usize,
    range: PriceRange,
    settings: &Settings,
    interval_ms: u64,
    minute_bars: &[UiBar],
    now_ms: u64,
    live_extrema: Option<(u64, rust_decimal::Decimal, rust_decimal::Decimal)>,
) -> Vec<f64> {
    raster_columns(model, bars, start, slots, range, settings, interval_ms,
        minute_bars, now_ms, live_extrema, COLUMNS)
}

#[allow(clippy::too_many_arguments)]
fn raster_columns(
    model: &LiquidationScenario,
    bars: &[UiBar],
    start: usize,
    slots: usize,
    range: PriceRange,
    settings: &Settings,
    interval_ms: u64,
    minute_bars: &[UiBar],
    now_ms: u64,
    live_extrema: Option<(u64, rust_decimal::Decimal, rust_decimal::Decimal)>,
    columns: usize,
) -> Vec<f64> {
    let mut cells = vec![0.0; ROWS * columns];
    if slots == 0 || range.high <= range.low || !range.high.is_finite() || !range.low.is_finite()
        || minute_bars.is_empty() {
        return cells;
    }
    let visible_bars = bars.get(start..start.saturating_add(slots).min(bars.len())).unwrap_or_default();
    if visible_bars.is_empty() { return cells; }
    let lifetime = u64::from(settings.lookback_hours) * 3_600_000;
    let gaps = minute_bars.windows(2).filter_map(|pair| {
        let end = pair[0].open_time_ms.saturating_add(60_000);
        (end != pair[1].open_time_ms).then_some((end, pair[1].open_time_ms))
    }).collect::<Vec<_>>();
    let mut unavailable_volume = Vec::with_capacity(minute_bars.len() + 1);
    unavailable_volume.push(0usize);
    for bar in minute_bars {
        unavailable_volume.push(unavailable_volume.last().copied().unwrap_or(0)
            + usize::from(bar.volume.is_none()));
    }
    let buckets = visible_bars.iter().map(|bar| {
        let end = bar.open_time_ms.saturating_add(interval_ms).min(now_ms);
        let cutoff = bar.open_time_ms.saturating_sub(lifetime);
        let first = minute_bars.partition_point(|minute| minute.open_time_ms < bar.open_time_ms);
        let last = minute_bars.partition_point(|minute| minute.open_time_ms < end);
        let lookback_first = minute_bars.partition_point(|minute| minute.open_time_ms < cutoff);
        let gap_index = gaps.partition_point(|gap| gap.1 <= cutoff);
        let covered = end > bar.open_time_ms && minute_bars.first().is_some_and(|minute| minute.open_time_ms <= cutoff)
            && first < last && minute_bars[first].open_time_ms == bar.open_time_ms
            && minute_bars[last - 1].open_time_ms.saturating_add(60_000) >= end
            && gaps.get(gap_index).is_none_or(|gap| gap.0 >= end)
            && unavailable_volume[last] == unavailable_volume[lookback_first];
        (bar.open_time_ms, end, covered)
    }).collect::<Vec<_>>();
    let decay = std::f64::consts::LN_2 / (f64::from(settings.half_life_hours.max(1)) * 3_600_000.0);
    let count = buckets.len();
    let mut changes = vec![0.0_f64; ROWS * (count + 1)];
    let mut edges = vec![0.0_f64; ROWS * count];
    let starts = buckets.iter().map(|bucket| bucket.0).collect::<Vec<_>>();
    let ends = buckets.iter().map(|bucket| bucket.1).collect::<Vec<_>>();
    for band in model.bands() {
        let price = decimal_to_f64(band.price);
        if price < range.low || price >= range.high {
            continue;
        }
        let row = (((range.high - price) / (range.high - range.low) * ROWS as f64).floor()
            as usize)
            .min(ROWS - 1);
        let weight = decimal_to_f64(band.weight);
        let preview_retired = live_extrema.and_then(|(open_time_ms, high, low)| {
            let touched = if band.long { band.price >= low } else { band.price <= high };
            (touched && band.valid_from_ms <= open_time_ms.saturating_add(1))
                .then_some(open_time_ms.saturating_add(1))
        });
        let end = band.retired_at_ms.or(preview_retired).unwrap_or(u64::MAX)
            .min(band.valid_from_ms.saturating_add(lifetime));
        let first = ends.partition_point(|time| *time <= band.valid_from_ms);
        let last = starts.partition_point(|time| *time < end);
        if first >= last || !weight.is_finite() || weight <= 0.0 { continue; }
        let integrate = |bucket: usize| {
            let a = starts[bucket].max(band.valid_from_ms);
            let b = ends[bucket].min(end);
            if a >= b { return 0.0; }
            weight / decay * ((-((a - band.valid_from_ms) as f64) * decay).exp()
                - (-((b - band.valid_from_ms) as f64) * decay).exp())
        };
        edges[row * count + first] += integrate(first);
        if last > first + 1 {
            edges[row * count + last - 1] += integrate(last - 1);
        }
        if last > first + 2 {
            // Store the active weight at each local bucket boundary. A single
            // exponential relative to the viewport's first bar overflows when
            // daily history spans many half-lives.
            changes[row * (count + 1) + first + 1] += weight *
                (-((starts[first + 1] - band.valid_from_ms) as f64) * decay).exp();
            changes[row * (count + 1) + last - 1] -= weight *
                (-((starts[last - 1] - band.valid_from_ms) as f64) * decay).exp();
        }
    }
    let mut bucket_values = vec![0.0_f64; ROWS * count];
    for row in 0..ROWS {
        let mut active = 0.0;
        let mut previous_start = buckets[0].0;
        for (index, &(from, to, covered)) in buckets.iter().enumerate() {
            active = active * (-((from - previous_start) as f64) * decay).exp()
                + changes[row * (count + 1) + index];
            previous_start = from;
            if !covered { continue; }
            let integral = edges[row * count + index] + active / decay *
                (1.0 - (-((to - from) as f64) * decay).exp());
            bucket_values[row * count + index] = (integral / (to - from) as f64).max(0.0);
        }
    }
    for column in 0..columns {
        let index = ((column as f64 + 0.5) / columns as f64 * slots as f64).floor() as usize;
        if index >= count { continue; }
        for row in 0..ROWS {
            cells[row * columns + column] = bucket_values[row * count + index];
        }
    }
    cells
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn raster_chunked(
    source: &[UiBar],
    entries: &[rust_decimal::Decimal],
    display: &[UiBar],
    slots: usize,
    range: PriceRange,
    settings: &Settings,
    interval_ms: u64,
    now_ms: u64,
    live_extrema: Option<(u64, rust_decimal::Decimal, rust_decimal::Decimal)>,
    price_tick: Option<rust_decimal::Decimal>,
    reuse: Option<&Cache>,
) -> ChunkRaster {
    raster_chunked_cancellable(source, entries, display, slots, range, settings,
        interval_ms, now_ms, live_extrema, price_tick, reuse, None)
}

#[allow(clippy::too_many_arguments)]
fn raster_chunked_cancellable(
    source: &[UiBar],
    entries: &[rust_decimal::Decimal],
    display: &[UiBar],
    slots: usize,
    range: PriceRange,
    settings: &Settings,
    interval_ms: u64,
    now_ms: u64,
    live_extrema: Option<(u64, rust_decimal::Decimal, rust_decimal::Decimal)>,
    price_tick: Option<rust_decimal::Decimal>,
    reuse: Option<&Cache>,
    cancel: Option<&AtomicBool>,
) -> ChunkRaster {
    let mut cells = vec![0.0; ROWS * COLUMNS];
    let mut maximum = 0.0_f64;
    let mut hashes = vec![0_u64; COLUMNS];
    let mut maxima = vec![0.0_f64; COLUMNS];
    let mut reused_columns = 0;
    let mut previous_column: Option<(usize, Vec<f64>, u64, f64)> = None;
    if slots == 0 { return ChunkRaster { cells, maximum, hashes, maxima, reused_columns }; }
    let prior_columns = reuse.map(|cache| {
        (0..COLUMNS).filter_map(|old_column| {
            let old_index = ((old_column as f64 + 0.5) / COLUMNS as f64
                * cache.slots as f64).floor() as usize;
            Some((cache.display_times.get(old_index).copied()?, old_column))
        }).collect::<std::collections::BTreeMap<_, _>>()
    });
    let lifetime = u64::from(settings.lookback_hours) * 3_600_000;
    for column in 0..COLUMNS {
        if cancel.is_some_and(|cancel| cancel.load(Ordering::Relaxed)) { break; }
        let index = ((column as f64 + 0.5) / COLUMNS as f64 * slots as f64).floor() as usize;
        let Some(bar) = display.get(index) else { continue; };
        if let Some((previous_index, values, hash, column_maximum)) = &previous_column
            && *previous_index == index {
            for row in 0..ROWS { cells[row * COLUMNS + column] = values[row]; }
            hashes[column] = *hash;
            maxima[column] = *column_maximum;
            maximum = maximum.max(*column_maximum);
            continue;
        }
        let cutoff = bar.open_time_ms.saturating_sub(lifetime);
        let end = bar.open_time_ms.saturating_add(interval_ms).min(now_ms);
        let first = source.partition_point(|minute| minute.open_time_ms < cutoff);
        let last = source.partition_point(|minute| minute.open_time_ms < end);
        if first >= last { continue; }
        let minutes = &source[first..last];
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        bar.open_time_ms.hash(&mut hasher);
        bar.open.hash(&mut hasher);
        bar.high.hash(&mut hasher);
        bar.low.hash(&mut hasher);
        bar.close.hash(&mut hasher);
        bar.volume.hash(&mut hasher);
        for (minute, entry) in minutes.iter().zip(&entries[first..last]) {
            minute.open_time_ms.hash(&mut hasher);
            minute.open.hash(&mut hasher);
            minute.high.hash(&mut hasher);
            minute.low.hash(&mut hasher);
            minute.close.hash(&mut hasher);
            minute.volume.hash(&mut hasher);
            entry.hash(&mut hasher);
        }
        if live_extrema.is_some_and(|(open_time_ms, _, _)| end > open_time_ms) {
            live_extrema.hash(&mut hasher);
            (now_ms / 60_000).hash(&mut hasher);
        }
        let hash = hasher.finish();
        if let Some((cache, old_column)) = reuse.zip(prior_columns.as_ref())
            .and_then(|(cache, columns)| columns.get(&bar.open_time_ms).map(|column| (cache, *column)))
            .filter(|(cache, old_column)| cache.column_hashes[*old_column] == hash) {
            reused_columns += 1;
            let values = (0..ROWS).map(|row| cache.cells[row * COLUMNS + old_column]).collect::<Vec<_>>();
            for (row, value) in values.iter().enumerate() { cells[row * COLUMNS + column] = *value; }
            let column_maximum = cache.column_maxima[old_column];
            hashes[column] = hash;
            maxima[column] = column_maximum;
            maximum = maximum.max(column_maximum);
            previous_column = Some((index, values, hash, column_maximum));
            continue;
        }
        let model = build_model(minutes, &entries[first..last], 60_000, settings, price_tick);
        let column_maximum = model.bands().iter().map(|band| decimal_to_f64(band.weight))
            .fold(0.0_f64, f64::max);
        maximum = maximum.max(column_maximum);
        let column_cells = raster_columns(&model, std::slice::from_ref(bar), 0, 1,
            range, settings, interval_ms, minutes, now_ms, live_extrema, 1);
        for row in 0..ROWS { cells[row * COLUMNS + column] = column_cells[row]; }
        hashes[column] = hash;
        maxima[column] = column_maximum;
        previous_column = Some((index, column_cells, hash, column_maximum));
    }
    ChunkRaster { cells, maximum, hashes, maxima, reused_columns }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn releasing_cached_result_does_not_cancel_active_worker() {
        let context = egui::Context::default();
        let id = egui::Id::new("budgeted-heatmap");
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let pending = Pending { demand: Demand { scope: None,
            source: SourceStamp::new(&[], None), display_times: Vec::new(), live_extrema: None,
            interval_ms: 60_000, settings: Settings::default(), offset: 0, start: 0, slots: 0,
            low: 0.0, high: 1.0, price_tick: None, render_minute: 0 },
            token: 1, running: true, cancel: Some(cancel_flag.clone()) };
        context.data_mut(|data| data.insert_temp(id.with("liquidation-scenario-pending"), pending));
        release_result(&context, id);
        assert!(!cancel_flag.load(Ordering::Relaxed));
        assert!(context.data(|data| data.get_temp::<Pending>(
            id.with("liquidation-scenario-pending"))).is_some_and(|pending| pending.running));
        cancel(&context, id);
        assert!(cancel_flag.load(Ordering::Relaxed));
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn two_full_minute_windows_fit_worker_reservations() {
        let one = estimated_async_input_bytes(crate::market::MAX_BASE_MINUTE_BARS,
            crate::chart::MAX_VISIBLE_BARS);
        assert!(one.saturating_mul(2) <= MAX_PENDING_INPUT_BYTES);
        assert!(InputReservation::try_new(MAX_PENDING_INPUT_BYTES + 1).is_none());
    }

    #[test]
    fn closing_chart_releases_its_cached_raster() {
        let cache = compute_async_cache(AsyncInput {
            source: Arc::new(Vec::new()), studies: None,
            entries: Some(Arc::new(Vec::new())), approximated: Some(0),
            display: Vec::new(), display_times: Vec::new(), scope: None,
            source_stamp: SourceStamp::new(&[], Some((1, 1))), live_extrema: None,
            interval_ms: 60_000, settings: Settings::default(), offset: 0,
            start: 0, slots: 0, range: PriceRange { low: 1.0, high: 2.0 },
            price_tick: None, display_start: 0, display_end: 0, now_ms: 0,
            render_minute: 0, reuse: None, cancel: Arc::new(AtomicBool::new(false)),
        });
        let weak = Arc::downgrade(&cache);
        let context = egui::Context::default();
        let id = egui::Id::new("chart-that-closes");
        let key = id.with("liquidation-scenario-raster");
        let mut output = context.run_ui(Default::default(), |ui| {
            ui.ctx().data_mut(|data| data.insert_temp(key, cache.clone()));
            crate::chart_view::mark_indicator_cache(ui.ctx(),
                crate::chart_view::IndicatorCacheKind::Heatmap, id);
            crate::chart_view::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        drop(cache);
        assert!(weak.upgrade().is_some());
        let mut output = context.run_ui(Default::default(), |ui| {
            crate::chart_view::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn budgeted_heatmap_still_releases_late_result_after_chart_closes() {
        let cache = compute_async_cache(AsyncInput {
            source: Arc::new(Vec::new()), studies: None,
            entries: Some(Arc::new(Vec::new())), approximated: Some(0),
            display: Vec::new(), display_times: Vec::new(), scope: None,
            source_stamp: SourceStamp::new(&[], Some((1, 1))), live_extrema: None,
            interval_ms: 60_000, settings: Settings::default(), offset: 0,
            start: 0, slots: 0, range: PriceRange { low: 1.0, high: 2.0 },
            price_tick: None, display_start: 0, display_end: 0, now_ms: 0,
            render_minute: 0, reuse: None, cancel: Arc::new(AtomicBool::new(false)),
        });
        let weak = Arc::downgrade(&cache);
        let context = egui::Context::default();
        let id = egui::Id::new("budgeted-chart-that-closes");
        let key = id.with("liquidation-scenario-raster");
        let mut output = context.run_ui(Default::default(), |ui| {
            ui.ctx().data_mut(|data| data.insert_temp(key, cache.clone()));
            crate::chart_view::mark_indicator_cache(ui.ctx(),
                crate::chart_view::IndicatorCacheKind::Heatmap, id);
            crate::chart_view::evict_indicator_caches_to_budget(ui.ctx(), 0);
        });
        output.textures_delta.clear();
        assert!(context.data(|data| data.get_temp::<Arc<Cache>>(key)).is_none());
        assert!(context.data(|data| data.get_temp::<Vec<crate::chart_view::IndicatorCacheEntry>>(
            crate::chart_view::indicator_cache_registry_id())).is_some());
        context.data_mut(|data| data.insert_temp(key, cache.clone()));
        drop(cache);
        let mut output = context.run_ui(Default::default(), |ui| {
            crate::chart_view::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn obsolete_worker_result_cannot_replace_new_market_or_viewport() {
        let a = crate::market::MarketSelection::binance_usd_m(
            "DOGE/USDC", crate::chart::ChartInterval::OneMinute).unwrap().binding;
        let b = crate::market::MarketSelection::binance_usd_m(
            "BTC/USDC", crate::chart::ChartInterval::OneMinute).unwrap().binding;
        let demand = Demand {
            scope: Some(a), source: SourceStamp { revision: Some((1, 1)),
                first: Some(0), last: Some(60_000), len: 2 },
            display_times: vec![0, 60_000], live_extrema: None,
            interval_ms: 60_000, settings: Settings::default(), offset: 0,
            start: 0, slots: 2, low: 80.0, high: 120.0,
            price_tick: None, render_minute: 0,
        };
        let mut first = Pending::latest(None, demand.clone());
        first.running = true;
        let cancelled = Arc::new(AtomicBool::new(false));
        first.cancel = Some(cancelled.clone());
        let duplicate = Pending::latest(Some(first.clone()), demand.clone());
        assert!(duplicate.running && duplicate.accepts(first.token, &demand));
        let mut switched = demand.clone();
        switched.scope = Some(b);
        let next = Pending::latest(Some(first), switched.clone());
        assert!(!next.running && !next.accepts(duplicate.token, &demand));
        assert!(cancelled.load(Ordering::Relaxed));
        let mut moved = switched.clone();
        moved.display_times = vec![60_000, 120_000];
        let latest = Pending::latest(Some(next.clone()), moved.clone());
        assert!(!latest.accepts(next.token, &switched));
        assert!(latest.accepts(latest.token, &moved));
    }

    #[test]
    fn cancelled_long_raster_stops_before_model_calculation() {
        let bar = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let cancelled = AtomicBool::new(true);
        let result = raster_chunked_cancellable(std::slice::from_ref(&bar),
            &[100.into()], std::slice::from_ref(&bar), 1,
            PriceRange { low: 80.0, high: 120.0 }, &Settings::default(),
            60_000, 60_000, None, None, None, Some(&cancelled));
        assert_eq!(result.maximum, 0.0);
        assert!(result.cells.iter().all(|value| *value == 0.0));
    }

    #[test]
    fn large_async_result_does_not_retain_duplicate_minute_inputs() {
        let count = MAX_RETAINED_INPUT_BYTES
            / (std::mem::size_of::<UiBar>() + std::mem::size_of::<rust_decimal::Decimal>()) + 1;
        let source = (0..count).map(|index| UiBar {
            open_time_ms: index as u64 * 60_000,
            open: 100.into(), high: 100.into(), low: 100.into(), close: 100.into(),
            volume: Some(10.into()),
        }).collect::<Vec<_>>();
        let stamp = SourceStamp::new(&source, Some((1, 1)));
        let cache = compute_async_cache(AsyncInput {
            source: Arc::new(source), studies: None,
            entries: Some(Arc::new(vec![100.into(); count])), approximated: Some(0),
            display: Vec::new(), display_times: Vec::new(), scope: None,
            source_stamp: stamp, live_extrema: None, interval_ms: 60_000,
            settings: Settings::default(), offset: 0, start: 0, slots: 0,
            range: PriceRange { low: 80.0, high: 120.0 }, price_tick: None,
            display_start: 0, display_end: 0, now_ms: 0, render_minute: 0,
            reuse: None, cancel: Arc::new(AtomicBool::new(false)),
        });
        assert!(cache.source_snapshot.is_none());
        assert!(cache.entries_snapshot.is_none());
        assert!(cache_retained_bytes(&cache) < MAX_RETAINED_INPUT_BYTES);
    }

    #[test]
    #[ignore = "large four-month capacity and latency probe"]
    fn four_month_daily_history_streams_in_bounded_chunks() {
        let days = 4 * 31;
        let count = days * 24 * 60;
        let minute = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let source = (0..count).map(|index| UiBar {
            open_time_ms: index as u64 * 60_000, ..minute.clone()
        }).collect::<Vec<_>>();
        let studies = vec![crate::chart::BaseMinuteStudy {
            confirmed: true, bar_vwap: Some(100.into()),
        }; source.len()];
        let display = (0..days).map(|index| UiBar {
            open_time_ms: index as u64 * 86_400_000, ..minute.clone()
        }).collect::<Vec<_>>();
        let followup_source = source.clone();
        let followup_display = display.clone();
        let started = std::time::Instant::now();
        let cache = compute_async_cache(AsyncInput {
            source: Arc::new(source),
            studies: Some(studies),
            entries: None,
            approximated: None,
            display_times: display.iter().map(|bar| bar.open_time_ms).collect(),
            display,
            scope: None,
            source_stamp: SourceStamp { revision: Some((1, 1)), first: Some(0),
                last: Some((count as u64 - 1) * 60_000), len: count },
            live_extrema: None,
            interval_ms: 86_400_000,
            settings: Settings::default(),
            offset: 0,
            start: 0,
            slots: days,
            range: PriceRange { low: 80.0, high: 120.0 },
            price_tick: None,
            display_start: 0,
            display_end: days as u64 * 86_400_000,
            now_ms: days as u64 * 86_400_000,
            render_minute: 0,
            reuse: None,
            cancel: Arc::new(AtomicBool::new(false)),
        });
        println!("four-month heatmap computation: {:?}", started.elapsed());
        assert!(cache.complete);
        assert!(cache.source_snapshot.is_none());
        assert!(cache.entries_snapshot.is_none());
        assert!(cache.maximum > 0.0);
        assert!(cache.cells.iter().all(|value| value.is_finite()));
        assert!(cache.cells.iter().any(|value| *value > 0.0));
        let started = std::time::Instant::now();
        let warmed = raster_chunked(&followup_source,
            &vec![100.into(); followup_source.len()], &followup_display, days,
            PriceRange { low: 80.0, high: 120.0 }, &Settings::default(),
            86_400_000, days as u64 * 86_400_000, None, None, Some(cache.as_ref()));
        println!("four-month unchanged follow-up: {:?}, reused {} columns",
            started.elapsed(), warmed.reused_columns);
        assert_eq!(warmed.maximum, cache.maximum);
        assert_eq!(warmed.cells, cache.cells);
        assert!(warmed.reused_columns >= days);
    }
    #[test]
    fn chunked_long_view_matches_monolithic_minute_scenarios() {
        let mut settings = Settings::default();
        settings.lookback_hours = 1;
        let template = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let minutes = (0..120).map(|index| UiBar {
            open_time_ms: index * 60_000, ..template.clone()
        }).collect::<Vec<_>>();
        let display = [template.clone(), UiBar { open_time_ms: 3_600_000, ..template }];
        let entries = vec![100.into(); minutes.len()];
        let range = PriceRange { low: 80.0, high: 120.0 };
        let model = build_model(&minutes, &entries, 60_000, &settings, None);
        let old = raster(&model, &display, 0, 2, range, &settings,
            3_600_000, &minutes, 7_200_000, None);
        let chunked = raster_chunked(&minutes, &entries, &display, 2,
            range, &settings, 3_600_000, 7_200_000, None, None, None);
        assert!(chunked.maximum > 0.0);
        assert!(old.iter().zip(chunked.cells).all(|(left, right)| (left - right).abs() < 1e-9));
    }
    #[test]
    fn daily_chunked_history_preserves_coverage_and_strength() {
        let mut settings = Settings::default();
        settings.lookback_hours = 1;
        let template = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let minutes = (0..3 * 24 * 60).map(|index| UiBar {
            open_time_ms: index * 60_000, ..template.clone()
        }).collect::<Vec<_>>();
        let display = (0..3).map(|index| UiBar {
            open_time_ms: index * 86_400_000, ..template.clone()
        }).collect::<Vec<_>>();
        let entries = vec![100.into(); minutes.len()];
        let range = PriceRange { low: 80.0, high: 120.0 };
        let model = build_model(&minutes, &entries, 60_000, &settings, None);
        let monolithic = raster(&model, &display, 0, display.len(), range, &settings,
            86_400_000, &minutes, 3 * 86_400_000, None);
        let chunked = raster_chunked(&minutes, &entries, &display, display.len(),
            range, &settings, 86_400_000, 3 * 86_400_000, None, None, None);
        assert!(monolithic.iter().zip(chunked.cells).all(|(left, right)| (left - right).abs() < 1e-9));
    }
    #[test]
    fn changed_tail_reuses_only_unchanged_historical_columns() {
        let mut settings = Settings::default();
        settings.lookback_hours = 1;
        let template = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let mut minutes = (0..20 * 60).map(|index| UiBar {
            open_time_ms: index * 60_000, ..template.clone()
        }).collect::<Vec<_>>();
        let display = (0..20).map(|index| UiBar {
            open_time_ms: index * 3_600_000, ..template.clone()
        }).collect::<Vec<_>>();
        let entries = vec![100.into(); minutes.len()];
        let range = PriceRange { low: 80.0, high: 120.0 };
        let original = compute_async_cache(AsyncInput {
            source: Arc::new(minutes.clone()),
            studies: Some(vec![crate::chart::BaseMinuteStudy { confirmed: true,
                bar_vwap: Some(100.into()) }; minutes.len()]),
            entries: None,
            approximated: None,
            display: display.clone(),
            display_times: display.iter().map(|bar| bar.open_time_ms).collect(),
            scope: None,
            source_stamp: SourceStamp::new(&minutes, Some((1, 1))),
            live_extrema: None,
            interval_ms: 3_600_000,
            settings: settings.clone(),
            offset: 0,
            start: 0,
            slots: display.len(),
            range,
            price_tick: None,
            display_start: 0,
            display_end: 20 * 3_600_000,
            now_ms: 20 * 3_600_000,
            render_minute: 0,
            reuse: None,
            cancel: Arc::new(AtomicBool::new(false)),
        });
        assert!(original.source_snapshot.is_some());
        assert!(original.entries_snapshot.is_some());
        minutes.last_mut().unwrap().volume = Some(100.into());
        let warmed = raster_chunked(&minutes, &entries, &display, display.len(),
            range, &settings, 3_600_000, 20 * 3_600_000, None, None, Some(original.as_ref()));
        let rebuilt = raster_chunked(&minutes, &entries, &display, display.len(),
            range, &settings, 3_600_000, 20 * 3_600_000, None, None, None);
        assert!(warmed.reused_columns >= 18);
        assert_ne!(warmed.hashes, original.column_hashes);
        assert_eq!(warmed.hashes, rebuilt.hashes);
        assert_eq!(warmed.maximum, rebuilt.maximum);
        assert_eq!(warmed.cells, rebuilt.cells);
        let shifted = &display[1..];
        let shifted_warm = raster_chunked(&minutes, &entries, shifted, shifted.len(),
            range, &settings, 3_600_000, 20 * 3_600_000, None, None, Some(original.as_ref()));
        let shifted_full = raster_chunked(&minutes, &entries, shifted, shifted.len(),
            range, &settings, 3_600_000, 20 * 3_600_000, None, None, None);
        assert!(shifted_warm.reused_columns >= 18);
        assert_eq!(shifted_warm.maximum, shifted_full.maximum);
        assert_eq!(shifted_warm.cells, shifted_full.cells);

        let preview_at = 20 * 3_600_000 - 60_000;
        let earlier_preview = Some((preview_at, 100.into(), 100.into()));
        let later_preview = Some((preview_at, 110.into(), 90.into()));
        let initial = raster_chunked(&minutes, &entries, &display, display.len(),
            range, &settings, 3_600_000, 20 * 3_600_000, earlier_preview, None, None);
        let mut prior = (*original).clone();
        prior.cells = initial.cells;
        prior.column_hashes = initial.hashes;
        prior.column_maxima = initial.maxima;
        let warmed_preview = raster_chunked(&minutes, &entries, &display, display.len(),
            range, &settings, 3_600_000, 20 * 3_600_000, later_preview, None, Some(&prior));
        let rebuilt_preview = raster_chunked(&minutes, &entries, &display, display.len(),
            range, &settings, 3_600_000, 20 * 3_600_000, later_preview, None, None);
        assert!(warmed_preview.reused_columns >= 18);
        assert_eq!(warmed_preview.hashes, rebuilt_preview.hashes);
        assert_eq!(warmed_preview.maximum, rebuilt_preview.maximum);
        assert_eq!(warmed_preview.cells, rebuilt_preview.cells);
    }
    #[test]
    fn unavailable_volume_masks_only_affected_history_window() {
        let mut settings = Settings::default();
        settings.lookback_hours = 1;
        let template = UiBar {
            open_time_ms: 0,
            open: 100.into(),
            high: 100.into(),
            low: 100.into(),
            close: 100.into(),
            volume: Some(10.into()),
        };
        let mut minutes = (0..120).map(|index| UiBar {
            open_time_ms: index * 60_000,
            ..template.clone()
        }).collect::<Vec<_>>();
        minutes[10].volume = None;
        let model = build_model(&minutes, &vec![100.into(); minutes.len()],
            60_000, &settings, None);
        let range = PriceRange { low: 80.0, high: 120.0 };
        let display = [minutes[20].clone(), minutes[110].clone()];
        let cells = raster(&model, &display, 0, 2, range, &settings,
            60_000, &minutes, 120 * 60_000, None);
        assert!(cells.iter().enumerate().filter(|(index, _)| index % COLUMNS < COLUMNS / 2)
            .all(|(_, value)| *value == 0.0));
        assert!(cells.iter().enumerate().filter(|(index, _)| index % COLUMNS >= COLUMNS / 2)
            .any(|(_, value)| *value > 0.0));
    }
    #[test]
    fn live_touch_cuts_extension_without_seeding_or_repainting_history() {
        let bar = UiBar {
            open_time_ms: 0,
            open: 100.into(),
            high: 100.into(),
            low: 100.into(),
            close: 100.into(),
            volume: Some(10.into()),
        };
        let mut model = build_model(&[bar.clone()], &[100.into()], 60_000, &Settings::default(), None);
        let before = model.bands().to_vec();
        model.retire(120_000, 120.into(), 80.into());
        assert_eq!(model.bands().len(), before.len());
        assert!(model.bands().iter().all(|band| band.retired_at_ms == Some(120_000)));
        let range = PriceRange {
            low: 80.0,
            high: 120.0,
        };
        let next = UiBar { open_time_ms: 60_000, ..bar.clone() };
        let cells = raster(&model, &[bar.clone(), next.clone()], 0, 4, range,
            &Settings::default(), 60_000, &[bar, next], 120_000, None);
        assert!(
            cells
                .iter()
                .enumerate()
                .filter(|(i, _)| i % COLUMNS < COLUMNS / 4)
                .all(|(_, value)| *value == 0.0)
        );
        assert!(cells.iter().any(|value| *value > 0.0));
        model.retire(120_000, 100.into(), 100.into());
        assert!(model.bands().iter().all(|band| band.retired_at_ms == Some(120_000)));
    }
    #[test]
    fn time_window_is_bounded_and_excludes_the_forming_bar() {
        let bars = (0..2_001)
            .map(|index| UiBar {
                open_time_ms: index * 60_000,
                open: 100.into(),
                high: 100.into(),
                low: 100.into(),
                close: 100.into(),
                volume: Some(10.into()),
            })
            .collect::<Vec<_>>();
        assert_eq!(history_range(&bars, 1_999*60_000..2_001*60_000, 1, false), 1_939..2_000);
        assert_eq!(history_range(&bars, 1_999*60_000..2_001*60_000, 4, false), 1_759..2_000);
        assert_eq!(history_range(&bars, 1_999*60_000..2_001*60_000, 24, false), 559..2_000);
        assert_eq!(history_range(&bars, 100*60_000..200*60_000, 1, true), 40..200);
        assert_eq!(history_range(&[], 0..0, 4, false), 0..0);
    }
    #[test]
    fn bands_are_causal_and_never_fill_before_seed() {
        let bar = UiBar {
            open_time_ms: 0,
            open: 100.into(),
            high: 100.into(),
            low: 100.into(),
            close: 100.into(),
            volume: Some(10.into()),
        };
        let range = PriceRange {
            low: 80.0,
            high: 120.0,
        };
        let model = build_model(&[bar.clone()], &[100.into()], 60_000, &Settings::default(), None);
        let next = UiBar { open_time_ms: 60_000, ..bar.clone() };
        let cells = raster(&model, &[bar.clone(), next.clone()], 0, 4, range,
            &Settings::default(), 60_000, &[bar, next], 120_000, None);
        assert!(
            cells
                .iter()
                .enumerate()
                .filter(|(i, _)| i % COLUMNS < COLUMNS / 4)
                .all(|(_, v)| *v == 0.0)
        );
        assert!(cells.iter().any(|v| *v > 0.0));
    }
    #[test]
    fn hour_mean_matches_four_quarter_hour_means_for_the_same_minute_facts() {
        let bar = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let minutes = (0..60).map(|index| UiBar { open_time_ms: index * 60_000, ..bar.clone() })
            .collect::<Vec<_>>();
        let quarter_hours = (0..4).map(|index| UiBar { open_time_ms: index * 900_000, ..bar.clone() })
            .collect::<Vec<_>>();
        let model = build_model(&minutes[..1], &[100.into()], 60_000, &Settings::default(), None);
        let range = PriceRange { low: 80.0, high: 120.0 };
        let quarters = raster(&model, &quarter_hours, 0, 4, range, &Settings::default(),
            900_000, &minutes, 3_600_000, None);
        let hour = raster(&model, &[bar], 0, 1, range, &Settings::default(),
            3_600_000, &minutes, 3_600_000, None);
        let row = model.bands().iter().find(|band| band.long).map(|band| {
            (((range.high - decimal_to_f64(band.price)) / (range.high - range.low) * ROWS as f64)
                .floor() as usize).min(ROWS - 1)
        }).unwrap_or(0);
        let quarter_mean = [20, 60, 100, 140].iter().map(|column| quarters[row * COLUMNS + column]).sum::<f64>() / 4.0;
        let hour_mean = hour[row * COLUMNS + 20];
        assert!(hour_mean > 0.0);
        assert!((quarter_mean - hour_mean).abs() < hour_mean * 1e-9);
    }

    #[test]
    fn preview_touch_matches_retirement_without_cloning_the_model() {
        let minute = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let next = UiBar { open_time_ms: 60_000, ..minute.clone() };
        let model = build_model(&[minute.clone()], &[100.into()], 60_000, &Settings::default(), None);
        let range = PriceRange { low: 80.0, high: 120.0 };
        let preview = raster(&model, &[minute.clone(), next.clone()], 0, 2, range,
            &Settings::default(), 60_000, &[minute.clone(), next.clone()], 120_000,
            Some((60_000, 120.into(), 80.into())));
        let mut retired = model.clone();
        retired.retire(60_001, 120.into(), 80.into());
        let committed = raster(&retired, &[minute.clone(), next.clone()], 0, 2, range,
            &Settings::default(), 60_000, &[minute, next], 120_000, None);
        assert_eq!(preview, committed);
        assert!(model.bands().iter().all(|band| band.retired_at_ms.is_none()));
    }

    #[test]
    fn late_history_stays_finite_after_many_half_lives() {
        let late = 50 * 86_400_000;
        let bar = UiBar { open_time_ms: 0, open: 100.into(), high: 100.into(),
            low: 100.into(), close: 100.into(), volume: Some(10.into()) };
        let display = std::iter::once(bar.clone()).chain((0..4).map(|index| UiBar {
            open_time_ms: late + index * 60_000, ..bar.clone()
        })).collect::<Vec<_>>();
        let minutes = std::iter::once(bar.clone()).chain((0..64).map(|index| UiBar {
            open_time_ms: late - 3_600_000 + index * 60_000, ..bar.clone()
        })).collect::<Vec<_>>();
        let mut settings = Settings::default();
        settings.half_life_hours = 1;
        settings.lookback_hours = 1;
        let model = build_model(&display[1..2], &[100.into()], 60_000, &settings, None);
        let cells = raster(&model, &display, 0, 5, PriceRange { low: 80.0, high: 120.0 },
            &settings, 60_000, &minutes, late + 240_000, None);
        let row = model.bands().iter().find(|band| band.long).map(|band| {
            (((120.0 - decimal_to_f64(band.price)) / 40.0 * ROWS as f64).floor()
                as usize).min(ROWS - 1)
        }).unwrap_or(0);
        assert!(cells[row * COLUMNS + 110].is_finite());
        assert!(cells[row * COLUMNS + 110] > 0.0);
    }
}
