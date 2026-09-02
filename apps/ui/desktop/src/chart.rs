use std::ops::Range;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_control_protocol::UiBar;

pub const DEFAULT_VISIBLE_BARS: usize = 120;
pub const MIN_VISIBLE_BARS: usize = 20;
pub const MAX_VISIBLE_BARS: usize = 400;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChartStudyPoint {
    pub custom_ema_adx: Option<venue_indicators::chart::EmaAdxValues>,
    pub open_time_ms: u64,
    pub confirmed: bool,
    pub sma: Option<Decimal>,
    pub sma_second: Option<Decimal>,
    pub sma_third: Option<Decimal>,
    pub ema: Option<Decimal>,
    pub ema_second: Option<Decimal>,
    pub ema_third: Option<Decimal>,
    pub wma: Option<Decimal>,
    pub wma_second: Option<Decimal>,
    pub wma_third: Option<Decimal>,
    pub bollinger_upper: Option<Decimal>,
    pub bollinger_middle: Option<Decimal>,
    pub bollinger_lower: Option<Decimal>,
    pub vwap: Option<Decimal>,
    pub avl: Option<Decimal>,
    pub trix: Option<Decimal>,
    pub sar: Option<Decimal>,
    pub sar_rising: bool,
    pub supertrend: Option<Decimal>,
    pub supertrend_rising: bool,
    pub rsi: Option<Decimal>,
    pub macd: Option<Decimal>,
    pub macd_signal: Option<Decimal>,
    pub macd_histogram: Option<Decimal>,
    pub atr: Option<Decimal>,
    pub mfi: Option<Decimal>,
    pub kdj_k: Option<Decimal>,
    pub kdj_d: Option<Decimal>,
    pub kdj_j: Option<Decimal>,
    pub obv: Option<Decimal>,
    pub cci: Option<Decimal>,
    pub stoch_rsi_k: Option<Decimal>,
    pub stoch_rsi_d: Option<Decimal>,
    pub williams_r: Option<Decimal>,
    pub dmi_plus: Option<Decimal>,
    pub dmi_minus: Option<Decimal>,
    pub dmi_adx: Option<Decimal>,
    pub momentum: Option<Decimal>,
    pub emv: Option<Decimal>,
}

/// UI-owned display interval. It deliberately has no exchange-native representation.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ChartInterval {
    #[default]
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
}

impl ChartInterval {
    pub const ALL: [Self; 6] = [
        Self::OneMinute,
        Self::FiveMinutes,
        Self::FifteenMinutes,
        Self::OneHour,
        Self::FourHours,
        Self::OneDay,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::FourHours => "4h",
            Self::OneDay => "1d",
        }
    }

    pub const fn duration_ms(self) -> u64 {
        match self {
            Self::OneMinute => 60_000,
            Self::FiveMinutes => 5 * 60_000,
            Self::FifteenMinutes => 15 * 60_000,
            Self::OneHour => 60 * 60_000,
            Self::FourHours => 4 * 60 * 60_000,
            Self::OneDay => 24 * 60 * 60_000,
        }
    }

    /// Wall-clock aligned spacing that keeps labels roughly fifteen candles apart.
    pub const fn timeline_step_ms(self) -> u64 {
        let bars = match self {
            Self::OneMinute => 15,
            Self::FiveMinutes => 12,
            Self::FifteenMinutes => 16,
            Self::OneHour => 12,
            Self::FourHours => 12,
            Self::OneDay => 14,
        };
        self.duration_ms() * bars
    }
}

pub fn format_timeline_label(open_time_ms: u64, interval: ChartInterval) -> String {
    let utc_seconds = i64::try_from(open_time_ms / 1_000).unwrap_or(i64::MAX);
    let total_seconds = utc_seconds.saturating_add(local_offset_seconds(utc_seconds));
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400) as u64;
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let (year, month, day) = civil_date_from_unix_days(days);
    match interval {
        ChartInterval::OneMinute | ChartInterval::FiveMinutes | ChartInterval::FifteenMinutes => {
            format!("{hour:02}:{minute:02}")
        }
        ChartInterval::OneHour | ChartInterval::FourHours => {
            format!("{month:02}-{day:02} {hour:02}:{minute:02}")
        }
        ChartInterval::OneDay => format!("{year:04}-{month:02}-{day:02}"),
    }
}

pub fn timeline_time_at_slot(bars: &[UiBar], interval: ChartInterval, index: usize) -> Option<u64> {
    if let Some(bar) = bars.get(index) {
        return Some(bar.open_time_ms);
    }
    let last = bars.last()?;
    let future_bars = u64::try_from(index.checked_sub(bars.len() - 1)?).ok()?;
    last.open_time_ms
        .checked_add(interval.duration_ms().checked_mul(future_bars)?)
}

fn local_offset_seconds(unix_seconds: i64) -> i64 {
    time::OffsetDateTime::from_unix_timestamp(unix_seconds)
        .ok()
        .and_then(|datetime| time::UtcOffset::local_offset_at(datetime).ok())
        .map_or(0, |offset| i64::from(offset.whole_seconds()))
}

fn civil_date_from_unix_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_phase = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_phase + 2) / 5 + 1;
    let month = month_phase + if month_phase < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

/// Persisted chart navigation state. `right_offset` counts bars hidden after the window, so a
/// zero offset follows the latest candle and a positive offset pans towards older candles.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartViewport {
    visible_bars: usize,
    right_offset: usize,
    right_padding: usize,
    #[serde(skip)]
    drag_remainder_milli_bars: i32,
    #[serde(skip)]
    drag_applied_milli_pixels: i32,
    /// Only used to keep an explicitly inspected historical window stable as new bars arrive.
    #[serde(skip)]
    last_total_bars: usize,
}

impl Default for ChartViewport {
    fn default() -> Self {
        Self {
            visible_bars: DEFAULT_VISIBLE_BARS,
            right_offset: 0,
            right_padding: 0,
            drag_remainder_milli_bars: 0,
            drag_applied_milli_pixels: 0,
            last_total_bars: 0,
        }
    }
}

impl ChartViewport {
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub fn history_prepended(&mut self, added: usize) {
        self.last_total_bars = self.last_total_bars.saturating_add(added);
    }
    pub const fn visible_bars(&self) -> usize {
        self.visible_bars
    }

    pub const fn right_offset(&self) -> usize {
        self.right_offset
    }

    /// Blank slots inside the fixed-width viewport, retained while following live candles.
    #[cfg(test)]
    pub const fn right_padding(&self) -> usize {
        self.right_padding
    }

    pub fn display_slots(&self, displayed_bars: usize) -> usize {
        displayed_bars.saturating_add(self.right_padding).max(1)
    }

    pub fn reset(&mut self) {
        self.visible_bars = DEFAULT_VISIBLE_BARS;
        self.right_offset = 0;
        self.right_padding = 0;
        self.drag_remainder_milli_bars = 0;
        self.drag_applied_milli_pixels = 0;
        self.last_total_bars = 0;
    }

    pub fn follow_latest(&mut self) {
        self.right_offset = 0;
        self.right_padding = 0;
        self.drag_remainder_milli_bars = 0;
        self.drag_applied_milli_pixels = 0;
        self.last_total_bars = 0;
    }

    pub fn visible_range(&mut self, total_bars: usize) -> Range<usize> {
        if total_bars == 0 {
            self.right_offset = 0;
            self.right_padding = 0;
            self.last_total_bars = 0;
            return 0..0;
        }
        self.visible_bars = self.visible_bars.clamp(MIN_VISIBLE_BARS, MAX_VISIBLE_BARS);
        let count = self.visible_bars.min(total_bars);
        if self.right_offset > 0 && total_bars > self.last_total_bars {
            self.right_offset = self
                .right_offset
                .saturating_add(total_bars.saturating_sub(self.last_total_bars));
        }
        let maximum_offset = total_bars.saturating_sub(count);
        self.right_offset = self.right_offset.min(maximum_offset);
        self.right_padding = if self.right_offset > 0 {
            0
        } else {
            self.right_padding.min(count.saturating_sub(1))
        };
        self.last_total_bars = total_bars;
        let end = total_bars.saturating_sub(self.right_offset);
        end.saturating_sub(count.saturating_sub(self.right_padding))..end
    }

    /// Positive values pan towards older bars; negative values pan back towards live data.
    pub fn pan_by_bars(&mut self, total_bars: usize, delta: isize) {
        let visible = self.visible_range(total_bars);
        let slots = self.display_slots(visible.len());
        let maximum_offset = total_bars.saturating_sub(slots);
        let amount = delta.unsigned_abs();
        if delta.is_negative() {
            let from_history = amount.min(self.right_offset);
            self.right_offset = self.right_offset.saturating_sub(from_history);
            let into_padding = amount
                .saturating_sub(from_history)
                .min(slots.saturating_sub(1).saturating_sub(self.right_padding));
            self.right_padding = self.right_padding.saturating_add(into_padding);
        } else {
            let from_padding = amount.min(self.right_padding);
            self.right_padding = self.right_padding.saturating_sub(from_padding);
            self.right_offset = self
                .right_offset
                .saturating_add(amount.saturating_sub(from_padding))
                .min(maximum_offset);
        }
    }

    /// Converts a horizontal drag into whole-candle panning. Rightward drags reveal older candles;
    /// leftward drags return towards live data and then make room after the latest candle.
    pub fn pan_by_drag(&mut self, total_bars: usize, chart_width: f32, drag_delta_x: f32) {
        let visible = self.visible_range(total_bars);
        if visible.is_empty() || !chart_width.is_finite() || chart_width <= 0.0 {
            return;
        }
        let width_per_bar = chart_width / self.display_slots(visible.len()) as f32;
        if !width_per_bar.is_finite() || width_per_bar <= 0.0 {
            return;
        }
        let delta_milli_bars = (drag_delta_x / width_per_bar * 1_000.0).round();
        if delta_milli_bars.is_finite() {
            let accumulated = self
                .drag_remainder_milli_bars
                .saturating_add(delta_milli_bars as i32);
            let whole_bars = accumulated / 1_000;
            self.drag_remainder_milli_bars = accumulated % 1_000;
            if whole_bars != 0 {
                self.pan_by_bars(total_bars, whole_bars as isize);
            }
        }
    }

    /// Applies the cumulative drag reported by the UI without counting earlier frames twice.
    pub fn pan_by_drag_total(&mut self, total_bars: usize, chart_width: f32, drag_total_x: f32) {
        if !drag_total_x.is_finite() {
            return;
        }
        let total_milli_pixels = (drag_total_x * 1_000.0).round() as i32;
        let delta_milli_pixels = total_milli_pixels.saturating_sub(self.drag_applied_milli_pixels);
        self.drag_applied_milli_pixels = total_milli_pixels;
        self.pan_by_drag(total_bars, chart_width, delta_milli_pixels as f32 / 1_000.0);
    }

    pub fn finish_drag(&mut self) {
        self.drag_remainder_milli_bars = 0;
        self.drag_applied_milli_pixels = 0;
    }

    /// Changes the visible count while retaining the bar under `anchor_ratio` in the same place.
    /// `0.0` is the oldest visible bar and `1.0` the newest visible bar.
    pub fn set_visible_bars_anchored(
        &mut self,
        total_bars: usize,
        requested_visible_bars: usize,
        anchor_ratio: f32,
    ) {
        let current = self.visible_range(total_bars);
        if current.is_empty() {
            self.visible_bars = requested_visible_bars.clamp(MIN_VISIBLE_BARS, MAX_VISIBLE_BARS);
            self.right_offset = 0;
            return;
        }
        let was_following_latest = self.right_offset == 0;
        let current_slots = self.display_slots(current.len());
        let ratio = anchor_ratio.clamp(0.0, 1.0);
        let current_anchor_offset =
            ((current.len().saturating_sub(1)) as f32 * ratio).round() as usize;
        let anchor_index = current.start.saturating_add(current_anchor_offset);
        self.visible_bars = requested_visible_bars.clamp(MIN_VISIBLE_BARS, MAX_VISIBLE_BARS);
        let new_count = self.visible_bars.min(total_bars);
        if was_following_latest {
            // Zoom changes candle density, not the user's chosen live-edge position.
            self.right_padding = self
                .right_padding
                .saturating_mul(new_count)
                .saturating_add(current_slots / 2)
                / current_slots;
            self.right_padding = self.right_padding.min(new_count.saturating_sub(1));
            return;
        }
        let new_anchor_offset = ((new_count.saturating_sub(1)) as f32 * ratio).round() as usize;
        let newest_allowed_start = total_bars.saturating_sub(new_count);
        let new_start = anchor_index
            .saturating_sub(new_anchor_offset)
            .min(newest_allowed_start);
        self.right_offset = total_bars.saturating_sub(new_start.saturating_add(new_count));
    }

    /// Positive steps zoom in and negative steps zoom out. Each step is about ten percent of the
    /// current visible window and remains within the chart-wide display limits.
    pub fn zoom_by_steps(&mut self, total_bars: usize, anchor_ratio: f32, steps: isize) {
        if steps == 0 {
            return;
        }
        let current = self.visible_range(total_bars);
        if current.is_empty() {
            return;
        }
        let increment = (self.display_slots(current.len()) / 10).max(1);
        let amount = increment.saturating_mul(steps.unsigned_abs());
        let requested = if steps.is_negative() {
            self.visible_bars.saturating_add(amount)
        } else {
            self.visible_bars.saturating_sub(amount)
        };
        self.set_visible_bars_anchored(total_bars, requested, anchor_ratio);
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PriceRange {
    pub low: f64,
    pub high: f64,
}

impl PriceRange {
    pub fn from_bars(bars: &[UiBar]) -> Option<Self> {
        let mut low = f64::INFINITY;
        let mut high = f64::NEG_INFINITY;
        for bar in bars {
            let Some(bar_low) = decimal_to_f64(bar.low) else {
                continue;
            };
            let Some(bar_high) = decimal_to_f64(bar.high) else {
                continue;
            };
            if !bar_low.is_finite() || !bar_high.is_finite() {
                continue;
            }
            low = low.min(bar_low.min(bar_high));
            high = high.max(bar_low.max(bar_high));
        }
        Self::from_extrema(low, high)
    }

    pub fn from_extrema(low: f64, high: f64) -> Option<Self> {
        if !low.is_finite() || !high.is_finite() || low > high {
            return None;
        }
        let span = high - low;
        let padding = if span > f64::EPSILON {
            span * 0.05
        } else {
            high.abs().max(1.0) * 0.005
        };
        Some(Self {
            low: low - padding,
            high: high + padding,
        })
    }

    pub fn price_to_y(self, top: f32, height: f32, price: f64) -> Option<f32> {
        if !top.is_finite() || !height.is_finite() || height <= 0.0 || !price.is_finite() {
            return None;
        }
        let span = self.high - self.low;
        if !span.is_finite() || span <= 0.0 {
            return None;
        }
        let y = top + height - (((price - self.low) / span) as f32 * height);
        y.is_finite().then_some(y)
    }

    pub fn y_to_price(self, top: f32, height: f32, y: f32) -> Option<f64> {
        if !top.is_finite() || !height.is_finite() || height <= 0.0 || !y.is_finite() {
            return None;
        }
        let span = self.high - self.low;
        if !span.is_finite() || span <= 0.0 {
            return None;
        }
        Some(self.high - f64::from((y - top) / height) * span)
    }

    /// Returns grid prices aligned to an integer number of exchange price ticks.
    pub fn grid_prices(self, price_scale: usize, target_rows: usize) -> Vec<f64> {
        let tick = 10_f64.powi(-(price_scale.min(18) as i32));
        let target_rows = target_rows.max(1) as f64;
        let ideal_ticks = ((self.high - self.low) / target_rows / tick)
            .ceil()
            .max(1.0);
        let magnitude = 10_f64.powf(ideal_ticks.log10().floor());
        let normalized = ideal_ticks / magnitude;
        let integer_ticks = if normalized <= 1.0 {
            1.0
        } else if normalized <= 2.0 {
            2.0
        } else if normalized <= 5.0 {
            5.0
        } else {
            10.0
        } * magnitude;
        let step = integer_ticks * tick;
        if !step.is_finite() || step <= 0.0 {
            return Vec::new();
        }
        let mut price = (self.low / step).ceil() * step;
        let mut lines = Vec::new();
        while price <= self.high + step * 0.000_001 && lines.len() < 128 {
            lines.push(price);
            price += step;
        }
        lines
    }
}

pub fn bar_center_x(left: f32, width: f32, count: usize, index: usize) -> Option<f32> {
    if !left.is_finite() || !width.is_finite() || width <= 0.0 || index >= count || count == 0 {
        return None;
    }
    Some(left + (index as f32 + 0.5) * (width / count as f32))
}

pub fn bar_index_at_x(left: f32, width: f32, count: usize, x: f32) -> Option<usize> {
    if !left.is_finite()
        || !width.is_finite()
        || !x.is_finite()
        || width <= 0.0
        || count == 0
        || x < left
        || x >= left + width
    {
        return None;
    }
    Some(((((x - left) / width) * count as f32).floor() as usize).min(count - 1))
}

fn decimal_to_f64(value: rust_decimal::Decimal) -> Option<f64> {
    value.to_string().parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        ChartInterval, ChartViewport, PriceRange, bar_center_x, bar_index_at_x,
        civil_date_from_unix_days, format_timeline_label, local_offset_seconds,
        timeline_time_at_slot,
    };
    use venue_control_protocol::UiBar;

    fn bar(low: i64, high: i64) -> UiBar {
        UiBar {
            open_time_ms: 0,
            open: Decimal::from(low),
            high: Decimal::from(high),
            low: Decimal::from(low),
            close: Decimal::from(high),
            volume: Decimal::ONE,
        }
    }

    #[test]
    fn follows_latest_window_and_pans_without_underflow() {
        let mut viewport = ChartViewport::default();
        assert_eq!(viewport.visible_bars(), 120);
        assert_eq!(viewport.visible_range(500), 380..500);
        viewport.pan_by_bars(500, 200);
        assert_eq!(viewport.visible_range(500), 180..300);
        viewport.pan_by_bars(500, 999);
        assert_eq!(viewport.visible_range(500), 0..120);
        viewport.pan_by_bars(500, -999);
        assert_eq!(viewport.visible_range(500), 499..500);
        assert_eq!(viewport.right_padding(), 119);
        viewport.reset();
        assert_eq!(viewport.visible_range(500), 380..500);
    }

    #[test]
    fn zoom_keeps_anchored_bar_visible_at_the_same_relative_side() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_bars(500, 100);
        let before = viewport.visible_range(500);
        let anchored_before = before.start + (before.len() - 1) / 2;
        viewport.zoom_by_steps(500, 0.5, 4);
        let after = viewport.visible_range(500);
        assert!(after.contains(&anchored_before));
        assert_eq!(after.start + (after.len() - 1) / 2, anchored_before);
    }

    #[test]
    fn zoom_stays_on_the_latest_bar_until_an_explicit_drag() {
        let mut viewport = ChartViewport::default();
        viewport.zoom_by_steps(500, 0.2, 4);
        assert_eq!(viewport.right_offset(), 0);
        assert_eq!(viewport.visible_range(501).end, 501);

        viewport.pan_by_drag(501, 1_000.0, -100.0);
        assert!(viewport.right_padding() > 0);
    }

    #[test]
    fn drag_pans_by_candle_width() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_drag(500, 120.0, 20.0);
        assert_eq!(viewport.right_padding(), 0);
        assert_eq!(viewport.right_offset(), 20);
        viewport.pan_by_drag(500, 120.0, -20.0);
        assert_eq!(viewport.right_padding(), 0);
        assert_eq!(viewport.right_offset(), 0);
    }

    #[test]
    fn drag_crosses_the_live_boundary_before_entering_history() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_bars(500, -60);
        assert_eq!(viewport.right_padding(), 60);
        let visible = viewport.visible_range(500);
        assert_eq!(visible, 440..500);
        assert_eq!(viewport.display_slots(visible.len()), 120);

        viewport.pan_by_bars(500, 100);
        assert_eq!(viewport.right_padding(), 0);
        assert_eq!(viewport.right_offset(), 40);
    }

    #[test]
    fn historical_window_does_not_shift_when_live_bars_arrive() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_bars(500, 130);
        let before = viewport.visible_range(500);
        assert_eq!(before, 250..370);
        assert_eq!(viewport.visible_range(501), before);
    }

    #[test]
    fn prepending_history_preserves_the_same_candle_times() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_bars(500, 130);
        let before = viewport.visible_range(500);
        viewport.history_prepended(500);
        assert_eq!(
            viewport.visible_range(1000),
            before.start + 500..before.end + 500
        );
        assert_eq!(
            viewport.visible_range(1001),
            before.start + 500..before.end + 500
        );
    }

    #[test]
    fn drag_retains_sub_candle_motion_until_it_becomes_visible() {
        let mut viewport = ChartViewport::default();
        for _ in 0..5 {
            viewport.pan_by_drag(500, 1_200.0, -2.0);
        }
        assert_eq!(viewport.right_padding(), 1);
        viewport.finish_drag();
        viewport.pan_by_drag(500, 1_200.0, -2.0);
        assert_eq!(viewport.right_padding(), 1);
    }

    #[test]
    fn cumulative_drag_is_applied_once_across_multiple_frames() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_drag_total(500, 1_200.0, -60.0);
        viewport.pan_by_drag_total(500, 1_200.0, -120.0);
        assert_eq!(viewport.right_padding(), 12);
        viewport.finish_drag();
        viewport.pan_by_drag_total(500, 1_200.0, -120.0);
        assert_eq!(viewport.right_padding(), 24);
    }

    #[test]
    fn dragging_live_edge_to_center_preserves_candle_width_and_future_anchor() {
        let mut viewport = ChartViewport::default();
        let before = viewport.visible_range(500);
        let slots = viewport.display_slots(before.len());
        let before_x = bar_center_x(0.0, 1_200.0, slots, before.len() - 1);
        viewport.pan_by_drag_total(500, 1_200.0, -600.0);
        viewport.finish_drag();
        assert_eq!(before_x, Some(1_195.0));
        for total in 500..520 {
            let visible = viewport.visible_range(total);
            assert_eq!(visible, total - 60..total);
            assert_eq!(viewport.display_slots(visible.len()), slots);
            assert_eq!(
                bar_center_x(0.0, 1_200.0, slots, visible.len() - 1),
                Some(595.0)
            );
            assert_eq!(viewport.right_offset(), 0);
        }
        viewport.history_prepended(500);
        assert_eq!(viewport.visible_range(1_019), 959..1_019);
        assert_eq!(viewport.right_padding(), 60);
    }

    #[test]
    fn zoom_preserves_centered_live_edge_and_follow_resets_it() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_bars(500, -60);
        viewport.set_visible_bars_anchored(500, 60, 0.1);
        assert_eq!(viewport.right_padding(), 30);
        assert_eq!(viewport.right_offset(), 0);
        let visible = viewport.visible_range(501);
        assert_eq!(visible, 471..501);
        assert_eq!(viewport.display_slots(visible.len()), 60);
        viewport.follow_latest();
        assert_eq!(viewport.visible_range(501), 441..501);
        assert_eq!(viewport.right_padding(), 0);
    }

    #[test]
    fn right_padding_has_future_time_labels_but_no_fabricated_candles() {
        let mut candle = bar(100, 110);
        candle.open_time_ms = 60_000;
        let bars = [candle];
        assert_eq!(
            timeline_time_at_slot(&bars, ChartInterval::OneMinute, 0),
            Some(60_000)
        );
        assert_eq!(
            timeline_time_at_slot(&bars, ChartInterval::OneMinute, 14),
            Some(900_000)
        );
        assert_eq!(
            timeline_time_at_slot(&[], ChartInterval::OneMinute, 14),
            None
        );
        assert_eq!(bars.len(), 1);
    }

    #[test]
    fn auto_price_range_pads_the_visible_extrema() -> Result<(), String> {
        let bars = [bar(100, 110), bar(90, 120)];
        let range = PriceRange::from_bars(&bars).ok_or("valid price range is required")?;
        assert!(range.low < 90.0);
        assert!(range.high > 120.0);
        let y = range
            .price_to_y(10.0, 100.0, 105.0)
            .ok_or("price maps into chart coordinates")?;
        let price = range
            .y_to_price(10.0, 100.0, y)
            .ok_or("chart coordinate maps to price")?;
        assert!((price - 105.0).abs() < 0.0001);
        Ok(())
    }

    #[test]
    fn price_grid_uses_integer_exchange_ticks() -> Result<(), String> {
        let range = PriceRange::from_extrema(99.2, 100.8).ok_or("valid price range is required")?;
        let lines = range.grid_prices(1, 4);
        assert_eq!(lines, vec![99.5, 100.0, 100.5]);
        Ok(())
    }

    #[test]
    fn x_mapping_only_returns_visible_bar_indices() {
        assert_eq!(bar_center_x(10.0, 100.0, 5, 2), Some(60.0));
        assert_eq!(bar_index_at_x(10.0, 100.0, 5, 69.9), Some(2));
        assert_eq!(bar_index_at_x(10.0, 100.0, 5, 110.0), None);
    }

    #[test]
    fn intervals_have_stable_labels_and_durations() {
        assert_eq!(ChartInterval::ALL.len(), 6);
        assert_eq!(ChartInterval::OneMinute.label(), "1m");
        assert_eq!(ChartInterval::FourHours.duration_ms(), 14_400_000);
        assert_eq!(ChartInterval::OneDay.duration_ms(), 86_400_000);
    }

    #[test]
    fn timeline_steps_are_wall_clock_aligned_and_near_fifteen_bars() {
        for (interval, expected_bars) in
            ChartInterval::ALL.into_iter().zip([15, 12, 16, 12, 12, 14])
        {
            assert_eq!(
                interval.timeline_step_ms() / interval.duration_ms(),
                expected_bars
            );
        }
    }

    #[test]
    fn timeline_labels_use_readable_local_clock_values() {
        let new_year_2024 = 1_704_067_200_000;
        let local_seconds = 1_704_067_200_i64.saturating_add(local_offset_seconds(1_704_067_200));
        let local_days = local_seconds.div_euclid(86_400);
        let local_day_seconds = local_seconds.rem_euclid(86_400) as u64;
        let local_hour = local_day_seconds / 3_600;
        let local_minute = local_day_seconds % 3_600 / 60;
        let (year, month, day) = civil_date_from_unix_days(local_days);
        assert_eq!(
            format_timeline_label(new_year_2024, ChartInterval::OneMinute),
            format!("{local_hour:02}:{local_minute:02}")
        );
        assert_eq!(
            format_timeline_label(new_year_2024, ChartInterval::OneHour),
            format!("{month:02}-{day:02} {local_hour:02}:{local_minute:02}")
        );
        assert_eq!(
            format_timeline_label(new_year_2024, ChartInterval::OneDay),
            format!("{year:04}-{month:02}-{day:02}")
        );
    }
}
