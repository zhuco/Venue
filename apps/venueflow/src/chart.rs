use std::ops::Range;

use serde::{Deserialize, Serialize};
use venue_control_protocol::UiBar;

pub const DEFAULT_VISIBLE_BARS: usize = 120;
pub const MIN_VISIBLE_BARS: usize = 20;
pub const MAX_VISIBLE_BARS: usize = 400;

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

    #[cfg(not(target_arch = "wasm32"))]
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
}

/// Persisted chart navigation state. `right_offset` counts bars hidden after the window, so a
/// zero offset follows the latest candle and a positive offset pans towards older candles.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartViewport {
    visible_bars: usize,
    right_offset: usize,
}

impl Default for ChartViewport {
    fn default() -> Self {
        Self {
            visible_bars: DEFAULT_VISIBLE_BARS,
            right_offset: 0,
        }
    }
}

impl ChartViewport {
    pub const fn visible_bars(&self) -> usize {
        self.visible_bars
    }

    pub const fn right_offset(&self) -> usize {
        self.right_offset
    }

    pub fn reset(&mut self) {
        self.visible_bars = DEFAULT_VISIBLE_BARS;
        self.right_offset = 0;
    }

    pub fn follow_latest(&mut self) {
        self.right_offset = 0;
    }

    pub fn visible_range(&mut self, total_bars: usize) -> Range<usize> {
        if total_bars == 0 {
            self.right_offset = 0;
            return 0..0;
        }
        self.visible_bars = self.visible_bars.clamp(MIN_VISIBLE_BARS, MAX_VISIBLE_BARS);
        let count = self.visible_bars.min(total_bars);
        let maximum_offset = total_bars.saturating_sub(count);
        self.right_offset = self.right_offset.min(maximum_offset);
        let end = total_bars.saturating_sub(self.right_offset);
        end.saturating_sub(count)..end
    }

    /// Positive values pan towards older bars; negative values pan back towards live data.
    pub fn pan_by_bars(&mut self, total_bars: usize, delta: isize) {
        let visible = self.visible_range(total_bars);
        let maximum_offset = total_bars.saturating_sub(visible.len());
        self.right_offset = if delta.is_negative() {
            self.right_offset.saturating_sub(delta.unsigned_abs())
        } else {
            self.right_offset
                .saturating_add(delta.unsigned_abs())
                .min(maximum_offset)
        };
    }

    /// Converts a horizontal drag into whole-candle panning. A rightward drag reveals history.
    pub fn pan_by_drag(&mut self, total_bars: usize, chart_width: f32, drag_delta_x: f32) {
        let visible = self.visible_range(total_bars);
        if visible.is_empty() || !chart_width.is_finite() || chart_width <= 0.0 {
            return;
        }
        let width_per_bar = chart_width / visible.len() as f32;
        if !width_per_bar.is_finite() || width_per_bar <= 0.0 {
            return;
        }
        let delta = (drag_delta_x / width_per_bar).round();
        if delta.is_finite() {
            self.pan_by_bars(total_bars, delta as isize);
        }
    }

    /// Changes the visible count while retaining the bar under `anchor_ratio` in the same place.
    /// `0.0` is the oldest visible bar and `1.0` the newest visible bar.
    pub fn set_visible_bars_anchored(
        &mut self,
        total_bars: usize,
        requested_visible_bars: usize,
        anchor_ratio: f32,
    ) {
        let was_following_latest = self.right_offset == 0;
        let current = self.visible_range(total_bars);
        if current.is_empty() {
            self.visible_bars = requested_visible_bars.clamp(MIN_VISIBLE_BARS, MAX_VISIBLE_BARS);
            self.right_offset = 0;
            return;
        }
        let ratio = anchor_ratio.clamp(0.0, 1.0);
        let current_anchor_offset =
            ((current.len().saturating_sub(1)) as f32 * ratio).round() as usize;
        let anchor_index = current.start.saturating_add(current_anchor_offset);
        self.visible_bars = requested_visible_bars.clamp(MIN_VISIBLE_BARS, MAX_VISIBLE_BARS);
        let new_count = self.visible_bars.min(total_bars);
        let new_anchor_offset = ((new_count.saturating_sub(1)) as f32 * ratio).round() as usize;
        let newest_allowed_start = total_bars.saturating_sub(new_count);
        let new_start = anchor_index
            .saturating_sub(new_anchor_offset)
            .min(newest_allowed_start);
        self.right_offset = total_bars.saturating_sub(new_start.saturating_add(new_count));
        if was_following_latest {
            self.right_offset = 0;
        }
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
        let increment = (current.len() / 10).max(1);
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

    use super::{ChartInterval, ChartViewport, PriceRange, bar_center_x, bar_index_at_x};
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
        assert_eq!(viewport.visible_range(500), 380..500);
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

        viewport.pan_by_drag(501, 1_000.0, 100.0);
        assert!(viewport.right_offset() > 0);
    }

    #[test]
    fn drag_pans_by_candle_width() {
        let mut viewport = ChartViewport::default();
        viewport.pan_by_drag(500, 120.0, 20.0);
        assert_eq!(viewport.right_offset(), 20);
        viewport.pan_by_drag(500, 120.0, -20.0);
        assert_eq!(viewport.right_offset(), 0);
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
}
