use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_gateway_api::PublicMarketBinding;
use venue_control_protocol::UiBar;
use venue_domain::PublicBar;
use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Stroke};
use std::sync::Arc;
use venue_indicators::chart::anchored_vwap::AnchoredVwapIndex;

use crate::{chart::PriceRange, model::decimal_to_f64, theme};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AvwapAnchor {
    pub id: u64,
    pub pane_instance: u32,
    pub binding: PublicMarketBinding,
    pub open_time_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub reference_price: Decimal,
    pub color: [u8; 3],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FixedProfileRange {
    pub pane_instance: u32,
    pub binding: PublicMarketBinding,
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AnalysisInteraction {
    pub mode: AnalysisMode,
    pub action: Option<AnalysisAction>,
    #[cfg(not(target_arch = "wasm32"))]
    selection: Option<crate::market::MarketSelection>,
}

impl AnalysisInteraction {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn select(&mut self, selection: Option<crate::market::MarketSelection>) {
        if self.selection != selection {
            self.mode = AnalysisMode::None;
            self.action = None;
            self.selection = selection;
        }
    }

    pub fn select_candle(&mut self, time_ms: u64, interval_ms: u64, price: Decimal) -> bool {
        let used = self.mode != AnalysisMode::None;
        let Some(end_ms) = time_ms.checked_add(interval_ms).filter(|end| *end > time_ms) else {
            return used;
        };
        use AnalysisMode as Mode;
        use AnalysisAction as Action;
        let (mode, action) = match self.mode {
            Mode::None => (Mode::None, None),
            Mode::AddAnchor => (Mode::None, Some(Action::AddAnchor { time_ms, price })),
            Mode::MoveAnchor(id) => (Mode::None, Some(Action::MoveAnchor { id, time_ms, price })),
            Mode::FixedStart => (Mode::FixedEnd(time_ms), None),
            Mode::FixedEnd(start) => (Mode::None, start.checked_add(interval_ms).map(|start_end|
                Action::SetFixedRange { start_ms: start.min(time_ms), end_ms: end_ms.max(start_end) })),
            Mode::MoveFixedStart(existing_end) if time_ms < existing_end =>
                (Mode::None, Some(Action::SetFixedRange { start_ms: time_ms, end_ms: existing_end })),
            Mode::MoveFixedEnd(existing_start) if end_ms > existing_start =>
                (Mode::None, Some(Action::SetFixedRange { start_ms: existing_start, end_ms })),
            // Crossing an endpoint must not persist a synthetic one-millisecond range.
            mode => (mode, None),
        };
        self.mode = mode;
        self.action = action;
        used
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum AnalysisMode {
    #[default]
    None,
    AddAnchor,
    MoveAnchor(u64),
    FixedStart,
    FixedEnd(u64),
    MoveFixedStart(u64),
    MoveFixedEnd(u64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum AnalysisAction {
    AddAnchor { time_ms: u64, price: Decimal },
    MoveAnchor { id: u64, time_ms: u64, price: Decimal },
    SetFixedRange { start_ms: u64, end_ms: u64 },
}

#[derive(Clone)]
struct AnchorSourceCache {
    scope: Option<PublicMarketBinding>,
    source_revision: (u64, u64),
    index: Option<AnchoredVwapIndex>,
}

fn anchor_cache_key(id: egui::Id, scope: Option<&PublicMarketBinding>,
    source_revision: Option<(u64, u64)>) -> egui::Id {
    match scope.zip(source_revision) {
        Some((scope, revision)) => egui::Id::new(("venueflow-avwap-index", scope, revision)),
        None => id.with("anchored-vwap-index"),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn draw_anchors(
    ui: &egui::Ui,
    id: egui::Id,
    painter: &egui::Painter,
    rect: Rect,
    visible_bars: &[UiBar],
    slots: usize,
    interval_ms: u64,
    range: PriceRange,
    minute_facts: Option<&[PublicBar]>,
    forming_minute: Option<&PublicBar>,
    source_revision: Option<(u64, u64)>,
    scope: Option<&PublicMarketBinding>,
    anchors: &[AvwapAnchor],
) {
    if visible_bars.is_empty() || slots == 0 || anchors.is_empty() { return; }
    let Some(facts) = minute_facts else {
        painter.text(rect.left_top() + egui::vec2(5.0, 5.0), Align2::LEFT_TOP,
            "AVWAP · waiting for 1m source", FontId::proportional(10.0), theme::WARNING);
        return;
    };
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    let cache_key = anchor_cache_key(id, scope, source_revision);
    if source_revision.is_some() {
        super::mark_indicator_cache(ui.ctx(), super::IndicatorCacheKind::Anchor, cache_key);
    }
    // Without a source revision there is no safe way to prove unchanged minute facts.
    let previous = source_revision.and_then(|_| ui.ctx().data(|data|
        data.get_temp::<Arc<AnchorSourceCache>>(cache_key)));
    let cache = previous.filter(|cached| cached.scope.as_ref() == scope
            && Some(cached.source_revision) == source_revision)
        .unwrap_or_else(|| {
            #[cfg(not(target_arch = "wasm32"))]
            let started = std::time::Instant::now();
            let cache = Arc::new(AnchorSourceCache { scope: scope.cloned(),
                source_revision: source_revision.unwrap_or_default(),
                index: AnchoredVwapIndex::build(facts).ok() });
            #[cfg(not(target_arch = "wasm32"))]
            tracing::info!(target: "venueflow::indicator_performance", study = "anchored_vwap",
                phase = "shared_index", source_bars = facts.len(),
                retained_bytes = cache.index.as_ref().map_or(0, AnchoredVwapIndex::memory_bytes),
                elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
                "indicator shared index completed");
            if source_revision.is_some() {
                ui.ctx().data_mut(|data| data.insert_temp(cache_key, cache.clone()));
            }
            cache
        });
    let Some(index) = &cache.index else { return; };
    for (position, anchor) in anchors.iter().take(8).enumerate() {
        let color = Color32::from_rgb(anchor.color[0], anchor.color[1], anchor.color[2]);
        let preview = forming_minute.and_then(|forming|
            index.preview(anchor.open_time_ms, forming).ok().flatten()
                .map(|value| (forming.open_time_ms, value)));
        if !index.contains_anchor(anchor.open_time_ms)
            && preview.is_none_or(|(time, _)| time != anchor.open_time_ms) {
            painter.text(rect.left_top() + egui::vec2(5.0, 18.0 + position as f32 * 12.0),
                Align2::LEFT_TOP, format!("AVWAP {} · anchor history needed", anchor.id),
                FontId::proportional(10.0), color);
            continue;
        }
        let mut previous: Option<(Pos2, u64)> = None;
        for (position, bar) in visible_bars.iter().enumerate() {
            let end = bar.open_time_ms.saturating_add(interval_ms);
            let confirmed = index.sample_in_range(anchor.open_time_ms, bar.open_time_ms, end).ok().flatten();
            let forming = preview.filter(|(time, _)| *time >= bar.open_time_ms && *time < end);
            let sample = forming.map(|(time, value)| (time.saturating_add(60_000), value)).or(confirmed);
            let point = sample.and_then(|(time, value)| range.price_to_y(rect.top(), rect.height(), decimal_to_f64(value))
                .map(|y| (Pos2::new(rect.left() + (position as f32 + 0.5) * rect.width() / slots as f32, y), time)));
            if let (Some((from, from_time)), Some((to, to_time))) = (previous, point)
                && index.continuous_range(from_time.saturating_sub(60_000),
                    forming.map_or(to_time, |(time, _)| time)) {
                painter.line_segment([from, to], Stroke::new(1.4,
                    if forming.is_some() { color.gamma_multiply(0.55) } else { color }));
            }
            if forming.is_some() && let Some((point, _)) = point {
                painter.circle_filled(point, 2.5, color.gamma_multiply(0.55));
            }
            previous = point;
        }
        if !index.complete(anchor.open_time_ms) {
            painter.text(rect.left_top() + egui::vec2(5.0, 18.0 + position as f32 * 12.0),
                Align2::LEFT_TOP, format!("AVWAP {} · partial", anchor.id),
                FontId::proportional(10.0), color);
        }
    }
}

pub(super) fn evict_anchor_cache(context: &egui::Context, key: egui::Id) {
    context.data_mut(|data| { data.remove::<Arc<AnchorSourceCache>>(key); });
}

pub(super) fn retained_bytes(context: &egui::Context, key: egui::Id) -> usize {
    context.data(|data| data.get_temp::<Arc<AnchorSourceCache>>(key))
        .map_or(0, |cache| std::mem::size_of::<AnchorSourceCache>()
            .saturating_add(cache.index.as_ref().map_or(0, AnchoredVwapIndex::memory_bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avwap_render_breaks_at_missing_minutes_and_does_not_extend_the_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_domain::{FieldState, Price};
        let binding = PublicMarketBinding::binance_usds_m("DOGE/USDC".parse()?)?;
        let facts = [0_u64, 1, 3, 4].into_iter().map(|minute| {
            let price = Price::new(Decimal::from(100))?;
            Ok::<_, Box<dyn std::error::Error>>(PublicBar {
                symbol: binding.symbol.clone(), generation: 1, received_at_ms: (minute + 1) * 60_000,
                sequence: minute + 1, open_time_ms: minute * 60_000,
                close_time_ms: (minute + 1) * 60_000 - 1, interval_ms: 60_000,
                open: price, high: price, low: price, close: price,
                base_volume: FieldState::Known(Decimal::ONE),
                quote_volume: FieldState::Known(Decimal::from(100)), trade_count: FieldState::Known(1),
                taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
                taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
            })
        }).collect::<Result<Vec<_>, _>>()?;
        // The display candles can be complete while the independent minute source has a hole.
        let visible = (0..6).map(|minute| UiBar { open_time_ms: minute * 60_000,
            open: Decimal::from(100), high: Decimal::from(101), low: Decimal::from(99),
            close: Decimal::from(100), volume: Some(Decimal::ONE) }).collect::<Vec<_>>();
        let anchor = AvwapAnchor { id: 1, pane_instance: 1, binding: binding.clone(),
            open_time_ms: 0, reference_price: Decimal::from(100), color: [240, 185, 11] };
        let context = egui::Context::default();
        let mut output = context.run_ui(Default::default(), |ui| {
            draw_anchors(ui, egui::Id::new("gap-anchor"), ui.painter(),
                Rect::from_min_size(Pos2::ZERO, egui::vec2(600.0, 100.0)),
                &visible, 6, 60_000, PriceRange { low: 90.0, high: 110.0 },
                Some(&facts), None, Some((1, 1)), Some(&binding), &[anchor.clone()]);
        });
        output.textures_delta.clear();
        let lines = output.shapes.iter().filter_map(|shape| match &shape.shape {
            egui::Shape::LineSegment { points, .. } => Some((points[0].x, points[1].x)),
            _ => None,
        }).collect::<Vec<_>>();
        assert_eq!(lines, vec![(50.0, 150.0), (350.0, 450.0)]);
        Ok(())
    }

    #[test]
    fn unfinished_edits_cancel_on_market_or_interval_change()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::{chart::ChartInterval, market::MarketSelection};
        let first = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute)?;
        let alternatives = [
            Some(MarketSelection::binance_usd_m("DOGE/USDT", ChartInterval::OneMinute)?),
            Some(MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneHour)?),
            Some(MarketSelection::for_server(crate::model::MarketServer::Bybit,
                "BTC/USDT", ChartInterval::OneMinute)?),
            None,
        ];
        for selection in alternatives {
            for mode in [AnalysisMode::AddAnchor, AnalysisMode::MoveAnchor(1),
                AnalysisMode::FixedEnd(60_000), AnalysisMode::MoveFixedStart(120_000),
                AnalysisMode::MoveFixedEnd(60_000)] {
                let mut state = AnalysisInteraction::default();
                state.select(Some(first.clone()));
                state.mode = mode;
                state.action = Some(AnalysisAction::SetFixedRange { start_ms: 0, end_ms: 120_000 });
                state.select(Some(first.clone()));
                assert_eq!(state.mode, mode);
                state.select(selection.clone());
                assert_eq!(state.mode, AnalysisMode::None);
                assert!(state.action.is_none());
                state.select(Some(first.clone()));
                assert_eq!(state.mode, AnalysisMode::None);
            }
        }
        Ok(())
    }

    #[test]
    fn crossing_fixed_endpoint_does_not_save_a_one_millisecond_range() {
        let mut state = AnalysisInteraction::default();
        state.mode = AnalysisMode::MoveFixedStart(120_000);
        assert!(state.select_candle(180_000, 60_000, Decimal::ONE));
        assert!(state.action.is_none());
        assert_eq!(state.mode, AnalysisMode::MoveFixedStart(120_000));
        state.select_candle(60_000, 60_000, Decimal::ONE);
        assert_eq!(state.action, Some(AnalysisAction::SetFixedRange { start_ms: 60_000, end_ms: 120_000 }));
        state.mode = AnalysisMode::MoveFixedEnd(120_000);
        state.select_candle(0, 60_000, Decimal::ONE);
        assert!(state.action.is_none());
        assert_eq!(state.mode, AnalysisMode::MoveFixedEnd(120_000));
        state.select_candle(180_000, 60_000, Decimal::ONE);
        assert_eq!(state.action, Some(AnalysisAction::SetFixedRange { start_ms: 120_000, end_ms: 240_000 }));
    }

    #[test]
    fn charts_share_only_the_same_binding_and_revision_index()
    -> Result<(), Box<dyn std::error::Error>> {
        let doge = PublicMarketBinding::binance_usds_m("DOGE/USDC".parse()?)?;
        let btc = PublicMarketBinding::binance_usds_m("BTC/USDC".parse()?)?;
        let first = egui::Id::new("first-chart");
        let second = egui::Id::new("second-chart");
        assert_eq!(anchor_cache_key(first, Some(&doge), Some((1, 2))),
            anchor_cache_key(second, Some(&doge), Some((1, 2))));
        assert_ne!(anchor_cache_key(first, Some(&doge), Some((1, 2))),
            anchor_cache_key(second, Some(&doge), Some((1, 3))));
        assert_ne!(anchor_cache_key(first, Some(&doge), Some((1, 2))),
            anchor_cache_key(second, Some(&btc), Some((1, 2))));
        assert_ne!(anchor_cache_key(first, None, None),
            anchor_cache_key(second, None, None));
        Ok(())
    }

    #[test]
    fn shared_index_survives_one_chart_closing_and_releases_after_last()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = PublicMarketBinding::binance_usds_m("DOGE/USDC".parse()?)?;
        let key = anchor_cache_key(egui::Id::new("first-chart"), Some(&binding), Some((1, 2)));
        let context = egui::Context::default();
        let cache = Arc::new(AnchorSourceCache { scope: Some(binding),
            source_revision: (1, 2), index: None });
        let weak = Arc::downgrade(&cache);
        let mut output = context.run_ui(Default::default(), |ui| {
            ui.ctx().data_mut(|data| data.insert_temp(key, cache.clone()));
            super::super::mark_indicator_cache(ui.ctx(),
                super::super::IndicatorCacheKind::Anchor, key);
            super::super::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        drop(cache);
        let mut output = context.run_ui(Default::default(), |ui| {
            super::super::mark_indicator_cache(ui.ctx(),
                super::super::IndicatorCacheKind::Anchor, key);
            super::super::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        assert!(weak.upgrade().is_some());
        let mut output = context.run_ui(Default::default(), |ui| {
            super::super::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        assert!(weak.upgrade().is_none());
        Ok(())
    }

    #[test]
    fn deleted_anchor_releases_cached_history() {
        let context = egui::Context::default();
        let key = egui::Id::new("anchor-that-was-deleted");
        let cache = Arc::new(AnchorSourceCache { scope: None, source_revision: (1, 1),
            index: None });
        let weak = Arc::downgrade(&cache);
        let mut output = context.run_ui(Default::default(), |ui| {
            ui.ctx().data_mut(|data| data.insert_temp(key, cache.clone()));
            super::super::mark_indicator_cache(ui.ctx(),
                super::super::IndicatorCacheKind::Anchor, key);
            super::super::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        drop(cache);
        assert!(weak.upgrade().is_some());
        let mut output = context.run_ui(Default::default(), |ui| {
            super::super::evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        assert!(weak.upgrade().is_none());
    }
}
