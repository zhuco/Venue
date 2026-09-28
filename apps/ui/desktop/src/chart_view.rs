use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Stroke};
use std::sync::Arc;
use venue_control_protocol::UiBar;

use crate::{
    chart::{
        ChartStudyPoint, PriceRange, bar_center_x, bar_index_at_x, format_timeline_label,
        timeline_time_at_slot,
    },
    chart_settings::ChartDisplaySettings,
    i18n::{Language, TextKey, text},
    model::{decimal_to_f64, format_decimal},
    theme,
};

type StudySelector = fn(&ChartStudyPoint) -> Option<rust_decimal::Decimal>;
pub(crate) mod analysis;
mod candles;
pub(crate) mod loading;
pub(crate) mod microstructure;
mod price_annotations;
mod price_axis;
mod session_levels;
mod study_readout;
mod support_resistance;
mod volume_profile;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndicatorCacheKind {
    Heatmap,
    Profile,
    Flow,
    Anchor,
}

#[derive(Clone, Copy)]
struct IndicatorCacheEntry {
    kind: IndicatorCacheKind,
    id: egui::Id,
    seen_frame: u64,
}

const MAX_RETAINED_INDICATOR_CACHE_BYTES: usize = 56 * 1024 * 1024;

fn indicator_cache_registry_id() -> egui::Id {
    egui::Id::new("venueflow-indicator-cache-registry")
}

fn mark_indicator_cache(context: &egui::Context, kind: IndicatorCacheKind, id: egui::Id) {
    let frame = context.cumulative_frame_nr();
    context.data_mut(|data| {
        let key = indicator_cache_registry_id();
        let mut entries = data
            .get_temp::<Vec<IndicatorCacheEntry>>(key)
            .unwrap_or_default();
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.kind == kind && entry.id == id)
        {
            entry.seen_frame = frame;
        } else {
            entries.push(IndicatorCacheEntry {
                kind,
                id,
                seen_frame: frame,
            });
        }
        data.insert_temp(key, entries);
    });
}

pub(crate) fn evict_inactive_indicator_caches(context: &egui::Context) {
    let frame = context.cumulative_frame_nr();
    let stale = context.data_mut(|data| {
        let key = indicator_cache_registry_id();
        let mut entries = data
            .get_temp::<Vec<IndicatorCacheEntry>>(key)
            .unwrap_or_default();
        let mut stale = Vec::new();
        entries.retain(|entry| {
            if entry.seen_frame == frame {
                true
            } else {
                stale.push(*entry);
                false
            }
        });
        if entries.is_empty() {
            data.remove::<Vec<IndicatorCacheEntry>>(key);
        } else {
            data.insert_temp(key, entries);
        }
        stale
    });
    for entry in stale {
        evict_indicator_cache(context, entry, true);
    }
    evict_indicator_caches_to_budget(context, MAX_RETAINED_INDICATOR_CACHE_BYTES);
}

fn evict_indicator_cache(
    context: &egui::Context,
    entry: IndicatorCacheEntry,
    cancel_pending: bool,
) {
    match entry.kind {
        IndicatorCacheKind::Heatmap if cancel_pending => {
            microstructure::evict_heatmap(context, entry.id)
        }
        IndicatorCacheKind::Heatmap => microstructure::evict_heatmap_result(context, entry.id),
        IndicatorCacheKind::Profile => volume_profile::evict(context, entry.id),
        IndicatorCacheKind::Flow => context.data_mut(|data| {
            data.remove::<Arc<MinuteFlowCache>>(entry.id.with("minute-flow-cache"));
        }),
        IndicatorCacheKind::Anchor => analysis::evict_anchor_cache(context, entry.id),
    }
}

fn indicator_cache_entry_bytes(context: &egui::Context, entry: &IndicatorCacheEntry) -> usize {
    match entry.kind {
        IndicatorCacheKind::Heatmap => microstructure::heatmap_retained_bytes(context, entry.id),
        IndicatorCacheKind::Profile => volume_profile::retained_bytes(context, entry.id),
        IndicatorCacheKind::Anchor => analysis::retained_bytes(context, entry.id),
        IndicatorCacheKind::Flow => context
            .data(|data| data.get_temp::<Arc<MinuteFlowCache>>(entry.id.with("minute-flow-cache")))
            .map_or(0, |cache| {
                std::mem::size_of::<MinuteFlowCache>()
                    .saturating_add(cache.buckets.capacity() * std::mem::size_of::<(u64, bool)>())
                    .saturating_add(
                        cache.values.capacity()
                            * std::mem::size_of::<venue_indicators::chart::OrderFlowValue>(),
                    )
            }),
    }
}

fn indicator_cache_total_bytes(context: &egui::Context, entries: &[IndicatorCacheEntry]) -> usize {
    entries.iter().fold(
        entries.len() * std::mem::size_of::<IndicatorCacheEntry>(),
        |total, entry| total.saturating_add(indicator_cache_entry_bytes(context, entry)),
    )
}

fn evict_indicator_caches_to_budget(context: &egui::Context, budget: usize) {
    let key = indicator_cache_registry_id();
    let mut entries = context.data(|data| {
        data.get_temp::<Vec<IndicatorCacheEntry>>(key)
            .unwrap_or_default()
    });
    while indicator_cache_total_bytes(context, &entries) > budget {
        let Some(index) = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| indicator_cache_entry_bytes(context, entry) > 0)
            .min_by_key(|(_, entry)| entry.seen_frame)
            .map(|(index, _)| index)
        else {
            break;
        };
        let entry = entries[index];
        evict_indicator_cache(context, entry, false);
        // A heatmap worker may finish after this frame; keep its registry entry so a
        // subsequently closed chart can still cancel and release that late result.
        if entry.kind != IndicatorCacheKind::Heatmap {
            entries.remove(index);
        }
    }
    context.data_mut(|data| {
        if entries.is_empty() {
            data.remove::<Vec<IndicatorCacheEntry>>(key);
        } else {
            data.insert_temp(key, entries);
        }
    });
}

pub(crate) fn retained_indicator_cache_bytes(context: &egui::Context) -> usize {
    let entries = context.data(|data| {
        data.get_temp::<Vec<IndicatorCacheEntry>>(indicator_cache_registry_id())
            .unwrap_or_default()
    });
    indicator_cache_total_bytes(context, &entries)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn pending_indicator_input_bytes() -> usize {
    microstructure::heatmap_pending_input_bytes()
}

#[derive(Clone, Copy)]
enum PaneScale {
    ZeroToHundred,
    MinusHundredToZero,
    Symmetric,
    Positive,
    Auto,
}

struct PaneSpec {
    label: String,
    selectors: [Option<StudySelector>; 3],
    series_labels: [&'static str; 3],
    colors: [Color32; 3],
    width: f32,
    scale: PaneScale,
    histogram: bool,
    histogram_colors: [Color32; 2],
    reference_levels: &'static [f64],
}

struct MinuteFlowCache {
    binding: venue_gateway_api::PublicMarketBinding,
    revision: (u64, u64),
    interval_ms: u64,
    reset_mode: venue_indicators::chart::CvdResetMode,
    buckets: Vec<(u64, bool)>,
    values: Vec<venue_indicators::chart::OrderFlowValue>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn candle_plot(
    ui: &mut egui::Ui,
    all_bars: &[UiBar],
    all_studies: &[ChartStudyPoint],
    viewport: &mut crate::chart::ChartViewport,
    language: Language,
    settings: &ChartDisplaySettings,
    scales: (usize, usize),
    price_tick: Option<rust_decimal::Decimal>,
    interval: crate::chart::ChartInterval,
    market_price: Option<rust_decimal::Decimal>,
    selected_price: Option<rust_decimal::Decimal>,
    trading_display: &crate::chart_trading::ChartTradingSettings,
    overlays: &[crate::chart_trading::ChartOverlay],
    bid_ask: (Option<rust_decimal::Decimal>, Option<rust_decimal::Decimal>),
    depth: Option<(
        &[venue_control_protocol::UiBookLevel],
        &[venue_control_protocol::UiBookLevel],
    )>,
    market_scope: Option<&venue_gateway_api::PublicMarketBinding>,
    minute_source: Option<(&[UiBar], &[crate::chart::BaseMinuteStudy], (u64, u64))>,
    day_source: Option<&[venue_domain::PublicBar]>,
    minute_facts: Option<&[venue_domain::PublicBar]>,
    forming_minute: Option<&venue_domain::PublicBar>,
    mut analysis: Option<(&[analysis::AvwapAnchor], &mut analysis::AnalysisInteraction)>,
    bar_revision: Option<u64>,
    oi_samples: Option<&[venue_domain::OpenInterestSample]>,
) -> Option<rust_decimal::Decimal> {
    let (price_scale, quantity_scale) = scales;
    let height = ui.available_height().max(1.0);
    if height < 80.0 || ui.available_width() < 120.0 {
        return None;
    }
    let (response, painter) = ui.allocate_painter(
        egui::vec2(ui.available_width(), height),
        Sense::click_and_drag(),
    );
    painter.rect_filled(response.rect, 0, theme::BG_PRIMARY);
    let analysis_active = analysis
        .as_ref()
        .is_some_and(|(_, state)| state.mode != analysis::AnalysisMode::None);
    if let Some((_, state)) = analysis.as_mut() {
        state.action = None;
        if ui.input(|input| input.key_pressed(egui::Key::Escape)) {
            state.mode = analysis::AnalysisMode::None;
        }
    }
    if all_bars.is_empty() {
        painter.text(
            response.rect.center(),
            Align2::CENTER_CENTER,
            text(language, TextKey::NoCandles),
            FontId::proportional(14.0),
            theme::TEXT_SECONDARY,
        );
        return None;
    }

    let full_rect = response.rect.shrink2(egui::vec2(8.0, 8.0));
    let axis_width =
        price_axis::width(&painter, all_bars, overlays, price_scale).min(full_rect.width() * 0.4);
    let full_plot_rect =
        Rect::from_min_max(full_rect.min, full_rect.max - egui::vec2(axis_width, 0.0));
    let profile_enabled = settings.profile.visible_range || settings.profile.fixed_range;
    let profile_width = if profile_enabled {
        (full_plot_rect.width() * f32::from(settings.profile.width_percent) / 100.0)
            .min(full_plot_rect.width() * 0.35)
    } else {
        0.0
    };
    let plot_rect = Rect::from_min_max(
        full_plot_rect.min,
        Pos2::new(
            full_plot_rect.right() - profile_width,
            full_plot_rect.bottom(),
        ),
    );
    let axis_painter = painter.clone();
    axis_painter.rect_filled(
        Rect::from_min_max(Pos2::new(plot_rect.right(), full_rect.top()), full_rect.max),
        0,
        theme::BG_PRIMARY,
    );
    let profile_painter = painter.clone();
    let painter = painter.with_clip_rect(plot_rect.intersect(painter.clip_rect()));
    let timeline_height = 16.0_f32.min(plot_rect.height() * 0.14);
    let content_rect = Rect::from_min_max(
        plot_rect.min,
        Pos2::new(plot_rect.right(), plot_rect.bottom() - timeline_height),
    );
    let timeline_rect = Rect::from_min_max(
        Pos2::new(plot_rect.left(), content_rect.bottom()),
        plot_rect.max,
    );
    if !analysis_active && response.dragged_by(egui::PointerButton::Primary) {
        ui.ctx().set_cursor_icon(egui::CursorIcon::Grabbing);
    } else if response
        .hover_pos()
        .is_some_and(|point| timeline_rect.contains(point))
    {
        ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeHorizontal);
    }
    let pointer_ratio = response.hover_pos().map_or(1.0, |point| {
        ((point.x - plot_rect.left()) / plot_rect.width()).clamp(0.0, 1.0)
    });
    let price_axis = ui.interact(
        Rect::from_min_max(
            Pos2::new(plot_rect.right(), plot_rect.top()),
            Pos2::new(full_rect.right(), content_rect.bottom()),
        ),
        response.id.with("price-axis"),
        Sense::click_and_drag(),
    );
    if price_axis.hovered() || price_axis.dragged() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeVertical);
    }
    if !analysis_active && price_axis.double_clicked() {
        viewport.reset_price_scale();
    }
    if !analysis_active && price_axis.dragged_by(egui::PointerButton::Primary) {
        viewport.auto_price_scale = false;
        let delta = ui.input(|input| input.pointer.delta().y);
        viewport.price_zoom_milli = ((viewport.price_zoom_milli.clamp(250, 4000) as f32)
            * (delta * 0.006).exp())
        .clamp(250.0, 4000.0) as u32;
    }
    if response.hovered() && !price_axis.hovered() && !analysis_active {
        let steps = ui.input(|input| {
            input
                .events
                .iter()
                .filter_map(|event| {
                    if let egui::Event::MouseWheel { unit, delta, .. } = event {
                        if !delta.y.is_finite() || delta.y == 0.0 {
                            return None;
                        }
                        let count = if *unit == egui::MouseWheelUnit::Line {
                            delta.y.abs().ceil() as isize
                        } else {
                            1
                        };
                        Some(if delta.y > 0.0 { count } else { -count })
                    } else {
                        None
                    }
                })
                .sum()
        });
        viewport.zoom_by_grid_steps(all_bars.len(), pointer_ratio, interval, steps);
    }
    if !analysis_active
        && response.dragged_by(egui::PointerButton::Primary)
        && !price_axis.dragged()
        && let Some(delta) = response.total_drag_delta()
    {
        viewport.pan_by_drag_total(all_bars.len(), plot_rect.width(), delta.x);
    }
    if response.drag_stopped_by(egui::PointerButton::Primary) {
        viewport.finish_drag();
    }

    let range = viewport.visible_range(all_bars.len());
    let bars = &all_bars[range.clone()];
    let flow_points = if settings.microstructure.show_delta || settings.microstructure.show_cvd {
        let buckets = bars
            .iter()
            .map(|bar| {
                let confirmed =
                    study_at(all_studies, bar.open_time_ms).is_some_and(|point| point.confirmed);
                (bar.open_time_ms, confirmed)
            })
            .collect::<Vec<_>>();
        let revision = minute_source.map(|(_, _, revision)| revision);
        let key = response.id.with("minute-flow-cache");
        if market_scope.is_some() && revision.is_some() && minute_facts.is_some() {
            mark_indicator_cache(ui.ctx(), IndicatorCacheKind::Flow, response.id);
        } else {
            ui.ctx().data_mut(|data| {
                data.remove::<Arc<MinuteFlowCache>>(key);
            });
        }
        let cached = market_scope
            .zip(revision)
            .filter(|_| minute_facts.is_some())
            .and_then(|(binding, revision)| {
                ui.ctx()
                    .data(|data| data.get_temp::<Arc<MinuteFlowCache>>(key))
                    .filter(|cache| {
                        cache.binding == *binding
                            && cache.revision == revision
                            && cache.interval_ms == interval.duration_ms()
                            && cache.reset_mode == settings.microstructure.cvd_reset_mode
                            && cache.buckets == buckets
                    })
            });
        let values = cached.map(|cache| cache.values.clone()).unwrap_or_else(|| {
            let values = minute_facts
                .map(|facts| {
                    venue_indicators::chart::aggregate_minute_flow(
                        facts,
                        &buckets,
                        interval.duration_ms(),
                        settings.microstructure.cvd_reset_mode,
                    )
                })
                .unwrap_or_else(|| {
                    vec![venue_indicators::chart::OrderFlowValue::default(); buckets.len()]
                });
            if let Some((binding, revision)) = market_scope.zip(revision)
                && minute_facts.is_some()
            {
                ui.ctx().data_mut(|data| {
                    data.insert_temp(
                        key,
                        Arc::new(MinuteFlowCache {
                            binding: binding.clone(),
                            revision,
                            interval_ms: interval.duration_ms(),
                            reset_mode: settings.microstructure.cvd_reset_mode,
                            buckets: buckets.clone(),
                            values: values.clone(),
                        }),
                    )
                });
            }
            values
        });
        buckets
            .into_iter()
            .zip(values)
            .map(|((open_time_ms, confirmed), order_flow)| ChartStudyPoint {
                open_time_ms,
                confirmed,
                order_flow,
                ..Default::default()
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let oi_points = if settings.oi_pane {
        bars.iter()
            .map(|bar| {
                let end = bar.open_time_ms.saturating_add(interval.duration_ms());
                let quantity = oi_samples.and_then(|samples| {
                    let last = samples.partition_point(|sample| sample.exchange_time_ms <= end);
                    last.checked_sub(1)
                        .and_then(|index| samples.get(index))
                        .filter(|sample| sample.exchange_time_ms > bar.open_time_ms)
                        .and_then(|sample| match &sample.base_quantity {
                            venue_domain::FieldState::Known(quantity) => Some(*quantity),
                            _ => None,
                        })
                });
                ChartStudyPoint {
                    open_time_ms: bar.open_time_ms,
                    open_interest: quantity,
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let display_slots = viewport.display_slots(bars.len());
    let has_local_studies = !all_studies.is_empty();
    let pane_specs = if has_local_studies {
        pane_specs(settings)
    } else {
        Vec::new()
    };
    let sub_count = pane_specs.len();
    let volume_ratio = if settings.volume.enabled { 0.12 } else { 0.0 };
    let sub_ratio = (0.12 * sub_count as f32).min(0.38);
    let price_ratio = 1.0 - volume_ratio - sub_ratio;
    let price_rect = Rect::from_min_max(
        content_rect.min,
        Pos2::new(
            content_rect.right(),
            content_rect.top() + content_rect.height() * price_ratio,
        ),
    );
    let mut cursor = price_rect.bottom() + 4.0;
    let volume_rect = settings.volume.enabled.then(|| {
        let rect = Rect::from_min_max(
            Pos2::new(plot_rect.left(), cursor),
            Pos2::new(
                plot_rect.right(),
                cursor + content_rect.height() * volume_ratio - 4.0,
            ),
        );
        cursor = rect.bottom() + 4.0;
        rect
    });
    let sub_height = if sub_count == 0 {
        0.0
    } else {
        (content_rect.bottom() - cursor - 4.0 * (sub_count.saturating_sub(1)) as f32)
            / sub_count as f32
    };
    let mut next_sub_rect = || {
        let rect = Rect::from_min_max(
            Pos2::new(plot_rect.left(), cursor),
            Pos2::new(plot_rect.right(), cursor + sub_height),
        );
        cursor = rect.bottom() + 4.0;
        rect
    };
    let sub_rects = (0..sub_count).map(|_| next_sub_rect()).collect::<Vec<_>>();
    let hovered_index = response
        .hover_pos()
        .filter(|point| plot_rect.contains(*point))
        .and_then(|point| {
            bar_index_at_x(
                price_rect.left(),
                price_rect.width(),
                display_slots,
                point.x,
            )
        })
        .filter(|index| *index < bars.len());
    let selected_index = hovered_index.or_else(|| bars.len().checked_sub(1));
    let readout_time = selected_index
        .and_then(|index| bars.get(index))
        .map_or(0, |bar| bar.open_time_ms);
    let line_height = painter
        .layout_no_wrap(
            "0".into(),
            FontId::proportional(f32::from(settings.chart_text_size)),
            theme::TEXT_PRIMARY,
        )
        .size()
        .y;
    let readout_top = 6.0 + line_height + 2.0;
    let mut readout_point = study_at(all_studies, readout_time)
        .cloned()
        .unwrap_or_default();
    if let Some(index) = selected_index
        && let Some(flow) = flow_points.get(index)
    {
        readout_point.order_flow = flow.order_flow;
    }
    let job = study_readout::job(
        settings,
        Some(&readout_point),
        selected_index
            .and_then(|index| bars.get(index))
            .map(|bar| bar.close),
        price_scale,
        price_rect.width() - 12.0,
    );
    let study_galley = (has_local_studies && !job.text.is_empty()).then(|| painter.layout_job(job));
    let custom_readout_y = readout_top
        + study_galley
            .as_ref()
            .map_or(0.0, |galley| galley.size().y + 2.0);
    let raw_range = PriceRange::from_bars(bars)?;
    // Keep the OHLC / study readouts and the high-price leader out of candle space.
    let headroom = (custom_readout_y
        + if has_local_studies && settings.effective_custom_legacy().enabled {
            line_height + 8.0
        } else {
            8.0
        })
    .min(price_rect.height() * 0.4);
    let readout_rect = price_rect;
    let mut price_rect = price_rect;
    price_rect.min.y += headroom;
    let automatic =
        raw_range.with_tick_height(price_tick.map(decimal_to_f64), price_rect.height(), 4.0);
    let price_range = viewport.resolve_price_scale(automatic);
    let mut analysis_used = false;
    if (response.clicked_by(egui::PointerButton::Primary)
        || (analysis_active && response.drag_stopped_by(egui::PointerButton::Primary)))
        && response
            .interact_pointer_pos()
            .is_some_and(|point| price_rect.contains(point))
        && let Some(index) = hovered_index
        && let Some((_, state)) = analysis.as_mut()
    {
        let time = bars[index].open_time_ms;
        let price = bars[index].close;
        analysis_used = state.select_candle(time, interval.duration_ms(), price);
    }
    if profile_enabled {
        let profile_rect = Rect::from_min_max(
            Pos2::new(plot_rect.right(), price_rect.top()),
            Pos2::new(full_plot_rect.right(), price_rect.bottom()),
        );
        volume_profile::draw(
            ui,
            &profile_painter,
            response.id,
            profile_rect,
            bars,
            interval.duration_ms(),
            price_range,
            price_tick,
            minute_facts,
            minute_source.map(|(_, _, revision)| revision),
            market_scope,
            &settings.profile,
        );
    }
    let width = price_rect.width() / display_slots as f32;
    let price_y = |price: f64| {
        price_range
            .price_to_y(price_rect.top(), price_rect.height(), price)
            .unwrap_or(price_rect.center().y)
    };
    microstructure::draw(
        ui,
        &painter,
        response.id,
        price_rect,
        all_bars,
        all_studies,
        interval.duration_ms(),
        range.clone(),
        display_slots,
        price_range,
        &settings.microstructure,
        language,
        depth,
        market_scope,
        minute_source,
        price_tick,
    );
    if let Some(days) = day_source {
        session_levels::draw(
            &painter,
            price_rect,
            bars,
            display_slots,
            price_range,
            price_scale,
            days,
            &settings.session,
        );
    }
    if settings.session.sr_current {
        support_resistance::draw(
            ui,
            &painter,
            response.id,
            price_rect,
            all_bars,
            all_studies,
            range.clone(),
            display_slots,
            interval.duration_ms(),
            price_range,
            price_tick,
            market_scope,
            bar_revision,
        );
    }
    if settings.session.sr_15m || settings.session.sr_1h || settings.session.sr_1d {
        support_resistance::draw_higher(
            ui,
            &painter,
            response.id,
            price_rect,
            bars,
            display_slots,
            interval.duration_ms(),
            price_range,
            price_tick,
            market_scope,
            minute_facts,
            day_source,
            [
                settings.session.sr_15m,
                settings.session.sr_1h,
                settings.session.sr_1d,
            ],
        );
    }
    if let Some((anchors, _)) = analysis.as_ref() {
        analysis::draw_anchors(
            ui,
            response.id,
            &painter,
            price_rect,
            bars,
            display_slots,
            interval.duration_ms(),
            price_range,
            minute_facts,
            forming_minute,
            minute_source.map(|(_, _, revision)| revision),
            market_scope,
            anchors,
        );
    }
    for price in price_range.grid_prices(price_scale, 5) {
        let y = price_y(price);
        painter.line_segment(
            [
                Pos2::new(price_rect.left(), y),
                Pos2::new(price_rect.right(), y),
            ],
            Stroke::new(1.0, theme::CHART_GRID),
        );
    }
    painter.line_segment(
        [timeline_rect.left_top(), timeline_rect.right_top()],
        Stroke::new(1.0, theme::DIVIDER),
    );
    for index in 0..display_slots {
        let Some(open_time_ms) = timeline_time_at_slot(bars, interval, index)
            .filter(|time| time.is_multiple_of(interval.timeline_step_ms()))
        else {
            continue;
        };
        let x = bar_center_x(price_rect.left(), price_rect.width(), display_slots, index)
            .unwrap_or(price_rect.left());
        painter.line_segment(
            [
                Pos2::new(x, price_rect.top()),
                Pos2::new(x, content_rect.bottom()),
            ],
            Stroke::new(1.0, theme::CHART_GRID),
        );
        painter.text(
            Pos2::new(x, timeline_rect.top() + 2.0),
            Align2::CENTER_TOP,
            format_timeline_label(open_time_ms, interval),
            FontId::proportional(f32::from(settings.chart_text_size)),
            theme::TEXT_SECONDARY,
        );
    }
    let maximum_volume = bars
        .iter()
        .filter_map(|bar| bar.volume.map(decimal_to_f64))
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    if has_local_studies {
        draw_price_fills(
            &painter,
            price_rect,
            bars,
            display_slots,
            all_studies,
            price_y,
            settings,
        );
    }
    let candle_painter = painter.with_clip_rect(price_rect.intersect(painter.clip_rect()));
    let custom_ids: Vec<u64> = settings
        .custom_library
        .as_ref()
        .map(|l| {
            l.entries
                .iter()
                .filter(|e| e.enabled)
                .map(|e| e.id)
                .collect()
        })
        .unwrap_or_default();
    if has_local_studies {
        crate::custom_indicator::draw_scripts(
            &painter,
            price_rect,
            bars,
            display_slots,
            all_studies,
            price_y,
            true,
            &custom_ids,
        );
    }
    for (index, bar) in bars.iter().enumerate() {
        let open = decimal_to_f64(bar.open);
        let close = decimal_to_f64(bar.close);
        let x = bar_center_x(price_rect.left(), price_rect.width(), display_slots, index)
            .unwrap_or(price_rect.left());
        let color = if close >= open {
            theme::BUY
        } else {
            theme::SELL
        };
        candles::draw(
            &candle_painter,
            x,
            width,
            [
                price_y(open),
                price_y(decimal_to_f64(bar.high)),
                price_y(decimal_to_f64(bar.low)),
                price_y(close),
            ],
            color,
        );
        if let (Some(volume_rect), Some(volume)) = (volume_rect, bar.volume) {
            let color = if close >= open {
                settings.volume.color()
            } else {
                settings.volume.secondary_color()
            };
            let volume_height =
                (decimal_to_f64(volume) / maximum_volume) as f32 * volume_rect.height();
            painter.rect_filled(
                Rect::from_min_max(
                    Pos2::new(x - width * 0.31, volume_rect.bottom() - volume_height),
                    Pos2::new(x + width * 0.31, volume_rect.bottom()),
                ),
                0.5,
                Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 150),
            );
        }
    }
    if let Some(rect) = volume_rect {
        painter.line_segment(
            [rect.left_top(), rect.right_top()],
            Stroke::new(1.0, theme::DIVIDER),
        );
        painter.text(
            rect.left_top() + egui::vec2(4.0, 2.0),
            Align2::LEFT_TOP,
            volume_readout(bars, selected_index, quantity_scale),
            FontId::proportional(f32::from(settings.chart_text_size)),
            theme::TEXT_SECONDARY,
        );
    }
    if has_local_studies {
        draw_price_studies(
            &painter,
            price_rect,
            bars,
            display_slots,
            all_studies,
            price_y,
            settings,
        );
        if let Some(galley) = study_galley {
            painter.galley(
                readout_rect.left_top() + egui::vec2(6.0, readout_top),
                galley,
                theme::TEXT_PRIMARY,
            );
        }
        crate::custom_indicator::draw(
            &painter,
            price_rect,
            bars,
            display_slots,
            all_studies,
            &settings.effective_custom_legacy(),
            price_y,
            readout_time,
            price_scale,
            settings.chart_text_size,
            language,
            custom_readout_y - headroom,
        );
        crate::custom_indicator::draw_scripts(
            &painter,
            price_rect,
            bars,
            display_slots,
            all_studies,
            price_y,
            false,
            &custom_ids,
        );
        for (spec, rect) in pane_specs.iter().zip(sub_rects) {
            let points = if spec.label.starts_with("Delta") || spec.label.starts_with("CVD") {
                flow_points.as_slice()
            } else if spec.label.starts_with("OI") {
                oi_points.as_slice()
            } else {
                all_studies
            };
            draw_sub_pane(
                &painter,
                rect,
                bars,
                display_slots,
                points,
                spec,
                settings.chart_text_size,
                selected_index,
            );
        }
    }
    let latest_price = market_price.or_else(|| all_bars.last().map(|bar| bar.close));
    let mut trading_overlays = overlays.to_vec();
    for (enabled, price, color, name) in [
        (
            trading_display.last_price,
            latest_price,
            if latest_price
                .zip(all_bars.last())
                .is_some_and(|(price, bar)| price >= bar.open)
            {
                theme::BUY
            } else {
                theme::SELL
            },
            "",
        ),
        (
            trading_display.bid_ask,
            bid_ask.0,
            theme::BUY,
            if language == Language::SimplifiedChinese {
                "买一"
            } else {
                "Bid"
            },
        ),
        (
            trading_display.bid_ask,
            bid_ask.1,
            theme::SELL,
            if language == Language::SimplifiedChinese {
                "卖一"
            } else {
                "Ask"
            },
        ),
    ] {
        if trading_display.price_lines
            && enabled
            && let Some(price) = price.filter(|price| *price > rust_decimal::Decimal::ZERO)
        {
            trading_overlays.push(crate::chart_trading::ChartOverlay {
                price,
                label: if trading_display.price_labels {
                    name.into()
                } else {
                    String::new()
                },
                color,
                time_ms: None,
                line: true,
                tick: !name.is_empty()
                    && trading_display.price_labels
                    && trading_display.ticks
                    && trading_display.tick_prices,
                badge: None,
            });
        }
    }
    price_annotations::draw(
        &painter,
        price_rect,
        bars,
        display_slots,
        price_range,
        price_scale,
    );
    crate::chart_trading::draw(
        ui,
        &painter,
        price_rect,
        bars,
        display_slots,
        interval,
        price_range,
        &trading_overlays,
        price_scale,
        trading_display,
    );
    for (price, color) in [(selected_price, theme::WARNING)] {
        let Some(price) = price.filter(|price| *price > rust_decimal::Decimal::ZERO) else {
            continue;
        };
        let y = price_y(decimal_to_f64(price));
        if price_rect.top() <= y && y <= price_rect.bottom() {
            painter.extend(egui::Shape::dashed_line(
                &[
                    Pos2::new(price_rect.left(), y),
                    Pos2::new(price_rect.right(), y),
                ],
                Stroke::new(1.0, color),
                5.0,
                4.0,
            ));
        }
    }
    if let Some(pointer) = response
        .hover_pos()
        .filter(|point| content_rect.contains(*point))
    {
        let snapped_x = hovered_index
            .and_then(|index| {
                bar_center_x(price_rect.left(), price_rect.width(), display_slots, index)
            })
            .unwrap_or(pointer.x);
        painter.extend(egui::Shape::dashed_line(
            &[
                Pos2::new(snapped_x, content_rect.top()),
                Pos2::new(snapped_x, content_rect.bottom()),
            ],
            Stroke::new(1.0, theme::TEXT_SECONDARY),
            5.0,
            4.0,
        ));
        if price_rect.contains(pointer) {
            painter.extend(egui::Shape::dashed_line(
                &[
                    Pos2::new(price_rect.left(), pointer.y),
                    Pos2::new(price_rect.right(), pointer.y),
                ],
                Stroke::new(1.0, theme::TEXT_SECONDARY),
                5.0,
                4.0,
            ));
            if let Some(price) =
                price_range.y_to_price(price_rect.top(), price_rect.height(), pointer.y)
            {
                draw_hover_price_readout(
                    &painter,
                    price_rect,
                    pointer.y,
                    price,
                    latest_price,
                    price_scale,
                    settings.chart_text_size,
                );
            }
        }
    }
    if let Some(index) = selected_index {
        draw_candle_readout(
            &painter,
            readout_rect,
            bars,
            index,
            language,
            interval,
            price_scale,
            quantity_scale,
            settings.chart_text_size,
        );
    }
    price_axis::draw(
        &axis_painter,
        Rect::from_min_max(
            Pos2::new(price_rect.right(), price_rect.top()),
            Pos2::new(full_rect.right(), price_rect.bottom()),
        ),
        price_range,
        &trading_overlays,
        price_scale,
        settings.chart_text_size,
    );
    if analysis_active || analysis_used {
        return None;
    }
    response
        .clicked()
        .then(|| response.interact_pointer_pos())
        .flatten()
        .filter(|pointer| price_rect.contains(*pointer) && !price_axis.rect.contains(*pointer))
        .and_then(|pointer| {
            price_range.y_to_price(price_rect.top(), price_rect.height(), pointer.y)
        })
        .and_then(|price| format_f64_fixed(price, price_scale).parse().ok())
}

#[allow(clippy::too_many_arguments)]
fn draw_candle_readout(
    painter: &egui::Painter,
    price_rect: Rect,
    bars: &[UiBar],
    index: usize,
    language: Language,
    interval: crate::chart::ChartInterval,
    price_scale: usize,
    quantity_scale: usize,
    text_size: u8,
) {
    let Some(bar) = bars.get(index) else {
        return;
    };
    let percent = |value| {
        if bar.open.is_zero() {
            rust_decimal::Decimal::ZERO
        } else {
            value * rust_decimal::Decimal::new(100, 0) / bar.open
        }
    };
    let change_percent = percent(bar.close - bar.open);
    let amplitude_percent = percent(bar.high - bar.low);
    let label_format = egui::TextFormat {
        font_id: FontId::proportional(f32::from(text_size)),
        color: theme::TEXT_SECONDARY,
        ..Default::default()
    };
    let value_format = egui::TextFormat {
        color: if bar.close >= bar.open {
            theme::BUY
        } else {
            theme::SELL
        },
        ..label_format.clone()
    };
    let mut stats = egui::text::LayoutJob::default();
    stats.append(
        &format!("{}  ", format_timeline_label(bar.open_time_ms, interval)),
        0.0,
        label_format.clone(),
    );
    for (label, value) in [
        (
            text(language, TextKey::Open),
            format_decimal(bar.open, price_scale),
        ),
        (
            text(language, TextKey::High),
            format_decimal(bar.high, price_scale),
        ),
        (
            text(language, TextKey::Low),
            format_decimal(bar.low, price_scale),
        ),
        (
            text(language, TextKey::Close),
            format_decimal(bar.close, price_scale),
        ),
        (
            text(language, TextKey::Change),
            format!("{:+.2}%", decimal_to_f64(change_percent)),
        ),
        (
            text(language, TextKey::Amplitude),
            format!("{:.2}%", decimal_to_f64(amplitude_percent)),
        ),
        (
            text(language, TextKey::Volume),
            bar.volume.map_or_else(
                || "—".to_owned(),
                |volume| format_decimal(volume, quantity_scale),
            ),
        ),
    ] {
        stats.append(&format!("{label} "), 0.0, label_format.clone());
        stats.append(&format!("{value}  "), 0.0, value_format.clone());
    }
    painter.galley(
        price_rect.left_top() + egui::vec2(6.0, 6.0),
        painter.layout_job(stats),
        theme::TEXT_PRIMARY,
    );
}

fn draw_price_studies(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    price_y: impl Fn(f64) -> f32 + Copy,
    settings: &ChartDisplaySettings,
) {
    let painter = &painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    if settings.ma.enabled {
        draw_triple_price(
            painter,
            rect,
            bars,
            slots,
            studies,
            [|p| p.sma, |p| p.sma_second, |p| p.sma_third],
            settings.ma,
            price_y,
        );
    }
    if settings.ema.enabled {
        draw_triple_price(
            painter,
            rect,
            bars,
            slots,
            studies,
            [|p| p.ema, |p| p.ema_second, |p| p.ema_third],
            settings.ema,
            price_y,
        );
    }
    if settings.wma.enabled {
        draw_triple_price(
            painter,
            rect,
            bars,
            slots,
            studies,
            [|p| p.wma, |p| p.wma_second, |p| p.wma_third],
            settings.wma,
            price_y,
        );
    }
    if settings.bollinger.enabled {
        for (index, (selector, color)) in [
            (
                (|p: &ChartStudyPoint| p.bollinger_upper) as StudySelector,
                settings.bollinger.color(),
            ),
            (
                (|p: &ChartStudyPoint| p.bollinger_middle) as StudySelector,
                settings.bollinger.secondary_color(),
            ),
            (
                (|p: &ChartStudyPoint| p.bollinger_lower) as StudySelector,
                settings.bollinger.color(),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            if !settings.bollinger.line_enabled[index] {
                continue;
            }
            draw_study_line(
                painter,
                rect,
                bars,
                slots,
                studies,
                selector,
                price_y,
                Stroke::new(settings.bollinger.line_width(), color),
            );
        }
    }
    for (enabled, selector, style) in [
        (
            settings.vwap.enabled,
            (|p: &ChartStudyPoint| p.vwap) as StudySelector,
            settings.vwap,
        ),
        (
            settings.avl.enabled,
            (|p: &ChartStudyPoint| p.avl) as StudySelector,
            settings.avl,
        ),
        (
            settings.trix.enabled,
            (|p: &ChartStudyPoint| p.trix) as StudySelector,
            settings.trix,
        ),
    ] {
        if enabled && style.line_enabled[0] {
            draw_study_line(
                painter,
                rect,
                bars,
                slots,
                studies,
                selector,
                price_y,
                Stroke::new(style.line_width(), style.color()),
            );
        }
    }
    if settings.sar.enabled {
        draw_directional_price(
            painter,
            rect,
            bars,
            slots,
            studies,
            |point| point.sar,
            |point| point.sar_rising,
            settings.sar,
            price_y,
            false,
        );
    }
    if settings.supertrend.enabled {
        draw_directional_price(
            painter,
            rect,
            bars,
            slots,
            studies,
            |point| point.supertrend,
            |point| point.supertrend_rising,
            settings.supertrend,
            price_y,
            true,
        );
    }
}

fn draw_triple_price(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    selectors: [StudySelector; 3],
    style: crate::chart_settings::IndicatorStyle,
    price_y: impl Fn(f64) -> f32 + Copy,
) {
    let colors = [
        style.color(),
        style.secondary_color(),
        style.tertiary_color(),
    ];
    for index in 0..3 {
        if style.line_enabled[index] {
            draw_study_line(
                painter,
                rect,
                bars,
                slots,
                studies,
                selectors[index],
                price_y,
                Stroke::new(style.line_width(), colors[index]),
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn draw_directional_price(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    selector: StudySelector,
    rising: fn(&ChartStudyPoint) -> bool,
    style: crate::chart_settings::IndicatorStyle,
    price_y: impl Fn(f64) -> f32,
    connected: bool,
) {
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    let mut previous: Option<(Pos2, bool)> = None;
    for (index, bar) in bars.iter().enumerate() {
        let Some(point) = study_at(studies, bar.open_time_ms) else {
            previous = None;
            continue;
        };
        let Some(value) = selector(point) else {
            previous = None;
            continue;
        };
        let Some(x) = bar_center_x(rect.left(), rect.width(), slots, index) else {
            continue;
        };
        let current = Pos2::new(x, price_y(decimal_to_f64(value)));
        let is_rising = rising(point);
        if !style.line_enabled[usize::from(!is_rising)] {
            previous = None;
            continue;
        }
        let color = if is_rising {
            style.color()
        } else {
            style.secondary_color()
        };
        if connected {
            if let Some((left, was_rising)) = previous
                && was_rising == is_rising
            {
                painter.line_segment([left, current], Stroke::new(style.line_width(), color));
            }
        } else {
            let radius = (style.line_width() + 0.4)
                .min(rect.width() / slots.max(1) as f32 * 0.25)
                .max(0.6);
            painter.circle_filled(current, radius, color);
        }
        previous = Some((current, is_rising));
    }
}

// Fills are inserted before candles. Missing values and direction changes break the mesh.
fn draw_price_fills(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    price_y: impl Fn(f64) -> f32,
    settings: &ChartDisplaySettings,
) {
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    for pair in bars.windows(2).enumerate() {
        let (index, pair) = pair;
        let (Some(left), Some(right)) = (
            study_at(studies, pair[0].open_time_ms),
            study_at(studies, pair[1].open_time_ms),
        ) else {
            continue;
        };
        let (Some(x0), Some(x1)) = (
            bar_center_x(rect.left(), rect.width(), slots, index),
            bar_center_x(rect.left(), rect.width(), slots, index + 1),
        ) else {
            continue;
        };
        let band = settings.bollinger;
        if band.enabled
            && band.background_enabled
            && let (Some(u0), Some(l0), Some(u1), Some(l1)) = (
                left.bollinger_upper,
                left.bollinger_lower,
                right.bollinger_upper,
                right.bollinger_lower,
            )
        {
            fill_between(
                &painter,
                [x0, x1],
                [price_y(decimal_to_f64(u0)), price_y(decimal_to_f64(u1))],
                [price_y(decimal_to_f64(l0)), price_y(decimal_to_f64(l1))],
                band.fill_color(band.color()),
            );
        }
        let trend = settings.supertrend;
        let rising = right.supertrend_rising;
        if trend.enabled
            && left.supertrend_rising == rising
            && (if rising {
                trend.background_enabled
            } else {
                trend.secondary_background_enabled
            })
            && let (Some(v0), Some(v1)) = (left.supertrend, right.supertrend)
        {
            let midpoint =
                |bar: &UiBar| price_y((decimal_to_f64(bar.open) + decimal_to_f64(bar.close)) * 0.5);
            fill_between(
                &painter,
                [x0, x1],
                [price_y(decimal_to_f64(v0)), price_y(decimal_to_f64(v1))],
                [midpoint(&pair[0]), midpoint(&pair[1])],
                trend.fill_color(if rising {
                    trend.color()
                } else {
                    trend.secondary_color()
                }),
            );
        }
    }
}

fn fill_between(painter: &egui::Painter, x: [f32; 2], a: [f32; 2], b: [f32; 2], color: Color32) {
    if color.a() == 0 || x.into_iter().chain(a).chain(b).any(|v| !v.is_finite()) {
        return;
    }
    let mut mesh = egui::Mesh::default();
    let vertices = [
        Pos2::new(x[0], a[0]),
        Pos2::new(x[1], a[1]),
        Pos2::new(x[1], b[1]),
        Pos2::new(x[0], b[0]),
    ];
    for vertex in vertices {
        mesh.colored_vertex(vertex, color);
    }
    let d0 = a[0] - b[0];
    let d1 = a[1] - b[1];
    if d0 * d1 < 0.0 {
        // Split at the intersection to avoid overlapping translucent triangles.
        let t = d0 / (d0 - d1);
        mesh.colored_vertex(
            Pos2::new(x[0] + (x[1] - x[0]) * t, a[0] + (a[1] - a[0]) * t),
            color,
        );
        mesh.add_triangle(0, 4, 3);
        mesh.add_triangle(4, 1, 2);
    } else {
        mesh.add_triangle(0, 1, 2);
        mesh.add_triangle(0, 2, 3);
    }
    painter.add(egui::Shape::mesh(mesh));
}
#[allow(clippy::too_many_arguments)]
fn draw_study_line(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    selector: fn(&ChartStudyPoint) -> Option<rust_decimal::Decimal>,
    value_y: impl Fn(f64) -> f32,
    stroke: Stroke,
) {
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    let mut previous = None;
    for (index, bar) in bars.iter().enumerate() {
        let current = study_at(studies, bar.open_time_ms)
            .and_then(selector)
            .and_then(|value| {
                bar_center_x(rect.left(), rect.width(), slots, index)
                    .map(|x| Pos2::new(x, value_y(decimal_to_f64(value))))
            });
        if let (Some(left), Some(right)) = (previous, current) {
            painter.line_segment([left, right], stroke);
        }
        previous = current;
    }
}
fn pane_specs(settings: &ChartDisplaySettings) -> Vec<PaneSpec> {
    let mut panes = Vec::new();
    let mut add = |enabled: bool,
                   label: String,
                   selectors: [Option<StudySelector>; 3],
                   series_labels: [&'static str; 3],
                   style: crate::chart_settings::IndicatorStyle,
                   scale: PaneScale,
                   histogram: bool| {
        if enabled {
            panes.push(PaneSpec {
                label,
                selectors: std::array::from_fn(|i| {
                    style.line_enabled[i].then_some(selectors[i]).flatten()
                }),
                series_labels,
                colors: [
                    style.color(),
                    style.secondary_color(),
                    style.tertiary_color(),
                ],
                width: style.line_width(),
                scale,
                histogram,
                histogram_colors: style
                    .histogram_colors
                    .map(|[r, g, b]| Color32::from_rgb(r, g, b)),
                reference_levels: &[],
            });
        }
    };
    add(
        settings.microstructure.show_delta,
        "Delta · base".into(),
        [None, None, Some(|p| p.order_flow.delta)],
        ["", "", "Delta"],
        crate::chart_settings::IndicatorStyle::new(true, [240, 185, 11], [14, 203, 129]),
        PaneScale::Symmetric,
        true,
    );
    add(
        settings.microstructure.show_cvd,
        "CVD · base".into(),
        [Some(|p| p.order_flow.cumulative), None, None],
        ["CVD", "", ""],
        crate::chart_settings::IndicatorStyle::new(true, [240, 185, 11], [14, 203, 129]),
        PaneScale::Auto,
        false,
    );
    add(
        settings.oi_pane,
        "OI · base".into(),
        [Some(|p| p.open_interest), None, None],
        ["OI", "", ""],
        crate::chart_settings::IndicatorStyle::new(true, [91, 159, 255], [91, 159, 255]),
        PaneScale::Positive,
        false,
    );
    add(
        settings.macd.enabled,
        format!(
            "MACD({},{},{})",
            settings.macd_fast_period, settings.macd_slow_period, settings.macd_signal_period
        ),
        [
            Some(|p| p.macd),
            Some(|p| p.macd_signal),
            Some(|p| p.macd_histogram),
        ],
        ["DIF", "DEA", "HIST"],
        settings.macd,
        PaneScale::Symmetric,
        true,
    );
    add(
        settings.rsi.enabled,
        format!("RSI({})", settings.rsi_period),
        [Some(|p| p.rsi), None, None],
        ["RSI", "", ""],
        settings.rsi,
        PaneScale::ZeroToHundred,
        false,
    );
    add(
        settings.mfi.enabled,
        format!("MFI({})", settings.mfi_period),
        [Some(|p| p.mfi), None, None],
        ["MFI", "", ""],
        settings.mfi,
        PaneScale::ZeroToHundred,
        false,
    );
    add(
        settings.kdj.enabled,
        format!(
            "KDJ({},{})",
            settings.kdj_period, settings.kdj_signal_period
        ),
        [Some(|p| p.kdj_k), Some(|p| p.kdj_d), Some(|p| p.kdj_j)],
        ["K", "D", "J"],
        settings.kdj,
        PaneScale::ZeroToHundred,
        false,
    );
    add(
        settings.obv.enabled,
        "OBV".to_owned(),
        [Some(|p| p.obv), None, None],
        ["OBV", "", ""],
        settings.obv,
        PaneScale::Auto,
        false,
    );
    add(
        settings.cci.enabled,
        format!("CCI({})", settings.cci_period),
        [Some(|p| p.cci), None, None],
        ["CCI", "", ""],
        settings.cci,
        PaneScale::Symmetric,
        false,
    );
    add(
        settings.stoch_rsi.enabled,
        format!(
            "StochRSI({},{},{})",
            settings.stoch_rsi_period,
            settings.stoch_rsi_stochastic_period,
            settings.stoch_rsi_signal_period
        ),
        [Some(|p| p.stoch_rsi_k), Some(|p| p.stoch_rsi_d), None],
        ["K", "D", ""],
        settings.stoch_rsi,
        PaneScale::ZeroToHundred,
        false,
    );
    add(
        settings.williams_r.enabled,
        format!("WR({})", settings.williams_r_period),
        [Some(|p| p.williams_r), None, None],
        ["WR", "", ""],
        settings.williams_r,
        PaneScale::MinusHundredToZero,
        false,
    );
    add(
        settings.dmi.enabled,
        format!("DMI({})", settings.dmi_period),
        [
            Some(|p| p.dmi_plus),
            Some(|p| p.dmi_minus),
            Some(|p| p.dmi_adx),
        ],
        ["+DI", "-DI", "ADX"],
        settings.dmi,
        PaneScale::Positive,
        false,
    );
    add(
        settings.momentum.enabled,
        format!("MTM({})", settings.momentum_period),
        [Some(|p| p.momentum), None, None],
        ["MTM", "", ""],
        settings.momentum,
        PaneScale::Symmetric,
        false,
    );
    add(
        settings.emv.enabled,
        format!("EMV({})", settings.emv_period),
        [Some(|p| p.emv), None, None],
        ["EMV", "", ""],
        settings.emv,
        PaneScale::Symmetric,
        false,
    );
    add(
        settings.atr.enabled,
        format!("ATR({})", settings.atr_period),
        [Some(|p| p.atr), None, None],
        ["ATR", "", ""],
        settings.atr,
        PaneScale::Positive,
        false,
    );
    for pane in &mut panes {
        pane.reference_levels = if pane.label.starts_with("RSI(") {
            &[30.0, 50.0, 70.0]
        } else if pane.label.starts_with("CCI(") {
            &[-100.0, 0.0, 100.0]
        } else {
            match pane.scale {
                PaneScale::ZeroToHundred => &[20.0, 50.0, 80.0],
                PaneScale::MinusHundredToZero => &[-80.0, -50.0, -20.0],
                _ => &[0.0],
            }
        };
    }
    panes
}

#[allow(clippy::too_many_arguments)]
fn draw_sub_pane(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    spec: &PaneSpec,
    text_size: u8,
    selected_index: Option<usize>,
) {
    let painter = &painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    painter.line_segment(
        [rect.left_top(), rect.right_top()],
        Stroke::new(1.0, theme::DIVIDER),
    );
    let mut values = Vec::new();
    for bar in bars {
        let Some(point) = study_at(studies, bar.open_time_ms) else {
            continue;
        };
        for selector in spec.selectors.into_iter().flatten() {
            if let Some(value) = selector(point) {
                values.push(decimal_to_f64(value));
            }
        }
    }
    let (low, high) = match spec.scale {
        PaneScale::ZeroToHundred => (
            values.iter().copied().fold(0.0_f64, f64::min),
            values.iter().copied().fold(100.0_f64, f64::max),
        ),
        PaneScale::MinusHundredToZero => (-100.0, 0.0),
        PaneScale::Symmetric => {
            let maximum = values
                .iter()
                .copied()
                .map(f64::abs)
                .fold(0.0_f64, f64::max)
                .max(f64::EPSILON);
            (-maximum, maximum)
        }
        PaneScale::Positive => {
            let maximum = values
                .iter()
                .copied()
                .fold(0.0_f64, f64::max)
                .max(f64::EPSILON);
            (0.0, maximum)
        }
        PaneScale::Auto => {
            let low = values.iter().copied().reduce(f64::min).unwrap_or(0.0);
            let high = values.iter().copied().reduce(f64::max).unwrap_or(1.0);
            (low, high.max(low + low.abs().max(1.0) * 0.001))
        }
    };
    let y = |value: f64| {
        let normalized = ((value - low) / (high - low)).clamp(0.0, 1.0);
        rect.bottom() - normalized as f32 * rect.height() * 0.88
    };
    let precision = sub_pane_precision(low, high, rect.height());
    for level in spec
        .reference_levels
        .iter()
        .filter(|&&v| v >= low && v <= high)
    {
        painter.line_segment(
            [
                Pos2::new(rect.left(), y(*level)),
                Pos2::new(rect.right(), y(*level)),
            ],
            Stroke::new(0.75, theme::CHART_GRID),
        );
    }
    if spec.histogram {
        let width = rect.width() / slots.max(1) as f32;
        if let Some(selector) = spec.selectors[2] {
            for (index, bar) in bars.iter().enumerate() {
                let Some(value) = study_at(studies, bar.open_time_ms).and_then(selector) else {
                    continue;
                };
                let value = decimal_to_f64(value);
                let Some(x) = bar_center_x(rect.left(), rect.width(), slots, index) else {
                    continue;
                };
                painter.rect_filled(
                    Rect::from_two_pos(
                        Pos2::new(x - width * 0.28, y(0.0)),
                        Pos2::new(x + width * 0.28, y(value)),
                    ),
                    0.0,
                    if value >= 0.0 {
                        spec.histogram_colors[0]
                    } else {
                        spec.histogram_colors[1]
                    },
                );
            }
        }
    }
    for index in 0..3 {
        if spec.histogram && index == 2 {
            continue;
        }
        if let Some(selector) = spec.selectors[index] {
            draw_study_line(
                painter,
                rect,
                bars,
                slots,
                studies,
                selector,
                y,
                Stroke::new(spec.width, spec.colors[index]),
            );
        }
    }
    let mut readout = egui::text::LayoutJob::default();
    let label_format = egui::TextFormat {
        font_id: FontId::proportional(f32::from(text_size)),
        color: theme::TEXT_SECONDARY,
        ..Default::default()
    };
    readout.append(&spec.label, 0.0, label_format.clone());
    if let Some(point) = selected_index
        .and_then(|index| bars.get(index))
        .and_then(|bar| study_at(studies, bar.open_time_ms))
    {
        for (index, selector) in spec.selectors.into_iter().enumerate() {
            let Some(value) = selector.and_then(|selector| selector(point)) else {
                continue;
            };
            let series_label = spec.series_labels[index];
            if !series_label.is_empty() {
                readout.append(&format!("  {series_label} "), 0.0, label_format.clone());
            }
            readout.append(
                &format_f64_fixed(decimal_to_f64(value), precision),
                0.0,
                egui::TextFormat {
                    color: if spec.histogram && index == 2 {
                        spec.histogram_colors[usize::from(value.is_sign_negative())]
                    } else {
                        spec.colors[index]
                    },
                    ..label_format.clone()
                },
            );
        }
    }
    painter.galley(
        rect.left_top() + egui::vec2(4.0, 2.0),
        painter.layout_job(readout),
        theme::TEXT_PRIMARY,
    );
}

fn volume_readout(bars: &[UiBar], selected_index: Option<usize>, quantity_scale: usize) -> String {
    selected_index
        .and_then(|index| bars.get(index))
        .map_or_else(
            || "VOL".to_owned(),
            |bar| {
                format!(
                    "VOL  {}",
                    bar.volume.map_or_else(
                        || "—".to_owned(),
                        |volume| format_decimal(volume, quantity_scale)
                    )
                )
            },
        )
}
fn study_at(studies: &[ChartStudyPoint], open_time_ms: u64) -> Option<&ChartStudyPoint> {
    studies
        .binary_search_by_key(&open_time_ms, |point| point.open_time_ms)
        .ok()
        .and_then(|index| studies.get(index))
}

fn draw_hover_price_readout(
    painter: &egui::Painter,
    price_rect: Rect,
    pointer_y: f32,
    price: f64,
    latest_price: Option<rust_decimal::Decimal>,
    price_scale: usize,
    text_size: u8,
) {
    let price_text = format_f64_fixed(price, price_scale);
    let change_text = hover_price_change_percent(price, latest_price)
        .map_or_else(|| "—".to_owned(), |change| format!("{change:+.4}%"));
    let font = FontId::proportional(f32::from(text_size));
    let price_galley = painter.layout_no_wrap(price_text, font.clone(), theme::TEXT_PRIMARY);
    let change_galley = painter.layout_no_wrap(change_text, font, theme::TEXT_PRIMARY);
    let width = price_galley.size().x.max(change_galley.size().x).max(72.0) + 12.0;
    let height = price_galley.size().y + change_galley.size().y + 8.0;
    let center_y = pointer_y.clamp(
        price_rect.top() + height * 0.5,
        price_rect.bottom() - height * 0.5,
    );
    let rect = Rect::from_center_size(
        Pos2::new(price_rect.right() - width * 0.5, center_y),
        egui::vec2(width, height),
    );
    painter.rect_filled(rect, 4.0, theme::DIVIDER);
    painter.galley(
        Pos2::new(
            rect.center().x - price_galley.size().x * 0.5,
            rect.top() + 3.0,
        ),
        price_galley,
        theme::TEXT_PRIMARY,
    );
    painter.galley(
        Pos2::new(
            rect.center().x - change_galley.size().x * 0.5,
            rect.bottom() - change_galley.size().y - 3.0,
        ),
        change_galley,
        theme::TEXT_PRIMARY,
    );
}

fn hover_price_change_percent(
    price: f64,
    latest_price: Option<rust_decimal::Decimal>,
) -> Option<f64> {
    let latest = latest_price.map(decimal_to_f64)?;
    (latest.is_finite() && latest > 0.0 && price.is_finite())
        .then(|| (price / latest - 1.0) * 100.0)
}

fn format_f64_fixed(value: f64, precision: usize) -> String {
    format!("{value:.precision$}")
}

fn sub_pane_precision(low: f64, high: f64, height: f32) -> usize {
    let resolution = (high - low).abs() / f64::from(height.max(1.0));
    if !resolution.is_finite() || resolution <= 0.0 {
        return 2;
    }
    (-resolution.log10().floor()).clamp(0.0, 12.0) as usize
}

#[cfg(test)]
#[path = "chart_view/tests.rs"]
mod tests;
