use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Stroke};
use venue_control_protocol::UiBar;

use crate::{
    chart::{ChartStudyPoint, PriceRange, bar_center_x, bar_index_at_x, format_timeline_label},
    chart_settings::ChartDisplaySettings,
    i18n::{Language, TextKey, text},
    model::{decimal_to_f64, format_decimal},
    theme,
};

type StudySelector = fn(&ChartStudyPoint) -> Option<rust_decimal::Decimal>;

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

#[allow(clippy::too_many_arguments)]
pub(crate) fn candle_plot(
    ui: &mut egui::Ui,
    all_bars: &[UiBar],
    all_studies: &[ChartStudyPoint],
    viewport: &mut crate::chart::ChartViewport,
    language: Language,
    settings: &ChartDisplaySettings,
    scales: (usize, usize),
    interval: crate::chart::ChartInterval,
    market_price: Option<rust_decimal::Decimal>,
    selected_price: Option<rust_decimal::Decimal>,
) -> Option<rust_decimal::Decimal> {
    let (price_scale, quantity_scale) = scales;
    let height = ui.available_height().max(120.0);
    let (response, painter) = ui.allocate_painter(
        egui::vec2(ui.available_width(), height),
        Sense::click_and_drag(),
    );
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

    let plot_rect = response.rect.shrink2(egui::vec2(8.0, 8.0));
    let timeline_height = 22.0_f32.min(plot_rect.height() * 0.18);
    let content_rect = Rect::from_min_max(
        plot_rect.min,
        Pos2::new(plot_rect.right(), plot_rect.bottom() - timeline_height),
    );
    let timeline_rect = Rect::from_min_max(
        Pos2::new(plot_rect.left(), content_rect.bottom()),
        plot_rect.max,
    );
    if response
        .hover_pos()
        .is_some_and(|point| timeline_rect.contains(point))
    {
        ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeHorizontal);
    }
    let pointer_ratio = response.hover_pos().map_or(1.0, |point| {
        ((point.x - plot_rect.left()) / plot_rect.width()).clamp(0.0, 1.0)
    });
    if response.hovered() {
        let wheel = ui.input(|input| input.smooth_scroll_delta.y);
        if wheel.abs() > f32::EPSILON {
            viewport.zoom_by_steps(
                all_bars.len(),
                pointer_ratio,
                if wheel > 0.0 { 1 } else { -1 },
            );
        }
    }
    if response.dragged() {
        viewport.pan_by_drag_total(all_bars.len(), plot_rect.width(), response.drag_delta().x);
    }
    if response.drag_stopped() {
        viewport.pan_by_drag_total(all_bars.len(), plot_rect.width(), response.drag_delta().x);
        viewport.finish_drag();
    }

    let range = viewport.visible_range(all_bars.len());
    let bars = &all_bars[range.clone()];
    let display_slots = viewport.display_slots(bars.len());
    let has_local_studies = !all_studies.is_empty();
    let pane_specs = if has_local_studies {
        pane_specs(settings)
    } else {
        Vec::new()
    };
    let sub_count = pane_specs.len();
    let volume_ratio = if settings.volume.enabled { 0.14 } else { 0.0 };
    let sub_ratio = (0.145 * sub_count as f32).min(0.48);
    let price_ratio = (1.0 - volume_ratio - sub_ratio).clamp(0.38, 0.86);
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
    let price_range = overlay_price_range(bars, all_studies, settings)?;
    let selected_index = response
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
        .filter(|index| *index < bars.len())
        .or_else(|| bars.len().checked_sub(1));
    let width = price_rect.width() / display_slots as f32;
    let price_y = |price: f64| {
        price_range
            .price_to_y(price_rect.top(), price_rect.height(), price)
            .unwrap_or(price_rect.center().y)
    };
    for price in price_range.grid_prices(price_scale, 5) {
        let y = price_y(price);
        painter.line_segment(
            [
                Pos2::new(price_rect.left(), y),
                Pos2::new(price_rect.right(), y),
            ],
            Stroke::new(1.0, theme::CHART_GRID),
        );
        painter.text(
            Pos2::new(price_rect.right() - 3.0, y - 2.0),
            Align2::RIGHT_BOTTOM,
            format_f64_trimmed(price, price_scale),
            FontId::monospace(f32::from(settings.chart_text_size)),
            theme::TEXT_SECONDARY,
        );
    }
    painter.line_segment(
        [timeline_rect.left_top(), timeline_rect.right_top()],
        Stroke::new(1.0, theme::DIVIDER),
    );
    for (index, bar) in bars
        .iter()
        .enumerate()
        .filter(|(_, bar)| bar.open_time_ms.is_multiple_of(interval.timeline_step_ms()))
    {
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
            Pos2::new(x, timeline_rect.top() + 4.0),
            Align2::CENTER_TOP,
            format_timeline_label(bar.open_time_ms, interval),
            FontId::monospace(f32::from(settings.chart_text_size)),
            theme::TEXT_SECONDARY,
        );
    }
    let maximum_volume = bars
        .iter()
        .map(|bar| decimal_to_f64(bar.volume))
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
        painter.line_segment(
            [
                Pos2::new(x, price_y(decimal_to_f64(bar.low))),
                Pos2::new(x, price_y(decimal_to_f64(bar.high))),
            ],
            Stroke::new(1.0, color),
        );
        let top = price_y(open.max(close));
        let bottom = price_y(open.min(close));
        let body = Rect::from_min_max(
            Pos2::new(x - width * 0.31, top),
            Pos2::new(x + width * 0.31, bottom.max(top + 1.0)),
        );
        painter.rect_filled(body, 0.5, color);
        if let Some(volume_rect) = volume_rect {
            let color = if close >= open {
                settings.volume.color()
            } else {
                settings.volume.secondary_color()
            };
            let volume_height =
                (decimal_to_f64(bar.volume) / maximum_volume) as f32 * volume_rect.height();
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
            FontId::monospace(f32::from(settings.chart_text_size)),
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
        for (spec, rect) in pane_specs.iter().zip(sub_rects) {
            draw_sub_pane(
                &painter,
                rect,
                bars,
                display_slots,
                all_studies,
                spec,
                settings.chart_text_size,
                selected_index,
            );
        }
    }
    for (price, color) in [
        (
            market_price.or_else(|| all_bars.last().map(|bar| bar.close)),
            theme::BUY,
        ),
        (selected_price, theme::WARNING),
    ] {
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
        painter.extend(egui::Shape::dashed_line(
            &[
                Pos2::new(pointer.x, content_rect.top()),
                Pos2::new(pointer.x, content_rect.bottom()),
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
                painter.text(
                    Pos2::new(price_rect.right() - 4.0, pointer.y - 4.0),
                    Align2::RIGHT_BOTTOM,
                    format_f64_trimmed(price, price_scale),
                    FontId::monospace(f32::from(settings.chart_text_size)),
                    theme::TEXT_PRIMARY,
                );
            }
        }
    }
    if let Some(index) = selected_index {
        draw_candle_readout(
            &painter,
            price_rect,
            bars,
            index,
            language,
            interval,
            price_scale,
            quantity_scale,
            settings.chart_text_size,
        );
    }
    response
        .clicked()
        .then(|| response.interact_pointer_pos())
        .flatten()
        .filter(|pointer| price_rect.contains(*pointer))
        .and_then(|pointer| {
            price_range.y_to_price(price_rect.top(), price_rect.height(), pointer.y)
        })
        .and_then(|price| format_f64_trimmed(price, price_scale).parse().ok())
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
        font_id: FontId::monospace(f32::from(text_size)),
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
            format_decimal(bar.volume, quantity_scale),
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

    let mut labels = Vec::new();
    if settings.ma.enabled {
        labels.push(format!(
            "MA({},{},{})",
            settings.ma_periods[0], settings.ma_periods[1], settings.ma_periods[2]
        ));
    }
    if settings.ema.enabled {
        labels.push(format!(
            "EMA({},{},{})",
            settings.ema_periods[0], settings.ema_periods[1], settings.ema_periods[2]
        ));
    }
    if settings.wma.enabled {
        labels.push(format!(
            "WMA({},{},{})",
            settings.wma_periods[0], settings.wma_periods[1], settings.wma_periods[2]
        ));
    }
    if settings.bollinger.enabled {
        labels.push(format!(
            "BOLL({},{:.2})",
            settings.bollinger_period,
            settings.bollinger_multiplier_hundredths as f32 / 100.0
        ));
    }
    if settings.vwap.enabled {
        labels.push("VWAP".to_owned());
    }
    if settings.avl.enabled {
        labels.push("AVL".to_owned());
    }
    if settings.trix.enabled {
        labels.push(format!("TRIX({})", settings.trix_period));
    }
    if settings.sar.enabled {
        labels.push("SAR".to_owned());
    }
    if settings.supertrend.enabled {
        labels.push(format!(
            "SUPER({},{:.2})",
            settings.supertrend_period,
            settings.supertrend_multiplier_hundredths as f32 / 100.0
        ));
    }
    if !labels.is_empty() {
        painter.text(
            rect.left_top() + egui::vec2(6.0, 22.0),
            Align2::LEFT_TOP,
            labels.join("  "),
            FontId::monospace(f32::from(settings.chart_text_size)),
            theme::TEXT_SECONDARY,
        );
    }
}

fn overlay_price_range(
    bars: &[UiBar],
    studies: &[ChartStudyPoint],
    settings: &ChartDisplaySettings,
) -> Option<PriceRange> {
    let range = PriceRange::from_bars(bars)?;
    let (mut low, mut high) = (range.low, range.high);
    for point in bars
        .iter()
        .filter_map(|bar| study_at(studies, bar.open_time_ms))
    {
        for (style, values) in [
            (settings.ma, [point.sma, point.sma_second, point.sma_third]),
            (settings.ema, [point.ema, point.ema_second, point.ema_third]),
            (settings.wma, [point.wma, point.wma_second, point.wma_third]),
            (
                settings.bollinger,
                [
                    point.bollinger_upper,
                    point.bollinger_middle,
                    point.bollinger_lower,
                ],
            ),
            (settings.vwap, [point.vwap, None, None]),
            (settings.avl, [point.avl, None, None]),
            (settings.sar, [point.sar, None, None]),
            (settings.supertrend, [point.supertrend, None, None]),
        ] {
            if !style.enabled {
                continue;
            }
            for (index, value) in values.into_iter().enumerate() {
                if !style.line_enabled[index]
                    && !style.background_enabled
                    && !style.secondary_background_enabled
                {
                    continue;
                }
                if let Some(value) = value {
                    let value = decimal_to_f64(value);
                    low = low.min(value);
                    high = high.max(value);
                }
            }
        }
    }
    if low < range.low || high > range.high {
        PriceRange::from_extrema(low, high)
    } else {
        Some(range)
    }
}

#[allow(clippy::too_many_arguments)]
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
        font_id: FontId::monospace(f32::from(text_size)),
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
                &format_f64_trimmed(decimal_to_f64(value), 6),
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
            |bar| format!("VOL  {}", format_decimal(bar.volume, quantity_scale)),
        )
}
fn study_at(studies: &[ChartStudyPoint], open_time_ms: u64) -> Option<&ChartStudyPoint> {
    studies
        .binary_search_by_key(&open_time_ms, |point| point.open_time_ms)
        .ok()
        .and_then(|index| studies.get(index))
}

fn format_f64_trimmed(value: f64, precision: usize) -> String {
    let rendered = format!("{value:.precision$}");
    if precision == 0 {
        return rendered;
    }
    let trimmed = rendered.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-0" {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn fixture() -> (Vec<UiBar>, Vec<ChartStudyPoint>) {
        let bars = (0..6)
            .map(|i| UiBar {
                open_time_ms: i * 60_000,
                open: Decimal::from(50),
                high: Decimal::from(55),
                low: Decimal::from(45),
                close: Decimal::from(52),
                volume: Decimal::ONE,
            })
            .collect::<Vec<_>>();
        let studies = bars
            .iter()
            .enumerate()
            .map(|(i, bar)| ChartStudyPoint {
                open_time_ms: bar.open_time_ms,
                supertrend: Some(Decimal::from(if i < 3 { 40 } else { 60 })),
                supertrend_rising: i < 3,
                bollinger_upper: Some(Decimal::from(70)),
                bollinger_lower: Some(Decimal::from(30)),
                ..Default::default()
            })
            .collect();
        (bars, studies)
    }

    fn render(draw: impl Fn(&egui::Painter, Rect)) -> Vec<egui::epaint::ClippedShape> {
        let ctx = egui::Context::default();
        let mut output = ctx.run_ui(egui::RawInput::default(), |ui| {
            let painter = ui.ctx().layer_painter(egui::LayerId::background());
            draw(
                &painter,
                Rect::from_min_size(Pos2::ZERO, egui::vec2(300.0, 100.0)),
            );
        });
        output.textures_delta.clear();
        output.shapes
    }

    #[test]
    fn supertrend_breaks_lines_at_reversals_and_missing_studies() {
        let (bars, mut studies) = fixture();
        studies.remove(1);
        let shapes = render(|painter, rect| {
            draw_directional_price(
                painter,
                rect,
                &bars,
                6,
                &studies,
                |p| p.supertrend,
                |p| p.supertrend_rising,
                ChartDisplaySettings::default().supertrend,
                |v| v as f32,
                true,
            );
        });
        let lines = shapes
            .iter()
            .filter_map(|shape| match &shape.shape {
                egui::Shape::LineSegment { points, stroke } => Some((points, stroke)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(
            lines
                .iter()
                .all(|(points, stroke)| points[0].x >= 175.0 && stroke.color == theme::SELL)
        );
    }

    #[test]
    fn trend_fill_stays_between_trend_and_body_and_stops_at_reversal() {
        let (bars, studies) = fixture();
        let mut settings = ChartDisplaySettings::default();
        settings.supertrend.enabled = true;
        let shapes = render(|painter, rect| {
            draw_price_fills(painter, rect, &bars, 6, &studies, |v| v as f32, &settings)
        });
        let meshes = shapes
            .iter()
            .filter_map(|shape| match &shape.shape {
                egui::Shape::Mesh(mesh) => Some(mesh),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(meshes.len(), 4);
        for mesh in meshes {
            assert!(
                mesh.vertices
                    .iter()
                    .all(|v| (40.0..=60.0).contains(&v.pos.y))
            );
            let min_x = mesh
                .vertices
                .iter()
                .map(|v| v.pos.x)
                .fold(f32::INFINITY, f32::min);
            let max_x = mesh
                .vertices
                .iter()
                .map(|v| v.pos.x)
                .fold(f32::NEG_INFINITY, f32::max);
            assert!(max_x <= 125.0 || min_x >= 175.0);
        }
    }

    #[test]
    fn enabled_bands_fit_price_scale_and_fill_does_not_bridge_missing_values() {
        let (bars, mut studies) = fixture();
        studies[2].bollinger_upper = None;
        let mut settings = ChartDisplaySettings::default();
        settings.bollinger.enabled = true;
        let range = overlay_price_range(&bars, &studies, &settings);
        assert!(range.is_some_and(|r| r.low < 30.0 && r.high > 70.0));
        let shapes = render(|painter, rect| {
            draw_price_fills(painter, rect, &bars, 6, &studies, |v| v as f32, &settings)
        });
        assert_eq!(
            shapes
                .iter()
                .filter(|s| matches!(s.shape, egui::Shape::Mesh(_)))
                .count(),
            3
        );
        assert!(shapes.iter().all(|s| s.clip_rect.max.y <= 100.0));
    }
}
