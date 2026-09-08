use eframe::egui::{self, Align2, FontId, Pos2, Rect, Stroke};
use venue_control_protocol::UiBar;

use crate::{
    chart::PriceRange,
    chart_trading::ChartOverlay,
    model::{decimal_to_f64, format_decimal},
    theme,
};

pub(super) fn width(
    painter: &egui::Painter,
    bars: &[UiBar],
    overlays: &[ChartOverlay],
    scale: usize,
) -> f32 {
    let largest = bars
        .iter()
        .map(|bar| bar.high)
        .chain(overlays.iter().map(|item| item.price))
        .max();
    let label = largest.map_or_else(|| "0.00000000".into(), |price| format_decimal(price, scale));
    (painter
        .layout_no_wrap(label, FontId::proportional(12.0), theme::TEXT_PRIMARY)
        .size()
        .x
        + 12.0)
        .max(60.0)
}

pub(super) fn draw(
    painter: &egui::Painter,
    rect: Rect,
    range: PriceRange,
    overlays: &[ChartOverlay],
    scale: usize,
    text_size: u8,
) {
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    painter.rect_filled(rect, 0, theme::BG_PRIMARY);
    painter.line_segment(
        [rect.left_top(), rect.left_bottom()],
        Stroke::new(1.0, theme::DIVIDER),
    );
    for price in range.grid_prices(scale, 5) {
        if let Some(y) = range.price_to_y(rect.top(), rect.height(), price) {
            painter.text(
                Pos2::new(rect.left() + 7.0, y),
                Align2::LEFT_CENTER,
                super::format_f64_fixed(price, scale),
                FontId::proportional(f32::from(text_size)),
                theme::TEXT_SECONDARY,
            );
        }
    }
    // Axis prices belong to visible horizontal lines, including order/position badges.
    // Historical fills remain anchored to candles and never become axis labels.
    for overlay in overlays
        .iter()
        .filter(|item| item.line && item.time_ms.is_none())
    {
        let Some(y) = range.price_to_y(rect.top(), rect.height(), decimal_to_f64(overlay.price))
        else {
            continue;
        };
        if y < rect.top() || y > rect.bottom() {
            continue;
        }
        let latest = overlay.label.is_empty();
        let galley = painter.layout_no_wrap(
            format_decimal(overlay.price, scale),
            FontId::proportional(12.0),
            if latest {
                theme::BG_PRIMARY
            } else {
                overlay.color
            },
        );
        let height = (galley.size().y + 4.0).min(rect.height());
        let top = (y - height * 0.5).clamp(rect.top(), rect.bottom() - height);
        let badge = Rect::from_min_size(
            Pos2::new(rect.left() + 2.0, top),
            egui::vec2((galley.size().x + 10.0).min(rect.width() - 4.0), height),
        );
        painter.rect_filled(
            badge,
            3,
            if latest {
                overlay.color
            } else {
                theme::BG_PRIMARY
            },
        );
        painter.rect_stroke(
            badge,
            3,
            Stroke::new(1.0, overlay.color),
            egui::StrokeKind::Inside,
        );
        painter.galley(
            badge.left_top() + egui::vec2(5.0, 2.0),
            galley,
            overlay.color,
        );
    }
}
