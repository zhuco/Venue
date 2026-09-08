use crate::{
    chart::{PriceRange, bar_center_x},
    model::{decimal_to_f64, format_decimal},
    theme,
};
use eframe::egui::{self, Align2, FontId, Pos2, Rect, Stroke};
use venue_control_protocol::UiBar;

fn extrema(bars: &[UiBar]) -> Option<(usize, usize)> {
    let first = bars.first()?;
    let (mut high, mut low) = (0, 0);
    let (mut highest, mut lowest) = (first.high, first.low);
    // Ties use the first visible occurrence; no off-screen candle enters this calculation.
    for (index, bar) in bars.iter().enumerate().skip(1) {
        if bar.high > highest {
            high = index;
            highest = bar.high;
        }
        if bar.low < lowest {
            low = index;
            lowest = bar.low;
        }
    }
    Some((high, low))
}

pub(super) fn draw(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    range: PriceRange,
    scale: usize,
) {
    let Some((high, low)) = extrema(bars) else {
        return;
    };
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    for (index, is_high) in [(high, true), (low, false)] {
        let value = if is_high {
            bars[index].high
        } else {
            bars[index].low
        };
        let Some(x) = bar_center_x(rect.left(), rect.width(), slots, index) else {
            continue;
        };
        let Some(y) = range.price_to_y(rect.top(), rect.height(), decimal_to_f64(value)) else {
            continue;
        };
        if !rect.contains(Pos2::new(x, y)) {
            continue;
        }
        let text = format_decimal(value, scale);
        let font = FontId::proportional(11.0);
        let galley = painter.layout_no_wrap(text.clone(), font.clone(), theme::TEXT_PRIMARY);
        let right = x + 18.0 + galley.size().x < rect.right();
        let end_x = (x + if right { 14.0 } else { -14.0 }).clamp(rect.left(), rect.right());
        let label_y =
            (y + if is_high { -12.0 } else { 12.0 }).clamp(rect.top() + 8.0, rect.bottom() - 8.0);
        painter.line_segment(
            [Pos2::new(x, y), Pos2::new(x, label_y)],
            Stroke::new(1.0, theme::TEXT_SECONDARY),
        );
        painter.line_segment(
            [Pos2::new(x, label_y), Pos2::new(end_x, label_y)],
            Stroke::new(1.0, theme::TEXT_SECONDARY),
        );
        painter.text(
            Pos2::new(end_x, label_y),
            if right {
                Align2::LEFT_CENTER
            } else {
                Align2::RIGHT_CENTER
            },
            text,
            font,
            theme::TEXT_PRIMARY,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn extrema_follow_visible_slice_and_ties_choose_first() {
        let bar = |high: i64, low: i64| UiBar {
            open_time_ms: 1,
            open: Decimal::from(low),
            high: Decimal::from(high),
            low: Decimal::from(low),
            close: Decimal::from(high),
            volume: Decimal::ONE,
        };
        let bars = [bar(100, 1), bar(20, 10), bar(30, 5), bar(30, 5)];
        assert_eq!(extrema(&bars), Some((0, 0)));
        assert_eq!(extrema(&bars[1..]), Some((1, 1)));
        assert_eq!(extrema(&[]), None);
    }
}
