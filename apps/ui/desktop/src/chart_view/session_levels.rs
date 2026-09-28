use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Stroke};
use venue_control_protocol::UiBar;
use venue_domain::PublicBar;
use venue_indicators::chart::session_levels::{SessionPeriod, previous_levels, session_start};

use crate::{chart::PriceRange, chart_settings::SessionDisplay, model::decimal_to_f64};

#[allow(clippy::too_many_arguments)]
pub(super) fn draw(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    range: PriceRange,
    price_scale: usize,
    days: &[PublicBar],
    settings: &SessionDisplay,
) {
    if bars.is_empty() || days.is_empty() || slots == 0 {
        return;
    }
    for period in [SessionPeriod::Daily, SessionPeriod::Weekly] {
        let enabled = match period {
            SessionPeriod::Daily => {
                settings.pdh || settings.pdl || settings.daily_open || settings.daily_pivot
            }
            SessionPeriod::Weekly => {
                settings.pwh || settings.pwl || settings.weekly_open || settings.weekly_pivot
            }
        };
        if !enabled {
            continue;
        }
        let mut start = 0;
        while start < bars.len() {
            let Some(session) = session_start(bars[start].open_time_ms, period) else {
                break;
            };
            let mut end = start + 1;
            while end < bars.len() && session_start(bars[end].open_time_ms, period) == Some(session)
            {
                end += 1;
            }
            let mut lines: Vec<(&str, f64, Color32)> = Vec::new();
            if let Ok(Some(levels)) = previous_levels(days, session, period) {
                let high = decimal_to_f64(levels.previous.high);
                let low = decimal_to_f64(levels.previous.low);
                match period {
                    SessionPeriod::Daily => {
                        if settings.pdh {
                            lines.push(("PDH", high, Color32::from_rgb(92, 174, 240)));
                        }
                        if settings.pdl {
                            lines.push(("PDL", low, Color32::from_rgb(92, 174, 240)));
                        }
                    }
                    SessionPeriod::Weekly => {
                        if settings.pwh {
                            lines.push(("PWH", high, Color32::from_rgb(168, 136, 236)));
                        }
                        if settings.pwl {
                            lines.push(("PWL", low, Color32::from_rgb(168, 136, 236)));
                        }
                    }
                }
                let pivot = levels.pivot;
                if (period == SessionPeriod::Daily && settings.daily_pivot)
                    || (period == SessionPeriod::Weekly && settings.weekly_pivot)
                {
                    let prefix = if period == SessionPeriod::Daily {
                        "D"
                    } else {
                        "W"
                    };
                    let color = Color32::from_rgb(218, 184, 94);
                    lines.extend([
                        (if prefix == "D" { "DP" } else { "WP" }, pivot.pivot, color),
                        (
                            if prefix == "D" { "DR1" } else { "WR1" },
                            pivot.resistance_1,
                            color,
                        ),
                        (
                            if prefix == "D" { "DR2" } else { "WR2" },
                            pivot.resistance_2,
                            color,
                        ),
                        (
                            if prefix == "D" { "DS1" } else { "WS1" },
                            pivot.support_1,
                            color,
                        ),
                        (
                            if prefix == "D" { "DS2" } else { "WS2" },
                            pivot.support_2,
                            color,
                        ),
                    ]);
                    if settings.pivot_r3_s3 {
                        lines.push((
                            if prefix == "D" { "DR3" } else { "WR3" },
                            pivot.resistance_3,
                            color,
                        ));
                        lines.push((
                            if prefix == "D" { "DS3" } else { "WS3" },
                            pivot.support_3,
                            color,
                        ));
                    }
                }
            }
            if (period == SessionPeriod::Daily && settings.daily_open)
                || (period == SessionPeriod::Weekly && settings.weekly_open)
            {
                if let Ok(index) = days.binary_search_by_key(&session, |bar| bar.open_time_ms) {
                    let label = if period == SessionPeriod::Daily {
                        "DO"
                    } else {
                        "WO"
                    };
                    lines.push((
                        label,
                        decimal_to_f64(days[index].open.value()),
                        Color32::from_rgb(109, 200, 155),
                    ));
                }
            }
            let step = rect.width() / slots as f32;
            let left = rect.left() + start as f32 * step;
            let right = (rect.left() + end as f32 * step).min(rect.right());
            for (name, price, color) in lines {
                let Some(y) = range.price_to_y(rect.top(), rect.height(), price) else {
                    continue;
                };
                if y < rect.top() || y > rect.bottom() || !price.is_finite() {
                    continue;
                }
                painter.line_segment(
                    [Pos2::new(left, y), Pos2::new(right, y)],
                    Stroke::new(1.0, color),
                );
                if end == bars.len() {
                    painter.text(
                        Pos2::new(right - 3.0, y - 2.0),
                        Align2::RIGHT_BOTTOM,
                        format!("{name} {price:.price_scale$}"),
                        FontId::proportional(9.0),
                        color,
                    );
                }
            }
            start = end;
        }
    }
}
