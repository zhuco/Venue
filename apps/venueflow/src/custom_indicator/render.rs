use super::{
    CustomSettings,
    text::{Key, SIGNAL_KEYS, text},
};
use crate::{
    chart::{ChartStudyPoint, bar_center_x},
    i18n::Language,
    model::{decimal_to_f64, format_decimal},
    theme,
};
use eframe::egui::{self, Color32, FontId, Pos2, Rect, Stroke};
use venue_control_protocol::UiBar;
use venue_indicators::chart::EmaAdxSignal;

#[allow(clippy::too_many_arguments)]
pub(crate) fn draw(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    settings: &CustomSettings,
    price_y: impl Fn(f64) -> f32,
    readout_time: u64,
    scale: usize,
    font_size: u8,
    language: Language,
) {
    if !settings.enabled {
        return;
    }
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    let colors = settings.colors.map(|c| Color32::from_rgb(c[0], c[1], c[2]));
    let mut previous = [None; 3];
    let mut occupied: Vec<Rect> = Vec::new();
    for (index, bar) in bars.iter().enumerate() {
        let Some(point) = at(studies, bar.open_time_ms) else {
            previous = [None; 3];
            continue;
        };
        let Some(values) = &point.custom_ema_adx else {
            previous = [None; 3];
            continue;
        };
        let Some(x) = bar_center_x(rect.left(), rect.width(), slots, index) else {
            continue;
        };
        for (line, prior) in previous.iter_mut().enumerate() {
            let Some(value) = values.ema[line] else {
                *prior = None;
                continue;
            };
            let point = Pos2::new(x, price_y(decimal_to_f64(value)));
            let color = if line == 2 {
                match values.trend {
                    1 => colors[0],
                    -1 => colors[1],
                    _ => colors[2],
                }
            } else {
                colors[line]
            };
            if let Some(start) = *prior {
                painter.line_segment(
                    [start, point],
                    Stroke::new(f32::from(settings.line_widths[line].clamp(1, 4)), color),
                );
            }
            *prior = Some(point);
        }
        if settings.confirmed_labels_only && !point.confirmed {
            continue;
        }
        for signal in &values.signals {
            let (i, below, color) = signal_style(*signal, colors);
            if !settings.signals[i] {
                continue;
            }
            let label = if point.confirmed {
                text(language, SIGNAL_KEYS[i]).to_owned()
            } else {
                format!(
                    "{} · {}",
                    text(language, SIGNAL_KEYS[i]),
                    text(language, Key::Preview)
                )
            };
            let galley = painter.layout_no_wrap(
                label,
                FontId::proportional(f32::from(font_size.max(11))),
                Color32::WHITE,
            );
            let size = galley.size() + egui::vec2(8.0, 4.0);
            let anchor_y = price_y(decimal_to_f64(if below { bar.low } else { bar.high }));
            let left =
                (x - size.x * 0.5).clamp(rect.left(), (rect.right() - size.x).max(rect.left()));
            let mut top = if below {
                anchor_y + 5.0
            } else {
                anchor_y - size.y - 5.0
            };
            let mut label_rect = Rect::from_min_size(Pos2::new(left, top), size);
            for _ in 0..6 {
                if occupied
                    .iter()
                    .all(|r| !r.intersects(label_rect.expand(1.0)))
                {
                    break;
                }
                top += if below { size.y + 2.0 } else { -size.y - 2.0 };
                label_rect = Rect::from_min_size(Pos2::new(left, top), size);
            }
            if !rect.contains_rect(label_rect) {
                continue;
            }
            painter.line_segment(
                [
                    Pos2::new(x, anchor_y),
                    Pos2::new(
                        x,
                        if below {
                            label_rect.top()
                        } else {
                            label_rect.bottom()
                        },
                    ),
                ],
                Stroke::new(0.7, color),
            );
            painter.rect_filled(
                label_rect,
                3.0,
                color.gamma_multiply(if point.confirmed { 0.92 } else { 0.55 }),
            );
            painter.galley(
                label_rect.min + egui::vec2(4.0, 2.0),
                galley,
                Color32::WHITE,
            );
            occupied.push(label_rect);
        }
    }
    let mut job = egui::text::LayoutJob::default();
    let format = egui::TextFormat {
        font_id: FontId::monospace(f32::from(font_size)),
        color: theme::TEXT_SECONDARY,
        ..Default::default()
    };
    job.append("EMA+ADX  ", 0.0, format.clone());
    if let Some(point) = at(studies, readout_time)
        && let Some(values) = &point.custom_ema_adx
    {
        for (i, value) in values.ema.iter().enumerate() {
            let value = value.map_or_else(|| "—".to_owned(), |v| format_decimal(v, scale));
            job.append(
                &format!("EMA{} {value}  ", settings.parameters.ema_periods[i]),
                0.0,
                egui::TextFormat {
                    color: colors[i],
                    ..format.clone()
                },
            );
        }
        for (name, value) in [
            ("ADX", values.adx),
            ("ATR", values.atr),
            ("MACD", values.histogram),
        ] {
            job.append(
                &format!(
                    "{name} {}  ",
                    value.map_or_else(|| "—".to_owned(), |v| format_decimal(v, scale.max(2)))
                ),
                0.0,
                format.clone(),
            );
        }
        let status = if values.histogram.is_none() || values.adx.is_none() {
            Key::Warmup
        } else if point.confirmed {
            Key::Confirmed
        } else {
            Key::Preview
        };
        job.append(
            &format!(
                "{} {} · {}",
                text(language, Key::VirtualPosition),
                values.virtual_position,
                text(language, status)
            ),
            0.0,
            format,
        );
    }
    painter.galley(
        rect.left_top() + egui::vec2(6.0, 44.0),
        painter.layout_job(job),
        theme::TEXT_PRIMARY,
    );
}

fn at(points: &[ChartStudyPoint], time: u64) -> Option<&ChartStudyPoint> {
    points
        .binary_search_by_key(&time, |p| p.open_time_ms)
        .ok()
        .and_then(|i| points.get(i))
}

fn signal_style(signal: EmaAdxSignal, colors: [Color32; 3]) -> (usize, bool, Color32) {
    use EmaAdxSignal::*;
    match signal {
        LongEntry => (0, true, colors[0]),
        ShortEntry => (1, false, colors[1]),
        LongExit => (2, false, Color32::from_rgb(255, 152, 0)),
        ShortExit => (3, true, Color32::from_rgb(255, 152, 0)),
        BullStart => (4, true, colors[0]),
        BearStart => (5, false, colors[1]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn labels_keep_source_order_and_high_low_anchors() {
        let colors = [Color32::GREEN, Color32::RED, Color32::GRAY];
        for (i, signal) in [
            EmaAdxSignal::LongEntry,
            EmaAdxSignal::ShortEntry,
            EmaAdxSignal::LongExit,
            EmaAdxSignal::ShortExit,
            EmaAdxSignal::BullStart,
            EmaAdxSignal::BearStart,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(signal_style(signal, colors).0, i);
            assert!(!text(Language::English, SIGNAL_KEYS[i]).is_empty());
            assert!(!text(Language::SimplifiedChinese, SIGNAL_KEYS[i]).is_empty());
        }
        assert!(signal_style(EmaAdxSignal::LongEntry, colors).1);
        assert!(!signal_style(EmaAdxSignal::LongExit, colors).1);
    }
}
