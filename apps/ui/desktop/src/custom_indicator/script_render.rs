use crate::{
    chart::{ChartStudyPoint, bar_center_x},
    model::decimal_to_f64,
};
use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Stroke};
use venue_control_protocol::UiBar;

fn color(s: &str) -> Color32 {
    s.strip_prefix('#')
        .filter(|v| v.len() == 6)
        .and_then(|v| u32::from_str_radix(v, 16).ok())
        .map(|v| Color32::from_rgb((v >> 16) as u8, (v >> 8) as u8, v as u8))
        .unwrap_or(Color32::LIGHT_BLUE)
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn draw_scripts(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    studies: &[ChartStudyPoint],
    price_y: impl Fn(f64) -> f32,
    fills_only: bool,
    enabled: &[u64],
) {
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    let mut previous = std::collections::HashMap::<
        u64,
        (f32, &venue_indicators::chart::script::ScriptFrame),
    >::new();
    for (index, bar) in bars.iter().enumerate() {
        let Some(x) = bar_center_x(rect.left(), rect.width(), slots, index) else {
            continue;
        };
        let Some(point) = studies
            .binary_search_by_key(&bar.open_time_ms, |p| p.open_time_ms)
            .ok()
            .and_then(|i| studies.get(i))
        else {
            previous.clear();
            continue;
        };
        for frame in &point.custom_scripts {
            if !enabled.contains(&frame.id) {
                continue;
            }
            let before = previous.insert(frame.id, (x, frame));
            if fills_only {
                if let Some((px, prior)) = before {
                    for fill in &frame.fills {
                        let values = (|| {
                            Some([
                                prior.lines.get(fill.first)?.value?,
                                frame.lines.get(fill.first)?.value?,
                                frame.lines.get(fill.second)?.value?,
                                prior.lines.get(fill.second)?.value?,
                            ])
                        })();
                        if let Some(v) = values {
                            let c = color(&fill.color);
                            let c = Color32::from_rgba_unmultiplied(c.r(), c.g(), c.b(), 35);
                            // Two triangles remain valid even when edited lines cross.
                            let p = [
                                Pos2::new(px, price_y(decimal_to_f64(v[0]))),
                                Pos2::new(x, price_y(decimal_to_f64(v[1]))),
                                Pos2::new(x, price_y(decimal_to_f64(v[2]))),
                                Pos2::new(px, price_y(decimal_to_f64(v[3]))),
                            ];
                            if p.iter().all(|p| p.is_finite()) {
                                for tri in [[p[0], p[1], p[2]], [p[0], p[2], p[3]]] {
                                    painter.add(egui::Shape::convex_polygon(
                                        tri.to_vec(),
                                        c,
                                        Stroke::NONE,
                                    ));
                                }
                            }
                        }
                    }
                }
                continue;
            }
            if let Some((px, prior)) = before {
                for (line, old) in frame.lines.iter().zip(&prior.lines) {
                    if let (Some(a), Some(b)) = (old.value, line.value) {
                        let p = [
                            Pos2::new(px, price_y(decimal_to_f64(a))),
                            Pos2::new(x, price_y(decimal_to_f64(b))),
                        ];
                        if p.iter().all(|p| p.is_finite()) {
                            painter.line_segment(
                                p,
                                Stroke::new(f32::from(line.width), color(&line.color)),
                            );
                        }
                    }
                }
            }
            if point.confirmed {
                let (mut above, mut below) = (0.0, 0.0);
                for label in &frame.labels {
                    let offset = if label.below { &mut below } else { &mut above };
                    let y = price_y(decimal_to_f64(label.value))
                        + if label.below {
                            8.0 + *offset
                        } else {
                            -8.0 - *offset
                        };
                    *offset += 18.0;
                    if !y.is_finite() {
                        continue;
                    }
                    let anchor = if label.below {
                        Align2::CENTER_TOP
                    } else {
                        Align2::CENTER_BOTTOM
                    };
                    let galley = painter.layout_no_wrap(
                        label.text.clone(),
                        FontId::proportional(f32::from(label.font_size)),
                        color(&label.color),
                    );
                    let r = anchor.anchor_rect(Rect::from_min_size(
                        Pos2::new(x, y),
                        galley.size() + egui::vec2(6.0, 2.0),
                    ));
                    painter.rect_filled(r, 2.0, color(&label.background));
                    painter.galley(r.min + egui::vec2(3.0, 1.0), galley, color(&label.color));
                }
            }
        }
    }
}
