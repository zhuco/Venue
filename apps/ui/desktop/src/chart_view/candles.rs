use eframe::egui::{self, Color32, Pos2, Rect};

struct Geometry {
    body: Rect,
    upper: Rect,
    lower: Rect,
}

fn geometry(x: f32, slot: f32, ohlc_y: [f32; 4], pixels_per_point: f32) -> Geometry {
    let [open, high, low, close] = ohlc_y;
    let pixel = pixels_per_point.recip();
    let body_width = slot * 0.62;
    let wick_width = pixel.min(body_width * 0.3);
    let top = open.min(close);
    let bottom = open.max(close);
    // Subpixel bodies stay centred and inside the real high/low, never grow only downward.
    let height = (bottom - top).max(pixel).min((low - high).max(pixel));
    let center = (top + bottom) * 0.5;
    let body_top = if low - high >= height {
        (center - height * 0.5).clamp(high, low - height)
    } else {
        center - height * 0.5
    };
    let body = Rect::from_min_max(
        Pos2::new(x - body_width * 0.5, body_top),
        Pos2::new(x + body_width * 0.5, body_top + height),
    );
    Geometry {
        body,
        upper: Rect::from_min_max(
            Pos2::new(x - wick_width * 0.5, high),
            Pos2::new(x + wick_width * 0.5, body.top().max(high)),
        ),
        lower: Rect::from_min_max(
            Pos2::new(x - wick_width * 0.5, body.bottom().min(low)),
            Pos2::new(x + wick_width * 0.5, low),
        ),
    }
}

pub(super) fn draw(painter: &egui::Painter, x: f32, slot: f32, ohlc_y: [f32; 4], color: Color32) {
    let geometry = geometry(x, slot, ohlc_y, painter.ctx().pixels_per_point());
    for rect in [geometry.upper, geometry.lower, geometry.body] {
        if rect.is_positive() {
            // Independent endpoint rounding and round line caps can manufacture visible wicks.
            painter
                .add(egui::epaint::RectShape::filled(rect, 0.0, color).with_round_to_pixels(false));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_full_bodies_do_not_gain_wicks_at_any_dpi() {
        for dpi in [1.0, 1.25, 2.0, 3.0] {
            for height in [0.2, 0.8, 2.0, 20.0] {
                for (open, close) in [(10.0, 10.0 + height), (10.0 + height, 10.0)] {
                    let g = geometry(20.3, 2.0, [open, 10.0, 10.0 + height, close], dpi);
                    assert_eq!(g.upper.height(), 0.0);
                    assert_eq!(g.lower.height(), 0.0);
                    assert!((g.body.center().y - (10.0 + height * 0.5)).abs() < 0.0001);
                    assert!(g.upper.width() < g.body.width());
                }
            }
        }
    }

    #[test]
    fn resolved_prices_preserve_body_and_wick_ratios_while_zooming() {
        for scale in [0.25, 0.5, 1.0, 4.0] {
            let g = geometry(
                20.0,
                8.0 * scale,
                [12.0 * scale, 10.0 * scale, 30.0 * scale, 24.0 * scale],
                2.0,
            );
            assert!((g.body.height() / (20.0 * scale) - 0.6).abs() < 0.0001);
            assert!((g.upper.height() / (20.0 * scale) - 0.1).abs() < 0.0001);
            assert!((g.lower.height() / (20.0 * scale) - 0.3).abs() < 0.0001);
        }
    }
}
