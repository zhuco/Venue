use std::sync::Arc;

use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Stroke};
use rust_decimal::Decimal;
use venue_control_protocol::UiBar;
use venue_domain::PublicBar;
use venue_gateway_api::PublicMarketBinding;
use venue_indicators::chart::volume_profile::{VolumeProfile, estimate};

use crate::{chart::PriceRange, chart_settings::ProfileDisplay, model::decimal_to_f64, theme};

#[derive(Clone)]
struct Cache {
    scope: Option<PublicMarketBinding>,
    source_revision: (u64, u64),
    start: u64,
    end: u64,
    step: Decimal,
    profile: Option<VolumeProfile>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn draw(
    ui: &egui::Ui,
    painter: &egui::Painter,
    id: egui::Id,
    rect: Rect,
    visible_bars: &[UiBar],
    interval_ms: u64,
    range: PriceRange,
    tick: Option<Decimal>,
    minute_facts: Option<&[PublicBar]>,
    source_revision: Option<(u64, u64)>,
    scope: Option<&PublicMarketBinding>,
    settings: &ProfileDisplay,
) {
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    painter.rect_filled(rect, 0.0, theme::BG_PRIMARY);
    painter.line_segment(
        [rect.left_top(), rect.left_bottom()],
        Stroke::new(1.0, theme::DIVIDER),
    );
    let Some(step) = tick
        .filter(|tick| *tick > Decimal::ZERO)
        .and_then(|tick| tick.checked_mul(Decimal::from(settings.tick_multiple)))
    else {
        status(&painter, rect, "Profile: tick unavailable");
        return;
    };
    let Some(facts) = minute_facts else {
        status(&painter, rect, "Profile: waiting for same-market 1m data");
        return;
    };
    let visible = visible_bars
        .first()
        .zip(visible_bars.last())
        .map(|(first, last)| {
            (
                first.open_time_ms,
                last.open_time_ms.saturating_add(interval_ms),
            )
        });
    let modes = [
        settings.visible_range.then(|| ("VR · estimate", visible)),
        settings.fixed_range.then(|| {
            (
                "FR · estimate",
                Some((settings.fixed_start_ms, settings.fixed_end_ms)),
            )
        }),
    ];
    let count = modes.iter().filter(|mode| mode.is_some()).count();
    if count == 0 {
        return;
    }
    ui.ctx().data_mut(|data| {
        for (mode_index, mode) in modes.iter().enumerate() {
            if mode.is_none() {
                data.remove::<Arc<Cache>>(id.with(("volume-profile", mode_index)));
            }
        }
    });
    super::mark_indicator_cache(ui.ctx(), super::IndicatorCacheKind::Profile, id);
    let mut column = 0;
    for (mode_index, mode) in modes.into_iter().enumerate() {
        let Some((label, times)) = mode else { continue };
        let left = rect.left() + rect.width() * column as f32 / count as f32;
        let right = rect.left() + rect.width() * (column + 1) as f32 / count as f32;
        let column_rect =
            Rect::from_min_max(Pos2::new(left, rect.top()), Pos2::new(right, rect.bottom()));
        column += 1;
        let Some((start, end)) = times.filter(|(start, end)| start < end) else {
            status(&painter, column_rect, "Select fixed range");
            continue;
        };
        let key = id.with(("volume-profile", mode_index));
        let previous = ui.ctx().data(|data| data.get_temp::<Arc<Cache>>(key));
        let cache = previous
            .filter(|cached| {
                cached.scope.as_ref() == scope
                    && Some(cached.source_revision) == source_revision
                    && cached.start == start
                    && cached.end == end
                    && cached.step == step
            })
            .unwrap_or_else(|| {
                #[cfg(not(target_arch = "wasm32"))]
                let started = std::time::Instant::now();
                let first = facts.partition_point(|bar| bar.close_time_ms < start);
                let last = facts.partition_point(|bar| bar.open_time_ms < end);
                let cache = Arc::new(Cache {
                    scope: scope.cloned(),
                    source_revision: source_revision.unwrap_or_default(),
                    start,
                    end,
                    step,
                    profile: estimate(&facts[first..last], start, end, step, 70).ok(),
                });
                #[cfg(not(target_arch = "wasm32"))]
                tracing::info!(target: "venueflow::indicator_performance", study = "volume_profile",
                    phase = "cache_miss", source_bars = last.saturating_sub(first),
                    buckets = cache.profile.as_ref().map_or(0, |profile| profile.buckets.len()),
                    elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
                    "indicator computation completed");
                ui.ctx()
                    .data_mut(|data| data.insert_temp(key, cache.clone()));
                cache
            });
        let Some(profile) = &cache.profile else {
            status(&painter, column_rect, "Profile: range/step unavailable");
            continue;
        };
        let maximum = profile
            .buckets
            .iter()
            .map(|bucket| decimal_to_f64(bucket.base_volume))
            .fold(0.0_f64, f64::max);
        let alpha = u16::from(settings.opacity_percent) * 255 / 100;
        if maximum > 0.0 {
            for bucket in &profile.buckets {
                let low = decimal_to_f64(bucket.price_low);
                let high = decimal_to_f64(bucket.price_high);
                let Some(y_top) = range.price_to_y(column_rect.top(), column_rect.height(), high)
                else {
                    continue;
                };
                let Some(y_bottom) = range.price_to_y(column_rect.top(), column_rect.height(), low)
                else {
                    continue;
                };
                if y_bottom < column_rect.top() || y_top > column_rect.bottom() {
                    continue;
                }
                let fraction =
                    (decimal_to_f64(bucket.base_volume) / maximum).clamp(0.0, 1.0) as f32;
                let bar_width = (column_rect.width() - 6.0).max(0.0) * fraction;
                painter.rect_filled(
                    Rect::from_min_max(
                        Pos2::new(
                            column_rect.right() - bar_width - 3.0,
                            y_top.max(column_rect.top()),
                        ),
                        Pos2::new(
                            column_rect.right() - 3.0,
                            y_bottom.min(column_rect.bottom()).max(y_top + 1.0),
                        ),
                    ),
                    0.0,
                    Color32::from_rgba_unmultiplied(95, 154, 232, alpha as u8),
                );
            }
        }
        for (enabled, name, value, color) in [
            (
                settings.poc,
                "POC",
                profile.poc,
                Color32::from_rgb(241, 187, 85),
            ),
            (
                settings.vah,
                "VAH",
                profile.vah,
                Color32::from_rgb(115, 211, 160),
            ),
            (
                settings.val,
                "VAL",
                profile.val,
                Color32::from_rgb(115, 211, 160),
            ),
        ] {
            if !enabled {
                continue;
            }
            if let Some(y) = value.and_then(|price| {
                range.price_to_y(
                    column_rect.top(),
                    column_rect.height(),
                    decimal_to_f64(price),
                )
            }) {
                if column_rect.top() <= y && y <= column_rect.bottom() {
                    painter.line_segment(
                        [
                            Pos2::new(column_rect.left(), y),
                            Pos2::new(column_rect.right(), y),
                        ],
                        Stroke::new(1.0, color),
                    );
                    painter.text(
                        Pos2::new(column_rect.right() - 3.0, y - 2.0),
                        Align2::RIGHT_BOTTOM,
                        name,
                        FontId::proportional(9.0),
                        color,
                    );
                }
            }
        }
        let title = if profile.complete {
            label.to_owned()
        } else {
            format!("{label} · partial")
        };
        painter.text(
            column_rect.left_top() + egui::vec2(4.0, 3.0),
            Align2::LEFT_TOP,
            title,
            FontId::proportional(10.0),
            theme::TEXT_SECONDARY,
        );
    }
}

pub(super) fn evict(context: &egui::Context, id: egui::Id) {
    context.data_mut(|data| {
        for mode_index in 0..2_usize {
            data.remove::<Arc<Cache>>(id.with(("volume-profile", mode_index)));
        }
    });
}

pub(super) fn retained_bytes(context: &egui::Context, id: egui::Id) -> usize {
    (0..2_usize).fold(0_usize, |total, mode_index| {
        let cache = context
            .data(|data| data.get_temp::<Arc<Cache>>(id.with(("volume-profile", mode_index))));
        let bytes = cache.map_or(0, |cache| {
            std::mem::size_of::<Cache>()
                .saturating_add(cache.profile.as_ref().map_or(0, |profile| {
                profile.buckets.capacity()
                    * std::mem::size_of::<venue_indicators::chart::volume_profile::VolumeBucket>()
            }))
        });
        total.saturating_add(bytes)
    })
}

fn status(painter: &egui::Painter, rect: Rect, message: &str) {
    painter.text(
        rect.left_top() + egui::vec2(4.0, 3.0),
        Align2::LEFT_TOP,
        message,
        FontId::proportional(10.0),
        theme::TEXT_SECONDARY,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inactive_chart_releases_both_profile_ranges() {
        let context = egui::Context::default();
        let id = egui::Id::new("profile-chart-that-closes");
        let cache = Arc::new(Cache {
            scope: None,
            source_revision: (1, 1),
            start: 0,
            end: 60_000,
            step: Decimal::ONE,
            profile: None,
        });
        let weak = Arc::downgrade(&cache);
        let mut output = context.run_ui(Default::default(), |ui| {
            ui.ctx().data_mut(|data| {
                for mode_index in 0..2_usize {
                    data.insert_temp(id.with(("volume-profile", mode_index)), cache.clone());
                }
            });
            super::super::mark_indicator_cache(
                ui.ctx(),
                super::super::IndicatorCacheKind::Profile,
                id,
            );
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
