use std::{ops::Range, sync::Arc};

use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Stroke};
use rust_decimal::Decimal;
use venue_control_protocol::UiBar;
use venue_domain::PublicBar;
use venue_gateway_api::PublicMarketBinding;
use venue_indicators::chart::{
    Atr,
    support_resistance::{Zone, ZoneBar, ZoneRole, ZoneState, calculate},
};

use crate::{
    chart::{ChartStudyPoint, PriceRange},
    model::decimal_to_f64,
    theme,
};

#[derive(Clone)]
struct Cache {
    scope: PublicMarketBinding,
    bar_revision: u64,
    visible_end: usize,
    interval_ms: u64,
    tick: Decimal,
    zones: Vec<Zone>,
}

#[derive(Clone)]
struct HigherCache {
    scope: PublicMarketBinding,
    source: Vec<ZoneBar>,
    interval_ms: u64,
    tick: Decimal,
    zones: Vec<Zone>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn draw(
    ui: &egui::Ui,
    painter: &egui::Painter,
    id: egui::Id,
    rect: Rect,
    all_bars: &[UiBar],
    studies: &[ChartStudyPoint],
    visible: Range<usize>,
    slots: usize,
    interval_ms: u64,
    range: PriceRange,
    tick: Option<Decimal>,
    scope: Option<&PublicMarketBinding>,
    bar_revision: Option<u64>,
) {
    let Some((scope, tick, revision)) = scope
        .zip(tick)
        .zip(bar_revision)
        .map(|((scope, tick), revision)| (scope, tick, revision))
    else {
        return;
    };
    if visible.is_empty() || slots == 0 || tick <= Decimal::ZERO {
        return;
    }
    let key = id.with("support-resistance-zones");
    let previous = ui.ctx().data(|data| data.get_temp::<Arc<Cache>>(key));
    let cache = previous
        .filter(|cached| {
            cached.scope == *scope
                && cached.bar_revision == revision
                && cached.visible_end == visible.end
                && cached.interval_ms == interval_ms
                && cached.tick == tick
        })
        .unwrap_or_else(|| {
            let input = all_bars
                .iter()
                .take(visible.end)
                .filter_map(|bar| {
                    let point = studies
                        .binary_search_by_key(&bar.open_time_ms, |point| point.open_time_ms)
                        .ok()
                        .and_then(|index| studies.get(index))?;
                    point.confirmed.then_some(ZoneBar {
                        open_time_ms: bar.open_time_ms,
                        high: bar.high,
                        low: bar.low,
                        close: bar.close,
                        atr: point.atr,
                    })
                })
                .collect::<Vec<_>>();
            let cache = Arc::new(Cache {
                scope: scope.clone(),
                bar_revision: revision,
                visible_end: visible.end,
                interval_ms,
                tick,
                zones: calculate(&input, interval_ms, tick).unwrap_or_default(),
            });
            ui.ctx()
                .data_mut(|data| data.insert_temp(key, cache.clone()));
            cache
        });
    render(
        ui,
        painter,
        id,
        rect,
        &all_bars[visible],
        slots,
        interval_ms,
        interval_ms,
        range,
        &cache.zones,
        3,
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn draw_higher(
    ui: &egui::Ui,
    painter: &egui::Painter,
    id: egui::Id,
    rect: Rect,
    visible_bars: &[UiBar],
    slots: usize,
    display_interval_ms: u64,
    range: PriceRange,
    tick: Option<Decimal>,
    scope: Option<&PublicMarketBinding>,
    minute_facts: Option<&[PublicBar]>,
    day_facts: Option<&[PublicBar]>,
    enabled: [bool; 3],
) {
    let Some((scope, tick)) = scope.zip(tick.filter(|tick| *tick > Decimal::ZERO)) else {
        return;
    };
    let Some(visible_end) = visible_bars
        .last()
        .map(|bar| bar.open_time_ms.saturating_add(display_interval_ms))
    else {
        return;
    };
    let mut sides = [0usize; 2];
    for (source_interval, on) in [
        (900_000, enabled[0]),
        (3_600_000, enabled[1]),
        (86_400_000, enabled[2]),
    ] {
        if !on || source_interval <= display_interval_ms {
            continue;
        }
        let facts = if source_interval == 86_400_000 {
            day_facts
        } else {
            minute_facts
        };
        let Some(facts) = facts else {
            continue;
        };
        let source = higher_zone_bars(facts, source_interval, visible_end);
        let key = id.with(("higher-support-resistance", source_interval));
        let previous = ui.ctx().data(|data| data.get_temp::<Arc<HigherCache>>(key));
        let cache = previous
            .filter(|cached| {
                cached.scope == *scope
                    && cached.source == source
                    && cached.interval_ms == source_interval
                    && cached.tick == tick
            })
            .unwrap_or_else(|| {
                let zones = calculate(&source, source_interval, tick).unwrap_or_default();
                let cache = Arc::new(HigherCache {
                    scope: scope.clone(),
                    source,
                    interval_ms: source_interval,
                    tick,
                    zones,
                });
                ui.ctx()
                    .data_mut(|data| data.insert_temp(key, cache.clone()));
                cache
            });
        render_limited(
            ui,
            painter,
            key,
            rect,
            visible_bars,
            slots,
            display_interval_ms,
            source_interval,
            range,
            &cache.zones,
            &mut sides,
            2,
        );
    }
}

fn higher_zone_bars(facts: &[PublicBar], interval_ms: u64, visible_end: u64) -> Vec<ZoneBar> {
    let mut grouped = Vec::<PublicBar>::new();
    if interval_ms == 86_400_000 {
        grouped.extend(
            facts
                .iter()
                .filter(|bar| {
                    bar.interval_ms == interval_ms
                        && bar.open_time_ms.saturating_add(interval_ms) <= visible_end
                })
                .cloned(),
        );
    } else {
        let per_bucket = (interval_ms / 60_000) as usize;
        let mut index = 0;
        while index + per_bucket <= facts.len() {
            let first = &facts[index];
            if first.interval_ms != 60_000 || first.open_time_ms % interval_ms != 0 {
                index += 1;
                continue;
            }
            let group = &facts[index..index + per_bucket];
            if first.open_time_ms.saturating_add(interval_ms) > visible_end {
                break;
            }
            if !group.iter().enumerate().all(|(offset, bar)| {
                bar.interval_ms == 60_000
                    && bar.open_time_ms == first.open_time_ms + offset as u64 * 60_000
            }) {
                index += 1;
                continue;
            }
            let mut aggregate = first.clone();
            aggregate.interval_ms = interval_ms;
            aggregate.close_time_ms = first
                .open_time_ms
                .saturating_add(interval_ms)
                .saturating_sub(1);
            aggregate.high = group.iter().map(|bar| bar.high).max().unwrap_or(first.high);
            aggregate.low = group.iter().map(|bar| bar.low).min().unwrap_or(first.low);
            aggregate.close = group[per_bucket - 1].close;
            grouped.push(aggregate);
            index += per_bucket;
        }
    }
    let start = grouped
        .windows(2)
        .rposition(|pair| pair[0].open_time_ms.saturating_add(interval_ms) != pair[1].open_time_ms)
        .map_or(0, |index| index + 1);
    let mut atr = Atr::new(14).ok();
    grouped[start..]
        .iter()
        .filter_map(|bar| {
            let value = atr.as_mut()?.update(bar).ok()?;
            Some(ZoneBar {
                open_time_ms: bar.open_time_ms,
                high: bar.high.value(),
                low: bar.low.value(),
                close: bar.close.value(),
                atr: value,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn render(
    ui: &egui::Ui,
    painter: &egui::Painter,
    id: egui::Id,
    rect: Rect,
    visible_bars: &[UiBar],
    slots: usize,
    display_interval_ms: u64,
    source_interval_ms: u64,
    range: PriceRange,
    zones: &[Zone],
    side_limit: usize,
) {
    let mut sides = [0usize; 2];
    render_limited(
        ui,
        painter,
        id,
        rect,
        visible_bars,
        slots,
        display_interval_ms,
        source_interval_ms,
        range,
        zones,
        &mut sides,
        side_limit,
    );
}

#[allow(clippy::too_many_arguments)]
fn render_limited(
    ui: &egui::Ui,
    painter: &egui::Painter,
    id: egui::Id,
    rect: Rect,
    visible_bars: &[UiBar],
    slots: usize,
    display_interval_ms: u64,
    source_interval_ms: u64,
    range: PriceRange,
    zones: &[Zone],
    sides: &mut [usize; 2],
    side_limit: usize,
) {
    let latest_price = visible_bars.last().map_or(Decimal::ZERO, |bar| bar.close);
    let visible_end_ms = visible_bars.last().map_or(0, |bar| {
        bar.open_time_ms.saturating_add(display_interval_ms)
    });
    let mut active = zones
        .iter()
        .filter(|zone| zone.state == ZoneState::Active && zone.confirmed_at_ms <= visible_end_ms)
        .collect::<Vec<_>>();
    active.sort_by(|left, right| {
        (left.center - latest_price)
            .abs()
            .cmp(&(right.center - latest_price).abs())
            .then_with(|| right.score.total_cmp(&left.score))
            .then_with(|| left.confirmed_at_ms.cmp(&right.confirmed_at_ms))
    });
    for zone in active {
        let accepted = match zone.role {
            ZoneRole::Support if zone.center <= latest_price && sides[0] < side_limit => {
                sides[0] += 1;
                true
            }
            ZoneRole::Resistance if zone.center >= latest_price && sides[1] < side_limit => {
                sides[1] += 1;
                true
            }
            _ => false,
        };
        if !accepted {
            continue;
        }
        let Some(top) = range.price_to_y(rect.top(), rect.height(), decimal_to_f64(zone.high))
        else {
            continue;
        };
        let Some(bottom) = range.price_to_y(rect.top(), rect.height(), decimal_to_f64(zone.low))
        else {
            continue;
        };
        if bottom < rect.top() || top > rect.bottom() {
            continue;
        }
        let first = visible_bars.partition_point(|bar| {
            bar.open_time_ms.saturating_add(display_interval_ms) <= zone.confirmed_at_ms
        });
        let left = rect.left() + first as f32 * rect.width() / slots as f32;
        let right = rect.left() + visible_bars.len() as f32 * rect.width() / slots as f32;
        let zone_rect = Rect::from_min_max(
            Pos2::new(left, top.max(rect.top())),
            Pos2::new(
                right.min(rect.right()),
                bottom.min(rect.bottom()).max(top + 1.0),
            ),
        );
        let color = match zone.role {
            ZoneRole::Support => Color32::from_rgba_unmultiplied(37, 181, 125, 38),
            ZoneRole::Resistance => Color32::from_rgba_unmultiplied(234, 91, 111, 38),
        };
        painter.rect_filled(zone_rect, 0.0, color);
        let border = if zone.role == ZoneRole::Support {
            theme::BUY
        } else {
            theme::SELL
        };
        painter.line_segment(
            [zone_rect.left_top(), zone_rect.right_top()],
            Stroke::new(1.0, border),
        );
        let label = if zone.role == ZoneRole::Support {
            "S"
        } else {
            "R"
        };
        painter.text(
            zone_rect.right_top() - egui::vec2(3.0, 1.0),
            Align2::RIGHT_TOP,
            format!("{label} {}m {:.0}", source_interval_ms / 60_000, zone.score),
            FontId::proportional(9.0),
            border,
        );
        ui.interact(zone_rect, id.with(("zone", zone.id)), Sense::hover())
            .on_hover_text(format!(
                "{label} · score {:.1} (rank only)\nRange {}–{}\nConfirmed {} ms UTC\nTouches {}",
                zone.score, zone.low, zone.high, zone.confirmed_at_ms, zone.touch_count
            ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::{FieldState, Price, Symbol};

    fn minute(index: u64) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let price = Price::new(Decimal::from(100 + index % 3))?;
        Ok(PublicBar {
            symbol: "DOGE/USDC".parse::<Symbol>()?,
            generation: 1,
            received_at_ms: index * 60_000 + 60_000,
            sequence: index + 1,
            open_time_ms: index * 60_000,
            close_time_ms: (index + 1) * 60_000 - 1,
            interval_ms: 60_000,
            open: price,
            high: price,
            low: price,
            close: price,
            base_volume: FieldState::Known(Decimal::ONE),
            quote_volume: FieldState::Known(price.value()),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        })
    }

    #[test]
    fn higher_source_uses_only_complete_contiguous_buckets_and_no_future_day()
    -> Result<(), Box<dyn std::error::Error>> {
        let minutes = (0..61).map(minute).collect::<Result<Vec<_>, _>>()?;
        let quarters = higher_zone_bars(&minutes, 900_000, 3_600_000);
        assert_eq!(quarters.len(), 4);
        assert_eq!(quarters[0].open_time_ms, 0);
        assert_eq!(quarters[3].open_time_ms, 2_700_000);
        assert_eq!(higher_zone_bars(&minutes, 3_600_000, 3_600_000).len(), 1);
        let mut gap = minutes.clone();
        gap.remove(17);
        assert_eq!(higher_zone_bars(&gap, 900_000, 3_600_000).len(), 2);
        let day = PublicBar {
            interval_ms: 86_400_000,
            close_time_ms: 86_399_999,
            ..minutes[0].clone()
        };
        assert!(higher_zone_bars(&[day.clone()], 86_400_000, 86_399_999).is_empty());
        assert_eq!(higher_zone_bars(&[day], 86_400_000, 86_400_000).len(), 1);
        Ok(())
    }
}
