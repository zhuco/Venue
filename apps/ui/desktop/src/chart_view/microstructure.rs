mod heatmap;

use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect};
use serde::{Deserialize, Serialize};
use venue_control_protocol::UiBookLevel;

use crate::{i18n::Language, model::decimal_to_f64, theme};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub show_delta: bool,
    pub show_cvd: bool,
    pub cvd_reset_mode: venue_indicators::chart::CvdResetMode,
    pub order_flow: bool,
    pub cumulative: bool,
    pub depth: bool,
    pub heatmap: bool,
    // Retained only to read older preferences; the model always uses the fixed scenario basket.
    pub leverage: [bool; 4],
    pub maintenance_bps: u16,
    pub margin_cost_bps: u16,
    pub lookback_hours: u16,
    pub half_life_hours: u16,
    pub opacity: u8,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            show_delta: false,
            show_cvd: false,
            cvd_reset_mode: venue_indicators::chart::CvdResetMode::UtcDaily,
            order_flow: false,
            cumulative: false,
            depth: false,
            heatmap: true,
            leverage: [true; 4],
            maintenance_bps: 50,
            margin_cost_bps: 0,
            lookback_hours: 4,
            half_life_hours: 2,
            opacity: 40,
        }
    }
}

impl Settings {
    pub fn validate(&self) -> Result<(), &'static str> {
        if ![1, 4, 24, 72].contains(&self.lookback_hours)
            || !(1..=72).contains(&self.half_life_hours)
            || !(1..=90).contains(&self.maintenance_bps)
            || self.margin_cost_bps > 90
            || u32::from(self.maintenance_bps + self.margin_cost_bps) * 100 >= 10_000
            || !(10..=70).contains(&self.opacity)
        {
            return Err("检查时间窗口与半衰期；维持保证金与费用消耗合计须小于100bp");
        }
        Ok(())
    }
}

pub(super) fn label(language: Language, zh: &'static str, en: &'static str) -> &'static str {
    if language == Language::SimplifiedChinese {
        zh
    } else {
        en
    }
}

pub(crate) fn settings_ui(ui: &mut egui::Ui, settings: &mut Settings, language: Language) {
    egui::ScrollArea::vertical().max_height((ui.available_height() - 65.0).max(120.0)).show(ui, |ui| {
        ui.spacing_mut().item_spacing = egui::vec2(12.0, 10.0);
        ui.checkbox(&mut settings.show_delta, "Delta · base");
        ui.checkbox(&mut settings.show_cvd, "CVD · base");
        ui.add_enabled_ui(settings.show_cvd, |ui| {
            ui.horizontal(|ui| {
                ui.label(label(language, "累计起点", "Cumulative start"));
                ui.selectable_value(&mut settings.cvd_reset_mode, venue_indicators::chart::CvdResetMode::UtcDaily, "UTC Daily");
                ui.selectable_value(&mut settings.cvd_reset_mode, venue_indicators::chart::CvdResetMode::LoadedContinuous, "Loaded Continuous");
            });
        });
        ui.small(label(language, "主动买量 − 主动卖量，单位为基础币。数据缺口后重新累计；缺少主动买卖量时留空。", "Taker buy minus sell, in base units. Accumulation restarts after a gap; unavailable aggregates stay blank."));
        ui.separator();
        ui.checkbox(&mut settings.depth, label(language, "订单簿深度（主图右侧横向柱）", "Order book depth (horizontal bars)"));
        ui.small(label(language, "绿买红卖，柱长表示该价位附近挂单名义量；仅当前已订阅档位，过期盘口不绘制。", "Green bids, red asks; width is resting notional near that price. Subscribed levels only; stale depth is hidden."));
        ui.separator();
        ui.checkbox(&mut settings.heatmap, label(language, "预测清算热力图 · 本地情景估算", "Predicted liquidation heatmap · local scenarios"));
        ui.add_enabled_ui(settings.heatmap, |ui| {
        egui::Grid::new("heatmap-settings-fields").num_columns(3).spacing([16.0, 10.0]).show(ui, |ui| {
            ui.label(label(language, "维持保证金率", "Maintenance margin"));
            ui.add(egui::Slider::new(&mut settings.maintenance_bps, 1..=90));
            ui.label("bp"); ui.end_row();
            ui.label(label(language, "保证金净消耗", "Margin depletion"));
            ui.add(egui::Slider::new(&mut settings.margin_cost_bps, 0..=90));
            ui.label("bp"); ui.end_row();
            ui.label(label(language, "回看窗口", "Lookback"));
            ui.horizontal(|ui| {
                for hours in [1,4,24,72] { ui.selectable_value(&mut settings.lookback_hours,hours,format!("{hours}h")); }
            });
            ui.label(""); ui.end_row();
            ui.label(label(language, "强度半衰期", "Intensity half-life"));
            ui.add(egui::Slider::new(&mut settings.half_life_hours, 1..=72));
            ui.label("h"); ui.end_row();
            ui.label(label(language, "热图不透明度", "Opacity"));
            ui.add(egui::Slider::new(&mut settings.opacity, 10..=70));
            ui.label("%"); ui.end_row();
        });
        });
        ui.colored_label(theme::WARNING, label(language, "亮度仅表示相对估算强度，不是爆仓金额或概率。", "Brightness is relative estimated intensity, not liquidation money or probability."));
        ui.collapsing(label(language, "计算说明", "Calculation details"), |ui| {
            ui.small(label(language, "固定杠杆情景与账户杠杆无关。已收盘同源1m成交均价或收盘价生成估算带；盘中触价只截断预览，不代表真实强平。历史按需分页；缺口不补零。", "Fixed leverage scenarios are independent of account leverage. Closed same-market 1m bars seed estimated bands; live touches only cut previews. History loads on demand, and gaps are not filled with zero."));
        });
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) fn draw(
    ui: &egui::Ui,
    painter: &egui::Painter,
    id: egui::Id,
    rect: Rect,
    all_bars: &[venue_control_protocol::UiBar],
    all_studies: &[crate::chart::ChartStudyPoint],
    interval_ms: u64,
    visible: std::ops::Range<usize>,
    slots: usize,
    price_range: crate::chart::PriceRange,
    settings: &Settings,
    language: Language,
    depth: Option<(&[UiBookLevel], &[UiBookLevel])>,
    market_scope: Option<&venue_gateway_api::PublicMarketBinding>,
    minute_source: Option<(&[venue_control_protocol::UiBar], &[crate::chart::BaseMinuteStudy], (u64, u64))>,
    price_tick: Option<rust_decimal::Decimal>,
) {
    let painter = painter.with_clip_rect(rect.intersect(painter.clip_rect()));
    if settings.heatmap {
        super::mark_indicator_cache(ui.ctx(), super::IndicatorCacheKind::Heatmap, id);
        heatmap::draw(
            ui,
            &painter,
            id,
            rect,
            all_bars,
            all_studies,
            interval_ms,
            visible,
            slots,
            price_range,
            settings,
            language,
            market_scope,
            minute_source,
            price_tick,
        );
    } else {
        heatmap::cancel(ui.ctx(), id);
    }
    if settings.depth {
        let title = if let Some((bids, asks)) =
            depth.filter(|(b, a)| !b.is_empty() || !a.is_empty())
        {
            let rows = depth_rows(bids, asks, rect, price_range);
            let maximum = rows
                .iter()
                .flat_map(|row| row.iter())
                .copied()
                .fold(0.0_f64, f64::max);
            if maximum > 0.0 {
                for (index, row) in rows.iter().enumerate() {
                    for (amount, color) in row.iter().zip([theme::BUY, theme::SELL]) {
                        let width =
                            ((*amount / maximum) as f32 * rect.width().min(800.0) * 0.22).max(0.0);
                        if width > 0.0 {
                            painter.rect_filled(
                                Rect::from_min_max(
                                    Pos2::new(
                                        rect.right() - width,
                                        rect.top() + index as f32 * 4.0,
                                    ),
                                    Pos2::new(
                                        rect.right(),
                                        (rect.top() + (index + 1) as f32 * 4.0).min(rect.bottom()),
                                    ),
                                ),
                                0,
                                Color32::from_rgba_unmultiplied(
                                    color.r(),
                                    color.g(),
                                    color.b(),
                                    100,
                                ),
                            );
                        }
                    }
                }
            }
            label(
                language,
                "实时深度 · 已订阅档位",
                "Live depth · subscribed levels",
            )
        } else {
            label(
                language,
                "深度：等待有效盘口",
                "Depth: waiting for fresh book",
            )
        };
        painter.text(
            rect.right_bottom() - egui::vec2(6.0, 5.0),
            Align2::RIGHT_BOTTOM,
            title,
            FontId::proportional(10.0),
            theme::TEXT_SECONDARY,
        );
    }
}

pub(super) fn evict_heatmap(context: &egui::Context, id: egui::Id) {
    heatmap::cancel(context, id);
}

pub(super) fn evict_heatmap_result(context: &egui::Context, id: egui::Id) {
    heatmap::release_result(context, id);
}

pub(super) fn heatmap_retained_bytes(context: &egui::Context, id: egui::Id) -> usize {
    heatmap::retained_bytes(context, id)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn heatmap_pending_input_bytes() -> usize {
    heatmap::pending_input_bytes()
}

fn depth_rows(
    bids: &[UiBookLevel],
    asks: &[UiBookLevel],
    rect: Rect,
    range: crate::chart::PriceRange,
) -> Vec<[f64; 2]> {
    let mut rows = vec![[0.0; 2]; (rect.height() / 4.0).ceil().clamp(1.0, 1_024.0) as usize];
    for (side, levels) in [bids, asks].into_iter().enumerate() {
        for level in levels.iter().take(1_000) {
            let price = decimal_to_f64(level.price);
            if price < range.low || price > range.high {
                continue;
            }
            let Some(y) = range.price_to_y(rect.top(), rect.height(), price) else {
                continue;
            };
            let Some(value) = level
                .price
                .checked_mul(level.quantity)
                .map(decimal_to_f64)
                .filter(|v| v.is_finite() && *v > 0.0)
            else {
                continue;
            };
            let row = ((y - rect.top()) / 4.0).floor().max(0.0) as usize;
            if let Some(bucket) = rows.get_mut(row) {
                bucket[side] += value;
            }
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn depth_buckets_preserve_sides_and_exclude_offscreen_levels() {
        let level = |price, quantity| UiBookLevel {
            price: rust_decimal::Decimal::from(price),
            quantity: rust_decimal::Decimal::from(quantity),
        };
        let rows = depth_rows(
            &[level(99, 2), level(1, 100)],
            &[level(101, 3)],
            Rect::from_min_size(Pos2::ZERO, egui::vec2(100.0, 100.0)),
            crate::chart::PriceRange {
                low: 90.0,
                high: 110.0,
            },
        );
        assert_eq!(rows.iter().map(|r| r[0]).sum::<f64>(), 198.0);
        assert_eq!(rows.iter().map(|r| r[1]).sum::<f64>(), 303.0);
    }

    #[test]
    fn defaults_and_saved_scenario_settings_are_compatible()
    -> Result<(), Box<dyn std::error::Error>> {
        let original: Settings = serde_json::from_str("{}")?;
        assert!(original.heatmap);
        assert!(!original.depth && !original.show_delta && !original.show_cvd);
        let settings = Settings {
            heatmap: true,
            depth: true,
            order_flow: true,
            cumulative: true,
            ..original
        };
        assert_eq!(
            serde_json::from_str::<Settings>(&serde_json::to_string(&settings)?)?,
            settings
        );
        settings.validate()?;
        assert!(
            Settings {
                leverage: [false; 4],
                ..settings
            }
            .validate()
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn full_chart_renders_flow_depth_and_estimation_legend() {
        let ctx = egui::Context::default();
        crate::theme::apply(&ctx);
        let bars = (0..150)
            .map(|i| {
                let close = rust_decimal::Decimal::new(10_000 + (i * 17) % 500, 2);
                venue_control_protocol::UiBar {
                    open_time_ms: i as u64 * 60_000,
                    open: close,
                    high: close + rust_decimal::Decimal::ONE,
                    low: close - rust_decimal::Decimal::ONE,
                    close,
                    volume: Some((10 + i % 8).into()),
                }
            })
            .collect::<Vec<_>>();
        let studies = bars
            .iter()
            .enumerate()
            .map(|(i, b)| crate::chart::ChartStudyPoint {
                open_time_ms: b.open_time_ms,
                order_flow: venue_indicators::chart::OrderFlowValue {
                    delta: Some((i as i32 % 9 - 4).into()),
                    cumulative: Some((i as i32).into()),
                    ..Default::default()
                },
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let bids = vec![UiBookLevel {
            price: 102.into(),
            quantity: 30.into(),
        }];
        let asks = vec![UiBookLevel {
            price: 103.into(),
            quantity: 40.into(),
        }];
        let mut viewport = crate::chart::ChartViewport::default();
        for fresh in [true, false] {
            let mut output = ctx.run_ui(
                egui::RawInput {
                    screen_rect: Some(Rect::from_min_size(Pos2::ZERO, egui::vec2(1_200.0, 650.0))),
                    ..Default::default()
                },
                |ui| {
                    let mut settings = crate::chart_settings::ChartDisplaySettings::default();
                    settings.microstructure = Settings {
                        show_delta: true,
                        depth: true,
                        heatmap: true,
                        ..Default::default()
                    };
                    super::super::candle_plot(
                        ui,
                        &bars,
                        &studies,
                        &mut viewport,
                        Language::English,
                        &settings,
                        (2, 2),
                        None,
                        crate::chart::ChartInterval::OneMinute,
                        None,
                        None,
                        &Default::default(),
                        &[],
                        (None, None),
                        fresh.then_some((bids.as_slice(), asks.as_slice())),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                },
            );
            output.textures_delta.clear();
            let texts = output
                .shapes
                .iter()
                .filter_map(|s| {
                    if let egui::Shape::Text(t) = &s.shape {
                        Some(t.galley.job.text.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert!(texts.iter().any(|t| t.contains("Liquidation scenarios")));
            assert!(texts.iter().any(|t| t.contains("Delta")));
            assert!(texts.iter().any(|t| t.contains(if fresh {
                "Live depth"
            } else {
                "waiting for fresh book"
            })));
            assert!(
                output
                    .shapes
                    .iter()
                    .any(|s| matches!(&s.shape,egui::Shape::Mesh(m)
                        if m.vertices.len() == 4 && matches!(m.texture_id, egui::TextureId::Managed(_))))
            );
        }
    }
}
