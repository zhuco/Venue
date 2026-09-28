use eframe::egui::{self, Align2, Color32, RichText, Stroke};
#[cfg(test)]
#[path = "settings_panel_custom_tests.rs"]
mod custom_save_tests;

use crate::{
    chart_settings::{ChartDisplaySettings, IndicatorStyle},
    i18n::{IndicatorTextKey, Language, TextKey, indicator_text, text},
    model::AppModel,
    theme,
};
use venue_indicators::chart::{ChartIndicatorCategory, ChartIndicatorId, ChartIndicatorRegistry};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum SettingsTab {
    #[default]
    PriceStructure,
    FlowLiquidity,
    Volatility,
    Traditional,
    Custom,
    Backtest,
    General,
}

type IndicatorKind = ChartIndicatorId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpecialPanel {
    Structure,
    OrderFlow,
    ProfileOi,
}

#[derive(Clone, Debug, Default)]
pub struct SettingsPanelState {
    custom_editor: crate::custom_indicator::LibraryEditor,
    tab: SettingsTab,
    indicator: IndicatorKind,
    special: Option<SpecialPanel>,
    draft: Option<ChartDisplaySettings>,
    original: Option<ChartDisplaySettings>,
    error: Option<String>,
    target: Option<String>,
    endpoint_draft: Option<String>,
}

impl SettingsPanelState {
    pub fn focus_general(&mut self) {
        self.clear();
        self.target = None;
        self.tab = SettingsTab::General;
    }
    pub fn focus_indicators(&mut self, target: Option<String>) {
        self.clear();
        self.target = target;
        self.tab = SettingsTab::PriceStructure;
        self.indicator = IndicatorKind::Supertrend;
        self.special = None;
    }

    fn clear(&mut self) {
        self.custom_editor = Default::default();
        self.draft = None;
        self.original = None;
        self.error = None;
        self.endpoint_draft = None;
    }
}

pub fn show(
    context: &egui::Context,
    open: &mut bool,
    state: &mut SettingsPanelState,
    model: &mut AppModel,
    reconnect: &mut bool,
) {
    if !*open {
        state.clear();
        return;
    }
    let current = state
        .target
        .as_ref()
        .and_then(|key| model.preferences.chart_overrides.get(key))
        .unwrap_or(&model.preferences.chart)
        .clone();
    state.original.get_or_insert_with(|| current.clone());
    state.draft.get_or_insert(current);
    let language = model.preferences.language;
    let mut window_open = true;
    let mut saved = false;
    let mut close_requested = false;
    egui::Window::new("indicator-settings")
        .open(&mut window_open)
        .title_bar(false)
        .resizable(false)
        .collapsible(false)
        .anchor(Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .fixed_size(
            egui::vec2(720.0, 535.0).min(context.content_rect().size() - egui::vec2(32.0, 32.0)),
        )
        .frame(
            egui::Frame::new()
                .fill(Color32::from_rgb(31, 38, 50))
                .stroke(Stroke::new(1.0, Color32::from_rgb(54, 64, 79)))
                .inner_margin(egui::Margin::same(16))
                .corner_radius(egui::CornerRadius::same(8)),
        )
        .show(context, |ui| {
            top_tabs(ui, state, language, &mut close_requested);
            ui.separator();
            match state.tab {
                SettingsTab::PriceStructure
                | SettingsTab::FlowLiquidity
                | SettingsTab::Volatility
                | SettingsTab::Traditional => indicator_body(ui, state, language),
                SettingsTab::Custom => {
                    if let Some(draft) = &mut state.draft {
                        egui::ScrollArea::vertical()
                            .id_salt("custom-library-body")
                            .max_height(400.0)
                            .show(ui, |ui| {
                                crate::custom_indicator::settings_ui(
                                    ui,
                                    draft,
                                    &mut state.custom_editor,
                                    language,
                                )
                            });
                    }
                }
                SettingsTab::Backtest => placeholder(ui, language),
                SettingsTab::General => general_settings(ui, state, model, reconnect, language),
            }
            ui.separator();
            bottom_actions(ui, state, language, &mut saved, &mut close_requested);
        });

    if let Some(draft) = state.draft.clone() {
        match apply_chart_settings(&draft, model, language, state.target.as_deref()) {
            Ok(()) => state.error = None,
            Err(error) => state.error = Some(error),
        }
    }
    if state.custom_editor.pending() && state.error.is_none() {
        state.error =
            Some("请先保存指标或取消编辑 / Save the indicator or cancel editing first".into());
    }
    if saved && state.error.is_some() {
        saved = false;
        close_requested = false;
        window_open = true;
    }
    if close_requested {
        window_open = false;
    }
    if !window_open {
        if !saved && let Some(original) = state.original.clone() {
            let _ = apply_chart_settings(&original, model, language, state.target.as_deref());
        }
        *open = false;
        state.clear();
    }
}

fn top_tabs(
    ui: &mut egui::Ui,
    state: &mut SettingsPanelState,
    language: Language,
    close: &mut bool,
) {
    ui.horizontal(|ui| {
        let width = (ui.available_width() - 42.0).max(180.0);
        egui::ScrollArea::horizontal()
            .max_width(width)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    let zh = language == Language::SimplifiedChinese;
                    for (tab, title) in [
                        (
                            SettingsTab::PriceStructure,
                            if zh {
                                "价格结构"
                            } else {
                                "Price structure"
                            },
                        ),
                        (
                            SettingsTab::FlowLiquidity,
                            if zh {
                                "成交与流动性"
                            } else {
                                "Flow & liquidity"
                            },
                        ),
                        (
                            SettingsTab::Volatility,
                            if zh { "波动" } else { "Volatility" },
                        ),
                        (
                            SettingsTab::Traditional,
                            if zh { "传统指标" } else { "Traditional" },
                        ),
                        (
                            SettingsTab::Custom,
                            indicator_text(language, IndicatorTextKey::CustomTab),
                        ),
                        (
                            SettingsTab::Backtest,
                            indicator_text(language, IndicatorTextKey::BacktestTab),
                        ),
                        (
                            SettingsTab::General,
                            indicator_text(language, IndicatorTextKey::GeneralTab),
                        ),
                    ] {
                        ui.add_enabled_ui(tab != SettingsTab::Backtest, |ui| {
                            tab_button(ui, &mut state.tab, tab, title);
                        })
                        .response
                        .on_disabled_hover_text(indicator_text(
                            language,
                            IndicatorTextKey::FeatureUnavailable,
                        ));
                    }
                });
            });
        if ui
            .add(egui::Button::new(RichText::new("×").size(28.0)).frame(false))
            .clicked()
        {
            *close = true;
        }
    });
}

fn tab_button(ui: &mut egui::Ui, current: &mut SettingsTab, tab: SettingsTab, title: &str) {
    let selected = *current == tab;
    let response = ui.add(
        egui::Button::new(RichText::new(title).size(14.0).strong().color(if selected {
            theme::TEXT_PRIMARY
        } else {
            theme::TEXT_SECONDARY
        }))
        .frame(false),
    );
    if selected {
        ui.painter().line_segment(
            [response.rect.left_bottom(), response.rect.right_bottom()],
            Stroke::new(2.0, theme::BRAND),
        );
    }
    if response.clicked() {
        *current = tab;
    }
}

fn indicator_body(ui: &mut egui::Ui, state: &mut SettingsPanelState, language: Language) {
    let category = match state.tab {
        SettingsTab::PriceStructure => ChartIndicatorCategory::PriceStructure,
        SettingsTab::FlowLiquidity => ChartIndicatorCategory::FlowLiquidity,
        SettingsTab::Volatility => ChartIndicatorCategory::Volatility,
        SettingsTab::Traditional => ChartIndicatorCategory::Traditional,
        _ => return,
    };
    let list = ChartIndicatorRegistry::all()
        .iter()
        .filter(|item| item.category == category)
        .collect::<Vec<_>>();
    if !list.iter().any(|item| item.id == state.indicator) {
        state.indicator = list[0].id;
    }
    if !matches!(
        (category, state.special),
        (
            ChartIndicatorCategory::PriceStructure,
            Some(SpecialPanel::Structure)
        ) | (
            ChartIndicatorCategory::FlowLiquidity,
            Some(SpecialPanel::OrderFlow | SpecialPanel::ProfileOi)
        )
    ) {
        state.special = None;
    }
    ui.horizontal(|ui| {
        ui.allocate_ui_with_layout(
            egui::vec2(150.0, 405.0),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                ui.add_space(10.0);
                ui.label(
                    RichText::new(if language == Language::SimplifiedChinese {
                        "指标"
                    } else {
                        "Studies"
                    })
                    .size(13.0)
                    .strong()
                    .color(theme::TEXT_PRIMARY),
                );
                ui.add_space(4.0);
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let Some(draft) = state.draft.as_mut() else {
                            return;
                        };
                        let zh = language == Language::SimplifiedChinese;
                        if category == ChartIndicatorCategory::PriceStructure {
                            special_list_row(
                                ui,
                                &mut state.special,
                                SpecialPanel::Structure,
                                if zh {
                                    "S/R · 日周位"
                                } else {
                                    "S/R · levels"
                                },
                            );
                        }
                        if category == ChartIndicatorCategory::FlowLiquidity {
                            special_list_row(
                                ui,
                                &mut state.special,
                                SpecialPanel::OrderFlow,
                                if zh {
                                    "热图 · Delta"
                                } else {
                                    "Heatmap · Delta"
                                },
                            );
                            special_list_row(
                                ui,
                                &mut state.special,
                                SpecialPanel::ProfileOi,
                                if zh { "Profile · OI" } else { "Profile · OI" },
                            );
                        }
                        for item in list {
                            indicator_list_row(
                                ui,
                                &mut state.indicator,
                                &mut state.special,
                                item.id,
                                item.short_label,
                                style_mut(draft, item.id),
                            );
                        }
                    });
            },
        );
        ui.separator();
        ui.add_space(12.0);
        ui.allocate_ui_with_layout(
            egui::vec2(535.0, 405.0),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                ui.add_space(12.0);
                let Some(draft) = state.draft.as_mut() else {
                    return;
                };
                if let Some(panel) = state.special
                    && ui
                        .small_button(if language == Language::SimplifiedChinese {
                            "恢复本组默认"
                        } else {
                            "Reset this group"
                        })
                        .clicked()
                {
                    reset_special_panel(draft, panel);
                }
                match state.special {
                    Some(SpecialPanel::Structure) => structure_settings(ui, draft, language),
                    Some(SpecialPanel::OrderFlow) => {
                        crate::chart_view::microstructure::settings_ui(
                            ui,
                            &mut draft.microstructure,
                            language,
                        )
                    }
                    Some(SpecialPanel::ProfileOi) => profile_oi_settings(ui, draft, language),
                    None => indicator_editor(ui, draft, state.indicator, language),
                }
            },
        );
    });
}

fn indicator_list_row(
    ui: &mut egui::Ui,
    selected: &mut IndicatorKind,
    special: &mut Option<SpecialPanel>,
    kind: IndicatorKind,
    name: &str,
    style: &mut IndicatorStyle,
) {
    let is_selected = *selected == kind && special.is_none();
    let frame = egui::Frame::new()
        .fill(if is_selected {
            Color32::from_rgb(45, 55, 70)
        } else {
            Color32::TRANSPARENT
        })
        .inner_margin(egui::Margin::symmetric(8, 4));
    frame.show(ui, |ui| {
        ui.set_min_width(132.0);
        ui.horizontal(|ui| {
            ui.checkbox(&mut style.enabled, "");
            let response = ui.add(
                egui::Button::new(RichText::new(name).size(13.0).color(theme::TEXT_PRIMARY))
                    .frame(false)
                    .min_size(egui::vec2(88.0, 25.0)),
            );
            ui.label(RichText::new("›").size(16.0).color(theme::TEXT_SECONDARY));
            if response.clicked() {
                *selected = kind;
                *special = None;
            }
        });
    });
}

fn special_list_row(
    ui: &mut egui::Ui,
    selected: &mut Option<SpecialPanel>,
    panel: SpecialPanel,
    name: &str,
) {
    if ui
        .selectable_label(*selected == Some(panel), name)
        .clicked()
    {
        *selected = Some(panel);
    }
}

fn structure_settings(ui: &mut egui::Ui, draft: &mut ChartDisplaySettings, language: Language) {
    let tr = |zh, en| {
        if language == Language::SimplifiedChinese {
            zh
        } else {
            en
        }
    };
    egui::ScrollArea::vertical()
        .max_height(370.0)
        .show(ui, |ui| {
            let session = &mut draft.session;
            ui.heading(tr(
                "自动支撑／阻力 · 本地结构",
                "Support / Resistance · local structure",
            ));
            ui.checkbox(
                &mut session.sr_current,
                tr("当前周期 S/R", "Current timeframe S/R"),
            );
            ui.horizontal(|ui| {
                ui.checkbox(&mut session.sr_15m, "15m");
                ui.checkbox(&mut session.sr_1h, "1h");
                ui.checkbox(&mut session.sr_1d, "1d");
            });
            ui.small(tr(
                "评分用于已确认拐点排序，不是交易成功概率。",
                "Score ranks confirmed swings; it is not a trading probability.",
            ));
            ui.separator();
            ui.heading(tr("UTC 日周位置", "UTC day and week levels"));
            ui.horizontal(|ui| {
                ui.checkbox(&mut session.pdh, "PDH");
                ui.checkbox(&mut session.pdl, "PDL");
            });
            ui.horizontal(|ui| {
                ui.checkbox(&mut session.pwh, "PWH");
                ui.checkbox(&mut session.pwl, "PWL");
            });
            ui.horizontal(|ui| {
                ui.checkbox(&mut session.daily_open, tr("当日开盘", "Daily Open"));
                ui.checkbox(&mut session.weekly_open, tr("当周开盘", "Weekly Open"));
            });
            ui.separator();
            ui.checkbox(
                &mut session.daily_pivot,
                tr(
                    "日 Pivot · P / R1 / R2 / S1 / S2",
                    "Daily Pivot · P / R1 / R2 / S1 / S2",
                ),
            );
            ui.checkbox(
                &mut session.weekly_pivot,
                tr(
                    "周 Pivot · P / R1 / R2 / S1 / S2",
                    "Weekly Pivot · P / R1 / R2 / S1 / S2",
                ),
            );
            ui.checkbox(&mut session.pivot_r3_s3, "R3 / S3");
            ui.small(tr(
                "只使用完整收盘的前一 UTC 日／周；来源缺失时不画线。",
                "Uses only the preceding complete UTC day/week. Missing source leaves a gap.",
            ));
        });
}

fn profile_oi_settings(ui: &mut egui::Ui, draft: &mut ChartDisplaySettings, language: Language) {
    let tr = |zh, en| {
        if language == Language::SimplifiedChinese {
            zh
        } else {
            en
        }
    };
    egui::ScrollArea::vertical()
        .max_height(370.0)
        .show(ui, |ui| {
            let profile = &mut draft.profile;
            ui.heading(tr(
                "成交量分布 · 1m OHLCV 估算",
                "Volume Profile · 1m OHLCV estimate",
            ));
            ui.checkbox(&mut profile.visible_range, tr("可见区间", "Visible Range"));
            ui.checkbox(&mut profile.fixed_range, tr("固定区间", "Fixed Range"));
            ui.horizontal(|ui| {
                ui.label(tr("价格桶 tick ×", "Bucket tick ×"));
                ui.add(egui::DragValue::new(&mut profile.tick_multiple).range(1..=1_000));
                ui.label(tr("宽度 %", "Width %"));
                ui.add(egui::DragValue::new(&mut profile.width_percent).range(10..=35));
                ui.label(tr("不透明度 %", "Opacity %"));
                ui.add(egui::DragValue::new(&mut profile.opacity_percent).range(10..=80));
            });
            ui.horizontal(|ui| {
                ui.checkbox(&mut profile.poc, "POC");
                ui.checkbox(&mut profile.vah, "VAH");
                ui.checkbox(&mut profile.val, "VAL");
            });
            if profile.fixed_range {
                ui.small(tr(
                    "在图表上使用“框选 Profile”依次点击起点与终点。",
                    "Use Select Profile on the chart, then click start and end.",
                ));
            }
            ui.separator();
            ui.heading(tr("持仓量 OI", "Open Interest"));
            ui.checkbox(
                &mut draft.oi_pane,
                tr("OI 副图 · 基础币数量", "OI pane · base asset quantity"),
            );
            ui.small(tr(
                "按所选场所和合约显示真实样本；没有历史来源时留空。",
                "Shows samples for the selected venue and contract; missing history stays blank.",
            ));
        });
}

fn indicator_editor(
    ui: &mut egui::Ui,
    settings: &mut ChartDisplaySettings,
    kind: IndicatorKind,
    language: Language,
) {
    ui.horizontal(|ui| {
        ui.label(
            RichText::new(
                ChartIndicatorRegistry::all()
                    .iter()
                    .find(|item| item.id == kind)
                    .map(|item| {
                        if language == Language::SimplifiedChinese {
                            item.name_zh_cn
                        } else {
                            item.name_en
                        }
                    })
                    .unwrap_or(kind.short_label()),
            )
            .size(14.0)
            .strong(),
        );
        if ui
            .small_button(if language == Language::SimplifiedChinese {
                "恢复本项默认"
            } else {
                "Reset this study"
            })
            .clicked()
        {
            reset_selected_indicator(settings, kind);
        }
    });
    ui.add_space(18.0);
    match kind {
        IndicatorKind::Ma => triple_lines(
            ui,
            language,
            "MA",
            &mut settings.ma_periods,
            &mut settings.ma,
        ),
        IndicatorKind::Ema => triple_lines(
            ui,
            language,
            "EMA",
            &mut settings.ema_periods,
            &mut settings.ema,
        ),
        IndicatorKind::Wma => triple_lines(
            ui,
            language,
            "WMA",
            &mut settings.wma_periods,
            &mut settings.wma,
        ),
        IndicatorKind::Bollinger => {
            period_line(
                ui,
                language,
                "BOLL",
                &mut settings.bollinger_period,
                &mut settings.bollinger.color,
                &mut settings.bollinger.line_width_tenths,
                true,
            );
            value_row(
                ui,
                language,
                IndicatorTextKey::Deviation,
                &mut settings.bollinger_multiplier_hundredths,
                1..=100_000,
                100.0,
            );
            secondary_style(
                ui,
                language,
                &mut settings.bollinger,
                indicator_text(language, IndicatorTextKey::Middle),
            );
            ui.checkbox(
                &mut settings.bollinger.line_enabled[0],
                indicator_text(language, IndicatorTextKey::OuterBands),
            );
            settings.bollinger.line_enabled[2] = settings.bollinger.line_enabled[0];
            ui.checkbox(
                &mut settings.bollinger.line_enabled[1],
                indicator_text(language, IndicatorTextKey::Middle),
            );
            ui.checkbox(
                &mut settings.bollinger.background_enabled,
                indicator_text(language, IndicatorTextKey::BandFill),
            );
            fill_opacity(ui, language, &mut settings.bollinger);
        }
        IndicatorKind::Vwap => single_style(ui, language, &mut settings.vwap),
        IndicatorKind::Avl => single_style(ui, language, &mut settings.avl),
        IndicatorKind::Trix => period_line(
            ui,
            language,
            "TRIX",
            &mut settings.trix_period,
            &mut settings.trix.color,
            &mut settings.trix.line_width_tenths,
            true,
        ),
        IndicatorKind::Sar => {
            value_row(
                ui,
                language,
                IndicatorTextKey::Step,
                &mut settings.sar_step_ten_thousandths,
                1..=10_000,
                10_000.0,
            );
            value_row(
                ui,
                language,
                IndicatorTextKey::Maximum,
                &mut settings.sar_maximum_ten_thousandths,
                1..=100_000,
                10_000.0,
            );
            directional_styles(ui, language, &mut settings.sar, false);
        }
        IndicatorKind::Supertrend => {
            period_line(
                ui,
                language,
                "ATR",
                &mut settings.supertrend_period,
                &mut settings.supertrend.color,
                &mut settings.supertrend.line_width_tenths,
                false,
            );
            value_row(
                ui,
                language,
                IndicatorTextKey::Multiplier,
                &mut settings.supertrend_multiplier_hundredths,
                1..=100_000,
                100.0,
            );
            directional_styles(ui, language, &mut settings.supertrend, true);
        }
        IndicatorKind::Volume => directional_styles(ui, language, &mut settings.volume, false),
        IndicatorKind::Macd => {
            three_periods(
                ui,
                language,
                [
                    IndicatorTextKey::Fast,
                    IndicatorTextKey::Slow,
                    IndicatorTextKey::Signal,
                ],
                [
                    &mut settings.macd_fast_period,
                    &mut settings.macd_slow_period,
                    &mut settings.macd_signal_period,
                ],
            );
            secondary_style(ui, language, &mut settings.macd, "DEA");
            for (index, key) in [
                IndicatorTextKey::PositiveHistogram,
                IndicatorTextKey::NegativeHistogram,
            ]
            .into_iter()
            .enumerate()
            {
                ui.horizontal(|ui| {
                    ui.label(indicator_text(language, key));
                    ui.color_edit_button_srgb(&mut settings.macd.histogram_colors[index]);
                });
            }
        }
        IndicatorKind::Rsi => simple_period_style(
            ui,
            language,
            "RSI",
            &mut settings.rsi_period,
            &mut settings.rsi,
        ),
        IndicatorKind::Mfi => simple_period_style(
            ui,
            language,
            "MFI",
            &mut settings.mfi_period,
            &mut settings.mfi,
        ),
        IndicatorKind::Kdj => {
            two_periods(
                ui,
                language,
                IndicatorTextKey::Period,
                &mut settings.kdj_period,
                IndicatorTextKey::Smoothing,
                &mut settings.kdj_signal_period,
            );
            triple_colors(ui, language, &mut settings.kdj);
        }
        IndicatorKind::Obv => single_style(ui, language, &mut settings.obv),
        IndicatorKind::Cci => simple_period_style(
            ui,
            language,
            "CCI",
            &mut settings.cci_period,
            &mut settings.cci,
        ),
        IndicatorKind::StochRsi => {
            three_periods(
                ui,
                language,
                [
                    IndicatorTextKey::RsiPeriod,
                    IndicatorTextKey::StochasticPeriod,
                    IndicatorTextKey::Smoothing,
                ],
                [
                    &mut settings.stoch_rsi_period,
                    &mut settings.stoch_rsi_stochastic_period,
                    &mut settings.stoch_rsi_signal_period,
                ],
            );
            secondary_style(ui, language, &mut settings.stoch_rsi, "%D");
        }
        IndicatorKind::WilliamsR => simple_period_style(
            ui,
            language,
            "WR",
            &mut settings.williams_r_period,
            &mut settings.williams_r,
        ),
        IndicatorKind::Dmi => {
            simple_period_style(
                ui,
                language,
                "DMI",
                &mut settings.dmi_period,
                &mut settings.dmi,
            );
            triple_colors(ui, language, &mut settings.dmi);
        }
        IndicatorKind::Momentum => simple_period_style(
            ui,
            language,
            "MTM",
            &mut settings.momentum_period,
            &mut settings.momentum,
        ),
        IndicatorKind::Emv => simple_period_style(
            ui,
            language,
            "EMV",
            &mut settings.emv_period,
            &mut settings.emv,
        ),
        IndicatorKind::Atr => {
            simple_period_style(
                ui,
                language,
                "ATR",
                &mut settings.atr_period,
                &mut settings.atr,
            );
            ui.checkbox(
                &mut settings.atr_value_readout,
                if language == Language::SimplifiedChinese {
                    "顶部显示 ATR 数值"
                } else {
                    "Show ATR value in readout"
                },
            );
            ui.checkbox(
                &mut settings.atr_percent_readout,
                if language == Language::SimplifiedChinese {
                    "顶部显示 ATR%"
                } else {
                    "Show ATR% in readout"
                },
            );
        }
    }
}

fn triple_lines(
    ui: &mut egui::Ui,
    language: Language,
    prefix: &str,
    periods: &mut [u32; 3],
    style: &mut IndicatorStyle,
) {
    for (index, period) in periods.iter_mut().enumerate() {
        let color = match index {
            0 => &mut style.color,
            1 => &mut style.secondary_color,
            _ => &mut style.tertiary_color,
        };
        ui.horizontal(|ui| {
            ui.allocate_ui_with_layout(
                egui::vec2(120.0, 32.0),
                egui::Layout::left_to_right(egui::Align::Center),
                |ui| {
                    ui.set_min_size(egui::vec2(120.0, 32.0));
                    ui.checkbox(
                        &mut style.line_enabled[index],
                        format!("{prefix}{}", index + 1),
                    );
                },
            );
            ui.add_sized(
                [102.0, 32.0],
                egui::DragValue::new(period).range(1..=100_000),
            );
            source_selector(ui, language, format!("{prefix}-{index}"));
            line_sample(ui, color, &mut style.line_width_tenths);
        });
        ui.add_space(8.0);
    }
}

fn field_label(ui: &mut egui::Ui, label: impl Into<egui::WidgetText>) {
    ui.allocate_ui_with_layout(
        egui::vec2(120.0, 32.0),
        egui::Layout::left_to_right(egui::Align::Center),
        |ui| {
            ui.set_min_size(egui::vec2(120.0, 32.0));
            ui.label(label);
        },
    );
}

fn simple_period_style(
    ui: &mut egui::Ui,
    language: Language,
    name: &str,
    period: &mut u32,
    style: &mut IndicatorStyle,
) {
    period_line(
        ui,
        language,
        name,
        period,
        &mut style.color,
        &mut style.line_width_tenths,
        true,
    );
}

fn period_line(
    ui: &mut egui::Ui,
    language: Language,
    name: &str,
    period: &mut u32,
    color: &mut [u8; 3],
    width: &mut u8,
    source: bool,
) {
    ui.horizontal(|ui| {
        field_label(ui, RichText::new(name).size(13.0).strong());
        ui.add_sized(
            [102.0, 32.0],
            egui::DragValue::new(period).range(1..=100_000),
        );
        if source {
            source_selector(ui, language, name.to_owned());
        } else {
            ui.add_space(118.0);
        }
        line_sample(ui, color, width);
    });
    ui.add_space(10.0);
}

fn source_selector(
    ui: &mut egui::Ui,
    language: Language,
    id: impl std::hash::Hash + std::fmt::Debug,
) {
    egui::ComboBox::from_id_salt(id)
        .width(104.0)
        .selected_text(indicator_text(language, IndicatorTextKey::ClosePrice))
        .show_ui(ui, |ui| {
            ui.label(indicator_text(language, IndicatorTextKey::ClosePrice));
        });
}

fn line_sample(ui: &mut egui::Ui, color: &mut [u8; 3], width: &mut u8) {
    ui.label(RichText::new("━━━━").color(Color32::from_rgb(color[0], color[1], color[2])));
    ui.add(egui::DragValue::new(width).range(5..=40).suffix("/10"));
    ui.color_edit_button_srgb(color);
}

fn value_row(
    ui: &mut egui::Ui,
    language: Language,
    key: IndicatorTextKey,
    value: &mut u32,
    range: std::ops::RangeInclusive<u32>,
    divisor: f64,
) {
    ui.horizontal(|ui| {
        field_label(ui, indicator_text(language, key));
        ui.add_sized(
            [102.0, 32.0],
            egui::DragValue::new(value)
                .range(range)
                .custom_formatter(move |raw, _| format!("{:.4}", raw / divisor)),
        );
    });
    ui.add_space(8.0);
}

fn two_periods(
    ui: &mut egui::Ui,
    language: Language,
    key_a: IndicatorTextKey,
    a: &mut u32,
    key_b: IndicatorTextKey,
    b: &mut u32,
) {
    for (key, value) in [(key_a, a), (key_b, b)] {
        ui.horizontal(|ui| {
            field_label(ui, indicator_text(language, key));
            ui.add_sized(
                [102.0, 32.0],
                egui::DragValue::new(value).range(1..=100_000),
            );
        });
    }
    ui.add_space(12.0);
}

fn three_periods(
    ui: &mut egui::Ui,
    language: Language,
    keys: [IndicatorTextKey; 3],
    values: [&mut u32; 3],
) {
    for (key, value) in keys.into_iter().zip(values) {
        ui.horizontal(|ui| {
            field_label(ui, indicator_text(language, key));
            ui.add_sized(
                [102.0, 32.0],
                egui::DragValue::new(value).range(1..=100_000),
            );
        });
        ui.add_space(6.0);
    }
}

fn single_style(ui: &mut egui::Ui, language: Language, style: &mut IndicatorStyle) {
    ui.horizontal(|ui| {
        field_label(ui, indicator_text(language, IndicatorTextKey::Line));
        line_sample(ui, &mut style.color, &mut style.line_width_tenths);
    });
}

fn secondary_style(ui: &mut egui::Ui, language: Language, style: &mut IndicatorStyle, title: &str) {
    single_style(ui, language, style);
    ui.horizontal(|ui| {
        field_label(ui, title);
        ui.color_edit_button_srgb(&mut style.secondary_color);
    });
}

fn triple_colors(ui: &mut egui::Ui, language: Language, style: &mut IndicatorStyle) {
    for (index, color) in [
        &mut style.color,
        &mut style.secondary_color,
        &mut style.tertiary_color,
    ]
    .into_iter()
    .enumerate()
    {
        ui.horizontal(|ui| {
            ui.label(format!(
                "{} {}",
                indicator_text(language, IndicatorTextKey::Line),
                index + 1
            ));
            ui.label(RichText::new("━━━━").color(Color32::from_rgb(color[0], color[1], color[2])));
            ui.color_edit_button_srgb(color);
        });
        ui.add_space(6.0);
    }
}

fn directional_styles(
    ui: &mut egui::Ui,
    language: Language,
    style: &mut IndicatorStyle,
    background: bool,
) {
    for (rising, color) in [
        (true, &mut style.color),
        (false, &mut style.secondary_color),
    ] {
        ui.horizontal(|ui| {
            field_label(
                ui,
                indicator_text(
                    language,
                    if rising {
                        IndicatorTextKey::RisingLine
                    } else {
                        IndicatorTextKey::FallingLine
                    },
                ),
            );
            ui.label(
                RichText::new("━━━━━━").color(Color32::from_rgb(color[0], color[1], color[2])),
            );
            ui.color_edit_button_srgb(color);
        });
        ui.add_space(8.0);
    }
    if background {
        ui.checkbox(
            &mut style.background_enabled,
            indicator_text(language, IndicatorTextKey::RisingBackground),
        );
        ui.checkbox(
            &mut style.secondary_background_enabled,
            indicator_text(language, IndicatorTextKey::FallingBackground),
        );
        fill_opacity(ui, language, style);
    }
}

fn fill_opacity(ui: &mut egui::Ui, language: Language, style: &mut IndicatorStyle) {
    ui.horizontal(|ui| {
        field_label(ui, indicator_text(language, IndicatorTextKey::FillOpacity));
        ui.add(egui::Slider::new(&mut style.fill_opacity_percent, 0..=40).suffix("%"));
    });
}

fn bottom_actions(
    ui: &mut egui::Ui,
    state: &mut SettingsPanelState,
    language: Language,
    saved: &mut bool,
    close: &mut bool,
) {
    ui.allocate_ui_with_layout(
        egui::vec2(ui.available_width(), 66.0),
        egui::Layout::right_to_left(egui::Align::Center),
        |ui| {
            ui.add_space(20.0);
            if ui
                .add_sized(
                    [136.0, 40.0],
                    egui::Button::new(
                        RichText::new(indicator_text(language, IndicatorTextKey::Save))
                            .strong()
                            .color(Color32::from_rgb(28, 33, 40)),
                    )
                    .fill(Color32::from_rgb(252, 213, 53)),
                )
                .clicked()
            {
                *saved = true;
                *close = true;
            }
            if ui
                .add_sized(
                    [136.0, 40.0],
                    egui::Button::new(indicator_text(language, IndicatorTextKey::RestoreDefaults))
                        .fill(Color32::from_rgb(47, 58, 73)),
                )
                .clicked()
            {
                state.draft = Some(ChartDisplaySettings::default());
                state.custom_editor = Default::default();
            }
            if let Some(error) = &state.error {
                ui.colored_label(theme::SELL, error);
            } else {
                ui.colored_label(
                    theme::TEXT_SECONDARY,
                    indicator_text(language, IndicatorTextKey::LiveRedraw),
                );
            }
        },
    );
}

fn placeholder(ui: &mut egui::Ui, language: Language) {
    ui.allocate_ui_with_layout(
        egui::vec2(ui.available_width(), 405.0),
        egui::Layout::centered_and_justified(egui::Direction::TopDown),
        |ui| {
            ui.colored_label(
                theme::TEXT_SECONDARY,
                indicator_text(language, IndicatorTextKey::FeatureUnavailable),
            );
        },
    );
}

fn general_settings(
    ui: &mut egui::Ui,
    state: &mut SettingsPanelState,
    model: &mut AppModel,
    reconnect: &mut bool,
    language: Language,
) {
    ui.allocate_ui_with_layout(
        egui::vec2(ui.available_width(), 405.0),
        egui::Layout::top_down(egui::Align::Min),
        |ui| {
            ui.add_space(18.0);
            ui.set_max_width(620.0);
            ui.label(text(language, TextKey::Language));
            egui::ComboBox::from_id_salt("venueflow-language")
                .selected_text(model.preferences.language.label())
                .show_ui(ui, |ui| {
                    for option in Language::ALL {
                        ui.selectable_value(
                            &mut model.preferences.language,
                            option,
                            option.label(),
                        );
                    }
                });
            ui.add_space(12.0);
            ui.label(text(language, TextKey::ControlUrl));
            let draft = state
                .endpoint_draft
                .get_or_insert_with(|| model.preferences.endpoint.clone());
            ui.add(
                egui::TextEdit::singleline(draft)
                    .desired_width(580.0)
                    .hint_text(crate::server_connection::DEFAULT_CONTROL_ENDPOINT),
            );
            ui.horizontal(|ui| {
                if ui.button(text(language, TextKey::DefaultServer)).clicked() {
                    *draft = crate::server_connection::DEFAULT_CONTROL_ENDPOINT.to_owned();
                }
                let normalized = crate::server_connection::normalize_endpoint(draft);
                if ui
                    .add_enabled(
                        normalized.is_some(),
                        egui::Button::new(text(language, TextKey::ApplyServer)),
                    )
                    .clicked()
                    && let Some(endpoint) = normalized
                {
                    model.preferences.endpoint = endpoint.clone();
                    *draft = endpoint;
                    *reconnect = true;
                }
            });
            let valid = crate::server_connection::normalize_endpoint(draft).is_some();
            ui.colored_label(
                if valid {
                    theme::TEXT_SECONDARY
                } else {
                    theme::SELL
                },
                text(
                    language,
                    if valid {
                        TextKey::ServerAddressHint
                    } else {
                        TextKey::InvalidServerAddress
                    },
                ),
            );
            ui.add_space(12.0);
            ui.add(
                egui::Slider::new(&mut model.preferences.ui_scale, 0.85..=1.35)
                    .text(text(language, TextKey::UiScale)),
            );
            ui.checkbox(
                &mut model.preferences.show_status_bar,
                text(language, TextKey::ShowStatus),
            );
        },
    );
}

fn apply_chart_settings(
    settings: &ChartDisplaySettings,
    model: &mut AppModel,
    _language: Language,
    target: Option<&str>,
) -> Result<(), String> {
    let current = target
        .and_then(|key| model.preferences.chart_overrides.get(key))
        .unwrap_or(&model.preferences.chart);
    if current == settings {
        return Ok(());
    }
    settings.validate().map_err(str::to_owned)?;
    if let Some(target) = target {
        model
            .preferences
            .chart_overrides
            .insert(target.to_owned(), settings.clone());
        return Ok(());
    }
    #[cfg(not(target_arch = "wasm32"))]
    model
        .local_markets
        .reconfigure_studies(settings.engine_config())
        .map_err(|error| {
            format!(
                "{}: {error}",
                indicator_text(_language, IndicatorTextKey::RecalculationFailed)
            )
        })?;
    model.preferences.chart = settings.clone();
    Ok(())
}

fn reset_special_panel(settings: &mut ChartDisplaySettings, panel: SpecialPanel) {
    let defaults = ChartDisplaySettings::default();
    match panel {
        SpecialPanel::Structure => settings.session = defaults.session,
        SpecialPanel::OrderFlow => settings.microstructure = defaults.microstructure,
        SpecialPanel::ProfileOi => {
            settings.profile = defaults.profile;
            settings.oi_pane = defaults.oi_pane;
        }
    }
}

fn reset_selected_indicator(settings: &mut ChartDisplaySettings, kind: IndicatorKind) {
    let mut defaults = ChartDisplaySettings::default();
    *style_mut(settings, kind) = *style_mut(&mut defaults, kind);
    match kind {
        IndicatorKind::Ma => settings.ma_periods = defaults.ma_periods,
        IndicatorKind::Ema => settings.ema_periods = defaults.ema_periods,
        IndicatorKind::Wma => settings.wma_periods = defaults.wma_periods,
        IndicatorKind::Bollinger => {
            settings.bollinger_period = defaults.bollinger_period;
            settings.bollinger_multiplier_hundredths = defaults.bollinger_multiplier_hundredths;
        }
        IndicatorKind::Vwap | IndicatorKind::Avl | IndicatorKind::Volume | IndicatorKind::Obv => {}
        IndicatorKind::Trix => settings.trix_period = defaults.trix_period,
        IndicatorKind::Sar => {
            settings.sar_step_ten_thousandths = defaults.sar_step_ten_thousandths;
            settings.sar_maximum_ten_thousandths = defaults.sar_maximum_ten_thousandths;
        }
        IndicatorKind::Supertrend => {
            settings.supertrend_period = defaults.supertrend_period;
            settings.supertrend_multiplier_hundredths = defaults.supertrend_multiplier_hundredths;
        }
        IndicatorKind::Macd => {
            settings.macd_fast_period = defaults.macd_fast_period;
            settings.macd_slow_period = defaults.macd_slow_period;
            settings.macd_signal_period = defaults.macd_signal_period;
        }
        IndicatorKind::Rsi => settings.rsi_period = defaults.rsi_period,
        IndicatorKind::Mfi => settings.mfi_period = defaults.mfi_period,
        IndicatorKind::Kdj => {
            settings.kdj_period = defaults.kdj_period;
            settings.kdj_signal_period = defaults.kdj_signal_period;
        }
        IndicatorKind::Cci => settings.cci_period = defaults.cci_period,
        IndicatorKind::StochRsi => {
            settings.stoch_rsi_period = defaults.stoch_rsi_period;
            settings.stoch_rsi_stochastic_period = defaults.stoch_rsi_stochastic_period;
            settings.stoch_rsi_signal_period = defaults.stoch_rsi_signal_period;
        }
        IndicatorKind::WilliamsR => settings.williams_r_period = defaults.williams_r_period,
        IndicatorKind::Dmi => settings.dmi_period = defaults.dmi_period,
        IndicatorKind::Momentum => settings.momentum_period = defaults.momentum_period,
        IndicatorKind::Emv => settings.emv_period = defaults.emv_period,
        IndicatorKind::Atr => {
            settings.atr_period = defaults.atr_period;
            settings.atr_value_readout = defaults.atr_value_readout;
            settings.atr_percent_readout = defaults.atr_percent_readout;
        }
    }
}

fn style_mut(settings: &mut ChartDisplaySettings, kind: IndicatorKind) -> &mut IndicatorStyle {
    match kind {
        IndicatorKind::Ma => &mut settings.ma,
        IndicatorKind::Ema => &mut settings.ema,
        IndicatorKind::Wma => &mut settings.wma,
        IndicatorKind::Bollinger => &mut settings.bollinger,
        IndicatorKind::Vwap => &mut settings.vwap,
        IndicatorKind::Avl => &mut settings.avl,
        IndicatorKind::Trix => &mut settings.trix,
        IndicatorKind::Sar => &mut settings.sar,
        IndicatorKind::Supertrend => &mut settings.supertrend,
        IndicatorKind::Volume => &mut settings.volume,
        IndicatorKind::Macd => &mut settings.macd,
        IndicatorKind::Rsi => &mut settings.rsi,
        IndicatorKind::Mfi => &mut settings.mfi,
        IndicatorKind::Kdj => &mut settings.kdj,
        IndicatorKind::Obv => &mut settings.obv,
        IndicatorKind::Cci => &mut settings.cci,
        IndicatorKind::StochRsi => &mut settings.stoch_rsi,
        IndicatorKind::WilliamsR => &mut settings.williams_r,
        IndicatorKind::Dmi => &mut settings.dmi,
        IndicatorKind::Momentum => &mut settings.momentum,
        IndicatorKind::Emv => &mut settings.emv,
        IndicatorKind::Atr => &mut settings.atr,
    }
}

#[cfg(test)]
mod tests {
    use super::SettingsTab;

    #[test]
    fn resetting_one_study_or_group_preserves_other_chart_choices() {
        use venue_indicators::chart::ChartIndicatorId;
        let defaults = crate::chart_settings::ChartDisplaySettings::default();
        let mut chart = defaults.clone();
        chart.ma_periods = [2, 3, 4];
        chart.ma.enabled = !defaults.ma.enabled;
        chart.ema_periods = [10, 20, 30];
        chart.session.pdh = false;
        chart.profile.visible_range = true;
        chart.oi_pane = true;
        super::reset_selected_indicator(&mut chart, ChartIndicatorId::Ma);
        assert_eq!(chart.ma_periods, defaults.ma_periods);
        assert_eq!(chart.ma, defaults.ma);
        assert_eq!(chart.ema_periods, [10, 20, 30]);
        assert!(!chart.session.pdh);
        assert!(chart.profile.visible_range);
        super::reset_special_panel(&mut chart, super::SpecialPanel::ProfileOi);
        assert_eq!(chart.profile, defaults.profile);
        assert_eq!(chart.oi_pane, defaults.oi_pane);
        assert!(!chart.session.pdh);
    }

    #[test]
    fn server_settings_apply_only_valid_explicit_changes_in_both_languages() {
        use eframe::egui;
        fn labels(shape: &egui::Shape, output: &mut Vec<(String, egui::Rect)>) {
            match shape {
                egui::Shape::Text(text) => output.push((
                    text.galley.job.text.clone(),
                    egui::Rect::from_min_size(text.pos, text.galley.size()),
                )),
                egui::Shape::Vec(shapes) => shapes.iter().for_each(|shape| labels(shape, output)),
                _ => (),
            }
        }
        for language in crate::i18n::Language::ALL {
            let context = egui::Context::default();
            crate::theme::apply(&context);
            let original = crate::server_connection::DEFAULT_CONTROL_ENDPOINT;
            let mut model = crate::model::AppModel::new(crate::model::Preferences {
                endpoint: original.into(),
                language,
                ..Default::default()
            });
            let mut state = super::SettingsPanelState {
                tab: SettingsTab::General,
                endpoint_draft: Some("https://other.example.com/".into()),
                ..Default::default()
            };
            let mut open = true;
            let mut reconnect = false;
            let screen = egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(1100.0, 700.0));
            let mut rendered = Vec::new();
            for _ in 0..3 {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(screen),
                        ..Default::default()
                    },
                    |ui| super::show(ui.ctx(), &mut open, &mut state, &mut model, &mut reconnect),
                );
                output.textures_delta.clear();
                rendered.clear();
                for shape in output.shapes {
                    labels(&shape.shape, &mut rendered);
                }
            }
            assert_eq!(model.preferences.endpoint, original);
            assert!(!reconnect);
            for key in [
                crate::i18n::TextKey::ControlUrl,
                crate::i18n::TextKey::DefaultServer,
                crate::i18n::TextKey::ApplyServer,
            ] {
                let label = crate::i18n::text(language, key);
                assert!(
                    rendered
                        .iter()
                        .any(|(text, rect)| text == label && screen.contains_rect(*rect)),
                    "Missing or clipped: {label}"
                );
            }
            let apply_label = crate::i18n::text(language, crate::i18n::TextKey::ApplyServer);
            let point = rendered
                .iter()
                .find(|(text, _)| text == apply_label)
                .map(|(_, rect)| rect.center());
            assert!(point.is_some());
            let Some(point) = point else {
                return;
            };
            for pressed in [true, false] {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(screen),
                        events: vec![
                            egui::Event::PointerMoved(point),
                            egui::Event::PointerButton {
                                pos: point,
                                button: egui::PointerButton::Primary,
                                pressed,
                                modifiers: Default::default(),
                            },
                        ],
                        ..Default::default()
                    },
                    |ui| super::show(ui.ctx(), &mut open, &mut state, &mut model, &mut reconnect),
                );
                output.textures_delta.clear();
            }
            assert_eq!(model.preferences.endpoint, "https://other.example.com");
            assert!(reconnect);
            state.endpoint_draft = Some("http://other.example.com".into());
            reconnect = false;
            for pressed in [true, false] {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(screen),
                        events: vec![
                            egui::Event::PointerMoved(point),
                            egui::Event::PointerButton {
                                pos: point,
                                button: egui::PointerButton::Primary,
                                pressed,
                                modifiers: Default::default(),
                            },
                        ],
                        ..Default::default()
                    },
                    |ui| super::show(ui.ctx(), &mut open, &mut state, &mut model, &mut reconnect),
                );
                output.textures_delta.clear();
            }
            assert!(!reconnect);
            assert_eq!(model.preferences.endpoint, "https://other.example.com");
        }
    }

    #[test]
    fn custom_tab_exposes_enable_and_save_in_both_languages() {
        use eframe::egui;
        fn collect(shape: &egui::Shape, labels: &mut Vec<(String, egui::Rect)>) {
            match shape {
                egui::Shape::Text(t) => labels.push((
                    t.galley.job.text.clone(),
                    egui::Rect::from_min_size(t.pos, t.galley.size()),
                )),
                egui::Shape::Vec(shapes) => shapes.iter().for_each(|s| collect(s, labels)),
                _ => (),
            }
        }
        for language in crate::i18n::Language::ALL {
            let context = egui::Context::default();
            crate::theme::apply(&context);
            let mut model = crate::model::AppModel::new(crate::model::Preferences {
                language,
                ..Default::default()
            });
            let mut state = super::SettingsPanelState {
                tab: SettingsTab::Custom,
                ..Default::default()
            };
            let mut open = true;
            let mut reconnect = false;
            let screen = egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(1100.0, 700.0));
            let mut labels = Vec::new();
            for _ in 0..3 {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(screen),
                        ..Default::default()
                    },
                    |ui| {
                        super::show(ui.ctx(), &mut open, &mut state, &mut model, &mut reconnect);
                    },
                );
                output.textures_delta.clear();
                labels.clear();
                for shape in output.shapes {
                    collect(&shape.shape, &mut labels);
                }
            }
            let enable = if language == crate::i18n::Language::English {
                "Enabled"
            } else {
                "启用"
            };
            for label in [
                enable,
                crate::i18n::indicator_text(language, crate::i18n::IndicatorTextKey::Save),
            ] {
                assert!(
                    labels
                        .iter()
                        .any(|(s, r)| s == label && screen.contains_rect(*r)),
                    "Missing or clipped: {label}"
                );
            }
            assert!(!reconnect);
        }
    }

    #[test]
    fn chart_settings_are_isolated_and_persist_without_private_data()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut model = crate::model::AppModel::new(Default::default());
        let original = model.preferences.chart.clone();
        let mut configured = original.clone();
        configured.ma_periods = [3, 9, 20];
        configured.custom_ema_adx.enabled = true;
        configured.custom_ema_adx.parameters.ema_periods = [5, 13, 34];
        super::apply_chart_settings(
            &configured,
            &mut model,
            crate::i18n::Language::English,
            Some("chart-a"),
        )?;
        assert_eq!(model.preferences.chart, original);
        assert!(!model.preferences.chart_overrides.contains_key("chart-b"));
        let restored: crate::model::Preferences =
            serde_json::from_str(&serde_json::to_string(&model.preferences)?)?;
        assert_eq!(restored.chart_overrides.get("chart-a"), Some(&configured));
        super::apply_chart_settings(
            &original,
            &mut model,
            crate::i18n::Language::English,
            Some("chart-a"),
        )?;
        assert_eq!(
            model.preferences.chart_overrides.get("chart-a"),
            Some(&original)
        );
        Ok(())
    }

    #[test]
    fn settings_groups_use_the_registry_without_duplicating_studies() {
        use venue_indicators::chart::{ChartIndicatorCategory, ChartIndicatorRegistry};
        assert_eq!(SettingsTab::default(), SettingsTab::PriceStructure);
        let descriptors = ChartIndicatorRegistry::all();
        assert_eq!(descriptors.len(), 22);
        for category in [
            ChartIndicatorCategory::PriceStructure,
            ChartIndicatorCategory::FlowLiquidity,
            ChartIndicatorCategory::Volatility,
            ChartIndicatorCategory::Traditional,
        ] {
            assert!(descriptors.iter().any(|item| item.category == category));
        }
        assert!(descriptors.iter().all(|item| !item.short_label.is_empty()));
    }

    #[test]
    fn structure_and_flow_controls_remain_visible_in_their_categories() {
        use eframe::egui;
        fn labels(shape: &egui::Shape, output: &mut Vec<String>) {
            match shape {
                egui::Shape::Text(text) => output.push(text.galley.job.text.clone()),
                egui::Shape::Vec(shapes) => shapes.iter().for_each(|shape| labels(shape, output)),
                _ => {}
            }
        }
        for (tab, special, expected) in [
            (
                SettingsTab::PriceStructure,
                super::SpecialPanel::Structure,
                "当前周期 S/R",
            ),
            (
                SettingsTab::FlowLiquidity,
                super::SpecialPanel::OrderFlow,
                "Delta · base",
            ),
            (
                SettingsTab::FlowLiquidity,
                super::SpecialPanel::ProfileOi,
                "OI 副图 · 基础币数量",
            ),
        ] {
            let context = egui::Context::default();
            crate::theme::apply(&context);
            let mut model = crate::model::AppModel::new(Default::default());
            let mut state = super::SettingsPanelState {
                tab,
                special: Some(special),
                ..Default::default()
            };
            let mut open = true;
            let mut reconnect = false;
            let mut rendered = Vec::new();
            for _ in 0..3 {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(egui::Rect::from_min_size(
                            egui::Pos2::ZERO,
                            egui::vec2(1100.0, 700.0),
                        )),
                        ..Default::default()
                    },
                    |ui| super::show(ui.ctx(), &mut open, &mut state, &mut model, &mut reconnect),
                );
                output.textures_delta.clear();
                rendered.clear();
                for shape in output.shapes {
                    labels(&shape.shape, &mut rendered);
                }
            }
            assert!(
                rendered.iter().any(|label| label == expected),
                "missing {expected} in {tab:?}"
            );
        }
    }
}
