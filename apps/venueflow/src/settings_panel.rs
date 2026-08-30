use eframe::egui::{self, Align2, Color32, RichText, Stroke};

use crate::{
    chart_settings::{ChartDisplaySettings, IndicatorStyle},
    i18n::{Language, TextKey, text},
    model::AppModel,
    theme,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum SettingsTab {
    #[default]
    Main,
    Sub,
    Data,
    Custom,
    Backtest,
    General,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum IndicatorKind {
    #[default]
    Sma,
    Ema,
    Bollinger,
    Vwap,
    Rsi,
    Macd,
    Atr,
}

#[derive(Clone, Debug, Default)]
pub struct SettingsPanelState {
    tab: SettingsTab,
    indicator: IndicatorKind,
    draft: Option<ChartDisplaySettings>,
    error: Option<String>,
}

impl SettingsPanelState {
    pub fn focus_indicators(&mut self) {
        self.tab = SettingsTab::Main;
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
        state.draft = None;
        state.error = None;
        return;
    }
    state
        .draft
        .get_or_insert_with(|| model.preferences.chart.clone());
    let language = model.preferences.language;
    let mut close_requested = false;
    egui::Window::new(label(language, "指标设置", "Indicator settings"))
        .open(open)
        .resizable(false)
        .collapsible(false)
        .anchor(Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .default_size(egui::vec2(720.0, 535.0))
        .frame(
            egui::Frame::new()
                .fill(theme::BG_SECONDARY)
                .stroke(Stroke::new(1.0, theme::DIVIDER))
                .inner_margin(egui::Margin::same(14))
                .corner_radius(egui::CornerRadius::same(6)),
        )
        .show(context, |ui| {
            ui.set_min_size(egui::vec2(700.0, 505.0));
            ui.horizontal(|ui| {
                for (tab, chinese, english) in [
                    (SettingsTab::Main, "主图", "Main"),
                    (SettingsTab::Sub, "副图", "Sub-chart"),
                    (SettingsTab::Data, "副图-大数据指标", "Data"),
                    (SettingsTab::Custom, "自定义", "Custom"),
                    (SettingsTab::Backtest, "回测测试", "Backtest"),
                ] {
                    tab_button(ui, &mut state.tab, tab, label(language, chinese, english));
                }
                tab_button(
                    ui,
                    &mut state.tab,
                    SettingsTab::General,
                    label(language, "通用", "General"),
                );
            });
            ui.separator();

            match state.tab {
                SettingsTab::Main | SettingsTab::Sub => indicator_settings(ui, state, language),
                SettingsTab::Data | SettingsTab::Custom | SettingsTab::Backtest => {
                    unavailable_indicator_category(ui, language)
                }
                SettingsTab::General => general_settings(ui, model, reconnect, language),
            }

            ui.separator();
            ui.horizontal(|ui| {
                if let Some(error) = &state.error {
                    ui.colored_label(theme::SELL, error);
                } else {
                    ui.colored_label(
                        theme::TEXT_SECONDARY,
                        label(
                            language,
                            "保存后仅重算一次历史指标，实时行情继续增量更新",
                            "Save recalculates history once; live updates remain incremental",
                        ),
                    );
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .add_sized(
                            [110.0, 34.0],
                            egui::Button::new(
                                RichText::new(label(language, "保存", "Save"))
                                    .strong()
                                    .color(theme::BG_PRIMARY),
                            )
                            .fill(theme::BRAND_HOVER),
                        )
                        .clicked()
                        && save_chart_settings(state, model)
                    {
                        close_requested = true;
                    }
                    if ui
                        .add_sized(
                            [110.0, 34.0],
                            egui::Button::new(label(language, "恢复默认", "Restore defaults")),
                        )
                        .clicked()
                    {
                        state.draft = Some(ChartDisplaySettings::default());
                        state.error = None;
                    }
                });
            });
        });
    if close_requested {
        *open = false;
        state.draft = None;
    }
}

fn tab_button(ui: &mut egui::Ui, current: &mut SettingsTab, tab: SettingsTab, title: &str) {
    if ui
        .selectable_label(*current == tab, RichText::new(title).size(14.0).strong())
        .clicked()
    {
        *current = tab;
    }
}

fn indicator_settings(ui: &mut egui::Ui, state: &mut SettingsPanelState, language: Language) {
    let tab = state.tab;
    if tab == SettingsTab::Main
        && matches!(
            state.indicator,
            IndicatorKind::Rsi | IndicatorKind::Macd | IndicatorKind::Atr
        )
    {
        state.indicator = IndicatorKind::Sma;
    } else if tab == SettingsTab::Sub
        && matches!(
            state.indicator,
            IndicatorKind::Sma
                | IndicatorKind::Ema
                | IndicatorKind::Bollinger
                | IndicatorKind::Vwap
        )
    {
        state.indicator = IndicatorKind::Rsi;
    }
    let selected = state.indicator;
    ui.horizontal(|ui| {
        ui.allocate_ui_with_layout(
            egui::vec2(150.0, 400.0),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                ui.colored_label(
                    theme::TEXT_SECONDARY,
                    label(
                        language,
                        if tab == SettingsTab::Main {
                            "主图"
                        } else {
                            "副图"
                        },
                        if tab == SettingsTab::Main {
                            "Main"
                        } else {
                            "Sub-chart"
                        },
                    ),
                );
                ui.add_space(5.0);
                if let Some(draft) = state.draft.as_mut() {
                    let kinds: &[(IndicatorKind, &str)] = match tab {
                        SettingsTab::Main => &[
                            (IndicatorKind::Sma, "MA"),
                            (IndicatorKind::Ema, "EMA"),
                            (IndicatorKind::Bollinger, "BOLL"),
                            (IndicatorKind::Vwap, "VWAP"),
                        ],
                        SettingsTab::Sub => &[
                            (IndicatorKind::Rsi, "RSI"),
                            (IndicatorKind::Macd, "MACD"),
                            (IndicatorKind::Atr, "ATR"),
                        ],
                        _ => &[],
                    };
                    for (kind, name) in kinds {
                        let enabled = &mut indicator_style_mut(draft, *kind).enabled;
                        indicator_button(ui, &mut state.indicator, *kind, name, enabled);
                    }
                    ui.add_space(14.0);
                    ui.checkbox(&mut draft.show_volume, label(language, "成交量", "Volume"));
                }
            },
        );
        ui.separator();
        ui.allocate_ui_with_layout(
            egui::vec2(525.0, 400.0),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                let Some(draft) = state.draft.as_mut() else {
                    return;
                };
                indicator_editor(ui, draft, selected, language);
            },
        );
    });
}

fn indicator_button(
    ui: &mut egui::Ui,
    selected: &mut IndicatorKind,
    kind: IndicatorKind,
    name: &str,
    enabled: &mut bool,
) {
    ui.horizontal(|ui| {
        ui.checkbox(enabled, "");
        if ui
            .add_sized(
                [112.0, 32.0],
                egui::Button::selectable(*selected == kind, format!("{name}      ›")),
            )
            .clicked()
        {
            *selected = kind;
        }
    });
}

fn indicator_style_mut(
    settings: &mut ChartDisplaySettings,
    kind: IndicatorKind,
) -> &mut IndicatorStyle {
    match kind {
        IndicatorKind::Sma => &mut settings.sma,
        IndicatorKind::Ema => &mut settings.ema,
        IndicatorKind::Bollinger => &mut settings.bollinger,
        IndicatorKind::Vwap => &mut settings.vwap,
        IndicatorKind::Rsi => &mut settings.rsi,
        IndicatorKind::Macd => &mut settings.macd,
        IndicatorKind::Atr => &mut settings.atr,
    }
}

fn indicator_editor(
    ui: &mut egui::Ui,
    settings: &mut ChartDisplaySettings,
    kind: IndicatorKind,
    language: Language,
) {
    let (zh_title, en_title) = match kind {
        IndicatorKind::Sma => ("MA - 移动平均线", "MA - Moving Average"),
        IndicatorKind::Ema => ("EMA - 指数移动平均线", "EMA - Exponential Moving Average"),
        IndicatorKind::Bollinger => ("BOLL - 布林带", "BOLL - Bollinger Bands"),
        IndicatorKind::Vwap => (
            "VWAP - 成交量加权均价",
            "VWAP - Volume Weighted Average Price",
        ),
        IndicatorKind::Rsi => ("RSI - 相对强弱指标", "RSI - Relative Strength Index"),
        IndicatorKind::Macd => (
            "MACD - 指数平滑异同移动平均线",
            "MACD - Moving Average Convergence Divergence",
        ),
        IndicatorKind::Atr => ("ATR - 平均真实波幅", "ATR - Average True Range"),
    };
    ui.label(
        RichText::new(label(language, zh_title, en_title))
            .size(14.0)
            .strong(),
    );
    ui.add_space(18.0);

    match kind {
        IndicatorKind::Sma => {
            period_editor(ui, language, "MA1", &mut settings.sma_period);
            style_editor(ui, language, &mut settings.sma, false);
        }
        IndicatorKind::Ema => {
            period_editor(ui, language, "EMA1", &mut settings.ema_period);
            style_editor(ui, language, &mut settings.ema, false);
        }
        IndicatorKind::Bollinger => {
            period_editor(ui, language, "BOLL", &mut settings.bollinger_period);
            ui.horizontal(|ui| {
                ui.label(label(language, "标准差倍数", "Deviation multiplier"));
                ui.add(
                    egui::DragValue::new(&mut settings.bollinger_multiplier_hundredths)
                        .range(1..=100_000)
                        .custom_formatter(|value, _| format!("{:.2}", value / 100.0)),
                );
            });
            style_editor(ui, language, &mut settings.bollinger, true);
        }
        IndicatorKind::Vwap => style_editor(ui, language, &mut settings.vwap, false),
        IndicatorKind::Rsi => {
            period_editor(ui, language, "RSI", &mut settings.rsi_period);
            style_editor(ui, language, &mut settings.rsi, true);
        }
        IndicatorKind::Macd => {
            ui.horizontal(|ui| {
                drag_period(ui, language, "快线", "Fast", &mut settings.macd_fast_period);
                drag_period(ui, language, "慢线", "Slow", &mut settings.macd_slow_period);
                drag_period(
                    ui,
                    language,
                    "信号线",
                    "Signal",
                    &mut settings.macd_signal_period,
                );
            });
            style_editor(ui, language, &mut settings.macd, true);
        }
        IndicatorKind::Atr => {
            period_editor(ui, language, "ATR", &mut settings.atr_period);
            style_editor(ui, language, &mut settings.atr, false);
        }
    }
}

fn period_editor(ui: &mut egui::Ui, language: Language, name: &str, period: &mut u32) {
    ui.horizontal(|ui| {
        ui.add_space(2.0);
        ui.strong(name);
        ui.add_space(28.0);
        ui.add_sized(
            [96.0, 32.0],
            egui::DragValue::new(period).range(1..=100_000),
        );
        egui::ComboBox::from_id_salt(format!("{name}-source"))
            .width(105.0)
            .selected_text(label(language, "收盘价", "Close"))
            .show_ui(ui, |ui| {
                let _ = ui.selectable_label(true, label(language, "收盘价", "Close"));
            });
    });
}

fn drag_period(ui: &mut egui::Ui, language: Language, zh: &str, en: &str, period: &mut u32) {
    ui.label(label(language, zh, en));
    ui.add(egui::DragValue::new(period).range(1..=100_000));
}

fn style_editor(
    ui: &mut egui::Ui,
    language: Language,
    style: &mut IndicatorStyle,
    second_color: bool,
) {
    ui.add_space(14.0);
    ui.horizontal(|ui| {
        ui.checkbox(&mut style.enabled, label(language, "显示", "Visible"));
        ui.add_space(16.0);
        ui.colored_label(theme::TEXT_SECONDARY, label(language, "线型", "Line"));
        ui.label(RichText::new("━━━━").color(Color32::from_rgb(
            style.color[0],
            style.color[1],
            style.color[2],
        )));
        ui.add(
            egui::DragValue::new(&mut style.line_width_tenths)
                .range(5..=40)
                .custom_formatter(|value, _| format!("{:.1}px", value / 10.0)),
        );
        ui.color_edit_button_srgb(&mut style.color);
    });
    ui.add_space(10.0);
    egui::Grid::new("indicator-style-editor")
        .num_columns(2)
        .spacing([18.0, 12.0])
        .show(ui, |ui| {
            if second_color {
                ui.label(label(language, "辅助线颜色", "Secondary color"));
                ui.color_edit_button_srgb(&mut style.secondary_color);
                ui.end_row();
            }
        });
}

fn unavailable_indicator_category(ui: &mut egui::Ui, language: Language) {
    ui.allocate_ui_with_layout(
        egui::vec2(ui.available_width(), 400.0),
        egui::Layout::centered_and_justified(egui::Direction::TopDown),
        |ui| {
            ui.colored_label(
                theme::TEXT_SECONDARY,
                label(
                    language,
                    "该指标分类将在对应计算能力接入后开放",
                    "This category becomes available with its calculation engine",
                ),
            );
        },
    );
}

fn general_settings(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    reconnect: &mut bool,
    language: Language,
) {
    ui.set_max_width(620.0);
    ui.label(text(language, TextKey::Language));
    egui::ComboBox::from_id_salt("venueflow-language")
        .selected_text(model.preferences.language.label())
        .show_ui(ui, |ui| {
            for option in Language::ALL {
                ui.selectable_value(&mut model.preferences.language, option, option.label());
            }
        });
    ui.add_space(12.0);
    ui.label(text(language, TextKey::ControlUrl));
    ui.text_edit_singleline(&mut model.preferences.endpoint);
    ui.small(text(language, TextKey::WebSameOrigin));
    if ui.button(text(language, TextKey::Reconnect)).clicked() {
        *reconnect = true;
    }
    ui.add_space(12.0);
    ui.label(text(language, TextKey::LocalSymbol));
    if ui
        .text_edit_singleline(&mut model.preferences.selected_symbol)
        .changed()
    {
        model.follow_latest_requested = true;
    }
    ui.add(
        egui::Slider::new(&mut model.preferences.ui_scale, 0.85..=1.35)
            .text(text(language, TextKey::UiScale)),
    );
    ui.checkbox(
        &mut model.preferences.show_status_bar,
        text(language, TextKey::ShowStatus),
    );
}

fn save_chart_settings(state: &mut SettingsPanelState, model: &mut AppModel) -> bool {
    let Some(draft) = state.draft.clone() else {
        return false;
    };
    if let Err(error) = draft.validate() {
        state.error = Some(error.to_owned());
        return false;
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Err(error) = model
        .local_markets
        .reconfigure_studies(draft.engine_config())
    {
        state.error = Some(format!("指标重算失败：{error}"));
        return false;
    }
    model.preferences.chart = draft;
    state.error = None;
    true
}

const fn label<'a>(language: Language, chinese: &'a str, english: &'a str) -> &'a str {
    match language {
        Language::SimplifiedChinese => chinese,
        Language::English => english,
    }
}
