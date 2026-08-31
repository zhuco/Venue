use eframe::egui::{self, Align2, Color32, RichText, Stroke};

use crate::{
    chart_settings::{ChartDisplaySettings, IndicatorStyle},
    i18n::{IndicatorTextKey, Language, TextKey, indicator_text, text},
    model::AppModel,
    theme,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum SettingsTab {
    #[default]
    Main,
    Sub,
    Custom,
    Backtest,
    General,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum IndicatorKind {
    Ma,
    Ema,
    Wma,
    Bollinger,
    Vwap,
    Avl,
    Trix,
    Sar,
    #[default]
    Supertrend,
    Volume,
    Macd,
    Rsi,
    Mfi,
    Kdj,
    Obv,
    Cci,
    StochRsi,
    WilliamsR,
    Dmi,
    Momentum,
    Emv,
    Atr,
}

const MAIN_INDICATORS: &[(IndicatorKind, &str)] = &[
    (IndicatorKind::Ma, "MA"),
    (IndicatorKind::Ema, "EMA"),
    (IndicatorKind::Wma, "WMA"),
    (IndicatorKind::Bollinger, "BOLL"),
    (IndicatorKind::Vwap, "VWAP"),
    (IndicatorKind::Avl, "AVL"),
    (IndicatorKind::Trix, "TRIX"),
    (IndicatorKind::Sar, "SAR"),
    (IndicatorKind::Supertrend, "SUPER"),
];

const SUB_INDICATORS: &[(IndicatorKind, &str)] = &[
    (IndicatorKind::Volume, "VOL"),
    (IndicatorKind::Macd, "MACD"),
    (IndicatorKind::Rsi, "RSI"),
    (IndicatorKind::Mfi, "MFI"),
    (IndicatorKind::Kdj, "KDJ"),
    (IndicatorKind::Obv, "OBV"),
    (IndicatorKind::Cci, "CCI"),
    (IndicatorKind::StochRsi, "StochRSI"),
    (IndicatorKind::WilliamsR, "WR"),
    (IndicatorKind::Dmi, "DMI"),
    (IndicatorKind::Momentum, "MTM"),
    (IndicatorKind::Emv, "EMV"),
    (IndicatorKind::Atr, "ATR"),
];

#[derive(Clone, Debug, Default)]
pub struct SettingsPanelState {
    tab: SettingsTab,
    indicator: IndicatorKind,
    draft: Option<ChartDisplaySettings>,
    original: Option<ChartDisplaySettings>,
    error: Option<String>,
}

impl SettingsPanelState {
    pub fn focus_indicators(&mut self) {
        self.tab = SettingsTab::Main;
        self.indicator = IndicatorKind::Supertrend;
    }

    fn clear(&mut self) {
        self.draft = None;
        self.original = None;
        self.error = None;
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
    state
        .original
        .get_or_insert_with(|| model.preferences.chart.clone());
    state
        .draft
        .get_or_insert_with(|| model.preferences.chart.clone());
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
        .fixed_size(egui::vec2(720.0, 535.0))
        .frame(
            egui::Frame::new()
                .fill(Color32::from_rgb(31, 38, 50))
                .stroke(Stroke::new(1.0, Color32::from_rgb(54, 64, 79)))
                .inner_margin(egui::Margin::ZERO)
                .corner_radius(egui::CornerRadius::same(8)),
        )
        .show(context, |ui| {
            ui.set_min_size(egui::vec2(720.0, 535.0));
            top_tabs(ui, state, language, &mut close_requested);
            ui.separator();
            match state.tab {
                SettingsTab::Main | SettingsTab::Sub => indicator_body(ui, state, language),
                SettingsTab::Custom | SettingsTab::Backtest => placeholder(ui, language),
                SettingsTab::General => general_settings(ui, model, reconnect, language),
            }
            ui.separator();
            bottom_actions(ui, state, language, &mut saved, &mut close_requested);
        });

    if let Some(draft) = state.draft.clone() {
        match apply_chart_settings(&draft, model, language) {
            Ok(()) => state.error = None,
            Err(error) => state.error = Some(error),
        }
    }
    if close_requested {
        window_open = false;
    }
    if !window_open {
        if !saved && let Some(original) = state.original.clone() {
            let _ = apply_chart_settings(&original, model, language);
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
    ui.allocate_ui_with_layout(
        egui::vec2(ui.available_width(), 58.0),
        egui::Layout::left_to_right(egui::Align::Center),
        |ui| {
            ui.add_space(18.0);
            for (tab, key) in [
                (SettingsTab::Main, IndicatorTextKey::MainTab),
                (SettingsTab::Sub, IndicatorTextKey::SubTab),
                (SettingsTab::Custom, IndicatorTextKey::CustomTab),
                (SettingsTab::Backtest, IndicatorTextKey::BacktestTab),
                (SettingsTab::General, IndicatorTextKey::GeneralTab),
            ] {
                tab_button(ui, &mut state.tab, tab, indicator_text(language, key));
                ui.add_space(16.0);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add(egui::Button::new(RichText::new("×").size(28.0)).frame(false))
                    .clicked()
                {
                    *close = true;
                }
            });
        },
    );
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
    let list = if state.tab == SettingsTab::Main {
        MAIN_INDICATORS
    } else {
        SUB_INDICATORS
    };
    if !list.iter().any(|(kind, _)| *kind == state.indicator) {
        state.indicator = list[0].0;
    }
    ui.horizontal(|ui| {
        ui.allocate_ui_with_layout(
            egui::vec2(150.0, 405.0),
            egui::Layout::top_down(egui::Align::Min),
            |ui| {
                ui.add_space(10.0);
                ui.label(
                    RichText::new(indicator_text(
                        language,
                        if state.tab == SettingsTab::Main {
                            IndicatorTextKey::MainGroup
                        } else {
                            IndicatorTextKey::SubGroup
                        },
                    ))
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
                        for (kind, name) in list {
                            indicator_list_row(
                                ui,
                                &mut state.indicator,
                                *kind,
                                name,
                                style_mut(draft, *kind),
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
                indicator_editor(ui, draft, state.indicator, language);
            },
        );
    });
}

fn indicator_list_row(
    ui: &mut egui::Ui,
    selected: &mut IndicatorKind,
    kind: IndicatorKind,
    name: &str,
    style: &mut IndicatorStyle,
) {
    let is_selected = *selected == kind;
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
            }
        });
    });
}

fn indicator_editor(
    ui: &mut egui::Ui,
    settings: &mut ChartDisplaySettings,
    kind: IndicatorKind,
    language: Language,
) {
    ui.label(
        RichText::new(indicator_text(language, indicator_title(kind)))
            .size(14.0)
            .strong(),
    );
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
        IndicatorKind::Atr => simple_period_style(
            ui,
            language,
            "ATR",
            &mut settings.atr_period,
            &mut settings.atr,
        ),
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
            ui.checkbox(
                &mut style.line_enabled[index],
                format!("{prefix}{}", index + 1),
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
        ui.label(RichText::new(name).size(13.0).strong());
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
        ui.add_sized(
            [120.0, 30.0],
            egui::Label::new(indicator_text(language, key)),
        );
        ui.add_sized(
            [108.0, 32.0],
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
    ui.horizontal(|ui| {
        ui.label(indicator_text(language, key_a));
        ui.add(egui::DragValue::new(a).range(1..=100_000));
        ui.add_space(20.0);
        ui.label(indicator_text(language, key_b));
        ui.add(egui::DragValue::new(b).range(1..=100_000));
    });
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
            ui.add_sized(
                [95.0, 28.0],
                egui::Label::new(indicator_text(language, key)),
            );
            ui.add_sized(
                [105.0, 30.0],
                egui::DragValue::new(value).range(1..=100_000),
            );
        });
        ui.add_space(6.0);
    }
}

fn single_style(ui: &mut egui::Ui, language: Language, style: &mut IndicatorStyle) {
    ui.horizontal(|ui| {
        ui.label(indicator_text(language, IndicatorTextKey::Line));
        line_sample(ui, &mut style.color, &mut style.line_width_tenths);
    });
}

fn secondary_style(ui: &mut egui::Ui, language: Language, style: &mut IndicatorStyle, title: &str) {
    single_style(ui, language, style);
    ui.horizontal(|ui| {
        ui.label(title);
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
            ui.add_sized(
                [120.0, 30.0],
                egui::Label::new(indicator_text(
                    language,
                    if rising {
                        IndicatorTextKey::RisingLine
                    } else {
                        IndicatorTextKey::FallingLine
                    },
                )),
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
        ui.label(indicator_text(language, IndicatorTextKey::FillOpacity));
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
                && state.error.is_none()
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
            ui.text_edit_singleline(&mut model.preferences.endpoint);
            if ui.button(text(language, TextKey::Reconnect)).clicked() {
                *reconnect = true;
            }
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
) -> Result<(), String> {
    settings.validate().map_err(str::to_owned)?;
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

const fn indicator_title(kind: IndicatorKind) -> IndicatorTextKey {
    match kind {
        IndicatorKind::Ma => IndicatorTextKey::MaTitle,
        IndicatorKind::Ema => IndicatorTextKey::EmaTitle,
        IndicatorKind::Wma => IndicatorTextKey::WmaTitle,
        IndicatorKind::Bollinger => IndicatorTextKey::BollTitle,
        IndicatorKind::Vwap => IndicatorTextKey::VwapTitle,
        IndicatorKind::Avl => IndicatorTextKey::AvlTitle,
        IndicatorKind::Trix => IndicatorTextKey::TrixTitle,
        IndicatorKind::Sar => IndicatorTextKey::SarTitle,
        IndicatorKind::Supertrend => IndicatorTextKey::SuperTitle,
        IndicatorKind::Volume => IndicatorTextKey::VolTitle,
        IndicatorKind::Macd => IndicatorTextKey::MacdTitle,
        IndicatorKind::Rsi => IndicatorTextKey::RsiTitle,
        IndicatorKind::Mfi => IndicatorTextKey::MfiTitle,
        IndicatorKind::Kdj => IndicatorTextKey::KdjTitle,
        IndicatorKind::Obv => IndicatorTextKey::ObvTitle,
        IndicatorKind::Cci => IndicatorTextKey::CciTitle,
        IndicatorKind::StochRsi => IndicatorTextKey::StochRsiTitle,
        IndicatorKind::WilliamsR => IndicatorTextKey::WilliamsRTitle,
        IndicatorKind::Dmi => IndicatorTextKey::DmiTitle,
        IndicatorKind::Momentum => IndicatorTextKey::MomentumTitle,
        IndicatorKind::Emv => IndicatorTextKey::EmvTitle,
        IndicatorKind::Atr => IndicatorTextKey::AtrTitle,
    }
}

#[cfg(test)]
mod tests {
    use super::{MAIN_INDICATORS, SUB_INDICATORS, SettingsTab};

    #[test]
    fn settings_expose_only_the_confirmed_main_and_sub_indicator_groups() {
        assert_eq!(MAIN_INDICATORS.len(), 9);
        assert_eq!(SUB_INDICATORS.len(), 13);
        assert_eq!(SettingsTab::default(), SettingsTab::Main);
        let names = MAIN_INDICATORS
            .iter()
            .chain(SUB_INDICATORS)
            .map(|(_, name)| *name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "MA", "EMA", "WMA", "BOLL", "VWAP", "AVL", "TRIX", "SAR", "SUPER", "VOL", "MACD",
                "RSI", "MFI", "KDJ", "OBV", "CCI", "StochRSI", "WR", "DMI", "MTM", "EMV", "ATR",
            ]
        );
    }
}
