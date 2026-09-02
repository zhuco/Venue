use super::{
    CustomSettings,
    text::{Key, SIGNAL_KEYS, text},
};
use crate::{i18n::Language, theme};
use eframe::egui::{self, RichText};
use rust_decimal::Decimal;

pub(crate) fn settings_ui(ui: &mut egui::Ui, settings: &mut CustomSettings, language: Language) {
    egui::ScrollArea::vertical()
        .id_salt("custom-indicator-settings")
        .max_height(405.0)
        .show(ui, |ui| {
            ui.set_min_height(395.0);
            ui.horizontal(|ui| {
                ui.heading(text(language, Key::Title));
                ui.checkbox(&mut settings.enabled, text(language, Key::Enabled));
            });
            ui.label(
                RichText::new(text(language, Key::Implementation)).color(theme::TEXT_SECONDARY),
            );
            ui.label(text(language, Key::Boundary));
            ui.label(RichText::new(text(language, Key::Compatibility)).color(theme::WARNING));
            if cfg!(target_arch = "wasm32") {
                ui.label(text(language, Key::WebUnavailable));
            }
            ui.separator();
            ui.strong(text(language, Key::Trend));
            egui::Grid::new("custom-ema-adx-parameters")
                .min_col_width(160.0)
                .spacing([18.0, 7.0])
                .show(ui, |ui| {
                    let p = &mut settings.parameters;
                    period(ui, language, Key::Fast, &mut p.ema_periods[0]);
                    period(ui, language, Key::Mid, &mut p.ema_periods[1]);
                    period(ui, language, Key::Slow, &mut p.ema_periods[2]);
                    period(ui, language, Key::Di, &mut p.di_period);
                    period(ui, language, Key::Adx, &mut p.adx_period);
                    decimal(ui, language, Key::Minimum, &mut p.adx_min, 100.0);
                    period(ui, language, Key::MacdFast, &mut p.macd_periods[0]);
                    period(ui, language, Key::MacdSlow, &mut p.macd_periods[1]);
                    period(ui, language, Key::MacdSignal, &mut p.macd_periods[2]);
                    period(ui, language, Key::Atr, &mut p.atr_period);
                    ui.strong(text(language, Key::Filters));
                    ui.end_row();
                    decimal(ui, language, Key::Range, &mut p.min_range_atr, 1000.0);
                    decimal(ui, language, Key::Histogram, &mut p.macd_atr, 1000.0);
                    decimal(ui, language, Key::Exit, &mut p.exit_buffer_atr, 1000.0);
                    decimal(
                        ui,
                        language,
                        Key::Breakout,
                        &mut p.breakout_buffer_atr,
                        1000.0,
                    );
                    period(ui, language, Key::Lookback, &mut p.breakout_lookback);
                    decimal(ui, language, Key::Distance, &mut p.cooldown_atr, 1000.0);
                    ui.label(text(language, Key::Bars));
                    ui.add(egui::DragValue::new(&mut p.min_bars_between).range(0..=10_000));
                    ui.end_row();
                    ui.label(text(language, Key::VolumeFilter));
                    ui.checkbox(&mut p.volume_filter, "");
                    ui.end_row();
                    period(ui, language, Key::VolumePeriod, &mut p.volume_period);
                    decimal(
                        ui,
                        language,
                        Key::VolumeMultiple,
                        &mut p.volume_multiplier,
                        1000.0,
                    );
                });
            ui.separator();
            ui.strong(text(language, Key::Display));
            for (i, key) in [Key::BullColor, Key::BearColor, Key::NeutralColor]
                .into_iter()
                .enumerate()
            {
                ui.horizontal(|ui| {
                    ui.label(text(language, key));
                    ui.color_edit_button_srgb(&mut settings.colors[i]);
                    ui.add(
                        egui::DragValue::new(&mut settings.line_widths[i])
                            .range(1..=4)
                            .suffix(" px"),
                    );
                });
            }
            ui.horizontal_wrapped(|ui| {
                for (enabled, key) in settings.signals.iter_mut().zip(SIGNAL_KEYS) {
                    ui.checkbox(enabled, text(language, key));
                }
            });
            ui.checkbox(
                &mut settings.confirmed_labels_only,
                text(language, Key::ConfirmedOnly),
            );
            if ui.button(text(language, Key::Restore)).clicked() {
                *settings = CustomSettings::default();
            }
            ui.collapsing(text(language, Key::Source), |ui| {
                let mut source = include_str!("ema_adx_source.txt");
                ui.add(
                    egui::TextEdit::multiline(&mut source)
                        .font(egui::TextStyle::Monospace)
                        .desired_width(f32::INFINITY),
                );
            });
        });
}

fn period(ui: &mut egui::Ui, language: Language, key: Key, value: &mut usize) {
    ui.label(text(language, key));
    ui.add(egui::DragValue::new(value).range(1..=10_000));
    ui.end_row();
}

fn decimal(ui: &mut egui::Ui, language: Language, key: Key, value: &mut Decimal, max: f64) {
    ui.label(text(language, key));
    let mut numeric = crate::model::decimal_to_f64(*value);
    if ui
        .add(
            egui::DragValue::new(&mut numeric)
                .range(0.0..=max)
                .speed(0.01)
                .max_decimals(3),
        )
        .changed()
        && let Ok(parsed) = format!("{numeric:.3}").parse::<Decimal>()
    {
        *value = parsed;
    }
    ui.end_row();
}
