use super::{
    CustomLibrary, Entry, EntryKind,
    library::{INTRADAY_SOURCE, STARTER_SOURCE},
};
use crate::{chart_settings::ChartDisplaySettings, i18n::Language};
use eframe::egui;
#[cfg(test)]
#[path = "library_ui_tests.rs"]
mod tests;

#[derive(Clone, Debug, Default)]
pub struct LibraryEditor {
    pub draft: Option<Entry>,
    error: Option<String>,
    deleting: Option<u64>,
}
impl LibraryEditor {
    pub fn pending(&self) -> bool {
        self.draft.is_some()
    }
}
pub(crate) fn settings_ui(
    ui: &mut egui::Ui,
    settings: &mut ChartDisplaySettings,
    editor: &mut LibraryEditor,
    language: Language,
) {
    let chinese = language == Language::SimplifiedChinese;
    let library = settings
        .custom_library
        .get_or_insert_with(|| CustomLibrary::migrate(&settings.custom_ema_adx));
    ui.set_min_height(390.0);
    ui.label(if chinese {
        "自定义指标 · 最多保存16项 / 同时启用4项"
    } else {
        "Custom indicators · 16 saved / 4 active"
    });
    if cfg!(target_arch = "wasm32") {
        ui.label(if chinese {
            "浏览器预览不计算本地自定义指标，请使用桌面端。"
        } else {
            "Local custom studies require the desktop application."
        });
    }
    if let Some(draft) = &mut editor.draft {
        ui.horizontal(|ui| {
            ui.label(if chinese { "名称" } else { "Name" });
            ui.text_edit_singleline(&mut draft.name);
            ui.checkbox(&mut draft.enabled, if chinese { "启用" } else { "Enabled" });
        });
        match &mut draft.kind {
            EntryKind::Script { source } => {
                ui.label(if chinese {
                    "编辑源码，校验通过后保存并重新计算。取消保留原版本。"
                } else {
                    "Edit source; save validates and recalculates. Cancel keeps the original."
                });
                egui::ScrollArea::both()
                    .id_salt("custom-source-editor")
                    .max_height(240.0)
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(source)
                                .font(egui::TextStyle::Monospace)
                                .code_editor()
                                .desired_width(670.0)
                                .desired_rows(14)
                                .char_limit(32_768),
                        );
                    });
                ui.collapsing(if chinese{"语法与兼容说明"}else{"Syntax and compatibility"},|ui|{
                    ui.label(venue_indicators::chart::script::COMPATIBILITY);
                    ui.label("input, ema, sma, rsi, macd, highest, lowest, ref / [n], valuewhen, td, ichimoku, abs, min, max, plot, fill, plotText, alertcondition; + - * /, comparisons, && || not, ?:.");
                    ui.label(if chinese{"无循环、可变赋值或前向引用。缺失值/除零形成断线。信号只在已收盘K线上显示。"}else{"No loops, reassignment or forward references. Missing values/division by zero create gaps. Labels use closed bars only."});
                });
            }
            EntryKind::Legacy(s) => {
                ui.label(if chinese{"原版是已接入的原生模板，可编辑参数和样式；新建指标支持源码编辑。"}else{"Legacy native template: edit parameters/styles. New entries support source editing."});
                super::ui::settings_ui(ui, s, language);
            }
        }
        let (mut save, mut cancel, mut validate) = (false, false, false);
        ui.horizontal(|ui| {
            validate = ui
                .button(if chinese { "校验源码" } else { "Validate" })
                .clicked();
            save = ui
                .button(if chinese {
                    "保存指标"
                } else {
                    "Save indicator"
                })
                .clicked();
            cancel = ui
                .button(if chinese {
                    "取消编辑"
                } else {
                    "Cancel edit"
                })
                .clicked();
        });
        if validate || save {
            let mut candidate = library.clone();
            match candidate.save(draft.clone()) {
                Ok(_) => {
                    editor.error = Some(
                        if chinese {
                            "校验通过"
                        } else {
                            "Validation passed"
                        }
                        .into(),
                    );
                    if save {
                        *library = candidate;
                        editor.draft = None;
                        editor.error = None;
                    }
                }
                Err(error) => editor.error = Some(error),
            }
        }
        if cancel {
            editor.draft = None;
            editor.error = None;
        }
    } else {
        ui.horizontal(|ui| {
            if ui
                .add_enabled(
                    library.entries.len() < 16,
                    egui::Button::new(if chinese {
                        "＋ 新建指标"
                    } else {
                        "＋ New indicator"
                    }),
                )
                .clicked()
            {
                editor.draft = Some(Entry {
                    id: 0,
                    name: if chinese {
                        "新指标"
                    } else {
                        "New indicator"
                    }
                    .into(),
                    enabled: true,
                    kind: EntryKind::Script {
                        source: STARTER_SOURCE.into(),
                    },
                });
                editor.error = None;
            }
            if ui
                .add_enabled(
                    library.entries.len() < 16,
                    egui::Button::new(if chinese {
                        "添加日内共振模板"
                    } else {
                        "Add intraday template"
                    }),
                )
                .clicked()
            {
                editor.draft = Some(Entry {
                    id: 0,
                    name: "日内敏捷共振 · TD / 高量K线 / 趋势隧道".into(),
                    enabled: true,
                    kind: EntryKind::Script {
                        source: INTRADAY_SOURCE.into(),
                    },
                });
                editor.error = None;
            }
        });
        egui::ScrollArea::vertical()
            .id_salt("custom-indicator-list")
            .max_height(280.0)
            .show(ui, |ui| {
                if library.entries.is_empty() {
                    ui.label(if chinese {
                        "暂无自定义指标，点击新建。"
                    } else {
                        "No custom indicators. Create one above."
                    });
                }
                let active = library.entries.iter().filter(|e| e.enabled).count();
                for entry in &mut library.entries {
                    ui.push_id(entry.id, |ui| {
                        ui.horizontal(|ui| {
                            ui.add_enabled_ui(entry.enabled || active < 4, |ui| {
                                ui.checkbox(
                                    &mut entry.enabled,
                                    if chinese { "启用" } else { "Enabled" },
                                );
                            });
                            ui.add_sized([410.0, 24.0], egui::Label::new(&entry.name).truncate());
                            if ui.button(if chinese { "编辑" } else { "Edit" }).clicked() {
                                editor.draft = Some(entry.clone());
                                editor.error = None;
                            }
                            if ui.button(if chinese { "删除" } else { "Delete" }).clicked() {
                                editor.deleting = Some(entry.id);
                            }
                        });
                    });
                }
            });
        if let Some(id) = editor.deleting {
            ui.horizontal(|ui| {
                ui.label(if chinese {
                    "确认删除此指标？"
                } else {
                    "Delete this indicator?"
                });
                if ui
                    .button(if chinese {
                        "确认删除"
                    } else {
                        "Confirm delete"
                    })
                    .clicked()
                {
                    library.remove(id);
                    editor.deleting = None;
                }
                if ui.button(if chinese { "取消" } else { "Cancel" }).clicked() {
                    editor.deleting = None;
                }
            });
        }
        ui.label(if chinese{"日内模板已更新为所附源码；勾选启用即可显示。删除仅影响当前图表配置。"}else{"Intraday template uses the supplied source. Enable it to display. Deletion affects this chart configuration."});
        ui.label(if chinese{"本地兼容计算：TD13 不是认证倒计数；POC 为高量K线参考位。"}else{"Local compatibility: TD13 is not certified Countdown; POC references high-volume candles."});
    }
    if let Some(error) = &editor.error {
        ui.colored_label(egui::Color32::YELLOW, error);
    }
}
