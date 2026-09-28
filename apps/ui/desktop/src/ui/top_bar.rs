use super::*;

fn label(language: Language, zh: &'static str, en: &'static str) -> &'static str {
    match language {
        Language::SimplifiedChinese => zh,
        Language::English => en,
    }
}

pub fn show(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    workspaces: &mut Workspaces,
    show_modules: &mut bool,
    show_trading_settings: &mut bool,
    show_execution_account: &mut bool,
    show_symbol_picker: &mut bool,
) {
    egui::Frame::new()
        .fill(theme::BG_SECONDARY)
        .inner_margin(egui::Margin::symmetric(10, 2))
        .show(ui, |ui| {
            let language = model.preferences.language;
            let picker_requested = std::cell::Cell::new(*show_symbol_picker);
            let mut filter = model.symbol_filter.clone();
            let (row, _) = ui
                .allocate_exact_size(egui::vec2(ui.available_width(), 40.0), egui::Sense::hover());
            // Equal side reservations keep search centered regardless of account name or language.
            let compact = row.width() < 800.0;
            let search_width =
                (row.width() - if compact { 320.0 } else { 600.0 }).clamp(120.0, 320.0);
            let search_rect =
                egui::Rect::from_center_size(row.center(), egui::vec2(search_width, 30.0));
            let left = egui::Rect::from_min_max(
                row.min,
                egui::pos2(search_rect.left() - 12.0, row.bottom()),
            );
            let right = egui::Rect::from_min_max(
                egui::pos2(search_rect.right() + 12.0, row.top()),
                row.max,
            );
            ui.scope_builder(
                egui::UiBuilder::new()
                    .max_rect(left)
                    .layout(egui::Layout::left_to_right(egui::Align::Center)),
                |ui| {
                    crate::branding::show_mark(ui);
                    if row.width() >= 580.0 {
                        ui.label(
                            RichText::new("VenueFlow")
                                .strong()
                                .size(15.0)
                                .color(theme::TEXT_PRIMARY),
                        )
                        .on_hover_text(format!(
                            "VenueFlow {}",
                            include_str!("../../../../../VERSION").trim()
                        ));
                    }
                    let drag = ui.allocate_response(
                        egui::vec2(ui.available_width().max(0.0), 30.0),
                        egui::Sense::click_and_drag(),
                    );
                    #[cfg(not(target_arch = "wasm32"))]
                    if drag.double_clicked() {
                        toggle_maximized(ui.ctx());
                    } else if drag.drag_started() {
                        ui.ctx().send_viewport_cmd(egui::ViewportCommand::StartDrag);
                    }
                    #[cfg(target_arch = "wasm32")]
                    let _ = drag;
                },
            );
            ui.scope_builder(
                egui::UiBuilder::new()
                    .max_rect(right)
                    .layout(egui::Layout::right_to_left(egui::Align::Center)),
                |ui| {
                    ui.spacing_mut().item_spacing.x = 4.0;
                    ui.visuals_mut().widgets.inactive.bg_stroke = Stroke::NONE;
                    #[cfg(not(target_arch = "wasm32"))]
                    window_controls(ui);
                    if compact {
                        ui.menu_button(label(language, "设置", "Settings"), |ui| {
                            if ui
                                .button(text(language, TextKey::TradingSettings))
                                .clicked()
                            {
                                *show_trading_settings = true;
                                ui.close();
                            }
                            if ui
                                .button(label(language, "界面与连接", "Appearance and connection"))
                                .clicked()
                            {
                                model.general_settings_requested = true;
                                ui.close();
                            }
                        });
                    } else {
                        if ui
                            .button(label(language, "设置", "Settings"))
                            .on_hover_text(label(
                                language,
                                "外观、语言与连接",
                                "Appearance, language and connection",
                            ))
                            .clicked()
                        {
                            model.general_settings_requested = true;
                        }
                        if ui
                            .button(text(language, TextKey::TradingSettings))
                            .clicked()
                        {
                            *show_trading_settings = true;
                        }
                    }
                },
            );
            let search_response = ui
                .scope_builder(
                    egui::UiBuilder::new()
                        .max_rect(search_rect)
                        .layout(egui::Layout::left_to_right(egui::Align::Center)),
                    |ui| {
                        ui.add_sized(
                            search_rect.size(),
                            egui::TextEdit::singleline(&mut filter)
                                .id_salt("top-market-search")
                                .hint_text(label(language, "搜索交易对", "Search markets"))
                                .margin(egui::vec2(12.0, 6.0))
                                .vertical_align(egui::Align::Center),
                        )
                    },
                )
                .inner;
            if search_response.gained_focus()
                || search_response.clicked()
                || filter != model.symbol_filter
            {
                picker_requested.set(true);
            }
            model.symbol_filter = filter;
            ui.separator();
            let (row, _) = ui
                .allocate_exact_size(egui::vec2(ui.available_width(), 52.0), egui::Sense::hover());
            let split = row.right() - row.width().min(if compact { 360.0 } else { 430.0 });
            let tabs_rect = egui::Rect::from_min_max(
                row.min,
                egui::pos2((split - 8.0).max(row.left()), row.bottom()),
            );
            let controls_rect = egui::Rect::from_min_max(egui::pos2(split, row.top()), row.max);
            ui.scope_builder(egui::UiBuilder::new().max_rect(tabs_rect), |ui| {
                egui::ScrollArea::horizontal()
                    .id_salt("favorite-symbol-tabs")
                    .auto_shrink([false, true])
                    .max_height(48.0)
                    .min_scrolled_height(48.0)
                    .scroll_bar_visibility(egui::scroll_area::ScrollBarVisibility::AlwaysHidden)
                    .show(ui, |ui| {
                        ui.horizontal_centered(|ui| {
                            show_symbol_tabs(ui, model, workspaces, &picker_requested);
                        });
                    });
            });
            ui.scope_builder(
                egui::UiBuilder::new()
                    .max_rect(controls_rect)
                    .layout(egui::Layout::right_to_left(egui::Align::Center)),
                |ui| {
                    ui.spacing_mut().item_spacing.x = 8.0;
                    layout_menu(ui, language, workspaces, show_modules);
                    account_menu(ui, model, show_execution_account, compact);
                    market_menu(ui, model, compact);
                },
            );
            *show_symbol_picker = picker_requested.get();
            let mut anchor = search_response;
            let bottom = ui.min_rect().bottom();
            anchor.rect.set_bottom(bottom);
            anchor.rect.set_top(bottom - 1.0);
            crate::symbol_picker::show(&anchor, show_symbol_picker, model, workspaces);
        });
}

fn control_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(theme::PANEL)
        .corner_radius(6)
        .inner_margin(egui::Margin::symmetric(8, 3))
}

fn market_menu(ui: &mut egui::Ui, model: &mut AppModel, compact: bool) {
    let language = model.preferences.language;
    let mut server = model.preferences.market_server;
    control_frame().show(ui, |ui| {
        ui.set_width(if compact { 96.0 } else { 112.0 });
        ui.with_layout(egui::Layout::top_down(egui::Align::Min), |ui| {
            ui.label(
                RichText::new(label(language, "行情源", "MARKET DATA"))
                    .size(10.0)
                    .color(theme::TEXT_SECONDARY),
            );
            egui::ComboBox::from_id_salt("market-server")
                .width(if compact { 80.0 } else { 96.0 })
                .truncate()
                .selected_text(server.label())
                .show_ui(ui, |ui| {
                    for value in crate::model::MarketServer::ALL {
                        if cfg!(all(target_arch = "wasm32", feature = "preview"))
                            && value != crate::model::MarketServer::Binance
                        {
                            continue;
                        }
                        ui.selectable_value(&mut server, value, value.label());
                    }
                });
        });
    });
    if server != model.preferences.market_server {
        model.select_market_server(server);
    }
}

fn account_menu(ui: &mut egui::Ui, model: &mut AppModel, open: &mut bool, compact: bool) {
    let language = model.preferences.language;
    let name = model
        .selected_execution_credential()
        .map(|c| c.label.as_str())
        .unwrap_or(label(language, "选择账户", "Select account"));
    let mut selected = None;
    control_frame().show(ui, |ui| {
        ui.set_width(if compact { 128.0 } else { 156.0 });
        ui.with_layout(egui::Layout::top_down(egui::Align::Min), |ui| {
            ui.label(
                RichText::new(label(language, "执行账户", "EXECUTION ACCOUNT"))
                    .size(10.0)
                    .color(theme::TEXT_SECONDARY),
            );
            egui::ComboBox::from_id_salt("execution-account-selection")
                .width(if compact { 112.0 } else { 140.0 })
                .truncate()
                .selected_text(name)
                .show_ui(ui, |ui| {
                    ui.set_min_width(230.0);
                    if let Some(overview) = &model.account_overview {
                        ui.weak(&overview.user.username);
                        ui.separator();
                        for credential in &overview.credentials {
                            if ui
                                .add_enabled(
                                    credential.selectable(crate::account_center::now_ms()),
                                    egui::Button::selectable(
                                        overview.selected_credential_id.as_deref()
                                            == Some(credential.credential_id.as_str()),
                                        format!("{} · {}", credential.label, credential.masked_key),
                                    ),
                                )
                                .clicked()
                            {
                                selected = Some(credential.credential_id.clone());
                                ui.close();
                            }
                        }
                        ui.separator();
                    }
                    if ui
                        .add_enabled(
                            !cfg!(all(target_arch = "wasm32", feature = "preview")),
                            egui::Button::new(label(language, "管理账户…", "Manage accounts…")),
                        )
                        .clicked()
                    {
                        *open = true;
                        ui.close();
                    }
                });
        });
    });
    if let Some(id) = selected {
        model.begin_account_selection(id);
    }
}

fn layout_menu(
    ui: &mut egui::Ui,
    language: Language,
    workspaces: &mut Workspaces,
    modules: &mut bool,
) {
    ui.menu_button(label(language, "布局管理", "Layout"), |ui| {
        for workspace in WorkspaceKind::ALL {
            ui.selectable_value(&mut workspaces.active, workspace, workspace.label(language));
        }
        ui.separator();
        if ui
            .button(text(language, TextKey::WorkspaceModules))
            .clicked()
        {
            *modules = true;
            ui.close();
        }
        if ui.button(text(language, TextKey::ResetLayout)).clicked() {
            workspaces.restore_active();
            ui.close();
        }
    });
}
