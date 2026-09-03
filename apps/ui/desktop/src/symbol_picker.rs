use eframe::egui::{self, RichText, Stroke};

use crate::{
    i18n::{TextKey, text},
    model::{AppModel, SymbolGroup, format_decimal},
    theme,
    ui::{available_symbols, favorite_rank, local_quote},
    workspace::Workspaces,
};

#[cfg(test)]
mod tests;

pub(crate) fn show(
    anchor: &egui::Response,
    open: &mut bool,
    model: &mut AppModel,
    workspaces: &mut Workspaces,
) {
    if !*open {
        return;
    }
    if anchor
        .ctx
        .input(|input| input.key_pressed(egui::Key::Escape))
    {
        *open = false;
        return;
    }
    let language = model.preferences.language;
    let mut symbols = available_symbols(model);
    symbols.extend(model.preferences.favorite_symbols.iter().cloned());
    symbols.sort_by(|left, right| {
        favorite_rank(&model.preferences.favorite_symbols, left)
            .cmp(&favorite_rank(&model.preferences.favorite_symbols, right))
            .then_with(|| left.cmp(right))
    });
    symbols.dedup();
    let normalized_filter = model
        .symbol_filter
        .trim()
        .to_ascii_uppercase()
        .replace(['/', '-', '_'], "");
    let filtered = symbols
        .into_iter()
        .filter(|symbol| {
            let group_match = match model.symbol_group {
                SymbolGroup::Favorites => model.preferences.favorite_symbols.contains(symbol),
                SymbolGroup::Usdc => symbol.ends_with("/USDC"),
                SymbolGroup::Usdt => symbol.ends_with("/USDT"),
                SymbolGroup::All => true,
            };
            let normalized_symbol = symbol.replace('/', "");
            group_match
                && (normalized_filter.is_empty() || normalized_symbol.contains(&normalized_filter))
        })
        .collect::<Vec<_>>();
    let mut selected = anchor
        .ctx
        .input(|input| input.key_pressed(egui::Key::Enter))
        .then(|| filtered.first().cloned())
        .flatten();
    egui::Popup::from_response(anchor)
        .open_bool(open)
        .close_behavior(egui::PopupCloseBehavior::CloseOnClickOutside)
        .align(egui::RectAlign::BOTTOM_START)
        .gap(4.0)
        .width(590.0)
        .frame(
            egui::Frame::new()
                .fill(theme::BG_SECONDARY)
                .stroke(Stroke::new(1.0, theme::DIVIDER)),
        )
        .show(|ui| {
            ui.set_min_width(560.0);
            ui.horizontal(|ui| {
                for (group, label) in [
                    (SymbolGroup::Favorites, "★ 收藏"),
                    (SymbolGroup::All, "全部"),
                    (SymbolGroup::Usdc, "USDC"),
                    (SymbolGroup::Usdt, "USDT"),
                ] {
                    ui.selectable_value(&mut model.symbol_group, group, label);
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small(format!("{} markets", filtered.len()));
                });
            });
            ui.separator();
            ui.horizontal(|ui| {
                ui.strong("★");
                ui.add_sized(
                    [118.0, 24.0],
                    egui::Label::new(text(language, TextKey::Symbol)),
                );
                ui.add_sized(
                    [130.0, 24.0],
                    egui::Label::new(text(language, TextKey::Last)),
                );
                ui.add_sized([100.0, 24.0], egui::Label::new("24h %"));
                ui.add_sized([170.0, 24.0], egui::Label::new("24h Quote Volume"));
            });
            // Keep the market viewport stable when a narrow group (especially Favorites)
            // is selected. Include the filter result size in the scroll id so removing a
            // favorite or changing groups cannot leave the new list scrolled past its end.
            let group_key = match model.symbol_group {
                SymbolGroup::Favorites => 0_u8,
                SymbolGroup::All => 1,
                SymbolGroup::Usdc => 2,
                SymbolGroup::Usdt => 3,
            };
            ui.allocate_ui_with_layout(
                egui::vec2(ui.available_width(), 488.0),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    egui::ScrollArea::vertical()
                        .id_salt((
                            "symbol-picker-markets",
                            group_key,
                            normalized_filter.as_str(),
                            filtered.len(),
                        ))
                        .max_height(488.0)
                        .show_rows(ui, 32.0, filtered.len(), |ui, rows| {
                            ui.spacing_mut().item_spacing.y = 0.0;
                            for index in rows {
                                let symbol = &filtered[index];
                                let favorite = model.preferences.favorite_symbols.contains(symbol);
                                let is_selected = model.preferences.selected_symbol == *symbol;
                                let row_width = ui.available_width();
                                let row_top = ui.cursor().top();
                                let row_rect = egui::Rect::from_min_size(
                                    egui::pos2(ui.min_rect().left(), row_top),
                                    egui::vec2(row_width, 32.0),
                                );
                                if is_selected {
                                    ui.painter().rect_filled(
                                        row_rect,
                                        3.0,
                                        theme::BRAND.gamma_multiply(0.12),
                                    );
                                    ui.painter().rect_stroke(
                                        row_rect,
                                        3.0,
                                        Stroke::new(1.0, theme::BRAND),
                                        egui::StrokeKind::Inside,
                                    );
                                } else if index % 2 == 1 {
                                    ui.painter().rect_filled(
                                        row_rect,
                                        0.0,
                                        theme::BG_PRIMARY.gamma_multiply(0.25),
                                    );
                                }
                                let row = ui.allocate_ui_with_layout(
                                    egui::vec2(row_width, 32.0),
                                    egui::Layout::left_to_right(egui::Align::Center),
                                    |ui| {
                                        if ui
                                            .add_sized(
                                                [32.0, 28.0],
                                                egui::Button::new(if favorite {
                                                    "★"
                                                } else {
                                                    "☆"
                                                }),
                                            )
                                            .clicked()
                                        {
                                            if favorite {
                                                model
                                                    .preferences
                                                    .favorite_symbols
                                                    .retain(|item| item != symbol);
                                            } else {
                                                model
                                                    .preferences
                                                    .favorite_symbols
                                                    .push(symbol.clone());
                                            }
                                        }
                                        ui.add_sized(
                                            [118.0, 28.0],
                                            egui::Label::new(RichText::new(symbol).strong().color(
                                                if is_selected {
                                                    theme::BRAND_HOVER
                                                } else {
                                                    theme::TEXT_PRIMARY
                                                },
                                            )),
                                        );
                                        if let Some(quote) = local_quote(model, symbol) {
                                            ui.add_sized(
                                                [130.0, 28.0],
                                                egui::Label::new(
                                                    RichText::new(
                                                        model.format_market_price(
                                                            symbol, quote.last,
                                                        ),
                                                    )
                                                    .monospace(),
                                                ),
                                            );
                                            let change_color = if quote.change_percent_24h
                                                >= rust_decimal::Decimal::ZERO
                                            {
                                                theme::BUY
                                            } else {
                                                theme::SELL
                                            };
                                            ui.add_sized(
                                                [100.0, 28.0],
                                                egui::Label::new(
                                                    RichText::new(format!(
                                                        "{:+.2}%",
                                                        quote.change_percent_24h
                                                    ))
                                                    .color(change_color)
                                                    .monospace(),
                                                ),
                                            );
                                            ui.add_sized(
                                                [170.0, 28.0],
                                                egui::Label::new(
                                                    RichText::new(format_decimal(
                                                        quote.quote_volume_24h,
                                                        0,
                                                    ))
                                                    .monospace(),
                                                ),
                                            );
                                        } else {
                                            for width in [130.0, 100.0, 170.0] {
                                                ui.add_sized([width, 28.0], egui::Label::new("—"));
                                            }
                                        }
                                    },
                                );
                                // The child UI's content bounds shrink around its labels. Hit-test
                                // the allocated row width, retaining the star as a separate action.
                                let mut hit_rect = egui::Rect::from_min_size(
                                    egui::pos2(row.response.rect.left(), row_top),
                                    egui::vec2(row_width, row.response.rect.height().max(32.0)),
                                );
                                hit_rect.min.x += 32.0;
                                let row_response = ui.interact(
                                    hit_rect,
                                    ui.make_persistent_id(("symbol-row", symbol)),
                                    egui::Sense::click(),
                                );
                                if row_response.clicked() {
                                    selected = Some(symbol.clone());
                                }
                            }
                        })
                },
            );
        });
    if let Some(symbol) = selected {
        model.select_symbol(symbol);
        model.symbol_filter.clear();
        workspaces.follow_dynamic_charts_latest();
        *open = false;
    }
}
