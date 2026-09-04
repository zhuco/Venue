use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Stroke};

use super::{ChartTradingSettings, label};
use crate::{i18n::Language, theme};

const MENU_BG: Color32 = Color32::from_rgb(32, 39, 49);
const ROW_HEIGHT: f32 = 38.0;

pub(crate) fn menu_button(
    ui: &mut egui::Ui,
    settings: &mut ChartTradingSettings,
    language: Language,
) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(egui::vec2(26.0, 26.0), Sense::click());
    let color = if response.hovered() {
        theme::TEXT_PRIMARY
    } else {
        theme::TEXT_SECONDARY
    };
    let center = rect.center();
    let vertices = (0..6)
        .map(|i| {
            let angle = std::f32::consts::TAU * i as f32 / 6.0 - std::f32::consts::FRAC_PI_2;
            center + egui::vec2(angle.cos(), angle.sin()) * 6.5
        })
        .collect();
    ui.painter()
        .add(egui::Shape::closed_line(vertices, Stroke::new(1.2, color)));
    ui.painter()
        .circle_stroke(center, 2.0, Stroke::new(1.0, color));
    response.widget_info(|| {
        egui::WidgetInfo::labeled(
            egui::WidgetType::Button,
            true,
            label(language, "主图显示设置", "Chart display settings"),
        )
    });
    let response =
        response.on_hover_text(label(language, "主图显示设置", "Chart display settings"));
    egui::Popup::menu(&response)
        .align(egui::emath::RectAlign::BOTTOM)
        .close_behavior(egui::PopupCloseBehavior::CloseOnClickOutside)
        .frame(menu_frame())
        .show(|ui| contents(ui, settings, language));
    response
}

pub(super) fn menu_frame() -> egui::Frame {
    egui::Frame::popup(&egui::Style::default())
        .fill(MENU_BG)
        .stroke(Stroke::new(1.0, Color32::from_rgb(43, 49, 57)))
        .corner_radius(6)
        .inner_margin(egui::Margin {
            left: 12,
            right: 12,
            top: 8,
            bottom: 9,
        })
}

pub(super) fn contents(ui: &mut egui::Ui, settings: &mut ChartTradingSettings, language: Language) {
    ui.set_width(if language == Language::SimplifiedChinese {
        116.0
    } else {
        176.0
    });
    ui.spacing_mut().item_spacing.y = 0.0;
    ui.visuals_mut().window_fill = MENU_BG;
    ui.visuals_mut().widgets.inactive.bg_fill = MENU_BG;
    let response = row(
        ui,
        &mut settings.quick_order,
        label(language, "快捷下单", "Quick order"),
        true,
        true,
    );
    submenu(ui, &response, |ui| {
        ui.checkbox(
            &mut settings.quick_buy,
            label(language, "买入 / 开多", "Buy / Long"),
        );
        ui.checkbox(
            &mut settings.quick_sell,
            label(language, "卖出 / 开空", "Sell / Short"),
        );
        ui.checkbox(
            &mut settings.quick_amount,
            label(language, "下单金额", "Order amount"),
        );
    });
    let response = row(
        ui,
        &mut settings.current_orders,
        label(language, "当前委托", "Open orders"),
        true,
        true,
    );
    submenu(ui, &response, |ui| {
        ui.checkbox(
            &mut settings.order_lines,
            label(language, "委托价格线", "Order price lines"),
        );
        ui.checkbox(
            &mut settings.order_quantity,
            label(language, "剩余委托数量", "Remaining quantity"),
        );
    });
    row(
        ui,
        &mut settings.positions,
        label(language, "持有仓位", "Positions"),
        false,
        true,
    );
    row(
        ui,
        &mut settings.history,
        label(language, "历史委托", "Order history"),
        false,
        true,
    )
    .on_hover_text(label(
        language,
        "按真实成交时间显示标记；悬停查看价格和数量",
        "Actual fill markers; hover for price and quantity",
    ));
    row(
        ui,
        &mut settings.liquidation,
        label(language, "强平价格", "Liquidation price"),
        false,
        true,
    )
    .on_hover_text(label(
        language,
        "当前账户投影暂无强平价数据",
        "Liquidation prices are unavailable in the current account data",
    ));
    row(
        ui,
        &mut settings.alerts,
        label(language, "价格提醒", "Price alerts"),
        false,
        true,
    )
    .on_hover_text(label(
        language,
        "管理本机提醒，仅在终端运行期间监测",
        "Local alerts are monitored only while this terminal is running",
    ));
    let response = row(
        ui,
        &mut settings.price_lines,
        label(language, "价格线", "Price lines"),
        true,
        true,
    );
    submenu(ui, &response, |ui| {
        ui.checkbox(
            &mut settings.last_price,
            label(language, "最新价格", "Last price"),
        );
        ui.checkbox(
            &mut settings.mark_price,
            label(language, "标记价格", "Mark price"),
        );
        ui.checkbox(
            &mut settings.bid_ask,
            label(language, "买一 / 卖一", "Best bid / ask"),
        );
        ui.checkbox(
            &mut settings.price_labels,
            label(language, "价格标签", "Price labels"),
        );
    });
    let ticks_available = settings.price_lines && settings.price_labels;
    let response = row(
        ui,
        &mut settings.ticks,
        label(language, "刻度", "Ticks"),
        true,
        ticks_available,
    );
    if ticks_available {
        submenu(ui, &response, |ui| {
            ui.checkbox(
                &mut settings.tick_prices,
                label(language, "市场价格", "Market prices"),
            );
            ui.checkbox(
                &mut settings.tick_orders,
                label(language, "委托价格", "Order prices"),
            );
            ui.checkbox(
                &mut settings.tick_positions,
                label(language, "仓位价格", "Position prices"),
            );
        });
    } else {
        response.on_disabled_hover_text(label(
            language,
            "开启价格线中的价格标签后可设置刻度",
            "Enable price labels under Price lines to configure ticks",
        ));
    }
    row(
        ui,
        &mut settings.order_preview,
        label(language, "订单预览线", "Order preview"),
        false,
        true,
    );
}

fn submenu(ui: &egui::Ui, response: &egui::Response, content: impl FnOnce(&mut egui::Ui)) {
    egui::containers::menu::SubMenu::new()
        .config(
            egui::containers::menu::MenuConfig::new()
                .close_behavior(egui::PopupCloseBehavior::CloseOnClickOutside),
        )
        .show(ui, response, |ui| {
            ui.set_min_width(150.0);
            ui.spacing_mut().item_spacing.y = 8.0;
            content(ui);
        });
}

fn row(
    ui: &mut egui::Ui,
    checked: &mut bool,
    text: &str,
    arrow: bool,
    enabled: bool,
) -> egui::Response {
    ui.add_enabled_ui(enabled, |ui| {
        let (rect, _) =
            ui.allocate_exact_size(egui::vec2(ui.available_width(), ROW_HEIGHT), Sense::hover());
        let box_rect = Rect::from_center_size(
            Pos2::new(rect.left() + 9.0, rect.center().y),
            egui::vec2(18.0, 18.0),
        );
        let checkbox = ui.interact(
            Rect::from_min_max(rect.min, Pos2::new(rect.left() + 25.0, rect.bottom())),
            ui.id().with(text).with("check"),
            Sense::click(),
        );
        let mut response = ui.interact(
            Rect::from_min_max(Pos2::new(rect.left() + 25.0, rect.top()), rect.max),
            ui.id().with(text),
            Sense::click(),
        );
        if checkbox.clicked() || (!arrow && response.clicked()) {
            *checked = !*checked;
            response.mark_changed();
        }
        checkbox.widget_info(|| {
            egui::WidgetInfo::selected(egui::WidgetType::Checkbox, enabled, *checked, text)
        });
        response.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, enabled, text));
        let foreground = if enabled {
            theme::TEXT_PRIMARY
        } else {
            Color32::from_rgb(78, 88, 101)
        };
        if enabled && (response.hovered() || checkbox.hovered()) {
            ui.painter().rect_filled(
                rect.expand2(egui::vec2(7.0, 0.0)),
                3,
                Color32::from_rgb(43, 51, 63),
            );
        }
        ui.painter().rect(
            box_rect,
            3,
            if *checked && enabled {
                theme::TEXT_PRIMARY
            } else if !enabled {
                Color32::from_rgb(48, 58, 71)
            } else {
                Color32::TRANSPARENT
            },
            Stroke::new(
                1.0,
                if *checked && enabled {
                    theme::TEXT_PRIMARY
                } else {
                    foreground.gamma_multiply(0.6)
                },
            ),
            egui::StrokeKind::Inside,
        );
        if *checked {
            ui.painter().add(egui::Shape::line(
                vec![
                    box_rect.left_center() + egui::vec2(3.5, 0.0),
                    box_rect.left_center() + egui::vec2(7.0, 3.5),
                    box_rect.right_top() + egui::vec2(-3.0, 4.5),
                ],
                Stroke::new(1.8, MENU_BG),
            ));
        }
        ui.painter().text(
            Pos2::new(rect.left() + 25.0, rect.center().y),
            Align2::LEFT_CENTER,
            text,
            FontId::proportional(15.0),
            foreground,
        );
        if arrow {
            let center = Pos2::new(rect.right() - 4.0, rect.center().y);
            ui.painter().add(egui::Shape::line(
                vec![
                    center + egui::vec2(-2.0, -4.0),
                    center + egui::vec2(2.0, 0.0),
                    center + egui::vec2(-2.0, 4.0),
                ],
                Stroke::new(1.2, theme::TEXT_SECONDARY),
            ));
        }
        response
    })
    .inner
}
