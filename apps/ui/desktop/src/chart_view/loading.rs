use crate::{i18n::Language, theme};
use eframe::egui::{self, Align2, FontId, Stroke};

pub(crate) fn preview_badge(ui: &mut egui::Ui, rect: egui::Rect, language: Language) {
    let caption = match language {
        Language::SimplifiedChinese => "缓存预览 · 正在更新",
        Language::English => "Cached preview · updating",
    };
    let badge =
        egui::Rect::from_min_size(rect.min + egui::vec2(12.0, 12.0), egui::vec2(210.0, 30.0));
    let painter = ui.painter().with_clip_rect(rect);
    painter.rect_filled(badge, 4, theme::BG_SECONDARY);
    painter.text(
        badge.center(),
        Align2::CENTER_CENTER,
        caption,
        FontId::proportional(12.0),
        theme::TEXT_SECONDARY,
    );
    ui.put(
        egui::Rect::from_min_size(badge.min + egui::vec2(4.0, 7.0), egui::vec2(16.0, 16.0)),
        egui::Spinner::new().size(14.0),
    );
}

pub(crate) fn show(
    ui: &mut egui::Ui,
    language: Language,
    symbol: &str,
    interval: &str,
    retrying: bool,
) {
    let (rect, _) = ui.allocate_exact_size(
        ui.available_size().max(egui::vec2(1.0, 1.0)),
        egui::Sense::hover(),
    );
    let painter = ui.painter().with_clip_rect(rect);
    painter.rect_filled(rect, 0, theme::BG_PRIMARY);
    let center = rect.center();
    let title = match (language, retrying) {
        (Language::SimplifiedChinese, false) => "正在加载 K 线",
        (Language::SimplifiedChinese, true) => "等待行情恢复",
        (Language::English, false) => "Loading candles",
        (Language::English, true) => "Waiting for market data",
    };
    painter.text(
        center - egui::vec2(0.0, 18.0),
        Align2::CENTER_CENTER,
        format!("{symbol} · {interval}"),
        FontId::proportional(13.0),
        theme::TEXT_PRIMARY,
    );
    painter.text(
        center + egui::vec2(0.0, 8.0),
        Align2::CENTER_CENTER,
        title,
        FontId::proportional(12.0),
        theme::TEXT_SECONDARY,
    );
    let width = 120.0_f32.min(rect.width() * 0.5);
    let left = center.x - width * 0.5;
    let y = center.y + 33.0;
    painter.line_segment(
        [egui::pos2(left, y), egui::pos2(left + width, y)],
        Stroke::new(2.0, theme::DIVIDER),
    );
    let phase = ui.ctx().input(|input| (input.time * 1.5).fract()) as f32;
    let offset = (0.5 - 0.5 * (phase * std::f32::consts::TAU).cos()) * (width - 24.0).max(0.0);
    painter.line_segment(
        [
            egui::pos2(left + offset, y),
            egui::pos2(left + offset + 24.0, y),
        ],
        Stroke::new(2.0, theme::BRAND),
    );
    ui.ctx()
        .request_repaint_after(std::time::Duration::from_millis(33));
}
