use eframe::egui::{self, Color32, FontId, Frame, Margin, Stroke, TextStyle};

pub const BG_PRIMARY: Color32 = Color32::from_rgb(0x07, 0x13, 0x1f);
pub const BG_SECONDARY: Color32 = Color32::from_rgb(0x0d, 0x1b, 0x2a);
pub const PANEL: Color32 = Color32::from_rgb(0x11, 0x23, 0x33);
pub const BRAND: Color32 = Color32::from_rgb(0x16, 0xb8, 0xa6);
pub const BRAND_HOVER: Color32 = Color32::from_rgb(0x26, 0xcf, 0xc0);
pub const BUY: Color32 = Color32::from_rgb(0x35, 0xc9, 0x88);
pub const SELL: Color32 = Color32::from_rgb(0xf0, 0x61, 0x74);
pub const WARNING: Color32 = Color32::from_rgb(0xe4, 0xb8, 0x55);
pub const TEXT_PRIMARY: Color32 = Color32::from_rgb(0xe7, 0xf0, 0xf4);
pub const TEXT_SECONDARY: Color32 = Color32::from_rgb(0x8f, 0xa6, 0xb5);
pub const DIVIDER: Color32 = Color32::from_rgb(0x20, 0x35, 0x43);

pub fn apply(context: &egui::Context) {
    context.set_theme(egui::Theme::Dark);
    let mut style = (*context.style_of(egui::Theme::Dark)).clone();
    let mut visuals = egui::Visuals::dark();
    visuals.override_text_color = Some(TEXT_PRIMARY);
    visuals.panel_fill = BG_PRIMARY;
    visuals.window_fill = BG_SECONDARY;
    visuals.extreme_bg_color = BG_PRIMARY;
    visuals.faint_bg_color = BG_SECONDARY;
    visuals.window_stroke = Stroke::new(1.0, DIVIDER);
    visuals.selection.bg_fill = Color32::from_rgba_unmultiplied(0x16, 0xb8, 0xa6, 64);
    visuals.selection.stroke = Stroke::new(1.0, BRAND);
    visuals.hyperlink_color = BRAND_HOVER;
    visuals.warn_fg_color = WARNING;
    visuals.error_fg_color = SELL;
    visuals.widgets.noninteractive.bg_fill = PANEL;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, DIVIDER);
    visuals.widgets.inactive.bg_fill = BG_SECONDARY;
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, DIVIDER);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(0x14, 0x31, 0x40);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, BRAND_HOVER);
    visuals.widgets.active.bg_fill = Color32::from_rgb(0x0e, 0x8f, 0x83);
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, BRAND_HOVER);
    style.visuals = visuals;
    style.spacing.item_spacing = egui::vec2(6.0, 5.0);
    style.spacing.button_padding = egui::vec2(10.0, 6.0);
    style.spacing.interact_size = egui::vec2(36.0, 28.0);
    style
        .text_styles
        .insert(TextStyle::Heading, FontId::proportional(20.0));
    style
        .text_styles
        .insert(TextStyle::Body, FontId::proportional(13.5));
    style
        .text_styles
        .insert(TextStyle::Button, FontId::proportional(13.0));
    style
        .text_styles
        .insert(TextStyle::Small, FontId::proportional(11.5));
    style
        .text_styles
        .insert(TextStyle::Monospace, FontId::monospace(12.0));
    context.set_style_of(egui::Theme::Dark, style);
}

pub fn panel_frame() -> Frame {
    Frame::new()
        .fill(PANEL)
        .stroke(Stroke::new(1.0, DIVIDER))
        .corner_radius(egui::CornerRadius::same(3))
        .inner_margin(Margin::same(7))
}

pub fn value_color(value: f64) -> Color32 {
    if value > 0.0 {
        BUY
    } else if value < 0.0 {
        SELL
    } else {
        TEXT_PRIMARY
    }
}
