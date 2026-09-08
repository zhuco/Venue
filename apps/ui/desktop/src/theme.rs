use eframe::egui::{self, Color32, FontId, Frame, Margin, Stroke, TextStyle};

use eframe::egui::{FontData, FontDefinitions, FontFamily};

pub const BG_PRIMARY: Color32 = Color32::from_rgb(0x0b, 0x0e, 0x11);
pub const BG_SECONDARY: Color32 = Color32::from_rgb(0x18, 0x1a, 0x20);
pub const PANEL: Color32 = Color32::from_rgb(0x0e, 0x12, 0x17);
pub const BRAND: Color32 = Color32::from_rgb(0xf0, 0xb9, 0x0b);
pub const BRAND_HOVER: Color32 = Color32::from_rgb(0xfc, 0xd5, 0x35);
pub const BUY: Color32 = Color32::from_rgb(0x0e, 0xcb, 0x81);
pub const SELL: Color32 = Color32::from_rgb(0xf6, 0x46, 0x5d);
pub const POSITION_LINE: Color32 = Color32::from_rgb(0x58, 0xa6, 0xff);
pub const WARNING: Color32 = Color32::from_rgb(0xf0, 0xb9, 0x0b);
pub const TEXT_PRIMARY: Color32 = Color32::from_rgb(0xea, 0xec, 0xef);
pub const TEXT_SECONDARY: Color32 = Color32::from_rgb(0x84, 0x8e, 0x9c);
pub const DIVIDER: Color32 = Color32::from_rgb(0x2b, 0x31, 0x39);
pub const CHART_GRID: Color32 = Color32::from_rgb(0x1e, 0x23, 0x29);

pub fn apply(context: &egui::Context) {
    #[cfg(not(target_arch = "wasm32"))]
    install_system_fonts(context);
    #[cfg(target_arch = "wasm32")]
    {
        let mut fonts = FontDefinitions::default();
        install_emphasis_family(&mut fonts);
        context.set_fonts(fonts);
    }
    context.set_theme(egui::Theme::Dark);
    let mut style = (*context.style_of(egui::Theme::Dark)).clone();
    let mut visuals = egui::Visuals::dark();
    visuals.override_text_color = Some(TEXT_PRIMARY);
    visuals.panel_fill = BG_PRIMARY;
    visuals.window_fill = BG_SECONDARY;
    visuals.extreme_bg_color = BG_PRIMARY;
    visuals.faint_bg_color = BG_SECONDARY;
    visuals.window_stroke = Stroke::new(1.0, DIVIDER);
    visuals.selection.bg_fill = Color32::from_rgba_unmultiplied(0xf0, 0xb9, 0x0b, 48);
    visuals.selection.stroke = Stroke::new(1.0, BRAND);
    visuals.hyperlink_color = BRAND_HOVER;
    visuals.warn_fg_color = WARNING;
    visuals.error_fg_color = SELL;
    visuals.widgets.noninteractive.bg_fill = PANEL;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, DIVIDER);
    visuals.widgets.inactive.bg_fill = BG_SECONDARY;
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, DIVIDER);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(0x2b, 0x31, 0x39);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, BRAND_HOVER);
    visuals.widgets.active.bg_fill = Color32::from_rgb(0x3a, 0x3f, 0x47);
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, BRAND_HOVER);
    visuals.window_corner_radius = egui::CornerRadius::same(8);
    visuals.menu_corner_radius = egui::CornerRadius::same(6);
    for widget in [
        &mut visuals.widgets.noninteractive,
        &mut visuals.widgets.inactive,
        &mut visuals.widgets.hovered,
        &mut visuals.widgets.active,
        &mut visuals.widgets.open,
    ] {
        widget.corner_radius = egui::CornerRadius::same(5);
    }
    style.visuals = visuals;
    style.spacing.item_spacing = egui::vec2(6.0, 4.0);
    style.spacing.button_padding = egui::vec2(9.0, 5.0);
    style.spacing.interact_size = egui::vec2(34.0, 26.0);
    style
        .text_styles
        .insert(TextStyle::Heading, FontId::proportional(18.0));
    style
        .text_styles
        .insert(TextStyle::Body, FontId::proportional(12.5));
    style
        .text_styles
        .insert(TextStyle::Button, FontId::proportional(12.0));
    style
        .text_styles
        .insert(TextStyle::Small, FontId::proportional(11.0));
    style
        .text_styles
        .insert(TextStyle::Monospace, FontId::monospace(11.5));
    context.set_style_of(egui::Theme::Dark, style);
}

#[cfg(not(target_arch = "wasm32"))]
fn install_system_fonts(context: &egui::Context) {
    let mut fonts = FontDefinitions::default();
    if let Some(bytes) = read_first_font(cjk_font_candidates()) {
        add_cjk_font(&mut fonts, bytes);
    }
    let mut candidates = Vec::new();
    if let Some(windows) = std::env::var_os("WINDIR") {
        candidates.push(std::path::PathBuf::from(windows).join("Fonts/segoeui.ttf"));
    }
    for path in [
        "/System/Library/Fonts/Helvetica.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ] {
        candidates.push(path.into());
    }
    if let Some(bytes) = read_first_font(candidates) {
        let name = "venueflow-system-ui".to_owned();
        fonts
            .font_data
            .insert(name.clone(), FontData::from_owned(bytes).into());
        fonts
            .families
            .entry(FontFamily::Proportional)
            .or_default()
            .insert(0, name);
    }
    install_emphasis_family(&mut fonts);
    if let Some(windows) = std::env::var_os("WINDIR")
        && let Some(bytes) = read_first_font(vec![
            std::path::PathBuf::from(windows).join("Fonts/seguisb.ttf"),
        ])
    {
        let name = "venueflow-system-semibold".to_owned();
        fonts
            .font_data
            .insert(name.clone(), FontData::from_owned(bytes).into());
        fonts
            .families
            .entry(emphasis_font(14.0).family)
            .or_default()
            .insert(0, name);
    }
    context.set_fonts(fonts);
}

pub fn emphasis_font(size: f32) -> FontId {
    FontId::new(size, FontFamily::Name("venueflow-emphasis".into()))
}

fn install_emphasis_family(fonts: &mut FontDefinitions) {
    let fallback = fonts
        .families
        .get(&FontFamily::Proportional)
        .cloned()
        .unwrap_or_default();
    fonts.families.insert(emphasis_font(14.0).family, fallback);
}

#[cfg(not(target_arch = "wasm32"))]
fn read_first_font(paths: Vec<std::path::PathBuf>) -> Option<Vec<u8>> {
    const MAX_FONT_BYTES: u64 = 32 * 1024 * 1024;
    paths.into_iter().find_map(|path| {
        if !std::fs::metadata(&path).is_ok_and(|metadata| {
            metadata.is_file() && metadata.len() > 0 && metadata.len() <= MAX_FONT_BYTES
        }) {
            return None;
        }
        std::fs::read(path).ok()
    })
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn install_cjk_bytes(context: &egui::Context, bytes: Vec<u8>) {
    let mut fonts = FontDefinitions::default();
    add_cjk_font(&mut fonts, bytes);
    install_emphasis_family(&mut fonts);
    context.set_fonts(fonts);
}

fn add_cjk_font(fonts: &mut FontDefinitions, bytes: Vec<u8>) {
    let name = "venueflow-system-cjk".to_owned();
    fonts
        .font_data
        .insert(name.clone(), FontData::from_owned(bytes).into());
    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        fonts.families.entry(family).or_default().push(name.clone());
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn cjk_font_candidates() -> Vec<std::path::PathBuf> {
    let mut candidates = Vec::new();
    if let Some(windows) = std::env::var_os("WINDIR") {
        let fonts = std::path::PathBuf::from(windows).join("Fonts");
        candidates.push(fonts.join("msyh.ttc"));
        candidates.push(fonts.join("Deng.ttf"));
        candidates.push(fonts.join("simhei.ttf"));
    }
    for path in [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
        "/System/Library/Fonts/PingFang.ttc",
    ] {
        candidates.push(path.into());
    }
    candidates
}

pub fn panel_frame() -> Frame {
    Frame::new()
        .fill(PANEL)
        .stroke(Stroke::new(1.0, DIVIDER))
        .corner_radius(egui::CornerRadius::same(6))
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
