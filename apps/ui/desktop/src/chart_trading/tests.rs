use super::*;
use rust_decimal::Decimal;

#[test]
fn legacy_layout_restores_display_defaults_and_roundtrips() -> Result<(), Box<dyn std::error::Error>>
{
    let mut pane: crate::workspace::Pane = serde_json::from_str("{}")?;
    assert_eq!(pane.trading_display, ChartTradingSettings::default());
    pane.trading_display.current_orders = false;
    pane.trading_display.order_preview = true;
    let restored: crate::workspace::Pane = serde_json::from_str(&serde_json::to_string(&pane)?)?;
    assert_eq!(restored.trading_display, pane.trading_display);
    Ok(())
}

#[test]
fn alerts_cross_once_ignore_stale_ticks_and_rearm_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let mut book = AlertBook::default();
    assert!(book.add("BTC/USDC", Decimal::from(100)));
    assert!(!book.add("BTC/USDC", Decimal::from(100)));
    assert!(book.observe("BTC/USDC", 1, Decimal::from(99)).is_empty());
    assert!(book.observe("ETH/USDC", 2, Decimal::from(101)).is_empty());
    assert!(book.observe("BTC/USDC", 0, Decimal::from(101)).is_empty());
    let mut restored: AlertBook = serde_json::from_str(&serde_json::to_string(&book)?)?;
    assert!(
        restored
            .observe("BTC/USDC", 2, Decimal::from(101))
            .is_empty()
    );
    assert_eq!(
        restored.observe("BTC/USDC", 3, Decimal::from(99)),
        vec![Decimal::from(100)]
    );
    assert!(
        restored
            .observe("BTC/USDC", 4, Decimal::from(101))
            .is_empty()
    );
    Ok(())
}

#[test]
fn no_account_does_not_create_private_price_lines() {
    let model = crate::model::AppModel::new(crate::model::Preferences::default());
    assert!(collect(&model, "BTC/USDC", &ChartTradingSettings::default()).is_empty());
}

#[test]
fn private_overlays_obey_account_symbol_and_visibility() -> Result<(), Box<dyn std::error::Error>> {
    use venue_control_protocol::kol::*;
    let mut model = crate::model::AppModel::new(crate::model::Preferences::default());
    model.preferences.execution_account_id = Some("selected-account".into());
    let projection = TerminalAccountProjection {
        schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
        credential_id: "credential".into(),
        trading_account_id: "selected-account".into(),
        observed_ms: 1,
        persisted_ms: 1,
        private_generation: 1,
        position_mode: TerminalPositionMode::Hedge,
        positions: vec![TerminalPosition {
            symbol: "BTC/USDC".parse()?,
            position_side: venue_domain::PositionSide::Long,
            quantity: Decimal::ONE,
            entry_price: Some(Decimal::from(100)),
            mark_price: None,
        }],
        position_history: vec![],
        open_orders: vec![],
        fills: vec![],
        assets: vec![],
    };
    model.execution.private_projection = Some(std::sync::Arc::new(projection));
    let mut settings = ChartTradingSettings::default();
    let lines = collect(&model, "BTC/USDC", &settings);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].price, Decimal::from(100));
    assert!(lines[0].badge.as_ref().is_some_and(|badge| badge.stale));
    assert!(collect(&model, "ETH/USDC", &settings).is_empty());
    settings.positions = false;
    assert!(collect(&model, "BTC/USDC", &settings).is_empty());
    settings.positions = true;
    model.preferences.execution_account_id = Some("another-account".into());
    assert!(collect(&model, "BTC/USDC", &settings).is_empty());
    Ok(())
}

fn text_shapes(shape: &egui::Shape, texts: &mut Vec<(String, egui::Rect)>) {
    match shape {
        egui::Shape::Text(text) => texts.push((
            text.galley.job.text.clone(),
            egui::Rect::from_min_size(text.pos, text.galley.size()),
        )),
        egui::Shape::Vec(shapes) => shapes.iter().for_each(|shape| text_shapes(shape, texts)),
        _ => (),
    }
}

struct MenuHarness {
    context: egui::Context,
    settings: ChartTradingSettings,
    button: egui::Rect,
    labels: Vec<(String, egui::Rect)>,
    textures: Vec<serde_json::Value>,
    time: f64,
}

impl MenuHarness {
    fn new() -> Self {
        let context = egui::Context::default();
        crate::theme::apply(&context);
        Self {
            context,
            settings: ChartTradingSettings::default(),
            button: egui::Rect::NOTHING,
            labels: Vec::new(),
            textures: Vec::new(),
            time: 1.0,
        }
    }

    fn frame(&mut self, events: Vec<egui::Event>) -> egui::FullOutput {
        self.time += 0.1;
        let mut output = self.context.run_ui(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(420.0, 440.0),
                )),
                events,
                time: Some(self.time),
                ..Default::default()
            },
            |ui| {
                ui.painter()
                    .rect_filled(ui.max_rect(), 0, crate::theme::BG_SECONDARY);
                ui.add_space(18.0);
                ui.horizontal(|ui| {
                    ui.add_space(89.0);
                    self.button = menu_button(
                        ui,
                        &mut self.settings,
                        crate::i18n::Language::SimplifiedChinese,
                    )
                    .rect;
                });
            },
        );
        self.labels.clear();
        for shape in &output.shapes {
            text_shapes(&shape.shape, &mut self.labels);
        }
        if std::env::var_os("VENUE_CHART_MENU_PREVIEW").is_some() {
            for (id, deltas) in &output.textures_delta.set {
                for delta in deltas {
                    let egui::ImageData::Color(image) = &delta.image;
                    self.textures.push(serde_json::json!({
                    "id": format!("{id:?}"), "pos": delta.pos, "size": image.size,
                    "pixels": image.pixels.iter().flat_map(|color| color.to_array()).collect::<Vec<_>>()
                    }));
                }
            }
        }
        output.textures_delta.clear();
        output
    }

    fn click(&mut self, pos: egui::Pos2) {
        for pressed in [true, false] {
            self.frame(vec![
                egui::Event::PointerMoved(pos),
                egui::Event::PointerButton {
                    pos,
                    button: egui::PointerButton::Primary,
                    pressed,
                    modifiers: egui::Modifiers::NONE,
                },
            ]);
        }
        self.frame(vec![]);
    }

    fn label(&self, name: &str) -> Result<egui::Rect, String> {
        self.labels
            .iter()
            .find(|(text, _)| text == name)
            .map(|(_, rect)| *rect)
            .ok_or_else(|| format!("Missing label: {name}"))
    }
}

#[test]
fn menu_layout_checkbox_submenu_and_outside_click() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = MenuHarness::new();
    harness.frame(vec![]);
    harness.frame(vec![]);
    harness.click(harness.button.center());
    let output = harness.frame(vec![egui::Event::PointerMoved(egui::pos2(390.0, 430.0))]);
    let names = [
        "快捷下单",
        "当前委托",
        "持有仓位",
        "历史委托",
        "强平价格",
        "价格提醒",
        "价格线",
        "刻度",
        "订单预览线",
    ];
    let rows = names
        .iter()
        .map(|name| harness.label(name))
        .collect::<Result<Vec<_>, _>>()?;
    for pair in rows.windows(2) {
        assert!((pair[1].center().y - pair[0].center().y - 38.0).abs() < 0.1);
        assert_eq!(pair[0].left(), pair[1].left());
    }
    if let Some(path) = std::env::var_os("VENUE_CHART_MENU_PREVIEW") {
        let primitives = harness
            .context
            .tessellate(output.shapes, output.pixels_per_point);
        let meshes = primitives.iter().filter_map(|primitive| {
            let egui::epaint::Primitive::Mesh(mesh) = &primitive.primitive else { return None; };
            Some(serde_json::json!({ "clip": primitive.clip_rect, "texture": format!("{:?}", mesh.texture_id), "vertices": mesh.vertices, "indices": mesh.indices }))
        }).collect::<Vec<_>>();
        std::fs::write(
            path,
            serde_json::to_vec(
                &serde_json::json!({ "size": [420, 440], "textures": harness.textures, "meshes": meshes }),
            )?,
        )?;
    }
    harness.click(rows[1].left_center() - egui::vec2(16.0, 0.0));
    assert!(!harness.settings.current_orders);
    harness.label("订单预览线")?;
    harness.click(rows[7].left_center() - egui::vec2(16.0, 0.0));
    assert!(!harness.settings.ticks);
    harness.click(rows[6].center());
    let labels = harness.label("价格标签")?;
    harness.click(labels.center());
    assert!(harness.settings.price_labels);
    harness.click(egui::pos2(390.0, 430.0));
    assert!(!egui::Popup::is_any_open(&harness.context));
    Ok(())
}
