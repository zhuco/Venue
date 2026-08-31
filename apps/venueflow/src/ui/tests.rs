use super::*;

fn collect_text(shape: &egui::Shape, output: &mut Vec<(String, egui::Rect)>) {
    match shape {
        egui::Shape::Text(value) => output.push((
            value.galley.job.text.clone(),
            egui::Rect::from_min_size(value.pos, value.galley.size()),
        )),
        egui::Shape::Vec(values) => values.iter().for_each(|value| collect_text(value, output)),
        _ => (),
    }
}

#[test]
fn trading_settings_header_and_done_remain_visible_in_small_windows() {
    for language in Language::ALL {
        for size in [egui::vec2(1100.0, 700.0), egui::vec2(850.0, 520.0)] {
            let context = egui::Context::default();
            theme::apply(&context);
            let mut model = AppModel::new(crate::model::Preferences {
                language,
                ..Default::default()
            });
            let mut open = true;
            let mut labels = Vec::new();
            let screen = egui::Rect::from_min_size(egui::Pos2::ZERO, size);
            for _ in 0..3 {
                let mut frame = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(screen),
                        ..Default::default()
                    },
                    |ui| crate::trading::show_settings(ui.ctx(), &mut open, &mut model),
                );
                frame.textures_delta.clear();
                labels.clear();
                for shape in frame.shapes {
                    collect_text(&shape.shape, &mut labels);
                }
            }
            for label in [
                text(language, TextKey::TradingSettings),
                if language == Language::English {
                    "Done"
                } else {
                    "完成"
                },
            ] {
                let bounds = labels
                    .iter()
                    .find(|(value, _)| value == label)
                    .map(|(_, rect)| *rect);
                assert!(
                    bounds.is_some_and(|rect| screen.contains_rect(rect)),
                    "{label} is clipped at {size:?}: {bounds:?}"
                );
            }
            assert!(
                labels
                    .iter()
                    .any(|(label, _)| label == text(language, TextKey::DisplayCadence))
            );
        }
    }
}

#[test]
fn top_button_opens_trading_settings_without_a_trade_dock() {
    let context = egui::Context::default();
    theme::apply(&context);
    let mut model = AppModel::new(crate::model::Preferences::default());
    let mut workspaces = Workspaces::default();
    let mut modules = false;
    let mut trading = false;
    let mut accounts = false;
    let mut picker = false;
    let mut button = None;
    for pressed in [None, None, Some(true), Some(false)] {
        let events = button.map_or_else(Vec::new, |rect: egui::Rect| {
            let mut events = vec![egui::Event::PointerMoved(rect.center())];
            if let Some(pressed) = pressed {
                events.push(egui::Event::PointerButton {
                    pos: rect.center(),
                    button: egui::PointerButton::Primary,
                    pressed,
                    modifiers: egui::Modifiers::NONE,
                });
            }
            events
        });
        let mut frame = context.run_ui(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(1100.0, 700.0),
                )),
                events,
                ..Default::default()
            },
            |ui| {
                show_top_bar(
                    ui,
                    &mut model,
                    &mut workspaces,
                    &mut modules,
                    &mut trading,
                    &mut accounts,
                    &mut picker,
                )
            },
        );
        frame.textures_delta.clear();
        let mut labels = Vec::new();
        for shape in frame.shapes {
            collect_text(&shape.shape, &mut labels);
        }
        button = labels
            .into_iter()
            .find(|(label, _)| label == text(model.preferences.language, TextKey::TradingSettings))
            .map(|(_, rect)| rect);
        assert!(button.is_some_and(|rect| rect.right() <= 1100.0));
    }
    assert!(trading);
    assert!(!modules && !accounts && !picker);
}
