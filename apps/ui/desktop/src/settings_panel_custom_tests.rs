use super::*;

#[test]
fn main_indicator_numeric_fields_share_one_column() {
    let ctx = egui::Context::default();
    let mut state = SettingsPanelState::default();
    let mut model = AppModel::new(Default::default());
    let mut open = true;
    let mut reconnect = false;
    fn centers(shape: &egui::Shape, values: &mut Vec<(String, f32)>) {
        match shape {
            egui::Shape::Text(t) => {
                values.push((t.galley.job.text.clone(), t.pos.x + t.galley.size().x / 2.0))
            }
            egui::Shape::Vec(v) => v.iter().for_each(|s| centers(s, values)),
            _ => {}
        }
    }
    let mut values = Vec::new();
    for _ in 0..3 {
        let mut output = ctx.run_ui(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(1100.0, 850.0),
                )),
                ..Default::default()
            },
            |ui| show(ui.ctx(), &mut open, &mut state, &mut model, &mut reconnect),
        );
        output.textures_delta.clear();
        values.clear();
        for shape in output.shapes {
            centers(&shape.shape, &mut values);
        }
    }
    let period = values.iter().find(|(text, _)| text == "10").map(|v| v.1);
    let multiplier = values
        .iter()
        .find(|(text, _)| text == "3.0000")
        .map(|v| v.1);
    assert!(
        period
            .zip(multiplier)
            .is_some_and(|(a, b)| (a - b).abs() < 1.0),
        "{values:?}"
    );
}

fn frame(
    ctx: &egui::Context,
    state: &mut SettingsPanelState,
    model: &mut AppModel,
    open: &mut bool,
    events: Vec<egui::Event>,
) -> Option<egui::Pos2> {
    let mut reconnect = false;
    let mut output = ctx.run_ui(
        egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(1100.0, 850.0),
            )),
            events,
            ..Default::default()
        },
        |ui| show(ui.ctx(), open, state, model, &mut reconnect),
    );
    output.textures_delta.clear();
    fn find(shape: &egui::Shape, label: &str) -> Option<egui::Pos2> {
        match shape {
            egui::Shape::Text(t) if t.galley.job.text == label => {
                Some(t.pos + t.galley.size() / 2.0)
            }
            egui::Shape::Vec(v) => v.iter().find_map(|s| find(s, label)),
            _ => None,
        }
    }
    let label = indicator_text(model.preferences.language, IndicatorTextKey::Save);
    output.shapes.iter().find_map(|s| find(&s.shape, label))
}

#[test]
fn latest_invalid_settings_and_pending_edits_cannot_close_save() -> Result<(), String> {
    for pending in [false, true] {
        let ctx = egui::Context::default();
        let mut state = SettingsPanelState {
            tab: SettingsTab::Custom,
            ..Default::default()
        };
        let mut model = AppModel::new(Default::default());
        let mut open = true;
        frame(&ctx, &mut state, &mut model, &mut open, Vec::new());
        let point = frame(&ctx, &mut state, &mut model, &mut open, Vec::new())
            .ok_or("Missing save button")?;
        let event = |pressed| {
            vec![
                egui::Event::PointerMoved(point),
                egui::Event::PointerButton {
                    pos: point,
                    button: egui::PointerButton::Primary,
                    pressed,
                    modifiers: Default::default(),
                },
            ]
        };
        frame(&ctx, &mut state, &mut model, &mut open, event(true));
        if pending {
            state.custom_editor.draft = Some(crate::custom_indicator::Entry {
                id: 0,
                name: "Unsaved".into(),
                enabled: true,
                kind: crate::custom_indicator::EntryKind::Script {
                    source: "plot(close);".into(),
                },
            });
        } else if let Some(draft) = &mut state.draft {
            draft.ma_periods[0] = 0;
        }
        state.error = None;
        frame(&ctx, &mut state, &mut model, &mut open, event(false));
        assert!(open);
        assert!(state.error.is_some());
        assert_ne!(model.preferences.chart.ma_periods[0], 0);
    }
    Ok(())
}
