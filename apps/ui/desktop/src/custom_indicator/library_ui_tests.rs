use super::*;
use eframe::egui;

fn frame(
    ctx: &egui::Context,
    settings: &mut ChartDisplaySettings,
    editor: &mut LibraryEditor,
    events: Vec<egui::Event>,
) -> Vec<(String, egui::Pos2)> {
    let mut output = ctx.run_ui(
        egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(1000.0, 900.0),
            )),
            events,
            ..Default::default()
        },
        |ui| {
            settings_ui(ui, settings, editor, Language::English);
        },
    );
    fn collect(shape: &egui::Shape, out: &mut Vec<(String, egui::Pos2)>) {
        match shape {
            egui::Shape::Text(t) => {
                out.push((t.galley.job.text.clone(), t.pos + t.galley.size() / 2.0))
            }
            egui::Shape::Vec(v) => {
                for s in v {
                    collect(s, out)
                }
            }
            _ => {}
        }
    }
    output.textures_delta.clear();
    let mut labels = Vec::new();
    for shape in output.shapes {
        collect(&shape.shape, &mut labels);
    }
    labels
}
fn click(
    ctx: &egui::Context,
    settings: &mut ChartDisplaySettings,
    editor: &mut LibraryEditor,
    label: &str,
) -> Result<(), String> {
    frame(ctx, settings, editor, Vec::new());
    let labels = frame(ctx, settings, editor, Vec::new());
    let point = labels
        .iter()
        .find(|(s, _)| s == label)
        .map(|(_, p)| *p)
        .ok_or_else(|| format!("Missing button {label}: {labels:?}"))?;
    for pressed in [true, false] {
        frame(
            ctx,
            settings,
            editor,
            vec![
                egui::Event::PointerMoved(point),
                egui::Event::PointerButton {
                    pos: point,
                    button: egui::PointerButton::Primary,
                    pressed,
                    modifiers: Default::default(),
                },
            ],
        );
    }
    Ok(())
}
#[test]
fn new_cancel_invalid_save_and_delete_through_ui() -> Result<(), String> {
    let ctx = egui::Context::default();
    let mut settings = ChartDisplaySettings::default();
    let mut editor = LibraryEditor::default();
    click(&ctx, &mut settings, &mut editor, "＋ New indicator")?;
    assert!(editor.pending());
    click(&ctx, &mut settings, &mut editor, "Cancel edit")?;
    assert_eq!(
        settings.custom_library.as_ref().map(|l| l.entries.len()),
        Some(2)
    );
    click(&ctx, &mut settings, &mut editor, "＋ New indicator")?;
    click(&ctx, &mut settings, &mut editor, "Save indicator")?;
    assert!(!editor.pending());
    assert_eq!(
        settings.custom_library.as_ref().map(|l| l.entries.len()),
        Some(3)
    );
    let entry = settings
        .custom_library
        .as_ref()
        .and_then(|l| l.entries.last())
        .cloned()
        .ok_or("Missing saved item")?;
    editor.draft = Some(Entry {
        kind: EntryKind::Script {
            source: "plot(no_such_series);".into(),
        },
        ..entry.clone()
    });
    click(&ctx, &mut settings, &mut editor, "Save indicator")?;
    assert!(editor.pending());
    assert!(editor.error.is_some());
    assert_eq!(
        settings
            .custom_library
            .as_ref()
            .and_then(|l| l.entries.last()),
        Some(&entry)
    );
    click(&ctx, &mut settings, &mut editor, "Cancel edit")?;
    click(&ctx, &mut settings, &mut editor, "Delete")?;
    click(&ctx, &mut settings, &mut editor, "Confirm delete")?;
    assert_eq!(
        settings.custom_library.as_ref().map(|l| l.entries.len()),
        Some(2)
    );
    Ok(())
}
