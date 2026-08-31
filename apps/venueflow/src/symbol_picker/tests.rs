use super::*;
use crate::model::Preferences;

fn texts(shape: &egui::Shape, found: &mut Vec<(String, egui::Rect)>) {
    match shape {
        egui::Shape::Text(t) => found.push((
            t.galley.job.text.clone(),
            egui::Rect::from_min_size(t.pos, t.galley.size()),
        )),
        egui::Shape::Vec(shapes) => {
            for shape in shapes {
                texts(shape, found);
            }
        }
        _ => (),
    }
}
fn frame(
    context: &egui::Context,
    model: &mut AppModel,
    workspaces: &mut Workspaces,
    open: &mut bool,
    events: Vec<egui::Event>,
) -> Vec<(String, egui::Rect)> {
    let mut output = context.run_ui(
        egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(900.0, 700.0),
            )),
            events,
            ..Default::default()
        },
        |ui| {
            let response = ui.add_sized([180.0, 28.0], egui::Button::new("Search"));
            show(&response, open, model, workspaces);
        },
    );
    output.textures_delta.clear();
    let mut result = Vec::new();
    for shape in output.shapes {
        texts(&shape.shape, &mut result);
    }
    result
}
fn click(
    context: &egui::Context,
    model: &mut AppModel,
    workspaces: &mut Workspaces,
    open: &mut bool,
    point: egui::Pos2,
) {
    frame(
        context,
        model,
        workspaces,
        open,
        vec![egui::Event::PointerMoved(point)],
    );
    for pressed in [true, false] {
        frame(
            context,
            model,
            workspaces,
            open,
            vec![egui::Event::PointerButton {
                pos: point,
                button: egui::PointerButton::Primary,
                pressed,
                modifiers: egui::Modifiers::NONE,
            }],
        );
    }
}
#[test]
fn blank_columns_select_the_entire_row_and_star_only_changes_favorite() -> Result<(), String> {
    for target in ["blank", "symbol", "star"] {
        let context = egui::Context::default();
        let mut model = AppModel::new(Preferences::default());
        let mut workspaces = Workspaces::default();
        let mut open = true;
        for _ in 0..3 {
            frame(&context, &mut model, &mut workspaces, &mut open, vec![]);
        }
        let labels = frame(&context, &mut model, &mut workspaces, &mut open, vec![]);
        let symbol = labels
            .iter()
            .find(|(text, _)| text == "ETH/USDC")
            .map(|(_, rect)| *rect)
            .ok_or("symbol not rendered")?;
        let same_row = |rect: &egui::Rect| (rect.center().y - symbol.center().y).abs() < 3.0;
        let point = match target {
            "star" => labels
                .iter()
                .find(|(text, rect)| text == "★" && same_row(rect))
                .map(|(_, rect)| rect.center())
                .ok_or("star missing")?,
            "symbol" => symbol.center(),
            _ => {
                let dash = labels
                    .iter()
                    .filter(|(text, rect)| text == "—" && same_row(rect))
                    .map(|(_, rect)| *rect)
                    .max_by(|a, b| a.right().total_cmp(&b.right()))
                    .ok_or("blank column missing")?;
                egui::pos2(dash.right() + 45.0, symbol.center().y)
            }
        };
        click(&context, &mut model, &mut workspaces, &mut open, point);
        if target == "star" {
            assert_eq!(model.preferences.selected_symbol, "BTC/USDC");
            assert!(
                !model
                    .preferences
                    .favorite_symbols
                    .iter()
                    .any(|s| s == "ETH/USDC")
            );
        } else {
            assert_eq!(
                model.preferences.selected_symbol, "ETH/USDC",
                "hit target {target}"
            );
            assert!(!open);
        }
    }
    Ok(())
}
