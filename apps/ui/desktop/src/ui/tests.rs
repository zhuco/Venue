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
            if size.y >= 700.0 {
                assert!(labels.iter().any(|(label, rect)| label
                    == text(language, TextKey::PriceValidity)
                    && screen.contains_rect(*rect)));
            }
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

#[test]
fn terminal_chrome_shows_product_name_and_hides_symbol_close_until_hover() {
    let context = egui::Context::default();
    theme::apply(&context);
    let mut model = AppModel::new(crate::model::Preferences::default());
    model.preferences.favorite_symbols = vec!["BTC/USDC".into()];
    model.preferences.selected_symbol = "BTC/USDC".into();
    let mut workspaces = Workspaces::default();
    let mut modules = false;
    let mut trading = false;
    let mut accounts = false;
    let mut picker = false;
    let render = |events: Vec<egui::Event>,
                  model: &mut AppModel,
                  workspaces: &mut Workspaces,
                  modules: &mut bool,
                  trading: &mut bool,
                  accounts: &mut bool,
                  picker: &mut bool| {
        let mut frame = context.run_ui(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    egui::vec2(1100.0, 700.0),
                )),
                events,
                ..Default::default()
            },
            |ui| show_top_bar(ui, model, workspaces, modules, trading, accounts, picker),
        );
        frame.textures_delta.clear();
        let mut labels = Vec::new();
        for shape in frame.shapes {
            collect_text(&shape.shape, &mut labels);
        }
        labels
    };
    let labels = render(
        vec![],
        &mut model,
        &mut workspaces,
        &mut modules,
        &mut trading,
        &mut accounts,
        &mut picker,
    );
    assert!(labels.iter().any(|(label, _)| label == "VenueFlow"));
    let symbol = labels
        .iter()
        .find(|(label, _)| label == "BTC/USDC")
        .map(|(_, rect)| *rect)
        .expect("symbol tab");
    assert!(
        !labels
            .iter()
            .any(|(label, rect)| label == "×" && rect.left() < 500.0)
    );
    let close_point = egui::pos2(symbol.left() + 136.0, symbol.top() + 14.0);
    let labels = render(
        vec![egui::Event::PointerMoved(close_point)],
        &mut model,
        &mut workspaces,
        &mut modules,
        &mut trading,
        &mut accounts,
        &mut picker,
    );
    assert!(
        labels
            .iter()
            .any(|(label, rect)| label == "×" && rect.left() < 500.0)
    );
}

#[test]
fn terminal_chrome_keeps_both_rows_at_the_top_of_the_window() {
    let context = egui::Context::default();
    theme::apply(&context);
    let mut model = AppModel::new(crate::model::Preferences::default());
    model.preferences.favorite_symbols = vec!["BTC/USDC".into()];
    model.preferences.selected_symbol = "BTC/USDC".into();
    let mut workspaces = Workspaces::default();
    let mut modules = false;
    let mut trading = false;
    let mut accounts = false;
    let mut picker = false;
    let mut frame = context.run_ui(
        egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(1900.0, 990.0),
            )),
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
            );
        },
    );
    frame.textures_delta.clear();
    let mut labels = Vec::new();
    for shape in frame.shapes {
        collect_text(&shape.shape, &mut labels);
    }
    let symbol = labels
        .iter()
        .find(|(label, _)| label == "BTC/USDC")
        .map(|(_, rect)| *rect)
        .expect("symbol tab");
    let trading = labels
        .iter()
        .find(|(label, _)| label == text(model.preferences.language, TextKey::TradingSettings))
        .map(|(_, rect)| *rect)
        .expect("trading settings");
    let add_tab = labels
        .iter()
        .find(|(label, _)| label == "+")
        .map(|(_, rect)| *rect)
        .expect("add tab");
    let layout = labels
        .iter()
        .find(|(label, _)| label.starts_with("布局管理") || label.starts_with("Layout"))
        .map(|(_, rect)| *rect)
        .expect("layout manager");
    assert!(
        symbol.top() <= 60.0,
        "symbol tabs start at {}px",
        symbol.top()
    );
    assert!(
        (add_tab.center().y - trading.center().y).abs() <= 4.0,
        "add_tab={add_tab:?}, trading={trading:?}"
    );
    assert!(
        (add_tab.center().y - layout.center().y).abs() <= 4.0,
        "add_tab={add_tab:?}, layout={layout:?}"
    );
    assert!(
        layout.left() >= 1_700.0,
        "layout manager is not right aligned"
    );
}

#[test]
fn status_bar_is_one_line_with_funds_visible_at_supported_widths() {
    for language in Language::ALL {
        for width in [850.0, 1100.0, 1680.0] {
            let context = egui::Context::default();
            theme::apply(&context);
            let model = AppModel::new(crate::model::Preferences {
                language,
                ..Default::default()
            });
            let mut labels = Vec::new();
            for _ in 0..3 {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(egui::Rect::from_min_size(
                            egui::Pos2::ZERO,
                            egui::vec2(width, 26.0),
                        )),
                        ..Default::default()
                    },
                    |ui| {
                        ui.spacing_mut().item_spacing = egui::Vec2::ZERO;
                        show_status_bar(ui, &model);
                        assert!(
                            ui.min_rect().height() <= 26.0,
                            "status height {}",
                            ui.min_rect().height()
                        );
                    },
                );
                output.textures_delta.clear();
                labels.clear();
                for shape in output.shapes {
                    collect_text(&shape.shape, &mut labels);
                }
            }
            for prefix in [
                text(language, TextKey::Equity),
                text(language, TextKey::MarginShort),
            ] {
                let rect = labels
                    .iter()
                    .find(|(label, _)| label.starts_with(prefix))
                    .map(|(_, rect)| rect);
                assert!(
                    rect.is_some_and(|r| r.right() <= width && r.bottom() <= 26.0),
                    "{prefix} missing/clipped at {width}: {rect:?}"
                );
            }
            let baselines = labels
                .iter()
                .map(|(_, rect)| rect.center().y)
                .collect::<Vec<_>>();
            assert!(baselines.iter().all(|y| (*y - baselines[0]).abs() < 3.0));
        }
    }
}

#[test]
fn order_panel_primary_buttons_fit_and_short_panels_can_scroll_to_cancellations() {
    for (size, scroll) in [
        (egui::vec2(680.0, 260.0), false),
        (egui::vec2(330.0, 160.0), true),
    ] {
        let context = egui::Context::default();
        theme::apply(&context);
        let mut model = AppModel::new(crate::model::Preferences::default());
        let mut labels = Vec::new();
        for frame in 0..30 {
            let mut events = vec![egui::Event::PointerMoved(egui::pos2(150.0, 90.0))];
            if scroll && frame == 3 {
                events.push(egui::Event::MouseWheel {
                    unit: egui::MouseWheelUnit::Point,
                    phase: egui::TouchPhase::Move,
                    delta: egui::vec2(0.0, -800.0),
                    modifiers: egui::Modifiers::NONE,
                });
            }
            let mut output = context.run_ui(
                egui::RawInput {
                    screen_rect: Some(egui::Rect::from_min_size(egui::Pos2::ZERO, size)),
                    time: Some(frame as f64 / 30.0),
                    events,
                    ..Default::default()
                },
                |ui| {
                    assert!(crate::trade_dock::controls(ui, &mut model).is_none());
                },
            );
            output.textures_delta.clear();
            labels.clear();
            for clipped in output.shapes {
                let mut contents = Vec::new();
                collect_text(&clipped.shape, &mut contents);
                labels.extend(
                    contents
                        .into_iter()
                        .filter(|(_, rect)| clipped.clip_rect.contains_rect(*rect)),
                );
            }
        }
        let expected: &[&str] = if scroll {
            &["撤当前", "撤全部"]
        } else {
            &["开多", "平多", "平空", "开空", "撤当前", "撤全部"]
        };
        for prefix in expected {
            assert!(
                labels
                    .iter()
                    .any(|(label, rect)| label.starts_with(prefix) && rect.right() <= size.x),
                "{prefix} not accessible in {size:?}: {labels:?}"
            );
        }
        assert!(!labels.iter().any(|(label, _)| label.contains("交易设置")
            || label.contains("快捷键 已启用")
            || label.contains("选择一个运行中的交易作用域")
            || label.starts_with("PnL")
            || label == "清除"
            || label == "回到市场"));
    }
}
