use super::*;
use venue_control_protocol::kol::{
    TerminalPosition, TerminalPositionHistoryEntry, TerminalPositionMode,
};

fn fill(id: &str, side: OrderSide, quantity: i64, price: i64, time: u64) -> TerminalFill {
    TerminalFill {
        native_trade_id: id.into(),
        native_order_id: id.into(),
        symbol: Symbol::new("BTC", "USDT").expect("valid static symbol"),
        order_side: side,
        position_side: PositionSide::Long,
        quantity: Decimal::from(quantity),
        price: Decimal::from(price),
        maker: Some(true),
        occurred_ms: Some(time),
    }
}

fn projection(fills: Vec<TerminalFill>) -> TerminalAccountProjection {
    TerminalAccountProjection {
        schema_version: 1,
        credential_id: "fixture".into(),
        trading_account_id: "fixture".into(),
        observed_ms: 100,
        persisted_ms: 100,
        private_generation: 1,
        position_mode: TerminalPositionMode::Hedge,
        positions: vec![],
        position_history: vec![flat(1), flat(90)],
        open_orders: vec![],
        conditional_orders: vec![],
        fills,
        assets: vec![],
    }
}

fn flat(time: u64) -> TerminalPositionHistoryEntry {
    TerminalPositionHistoryEntry {
        observed_ms: time,
        position: TerminalPosition {
            symbol: Symbol::new("BTC", "USDT").expect("valid static symbol"),
            position_side: PositionSide::Long,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: None,
        },
    }
}

#[test]
fn weighted_cost_survives_partial_close_then_add_and_dedup() {
    let mut facts = projection(vec![
        fill("1", OrderSide::Buy, 2, 100, 10),
        fill("2", OrderSide::Sell, 1, 120, 20),
        fill("3", OrderSide::Buy, 1, 140, 30),
        fill("4", OrderSide::Sell, 2, 150, 40),
    ]);
    facts.fills.push(facts.fills[0].clone());
    facts.fills.reverse();
    let rows = rebuild(&facts);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].pnl, Decimal::from(80));
    assert_eq!(rows[0].opened, Some(10));
    assert_eq!(rows[0].closed, Some(40));
    assert_eq!(rows[0].closed_qty, Decimal::from(3));
}

#[test]
fn missing_opening_boundary_never_invents_cost_or_open_time() {
    let mut facts = projection(vec![fill("1", OrderSide::Sell, 1, 120, 20)]);
    facts.position_history.clear();
    let rows = rebuild(&facts);
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].known_start);
    assert_eq!(rows[0].opened, None);
    assert_eq!(rows[0].closed, None);
}

#[test]
fn old_flat_before_a_nonzero_snapshot_cannot_anchor_first_fill() {
    let mut facts = projection(vec![fill("1", OrderSide::Buy, 1, 120, 20)]);
    let mut nonzero = flat(15);
    nonzero.position.quantity = Decimal::ONE;
    facts.position_history.push(nonzero);
    assert!(!rebuild(&facts)[0].known_start);
}

#[test]
fn flat_boundaries_separate_cycles_and_long_short_are_independent() {
    let mut facts = projection(vec![
        fill("1", OrderSide::Buy, 1, 100, 10),
        fill("2", OrderSide::Sell, 1, 110, 20),
        fill("3", OrderSide::Buy, 1, 120, 40),
        fill("4", OrderSide::Sell, 1, 115, 50),
    ]);
    facts.position_history.push(flat(30));
    let mut short = fill("1", OrderSide::Sell, 1, 200, 10);
    short.position_side = PositionSide::Short;
    facts.fills.push(short);
    let rows = rebuild(&facts);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].pnl, Decimal::from(-5));
    assert_eq!(rows[1].pnl, Decimal::from(10));
    assert!(!rows[2].known_start);
}

#[test]
fn contradictory_or_unordered_fills_and_overflow_do_not_produce_estimates() {
    let original = fill("1", OrderSide::Buy, 1, 100, 10);
    let mut other = original.clone();
    other.price = Decimal::from(101);
    assert!(rebuild(&projection(vec![original.clone(), other])).is_empty());
    let other = fill("2", OrderSide::Sell, 1, 110, 10);
    assert!(rebuild(&projection(vec![original.clone(), other])).is_empty());
    let mut invalid = original.clone();
    invalid.occurred_ms = None;
    assert!(rebuild(&projection(vec![invalid])).is_empty());
    let mut huge = original;
    huge.quantity = Decimal::MAX;
    assert!(rebuild(&projection(vec![huge])).is_empty());
}

#[test]
fn net_fills_without_start_position_are_not_interpreted_as_hedge() {
    let mut row = fill("1", OrderSide::Buy, 1, 100, 10);
    row.position_side = PositionSide::Net;
    assert!(rebuild(&projection(vec![row])).is_empty());
}

#[test]
fn history_time_includes_year_seconds_and_timezone() {
    assert_eq!(history_time(1_000), "1970-01-01 00:00:01.000 UTC");
}

#[test]
fn history_view_renders_both_languages_and_account_clear_removes_rows() {
    fn texts(shape: &egui::Shape, output: &mut String) {
        match shape {
            egui::Shape::Text(text) => output.push_str(&text.galley.job.text),
            egui::Shape::Vec(shapes) => shapes.iter().for_each(|shape| texts(shape, output)),
            _ => {}
        }
    }
    let facts = projection(vec![
        fill("1", OrderSide::Buy, 1, 100, 10),
        fill("2", OrderSide::Sell, 1, 120, 20),
    ]);
    let mut model = AppModel::new(Default::default());
    model.execution.current_symbol = false;
    for (language, expected) in [
        (Language::SimplifiedChinese, "毛 PnL（估算）"),
        (Language::English, "Gross PnL (est.)"),
    ] {
        model.preferences.language = language;
        model.execution.position_cycles = rebuild(&facts);
        let context = egui::Context::default();
        let mut rendered = String::new();
        for _ in 0..2 {
            let mut output = context.run_ui(
                egui::RawInput {
                    screen_rect: Some(egui::Rect::from_min_size(
                        egui::Pos2::ZERO,
                        egui::vec2(2200.0, 400.0),
                    )),
                    ..Default::default()
                },
                |ui| show(ui, &model),
            );
            rendered.clear();
            for shape in &output.shapes {
                texts(&shape.shape, &mut rendered);
            }
            output.textures_delta.clear();
        }
        assert!(rendered.contains(expected), "{rendered}");
        assert!(rendered.contains("20.0000 USDT"), "{rendered}");
        assert!(rendered.contains("UTC"), "{rendered}");
        model.execution.clear_account_view();
        assert!(model.execution.position_cycles.is_empty());
    }
}
