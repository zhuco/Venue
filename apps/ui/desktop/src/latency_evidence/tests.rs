use super::*;
fn stamp(utc_ms: u64, mono_ms: u64) -> Stamp {
    Stamp { utc_ms, mono_ms }
}
fn calibration() -> Calibration {
    Calibration {
        verified: true,
        local_minus_exchange_ms: 100,
        uncertainty_ms: 10,
        venue: "Binance".into(),
        source: "offline clock fixture".into(),
    }
}
#[test]
fn clock_offset_uncertainty_jump_and_missing_calibration_do_not_manufacture_passes() {
    let mut s = Sample::empty("market_last_price", "fixture".into());
    s.event_ms = Some(1000);
    s.finish(stamp(1250, 250), &calibration(), stamp(1000, 0));
    assert_eq!(s.raw_event_to_framebuffer_ms, Some(250));
    assert_eq!(s.corrected_event_to_framebuffer_ms, Some(150));
    assert_eq!(s.upper_bound_ms, Some(160));
    let mut missing = Sample::empty("market_last_price", "fixture".into());
    missing.event_ms = Some(1000);
    missing.finish(stamp(1250, 250), &Calibration::default(), stamp(1000, 0));
    assert_eq!(missing.upper_bound_ms, None);
    let mut jumped = missing.clone();
    jumped.finish(stamp(1550, 250), &calibration(), stamp(1000, 0));
    assert!(!jumped.clock_valid);
    assert_eq!(jumped.upper_bound_ms, None);
    let mut negative = missing.clone();
    negative.finish(stamp(1050, 50), &calibration(), stamp(1000, 0));
    assert_eq!(negative.corrected_event_to_framebuffer_ms, Some(-50));
    assert_eq!(negative.upper_bound_ms, None);
    let mut order = Sample::empty("terminal_order_line", "fixture".into());
    order.click = Some(stamp(1000, 0));
    order.finish(stamp(800, 250), &Calibration::default(), stamp(1000, 0));
    assert_eq!(order.upper_bound_ms, Some(250));
    assert!(!order.clock_valid);
}
#[test]
fn bounded_readback_requires_matching_frame_and_reports_loss() {
    let mut c = Capture::default();
    c.start(calibration(), stamp(1000, 0));
    for i in 0..LIMIT + 3 {
        let mut s = Sample::empty("terminal_order_line", format!("fixture-{i}"));
        s.click = Some(stamp(1000, 0));
        c.flight = Some(Flight {
            token: i as u64,
            sent: stamp(1100, 100),
            samples: vec![s],
        });
        c.finish_frame(i as u64 + 1, stamp(1200, 200));
        assert!(c.flight.is_some());
        c.finish_frame(i as u64, stamp(1200, 200));
    }
    assert_eq!(c.samples.len(), LIMIT);
    assert_eq!(c.dropped, 3);
    c.stage(Sample::empty("market_last_price", "same-event".into()));
    c.stage(Sample::empty("market_last_price", "same-event".into()));
    assert_eq!(c.staged.len(), 1);
    c.flight = Some(Flight {
        token: 999,
        sent: stamp(1000, 0),
        samples: c.staged.clone(),
    });
    c.tick(stamp(7000, 6000));
    assert!(c.flight.is_none());
    assert_eq!(c.incomplete, 1);
    c.tick(stamp(121000, WINDOW_MS));
    assert!(!c.active);
    let json = panel::export(&c).unwrap();
    assert!(json.contains("NOT_RUN") && json.contains("p95_ms"));
}
#[test]
fn nearest_rank_percentiles_keep_invalid_samples_out_of_denominator() {
    let mut samples = VecDeque::new();
    for value in 1..=100 {
        let mut s = Sample::empty("market_last_price", value.to_string());
        s.upper_bound_ms = Some(value);
        samples.push_back(s);
    }
    samples.push_back(Sample::empty("market_last_price", "invalid".into()));
    let d = panel::distribution(&samples, "market_last_price", 300);
    assert_eq!(
        (d.eligible, d.excluded, d.p50_ms, d.p95_ms),
        (100, 1, Some(50), Some(95))
    );
    assert_eq!(
        panel::distribution(&VecDeque::new(), "market_last_price", 300).p95_ms,
        None
    );
}
#[test]
fn safe_signed_fixture_requires_identity_facts_and_reconciled_state()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::account_scope::tests::{id, model, projection, receipt};
    let mut model = model();
    let scope = model.confirmed_account_scope().ok_or("scope")?;
    capture().lock().start(Calibration::default(), now());
    click(Some(scope.clone()), &id(81), "BTC/USDC", now());
    let mut row = receipt(1);
    row.state = ExecutorCommandState::Accepted;
    row.native_order_id = Some("123".into());
    model.execution.apply_terminal_execution(row.clone());
    let selection = crate::trading::TerminalOrderSelection {
        credential_id: scope.credential_id.clone(),
        trading_account_id: scope.trading_account_id.clone(),
        symbol: "BTC/USDC".parse()?,
        native_order_id: "123".into(),
    };
    summary(&scope, &row);
    order_painted(&selection);
    assert!(capture().lock().staged.is_empty());
    let mut p = projection(1);
    p.open_orders
        .push(venue_control_protocol::kol::TerminalOpenOrder {
            client_order_id: "original-client-identity".into(),
            native_order_id: Some("123".into()),
            symbol: "BTC/USDC".parse()?,
            order_side: venue_domain::OrderSide::Buy,
            position_side: venue_domain::PositionSide::Long,
            quantity: 1.into(),
            filled_quantity: Some(0.into()),
            limit_price: Some(100.into()),
            time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
            post_only: true,
            reduce_only: false,
            state: venue_control_protocol::kol::TerminalOrderState::New,
            created_ms: Some(p.observed_ms),
        });
    projection_received(&scope, &p);
    row.state = ExecutorCommandState::ReconcileRequired;
    model.execution.apply_terminal_execution(row.clone());
    bind_orders(&model, &p);
    order_painted(&selection);
    assert!(capture().lock().staged.is_empty());
    row.state = ExecutorCommandState::Reconciled;
    model.execution.apply_terminal_execution(row);
    bind_orders(&model, &p);
    let mut crossed = selection.clone();
    crossed.trading_account_id = id(12);
    order_painted(&crossed);
    assert!(capture().lock().staged.is_empty());
    order_painted(&selection);
    assert_eq!(capture().lock().staged.len(), 1);
    order_painted(&selection);
    assert_eq!(capture().lock().staged.len(), 1);
    assert!(capture().lock().samples.is_empty()); // Paint submission alone is not a completed sample.
    let ctx = egui::Context::default();
    let mut output = ctx.run_ui(egui::RawInput::default(), |ui| end_pass(ui.ctx()));
    output.textures_delta.clear();
    let token = capture()
        .lock()
        .flight
        .as_ref()
        .ok_or("missing screenshot request")?
        .token;
    assert!(capture().lock().samples.is_empty());
    let input = egui::RawInput {
        events: vec![egui::Event::Screenshot {
            viewport_id: egui::ViewportId::ROOT,
            user_data: egui::UserData::new(FrameToken(token)),
            image: std::sync::Arc::new(egui::ColorImage::filled([8, 8], egui::Color32::BLACK)),
        }],
        ..Default::default()
    };
    let mut output = ctx.run_ui(input, |ui| begin_pass(ui.ctx(), Some(scope.clone())));
    output.textures_delta.clear();
    assert_eq!(capture().lock().samples.len(), 1);
    assert!(capture().lock().samples[0].framebuffer_received.is_some());
    click(Some(scope.clone()), &id(82), "BTC/USDC", now());
    model.begin_account_selection(id(2));
    begin_pass(&egui::Context::default(), model.confirmed_account_scope());
    assert!(capture().lock().pending.is_empty());
    assert!(capture().lock().staged.is_empty());
    *capture().lock() = Capture::default();
    Ok(())
}
