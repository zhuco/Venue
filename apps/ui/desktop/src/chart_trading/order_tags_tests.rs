use super::*;
use crate::chart_trading::ChartTradingSettings;

fn selection() -> Result<TerminalOrderSelection, Box<dyn std::error::Error>> {
    Ok(TerminalOrderSelection {
        credential_id: "00000000-0000-4000-8000-000000000001".into(),
        trading_account_id: "00000000-0000-4000-8000-000000000002".into(),
        symbol: "DOGE/USDC".parse()?,
        native_order_id: "order-a".into(),
    })
}

struct Harness {
    context: egui::Context,
    viewport: crate::chart::ChartViewport,
    overlays: Vec<ChartOverlay>,
    body: Rect,
    cancel: Rect,
    texts: Vec<String>,
    textures: Vec<serde_json::Value>,
    selected: Option<Decimal>,
}

impl Harness {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let context = egui::Context::default();
        crate::theme::apply(&context);
        Ok(Self {
            context,
            viewport: Default::default(),
            body: Rect::NOTHING,
            cancel: Rect::NOTHING,
            textures: vec![],
            texts: vec![],
            selected: None,
            overlays: vec![ChartOverlay {
                price: Decimal::new(8727, 5),
                label: "只做Maker".into(),
                color: theme::SELL,
                time_ms: None,
                line: true,
                tick: false,
                badge: Some(TradingBadge {
                    language: crate::i18n::Language::SimplifiedChinese,
                    quantity: Some("289".into()),
                    stale: false,
                    pending: false,
                    provisional: false,
                    pnl: None,
                    selection: Some(selection()?),
                }),
            }],
        })
    }

    fn frame(&mut self, events: Vec<egui::Event>) -> egui::FullOutput {
        let bars = (0..150)
            .map(|index| {
                let close = Decimal::new(8400 + ((index * 17) % 370), 5);
                venue_control_protocol::UiBar {
                    open_time_ms: index as u64 * 60_000,
                    open: close + Decimal::new(if index % 3 == 0 { -8 } else { 8 }, 5),
                    high: close + Decimal::new(20, 5),
                    low: close - Decimal::new(20, 5),
                    close,
                    volume: Decimal::ONE,
                }
            })
            .collect::<Vec<_>>();
        let mut output = self.context.run_ui(
            egui::RawInput {
                screen_rect: Some(Rect::from_min_size(Pos2::ZERO, egui::vec2(520.0, 410.0))),
                events,
                ..Default::default()
            },
            |ui| {
                ui.painter()
                    .rect_filled(ui.max_rect(), 0, theme::BG_SECONDARY);
                ui.label("委托交互 · 离线测试数据");
                let mut settings = crate::chart_settings::ChartDisplaySettings::default();
                settings.volume.enabled = false;
                self.selected = crate::chart_view::candle_plot(
                    ui,
                    &bars,
                    &[],
                    &mut self.viewport,
                    crate::i18n::Language::SimplifiedChinese,
                    &settings,
                    (5, 0),
                    crate::chart::ChartInterval::OneMinute,
                    None,
                    None,
                    &ChartTradingSettings::default(),
                    &self.overlays,
                    (None, None),
                );
                if let Some(selection) = self
                    .overlays
                    .first()
                    .and_then(|overlay| overlay.badge.as_ref())
                    .and_then(|badge| badge.selection.as_ref())
                {
                    let id = ui.id().with(order_id(selection));
                    if let Some(response) = ui.ctx().read_response(id.with("cancel")) {
                        self.cancel = response.rect;
                    }
                    if let Some(response) = ui.ctx().read_response(id.with("drag")) {
                        self.body = response.rect;
                    }
                }
            },
        );
        self.texts.clear();
        fn text(shape: &egui::Shape, texts: &mut Vec<String>) {
            match shape {
                egui::Shape::Text(shape) => texts.push(shape.galley.job.text.clone()),
                egui::Shape::Vec(shapes) => shapes.iter().for_each(|shape| text(shape, texts)),
                _ => (),
            }
        }
        for shape in &output.shapes {
            text(&shape.shape, &mut self.texts);
        }
        if std::env::var_os("VENUE_CHART_TAG_PREVIEW").is_some() {
            for (id, deltas) in &output.textures_delta.set {
                for delta in deltas {
                    let egui::ImageData::Color(image) = &delta.image;
                    self.textures.push(serde_json::json!({ "id": format!("{id:?}"), "pos": delta.pos,
                        "size": image.size, "pixels": image.pixels.iter().flat_map(|pixel| pixel.to_array()).collect::<Vec<_>>() }));
                }
            }
        }
        output.textures_delta.clear();
        output
    }

    fn press(&mut self, pos: Pos2, pressed: bool) {
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

    fn action(&self) -> Option<Interaction> {
        self.context.data(|data| data.get_temp(action_id()))
    }
}

#[test]
fn tag_x_targets_exact_order_and_does_not_select_chart_price()
-> Result<(), Box<dyn std::error::Error>> {
    let mut harness = Harness::new()?;
    harness.frame(vec![]);
    harness.frame(vec![]);
    assert_eq!(harness.cancel.width(), 24.0);
    assert!(harness.cancel.left() >= harness.body.right());
    let point = harness.cancel.center();
    harness.press(point, true);
    harness.press(point, false);
    assert!(
        matches!(harness.action(), Some(Interaction::Cancel(target)) if target == selection()?)
    );
    assert!(harness.selected.is_none());
    assert_eq!(harness.viewport.right_padding(), 0);
    Ok(())
}

#[test]
fn drag_previews_price_without_panning_or_cancelling() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = Harness::new()?;
    harness.frame(vec![]);
    harness.frame(vec![]);
    let point = harness.body.center();
    harness.press(point, true);
    let next = point + egui::vec2(40.0, 65.0);
    harness.frame(vec![egui::Event::PointerMoved(next)]);
    assert!(harness.texts.iter().any(|text| text.contains("未提交")));
    assert!(harness.action().is_none());
    harness.press(next, false);
    assert!(
        matches!(harness.action(), Some(Interaction::Preview(target, old, new))
        if target == selection()? && old == Decimal::new(8727, 5) && new < old)
    );
    assert_eq!(harness.viewport.right_padding(), 0);
    assert!(harness.selected.is_none());
    assert_eq!(harness.overlays[0].price, Decimal::new(8727, 5));
    Ok(())
}

#[test]
fn stale_and_pending_tags_disable_order_actions() -> Result<(), Box<dyn std::error::Error>> {
    for pending in [false, true] {
        let mut harness = Harness::new()?;
        if pending && let Some(badge) = harness.overlays[0].badge.as_mut() {
            badge.pending = true;
        } else if let Some(badge) = harness.overlays[0].badge.as_mut() {
            badge.stale = true;
        }
        harness.frame(vec![]);
        harness.frame(vec![]);
        let point = harness.cancel.center();
        harness.press(point, true);
        harness.press(point, false);
        assert!(harness.action().is_none());
        assert!(harness.selected.is_none());
    }
    Ok(())
}

#[test]
fn escape_or_release_outside_aborts_price_preview() -> Result<(), Box<dyn std::error::Error>> {
    for escape in [false, true] {
        let mut harness = Harness::new()?;
        harness.frame(vec![]);
        harness.frame(vec![]);
        let point = harness.body.center();
        harness.press(point, true);
        let next = point + egui::vec2(0.0, 60.0);
        harness.frame(vec![egui::Event::PointerMoved(next)]);
        if escape {
            harness.frame(vec![egui::Event::Key {
                key: egui::Key::Escape,
                physical_key: None,
                pressed: true,
                repeat: false,
                modifiers: egui::Modifiers::NONE,
            }]);
        }
        harness.press(
            if escape {
                next
            } else {
                egui::pos2(600.0, 600.0)
            },
            false,
        );
        assert!(harness.action().is_none());
    }
    Ok(())
}

#[test]
fn order_identity_includes_account_credential_and_symbol() -> Result<(), Box<dyn std::error::Error>>
{
    let original = selection()?;
    for changed in [0, 1, 2] {
        let mut other = original.clone();
        match changed {
            0 => other.credential_id = "other".into(),
            1 => other.trading_account_id = "other".into(),
            _ => other.symbol = "BTC/USDC".parse()?,
        }
        assert_ne!(order_id(&original), order_id(&other));
    }
    assert!(!target_is_current(
        &crate::model::AppModel::new(Default::default()),
        &original
    ));
    Ok(())
}

#[test]
fn order_tags_fixture_preview() -> Result<(), Box<dyn std::error::Error>> {
    let mut harness = Harness::new()?;
    for (index, price) in [8679, 8616, 8586].into_iter().enumerate() {
        let mut overlay = harness.overlays[0].clone();
        overlay.price = Decimal::new(price, 5);
        if index == 0 {
            overlay.label = "限价委托".into();
        }
        if let Some(badge) = &mut overlay.badge {
            if let Some(selection) = &mut badge.selection {
                selection.native_order_id = format!("order-{index}");
            }
        }
        harness.overlays.push(overlay);
    }
    harness.overlays.push(ChartOverlay {
        price: Decimal::new(8524, 5),
        label: "多仓".into(),
        color: theme::BUY,
        time_ms: None,
        line: true,
        tick: false,
        badge: Some(TradingBadge {
            language: crate::i18n::Language::SimplifiedChinese,
            quantity: Some("1175".into()),
            stale: false,
            pending: false,
            provisional: false,
            pnl: Some(Decimal::new(-148, 2)),
            selection: None,
        }),
    });
    harness.frame(vec![]);
    let output = harness.frame(vec![]);
    assert!(harness.texts.iter().any(|text| text.contains("盈亏")));
    assert!(harness.texts.iter().any(|text| text.contains("限价委托")));
    if let Some(path) = std::env::var_os("VENUE_CHART_TAG_PREVIEW") {
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
                &serde_json::json!({ "size": [520, 410], "textures": harness.textures, "meshes": meshes }),
            )?,
        )?;
    }
    Ok(())
}

fn projection()
-> Result<venue_control_protocol::kol::TerminalAccountProjection, Box<dyn std::error::Error>> {
    use venue_control_protocol::kol::*;
    let selection = selection()?;
    let now = crate::account_center::now_ms();
    Ok(TerminalAccountProjection {
        schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
        credential_id: selection.credential_id,
        trading_account_id: selection.trading_account_id,
        observed_ms: now,
        persisted_ms: now,
        private_generation: 1,
        position_mode: TerminalPositionMode::Hedge,
        positions: vec![],
        position_history: vec![],
        fills: vec![],
        assets: vec![],
        open_orders: vec![TerminalOpenOrder {
            symbol: selection.symbol,
            native_order_id: Some(selection.native_order_id),
            client_order_id: "fixture".into(),
            order_side: venue_domain::OrderSide::Sell,
            position_side: venue_domain::PositionSide::Short,
            quantity: Decimal::from(300),
            filled_quantity: Some(Decimal::from(11)),
            limit_price: Some(Decimal::new(8727, 5)),
            time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
            post_only: true,
            reduce_only: false,
            state: TerminalOrderState::PartiallyFilled,
            created_ms: Some(now),
        }],
    })
}

#[test]
fn cancel_pending_keeps_unknown_and_waits_for_matching_account_absence()
-> Result<(), Box<dyn std::error::Error>> {
    let target = selection()?;
    let mut state = OrderTagState::default();
    state.pending.push((target.clone(), "request-a".into()));
    state.submission_failed("request-a", false);
    assert!(state.is_pending(&target));
    state.submission_failed("other-request", true);
    assert!(state.is_pending(&target));
    state.observe(&projection()?);
    assert!(state.is_pending(&target));
    let mut empty = projection()?;
    empty.open_orders.clear();
    empty.credential_id = "other-credential".into();
    state.observe(&empty);
    assert!(state.is_pending(&target));
    empty.credential_id = target.credential_id.clone();
    state.observe(&empty);
    assert!(!state.is_pending(&target));
    state.pending.push((target.clone(), "request-b".into()));
    state.submission_failed("request-b", true);
    assert!(!state.is_pending(&target));
    Ok(())
}

#[test]
fn action_revalidates_account_and_order_and_unknown_quantity_is_not_zero()
-> Result<(), Box<dyn std::error::Error>> {
    use venue_control_protocol::accounts::{AccountOverview, UserSummary};
    let target = selection()?;
    let mut model = crate::model::AppModel::new(Default::default());
    model.preferences.execution_account_id = Some(target.trading_account_id.clone());
    model.account_overview = Some(AccountOverview {
        user: UserSummary {
            user_id: "fixture-user".into(),
            username: "fixture".into(),
        },
        credentials: vec![],
        selected_credential_id: Some(target.credential_id.clone()),
    });
    model
        .execution
        .apply_private(Some(projection()?), &mut model.trade_dock);
    assert!(target_is_current(&model, &target));
    let lines = super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
    assert_eq!(
        lines[0]
            .badge
            .as_ref()
            .and_then(|badge| badge.quantity.as_deref()),
        Some("289")
    );
    assert!(!lines[0].label.contains("待刷新"));
    let mut changed = target.clone();
    changed.native_order_id = "different-order".into();
    assert!(!target_is_current(&model, &changed));
    changed = target.clone();
    changed.credential_id = "different-credential".into();
    assert!(!target_is_current(&model, &changed));
    model.preferences.execution_account_id = Some("other-account".into());
    assert!(!target_is_current(&model, &target));
    model.preferences.execution_account_id = Some(target.trading_account_id.clone());
    let mut unknown = projection()?;
    unknown.open_orders[0].filled_quantity = None;
    model
        .execution
        .apply_private(Some(unknown), &mut model.trade_dock);
    let lines = super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
    assert_eq!(
        lines[0]
            .badge
            .as_ref()
            .and_then(|badge| badge.quantity.as_deref()),
        Some("—")
    );
    model.execution.private_error = Some("fixture disconnect".into());
    assert!(!target_is_current(&model, &target));
    Ok(())
}

fn request_fixture()
-> Result<venue_control_protocol::kol::TerminalOrderRequest, Box<dyn std::error::Error>> {
    use venue_control_protocol::kol::*;
    Ok(TerminalOrderRequest {
        schema_version: TERMINAL_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000003".into(),
        credential_id: selection()?.credential_id,
        symbol: "DOGE/USDC".parse()?,
        action: TerminalAction::OpenShort,
        order_kind: TerminalOrderKind::LimitPostOnly,
        quote_notional: Decimal::from(25),
        limit_price: Some(Decimal::new(8727, 5)),
        close_quantity_cap: None,
        market_risk_confirmed: false,
    })
}

fn model_fixture() -> Result<crate::model::AppModel, Box<dyn std::error::Error>> {
    use venue_control_protocol::accounts::{AccountOverview, UserSummary};
    let target = selection()?;
    let mut model = crate::model::AppModel::new(Default::default());
    model.preferences.execution_account_id = Some(target.trading_account_id);
    model.account_overview = Some(AccountOverview {
        user: UserSummary {
            user_id: "fixture-user".into(),
            username: "fixture".into(),
        },
        credentials: vec![],
        selected_credential_id: Some(target.credential_id),
    });
    Ok(model)
}

#[test]
fn sent_order_is_visible_in_same_frame_with_immediate_repaint_and_no_status_copy()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = model_fixture()?;
    let request = request_fixture()?;
    request.validate()?;
    let account = selection()?.trading_account_id;
    let context = egui::Context::default();
    for _ in 0..3 {
        let mut output = context.run_ui(Default::default(), |_| {});
        output.textures_delta.clear();
    }
    let mut output = context.run_ui(
        egui::RawInput {
            time: Some(1.0),
            ..Default::default()
        },
        |ui| {
            model.execution.chart_orders.submitted_order(
                account.clone(),
                request.clone(),
                ui.ctx(),
            );
            let overlays =
                super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
            assert_eq!(overlays.len(), 1);
            assert_eq!(overlays[0].label, "只做Maker");
            assert_eq!(overlays[0].price, Decimal::new(8727, 5));
            assert!(
                overlays[0]
                    .badge
                    .as_ref()
                    .is_some_and(|badge| badge.provisional)
            );
            assert!(model.execution.private_projection.is_none());
        },
    );
    assert!(
        output
            .viewport_output
            .get(&egui::ViewportId::ROOT)
            .is_some_and(|viewport| viewport.repaint_delay < std::time::Duration::from_millis(100))
    );
    output.textures_delta.clear();
    model
        .execution
        .chart_orders
        .submission_failed(&request.request_id, false);
    assert_eq!(
        super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default()).len(),
        1
    );
    assert!(super::super::collect(&model, "BTC/USDC", &ChartTradingSettings::default()).is_empty());
    model
        .execution
        .chart_orders
        .submission_failed(&request.request_id, true);
    assert!(
        super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default()).is_empty()
    );
    Ok(())
}

#[test]
fn sent_cancel_hides_immediately_and_uncertainty_restores_without_mutating_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = model_fixture()?;
    model
        .execution
        .apply_private(Some(projection()?), &mut model.trade_dock);
    let target = selection()?;
    let context = egui::Context::default();
    let mut output = context.run_ui(Default::default(), |ui| {
        model.execution.chart_orders.submitted_cancel(
            target.clone(),
            "cancel-request".into(),
            ui.ctx(),
        );
        assert!(
            super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default()).is_empty()
        );
        assert_eq!(
            model
                .execution
                .private_projection
                .as_ref()
                .map(|projection| projection.open_orders.len()),
            Some(1)
        );
    });
    assert!(
        output
            .viewport_output
            .get(&egui::ViewportId::ROOT)
            .is_some_and(|viewport| viewport.repaint_delay < std::time::Duration::from_millis(100))
    );
    output.textures_delta.clear();
    model
        .execution
        .position_submission_failed("cancel-request", false);
    let overlays = super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
    assert_eq!(overlays.len(), 1);
    assert_eq!(overlays[0].label, "只做Maker");
    assert!(
        overlays[0]
            .badge
            .as_ref()
            .is_some_and(|badge| badge.pending)
    );
    model
        .execution
        .position_submission_failed("cancel-request", true);
    assert!(!model.execution.chart_orders.is_pending(&target));
    Ok(())
}

#[test]
fn request_tag_merges_by_exact_receipt_identity_not_price_or_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    use venue_control_protocol::kol::*;
    let mut model = model_fixture()?;
    let old = projection()?;
    model
        .execution
        .apply_private(Some(old.clone()), &mut model.trade_dock);
    let request = request_fixture()?;
    model.execution.chart_orders.submitted_order(
        selection()?.trading_account_id,
        request.clone(),
        &egui::Context::default(),
    );
    let receipt = ExecutorCommandSummary {
        command_id: "00000000-0000-4000-8000-000000000004".into(),
        request_id: Some(request.request_id),
        origin: ExecutorCommandOrigin::Terminal,
        phase: ExecutorCommandPhase::Open,
        trading_account_id: selection()?.trading_account_id,
        symbol: request.symbol,
        position_side: Some(venue_domain::PositionSide::Short),
        order_side: Some(venue_domain::OrderSide::Sell),
        order_kind: ExecutorOrderKind::LimitPostOnly,
        requested_quantity: Some(Decimal::from(289)),
        limit_price: request.limit_price,
        state: ExecutorCommandState::Reconciled,
        native_order_id: Some("new-order".into()),
        created_ms: old.observed_ms,
        updated_ms: old.observed_ms + 1,
        sanitized_error_code: None,
    };
    model.execution.apply_terminal_execution(receipt.clone());
    assert_eq!(
        super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default()).len(),
        2
    );
    let mut latest = old;
    latest.observed_ms += 2;
    latest.persisted_ms += 2;
    latest.open_orders[0].native_order_id = receipt.native_order_id;
    model
        .execution
        .apply_private(Some(latest), &mut model.trade_dock);
    let overlays = super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
    assert_eq!(overlays.len(), 1);
    assert!(
        overlays[0]
            .badge
            .as_ref()
            .is_some_and(|badge| !badge.provisional)
    );
    Ok(())
}
