use super::*;
use crate::chart_trading::ChartTradingSettings;

#[test]
fn crossed_line_waits_for_newer_private_facts_without_changing_orders()
-> Result<(), Box<dyn std::error::Error>> {
    let target = selection()?;
    let mut facts = projection()?;
    let original = facts.clone();
    let mut state = OrderTagState::default();
    state.crossed_price(target.clone(), facts.observed_ms + 1);
    assert!(state.hidden(&target));
    assert!(!state.is_pending(&target));
    state.observe(&facts);
    assert!(state.hidden(&target));
    assert_eq!(facts, original);
    facts.observed_ms += 2;
    state.observe(&facts);
    assert!(!state.hidden(&target));
    state.crossed_price(target.clone(), facts.observed_ms + 1);
    facts.open_orders.clear();
    state.observe(&facts);
    assert!(!state.hidden(&target));
    Ok(())
}

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
    display: ChartTradingSettings,
    market_price: Option<Decimal>,
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
            display: Default::default(),
            market_price: None,
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
                    pnl: None,
                    position: None,
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
                    self.market_price,
                    None,
                    &self.display,
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
    assert_eq!(harness.cancel.width(), 20.0);
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
    harness.market_price = Some(Decimal::new(8660, 5));
    harness.overlays[0].label = "限价委托".into();
    let mut buy = harness.overlays[0].clone();
    buy.price = Decimal::new(8586, 5);
    buy.label = "只做Maker".into();
    buy.color = theme::BUY;
    if let Some(badge) = &mut buy.badge {
        if let Some(selection) = &mut badge.selection {
            selection.native_order_id = "buy-order".into();
        }
    }
    let mut nearby = buy.clone();
    nearby.price += Decimal::new(3, 5);
    if let Some(badge) = &mut nearby.badge {
        if let Some(selection) = &mut badge.selection {
            selection.native_order_id = "nearby-buy".into();
        }
    }
    harness.overlays.push(nearby);
    harness.overlays.push(buy);
    harness.overlays.push(ChartOverlay {
        price: Decimal::new(8524, 5),
        label: "多仓".into(),
        color: theme::POSITION_LINE,
        time_ms: None,
        line: true,
        tick: false,
        badge: Some(TradingBadge {
            language: crate::i18n::Language::SimplifiedChinese,
            quantity: Some("1175".into()),
            stale: false,
            pending: false,
            pnl: Some(Decimal::new(540, 2)),
            position: None,
            selection: None,
        }),
    });
    for (bar, color, count) in [(116_u64, theme::BUY, 3), (123, theme::SELL, 3)] {
        for fill in 0..count {
            harness.overlays.push(ChartOverlay {
                price: Decimal::new(8400 + ((bar * 17) % 370) as i64, 5),
                label: format!("成交 {fill}"),
                color,
                time_ms: Some(bar * 60_000 + fill * 1000),
                line: false,
                tick: false,
                badge: None,
            });
        }
    }
    harness.frame(vec![]);
    let output = harness.frame(vec![]);
    assert!(harness.texts.iter().any(|text| text.contains("盈亏")));
    assert!(harness.texts.iter().any(|text| text.contains("限价委托")));
    assert_eq!(harness.texts.iter().filter(|text| *text == "B").count(), 3);
    assert_eq!(harness.texts.iter().filter(|text| *text == "S").count(), 3);
    fn marker_positions(shape: &egui::Shape, letter: &str, positions: &mut Vec<Pos2>) {
        match shape {
            egui::Shape::Text(text) if text.galley.job.text == letter => positions.push(text.pos),
            egui::Shape::Vec(shapes) => {
                for shape in shapes {
                    marker_positions(shape, letter, positions);
                }
            }
            _ => (),
        }
    }
    for letter in ["B", "S"] {
        let mut positions = Vec::new();
        for shape in &output.shapes {
            marker_positions(&shape.shape, letter, &mut positions);
        }
        positions.sort_by(|a, b| a.y.total_cmp(&b.y));
        for pair in positions.windows(2) {
            assert!((pair[0].x - pair[1].x).abs() < 0.1);
            assert!(pair[1].y - pair[0].y >= 19.0, "fills must remain readable");
        }
    }
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
        conditional_orders: vec![],
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
    let target = selection()?;
    let mut model = model_fixture()?;
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

fn model_fixture() -> Result<crate::model::AppModel, Box<dyn std::error::Error>> {
    let target = selection()?;
    let mut model = crate::model::AppModel::new(Default::default());
    let mut overview = crate::account_scope::tests::overview(1);
    overview.credentials.truncate(1);
    overview.credentials[0].credential_id = target.credential_id.clone();
    overview.credentials[0].trading_account_id = Some(target.trading_account_id);
    overview.selected_credential_id = Some(target.credential_id);
    model.apply_account_overview(overview);
    Ok(model)
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
    assert_eq!(overlays[0].label, "开空 · 只做Maker");
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
fn signed_orders_have_one_tag_each_and_fills_remove_only_the_finished_order()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = model_fixture()?;
    let mut facts = projection()?;
    let mut second = facts.open_orders[0].clone();
    second.native_order_id = Some("same-price-second-order".into());
    second.client_order_id = "second-client".into();
    facts.open_orders.push(second);
    model
        .execution
        .apply_private(Some(facts.clone()), &mut model.trade_dock);
    let settings = ChartTradingSettings::default();
    let tags = super::super::collect(&model, "DOGE/USDC", &settings);
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].price, tags[1].price);
    // Partial fills retain the remaining amount, even for two orders at the same price.
    assert_eq!(
        tags[0].badge.as_ref().and_then(|b| b.quantity.as_deref()),
        Some("289")
    );
    facts.observed_ms += 1;
    facts.persisted_ms += 1;
    facts.open_orders.remove(0);
    model
        .execution
        .apply_private(Some(facts.clone()), &mut model.trade_dock);
    let tags = super::super::collect(&model, "DOGE/USDC", &settings);
    assert_eq!(tags.len(), 1);
    assert_eq!(
        tags[0]
            .badge
            .as_ref()
            .and_then(|b| b.selection.as_ref())
            .map(|s| s.native_order_id.as_str()),
        Some("same-price-second-order")
    );
    facts.observed_ms += 1;
    facts.persisted_ms += 1;
    facts.open_orders.clear();
    model
        .execution
        .apply_private(Some(facts), &mut model.trade_dock);
    assert!(super::super::collect(&model, "DOGE/USDC", &settings).is_empty());
    Ok(())
}

#[test]
fn realtime_pnl_tracks_quote_and_hedge_side_without_changing_signed_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = model_fixture()?;
    let mut facts = projection()?;
    facts.open_orders.clear();
    let position = venue_control_protocol::kol::TerminalPosition {
        symbol: "DOGE/USDC".parse()?,
        position_side: venue_domain::PositionSide::Long,
        quantity: Decimal::from(100),
        entry_price: Some(Decimal::ONE),
        mark_price: Some(Decimal::ONE),
    };
    facts.positions.push(position.clone());
    let mut short = position.clone();
    short.position_side = venue_domain::PositionSide::Short;
    facts.positions.push(short);
    model
        .execution
        .apply_private(Some(facts), &mut model.trade_dock);
    let now = crate::account_center::now_ms();
    for (price, expected) in [
        (Decimal::new(11, 1), Decimal::from(10)),
        (Decimal::new(9, 1), Decimal::from(-10)),
    ] {
        model.local_quotes.insert(
            "DOGE/USDC".into(),
            crate::model::MarketQuote {
                symbol: "DOGE/USDC".into(),
                last: price,
                change_percent_24h: Decimal::ZERO,
                quote_volume_24h: Some(Decimal::ZERO),
                exchange_time_ms: now,
                received_ms: now,
            },
        );
        let tags = super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
        assert_eq!(tags[0].badge.as_ref().and_then(|b| b.pnl), Some(expected));
        assert_eq!(tags[1].badge.as_ref().and_then(|b| b.pnl), Some(-expected));
        assert_eq!(tags[0].color, theme::POSITION_LINE);
        assert_ne!(tags[0].color, theme::BUY);
        assert_ne!(tags[0].color, theme::SELL);
        assert_ne!(tags[0].color, theme::WARNING);
    }
    assert_eq!(crate::execution_view::pnl_color(Decimal::ONE), theme::BUY);
    assert_eq!(crate::execution_view::pnl_color(-Decimal::ONE), theme::SELL);
    if let Some(quote) = model.local_quotes.get_mut("DOGE/USDC") {
        quote.exchange_time_ms = now - 16_000;
    }
    assert_eq!(
        crate::execution_view::live_position_pnl_value(&model, &position),
        Some(Decimal::ZERO)
    );
    let mut missing = position;
    missing.mark_price = None;
    assert_eq!(
        crate::execution_view::live_position_pnl_value(&model, &missing),
        None
    );
    // No further private-account refresh is needed for either chart or table PnL.
    let selection = crate::market::MarketSelection::binance_usd_m(
        "DOGE/USDC",
        crate::chart::ChartInterval::OneMinute,
    )?;
    let generation = model
        .local_markets
        .replace([selection.clone()])?
        .ok_or("generation")?;
    for (event_ms, price, expected) in [
        (now - 300, Decimal::new(12, 1), Decimal::from(20)),
        (now - 100, Decimal::new(8, 1), Decimal::from(-20)),
        (now - 200, Decimal::new(15, 1), Decimal::from(-20)),
    ] {
        model.local_markets.apply(crate::market::MarketEnvelope {
            generation,
            selection: selection.clone(),
            event_time_ms: event_ms,
            received_ms: now,
            payload: crate::market::MarketPayload::Trade(venue_control_protocol::UiTrade {
                trade_id: event_ms.to_string(),
                occurred_ms: event_ms,
                price,
                quantity: Decimal::ONE,
                aggressor: venue_control_protocol::AggressorSide::Buy,
            }),
        })?;
        assert_eq!(
            crate::execution_view::live_position_pnl_value(&model, &missing),
            Some(expected)
        );
        let tags = super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
        assert_eq!(tags[0].badge.as_ref().and_then(|b| b.pnl), Some(expected));
        assert_eq!(tags[1].badge.as_ref().and_then(|b| b.pnl), Some(-expected));
    }
    let mut net_short = missing.clone();
    net_short.position_side = venue_domain::PositionSide::Net;
    net_short.quantity = -net_short.quantity;
    assert_eq!(
        crate::execution_view::live_position_pnl_value(&model, &net_short),
        Some(Decimal::from(20))
    );
    net_short.entry_price = None;
    assert_eq!(
        crate::execution_view::live_position_pnl_value(&model, &net_short),
        None
    );
    Ok(())
}

#[test]
fn nearby_badges_overlap_at_exact_prices_and_lines_never_cross_badges()
-> Result<(), Box<dyn std::error::Error>> {
    let context = egui::Context::default();
    crate::theme::apply(&context);
    let mut orders = Harness::new()?.overlays;
    let mut nearby = orders[0].clone();
    nearby.price += Decimal::new(1, 5);
    if let Some(badge) = &mut nearby.badge {
        if let Some(selection) = &mut badge.selection {
            selection.native_order_id = "nearby".into();
        }
    }
    orders.push(nearby);
    let mut badges = Vec::new();
    let plot = Rect::from_min_size(Pos2::new(10.0, 10.0), egui::vec2(650.0, 300.0));
    let range = PriceRange::from_extrema(0.08, 0.09).ok_or("range")?;
    let mut output = context.run_ui(
        egui::RawInput {
            screen_rect: Some(Rect::from_min_size(Pos2::ZERO, egui::vec2(700.0, 400.0))),
            ..Default::default()
        },
        |ui| {
            super::super::draw(
                ui,
                ui.painter(),
                plot,
                &[],
                1,
                crate::chart::ChartInterval::OneMinute,
                range,
                &orders,
                5,
                &ChartTradingSettings::default(),
            );
            for overlay in &orders {
                if let Some(selection) = overlay.badge.as_ref().and_then(|b| b.selection.as_ref()) {
                    let id = ui.id().with(order_id(selection));
                    if let (Some(body), Some(cancel)) = (
                        ui.ctx().read_response(id.with("drag")),
                        ui.ctx().read_response(id.with("cancel")),
                    ) {
                        let rect = body.rect.union(cancel.rect);
                        let expected = range.price_to_y(
                            plot.top(),
                            plot.height(),
                            crate::model::decimal_to_f64(overlay.price),
                        );
                        assert!(expected.is_some_and(|y| (rect.center().y - y).abs() < 0.01));
                        badges.push(rect);
                    }
                }
            }
        },
    );
    output.textures_delta.clear();
    assert_eq!(badges.len(), 2);
    assert!(badges[0].intersects(badges[1]));
    assert_eq!(badges[0].left(), badges[1].left());
    let lines = output
        .shapes
        .iter()
        .filter_map(|shape| match &shape.shape {
            egui::Shape::LineSegment { points, stroke }
                if stroke.color == theme::SELL && (stroke.width - 1.25).abs() < 0.001 =>
            {
                Some(points)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!lines.is_empty());
    for points in lines {
        assert!((points[0].y - points[1].y).abs() < 0.01);
        assert!(
            badges
                .iter()
                .any(|b| (points[0].y - b.center().y).abs() < 0.01)
        );
        for badge in &badges {
            if points[0].y >= badge.top() && points[0].y <= badge.bottom() {
                assert!(points[1].x < badge.left() || points[0].x > badge.right());
            }
        }
    }
    Ok(())
}

#[test]
fn latest_price_has_one_axis_price_without_a_canvas_title() -> Result<(), Box<dyn std::error::Error>>
{
    let mut harness = Harness::new()?;
    harness.overlays.clear();
    harness.market_price = Some(Decimal::new(8660, 5));
    harness.display.price_labels = true;
    harness.display.ticks = true;
    harness.frame(vec![]);
    harness.frame(vec![]);
    assert!(
        !harness
            .texts
            .iter()
            .any(|text| text.contains("最新价格") || text.contains("Last price"))
    );
    assert_eq!(
        harness
            .texts
            .iter()
            .filter(|text| *text == "0.08660")
            .count(),
        1
    );
    Ok(())
}

#[test]
fn position_icons_queue_only_the_clicked_action_and_do_not_select_chart_price()
-> Result<(), Box<dyn std::error::Error>> {
    use venue_control_protocol::{accounts::*, terminal_position::PositionAction};
    let mut model = model_fixture()?;
    let target = selection()?;
    if let Some(overview) = &mut model.account_overview {
        overview.credentials.push(CredentialSummary {
            credential_id: target.credential_id.clone(),
            label: "fixture".into(),
            venue: venue_control_protocol::VenueId::Binance,
            masked_key: "***".into(),
            trading_account_id: Some(target.trading_account_id.clone()),
            verification: ApiVerificationState::Verified,
            verified_ms: Some(1),
            expires_ms: None,
            api_reachable: true,
            dual_position: true,
            account_mode: None,
            has_exposure: Some(true),
            equity: None,
            available_margin: None,
            balance_observed_ms: None,
        });
    }
    let mut facts = projection()?;
    facts.open_orders.clear();
    facts
        .positions
        .push(venue_control_protocol::kol::TerminalPosition {
            symbol: target.symbol,
            position_side: venue_domain::PositionSide::Long,
            quantity: Decimal::from(10),
            entry_price: Some(Decimal::new(8524, 5)),
            mark_price: Some(Decimal::new(8600, 5)),
        });
    model
        .execution
        .apply_private(Some(facts), &mut model.trade_dock);
    for action in [PositionAction::Close, PositionAction::Reverse] {
        let mut harness = Harness::new()?;
        harness.overlays =
            super::super::collect(&model, "DOGE/USDC", &ChartTradingSettings::default());
        assert!(
            harness.overlays[0]
                .badge
                .as_ref()
                .is_some_and(|b| b.position.is_some())
        );
        harness.frame(vec![]);
        harness.frame(vec![]);
        // Find the actual painted label bounds; icons occupy its two rightmost segments.
        let mut rect = Rect::NOTHING;
        let mut output = harness.frame(vec![]);
        for shape in &output.shapes {
            if let egui::Shape::Rect(shape) = &shape.shape {
                if shape.stroke.color == theme::BUY && (shape.rect.height() - 20.0).abs() < 0.01 {
                    rect = shape.rect;
                }
            }
        }
        output.textures_delta.clear();
        assert!(rect.is_positive());
        let point = Pos2::new(
            rect.right()
                - if action == PositionAction::Close {
                    10.0
                } else {
                    31.0
                },
            rect.center().y,
        );
        harness.press(point, true);
        harness.press(point, false);
        let queued = harness.context.data(|data| {
            data.get_temp::<(crate::execution_view::PositionActionDraft, PositionAction)>(
                action_id().with("position"),
            )
        });
        assert!(queued.is_some_and(|(_, queued)| queued == action));
        assert!(harness.action().is_none());
        assert!(harness.selected.is_none());
    }
    Ok(())
}

#[test]
fn replacement_confirmation_requires_original_account_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = model_fixture()?;
    let target = selection()?;
    model
        .execution
        .apply_private(Some(projection()?), &mut model.trade_dock);
    let scope = model.confirmed_account_scope().ok_or("scope missing")?;
    assert!(preview_is_current(&model, &target, Some(&scope)));
    assert!(!preview_is_current(&model, &target, None));
    let mut changed = scope.clone();
    changed.generation += 1;
    assert!(!preview_is_current(&model, &target, Some(&changed)));
    model.begin_account_selection(target.credential_id.clone());
    assert!(!preview_is_current(&model, &target, Some(&scope)));
    Ok(())
}
