    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn result_cache_budget_evicts_oldest_rebuildable_entry()
    -> Result<(), crate::market::LocalMarketError> {
        let context = egui::Context::default();
        let binding = crate::market::MarketSelection::binance_usd_m(
            "DOGE/USDC", crate::chart::ChartInterval::OneMinute)?.binding;
        let first = egui::Id::new("old-flow-chart");
        let second = egui::Id::new("new-flow-chart");
        let make = || Arc::new(MinuteFlowCache { binding: binding.clone(), revision: (1, 1),
            interval_ms: 60_000, reset_mode: venue_indicators::chart::CvdResetMode::UtcDaily,
            buckets: Vec::new(), values: Vec::new() });
        let old = make();
        let current = make();
        let old_weak = Arc::downgrade(&old);
        let current_weak = Arc::downgrade(&current);
        let budget = 2 * std::mem::size_of::<IndicatorCacheEntry>()
            + std::mem::size_of::<MinuteFlowCache>();
        let mut output = context.run_ui(Default::default(), |ui| {
            ui.ctx().data_mut(|data| {
                data.insert_temp(first.with("minute-flow-cache"), old.clone());
                data.insert_temp(second.with("minute-flow-cache"), current.clone());
            });
            mark_indicator_cache(ui.ctx(), IndicatorCacheKind::Flow, first);
            mark_indicator_cache(ui.ctx(), IndicatorCacheKind::Flow, second);
            ui.ctx().data_mut(|data| {
                let key = indicator_cache_registry_id();
                let mut entries = data.get_temp::<Vec<IndicatorCacheEntry>>(key).unwrap_or_default();
                for entry in &mut entries {
                    entry.seen_frame = if entry.id == first { 0 } else { 1 };
                }
                data.insert_temp(key, entries);
            });
            evict_indicator_caches_to_budget(ui.ctx(), budget);
        });
        output.textures_delta.clear();
        drop(old);
        drop(current);
        assert!(old_weak.upgrade().is_none());
        assert!(current_weak.upgrade().is_some());
        assert!(retained_indicator_cache_bytes(&context) <= budget);
        Ok(())
    }

    #[test]
    fn hidden_flow_pane_releases_its_aggregation_cache()
    -> Result<(), crate::market::LocalMarketError> {
        let context = egui::Context::default();
        let id = egui::Id::new("flow-chart-that-closes");
        let binding = crate::market::MarketSelection::binance_usd_m(
            "DOGE/USDC", crate::chart::ChartInterval::OneMinute)?.binding;
        let cache = Arc::new(MinuteFlowCache { binding, revision: (1, 1),
            interval_ms: 60_000, reset_mode: venue_indicators::chart::CvdResetMode::UtcDaily,
            buckets: Vec::new(), values: Vec::new() });
        let weak = Arc::downgrade(&cache);
        let mut output = context.run_ui(Default::default(), |ui| {
            ui.ctx().data_mut(|data| data.insert_temp(id.with("minute-flow-cache"), cache.clone()));
            mark_indicator_cache(ui.ctx(), IndicatorCacheKind::Flow, id);
            evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        drop(cache);
        assert!(weak.upgrade().is_some());
        assert!(retained_indicator_cache_bytes(&context) >= std::mem::size_of::<MinuteFlowCache>());
        let mut output = context.run_ui(Default::default(), |ui| {
            evict_inactive_indicator_caches(ui.ctx());
        });
        output.textures_delta.clear();
        assert!(weak.upgrade().is_none());
        assert_eq!(retained_indicator_cache_bytes(&context), 0);
        Ok(())
    }

    #[test]
    fn pane_precision_tracks_visible_range_and_height() {
        assert_eq!(sub_pane_precision(0.0, 100.0, 100.0), 0);
        assert_eq!(sub_pane_precision(0.0, 1.0, 100.0), 2);
        assert_eq!(sub_pane_precision(0.0, 0.0001, 100.0), 6);
        assert!(sub_pane_precision(0.0, 1.0, 500.0) > sub_pane_precision(0.0, 1.0, 50.0));
    }

    fn fixture() -> (Vec<UiBar>, Vec<ChartStudyPoint>) {
        let bars = (0..6)
            .map(|i| UiBar {
                open_time_ms: i * 60_000,
                open: Decimal::from(50),
                high: Decimal::from(55),
                low: Decimal::from(45),
                close: Decimal::from(52),
                volume: Some(Decimal::ONE),
            })
            .collect::<Vec<_>>();
        let studies = bars
            .iter()
            .enumerate()
            .map(|(i, bar)| ChartStudyPoint {
                open_time_ms: bar.open_time_ms,
                supertrend: Some(Decimal::from(if i < 3 { 40 } else { 60 })),
                supertrend_rising: i < 3,
                bollinger_upper: Some(Decimal::from(70)),
                bollinger_lower: Some(Decimal::from(30)),
                ..Default::default()
            })
            .collect();
        (bars, studies)
    }

    fn render(draw: impl Fn(&egui::Painter, Rect)) -> Vec<egui::epaint::ClippedShape> {
        let ctx = egui::Context::default();
        let mut output = ctx.run_ui(egui::RawInput::default(), |ui| {
            let painter = ui.ctx().layer_painter(egui::LayerId::background());
            draw(
                &painter,
                Rect::from_min_size(Pos2::ZERO, egui::vec2(300.0, 100.0)),
            );
        });
        output.textures_delta.clear();
        output.shapes
    }

    fn pointer_frame(
        context: &egui::Context,
        viewport: &mut crate::chart::ChartViewport,
        bars: &[UiBar],
        events: Vec<egui::Event>,
    ) -> bool {
        pointer_frame_with_analysis(context, viewport, bars, events, None)
    }

    fn pointer_frame_with_analysis(
        context: &egui::Context,
        viewport: &mut crate::chart::ChartViewport,
        bars: &[UiBar],
        events: Vec<egui::Event>,
        mut analysis: Option<&mut analysis::AnalysisInteraction>,
    ) -> bool {
        let mut selected = None;
        let mut output = context.run_ui(
            egui::RawInput {
                screen_rect: Some(Rect::from_min_size(Pos2::ZERO, egui::vec2(1_200.0, 500.0))),
                events,
                ..Default::default()
            },
            |ui| {
                selected = candle_plot(
                    ui,
                    bars,
                    &[],
                    viewport,
                    Language::English,
                    &ChartDisplaySettings::default(),
                    (2, 2),
                    None,
                    crate::chart::ChartInterval::OneMinute,
                    None,
                    None,
                    &crate::chart_trading::ChartTradingSettings::default(),
                    &[],
                    (None, None),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    analysis.as_deref_mut().map(|state| (&[][..], state)),
                    None,
                    None,
                );
            },
        );
        output.textures_delta.clear();
        selected.is_some()
    }

    #[test]
    fn analysis_click_and_escape_never_return_a_trading_price() {
        let (bars, _) = fixture();
        for cancel in [false, true] {
            for mode in [analysis::AnalysisMode::AddAnchor, analysis::AnalysisMode::MoveAnchor(1),
                analysis::AnalysisMode::FixedStart, analysis::AnalysisMode::FixedEnd(0)] {
                let context = egui::Context::default();
                let mut viewport = crate::chart::ChartViewport::default();
                let mut state = analysis::AnalysisInteraction::default();
                state.mode = mode;
                pointer_frame_with_analysis(&context, &mut viewport, &bars, vec![], Some(&mut state));
                let point = egui::pos2(22.0, 220.0);
                assert!(!pointer_frame_with_analysis(&context, &mut viewport, &bars, vec![
                    egui::Event::PointerMoved(point),
                    egui::Event::PointerButton { pos: point, button: egui::PointerButton::Primary,
                        pressed: true, modifiers: egui::Modifiers::NONE },
                ], Some(&mut state)));
                let mut events = vec![egui::Event::PointerButton { pos: point,
                    button: egui::PointerButton::Primary, pressed: false, modifiers: egui::Modifiers::NONE }];
                if cancel {
                    events.push(egui::Event::Key { key: egui::Key::Escape, physical_key: None,
                        pressed: true, repeat: false, modifiers: egui::Modifiers::NONE });
                }
                assert!(!pointer_frame_with_analysis(&context, &mut viewport, &bars, events, Some(&mut state)));
                if cancel {
                    assert_eq!(state.mode, analysis::AnalysisMode::None);
                    assert!(state.action.is_none());
                } else if mode == analysis::AnalysisMode::FixedStart {
                    assert!(matches!(state.mode, analysis::AnalysisMode::FixedEnd(_)));
                } else {
                    assert!(state.action.is_some(), "click did not produce analysis action: {mode:?}");
                }
            }
        }
    }

    #[test]
    fn left_drag_in_chart_and_timeline_keeps_padding_after_release() {
        for y in [200.0, 480.0] {
            for button in [egui::PointerButton::Primary, egui::PointerButton::Secondary] {
                let context = egui::Context::default();
                let mut viewport = crate::chart::ChartViewport::default();
                let (seed, _) = fixture();
                let mut bars = seed.into_iter().cycle().take(500).collect::<Vec<_>>();
                for (index, bar) in bars.iter_mut().enumerate() {
                    bar.open_time_ms = index as u64 * 60_000;
                }
                pointer_frame(&context, &mut viewport, &bars, vec![]);
                pointer_frame(
                    &context,
                    &mut viewport,
                    &bars,
                    vec![
                        egui::Event::PointerMoved(egui::pos2(900.0, y)),
                        egui::Event::PointerButton {
                            pos: egui::pos2(900.0, y),
                            button,
                            pressed: true,
                            modifiers: egui::Modifiers::NONE,
                        },
                    ],
                );
                pointer_frame(
                    &context,
                    &mut viewport,
                    &bars,
                    vec![egui::Event::PointerMoved(egui::pos2(800.0, y))],
                );
                let first_padding = viewport.right_padding();
                pointer_frame(
                    &context,
                    &mut viewport,
                    &bars,
                    vec![egui::Event::PointerMoved(egui::pos2(700.0, y))],
                );
                let final_padding = viewport.right_padding();
                if button == egui::PointerButton::Primary {
                    assert!(first_padding > 0);
                    assert!(final_padding > first_padding);
                } else {
                    assert_eq!(final_padding, 0);
                }
                assert!(!pointer_frame(
                    &context,
                    &mut viewport,
                    &bars,
                    vec![egui::Event::PointerButton {
                        pos: egui::pos2(700.0, y),
                        button,
                        pressed: false,
                        modifiers: egui::Modifiers::NONE,
                    },]
                ));
                pointer_frame(&context, &mut viewport, &bars, vec![]);
                assert_eq!(viewport.right_padding(), final_padding);
                assert_eq!(viewport.right_offset(), 0);
                let visible = viewport.visible_range(bars.len() + 1);
                assert_eq!(visible.end, bars.len() + 1);
                assert_eq!(viewport.right_padding(), final_padding);
            }
        }
    }

    #[test]
    fn price_axis_drag_changes_height_without_panning_or_selecting_price() {
        let context = egui::Context::default();
        let mut viewport = crate::chart::ChartViewport::default();
        let (bars, _) = fixture();
        pointer_frame(&context, &mut viewport, &bars, vec![]);
        pointer_frame(
            &context,
            &mut viewport,
            &bars,
            vec![
                egui::Event::PointerMoved(egui::pos2(1160.0, 150.0)),
                egui::Event::PointerButton {
                    pos: egui::pos2(1160.0, 150.0),
                    button: egui::PointerButton::Primary,
                    pressed: true,
                    modifiers: egui::Modifiers::NONE,
                },
            ],
        );
        pointer_frame(
            &context,
            &mut viewport,
            &bars,
            vec![egui::Event::PointerMoved(egui::pos2(1160.0, 210.0))],
        );
        assert!(viewport.price_zoom_milli > 1000);
        assert!(!viewport.auto_price_scale);
        assert_eq!(viewport.right_padding(), 0);
        assert!(!pointer_frame(
            &context,
            &mut viewport,
            &bars,
            vec![egui::Event::PointerButton {
                pos: egui::pos2(1160.0, 210.0),
                button: egui::PointerButton::Primary,
                pressed: false,
                modifiers: egui::Modifiers::NONE
            }]
        ));
        viewport.reset();
        assert_eq!(viewport.price_zoom_milli, 1000);
        assert!(viewport.auto_price_scale);
    }

    #[test]
    fn candles_are_clipped_before_the_separate_price_axis() {
        let context = egui::Context::default();
        let mut viewport = crate::chart::ChartViewport::default();
        let (bars, _) = fixture();
        let mut output = context.run_ui(
            egui::RawInput {
                screen_rect: Some(Rect::from_min_size(Pos2::ZERO, egui::vec2(1200.0, 500.0))),
                ..Default::default()
            },
            |ui| {
                candle_plot(
                    ui,
                    &bars,
                    &[],
                    &mut viewport,
                    Language::English,
                    &ChartDisplaySettings::default(),
                    (2, 2),
                    None,
                    crate::chart::ChartInterval::OneMinute,
                    None,
                    None,
                    &crate::chart_trading::ChartTradingSettings::default(),
                    &[],
                    (None, None),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                );
            },
        );
        output.textures_delta.clear();
        let axis_left = output
            .shapes
            .iter()
            .filter_map(|shape| {
                if let egui::Shape::Rect(rect) = &shape.shape
                    && rect.fill == theme::BG_PRIMARY
                    && rect.rect.left() > 900.0
                {
                    Some(rect.rect.left())
                } else {
                    None
                }
            })
            .reduce(f32::min)
            .unwrap_or(0.0);
        assert!(axis_left > 900.0);
        let candles = output.shapes.iter().filter(|shape| matches!(&shape.shape,
            egui::Shape::Rect(rect) if (rect.fill == theme::BUY || rect.fill == theme::SELL) && rect.rect.center().x < axis_left)).collect::<Vec<_>>();
        assert!(!candles.is_empty());
        assert!(
            candles
                .iter()
                .all(|shape| shape.clip_rect.right() <= axis_left)
        );
    }

    #[test]
    fn supertrend_breaks_lines_at_reversals_and_missing_studies() {
        let (bars, mut studies) = fixture();
        studies.remove(1);
        let shapes = render(|painter, rect| {
            draw_directional_price(
                painter,
                rect,
                &bars,
                6,
                &studies,
                |p| p.supertrend,
                |p| p.supertrend_rising,
                ChartDisplaySettings::default().supertrend,
                |v| v as f32,
                true,
            );
        });
        let lines = shapes
            .iter()
            .filter_map(|shape| match &shape.shape {
                egui::Shape::LineSegment { points, stroke } => Some((points, stroke)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(
            lines
                .iter()
                .all(|(points, stroke)| points[0].x >= 175.0 && stroke.color == theme::SELL)
        );
    }

    #[test]
    fn trend_fill_stays_between_trend_and_body_and_stops_at_reversal() {
        let (bars, studies) = fixture();
        let mut settings = ChartDisplaySettings::default();
        settings.supertrend.enabled = true;
        let shapes = render(|painter, rect| {
            draw_price_fills(painter, rect, &bars, 6, &studies, |v| v as f32, &settings)
        });
        let meshes = shapes
            .iter()
            .filter_map(|shape| match &shape.shape {
                egui::Shape::Mesh(mesh) => Some(mesh),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(meshes.len(), 4);
        for mesh in meshes {
            assert!(
                mesh.vertices
                    .iter()
                    .all(|v| (40.0..=60.0).contains(&v.pos.y))
            );
            let min_x = mesh
                .vertices
                .iter()
                .map(|v| v.pos.x)
                .fold(f32::INFINITY, f32::min);
            let max_x = mesh
                .vertices
                .iter()
                .map(|v| v.pos.x)
                .fold(f32::NEG_INFINITY, f32::max);
            assert!(max_x <= 125.0 || min_x >= 175.0);
        }
    }

    #[test]
    fn enabled_band_fill_does_not_bridge_missing_values() {
        let (bars, mut studies) = fixture();
        studies[2].bollinger_upper = None;
        let mut settings = ChartDisplaySettings::default();
        settings.bollinger.enabled = true;
        let shapes = render(|painter, rect| {
            draw_price_fills(painter, rect, &bars, 6, &studies, |v| v as f32, &settings)
        });
        assert_eq!(
            shapes
                .iter()
                .filter(|s| matches!(s.shape, egui::Shape::Mesh(_)))
                .count(),
            3
        );
        assert!(shapes.iter().all(|s| s.clip_rect.max.y <= 100.0));
    }

    #[test]
    fn hover_readout_measures_price_against_latest_trade() {
        let change = hover_price_change_percent(0.089_350, Some(Decimal::new(8_893, 5)));
        assert!(change.is_some_and(|change| (change - 0.472_281_57).abs() < 0.000_001));
        assert_eq!(format_f64_fixed(0.0889, 5), "0.08890");
        assert_eq!(hover_price_change_percent(1.0, Some(Decimal::ZERO)), None);
    }
