    #[path = "tests/live_studies.rs"]
    mod live_studies;
    #[test]
    fn rtt_expires_without_making_market_data_fresh() -> Result<(), super::LocalMarketError> {
        let mut reducer = super::LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let old = std::time::Instant::now() - std::time::Duration::from_secs(46);
        reducer.apply(envelope(
            &reducer,
            0,
            super::MarketPayload::ConnectionRtt {
                millis: 120,
                measured_at: old,
            },
        ))?;
        assert_eq!(reducer.view().recent_rtt_ms(), None);
        assert_eq!(reducer.view().last_received_ms, None);
        let sample = envelope(
            &reducer,
            0,
            super::MarketPayload::ConnectionRtt {
                millis: 1500,
                measured_at: std::time::Instant::now(),
            },
        );
        reducer.apply(sample)?;
        assert_eq!(reducer.view().recent_rtt_ms(), Some(1500));
        assert_eq!(reducer.view().last_received_ms, None);
        Ok(())
    }
    use super::*;
    use venue_control_protocol::AggressorSide;
    use venue_domain::{Price, UnknownReason};

    fn selection(symbol: &str) -> Result<MarketSelection, LocalMarketError> {
        MarketSelection::binance_usd_m(symbol, ChartInterval::OneMinute)
    }

    #[test]
    fn pnl_price_freshness_only_advances_with_new_prices() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        reducer.apply(envelope(
            &reducer,
            100,
            MarketPayload::Trade(UiTrade {
                trade_id: "first".into(),
                occurred_ms: 100,
                price: Decimal::from(100),
                quantity: Decimal::ONE,
                aggressor: AggressorSide::Buy,
            }),
        ))?;
        for payload in [
            MarketPayload::Bbo {
                bid: Decimal::from(99),
                ask: Decimal::from(101),
            },
            MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
            MarketPayload::Trade(UiTrade {
                trade_id: "late".into(),
                occurred_ms: 90,
                price: Decimal::from(90),
                quantity: Decimal::ONE,
                aggressor: AggressorSide::Buy,
            }),
        ] {
            reducer.apply(envelope(&reducer, 20_000, payload))?;
            assert_eq!(reducer.view().last, Some(Decimal::from(100)));
            assert_eq!(reducer.view().last_price_event_ms, Some(100));
            assert_eq!(reducer.view().last_price_received_ms, Some(107));
        }
        reducer.select(selection("ETH/USDT")?)?;
        assert_eq!(reducer.view().last_price_event_ms, None);
        assert_eq!(reducer.view().last_price_received_ms, None);
        Ok(())
    }

    fn bar(open_time_ms: u64, close: i64) -> UiBar {
        UiBar {
            open_time_ms,
            open: Decimal::new(close - 1, 0),
            high: Decimal::new(close + 1, 0),
            low: Decimal::new(close - 2, 0),
            close: Decimal::new(close, 0),
            volume: Some(Decimal::new(10, 0)),
        }
    }

    fn study_bar(open_time_ms: u64, close: i64) -> Result<PublicBar, LocalMarketError> {
        let ui = bar(open_time_ms, close);
        let price = |value| Price::new(value).map_err(|_| LocalMarketError::InvalidBar);
        Ok(PublicBar {
            symbol: "BTC/USDT"
                .parse()
                .map_err(|_| LocalMarketError::InvalidSymbol)?,
            generation: 1,
            received_at_ms: open_time_ms + 60_000,
            sequence: open_time_ms / 60_000,
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            interval_ms: 60_000,
            open: price(ui.open)?,
            high: price(ui.high)?,
            low: price(ui.low)?,
            close: price(ui.close)?,
            base_volume: FieldState::Known(ui.volume.ok_or(LocalMarketError::InvalidBar)?),
            quote_volume: FieldState::Known(ui.volume.ok_or(LocalMarketError::InvalidBar)? * ui.close),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        })
    }

    fn envelope(
        reducer: &LocalMarketReducer,
        event_time_ms: u64,
        payload: MarketPayload,
    ) -> MarketEnvelope {
        MarketEnvelope {
            generation: reducer.view().generation,
            selection: reducer.view().selection.clone(),
            event_time_ms,
            received_ms: event_time_ms + 7,
            payload,
        }
    }

    #[test]
    fn selection_is_fixed_to_canonical_binance_usd_m() -> Result<(), LocalMarketError> {
        assert_eq!(
            selection("BTC/USDT")?.binding.symbol.to_string(),
            "BTC/USDT"
        );
        assert_eq!(
            selection("ETH/USDC")?.binding.symbol.to_string(),
            "ETH/USDC"
        );
        assert_eq!(selection("btcusdt"), Err(LocalMarketError::InvalidSymbol));
        assert_eq!(selection("BTC/USD"), Err(LocalMarketError::InvalidBinding));
        Ok(())
    }

    #[test]
    fn live_prints_update_candle_shape_color_and_price_together() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        reducer.apply(envelope(
            &reducer,
            120_000,
            MarketPayload::RestHistory {
                bars: vec![study_bar(60_000, 100)?],
            },
        ))?;
        let forming = study_bar(120_000, 101)?;
        reducer.apply(envelope(
            &reducer,
            120_100,
            MarketPayload::WsBar {
                bar: ui_bar_from_public(&forming)?,
                study_bar: Box::new(forming.clone()),
                closed: false,
            },
        ))?;
        let before = reducer.view().bars[0].clone();
        for (time, price) in [(120_200, 105), (120_300, 95)] {
            reducer.apply(envelope(
                &reducer,
                time,
                MarketPayload::Trade(UiTrade {
                    trade_id: time.to_string(),
                    occurred_ms: time,
                    price: Decimal::from(price),
                    quantity: Decimal::ONE,
                    aggressor: AggressorSide::Buy,
                }),
            ))?;
            let candle = &reducer.view().bars[1];
            assert_eq!(candle.close, Decimal::from(price));
            assert_eq!(reducer.view().last, Some(candle.close));
            assert_eq!(candle.close >= candle.open, price > 100);
            assert_eq!(candle.high, Decimal::from(105));
            assert_eq!(candle.volume, Some(Decimal::from(10)));
            assert_eq!(reducer.view().bars[0], before);
        }
        assert_eq!(reducer.view().bars[1].low, Decimal::from(95));
        // A lagging kline must not revert a newer trade, body or wick.
        reducer.apply(envelope(
            &reducer,
            120_150,
            MarketPayload::WsBar {
                bar: ui_bar_from_public(&forming)?,
                study_bar: Box::new(forming),
                closed: false,
            },
        ))?;
        assert_eq!(reducer.view().bars[1].close, Decimal::from(95));
        assert_eq!(reducer.view().bars[1].high, Decimal::from(105));
        reducer.apply(envelope(
            &reducer,
            120_310,
            MarketPayload::Trade(UiTrade {
                trade_id: "late".into(),
                occurred_ms: 120_250,
                price: Decimal::from(104),
                quantity: Decimal::ONE,
                aggressor: AggressorSide::Buy,
            }),
        ))?;
        assert_eq!(reducer.view().last, Some(Decimal::from(95)));
        assert_eq!(reducer.view().bars[1].close, Decimal::from(95));
        Ok(())
    }

    #[test]
    fn switching_generation_clears_state_and_ignores_old_results() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let old = envelope(
            &reducer,
            120_000,
            MarketPayload::RestHistory {
                bars: vec![study_bar(60_000, 10)?],
            },
        );
        reducer.apply(old.clone())?;
        let generation = reducer.select(MarketSelection::binance_usd_m(
            "ETH/USDT",
            ChartInterval::FiveMinutes,
        )?)?;

        assert_eq!(generation, 2);
        assert!(reducer.view().bars.is_empty());
        assert_eq!(reducer.apply(old)?, ReduceOutcome::IgnoredOldGeneration);
        assert_eq!(
            reducer.view().selection.binding.symbol.to_string(),
            "ETH/USDT"
        );
        Ok(())
    }

    #[test]
    fn cached_chart_is_bounded_preview_only_and_never_live_state() -> Result<(), LocalMarketError> {
        let mut store = LocalMarketStore::default();
        let first = selection("BTC/USDT")?;
        store.replace([first.clone()])?;
        let old = MarketEnvelope {
            generation: store.generation(),
            selection: first.clone(),
            event_time_ms: 120_000,
            received_ms: 120_000,
            payload: MarketPayload::RestHistory {
                bars: vec![study_bar(60_000, 10)?],
            },
        };
        store.apply(old.clone())?;
        let other = MarketSelection::binance_usd_m("BTC/USDT", ChartInterval::FiveMinutes)?;
        store.replace([other.clone()])?;
        assert!(store.chart_preview(&other).is_none());
        assert_eq!(store.chart_preview(&first).map(<[UiBar]>::len), Some(1));
        store.replace([first.clone()])?;
        assert!(store.view(&first).unwrap().bars.is_empty());
        assert!(store.view(&first).unwrap().last.is_none());
        assert_eq!(store.apply(old)?, ReduceOutcome::IgnoredOldGeneration);
        for index in 0..12 {
            let next = selection(&format!("COIN{index}/USDT"))?;
            store.replace([next.clone()])?;
            // Public adapter output is already validated by the reducer in production.
            store.reducers.get_mut(&next).unwrap().view.bars = vec![UiBar {
                open_time_ms: 60_000,
                open: 1.into(),
                high: 1.into(),
                low: 1.into(),
                close: 1.into(),
                volume: Some(1.into()),
            }];
        }
        assert_eq!(store.chart_previews.len(), 8);
        store.replace([])?;
        assert!(store.chart_previews.is_empty());
        Ok(())
    }

    #[test]
    fn rejects_current_generation_with_wrong_scope() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let mut result = envelope(
            &reducer,
            120_000,
            MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        );
        result.selection = selection("ETH/USDT")?;
        assert_eq!(reducer.apply(result), Err(LocalMarketError::ScopeMismatch));
        Ok(())
    }

    #[test]
    fn history_and_tail_upsert_are_sorted_and_closed_never_regresses()
    -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let history = envelope(
            &reducer,
            180_000,
            MarketPayload::RestHistory {
                bars: vec![study_bar(120_000, 12)?, study_bar(60_000, 11)?],
            },
        );
        reducer.apply(history)?;
        let closed_update = envelope(
            &reducer,
            181_000,
            MarketPayload::WsBar {
                bar: bar(120_000, 13),
                study_bar: Box::new(study_bar(120_000, 13)?),
                closed: true,
            },
        );
        reducer.apply(closed_update)?;
        let regressing_update = envelope(
            &reducer,
            182_000,
            MarketPayload::WsBar {
                bar: bar(120_000, 99),
                study_bar: Box::new(study_bar(120_000, 99)?),
                closed: false,
            },
        );
        reducer.apply(regressing_update)?;

        assert_eq!(reducer.view().bars[0].open_time_ms, 60_000);
        assert_eq!(reducer.view().bars[1].close, Decimal::new(13, 0));
        assert_eq!(reducer.view().last, Some(Decimal::new(13, 0)));
        Ok(())
    }

    #[test]
    fn missing_volume_keeps_price_history_without_inventing_zero()
    -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let mut missing = study_bar(120_000, 101)?;
        missing.base_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.trade_count = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.taker_buy_base_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        missing.taker_buy_quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        reducer.apply(envelope(&reducer, 180_000, MarketPayload::RestHistory {
            bars: vec![study_bar(60_000, 100)?, missing],
        }))?;
        assert_eq!(reducer.view().bars.len(), 2);
        assert_eq!(reducer.view().bars[1].close, Decimal::from(101));
        assert_eq!(reducer.view().bars[1].volume, None);
        assert_eq!(reducer.view().studies[1].vwap, None);
        assert_eq!(reducer.view().study_error, None);
        Ok(())
    }

    #[test]
    fn indicator_arithmetic_error_does_not_reject_valid_price_history()
    -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let mut extreme = study_bar(60_000, 100)?;
        let price = Price::new(Decimal::MAX).map_err(|_| LocalMarketError::InvalidBar)?;
        extreme.open = price;
        extreme.high = price;
        extreme.low = price;
        extreme.close = price;
        extreme.base_volume = FieldState::Known(Decimal::MAX);
        extreme.quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        extreme.trade_count = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        extreme.taker_buy_quote_volume = FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        };
        reducer.apply(envelope(&reducer, 120_000, MarketPayload::RestHistory {
            bars: vec![extreme],
        }))?;
        assert_eq!(reducer.view().bars[0].close, Decimal::MAX);
        assert!(reducer.view().study_error.is_some());
        Ok(())
    }

    #[test]
    fn indicator_reconfiguration_rebuilds_closed_history_without_resubscribing()
    -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let bars = (1_u64..=5)
            .map(|index| study_bar(index * 60_000, 100 + index as i64))
            .collect::<Result<Vec<_>, _>>()?;
        let history = envelope(&reducer, 360_000, MarketPayload::RestHistory { bars });
        reducer.apply(history)?;
        let generation = reducer.view().generation;
        assert!(
            reducer
                .view()
                .studies
                .last()
                .is_some_and(|point| point.sma.is_none())
        );

        let configuration = ChartStudyConfig {
            sma_period: 2,
            ..ChartStudyConfig::default()
        };
        reducer.reconfigure_studies(configuration)?;

        assert_eq!(reducer.view().generation, generation);
        assert_eq!(reducer.view().bars.len(), 5);
        assert!(
            reducer
                .view()
                .studies
                .last()
                .is_some_and(|point| point.sma.is_some())
        );
        Ok(())
    }

    #[test]
    fn bars_and_recompute_facts_are_bounded() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let bars = (1_u64..=MAX_BARS as u64 + 10)
            .map(|index| study_bar(index * 60_000, 10))
            .collect::<Result<Vec<_>, _>>()?;
        let result = envelope(
            &reducer,
            (MAX_BARS as u64 + 11) * 60_000,
            MarketPayload::RestHistory { bars },
        );
        reducer.apply(result)?;
        assert_eq!(reducer.view().bars.len(), MAX_BARS);
        assert_eq!(reducer.view().bars[0].open_time_ms, 11 * 60_000);
        Ok(())
    }

    #[test]
    fn forming_bar_survives_reconfiguration_and_history_prepend() -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let mut store = LocalMarketStore::default();
        store.replace([selected.clone()])?;
        let history = MarketEnvelope {
            generation: store.generation(),
            selection: selected.clone(),
            event_time_ms: 420_000,
            received_ms: 420_000,
            payload: MarketPayload::RestHistory {
                bars: (4..=6)
                    .map(|i| study_bar(i * 60_000, 100 + i as i64))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        };
        store.apply(history)?;
        let forming = study_bar(420_000, 150)?;
        store.apply(MarketEnvelope {
            generation: store.generation(),
            selection: selected.clone(),
            event_time_ms: 421_000,
            received_ms: 421_000,
            payload: MarketPayload::WsBar {
                bar: ui_bar_from_public(&forming)?,
                study_bar: Box::new(forming),
                closed: false,
            },
        })?;
        let fast = ChartStudyConfig {
            sma_period: 2,
            custom_ema_adx: Some(venue_indicators::chart::EmaAdxConfig::default()),
            ..Default::default()
        };
        let slow = ChartStudyConfig {
            sma_period: 5,
            ..Default::default()
        };
        store.configure_chart("fast", &selected, fast)?;
        store.configure_chart("slow", &selected, slow)?;
        let fast = store
            .chart_view("fast")
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!(fast.bars.len(), 4);
        assert!(fast.studies.iter().all(|p| p.custom_ema_adx.is_some()));
        assert!(
            store
                .chart_view("slow")
                .is_some_and(|v| v.studies.iter().all(|p| p.custom_ema_adx.is_none()))
        );
        assert!(
            !fast
                .studies
                .last()
                .ok_or(LocalMarketError::InvalidBar)?
                .confirmed
        );
        assert!(
            fast.studies
                .last()
                .ok_or(LocalMarketError::InvalidBar)?
                .sma
                .is_some()
        );
        assert!(
            store
                .chart_view("slow")
                .and_then(|v| v.studies.last())
                .is_some_and(|v| v.sma.is_none())
        );
        let request = store
            .begin_history(&selected, false)
            .ok_or(LocalMarketError::InvalidBar)?;
        assert!(store.begin_history(&selected, false).is_none());
        assert_eq!(
            store.finish_history(
                &request,
                Ok((1..=3)
                    .map(|i| study_bar(i * 60_000, 100 + i as i64))
                    .collect::<Result<Vec<_>, _>>()?)
            )?,
            3
        );
        let chart = store
            .chart_view("slow")
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!(chart.bars.len(), 7);
        assert_eq!(
            chart.bars.last().map(|bar| bar.close),
            Some(Decimal::new(150, 0))
        );
        assert!(
            !chart
                .studies
                .last()
                .ok_or(LocalMarketError::InvalidBar)?
                .confirmed
        );
        assert!(
            chart
                .studies
                .last()
                .ok_or(LocalMarketError::InvalidBar)?
                .sma
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn old_or_discontinuous_history_cannot_overwrite_current_chart() -> Result<(), LocalMarketError>
    {
        let selected = selection("BTC/USDT")?;
        let mut store = LocalMarketStore::default();
        store.replace([selected.clone()])?;
        store.apply(MarketEnvelope {
            generation: store.generation(),
            selection: selected.clone(),
            event_time_ms: 240_000,
            received_ms: 240_000,
            payload: MarketPayload::RestHistory {
                bars: vec![study_bar(180_000, 100)?],
            },
        })?;
        let request = store
            .begin_history(&selected, false)
            .ok_or(LocalMarketError::InvalidBar)?;
        assert!(
            store
                .finish_history(&request, Ok(vec![study_bar(60_000, 99)?]))
                .is_err()
        );
        assert_eq!(store.view(&selected).map(|v| v.bars.len()), Some(1));
        store.replace([selection("ETH/USDT")?])?;
        assert_eq!(
            store.finish_history(&request, Ok(vec![study_bar(120_000, 99)?]))?,
            0
        );
        assert!(store.view(&selected).is_none());
        Ok(())
    }

    #[test]
    fn book_is_deduplicated_sorted_and_bounded() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let bids = (1_i64..=25)
            .map(|price| UiBookLevel {
                price: Decimal::new(price, 0),
                quantity: Decimal::new(price, 0),
            })
            .chain(std::iter::once(UiBookLevel {
                price: Decimal::new(25, 0),
                quantity: Decimal::new(99, 0),
            }))
            .collect();
        let asks = (30_i64..=55)
            .map(|price| UiBookLevel {
                price: Decimal::new(price, 0),
                quantity: Decimal::new(1, 0),
            })
            .collect();
        let result = envelope(&reducer, 60_000, MarketPayload::BookSnapshot { bids, asks });
        reducer.apply(result)?;
        assert_eq!(reducer.view().bids.len(), MAX_BOOK_LEVELS);
        assert_eq!(reducer.view().asks.len(), MAX_BOOK_LEVELS);
        assert_eq!(reducer.view().bids[0].price, Decimal::new(25, 0));
        assert_eq!(reducer.view().bids[0].quantity, Decimal::new(99, 0));
        assert_eq!(reducer.view().asks[0].price, Decimal::new(30, 0));
        assert_eq!(reducer.view().bid, Some(Decimal::new(25, 0)));
        assert_eq!(reducer.view().ask, Some(Decimal::new(30, 0)));
        Ok(())
    }

    #[test]
    fn trades_are_deduplicated_sorted_and_bounded() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        for index in (1_u64..=205).rev() {
            let result = envelope(
                &reducer,
                index,
                MarketPayload::Trade(UiTrade {
                    trade_id: format!("trade-{index:03}"),
                    occurred_ms: index,
                    price: Decimal::new(10, 0),
                    quantity: Decimal::new(1, 0),
                    aggressor: AggressorSide::Buy,
                }),
            );
            reducer.apply(result)?;
        }
        let duplicate = envelope(
            &reducer,
            205,
            MarketPayload::Trade(reducer.view().trades[0].clone()),
        );
        reducer.apply(duplicate)?;

        assert_eq!(reducer.view().trades.len(), MAX_TRADES);
        assert_eq!(reducer.view().trades[0].occurred_ms, 6);
        assert_eq!(reducer.view().trades[199].occurred_ms, 205);
        Ok(())
    }

    #[test]
    fn tracks_latency_and_marks_live_feed_stale() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let live = envelope(
            &reducer,
            1_000,
            MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        );
        reducer.apply(live)?;
        assert_eq!(reducer.view().latency_ms, None);
        reducer.apply(envelope(
            &reducer,
            1_001,
            MarketPayload::Bbo {
                bid: Decimal::ONE,
                ask: Decimal::new(2, 0),
            },
        ))?;
        assert_eq!(reducer.view().latency_ms, Some(7));
        let received = reducer.view().last_received_ms;
        reducer.apply(envelope(
            &reducer,
            5_999,
            MarketPayload::ConnectionRtt {
                millis: 1501,
                measured_at: std::time::Instant::now(),
            },
        ))?;
        assert_eq!(reducer.view().connection_rtt_ms, Some(1501));
        assert_eq!(reducer.view().last_received_ms, received);
        assert_eq!(reducer.view().latency_ms, Some(7));
        reducer.apply(envelope(
            &reducer,
            1_002,
            MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        ))?;
        assert_eq!(reducer.view().latency_ms, Some(7));
        reducer.refresh_staleness(6_010, 5_000);
        assert_eq!(reducer.view().status, MarketStatus::Stale);
        assert_eq!(
            reducer.view().status_detail.as_deref(),
            Some("market event timeout")
        );
        reducer.apply(envelope(
            &reducer,
            6_011,
            MarketPayload::Bbo {
                bid: Decimal::ONE,
                ask: Decimal::new(2, 0),
            },
        ))?;
        assert_eq!(reducer.view().status, MarketStatus::Live);
        assert!(reducer.view().status_detail.is_none());
        assert!(reducer.view().last_price_received_ms.is_none());
        let received = reducer.view().last_received_ms;
        reducer.apply(envelope(
            &reducer,
            10_000,
            MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        ))?;
        assert_eq!(reducer.view().last_received_ms, received);
        reducer.refresh_staleness(11_019, 5_000);
        assert_eq!(reducer.view().status, MarketStatus::Stale);
        Ok(())
    }

    #[test]
    fn rejects_invalid_market_values() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let crossed = envelope(
            &reducer,
            60_000,
            MarketPayload::BookSnapshot {
                bids: vec![UiBookLevel {
                    price: Decimal::new(11, 0),
                    quantity: Decimal::ONE,
                }],
                asks: vec![UiBookLevel {
                    price: Decimal::new(10, 0),
                    quantity: Decimal::ONE,
                }],
            },
        );
        assert_eq!(reducer.apply(crossed), Err(LocalMarketError::CrossedBook));
        Ok(())
    }

    #[test]
    fn store_reuses_identical_subscriptions_and_fences_replaced_sets()
    -> Result<(), LocalMarketError> {
        let btc = selection("BTC/USDT")?;
        let eth = selection("ETH/USDT")?;
        let mut store = LocalMarketStore::default();
        assert_eq!(store.replace([btc.clone(), eth.clone()])?, Some(1));
        assert_eq!(store.replace([eth.clone(), btc.clone()])?, None);

        let old = MarketEnvelope {
            generation: 1,
            selection: btc.clone(),
            event_time_ms: 60_000,
            received_ms: 60_007,
            payload: MarketPayload::Status {
                status: MarketStatus::Live,
                detail: None,
            },
        };
        assert_eq!(store.apply(old.clone())?, ReduceOutcome::Applied);
        assert_eq!(store.replace([eth])?, Some(2));
        assert_eq!(store.apply(old)?, ReduceOutcome::IgnoredOldGeneration);
        assert!(store.view(&btc).is_none());
        Ok(())
    }

    #[test]
    fn funding_is_scoped_and_cleared_on_market_switch() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("DOGE/USDC")?)?;
        let funding = MarkFunding {
            symbol: "DOGE/USDC".parse().map_err(|_| LocalMarketError::InvalidSymbol)?,
            generation: reducer.view().generation,
            received_at_ms: 1_007,
            exchange_time_ms: 1_000,
            time_source: venue_domain::MarketTimeSource::Exchange,
            next_funding_time_ms: Some(10_000),
            mark_price: FieldState::Known(Price::new(Decimal::ONE).map_err(|_| LocalMarketError::InvalidBar)?),
            index_price: FieldState::Known(Price::new(Decimal::ONE).map_err(|_| LocalMarketError::InvalidBar)?),
            funding_rate: Decimal::new(-1, 4),
            estimated_settle_price: FieldState::NotApplicable,
            predicted_funding_rate: FieldState::NotApplicable,
            unknown_reason: None,
        };
        reducer.apply(envelope(&reducer, 1_000, MarketPayload::Funding(funding.clone())))?;
        assert_eq!(reducer.view().funding, Some(funding.clone()));
        let mut wrong = funding;
        wrong.symbol = "DOGE/USDT".parse().map_err(|_| LocalMarketError::InvalidSymbol)?;
        assert_eq!(reducer.apply(envelope(&reducer, 1_000, MarketPayload::Funding(wrong))), Err(LocalMarketError::ScopeMismatch));
        reducer.select(selection("BTC/USDT")?)?;
        assert!(reducer.view().funding.is_none());
        Ok(())
    }

    #[test]
    fn open_interest_history_cannot_cross_binding_or_replace_newer_samples() -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("DOGE/USDC")?)?;
        let symbol: venue_domain::Symbol = "DOGE/USDC".parse().map_err(|_| LocalMarketError::InvalidSymbol)?;
        let generation = reducer.view().generation;
        let sample = |time, interval| OpenInterestSample {
            symbol: symbol.clone(),
            generation,
            received_at_ms: time + 7,
            exchange_time_ms: time,
            time_source: venue_domain::MarketTimeSource::Exchange,
            sampling_interval_ms: interval,
            native_quantity: Decimal::from(100),
            native_unit: venue_domain::OpenInterestUnit::BaseAsset,
            base_quantity: FieldState::Known(Decimal::from(100)),
            quote_notional: FieldState::NotApplicable,
            quote_asset: None,
        };
        let current = sample(600_000, None);
        reducer.apply(envelope(&reducer, 600_000, MarketPayload::OpenInterestCurrent(current.clone())))?;
        assert_eq!(reducer.view().open_interest_current, Some(current));
        let history = vec![sample(300_000, Some(300_000)), sample(600_000, Some(300_000))];
        reducer.apply(envelope(&reducer, 600_000, MarketPayload::OpenInterestHistory(history.clone())))?;
        assert_eq!(reducer.view().open_interest_history, history);
        reducer.apply(envelope(&reducer, 900_000,
            MarketPayload::OpenInterestHistoryUnavailable("history 404".into())))?;
        reducer.apply(envelope(&reducer, 900_000,
            MarketPayload::OpenInterestCurrent(sample(900_000, None))))?;
        assert_eq!(reducer.view().open_interest_history_error.as_deref(), Some("history 404"));
        reducer.apply(envelope(&reducer, 300_000, MarketPayload::OpenInterestHistory(vec![sample(300_000, Some(300_000))])))?;
        assert_eq!(reducer.view().open_interest_history.len(), 2);
        assert_eq!(reducer.view().open_interest_history_error.as_deref(), Some("history 404"));
        reducer.apply(envelope(&reducer, 900_000, MarketPayload::OpenInterestHistory(
            vec![sample(300_000, Some(300_000)), sample(600_000, Some(300_000)),
                sample(900_000, Some(300_000))])))?;
        assert!(reducer.view().open_interest_history_error.is_none());
        let mut wrong = sample(900_000, None);
        wrong.symbol = "DOGE/USDT".parse().map_err(|_| LocalMarketError::InvalidSymbol)?;
        assert_eq!(reducer.apply(envelope(&reducer, 900_000, MarketPayload::OpenInterestCurrent(wrong))), Err(LocalMarketError::ScopeMismatch));
        reducer.select(selection("BTC/USDT")?)?;
        assert!(reducer.view().open_interest_current.is_none());
        assert!(reducer.view().open_interest_history.is_empty());
        Ok(())
    }

    #[test]
    fn shared_base_minutes_are_scoped_and_closed_candles_do_not_regress() -> Result<(), LocalMarketError> {
        let selected = MarketSelection::binance_usd_m("BTC/USDT", ChartInterval::FiveMinutes)?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_history(1, binding.clone(),
            vec![study_bar(60_000, 10)?, study_bar(120_000, 11)?],
            Some(study_bar(180_000, 12)?))?;
        assert_eq!(store.base_minutes(&binding).map(|(bars, _, _)| bars.len()), Some(3));
        assert!(store.base_minutes(&binding).is_some_and(|(_, studies, _)| !studies[2].confirmed));
        assert_eq!(store.base_minute_forming_fact(&binding).map(|bar| bar.open_time_ms), Some(180_000));
        store.apply_base_minute(1, binding.clone(), study_bar(180_000, 13)?, true)?;
        assert!(store.base_minute_forming_fact(&binding).is_none());
        store.apply_base_minute(1, binding.clone(), study_bar(180_000, 99)?, false)?;
        let (bars, studies, _) = store.base_minutes(&binding).ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!(bars[2].close, Decimal::from(13));
        assert!(studies[2].confirmed);
        let revision = store.base_minutes(&binding).map(|(_, _, revision)| revision);
        store.apply_base_minute(1, binding.clone(), study_bar(180_000, 13)?, true)?;
        assert_eq!(store.base_minutes(&binding).map(|(_, _, revision)| revision), revision);
        store.replace([MarketSelection::binance_usd_m("ETH/USDT", ChartInterval::OneHour)?])?;
        assert!(store.base_minutes(&binding).is_none());
        assert_eq!(store.apply_base_minute(1, binding, study_bar(240_000, 14)?, true)?, ReduceOutcome::IgnoredOldGeneration);
        Ok(())
    }

    #[test]
    fn switching_back_reuses_only_closed_shared_facts_in_the_new_generation()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected.clone()])?;
        store.apply_base_history(1, binding.clone(),
            vec![study_bar(60_000, 10)?, study_bar(120_000, 11)?],
            Some(study_bar(180_000, 12)?))?;
        let mut day = study_bar(0, 10)?;
        day.sequence = 1;
        day.interval_ms = 86_400_000;
        day.close_time_ms = 86_399_999;
        store.apply_session_day(1, binding.clone(), day, true)?;
        let mut forming_day = study_bar(86_400_000, 11)?;
        forming_day.interval_ms = 86_400_000;
        forming_day.close_time_ms = 172_799_999;
        store.apply_session_day(1, binding.clone(), forming_day, false)?;
        let identity = store.base_minutes(&binding)
            .ok_or(LocalMarketError::ScopeMismatch)?.2;

        store.replace([selection("ETH/USDT")?])?;
        assert!(store.base_minute_facts(&binding).is_none());
        assert!(store.session_days(&binding).is_none());
        assert_eq!(store.apply_base_minute(1, binding.clone(), study_bar(240_000, 13)?, true)?,
            ReduceOutcome::IgnoredOldGeneration);

        assert_eq!(store.replace([selected])?, Some(3));
        let (bars, studies, new_identity) = store.base_minutes(&binding)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!(bars.len(), 2);
        assert!(studies.iter().all(|point| point.confirmed));
        assert_ne!(identity, new_identity);
        assert!(store.base_minute_facts(&binding).is_some_and(|facts|
            facts.len() == 2 && facts.iter().all(|bar| bar.generation == 3)));
        assert!(store.base_minute_forming_fact(&binding).is_none());
        assert!(store.session_days(&binding).is_some_and(|days|
            days.len() == 1 && days[0].generation == 3));
        let mut latest = study_bar(180_000, 13)?;
        latest.generation = 3;
        store.apply_base_minute(3, binding.clone(), latest, true)?;
        assert_eq!(store.base_minute_facts(&binding).map(<[PublicBar]>::len), Some(3));
        Ok(())
    }

    #[test]
    fn corrected_closed_minute_invalidates_profile_and_avwap_source_revision()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_history(1, binding.clone(),
            vec![study_bar(60_000, 10)?, study_bar(120_000, 11)?], None)?;
        let initial = store.base_minutes(&binding)
            .ok_or(LocalMarketError::ScopeMismatch)?.2;
        store.apply_base_minute(1, binding.clone(), study_bar(120_000, 12)?, true)?;
        let corrected = store.base_minutes(&binding)
            .ok_or(LocalMarketError::ScopeMismatch)?.2;
        assert_ne!(initial, corrected);
        store.apply_base_minute(1, binding.clone(), study_bar(120_000, 12)?, true)?;
        assert_eq!(store.base_minutes(&binding).map(|(_, _, revision)| revision), Some(corrected));
        assert_eq!(store.base_minute_facts(&binding).and_then(|bars| bars.last())
            .map(|bar| bar.close.value()), Some(Decimal::from(12)));
        Ok(())
    }

    #[test]
    fn minute_budget_can_admit_four_months_and_evicts_other_binding()
    -> Result<(), LocalMarketError> {
        let months_four: usize = 4 * 31 * 24 * 60;
        assert!(months_four < MAX_BASE_MINUTE_BARS);
        let allocated = months_four.next_power_of_two();
        let allocated_per_bar = std::mem::size_of::<PublicBar>()
            + std::mem::size_of::<UiBar>()
            + std::mem::size_of::<BaseMinuteStudy>();
        assert!(allocated_per_bar * allocated + 64 * months_four
            < MAX_BASE_MINUTE_CACHE_BYTES);
        assert!((allocated_per_bar + 64 + 48) * MAX_BASE_MINUTE_BARS
            < MAX_BASE_MINUTE_CACHE_BYTES);

        let btc = selection("BTC/USDT")?;
        let doge = selection("DOGE/USDT")?;
        let mut store = LocalMarketStore::default();
        store.replace([btc.clone(), doge.clone()])?;
        store.apply_base_minute(1, btc.binding.clone(), study_bar(60_000, 100)?, true)?;
        let first_identity = store.base_minutes(&btc.binding)
            .ok_or(LocalMarketError::ScopeMismatch)?.2;
        let mut doge_bar = study_bar(60_000, 100)?;
        doge_bar.symbol = "DOGE/USDT".parse().map_err(|_| LocalMarketError::InvalidSymbol)?;
        store.apply_base_minute(1, doge.binding.clone(), doge_bar, true)?;
        let doge_size = store.base_minutes.get(&doge.binding)
            .ok_or(LocalMarketError::ScopeMismatch)?.memory_bytes();
        store.trim_base_minute_cache(&doge.binding, doge_size);
        assert!(store.base_minutes(&btc.binding).is_none());
        assert!(store.base_minutes(&doge.binding).is_some());
        store.apply_base_minute(1, btc.binding.clone(), study_bar(60_000, 100)?, true)?;
        let second_identity = store.base_minutes(&btc.binding)
            .ok_or(LocalMarketError::ScopeMismatch)?.2;
        assert_ne!(first_identity, second_identity);
        Ok(())
    }

    #[test]
    fn protected_minute_source_releases_spare_capacity_without_losing_history()
    -> Result<(), LocalMarketError> {
        let doge = selection("DOGE/USDT")?;
        let btc = selection("BTC/USDT")?;
        let mut store = LocalMarketStore::default();
        store.replace([doge.clone(), btc.clone()])?;
        store.apply_base_minute(1, btc.binding.clone(), study_bar(60_000, 100)?, true)?;
        let mut bar = study_bar(60_000, 100)?;
        bar.symbol = "DOGE/USDT".parse().map_err(|_| LocalMarketError::InvalidSymbol)?;
        store.apply_base_minute(1, doge.binding.clone(), bar.clone(), true)?;
        let series = store.base_minutes.get_mut(&doge.binding)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        series.bars.reserve(4_096);
        series.studies.reserve(4_096);
        series.facts.reserve(4_096);
        assert!(series.memory_bytes() > 8_192);
        store.trim_base_minute_cache(&doge.binding, 8_192);
        let series = store.base_minutes.get(&doge.binding)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert!(series.memory_bytes() <= 8_192);
        assert_eq!(series.facts, vec![bar]);
        assert_eq!(series.studies.len(), 1);
        assert!(store.base_minutes.contains_key(&btc.binding));
        assert!(store.retained_study_source_bytes() <= 8_192);
        Ok(())
    }

    #[test]
    fn shared_history_page_uses_generation_and_exact_binding() -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_history(1, binding.clone(),
            vec![study_bar(240_000, 10)?, study_bar(300_000, 11)?], None)?;
        let request = store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!(request.before, 240_000);
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000).is_none());
        assert_eq!(store.finish_shared_history(&request, Ok(vec![
            study_bar(60_000, 7)?, study_bar(120_000, 8)?, study_bar(180_000, 9)?,
        ]))?, 3);
        assert_eq!(store.base_minute_facts(&binding).map(<[PublicBar]>::len), Some(5));
        store.replace([selection("ETH/USDT")?])?;
        assert_eq!(store.finish_shared_history(&request, Ok(vec![study_bar(60_000, 7)?]))?, 0);
        assert!(store.base_minute_facts(&binding).is_none());
        Ok(())
    }

    #[test]
    fn same_generation_rest_refresh_keeps_loaded_prefix_and_respects_real_gap()
    -> Result<(), LocalMarketError> {
        let mut reducer = LocalMarketReducer::new(selection("BTC/USDT")?)?;
        let full = (1_u64..=180).map(|index|
            study_bar(index * 60_000, 100 + index as i64))
            .collect::<Result<Vec<_>, _>>()?;
        reducer.apply(envelope(&reducer, 11_000_000,
            MarketPayload::RestHistory { bars: full.clone() }))?;
        let revision = reducer.view().bar_revision;
        let mut replay = full[60..].to_vec();
        for bar in &mut replay { bar.received_at_ms = bar.received_at_ms.saturating_add(1); }
        reducer.apply(envelope(&reducer, 11_000_001,
            MarketPayload::RestHistory { bars: replay }))?;
        assert_eq!(reducer.view().bars.len(), 180);
        assert_eq!(reducer.view().bar_revision, revision);
        let newer = vec![full[179].clone(), study_bar(181 * 60_000, 281)?];
        reducer.apply(envelope(&reducer, 11_100_000,
            MarketPayload::RestHistory { bars: newer }))?;
        assert_eq!(reducer.view().bars.len(), 181);
        assert!(reducer.view().bar_revision > revision);
        reducer.apply(envelope(&reducer, 18_100_000,
            MarketPayload::RestHistory { bars: vec![study_bar(300 * 60_000, 400)?] }))?;
        assert_eq!(reducer.view().bars.len(), 1);
        Ok(())
    }

    #[test]
    fn inner_bar_generation_and_duplicate_history_are_rejected_before_mutation()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let mut reducer = LocalMarketReducer::new(selected.clone())?;
        reducer.select(selected)?;
        let old = study_bar(60_000, 10)?;
        assert_eq!(reducer.apply(envelope(&reducer, 120_000,
            MarketPayload::RestHistory { bars: vec![old.clone()] })),
            Err(LocalMarketError::InvalidBar));
        assert_eq!(reducer.apply(envelope(&reducer, 120_000,
            MarketPayload::WsBar { bar: bar(60_000, 10), study_bar: Box::new(old), closed: true })),
            Err(LocalMarketError::InvalidBar));
        let mut current = study_bar(60_000, 10)?;
        current.generation = reducer.view().generation;
        assert_eq!(reducer.apply(envelope(&reducer, 120_000,
            MarketPayload::RestHistory { bars: vec![current.clone(), current.clone()] })),
            Err(LocalMarketError::InvalidBar));
        assert!(reducer.view().bars.is_empty());
        reducer.apply(envelope(&reducer, 120_000,
            MarketPayload::RestHistory { bars: vec![current] }))?;
        assert_eq!(reducer.view().bars.len(), 1);
        Ok(())
    }

    #[test]
    fn visible_history_page_rejects_inner_bar_from_another_generation()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let mut store = LocalMarketStore::default();
        store.replace([selected.clone()])?;
        store.apply(MarketEnvelope { generation: 1, selection: selected.clone(),
            event_time_ms: 240_000, received_ms: 240_000,
            payload: MarketPayload::RestHistory { bars: vec![study_bar(180_000, 10)?] } })?;
        let request = store.begin_history(&selected, false)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        let mut wrong = study_bar(60_000, 9)?;
        wrong.generation = 2;
        assert_eq!(store.finish_history(&request, Ok(vec![wrong])), Err(LocalMarketError::InvalidBar));
        assert_eq!(store.view(&selected).map(|view| view.bars.len()), Some(1));
        Ok(())
    }

    #[test]
    fn disabled_shared_source_cancels_pending_page_and_fences_late_reply()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_history(1, binding.clone(),
            vec![study_bar(240_000, 10)?, study_bar(300_000, 11)?], None)?;
        let old = store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        store.retain_shared_history_demands(&BTreeSet::new());
        let current = store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert_ne!(old.request_id, current.request_id);
        assert_eq!(store.finish_shared_history(&old,
            Ok(vec![study_bar(180_000, 9)?]))?, 0);
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000).is_none());
        assert_eq!(store.finish_shared_history(&current,
            Ok(vec![study_bar(60_000, 7)?, study_bar(120_000, 8)?,
                study_bar(180_000, 9)?]))?, 3);
        Ok(())
    }

    #[test]
    fn empty_shared_history_page_stops_repeated_requests_at_same_cursor() -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_minute(1, binding.clone(), study_bar(240_000, 10)?, true)?;
        let request = store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!(store.finish_shared_history(&request, Ok(Vec::new()))?, 0);
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000).is_none());
        store.apply_base_history(1, binding.clone(), vec![study_bar(180_000, 9)?], None)?;
        assert_eq!(store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000)
            .ok_or(LocalMarketError::ScopeMismatch)?.before, 180_000);
        Ok(())
    }

    #[test]
    fn malformed_shared_history_page_backs_off_instead_of_retrying_each_frame()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_minute(1, binding.clone(), study_bar(240_000, 10)?, true)?;
        let request = store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000)
            .ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!(store.finish_shared_history(&request,
            Ok(vec![study_bar(request.before, 9)?])), Err(LocalMarketError::ScopeMismatch));
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 300_000).is_none());
        assert_eq!(store.base_minute_facts(&binding).map(<[PublicBar]>::len), Some(1));
        Ok(())
    }

    #[test]
    fn visible_interior_minute_gap_is_filled_once_and_stale_pages_do_not_loop()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_history(1, binding.clone(), vec![
            study_bar(60_000, 10)?, study_bar(180_000, 12)?,
            study_bar(240_000, 13)?, study_bar(360_000, 15)?,
        ], None)?;
        let request = store.begin_shared_history(&binding, ChartInterval::OneMinute,
            180_000, 360_000).ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!((request.before, request.gap_after), (360_000, Some(240_000)));
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute,
            60_000, 360_000).is_none());
        assert!(!history_page_covers_gap(&[study_bar(180_000, 12)?],
            request.before, request.gap_after));
        assert_eq!(store.finish_shared_history(&request,
            Ok(vec![study_bar(180_000, 12)?]))?, 0);
        let request = store.begin_shared_history(&binding, ChartInterval::OneMinute,
            60_000, 360_000).ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!((request.before, request.gap_after), (180_000, Some(60_000)));
        assert_eq!(store.finish_shared_history(&request,
            Ok(vec![study_bar(120_000, 11)?]))?, 1);
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute,
            60_000, 240_000).is_none());
        assert_eq!(store.base_minute_facts(&binding).map(<[PublicBar]>::len), Some(5));
        Ok(())
    }

    #[test]
    fn late_minute_inside_exhausted_gap_reopens_only_the_remaining_gap()
    -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        store.apply_base_history(1, binding.clone(),
            vec![study_bar(60_000, 10)?, study_bar(240_000, 14)?], None)?;
        let first = store.begin_shared_history(&binding, ChartInterval::OneMinute,
            60_000, 240_000).ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!((first.before, first.gap_after), (240_000, Some(60_000)));
        assert_eq!(store.finish_shared_history(&first,
            Ok(vec![study_bar(60_000, 10)?]))?, 0);
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute,
            60_000, 240_000).is_none());
        store.apply_base_minute(1, binding.clone(), study_bar(120_000, 11)?, true)?;
        let remaining = store.begin_shared_history(&binding, ChartInterval::OneMinute,
            60_000, 240_000).ok_or(LocalMarketError::ScopeMismatch)?;
        assert_eq!((remaining.before, remaining.gap_after), (240_000, Some(120_000)));
        assert_eq!(store.finish_shared_history(&remaining,
            Ok(vec![study_bar(180_000, 12)?]))?, 1);
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute,
            60_000, 240_000).is_none());
        Ok(())
    }

    #[test]
    fn shared_minute_source_keeps_history_beyond_display_limit() -> Result<(), LocalMarketError> {
        let selected = selection("BTC/USDT")?;
        let binding = selected.binding.clone();
        let mut store = LocalMarketStore::default();
        store.replace([selected])?;
        for minute in 1..=MAX_BARS + 1 {
            store.apply_base_minute(1, binding.clone(),
                study_bar(minute as u64 * 60_000, 10)?, true)?;
        }
        assert_eq!(store.base_minute_facts(&binding).map(<[PublicBar]>::len), Some(MAX_BARS + 1));
        assert!(store.begin_shared_history(&binding, ChartInterval::OneMinute, 0, 60_000).is_some());
        Ok(())
    }
