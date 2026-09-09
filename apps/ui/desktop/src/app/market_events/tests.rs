use super::*;
use crate::{
    chart::ChartInterval,
    market::*,
    model::{MarketInstrument, MarketQuote, MarketServer},
};
use rust_decimal::Decimal;
use venue_control_protocol::{AggressorSide, UiBar, UiBookLevel, UiTrade};
use venue_domain::{FieldState, Price, PublicBar};

#[test]
fn startup_history_before_catalog_is_retained_and_matches_catalog_first()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MarketServer::Binance;
    let selected = selection(server);
    let mut results = Vec::new();
    for catalog_first in [false, true] {
        let mut model = AppModel::new(Default::default());
        let mut workspaces = Workspaces::default();
        let generation = model
            .local_markets
            .replace([selected.clone()])?
            .ok_or("generation")?;
        if catalog_first {
            apply_all(
                &mut model,
                &mut workspaces,
                server,
                0,
                vec![catalog("BTC/USDT", 2)],
            );
        }
        let bars = (0..300)
            .map(|index| {
                let mut candle = bar(generation);
                candle.open_time_ms += index * 60_000;
                candle.close_time_ms += index * 60_000;
                candle.sequence += index;
                candle.received_at_ms = 18_060_000;
                candle
            })
            .collect();
        apply_all(
            &mut model,
            &mut workspaces,
            server,
            0,
            vec![LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
                generation,
                selection: selected.clone(),
                event_time_ms: 18_060_000,
                received_ms: 18_060_000,
                payload: MarketPayload::RestHistory { bars },
            }))],
        );
        assert_eq!(
            model
                .local_markets
                .view(&selected)
                .ok_or("view")?
                .bars
                .len(),
            300
        );
        if !catalog_first {
            apply_all(
                &mut model,
                &mut workspaces,
                server,
                0,
                vec![catalog("BTC/USDT", 2)],
            );
        }
        let view = model.local_markets.view(&selected).ok_or("view")?;
        assert_eq!(model.local_markets.generation(), generation);
        results.push((view.bars.clone(), view.studies.clone()));
    }
    assert_eq!(results[0], results[1]);
    Ok(())
}

#[test]
fn missing_close_requests_history_resync_and_rejects_old_events()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = AppModel::new(Default::default());
    let mut workspaces = Workspaces::default();
    let server = MarketServer::Binance;
    let selected = selection(server);
    let generation = model
        .local_markets
        .replace([selected.clone()])?
        .ok_or("generation")?;
    apply_all(
        &mut model,
        &mut workspaces,
        server,
        0,
        vec![catalog("BTC/USDT", 2)],
    );
    apply_all(
        &mut model,
        &mut workspaces,
        server,
        0,
        data(server, generation),
    );
    let mut missing_predecessor = bar(generation);
    missing_predecessor.open_time_ms += 120_000;
    missing_predecessor.close_time_ms += 120_000;
    missing_predecessor.received_at_ms += 120_000;
    missing_predecessor.sequence += 2;
    apply_all(
        &mut model,
        &mut workspaces,
        server,
        0,
        vec![LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
            generation,
            selection: selected.clone(),
            event_time_ms: 240_000,
            received_ms: 240_000,
            payload: MarketPayload::WsBar {
                bar: UiBar {
                    open_time_ms: 180_000,
                    open: 100.into(),
                    high: 100.into(),
                    low: 100.into(),
                    close: 100.into(),
                    volume: 1.into(),
                },
                study_bar: Box::new(missing_predecessor),
                closed: true,
            },
        }))],
    );
    assert!(model.local_markets.generation() > generation);
    assert!(model.local_markets.selections().next().is_none());
    apply_all(
        &mut model,
        &mut workspaces,
        server,
        0,
        data(server, generation),
    );
    assert!(model.local_markets.selections().next().is_none());
    let fresh = model
        .local_markets
        .replace([selected.clone()])?
        .ok_or("fresh generation")?;
    apply_all(&mut model, &mut workspaces, server, 0, data(server, fresh));
    assert_eq!(
        model
            .local_markets
            .view(&selected)
            .ok_or("view")?
            .bars
            .len(),
        1
    );
    Ok(())
}

fn selection(server: MarketServer) -> MarketSelection {
    MarketSelection::for_server(server, "BTC/USDT", ChartInterval::OneMinute).unwrap()
}
fn catalog(symbol: &str, scale: u32) -> LocalMarketClientEvent {
    LocalMarketClientEvent::Catalog(vec![MarketInstrument {
        symbol: symbol.into(),
        price_scale: scale,
        quantity_scale: 3,
    }])
}
fn bar(generation: u64) -> PublicBar {
    let price = Price::new(100.into()).unwrap();
    PublicBar {
        symbol: "BTC/USDT".parse().unwrap(),
        generation,
        received_at_ms: 120_000,
        sequence: 1,
        open_time_ms: 60_000,
        close_time_ms: 119_999,
        interval_ms: 60_000,
        open: price,
        high: price,
        low: price,
        close: price,
        base_volume: FieldState::Known(Decimal::ONE),
        quote_volume: FieldState::Known(100.into()),
        trade_count: FieldState::Known(1),
        taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
        taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
    }
}
fn data(server: MarketServer, generation: u64) -> Vec<LocalMarketClientEvent> {
    let payloads = vec![
        MarketPayload::RestHistory {
            bars: vec![bar(generation)],
        },
        MarketPayload::Trade(UiTrade {
            trade_id: "1".into(),
            occurred_ms: 120_001,
            price: 101.into(),
            quantity: 1.into(),
            aggressor: AggressorSide::Buy,
        }),
        MarketPayload::BookSnapshot {
            bids: vec![UiBookLevel {
                price: 100.into(),
                quantity: 1.into(),
            }],
            asks: vec![UiBookLevel {
                price: 102.into(),
                quantity: 1.into(),
            }],
        },
        MarketPayload::Status {
            status: MarketStatus::Live,
            detail: Some("source online".into()),
        },
    ];
    let mut events: Vec<_> = payloads
        .into_iter()
        .map(|payload| {
            LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
                generation,
                selection: selection(server),
                event_time_ms: 120_001,
                received_ms: 120_002,
                payload,
            }))
        })
        .collect();
    events.push(LocalMarketClientEvent::Quotes(vec![MarketQuote {
        symbol: "BTC/USDT".into(),
        last: 101.into(),
        change_percent_24h: Decimal::ZERO,
        quote_volume_24h: Some(100.into()),
        exchange_time_ms: 120_001,
        received_ms: 120_002,
    }]));
    events
}
fn apply_all(
    model: &mut AppModel,
    workspaces: &mut Workspaces,
    server: MarketServer,
    epoch: u64,
    events: Vec<LocalMarketClientEvent>,
) {
    for event in events {
        apply(
            model,
            workspaces,
            server,
            epoch,
            event,
            &egui::Context::default(),
        );
    }
}
fn empty(model: &AppModel) {
    assert!(model.local_markets.view_for_symbol("BTC/USDT").is_none());
    assert!(model.local_markets.chart_view("fixture").is_none());
    assert!(model.local_quotes.is_empty());
    assert!(model.local_symbols.is_empty());
    assert!(model.local_precisions.is_empty());
    assert!(model.history_requests.is_empty());
    assert!(model.trade_dock.selected_price.is_none());
}

#[test]
fn market_switch_same_symbol_interval_interleaves_every_old_result() {
    let mut model = AppModel::new(Default::default());
    model.select_symbol("BTC/USDT".into());
    let mut workspaces = Workspaces::default();
    let binance = model
        .local_markets
        .replace([selection(MarketServer::Binance)])
        .unwrap()
        .unwrap();
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Binance,
        0,
        vec![catalog("BTC/USDT", 2)],
    );
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Binance,
        0,
        data(MarketServer::Binance, binance),
    );
    let view = model
        .local_markets
        .view(&selection(MarketServer::Binance))
        .unwrap();
    assert_eq!(
        (
            view.bars.len(),
            view.bids.len(),
            view.asks.len(),
            view.trades.len()
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(view.last, Some(101.into()));
    assert_eq!(view.status, MarketStatus::Live);
    model
        .local_markets
        .configure_chart(
            "fixture",
            &selection(MarketServer::Binance),
            Default::default(),
        )
        .unwrap();
    let history = model
        .local_markets
        .begin_history(&selection(MarketServer::Binance), false)
        .unwrap();
    model.history_requests.push(history.clone());
    model.trade_dock.select_price(101.into(), 0.0).unwrap();
    model.select_market_server(MarketServer::Bybit);
    empty(&model);
    assert_eq!(model.preferences.selected_symbol, "BTC/USDT");
    let epoch = model.market_generation;
    let bybit = model
        .local_markets
        .replace([selection(MarketServer::Bybit)])
        .unwrap()
        .unwrap();
    // Data cannot make the old symbol/precision usable before this source's catalog.
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        epoch,
        data(MarketServer::Bybit, bybit),
    );
    assert!(
        model
            .local_markets
            .view(&selection(MarketServer::Bybit))
            .unwrap()
            .bars
            .is_empty()
    );
    let mut late = data(MarketServer::Binance, binance);
    late.extend([
        catalog("ETH/USDT", 7),
        LocalMarketClientEvent::History {
            request: history,
            result: Err("old history".into()),
        },
        LocalMarketClientEvent::CatalogUnavailable("old catalog".into()),
        LocalMarketClientEvent::QuotesUnavailable("old quotes".into()),
        LocalMarketClientEvent::WorkerFailed("old worker".into()),
    ]);
    apply_all(&mut model, &mut workspaces, MarketServer::Binance, 0, late);
    assert!(model.local_symbols.is_empty() && model.local_catalog_error.is_none());
    assert!(model.notices.is_empty());
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        epoch,
        vec![catalog("BTC/USDT", 4)],
    );
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        epoch,
        data(MarketServer::Bybit, bybit),
    );
    assert_eq!(model.market_scales("BTC/USDT"), (4, 3));
    let before = model
        .local_markets
        .view(&selection(MarketServer::Bybit))
        .unwrap()
        .clone();
    // Matching worker identity is insufficient when the envelope has a different venue or selection.
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        epoch,
        data(MarketServer::Binance, bybit),
    );
    assert_eq!(
        model.local_markets.view(&selection(MarketServer::Bybit)),
        Some(&before)
    );
    model.select_market_server(MarketServer::Okx);
    empty(&model);
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        epoch,
        data(MarketServer::Bybit, bybit),
    );
    empty(&model);
    let okx_epoch = model.market_generation;
    let okx = model
        .local_markets
        .replace([selection(MarketServer::Okx)])
        .unwrap()
        .unwrap();
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Okx,
        okx_epoch,
        vec![catalog("BTC/USDT", 1)],
    );
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Okx,
        okx_epoch,
        data(MarketServer::Okx, okx),
    );
    assert_eq!(
        model
            .local_markets
            .view(&selection(MarketServer::Okx))
            .unwrap()
            .status,
        MarketStatus::Live
    );
    assert_eq!(model.preferences.selected_symbol, "BTC/USDT");
    assert_eq!(
        selection(MarketServer::Okx).interval,
        ChartInterval::OneMinute
    );
}

#[test]
fn market_switch_catalog_fallback_worker_failure_and_return_generation() {
    let mut model = AppModel::new(Default::default());
    let mut workspaces = Workspaces::default();
    model.select_market_server(MarketServer::Bybit);
    let old_epoch = model.market_generation;
    model.select_market_server(MarketServer::Okx);
    model.select_market_server(MarketServer::Bybit);
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        old_epoch,
        vec![catalog("ETH/USDT", 7)],
    );
    assert!(model.local_symbols.is_empty());
    model.trade_dock.select_price(100.into(), 0.0).unwrap();
    for (_, tile) in workspaces.trading.tiles.iter_mut() {
        if let egui_tiles::Tile::Pane(pane) = tile {
            pane.viewport.pan_by_bars(1000, 30);
        }
    }
    let epoch = model.market_generation;
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        epoch,
        vec![catalog("BTC/USDT", 4)],
    );
    assert_eq!(model.preferences.selected_symbol, "BTC/USDT");
    assert!(model.trade_dock.price_input.is_empty());
    for (_, tile) in workspaces.trading.tiles.iter() {
        if let egui_tiles::Tile::Pane(pane) = tile {
            assert_eq!(pane.viewport.right_offset(), 0);
        }
    }
    apply_all(
        &mut model,
        &mut workspaces,
        MarketServer::Bybit,
        epoch,
        vec![LocalMarketClientEvent::WorkerFailed(
            "fixture start failed".into(),
        )],
    );
    empty(&model);
    assert_eq!(
        model.local_catalog_error.as_deref(),
        Some("fixture start failed")
    );
}
