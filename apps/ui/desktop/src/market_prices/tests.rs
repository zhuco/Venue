use super::*;

#[test]
fn tab_retains_real_stale_trade_without_relaxing_execution_prices() {
    let mut model = AppModel::new(Default::default());
    model.preferences.market_server = crate::model::MarketServer::Binance;
    model.local_quotes.insert(
        "BTC/USDC".into(),
        crate::model::MarketQuote {
            symbol: "BTC/USDC".into(),
            last: 100.into(),
            exchange_time_ms: 10_000,
            received_ms: 10_001,
            change_percent_24h: Decimal::ZERO,
            quote_volume_24h: None,
        },
    );
    assert_eq!(
        model.market_prices("BTC/USDC", 30_000).reference_price(),
        None
    );
    assert_eq!(
        model
            .last_trade_for_tab("BTC/USDC", 30_000)
            .map(|p| p.value),
        Some(100.into())
    );
    assert_eq!(model.last_trade_for_tab("DOGE/USDC", 30_000), None);
    assert_eq!(model.last_trade_for_tab("BTC/USDC", 1_000), None);
    model.preferences.market_server = crate::model::MarketServer::Hyperliquid;
    assert_eq!(model.last_trade_for_tab("BTC/USDC", 30_000), None);
}

#[cfg(not(target_arch = "wasm32"))]
use crate::market::{MarketEnvelope, MarketPayload, MarketSelection};

#[test]
fn a_recent_receipt_cannot_freshen_an_old_or_future_price() {
    let mut prices = MarketPrices::default();
    for (event, received) in [(1, 10_000), (10_001, 10_000), (10_000, 10_001), (0, 10_000)] {
        prices.observe_last(100.into(), event, received, 10_000);
        assert_eq!(prices.reference_price(), None);
    }
    prices.observe_last(100.into(), 9_000, 10_000, 10_000);
    prices.observe_last(1.into(), 8_000, 10_000, 10_000);
    assert_eq!(prices.reference_price(), Some(100.into()));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn every_consumer_uses_the_same_scoped_price_and_book_clock()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = AppModel::new(Default::default());
    let selection =
        MarketSelection::binance_usd_m("BTC/USDT", crate::chart::ChartInterval::OneMinute)?;
    model.local_markets.replace([selection.clone()])?;
    let generation = model
        .local_markets
        .view(&selection)
        .ok_or("view")?
        .generation;
    let mut send = |event, received, payload| {
        model.local_markets.apply(MarketEnvelope {
            generation,
            selection: selection.clone(),
            event_time_ms: event,
            received_ms: received,
            payload,
        })
    };
    send(
        10_000,
        10_000,
        MarketPayload::Bbo {
            bid: 99.into(),
            ask: 101.into(),
        },
    )?;
    // Depth has its own clock. A newer BBO cannot starve depth, nor can older depth rewind BBO.
    send(
        9_000,
        10_000,
        MarketPayload::BookSnapshot {
            bids: vec![venue_control_protocol::UiBookLevel {
                price: 98.into(),
                quantity: Decimal::ONE,
            }],
            asks: vec![venue_control_protocol::UiBookLevel {
                price: 102.into(),
                quantity: Decimal::ONE,
            }],
        },
    )?;
    let view = model.local_markets.view(&selection).ok_or("view")?;
    assert_eq!(view.bids[0].price, Decimal::from(98));
    assert_eq!(view.bid, Some(99.into()));
    assert_eq!(view.depth_event_ms, Some(9_000));
    let prices = model.market_prices("BTC/USDT", 10_000);
    assert_eq!(
        prices.position_price(PositionSide::Long, Decimal::ONE),
        Some(99.into())
    );
    assert_eq!(
        prices.position_price(PositionSide::Short, Decimal::ONE),
        Some(101.into())
    );
    assert_eq!(
        prices.position_price(PositionSide::Net, -Decimal::ONE),
        Some(101.into())
    );
    assert_eq!(model.market_prices("ETH/USDT", 10_000).bid, None);
    model.local_quotes.insert(
        "BTC/USDT".into(),
        crate::model::MarketQuote {
            symbol: "BTC/USDT".into(),
            last: 100.into(),
            exchange_time_ms: 10_000,
            received_ms: 10_000,
            change_percent_24h: Decimal::ZERO,
            quote_volume_24h: None,
        },
    );
    model.local_markets.apply(MarketEnvelope {
        generation,
        selection: selection.clone(),
        event_time_ms: 16_000,
        received_ms: 16_000,
        payload: MarketPayload::Trade(venue_control_protocol::UiTrade {
            trade_id: "fill".into(),
            occurred_ms: 16_000,
            price: 105.into(),
            quantity: Decimal::ONE,
            aggressor: venue_control_protocol::AggressorSide::Buy,
        }),
    })?;
    let prices = model.market_prices("BTC/USDT", 16_000);
    assert_eq!(prices.reference_price(), Some(105.into()));
    assert_eq!(prices.bid, None);
    assert!(model.market_depth("BTC/USDT", 16_000).is_none());
    assert_eq!(
        prices.position_price(PositionSide::Long, Decimal::ONE),
        prices.reference_price()
    );
    assert_eq!(
        model.market_prices("BTC/USDT", 21_001).reference_price(),
        None
    );
    model.preferences.market_server = crate::model::MarketServer::Bybit;
    assert_eq!(
        model.market_prices("BTC/USDT", 16_000).reference_price(),
        None
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn public_trade_updates_alerts_without_a_ticker_or_private_account()
-> Result<(), Box<dyn std::error::Error>> {
    let mut model = AppModel::new(Default::default());
    let selection =
        MarketSelection::binance_usd_m("BTC/USDT", crate::chart::ChartInterval::OneMinute)?;
    model.local_markets.replace([selection.clone()])?;
    let generation = model
        .local_markets
        .view(&selection)
        .ok_or("view")?
        .generation;
    let now = crate::account_center::now_ms();
    assert!(model.preferences.chart_alerts.add("BTC/USDT", 105.into()));
    for (time, price) in [(now - 2, 100), (now - 1, 110)] {
        model.local_markets.apply(MarketEnvelope {
            generation,
            selection: selection.clone(),
            event_time_ms: time,
            received_ms: time,
            payload: MarketPayload::Trade(venue_control_protocol::UiTrade {
                trade_id: time.to_string(),
                occurred_ms: time,
                price: price.into(),
                quantity: Decimal::ONE,
                aggressor: venue_control_protocol::AggressorSide::Buy,
            }),
        })?;
        crate::chart_trading::poll(&mut model);
    }
    assert!(model.preferences.chart_alerts.notification.is_some());
    assert!(model.local_quotes.is_empty());
    assert!(model.execution.private_projection_for(None).is_none());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn mark_ticker_never_overwrites_a_fresh_actual_trade() -> Result<(), Box<dyn std::error::Error>> {
    let mut model = AppModel::new(Default::default());
    model.preferences.market_server = crate::model::MarketServer::Hyperliquid;
    let selection = MarketSelection::for_server(
        model.preferences.market_server,
        "BTC/USDC",
        crate::chart::ChartInterval::OneMinute,
    )?;
    model.local_markets.replace([selection.clone()])?;
    let generation = model
        .local_markets
        .view(&selection)
        .ok_or("view")?
        .generation;
    model.local_quotes.insert(
        "BTC/USDC".into(),
        crate::model::MarketQuote {
            symbol: "BTC/USDC".into(),
            last: 100.into(),
            exchange_time_ms: 10_000,
            received_ms: 10_000,
            change_percent_24h: Decimal::ZERO,
            quote_volume_24h: None,
        },
    );
    assert!(model.market_prices("BTC/USDC", 10_000).is_mark());
    model.local_markets.apply(MarketEnvelope {
        generation,
        selection,
        event_time_ms: 9_000,
        received_ms: 10_000,
        payload: MarketPayload::Trade(venue_control_protocol::UiTrade {
            trade_id: "trade".into(),
            occurred_ms: 9_000,
            price: 99.into(),
            quantity: Decimal::ONE,
            aggressor: venue_control_protocol::AggressorSide::Buy,
        }),
    })?;
    let prices = model.market_prices("BTC/USDC", 10_000);
    assert_eq!(prices.reference_price(), Some(99.into()));
    assert!(!prices.is_mark());
    assert_eq!(prices.mark.map(|price| price.value), Some(100.into()));
    Ok(())
}
