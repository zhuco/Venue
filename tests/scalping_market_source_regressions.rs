use std::num::NonZeroUsize;

use rust_decimal::Decimal;
use venue::{
    domain::{
        AggressorSide, FieldState, MarketDelta, MarketEvent, MarketLevel, MarketSnapshot, Price,
        PublicBar, PublicTrade,
    },
    indicator::{
        FeatureState, PublicMarketSourceError, RecordedPublicEvent, ScalpingPublicMarketSource,
    },
    market::OrderBook,
};

fn price(value: i64) -> Result<Price, Box<dyn std::error::Error>> {
    Ok(Price::new(Decimal::new(value, 0))?)
}

fn source() -> Result<ScalpingPublicMarketSource, Box<dyn std::error::Error>> {
    Ok(ScalpingPublicMarketSource::new(
        "BTC/USDT".parse()?,
        "scalping-shadow-v1",
        "0".repeat(64),
        65_000,
        NonZeroUsize::new(2_048).ok_or("history")?,
    )?)
}

fn recorded(capture_sequence: u64, received_at_ms: u64, event: MarketEvent) -> RecordedPublicEvent {
    RecordedPublicEvent {
        capture_sequence,
        received_at_ms,
        event,
    }
}

fn consume(
    source: &mut ScalpingPublicMarketSource,
    book: &mut OrderBook,
    input: RecordedPublicEvent,
    now_ms: u64,
) -> Result<venue::indicator::PublicMarketSourceOutput, PublicMarketSourceError> {
    match &input.event {
        MarketEvent::Snapshot(snapshot) => book.apply_snapshot(snapshot.clone()),
        MarketEvent::Delta(delta) => book
            .apply_delta(delta.clone())
            .map_err(|_| PublicMarketSourceError::Generation)?,
        MarketEvent::Trade(_)
        | MarketEvent::Bar(_)
        | MarketEvent::Ticker(_)
        | MarketEvent::MarkFunding(_) => {}
    }
    source.consume(input, book, now_ms)
}

fn snapshot() -> Result<MarketEvent, Box<dyn std::error::Error>> {
    Ok(MarketEvent::Snapshot(MarketSnapshot {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        sequence: 10,
        exchange_time_ms: Some(1),
        bids: vec![MarketLevel {
            price: price(100)?,
            quantity: Decimal::new(10, 0),
        }],
        asks: vec![MarketLevel {
            price: price(101)?,
            quantity: Decimal::new(10, 0),
        }],
    }))
}

fn book_delta(sequence: u64) -> Result<MarketEvent, Box<dyn std::error::Error>> {
    Ok(MarketEvent::Delta(MarketDelta {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        first_sequence: sequence,
        previous_sequence: Some(sequence - 1),
        sequence,
        exchange_time_ms: Some(1_260_100),
        bids: vec![MarketLevel {
            price: price(100)?,
            quantity: Decimal::new(11, 0),
        }],
        asks: vec![MarketLevel {
            price: price(101)?,
            quantity: Decimal::new(9, 0),
        }],
    }))
}

fn bar(index: u64) -> Result<MarketEvent, Box<dyn std::error::Error>> {
    let close_time_ms = index * 60_000;
    Ok(MarketEvent::Bar(PublicBar {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        received_at_ms: close_time_ms,
        sequence: index,
        open_time_ms: close_time_ms - 60_000,
        close_time_ms,
        interval_ms: 60_000,
        open: price(100 + index as i64)?,
        high: price(102 + index as i64)?,
        low: price(99 + index as i64)?,
        close: price(101 + index as i64)?,
        base_volume: FieldState::Unavailable {
            reason: venue::domain::UnknownReason::SourceOmitted,
        },
        quote_volume: FieldState::Unavailable {
            reason: venue::domain::UnknownReason::SourceOmitted,
        },
        trade_count: FieldState::Unavailable {
            reason: venue::domain::UnknownReason::SourceOmitted,
        },
        taker_buy_base_volume: FieldState::Unavailable {
            reason: venue::domain::UnknownReason::SourceOmitted,
        },
        taker_buy_quote_volume: FieldState::Unavailable {
            reason: venue::domain::UnknownReason::SourceOmitted,
        },
    }))
}

fn trade(index: u64) -> Result<MarketEvent, Box<dyn std::error::Error>> {
    let observed_at_ms = 1_260_001 + index;
    Ok(MarketEvent::Trade(PublicTrade {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        received_at_ms: observed_at_ms,
        exchange_time_ms: observed_at_ms,
        transaction_time_ms: observed_at_ms,
        aggregate_trade_id: index.into(),
        first_trade_id: Some(index),
        last_trade_id: Some(index),
        ordering: venue::domain::PublicTradeOrdering::NativeAggregateId,
        price: price(121)?,
        quantity: Decimal::ONE,
        quote_quantity: Decimal::new(121, 0),
        aggressor: FieldState::Known(AggressorSide::Buy),
    }))
}

#[test]
fn source_emits_only_after_book_trades_and_closed_bar_window_are_complete()
-> Result<(), Box<dyn std::error::Error>> {
    let mut source = source()?;
    let mut book = OrderBook::default();
    let mut capture_sequence = 1;
    let mut output = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1, snapshot()?),
        1,
    )?;
    assert_eq!(output.state, FeatureState::Warmup);
    assert!(output.frame.is_none());

    capture_sequence += 1;
    output = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 2, book_delta(11)?),
        2,
    )?;
    assert_eq!(output.state, FeatureState::Warmup);
    assert!(output.frame.is_none());

    for index in 1..=21 {
        capture_sequence += 1;
        output = consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, index * 60_000, bar(index)?),
            1_260_100,
        )?;
        assert!(output.frame.is_none());
    }
    for index in 1..=64 {
        capture_sequence += 1;
        output = consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, 1_260_001 + index, trade(index)?),
            1_260_100,
        )?;
        assert!(output.frame.is_none());
    }

    capture_sequence += 1;
    output = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1_260_100, book_delta(12)?),
        1_260_100,
    )?;
    let frame = output.frame.ok_or("complete public frame")?;
    assert_eq!(output.state, FeatureState::Ready);
    assert_eq!(frame.generation, 1);
    assert_eq!(frame.cursors.len(), 3);
    assert!(frame.cursors.values().all(|cursor| cursor.generation == 1));

    capture_sequence += 1;
    let throttled = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1_260_066, trade(65)?),
        1_260_200,
    )?;
    assert_eq!(throttled.state, FeatureState::Ready);
    assert!(throttled.frame.is_none());

    capture_sequence += 1;
    let next_interval = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1_260_067, trade(66)?),
        1_260_350,
    )?;
    assert_eq!(next_interval.state, FeatureState::Ready);
    assert!(next_interval.frame.is_some());

    capture_sequence += 1;
    let mut wrong_symbol = trade(67)?;
    if let MarketEvent::Trade(trade) = &mut wrong_symbol {
        trade.symbol = "ETH/USDT".parse()?;
    }
    assert!(matches!(
        consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, 1_260_066, wrong_symbol),
            1_260_100
        ),
        Err(PublicMarketSourceError::Identity)
    ));
    assert_eq!(source.state(), FeatureState::DataGap);

    capture_sequence += 1;
    assert!(matches!(
        consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, 1_260_069, trade(68)?),
            1_260_350,
        ),
        Err(PublicMarketSourceError::DataGap)
    ));

    capture_sequence += 1;
    let mut zero_generation = trade(69)?;
    if let MarketEvent::Trade(trade) = &mut zero_generation {
        trade.generation = 0;
    }
    assert!(matches!(
        consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, 1_260_068, zero_generation),
            1_260_100
        ),
        Err(PublicMarketSourceError::Identity)
    ));

    capture_sequence += 1;
    let mut next_generation = snapshot()?;
    if let MarketEvent::Snapshot(snapshot) = &mut next_generation {
        snapshot.generation = 2;
        snapshot.sequence = 20;
    }
    let rebuilt = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1_260_100, next_generation),
        1_260_100,
    )?;
    assert_eq!(rebuilt.generation, Some(2));
    assert_eq!(rebuilt.state, FeatureState::Warmup);
    assert!(rebuilt.frame.is_none());
    Ok(())
}

#[test]
fn capture_gap_fences_same_generation_until_a_new_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut source = source()?;
    let mut book = OrderBook::default();
    consume(&mut source, &mut book, recorded(1, 1, snapshot()?), 1)?;

    assert!(matches!(
        consume(&mut source, &mut book, recorded(3, 3, book_delta(11)?), 3),
        Err(PublicMarketSourceError::Sequence)
    ));
    assert_eq!(source.state(), FeatureState::DataGap);

    let mut next = snapshot()?;
    if let MarketEvent::Snapshot(snapshot) = &mut next {
        snapshot.generation = 2;
        snapshot.sequence = 20;
    }
    let output = consume(&mut source, &mut book, recorded(4, 4, next), 4)?;
    assert_eq!(output.generation, Some(2));
    assert_eq!(output.state, FeatureState::Warmup);
    assert!(output.frame.is_none());
    Ok(())
}

#[test]
fn malformed_or_unclosed_bar_never_becomes_a_ready_frame() -> Result<(), Box<dyn std::error::Error>>
{
    let mut source = source()?;
    let mut book = OrderBook::default();
    consume(&mut source, &mut book, recorded(1, 1, snapshot()?), 1)?;
    consume(&mut source, &mut book, recorded(2, 2, book_delta(11)?), 2)?;
    let mut invalid = bar(1)?;
    if let MarketEvent::Bar(bar) = &mut invalid {
        bar.interval_ms = 30_000;
    }
    assert!(matches!(
        consume(&mut source, &mut book, recorded(3, 3, invalid), 3),
        Err(PublicMarketSourceError::Feature(_))
    ));
    assert_eq!(source.state(), FeatureState::DataGap);

    assert!(matches!(
        consume(&mut source, &mut book, recorded(4, 4, trade(1)?), 4),
        Err(PublicMarketSourceError::DataGap)
    ));
    assert!(matches!(
        consume(&mut source, &mut book, recorded(5, 5, book_delta(12)?), 5),
        Err(PublicMarketSourceError::DataGap)
    ));

    let mut next_generation = snapshot()?;
    if let MarketEvent::Snapshot(snapshot) = &mut next_generation {
        snapshot.generation = 2;
        snapshot.sequence = 20;
    }
    let rebuilt = consume(&mut source, &mut book, recorded(6, 6, next_generation), 6)?;
    assert_eq!(rebuilt.generation, Some(2));
    assert_eq!(rebuilt.state, FeatureState::Warmup);
    assert!(rebuilt.frame.is_none());
    Ok(())
}

#[test]
fn mature_trades_and_bars_cannot_ready_until_book_is_bridged()
-> Result<(), Box<dyn std::error::Error>> {
    let mut source = source()?;
    let mut book = OrderBook::default();
    let mut capture_sequence = 1;
    consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1, snapshot()?),
        1,
    )?;

    for index in 1..=21 {
        capture_sequence += 1;
        let output = consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, index * 60_000, bar(index)?),
            1_260_100,
        )?;
        assert!(output.frame.is_none());
    }
    for index in 1..=64 {
        capture_sequence += 1;
        let output = consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, 1_260_001 + index, trade(index)?),
            1_260_100,
        )?;
        assert!(output.frame.is_none());
    }
    assert_eq!(source.state(), FeatureState::Warmup);
    assert!(!book.bridged());

    capture_sequence += 1;
    let output = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1_260_100, book_delta(11)?),
        1_260_100,
    )?;
    assert_eq!(output.state, FeatureState::Warmup);
    assert!(output.frame.is_none());
    assert!(book.bridged());

    for index in 1..=21 {
        capture_sequence += 1;
        let output = consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, index * 60_000, bar(index)?),
            1_260_100,
        )?;
        assert!(output.frame.is_none());
    }
    for index in 1..=64 {
        capture_sequence += 1;
        let output = consume(
            &mut source,
            &mut book,
            recorded(capture_sequence, 1_260_001 + index, trade(index)?),
            1_260_100,
        )?;
        assert!(output.frame.is_none());
    }
    capture_sequence += 1;
    let output = consume(
        &mut source,
        &mut book,
        recorded(capture_sequence, 1_260_100, book_delta(12)?),
        1_260_100,
    )?;
    assert_eq!(output.state, FeatureState::Ready);
    assert!(output.frame.is_some());
    Ok(())
}

#[test]
fn invalid_capture_identity_fences_nonzero_generation_until_a_new_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut source = source()?;
    let mut book = OrderBook::default();
    assert!(matches!(
        consume(&mut source, &mut book, recorded(0, 1, snapshot()?), 1),
        Err(PublicMarketSourceError::Identity)
    ));
    assert!(matches!(
        consume(&mut source, &mut book, recorded(1, 2, snapshot()?), 2),
        Err(PublicMarketSourceError::DataGap)
    ));

    let mut next_generation = snapshot()?;
    if let MarketEvent::Snapshot(snapshot) = &mut next_generation {
        snapshot.generation = 2;
        snapshot.sequence = 20;
    }
    let rebuilt = consume(&mut source, &mut book, recorded(2, 3, next_generation), 3)?;
    assert_eq!(rebuilt.generation, Some(2));
    assert_eq!(rebuilt.state, FeatureState::Warmup);
    Ok(())
}
