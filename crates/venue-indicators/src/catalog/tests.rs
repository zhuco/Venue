use rust_decimal::Decimal;
use venue_domain::{
    AggressorSide, FieldState, MarketLevel, Price, PublicBar, PublicTrade, PublicTradeOrdering,
    Symbol,
};

use super::{
    BarIndicator as _, BookIndicator as _, IndicatorError, ScalarPairIndicator as _,
    TradeIndicator as _, book::Spread, momentum::Mfi, price::DonchianChannel,
    statistics::PearsonCorrelation, trade::CumulativeVolumeDelta, trend::Sma, volatility::Atr,
    volume::Obv,
};
use crate::PublicBook;

fn bar(sequence: u64, close: i64) -> Result<PublicBar, Box<dyn std::error::Error>> {
    let open_time_ms = sequence * 60_000;
    let close = Decimal::from(close);
    let volume = Decimal::from(10);
    Ok(PublicBar {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        received_at_ms: open_time_ms + 60_000,
        sequence,
        open_time_ms,
        close_time_ms: open_time_ms + 59_999,
        interval_ms: 60_000,
        open: Price::new(close)?,
        high: Price::new(close + Decimal::ONE)?,
        low: Price::new(close - Decimal::ONE)?,
        close: Price::new(close)?,
        base_volume: FieldState::Known(volume),
        quote_volume: FieldState::Known(volume * close),
        trade_count: FieldState::Known(2),
        taker_buy_base_volume: FieldState::Known(Decimal::from(4)),
        taker_buy_quote_volume: FieldState::Known(Decimal::from(4) * close),
    })
}

fn trade(side: AggressorSide) -> Result<PublicTrade, Box<dyn std::error::Error>> {
    Ok(PublicTrade {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        received_at_ms: 1_001,
        exchange_time_ms: 1_000,
        transaction_time_ms: 1_000,
        aggregate_trade_id: 1.into(),
        first_trade_id: Some(1),
        last_trade_id: Some(1),
        ordering: PublicTradeOrdering::NativeAggregateId,
        price: Price::new(Decimal::from(100))?,
        quantity: Decimal::from(2),
        quote_quantity: Decimal::from(200),
        aggressor: FieldState::Known(side),
    })
}

#[derive(Clone)]
struct TestBook {
    symbol: Symbol,
    bids: Vec<MarketLevel>,
    asks: Vec<MarketLevel>,
}

impl PublicBook for TestBook {
    fn synchronized(&self) -> bool {
        true
    }
    fn bridged(&self) -> bool {
        true
    }
    fn symbol(&self) -> Option<&Symbol> {
        Some(&self.symbol)
    }
    fn generation(&self) -> Option<u64> {
        Some(1)
    }
    fn sequence(&self) -> Option<u64> {
        Some(1)
    }
    fn bids(&self) -> Vec<MarketLevel> {
        self.bids.clone()
    }
    fn asks(&self) -> Vec<MarketLevel> {
        self.asks.clone()
    }
}

fn book() -> Result<TestBook, Box<dyn std::error::Error>> {
    Ok(TestBook {
        symbol: "BTC/USDT".parse()?,
        bids: vec![MarketLevel {
            price: Price::new(Decimal::from(99))?,
            quantity: Decimal::from(3),
        }],
        asks: vec![MarketLevel {
            price: Price::new(Decimal::from(101))?,
            quantity: Decimal::ONE,
        }],
    })
}

#[test]
fn canonical_inputs_drive_every_migrated_indicator_family() -> Result<(), Box<dyn std::error::Error>>
{
    let bars = [bar(1, 100)?, bar(2, 101)?, bar(3, 102)?];
    let mut sma = Sma::new(3)?;
    let mut atr = Atr::new(3)?;
    let mut donchian = DonchianChannel::new(3)?;
    let mut mfi = Mfi::new(2)?;
    let mut obv = Obv::new();
    for value in &bars {
        let _ = sma.update(value)?;
        let _ = atr.update(value)?;
        let _ = donchian.update(value)?;
        let _ = mfi.update(value)?;
        let _ = obv.update(value)?;
    }
    assert_eq!(sma.update(&bar(4, 103)?)?, Some(102.0));
    assert!(atr.update(&bar(4, 103)?)?.is_some());
    assert!(donchian.update(&bar(4, 103)?)?.is_some());
    assert!(mfi.update(&bar(4, 103)?)?.is_some());
    assert!(obv.update(&bar(4, 103)?)?.is_some());

    let mut correlation = PearsonCorrelation::new(3)?;
    assert!(
        correlation
            .update(Decimal::ONE, Decimal::from(2))?
            .is_none()
    );
    assert!(
        correlation
            .update(Decimal::from(2), Decimal::from(4))?
            .is_none()
    );
    assert_eq!(
        correlation.update(Decimal::from(3), Decimal::from(6))?,
        Some(1.0)
    );

    let mut delta = CumulativeVolumeDelta::new();
    assert_eq!(delta.update(&trade(AggressorSide::Buy)?)?, Some(2.0));
    assert_eq!(delta.update(&trade(AggressorSide::Sell)?)?, Some(0.0));

    let mut spread = Spread::new();
    let output = spread.update(&book()?)?.ok_or("spread should emit")?;
    assert_eq!(output.absolute, 2.0);
    Ok(())
}

#[test]
fn volume_dependent_indicators_fail_closed_without_blocking_price_only_algorithms()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = bar(1, 100)?;
    value.base_volume = FieldState::Unavailable {
        reason: venue_domain::UnknownReason::SourceOmitted,
    };
    value.quote_volume = FieldState::Unavailable {
        reason: venue_domain::UnknownReason::SourceOmitted,
    };
    value.trade_count = FieldState::Unavailable {
        reason: venue_domain::UnknownReason::SourceOmitted,
    };
    value.taker_buy_base_volume = FieldState::Unavailable {
        reason: venue_domain::UnknownReason::SourceOmitted,
    };
    value.taker_buy_quote_volume = FieldState::Unavailable {
        reason: venue_domain::UnknownReason::SourceOmitted,
    };
    let mut sma = Sma::new(1)?;
    assert_eq!(sma.update(&value)?, Some(100.0));
    let mut mfi = Mfi::new(2)?;
    assert_eq!(mfi.update(&value), Err(IndicatorError::VolumeUnavailable));
    Ok(())
}
