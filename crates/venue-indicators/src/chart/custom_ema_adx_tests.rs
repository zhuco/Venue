use super::*;
use crate::{
    catalog::Warmup as _,
    chart::{ChartStudyConfig, ChartStudyEngine},
};
use venue_domain::Price;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn bar(index: u64, close: i64) -> Result<PublicBar, Box<dyn std::error::Error>> {
    let close = Decimal::from(close);
    Ok(PublicBar {
        symbol: "BTC/USDC".parse()?,
        generation: 1,
        received_at_ms: index * 60_000 + 60_000,
        sequence: index + 1,
        open_time_ms: index * 60_000,
        close_time_ms: index * 60_000 + 59_999,
        interval_ms: 60_000,
        open: Price::new(close - Decimal::ONE)?,
        high: Price::new(close + Decimal::ONE)?,
        low: Price::new(close - Decimal::TWO)?,
        close: Price::new(close)?,
        base_volume: FieldState::Known(Decimal::from(10)),
        quote_volume: FieldState::Known(close * Decimal::from(10)),
        trade_count: FieldState::Known(1),
        taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
        taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
    })
}

fn config() -> EmaAdxConfig {
    EmaAdxConfig {
        ema_periods: [2, 3, 5],
        di_period: 2,
        adx_period: 2,
        atr_period: 2,
        macd_periods: [2, 4, 2],
        breakout_lookback: 2,
        volume_period: 2,
        adx_min: Decimal::ZERO,
        macd_atr: Decimal::ZERO,
        breakout_buffer_atr: Decimal::ZERO,
        min_range_atr: Decimal::ZERO,
        ..Default::default()
    }
}

#[test]
fn defaults_match_supplied_parameters_and_reject_unbounded_input() {
    let p = EmaAdxConfig::default();
    assert_eq!(p.ema_periods, [9, 21, 55]);
    assert_eq!((p.di_period, p.adx_period), (14, 6));
    assert_eq!(p.min_range_atr, Decimal::new(35, 2));
    assert!(!p.volume_filter);
    let mut invalid = p.clone();
    invalid.ema_periods[0] = usize::MAX;
    assert_eq!(invalid.validate(), Err(Error::InvalidParameters));
    invalid = p.clone();
    invalid.macd_periods = [26, 12, 9];
    assert_eq!(invalid.validate(), Err(Error::InvalidParameters));
    invalid = p;
    invalid.cooldown_atr = Decimal::NEGATIVE_ONE;
    assert_eq!(invalid.validate(), Err(Error::InvalidParameters));
}

#[test]
fn dmi_original_constructor_preserves_all_outputs_and_reset() -> TestResult {
    let mut original = Dmi::new(14)?;
    let mut explicit = Dmi::with_periods(14, 14)?;
    assert_eq!(original.warmup_period(), explicit.warmup_period());
    for run in 0..2 {
        for i in 0..150 {
            let b = bar(i, 1000 + ((i * 7 + run) % 31) as i64)?;
            assert_eq!(original.update(&b)?, explicit.update(&b)?);
        }
        original.reset();
        explicit.reset();
    }
    let mut independent = Dmi::with_periods(14, 6)?;
    assert_eq!(independent.warmup_period(), 20);
    let mut first = None;
    for i in 0..30 {
        if let Some(v) = independent.update(&bar(i, 1000 + i as i64)?)? {
            first.get_or_insert(i);
            assert!((v.adx - 100.0).abs() < 1e-9);
            assert_eq!(v.minus_di, 0.0);
        }
    }
    assert_eq!(first, Some(19));
    assert!(Dmi::with_periods(14, 0).is_err());
    Ok(())
}

#[test]
fn atr_is_simple_mean_and_macd_uses_double_histogram() -> TestResult {
    let mut study = EmaAdxStudy::new(&config())?;
    let mut macd = Macd::new(2, 4, 2)?;
    for i in 0..40 {
        let b = bar(i, 1000 + (i * i) as i64)?;
        let values = study.update(&b)?;
        let expected = macd.update(&b)?.map(|v| v.histogram * Decimal::TWO);
        assert_eq!(values.histogram, expected);
        if i == 2 {
            assert_eq!(values.atr, Some(Decimal::new(35, 1)));
        }
    }
    Ok(())
}

#[test]
fn raw_cooldown_retains_literal_source_instead_of_silent_repair() -> TestResult {
    let mut study = EmaAdxStudy::new(&config())?;
    study.bar_index = 100;
    assert!(!study.raw_cooldown([true, false], Decimal::from(100), Decimal::ONE)?);
    study.bar_index = 106;
    assert!(study.raw_cooldown([false, false], Decimal::from(110), Decimal::ONE)?);
    // A new raw signal refreshes the source condition and resets both distances.
    assert!(!study.raw_cooldown([false, true], Decimal::from(110), Decimal::ONE)?);
    Ok(())
}

#[test]
fn starts_are_environment_edges_not_position_entries() -> TestResult {
    let mut study = EmaAdxStudy::new(&config())?;
    let mut starts = 0;
    for i in 0..70 {
        let values = study.update(&bar(i, 1000 + (i * i) as i64)?)?;
        starts += values
            .signals
            .iter()
            .filter(|s| **s == EmaAdxSignal::BullStart)
            .count();
        assert!(!values.signals.contains(&EmaAdxSignal::LongEntry));
        assert_eq!(values.virtual_position, 0);
    }
    assert_eq!(starts, 1);
    assert!(signal_events([false; 2], [false; 2], [true, false], [true, false]).is_empty());
    Ok(())
}

#[test]
fn position_transition_priority_and_label_events_match_source() {
    assert_eq!(next_position(0, [true, false], [false; 2]), 1);
    assert_eq!(next_position(1, [false; 2], [true, false]), 0);
    assert_eq!(next_position(1, [false, true], [true, false]), -1);
    assert_eq!(next_position(-1, [false; 2], [false, true]), 0);
    assert_eq!(next_position(0, [false; 2], [true, true]), 0);
    assert_eq!(
        signal_events([true; 2], [true; 2], [true; 2], [false; 2]),
        vec![
            EmaAdxSignal::LongEntry,
            EmaAdxSignal::ShortEntry,
            EmaAdxSignal::LongExit,
            EmaAdxSignal::ShortExit,
            EmaAdxSignal::BullStart,
            EmaAdxSignal::BearStart,
        ]
    );
}

#[test]
fn zero_cooldown_can_emit_entry_and_volume_filter_suppresses_it() -> TestResult {
    let mut p = config();
    p.cooldown_atr = Decimal::ZERO;
    p.min_bars_between = 0;
    let mut study = EmaAdxStudy::new(&p)?;
    p.volume_filter = true;
    let mut filtered = EmaAdxStudy::new(&p)?;
    let mut entries = 0;
    for i in 0..70 {
        let b = bar(i, 1000 + (i * i) as i64)?;
        let values = study.update(&b)?;
        entries += values
            .signals
            .iter()
            .filter(|s| **s == EmaAdxSignal::LongEntry)
            .count();
        assert!(
            !filtered
                .update(&b)?
                .signals
                .contains(&EmaAdxSignal::LongEntry)
        );
    }
    assert_eq!(entries, 1);
    Ok(())
}

#[test]
fn preview_replay_and_scope_reset_do_not_mutate_confirmed_state() -> TestResult {
    let p = ChartStudyConfig {
        custom_ema_adx: Some(config()),
        ..Default::default()
    };
    let mut engine = ChartStudyEngine::with_config(&p)?;
    let mut clean = ChartStudyEngine::with_config(&p)?;
    for i in 0..100 {
        let b = bar(i, 1000 + (i * i) as i64)?;
        let preview = engine.preview(&b)?;
        assert_eq!(preview, engine.preview(&b)?);
        assert_eq!(preview, engine.ingest_closed(&b)?);
        assert_eq!(preview, clean.ingest_closed(&b)?);
    }
    assert!(engine.ingest_closed(&bar(99, 10000)?).is_err());
    let mut other = bar(100, 10000)?;
    other.generation = 2;
    assert_eq!(engine.preview(&other), Err(Error::ScopeChanged));
    other.generation = 1;
    other.symbol = "ETH/USDC".parse()?;
    assert_eq!(engine.ingest_closed(&other), Err(Error::ScopeChanged));
    assert_eq!(
        engine.ingest_closed(&bar(102, 10000)?),
        Err(Error::DiscontinuousBar)
    );
    engine.reset();
    clean.reset();
    assert_eq!(
        engine.ingest_closed(&bar(0, 1000)?)?,
        clean.ingest_closed(&bar(0, 1000)?)?
    );
    Ok(())
}

#[test]
fn zero_atr_and_missing_volume_do_not_emit_false_signals() -> TestResult {
    let mut study = EmaAdxStudy::new(&config())?;
    for i in 0..80 {
        let mut b = bar(i, 1000)?;
        b.open = b.close;
        b.high = b.close;
        b.low = b.close;
        let values = study.update(&b)?;
        assert!(values.signals.is_empty());
    }
    assert_eq!(study.position, 0);
    let mut p = config();
    p.volume_filter = true;
    p.min_bars_between = 0;
    p.cooldown_atr = Decimal::ZERO;
    let mut filtered = EmaAdxStudy::new(&p)?;
    for i in 0..50 {
        let mut b = bar(i, 1000 + (i * i) as i64)?;
        b.base_volume = FieldState::Missing;
        assert_eq!(filtered.update(&b), Err(Error::InvalidBar));
        assert_eq!(filtered.position, 0);
    }
    Ok(())
}

#[test]
fn failed_composite_update_cannot_advance_signal_state() -> TestResult {
    let p = ChartStudyConfig {
        custom_ema_adx: Some(config()),
        ..Default::default()
    };
    let mut engine = ChartStudyEngine::with_config(&p)?;
    let mut clean = ChartStudyEngine::with_config(&p)?;
    for i in 0..70 {
        let b = bar(i, 1000 + (i * i) as i64)?;
        engine.ingest_closed(&b)?;
        clean.ingest_closed(&b)?;
    }
    let good = bar(70, 6000)?;
    let mut overflowing = good.clone();
    overflowing.base_volume = FieldState::Known(Decimal::MAX);
    overflowing.quote_volume = FieldState::Known(Decimal::MAX);
    assert!(engine.ingest_closed(&overflowing).is_err());
    assert_eq!(engine.ingest_closed(&good)?, clean.ingest_closed(&good)?);
    Ok(())
}
