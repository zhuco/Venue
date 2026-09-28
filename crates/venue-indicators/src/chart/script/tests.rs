use super::*;
use venue_domain::{FieldState, Price, PublicBar};
fn bar(index: u64, close: i64) -> Result<PublicBar, Box<dyn std::error::Error>> {
    let price = Price::new(Decimal::from(close))?;
    Ok(PublicBar {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        received_at_ms: (index + 1) * 60_000,
        sequence: index + 1,
        open_time_ms: index * 60_000,
        close_time_ms: (index + 1) * 60_000 - 1,
        interval_ms: 60_000,
        open: price,
        high: price,
        low: price,
        close: price,
        base_volume: FieldState::Known(10.into()),
        quote_volume: FieldState::Known((close * 10).into()),
        trade_count: FieldState::Known(1),
        taker_buy_base_volume: FieldState::Known(5.into()),
        taker_buy_quote_volume: FieldState::Known((close * 5).into()),
    })
}
fn spec(source: &str) -> ScriptSpec {
    ScriptSpec {
        id: 7,
        source: source.into(),
    }
}
#[test]
fn references_rolling_valuewhen_and_missing_gaps() -> Result<(), Box<dyn std::error::Error>> {
    let mut e = ScriptEngine::compile(&spec(
        "v=valuewhen(close>102,close,0); plot(v); plot(highest(close[1],2)); plot(close*0/0);",
    ))?;
    for i in 0..3 {
        let f = e.update(&bar(i, 100 + i as i64)?);
        assert!(f.lines[0].value.is_none());
        assert!(f.lines[2].value.is_none());
        if i == 2 {
            assert_eq!(f.lines[1].value, Some(101.into()));
        }
    }
    assert_eq!(e.update(&bar(3, 105)?).lines[0].value, Some(105.into()));
    assert_eq!(e.update(&bar(4, 101)?).lines[0].value, Some(105.into()));
    Ok(())
}
#[test]
fn closed_and_preview_are_equal_without_double_advance() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = crate::chart::ChartStudyConfig {
        custom_scripts: vec![spec("plot(ema(close,2)); [t]=td(close); plot(t);")],
        ..Default::default()
    };
    let mut e = crate::chart::ChartStudyEngine::with_config(&cfg)?;
    for i in 0..8 {
        let b = bar(i, 100 + i as i64)?;
        let a = e.preview(&b)?.custom_scripts;
        assert_eq!(a, e.preview(&b)?.custom_scripts);
        assert_eq!(a, e.ingest_closed(&b)?.custom_scripts);
    }
    e.reset();
    assert!(
        e.ingest_closed(&bar(20, 120)?)?.custom_scripts[0].lines[0]
            .value
            .is_none()
    );
    Ok(())
}
#[test]
fn rejects_unsupported_and_unbounded_code() {
    for source in [
        "plot(fetch(close));",
        "plot(ema(close,0));",
        "plot(close[1001]);",
        "x=1; x=2; plot(x);",
        "plot(close, mystery=3);",
        "plot(close); while(true) {}",
    ] {
        assert!(ScriptEngine::compile(&spec(source)).is_err(), "{source}");
    }
    assert!(ScriptEngine::compile(&spec(&" ".repeat(MAX_SOURCE_BYTES + 1))).is_err());
}

#[test]
fn missing_input_resets_rolling_scalar_warmup() -> Result<(), Box<dyn std::error::Error>> {
    let mut e = ScriptEngine::compile(&spec(
        "x=close==102 ? 0/0 : close; plot(ema(x,2)); plot(sma(x,2));",
    ))?;
    assert!(e.update(&bar(0, 100)?).lines[0].value.is_none());
    assert!(e.update(&bar(1, 101)?).lines[0].value.is_some());
    for i in 2..4 {
        assert!(
            e.update(&bar(i, 100 + i as i64)?)
                .lines
                .iter()
                .all(|l| l.value.is_none())
        );
    }
    assert!(
        e.update(&bar(4, 104)?)
            .lines
            .iter()
            .all(|l| l.value.is_some())
    );
    Ok(())
}
