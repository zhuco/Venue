use super::*;

#[test]
#[ignore = "reads public DOGE/USDC candles for 75 seconds; no credentials or orders"]
fn live_custom_indicator_stream() -> Result<(), Box<dyn std::error::Error>> {
    use crate::market_client::{LocalMarketClient, LocalMarketClientEvent};
    use std::time::{Duration, Instant};
    let selected = selection("DOGE/USDC")?;
    let mut reducer = LocalMarketReducer::new(selected.clone())?;
    reducer.reconfigure_studies(ChartStudyConfig {
        custom_ema_adx: Some(venue_indicators::chart::EmaAdxConfig::default()),
        ..Default::default()
    })?;
    let client = LocalMarketClient::start_with_context(crate::model::MarketServer::Binance, None)?;
    client.replace_subscriptions(reducer.view.generation, vec![selected])?;
    let started = Instant::now();
    let (mut updates, mut closes, mut changes) = (0, 0, 0);
    let mut previous = None;
    while started.elapsed() < Duration::from_secs(75) {
        for event in client.drain(10_000) {
            match event {
                LocalMarketClientEvent::Market(event) => {
                    let candle = matches!(event.payload, MarketPayload::WsBar { .. });
                    let closed = matches!(event.payload, MarketPayload::WsBar { closed: true, .. });
                    reducer.apply(*event)?;
                    if candle {
                        updates += 1;
                        let current = reducer
                            .view
                            .studies
                            .last()
                            .and_then(|p| p.custom_ema_adx.clone());
                        if current.is_some() && current != previous {
                            changes += 1;
                        }
                        previous = current;
                    }
                    if closed {
                        closes += 1;
                        let before = reducer.view.studies.clone();
                        reducer.rebuild_studies_and_bars()?;
                        assert_eq!(
                            before, reducer.view.studies,
                            "stream and full replay differ"
                        );
                    }
                }
                LocalMarketClientEvent::WorkerFailed(error) => return Err(error.into()),
                _ => {}
            }
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    println!("DOGE/USDC custom indicator: updates={updates}, closes={closes}, changes={changes}");
    assert!(updates > 1 && closes > 0 && changes > 1);
    Ok(())
}

#[test]
fn live_custom_studies_match_history_refresh() -> Result<(), Box<dyn std::error::Error>> {
    let selected = selection("BTC/USDT")?;
    let config = ChartStudyConfig {
        custom_ema_adx: Some(venue_indicators::chart::EmaAdxConfig::default()),
        ..Default::default()
    };
    let mut live = LocalMarketReducer::new(selected.clone())?;
    live.reconfigure_studies(config.clone())?;
    let mut history = (1..=100)
        .map(|i| study_bar(i * 60_000, 100 + (i % 13) as i64))
        .collect::<Result<Vec<_>, _>>()?;
    live.apply_history(history.clone())?;
    for i in 101..=140 {
        let mut previous_preview = None;
        for price in [115, 95, 120] {
            let forming = study_bar(i * 60_000, price)?;
            live.apply_bar(
                ui_bar_from_public(&forming)?,
                forming,
                false,
                i * 60_000 + 1,
            )?;
            let preview = live
                .view
                .studies
                .last()
                .and_then(|p| p.custom_ema_adx.clone());
            assert!(preview.is_some());
            assert_ne!(
                preview, previous_preview,
                "forming values must follow price changes"
            );
            previous_preview = preview;
        }
        let closed = study_bar(i * 60_000, 100 + (i % 13) as i64)?;
        live.apply_bar(
            ui_bar_from_public(&closed)?,
            closed.clone(),
            true,
            (i + 1) * 60_000,
        )?;
        history.push(closed);
        let mut refreshed = LocalMarketReducer::new(selected.clone())?;
        refreshed.reconfigure_studies(config.clone())?;
        refreshed.apply_history(history.clone())?;
        assert_eq!(live.view.studies, refreshed.view.studies, "bar {i}");
    }
    Ok(())
}

#[test]
fn late_close_recomputes_the_current_custom_preview() -> Result<(), Box<dyn std::error::Error>> {
    let mut live = LocalMarketReducer::new(selection("BTC/USDT")?)?;
    live.reconfigure_studies(ChartStudyConfig {
        custom_ema_adx: Some(venue_indicators::chart::EmaAdxConfig::default()),
        ..Default::default()
    })?;
    live.apply_history(
        (1..=100)
            .map(|i| study_bar(i * 60_000, 100 + (i % 13) as i64))
            .collect::<Result<Vec<_>, _>>()?,
    )?;
    let closed = study_bar(101 * 60_000, 150)?;
    let forming = study_bar(102 * 60_000, 160)?;
    live.apply_bar(
        ui_bar_from_public(&forming)?,
        forming.clone(),
        false,
        102 * 60_000 + 1,
    )?;
    live.apply_bar(ui_bar_from_public(&closed)?, closed, true, 102 * 60_000)?;
    let expected = live.studies.preview(&forming)?;
    assert_eq!(
        live.view
            .studies
            .last()
            .and_then(|p| p.custom_ema_adx.clone()),
        expected.custom_ema_adx
    );
    Ok(())
}
