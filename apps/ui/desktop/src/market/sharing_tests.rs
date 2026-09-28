use super::*;

#[test]
fn identical_chart_configurations_share_one_engine_and_release_unused_configs()
-> Result<(), LocalMarketError> {
    let selected = MarketSelection::binance_usd_m("BTC/USDT", ChartInterval::OneMinute)?;
    let mut store = LocalMarketStore::default();
    store.replace([selected.clone()])?;
    let default = ChartStudyConfig::default();
    store.configure_chart("a", &selected, default.clone())?;
    store.configure_chart("b", &selected, default.clone())?;
    assert!(store.chart_reducers.is_empty());
    assert!(std::ptr::eq(
        store
            .chart_view("a")
            .ok_or(LocalMarketError::ScopeMismatch)?,
        store
            .view(&selected)
            .ok_or(LocalMarketError::ScopeMismatch)?
    ));
    let custom = ChartStudyConfig {
        sma_period: 2,
        ..default.clone()
    };
    store.configure_chart("a", &selected, custom.clone())?;
    store.configure_chart("b", &selected, custom.clone())?;
    assert_eq!(store.chart_reducers.len(), 1);
    assert!(std::ptr::eq(
        store
            .chart_view("a")
            .ok_or(LocalMarketError::ScopeMismatch)?,
        store
            .chart_view("b")
            .ok_or(LocalMarketError::ScopeMismatch)?
    ));
    store.configure_chart("a", &selected, default.clone())?;
    assert_eq!(store.chart_reducers.len(), 1);
    store.configure_chart("b", &selected, default)?;
    assert!(store.chart_reducers.is_empty());
    store.reconfigure_studies(custom)?;
    assert_eq!(store.chart_reducers.len(), 1);
    assert!(std::ptr::eq(
        store
            .chart_view("a")
            .ok_or(LocalMarketError::ScopeMismatch)?,
        store
            .chart_view("b")
            .ok_or(LocalMarketError::ScopeMismatch)?
    ));
    store.replace([MarketSelection::binance_usd_m(
        "ETH/USDT",
        ChartInterval::OneMinute,
    )?])?;
    assert!(store.chart_view("a").is_none());
    assert!(store.chart_reducers.is_empty());
    Ok(())
}
