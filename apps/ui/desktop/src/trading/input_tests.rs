use super::*;

#[test]
fn preset_modes_persist_and_percentages_use_checked_local_equity()
-> Result<(), Box<dyn std::error::Error>> {
    let mut settings: TradingSettings = serde_json::from_str("{}")?;
    assert_eq!(settings.size_preset_mode, SizePresetMode::Amount);
    settings.size_presets[0] = Decimal::from(77);
    settings.size_preset_mode = SizePresetMode::EquityPercent;
    for (index, expected) in [50, 100, 200, 300, 500].into_iter().enumerate() {
        assert_eq!(
            settings.preset_notional(index, Some(1000.into()))?,
            Decimal::from(expected)
        );
    }
    for equity in [None, Some(Decimal::ZERO), Some(-Decimal::ONE)] {
        assert_eq!(
            settings.preset_notional(0, equity),
            Err(TradePlanError::EquityUnavailable)
        );
    }
    let mut restored: TradingSettings = serde_json::from_str(&serde_json::to_string(&settings)?)?;
    assert_eq!(restored.size_preset_mode, SizePresetMode::EquityPercent);
    restored.size_preset_mode = SizePresetMode::Amount;
    assert_eq!(restored.preset_notional(0, None)?, Decimal::from(77));
    let state = TradeDockState {
        amount_input: "125".into(),
        ..Default::default()
    };
    assert_eq!(
        state.quote_notional(&settings, Decimal::ONE, None)?,
        Decimal::from(125)
    );
    Ok(())
}

#[test]
fn manual_inputs_never_fall_back_after_invalid_edits() {
    let mut state = TradeDockState::default();
    state.edit_price("12.34".into(), 1.0);
    assert_eq!(state.selected_price, Some(Decimal::new(1234, 2)));
    state.edit_price("broken".into(), 2.0);
    assert!(state.selected_price.is_none());
    state.edit_price("-1".into(), 3.0);
    assert!(state.selected_price.is_none());
    let settings = TradingSettings::default();
    state.amount_input = "invalid".into();
    assert_eq!(
        state.quote_notional(&settings, Decimal::ONE, None),
        Err(TradePlanError::InvalidSize)
    );
    state.amount_input = "0".into();
    assert_eq!(
        state.quote_notional(&settings, Decimal::ONE, None),
        Err(TradePlanError::InvalidSize)
    );
}

#[test]
fn base_size_uses_exact_selected_price_and_checked_arithmetic() {
    let mut state = TradeDockState {
        amount_in_base: true,
        amount_input: "0.25".into(),
        ..Default::default()
    };
    let settings = TradingSettings::default();
    assert_eq!(
        state.quote_notional(&settings, Decimal::new(100, 0), None),
        Ok(Decimal::new(25, 0))
    );
    state.amount_input = Decimal::MAX.to_string();
    assert_eq!(
        state.quote_notional(&settings, Decimal::new(100, 0), None),
        Err(TradePlanError::InvalidSize)
    );
    state.amount_input.clear();
    assert_eq!(
        state.quote_notional(&settings, Decimal::ONE, None),
        Err(TradePlanError::InvalidSize)
    );
}

#[test]
fn instance_epoch_change_invalidates_order_and_draft() {
    let mut state = TradeDockState::default();
    let mut scope = TradingScope {
        venue: "BINANCE".into(),
        trading_account_id: "account".into(),
        symbol: "BTC/USDC".into(),
        instance_id: "one".into(),
        config_epoch: 1,
    };
    state.observe_scope("BTC/USDC", Some(scope.clone()));
    state.edit_price("100".into(), 1.0);
    state.selected_order_id = Some("order".into());
    state.amount_input = "50".into();
    scope.config_epoch = 2;
    state.observe_scope("BTC/USDC", Some(scope));
    assert!(state.selected_price.is_none());
    assert!(state.selected_order_id.is_none());
    assert!(state.amount_input.is_empty());
}
