use super::*;

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
        state.quote_notional(&settings, Decimal::ONE),
        Err(TradePlanError::InvalidSize)
    );
    state.amount_input = "0".into();
    assert_eq!(
        state.quote_notional(&settings, Decimal::ONE),
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
        state.quote_notional(&settings, Decimal::new(100, 0)),
        Ok(Decimal::new(25, 0))
    );
    state.amount_input = Decimal::MAX.to_string();
    assert_eq!(
        state.quote_notional(&settings, Decimal::new(100, 0)),
        Err(TradePlanError::InvalidSize)
    );
    state.amount_input.clear();
    assert_eq!(
        state.quote_notional(&settings, Decimal::ONE),
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
