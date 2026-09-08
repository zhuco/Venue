use rust_decimal::Decimal;
use venue_domain::domain::{
    AccountRiskSnapshot, Amount, Asset, FieldState, Instrument, InstrumentMetadata, MarketKind,
    Order, OrderSide, OrderState, PositionSide, Precision, Price, RiskSourceStatus,
};

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn input() -> Result<MmInput, Box<dyn std::error::Error>> {
    let quote = Asset::new("USDC")?;
    let amount = |value| Amount::new(quote.clone(), Decimal::from(value));
    let symbol = "XRP/USDC".parse()?;
    let tick = Decimal::new(1, 4);
    let step = Decimal::new(1, 1);
    Ok(MmInput {
        config: MmConfig {
            symbol,
            order_notional: amount(5),
            max_leg_notional: amount(400),
            max_gross_notional: amount(800),
            max_net_notional: amount(20),
            base_half_spread_bps: Decimal::from(5),
            inventory_skew_bps: Decimal::from(10),
            volatility_multiplier: Decimal::from(2),
            volatility_period: 10,
            refresh_interval_ms: 1_000,
            max_market_age_ms: 2_000,
            max_private_age_ms: 5_000,
            max_loss_quote: amount(5),
            max_drawdown_quote: amount(5),
        },
        instrument: InstrumentMetadata::new(
            Instrument {
                symbol: "XRP/USDC".parse()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(quote.clone()),
                generation: 1,
                price_tick: Price::new(tick)?,
                quantity_step: step,
                minimum_notional: amount(5),
            },
            Precision::new(tick, tick)?,
            Precision::new(step, step)?,
            None,
            true,
        )?,
        maximum_quantity: Decimal::from(10_000),
        maximum_price: Price::new(Decimal::from(100_000))?,
        best_bid: Price::new(Decimal::new(19999, 4))?,
        best_ask: Price::new(Decimal::new(20001, 4))?,
        mark_price: Price::new(Decimal::from(2))?,
        market_observed_at_ms: 10_000,
        long_quantity: Decimal::ZERO,
        short_quantity: Decimal::ZERO,
        live_orders: Vec::new(),
        owned_order_ids: Default::default(),
        pending_quotes: Vec::new(),
        unknown_results: false,
        account: AccountRiskSnapshot {
            exchange: "venue".to_owned(),
            account: "account".to_owned(),
            risk_currency: quote.clone(),
            account_equity: Decimal::from(100),
            private_generation: 1,
            observed_at_ms: 10_000,
            source_status: RiskSourceStatus::Complete,
        },
        equity_baseline: amount(100),
        equity_peak: amount(100),
        available_open_notional: amount(100),
        volatility_bps: Decimal::ZERO,
        previous_quotes_at_ms: None,
        now_ms: 10_000,
        control: MmControl::Run,
    })
}

fn quotes(plan: MmPlan) -> Result<Vec<MmQuote>, Box<dyn std::error::Error>> {
    match plan.action {
        MmAction::Quote { quotes, .. } => Ok(quotes),
        other => Err(format!("expected quote, got {other:?}").into()),
    }
}

fn own(i: &mut MmInput, q: MmQuote, id: &str) {
    i.owned_order_ids.insert(id.to_owned());
    i.live_orders.push(Order {
        order_id: id.to_owned(),
        client_order_id: FieldState::Known(id.to_owned()),
        symbol: i.config.symbol.clone(),
        side: q.side,
        position_side: FieldState::Known(q.position_side),
        purpose: FieldState::Missing,
        state: OrderState::New,
        quantity: q.quantity,
        filled_quantity: Decimal::ZERO,
        limit_price: Some(q.price),
        time_in_force: FieldState::Missing,
        average_price: FieldState::Missing,
        reduce_only: false,
    });
}

#[test]
fn inventory_mm_admission_two_quotes_and_stable_signed_surface() -> TestResult {
    let mut i = input()?;
    let result = quotes(plan(&i)?)?;
    assert_eq!(result.len(), 2);
    assert!(
        result
            .iter()
            .all(|q| !q.reduce_only && q.quantity * q.price.value() >= Decimal::from(5))
    );
    for (n, q) in result.into_iter().enumerate() {
        own(&mut i, q, &n.to_string());
    }
    assert!(matches!(
        plan(&i)?.action,
        MmAction::Keep {
            reason: MmReason::Normal
        }
    ));
    Ok(())
}

#[test]
fn inventory_mm_admission_excess_net_cancels_then_reduces_and_keeps_exit() -> TestResult {
    let mut i = input()?;
    let increasing = quotes(plan(&i)?)?
        .into_iter()
        .find(|q| q.side == OrderSide::Buy)
        .ok_or("no bid")?;
    own(&mut i, increasing, "old_bid");
    i.long_quantity = Decimal::new(105, 1);
    assert!(matches!(
        plan(&i)?.action,
        MmAction::CancelThenReplan {
            reason: MmReason::RiskReduction,
            ..
        }
    ));
    i.live_orders.clear();
    let result = quotes(plan(&i)?)?;
    assert_eq!(result.len(), 1);
    assert!(result[0].reduce_only && result[0].side == OrderSide::Sell);
    own(&mut i, result[0].clone(), "exit");
    assert!(matches!(plan(&i)?.action, MmAction::Keep { .. }));
    Ok(())
}

#[test]
fn inventory_mm_admission_subminimum_close_survives() -> TestResult {
    let mut i = input()?;
    i.long_quantity = Decimal::new(1, 1);
    let result = quotes(plan(&i)?)?;
    let close = result
        .iter()
        .find(|q| q.reduce_only)
        .ok_or("missing subminimum close")?;
    assert_eq!(close.quantity, i.long_quantity);
    assert!(close.quantity * close.price.value() < i.instrument.instrument.minimum_notional.value);
    Ok(())
}

#[test]
fn pending_places_and_unknown_results_never_create_replacements() -> TestResult {
    let mut i = input()?;
    i.pending_quotes = quotes(plan(&i)?)?;
    assert!(matches!(
        plan(&i)?.action,
        MmAction::Keep {
            reason: MmReason::PendingConfirmation
        }
    ));
    i.unknown_results = true;
    assert!(matches!(
        plan(&i)?.action,
        MmAction::Halt {
            reason: MmReason::UnknownResults,
            ..
        }
    ));
    Ok(())
}

#[test]
fn close_short_is_risk_increasing_when_net_long() -> TestResult {
    let mut i = input()?;
    i.long_quantity = Decimal::from(20);
    i.short_quantity = Decimal::from(10);
    let result = quotes(plan(&i)?)?;
    assert!(result.iter().all(|q| q.side == OrderSide::Sell));
    Ok(())
}

#[test]
fn gross_hedging_does_not_hide_direction_risk() -> TestResult {
    let mut i = input()?;
    i.long_quantity = Decimal::from(190);
    i.short_quantity = Decimal::from(190);
    let p = plan(&i)?;
    assert_eq!(p.net_notional, Decimal::ZERO);
    assert_eq!(p.gross_notional, Decimal::from(760));
    let result = quotes(p)?;
    assert!(result.iter().all(|q| q.reduce_only));
    Ok(())
}

#[test]
fn minimum_open_roundup_must_fit_actual_net_cap() -> TestResult {
    let mut i = input()?;
    i.config.max_net_notional.value = Decimal::from(5);
    let result = plan(&i)?;
    match result.action {
        MmAction::Quote { quotes, .. } => assert!(
            quotes
                .iter()
                .all(|q| q.quantity * i.mark_price.value() <= Decimal::from(5))
        ),
        MmAction::Keep {
            reason: MmReason::NoSafeQuote,
        } => (),
        other => return Err(format!("unexpected plan {other:?}").into()),
    }
    Ok(())
}

#[test]
fn loss_drawdown_and_stale_data_cancel_without_new_orders() -> TestResult {
    let mut i = input()?;
    let q = quotes(plan(&i)?)?.remove(0);
    own(&mut i, q, "quote");
    i.account.account_equity = Decimal::from(95);
    assert!(matches!(
        plan(&i)?.action,
        MmAction::Halt {
            reason: MmReason::LossLimit,
            ..
        }
    ));
    i.account.account_equity = Decimal::from(100);
    i.equity_peak.value = Decimal::from(105);
    assert!(matches!(
        plan(&i)?.action,
        MmAction::Halt {
            reason: MmReason::DrawdownLimit,
            ..
        }
    ));
    i.equity_peak.value = Decimal::from(100);
    i.now_ms += 2_001;
    assert!(matches!(
        plan(&i)?.action,
        MmAction::Halt {
            reason: MmReason::StaleFacts,
            ..
        }
    ));
    Ok(())
}

#[test]
fn midpoint_volatility_and_inventory_move_quotes_without_grid_levels() -> TestResult {
    let mut i = input()?;
    let flat = quotes(plan(&i)?)?;
    let flat_bid = flat
        .iter()
        .find(|q| q.side == OrderSide::Buy)
        .ok_or("no bid")?
        .price;
    i.long_quantity = Decimal::from(5);
    let long = quotes(plan(&i)?)?;
    assert!(
        long.iter()
            .find(|q| q.side == OrderSide::Buy)
            .ok_or("no bid")?
            .price
            < flat_bid
    );
    i.volatility_bps = Decimal::from(10);
    let p = plan(&i)?;
    assert_eq!(p.half_spread_bps, Decimal::from(20));
    assert!(
        quotes(p)?
            .iter()
            .find(|q| q.side == OrderSide::Buy)
            .ok_or("no bid")?
            .price
            < flat_bid
    );
    Ok(())
}

#[test]
fn refreshed_quotes_cancel_before_replacement_and_retain_pending_cancel_risk() -> TestResult {
    let mut i = input()?;
    for (n, q) in quotes(plan(&i)?)?.into_iter().enumerate() {
        own(&mut i, q, &n.to_string());
    }
    i.previous_quotes_at_ms = Some(8_000);
    i.best_bid = Price::new(Decimal::new(20099, 4))?;
    i.best_ask = Price::new(Decimal::new(20101, 4))?;
    assert!(matches!(
        plan(&i)?.action,
        MmAction::CancelThenReplan { .. }
    ));
    assert!(matches!(
        plan(&i)?.action,
        MmAction::CancelThenReplan { .. }
    ));
    i.live_orders.clear();
    assert!(matches!(plan(&i)?.action, MmAction::Quote { .. }));
    Ok(())
}

#[test]
fn volatility_warmup_uses_absolute_returns_and_rejects_duplicate_time() -> TestResult {
    let mut v = MmVolatility::new(2)?;
    assert_eq!(v.update(1, Price::new(Decimal::from(100))?)?, None);
    assert_eq!(v.update(2, Price::new(Decimal::from(101))?)?, None);
    assert!(v.update(3, Price::new(Decimal::from(100))?)?.is_some());
    assert_eq!(
        v.update(3, Price::new(Decimal::from(100))?),
        Err(MmError::Facts)
    );
    Ok(())
}

#[test]
fn no_margin_permits_legal_closes_but_never_opens() -> TestResult {
    let mut i = input()?;
    i.available_open_notional.value = Decimal::ZERO;
    assert!(matches!(
        plan(&i)?.action,
        MmAction::Keep {
            reason: MmReason::NoSafeQuote
        }
    ));
    i.long_quantity = Decimal::new(1, 1);
    let result = quotes(plan(&i)?)?;
    assert_eq!(result.len(), 1);
    assert!(result[0].reduce_only);
    Ok(())
}

#[test]
fn foreign_closes_reserve_both_position_and_worst_net() -> TestResult {
    let mut i = input()?;
    i.long_quantity = Decimal::from(10);
    i.short_quantity = Decimal::from(10);
    let price = i.best_bid;
    own(
        &mut i,
        MmQuote {
            side: OrderSide::Buy,
            position_side: PositionSide::Short,
            price,
            quantity: Decimal::from(10),
            reduce_only: true,
        },
        "manual",
    );
    i.owned_order_ids.clear();
    let result = quotes(plan(&i)?)?;
    assert!(result.iter().all(|q| q.side == OrderSide::Sell));
    Ok(())
}

#[test]
fn duplicate_signed_identity_and_invalid_pending_direction_fail_closed() -> TestResult {
    let mut i = input()?;
    let q = quotes(plan(&i)?)?.remove(0);
    own(&mut i, q.clone(), "same");
    own(&mut i, q, "same");
    assert_eq!(plan(&i), Err(MmError::Facts));
    i.live_orders.clear();
    i.pending_quotes.push(MmQuote {
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        price: i.best_bid,
        quantity: Decimal::ONE,
        reduce_only: true,
    });
    assert_eq!(plan(&i), Err(MmError::Facts));
    Ok(())
}
