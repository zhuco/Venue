use super::{Plan, TakeProfitOrder};
use rust_decimal::Decimal;
use venue_control_protocol::support_martingale::SupportMartingaleInstance;
use venue_domain::{PositionSide, Symbol};
use venue_execution::SignedAccountSnapshot;

// A latched stop continues after a cancel/price rebound. Ambiguous orders remain in the
// existing durable account queue; a rejected stop requires attention instead of a blind retry.
pub(super) fn stop_loss_plan(
    instance: &SupportMartingaleInstance,
    symbol: &Symbol,
    snapshot: &SignedAccountSnapshot,
    take_profit: Option<&TakeProfitOrder>,
    now: u64,
) -> Option<Plan> {
    let state = instance
        .symbols
        .iter()
        .find(|state| state.symbol == *symbol)?;
    if state.quantity <= Decimal::ZERO
        || state.status == "sl_failed"
        || snapshot.observed_at_ms() > now
        || now.saturating_sub(snapshot.observed_at_ms()) > 5_000
    {
        return None;
    }
    let position = snapshot
        .positions()
        .iter()
        .find(|p| p.symbol == *symbol && p.position_side == PositionSide::Long)?;
    let latched = state.status == "sl_ready";
    if !latched {
        let setting = instance
            .config
            .symbol_parameters
            .iter()
            .find(|p| p.symbol == *symbol)?
            .stop_loss?;
        let trigger = setting.trigger_price(position.entry_price?)?;
        let mark = position.mark_price.filter(|price| *price > Decimal::ZERO)?;
        if mark > trigger {
            return None;
        }
    }
    if let Some(order) = take_profit {
        return Some(Plan::CancelTakeProfit {
            symbol: symbol.clone(),
            client_order_id: order.client_order_id.clone(),
            for_stop_loss: true,
        });
    }
    let quantity = position.quantity.min(state.quantity);
    (quantity > Decimal::ZERO).then(|| Plan::MarketStopLoss {
        symbol: symbol.clone(),
        quantity,
        position_generation: snapshot.private_generation(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::support_martingale::*;
    use venue_execution::{SignedAccountPositionFact, SignedAccountPositionMode};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    fn instance() -> Result<SupportMartingaleInstance, Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDT".parse()?;
        Ok(SupportMartingaleInstance {
            instance_id: "instance".into(),
            owner_user_id: "owner".into(),
            credential_id: "credential".into(),
            trading_account_id: "00000000-0000-4000-8000-000000000001".into(),
            execution_venue: VenueId::Bybit,
            mode: GatewayMode::Live,
            lifecycle: SupportMartingaleLifecycle::Running,
            health: SupportMartingaleHealth::Healthy,
            revision: 1,
            reserved_budget: Decimal::ZERO,
            config: SupportMartingaleConfig {
                entry_mode: MartingaleEntryMode::Support,
                symbol_parameters: vec![MartingaleSymbolParameters {
                    symbol: symbol.clone(),
                    entry_price: None,
                    add_drop_rate: Decimal::new(2, 2),
                    stop_loss: Some(MartingaleStopLoss::AveragePricePercent {
                        rate: Decimal::new(1, 1),
                    }),
                }],
                reference_venue: VenueId::Binance,
                execution_venue: VenueId::Bybit,
                symbols: vec![symbol.clone()],
                total_budget: Decimal::from(1000),
                first_order_notional: Decimal::from(5),
                max_entries: 3,
                size_multiplier: Decimal::ONE,
                target_profit_rate: Decimal::new(5, 3),
                minimum_profit_quote: Decimal::ZERO,
                max_active_positions: 1,
            },
            symbols: vec![SupportMartingaleSymbolState {
                symbol,
                cycle_id: Some("cycle".into()),
                layer: 1,
                average_price: Some(Decimal::from(100)),
                quantity: Decimal::from(2),
                invested: Decimal::from(200),
                take_profit_price: None,
                net_pnl: None,
                status: "holding".into(),
                health_reason: None,
            }],
        })
    }

    fn snapshot(
        instance: &SupportMartingaleInstance,
        mark: Option<i64>,
        quantity: i64,
    ) -> Result<SignedAccountSnapshot, Box<dyn std::error::Error>> {
        let symbol = instance.config.symbols[0].clone();
        Ok(SignedAccountSnapshot::complete(
            GatewayBinding::new(
                VenueId::Bybit,
                GatewayMode::Live,
                &instance.trading_account_id,
                symbol.clone(),
            )?,
            1000,
            1,
            7,
            1,
            SignedAccountPositionMode::Hedge,
            vec![],
            vec![SignedAccountPositionFact {
                symbol,
                position_side: PositionSide::Long,
                quantity: Decimal::from(quantity),
                entry_price: Some(Decimal::from(100)),
                mark_price: mark.map(Decimal::from),
            }],
            "cursor".into(),
            vec![],
        )?)
    }

    #[test]
    fn fixed_layers_use_execution_prices_and_stop_floor_without_support_history()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::super::{NoopReason, PlannerInput, ReferenceSnapshot, plan};
        use venue_domain::{
            Amount, Asset, Instrument, InstrumentMetadata, MarketKind, Precision, Price,
        };
        let mut instance = instance()?;
        instance.config.entry_mode = MartingaleEntryMode::FixedPrice;
        instance.config.symbol_parameters[0].entry_price = Some(Decimal::from(100));
        let symbol = instance.config.symbols[0].clone();
        let account = snapshot(&instance, Some(97), 2)?;
        let asset = Asset::new("USDT")?;
        let precision = Precision::new(Decimal::new(1, 3), Decimal::new(1, 3))?;
        let mut market = venue_execution::DurableMarketFacts {
            binding: account.binding().clone(),
            reference_price: Price::new(Decimal::from(99))?,
            observed_at_ms: 1000,
            maximum_quantity: None,
            maximum_price: None,
            metadata: InstrumentMetadata::new(
                Instrument {
                    symbol: symbol.clone(),
                    market: MarketKind::LinearPerpetual,
                    settlement_asset: Some(asset.clone()),
                    generation: 1,
                    price_tick: Price::new(Decimal::new(1, 2))?,
                    quantity_step: Decimal::new(1, 3),
                    minimum_notional: Amount::new(asset, Decimal::ONE),
                },
                Precision::new(Decimal::new(1, 2), Decimal::new(1, 2))?,
                precision,
                None,
                true,
            )?,
        };
        let reference = ReferenceSnapshot {
            fetched_at_ms: 1000,
            btc_environment: vec![],
            symbols: Default::default(),
        };
        let consumed = Default::default();
        let evaluate = |instance: &SupportMartingaleInstance,
                        market: &venue_execution::DurableMarketFacts| {
            plan(&PlannerInput {
                instance,
                symbol: &symbol,
                account: &account,
                reference: &reference,
                execution_market: market,
                consumed_supports: &consumed,
                last_support_lower: Some(Decimal::from(100)),
                current_take_profit: None,
                prefer_add_after_cancel: true,
                now_ms: 1001,
            })
        };
        assert_eq!(
            evaluate(&instance, &market),
            Plan::Noop(NoopReason::NoSupport)
        );
        market.reference_price = Price::new(Decimal::from(98))?;
        assert!(
            matches!(evaluate(&instance, &market), Plan::MarketEntry {layer: 1, support_upper, ..} if support_upper == Decimal::from(98))
        );
        market.reference_price = Price::new(Decimal::from(90))?;
        assert_eq!(
            evaluate(&instance, &market),
            Plan::Noop(NoopReason::ExitOnly)
        );
        instance.symbols[0].layer = instance.config.max_entries;
        market.reference_price = Price::new(Decimal::from(93))?;
        assert_eq!(
            evaluate(&instance, &market),
            Plan::Noop(NoopReason::BudgetExhausted)
        );
        Ok(())
    }

    #[test]
    fn stop_cancels_take_profit_then_latches_across_a_price_rebound()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut instance = instance()?;
        let symbol = instance.config.symbols[0].clone();
        let snapshot = snapshot(&instance, Some(89), 1)?;
        let tp = TakeProfitOrder {
            client_order_id: "original-tp".into(),
            price: Decimal::from(110),
            quantity: Decimal::from(2),
            filled_quantity: Decimal::ONE,
        };
        assert!(
            matches!(stop_loss_plan(&instance, &symbol, &snapshot, Some(&tp), 1001), Some(Plan::CancelTakeProfit { for_stop_loss: true, client_order_id, .. }) if client_order_id == "original-tp")
        );
        instance.symbols[0].status = "sl_ready".into();
        let rebound = self::snapshot(&instance, Some(105), 1)?;
        assert!(
            matches!(stop_loss_plan(&instance, &symbol, &rebound, None, 1001), Some(Plan::MarketStopLoss { quantity, position_generation: 7, .. }) if quantity == Decimal::ONE)
        );
        Ok(())
    }

    #[test]
    fn disabled_missing_stale_and_failed_stops_do_not_send()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut instance = instance()?;
        let symbol = instance.config.symbols[0].clone();
        assert!(
            stop_loss_plan(
                &instance,
                &symbol,
                &snapshot(&instance, None, 2)?,
                None,
                1001
            )
            .is_none()
        );
        assert!(
            stop_loss_plan(
                &instance,
                &symbol,
                &snapshot(&instance, Some(90), 2)?,
                None,
                6001
            )
            .is_none()
        );
        assert!(
            stop_loss_plan(
                &instance,
                &symbol,
                &snapshot(&instance, Some(90), 2)?,
                None,
                999
            )
            .is_none()
        );
        instance.symbols[0].status = "sl_failed".into();
        assert!(
            stop_loss_plan(
                &instance,
                &symbol,
                &snapshot(&instance, Some(80), 2)?,
                None,
                1001
            )
            .is_none()
        );
        instance.symbols[0].status = "holding".into();
        instance.config.symbol_parameters[0].stop_loss = None;
        assert!(
            stop_loss_plan(
                &instance,
                &symbol,
                &snapshot(&instance, Some(80), 2)?,
                None,
                1001
            )
            .is_none()
        );
        Ok(())
    }
}
