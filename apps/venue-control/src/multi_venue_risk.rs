//! Quote-denominated limits for independent strategies. No USDC/USDT parity is assumed.
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::{ExecutionCommand, OrderPurpose, OrderSide, PositionSide};
use venue_execution::{DurableMarketFacts, SignedAccountSnapshot};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyRiskLimits {
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_symbol_notional: Decimal,
}

impl StrategyRiskLimits {
    pub fn validate(&self) -> bool {
        self.max_order_notional > Decimal::ZERO
            && self.max_symbol_notional >= self.max_order_notional
            && self.max_symbol_notional < Decimal::MAX
    }
}

pub fn market_guard(
    command: &ExecutionCommand,
    snapshot: &SignedAccountSnapshot,
    market: &DurableMarketFacts,
    limits: Option<&StrategyRiskLimits>,
    now_ms: u64,
) -> bool {
    if matches!(command, ExecutionCommand::Cancel(_)) {
        return true;
    }
    if market.binding != *snapshot.binding()
        || market.metadata.instrument.symbol != command.mutation_owner().symbol
        || market.metadata.validate().is_err()
        || !market.metadata.trading_enabled
        || market.observed_at_ms == 0
        || now_ms < market.observed_at_ms
        || now_ms.saturating_sub(market.observed_at_ms) > 5_000
    {
        return false;
    }
    let (quantity, price, increasing) = match command {
        ExecutionCommand::PlaceLimit(c) => (c.quantity, c.limit_price.value(), !c.reduce_only),
        ExecutionCommand::PlaceMarket(c) => (c.quantity, market.reference_price.value(), true),
        ExecutionCommand::MarketReduce(c) => (c.quantity, market.reference_price.value(), false),
        ExecutionCommand::StopMarketFullPosition(c) => {
            let long = match c.position_side {
                PositionSide::Long => true,
                PositionSide::Short => false,
                PositionSide::Net => c.side == OrderSide::Sell,
            };
            let trigger_above = match c.owner.purpose {
                OrderPurpose::Protection => !long,
                OrderPurpose::TakeProfit => long,
                _ => return false,
            };
            if (trigger_above && c.trigger_price.value() <= market.reference_price.value())
                || (!trigger_above && c.trigger_price.value() >= market.reference_price.value())
                || !market
                    .metadata
                    .price
                    .accepts(c.trigger_price.value())
                    .unwrap_or(false)
                || market
                    .maximum_price
                    .is_some_and(|max| c.trigger_price > max)
            {
                return false;
            }
            (c.quantity, market.reference_price.value(), false)
        }
        _ => return false,
    };
    if !market.metadata.quantity.accepts(quantity).unwrap_or(false)
        || market.maximum_quantity.is_some_and(|max| quantity > max)
    {
        return false;
    }
    if let ExecutionCommand::PlaceLimit(c) = command {
        if !market
            .metadata
            .price
            .accepts(c.limit_price.value())
            .unwrap_or(false)
            || market.maximum_price.is_some_and(|max| c.limit_price > max)
        {
            return false;
        }
    }
    let Some(notional) = quantity.checked_mul(price) else {
        return false;
    };
    if increasing && notional < market.metadata.instrument.minimum_notional.value {
        return false;
    }
    if !increasing {
        return true;
    }
    let Some(limits) = limits.filter(|l| l.validate()) else {
        return false;
    };
    // This is a pre-send mark valuation, not a guarantee of execution price. The adapter
    // enforces its native execution bounds; existing exposure uses this symbol's quote currency.
    if notional > limits.max_order_notional {
        return false;
    }
    let symbol = &command.mutation_owner().symbol;
    let mut total = notional;
    for position in snapshot.positions().iter().filter(|p| &p.symbol == symbol) {
        let Some(value) = position
            .quantity
            .abs()
            .checked_mul(market.reference_price.value())
        else {
            return false;
        };
        let Some(sum) = total.checked_add(value) else {
            return false;
        };
        total = sum;
    }
    for order in snapshot
        .open_orders()
        .iter()
        .filter(|o| &o.symbol == symbol && !o.reduce_only)
    {
        let Some(filled) = order.filled_quantity else {
            return false;
        };
        let Some(remaining) = order
            .quantity
            .checked_sub(filled)
            .filter(|v| *v >= Decimal::ZERO)
        else {
            return false;
        };
        let reference = order
            .limit_price
            .unwrap_or(market.reference_price.value())
            .max(market.reference_price.value());
        let Some(value) = remaining.checked_mul(reference) else {
            return false;
        };
        let Some(sum) = total.checked_add(value) else {
            return false;
        };
        total = sum;
    }
    total <= limits.max_symbol_notional
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::{
        Amount, Asset, CommandId, Instrument, InstrumentMetadata, MarketKind, MarketOrderCommand,
        OrderOwner, Precision, Price, StopMarketFullPositionCommand, Symbol,
    };
    use venue_execution::{SignedAccountPositionFact, SignedAccountPositionMode};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    fn fixture() -> Result<
        (
            ExecutionCommand,
            SignedAccountSnapshot,
            DurableMarketFacts,
            StrategyRiskLimits,
        ),
        Box<dyn std::error::Error>,
    > {
        let symbol = Symbol::new("BTC", "USDT")?;
        let binding = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            symbol.clone(),
        )?;
        let owner = OrderOwner {
            strategy_instance_id: "grid".into(),
            run_id: "run".into(),
            exchange: "bybit".into(),
            account: binding.trading_account_id.clone(),
            symbol: symbol.clone(),
            purpose: OrderPurpose::Entry,
        };
        let command = ExecutionCommand::PlaceMarket(MarketOrderCommand {
            command_id: CommandId::new("cmd")?,
            client_order_id: CommandId::new("client")?,
            owner,
            position_side: PositionSide::Long,
            side: OrderSide::Buy,
            quantity: Decimal::ONE,
            reduce_only: false,
        });
        let snapshot = SignedAccountSnapshot::complete(
            binding.clone(),
            1000,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            vec![],
            vec![SignedAccountPositionFact {
                symbol: symbol.clone(),
                position_side: PositionSide::Long,
                quantity: Decimal::ONE,
                entry_price: Some(Decimal::from(100)),
                mark_price: Some(Decimal::from(100)),
            }],
            "cursor".into(),
            vec![],
        )?;
        let quote = Asset::new("USDT")?;
        let metadata = InstrumentMetadata::new(
            Instrument {
                symbol,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(quote.clone()),
                generation: 1,
                price_tick: Price::new(Decimal::ONE)?,
                quantity_step: Decimal::ONE,
                minimum_notional: Amount::new(quote, Decimal::from(5)),
            },
            Precision::new(Decimal::ONE, Decimal::ONE)?,
            Precision::new(Decimal::ONE, Decimal::ONE)?,
            None,
            true,
        )?;
        Ok((
            command,
            snapshot,
            DurableMarketFacts {
                binding,
                metadata,
                reference_price: Price::new(Decimal::from(100))?,
                observed_at_ms: 1000,
                maximum_quantity: Some(Decimal::from(10)),
                maximum_price: None,
            },
            StrategyRiskLimits {
                max_order_notional: Decimal::from(100),
                max_symbol_notional: Decimal::from(200),
            },
        ))
    }
    #[test]
    fn market_entry_requires_explicit_limits_and_counts_existing_inventory()
    -> Result<(), Box<dyn std::error::Error>> {
        let (command, snapshot, market, mut limits) = fixture()?;
        assert!(!market_guard(&command, &snapshot, &market, None, 1001));
        assert!(market_guard(
            &command,
            &snapshot,
            &market,
            Some(&limits),
            1001
        ));
        limits.max_symbol_notional = Decimal::from(199);
        assert!(!market_guard(
            &command,
            &snapshot,
            &market,
            Some(&limits),
            1001
        ));
        assert!(!market_guard(
            &command,
            &snapshot,
            &market,
            Some(&limits),
            7000
        ));
        Ok(())
    }
    #[test]
    fn protection_and_take_profit_use_opposite_trigger_directions()
    -> Result<(), Box<dyn std::error::Error>> {
        let (command, snapshot, market, _) = fixture()?;
        for (purpose, price) in [
            (OrderPurpose::Protection, 90),
            (OrderPurpose::TakeProfit, 110),
        ] {
            let mut owner = command.mutation_owner().clone();
            owner.purpose = purpose;
            let mut stop = StopMarketFullPositionCommand {
                command_id: CommandId::new("stop")?,
                client_algo_id: CommandId::new("algo")?,
                owner,
                side: OrderSide::Sell,
                position_side: PositionSide::Long,
                quantity: Decimal::ONE,
                trigger_price: Price::new(Decimal::from(price))?,
                position_generation: 1,
            };
            assert!(market_guard(
                &ExecutionCommand::StopMarketFullPosition(stop.clone()),
                &snapshot,
                &market,
                None,
                1001
            ));
            stop.trigger_price = Price::new(Decimal::from(200 - price))?;
            assert!(!market_guard(
                &ExecutionCommand::StopMarketFullPosition(stop),
                &snapshot,
                &market,
                None,
                1001
            ));
        }
        Ok(())
    }
}
