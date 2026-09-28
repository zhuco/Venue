use rust_decimal::Decimal;
use std::collections::BTreeMap;

/// These bands are volume-weighted leverage scenarios, never open-interest or liquidation facts.
#[derive(Clone, Debug, PartialEq)]
pub struct LiquidationBand {
    pub seed_open_time_ms: u64,
    pub valid_from_ms: u64,
    pub retired_at_ms: Option<u64>,
    pub price: Decimal,
    pub weight: Decimal,
    pub long: bool,
}

#[derive(Clone, Debug, Default)]
pub struct LiquidationScenario {
    bands: Vec<LiquidationBand>,
    active_long: BTreeMap<Decimal, Vec<usize>>,
    active_short: BTreeMap<Decimal, Vec<usize>>,
    last_observed_end_ms: Option<u64>,
}

impl LiquidationScenario {
    pub fn bands(&self) -> &[LiquidationBand] {
        &self.bands
    }

    pub fn estimated_retained_bytes(&self) -> usize {
        let active = [&self.active_long, &self.active_short].into_iter().fold(0_usize,
            |total, prices| total.saturating_add(prices.len().saturating_mul(96))
                .saturating_add(prices.values().fold(0_usize, |size, indices|
                    size.saturating_add(indices.capacity() * std::mem::size_of::<usize>()))));
        self.bands.capacity().saturating_mul(std::mem::size_of::<LiquidationBand>())
            .saturating_add(active)
    }

    /// End only existing bands. Intrabar high/low retain touches when price rebounds.
    pub fn retire(&mut self, observed_at_ms: u64, high: Decimal, low: Decimal) {
        if high < low || low <= Decimal::ZERO {
            return;
        }
        let long_prices = self.active_long.range(low..).map(|(price, _)| *price).collect::<Vec<_>>();
        let short_prices = self.active_short.range(..=high).map(|(price, _)| *price).collect::<Vec<_>>();
        for price in long_prices {
            if let Some(indices) = self.active_long.remove(&price) {
                let mut future = Vec::new();
                for index in indices {
                    if let Some(band) = self.bands.get_mut(index) {
                        if band.valid_from_ms <= observed_at_ms { band.retired_at_ms = Some(observed_at_ms); }
                        else { future.push(index); }
                    }
                }
                if !future.is_empty() { self.active_long.insert(price, future); }
            }
        }
        for price in short_prices {
            if let Some(indices) = self.active_short.remove(&price) {
                let mut future = Vec::new();
                for index in indices {
                    if let Some(band) = self.bands.get_mut(index) {
                        if band.valid_from_ms <= observed_at_ms { band.retired_at_ms = Some(observed_at_ms); }
                        else { future.push(index); }
                    }
                }
                if !future.is_empty() { self.active_short.insert(price, future); }
            }
        }
    }

    /// A closed bar's assumed entry seeds both sides after its close; later touches retire a band.
    /// Aggressive buys are not assumed to be opening longs. Equal tier weights are assumptions.
    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &mut self,
        bar_open_time_ms: u64,
        bar_end_ms: u64,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        leverage: &[u16],
        maintenance_bps: u16,
        margin_cost_bps: u16,
    ) {
        self.observe_with_tick(bar_open_time_ms, bar_end_ms, high, low, close, volume,
            leverage, maintenance_bps, margin_cost_bps, None);
    }

    /// Quantize scenario centers to the selected instrument's exchange tick before indexing.
    #[allow(clippy::too_many_arguments)]
    pub fn observe_with_tick(
        &mut self, bar_open_time_ms: u64, bar_end_ms: u64, high: Decimal, low: Decimal,
        close: Decimal, volume: Decimal, leverage: &[u16], maintenance_bps: u16,
        margin_cost_bps: u16, tick: Option<Decimal>,
    ) {
        if bar_end_ms <= bar_open_time_ms
            || self.last_observed_end_ms.is_some_and(|last| bar_end_ms <= last)
            || high < low
            || low <= Decimal::ZERO
            || close < low
            || close > high
            || volume < Decimal::ZERO
        {
            return;
        }
        self.retire(bar_end_ms, high, low);
        self.last_observed_end_ms = Some(bar_end_ms);
        if leverage.is_empty() || leverage.len() > 4 || volume.is_zero() {
            return;
        }
        let Some(weight) = close
            .checked_mul(volume)
            .and_then(|v| v.checked_div(Decimal::from(leverage.len() * 2)))
        else {
            return;
        };
        for &tier in leverage {
            for long in [true, false] {
                if let Some(mut price) =
                    scenario_price_after_cost(close, tier, maintenance_bps, margin_cost_bps, long)
                {
                    if let Some(tick) = tick.filter(|tick| *tick > Decimal::ZERO) {
                        let Some(quantized) = price.checked_div(tick)
                            .map(|units| units.round()).and_then(|units| units.checked_mul(tick)) else { continue; };
                        if quantized <= Decimal::ZERO { continue; }
                        price = quantized;
                    }
                    let index = self.bands.len();
                    self.bands.push(LiquidationBand {
                        seed_open_time_ms: bar_open_time_ms,
                        valid_from_ms: bar_end_ms,
                        retired_at_ms: None,
                        price,
                        weight,
                        long,
                    });
                    if long { self.active_long.entry(price).or_default().push(index); }
                    else { self.active_short.entry(price).or_default().push(index); }
                }
            }
        }
    }
}

/// Isolated linear-contract scenario with fixed maintenance margin and zero extra margin depletion.
pub fn scenario_price(
    entry: Decimal,
    leverage: u16,
    maintenance_bps: u16,
    long: bool,
) -> Option<Decimal> {
    scenario_price_after_cost(entry, leverage, maintenance_bps, 0, long)
}

/// Explicit assumed margin depletion as a fraction of entry notional, not a liquidation fee rate.
pub fn scenario_price_after_cost(
    entry: Decimal,
    leverage: u16,
    maintenance_bps: u16,
    margin_cost_bps: u16,
    long: bool,
) -> Option<Decimal> {
    if entry <= Decimal::ZERO || !(2..=100).contains(&leverage) {
        return None;
    }
    let initial = Decimal::ONE.checked_div(Decimal::from(leverage))?;
    let maintenance = Decimal::from(maintenance_bps).checked_div(Decimal::from(10_000))?;
    let cost = Decimal::from(margin_cost_bps).checked_div(Decimal::from(10_000))?;
    let available = initial.checked_sub(cost)?;
    if maintenance >= available {
        return None;
    }
    let (numerator, denominator) = if long {
        (
            Decimal::ONE.checked_sub(available)?,
            Decimal::ONE.checked_sub(maintenance)?,
        )
    } else {
        (
            Decimal::ONE.checked_add(available)?,
            Decimal::ONE.checked_add(maintenance)?,
        )
    };
    entry.checked_mul(numerator)?.checked_div(denominator)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leverage_and_margin_move_bands_toward_entry() -> Result<(), Box<dyn std::error::Error>> {
        let p = Decimal::from(100);
        let long = scenario_price(p, 10, 50, true).ok_or("long")?;
        let short = scenario_price(p, 10, 50, false).ok_or("short")?;
        assert!(long < p && short > p);
        assert!(scenario_price(p, 50, 50, true).ok_or("50x")? > long);
        assert!(scenario_price(p, 10, 60, false).ok_or("margin")? < short);
        assert!(scenario_price(p, 100, 100, true).is_none());
        assert!(scenario_price_after_cost(p, 10, 50, 10, true).ok_or("cost")? > long);
        assert!(scenario_price_after_cost(p, 10, 50, 10, false).ok_or("cost")? < short);
        assert!(scenario_price_after_cost(p, 100, 50, 50, true).is_none());
        Ok(())
    }

    #[test]
    fn no_same_bar_liquidation_and_no_future_bands_in_past() {
        let mut model = LiquidationScenario::default();
        model.observe(
            0,
            60_000,
            120.into(),
            80.into(),
            100.into(),
            10.into(),
            &[10],
            50,
            0,
        );
        assert_eq!(model.bands().len(), 2);
        assert!(
            model
                .bands()
                .iter()
                .all(|b| b.valid_from_ms == 60_000 && b.retired_at_ms.is_none())
        );
        model.observe(60_000, 120_000, 120.into(), 80.into(), 100.into(), 0.into(), &[10], 50, 0);
        assert!(model.bands().iter().all(|b| b.retired_at_ms == Some(120_000)));
        model.observe(
            120_000,
            180_000,
            120.into(),
            80.into(),
            100.into(),
            10.into(),
            &[10],
            50,
            0,
        );
        assert_eq!(model.bands().len(), 4);
    }

    #[test]
    fn scenario_centers_use_exchange_tick_before_touch_indexing() {
        let mut model = LiquidationScenario::default();
        let tick = Decimal::new(5, 2);
        model.observe_with_tick(0, 60_000, 100.into(), 100.into(), 100.into(),
            10.into(), &[10], 50, 0, Some(tick));
        assert_eq!(model.bands().len(), 2);
        assert!(model.bands().iter().all(|band| band.price % tick == Decimal::ZERO));
    }
}
