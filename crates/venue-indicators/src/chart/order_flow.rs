use rust_decimal::Decimal;
use venue_domain::{FieldState, PublicBar};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CvdResetMode {
    #[default]
    LoadedContinuous,
    UtcDaily,
}

/// Venue taker aggregates classify volume; candle direction does not classify trade side.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OrderFlowValue {
    pub buy: Option<Decimal>,
    pub sell: Option<Decimal>,
    pub delta: Option<Decimal>,
    pub cumulative: Option<Decimal>,
    pub cumulative_start_ms: Option<u64>,
    pub complete_day: bool,
}

/// Aggregates one shared minute source into display buckets. Missing minutes or aggressor
/// volume leave a confirmed bucket empty; forming buckets may show a replaceable partial value.
pub fn aggregate_minute_flow(
    minutes: &[PublicBar],
    buckets: &[(u64, bool)],
    bucket_interval_ms: u64,
    mode: CvdResetMode,
) -> Vec<OrderFlowValue> {
    if bucket_interval_ms == 0 {
        return vec![OrderFlowValue::default(); buckets.len()];
    }
    let mut flow = OrderFlow::new(mode);
    let mut values = Vec::with_capacity(minutes.len());
    let mut previous = None;
    let mut scope = None;
    for bar in minutes {
        if !bar.is_valid()
            || bar.interval_ms != 60_000
            || previous.is_some_and(|time| bar.open_time_ms <= time)
            || scope.is_some_and(|(symbol, generation)| {
                symbol != &bar.symbol || generation != bar.generation
            })
        {
            return vec![OrderFlowValue::default(); buckets.len()];
        }
        scope = Some((&bar.symbol, bar.generation));
        values.push(flow.update(bar));
        previous = Some(bar.open_time_ms);
    }
    buckets
        .iter()
        .map(|&(start, confirmed)| {
            let Some(end) = start.checked_add(bucket_interval_ms) else {
                return OrderFlowValue::default();
            };
            let first = minutes.partition_point(|bar| bar.open_time_ms < start);
            let last = minutes.partition_point(|bar| bar.open_time_ms < end);
            let selected = &minutes[first..last];
            if selected.is_empty()
                || selected[0].open_time_ms != start
                || selected
                    .windows(2)
                    .any(|pair| pair[0].open_time_ms.saturating_add(60_000) != pair[1].open_time_ms)
                || (confirmed
                    && selected
                        .last()
                        .is_none_or(|bar| bar.open_time_ms.saturating_add(60_000) != end))
            {
                return OrderFlowValue::default();
            }
            let mut buy = Decimal::ZERO;
            let mut sell = Decimal::ZERO;
            let mut delta = Decimal::ZERO;
            for item in &values[first..last] {
                let Some((b, s, d)) = item
                    .buy
                    .zip(item.sell)
                    .zip(item.delta)
                    .map(|((b, s), d)| (b, s, d))
                else {
                    return OrderFlowValue::default();
                };
                let Some(sum) = buy
                    .checked_add(b)
                    .zip(sell.checked_add(s))
                    .zip(delta.checked_add(d))
                    .map(|((b, s), d)| (b, s, d))
                else {
                    return OrderFlowValue::default();
                };
                (buy, sell, delta) = sum;
            }
            let final_value = values[last - 1];
            OrderFlowValue {
                buy: Some(buy),
                sell: Some(sell),
                delta: Some(delta),
                cumulative: final_value.cumulative,
                cumulative_start_ms: final_value.cumulative_start_ms,
                complete_day: final_value.complete_day,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::ChartStudyEngine;
    use venue_domain::{Price, UnknownReason};

    fn bar(index: u64, buy: Option<i64>) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let price = Price::new(Decimal::from(100))?;
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
            base_volume: FieldState::Known(Decimal::from(10)),
            quote_volume: FieldState::Known(Decimal::from(1_000)),
            trade_count: FieldState::Known(10),
            taker_buy_base_volume: buy.map(|v| FieldState::Known(Decimal::from(v))).unwrap_or(
                FieldState::Unavailable {
                    reason: UnknownReason::SourceOmitted,
                },
            ),
            taker_buy_quote_volume: FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            },
        })
    }

    #[test]
    fn missing_side_is_a_gap_and_resets_cumulative() -> Result<(), Box<dyn std::error::Error>> {
        let mut flow = OrderFlow::default();
        let first = flow.update(&bar(0, Some(7))?);
        assert_eq!(first.sell, Some(3.into()));
        assert_eq!(first.delta, Some(4.into()));
        assert_eq!(flow.update(&bar(1, None)?), OrderFlowValue::default());
        assert_eq!(flow.update(&bar(2, Some(2))?).cumulative, Some((-6).into()));
        assert_eq!(flow.update(&bar(3, Some(11))?), OrderFlowValue::default());
        Ok(())
    }

    #[test]
    fn previews_do_not_double_count_volume_and_reset_clears_anchor()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut engine = ChartStudyEngine::standard()?;
        engine.ingest_closed(&bar(0, Some(7))?)?;
        let next = bar(1, Some(6))?;
        let preview = engine.preview(&next)?.order_flow;
        assert_eq!(preview.cumulative, Some(6.into()));
        assert_eq!(engine.preview(&next)?.order_flow, preview);
        assert_eq!(engine.ingest_closed(&next)?.order_flow, preview);
        engine.reset();
        assert_eq!(
            engine
                .ingest_closed(&bar(5, Some(6))?)?
                .order_flow
                .cumulative,
            Some(2.into())
        );
        Ok(())
    }

    #[test]
    fn utc_daily_resets_at_midnight_and_reports_partial_coverage()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut flow = OrderFlow::new(CvdResetMode::UtcDaily);
        let mut first = bar(1_439, Some(7))?;
        assert_eq!(flow.update(&first).cumulative, Some(Decimal::from(4)));
        first.open_time_ms = 86_400_000;
        first.close_time_ms = 86_459_999;
        let daily = flow.update(&first);
        assert_eq!(daily.cumulative, Some(Decimal::from(4)));
        assert_eq!(daily.cumulative_start_ms, Some(86_400_000));
        assert!(daily.complete_day);
        first.open_time_ms = 86_520_000;
        first.close_time_ms = 86_579_999;
        let gap = flow.update(&first);
        assert_eq!(gap.cumulative_start_ms, Some(86_520_000));
        assert!(!gap.complete_day);
        Ok(())
    }

    #[test]
    fn five_minute_delta_uses_five_complete_minute_facts() -> Result<(), Box<dyn std::error::Error>>
    {
        let minutes = (0..5)
            .map(|minute| bar(minute, Some(7)))
            .collect::<Result<Vec<_>, _>>()?;
        let result = aggregate_minute_flow(&minutes, &[(0, true)], 300_000, CvdResetMode::UtcDaily);
        assert_eq!(result[0].delta, Some(Decimal::from(20)));
        assert_eq!(result[0].cumulative, Some(Decimal::from(20)));
        assert!(result[0].complete_day);
        let missing = [
            minutes[0].clone(),
            minutes[2].clone(),
            minutes[3].clone(),
            minutes[4].clone(),
        ];
        assert_eq!(
            aggregate_minute_flow(&missing, &[(0, true)], 300_000, CvdResetMode::UtcDaily)[0],
            OrderFlowValue::default()
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct OrderFlow {
    cumulative: Decimal,
    mode: CvdResetMode,
    start_ms: Option<u64>,
    previous_open_ms: Option<u64>,
    previous_interval_ms: Option<u64>,
}

impl OrderFlow {
    pub fn mode(&self) -> CvdResetMode {
        self.mode
    }
    pub fn new(mode: CvdResetMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    pub fn update(&mut self, bar: &PublicBar) -> OrderFlowValue {
        let day_ms = 86_400_000;
        if self.mode == CvdResetMode::UtcDaily
            && self
                .start_ms
                .is_some_and(|start| start / day_ms != bar.open_time_ms / day_ms)
        {
            self.cumulative = Decimal::ZERO;
            self.start_ms = None;
        }
        if self
            .previous_open_ms
            .zip(self.previous_interval_ms)
            .is_some_and(|(open, interval)| open.checked_add(interval) != Some(bar.open_time_ms))
        {
            self.cumulative = Decimal::ZERO;
            self.start_ms = None;
        }
        self.previous_open_ms = Some(bar.open_time_ms);
        self.previous_interval_ms = Some(bar.interval_ms);
        let (FieldState::Known(total), FieldState::Known(buy)) =
            (&bar.base_volume, &bar.taker_buy_base_volume)
        else {
            self.cumulative = Decimal::ZERO;
            self.start_ms = None;
            return OrderFlowValue::default();
        };
        let values = (|| {
            if *total < Decimal::ZERO || *buy < Decimal::ZERO || buy > total {
                return None;
            }
            let sell = total.checked_sub(*buy)?;
            let delta = buy.checked_sub(sell)?;
            let cumulative = self.cumulative.checked_add(delta)?;
            Some((sell, delta, cumulative))
        })();
        let Some((sell, delta, cumulative)) = values else {
            self.cumulative = Decimal::ZERO;
            self.start_ms = None;
            return OrderFlowValue::default();
        };
        self.cumulative = cumulative;
        let start_ms = *self.start_ms.get_or_insert(bar.open_time_ms);
        OrderFlowValue {
            buy: Some(*buy),
            sell: Some(sell),
            delta: Some(delta),
            cumulative: Some(cumulative),
            cumulative_start_ms: Some(start_ms),
            complete_day: self.mode == CvdResetMode::UtcDaily
                && start_ms == bar.open_time_ms / day_ms * day_ms,
        }
    }
}
