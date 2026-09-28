use std::collections::BTreeMap;
use venue_gateway_api::PublicMarketBinding;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SharedSourceDemand {
    pub minute: bool,
    pub day: bool,
}

impl SharedSourceDemand {
    pub(crate) fn for_chart(settings: &crate::chart_settings::ChartDisplaySettings,
        has_anchor: bool) -> Self {
        Self {
            minute: settings.needs_minute_source(has_anchor),
            day: settings.needs_day_source(),
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.minute |= other.minute;
        self.day |= other.day;
    }

    pub(crate) fn allows(self, interval: crate::chart::ChartInterval) -> bool {
        match interval {
            crate::chart::ChartInterval::OneMinute => self.minute,
            crate::chart::ChartInterval::OneDay => self.day,
            _ => false,
        }
    }
}

pub(crate) type SourceDemands = BTreeMap<PublicMarketBinding, SharedSourceDemand>;

pub(crate) fn source_is_requested(demands: &SourceDemands,
    binding: &PublicMarketBinding, interval: crate::chart::ChartInterval) -> bool {
    demands.get(binding).is_some_and(|demand| demand.allows(interval))
}

pub(crate) async fn wait_until_source_disabled(
    mut changes: tokio::sync::watch::Receiver<SourceDemands>,
    binding: PublicMarketBinding,
    interval: crate::chart::ChartInterval,
) {
    loop {
        let requested = source_is_requested(&changes.borrow(), &binding, interval);
        if !requested || changes.changed().await.is_err() { return; }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{chart::ChartInterval, market::MarketSelection};
    use std::time::Duration;

    #[tokio::test]
    async fn cancelling_one_binding_does_not_stop_another_shared_page()
    -> Result<(), Box<dyn std::error::Error>> {
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::FiveMinutes)?.binding;
        let btc = MarketSelection::binance_usd_m("BTC/USDC", ChartInterval::FiveMinutes)?.binding;
        let mut demands = SourceDemands::new();
        demands.insert(doge.clone(), SharedSourceDemand { minute: true, day: false });
        demands.insert(btc.clone(), SharedSourceDemand { minute: true, day: false });
        let (sender, receiver) = tokio::sync::watch::channel(demands.clone());
        let waiting = wait_until_source_disabled(receiver, doge.clone(), ChartInterval::OneMinute);
        tokio::pin!(waiting);
        demands.get_mut(&btc).ok_or("missing BTC demand")?.minute = false;
        sender.send_replace(demands.clone());
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiting).await.is_err());
        demands.get_mut(&doge).ok_or("missing DOGE demand")?.minute = false;
        sender.send_replace(demands);
        tokio::time::timeout(Duration::from_secs(1), &mut waiting).await?;
        Ok(())
    }
}
