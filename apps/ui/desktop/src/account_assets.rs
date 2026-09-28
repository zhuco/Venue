use crate::model::AppModel;
use rust_decimal::Decimal;
use venue_control_protocol::kol::TerminalAsset;

#[derive(Clone, Copy)]
pub(crate) enum AssetPurpose<'a> {
    PortfolioUsd,
    QuoteOrPortfolio(&'a str),
}

#[derive(Clone, Copy)]
pub(crate) struct AccountAsset<'a> {
    pub asset: &'a str,
    pub equity: Decimal,
    pub available_margin: Option<Decimal>,
    /// Observation time of the signed projection; not the public price clock.
    pub observed_ms: u64,
}

pub(crate) fn select<'a>(
    assets: &'a [TerminalAsset],
    purpose: AssetPurpose<'_>,
) -> Option<&'a TerminalAsset> {
    let requested = match purpose {
        AssetPurpose::PortfolioUsd => "USD",
        AssetPurpose::QuoteOrPortfolio(symbol) => symbol.split_once('/')?.1,
    };
    assets
        .iter()
        .find(|asset| asset.asset == requested)
        .or_else(|| assets.iter().find(|asset| asset.asset == "USD"))
}

pub(crate) fn for_model<'a>(
    model: &'a AppModel,
    purpose: AssetPurpose<'_>,
) -> Option<AccountAsset<'a>> {
    let scope = model.confirmed_account_scope()?;
    let projection = model
        .execution
        .private_projection_for(Some(&scope.trading_account_id))?;
    if projection.credential_id != scope.credential_id {
        return None;
    }
    let asset = select(&projection.assets, purpose)?;
    Some(AccountAsset {
        asset: &asset.asset,
        equity: asset.equity,
        available_margin: asset.available_margin,
        observed_ms: projection.balance_observed_ms.unwrap_or(0),
    })
}

#[derive(Debug, Default)]
pub(crate) struct EquityEstimate {
    basis: Option<(
        String,
        String,
        Option<u64>,
        Decimal,
        Vec<venue_control_protocol::kol::TerminalPosition>,
        Vec<Decimal>,
    )>,
}

impl EquityEstimate {
    fn calculate(
        &mut self,
        projection: &venue_control_protocol::kol::TerminalAccountProjection,
        asset: &TerminalAsset,
        prices: Vec<Decimal>,
    ) -> Option<Decimal> {
        let mut inventory = projection.positions.clone();
        // Mark changes do not reset a balance baseline.
        for position in &mut inventory {
            position.mark_price = None;
        }
        let reset =
            self.basis
                .as_ref()
                .is_none_or(|(account, currency, time, equity, positions, _)| {
                    account != &projection.credential_id
                        || currency != &asset.asset
                        || *time != projection.balance_observed_ms
                        || *equity != asset.equity
                        || positions != &inventory
                });
        if reset {
            self.basis = Some((
                projection.credential_id.clone(),
                asset.asset.clone(),
                projection.balance_observed_ms,
                asset.equity,
                inventory,
                prices.clone(),
            ));
        }
        let (_, _, _, equity, positions, base_prices) = self.basis.as_ref()?;
        positions.iter().zip(base_prices).zip(prices).try_fold(
            *equity,
            |total, ((position, base), price)| {
                let delta = if position.position_side == venue_domain::PositionSide::Short {
                    base.checked_sub(price)?
                } else {
                    price.checked_sub(*base)?
                };
                total.checked_add(delta.checked_mul(position.quantity)?)
            },
        )
    }
}

/// Display-only movement since receiving the signed equity; never used for sizing or margin.
pub(crate) fn estimated_equity(model: &AppModel) -> Option<(Decimal, bool)> {
    let scope = model.confirmed_account_scope()?;
    let projection = model
        .execution
        .private_projection_for(Some(&scope.trading_account_id))?;
    if projection.credential_id != scope.credential_id
        || !model.execution.private_ready(
            Some(&scope.trading_account_id),
            crate::account_center::now_ms(),
        )
    {
        return None;
    }
    if model.selected_execution_credential()?.venue != model.preferences.market_server.venue() {
        return None;
    }
    let asset = select(&projection.assets, AssetPurpose::PortfolioUsd)?;
    let now = crate::market_prices::now_ms();
    let mut proxy = false;
    let prices = projection
        .positions
        .iter()
        .map(|position| {
            let quote = position.symbol.quote().to_string();
            if quote != asset.asset {
                if asset.asset == "USD" && matches!(quote.as_str(), "USDT" | "USDC") {
                    proxy = true;
                } else {
                    return None;
                }
            }
            model
                .market_prices(&position.symbol.to_string(), now)
                .position_price(position.position_side, position.quantity)
        })
        .collect::<Option<Vec<_>>>()?;
    model
        .execution
        .equity_estimate
        .borrow_mut()
        .calculate(projection, asset, prices)
        .map(|value| (value, proxy))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn equity_movement_is_not_double_counted_and_new_asset_clock_reanchors()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = crate::account_scope::tests::projection(1);
        projection.balance_observed_ms = Some(1000);
        projection
            .positions
            .push(venue_control_protocol::kol::TerminalPosition {
                symbol: "BTC/USDT".parse()?,
                position_side: venue_domain::PositionSide::Long,
                quantity: 2.into(),
                entry_price: Some(90.into()),
                mark_price: Some(100.into()),
            });
        let mut asset = TerminalAsset {
            asset: "USD".into(),
            equity: 1020.into(),
            available_margin: Some(900.into()),
        };
        let mut estimator = EquityEstimate::default();
        assert_eq!(
            estimator.calculate(&projection, &asset, vec![100.into()]),
            Some(1020.into())
        );
        assert_eq!(
            estimator.calculate(&projection, &asset, vec![110.into()]),
            Some(1040.into())
        );
        assert_eq!(
            estimator.calculate(&projection, &asset, vec![110.into()]),
            Some(1040.into())
        );
        asset.equity = 1040.into();
        projection.balance_observed_ms = Some(2000);
        assert_eq!(
            estimator.calculate(&projection, &asset, vec![110.into()]),
            Some(1040.into())
        );
        projection.positions[0].quantity = 3.into();
        assert_eq!(
            estimator.calculate(&projection, &asset, vec![110.into()]),
            Some(1040.into())
        );
        assert_eq!(asset.available_margin, Some(900.into()));
        Ok(())
    }
    #[test]
    fn currency_purpose_and_account_switch_never_relabel_funds() {
        let mut model = crate::account_scope::tests::model();
        let mut projection = crate::account_scope::tests::projection(1);
        projection.assets = vec![TerminalAsset {
            asset: "USDC".into(),
            equity: Decimal::from(9),
            available_margin: None,
        }];
        model
            .execution
            .apply_private(Some(projection), &mut model.trade_dock);
        assert!(for_model(&model, AssetPurpose::PortfolioUsd).is_none());
        assert_eq!(
            for_model(&model, AssetPurpose::QuoteOrPortfolio("BTC/USDC")).map(|a| a.asset),
            Some("USDC")
        );
        model.begin_account_selection(crate::account_scope::tests::id(2));
        assert!(for_model(&model, AssetPurpose::QuoteOrPortfolio("BTC/USDC")).is_none());
    }
}
