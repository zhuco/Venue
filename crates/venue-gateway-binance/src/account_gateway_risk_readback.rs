use super::*;
use crate::{BinancePrivateSurface, BinanceRawPrivatePage};

/// Account-wide notionals from the same signed pages used for a selected-symbol execution.
/// Keeping the native-page projection in the adapter prevents other-symbol exposure being lost.
pub struct BinanceExecutionRiskInput {
    binding: GatewayBinding,
    observed_at_ms: u64,
    private_generation: u64,
    positions: Vec<AccountRiskAmount>,
    orders: Vec<AccountRiskAmount>,
    quote_assets: BTreeSet<Asset>,
}

impl BinanceExecutionRiskInput {
    #[must_use]
    pub fn quote_assets(&self) -> &BTreeSet<Asset> {
        &self.quote_assets
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    pub fn complete(
        self,
        indices: &BTreeMap<Asset, crate::portfolio::UsdConversionEvidence>,
        now_ms: u64,
    ) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        let rates = quote_to_usdt_rates(&self.quote_assets, indices, self.private_generation)?;
        AccountRiskEvidence::complete_with_usdt_valuation(
            self.binding,
            now_ms,
            self.private_generation,
            self.positions,
            self.orders,
            rates,
        )?
        .with_earliest_observation(self.observed_at_ms)
    }
}

pub fn prepare_execution_risk_readback(
    catalogue: &str,
    scope: &BinancePrivateReadScope,
    pages: Vec<BinanceRawPrivatePage>,
) -> Result<(BinanceExecutionRiskInput, Vec<BinanceRawPrivatePage>), AccountHostValidationError> {
    let invalid = || AccountHostValidationError::RiskEvidence;
    let mut wide = BTreeMap::new();
    for page in &pages {
        page.validate().map_err(|_| invalid())?;
        if &page.scope != scope {
            return Err(invalid());
        }
        if matches!(
            page.surface,
            BinancePrivateSurface::Positions
                | BinancePrivateSurface::RegularOrders
                | BinancePrivateSurface::AlgoOrders
        ) {
            let expected = match page.surface {
                BinancePrivateSurface::Positions => build_account_wide_positions_request(scope),
                BinancePrivateSurface::RegularOrders => {
                    build_account_wide_regular_orders_request(scope)
                }
                _ => build_account_wide_algo_orders_request(scope),
            }
            .map_err(|_| invalid())?;
            if page.page_index != 1
                || page.request_parameters() != expected.parameters()
                || wide.insert(page.surface, page).is_some()
            {
                return Err(invalid());
            }
        }
    }
    let payload = |surface| {
        wide.get(&surface)
            .map(|page| page.payload.as_ref())
            .ok_or_else(invalid)
    };
    let positions = account_position_notionals(
        catalogue,
        payload(BinancePrivateSurface::Positions)?,
        scope.private_generation(),
    )?;
    let orders = account_entry_order_notionals(
        catalogue,
        payload(BinancePrivateSurface::RegularOrders)?,
        payload(BinancePrivateSurface::AlgoOrders)?,
        scope.private_generation(),
    )?;
    let mut quote_assets = positions
        .iter()
        .chain(orders.iter())
        .map(|amount| amount.asset.clone())
        .collect::<BTreeSet<_>>();
    quote_assets.insert(Asset::new(scope.binding().symbol.quote()).map_err(|_| invalid())?);
    if quote_assets.len() > MAX_ACCOUNT_RISK_QUOTE_ASSETS {
        return Err(invalid());
    }
    let input = BinanceExecutionRiskInput {
        binding: scope.binding().clone(),
        observed_at_ms: scope.requested_at_ms(),
        private_generation: scope.private_generation(),
        positions,
        orders,
        quote_assets,
    };
    let native = crate::native_symbol(&scope.binding().symbol);
    let mut selected = Vec::with_capacity(pages.len());
    for mut page in pages {
        if matches!(
            page.surface,
            BinancePrivateSurface::Positions
                | BinancePrivateSurface::RegularOrders
                | BinancePrivateSurface::AlgoOrders
        ) {
            let rows = json_rows(&page.payload)?;
            let mut matching = Vec::new();
            for row in rows {
                if text_field(&row, "symbol")? == native {
                    matching.push(row);
                }
            }
            page.payload = serde_json::to_vec(&matching).map_err(|_| invalid())?.into();
        }
        selected.push(page);
    }
    Ok((input, selected))
}
