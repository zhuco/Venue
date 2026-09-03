use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use venue_domain::domain::{Asset, Symbol};
use venue_gateway_api::GatewayBinding;

use crate::{
    BinanceAccountBinding, BinanceAccountGatewayError, BinanceConfig,
    BinanceGridBootstrapMarketFacts, BinanceHttpTransport, BinanceInstrumentRules,
    BinanceTransportLimits, parse_instrument_rules,
};

const RULE_REFRESH_INTERVAL_MS: u64 = 60_000;

/// Credential-free, bounded Binance market reader used by the singleton Grid coordinator.
/// Rule generations are stable content fingerprints: a changed exchange rule set changes the
/// generation even across process restarts, so an old anchor cannot inherit different rules.
pub struct BinanceGridMarketReader {
    binding: GatewayBinding,
    transport: BinanceHttpTransport,
    rules: Option<BinanceInstrumentRules>,
    last_rules_check_ms: u64,
}

impl BinanceGridMarketReader {
    pub fn new(
        binding: GatewayBinding,
        limits: BinanceTransportLimits,
    ) -> Result<Self, BinanceAccountGatewayError> {
        let config = BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)
            .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let transport = BinanceHttpTransport::new(config, 1, 1, limits)?;
        Ok(Self {
            binding,
            transport,
            rules: None,
            last_rules_check_ms: 0,
        })
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.binding.symbol
    }

    /// Reads a fresh BBO on every call and revalidates the complete instrument rule row at a
    /// bounded cadence. No API credential, private generation, or mutation path is involved.
    pub async fn refresh(
        &mut self,
        now_ms: u64,
    ) -> Result<BinanceGridBootstrapMarketFacts, BinanceAccountGatewayError> {
        if now_ms == 0 {
            return Err(BinanceAccountGatewayError::Clock);
        }
        if self.rules.is_none()
            || now_ms.saturating_sub(self.last_rules_check_ms) >= RULE_REFRESH_INTERVAL_MS
        {
            self.refresh_rules(now_ms).await?;
        }
        let rules = self
            .rules
            .as_ref()
            .ok_or(BinanceAccountGatewayError::Instrument)?;
        let response = self
            .transport
            .fetch_usd_m_book_ticker(&rules.native_symbol)
            .await?;
        super::account_gateway::parse_grid_bootstrap_bbo(
            &response.payload,
            &self.binding,
            rules,
            now_ms,
        )
    }

    /// Reads Binance's explicit quote-to-USD index and binds it to the caller's current private
    /// generation. Portfolio Margin account equity is denominated in USD, so Grid exposure and
    /// profit checks must not assume that USDT or USDC is exactly one USD.
    pub async fn quote_usd_evidence(
        &self,
        private_generation: u64,
        max_age_ms: u64,
    ) -> Result<crate::portfolio::UsdConversionEvidence, BinanceAccountGatewayError> {
        if private_generation == 0 || max_age_ms == 0 {
            return Err(BinanceAccountGatewayError::Readback);
        }
        let quote = Asset::new(self.binding.symbol.quote())
            .map_err(|_| BinanceAccountGatewayError::Instrument)?;
        let response = self.transport.fetch_usd_m_asset_index(&quote).await?;
        let payload = std::str::from_utf8(&response.payload)
            .map_err(|_| BinanceAccountGatewayError::Readback)?;
        let observed_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BinanceAccountGatewayError::Clock)?
            .as_millis()
            .try_into()
            .map_err(|_| BinanceAccountGatewayError::Clock)?;
        crate::portfolio::parse_usd_conversion_evidence(
            payload,
            quote,
            private_generation,
            observed_at_ms,
            max_age_ms,
        )
        .map_err(|_| BinanceAccountGatewayError::Readback)
    }

    async fn refresh_rules(&mut self, now_ms: u64) -> Result<(), BinanceAccountGatewayError> {
        let response = self.transport.fetch_usd_m_exchange_info().await?;
        let payload = std::str::from_utf8(&response.payload)
            .map_err(|_| BinanceAccountGatewayError::Instrument)?;
        let candidate = parse_instrument_rules(payload, self.binding.symbol.clone(), 1)
            .map_err(|_| BinanceAccountGatewayError::Instrument)?;
        let generation = stable_rules_generation(&candidate);
        self.rules = Some(
            parse_instrument_rules(payload, self.binding.symbol.clone(), generation)
                .map_err(|_| BinanceAccountGatewayError::Instrument)?,
        );
        self.last_rules_check_ms = now_ms;
        Ok(())
    }
}

/// A content-derived generation survives an Executor restart. A process-local counter would miss
/// an exchange rule change made while the process was offline and could wrongly reuse an anchor.
fn stable_rules_generation(rules: &BinanceInstrumentRules) -> u64 {
    let mut digest = Sha256::new();
    for value in [
        rules.native_symbol.as_str().to_owned(),
        rules.instrument.symbol.to_string(),
        match rules.instrument.market {
            venue_domain::domain::MarketKind::Spot => "spot".to_owned(),
            venue_domain::domain::MarketKind::LinearPerpetual => "linear_perpetual".to_owned(),
        },
        rules
            .instrument
            .settlement_asset
            .as_ref()
            .map_or_else(String::new, ToString::to_string),
        rules.instrument.price_tick.value().normalize().to_string(),
        rules.instrument.quantity_step.normalize().to_string(),
        rules.instrument.minimum_notional.asset.to_string(),
        rules
            .instrument
            .minimum_notional
            .value
            .normalize()
            .to_string(),
        rules.minimum_quantity.normalize().to_string(),
        rules.maximum_quantity.normalize().to_string(),
        rules.minimum_price.normalize().to_string(),
        rules.maximum_price.normalize().to_string(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    let bytes = digest.finalize();
    let mut generation = [0_u8; 8];
    generation.copy_from_slice(&bytes[..8]);
    (u64::from_be_bytes(generation) & 0x7fff_ffff_ffff_ffff).max(1)
}

#[cfg(test)]
fn same_rules_ignoring_generation(
    left: &BinanceInstrumentRules,
    right: &BinanceInstrumentRules,
) -> bool {
    left.native_symbol == right.native_symbol
        && left.instrument.symbol == right.instrument.symbol
        && left.instrument.market == right.instrument.market
        && left.instrument.settlement_asset == right.instrument.settlement_asset
        && left.instrument.price_tick == right.instrument.price_tick
        && left.instrument.quantity_step == right.instrument.quantity_step
        && left.instrument.minimum_notional == right.instrument.minimum_notional
        && left.minimum_quantity == right.minimum_quantity
        && left.maximum_quantity == right.maximum_quantity
        && left.minimum_price == right.minimum_price
        && left.maximum_price == right.maximum_price
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_comparison_ignores_only_generation() -> Result<(), Box<dyn std::error::Error>> {
        let payload = include_str!("../tests/fixtures/exchange_info_btcusdt.json");
        let first = parse_instrument_rules(payload, "BTC/USDT".parse()?, 1)?;
        let second = parse_instrument_rules(payload, "BTC/USDT".parse()?, 99)?;
        assert!(same_rules_ignoring_generation(&first, &second));
        let mut changed = second;
        changed.maximum_quantity += changed.instrument.quantity_step;
        assert!(!same_rules_ignoring_generation(&first, &changed));
        Ok(())
    }

    #[test]
    fn rule_generation_is_stable_across_restart_and_changes_with_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = include_str!("../tests/fixtures/exchange_info_btcusdt.json");
        let first = parse_instrument_rules(payload, "BTC/USDT".parse()?, 1)?;
        let restarted = parse_instrument_rules(payload, "BTC/USDT".parse()?, 999)?;
        assert_eq!(
            stable_rules_generation(&first),
            stable_rules_generation(&restarted)
        );

        let mut changed = restarted;
        changed.maximum_quantity += changed.instrument.quantity_step;
        assert_ne!(
            stable_rules_generation(&first),
            stable_rules_generation(&changed)
        );
        Ok(())
    }
}
