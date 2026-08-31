use std::{io, path::PathBuf};

use rust_decimal::Decimal;
use tempfile::TempDir;
use venue_domain::domain::Asset;
use venue_gateway_api::{GatewayMode, VenueId};

use super::*;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000009";

#[derive(Debug)]
struct CursorGateway {
    binding: GatewayBinding,
    seen: Option<String>,
}

impl AccountPhysicalGateway for CursorGateway {
    type Error = io::Error;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        self.seen = request.previous_fills_cursor().map(str::to_owned);
        AccountRecoveryReport::new(self.binding.clone(), 1, Vec::new()).map_err(io::Error::other)
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        self.seen = request.previous_fills_cursor().map(str::to_owned);
        SignedAccountSnapshot::complete(
            self.binding.clone(),
            now_ms()?,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            Vec::new(),
            Vec::new(),
            "bybit-exec:100:fill-1".to_owned(),
            Vec::new(),
        )
    }

    fn dispatch(&mut self, _permit: AccountDispatchPermit) -> AccountGatewayResult {
        AccountGatewayResult::Unknown
    }
}

fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
    Ok(GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?)
}

fn root(temp: &TempDir) -> PathBuf {
    temp.path().join("bybit").join("LIVE").join(ACCOUNT)
}

#[test]
fn successful_snapshot_persists_cursor_and_restart_supplies_it()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let mut host = AccountMutationHost::open(
        root(&temp),
        binding.clone(),
        Decimal::TEN,
        CursorGateway {
            binding: binding.clone(),
            seen: None,
        },
    )?;
    host.refresh_signed_snapshot()?;
    assert_eq!(host.gateway.seen, None);
    drop(host);

    let reopened = AccountMutationHost::open(
        root(&temp),
        binding.clone(),
        Decimal::TEN,
        CursorGateway {
            binding,
            seen: None,
        },
    )?;
    assert_eq!(
        reopened.gateway.seen.as_deref(),
        Some("bybit-exec:100:fill-1")
    );
    Ok(())
}

fn usdc_valuation(
    observed_at_ms: u64,
    rates: Vec<AccountQuoteToUsdtRate>,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    let usdc = Asset::new("USDC").map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let binding = GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        ACCOUNT,
        "SOL/USDC"
            .parse()
            .map_err(|_| AccountHostValidationError::RiskEvidence)?,
    )
    .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    AccountRiskEvidence::complete_with_usdt_valuation(
        binding,
        observed_at_ms,
        7,
        vec![AccountRiskAmount {
            asset: usdc,
            value: Decimal::from(2),
        }],
        Vec::new(),
        rates,
    )
}

#[test]
fn usdc_risk_requires_a_fresh_same_generation_non_parity_rate()
-> Result<(), Box<dyn std::error::Error>> {
    let usdc = Asset::new("USDC")?;
    let rate = AccountQuoteToUsdtRate {
        asset: usdc.clone(),
        usdt_per_asset: Decimal::new(125, 2),
        observed_at_ms: 10_000,
        private_generation: 7,
    };
    let evidence = usdc_valuation(10_000, vec![rate.clone()])?;
    assert_eq!(
        evidence.value_in_usdt(&usdc, Decimal::from(2))?,
        Decimal::new(25, 1)
    );
    let binding = GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        ACCOUNT,
        "SOL/USDC".parse()?,
    )?;
    assert!(evidence.validate_for(&binding, 70_001).is_err());
    assert!(usdc_valuation(70_001, vec![rate.clone()]).is_err());
    let mut wrong_generation = rate.clone();
    wrong_generation.private_generation = 8;
    assert!(usdc_valuation(10_000, vec![wrong_generation]).is_err());
    assert!(usdc_valuation(10_000, Vec::new()).is_err());
    assert!(usdc_valuation(10_000, vec![rate.clone(), rate]).is_err());
    Ok(())
}
