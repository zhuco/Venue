use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{Asset, PositionSide, Price, Symbol};

/// Completeness of one signed account-risk observation. Only `Complete` may authorize mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskSourceStatus {
    Complete,
    Incomplete,
    AccountRestricted,
    Conflicting,
}

/// Account equity normalized by an adapter into one explicit risk currency.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountRiskSnapshot {
    pub exchange: String,
    pub account: String,
    pub risk_currency: Asset,
    #[serde(with = "rust_decimal::serde::str")]
    pub account_equity: Decimal,
    pub private_generation: u64,
    pub observed_at_ms: u64,
    pub source_status: RiskSourceStatus,
}

/// One authoritative Hedge leg expressed in the same risk currency as account equity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LegRiskSnapshot {
    pub symbol: Symbol,
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub mark_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub contract_multiplier: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl: Decimal,
    pub risk_currency: Asset,
    pub private_generation: u64,
    pub observed_at_ms: u64,
}

/// Validates the account and leg as one mutation-authorizing observation.
pub fn validate_risk_snapshot_pair(
    account: &AccountRiskSnapshot,
    leg: &LegRiskSnapshot,
    now_ms: u64,
    max_age_ms: u64,
) -> Result<(), RiskSnapshotError> {
    account.validate_at(now_ms, max_age_ms)?;
    leg.validate_at(now_ms, max_age_ms)?;
    if account.risk_currency != leg.risk_currency {
        return Err(RiskSnapshotError::Currency);
    }
    if account.private_generation != leg.private_generation {
        return Err(RiskSnapshotError::GenerationMismatch);
    }
    Ok(())
}

impl AccountRiskSnapshot {
    pub fn validate_at(&self, now_ms: u64, max_age_ms: u64) -> Result<(), RiskSnapshotError> {
        if !valid_scope(&self.exchange) || !valid_scope(&self.account) {
            return Err(RiskSnapshotError::Scope);
        }
        if self.source_status != RiskSourceStatus::Complete {
            return Err(RiskSnapshotError::SourceStatus);
        }
        if self.account_equity <= Decimal::ZERO {
            return Err(RiskSnapshotError::Equity);
        }
        validate_generation_and_freshness(
            self.private_generation,
            self.observed_at_ms,
            now_ms,
            max_age_ms,
        )
    }
}

impl LegRiskSnapshot {
    pub fn validate_at(&self, now_ms: u64, max_age_ms: u64) -> Result<(), RiskSnapshotError> {
        if !matches!(self.position_side, PositionSide::Long | PositionSide::Short) {
            return Err(RiskSnapshotError::PositionSide);
        }
        if self.quantity <= Decimal::ZERO
            || self.contract_multiplier <= Decimal::ZERO
            || self.notional <= Decimal::ZERO
        {
            return Err(RiskSnapshotError::LegValue);
        }
        let normalized_notional = self
            .quantity
            .checked_mul(self.mark_price.value())
            .and_then(|value| value.checked_mul(self.contract_multiplier))
            .ok_or(RiskSnapshotError::Notional)?;
        if self.notional != normalized_notional {
            return Err(RiskSnapshotError::Notional);
        }
        validate_generation_and_freshness(
            self.private_generation,
            self.observed_at_ms,
            now_ms,
            max_age_ms,
        )
    }
}

fn validate_generation_and_freshness(
    generation: u64,
    observed_at_ms: u64,
    now_ms: u64,
    max_age_ms: u64,
) -> Result<(), RiskSnapshotError> {
    if generation == 0 {
        return Err(RiskSnapshotError::Generation);
    }
    if observed_at_ms == 0 || observed_at_ms > now_ms {
        return Err(RiskSnapshotError::ObservedAt);
    }
    if now_ms.saturating_sub(observed_at_ms) > max_age_ms {
        return Err(RiskSnapshotError::Stale);
    }
    Ok(())
}

fn valid_scope(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RiskSnapshotError {
    #[error("risk snapshot exchange and account scopes are invalid")]
    Scope,
    #[error("risk snapshot source is incomplete, restricted, or conflicting")]
    SourceStatus,
    #[error("account equity must be positive")]
    Equity,
    #[error("risk snapshot generation must be positive")]
    Generation,
    #[error("risk snapshot observation time must be positive and not in the future")]
    ObservedAt,
    #[error("risk snapshot is older than its maximum accepted age")]
    Stale,
    #[error("risk snapshot leg must identify LONG or SHORT")]
    PositionSide,
    #[error("risk snapshot quantity, contract multiplier, and notional must be positive")]
    LegValue,
    #[error("risk snapshot notional differs from quantity times mark price times multiplier")]
    Notional,
    #[error("account and leg risk currencies differ")]
    Currency,
    #[error("account and leg private generations differ")]
    GenerationMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshots() -> Result<(AccountRiskSnapshot, LegRiskSnapshot), Box<dyn std::error::Error>> {
        let currency: Asset = "USDT".parse()?;
        Ok((
            AccountRiskSnapshot {
                exchange: "bitget".to_owned(),
                account: "uta_usdt".to_owned(),
                risk_currency: currency.clone(),
                account_equity: Decimal::new(20, 0),
                private_generation: 7,
                observed_at_ms: 1_000,
                source_status: RiskSourceStatus::Complete,
            },
            LegRiskSnapshot {
                symbol: "DOGE/USDT".parse()?,
                position_side: PositionSide::Long,
                quantity: Decimal::new(600, 0),
                mark_price: Price::new(Decimal::new(1, 1))?,
                contract_multiplier: Decimal::ONE,
                notional: Decimal::new(60, 0),
                unrealized_pnl: Decimal::new(101, 2),
                risk_currency: currency,
                private_generation: 7,
                observed_at_ms: 1_000,
            },
        ))
    }

    #[test]
    fn pair_requires_same_fresh_generation_and_currency() -> Result<(), Box<dyn std::error::Error>>
    {
        let (account, mut leg) = snapshots()?;
        validate_risk_snapshot_pair(&account, &leg, 4_000, 3_000)?;

        leg.private_generation = 8;
        assert_eq!(
            validate_risk_snapshot_pair(&account, &leg, 4_000, 3_000),
            Err(RiskSnapshotError::GenerationMismatch)
        );
        leg.private_generation = 7;
        leg.risk_currency = "USD".parse()?;
        assert_eq!(
            validate_risk_snapshot_pair(&account, &leg, 4_000, 3_000),
            Err(RiskSnapshotError::Currency)
        );
        Ok(())
    }

    #[test]
    fn stale_future_and_incomplete_observations_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut account, leg) = snapshots()?;
        assert_eq!(
            validate_risk_snapshot_pair(&account, &leg, 4_001, 3_000),
            Err(RiskSnapshotError::Stale)
        );
        account.observed_at_ms = 4_002;
        assert_eq!(
            validate_risk_snapshot_pair(&account, &leg, 4_001, 3_000),
            Err(RiskSnapshotError::ObservedAt)
        );
        account.observed_at_ms = 1_000;
        account.source_status = RiskSourceStatus::Conflicting;
        assert_eq!(
            validate_risk_snapshot_pair(&account, &leg, 4_000, 3_000),
            Err(RiskSnapshotError::SourceStatus)
        );
        Ok(())
    }

    #[test]
    fn zero_equity_and_net_leg_are_never_authoritative() -> Result<(), Box<dyn std::error::Error>> {
        let (mut account, mut leg) = snapshots()?;
        account.account_equity = Decimal::ZERO;
        assert_eq!(
            validate_risk_snapshot_pair(&account, &leg, 4_000, 3_000),
            Err(RiskSnapshotError::Equity)
        );
        account.account_equity = Decimal::ONE;
        leg.position_side = PositionSide::Net;
        assert_eq!(
            validate_risk_snapshot_pair(&account, &leg, 4_000, 3_000),
            Err(RiskSnapshotError::PositionSide)
        );
        Ok(())
    }

    #[test]
    fn normalized_notional_requires_exact_currency_aware_product()
    -> Result<(), Box<dyn std::error::Error>> {
        let (account, mut leg) = snapshots()?;
        leg.risk_currency = "USD".parse()?;
        leg.contract_multiplier = Decimal::new(999, 3);
        leg.notional = Decimal::new(5994, 2);
        let mut usd_account = account;
        usd_account.risk_currency = "USD".parse()?;
        validate_risk_snapshot_pair(&usd_account, &leg, 4_000, 3_000)?;

        leg.notional += Decimal::new(1, 3);
        assert_eq!(
            validate_risk_snapshot_pair(&usd_account, &leg, 4_000, 3_000),
            Err(RiskSnapshotError::Notional)
        );
        Ok(())
    }
}
