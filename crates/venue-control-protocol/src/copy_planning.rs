//! Bounded, signed account observations used to assemble immutable Copy planning inputs.
//!
//! These facts travel beside a node projection and are not part of the browser read model.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use venue_domain::domain::{Amount, InstrumentIdentity, MarketKind};

use crate::{ExecutionFactBinding, ProtocolError, is_uuid};

pub const MAX_COPY_PLANNING_FACTS: usize = 16;
pub const MAX_COPY_PLANNING_FACT_TTL_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyPlanningFactRole {
    Leader,
    Follower,
}

/// One exact-side observation.  It is evidence only: Control combines a fresh Leader and
/// Follower fact with the current relation before it may create an immutable planner snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyPlanningFact {
    pub role: CopyPlanningFactRole,
    pub relation_id: String,
    pub relation_revision: u64,
    pub policy_digest: [u8; 32],
    pub binding: ExecutionFactBinding,
    pub instrument: InstrumentIdentity,
    pub private_generation: u64,
    pub rules_generation: u64,
    pub observed_ms: u64,
    pub expires_ms: u64,
    /// Exact quote-asset signed net position, never a UI-derived desired target.
    pub quote_net_exposure: Amount,
    /// Present only for the follower and kept in its native quote asset.  It is not equity.
    pub follower_available_margin: Option<Amount>,
    /// Present only for the leader and explicitly configured per strategy; it is never inferred
    /// from an account balance.
    pub leader_configured_capital: Option<Amount>,
    pub fact_digest: [u8; 32],
}

impl CopyPlanningFact {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if !is_uuid(&self.relation_id)
            || self.relation_revision == 0
            || self.policy_digest == [0; 32]
            || self.private_generation == 0
            || self.rules_generation == 0
            || self.observed_ms == 0
            || self.expires_ms <= self.observed_ms
            || self.expires_ms.saturating_sub(self.observed_ms) > MAX_COPY_PLANNING_FACT_TTL_MS
            || self.fact_digest == [0; 32]
        {
            return Err(ProtocolError::SnapshotContent);
        }
        self.binding.validate()?;
        let valid_instrument = match self.instrument.market {
            MarketKind::Spot => self.instrument.settlement_asset.is_none(),
            MarketKind::LinearPerpetual => self
                .instrument
                .settlement_asset
                .as_ref()
                .is_some_and(|asset| asset.as_str() == self.binding.symbol.quote()),
        };
        if self.instrument.symbol != self.binding.symbol
            || !valid_instrument
            || self.quote_net_exposure.asset.as_str() != self.binding.symbol.quote()
        {
            return Err(ProtocolError::SnapshotContent);
        }
        let correct_role_fields = match self.role {
            CopyPlanningFactRole::Leader => {
                self.follower_available_margin.is_none()
                    && self
                        .leader_configured_capital
                        .as_ref()
                        .is_some_and(|capital| {
                            capital.asset == self.quote_net_exposure.asset
                                && capital.value.is_sign_positive()
                                && !capital.value.is_zero()
                        })
            }
            CopyPlanningFactRole::Follower => {
                self.leader_configured_capital.is_none()
                    && self
                        .follower_available_margin
                        .as_ref()
                        // A negative signed available margin is risk evidence, not malformed
                        // data. Planner admission decides whether it can fund new Copy risk.
                        .is_some_and(|margin| margin.asset == self.quote_net_exposure.asset)
            }
        };
        if !correct_role_fields {
            return Err(ProtocolError::SnapshotContent);
        }
        Ok(())
    }
}

pub(crate) fn validate_copy_planning_facts(
    facts: &[CopyPlanningFact],
    generated_ms: u64,
) -> Result<(), ProtocolError> {
    if facts.len() > MAX_COPY_PLANNING_FACTS
        || facts.iter().any(|fact| {
            fact.validate().is_err()
                || fact.observed_ms > generated_ms
                || generated_ms >= fact.expires_ms
        })
        || facts
            .iter()
            .map(|fact| {
                (
                    fact.relation_id.clone(),
                    fact.relation_revision,
                    fact.role,
                    fact.binding.venue,
                    fact.binding.mode,
                    fact.binding.trading_account_id.clone(),
                    fact.binding.symbol.to_string(),
                    fact.binding.instance_id.clone(),
                    fact.binding.config_epoch,
                )
            })
            .collect::<BTreeSet<_>>()
            .len()
            != facts.len()
    {
        return Err(ProtocolError::SnapshotContent);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{Asset, Symbol};

    use super::*;
    use crate::{GatewayMode, VenueId};

    fn leader() -> Result<CopyPlanningFact, Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        let asset = Asset::new("USDT")?;
        Ok(CopyPlanningFact {
            role: CopyPlanningFactRole::Leader,
            relation_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            relation_revision: 1,
            policy_digest: [1; 32],
            binding: ExecutionFactBinding {
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: "00000000-0000-4000-8000-000000000002".to_owned(),
                symbol: symbol.clone(),
                instance_id: "leader".to_owned(),
                config_epoch: 1,
            },
            instrument: InstrumentIdentity {
                symbol,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(asset.clone()),
            },
            private_generation: 3,
            rules_generation: 4,
            observed_ms: 100,
            expires_ms: 200,
            quote_net_exposure: Amount::new(asset.clone(), Decimal::from(-2)),
            follower_available_margin: None,
            leader_configured_capital: Some(Amount::new(asset, Decimal::TEN)),
            fact_digest: [2; 32],
        })
    }

    #[test]
    fn planning_facts_bind_exact_role_currency_and_fresh_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let fact = leader()?;
        assert_eq!(fact.validate(), Ok(()));
        assert_eq!(
            validate_copy_planning_facts(std::slice::from_ref(&fact), 150),
            Ok(())
        );
        let mut invalid = fact.clone();
        invalid.follower_available_margin = Some(Amount::new(Asset::new("USDT")?, Decimal::ONE));
        assert!(invalid.validate().is_err());
        let mut invalid = fact;
        invalid.instrument.settlement_asset = Some(Asset::new("USDC")?);
        assert!(invalid.validate().is_err());
        Ok(())
    }

    #[test]
    fn signed_negative_follower_margin_remains_observable() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut fact = leader()?;
        fact.role = CopyPlanningFactRole::Follower;
        fact.leader_configured_capital = None;
        fact.follower_available_margin = Some(Amount::new(Asset::new("USDT")?, Decimal::from(-3)));
        assert_eq!(fact.validate(), Ok(()));
        Ok(())
    }
}
