use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use venue_domain::domain::{Amount, Price, Symbol};

use super::{BARS_SOURCE, BOOK_SOURCE, TRADES_SOURCE};

use super::candidate_memory::BreakoutCursor;

pub const PHASE8_ATR14_PARAMETER_RELEASE_ID: &str = "scalping_binance_phase8_atr14_5u_entry08_v2";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    Scalping,
}

/// Controller lifecycle target consumed by the pure strategy reducer.  The controller owns how
/// this target is authorized and persisted; strategy only maps it to safe semantic exits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlTarget {
    Running,
    StopAndProtect,
    FlattenAndStop,
    EmergencyStop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyBinding {
    pub strategy_kind: StrategyKind,
    pub strategy_instance_id: String,
    pub run_id: String,
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub parameter_release_id: String,
    pub owner_scope: String,
    pub risk_budget: Amount,
}

impl StrategyBinding {
    #[must_use]
    pub const fn strategy_kind(&self) -> StrategyKind {
        self.strategy_kind
    }

    pub fn validate(&self) -> Result<(), ScalpingError> {
        if self.strategy_kind != StrategyKind::Scalping
            || [
                self.strategy_instance_id.as_str(),
                self.run_id.as_str(),
                self.exchange.as_str(),
                self.account.as_str(),
                self.parameter_release_id.as_str(),
                self.owner_scope.as_str(),
            ]
            .iter()
            .any(|value| value.trim().is_empty())
            || self.risk_budget.value <= Decimal::ZERO
        {
            return Err(ScalpingError::Binding);
        }
        Ok(())
    }

    pub fn digest(&self) -> String {
        let mut digest = Sha256::new();
        for field in [
            match self.strategy_kind {
                StrategyKind::Scalping => b"scalping".as_slice(),
            },
            self.strategy_instance_id.as_bytes(),
            self.run_id.as_bytes(),
            self.exchange.as_bytes(),
            self.account.as_bytes(),
            self.symbol.to_string().as_bytes(),
            self.parameter_release_id.as_bytes(),
            self.owner_scope.as_bytes(),
            self.risk_budget.asset.as_str().as_bytes(),
            self.risk_budget.value.normalize().to_string().as_bytes(),
        ] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field);
        }
        format!("{:x}", digest.finalize())
    }
}

/// Opaque unit for a strategy-level risk measure. It is intentionally distinct from `Asset`:
/// valuations may use a model-defined risk scale without claiming it is a transferable currency.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RiskUnit(String);

impl RiskUnit {
    pub fn new(value: impl Into<String>) -> Result<Self, ScalpingError> {
        let value = value.into();
        if !Self::is_valid(&value) {
            return Err(ScalpingError::Parameters);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn shadow() -> Self {
        Self("risk".to_owned())
    }

    fn is_valid(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    }
}

impl venue_domain::RiskUnitValue for RiskUnit {
    fn as_str(&self) -> &str {
        self.as_str()
    }
}

/// A positive magnitude in a declared logical risk unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskLimit {
    #[serde(with = "rust_decimal::serde::str")]
    pub value: Decimal,
    pub unit: RiskUnit,
}

impl RiskLimit {
    #[must_use]
    pub fn new(unit: RiskUnit, value: Decimal) -> Self {
        Self { value, unit }
    }

    pub fn is_valid(&self) -> bool {
        self.value > Decimal::ZERO && RiskUnit::is_valid(self.unit.as_str())
    }
}

/// The three independent strategy limits frozen into every admitted episode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskPlan {
    pub risk_per_episode: RiskLimit,
    pub quote_cap: Amount,
    pub max_episode_loss: RiskLimit,
}

impl RiskPlan {
    pub fn is_valid(&self) -> bool {
        self.risk_per_episode.is_valid()
            && self.max_episode_loss.is_valid()
            && self.risk_per_episode.unit == self.max_episode_loss.unit
            && self.max_episode_loss.value >= self.risk_per_episode.value
            && self.quote_cap.value > Decimal::ZERO
    }

    pub fn admits_worst_loss(&self, worst_loss: &RiskLimit) -> bool {
        worst_loss.is_valid()
            && worst_loss.unit == self.risk_per_episode.unit
            && worst_loss.value <= self.risk_per_episode.value
            && worst_loss.value <= self.max_episode_loss.value
    }
}

/// Strategy-release values use logical quote amounts and never encode tick, step, native symbol,
/// credentials, or an exchange order type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "parameters")]
pub enum ExitDistancePolicy {
    /// Compatibility policy for the existing phase-6 release.
    StaticHardStop {
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
    },
    /// ATR is represented by the feature frame's normalized ATR (`expected_move_bps`).
    AtrMultiples {
        atr_period: u16,
        #[serde(with = "rust_decimal::serde::str")]
        stop_multiplier: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        target_multiplier: Decimal,
    },
}

impl ExitDistancePolicy {
    fn phase6() -> Self {
        Self::StaticHardStop {
            hard_stop_distance_bps: Decimal::new(20, 0),
        }
    }

    pub fn phase8() -> Self {
        Self::AtrMultiples {
            atr_period: 14,
            stop_multiplier: Decimal::new(12, 1),
            target_multiplier: Decimal::new(8, 1),
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            Self::StaticHardStop {
                hard_stop_distance_bps,
            } => *hard_stop_distance_bps > Decimal::ZERO,
            Self::AtrMultiples {
                atr_period,
                stop_multiplier,
                target_multiplier,
            } => {
                *atr_period == 14
                    && *stop_multiplier > Decimal::ZERO
                    && *target_multiplier > Decimal::ZERO
            }
        }
    }

    pub fn distances_bps(&self, normalized_atr_bps: Decimal) -> (Decimal, Decimal) {
        match self {
            Self::StaticHardStop {
                hard_stop_distance_bps,
            } => (*hard_stop_distance_bps, normalized_atr_bps.abs()),
            Self::AtrMultiples {
                stop_multiplier,
                target_multiplier,
                ..
            } => (
                normalized_atr_bps.abs() * *stop_multiplier,
                normalized_atr_bps.abs() * *target_multiplier,
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingParams {
    pub feature_profile: String,
    pub feature_digest: String,
    pub calibration_model_version: String,
    pub calibration_model_digest: String,
    pub required_sources: BTreeSet<String>,
    pub enabled_experts: BTreeSet<Expert>,
    pub enabled_entry_styles: BTreeSet<EntryStyle>,
    pub short_window_ms: u64,
    pub mid_window_ms: u64,
    pub slow_window_ms: u64,
    pub max_data_age_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_spread_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub min_depth_quote: Decimal,
    pub max_decision_latency_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub regime_min_confidence: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub regime_confidence_margin: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_toxicity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub min_deviation_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub trend_threshold: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub breakout_threshold: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub shock_return_bps: Decimal,
    pub regime_dwell_ms: u64,
    pub max_order_attempts: u32,
    pub max_reprices: u32,
    pub entry_retry_cooldown_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub min_net_ev_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub uncertainty_multiplier: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub conflict_margin_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_entry_slippage_bps: Decimal,
    pub risk_per_episode: RiskLimit,
    pub quote_cap: Amount,
    pub max_episode_loss: RiskLimit,
    pub candidate_ttl_ms: u64,
    pub entry_ttl_ms: u64,
    pub cooldown_ms: u64,
    pub max_hold_ms: u64,
    pub max_unprotected_ms: u64,
    pub loss_window_ms: u64,
    pub drawdown_window_ms: u64,
    pub loss_window_limit: RiskLimit,
    pub drawdown_limit: RiskLimit,
    pub max_loss_streak: u32,
    pub loss_cooldown_ms: u64,
    #[serde(default = "ExitDistancePolicy::phase6")]
    pub exit_distance_policy: ExitDistancePolicy,
}

impl ScalpingParams {
    /// Builds the legacy-equivalent release. The caller-supplied binding is a separate hard
    /// account cap; it is applied only when an intent takes `min(quote_cap, binding.risk_budget)`.
    pub fn shadow(quote_hard_cap: Amount) -> Self {
        let risk_unit = RiskUnit::shadow();
        let risk_per_episode = RiskLimit::new(risk_unit.clone(), Decimal::ONE);
        let max_episode_loss = RiskLimit::new(risk_unit.clone(), Decimal::ONE);
        let loss_window_limit = RiskLimit::new(risk_unit.clone(), Decimal::ONE);
        let drawdown_limit = RiskLimit::new(risk_unit, Decimal::ONE);
        let quote_cap = Amount::new(quote_hard_cap.asset.clone(), Decimal::new(100, 0));
        Self {
            feature_profile: "scalping-shadow-v1".to_owned(),
            feature_digest: "0".repeat(64),
            calibration_model_version: "scalping-shadow-calibration-v1".to_owned(),
            calibration_model_digest: "0".repeat(64),
            required_sources: BTreeSet::from([
                BOOK_SOURCE.to_owned(),
                TRADES_SOURCE.to_owned(),
                BARS_SOURCE.to_owned(),
            ]),
            enabled_experts: BTreeSet::from([
                Expert::RangeFade,
                Expert::TrendPullback,
                Expert::BreakoutContinuation,
            ]),
            enabled_entry_styles: BTreeSet::from([
                EntryStyle::PassiveMaker,
                EntryStyle::MarketableLimit,
            ]),
            short_window_ms: 5_000,
            mid_window_ms: 30_000,
            slow_window_ms: 120_000,
            max_data_age_ms: 65_000,
            max_spread_bps: Decimal::new(8, 1),
            min_depth_quote: Decimal::new(100, 0),
            max_decision_latency_ms: 250,
            regime_min_confidence: Decimal::new(6, 1),
            regime_confidence_margin: Decimal::new(1, 1),
            max_toxicity: Decimal::new(8, 1),
            // WeightedMid divergence is bounded by half the spread. The old 0.5 bps default was
            // unreachable under its own 0.8 bps spread ceiling, so the migrated baseline keeps
            // the previously repaired 0.2 bps gate while preserving the legacy formula.
            min_deviation_bps: Decimal::new(2, 1),
            trend_threshold: Decimal::new(6, 1),
            breakout_threshold: Decimal::new(5, 1),
            shock_return_bps: Decimal::new(30, 0),
            regime_dwell_ms: 2_000,
            max_order_attempts: 2,
            max_reprices: 1,
            entry_retry_cooldown_ms: 250,
            min_net_ev_bps: Decimal::new(2, 1),
            uncertainty_multiplier: Decimal::ONE,
            conflict_margin_bps: Decimal::new(1, 1),
            max_entry_slippage_bps: Decimal::ONE,
            risk_per_episode,
            quote_cap,
            max_episode_loss,
            candidate_ttl_ms: 1_000,
            entry_ttl_ms: 1_000,
            cooldown_ms: 1_000,
            max_hold_ms: 60_000,
            max_unprotected_ms: 1_000,
            loss_window_ms: 3_600_000,
            drawdown_window_ms: 3_600_000,
            loss_window_limit,
            drawdown_limit,
            max_loss_streak: 3,
            loss_cooldown_ms: 5_000,
            exit_distance_policy: ExitDistancePolicy::phase6(),
        }
    }

    /// Builds the operator-authorized phase-8 parameter set. The release remains inactive until
    /// its exact deployment binding selects it; constructing the value grants no authority.
    pub fn phase8(quote_hard_cap: Amount) -> Self {
        let mut params = Self::shadow(quote_hard_cap);
        params.feature_profile = "scalping-phase8-atr14-v1".to_owned();
        params.enabled_entry_styles = [EntryStyle::PassiveMaker].into_iter().collect();
        // The 0.8 bps Shadow ceiling excludes liquid low-priced contracts whose single tick is
        // already wider. Phase 8 derives its passive distance from normalized ATR14 and the
        // instrument tick, so this bounded release-specific ceiling removes that structural veto.
        params.max_spread_bps = Decimal::new(20, 0);
        params.exit_distance_policy = ExitDistancePolicy::phase8();
        params.max_loss_streak = 3;
        params.loss_cooldown_ms = 3_600_000;
        params
    }

    /// Resolves only the explicitly named deployed release. Existing stage-6 bindings retain
    /// their compatibility parameters; a similarly named value cannot activate phase 8.
    pub fn for_binding(binding: &StrategyBinding) -> Self {
        if binding.parameter_release_id == PHASE8_ATR14_PARAMETER_RELEASE_ID {
            Self::phase8(binding.risk_budget.clone())
        } else {
            Self::shadow(binding.risk_budget.clone())
        }
    }

    pub fn validate_for(&self, binding: &StrategyBinding) -> Result<(), ScalpingError> {
        let digest_valid = [
            self.feature_digest.as_str(),
            self.calibration_model_digest.as_str(),
        ]
        .iter()
        .all(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
        if self.feature_profile.trim().is_empty()
            || self.calibration_model_version.trim().is_empty()
            || !digest_valid
            || self.required_sources.is_empty()
            || self.enabled_experts.is_empty()
            || self.enabled_entry_styles.is_empty()
            || self
                .required_sources
                .iter()
                .any(|source| source.trim().is_empty())
            || self.max_data_age_ms == 0
            || self.short_window_ms == 0
            || self.short_window_ms >= self.mid_window_ms
            || self.mid_window_ms >= self.slow_window_ms
            || self.max_decision_latency_ms == 0
            || self.candidate_ttl_ms == 0
            || self.candidate_ttl_ms > self.short_window_ms
            || self.entry_ttl_ms == 0
            || self.cooldown_ms == 0
            || self.entry_retry_cooldown_ms == 0
            || self.entry_retry_cooldown_ms > self.candidate_ttl_ms
            || self.max_hold_ms == 0
            || self.max_unprotected_ms == 0
            || self.max_unprotected_ms > self.max_hold_ms
            || self.loss_window_ms == 0
            || self.drawdown_window_ms == 0
            || self.drawdown_window_ms < self.loss_window_ms
            || self.max_loss_streak == 0
            || self.regime_dwell_ms == 0
            || self.max_order_attempts == 0
            || self.min_net_ev_bps <= Decimal::ZERO
            || self.uncertainty_multiplier <= Decimal::ZERO
            || self.conflict_margin_bps < Decimal::ZERO
            || self.max_spread_bps <= Decimal::ZERO
            || self.min_depth_quote <= Decimal::ZERO
            || self.regime_min_confidence <= Decimal::ZERO
            || self.regime_min_confidence > Decimal::ONE
            || self.regime_confidence_margin < Decimal::ZERO
            || self.regime_confidence_margin > Decimal::ONE
            || self.max_toxicity < Decimal::ZERO
            || self.max_toxicity > Decimal::ONE
            || self.min_deviation_bps <= Decimal::ZERO
            || self.trend_threshold <= Decimal::ZERO
            || self.trend_threshold > Decimal::ONE
            || self.breakout_threshold <= Decimal::ZERO
            || self.shock_return_bps <= Decimal::ZERO
            || self.max_entry_slippage_bps <= Decimal::ZERO
            || !self.exit_distance_policy.is_valid()
            || !(RiskPlan {
                risk_per_episode: self.risk_per_episode.clone(),
                quote_cap: self.quote_cap.clone(),
                max_episode_loss: self.max_episode_loss.clone(),
            })
            .is_valid()
            || self.quote_cap.asset != binding.risk_budget.asset
            || !self.loss_window_limit.is_valid()
            || !self.drawdown_limit.is_valid()
            || self.loss_window_limit.unit != self.risk_per_episode.unit
            || self.drawdown_limit.unit != self.risk_per_episode.unit
            || self.loss_cooldown_ms < self.cooldown_ms
        {
            return Err(ScalpingError::Parameters);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use venue_domain::domain::{Amount, Asset};

    use super::{
        EntryStyle, ExitDistancePolicy, PHASE8_ATR14_PARAMETER_RELEASE_ID, RiskUnit,
        ScalpingParams, StrategyBinding, StrategyKind,
    };

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
            strategy_instance_id: "scalping_primary".to_owned(),
            run_id: "shadow_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol: "BTC/USDT".parse()?,
            parameter_release_id: "scalping-shadow-v1".to_owned(),
            owner_scope: "scalping_primary:shadow_1".to_owned(),
            risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
        })
    }

    #[test]
    fn shadow_defaults_to_a_positive_minimum_net_ev_gate() -> Result<(), Box<dyn std::error::Error>>
    {
        let binding = binding()?;
        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        assert_eq!(params.min_deviation_bps, Decimal::new(2, 1));
        assert_eq!(params.max_reprices, 1);
        assert_eq!(params.min_net_ev_bps, Decimal::new(2, 1));
        assert_eq!(params.conflict_margin_bps, Decimal::new(1, 1));
        assert_eq!(
            (
                params.short_window_ms,
                params.mid_window_ms,
                params.slow_window_ms
            ),
            (5_000, 30_000, 120_000)
        );
        assert_eq!(params.max_decision_latency_ms, 250);
        assert_eq!(params.entry_ttl_ms, 1_000);
        assert_eq!(params.loss_window_ms, 3_600_000);
        assert_eq!(params.drawdown_window_ms, 3_600_000);
        assert_eq!(params.loss_cooldown_ms, 5_000);
        assert_eq!(params.regime_min_confidence, Decimal::new(6, 1));
        assert_eq!(params.regime_confidence_margin, Decimal::new(1, 1));
        assert_eq!(params.risk_per_episode.value, Decimal::ONE);
        assert_eq!(params.risk_per_episode.unit.as_str(), "risk");
        assert_eq!(params.max_episode_loss.value, Decimal::ONE);
        assert_eq!(params.loss_window_limit.value, Decimal::ONE);
        assert_eq!(params.drawdown_limit.value, Decimal::ONE);
        assert_eq!(params.quote_cap.value, Decimal::new(100, 0));
        assert_eq!(params.quote_cap.asset, binding.risk_budget.asset);
        assert!(params.validate_for(&binding).is_ok());

        params.min_net_ev_bps = Decimal::ZERO;
        assert!(params.validate_for(&binding).is_err());
        Ok(())
    }

    #[test]
    fn phase8_exit_policy_uses_the_authorized_atr_multiples() {
        let policy = ExitDistancePolicy::phase8();
        assert!(policy.is_valid());
        assert_eq!(
            policy.distances_bps(Decimal::new(25, 1)),
            (Decimal::new(30, 1), Decimal::new(20, 1))
        );
    }

    #[test]
    fn phase8_uses_three_losses_and_one_hour_cooldown() -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::phase8(binding.risk_budget.clone());
        assert_eq!(params.max_loss_streak, 3);
        assert_eq!(params.loss_cooldown_ms, 3_600_000);
        assert_eq!(params.max_spread_bps, Decimal::new(20, 0));
        assert_eq!(params.exit_distance_policy, ExitDistancePolicy::phase8());
        assert_eq!(
            params.enabled_entry_styles,
            [EntryStyle::PassiveMaker].into_iter().collect()
        );
        assert!(params.validate_for(&binding).is_ok());
        Ok(())
    }

    #[test]
    fn only_the_exact_phase8_release_selects_phase8_parameters()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut binding = binding()?;
        assert_eq!(
            ScalpingParams::for_binding(&binding).exit_distance_policy,
            ExitDistancePolicy::phase6()
        );
        binding.parameter_release_id = PHASE8_ATR14_PARAMETER_RELEASE_ID.to_owned();
        assert_eq!(
            ScalpingParams::for_binding(&binding).exit_distance_policy,
            ExitDistancePolicy::phase8()
        );
        binding.parameter_release_id.push_str("-lookalike");
        assert_eq!(
            ScalpingParams::for_binding(&binding).exit_distance_policy,
            ExitDistancePolicy::phase6()
        );
        Ok(())
    }

    #[test]
    fn validates_legacy_time_and_confidence_relationships() -> Result<(), Box<dyn std::error::Error>>
    {
        let binding = binding()?;
        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.mid_window_ms = params.short_window_ms;
        assert!(params.validate_for(&binding).is_err());

        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.candidate_ttl_ms = params.short_window_ms.saturating_add(1);
        assert!(params.validate_for(&binding).is_err());

        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.regime_confidence_margin = Decimal::new(11, 1);
        assert!(params.validate_for(&binding).is_err());
        Ok(())
    }

    #[test]
    fn rejects_mixed_or_inverted_risk_budget_units() -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.max_episode_loss.value = Decimal::new(9, 1);
        assert!(params.validate_for(&binding).is_err());

        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.drawdown_limit.unit = RiskUnit::new("different-risk")?;
        assert!(params.validate_for(&binding).is_err());

        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.quote_cap.asset = "USDC".parse()?;
        assert!(params.validate_for(&binding).is_err());
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expert {
    RangeFade,
    TrendPullback,
    BreakoutContinuation,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryStyle {
    PassiveMaker,
    MarketableLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitTemplate {
    FairValue,
    TrendTrail,
    Breakout,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    Range,
    TrendUp,
    TrendDown,
    ExpansionUp,
    ExpansionDown,
    Shock,
    RegimeUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExposureState {
    Flat,
    Open,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectionState {
    Complete,
    Gap,
    Unknown,
}

/// Anonymous projection of authority facts. It deliberately carries no order identifier,
/// quantity, hedge side, or raw exchange field into the strategy layer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SafetyProjection {
    pub private_snapshot_ready: bool,
    pub exposure: ExposureState,
    pub execution_unknown: bool,
    pub protection: ProtectionState,
    pub owner_conflict: bool,
    pub risk_budget_available: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Long,
    Short,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FillSlice {
    #[serde(with = "rust_decimal::serde::str")]
    pub fill_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub probability: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeProbabilities {
    #[serde(with = "rust_decimal::serde::str")]
    pub target: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub stop: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub other: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateCosts {
    #[serde(with = "rust_decimal::serde::str")]
    pub entry_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub exit_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub nonfill_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub opportunity_cost_bps: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticPurpose {
    Entry,
}

/// An execution-independent strategy proposal. Execution binds it to an owner, quantity, exact
/// price, position side, journal, and venue request only after all lower gates have passed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SemanticIntent {
    pub intent_id: String,
    pub symbol: Symbol,
    pub direction: Direction,
    pub purpose: SemanticPurpose,
    pub expert: Expert,
    pub entry_style: EntryStyle,
    pub exit_template: ExitTemplate,
    pub attempt_cap: u32,
    pub max_reprices: u32,
    pub risk_plan: RiskPlan,
    pub target_quote: Amount,
    pub reference_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_slippage_bps: Decimal,
    pub valid_until_ms: u64,
    pub entry_ttl_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub hard_stop_distance_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub target_distance_bps: Decimal,
    pub max_hold_ms: u64,
    pub max_unprotected_ms: u64,
    pub requires_server_protection: bool,
    pub opportunity_key: String,
    pub breakout_cursor: Option<BreakoutCursor>,
    pub idempotency_seed: String,
}

/// A deterministic, execution-independent proposal set. It is bound to the feature cursors and
/// controller authority that produced it, but deliberately contains neither venue nor order data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidatePreparation {
    pub preparation_id: String,
    pub binding_digest: String,
    pub controller_revision: u64,
    pub authority_generation: u64,
    pub market_regime: MarketRegime,
    pub frame_generation: u64,
    pub watermark_ms: u64,
    pub valid_until_ms: u64,
    pub candidates: Vec<SemanticIntent>,
}

/// Anonymous evidence from independently owned calibration, cost, and risk projections. The
/// digests identify their releases without importing those owners' domain types into strategy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateEvidence {
    pub candidate_id: String,
    pub preparation_id: String,
    pub binding_digest: String,
    pub frame_generation: u64,
    pub watermark_ms: u64,
    pub valid_until_ms: u64,
    pub calibration_model_version: String,
    pub calibration_digest: String,
    pub cost_digest: String,
    pub risk_digest: String,
    pub worst_loss: RiskLimit,
    #[serde(with = "rust_decimal::serde::str")]
    pub fill_probability: Decimal,
    pub fill_distribution: Vec<FillSlice>,
    pub outcomes: OutcomeProbabilities,
    pub costs: CandidateCosts,
    #[serde(with = "rust_decimal::serde::str")]
    pub target_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub stop_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub other_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub outcome_expected_value_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub net_expected_value_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub uncertainty_bps: Decimal,
    pub admissible: bool,
}

/// Strategy-owned projection for one admitted opportunity. It stores only the frozen semantic
/// candidate and anonymous evidence; physical orders, fills, and venue identifiers stay outside
/// the strategy boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeProjection {
    pub episode_id: String,
    pub frozen_intent: SemanticIntent,
    /// Optional only for the retired external valuation path. Direct strategy admission does not
    /// invent calibration, quote, or bundle evidence merely to persist an episode.
    #[serde(default)]
    pub frozen_evidence: Option<CandidateEvidence>,
    pub state: EpisodeState,
    pub attempts_started: u32,
    pub retry_not_before_ms: Option<u64>,
    pub opened_at_ms: Option<u64>,
    pub last_observed_at_ms: u64,
    pub last_fact_id: Option<String>,
    #[serde(default)]
    pub episode_fault_deadline: Option<ArmedEpisodeFaultDeadline>,
    #[serde(default)]
    pub control_fault_deadline: Option<SafetyDeadline>,
    #[serde(default)]
    pub fault: Option<FaultProjection>,
    #[serde(default)]
    pub last_deadline_fired_id: Option<String>,
    #[serde(default)]
    pub last_deadline_fired_generation: Option<u64>,
    #[serde(default)]
    pub last_recovery_authorization_id: Option<String>,
}

impl EpisodeProjection {
    pub fn reserve(
        frozen_intent: SemanticIntent,
        frozen_evidence: Option<CandidateEvidence>,
        observed_at_ms: u64,
    ) -> Self {
        Self {
            episode_id: frozen_intent.intent_id.clone(),
            frozen_intent,
            frozen_evidence,
            state: EpisodeState::Reserved,
            attempts_started: 1,
            retry_not_before_ms: None,
            opened_at_ms: None,
            last_observed_at_ms: observed_at_ms,
            last_fact_id: None,
            episode_fault_deadline: None,
            control_fault_deadline: None,
            fault: None,
            last_deadline_fired_id: None,
            last_deadline_fired_generation: None,
            last_recovery_authorization_id: None,
        }
    }

    pub fn validate_persisted(&self) -> Result<(), ScalpingError> {
        if self.episode_id.trim().is_empty()
            || self.episode_id != self.frozen_intent.intent_id
            || self
                .frozen_evidence
                .as_ref()
                .is_some_and(|evidence| evidence.candidate_id != self.frozen_intent.intent_id)
            || !self.frozen_intent.risk_plan.is_valid()
            || self.frozen_intent.target_quote.asset != self.frozen_intent.risk_plan.quote_cap.asset
            || self.frozen_intent.target_quote.value <= Decimal::ZERO
            || self.frozen_intent.target_quote.value > self.frozen_intent.risk_plan.quote_cap.value
            || self.attempts_started == 0
            || self.attempts_started > self.frozen_intent.attempt_cap
            || self.frozen_intent.attempt_cap == 0
            || self.frozen_intent.max_hold_ms == 0
            || self.frozen_intent.max_unprotected_ms == 0
            || self.last_observed_at_ms == 0
            || matches!(self.state, EpisodeState::EntryRetryWait)
                != self.retry_not_before_ms.is_some()
            || self
                .opened_at_ms
                .is_some_and(|opened_at| opened_at == 0 || opened_at > self.last_observed_at_ms)
            || self.episode_fault_deadline.as_ref().is_some_and(|armed| {
                armed.deadline.validate().is_err()
                    || armed.deadline.expires_at_ms
                        > armed
                            .deadline
                            .armed_at_ms
                            .saturating_add(self.frozen_intent.max_unprotected_ms)
            })
            || self
                .control_fault_deadline
                .as_ref()
                .is_some_and(|deadline| deadline.validate().is_err())
            || self
                .fault
                .as_ref()
                .is_some_and(|fault| fault.validate().is_err())
            || self.fault.as_ref().is_some_and(|fault| {
                !matches!(
                    (fault.scope, self.state),
                    (FaultScope::Episode(_), EpisodeState::EpisodeFaulted)
                        | (FaultScope::Control, EpisodeState::ControlFaulted)
                )
            })
            || self.last_deadline_fired_id.is_some()
                != self.last_deadline_fired_generation.is_some()
            || matches!(
                self.state,
                EpisodeState::EpisodeFaulted | EpisodeState::ControlFaulted
            ) != self.fault.is_some()
        {
            return Err(ScalpingError::Checkpoint);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeState {
    Reserved,
    EntryRetryWait,
    Open,
    ExitPending,
    StoppedProtected,
    StoppedFlat,
    Cooldown,
    EpisodeFaulted,
    ControlFaulted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeFaultKind {
    UnprotectedExposure,
    ExecutionUnknown,
    OwnerConflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "scope", content = "kind")]
pub enum FaultScope {
    Episode(EpisodeFaultKind),
    Control,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SafetyDeadline {
    pub deadline_id: String,
    pub generation: u64,
    pub armed_at_ms: u64,
    pub expires_at_ms: u64,
}

impl SafetyDeadline {
    pub fn validate(&self) -> Result<(), ScalpingError> {
        if self.deadline_id.trim().is_empty()
            || self.generation == 0
            || self.armed_at_ms == 0
            || self.expires_at_ms <= self.armed_at_ms
        {
            return Err(ScalpingError::Fault);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArmedEpisodeFaultDeadline {
    pub kind: EpisodeFaultKind,
    pub deadline: SafetyDeadline,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeadlineFired {
    pub deadline_id: String,
    pub generation: u64,
    pub fired_at_ms: u64,
    pub root_cause_fact_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FaultProjection {
    pub scope: FaultScope,
    pub deadline_id: String,
    pub generation: u64,
    pub root_cause_fact_id: String,
    pub faulted_at_ms: u64,
}

impl FaultProjection {
    fn validate(&self) -> Result<(), ScalpingError> {
        if self.deadline_id.trim().is_empty()
            || self.generation == 0
            || self.root_cause_fact_id.trim().is_empty()
            || self.faulted_at_ms == 0
        {
            return Err(ScalpingError::Fault);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FaultRecoveryAuthorization {
    pub authorization_id: String,
    pub episode_id: String,
    pub scope: FaultScope,
    pub fault_generation: u64,
    pub root_cause_fact_id: String,
    pub valid_until_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeExitReason {
    HardStop,
    TargetReached,
    MaxHoldElapsed,
    StopAndProtect,
    FlattenAndStop,
    EmergencyStop,
    SafetyProjectionLost,
    UnprotectedDeadline,
}

/// Pure semantic output for the owner/risk adapter. It deliberately cannot identify or mutate a
/// physical order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EpisodeAction {
    CancelEntry {
        reason: EpisodeExitReason,
    },
    MaintainProtection {
        direction: Direction,
        hard_stop_distance_bps: Decimal,
    },
    Exit {
        direction: Direction,
        reason: EpisodeExitReason,
        opportunity_key: String,
    },
    ArmFaultDeadline {
        kind: EpisodeFaultKind,
        no_later_than_ms: u64,
    },
    CancelFaultDeadline {
        deadline_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "detail")]
pub enum ScalpingState {
    Bootstrapping,
    Ready,
    CandidatePending(Box<CandidatePreparation>),
    Reserved(Box<SemanticIntent>),
    Cooldown { until_ms: u64 },
    Blocked(BlockingReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockingReason {
    ControlStopped,
    PrivateSnapshot,
    ExposureNotFlat,
    ExecutionUnknown,
    ProtectionGap,
    OwnerConflict,
    RiskBudget,
    StrategyRisk,
    RecoveryAuthorization,
    FeatureFrame,
    FeatureProgress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoopReason {
    Blocked(BlockingReason),
    DecisionExpired,
    RegimeAmbiguous,
    CandidatePending,
    ActiveEpisode,
    EvidenceUnavailable,
    DuplicateOpportunity,
    CandidateMemoryFull,
    RecoveryWarmup,
    Cooldown,
    NoSignal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalpingDecision {
    Prepared(Box<CandidatePreparation>),
    Intent(Box<SemanticIntent>),
    Noop(NoopReason),
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingError {
    #[error("strategy binding is incomplete or has an invalid risk budget")]
    Binding,
    #[error("scalping parameter release is invalid or incompatible with its binding")]
    Parameters,
    #[error("feature frame belongs to a different symbol")]
    Symbol,
    #[error("feature frame is invalid: {detail}")]
    Feature { detail: String },
    #[error("feature profile identity differs from the parameter release")]
    FeatureProfile,
    #[error("feature frame does not advance the persisted strategy watermark")]
    FeatureProgress,
    #[error("checkpoint is incompatible with the current strategy binding or release")]
    Checkpoint,
    #[error("strategy projection persistence failed: {detail}")]
    Persistence { detail: String },
    #[error("controller authorization is absent, stale, or bound to a different instance")]
    Authorization,
    #[error("shadow outcome does not match the pending semantic intent")]
    ShadowOutcome,
    #[error("candidate evidence is malformed or does not bind to the pending preparation")]
    Evidence,
    #[error("fresh reprice evidence is incompatible with the frozen candidate or removes its edge")]
    Reprice,
    #[error("episode projection is absent, stale, or incompatible with its frozen candidate")]
    Episode,
    #[error("strategy risk projection is stale, unordered, or incomplete")]
    Risk,
    #[error("fault deadline or recovery authorization is stale or incompatible")]
    Fault,
}
