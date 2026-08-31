use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use crate::NodeError;
use serde::Deserialize;
use venue_domain::domain::Amount;
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use venue_runtime::{
    AccountSymbolSet, StrategyBinding, StrategyInstanceKey, StrategyKind, account::AccountKey,
};
use venue_strategies::{
    hedged_grid::{HedgedGridBinding, HedgedGridParams, HedgedGridState},
    scalping::{
        ScalpingParams, StrategyBinding as ScalpingStrategyBinding,
        StrategyKind as ScalpingStrategyKind,
    },
};

pub const NODE_RUNTIME_CONFIG_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize)]
pub struct NodeRuntimeConfig {
    pub version: u16,
    pub mode: GatewayMode,
    pub venue: VenueId,
    pub trading_account_id: String,
    pub node_id: String,
    pub control: NodeControlLoopConfig,
    pub strategies: Vec<NodeRuntimeStrategy>,
}

/// Exact local Control transport and resident scheduling bounds. All fields are explicit so a
/// production Node cannot accidentally inherit an unbounded or remote control path.
#[derive(Clone, Debug, Deserialize)]
pub struct NodeControlLoopConfig {
    pub loopback_origin: String,
    pub poll_interval_ms: u64,
    pub projection_interval_ms: u64,
    pub lease_duration_ms: u64,
    pub claim_limit: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeRuntimeStrategy {
    pub strategy_kind: StrategyKind,
    pub instance_id: String,
    pub run_id: String,
    pub config_digest: String,
    pub config_epoch: u64,
    pub symbol: venue_domain::domain::Symbol,
    /// Grid deployment parameters and recovery discipline. It is mandatory for Grid and absent
    /// for every other actor so a generic strategy record cannot accidentally bootstrap a grid.
    #[serde(default)]
    pub grid: Option<NodeGridRuntimeConfig>,
    /// The pure Scalping release identity and its strategy-local hard budget. It is mandatory
    /// for Scalping so a generic actor cannot manufacture a feature profile or checkpoint.
    #[serde(default)]
    pub scalping: Option<NodeScalpingRuntimeConfig>,
    /// An opt-in, strategy-scoped Copy leader capital allocation.  Omission disables leader
    /// fact publication; total account equity is never substituted.
    #[serde(default)]
    pub copy_leader_capital: Option<Amount>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NodeGridRecoveryPolicy {
    /// An existing account-local grid bridge checkpoint must be restored exactly.
    RequireExisting,
    /// First installation may build an empty state only when the fixed checkpoint is absent.
    /// Existing state is always restored, never replaced from a price-derived order key.
    BootstrapWhenAbsent,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeGridRuntimeConfig {
    pub params: HedgedGridParams,
    pub recovery: NodeGridRecoveryPolicy,
    /// Explicitly permits the reducer's durable, no-market-replenishment rebuild mode.  It is
    /// intentionally opt-in: ordinary first installation still rejects a leg below one grid.
    #[serde(default)]
    pub skip_inventory_replenishment_until_recovered: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeScalpingRuntimeConfig {
    pub parameter_release_id: String,
    pub owner_scope: String,
    pub risk_budget: Amount,
}

impl NodeRuntimeConfig {
    pub fn load(path: &Path, binding: &GatewayBinding) -> Result<Self, NodeError> {
        let bytes = fs::read(path).map_err(|_| NodeError::RuntimeConfig)?;
        if bytes.is_empty() || bytes.len() > 1_048_576 {
            return Err(NodeError::RuntimeConfig);
        }
        let config =
            serde_json::from_slice::<Self>(&bytes).map_err(|_| NodeError::RuntimeConfig)?;
        config.validate(binding)?;
        Ok(config)
    }

    pub fn validate(&self, binding: &GatewayBinding) -> Result<(), NodeError> {
        if self.version != NODE_RUNTIME_CONFIG_VERSION
            || self.mode != GatewayMode::Live
            || self.venue != binding.venue
            || self.mode != binding.mode
            || self.trading_account_id != binding.trading_account_id
            || self.node_id.trim().is_empty()
            || self.node_id.len() > 128
            || self.control.validate().is_err()
            || self.strategies.is_empty()
        {
            return Err(NodeError::RuntimeConfig);
        }
        let account = AccountKey::new(self.venue, self.trading_account_id.clone())
            .map_err(|_| NodeError::RuntimeConfig)?;
        let mut ids = BTreeSet::new();
        let mut symbols = BTreeSet::new();
        for strategy in &self.strategies {
            if strategy.config_epoch != 1
                || !ids.insert(strategy.instance_id.clone())
                || !symbols.insert(strategy.symbol.clone())
            {
                return Err(NodeError::RuntimeConfig);
            }
            let key = StrategyInstanceKey::new(
                account.clone(),
                strategy.strategy_kind,
                strategy.instance_id.clone(),
                strategy.symbol.clone(),
            )
            .map_err(|_| NodeError::RuntimeConfig)?;
            StrategyBinding::new(key, strategy.run_id.clone(), strategy.config_digest.clone())
                .map_err(|_| NodeError::RuntimeConfig)?;
            if strategy
                .copy_leader_capital
                .as_ref()
                .is_some_and(|capital| {
                    capital.asset.as_str() != strategy.symbol.quote()
                        || !capital.value.is_sign_positive()
                        || capital.value.is_zero()
                })
            {
                return Err(NodeError::RuntimeConfig);
            }
            match (strategy.strategy_kind, &strategy.grid, &strategy.scalping) {
                // Binance and Gate each install Grid only from complete signed inventory plus a
                // bounded fresh BBO, then route authenticated fills through the same Runtime
                // facts journal and account lane. Bitget remains rejected until it has that
                // exact bridge; accepting it early would create an actor with no safe roll path.
                (StrategyKind::HedgedGrid, Some(grid), None)
                    if matches!(self.venue, VenueId::Binance | VenueId::Gate)
                        && grid.params.validate().is_ok() =>
                {
                    self.grid_initial_state(strategy)?;
                }
                (StrategyKind::Scalping, None, Some(_))
                    if self.scalping_binding_for(strategy).is_ok_and(|binding| {
                        ScalpingParams::for_binding(&binding)
                            .validate_for(&binding)
                            .is_ok()
                    }) => {}
                (StrategyKind::HedgedGrid, _, _)
                | (StrategyKind::Scalping, _, _)
                | (_, Some(_), _)
                | (_, _, Some(_)) => {
                    return Err(NodeError::RuntimeConfig);
                }
                _ => {}
            }
        }
        AccountSymbolSet::new(binding, symbols).map_err(|_| NodeError::RuntimeConfig)?;
        Ok(())
    }

    /// The exact finite symbol scope consumed by the single account Host.  Runtime validation
    /// already rejected duplicates; the launch anchor must remain one of these symbols.
    pub fn configured_symbols(
        &self,
        binding: &GatewayBinding,
    ) -> Result<AccountSymbolSet, NodeError> {
        AccountSymbolSet::new(
            binding,
            self.strategies
                .iter()
                .map(|strategy| strategy.symbol.clone()),
        )
        .map_err(|_| NodeError::RuntimeConfig)
    }

    #[must_use]
    pub fn has_scalping_strategy(&self) -> bool {
        self.strategies
            .iter()
            .any(|strategy| strategy.strategy_kind == StrategyKind::Scalping)
    }

    /// The only initial Grid state derives identity from the current account binding and the
    /// explicit fixed params. The active checkpoint lives below this account's artifacts root;
    /// it is not configurable and cannot be borrowed from another account or guessed from BBO.
    pub fn grid_initial_state(
        &self,
        strategy: &NodeRuntimeStrategy,
    ) -> Result<HedgedGridState, NodeError> {
        let grid = strategy.grid.as_ref().ok_or(NodeError::RuntimeConfig)?;
        if strategy.strategy_kind != StrategyKind::HedgedGrid || grid.params.validate().is_err() {
            return Err(NodeError::RuntimeConfig);
        }
        let binding = HedgedGridBinding {
            strategy_instance_id: strategy.instance_id.clone(),
            run_id: strategy.run_id.clone(),
            exchange: self.venue.as_str().to_owned(),
            account: self.trading_account_id.clone(),
            symbol: strategy.symbol.clone(),
            config_version: strategy.config_digest.clone(),
            owner_scope: strategy.instance_id.clone(),
        };
        HedgedGridState::new_with_params(binding, grid.params.clone())
            .map_err(|_| NodeError::RuntimeConfig)
    }

    /// Fixed, account-local Actor Applied checkpoint location consumed by the Grid bridge.
    #[must_use]
    pub fn grid_checkpoint_path(
        &self,
        artifacts_root: &Path,
        strategy: &NodeRuntimeStrategy,
    ) -> Option<PathBuf> {
        (strategy.strategy_kind == StrategyKind::HedgedGrid && strategy.grid.is_some()).then(|| {
            artifacts_root
                .join("strategies")
                .join(&strategy.instance_id)
                .join("actor-applied.json")
        })
    }

    pub fn binding_for(
        &self,
        strategy: &NodeRuntimeStrategy,
    ) -> Result<StrategyBinding, NodeError> {
        let account = AccountKey::new(self.venue, self.trading_account_id.clone())
            .map_err(|_| NodeError::RuntimeConfig)?;
        let key = StrategyInstanceKey::new(
            account,
            strategy.strategy_kind,
            strategy.instance_id.clone(),
            strategy.symbol.clone(),
        )
        .map_err(|_| NodeError::RuntimeConfig)?;
        StrategyBinding::new(key, strategy.run_id.clone(), strategy.config_digest.clone())
            .map_err(|_| NodeError::RuntimeConfig)
    }

    /// Maps only explicit Node configuration into the pure Scalping binding. This binding has no
    /// Runtime token or execution authority; it makes the feature profile and checkpoint release
    /// identity reproducible on restart.
    pub fn scalping_binding_for(
        &self,
        strategy: &NodeRuntimeStrategy,
    ) -> Result<ScalpingStrategyBinding, NodeError> {
        let scalping = strategy.scalping.as_ref().ok_or(NodeError::RuntimeConfig)?;
        if strategy.strategy_kind != StrategyKind::Scalping
            || scalping.parameter_release_id.trim().is_empty()
            || scalping.owner_scope.trim().is_empty()
            || scalping.risk_budget.asset.as_str() != strategy.symbol.quote()
            || !scalping.risk_budget.value.is_sign_positive()
            || scalping.risk_budget.value.is_zero()
        {
            return Err(NodeError::RuntimeConfig);
        }
        let binding = ScalpingStrategyBinding {
            strategy_kind: ScalpingStrategyKind::Scalping,
            strategy_instance_id: strategy.instance_id.clone(),
            run_id: strategy.run_id.clone(),
            exchange: self.venue.as_str().to_owned(),
            account: self.trading_account_id.clone(),
            symbol: strategy.symbol.clone(),
            parameter_release_id: scalping.parameter_release_id.clone(),
            owner_scope: scalping.owner_scope.clone(),
            risk_budget: scalping.risk_budget.clone(),
        };
        binding.validate().map_err(|_| NodeError::RuntimeConfig)?;
        Ok(binding)
    }

    /// Returns only the explicit strategy allocation which passed the exact quote validation at
    /// load time.  It is observation configuration, never a claim on account equity.
    #[must_use]
    pub fn copy_leader_capital(&self, strategy: &NodeRuntimeStrategy) -> Option<Amount> {
        strategy.copy_leader_capital.clone()
    }
}

impl NodeControlLoopConfig {
    fn validate(&self) -> Result<(), NodeError> {
        if !(10..=60_000).contains(&self.poll_interval_ms)
            || !(self.poll_interval_ms..=60_000).contains(&self.projection_interval_ms)
            || !(1..=crate::MAX_CONTROL_LEASE_DURATION_MS).contains(&self.lease_duration_ms)
            || !(1..=crate::MAX_CONTROL_CLAIM_LIMIT).contains(&self.claim_limit)
        {
            return Err(NodeError::RuntimeConfig);
        }
        crate::ControlHttpClient::new(crate::ControlHttpClientConfig::local(
            self.loopback_origin.clone(),
        ))
        .map(|_| ())
        .map_err(|_| NodeError::RuntimeConfig)
    }
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::Symbol;

    use super::*;

    #[test]
    fn accepts_a_valid_live_configuration_and_rejects_each_scope_violation()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDT".parse::<Symbol>()?,
        )?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("node-runtime.json");
        let fixture = r#"{
          "version":1,"mode":"LIVE","venue":"bybit",
          "trading_account_id":"00000000-0000-4000-8000-000000000001","node_id":"node-a",
          "control":{"loopback_origin":"http://127.0.0.1:8080/","poll_interval_ms":100,"projection_interval_ms":100,"lease_duration_ms":1000,"claim_limit":1},
          "strategies":[
            {"strategy_kind":"scalping","instance_id":"a","run_id":"run-a","config_digest":"digest-a","config_epoch":1,"symbol":"DOGE/USDT","scalping":{"parameter_release_id":"scalping-shadow-v1","owner_scope":"a","risk_budget":{"asset":"USDT","value":"10"}}}
          ]
        }"#;
        std::fs::write(&path, fixture)?;
        assert!(NodeRuntimeConfig::load(&path, &binding).is_ok());
        let two_symbols = fixture.replace(
            r#"{"strategy_kind":"scalping","instance_id":"a","run_id":"run-a","config_digest":"digest-a","config_epoch":1,"symbol":"DOGE/USDT","scalping":{"parameter_release_id":"scalping-shadow-v1","owner_scope":"a","risk_budget":{"asset":"USDT","value":"10"}}}"#,
            r#"{"strategy_kind":"scalping","instance_id":"a","run_id":"run-a","config_digest":"digest-a","config_epoch":1,"symbol":"DOGE/USDT","scalping":{"parameter_release_id":"scalping-shadow-v1","owner_scope":"a","risk_budget":{"asset":"USDT","value":"10"}}},{"strategy_kind":"copy","instance_id":"b","run_id":"run-b","config_digest":"digest-b","config_epoch":1,"symbol":"BTC/USDT"}"#,
        );
        std::fs::write(&path, two_symbols)?;
        let configured = NodeRuntimeConfig::load(&path, &binding)?;
        assert_eq!(configured.configured_symbols(&binding)?.iter().count(), 2);
        for invalid in [
            fixture.replace("\"mode\":\"LIVE\"", "\"mode\":\"live\""),
            fixture.replace("\"node-a\"", "\"\""),
            fixture.replace("\"config_epoch\":1", "\"config_epoch\":2"),
            fixture.replace("\"DOGE/USDT\"", "\"BTC/USDT\""),
            fixture.replace("\"http://127.0.0.1:8080/\"", "\"https://127.0.0.1:8080/\""),
        ] {
            std::fs::write(&path, invalid)?;
            assert!(matches!(
                NodeRuntimeConfig::load(&path, &binding),
                Err(NodeError::RuntimeConfig)
            ));
        }
        let duplicate = r#"{
          "version":1,"mode":"LIVE","venue":"bybit",
          "trading_account_id":"00000000-0000-4000-8000-000000000001","node_id":"node-a",
          "control":{"loopback_origin":"http://127.0.0.1:8080/","poll_interval_ms":100,"projection_interval_ms":100,"lease_duration_ms":1000,"claim_limit":1},
          "strategies":[
            {"strategy_kind":"scalping","instance_id":"a","run_id":"run-a","config_digest":"digest-a","config_epoch":1,"symbol":"DOGE/USDT","scalping":{"parameter_release_id":"scalping-shadow-v1","owner_scope":"a","risk_budget":{"asset":"USDT","value":"10"}}},
            {"strategy_kind":"copy","instance_id":"b","run_id":"run-b","config_digest":"digest-b","config_epoch":1,"symbol":"DOGE/USDT"}
          ]
        }"#;
        std::fs::write(&path, duplicate)?;
        assert!(matches!(
            NodeRuntimeConfig::load(&path, &binding),
            Err(NodeError::RuntimeConfig)
        ));
        let missing_scalping = r#"{
          "version":1,"mode":"LIVE","venue":"bybit",
          "trading_account_id":"00000000-0000-4000-8000-000000000001","node_id":"node-a",
          "control":{"loopback_origin":"http://127.0.0.1:8080/","poll_interval_ms":100,"projection_interval_ms":100,"lease_duration_ms":1000,"claim_limit":1},
          "strategies":[{"strategy_kind":"scalping","instance_id":"a","run_id":"run-a","config_digest":"digest-a","config_epoch":1,"symbol":"DOGE/USDT"}]
        }"#;
        std::fs::write(&path, missing_scalping)?;
        assert!(matches!(
            NodeRuntimeConfig::load(&path, &binding),
            Err(NodeError::RuntimeConfig)
        ));
        Ok(())
    }

    #[test]
    fn grid_requires_explicit_params_and_a_fixed_account_local_recovery_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDT".parse::<Symbol>()?,
        )?;
        let fixture = r#"{
          "version":1,"mode":"LIVE","venue":"binance",
          "trading_account_id":"00000000-0000-4000-8000-000000000001","node_id":"node-a",
          "control":{"loopback_origin":"http://127.0.0.1:8080/","poll_interval_ms":100,"projection_interval_ms":100,"lease_duration_ms":1000,"claim_limit":1},
          "strategies":[{"strategy_kind":"hedged_grid","instance_id":"grid_a","run_id":"run_a","config_digest":"abc123","config_epoch":1,"symbol":"DOGE/USDT","grid":{"params":{"order_notional":{"asset":"USDT","value":"5"},"spacing_rate":"0.002","grid_count":10,"inventory_replenish_grid_count":3},"recovery":"bootstrap_when_absent"}}]
        }"#;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("node-runtime.json");
        std::fs::write(&path, fixture)?;
        let config = NodeRuntimeConfig::load(&path, &binding)?;
        let strategy = config.strategies.first().ok_or("missing strategy")?;
        assert_eq!(
            config.grid_initial_state(strategy)?.binding.symbol,
            binding.symbol
        );
        assert_eq!(
            config.grid_checkpoint_path(directory.path(), strategy),
            Some(
                directory
                    .path()
                    .join("strategies")
                    .join("grid_a")
                    .join("actor-applied.json")
            )
        );
        std::fs::write(&path, fixture.replace("\"grid\":{", "\"grid_missing\":{"))?;
        assert!(NodeRuntimeConfig::load(&path, &binding).is_err());
        Ok(())
    }
}
