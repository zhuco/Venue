use std::{fs, path::Path};

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    Result,
    domain::{Symbol, is_canonical_trading_account_id},
    error::Error,
    strategy::hedged_grid::{MAX_GRID_COUNT, MIN_GRID_COUNT},
};

pub const DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS: u64 = 5_000;
pub const MIN_PRIVATE_CUSTODY_MAX_STALE_MS: u64 = 1_000;
pub const MAX_PRIVATE_CUSTODY_MAX_STALE_MS: u64 = 60_000;
pub const EXPOSURE_SNAPSHOT_INTERVAL_MS: u64 = 120_000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub log: LogLevel,
    pub trading_account_id: String,
    pub symbol: Symbol,
    #[serde(default)]
    pub binance: Option<BinanceConfig>,
    #[serde(default)]
    pub gate: Option<GateConfig>,
    #[serde(default)]
    pub bitget: Option<BitgetConfig>,
    #[serde(default)]
    pub hedged_grid: Option<HedgedGridConfig>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HedgedGridConfig {
    pub grid_count: u8,
    #[serde(default)]
    pub exposure_take_profit: Option<ExposureTakeProfitConfig>,
}

/// Versioned hedged-grid exposure release. The numeric values are deliberately identical across
/// venues; only `enabled` and `shadow` are deployment rollout switches.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExposureTakeProfitConfig {
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub shadow: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub position_equity_multiple: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl_equity_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub reduce_ratio: Decimal,
    pub snapshot_interval_ms: u64,
    pub max_snapshot_age_ms: u64,
    pub rearm_clear_generations: u8,
}

impl ExposureTakeProfitConfig {
    pub fn validate(&self) -> Result<()> {
        if self.position_equity_multiple != Decimal::new(3, 0)
            || self.unrealized_pnl_equity_ratio != Decimal::new(5, 2)
            || self.reduce_ratio != Decimal::new(30, 2)
            || self.snapshot_interval_ms != EXPOSURE_SNAPSHOT_INTERVAL_MS
            || self.max_snapshot_age_ms != 3_000
            || self.rearm_clear_generations != 2
        {
            return Err(Error::HedgedGridExposureRelease);
        }
        Ok(())
    }
}

/// The private API family is a deployment binding, never a capability probe.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BinanceConfig {
    pub account_binding: BinanceAccountBinding,
    #[serde(default = "default_private_custody_max_stale_ms")]
    pub private_custody_max_stale_ms: u64,
}

/// Gate.io USDT perpetual deployment binding. The runtime verifies the configured account is
/// already in the declared hedge mode; it never changes a venue account mode itself.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GateConfig {
    pub account_binding: GateAccountBinding,
    #[serde(default = "default_private_custody_max_stale_ms")]
    pub private_custody_max_stale_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GateAccountBinding {
    UsdtFuturesDual,
}

/// Bitget UTA USDT perpetual deployment binding. The runtime reads and verifies hedge mode before
/// any mutation, rather than attempting to change it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BitgetConfig {
    pub account_binding: BitgetAccountBinding,
    #[serde(default = "default_private_custody_max_stale_ms")]
    pub private_custody_max_stale_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BitgetAccountBinding {
    UtaUsdtFuturesHedge,
}

const fn default_private_custody_max_stale_ms() -> u64 {
    DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS
}

const fn default_true() -> bool {
    true
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BinanceAccountBinding {
    PortfolioMarginUm,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;

        let config: Self = toml::from_str(&text).map_err(|source| Error::Config {
            path: path.to_path_buf(),
            source,
        })?;
        let configured_exchanges = usize::from(config.binance.is_some())
            + usize::from(config.gate.is_some())
            + usize::from(config.bitget.is_some());
        if configured_exchanges != 1 {
            return Err(Error::ExchangeConfiguration);
        }
        if !is_canonical_trading_account_id(&config.trading_account_id) {
            return Err(Error::TradingAccountId);
        }
        for private_custody_max_stale_ms in [
            config
                .binance
                .as_ref()
                .map(|binding| binding.private_custody_max_stale_ms),
            config
                .gate
                .as_ref()
                .map(|binding| binding.private_custody_max_stale_ms),
            config
                .bitget
                .as_ref()
                .map(|binding| binding.private_custody_max_stale_ms),
        ]
        .into_iter()
        .flatten()
        {
            if !(MIN_PRIVATE_CUSTODY_MAX_STALE_MS..=MAX_PRIVATE_CUSTODY_MAX_STALE_MS)
                .contains(&private_custody_max_stale_ms)
            {
                return Err(Error::PrivateCustodyFreshness {
                    value: private_custody_max_stale_ms,
                    min: MIN_PRIVATE_CUSTODY_MAX_STALE_MS,
                    max: MAX_PRIVATE_CUSTODY_MAX_STALE_MS,
                });
            }
        }
        if let Some(hedged_grid) = config.hedged_grid
            && !(MIN_GRID_COUNT..=MAX_GRID_COUNT).contains(&hedged_grid.grid_count)
        {
            return Err(Error::HedgedGridCount {
                value: hedged_grid.grid_count,
                min: MIN_GRID_COUNT,
                max: MAX_GRID_COUNT,
            });
        }
        if let Some(exposure_take_profit) = config
            .hedged_grid
            .and_then(|hedged_grid| hedged_grid.exposure_take_profit)
        {
            exposure_take_profit.validate()?;
        }
        Ok(config)
    }

    pub fn binance_config(&self) -> Result<&BinanceConfig> {
        self.binance.as_ref().ok_or(Error::ExchangeConfiguration)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

    #[test]
    fn reads_minimal_config() -> std::result::Result<(), toml::de::Error> {
        let cfg: Config = toml::from_str(
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'btc/usdt'\n[binance]\naccount_binding = 'portfolio_margin_um'",
        )?;

        assert_eq!(cfg.log, LogLevel::Info);
        assert_eq!(cfg.symbol.to_string(), "BTC/USDT");
        assert_eq!(cfg.hedged_grid, None);
        assert!(cfg.binance.is_some());
        assert_eq!(
            cfg.binance
                .as_ref()
                .map(|binding| binding.private_custody_max_stale_ms),
            Some(DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS)
        );
        Ok(())
    }

    #[test]
    fn reads_hedged_grid_count_from_config() -> std::result::Result<(), toml::de::Error> {
        let cfg: Config = toml::from_str(
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDC'\n[binance]\naccount_binding = 'portfolio_margin_um'\n[hedged_grid]\ngrid_count = 3",
        )?;

        assert_eq!(
            cfg.hedged_grid,
            Some(HedgedGridConfig {
                grid_count: 3,
                exposure_take_profit: None,
            })
        );
        Ok(())
    }

    #[test]
    fn rejects_hedged_grid_count_outside_the_strategy_range()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("venue.toml");
        fs::write(
            &path,
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDC'\n[binance]\naccount_binding = 'portfolio_margin_um'\n[hedged_grid]\ngrid_count = 0",
        )?;
        assert!(matches!(
            Config::load(path),
            Err(Error::HedgedGridCount {
                value: 0,
                min: MIN_GRID_COUNT,
                max: MAX_GRID_COUNT,
            })
        ));
        Ok(())
    }

    #[test]
    fn reads_gate_stage7_grid_deployment() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("gate.toml");
        fs::write(
            &path,
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDT'\n[gate]\naccount_binding = 'usdt_futures_dual'\n[hedged_grid]\ngrid_count = 3",
        )?;

        let config = Config::load(path)?;

        assert!(config.binance.is_none());
        assert_eq!(
            config.gate,
            Some(GateConfig {
                account_binding: GateAccountBinding::UsdtFuturesDual,
                private_custody_max_stale_ms: DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
            })
        );
        assert_eq!(
            config.hedged_grid,
            Some(HedgedGridConfig {
                grid_count: 3,
                exposure_take_profit: None,
            })
        );
        Ok(())
    }

    #[test]
    fn reads_bitget_stage7_grid_deployment() -> std::result::Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bitget.toml");
        fs::write(
            &path,
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDT'\n[bitget]\naccount_binding = 'uta_usdt_futures_hedge'\n[hedged_grid]\ngrid_count = 3",
        )?;

        let config = Config::load(path)?;

        assert!(config.binance.is_none());
        assert_eq!(
            config.bitget,
            Some(BitgetConfig {
                account_binding: BitgetAccountBinding::UtaUsdtFuturesHedge,
                private_custody_max_stale_ms: DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
            })
        );
        Ok(())
    }

    #[test]
    fn rejects_missing_or_ambiguous_exchange_deployment()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let missing = directory.path().join("missing.toml");
        fs::write(
            &missing,
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDT'",
        )?;
        assert!(matches!(
            Config::load(missing),
            Err(Error::ExchangeConfiguration)
        ));

        let ambiguous = directory.path().join("ambiguous.toml");
        fs::write(
            &ambiguous,
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDT'\n[gate]\naccount_binding = 'usdt_futures_dual'\n[bitget]\naccount_binding = 'uta_usdt_futures_hedge'",
        )?;
        assert!(matches!(
            Config::load(ambiguous),
            Err(Error::ExchangeConfiguration)
        ));
        Ok(())
    }

    #[test]
    fn rejects_private_custody_freshness_outside_the_deployment_range()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("venue.toml");
        fs::write(
            &path,
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'BTC/USDT'\n[binance]\naccount_binding = 'portfolio_margin_um'\nprivate_custody_max_stale_ms = 999",
        )?;
        assert!(matches!(
            Config::load(path),
            Err(Error::PrivateCustodyFreshness { .. })
        ));
        Ok(())
    }

    #[test]
    fn rejects_unknown_key() {
        let cfg = toml::from_str::<Config>(
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'BTC/USDT'\n[binance]\naccount_binding = 'portfolio_margin_um'\nextra = true",
        );

        assert!(cfg.is_err());
    }

    #[test]
    fn reads_fixed_exposure_take_profit_release()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("venue.toml");
        fs::write(
            &path,
            "trading_account_id='00000000-0000-4000-8000-000000000001'\nsymbol='SOL/USDC'\n[binance]\naccount_binding='portfolio_margin_um'\n[hedged_grid]\ngrid_count=10\n[hedged_grid.exposure_take_profit]\nenabled=true\nshadow=true\nposition_equity_multiple='3'\nunrealized_pnl_equity_ratio='0.05'\nreduce_ratio='0.30'\nsnapshot_interval_ms=120000\nmax_snapshot_age_ms=3000\nrearm_clear_generations=2",
        )?;

        let config = Config::load(path)?;
        let exposure = config
            .hedged_grid
            .and_then(|grid| grid.exposure_take_profit)
            .ok_or("missing exposure config")?;
        assert!(exposure.enabled);
        assert!(exposure.shadow);
        assert_eq!(exposure.reduce_ratio, Decimal::new(30, 2));
        assert_eq!(exposure.snapshot_interval_ms, EXPOSURE_SNAPSHOT_INTERVAL_MS);
        Ok(())
    }

    #[test]
    fn rejects_exposure_parameter_drift_between_deployments()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("venue.toml");
        fs::write(
            &path,
            "trading_account_id='00000000-0000-4000-8000-000000000001'\nsymbol='SOL/USDC'\n[binance]\naccount_binding='portfolio_margin_um'\n[hedged_grid]\ngrid_count=10\n[hedged_grid.exposure_take_profit]\nenabled=true\nposition_equity_multiple='4'\nunrealized_pnl_equity_ratio='0.05'\nreduce_ratio='0.30'\nsnapshot_interval_ms=120000\nmax_snapshot_age_ms=3000\nrearm_clear_generations=2",
        )?;

        assert!(matches!(
            Config::load(path),
            Err(Error::HedgedGridExposureRelease)
        ));
        Ok(())
    }

    #[test]
    fn rejects_legacy_two_second_exposure_snapshot_interval()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("venue.toml");
        fs::write(
            &path,
            "trading_account_id='00000000-0000-4000-8000-000000000001'\nsymbol='SOL/USDC'\n[binance]\naccount_binding='portfolio_margin_um'\n[hedged_grid]\ngrid_count=10\n[hedged_grid.exposure_take_profit]\nenabled=true\nposition_equity_multiple='3'\nunrealized_pnl_equity_ratio='0.05'\nreduce_ratio='0.30'\nsnapshot_interval_ms=2000\nmax_snapshot_age_ms=3000\nrearm_clear_generations=2",
        )?;

        assert!(matches!(
            Config::load(path),
            Err(Error::HedgedGridExposureRelease)
        ));
        Ok(())
    }

    #[test]
    fn rejects_missing_or_noncanonical_trading_account_id()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        for (name, body) in [
            (
                "missing.toml",
                "symbol='SOL/USDT'\n[binance]\naccount_binding='portfolio_margin_um'",
            ),
            (
                "invalid.toml",
                "trading_account_id='portfolio_margin_um'\nsymbol='SOL/USDT'\n[binance]\naccount_binding='portfolio_margin_um'",
            ),
        ] {
            let path = directory.path().join(name);
            fs::write(&path, body)?;
            assert!(Config::load(path).is_err());
        }
        assert!(is_canonical_trading_account_id(TEST_ACCOUNT_ID));
        Ok(())
    }
}
