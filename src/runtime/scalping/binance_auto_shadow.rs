use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    config::{BinanceAccountBinding, Config, LogLevel},
    controller::ControlTarget,
    domain::{Amount, Asset},
    storage::{ProjectionStore, StorageError},
    strategy::scalping::{PHASE8_ATR14_PARAMETER_RELEASE_ID, StrategyBinding, StrategyKind},
};

use super::{
    BinanceMarketScanError, ScalpingControlError, ScalpingControlRequest, apply_scalping_control,
    scan_binance_usdt_perpetuals,
};

const AUTHORITY_TTL_MS: u64 = 14 * 60 * 1_000;
const AUTHORITY_RENEW_MS: u64 = 7 * 60 * 1_000;
const MANIFEST_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceAutoShadowRequest {
    pub artifacts_root: PathBuf,
    pub initial_fill_recovery_from_ms: u64,
    pub max_turns: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceAutoShadowReport {
    pub scan_sequence: u64,
    pub symbols: Vec<String>,
    pub run_root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceAutoLiveRequest {
    pub artifacts_root: PathBuf,
    pub initial_fill_recovery_from_ms: u64,
    pub max_turns: Option<u64>,
    pub max_live_symbols: usize,
    pub confirm_mainnet_strategy_mutations: bool,
}

pub type BinanceAutoLiveReport = BinanceAutoShadowReport;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutoResidentMode {
    Shadow,
    Live,
}

impl AutoResidentMode {
    const fn run_directory(self) -> &'static str {
        match self {
            Self::Shadow => "shadow-runs",
            Self::Live => "live-runs",
        }
    }

    const fn command(self) -> &'static str {
        match self {
            Self::Shadow => "shadow-resident",
            Self::Live => "live-resident",
        }
    }

    const fn control_prefix(self) -> &'static str {
        match self {
            Self::Shadow => "auto-shadow",
            Self::Live => "auto-live",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AutoShadowManifest {
    schema_version: u16,
    scan_sequence: u64,
    selection_sha256: String,
    bindings: Vec<StrategyBinding>,
}

struct ChildResident {
    child: Child,
    config: Config,
    artifacts_root: PathBuf,
    binding_path: PathBuf,
    index: usize,
}

/// Runs the selected set through the real public/private resident composition without exposing a
/// mutation-capable CLI. Each child has an isolated binding, controller and artifact root.
pub fn run_binance_auto_shadow(
    config: &Config,
    request: BinanceAutoShadowRequest,
) -> Result<BinanceAutoShadowReport, BinanceAutoShadowError> {
    run_binance_auto_residents(
        config,
        request.artifacts_root,
        request.initial_fill_recovery_from_ms,
        request.max_turns,
        3,
        AutoResidentMode::Shadow,
    )
}

/// Scans first, seals at most three exact phase-8 bindings, then starts only the selected Live
/// residents. The explicit confirmation is consumed here and repeated on every mutation-capable
/// child; selection and the supervisor itself never own an exchange writer.
pub fn run_binance_auto_live(
    config: &Config,
    request: BinanceAutoLiveRequest,
) -> Result<BinanceAutoLiveReport, BinanceAutoShadowError> {
    if !request.confirm_mainnet_strategy_mutations {
        return Err(BinanceAutoShadowError::Confirmation);
    }
    if !(1..=3).contains(&request.max_live_symbols) {
        return Err(BinanceAutoShadowError::Selection);
    }
    run_binance_auto_residents(
        config,
        request.artifacts_root,
        request.initial_fill_recovery_from_ms,
        request.max_turns,
        request.max_live_symbols,
        AutoResidentMode::Live,
    )
}

fn run_binance_auto_residents(
    config: &Config,
    artifacts_root: PathBuf,
    initial_fill_recovery_from_ms: u64,
    max_turns: Option<u64>,
    max_symbols: usize,
    mode: AutoResidentMode,
) -> Result<BinanceAutoShadowReport, BinanceAutoShadowError> {
    if !artifacts_root.is_absolute() || initial_fill_recovery_from_ms == 0 {
        return Err(BinanceAutoShadowError::Request);
    }
    fs::create_dir_all(&artifacts_root).map_err(|source| BinanceAutoShadowError::Io {
        path: artifacts_root.clone(),
        source,
    })?;
    let _live_lock = if mode == AutoResidentMode::Live {
        Some(acquire_auto_live_lock(&artifacts_root)?)
    } else {
        None
    };
    let scan = scan_binance_usdt_perpetuals(&artifacts_root)?;
    if scan.record.selection.selected.is_empty() || scan.record.selection.selected.len() > 3 {
        return Err(BinanceAutoShadowError::Selection);
    }
    let run_root = artifacts_root
        .join(mode.run_directory())
        .join(format!("scan-{}", scan.record.scan_sequence));
    fs::create_dir_all(&run_root).map_err(|source| BinanceAutoShadowError::Io {
        path: run_root.clone(),
        source,
    })?;
    let quote: Asset = "USDT"
        .parse()
        .map_err(|_| BinanceAutoShadowError::Selection)?;
    let bindings = scan
        .record
        .selection
        .selected
        .iter()
        .take(max_symbols)
        .map(|selected| {
            let slug = symbol_slug(&selected.sample.symbol.to_string());
            StrategyBinding {
                strategy_kind: StrategyKind::Scalping,
                strategy_instance_id: format!("binance_auto_{slug}"),
                run_id: format!("binance_auto_scan_{}_{slug}", scan.record.scan_sequence),
                exchange: "binance".to_owned(),
                account: config.trading_account_id.clone(),
                symbol: selected.sample.symbol.clone(),
                parameter_release_id: PHASE8_ATR14_PARAMETER_RELEASE_ID.to_owned(),
                owner_scope: format!("scalping:binance_auto_{slug}"),
                risk_budget: Amount::new(quote.clone(), Decimal::new(5, 0)),
            }
        })
        .collect::<Vec<_>>();
    if bindings.iter().any(|binding| binding.validate().is_err()) {
        return Err(BinanceAutoShadowError::Selection);
    }
    let manifest = AutoShadowManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        scan_sequence: scan.record.scan_sequence,
        selection_sha256: scan.record.content_sha256.clone(),
        bindings: bindings.clone(),
    };
    let manifest_store = ProjectionStore::new(run_root.join("manifest.json"));
    match manifest_store.load::<AutoShadowManifest>()? {
        Some(existing) if existing == manifest => {}
        Some(_) => return Err(BinanceAutoShadowError::Manifest),
        None => manifest_store.save(&manifest)?,
    }

    let executable = std::env::current_exe().map_err(|source| BinanceAutoShadowError::Io {
        path: PathBuf::from("current_exe"),
        source,
    })?;
    let mut residents = Vec::with_capacity(bindings.len());
    for (index, binding) in bindings.iter().enumerate() {
        let symbol_root = run_root.join(symbol_slug(&binding.symbol.to_string()));
        fs::create_dir_all(&symbol_root).map_err(|source| BinanceAutoShadowError::Io {
            path: symbol_root.clone(),
            source,
        })?;
        let binding_path = symbol_root.join("binding.json");
        let binding_store = ProjectionStore::new(&binding_path);
        match binding_store.load::<StrategyBinding>()? {
            Some(existing) if existing == *binding => {}
            Some(_) => return Err(BinanceAutoShadowError::Manifest),
            None => binding_store.save(binding)?,
        }
        let mut child_config = config.clone();
        child_config.symbol = binding.symbol.clone();
        let config_path = symbol_root.join("venue.toml");
        write_config(&config_path, &child_config)?;
        if let Err(error) = renew_authority(
            &child_config,
            &symbol_root,
            &binding_path,
            index,
            wall_clock_ms()?,
            mode,
        ) {
            stop_residents(&residents, mode, wall_clock_ms()?)?;
            return Err(error);
        }
        let mut command = Command::new(&executable);
        command
            .arg("--config")
            .arg(&config_path)
            .arg(mode.command())
            .arg("--artifacts-root")
            .arg(&symbol_root)
            .arg("--binding")
            .arg(&binding_path)
            .arg("--initial-fill-recovery-from-ms")
            .arg(initial_fill_recovery_from_ms.to_string());
        if mode == AutoResidentMode::Live {
            command.arg("--confirm-mainnet-strategy-mutations");
        }
        if let Some(max_turns) = max_turns {
            command.arg("--max-turns").arg(max_turns.to_string());
        }
        let child = match command.spawn() {
            Ok(child) => child,
            Err(source) => {
                stop_residents(&residents, mode, wall_clock_ms()?)?;
                return Err(BinanceAutoShadowError::Io {
                    path: executable.clone(),
                    source,
                });
            }
        };
        residents.push(ChildResident {
            child,
            config: child_config,
            artifacts_root: symbol_root,
            binding_path,
            index,
        });
    }

    let mut next_renew_ms = wall_clock_ms()?.saturating_add(AUTHORITY_RENEW_MS);
    while !residents.is_empty() {
        let mut index = 0;
        while index < residents.len() {
            match residents[index].child.try_wait() {
                Ok(Some(status)) if status.success() => {
                    residents.swap_remove(index);
                }
                Ok(Some(_)) => {
                    stop_residents(&residents, mode, wall_clock_ms()?)?;
                    return Err(BinanceAutoShadowError::Child);
                }
                Ok(None) => index += 1,
                Err(source) => {
                    stop_residents(&residents, mode, wall_clock_ms()?)?;
                    return Err(BinanceAutoShadowError::Io {
                        path: executable.clone(),
                        source,
                    });
                }
            }
        }
        if residents.is_empty() {
            break;
        }
        let now_ms = wall_clock_ms()?;
        if now_ms >= next_renew_ms {
            for resident in &residents {
                if let Err(error) = renew_authority(
                    &resident.config,
                    &resident.artifacts_root,
                    &resident.binding_path,
                    resident.index,
                    now_ms,
                    mode,
                ) {
                    stop_residents(&residents, mode, now_ms)?;
                    return Err(error);
                }
            }
            next_renew_ms = now_ms.saturating_add(AUTHORITY_RENEW_MS);
        }
        thread::sleep(Duration::from_millis(100));
    }

    Ok(BinanceAutoShadowReport {
        scan_sequence: scan.record.scan_sequence,
        symbols: bindings
            .iter()
            .map(|binding| binding.symbol.to_string())
            .collect(),
        run_root,
    })
}

fn acquire_auto_live_lock(artifacts_root: &Path) -> Result<File, BinanceAutoShadowError> {
    let path = artifacts_root.join("auto_live.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| BinanceAutoShadowError::Io {
            path: path.clone(),
            source,
        })?;
    file.try_lock_exclusive()
        .map_err(|_| BinanceAutoShadowError::Busy)?;
    Ok(file)
}

fn stop_residents(
    residents: &[ChildResident],
    mode: AutoResidentMode,
    now_ms: u64,
) -> Result<(), BinanceAutoShadowError> {
    let mut first_error = None;
    for resident in residents {
        let result = apply_scalping_control(
            &resident.config,
            ScalpingControlRequest {
                artifacts_root: resident.artifacts_root.clone(),
                binding_path: resident.binding_path.clone(),
                target: ControlTarget::StopAndProtect,
                command_id: format!("{}-stop-{}-{now_ms}", mode.control_prefix(), resident.index),
                idempotency_key: format!(
                    "{}-stop-{}-{now_ms}",
                    mode.control_prefix(),
                    resident.index
                ),
                entry_expires_at_ms: None,
                confirm_entry_authority: false,
            },
        );
        if first_error.is_none() {
            first_error = result.err();
        }
    }
    first_error.map_or(Ok(()), |error| Err(error.into()))
}

fn renew_authority(
    config: &Config,
    artifacts_root: &Path,
    binding_path: &Path,
    index: usize,
    now_ms: u64,
    mode: AutoResidentMode,
) -> Result<(), BinanceAutoShadowError> {
    let deadline = now_ms
        .checked_add(AUTHORITY_TTL_MS)
        .ok_or(BinanceAutoShadowError::Clock)?;
    apply_scalping_control(
        config,
        ScalpingControlRequest {
            artifacts_root: artifacts_root.to_path_buf(),
            binding_path: binding_path.to_path_buf(),
            target: ControlTarget::Running,
            command_id: format!("{}-{index}-{now_ms}", mode.control_prefix()),
            idempotency_key: format!("{}-renew-{index}-{now_ms}", mode.control_prefix()),
            entry_expires_at_ms: Some(deadline),
            confirm_entry_authority: true,
        },
    )?;
    Ok(())
}

fn write_config(path: &Path, config: &Config) -> Result<(), BinanceAutoShadowError> {
    let log = match config.log {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    };
    let binance = config
        .binance
        .as_ref()
        .ok_or(BinanceAutoShadowError::Config)?;
    let account_binding = match binance.account_binding {
        BinanceAccountBinding::PortfolioMarginUm => "portfolio_margin_um",
    };
    let mut body = format!(
        "log = \"{log}\"\ntrading_account_id = \"{}\"\nsymbol = \"{}\"\n\n[binance]\naccount_binding = \"{account_binding}\"\nprivate_custody_max_stale_ms = {}\n",
        config.trading_account_id, config.symbol, binance.private_custody_max_stale_ms
    );
    if let Some(hedged_grid) = config.hedged_grid {
        body.push_str(&format!(
            "\n[hedged_grid]\ngrid_count = {}\n",
            hedged_grid.grid_count
        ));
    }
    let temporary = path.with_extension("toml.tmp");
    let mut file = File::create(&temporary).map_err(|source| BinanceAutoShadowError::Io {
        path: temporary.clone(),
        source,
    })?;
    file.write_all(body.as_bytes())
        .and_then(|_| file.sync_all())
        .map_err(|source| BinanceAutoShadowError::Io {
            path: temporary.clone(),
            source,
        })?;
    fs::rename(&temporary, path).map_err(|source| BinanceAutoShadowError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn symbol_slug(symbol: &str) -> String {
    symbol
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn wall_clock_ms() -> Result<u64, BinanceAutoShadowError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceAutoShadowError::Clock)
        .and_then(|duration| {
            u64::try_from(duration.as_millis()).map_err(|_| BinanceAutoShadowError::Clock)
        })
}

#[derive(Debug, thiserror::Error)]
pub enum BinanceAutoShadowError {
    #[error("automatic Shadow requires an absolute root and a nonzero fill-recovery floor")]
    Request,
    #[error("automatic Live requires explicit mainnet strategy-mutation confirmation")]
    Confirmation,
    #[error("another automatic Live supervisor already owns this account artifact root")]
    Busy,
    #[error("automatic Shadow selection is empty or exceeds three symbols")]
    Selection,
    #[error("automatic Shadow manifest conflicts with durable state")]
    Manifest,
    #[error("an automatic Shadow child failed closed")]
    Child,
    #[error("automatic Shadow clock is invalid")]
    Clock,
    #[error("automatic Shadow requires the Binance deployment configuration")]
    Config,
    #[error("automatic Shadow I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Scan(#[from] BinanceMarketScanError),
    #[error(transparent)]
    Control(#[from] ScalpingControlError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        AutoResidentMode, BinanceAutoLiveRequest, BinanceAutoShadowError, acquire_auto_live_lock,
        run_binance_auto_live,
    };

    #[test]
    fn live_requires_confirmation_before_scan_or_filesystem_access()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = crate::config::Config {
            log: crate::config::LogLevel::Info,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            binance: Some(crate::config::BinanceConfig {
                account_binding: crate::config::BinanceAccountBinding::PortfolioMarginUm,
                private_custody_max_stale_ms: crate::config::DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
            }),
            gate: None,
            bitget: None,
            hedged_grid: None,
        };
        let error = run_binance_auto_live(
            &config,
            BinanceAutoLiveRequest {
                artifacts_root: PathBuf::from("relative"),
                initial_fill_recovery_from_ms: 0,
                max_turns: None,
                max_live_symbols: 1,
                confirm_mainnet_strategy_mutations: false,
            },
        )
        .err();
        assert!(matches!(error, Some(BinanceAutoShadowError::Confirmation)));
        Ok(())
    }

    #[test]
    fn live_and_shadow_use_separate_run_and_control_identities() {
        assert_eq!(AutoResidentMode::Shadow.run_directory(), "shadow-runs");
        assert_eq!(AutoResidentMode::Live.run_directory(), "live-runs");
        assert_eq!(AutoResidentMode::Shadow.command(), "shadow-resident");
        assert_eq!(AutoResidentMode::Live.command(), "live-resident");
        assert_ne!(
            AutoResidentMode::Shadow.control_prefix(),
            AutoResidentMode::Live.control_prefix()
        );
    }

    #[test]
    fn live_symbol_limit_is_validated_before_scan() -> Result<(), Box<dyn std::error::Error>> {
        let config = crate::config::Config {
            log: crate::config::LogLevel::Info,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            binance: Some(crate::config::BinanceConfig {
                account_binding: crate::config::BinanceAccountBinding::PortfolioMarginUm,
                private_custody_max_stale_ms: crate::config::DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
            }),
            gate: None,
            bitget: None,
            hedged_grid: None,
        };
        assert!(matches!(
            run_binance_auto_live(
                &config,
                BinanceAutoLiveRequest {
                    artifacts_root: PathBuf::from("relative"),
                    initial_fill_recovery_from_ms: 0,
                    max_turns: None,
                    max_live_symbols: 4,
                    confirm_mainnet_strategy_mutations: true,
                }
            ),
            Err(BinanceAutoShadowError::Selection)
        ));
        Ok(())
    }

    #[test]
    fn one_artifact_root_allows_only_one_live_supervisor() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let first = acquire_auto_live_lock(directory.path())?;
        assert!(matches!(
            acquire_auto_live_lock(directory.path()),
            Err(BinanceAutoShadowError::Busy)
        ));
        drop(first);
        assert!(acquire_auto_live_lock(directory.path()).is_ok());
        Ok(())
    }
}
