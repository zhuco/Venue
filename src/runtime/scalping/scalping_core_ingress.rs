use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

use fs2::FileExt;

use crate::{
    config::{BinanceAccountBinding, Config},
    strategy::scalping::StrategyBinding,
};

use super::{
    ScalpingCoreQuoteReceipt, ScalpingCoreQuoteReceiptError, ScalpingCoreQuoteReceiptJournal,
    ScalpingOwnerRiskInboxError, ScalpingOwnerRiskInboxJournal, ScalpingOwnerRiskPage,
};

pub const SCALPING_CORE_OWNER_RISK_PAGES_FILE: &str = "owner_risk_pages.jsonl";
pub const SCALPING_CORE_QUOTE_RECEIPTS_FILE: &str = "core_quote_receipts.jsonl";
const SCALPING_CORE_INGRESS_LOCK_FILE: &str = "scalping_core_ingress.lock";

#[derive(Debug)]
struct CoreIngressLock {
    file: File,
}

impl Drop for CoreIngressLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// Explicit file-only input for one already-valued Core owner-risk page. This ingress owns only
/// the durable envelope; it never reads private facts, derives account risk, or contacts a venue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingCoreOwnerRiskCommitRequest {
    pub artifacts_root: PathBuf,
    pub binding_path: PathBuf,
    pub page_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingCoreOwnerRiskCommitReport {
    pub sequence: u64,
    pub cursor_id: String,
    pub inbox_path: PathBuf,
}

/// Explicit file-only input for one complete Core quote receipt. Quote valuation and authority
/// remain external; this boundary validates and fsyncs the exact supplied receipt only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingCoreQuoteCommitRequest {
    pub artifacts_root: PathBuf,
    pub binding_path: PathBuf,
    pub receipt_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingCoreQuoteCommitReport {
    pub sequence: u64,
    pub quote_id: String,
    pub receipt_path: PathBuf,
}

pub fn commit_scalping_core_owner_risk_page(
    config: &Config,
    request: ScalpingCoreOwnerRiskCommitRequest,
) -> Result<ScalpingCoreOwnerRiskCommitReport, ScalpingCoreIngressError> {
    validate_paths(
        &request.artifacts_root,
        &request.binding_path,
        &request.page_path,
    )?;
    let binding = load_binding(&request.binding_path)?;
    validate_binding(config, &binding)?;
    let page = load_page(&request.page_path)?;
    fs::create_dir_all(&request.artifacts_root).map_err(|source| ScalpingCoreIngressError::Io {
        path: request.artifacts_root.clone(),
        source,
    })?;
    let _lock = acquire_lock(&request.artifacts_root)?;
    let inbox_path = request
        .artifacts_root
        .join(SCALPING_CORE_OWNER_RISK_PAGES_FILE);
    let mut journal = ScalpingOwnerRiskInboxJournal::open(
        &inbox_path,
        binding,
        page.cursor.binding.risk_unit.clone(),
    )?;
    let record = journal.append(page)?;
    Ok(ScalpingCoreOwnerRiskCommitReport {
        sequence: record.sequence,
        cursor_id: record.page.cursor.cursor_id,
        inbox_path,
    })
}

pub fn commit_scalping_core_quote_receipt(
    config: &Config,
    request: ScalpingCoreQuoteCommitRequest,
) -> Result<ScalpingCoreQuoteCommitReport, ScalpingCoreIngressError> {
    validate_paths(
        &request.artifacts_root,
        &request.binding_path,
        &request.receipt_path,
    )?;
    let binding = load_binding(&request.binding_path)?;
    validate_binding(config, &binding)?;
    let receipt = load_quote_receipt(&request.receipt_path)?;
    if receipt.binding != binding {
        return Err(ScalpingCoreIngressError::Binding);
    }
    fs::create_dir_all(&request.artifacts_root).map_err(|source| ScalpingCoreIngressError::Io {
        path: request.artifacts_root.clone(),
        source,
    })?;
    let _lock = acquire_lock(&request.artifacts_root)?;
    let receipt_path = request
        .artifacts_root
        .join(SCALPING_CORE_QUOTE_RECEIPTS_FILE);
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&receipt_path, binding)?;
    let record = journal.append(receipt)?;
    Ok(ScalpingCoreQuoteCommitReport {
        sequence: record.sequence,
        quote_id: record.receipt.quote.quote_id,
        receipt_path,
    })
}

fn acquire_lock(artifacts_root: &Path) -> Result<CoreIngressLock, ScalpingCoreIngressError> {
    let path = artifacts_root.join(SCALPING_CORE_INGRESS_LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| ScalpingCoreIngressError::Io {
            path: path.clone(),
            source,
        })?;
    file.try_lock_exclusive()
        .map_err(|_| ScalpingCoreIngressError::Busy)?;
    Ok(CoreIngressLock { file })
}

fn validate_paths(
    artifacts_root: &Path,
    binding_path: &Path,
    input_path: &Path,
) -> Result<(), ScalpingCoreIngressError> {
    if !artifacts_root.is_absolute() || !binding_path.is_absolute() || !input_path.is_absolute() {
        return Err(ScalpingCoreIngressError::Request);
    }
    Ok(())
}

fn load_binding(path: &Path) -> Result<StrategyBinding, ScalpingCoreIngressError> {
    let bytes = fs::read(path).map_err(|source| ScalpingCoreIngressError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(ScalpingCoreIngressError::BindingDecode)
}

fn load_page(path: &Path) -> Result<ScalpingOwnerRiskPage, ScalpingCoreIngressError> {
    let bytes = fs::read(path).map_err(|source| ScalpingCoreIngressError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(ScalpingCoreIngressError::PageDecode)
}

fn load_quote_receipt(path: &Path) -> Result<ScalpingCoreQuoteReceipt, ScalpingCoreIngressError> {
    let bytes = fs::read(path).map_err(|source| ScalpingCoreIngressError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(ScalpingCoreIngressError::QuoteDecode)
}

fn validate_binding(
    config: &Config,
    binding: &StrategyBinding,
) -> Result<(), ScalpingCoreIngressError> {
    if binding.validate().is_err()
        || binding.exchange != "binance"
        || binding.account != config.trading_account_id
        || binding.symbol != config.symbol
        || binding.risk_budget.asset.as_str() != "USDT"
        || config.binance.as_ref().is_none_or(|binding| {
            binding.account_binding != BinanceAccountBinding::PortfolioMarginUm
        })
    {
        return Err(ScalpingCoreIngressError::Binding);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingCoreIngressError {
    #[error("Core ingress requires absolute artifact, binding, and input paths")]
    Request,
    #[error("Core ingress binding is invalid or differs from the configured deployment")]
    Binding,
    #[error("another Core ingress writer currently owns this artifact root")]
    Busy,
    #[error("Core ingress binding JSON is invalid: {0}")]
    BindingDecode(serde_json::Error),
    #[error("Core owner-risk page JSON is invalid: {0}")]
    PageDecode(serde_json::Error),
    #[error("Core quote receipt JSON is invalid: {0}")]
    QuoteDecode(serde_json::Error),
    #[error("Core ingress filesystem failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Core owner-risk inbox failed: {0}")]
    OwnerRisk(#[from] ScalpingOwnerRiskInboxError),
    #[error("Core quote receipt failed: {0}")]
    Quote(#[from] ScalpingCoreQuoteReceiptError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        domain::{Amount, Asset},
        storage::{ScalpingRiskBinding, ScalpingRiskCursor, ScalpingRiskFact},
        strategy::scalping::{RiskFact, RiskUnit, StrategyKind},
    };

    use super::*;

    fn config() -> Result<Config, toml::de::Error> {
        toml::from_str(
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDT'\n[binance]\naccount_binding = 'portfolio_margin_um'",
        )
    }

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
            strategy_instance_id: "core-ingress".to_owned(),
            run_id: "shadow-1".to_owned(),
            exchange: "binance".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "SOL/USDT".parse()?,
            parameter_release_id: "scalping-shadow-v1".to_owned(),
            owner_scope: "core-ingress:shadow-1".to_owned(),
            risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
        })
    }

    fn page(
        binding: &StrategyBinding,
    ) -> Result<ScalpingOwnerRiskPage, Box<dyn std::error::Error>> {
        let risk_unit = RiskUnit::new("risk")?;
        let risk_binding = ScalpingRiskBinding {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            owner_scope: binding.owner_scope.clone(),
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            parameter_release_id: binding.parameter_release_id.clone(),
            symbol: binding.symbol.clone(),
            risk_unit: risk_unit.clone(),
            valuation_generation: 1,
        };
        let fact_id = "core-ingress-fact".to_owned();
        Ok(ScalpingOwnerRiskPage {
            requested_after: None,
            facts: vec![ScalpingRiskFact {
                binding: risk_binding.clone(),
                fact: RiskFact {
                    fact_id: fact_id.clone(),
                    event_time_ms: 100,
                    valuation_generation: 1,
                    risk_unit,
                    realized_pnl: Decimal::ZERO,
                },
            }],
            cursor: ScalpingRiskCursor {
                cursor_id: "core-ingress-cursor".to_owned(),
                binding: risk_binding,
                source_sequence: 1,
                complete_from_ms: 100,
                observed_through_ms: 100,
                has_more: false,
                source_fact_ids: vec![fact_id],
            },
        })
    }

    #[test]
    fn owner_risk_commit_fsyncs_an_external_page_and_retries_idempotently()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let artifacts_root = directory.path().join("artifacts");
        let binding_path = directory.path().join("binding.json");
        let page_path = directory.path().join("page.json");
        let binding = binding()?;
        fs::write(&binding_path, serde_json::to_vec(&binding)?)?;
        fs::write(&page_path, serde_json::to_vec(&page(&binding)?)?)?;
        let request = ScalpingCoreOwnerRiskCommitRequest {
            artifacts_root: artifacts_root.clone(),
            binding_path,
            page_path,
        };

        let first = commit_scalping_core_owner_risk_page(&config()?, request.clone())?;
        let retry = commit_scalping_core_owner_risk_page(&config()?, request)?;
        assert_eq!(first.sequence, 1);
        assert_eq!(retry.sequence, 1);
        assert_eq!(
            first.inbox_path,
            artifacts_root.join(SCALPING_CORE_OWNER_RISK_PAGES_FILE)
        );
        Ok(())
    }

    #[test]
    fn quote_commit_rejects_a_nonabsolute_input_before_reading_it() -> Result<(), toml::de::Error> {
        let result = commit_scalping_core_quote_receipt(
            &config()?,
            ScalpingCoreQuoteCommitRequest {
                artifacts_root: PathBuf::from("artifacts"),
                binding_path: PathBuf::from("binding.json"),
                receipt_path: PathBuf::from("receipt.json"),
            },
        );
        assert!(matches!(result, Err(ScalpingCoreIngressError::Request)));
        Ok(())
    }

    #[test]
    fn ingress_lock_rejects_a_concurrent_writer_for_the_same_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let root = directory.path().join("artifacts");
        fs::create_dir_all(&root)?;
        let _first = acquire_lock(&root)?;
        assert!(matches!(
            acquire_lock(&root),
            Err(ScalpingCoreIngressError::Busy)
        ));
        Ok(())
    }
}
