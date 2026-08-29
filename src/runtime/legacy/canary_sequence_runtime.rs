use std::{fs, path::Path};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    domain::{OrderCommand, Symbol},
    execution::{
        BnbMutationPermit, BnbMutationRequest, CanaryCampaignBinding, CanaryEvidenceJournal,
        CanarySequenceError, CanarySequenceGate, recover_discovered_canary_evidence,
    },
};

const RELEASE_ID: &str = "stage4_manual_canary_v1";
const SOL_SYMBOL: &str = "SOL/USDT";
const BNB_SYMBOL: &str = "BNB/USDT";

pub fn ensure_sol_pending(
    artifacts_root: &Path,
    trading_account_id: &str,
    symbol: &Symbol,
) -> Result<(), CanarySequenceRuntimeError> {
    if symbol.to_string() == SOL_SYMBOL {
        gate(artifacts_root, trading_account_id)?.ensure_sol_pending()?;
    }
    Ok(())
}

pub fn authorize_bnb(
    artifacts_root: &Path,
    trading_account_id: &str,
    symbol: &Symbol,
    command: &OrderCommand,
) -> Result<Option<BnbMutationPermit>, CanarySequenceRuntimeError> {
    if symbol.to_string() != BNB_SYMBOL {
        return Ok(None);
    }
    let gate = gate(artifacts_root, trading_account_id)?;
    let request = BnbMutationRequest {
        binding: campaign(trading_account_id),
        symbol: symbol.to_string(),
        mutation_id: command.command_id.as_str().to_owned(),
        mutation_sha256: digest(command)?,
    };
    let sol_root = artifacts_root.join("solusdt");
    let canonical_root =
        fs::canonicalize(&sol_root).map_err(|source| CanarySequenceRuntimeError::Artifact {
            path: sol_root.clone(),
            source,
        })?;
    for entry in
        fs::read_dir(&canonical_root).map_err(|source| CanarySequenceRuntimeError::Artifact {
            path: canonical_root.clone(),
            source,
        })?
    {
        let entry = entry.map_err(|source| CanarySequenceRuntimeError::Artifact {
            path: canonical_root.clone(),
            source,
        })?;
        let metadata =
            entry
                .file_type()
                .map_err(|source| CanarySequenceRuntimeError::Artifact {
                    path: entry.path(),
                    source,
                })?;
        if !metadata.is_dir() || metadata.is_symlink() {
            continue;
        }
        let evidence_path = entry.path().join("evidence.jsonl");
        let Ok(evidence_metadata) = fs::symlink_metadata(&evidence_path) else {
            continue;
        };
        if !evidence_metadata.is_file() || evidence_metadata.file_type().is_symlink() {
            continue;
        }
        let Ok(recovery) = recover_discovered_canary_evidence(&evidence_path) else {
            continue;
        };
        if let Ok(permit) = gate.permit_bnb_mutation(&request, &recovery) {
            return Ok(Some(permit));
        }
    }
    Err(CanarySequenceRuntimeError::NoMatchingSolEvidence)
}

pub fn complete_sol(
    artifacts_root: &Path,
    trading_account_id: &str,
    symbol: &Symbol,
    evidence: &CanaryEvidenceJournal,
) -> Result<(), CanarySequenceRuntimeError> {
    if symbol.to_string() == SOL_SYMBOL {
        gate(artifacts_root, trading_account_id)?.complete_sol_protection(&evidence.recover()?)?;
    }
    Ok(())
}

fn gate(
    artifacts_root: &Path,
    trading_account_id: &str,
) -> Result<CanarySequenceGate, CanarySequenceRuntimeError> {
    Ok(CanarySequenceGate::open(
        artifacts_root,
        campaign(trading_account_id),
    )?)
}

fn campaign(trading_account_id: &str) -> CanaryCampaignBinding {
    CanaryCampaignBinding {
        exchange: "binance".to_owned(),
        account: trading_account_id.to_owned(),
        release_id: RELEASE_ID.to_owned(),
    }
}

fn digest(value: &impl Serialize) -> Result<String, CanarySequenceRuntimeError> {
    let bytes = serde_json::to_vec(value).map_err(CanarySequenceRuntimeError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[derive(Debug, thiserror::Error)]
pub enum CanarySequenceRuntimeError {
    #[error("no recovered SOL protection evidence matches the durable campaign receipt")]
    NoMatchingSolEvidence,
    #[error("Canary sequence artifact access failed for {path}: {source}", path = path.display())]
    Artifact {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Sequence(#[from] CanarySequenceError),
    #[error(transparent)]
    Evidence(#[from] crate::execution::CanaryEvidenceError),
    #[error("Canary mutation binding encoding failed: {0}")]
    Encode(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        domain::{
            Amount, Asset, CommandId, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
        },
        execution::{CanaryEvidenceBinding, CanaryTerminalState},
    };

    const TEST_ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

    #[test]
    fn runtime_wires_sol_completion_to_exact_bnb_command() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let sol: Symbol = SOL_SYMBOL.parse()?;
        let sol_dir = directory.path().join("solusdt").join("protection_1");
        fs::create_dir_all(&sol_dir)?;
        let usdt: Asset = "USDT".parse()?;
        let binding = CanaryEvidenceBinding {
            canary_id: "protection_1".to_owned(),
            exchange: "binance".to_owned(),
            account: TEST_ACCOUNT_ID.to_owned(),
            symbol: sol.clone(),
            owner_scope: "manual_canary_mainnet".to_owned(),
            release_id: RELEASE_ID.to_owned(),
            position_side: PositionSide::Long,
            quote_cap: Amount::new(usdt.clone(), Decimal::new(10, 0)),
            risk_cap: Amount::new(usdt, Decimal::new(10, 0)),
            valid_until_ms: 10_000,
        };
        let mut journal =
            CanaryEvidenceJournal::create_new(sol_dir.join("evidence.jsonl"), binding, 100)?;
        journal.append_stage(
            "protection_custody",
            101,
            BTreeMap::from([("custody".to_owned(), hash_label("custody"))]),
        )?;
        journal.seal_terminal(
            102,
            CanaryTerminalState::Flat {
                exact_readback_sha256: hash_label("flat"),
            },
        )?;

        ensure_sol_pending(directory.path(), TEST_ACCOUNT_ID, &sol)?;
        complete_sol(directory.path(), TEST_ACCOUNT_ID, &sol, &journal)?;
        assert!(ensure_sol_pending(directory.path(), TEST_ACCOUNT_ID, &sol).is_err());

        let bnb: Symbol = BNB_SYMBOL.parse()?;
        let command = bnb_command(bnb.clone())?;
        let permit = authorize_bnb(directory.path(), TEST_ACCOUNT_ID, &bnb, &command)?
            .ok_or_else(|| std::io::Error::other("BNB permit missing"))?;
        assert_eq!(permit.mutation_id, command.command_id.as_str());
        assert_eq!(permit.mutation_sha256, digest(&command)?);
        Ok(())
    }

    #[test]
    fn bnb_runtime_fails_closed_without_sol_artifacts() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        fs::create_dir(directory.path().join("solusdt"))?;
        let bnb: Symbol = BNB_SYMBOL.parse()?;
        assert!(
            authorize_bnb(
                directory.path(),
                TEST_ACCOUNT_ID,
                &bnb,
                &bnb_command(bnb.clone())?,
            )
            .is_err()
        );
        Ok(())
    }

    fn bnb_command(symbol: Symbol) -> Result<OrderCommand, Box<dyn std::error::Error>> {
        Ok(OrderCommand {
            command_id: CommandId::new("cmd_bnb_1")?,
            client_order_id: CommandId::new("vcn_bnb_1")?,
            owner: OrderOwner {
                strategy_instance_id: "manual_canary".to_owned(),
                run_id: "protection_bnb_1".to_owned(),
                exchange: "binance".to_owned(),
                account: TEST_ACCOUNT_ID.to_owned(),
                symbol,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 2),
            limit_price: Price::new(Decimal::new(600, 0))?,
            reduce_only: false,
        })
    }

    fn hash_label(label: &str) -> String {
        format!("{:x}", Sha256::digest(label.as_bytes()))
    }
}
