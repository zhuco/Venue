use std::{collections::BTreeMap, fs::OpenOptions, io::Write};

use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::domain::{Amount, Asset, PositionSide};

use venue::execution::{
    CanaryEvidenceBinding, CanaryEvidenceError, CanaryEvidenceJournal, CanaryEvidenceTerminal,
    CanaryTerminalState, recover_canary_evidence,
};

fn binding(canary_id: &str) -> Result<CanaryEvidenceBinding, Box<dyn std::error::Error>> {
    let asset: Asset = "USDT".parse()?;
    Ok(CanaryEvidenceBinding {
        canary_id: canary_id.to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "BTC/USDT".parse()?,
        owner_scope: "scalping_canary:run_1".to_owned(),
        release_id: "scalping-canary-v1".to_owned(),
        position_side: PositionSide::Long,
        quote_cap: Amount::new(asset.clone(), Decimal::new(5, 0)),
        risk_cap: Amount::new(asset, Decimal::new(1, 1)),
        valid_until_ms: 10_000,
    })
}

fn stage() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("private_generation".to_owned(), "17".to_owned()),
        ("authority_identity_sha256".to_owned(), "a".repeat(64)),
    ])
}

#[test]
fn create_new_never_overwrites_a_prior_canary_receipt() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("canary.jsonl");
    let current = binding("canary-1")?;
    let _journal = CanaryEvidenceJournal::create_new(&path, current.clone(), 100)?;
    assert!(matches!(
        CanaryEvidenceJournal::create_new(&path, current, 100),
        Err(CanaryEvidenceError::Io { .. })
    ));
    Ok(())
}

#[test]
fn header_rejects_non_positive_or_over_limit_usdt_caps() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("canary.jsonl");
    let mut over_limit = binding("canary-1")?;
    over_limit.quote_cap.value = Decimal::new(1_001, 2);
    assert!(matches!(
        CanaryEvidenceJournal::create_new(&path, over_limit, 100),
        Err(CanaryEvidenceError::Binding)
    ));

    let mut zero = binding("canary-2")?;
    zero.risk_cap.value = Decimal::ZERO;
    assert!(matches!(
        CanaryEvidenceJournal::create_new(directory.path().join("zero.jsonl"), zero, 100),
        Err(CanaryEvidenceError::Binding)
    ));
    Ok(())
}

#[test]
fn recovery_rejects_truncation_hash_sequence_and_cross_binding()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("canary.jsonl");
    let current = binding("canary-1")?;
    let mut journal = CanaryEvidenceJournal::create_new(&path, current.clone(), 100)?;
    journal.append_stage("preflight_ready", 101, stage())?;
    recover_canary_evidence(&path, &current)?;

    let other = binding("canary-2")?;
    assert!(matches!(
        recover_canary_evidence(&path, &other),
        Err(CanaryEvidenceError::Binding)
    ));

    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(br#"{"record":"stage""#)?;
    file.sync_data()?;
    assert!(matches!(
        recover_canary_evidence(&path, &current),
        Err(CanaryEvidenceError::Truncated)
    ));

    let hash_path = directory.path().join("hash.jsonl");
    let mut hash_journal = CanaryEvidenceJournal::create_new(&hash_path, current.clone(), 100)?;
    hash_journal.append_stage("preflight_ready", 101, stage())?;
    let bytes = std::fs::read_to_string(&hash_path)?;
    std::fs::write(
        &hash_path,
        bytes.replacen("preflight_ready", "preflight_tampered", 1),
    )?;
    assert!(matches!(
        recover_canary_evidence(&hash_path, &current),
        Err(CanaryEvidenceError::Hash)
    ));

    let sequence_path = directory.path().join("sequence.jsonl");
    let mut sequence_journal =
        CanaryEvidenceJournal::create_new(&sequence_path, current.clone(), 100)?;
    sequence_journal.append_stage("preflight_ready", 101, stage())?;
    let bytes = std::fs::read_to_string(&sequence_path)?;
    std::fs::write(
        &sequence_path,
        bytes.replacen("\"sequence\":2", "\"sequence\":9", 1),
    )?;
    assert!(matches!(
        recover_canary_evidence(&sequence_path, &current),
        Err(CanaryEvidenceError::Hash)
    ));
    Ok(())
}

#[test]
fn terminal_requires_exact_safe_readback_and_is_single_use()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("canary.jsonl");
    let current = binding("canary-1")?;
    let mut journal = CanaryEvidenceJournal::create_new(&path, current.clone(), 100)?;

    journal.append_stage("entry_admitted", 101, stage())?;
    assert!(journal.recover()?.terminal().is_none());
    assert!(matches!(
        journal.seal_terminal(
            102,
            CanaryTerminalState::Flat {
                exact_readback_sha256: "not-a-digest".to_owned(),
            },
        ),
        Err(CanaryEvidenceError::Terminal)
    ));

    journal.seal_terminal(
        102,
        CanaryTerminalState::Protected {
            exact_readback_sha256: "b".repeat(64),
        },
    )?;
    assert!(matches!(
        journal.append_stage("flat_verified", 103, stage()),
        Err(CanaryEvidenceError::Terminal)
    ));
    assert!(matches!(
        journal.seal_terminal(
            103,
            CanaryTerminalState::Flat {
                exact_readback_sha256: "c".repeat(64),
            },
        ),
        Err(CanaryEvidenceError::Terminal)
    ));
    assert!(matches!(
        CanaryEvidenceJournal::open_existing(&path, &current),
        Err(CanaryEvidenceError::Terminal)
    ));

    let flat_path = directory.path().join("flat.jsonl");
    let mut flat = CanaryEvidenceJournal::create_new(&flat_path, current.clone(), 100)?;
    flat.seal_terminal(
        101,
        CanaryTerminalState::Flat {
            exact_readback_sha256: "d".repeat(64),
        },
    )?;
    assert!(matches!(
        flat.recover()?.terminal(),
        Some(CanaryEvidenceTerminal {
            terminal: CanaryTerminalState::Flat { .. },
            ..
        })
    ));
    Ok(())
}

#[test]
fn stages_reject_credential_named_fields() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("canary.jsonl");
    let mut journal = CanaryEvidenceJournal::create_new(&path, binding("canary-1")?, 100)?;
    assert!(matches!(
        journal.append_stage(
            "preflight_ready",
            101,
            BTreeMap::from([("bearer_token".to_owned(), "redacted".to_owned())]),
        ),
        Err(CanaryEvidenceError::Stage)
    ));
    assert!(matches!(
        journal.append_stage("preflight_ready", 10_001, stage()),
        Err(CanaryEvidenceError::Stage)
    ));
    journal.append_stage("preflight_ready", 102, stage())?;
    assert!(matches!(
        journal.append_stage("time_regressed", 101, stage()),
        Err(CanaryEvidenceError::Stage)
    ));
    Ok(())
}
