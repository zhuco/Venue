use std::{
    collections::BTreeMap,
    fs,
    sync::{Arc, Barrier},
    thread,
};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset, PositionSide},
    execution::{
        BnbMutationRequest, CanaryCampaignBinding, CanaryEvidenceBinding, CanaryEvidenceJournal,
        CanaryEvidenceRecovery, CanarySequenceGate, CanaryTerminalState, recover_canary_evidence,
        recover_discovered_canary_evidence,
    },
};

fn campaign() -> CanaryCampaignBinding {
    CanaryCampaignBinding {
        exchange: "binance".to_owned(),
        account: "00000000-0000-4000-8000-000000000001".to_owned(),
        release_id: "sol-bnb-canary-r1".to_owned(),
    }
}

fn digest(label: &str) -> String {
    format!("{:x}", Sha256::digest(label.as_bytes()))
}

fn evidence_binding(
    campaign: &CanaryCampaignBinding,
) -> Result<CanaryEvidenceBinding, Box<dyn std::error::Error>> {
    let asset: Asset = "USDT".parse()?;
    Ok(CanaryEvidenceBinding {
        canary_id: "sol-canary-1".to_owned(),
        exchange: campaign.exchange.clone(),
        account: campaign.account.clone(),
        symbol: "SOL/USDT".parse()?,
        owner_scope: "scalping_canary:sol".to_owned(),
        release_id: campaign.release_id.clone(),
        position_side: PositionSide::Long,
        quote_cap: Amount::new(asset.clone(), Decimal::new(5, 0)),
        risk_cap: Amount::new(asset, Decimal::ONE),
        valid_until_ms: 10_000,
    })
}

fn protected_recovery(
    path: &std::path::Path,
    campaign: &CanaryCampaignBinding,
    custody: &str,
) -> Result<CanaryEvidenceRecovery, Box<dyn std::error::Error>> {
    let binding = evidence_binding(campaign)?;
    let mut journal = CanaryEvidenceJournal::create_new(path, binding, 100)?;
    journal.append_stage(
        "protection_custody",
        101,
        BTreeMap::from([("custody".to_owned(), digest(custody))]),
    )?;
    journal.seal_terminal(
        102,
        CanaryTerminalState::Flat {
            exact_readback_sha256: digest("sol-terminal-flat"),
        },
    )?;
    Ok(journal.recover()?)
}

fn bnb_request(campaign: CanaryCampaignBinding) -> BnbMutationRequest {
    BnbMutationRequest {
        binding: campaign,
        symbol: "BNB/USDT".to_owned(),
        mutation_id: "bnb-mutation-1".to_owned(),
        mutation_sha256: digest("bnb-mutation-1"),
    }
}

#[test]
fn recovered_protected_flat_sol_evidence_allows_exactly_bound_bnb_permit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let campaign = campaign();
    let recovery = protected_recovery(&directory.path().join("sol.jsonl"), &campaign, "custody-1")?;
    let gate = CanarySequenceGate::open(directory.path(), campaign.clone())?;
    let receipt = gate.complete_sol_protection(&recovery)?;
    assert_eq!(
        recover_discovered_canary_evidence(&directory.path().join("sol.jsonl"))?,
        recovery
    );
    let permit = gate.permit_bnb_mutation(&bnb_request(campaign), &recovery)?;

    assert_eq!(permit.header_binding_sha256, receipt.header_binding_sha256);
    assert_eq!(permit.receipt_sha256, receipt.receipt_sha256);
    Ok(())
}

#[test]
fn place_cancel_or_nonterminal_journal_does_not_count_as_sol_protection()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let campaign = campaign();
    let binding = evidence_binding(&campaign)?;
    let mut journal = CanaryEvidenceJournal::create_new(
        directory.path().join("place-cancel.jsonl"),
        binding,
        100,
    )?;
    journal.append_stage(
        "post_only_place_cancel",
        101,
        BTreeMap::from([("command".to_owned(), digest("sol-place-cancel"))]),
    )?;
    journal.seal_terminal(
        102,
        CanaryTerminalState::Flat {
            exact_readback_sha256: digest("sol-flat-after-cancel"),
        },
    )?;
    let gate = CanarySequenceGate::open(directory.path(), campaign.clone())?;
    assert!(gate.complete_sol_protection(&journal.recover()?).is_err());

    let binding = evidence_binding(&campaign)?;
    let mut nonterminal = CanaryEvidenceJournal::create_new(
        directory.path().join("nonterminal.jsonl"),
        binding,
        100,
    )?;
    nonterminal.append_stage(
        "protection_custody",
        101,
        BTreeMap::from([("custody".to_owned(), digest("custody-2"))]),
    )?;
    assert!(
        gate.complete_sol_protection(&nonterminal.recover()?)
            .is_err()
    );
    Ok(())
}

#[test]
fn wrong_symbol_account_or_release_and_corrupt_journal_are_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let campaign = campaign();
    let recovery = protected_recovery(&directory.path().join("sol.jsonl"), &campaign, "custody-1")?;
    let gate = CanarySequenceGate::open(directory.path(), campaign.clone())?;
    let _ = gate.complete_sol_protection(&recovery)?;

    let mut wrong_symbol = bnb_request(campaign.clone());
    wrong_symbol.symbol = "SOL/USDT".to_owned();
    assert!(gate.permit_bnb_mutation(&wrong_symbol, &recovery).is_err());

    let mut wrong_sol_binding = evidence_binding(&campaign)?;
    wrong_sol_binding.symbol = "BNB/USDT".parse()?;
    let mut wrong_sol_journal = CanaryEvidenceJournal::create_new(
        directory.path().join("wrong-sol-symbol.jsonl"),
        wrong_sol_binding,
        100,
    )?;
    wrong_sol_journal.append_stage(
        "protection_custody",
        101,
        BTreeMap::from([("custody".to_owned(), digest("custody-wrong-symbol"))]),
    )?;
    wrong_sol_journal.seal_terminal(
        102,
        CanaryTerminalState::Flat {
            exact_readback_sha256: digest("wrong-symbol-flat"),
        },
    )?;
    assert!(
        gate.complete_sol_protection(&wrong_sol_journal.recover()?)
            .is_err()
    );

    let mut wrong_account = campaign.clone();
    wrong_account.account = "other".to_owned();
    let wrong_account_recovery = protected_recovery(
        &directory.path().join("wrong-account.jsonl"),
        &wrong_account,
        "custody-1",
    )?;
    assert!(
        gate.complete_sol_protection(&wrong_account_recovery)
            .is_err()
    );

    let mut wrong_release = campaign.clone();
    wrong_release.release_id = "sol-bnb-canary-r2".to_owned();
    let wrong_release_recovery = protected_recovery(
        &directory.path().join("wrong-release.jsonl"),
        &wrong_release,
        "custody-1",
    )?;
    assert!(
        gate.complete_sol_protection(&wrong_release_recovery)
            .is_err()
    );

    let path = directory.path().join("corrupt.jsonl");
    let binding = evidence_binding(&campaign)?;
    let mut corrupt = CanaryEvidenceJournal::create_new(&path, binding.clone(), 100)?;
    corrupt.append_stage(
        "protection_custody",
        101,
        BTreeMap::from([("custody".to_owned(), digest("custody-3"))]),
    )?;
    let bytes = fs::read_to_string(&path)?;
    fs::write(
        &path,
        bytes.replacen("protection_custody", "tampered_custody", 1),
    )?;
    assert!(recover_canary_evidence(&path, &binding).is_err());
    Ok(())
}

#[test]
fn conflicting_receipts_fail_closed_and_pending_receipt_recovers()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let campaign = campaign();
    let first = protected_recovery(
        &directory.path().join("sol-1.jsonl"),
        &campaign,
        "custody-1",
    )?;
    let second = protected_recovery(
        &directory.path().join("sol-2.jsonl"),
        &campaign,
        "custody-2",
    )?;
    let gate = CanarySequenceGate::open(directory.path(), campaign.clone())?;
    let receipt = gate.complete_sol_protection(&first)?;
    assert!(gate.complete_sol_protection(&second).is_err());

    let receipt_path = gate.receipt_path();
    let pending_path = receipt_path.with_file_name("sol_to_bnb_canary_receipt.json.next");
    fs::rename(&receipt_path, &pending_path)?;
    assert_eq!(gate.complete_sol_protection(&first)?, receipt);
    assert!(receipt_path.exists());

    fs::write(&receipt_path, b"corrupt")?;
    assert!(
        gate.permit_bnb_mutation(&bnb_request(campaign), &first)
            .is_err()
    );
    Ok(())
}

#[test]
fn concurrent_completion_has_one_durable_receipt_and_relative_roots_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(CanarySequenceGate::open("relative-canary-root", campaign()).is_err());

    let directory = tempdir()?;
    let campaign = campaign();
    let recovery = Arc::new(protected_recovery(
        &directory.path().join("sol.jsonl"),
        &campaign,
        "custody-1",
    )?);
    let gate = Arc::new(CanarySequenceGate::open(directory.path(), campaign)?);
    let barrier = Arc::new(Barrier::new(2));
    let left_gate = Arc::clone(&gate);
    let left_recovery = Arc::clone(&recovery);
    let left_barrier = Arc::clone(&barrier);
    let left = thread::spawn(move || {
        left_barrier.wait();
        left_gate.complete_sol_protection(&left_recovery)
    });
    let right_gate = Arc::clone(&gate);
    let right_recovery = Arc::clone(&recovery);
    let right_barrier = Arc::clone(&barrier);
    let right = thread::spawn(move || {
        right_barrier.wait();
        right_gate.complete_sol_protection(&right_recovery)
    });
    let left = left
        .join()
        .map_err(|_| std::io::Error::other("left completion thread panicked"))??;
    let right = right
        .join()
        .map_err(|_| std::io::Error::other("right completion thread panicked"))??;

    assert_eq!(left, right);
    assert!(gate.receipt_path().exists());
    Ok(())
}
