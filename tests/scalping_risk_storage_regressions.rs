use std::{fs::OpenOptions, io::Write};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use venue::{
    storage::{
        ScalpingRiskBinding, ScalpingRiskCursor, ScalpingRiskEntry, ScalpingRiskError,
        ScalpingRiskFact, ScalpingRiskJournal, ScalpingRiskRecord,
    },
    strategy::scalping::{RiskFact, RiskUnit},
};

fn binding(generation: u64) -> Result<ScalpingRiskBinding, Box<dyn std::error::Error>> {
    Ok(ScalpingRiskBinding {
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        owner_scope: "scalping-owner".to_owned(),
        strategy_instance_id: "instance-1".to_owned(),
        run_id: "run-1".to_owned(),
        parameter_release_id: "release-1".to_owned(),
        symbol: "BTC/USDT".parse()?,
        risk_unit: RiskUnit::new("logical-risk")?,
        valuation_generation: generation,
    })
}

fn fact(binding: ScalpingRiskBinding, id: &str, event_time_ms: u64) -> ScalpingRiskFact {
    ScalpingRiskFact {
        fact: RiskFact {
            fact_id: id.to_owned(),
            event_time_ms,
            valuation_generation: binding.valuation_generation,
            risk_unit: binding.risk_unit.clone(),
            realized_pnl: Decimal::ONE,
        },
        binding,
    }
}

fn cursor(
    binding: ScalpingRiskBinding,
    id: &str,
    sequence: u64,
    facts: &[&str],
) -> ScalpingRiskCursor {
    ScalpingRiskCursor {
        cursor_id: id.to_owned(),
        binding,
        source_sequence: sequence,
        complete_from_ms: 100,
        observed_through_ms: 100 + sequence,
        has_more: false,
        source_fact_ids: facts.iter().map(|value| (*value).to_owned()).collect(),
    }
}

#[test]
fn exact_duplicate_page_is_idempotent_but_conflicting_fact_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-risk.jsonl");
    let mut journal = ScalpingRiskJournal::open(&path)?;
    let item = fact(binding(1)?, "risk-1", 101);
    let watermark = cursor(binding(1)?, "cursor-1", 1, &["risk-1"]);

    assert!(matches!(
        journal.append_page(vec![item.clone(), item.clone()], watermark.clone()),
        Err(ScalpingRiskError::DuplicatePageFact)
    ));

    let first = journal.append_page(vec![item.clone()], watermark.clone())?;
    assert_eq!(first.fact_sequences, vec![1]);
    assert_eq!(first.cursor_sequence, 2);
    assert_eq!(journal.append_page(vec![item.clone()], watermark)?, first);
    assert_eq!(journal.recover()?.records.len(), 2);

    let mut conflicting = item;
    conflicting.fact.realized_pnl = Decimal::NEGATIVE_ONE;
    assert!(matches!(
        journal.append_page(
            vec![conflicting],
            cursor(binding(1)?, "cursor-2", 2, &["risk-1"])
        ),
        Err(ScalpingRiskError::ConflictingFact)
    ));
    Ok(())
}

#[test]
fn torn_tail_is_removed_before_a_retry_writes_the_cursor() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let path = directory.path().join("scalping-risk.jsonl");
    let initial = fact(binding(1)?, "risk-1", 101);
    let mut journal = ScalpingRiskJournal::open(&path)?;
    journal.append_page(
        vec![initial],
        cursor(binding(1)?, "cursor-1", 1, &["risk-1"]),
    )?;
    drop(journal);

    let pending = fact(binding(1)?, "risk-2", 102);
    let entry = ScalpingRiskEntry::Fact(pending.clone());
    let content_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&entry)?));
    let record = ScalpingRiskRecord {
        sequence: 3,
        content_sha256,
        entry,
    };
    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(&serde_json::to_vec(&record)?)?;
    file.write_all(b"\n{\"torn\"")?;
    file.sync_data()?;
    drop(file);

    let mut recovered = ScalpingRiskJournal::open(&path)?;
    assert_eq!(recovered.committed_replays()?.len(), 1);
    let commit = recovered.append_page(
        vec![pending],
        cursor(binding(1)?, "cursor-2", 2, &["risk-2"]),
    )?;
    assert_eq!(commit.fact_sequences, vec![3]);
    assert_eq!(commit.cursor_sequence, 4);
    assert_eq!(recovered.recover()?.records.len(), 4);
    assert_eq!(recovered.committed_replays()?.len(), 2);
    Ok(())
}

#[test]
fn committed_replays_preserve_cursor_fact_order_and_complete_window()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-risk.jsonl");
    let mut journal = ScalpingRiskJournal::open(&path)?;
    let first = fact(binding(1)?, "risk-1", 101);
    let second = fact(binding(1)?, "risk-2", 102);
    let watermark = cursor(binding(1)?, "cursor-1", 2, &["risk-2", "risk-1"]);
    journal.append_page(vec![first, second], watermark.clone())?;

    let replays = journal.committed_replays()?;
    assert_eq!(replays.len(), 1);
    assert_eq!(replays[0].cursor, watermark);
    assert_eq!(
        replays[0]
            .facts
            .iter()
            .map(|fact| fact.fact.fact_id.as_str())
            .collect::<Vec<_>>(),
        vec!["risk-2", "risk-1"]
    );
    assert_eq!(journal.recover_committed_replays()?, replays);
    Ok(())
}

#[test]
fn cursor_cannot_overtake_missing_facts_or_cross_a_binding()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-risk.jsonl");
    let mut journal = ScalpingRiskJournal::open(&path)?;
    assert!(matches!(
        journal.append_page(Vec::new(), cursor(binding(1)?, "cursor-1", 1, &["missing"])),
        Err(ScalpingRiskError::CursorFacts)
    ));
    assert!(journal.recover()?.records.is_empty());

    journal.append_page(
        vec![fact(binding(1)?, "risk-1", 101)],
        cursor(binding(1)?, "cursor-1", 1, &["risk-1"]),
    )?;
    let mut other = binding(1)?;
    other.parameter_release_id = "release-2".to_owned();
    assert!(matches!(
        journal.append_page(
            vec![fact(other.clone(), "risk-2", 102)],
            cursor(other, "cursor-2", 2, &["risk-2"])
        ),
        Err(ScalpingRiskError::Scope)
    ));
    Ok(())
}

#[test]
fn generation_change_is_preserved_but_a_cursor_cannot_cross_generations()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-risk.jsonl");
    let mut journal = ScalpingRiskJournal::open(&path)?;
    let first = fact(binding(1)?, "risk-1", 101);
    let second = fact(binding(2)?, "risk-2", 102);
    journal.append_page(vec![first], cursor(binding(1)?, "cursor-1", 1, &["risk-1"]))?;
    journal.append_page(
        vec![second.clone()],
        cursor(binding(2)?, "cursor-2", 2, &["risk-2"]),
    )?;
    let recovered = journal.recover()?;
    let generations = recovered
        .records
        .iter()
        .filter_map(|record| match &record.entry {
            ScalpingRiskEntry::Fact(fact) => Some(fact.binding.valuation_generation),
            ScalpingRiskEntry::Cursor(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(generations, vec![1, 2]);

    assert!(matches!(
        journal.append_page(
            vec![second],
            cursor(binding(1)?, "cursor-1-late", 2, &["risk-2"])
        ),
        Err(ScalpingRiskError::CursorFacts)
    ));
    assert_eq!(journal.recover()?.records.len(), 4);
    Ok(())
}

#[test]
fn same_sequence_terminal_manifest_requires_an_unfinished_previous_page()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-risk.jsonl");
    let mut journal = ScalpingRiskJournal::open(&path)?;
    let mut middle = cursor(binding(1)?, "cursor-middle", 1, &["risk-1"]);
    middle.has_more = true;
    journal.append_page(vec![fact(binding(1)?, "risk-1", 101)], middle)?;
    let terminal = cursor(binding(1)?, "cursor-terminal", 1, &["risk-1"]);
    journal.append_page(Vec::new(), terminal)?;

    let mut repeated_terminal = cursor(binding(1)?, "cursor-repeat", 1, &["risk-1"]);
    repeated_terminal.observed_through_ms = 103;
    assert!(matches!(
        journal.append_page(Vec::new(), repeated_terminal),
        Err(ScalpingRiskError::CursorRegression)
    ));
    Ok(())
}
