use std::{fs::OpenOptions, io::Write};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset},
    runtime::{
        BoundRiskRevaluation, MAX_RISK_FACTS_PER_PAGE, MAX_RISK_REPLAY_PAGES, RiskProofClock,
        RiskRevaluationProducer, RiskRevaluationProducerError,
    },
    storage::{
        ScalpingRiskBinding, ScalpingRiskCursor, ScalpingRiskEntry, ScalpingRiskFact,
        ScalpingRiskJournal, ScalpingRiskRecord,
    },
    strategy::scalping::{RiskFact, RiskUnit, StrategyBinding, StrategyKind},
};

#[test]
fn producer_enforces_legacy_page_and_fact_bounds_before_persisting()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let facts_path = directory.path().join("too-many-facts.jsonl");
    let mut facts_producer = open(&facts_path)?;
    let generation_binding = binding(1)?;
    let too_many = (0..=MAX_RISK_FACTS_PER_PAGE)
        .map(|index| fact(generation_binding.clone(), &format!("fact-{index}"), 101))
        .collect::<Vec<_>>();
    let id_strings = too_many
        .iter()
        .map(|value| value.fact.fact_id.clone())
        .collect::<Vec<_>>();
    let ids = id_strings.iter().map(String::as_str).collect::<Vec<_>>();
    assert!(matches!(
        facts_producer.commit_page(
            clock(101),
            too_many,
            cursor(binding(1)?, "oversized", 1, 101, &ids),
        ),
        Err(RiskRevaluationProducerError::ReplayBound)
    ));
    assert!(
        ScalpingRiskJournal::open(&facts_path)?
            .recover()?
            .records
            .is_empty()
    );

    let pages_path = directory.path().join("too-many-pages.jsonl");
    let mut pages_producer = open(&pages_path)?;
    for page in 1..MAX_RISK_REPLAY_PAGES {
        let mut page_cursor = cursor(
            binding(1)?,
            &format!("cursor-{page}"),
            page as u64,
            101,
            &[],
        );
        page_cursor.has_more = true;
        assert_eq!(
            pages_producer.commit_page(clock(101), Vec::new(), page_cursor)?,
            None
        );
    }
    let mut overflow = cursor(
        binding(1)?,
        "cursor-overflow",
        MAX_RISK_REPLAY_PAGES as u64,
        101,
        &[],
    );
    overflow.has_more = true;
    assert!(matches!(
        pages_producer.commit_page(clock(101), Vec::new(), overflow),
        Err(RiskRevaluationProducerError::ReplayBound)
    ));
    Ok(())
}

fn unit() -> Result<RiskUnit, Box<dyn std::error::Error>> {
    Ok(RiskUnit::new("logical-risk")?)
}

fn binding(generation: u64) -> Result<ScalpingRiskBinding, Box<dyn std::error::Error>> {
    Ok(ScalpingRiskBinding {
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        owner_scope: "owner-1".to_owned(),
        strategy_instance_id: "instance-1".to_owned(),
        run_id: "run-1".to_owned(),
        parameter_release_id: "release-1".to_owned(),
        symbol: "BTC/USDT".parse()?,
        risk_unit: unit()?,
        valuation_generation: generation,
    })
}

fn strategy_binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "instance-1".to_owned(),
        run_id: "run-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "release-1".to_owned(),
        owner_scope: "owner-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::ONE),
    })
}

fn clock(now_ms: u64) -> RiskProofClock {
    RiskProofClock {
        now_ms,
        max_stale_ms: 5,
    }
}

fn complete(
    result: Option<BoundRiskRevaluation>,
) -> Result<BoundRiskRevaluation, Box<dyn std::error::Error>> {
    result.ok_or_else(|| std::io::Error::other("expected complete risk proof").into())
}

fn fact(binding: ScalpingRiskBinding, id: &str, event_time_ms: u64) -> ScalpingRiskFact {
    fact_with_pnl(binding, id, event_time_ms, Decimal::ONE)
}

fn fact_with_pnl(
    binding: ScalpingRiskBinding,
    id: &str,
    event_time_ms: u64,
    realized_pnl: Decimal,
) -> ScalpingRiskFact {
    ScalpingRiskFact {
        fact: RiskFact {
            fact_id: id.to_owned(),
            event_time_ms,
            valuation_generation: binding.valuation_generation,
            risk_unit: binding.risk_unit.clone(),
            realized_pnl,
        },
        binding,
    }
}

fn cursor(
    binding: ScalpingRiskBinding,
    id: &str,
    sequence: u64,
    observed_through_ms: u64,
    fact_ids: &[&str],
) -> ScalpingRiskCursor {
    ScalpingRiskCursor {
        cursor_id: id.to_owned(),
        binding,
        source_sequence: sequence,
        complete_from_ms: 100,
        observed_through_ms,
        has_more: false,
        source_fact_ids: fact_ids.iter().map(|id| (*id).to_owned()).collect(),
    }
}

fn open(path: &std::path::Path) -> Result<RiskRevaluationProducer, Box<dyn std::error::Error>> {
    Ok(RiskRevaluationProducer::open(
        path,
        &strategy_binding()?,
        unit()?,
    )?)
}

#[test]
fn commit_writes_facts_before_cursor_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    let item = fact(binding(1)?, "fact-1", 101);
    let watermark = cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"]);

    let first =
        complete(producer.commit_page(clock(101), vec![item.clone()], watermark.clone())?)?;
    let retry = complete(producer.commit_page(clock(101), vec![item], watermark)?)?;
    assert_eq!(retry, first);
    assert_eq!(first.cursor_sequence, 2);
    let records = ScalpingRiskJournal::open(&path)?.recover()?.records;
    assert!(matches!(records[0].entry, ScalpingRiskEntry::Fact(_)));
    assert!(matches!(records[1].entry, ScalpingRiskEntry::Cursor(_)));
    assert_eq!(records[1].sequence, first.cursor_sequence);
    Ok(())
}

#[test]
fn restart_returns_only_the_latest_complete_proof_after_the_applied_one()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    let first = complete(producer.commit_page(
        clock(101),
        vec![fact(binding(1)?, "fact-1", 101)],
        cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"]),
    )?)?;
    let second = complete(producer.commit_page(
        clock(102),
        vec![fact(binding(1)?, "fact-2", 102)],
        cursor(binding(1)?, "cursor-2", 2, 102, &["fact-2"]),
    )?)?;
    drop(producer);

    let restarted = open(&path)?;
    assert_eq!(second.cursor_sequence, 4);
    assert_eq!(
        restarted.recover_complete(clock(102), None)?,
        Some(second.clone())
    );
    assert_eq!(
        restarted.recover_complete(clock(102), Some(&first.proof.proof_id))?,
        Some(second.clone())
    );
    assert_eq!(
        restarted.recover_complete(clock(102), Some(&second.proof.proof_id))?,
        None
    );
    Ok(())
}

#[test]
fn producer_rejects_a_binding_that_differs_from_its_fixed_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    let mut wrong = binding(1)?;
    wrong.owner_scope = "owner-2".to_owned();
    assert!(matches!(
        producer.commit_page(
            clock(101),
            vec![fact(wrong.clone(), "fact-1", 101)],
            cursor(wrong, "cursor-1", 1, 101, &["fact-1"])
        ),
        Err(RiskRevaluationProducerError::Binding)
    ));
    assert!(
        ScalpingRiskJournal::open(&path)?
            .recover()?
            .records
            .is_empty()
    );
    Ok(())
}

#[test]
fn unknown_applied_proof_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    producer.commit_page(
        clock(101),
        vec![fact(binding(1)?, "fact-1", 101)],
        cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"]),
    )?;
    assert!(matches!(
        producer.recover_complete(clock(101), Some("unknown-proof")),
        Err(RiskRevaluationProducerError::UnknownAppliedProof)
    ));
    Ok(())
}

#[test]
fn source_order_is_preserved_while_revalued_facts_are_event_time_sorted()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    let proof = complete(producer.commit_page(
        clock(102),
        vec![
            fact(binding(1)?, "late", 102),
            fact(binding(1)?, "early", 101),
        ],
        cursor(binding(1)?, "cursor-1", 2, 102, &["late", "early"]),
    )?)?;
    assert_eq!(proof.proof.source_fact_ids, vec!["late", "early"]);
    assert_eq!(
        proof
            .proof
            .revalued_facts
            .iter()
            .map(|fact| fact.fact_id.as_str())
            .collect::<Vec<_>>(),
        vec!["early", "late"]
    );
    Ok(())
}

#[test]
fn orphan_facts_without_a_cursor_are_never_recovered_as_a_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let entry = ScalpingRiskEntry::Fact(fact(binding(1)?, "orphan", 101));
    let record = ScalpingRiskRecord {
        sequence: 1,
        content_sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&entry)?)),
        entry,
    };
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    file.write_all(&serde_json::to_vec(&record)?)?;
    file.write_all(b"\n")?;
    file.sync_data()?;

    assert_eq!(open(&path)?.recover_complete(clock(101), None)?, None);
    Ok(())
}

#[test]
fn proof_identity_changes_when_logical_pnl_changes() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let first_path = directory.path().join("first.jsonl");
    let second_path = directory.path().join("second.jsonl");
    let mut first = open(&first_path)?;
    let mut second = open(&second_path)?;
    let first_proof = complete(first.commit_page(
        clock(101),
        vec![fact(binding(1)?, "fact-1", 101)],
        cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"]),
    )?)?;
    let mut changed = fact(binding(1)?, "fact-1", 101);
    changed.fact.realized_pnl = Decimal::NEGATIVE_ONE;
    let second_proof = complete(second.commit_page(
        clock(101),
        vec![changed],
        cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"]),
    )?)?;
    assert_ne!(first_proof.proof.proof_id, second_proof.proof.proof_id);
    Ok(())
}

#[test]
fn intermediate_page_persists_without_a_proof_and_terminal_covers_the_whole_window()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    let mut middle = cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"]);
    middle.has_more = true;
    assert_eq!(
        producer.commit_page(clock(101), vec![fact(binding(1)?, "fact-1", 101)], middle)?,
        None
    );

    let terminal = complete(producer.commit_page(
        clock(102),
        vec![fact(binding(1)?, "fact-2", 102)],
        cursor(binding(1)?, "cursor-2", 2, 102, &["fact-1", "fact-2"]),
    )?)?;
    assert_eq!(terminal.proof.source_fact_ids, vec!["fact-1", "fact-2"]);
    Ok(())
}

#[test]
fn same_sequence_empty_terminal_page_uses_the_accumulated_manifest()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    let mut middle = cursor(binding(1)?, "cursor-middle", 1, 101, &["fact-1"]);
    middle.has_more = true;
    assert_eq!(
        producer.commit_page(clock(101), vec![fact(binding(1)?, "fact-1", 101)], middle)?,
        None
    );
    let terminal = complete(producer.commit_page(
        clock(101),
        Vec::new(),
        cursor(binding(1)?, "cursor-terminal", 1, 101, &["fact-1"]),
    )?)?;
    assert_eq!(terminal.proof.source_fact_ids, vec!["fact-1"]);
    Ok(())
}

#[test]
fn stale_or_future_terminal_pages_do_not_produce_proofs() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let stale_path = directory.path().join("stale.jsonl");
    let future_path = directory.path().join("future.jsonl");
    let mut stale = open(&stale_path)?;
    let mut future = open(&future_path)?;
    assert!(matches!(
        stale.commit_page(
            RiskProofClock {
                now_ms: 200,
                max_stale_ms: 5,
            },
            vec![fact(binding(1)?, "fact-1", 101)],
            cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"])
        ),
        Err(RiskRevaluationProducerError::Replay)
    ));
    assert!(matches!(
        future.commit_page(
            RiskProofClock {
                now_ms: 90,
                max_stale_ms: 5,
            },
            vec![fact(binding(1)?, "fact-1", 101)],
            cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"])
        ),
        Err(RiskRevaluationProducerError::Replay)
    ));
    Ok(())
}

#[test]
fn stale_history_does_not_block_recovery_of_the_latest_fresh_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    let old = complete(producer.commit_page(
        clock(101),
        vec![fact(binding(1)?, "fact-1", 101)],
        cursor(binding(1)?, "cursor-1", 1, 101, &["fact-1"]),
    )?)?;
    let latest = complete(producer.commit_page(
        RiskProofClock {
            now_ms: 200,
            max_stale_ms: 5,
        },
        vec![fact(binding(1)?, "fact-2", 200)],
        cursor(binding(1)?, "cursor-2", 2, 200, &["fact-2"]),
    )?)?;
    drop(producer);

    assert_eq!(
        open(&path)?.recover_complete(
            RiskProofClock {
                now_ms: 200,
                max_stale_ms: 5,
            },
            Some(&old.proof.proof_id),
        )?,
        Some(latest)
    );
    Ok(())
}

#[test]
fn revaluation_can_replace_a_source_fact_in_a_new_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("risk.jsonl");
    let mut producer = open(&path)?;
    complete(producer.commit_page(
        clock(101),
        vec![fact_with_pnl(binding(1)?, "risk-1", 101, Decimal::ONE)],
        cursor(binding(1)?, "cursor-1", 1, 101, &["risk-1"]),
    )?)?;
    let revalued = complete(producer.commit_page(
        clock(102),
        vec![
            fact_with_pnl(binding(2)?, "risk-1", 101, Decimal::NEGATIVE_ONE),
            fact_with_pnl(binding(2)?, "risk-2", 102, Decimal::ONE),
        ],
        cursor(binding(2)?, "cursor-2", 1, 102, &["risk-1", "risk-2"]),
    )?)?;
    assert_eq!(revalued.proof.target_generation, 2);
    assert_eq!(revalued.proof.source_fact_ids, vec!["risk-1", "risk-2"]);
    assert_eq!(
        revalued.proof.revalued_facts[0].realized_pnl,
        Decimal::NEGATIVE_ONE
    );
    Ok(())
}
