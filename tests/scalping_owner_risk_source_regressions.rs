use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset},
    runtime::{
        RiskProofClock, ScalpingOwnerRiskPage, ScalpingOwnerRiskSource,
        ScalpingOwnerRiskSourceError, ScalpingOwnerRiskTurn,
        scalping_owner_risk_source_checkpoint_path,
    },
    storage::{ScalpingRiskBinding, ScalpingRiskCursor, ScalpingRiskFact},
    strategy::scalping::{RiskFact, RiskUnit, StrategyBinding, StrategyKind},
};

fn unit() -> Result<RiskUnit, Box<dyn std::error::Error>> {
    Ok(RiskUnit::new("logical-risk")?)
}

fn strategy_binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "owner-risk-source".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDT".parse()?,
        parameter_release_id: "release-1".to_owned(),
        owner_scope: "owner-risk-source:shadow-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::ONE),
    })
}

fn risk_binding(generation: u64) -> Result<ScalpingRiskBinding, Box<dyn std::error::Error>> {
    let binding = strategy_binding()?;
    Ok(ScalpingRiskBinding {
        exchange: binding.exchange,
        account: binding.account,
        owner_scope: binding.owner_scope,
        strategy_instance_id: binding.strategy_instance_id,
        run_id: binding.run_id,
        parameter_release_id: binding.parameter_release_id,
        symbol: binding.symbol,
        risk_unit: unit()?,
        valuation_generation: generation,
    })
}

fn fact(binding: ScalpingRiskBinding, fact_id: &str, event_time_ms: u64) -> ScalpingRiskFact {
    ScalpingRiskFact {
        fact: RiskFact {
            fact_id: fact_id.to_owned(),
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
    cursor_id: &str,
    source_sequence: u64,
    observed_through_ms: u64,
    has_more: bool,
    source_fact_ids: &[&str],
) -> ScalpingRiskCursor {
    ScalpingRiskCursor {
        cursor_id: cursor_id.to_owned(),
        binding,
        source_sequence,
        complete_from_ms: 100,
        observed_through_ms,
        has_more,
        source_fact_ids: source_fact_ids
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
    }
}

fn clock(now_ms: u64) -> RiskProofClock {
    RiskProofClock {
        now_ms,
        max_stale_ms: 5,
    }
}

fn open(path: &std::path::Path) -> Result<ScalpingOwnerRiskSource, Box<dyn std::error::Error>> {
    Ok(ScalpingOwnerRiskSource::open(
        path,
        &strategy_binding()?,
        unit()?,
    )?)
}

#[test]
fn source_commits_one_page_per_turn_then_outputs_one_terminal_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("owner-risk.jsonl");
    let mut source = open(&path)?;
    let first_cursor = cursor(risk_binding(1)?, "cursor-1", 1, 101, true, &["fact-1"]);
    let first = source.drive_turn(
        clock(101),
        None,
        Some(ScalpingOwnerRiskPage {
            requested_after: None,
            facts: vec![fact(risk_binding(1)?, "fact-1", 101)],
            cursor: first_cursor.clone(),
        }),
    )?;
    assert!(matches!(
        first,
        ScalpingOwnerRiskTurn::PageCommitted { ref resume_after }
            if resume_after == &first_cursor
    ));

    let terminal_cursor = cursor(
        risk_binding(1)?,
        "cursor-2",
        2,
        102,
        false,
        &["fact-1", "fact-2"],
    );
    let terminal = source.drive_turn(
        clock(102),
        None,
        Some(ScalpingOwnerRiskPage {
            requested_after: Some(first_cursor),
            facts: vec![fact(risk_binding(1)?, "fact-2", 102)],
            cursor: terminal_cursor.clone(),
        }),
    )?;
    let ScalpingOwnerRiskTurn::Proof {
        proof,
        resume_after,
    } = terminal
    else {
        return Err("terminal page did not output a proof".into());
    };
    assert_eq!(resume_after, terminal_cursor);
    assert_eq!(proof.proof.source_fact_ids, ["fact-1", "fact-2"]);
    assert_eq!(proof.proof.revalued_facts.len(), 2);
    let first_proof_id = proof.proof.proof_id;

    let replacement = source.drive_turn(
        clock(103),
        Some(&first_proof_id),
        Some(ScalpingOwnerRiskPage {
            requested_after: None,
            facts: vec![fact(risk_binding(2)?, "fact-1", 103)],
            cursor: cursor(
                risk_binding(2)?,
                "cursor-generation-2",
                1,
                103,
                false,
                &["fact-1"],
            ),
        }),
    )?;
    assert!(matches!(
        replacement,
        ScalpingOwnerRiskTurn::Proof { ref proof, .. } if proof.proof.target_generation == 2
    ));
    Ok(())
}

#[test]
fn reopened_source_uses_durable_cursor_and_emits_pending_proof_before_new_page()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("owner-risk.jsonl");
    let first_cursor = cursor(risk_binding(1)?, "cursor-1", 1, 101, false, &["fact-1"]);
    let proof_id = {
        let mut source = open(&path)?;
        let turn = source.drive_turn(
            clock(101),
            None,
            Some(ScalpingOwnerRiskPage {
                requested_after: None,
                facts: vec![fact(risk_binding(1)?, "fact-1", 101)],
                cursor: first_cursor.clone(),
            }),
        )?;
        let ScalpingOwnerRiskTurn::Proof { proof, .. } = turn else {
            return Err("first terminal page did not emit proof".into());
        };
        proof.proof.proof_id
    };

    let mut reopened = open(&path)?;
    assert_eq!(reopened.resume_after(), Some(&first_cursor));
    assert!(matches!(
        reopened.drive_turn(clock(101), None, None)?,
        ScalpingOwnerRiskTurn::PendingProof { ref proof } if proof.proof.proof_id == proof_id
    ));
    assert!(matches!(
        reopened.drive_turn(clock(101), Some(&proof_id), None)?,
        ScalpingOwnerRiskTurn::Idle {
            resume_after: Some(ref cursor)
        } if cursor == &first_cursor
    ));
    Ok(())
}

#[test]
fn unknown_applied_proof_and_wrong_requested_cursor_fence_source()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("owner-risk.jsonl");
    let mut source = open(&path)?;
    assert!(matches!(
        source.drive_turn(clock(101), Some("unknown-proof"), None),
        Err(ScalpingOwnerRiskSourceError::Producer(_))
    ));
    assert!(matches!(
        source.drive_turn(clock(101), None, None),
        Err(ScalpingOwnerRiskSourceError::Fenced)
    ));
    let mut reopened = open(&path)?;
    assert!(reopened.is_fenced());
    assert!(matches!(
        reopened.drive_turn(clock(101), None, None),
        Err(ScalpingOwnerRiskSourceError::Fenced)
    ));

    let second_path = directory.path().join("wrong-cursor.jsonl");
    let mut second = open(&second_path)?;
    assert!(matches!(
        second.drive_turn(
            clock(101),
            None,
            Some(ScalpingOwnerRiskPage {
                requested_after: Some(cursor(risk_binding(1)?, "wrong", 9, 101, false, &[])),
                facts: vec![fact(risk_binding(1)?, "fact-1", 101)],
                cursor: cursor(risk_binding(1)?, "cursor-1", 1, 101, false, &["fact-1"]),
            })
        ),
        Err(ScalpingOwnerRiskSourceError::Cursor)
    ));
    assert!(matches!(
        second.drive_turn(clock(101), None, None),
        Err(ScalpingOwnerRiskSourceError::Fenced)
    ));
    let reopened_second = open(&second_path)?;
    assert!(reopened_second.is_fenced());
    Ok(())
}

#[test]
fn tampered_source_fence_checkpoint_is_rejected_on_reopen() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let path = directory.path().join("owner-risk.jsonl");
    let mut source = open(&path)?;
    assert!(matches!(
        source.drive_turn(clock(101), Some("unknown-proof"), None),
        Err(ScalpingOwnerRiskSourceError::Producer(_))
    ));
    let checkpoint_path = scalping_owner_risk_source_checkpoint_path(&path);
    let mut checkpoint: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&checkpoint_path)?)?;
    checkpoint["fenced_reason"] = serde_json::Value::String("cursor".to_owned());
    std::fs::write(&checkpoint_path, serde_json::to_vec(&checkpoint)?)?;

    assert!(matches!(
        ScalpingOwnerRiskSource::open(&path, &strategy_binding()?, unit()?),
        Err(ScalpingOwnerRiskSourceError::Checkpoint)
    ));
    Ok(())
}

#[test]
fn conflicting_retry_content_is_rejected_and_fenced() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("owner-risk.jsonl");
    let mut source = open(&path)?;
    let terminal_cursor = cursor(risk_binding(1)?, "cursor-1", 1, 101, false, &["fact-1"]);
    let proof_id = match source.drive_turn(
        clock(101),
        None,
        Some(ScalpingOwnerRiskPage {
            requested_after: None,
            facts: vec![fact(risk_binding(1)?, "fact-1", 101)],
            cursor: terminal_cursor.clone(),
        }),
    )? {
        ScalpingOwnerRiskTurn::Proof { proof, .. } => proof.proof.proof_id,
        _ => return Err("terminal page did not emit proof".into()),
    };
    let mut conflicting = fact(risk_binding(1)?, "fact-1", 101);
    conflicting.fact.realized_pnl = Decimal::NEGATIVE_ONE;
    assert!(matches!(
        source.drive_turn(
            clock(101),
            Some(&proof_id),
            Some(ScalpingOwnerRiskPage {
                requested_after: None,
                facts: vec![conflicting],
                cursor: terminal_cursor,
            }),
        ),
        Err(ScalpingOwnerRiskSourceError::Producer(_))
    ));
    assert!(matches!(
        source.drive_turn(clock(101), Some(&proof_id), None),
        Err(ScalpingOwnerRiskSourceError::Fenced)
    ));
    Ok(())
}

#[test]
fn cursor_rollback_and_binding_drift_are_rejected_before_any_later_turn()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let rollback_path = directory.path().join("rollback.jsonl");
    let mut rollback = open(&rollback_path)?;
    let current = cursor(risk_binding(1)?, "cursor-2", 2, 101, true, &["fact-1"]);
    rollback.drive_turn(
        clock(101),
        None,
        Some(ScalpingOwnerRiskPage {
            requested_after: None,
            facts: vec![fact(risk_binding(1)?, "fact-1", 101)],
            cursor: current.clone(),
        }),
    )?;
    assert!(matches!(
        rollback.drive_turn(
            clock(102),
            None,
            Some(ScalpingOwnerRiskPage {
                requested_after: Some(current),
                facts: vec![fact(risk_binding(1)?, "fact-2", 102)],
                cursor: cursor(risk_binding(1)?, "cursor-1", 1, 102, false, &["fact-2"]),
            }),
        ),
        Err(ScalpingOwnerRiskSourceError::Producer(_))
    ));
    assert!(matches!(
        rollback.drive_turn(clock(102), None, None),
        Err(ScalpingOwnerRiskSourceError::Fenced)
    ));

    let binding_path = directory.path().join("binding.jsonl");
    let mut binding_drift = open(&binding_path)?;
    let mut wrong_binding = risk_binding(1)?;
    wrong_binding.owner_scope = "other-owner".to_owned();
    assert!(matches!(
        binding_drift.drive_turn(
            clock(101),
            None,
            Some(ScalpingOwnerRiskPage {
                requested_after: None,
                facts: vec![fact(wrong_binding.clone(), "fact-1", 101)],
                cursor: cursor(wrong_binding, "cursor-1", 1, 101, false, &["fact-1"]),
            }),
        ),
        Err(ScalpingOwnerRiskSourceError::Producer(_))
    ));
    assert!(matches!(
        binding_drift.drive_turn(clock(101), None, None),
        Err(ScalpingOwnerRiskSourceError::Fenced)
    ));
    Ok(())
}

#[test]
fn valuation_generation_cannot_restart_backwards_from_an_empty_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("generation-rollback.jsonl");
    let mut source = open(&path)?;
    let current = cursor(risk_binding(2)?, "generation-2", 1, 101, false, &["fact-2"]);
    let applied = match source.drive_turn(
        clock(101),
        None,
        Some(ScalpingOwnerRiskPage {
            requested_after: None,
            facts: vec![fact(risk_binding(2)?, "fact-2", 101)],
            cursor: current,
        }),
    )? {
        ScalpingOwnerRiskTurn::Proof { proof, .. } => proof.proof.proof_id,
        _ => return Err("generation 2 terminal page did not emit proof".into()),
    };

    assert!(matches!(
        source.drive_turn(
            clock(102),
            Some(&applied),
            Some(ScalpingOwnerRiskPage {
                requested_after: None,
                facts: vec![fact(risk_binding(1)?, "fact-1", 102)],
                cursor: cursor(risk_binding(1)?, "generation-1", 1, 102, false, &["fact-1"]),
            }),
        ),
        Err(ScalpingOwnerRiskSourceError::Cursor)
    ));
    assert!(matches!(
        source.drive_turn(clock(102), Some(&applied), None),
        Err(ScalpingOwnerRiskSourceError::Fenced)
    ));
    Ok(())
}
