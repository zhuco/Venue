use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset},
    runtime::{
        AppliedRiskFenceReason, AppliedRiskOwnerTurn, BoundRiskRevaluation, ControlDisposition,
        CustodyStatus, EntryDisposition, LifecycleReport, PrivateEntryGateReport, PrivateFacts,
        ScalpingAppliedRiskOwner, ScalpingAppliedRiskOwnerError, ScalpingResidentRuntime,
        ScalpingResidentSources, ScalpingResidentSourcesConfig, ScalpingShadowHost,
    },
    storage::ScalpingRiskBinding,
    strategy::scalping::{
        ExposureState, ProtectionState, RiskRevaluation, RiskUnit, SafetyProjection,
        ScalpingParams, StrategyBinding, StrategyKind,
    },
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "applied-risk-owner".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "SOL/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "applied-risk-owner:shadow-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(10, 0)),
    })
}

fn params(binding: &StrategyBinding) -> ScalpingParams {
    ScalpingParams::shadow(binding.risk_budget.clone())
}

fn private(generation: u64, observed_at_ms: u64) -> PrivateEntryGateReport {
    PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Armed,
            control: ControlDisposition::None,
        },
        entry_ready: true,
        forwarded_private: Some(PrivateFacts {
            generation,
            observed_at_ms,
            root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:1"),
            safety: SafetyProjection {
                private_snapshot_ready: true,
                exposure: ExposureState::Flat,
                execution_unknown: false,
                protection: ProtectionState::Complete,
                owner_conflict: false,
                risk_budget_available: true,
            },
            custody: CustodyStatus::Complete,
        }),
        control: None,
    }
}

fn bound(
    binding: &StrategyBinding,
    unit: &RiskUnit,
    cursor_sequence: u64,
    proof_id: &str,
    generation: u64,
    through_ms: u64,
) -> BoundRiskRevaluation {
    BoundRiskRevaluation {
        binding: ScalpingRiskBinding {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            owner_scope: binding.owner_scope.clone(),
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            parameter_release_id: binding.parameter_release_id.clone(),
            symbol: binding.symbol.clone(),
            risk_unit: unit.clone(),
            valuation_generation: generation,
        },
        proof: RiskRevaluation {
            proof_id: proof_id.to_owned(),
            target_generation: generation,
            risk_unit: unit.clone(),
            window_start_ms: 0,
            complete_through_ms: through_ms,
            source_fact_ids: Vec::new(),
            revalued_facts: Vec::new(),
        },
        cursor_sequence,
    }
}

#[test]
fn normal_apply_persists_receipt_only_after_host_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let mut host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    host.on_private_gate(&private(1, 100))?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 100);
    let owner_path = directory.path().join("receipt.json");
    let mut owner =
        ScalpingAppliedRiskOwner::open_or_restore(&owner_path, binding.clone(), unit.clone())?;

    assert_eq!(
        owner.turn(&host.checkpoint(), &proof)?,
        AppliedRiskOwnerTurn::ApplyRequired
    );
    assert!(owner.last_ack().is_none());
    host.on_bound_risk_revaluation(proof.clone())?;
    let AppliedRiskOwnerTurn::Persisted(receipt) = owner.turn(&host.checkpoint(), &proof)? else {
        return Err("receipt was not persisted".into());
    };
    assert_eq!(receipt.binding, binding);
    assert_eq!(receipt.target_generation, 1);
    assert_eq!(receipt.valuation_generation, 1);
    assert_eq!(owner.last_ack(), Some(&receipt));

    drop(owner);
    let mut restored =
        ScalpingAppliedRiskOwner::open_or_restore(&owner_path, receipt.binding.clone(), unit)?;
    assert_eq!(restored.last_ack_proof_id(), Some("proof-1"));
    assert!(matches!(
        restored.turn(&host.checkpoint(), &proof)?,
        AppliedRiskOwnerTurn::Duplicate(_)
    ));
    Ok(())
}

#[test]
fn host_save_failure_never_creates_receipt() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let host_path = directory.path().join("host.json");
    let mut host = ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params)?;
    host.on_private_gate(&private(1, 100))?;
    std::fs::remove_file(&host_path)?;
    std::fs::create_dir(&host_path)?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 100);
    assert!(host.on_bound_risk_revaluation(proof.clone()).is_err());

    let mut owner = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("receipt.json"),
        binding.clone(),
        unit.clone(),
    )?;
    assert_eq!(
        owner.turn(&host.checkpoint(), &proof)?,
        AppliedRiskOwnerTurn::ApplyRequired
    );
    assert!(owner.last_ack().is_none());
    Ok(())
}

#[test]
fn host_applied_receipt_save_crash_is_backfilled_without_host_reapply()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let host_path = directory.path().join("host.json");
    let mut host =
        ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params.clone())?;
    host.on_private_gate(&private(1, 100))?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 100);
    host.on_bound_risk_revaluation(proof.clone())?;

    let owner_path = directory.path().join("receipt.json");
    let mut owner =
        ScalpingAppliedRiskOwner::open_or_restore(&owner_path, binding.clone(), unit.clone())?;
    std::fs::create_dir(&owner_path)?;
    assert!(owner.turn(&host.checkpoint(), &proof).is_err());
    assert!(owner.last_ack().is_none());
    drop(owner);
    std::fs::remove_dir(&owner_path)?;

    drop(host);
    let host = ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params)?;
    assert!(host.awaiting_private_recovery());
    let mut restored = ScalpingAppliedRiskOwner::open_or_restore(&owner_path, binding, unit)?;
    assert!(matches!(
        restored.turn(&host.checkpoint(), &proof)?,
        AppliedRiskOwnerTurn::Persisted(_)
    ));
    assert_eq!(host.checkpoint().last_risk_cursor_sequence, Some(1));
    Ok(())
}

#[test]
fn source_applies_then_recovers_receipt_from_owner_ack_not_host_guess()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let host_path = directory.path().join("host.json");
    let mut host =
        ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params.clone())?;
    host.on_private_gate(&private(1, 100))?;
    let resident = ScalpingResidentRuntime::new(host);
    let mut sources = ScalpingResidentSources::open_recovered(
        resident,
        ScalpingResidentSourcesConfig {
            artifacts_root: directory.path().join("sources"),
            binding: binding.clone(),
            params: params.clone(),
            mark_stale_after_ms: 65_000,
        },
    )?;
    sources.drive_control_private(None, Some(private(2, 101)), None)?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 101);
    let applied = sources.drive_applied_risk(proof.clone())?;
    assert!(applied.resident.is_some());
    assert!(!applied.recovered_after_host_apply);
    assert_eq!(sources.applied_risk_last_ack_proof_id(), Some("proof-1"));
    assert_eq!(sources.status().applied_risk_ack_cursor_sequence, Some(1));

    drop(sources);
    let host = ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params.clone())?;
    let restored = ScalpingResidentSources::open_recovered(
        ScalpingResidentRuntime::new(host),
        ScalpingResidentSourcesConfig {
            artifacts_root: directory.path().join("sources"),
            binding,
            params,
            mark_stale_after_ms: 65_000,
        },
    )?;
    assert_eq!(restored.applied_risk_last_ack_proof_id(), Some("proof-1"));
    Ok(())
}

#[test]
fn source_recovery_backfills_host_applied_receipt_while_still_awaiting_private()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let host_path = directory.path().join("host.json");
    let mut host =
        ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params.clone())?;
    host.on_private_gate(&private(1, 100))?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 100);
    host.on_bound_risk_revaluation(proof.clone())?;
    drop(host);

    let host = ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params.clone())?;
    let mut sources = ScalpingResidentSources::open_recovered(
        ScalpingResidentRuntime::new(host),
        ScalpingResidentSourcesConfig {
            artifacts_root: directory.path().join("sources"),
            binding,
            params,
            mark_stale_after_ms: 65_000,
        },
    )?;
    assert!(sources.status().awaiting_private_recovery);
    let recovered = sources.recover_applied_risk_receipt(&proof)?;
    assert!(recovered.recovered_after_host_apply);
    assert!(recovered.resident.is_none());
    assert!(sources.status().awaiting_private_recovery);
    assert_eq!(sources.applied_risk_last_ack_proof_id(), Some("proof-1"));
    Ok(())
}

#[test]
fn rollback_equivocation_binding_and_unknown_host_proof_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let mut host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    host.on_private_gate(&private(1, 100))?;
    let first = bound(&binding, &unit, 2, "proof-2", 2, 100);
    host.on_bound_risk_revaluation(first.clone())?;
    let mut owner = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("receipt.json"),
        binding.clone(),
        unit.clone(),
    )?;
    assert!(matches!(
        owner.turn(&host.checkpoint(), &first)?,
        AppliedRiskOwnerTurn::Persisted(_)
    ));

    let rollback = bound(&binding, &unit, 1, "proof-1", 1, 100);
    assert!(owner.turn(&host.checkpoint(), &rollback).is_err());
    assert!(owner.is_fenced());

    let mut equivocation = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("receipt.json"),
        binding.clone(),
        unit.clone(),
    )?;
    let same_cursor = bound(&binding, &unit, 2, "proof-other", 2, 100);
    assert!(equivocation.turn(&host.checkpoint(), &same_cursor).is_err());

    let mut cross_binding = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("other.json"),
        binding.clone(),
        unit.clone(),
    )?;
    let mut wrong = first.clone();
    wrong.binding.owner_scope = "other-owner".to_owned();
    assert!(cross_binding.turn(&host.checkpoint(), &wrong).is_err());

    let mut cross_unit = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("wrong-unit.json"),
        binding.clone(),
        unit.clone(),
    )?;
    let other_unit = RiskUnit::new("other-risk")?;
    let wrong_unit = bound(&binding, &other_unit, 2, "proof-2", 2, 100);
    assert!(cross_unit.turn(&host.checkpoint(), &wrong_unit).is_err());

    let mut cross_generation = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("wrong-generation.json"),
        binding.clone(),
        unit.clone(),
    )?;
    let mut wrong_generation = first.clone();
    wrong_generation.proof.target_generation = 3;
    assert!(
        cross_generation
            .turn(&host.checkpoint(), &wrong_generation)
            .is_err()
    );

    let mut unknown = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("unknown.json"),
        binding,
        unit,
    )?;
    let newer = bound(
        &unknown.checkpoint().binding,
        &unknown.checkpoint().risk_unit,
        3,
        "proof-3",
        3,
        100,
    );
    assert!(unknown.turn(&host.checkpoint(), &newer).is_err());
    assert!(unknown.is_fenced());
    Ok(())
}

#[test]
fn same_proof_cursor_from_wrong_host_binding_is_fenced() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let mut host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    host.on_private_gate(&private(1, 100))?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 100);
    host.on_bound_risk_revaluation(proof.clone())?;
    let mut wrong_host = host.checkpoint();
    wrong_host.strategy.binding_digest = "f".repeat(64);
    let mut owner = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("receipt.json"),
        binding.clone(),
        unit.clone(),
    )?;

    assert!(owner.turn(&wrong_host, &proof).is_err());
    assert!(owner.is_fenced());
    assert!(owner.last_ack().is_none());
    assert_eq!(
        owner.checkpoint().fenced_reason,
        Some(AppliedRiskFenceReason::Binding)
    );
    drop(owner);

    let mut restored = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("receipt.json"),
        binding.clone(),
        unit.clone(),
    )?;
    assert!(restored.is_fenced());
    assert!(matches!(
        restored.turn(&host.checkpoint(), &proof),
        Err(ScalpingAppliedRiskOwnerError::Fenced)
    ));

    let mut malformed_host = host.checkpoint();
    malformed_host.last_risk_proof_id = None;
    let mut malformed = ScalpingAppliedRiskOwner::open_or_restore(
        directory.path().join("malformed-host.json"),
        binding,
        unit,
    )?;
    assert!(malformed.turn(&malformed_host, &proof).is_err());
    assert!(malformed.is_fenced());
    Ok(())
}

#[test]
fn duplicate_rechecks_host_risk_projection_generation_and_proof_id()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let mut host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    host.on_private_gate(&private(1, 100))?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 100);
    host.on_bound_risk_revaluation(proof.clone())?;
    let seed_path = directory.path().join("seed.json");
    let mut seed =
        ScalpingAppliedRiskOwner::open_or_restore(&seed_path, binding.clone(), unit.clone())?;
    assert!(matches!(
        seed.turn(&host.checkpoint(), &proof)?,
        AppliedRiskOwnerTurn::Persisted(_)
    ));
    drop(seed);

    let generation_path = directory.path().join("generation.json");
    std::fs::copy(&seed_path, &generation_path)?;
    let mut wrong_generation = host.checkpoint();
    wrong_generation.strategy.risk.valuation_generation = Some(2);
    let mut generation_owner =
        ScalpingAppliedRiskOwner::open_or_restore(generation_path, binding.clone(), unit.clone())?;
    assert!(generation_owner.turn(&wrong_generation, &proof).is_err());
    assert_eq!(
        generation_owner.checkpoint().fenced_reason,
        Some(AppliedRiskFenceReason::UnknownHostProof)
    );

    let proof_id_path = directory.path().join("proof-id.json");
    std::fs::copy(&seed_path, &proof_id_path)?;
    let mut wrong_proof_id = host.checkpoint();
    wrong_proof_id.strategy.risk.last_revaluation_id = Some("other-proof".to_owned());
    let mut proof_id_owner =
        ScalpingAppliedRiskOwner::open_or_restore(proof_id_path, binding, unit)?;
    assert!(proof_id_owner.turn(&wrong_proof_id, &proof).is_err());
    assert_eq!(
        proof_id_owner.checkpoint().fenced_reason,
        Some(AppliedRiskFenceReason::UnknownHostProof)
    );
    Ok(())
}

#[test]
fn tampered_durable_fence_checkpoint_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = params(&binding);
    let unit = params.risk_per_episode.unit.clone();
    let mut host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    host.on_private_gate(&private(1, 100))?;
    let proof = bound(&binding, &unit, 1, "proof-1", 1, 100);
    let owner_path = directory.path().join("receipt.json");
    let mut owner =
        ScalpingAppliedRiskOwner::open_or_restore(&owner_path, binding.clone(), unit.clone())?;
    let mut wrong_host = host.checkpoint();
    wrong_host.strategy.binding_digest = "e".repeat(64);
    assert!(owner.turn(&wrong_host, &proof).is_err());
    drop(owner);

    let mut encoded: serde_json::Value = serde_json::from_slice(&std::fs::read(&owner_path)?)?;
    encoded["fenced_reason"] = serde_json::Value::Null;
    std::fs::write(&owner_path, serde_json::to_vec(&encoded)?)?;
    assert!(ScalpingAppliedRiskOwner::open_or_restore(owner_path, binding, unit).is_err());
    Ok(())
}
