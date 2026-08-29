use rust_decimal::Decimal;
use venue::{
    controller::ControlTarget,
    runtime::{
        AnonymousProtectionCustody, RUNTIME_RECOVERY_SCHEMA_VERSION, RecoveryFactValue,
        RuntimeReconciliationFacts, RuntimeRecoveryDirective, RuntimeRecoveryIdentity,
        RuntimeRecoveryPhase, RuntimeRecoveryState, RuntimeTakeoverReceipt, TakeoverCoverage,
    },
    strategy::scalping::StrategyKind,
};

fn identity(
    run_id: &str,
    generation: u64,
) -> Result<RuntimeRecoveryIdentity, Box<dyn std::error::Error>> {
    Ok(RuntimeRecoveryIdentity {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "scalping-primary".to_owned(),
        run_id: run_id.to_owned(),
        owner_scope: "scalping-primary:BTC/USDT".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        authority_root_digest: "a".repeat(64),
        generation,
    })
}

fn custody(identity: &RuntimeRecoveryIdentity, valid_until_ms: u64) -> AnonymousProtectionCustody {
    AnonymousProtectionCustody {
        episode_id: "episode-1".to_owned(),
        custody_fact_id: "custody-1".to_owned(),
        owner_scope: identity.owner_scope.clone(),
        run_id: identity.run_id.clone(),
        authority_root_digest: identity.authority_root_digest.clone(),
        exposure_unit: "BTC".to_owned(),
        remaining_exposure: Decimal::ONE,
        protected_exposure: Decimal::ONE,
        custody_generation: 1,
        exit_group_generation: 1,
        supervisor_generation: 1,
        clock_generation: 1,
        valid_until_ms,
    }
}

fn facts(
    identity: &RuntimeRecoveryIdentity,
    fact_id: &str,
    fact_generation: u64,
    observed_at_ms: u64,
) -> RuntimeReconciliationFacts {
    RuntimeReconciliationFacts {
        fact_id: fact_id.to_owned(),
        fact_generation,
        observed_at_ms,
        owner_scope: identity.owner_scope.clone(),
        run_id: identity.run_id.clone(),
        authority_root_digest: identity.authority_root_digest.clone(),
        runtime_generation: identity.generation,
        private_snapshot_ready: RecoveryFactValue::True,
        owner_conflict: RecoveryFactValue::False,
        execution_unknown: RecoveryFactValue::False,
        flat: RecoveryFactValue::Unknown,
        entry_terminal: RecoveryFactValue::Unknown,
        protection_terminal: RecoveryFactValue::Unknown,
        custody: None,
    }
}

fn flat_receipt(
    predecessor: &RuntimeRecoveryIdentity,
    successor: &RuntimeRecoveryIdentity,
    generation: u64,
    target: ControlTarget,
) -> RuntimeTakeoverReceipt {
    RuntimeTakeoverReceipt {
        schema_version: RUNTIME_RECOVERY_SCHEMA_VERSION,
        receipt_id: format!("takeover-flat-{generation}"),
        generation,
        issued_at_ms: 100,
        valid_until_ms: 1_000,
        predecessor: predecessor.clone(),
        successor: successor.clone(),
        persistent_control_target: target,
        coverage: TakeoverCoverage::StoppedFlat {
            instance_fact_id: "flat-fact".to_owned(),
            open_permission_generation: 4,
        },
    }
}

#[test]
fn takeover_is_exactly_bound_and_never_resumes_from_unknown_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let predecessor = identity("run-1", 1)?;
    let successor = identity("run-2", 2)?;
    let persisted = RuntimeRecoveryState::new(predecessor.clone(), Some(ControlTarget::Running))?;
    let mut recovery = RuntimeRecoveryState::restore_for(persisted, successor.clone())?;
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::Isolated);

    let mut wrong_generation = flat_receipt(&predecessor, &successor, 2, ControlTarget::Running);
    assert!(recovery.apply_takeover(&wrong_generation, 150).is_err());
    wrong_generation.generation = 1;
    wrong_generation.persistent_control_target = ControlTarget::StopAndProtect;
    assert!(recovery.apply_takeover(&wrong_generation, 150).is_err());

    let receipt = flat_receipt(&predecessor, &successor, 1, ControlTarget::Running);
    assert_eq!(
        recovery.apply_takeover(&receipt, 150)?,
        RuntimeRecoveryDirective::ReconcileOnly
    );
    assert_eq!(recovery.active_identity, successor);
    assert_eq!(recovery.effective_control_target(), ControlTarget::Running);

    let mut unknown = facts(&recovery.active_identity, "unknown", 1, 200);
    let mut wrong_owner = unknown.clone();
    wrong_owner.owner_scope = "another-owner".to_owned();
    assert!(recovery.project(&wrong_owner).is_err());
    let mut wrong_root = unknown.clone();
    wrong_root.authority_root_digest = "b".repeat(64);
    assert!(recovery.project(&wrong_root).is_err());
    let mut wrong_generation = unknown.clone();
    wrong_generation.runtime_generation += 1;
    assert!(recovery.project(&wrong_generation).is_err());
    unknown.private_snapshot_ready = RecoveryFactValue::Unknown;
    assert_eq!(
        recovery.project(&unknown)?,
        RuntimeRecoveryDirective::ReconcileOnly
    );
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::Reconciling);

    let mut flat = facts(&recovery.active_identity, "flat", 2, 201);
    flat.flat = RecoveryFactValue::True;
    flat.entry_terminal = RecoveryFactValue::True;
    flat.protection_terminal = RecoveryFactValue::True;
    assert_eq!(
        recovery.project(&flat)?,
        RuntimeRecoveryDirective::StoppedFlat
    );
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::StoppedFlat);
    assert_eq!(recovery.effective_control_target(), ControlTarget::Running);
    Ok(())
}

#[test]
fn missing_target_defaults_to_stop_and_protected_terminal_is_continuous()
-> Result<(), Box<dyn std::error::Error>> {
    let active = identity("run-protected", 3)?;
    let mut recovery = RuntimeRecoveryState::new(active.clone(), None)?;
    assert_eq!(
        recovery.effective_control_target(),
        ControlTarget::StopAndProtect
    );

    let current_custody = custody(&active, 120);
    let mut unknown_protection = facts(&active, "unknown-protection", 1, 99);
    unknown_protection.flat = RecoveryFactValue::False;
    unknown_protection.entry_terminal = RecoveryFactValue::True;
    unknown_protection.custody = Some(current_custody.clone());
    assert_eq!(
        recovery.project(&unknown_protection)?,
        RuntimeRecoveryDirective::ReconcileOnly
    );

    let mut protected = facts(&active, "protected-1", 2, 100);
    protected.flat = RecoveryFactValue::False;
    protected.entry_terminal = RecoveryFactValue::True;
    protected.protection_terminal = RecoveryFactValue::False;
    protected.custody = Some(current_custody.clone());
    assert_eq!(
        recovery.project(&protected)?,
        RuntimeRecoveryDirective::StoppedProtected
    );
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::StoppedProtected);

    let mut continuous = facts(&active, "protected-2", 3, 109);
    continuous.flat = RecoveryFactValue::False;
    continuous.entry_terminal = RecoveryFactValue::True;
    continuous.protection_terminal = RecoveryFactValue::False;
    continuous.custody = Some(current_custody.clone());
    assert_eq!(
        recovery.project(&continuous)?,
        RuntimeRecoveryDirective::StoppedProtected
    );

    let mut expired = facts(&active, "protected-expired", 4, 120);
    expired.flat = RecoveryFactValue::False;
    expired.entry_terminal = RecoveryFactValue::True;
    expired.protection_terminal = RecoveryFactValue::False;
    expired.custody = Some(current_custody);
    assert!(matches!(
        recovery.project(&expired)?,
        RuntimeRecoveryDirective::LowerRisk {
            repair_protection: true,
            ..
        }
    ));
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::LoweringRisk);

    let mut unknown = facts(&active, "unknown-after-expiry", 5, 121);
    unknown.execution_unknown = RecoveryFactValue::Unknown;
    assert_eq!(
        recovery.project(&unknown)?,
        RuntimeRecoveryDirective::ReconcileOnly
    );

    let mut flat = facts(&active, "flat-after-expiry", 6, 122);
    flat.flat = RecoveryFactValue::True;
    flat.entry_terminal = RecoveryFactValue::True;
    flat.protection_terminal = RecoveryFactValue::True;
    assert_eq!(
        recovery.project(&flat)?,
        RuntimeRecoveryDirective::StoppedFlat
    );
    let encoded = serde_json::to_vec(&recovery)?;
    let persisted: RuntimeRecoveryState = serde_json::from_slice(&encoded)?;
    let restored = RuntimeRecoveryState::restore_for(persisted, active)?;
    assert_eq!(restored.phase(), RuntimeRecoveryPhase::Reconciling);
    Ok(())
}

#[test]
fn protected_takeover_keeps_predecessor_active_until_later_flat_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let predecessor = identity("run-old", 7)?;
    let successor = identity("run-new", 8)?;
    let persisted = RuntimeRecoveryState::new(predecessor.clone(), None)?;
    let mut recovery = RuntimeRecoveryState::restore_for(persisted, successor.clone())?;
    let protected = RuntimeTakeoverReceipt {
        schema_version: RUNTIME_RECOVERY_SCHEMA_VERSION,
        receipt_id: "takeover-protected-1".to_owned(),
        generation: 1,
        issued_at_ms: 100,
        valid_until_ms: 1_000,
        predecessor: predecessor.clone(),
        successor: successor.clone(),
        persistent_control_target: ControlTarget::StopAndProtect,
        coverage: TakeoverCoverage::StoppedProtected {
            custody: custody(&predecessor, 900),
        },
    };
    assert_eq!(
        recovery.apply_takeover(&protected, 150)?,
        RuntimeRecoveryDirective::ReconcileOnly
    );
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::ProtectionOnly);
    assert_eq!(recovery.active_identity, predecessor);
    assert_eq!(recovery.pending_successor.as_ref(), Some(&successor));
    assert!(
        recovery
            .project(&facts(&successor, "foreign-successor", 1, 151))
            .is_err()
    );

    let encoded = serde_json::to_vec(&recovery)?;
    let persisted: RuntimeRecoveryState = serde_json::from_slice(&encoded)?;
    recovery = RuntimeRecoveryState::restore_for(persisted, predecessor.clone())?;
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::ProtectionOnly);
    assert_eq!(recovery.pending_successor.as_ref(), Some(&successor));

    let flat = flat_receipt(&predecessor, &successor, 2, ControlTarget::StopAndProtect);
    assert_eq!(
        recovery.apply_takeover(&flat, 180)?,
        RuntimeRecoveryDirective::ReconcileOnly
    );
    assert_eq!(recovery.active_identity, successor);
    assert_eq!(recovery.pending_successor, None);
    assert_eq!(recovery.phase(), RuntimeRecoveryPhase::Reconciling);
    Ok(())
}
