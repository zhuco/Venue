use venue::runtime::{
    ControlDisposition, EntryDisposition, ExecutionProjection, OwnerProjection, PrivateEntryGate,
    PrivateEntryGateInput, PrivateExposure, PrivateFactsProjectionInput, PrivateFactsReadiness,
    PrivateProjection, ProtectionProjection, RiskBudgetProjection,
};

fn readiness() -> PrivateFactsReadiness {
    PrivateFactsReadiness {
        generation: 8,
        observed_at_ms: 400,
        root_cause_fact_id: "private-readback:8:400:0".to_owned(),
        exposure: PrivateExposure::Flat,
        ordinary_order_debt: false,
        algo_order_debt: false,
    }
}

fn projection<T>(value: T) -> PrivateProjection<T> {
    PrivateProjection {
        generation: 8,
        observed_at_ms: 400,
        value,
    }
}

fn projections() -> PrivateFactsProjectionInput {
    PrivateFactsProjectionInput {
        execution: projection(ExecutionProjection::Known),
        owner: projection(OwnerProjection::Clear),
        protection: projection(ProtectionProjection::Complete),
        risk_budget: projection(RiskBudgetProjection::Available),
    }
}

fn input(active_episode: bool) -> PrivateEntryGateInput {
    PrivateEntryGateInput {
        active_episode,
        entry_requested: true,
        now_ms: 500,
    }
}

#[test]
fn only_complete_same_epoch_projections_arm_and_forward_private_facts() {
    let mut gate = PrivateEntryGate::new();
    let accepted = gate.observe_readiness(Some(readiness()), projections(), input(false));
    assert!(accepted.entry_ready);
    assert_eq!(accepted.lifecycle.entry, EntryDisposition::Armed);
    assert!(accepted.forwarded_private.is_some());
    assert_eq!(accepted.coordinator_inputs().len(), 1);

    let duplicate = gate.observe_readiness(Some(readiness()), projections(), input(false));
    assert!(duplicate.entry_ready);
    assert!(duplicate.forwarded_private.is_none());
    assert!(duplicate.coordinator_inputs().is_empty());
}

#[test]
fn wrong_generation_watermark_or_unknown_source_cannot_build_private_facts() {
    let mut wrong_generation = projections();
    wrong_generation.execution.generation = 9;
    assert!(wrong_generation.build(readiness()).is_none());

    let mut wrong_watermark = projections();
    wrong_watermark.protection.observed_at_ms = 401;
    assert!(wrong_watermark.build(readiness()).is_none());

    let mut unknown = projections();
    unknown.owner.value = OwnerProjection::Unknown;
    assert!(unknown.build(readiness()).is_none());
}

#[test]
fn revoking_a_previously_ready_private_epoch_stops_active_episodes_first() {
    let mut gate = PrivateEntryGate::new();
    let _ = gate.observe_readiness(Some(readiness()), projections(), input(false));

    let revoked = gate.observe_readiness(None, projections(), input(true));
    assert!(!revoked.entry_ready);
    assert_eq!(revoked.lifecycle.entry, EntryDisposition::Disarmed);
    assert_eq!(
        revoked.lifecycle.control,
        ControlDisposition::StopAndProtect
    );
    assert_eq!(revoked.coordinator_inputs().len(), 1);
}

#[test]
fn owned_protected_open_debt_forwards_facts_but_never_arms_entry() {
    let mut open = readiness();
    open.exposure = PrivateExposure::Open;
    open.ordinary_order_debt = true;
    open.algo_order_debt = true;
    let mut gate = PrivateEntryGate::new();

    let report = gate.observe_readiness(Some(open), projections(), input(true));

    assert!(!report.entry_ready);
    assert_eq!(report.lifecycle.entry, EntryDisposition::Disarmed);
    assert_eq!(report.lifecycle.control, ControlDisposition::None);
    assert!(report.forwarded_private.as_ref().is_some_and(|facts| {
        facts.custody == venue::runtime::CustodyStatus::Complete
            && facts.safety.exposure == venue::strategy::scalping::ExposureState::Open
    }));
}

#[test]
fn flat_residual_order_debt_cannot_be_explained_as_entry_safe_custody() {
    let mut flat = readiness();
    flat.algo_order_debt = true;
    let mut gate = PrivateEntryGate::new();

    let report = gate.observe_readiness(Some(flat), projections(), input(false));

    assert!(!report.entry_ready);
    assert!(report.forwarded_private.is_none());
}

#[test]
fn known_protection_gap_is_forwarded_only_for_active_episode_reduction() {
    let mut open = readiness();
    open.exposure = PrivateExposure::Open;
    let mut gap = projections();
    gap.protection.value = ProtectionProjection::Gap;
    let mut gate = PrivateEntryGate::new();

    let report = gate.observe_readiness(Some(open), gap, input(true));

    assert!(!report.entry_ready);
    assert_eq!(report.lifecycle.control, ControlDisposition::StopAndProtect);
    assert!(report.forwarded_private.as_ref().is_some_and(|facts| {
        facts.safety.protection == venue::strategy::scalping::ProtectionState::Gap
            && facts.custody == venue::runtime::CustodyStatus::Incomplete
    }));
    assert_eq!(report.coordinator_inputs().len(), 2);
}
