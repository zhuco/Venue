use tempfile::tempdir;
use venue::{
    domain::{PositionSide, Symbol},
    execution::{
        CanaryRunBinding, CanaryRunPhase, CanaryRunState, CanaryRunStateError, MAX_UNPROTECTED_MS,
    },
};
fn binding() -> Result<CanaryRunBinding, Box<dyn std::error::Error>> {
    Ok(CanaryRunBinding {
        canary_id: "canary_1".into(),
        exchange: "binance".into(),
        account: "primary".into(),
        symbol: "BTC/USDT".parse::<Symbol>()?,
        owner_scope: "scalping_run_1".into(),
        release_id: "release_1".into(),
        position_side: PositionSide::Long,
        writer_generation: 7,
        readback_generation: 7,
        valid_until_ms: 10_000,
    })
}
fn hash() -> String {
    "a".repeat(64)
}
#[test]
fn recovery_keeps_original_unprotected_deadline() -> Result<(), Box<dyn std::error::Error>> {
    let d = tempdir()?;
    let p = d.path().join("run.json");
    let b = binding()?;
    let mut state = CanaryRunState::create_new(&p, b.clone(), 100)?;
    state.entry_submitted(hash(), 101)?;
    let deadline = state.filled_unprotected(hash(), 200)?;
    assert_eq!(deadline, 101 + MAX_UNPROTECTED_MS);
    let mut recovered = CanaryRunState::recover(&p, &b, 300)?;
    assert!(
        matches!(recovered.phase(),CanaryRunPhase::FilledUnprotected{deadline_ms,..} if *deadline_ms==deadline)
    );
    assert!(matches!(
        recovered.require_unprotected_before(deadline),
        Err(CanaryRunStateError::Expired)
    ));
    assert!(recovered.is_frozen());
    Ok(())
}
#[test]
fn state_only_accepts_protection_or_emergency_then_exact_flat()
-> Result<(), Box<dyn std::error::Error>> {
    let d = tempdir()?;
    let mut s = CanaryRunState::create_new(d.path().join("run.json"), binding()?, 100)?;
    assert!(s.protected(hash(), 101).is_err());
    s.entry_submitted(hash(), 101)?;
    s.filled_unprotected(hash(), 102)?;
    s.emergency_flattening(hash(), 103)?;
    s.flat(hash(), 104)?;
    assert!(matches!(s.phase(), CanaryRunPhase::Flat { .. }));
    Ok(())
}
#[test]
fn clock_regression_and_expired_binding_freeze() -> Result<(), Box<dyn std::error::Error>> {
    let d = tempdir()?;
    let mut s = CanaryRunState::create_new(d.path().join("run.json"), binding()?, 100)?;
    s.entry_submitted(hash(), 200)?;
    assert!(matches!(
        s.filled_unprotected(hash(), 199),
        Err(CanaryRunStateError::Clock)
    ));
    assert!(s.is_frozen());
    let d = tempdir()?;
    let mut unknown = CanaryRunState::create_new(d.path().join("unknown.json"), binding()?, 100)?;
    unknown.freeze_unknown()?;
    assert!(matches!(
        unknown.entry_submitted(hash(), 101),
        Err(CanaryRunStateError::Frozen)
    ));
    Ok(())
}

#[test]
fn frozen_unknown_entry_can_only_advance_to_emergency_flatten()
-> Result<(), Box<dyn std::error::Error>> {
    let d = tempdir()?;
    let mut state = CanaryRunState::create_new(d.path().join("late.json"), binding()?, 100)?;
    state.entry_submitted(hash(), 101)?;
    state.freeze_unknown()?;
    assert!(state.filled_unprotected(hash(), 102).is_err());
    state.emergency_flattening(hash(), 103)?;
    state.flat(hash(), 104)?;
    assert!(matches!(state.phase(), CanaryRunPhase::Flat { .. }));
    Ok(())
}

#[test]
fn unbound_discovery_recovers_and_fences_an_expired_nonterminal_run()
-> Result<(), Box<dyn std::error::Error>> {
    let d = tempdir()?;
    let path = d.path().join("unfinished.json");
    let mut state = CanaryRunState::create_new(&path, binding()?, 100)?;
    state.entry_submitted(hash(), 101)?;
    let deadline = state.filled_unprotected(hash(), 102)?;

    let recovered = CanaryRunState::recover_existing(&path, deadline)?;
    assert!(recovered.is_frozen());
    assert!(!recovered.is_terminal());
    assert!(matches!(
        recovered.phase(),
        CanaryRunPhase::FilledUnprotected { .. }
    ));
    Ok(())
}

#[test]
fn recovery_receipt_can_seal_prepared_or_frozen_phase_without_reviving_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let d = tempdir()?;
    let prepared_path = d.path().join("prepared.json");
    let mut prepared = CanaryRunState::create_new(&prepared_path, binding()?, 100)?;
    let revision = prepared.revision();
    prepared.seal_recovered_flat(hash(), 200)?;
    assert_eq!(prepared.revision(), revision + 1);
    assert!(prepared.is_terminal());

    let frozen_path = d.path().join("frozen.json");
    let mut frozen = CanaryRunState::create_new(&frozen_path, binding()?, 100)?;
    frozen.entry_submitted(hash(), 101)?;
    frozen.freeze_unknown()?;
    frozen.seal_recovered_flat(hash(), 200)?;
    assert!(frozen.is_terminal());
    assert!(!frozen.is_frozen());
    Ok(())
}
