use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    controller::{
        CONTROL_SCHEMA_VERSION, ControlAuthority, ControlTarget, InstanceControlRecord,
        InstanceControlStore, ScalpingControllerBlock, ScalpingControllerSource,
    },
    domain::{Amount, Asset},
    strategy::scalping::{StrategyBinding, StrategyKind},
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "resident-sol".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "resident-sol:shadow-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    })
}

fn record(
    binding: &StrategyBinding,
    revision: u64,
    target: ControlTarget,
    deadline_ms: Option<u64>,
) -> InstanceControlRecord {
    InstanceControlRecord {
        schema_version: CONTROL_SCHEMA_VERSION,
        binding: binding.clone(),
        target,
        command_id: format!("controller-{revision}"),
        idempotency_key: format!("controller-key-{revision}"),
        safety_deadline_ms: deadline_ms,
        revision,
    }
}

fn authority(binding: &StrategyBinding, generation: u64) -> ControlAuthority {
    ControlAuthority {
        generation,
        parameter_release_id: binding.parameter_release_id.clone(),
        private_snapshot_ready: true,
        execution_unknown: false,
        protection_complete: true,
        owner_conflict: false,
    }
}

#[test]
fn missing_or_stopped_durable_target_never_emits_authorization()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let control_path = directory.path().join("controller.json");
    let cursor_path = directory.path().join("controller_source.json");
    let mut source = ScalpingControllerSource::open(&control_path, &cursor_path, binding.clone())?;

    let missing = source.observe(None, 100)?;
    assert_eq!(missing.block(), Some(ScalpingControllerBlock::Missing));
    assert_eq!(missing.control(), Some(ControlTarget::StopAndProtect));
    assert!(missing.authorization().is_none());

    InstanceControlStore::new(&control_path).save(
        &record(&binding, 1, ControlTarget::StopAndProtect, Some(1_000)),
        None,
    )?;
    let stopped = source.observe(Some(&authority(&binding, 1)), 101)?;
    assert_eq!(stopped.block(), Some(ScalpingControllerBlock::Target));
    assert_eq!(stopped.control(), Some(ControlTarget::StopAndProtect));
    assert!(stopped.authorization().is_none());
    Ok(())
}

#[test]
fn restart_requires_a_strictly_newer_authority_generation() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let binding = binding()?;
    let control_path = directory.path().join("controller.json");
    let cursor_path = directory.path().join("controller_source.json");
    InstanceControlStore::new(&control_path).save(
        &record(&binding, 1, ControlTarget::Running, Some(1_000)),
        None,
    )?;

    let mut first = ScalpingControllerSource::open(&control_path, &cursor_path, binding.clone())?;
    let issued = first.observe(Some(&authority(&binding, 1)), 100)?;
    let authorization = issued.authorization().ok_or("missing authorization")?;
    assert!(authorization.is_allowed());
    assert!(authorization.is_valid_at(999));
    assert!(!authorization.is_valid_at(1_000));
    assert_eq!(authorization.revision(), 1);
    assert_eq!(authorization.authority_generation(), 1);
    assert_eq!(authorization.expires_at_ms(), 1_000);

    let mut restored =
        ScalpingControllerSource::open(&control_path, &cursor_path, binding.clone())?;
    let stale = restored.observe(Some(&authority(&binding, 1)), 101)?;
    assert_eq!(
        stale.block(),
        Some(ScalpingControllerBlock::RecoveryGeneration)
    );
    assert!(stale.authorization().is_none());
    let fresh = restored.observe(Some(&authority(&binding, 2)), 102)?;
    assert!(
        fresh
            .authorization()
            .is_some_and(|value| value.is_allowed())
    );
    Ok(())
}

#[test]
fn a_new_running_revision_accepts_the_current_complete_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let control_path = directory.path().join("controller.json");
    let cursor_path = directory.path().join("controller_source.json");
    let store = InstanceControlStore::new(&control_path);
    store.save(
        &record(&binding, 1, ControlTarget::Running, Some(1_000)),
        None,
    )?;
    let mut source = ScalpingControllerSource::open(&control_path, &cursor_path, binding.clone())?;
    assert!(
        source
            .observe(Some(&authority(&binding, 1)), 100)?
            .authorization()
            .is_some()
    );

    store.save(
        &record(&binding, 2, ControlTarget::Running, Some(2_000)),
        Some(1),
    )?;
    let current = source.observe(Some(&authority(&binding, 1)), 101)?;
    assert_eq!(current.revision(), Some(2));
    assert_eq!(
        current.authorization().map(|value| value.revision()),
        Some(2)
    );
    assert_eq!(
        current
            .authorization()
            .map(|value| value.authority_generation()),
        Some(1)
    );
    Ok(())
}

#[test]
fn renewal_after_expiry_does_not_require_a_private_reconnect()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let control_path = directory.path().join("controller.json");
    let cursor_path = directory.path().join("controller_source.json");
    let store = InstanceControlStore::new(&control_path);
    store.save(
        &record(&binding, 1, ControlTarget::Running, Some(500)),
        None,
    )?;
    let mut source = ScalpingControllerSource::open(&control_path, &cursor_path, binding.clone())?;
    assert!(
        source
            .observe(Some(&authority(&binding, 1)), 100)?
            .authorization()
            .is_some()
    );
    assert_eq!(
        source.observe(Some(&authority(&binding, 2)), 500)?.block(),
        Some(ScalpingControllerBlock::Deadline)
    );

    store.save(
        &record(&binding, 2, ControlTarget::Running, Some(1_000)),
        Some(1),
    )?;
    let renewed = source.observe(Some(&authority(&binding, 2)), 501)?;
    assert_eq!(renewed.revision(), Some(2));
    assert_eq!(
        renewed
            .authorization()
            .map(|value| value.authority_generation()),
        Some(2)
    );
    Ok(())
}

#[test]
fn release_binding_and_deadline_mismatch_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let expected = binding()?;
    let control_path = directory.path().join("controller.json");
    let cursor_path = directory.path().join("controller_source.json");
    InstanceControlStore::new(&control_path).save(
        &record(&expected, 1, ControlTarget::Running, Some(500)),
        None,
    )?;
    let mut source = ScalpingControllerSource::open(&control_path, &cursor_path, expected.clone())?;
    let mut wrong_release = authority(&expected, 1);
    wrong_release.parameter_release_id = "other-release".to_owned();
    let release = source.observe(Some(&wrong_release), 100)?;
    assert_eq!(release.block(), Some(ScalpingControllerBlock::Release));
    assert_eq!(release.control(), Some(ControlTarget::StopAndProtect));

    let expired = source.observe(Some(&authority(&expected, 2)), 500)?;
    assert_eq!(expired.block(), Some(ScalpingControllerBlock::Deadline));
    assert_eq!(expired.control(), Some(ControlTarget::StopAndProtect));

    let other_directory = tempdir()?;
    let other_control = other_directory.path().join("controller.json");
    let other_cursor = other_directory.path().join("controller_source.json");
    let mut wrong_binding = expected.clone();
    wrong_binding.run_id = "other-run".to_owned();
    InstanceControlStore::new(&other_control).save(
        &record(&wrong_binding, 1, ControlTarget::Running, Some(1_000)),
        None,
    )?;
    let mut mismatched =
        ScalpingControllerSource::open(&other_control, &other_cursor, expected.clone())?;
    let blocked = mismatched.observe(Some(&authority(&expected, 1)), 100)?;
    assert_eq!(blocked.block(), Some(ScalpingControllerBlock::Binding));
    assert_eq!(blocked.control(), Some(ControlTarget::StopAndProtect));
    Ok(())
}
