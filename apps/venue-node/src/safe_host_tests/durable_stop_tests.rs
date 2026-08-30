use super::*;

#[test]
fn completed_stop_receipt_restores_stopped_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![
                ReadbackPlan::initial(),
                ReadbackPlan::recovery(ReadbackResolution::Absent),
            ],
        ),
        None,
        1_000,
    )?;
    apply_control(
        &mut first,
        launch.binding(),
        ControlAction::Stop,
        "durable-stop",
        1_000,
    )?;
    let completion = first.complete_control(12_000)?;
    assert_eq!(completion.request_id, "durable-stop");
    completion.receipt.validate()?;
    drop(first);

    let mut reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan {
                connection_generation: 3,
                private_generation: 3,
                observed_ms: 22_000,
                resolution: ReadbackResolution::Absent,
                nonzero_position: false,
                omit_family: false,
            }],
        ),
        None,
        23_000,
    )?;
    assert!(matches!(
        reopened.prepare_dispatch(
            entry_command(launch.binding(), "after-durable-stop")?,
            23_000,
        ),
        Err(SafeHostError::ControlLifecycle)
    ));
    assert!(matches!(
        reopened.complete_control(23_000),
        Err(SafeHostError::ControlLifecycle)
    ));
    Ok(())
}
