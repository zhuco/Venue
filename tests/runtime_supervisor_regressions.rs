//! Historical alpha-runtime failure contracts, expressed without a gateway, executor, or writer.

use venue::runtime::{
    ActionKind, ControlDisposition, EntryDisposition, InstanceKey, LifecycleFault, LifecycleInput,
    ParallelCompletion, ShutdownState, SubmissionOutcome, SupervisorAction, SupervisorError,
    SupervisorFailure, report_lifecycle, report_parallel_submission, report_shutdown_convergence,
    report_termination, request_instance_shutdown,
};

fn action(action_id: &str, kind: ActionKind) -> SupervisorAction {
    SupervisorAction {
        action_id: action_id.to_owned(),
        kind,
    }
}

#[test]
fn action_identity_is_unique_before_parallel_submission_starts() {
    let actions = [
        action("same_action", ActionKind::Place),
        action("same_action", ActionKind::Cancel),
    ];
    let result = report_parallel_submission(
        &actions,
        &[
            ParallelCompletion {
                action_index: 0,
                outcome: SubmissionOutcome::Confirmed,
            },
            ParallelCompletion {
                action_index: 1,
                outcome: SubmissionOutcome::Confirmed,
            },
        ],
    );

    assert!(matches!(result, Err(SupervisorError::DuplicateAction)));
}

#[test]
fn instance_shutdown_names_only_its_target_and_cannot_stop_gateway_or_siblings()
-> Result<(), Box<dyn std::error::Error>> {
    let decision = request_instance_shutdown(InstanceKey {
        strategy_instance_id: "scalping_primary".to_owned(),
        run_id: "run_7".to_owned(),
    })?;

    assert_eq!(decision.instance.strategy_instance_id, "scalping_primary");
    assert_eq!(decision.instance.run_id, "run_7");
    assert!(!report_shutdown_convergence(decision.clone(), ShutdownState::Pending).converged());
    assert!(report_shutdown_convergence(decision, ShutdownState::StoppedProtected).converged());
    Ok(())
}

#[test]
fn deadline_and_faults_converge_to_cooldown_disarm_or_protection_without_gateway_access() {
    assert_eq!(
        report_lifecycle(LifecycleInput {
            active_episode: false,
            entry_armed: true,
            now_ms: 100,
            admission_deadline_ms: Some(100),
            fault: None,
        }),
        venue::runtime::LifecycleReport {
            entry: EntryDisposition::Cooldown,
            control: ControlDisposition::None,
        }
    );
    assert_eq!(
        report_lifecycle(LifecycleInput {
            active_episode: false,
            entry_armed: true,
            now_ms: 100,
            admission_deadline_ms: None,
            fault: Some(LifecycleFault::PrivateReconciliationTransient),
        }),
        venue::runtime::LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: ControlDisposition::None,
        }
    );
    assert_eq!(
        report_lifecycle(LifecycleInput {
            active_episode: true,
            entry_armed: true,
            now_ms: 100,
            admission_deadline_ms: None,
            fault: Some(LifecycleFault::CapabilityGenerationChanged),
        }),
        venue::runtime::LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: ControlDisposition::StopAndProtect,
        }
    );
}

#[test]
fn parallel_submission_commits_confirmed_actions_in_original_action_order()
-> Result<(), Box<dyn std::error::Error>> {
    let actions = [
        action("place_a", ActionKind::Place),
        action("cancel_b", ActionKind::Cancel),
        action("reduce_c", ActionKind::Reduce),
    ];
    let report = report_parallel_submission(
        &actions,
        &[
            ParallelCompletion {
                action_index: 2,
                outcome: SubmissionOutcome::Confirmed,
            },
            ParallelCompletion {
                action_index: 0,
                outcome: SubmissionOutcome::Confirmed,
            },
            ParallelCompletion {
                action_index: 1,
                outcome: SubmissionOutcome::Confirmed,
            },
        ],
    )?;

    assert_eq!(
        report
            .committed
            .iter()
            .map(|action| action.action_id.as_str())
            .collect::<Vec<_>>(),
        ["place_a", "cancel_b", "reduce_c"]
    );
    assert!(!report.recovery_required());
    Ok(())
}

#[test]
fn unknown_or_transient_submission_compensates_only_confirmed_places_and_fences_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let actions = [
        action("confirmed_place", ActionKind::Place),
        action("unknown_place", ActionKind::Place),
        action("rejected_place", ActionKind::Place),
        action("transient_reduce", ActionKind::Reduce),
    ];
    let report = report_parallel_submission(
        &actions,
        &[
            ParallelCompletion {
                action_index: 3,
                outcome: SubmissionOutcome::Transient {
                    reason: "connection reset".to_owned(),
                },
            },
            ParallelCompletion {
                action_index: 2,
                outcome: SubmissionOutcome::Rejected {
                    reason: "risk denied".to_owned(),
                },
            },
            ParallelCompletion {
                action_index: 0,
                outcome: SubmissionOutcome::Confirmed,
            },
            ParallelCompletion {
                action_index: 1,
                outcome: SubmissionOutcome::Unknown,
            },
        ],
    )?;

    assert!(report.recovery_required());
    assert_eq!(
        report
            .compensate_confirmed_places
            .iter()
            .map(|action| action.action_id.as_str())
            .collect::<Vec<_>>(),
        ["confirmed_place"]
    );
    assert_eq!(
        report
            .fenced_unknown
            .iter()
            .map(|action| action.action_id.as_str())
            .collect::<Vec<_>>(),
        ["unknown_place"]
    );
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].action.action_id, "rejected_place");
    assert!(
        !report
            .compensate_confirmed_places
            .iter()
            .any(|action| action.action_id == "rejected_place")
    );
    assert!(
        !report
            .fenced_unknown
            .iter()
            .any(|action| action.action_id == "rejected_place")
    );
    assert_eq!(report.transient.len(), 1);
    assert_eq!(report.transient[0].action.action_id, "transient_reduce");
    Ok(())
}

#[test]
fn runtime_and_cleanup_failures_are_reported_together_without_collapsing_either()
-> Result<(), Box<dyn std::error::Error>> {
    let report = report_termination(
        Some(SupervisorFailure::new("private stream unknown")?),
        vec![
            SupervisorFailure::new("owned cancel failed")?,
            SupervisorFailure::new("protection stop failed")?,
        ],
    );

    assert_eq!(
        report
            .runtime_error
            .as_ref()
            .map(|failure| failure.message.as_str()),
        Some("private stream unknown")
    );
    assert_eq!(
        report
            .cleanup_errors
            .iter()
            .map(|failure| failure.message.as_str())
            .collect::<Vec<_>>(),
        ["owned cancel failed", "protection stop failed"]
    );
    Ok(())
}
