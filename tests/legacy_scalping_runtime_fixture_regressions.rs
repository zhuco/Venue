use serde::Deserialize;
use venue::runtime::{
    MAX_RISK_FACTS_PER_PAGE, MAX_RISK_REPLAY_PAGES, SCALPING_RESIDENT_LEGACY_PRIORITY,
    SCALPING_RESIDENT_PRIORITY,
};

const FIXTURE: &str = include_str!("fixtures/scalping_legacy_runtime_v1.json");

#[derive(Debug, Deserialize)]
struct Fixture {
    schema_version: u16,
    provenance: Vec<SourceSpan>,
    coordinator_actor: CoordinatorActor,
    runner_composition: RunnerComposition,
    risk_replay: RiskReplay,
    safety_regression: SafetyRegression,
}

#[derive(Debug, Deserialize)]
struct SourceSpan {
    source_path: String,
    source_sha256: String,
    line_start: u32,
    line_end: u32,
    symbols: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CoordinatorActor {
    bootstrap_before_loop: bool,
    biased_priority: Vec<String>,
    timer_missed_tick_behavior: String,
    command_channel_closed: ClosedCommand,
    market_enabled_states: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ClosedCommand {
    control: String,
    wait_for_safe_stop: bool,
}

#[derive(Debug, Deserialize)]
struct RunnerComposition {
    command_channel_capacity: usize,
    private_reconcile_signal_capacity: usize,
    private_event_signal_is_try_send: bool,
    runtime_reconcile_interval_ms: u64,
    risk_poll_missed_tick_behavior: String,
    risk_delivery: String,
    shutdown: RunnerShutdown,
}

#[derive(Debug, Deserialize)]
struct RunnerShutdown {
    command: String,
    await_runtime_safe_convergence: bool,
}

#[derive(Debug, Deserialize)]
struct RiskReplay {
    max_pages: u16,
    max_facts_per_page: u16,
    generation_change_refetches_from_initial_cursor: bool,
    validate_cursor_before_event_time_sort: bool,
    terminal_empty_page_may_hold_cursor: bool,
    stale_replay_after_max_stale_ms: String,
}

#[derive(Debug, Deserialize)]
struct SafetyRegression {
    repeated_capability_gap_control: String,
    convergence_terminal_state: String,
    durable_pending_mutation: String,
}

fn fixture() -> Result<Fixture, serde_json::Error> {
    serde_json::from_str(FIXTURE)
}

#[test]
fn fixture_preserves_verified_two_layer_scheduling_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    assert_eq!(fixture.schema_version, 1);
    assert!(fixture.coordinator_actor.bootstrap_before_loop);
    assert_eq!(
        fixture.coordinator_actor.biased_priority,
        SCALPING_RESIDENT_LEGACY_PRIORITY
    );
    assert_eq!(
        SCALPING_RESIDENT_PRIORITY,
        [
            "controller_command",
            "private_gate_control",
            "private_reconcile_signal",
            "risk_proof",
            "episode_projection",
            "timer_reconcile",
            "market_frame",
        ]
    );
    assert_eq!(fixture.coordinator_actor.timer_missed_tick_behavior, "skip");
    assert_eq!(
        fixture.coordinator_actor.market_enabled_states,
        ["running", "resuming_cooldown"]
    );
    assert_eq!(fixture.runner_composition.command_channel_capacity, 64);
    assert_eq!(
        fixture.runner_composition.private_reconcile_signal_capacity,
        1
    );
    assert!(fixture.runner_composition.private_event_signal_is_try_send);
    assert_eq!(
        fixture.runner_composition.runtime_reconcile_interval_ms,
        250
    );
    assert_eq!(
        fixture.runner_composition.risk_poll_missed_tick_behavior,
        "skip"
    );
    assert_eq!(fixture.runner_composition.risk_delivery, "command_channel");
    Ok(())
}

#[test]
fn fixture_preserves_shutdown_and_bounded_risk_replay_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    assert_eq!(
        fixture.coordinator_actor.command_channel_closed.control,
        "stop_and_protect"
    );
    assert!(
        fixture
            .coordinator_actor
            .command_channel_closed
            .wait_for_safe_stop
    );
    assert_eq!(fixture.runner_composition.shutdown.command, "shutdown");
    assert!(
        fixture
            .runner_composition
            .shutdown
            .await_runtime_safe_convergence
    );
    assert_eq!(
        usize::from(fixture.risk_replay.max_pages),
        MAX_RISK_REPLAY_PAGES
    );
    assert_eq!(
        usize::from(fixture.risk_replay.max_facts_per_page),
        MAX_RISK_FACTS_PER_PAGE
    );
    assert!(
        fixture
            .risk_replay
            .generation_change_refetches_from_initial_cursor
    );
    assert!(fixture.risk_replay.validate_cursor_before_event_time_sort);
    assert!(fixture.risk_replay.terminal_empty_page_may_hold_cursor);
    assert_eq!(
        fixture.risk_replay.stale_replay_after_max_stale_ms,
        "shutdown"
    );
    assert_eq!(
        fixture.safety_regression.repeated_capability_gap_control,
        "stop_and_protect_once"
    );
    assert_eq!(
        fixture.safety_regression.convergence_terminal_state,
        "stopped_flat"
    );
    assert_eq!(fixture.safety_regression.durable_pending_mutation, "none");
    Ok(())
}

#[test]
fn fixture_keeps_exact_non_runtime_provenance_separate_from_actor_priority()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    assert_eq!(fixture.provenance.len(), 5);
    assert!(fixture.provenance.iter().all(|span| {
        !span.source_path.trim().is_empty()
            && span.source_sha256.len() == 64
            && span.line_start > 0
            && span.line_end >= span.line_start
            && !span.symbols.is_empty()
    }));
    assert!(fixture.provenance.iter().any(|span| {
        span.source_path.ends_with("runtime.rs") && span.line_start == 636 && span.line_end == 681
    }));
    assert!(fixture.provenance.iter().any(|span| {
        span.source_path.ends_with("risk_replay.rs")
            && span.line_start == 58
            && span.line_end == 129
    }));
    Ok(())
}
