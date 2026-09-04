use super::*;

#[test]
fn migration_has_only_the_minimal_postgres_grid_boundary() {
    for required in [
        "venue_binance_grid_instances",
        "venue_binance_grid_config_revisions",
        "venue_binance_grid_anchors",
        "venue_binance_grid_desired_orders",
        "venue_binance_grid_order_owners",
        "venue_binance_grid_fill_allocations",
        "grid_plan_revision",
        "grid_semantic_key",
        "target_client_order_id",
        "command_origin IN ('copy', 'terminal', 'grid')",
        "convergence_started_ms",
        "consecutive_failures",
        "last_facts_ms",
    ] {
        assert!(MIGRATION_0021.contains(required), "missing {required}");
    }
    for forbidden in [
        "venue_binance_grid_actors",
        "writer_lease_id",
        "journal_path",
        "checkpoint_json",
    ] {
        assert!(!MIGRATION_0021.to_ascii_lowercase().contains(forbidden));
    }
}

#[test]
fn runtime_transition_table_is_fail_closed() {
    assert!(runtime_transition_allowed(
        GridInstanceState::StartPending,
        GridInstanceState::Running
    ));
    assert!(runtime_transition_allowed(
        GridInstanceState::StopPending,
        GridInstanceState::Stopped
    ));
    assert!(!runtime_transition_allowed(
        GridInstanceState::Stopped,
        GridInstanceState::Running
    ));
}

#[test]
fn lifecycle_and_config_changes_fence_only_unsent_risk_commands() {
    let source = [
        include_str!("../grid_store.rs"),
        include_str!("convergence.rs"),
    ]
    .join("\n");
    assert_eq!(
        source
            .matches("cancel_pending_risk_commands(&mut tx")
            .count(),
        4
    );
    assert!(source.contains("command_state='cancelled'"));
    assert!(source.contains("sanitized_error_code='lifecycle_fenced'"));
    assert!(source.contains("command_state='pending'"));
    assert!(source.contains("command_phase<>'cancel'"));
    assert!(!source.contains("command_state='sending',sanitized_error_code='lifecycle_fenced'"));
}

#[test]
fn plan_surface_commit_is_atomic_cas_and_preserves_failure_history() {
    let source = include_str!("surface.rs");
    assert!(source.contains("pub async fn commit_plan_surface"));
    assert!(source.contains("expected_instance_revision"));
    assert!(source.contains("expected_config_revision"));
    assert!(source.contains("AND instance_state IN ('start_pending','running')"));
    assert!(source.contains("write_anchor("));
    assert!(source.contains("replace_desired_rows("));
    assert!(!source.contains("consecutive_failures=0"));
}

#[test]
fn reduce_reservations_cover_inflight_and_projection_lag() {
    let source = include_str!("reads.rs");
    assert!(source.contains("command_phase='close'"));
    assert!(source.contains("'pending','sending','accepted','reconcile_required','reconciled'"));
    assert!(source.contains("grid_instance_id"));
    assert!(source.contains("client_order_id"));
    assert!(source.contains("updated_ms"));
}

#[test]
fn command_history_is_scoped_by_config_and_plan_revision() {
    let source = include_str!("reads.rs");
    assert!(source.contains("grid_config_revision=$2"));
    assert!(source.contains("grid_plan_revision=$3"));
    assert!(source.contains("pub async fn latest_grid_command_updated_ms"));
    assert!(source.contains("SELECT MAX(updated_ms)"));
}

#[test]
fn grid_ledger_ids_are_bounded_opaque_ids_not_account_ids() {
    let command = GridLedgerCommand {
        command_id: "gp-0123456789abcdef".into(),
        client_order_id: "vgp-0123456789".into(),
        instance_id: "00000000-0000-4000-8000-000000000001".into(),
        config_revision: 1,
        plan_revision: 1,
        semantic_key: "long:open:1:1".into(),
        rule_version: "binance-pm-um-grid-r1".into(),
        source_digest: [7; 32],
        intent: GridCommandIntent::LimitPostOnly {
            key: GridOrderSemanticKey {
                position_side: PositionSide::Long,
                role: GridOrderRole::Open,
                level: 1,
                sequence: 1,
            },
            quantity: Decimal::ONE,
            limit_price: Decimal::from(100),
        },
    };
    assert_eq!(validate_command(&command, 10), Ok(()));
    let ownership = GridOrderOwnership {
        instance_id: command.instance_id.clone(),
        trading_account_id: "00000000-0000-4000-8000-000000000002".into(),
        config_revision: 1,
        plan_revision: 1,
        key: match &command.intent {
            GridCommandIntent::LimitPostOnly { key, .. } => key.clone(),
            _ => unreachable!(),
        },
        place_command_id: command.command_id,
        client_order_id: command.client_order_id,
        symbol: "BTC/USDT".parse().expect("static symbol"),
        quantity: Decimal::ONE,
        filled_quantity: Decimal::ZERO,
        limit_price: Decimal::from(100),
        native_order_id: None,
        state: GridOwnedOrderState::Working,
        first_seen_ms: 10,
        last_seen_ms: 10,
    };
    assert_eq!(validate_ownership(&ownership), Ok(()));
}

#[test]
fn reset_config_clone_identity_is_deterministic_and_scoped() {
    let first = synthetic_config_request_id("manual-reset", "instance", "request", 1);
    assert_eq!(
        first,
        synthetic_config_request_id("manual-reset", "instance", "request", 1)
    );
    assert_ne!(
        first,
        synthetic_config_request_id("runtime-reset", "instance", "request", 1)
    );
    assert!(venue_domain::is_canonical_trading_account_id(&first));
}

#[test]
fn convergence_cas_fences_every_lifecycle_change() {
    let mut update = GridConvergenceUpdate {
        instance_id: "00000000-0000-4000-8000-000000000001".into(),
        expected_instance_revision: 7,
        expected_state: GridInstanceState::Running,
        expected_plan_revision: 3,
        next_plan_revision: 3,
        desired_digest: [1; 32],
        dirty: true,
        consecutive_failures: 1,
        last_facts_ms: 10,
    };
    assert!(convergence_cas_matches(
        &update,
        7,
        GridInstanceState::Running
    ));
    assert!(!convergence_cas_matches(
        &update,
        8,
        GridInstanceState::Running
    ));
    update.expected_state = GridInstanceState::Paused;
    update.dirty = false;
    assert!(convergence_cas_matches(
        &update,
        7,
        GridInstanceState::Paused
    ));
    update.dirty = true;
    for state in [
        GridInstanceState::StopPending,
        GridInstanceState::ResetRequired,
        GridInstanceState::NeedsAttention,
    ] {
        update.expected_state = state;
        assert!(!convergence_cas_matches(&update, 7, state));
    }
    let source = include_str!("convergence.rs");
    assert!(source.contains("AND revision=$12 AND plan_revision=$13"));
    assert!(source.contains("AND instance_state=$14"));
    assert!(source.contains("update.expected_state == GridInstanceState::Paused && update.dirty"));
}

#[test]
fn pause_enters_a_dirty_cancel_only_convergence() {
    assert_eq!(
        lifecycle_transition(GridInstanceState::Running, GridLifecycleAction::Pause),
        Ok((GridInstanceState::Paused, true, None))
    );
    assert!(runtime_transition_allowed(
        GridInstanceState::Paused,
        GridInstanceState::NeedsAttention
    ));
}
