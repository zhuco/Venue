use super::*;
use venue_control_protocol::grid::GridInstanceSummary;

#[tokio::test]
async fn first_exchange_rejection_deadline_survives_retries_convergence_and_restart() -> TestResult
{
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result = exercise_rejection_deadline(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

async fn update(
    store: &BinanceGridStore,
    current: &GridInstanceSummary,
    dirty: bool,
    failures: u16,
    now: u64,
) -> Result<GridInstanceSummary, Box<dyn std::error::Error>> {
    Ok(store
        .update_convergence(
            &GridConvergenceUpdate {
                instance_id: current.instance_id.clone(),
                expected_instance_revision: current.revision,
                expected_state: current.state,
                expected_plan_revision: current.plan_revision,
                next_plan_revision: current.plan_revision,
                desired_digest: [11; 32],
                dirty,
                consecutive_failures: failures,
                last_facts_ms: now,
            },
            now,
        )
        .await?)
}

async fn exercise_rejection_deadline(pool: &PgPool) -> TestResult {
    seed_verified_account(pool).await?;
    let store = BinanceGridStore::new(pool.clone());
    let ledger = BinanceCommandLedger::new(pool.clone());
    let owner = id(1);
    let account = id(2);
    let instance = id(300);
    let now = 1_900_400_000_000_u64;
    let created = store
        .create_instance(
            &owner,
            &account,
            &instance,
            &GridInstanceCreateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: id(301),
                credential_id: id(3),
                symbol: "BTC/USDT".parse()?,
                config: config(),
            },
            now,
        )
        .await?;
    let current = store
        .request_lifecycle(
            &owner,
            &lifecycle(
                id(302),
                &instance,
                created.revision,
                GridLifecycleAction::Start,
            ),
            now + 1,
        )
        .await?;
    let mut current = store
        .commit_plan_surface(
            &instance,
            current.revision,
            current.config_revision,
            current.plan_revision,
            current.plan_revision,
            None,
            [11; 32],
            &[],
            now + 2,
            now + 2,
        )
        .await?;
    current = update(&store, &current, false, 0, now + 3).await?;

    // A local preflight rejection must not start the exchange clock.
    for (index, error) in [
        (0, "preflight_failed"),
        (1, "binance_-5022"),
        (2, "binance_-5022"),
    ] {
        let time = now + 10 + index * 10_000;
        let command = GridLedgerCommand {
            command_id: format!("grid-rejection-{index}"),
            client_order_id: format!("grid-rejection-client-{index}"),
            instance_id: instance.clone(),
            config_revision: current.config_revision,
            plan_revision: current.plan_revision,
            semantic_key: format!("replenish:long:{index}"),
            rule_version: "fixture-rules".into(),
            source_digest: [11; 32],
            intent: GridCommandIntent::Market {
                position_side: PositionSide::Long,
                role: GridOrderRole::Open,
                quantity: Decimal::new(1, 3),
            },
        };
        store.enqueue_command(&command, time).await?;
        let claimed = ledger
            .claim_next(&account, time + 1)
            .await?
            .ok_or("missing command")?;
        assert_eq!(claimed.command_id, command.command_id);
        ledger
            .settle(
                &command.command_id,
                ExecutorCommandState::Rejected,
                time + 2,
                Some(error),
            )
            .await?;
        current = update(&store, &current, true, 1, time + 3).await?;
        assert_eq!(current.state, GridInstanceState::Running);
        if index == 0 {
            current = update(&store, &current, false, 0, time + 4).await?;
        }
    }
    let first_rejected = now + 10_012;
    // Three failures do not shorten the delay. Repairing the surface/rolling a new plan cannot
    // postpone or erase the deadline, and reconstruction of the Store retains the same fact.
    current = update(&store, &current, true, 3, first_rejected + 20_000).await?;
    assert_eq!(current.state, GridInstanceState::Running);
    current = store
        .commit_plan_surface(
            &instance,
            current.revision,
            current.config_revision,
            current.plan_revision,
            current.plan_revision + 1,
            None,
            [11; 32],
            &[],
            first_rejected + 21_000,
            first_rejected + 21_000,
        )
        .await?;
    current = update(&store, &current, false, 0, first_rejected + 29_999).await?;
    assert_eq!(current.state, GridInstanceState::Running);
    let restarted = BinanceGridStore::new(pool.clone());
    let reset = update(&restarted, &current, false, 0, first_rejected + 30_000).await?;
    assert_eq!(reset.state, GridInstanceState::ResetRequired);
    assert!(reset.dirty);
    assert_eq!(reset.config_revision, current.config_revision + 1);
    assert_eq!(reset.convergence_started_ms, Some(first_rejected + 30_000));
    assert_eq!(
        reset.attention_code.as_deref(),
        Some("exchange_rejection_delay_elapsed")
    );

    // Completing teardown starts fresh planning, even if the preceding countdown was long.
    let running = restarted
        .settle_runtime_state(
            &instance,
            GridInstanceState::ResetRequired,
            GridInstanceState::Running,
            None,
            first_rejected + 35_000,
        )
        .await?;
    assert!(!running.dirty);
    assert_eq!(running.convergence_started_ms, None);
    let planned = restarted
        .commit_plan_surface(
            &instance,
            running.revision,
            running.config_revision,
            running.plan_revision,
            running.plan_revision + 1,
            None,
            [11; 32],
            &[],
            first_rejected + 36_000,
            first_rejected + 36_000,
        )
        .await?;
    let current = update(&restarted, &planned, false, 0, first_rejected + 100_000).await?;
    assert_eq!(current.state, GridInstanceState::Running);
    assert_eq!(current.config_revision, reset.config_revision);

    // Other reset causes also receive a fresh teardown clock rather than inherited dirty age.
    let dirty = restarted
        .commit_plan_surface(
            &instance,
            current.revision,
            current.config_revision,
            current.plan_revision,
            current.plan_revision + 1,
            None,
            [11; 32],
            &[],
            first_rejected + 100_001,
            first_rejected + 100_001,
        )
        .await?;
    let reset_at = first_rejected + 150_000;
    let reset = restarted
        .settle_runtime_state(
            &instance,
            dirty.state,
            GridInstanceState::ResetRequired,
            Some("surface_conflict"),
            reset_at,
        )
        .await?;
    assert_eq!(reset.convergence_started_ms, Some(reset_at));
    let terminal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM venue_binance_commands WHERE grid_instance_id=$1 AND command_state='rejected'",
    ).bind(&instance).fetch_one(pool).await?;
    assert_eq!(terminal_count, 3);
    Ok(())
}
