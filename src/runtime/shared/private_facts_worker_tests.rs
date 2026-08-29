use rust_decimal::Decimal;
use tempfile::tempdir;

use crate::{
    domain::{AccountBalance, Asset, Position, Price},
    exchange::{
        binance::{RecentFillsCursor, RecentFillsReadback},
        binance_private::{PrivateAccountCapabilities, PrivateReadback},
    },
    runtime::{
        ExecutionProjection, OwnerProjection, PrivateEntryGate, PrivateEntryGateInput,
        PrivateFactsProjectionInput, PrivateProjection, ProtectionProjection, RiskBudgetProjection,
    },
    strategy::scalping::{StrategyBinding, StrategyKind},
};

use super::*;

const TEST_ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

fn worker() -> Result<(tempfile::TempDir, BinancePrivateFactsWorker), PrivateFactsWorkerError> {
    let directory = tempdir().map_err(|source| PrivateFactsWorkerError::Io {
        path: PathBuf::from("temporary-private-worker"),
        source,
    })?;
    let worker = BinancePrivateFactsWorker::open(BinancePrivateFactsWorkerConfig {
        account: TEST_ACCOUNT_ID.to_owned(),
        symbol: "SOL/USDT"
            .parse()
            .map_err(|_| PrivateFactsWorkerError::Config)?,
        artifacts_root: directory.path().join("facts"),
        initial_fill_recovery_from_ms: 100,
    })?;
    Ok((directory, worker))
}

fn worker_with_authority()
-> Result<(tempfile::TempDir, BinancePrivateFactsWorker), PrivateFactsWorkerError> {
    let directory = tempdir().map_err(|source| PrivateFactsWorkerError::Io {
        path: PathBuf::from("temporary-private-worker"),
        source,
    })?;
    let symbol: Symbol = "SOL/USDT"
        .parse()
        .map_err(|_| PrivateFactsWorkerError::Config)?;
    let config = BinancePrivateFactsWorkerConfig {
        account: TEST_ACCOUNT_ID.to_owned(),
        symbol: symbol.clone(),
        artifacts_root: directory.path().join("facts"),
        initial_fill_recovery_from_ms: 100,
    };
    let binding = StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "resident-sol".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: EXCHANGE.to_owned(),
        account: TEST_ACCOUNT_ID.to_owned(),
        symbol,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "resident-sol:shadow-1".to_owned(),
        risk_budget: crate::domain::Amount::new(
            Asset::new("USDT").map_err(|_| PrivateFactsWorkerError::Config)?,
            Decimal::new(5, 0),
        ),
    };
    let worker = BinancePrivateFactsWorker::open_with_projection_authority(
        config,
        BinancePrivateProjectionAuthorityConfig {
            binding,
            command_journal_path: directory.path().join("commands.jsonl"),
            writer_authority_path: directory.path().join("writer.json"),
            custody_max_stale_ms: 1_000,
        },
    )?;
    Ok((directory, worker))
}

fn complete_account(symbol: &Symbol) -> Result<PrivateReadback, PrivateFactsWorkerError> {
    let asset = Asset::new("USDT").map_err(|_| PrivateFactsWorkerError::Config)?;
    Ok(PrivateReadback {
        capabilities: PrivateAccountCapabilities {
            can_trade: true,
            one_way_position: false,
            hedge_position: true,
        },
        balances: vec![AccountBalance {
            asset: asset.clone(),
            wallet_balance: Decimal::new(100, 0),
            available_balance: Decimal::new(100, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        }],
        positions: vec![
            Position {
                symbol: symbol.clone(),
                side: PositionSide::Long,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: Some(
                    Price::new(Decimal::ONE).map_err(|_| PrivateFactsWorkerError::Config)?,
                ),
            },
            Position {
                symbol: symbol.clone(),
                side: PositionSide::Short,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: Some(
                    Price::new(Decimal::ONE).map_err(|_| PrivateFactsWorkerError::Config)?,
                ),
            },
        ],
        orders: Vec::new(),
        fills: Vec::new(),
    })
}

fn bootstrap(
    worker: &BinancePrivateFactsWorker,
    ticket: PrivateReadbackTicket,
    target: u64,
) -> Result<PrivateFactsBootstrap, PrivateFactsWorkerError> {
    Ok(PrivateFactsBootstrap {
        generation: ticket.generation(),
        target_through_ms: target,
        account: complete_account(&worker.config.symbol)?,
        fills: RecentFillsReadback {
            payload: "[]".to_owned(),
            cursor: RecentFillsCursor {
                observed_through_ms: target,
                last_trade_id: None,
                last_event_time_ms: None,
            },
            pages: 1,
        },
        open_algo_orders: Vec::new(),
    })
}

fn complete_projection(generation: u64, observed_at_ms: u64) -> PrivateFactsProjectionInput {
    PrivateFactsProjectionInput {
        execution: PrivateProjection {
            generation,
            observed_at_ms,
            value: ExecutionProjection::Known,
        },
        owner: PrivateProjection {
            generation,
            observed_at_ms,
            value: OwnerProjection::Clear,
        },
        protection: PrivateProjection {
            generation,
            observed_at_ms,
            value: ProtectionProjection::Complete,
        },
        risk_budget: PrivateProjection {
            generation,
            observed_at_ms,
            value: RiskBudgetProjection::Available,
        },
    }
}

fn connect_and_bootstrap(
    worker: &mut BinancePrivateFactsWorker,
    now_ms: u64,
) -> Result<(), PrivateFactsWorkerError> {
    let Some(PrivateFactsEffect::Connect { effect_id }) = worker.next_effect(now_ms)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_connect(effect_id)?;
    let (effect_id, ticket) = advance_to_fills(worker, now_ms)?;
    worker.complete_bootstrap(effect_id, bootstrap(worker, ticket, now_ms)?, now_ms)?;
    Ok(())
}

fn advance_to_fills(
    worker: &mut BinancePrivateFactsWorker,
    now_ms: u64,
) -> Result<(u64, PrivateReadbackTicket), PrivateFactsWorkerError> {
    let scopes = [
        PrivateBootstrapScope::Account,
        PrivateBootstrapScope::Positions,
        PrivateBootstrapScope::PositionMode,
        PrivateBootstrapScope::AccountConfig,
        PrivateBootstrapScope::Orders,
        PrivateBootstrapScope::AlgoOrders,
    ];
    let mut common_ticket = None;
    for expected_scope in scopes {
        let Some(PrivateFactsEffect::Bootstrap {
            effect_id,
            ticket,
            scope,
        }) = worker.next_effect(now_ms)?
        else {
            return Err(PrivateFactsWorkerError::Effect);
        };
        if scope != expected_scope || common_ticket.is_some_and(|common| common != ticket) {
            return Err(PrivateFactsWorkerError::Effect);
        }
        common_ticket = Some(ticket);
        worker.complete_bootstrap_scope(effect_id, ticket, scope, now_ms)?;
    }
    let Some(PrivateFactsEffect::Bootstrap {
        effect_id,
        ticket,
        scope: PrivateBootstrapScope::Fills,
    }) = worker.next_effect(now_ms)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    if common_ticket != Some(ticket) {
        return Err(PrivateFactsWorkerError::Effect);
    }
    Ok((effect_id, ticket))
}

#[test]
fn scheduler_emits_only_one_effect_until_exact_completion() -> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    let effect = worker
        .next_effect(1)?
        .ok_or(PrivateFactsWorkerError::Effect)?;
    assert!(matches!(effect, PrivateFactsEffect::Connect { .. }));
    assert_eq!(worker.next_effect(1)?, None);
    assert!(
        worker
            .complete_connect(effect.effect_id().saturating_add(1))
            .is_err()
    );
    assert_eq!(worker.next_effect(1)?, None);
    worker.complete_connect(effect.effect_id())?;
    assert!(matches!(
        worker.next_effect(1)?,
        Some(PrivateFactsEffect::Bootstrap { .. })
    ));
    Ok(())
}

#[test]
fn post_mutation_reconciliation_revokes_ready_before_a_new_generation()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    connect_and_bootstrap(&mut worker, 100)?;
    let prior = worker.generation()?;
    assert!(worker.readiness()?.is_some());

    worker.request_post_mutation_reconciliation(200)?;
    assert!(worker.readiness()?.is_none());
    assert_eq!(worker.state(), PrivateFactsWorkerState::Backoff);
    assert!(matches!(
        worker.next_effect(450)?,
        Some(PrivateFactsEffect::Connect { .. })
    ));
    assert!(worker.generation()? > prior);
    Ok(())
}

#[test]
fn complete_bootstrap_is_required_before_ready_and_binds_fill_epoch()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    connect_and_bootstrap(&mut worker, 200)?;
    assert!(worker.entry_ready());
    assert_eq!(worker.state(), PrivateFactsWorkerState::Ready);
    assert_eq!(
        worker.readiness()?,
        Some(PrivateFactsReadiness {
            generation: worker.generation()?,
            observed_at_ms: 200,
            root_cause_fact_id: format!("private-readback:{}:200:0", worker.generation()?),
            exposure: PrivateExposure::Flat,
            ordinary_order_debt: false,
            algo_order_debt: false,
        })
    );
    let mut gate = PrivateEntryGate::new();
    let report = gate.observe_worker(
        &worker,
        complete_projection(worker.generation()?, 200),
        PrivateEntryGateInput {
            active_episode: false,
            entry_requested: true,
            now_ms: 200,
        },
    );
    assert!(report.entry_ready);
    assert!(report.forwarded_private.is_some());
    assert!(
        worker
            .fill_recovery
            .epoch_gate()
            .allows_ready(worker.generation()?)
    );
    Ok(())
}

#[test]
fn bootstrap_authority_produces_same_generation_anonymous_projections()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker_with_authority()?;
    connect_and_bootstrap(&mut worker, 200)?;
    let readiness = worker
        .readiness()?
        .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?;
    let projections = worker
        .authoritative_projections()?
        .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?;

    assert_eq!(projections.execution.value, ExecutionProjection::Known);
    assert_eq!(projections.owner.value, OwnerProjection::Clear);
    assert_eq!(projections.protection.value, ProtectionProjection::Complete);
    assert_eq!(
        projections.risk_budget.value,
        RiskBudgetProjection::Available
    );
    assert!(
        [
            (
                projections.execution.generation,
                projections.execution.observed_at_ms,
            ),
            (
                projections.owner.generation,
                projections.owner.observed_at_ms
            ),
            (
                projections.protection.generation,
                projections.protection.observed_at_ms,
            ),
            (
                projections.risk_budget.generation,
                projections.risk_budget.observed_at_ms,
            ),
        ]
        .into_iter()
        .all(|identity| identity == (readiness.generation, readiness.observed_at_ms))
    );
    let mut gate = PrivateEntryGate::new();
    let report = gate.observe_authoritative_worker(
        &worker,
        PrivateEntryGateInput {
            active_episode: false,
            entry_requested: true,
            now_ms: 200,
        },
    );
    assert!(report.entry_ready);
    assert!(report.forwarded_private.is_some());
    let clock = worker
        .authoritative_clock_root()?
        .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?;
    assert_eq!(clock.observed_at_ms, readiness.observed_at_ms);
    assert_eq!(
        clock.root_cause_fact_id,
        format!(
            "private-readback:{}:{}:0",
            readiness.generation, readiness.observed_at_ms
        )
    );
    Ok(())
}

#[test]
fn authority_worker_refreshes_before_its_private_projection_expires()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker_with_authority()?;
    connect_and_bootstrap(&mut worker, 200)?;
    let generation = worker.generation()?;

    let Some(PrivateFactsEffect::ReceiveFrame { effect_id, .. }) = worker.next_effect(700)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_no_frame(effect_id)?;
    let Some(PrivateFactsEffect::Bootstrap {
        effect_id,
        ticket,
        scope,
    }) = worker.next_effect(700)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    assert_eq!(scope, PrivateBootstrapScope::Account);
    assert!(!worker.entry_ready());
    worker.complete_bootstrap_scope(effect_id, ticket, scope, 700)?;
    for expected_scope in [
        PrivateBootstrapScope::Positions,
        PrivateBootstrapScope::PositionMode,
        PrivateBootstrapScope::AccountConfig,
        PrivateBootstrapScope::Orders,
        PrivateBootstrapScope::AlgoOrders,
    ] {
        let Some(PrivateFactsEffect::Bootstrap {
            effect_id,
            ticket: next_ticket,
            scope,
        }) = worker.next_effect(700)?
        else {
            return Err(PrivateFactsWorkerError::Effect);
        };
        if scope != expected_scope || next_ticket != ticket {
            return Err(PrivateFactsWorkerError::Effect);
        }
        worker.complete_bootstrap_scope(effect_id, next_ticket, scope, 700)?;
    }
    let Some(PrivateFactsEffect::Bootstrap {
        effect_id,
        ticket: final_ticket,
        scope: PrivateBootstrapScope::Fills,
    }) = worker.next_effect(700)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    if final_ticket != ticket {
        return Err(PrivateFactsWorkerError::Effect);
    }
    assert_eq!(ticket.generation(), generation);
    worker.complete_bootstrap(effect_id, bootstrap(&worker, ticket, 700)?, 700)?;

    let readiness = worker
        .readiness()?
        .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?;
    assert_eq!(readiness.generation, generation);
    assert_eq!(readiness.observed_at_ms, 700);
    assert!(worker.entry_ready());
    Ok(())
}

#[test]
fn caller_owned_periodic_readback_refreshes_without_projection_authority()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    worker.set_periodic_readback_interval(100)?;
    connect_and_bootstrap(&mut worker, 200)?;
    let generation = worker.generation()?;

    let Some(PrivateFactsEffect::ReceiveFrame { effect_id, .. }) = worker.next_effect(300)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_no_frame(effect_id)?;
    let Some(PrivateFactsEffect::Bootstrap { ticket, scope, .. }) = worker.next_effect(300)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    assert_eq!(scope, PrivateBootstrapScope::Account);
    assert_eq!(ticket.generation(), generation);
    assert!(worker.periodic_readback_in_progress());
    assert!(worker.readiness()?.is_none());
    Ok(())
}

#[test]
fn durable_full_fill_fast_path_defers_routine_readback() -> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    worker.enable_durable_fill_fast_path();
    worker.set_periodic_readback_interval(600_000)?;
    connect_and_bootstrap(&mut worker, 200)?;
    let private_generation = worker.generation()?;
    let Some(PrivateFactsEffect::ReceiveFrame {
        effect_id,
        next_sequence,
        ..
    }) = worker.next_effect(300)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    let payload = r#"{"e":"ORDER_TRADE_UPDATE","E":300,"o":{"s":"SOLUSDT","c":"hgo_e1_long_open_l1","x":"TRADE","X":"FILLED","t":42,"L":"140.25","m":true}}"#;

    worker.complete_frame(effect_id, next_sequence, 301, payload.to_owned(), 301)?;

    assert_eq!(worker.state(), PrivateFactsWorkerState::Ready);
    assert!(worker.readiness()?.is_none());
    assert_eq!(
        worker.take_durable_stream_full_fill(),
        Some(DurableStreamFullFill {
            fill_id: "42".to_owned(),
            private_generation,
            client_order_id: "hgo_e1_long_open_l1".to_owned(),
            event_time_ms: 300,
            received_at_ms: 301,
            fill_price: Price::new(Decimal::new(14025, 2))
                .map_err(|_| PrivateFactsWorkerError::Effect)?,
            maker: FieldState::Known(true),
        })
    );
    assert!(matches!(
        worker.next_effect(302)?,
        Some(PrivateFactsEffect::ReceiveFrame { .. })
    ));
    let pending_effect_id = worker
        .scheduler
        .pending_effect_id()
        .ok_or(PrivateFactsWorkerError::Effect)?;
    worker.complete_no_frame(pending_effect_id)?;
    let Some(PrivateFactsEffect::ReceiveFrame { effect_id, .. }) = worker.next_effect(600_200)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_no_frame(effect_id)?;
    assert!(matches!(
        worker.next_effect(600_201)?,
        Some(PrivateFactsEffect::Bootstrap {
            scope: PrivateBootstrapScope::Account,
            ..
        })
    ));
    Ok(())
}

#[test]
fn due_periodic_readback_never_jumps_over_a_buffered_user_stream_fill()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    worker.set_periodic_readback_interval(100)?;
    connect_and_bootstrap(&mut worker, 200)?;

    let Some(PrivateFactsEffect::ReceiveFrame {
        effect_id,
        next_sequence,
        ..
    }) = worker.next_effect(300)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    let signal = worker.complete_frame(
        effect_id,
        next_sequence,
        300,
        r#"{"e":"ORDER_TRADE_UPDATE"}"#.to_owned(),
        300,
    )?;
    assert_eq!(signal, PrivateSignal::ReadbackRequired);
    assert!(!worker.periodic_readback_in_progress());
    assert!(matches!(
        worker.next_effect(300)?,
        Some(PrivateFactsEffect::Bootstrap {
            scope: PrivateBootstrapScope::Account,
            ..
        })
    ));
    Ok(())
}

#[test]
fn ordinary_order_burst_is_drained_before_one_debounced_readback()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    connect_and_bootstrap(&mut worker, 200)?;

    for (now_ms, sequence) in [(201, 1), (250, 2)] {
        let Some(PrivateFactsEffect::ReceiveFrame {
            effect_id,
            next_sequence,
            ..
        }) = worker.next_effect(now_ms)?
        else {
            return Err(PrivateFactsWorkerError::Effect);
        };
        assert_eq!(next_sequence, sequence);
        assert_eq!(
            worker.complete_frame(
                effect_id,
                next_sequence,
                now_ms,
                r#"{"e":"ORDER_TRADE_UPDATE","o":{"x":"NEW","X":"NEW"}}"#.to_owned(),
                now_ms,
            )?,
            PrivateSignal::OrderLifecycleDebounced
        );
    }
    assert!(worker.readiness()?.is_none());

    let Some(PrivateFactsEffect::ReceiveFrame { effect_id, .. }) = worker.next_effect(349)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_no_frame(effect_id)?;
    assert!(matches!(
        worker.next_effect(350)?,
        Some(PrivateFactsEffect::Bootstrap {
            scope: PrivateBootstrapScope::Account,
            ..
        })
    ));
    Ok(())
}

#[test]
fn stale_individual_bootstrap_completion_is_rejected_and_fenced()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    let Some(PrivateFactsEffect::Connect { effect_id }) = worker.next_effect(1)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_connect(effect_id)?;
    let Some(PrivateFactsEffect::Bootstrap {
        effect_id,
        ticket,
        scope: PrivateBootstrapScope::Account,
    }) = worker.next_effect(2)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    let replacement = BinancePrivateFactsWorker::open(worker.config.clone())?;
    assert!(replacement.generation()? > ticket.generation());
    assert!(matches!(
        worker.complete_bootstrap_scope(effect_id, ticket, PrivateBootstrapScope::Account, 2,),
        Err(PrivateFactsWorkerError::StaleEpoch)
    ));
    assert!(!worker.entry_ready());
    assert_eq!(worker.state(), PrivateFactsWorkerState::Backoff);
    Ok(())
}

#[test]
fn empty_or_single_leg_bootstrap_never_becomes_ready() -> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    let Some(PrivateFactsEffect::Connect { effect_id }) = worker.next_effect(1)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_connect(effect_id)?;
    let (effect_id, ticket) = advance_to_fills(&mut worker, 2)?;
    let mut incomplete = bootstrap(&worker, ticket, 200)?;
    incomplete.account.positions.pop();
    assert!(matches!(
        worker.complete_bootstrap(effect_id, incomplete, 2),
        Err(PrivateFactsWorkerError::IncompleteBootstrap)
    ));
    assert!(!worker.entry_ready());
    assert_eq!(worker.state(), PrivateFactsWorkerState::Backoff);
    Ok(())
}

#[test]
fn residual_algo_debt_is_committed_but_flat_entry_remains_fenced()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    let Some(PrivateFactsEffect::Connect { effect_id }) = worker.next_effect(1)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_connect(effect_id)?;
    let (effect_id, ticket) = advance_to_fills(&mut worker, 2)?;
    let mut with_debt = bootstrap(&worker, ticket, 200)?;
    with_debt.open_algo_orders = vec![binance_private::parse_algo_order(
        r#"{"symbol":"SOLUSDT","algoId":1,"clientAlgoId":"unresolved-protection","algoStatus":"NEW"}"#,
        &worker.config.symbol,
        "unresolved-protection",
    )?];
    worker.complete_bootstrap(effect_id, with_debt, 2)?;
    assert!(worker.entry_ready());
    assert_eq!(worker.state(), PrivateFactsWorkerState::Ready);
    let readiness = worker
        .readiness()?
        .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?;
    assert!(readiness.algo_order_debt);
    let mut gate = PrivateEntryGate::new();
    let report = gate.observe_worker(
        &worker,
        complete_projection(readiness.generation, readiness.observed_at_ms),
        PrivateEntryGateInput {
            active_episode: false,
            entry_requested: true,
            now_ms: 200,
        },
    );
    assert!(!report.entry_ready);
    assert!(report.forwarded_private.is_none());
    Ok(())
}

#[test]
fn backoff_is_jittered_exponential_and_explicitly_capped() -> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    let mut now = 1_u64;
    for failure in 1_u8..=12 {
        let Some(PrivateFactsEffect::Connect { effect_id }) = worker.next_effect(now)? else {
            return Err(PrivateFactsWorkerError::Effect);
        };
        worker.complete_transport_failure(effect_id, now)?;
        let delay = worker.scheduler.next_retry_at_ms().saturating_sub(now);
        let upper = BASE_BACKOFF_MS
            .saturating_mul(1_u64 << u32::from((failure - 1).min(15)))
            .min(MAX_BACKOFF_MS);
        assert!(delay >= (upper / 2).max(1));
        assert!(delay <= upper);
        now = worker.scheduler.next_retry_at_ms();
    }
    assert_eq!(
        worker.idle_wait(now.saturating_sub(1)),
        Duration::from_millis(1)
    );
    Ok(())
}

#[test]
fn sequence_gap_fences_session_and_uses_bounded_exponential_backoff()
-> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    connect_and_bootstrap(&mut worker, 200)?;
    let Some(PrivateFactsEffect::ReceiveFrame {
        effect_id,
        next_sequence,
        ..
    }) = worker.next_effect(201)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    assert!(matches!(
        worker.complete_frame(
            effect_id,
            next_sequence.saturating_add(1),
            201,
            r#"{"e":"ORDER_TRADE_UPDATE"}"#.to_owned(),
            201,
        ),
        Err(PrivateFactsWorkerError::SequenceGap)
    ));
    assert!(!worker.entry_ready());
    let retry_at = worker.scheduler.next_retry_at_ms();
    assert!(retry_at > 201);
    assert_eq!(worker.next_effect(retry_at.saturating_sub(1))?, None);
    assert!(matches!(
        worker.next_effect(retry_at)?,
        Some(PrivateFactsEffect::Connect { .. })
    ));
    Ok(())
}

#[test]
fn frame_revokes_ready_and_cross_epoch_bootstrap_is_rejected() -> Result<(), PrivateFactsWorkerError>
{
    let (_directory, mut worker) = worker()?;
    connect_and_bootstrap(&mut worker, 200)?;
    let Some(PrivateFactsEffect::ReceiveFrame {
        effect_id,
        next_sequence,
        ..
    }) = worker.next_effect(201)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    assert_eq!(
        worker.complete_frame(
            effect_id,
            next_sequence,
            201,
            r#"{"e":"ACCOUNT_UPDATE"}"#.to_owned(),
            201,
        )?,
        PrivateSignal::ReadbackRequired
    );
    assert!(!worker.entry_ready());
    assert_eq!(worker.readiness()?, None);
    let Some(PrivateFactsEffect::Bootstrap {
        effect_id, ticket, ..
    }) = worker.next_effect(202)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_transport_failure(effect_id, 202)?;
    let stale = bootstrap(&worker, ticket, 300)?;
    assert_ne!(worker.generation()?, ticket.generation());
    assert!(matches!(
        worker.commit_bootstrap(ticket, stale),
        Err(PrivateFactsWorkerError::StaleEpoch)
    ));
    Ok(())
}

#[test]
fn parser_failure_fences_ready_after_persisting_raw_evidence() -> Result<(), PrivateFactsWorkerError>
{
    let (_directory, mut worker) = worker()?;
    connect_and_bootstrap(&mut worker, 200)?;
    let Some(PrivateFactsEffect::ReceiveFrame {
        effect_id,
        next_sequence,
        ..
    }) = worker.next_effect(201)?
    else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    assert!(
        worker
            .complete_frame(
                effect_id,
                next_sequence,
                201,
                r#"{"unsupported":true}"#.to_owned(),
                201,
            )
            .is_err()
    );
    assert!(!worker.entry_ready());
    let session = worker.lock_session()?;
    assert_eq!(session.journal().recover()?.len(), 1);
    Ok(())
}

#[test]
fn fill_facts_and_account_facts_share_one_journal() -> Result<(), PrivateFactsWorkerError> {
    let (_directory, mut worker) = worker()?;
    let Some(PrivateFactsEffect::Connect { effect_id }) = worker.next_effect(1)? else {
        return Err(PrivateFactsWorkerError::Effect);
    };
    worker.complete_connect(effect_id)?;
    let (effect_id, ticket) = advance_to_fills(&mut worker, 2)?;
    let mut value = bootstrap(&worker, ticket, 200)?;
    value.fills.payload = r#"[{"symbol":"SOLUSDT","id":"1","orderId":"10","side":"BUY","positionSide":"LONG","qty":"0.01","price":"100","commission":"0.001","commissionAsset":"USDT","realizedPnl":"0","marginAsset":"USDT","maker":false,"time":150}]"#.to_owned();
    value.fills.cursor.last_trade_id = Some(1);
    value.fills.cursor.last_event_time_ms = Some(150);
    worker.complete_bootstrap(effect_id, value, 2)?;
    let entries = worker.facts.recover()?.entries;
    assert_eq!(
        worker.fill_recovery.reconciler().facts().records().len(),
        entries.len()
    );
    assert!(
        entries
            .iter()
            .any(|entry| matches!(entry.record.event, crate::domain::DomainEvent::Fill(_)))
    );
    assert!(
        entries
            .iter()
            .any(|entry| matches!(entry.record.event, crate::domain::DomainEvent::Position(_)))
    );
    Ok(())
}
