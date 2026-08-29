use super::*;

fn prepare_wal(
    runtime: &mut AccountRuntime,
    wal_sequence: u64,
) -> Result<(PersistedWalPreparedReceipt, PersistedWriterLeaseReceipt), Box<dyn Error>> {
    let candidate = runtime
        .next_execution_for_wal()?
        .ok_or("runtime mutation candidate missing")?;
    let wal = PersistedWalPreparedReceipt::test_persisted(candidate, wal_sequence)?;
    let writer = PersistedWriterLeaseReceipt::test_verified_current(
        &wal,
        AccountWriterCapability::EntryAndRiskReduction,
        wal_sequence,
    )?;
    Ok((wal, writer))
}

fn settle_fenced(
    runtime: &mut AccountRuntime,
    fence: &crate::execution::AccountWalPreparedFence,
    outcome_sequence: u64,
) -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        runtime.next_execution_for_wal(),
        Err(AccountRuntimeError::ExecutionLane(
            AccountLaneError::WalPreparedPending
        ))
    ));
    assert!(matches!(
        runtime.record_execution_outcome(
            PersistedMutationOutcomeReceipt::test_persisted_without_dispatch(
                fence,
                AccountMutationOutcome::NotDispatched,
                outcome_sequence,
            )?,
        )?,
        AccountLaneFollowUp::StrategyReplanRequired {
            reason: crate::execution::AccountReplanReason::DispatchFenced,
            ..
        }
    ));
    Ok(())
}

#[test]
fn wal_prepared_before_pause_cannot_dispatch_or_return_to_queue() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.enqueue_execution(runtime_place_intent(
        &runtime,
        &grid,
        "wal_then_pause",
        AccountLanePriority::Normal,
    )?)?;
    let (wal, writer) = prepare_wal(&mut runtime, 1)?;

    runtime.request_pause(&grid.key)?;
    let AccountDispatchDecision::Fenced(fence) =
        runtime.authorize_execution_dispatch(wal, writer)?
    else {
        return Err("paused WAL-prepared mutation received a dispatch permit".into());
    };
    settle_fenced(&mut runtime, &fence, 1)?;
    assert!(runtime.next_execution_for_wal()?.is_none());
    Ok(())
}

#[test]
fn wal_prepared_before_private_change_cannot_dispatch_or_return_to_queue()
-> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.enqueue_execution(runtime_place_intent(
        &runtime,
        &grid,
        "wal_then_private",
        AccountLanePriority::Normal,
    )?)?;
    let (wal, writer) = prepare_wal(&mut runtime, 2)?;

    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 100, "private fact after WAL")?;
    route_persisted_private(
        &mut runtime,
        private_fact(
            &persisted,
            DomainEvent::Balance(AccountBalance {
                asset: "USDT".parse()?,
                wallet_balance: Decimal::ONE,
                available_balance: Decimal::ONE,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            }),
        )?,
    )?;
    let AccountDispatchDecision::Fenced(fence) =
        runtime.authorize_execution_dispatch(wal, writer)?
    else {
        return Err("private-change WAL-prepared mutation received a dispatch permit".into());
    };
    settle_fenced(&mut runtime, &fence, 2)?;
    assert!(runtime.next_execution_for_wal()?.is_none());
    Ok(())
}
