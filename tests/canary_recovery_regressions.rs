use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{PositionSide, Symbol},
    execution::{CanaryRunBinding, CanaryRunState},
    runtime::{
        AlgoOrderReadback, CANARY_RECOVERY_SCHEMA_VERSION, CanaryRecoveryPlan,
        HedgePositionReadback, OrdinaryOrderReadback, ProtectionDebtState, RecoveryAlgoOrder,
        RecoveryOrdinaryOrder, RemainFencedReason, SignedCanaryReadback, plan_canary_recovery,
        scan_unfinished,
    },
};

fn digest(byte: char) -> String {
    byte.to_string().repeat(64)
}

fn binding() -> Result<CanaryRunBinding, Box<dyn std::error::Error>> {
    Ok(CanaryRunBinding {
        canary_id: "canary-recovery-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse::<Symbol>()?,
        owner_scope: "canary-recovery-owner".to_owned(),
        release_id: "release-1".to_owned(),
        position_side: PositionSide::Long,
        writer_generation: 7,
        readback_generation: 7,
        valid_until_ms: 10_000,
    })
}

fn clean_readback(
    readback_id: &str,
    observed_at_ms: u64,
    generation: u64,
) -> Result<SignedCanaryReadback, Box<dyn std::error::Error>> {
    let mut readback = SignedCanaryReadback {
        schema_version: CANARY_RECOVERY_SCHEMA_VERSION,
        readback_id: readback_id.to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        generation,
        observed_at_ms,
        signer_sha256: digest('a'),
        payload_sha256: String::new(),
        signature_sha256: digest(if readback_id == "first" { 'd' } else { 'e' }),
        signature_verified: true,
        positions: HedgePositionReadback::Known {
            long_quantity: Decimal::ZERO,
            short_quantity: Decimal::ZERO,
        },
        ordinary_orders: OrdinaryOrderReadback::Known(Vec::new()),
        algo_orders: AlgoOrderReadback::Known(Vec::new()),
    };
    refresh_payload(&mut readback)?;
    Ok(readback)
}

fn refresh_payload(readback: &mut SignedCanaryReadback) -> Result<(), Box<dyn std::error::Error>> {
    readback.payload_sha256 = readback.calculate_payload_sha256()?;
    Ok(())
}

fn recovered_candidate() -> Result<(tempfile::TempDir, CanaryRunState), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let path = directory.path().join("run.json");
    let mut run = CanaryRunState::create_new(&path, binding()?, 100)?;
    run.entry_submitted(digest('f'), 101)?;
    drop(run);
    let recovered = CanaryRunState::recover_existing(path, 200)?;
    Ok((directory, recovered))
}

#[test]
fn crash_restart_requires_two_distinct_clean_confirmations()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, recovered) = recovered_candidate()?;
    let candidates = scan_unfinished([&recovered]);
    assert_eq!(candidates.len(), 1);
    let first = clean_readback("first", 201, 7)?;
    assert!(matches!(
        plan_canary_recovery(&candidates[0], &digest('a'), &first, &first),
        CanaryRecoveryPlan::RemainFenced {
            reason: RemainFencedReason::DuplicateConfirmation,
            ..
        }
    ));

    let second = clean_readback("second", 202, 7)?;
    assert!(matches!(
        plan_canary_recovery(&candidates[0], &digest('a'), &first, &second),
        CanaryRecoveryPlan::SealFlat { generation: 7, .. }
    ));
    Ok(())
}

#[test]
fn late_fill_after_first_flat_confirmation_emergency_flattens()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, recovered) = recovered_candidate()?;
    let candidates = scan_unfinished([&recovered]);
    let candidate = &candidates[0];
    let first = clean_readback("first", 201, 7)?;
    let mut late_fill = clean_readback("second", 202, 7)?;
    late_fill.positions = HedgePositionReadback::Known {
        long_quantity: Decimal::new(1, 3),
        short_quantity: Decimal::ZERO,
    };
    refresh_payload(&mut late_fill)?;
    assert!(matches!(
        plan_canary_recovery(candidate, &digest('a'), &first, &late_fill),
        CanaryRecoveryPlan::EmergencyFlatten {
            protection_debt: ProtectionDebtState::Confirmed,
            ref legs,
            ..
        } if legs.len() == 1 && legs[0].position_side == PositionSide::Long
    ));
    Ok(())
}

#[test]
fn flat_with_residual_algo_emits_only_exact_algo_cancel() -> Result<(), Box<dyn std::error::Error>>
{
    let (_directory, recovered) = recovered_candidate()?;
    let candidates = scan_unfinished([&recovered]);
    let candidate = &candidates[0];
    let first = clean_readback("first", 201, 7)?;
    let mut second = clean_readback("second", 202, 7)?;
    second.algo_orders = AlgoOrderReadback::Known(vec![RecoveryAlgoOrder {
        owner_scope: binding()?.owner_scope,
        command_id: "cancel-protection-command".to_owned(),
        client_algo_id: "canary-protection-algo".to_owned(),
    }]);
    refresh_payload(&mut second)?;
    assert!(matches!(
        plan_canary_recovery(candidate, &digest('a'), &first, &second),
        CanaryRecoveryPlan::ExactCancel {
            ref ordinary,
            ref algos,
            ..
        } if ordinary.is_empty()
            && algos.len() == 1
            && algos[0].client_algo_id == "canary-protection-algo"
    ));
    Ok(())
}

#[test]
fn readback_generation_mismatch_and_unknown_algo_remain_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, recovered) = recovered_candidate()?;
    let candidates = scan_unfinished([&recovered]);
    let candidate = &candidates[0];
    let first = clean_readback("first", 201, 7)?;
    let wrong_generation = clean_readback("second", 202, 8)?;
    assert!(matches!(
        plan_canary_recovery(candidate, &digest('a'), &first, &wrong_generation),
        CanaryRecoveryPlan::RemainFenced {
            reason: RemainFencedReason::GenerationMismatch,
            ..
        }
    ));

    let mut unknown_algo = clean_readback("second", 202, 7)?;
    unknown_algo.algo_orders = AlgoOrderReadback::Unknown;
    refresh_payload(&mut unknown_algo)?;
    assert!(matches!(
        plan_canary_recovery(candidate, &digest('a'), &first, &unknown_algo),
        CanaryRecoveryPlan::RemainFenced {
            reason: RemainFencedReason::UnknownFacts,
            ..
        }
    ));
    Ok(())
}

#[test]
fn known_entry_cancel_debt_is_cancelled_even_when_positions_are_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, recovered) = recovered_candidate()?;
    let candidates = scan_unfinished([&recovered]);
    let candidate = &candidates[0];
    let first = clean_readback("first", 201, 7)?;
    let mut second = clean_readback("second", 202, 7)?;
    second.positions = HedgePositionReadback::Unknown;
    second.ordinary_orders = OrdinaryOrderReadback::Known(vec![RecoveryOrdinaryOrder {
        owner_scope: binding()?.owner_scope,
        command_id: "cancel-entry-command".to_owned(),
        client_order_id: "canary-entry-order".to_owned(),
    }]);
    refresh_payload(&mut second)?;
    assert!(matches!(
        plan_canary_recovery(candidate, &digest('a'), &first, &second),
        CanaryRecoveryPlan::ExactCancel {
            ref ordinary,
            ref algos,
            ..
        } if ordinary.len() == 1
            && ordinary[0].client_order_id == "canary-entry-order"
            && algos.is_empty()
    ));
    Ok(())
}
