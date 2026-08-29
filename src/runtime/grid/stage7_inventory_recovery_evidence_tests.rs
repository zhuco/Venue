use std::fs;

use rust_decimal::Decimal;

use super::{
    stage7_canary_support::{STAGE7_LIVE_ADMISSION_FILE, Stage7LiveAdmissionEvidence},
    stage7_inventory_recovery_evidence::{
        INVENTORY_RECOVERY_EVIDENCE_FILE, InventoryRecoveryEvidenceError,
        capture_stage7_settlement, recovery_predates_generation,
        verify_stage7_inventory_recovery_evidence,
    },
    *,
};
use crate::{
    domain::{Amount, Asset, FieldState, Instrument, MarketKind, Price},
    execution::{CapabilityBinding, CommandJournal},
    storage::{PrivateEvidence, PrivateEvidenceJournal, ProjectionStore},
    strategy::hedged_grid::{
        GridEpoch, GridInventory, GridOrderRole, GridPosition, HedgedGridBinding, HedgedGridParams,
        HedgedGridState, InventoryRecoveryState, OwnedGridFill,
    },
};

fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
    Ok(HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "binance".to_owned(),
        account: "00000000-0000-4000-8000-000000000001".to_owned(),
        symbol: "SOL/USDC".parse()?,
        config_version: "shared-grid-v1".to_owned(),
        owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
    })
}

fn inventory(
    generation: u64,
    quantity: Decimal,
) -> Result<GridInventory, Box<dyn std::error::Error>> {
    Ok(GridInventory {
        private_generation: generation,
        private_observed_at_ms: generation * 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: quantity,
        short_quantity: quantity,
    })
}

fn epoch(epoch: u64, anchor_price: Price) -> Result<GridEpoch, Box<dyn std::error::Error>> {
    Ok(GridEpoch {
        epoch,
        anchor_price,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 0),
        passive_book_fallback: None,
    })
}

fn persist_admission(
    root: &std::path::Path,
    binding: &HedgedGridBinding,
    params: &HedgedGridParams,
) -> Result<Stage7LiveAdmissionEvidence, Box<dyn std::error::Error>> {
    let capability = CapabilityBinding {
        exchange: "binance".to_owned(),
        account_binding: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDC".to_owned(),
        api_key_sha256: "a".repeat(64),
    };
    let instrument = Instrument {
        symbol: binding.symbol.clone(),
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDC")?),
        generation: 9,
        price_tick: Price::new(Decimal::new(1, 3))?,
        quantity_step: Decimal::new(1, 2),
        minimum_notional: Amount::new(Asset::new("USDC")?, Decimal::ZERO),
    };
    let admission = Stage7LiveAdmissionEvidence::new(
        capability,
        binding.clone(),
        params.clone(),
        instrument,
        Decimal::new(1, 2),
        "b".repeat(64),
        10,
        100,
        1,
        1,
    )?;
    ProjectionStore::new(root.join(STAGE7_LIVE_ADMISSION_FILE)).save(&admission)?;
    Ok(admission)
}

fn complete_ten_level_evidence_with_fallback(
    use_passive_book_fallback: bool,
) -> Result<(tempfile::TempDir, InventoryRecoveryAcceptanceReport), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let binding = binding()?;
    let params = HedgedGridParams::fixed_release(Asset::new("USDC")?, 10)?;
    let admission = persist_admission(root, &binding, &params)?;
    let mut private = PrivateEvidenceJournal::open(root.join(PRIVATE_EVIDENCE_FILE))?;
    for generation in 1..=5 {
        private.append(PrivateEvidence::new(
            generation,
            generation * 100,
            format!("signed-private-generation-{generation}"),
        )?)?;
    }
    fs::write(root.join(COMMAND_FILE), b"")?;

    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let _ = state.observe_inventory(inventory(1, Decimal::new(45, 0))?)?;
    let original_anchor = Price::new(Decimal::new(100, 0))?;
    let _ = state.install_epoch(epoch(1, original_anchor)?)?;
    assert!(matches!(
        state.inventory_recovery,
        InventoryRecoveryState::Deficient { .. }
    ));
    let mut checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let store = ProjectionStore::new(root.join(CHECKPOINT_FILE));
    save_checkpoint(&store, &checkpoint)?;

    let _ = checkpoint
        .state
        .observe_inventory(inventory(2, Decimal::new(50, 0))?)?;
    checkpoint.private_generation = 2;
    save_checkpoint(&store, &checkpoint)?;
    assert_eq!(
        checkpoint.state.inventory_recovery,
        InventoryRecoveryState::AwaitingNextOwnedFill {
            armed_generation: 2
        }
    );

    let source_order = checkpoint
        .state
        .owned_orders
        .keys()
        .find(|key| {
            key.position == GridPosition::Short && key.role == GridOrderRole::Open && key.level == 1
        })
        .cloned()
        .ok_or("missing owned maker source")?;
    let fill_price = Price::new(Decimal::new(101_234, 3))?;
    let _ = checkpoint.state.observe_owned_fill(OwnedGridFill {
        fill_id: "binance-owned-maker-1".to_owned(),
        private_generation: 3,
        source_order,
        fill_price,
        complete: true,
        maker: FieldState::Known(true),
    })?;
    checkpoint.private_generation = 3;
    // Simulate a crash after the ReanchorPending checkpoint was durable but before its evidence
    // append. Rebuilding recovery reconstructs both records from the immutable owned-fill fact.
    checkpoint.state.begin_reanchor_rebuild()?;
    save_checkpoint(&store, &checkpoint)?;

    checkpoint.state.reset_orders_settled()?;
    let _ = checkpoint
        .state
        .observe_inventory(inventory(4, Decimal::new(50, 0))?)?;
    let mut rebuilt = epoch(
        2,
        if use_passive_book_fallback {
            Price::new(Decimal::new(100, 0))?
        } else {
            fill_price
        },
    )?;
    if use_passive_book_fallback {
        rebuilt.passive_book_fallback =
            Some(crate::strategy::hedged_grid::PassiveBookFallbackAnchor {
                fill_id: "binance-owned-maker-1".to_owned(),
                fill_price,
                anchor_price: rebuilt.anchor_price,
                crossing_side: crate::domain::OrderSide::Buy,
                crossing_limit_price: Price::new(Decimal::new(101_034, 3))?,
                bid: Price::new(Decimal::new(999, 1))?,
                ask: Price::new(Decimal::new(1001, 1))?,
                selected_at_ms: 450,
            });
    }
    let _ = checkpoint.state.install_epoch(rebuilt)?;
    // The independent exposure poll may already have advanced the account-level checkpoint
    // watermark while the grid inventory projection still belongs to generation 4.
    checkpoint.private_generation = 5;
    checkpoint.state.complete_reanchor_rebuild()?;
    save_checkpoint(&store, &checkpoint)?;
    let commands = CommandJournal::open(root.join(COMMAND_FILE))?;
    capture_stage7_settlement(
        root,
        &checkpoint,
        &commands,
        checkpoint.state.owned_orders.len(),
    )?;

    let report = verify_stage7_inventory_recovery_evidence(root)?;
    assert_eq!(report.admission_sha256, admission.admission_sha256);
    Ok((temporary, report))
}

fn complete_ten_level_evidence()
-> Result<(tempfile::TempDir, InventoryRecoveryAcceptanceReport), Box<dyn std::error::Error>> {
    complete_ten_level_evidence_with_fallback(false)
}

#[test]
fn ten_level_recovery_persists_signed_fill_anchor_and_offline_acceptance()
-> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, report) = complete_ten_level_evidence()?;
    assert_eq!(report.deficient_generation, 1);
    assert_eq!(report.armed_generation, 2);
    assert_eq!(report.fill_generation, 3);
    assert_eq!(report.settled_generation, 4);
    assert_eq!(report.fill_id, "binance-owned-maker-1");
    assert_eq!(report.fill_price, Price::new(Decimal::new(101_234, 3))?);
    assert_eq!(report.rebuilt_anchor, report.fill_price);
    assert_eq!(report.rebuilt_epoch, 2);
    assert_eq!(report.desired_orders, 40);
    assert_eq!(report.observed_orders, 40);
    Ok(())
}

#[test]
fn crossing_fill_anchor_accepts_only_the_persisted_passive_book_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    let (temporary, report) = complete_ten_level_evidence_with_fallback(true)?;
    assert_ne!(report.rebuilt_anchor, report.fill_price);
    let fallback = report
        .passive_book_fallback
        .as_ref()
        .ok_or("missing fallback evidence")?;
    assert_eq!(fallback.anchor_price, report.rebuilt_anchor);
    assert!(fallback.validate().is_ok());

    let store = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE));
    let mut checkpoint = store
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;
    checkpoint
        .state
        .epoch
        .as_mut()
        .ok_or("missing epoch")?
        .passive_book_fallback = None;
    store.save(&checkpoint)?;
    assert!(matches!(
        verify_stage7_inventory_recovery_evidence(temporary.path()),
        Err(InventoryRecoveryEvidenceError::Transition)
    ));
    Ok(())
}

#[test]
fn verifier_rejects_checkpoint_watermark_rollback_below_settlement()
-> Result<(), Box<dyn std::error::Error>> {
    let (temporary, report) = complete_ten_level_evidence()?;
    assert_eq!(report.settled_generation, 4);
    assert_eq!(report.settlement_checkpoint_generation, 5);

    let store = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE));
    let mut checkpoint = store
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;
    checkpoint.private_generation = 4;
    store.save(&checkpoint)?;
    assert!(matches!(
        verify_stage7_inventory_recovery_evidence(temporary.path()),
        Err(InventoryRecoveryEvidenceError::Transition)
    ));
    Ok(())
}

#[test]
fn relocation_boundary_skips_only_an_inherited_recovery_episode()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let params = HedgedGridParams::fixed_release(Asset::new("USDC")?, 10)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let source_order = crate::strategy::hedged_grid::GridOrderIntent {
        key: crate::strategy::hedged_grid::GridOrderKey {
            epoch: 1,
            position: GridPosition::Long,
            role: GridOrderRole::Close,
            level: 1,
        },
        side: crate::domain::OrderSide::Sell,
        quantity: Decimal::ONE,
        price: Price::new(Decimal::new(100, 0))?,
        reduce_only: true,
    };
    state.owned_fill_records.insert(
        "inherited-fill".to_owned(),
        crate::strategy::hedged_grid::OwnedGridFillRecord {
            source_order,
            fill_price: Price::new(Decimal::new(100, 0))?,
            private_generation: 9,
            maker: Some(true),
            grid_action_emitted: true,
            retired_without_action: false,
        },
    );
    state.inventory_recovery = InventoryRecoveryState::Rebuilding {
        fill_id: "inherited-fill".to_owned(),
        fill_price: Price::new(Decimal::new(100, 0))?,
    };
    let mut checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding,
        state,
        private_generation: 10,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };

    assert!(recovery_predates_generation(&checkpoint, 10));
    checkpoint
        .state
        .owned_fill_records
        .get_mut("inherited-fill")
        .ok_or("missing inherited fill")?
        .private_generation = 10;
    assert!(!recovery_predates_generation(&checkpoint, 10));
    Ok(())
}

#[test]
fn evidence_tampering_fails_closed_before_acceptance() -> Result<(), Box<dyn std::error::Error>> {
    let (temporary, _) = complete_ten_level_evidence()?;
    let path = temporary.path().join(INVENTORY_RECOVERY_EVIDENCE_FILE);
    let original = fs::read_to_string(&path)?;
    let tampered = original.replacen("\"maker\":true", "\"maker\":false", 1);
    if original == tampered {
        return Err("maker field was not present in evidence".into());
    }
    fs::write(path, tampered)?;
    assert!(matches!(
        verify_stage7_inventory_recovery_evidence(temporary.path()),
        Err(InventoryRecoveryEvidenceError::HashChain)
    ));
    Ok(())
}

#[test]
fn incomplete_chain_returns_before_opening_large_journals() -> Result<(), Box<dyn std::error::Error>>
{
    let (temporary, _) = complete_ten_level_evidence()?;
    let root = temporary.path();
    let evidence_path = root.join(INVENTORY_RECOVERY_EVIDENCE_FILE);
    let complete = fs::read_to_string(&evidence_path)?;
    let mut lines = complete.lines().collect::<Vec<_>>();
    if lines.len() != 5 {
        return Err("expected one complete five-stage episode".into());
    }
    lines.pop();
    fs::write(evidence_path, format!("{}\n", lines.join("\n")))?;

    // Directories are deliberately not openable as journals. Incomplete evidence must be
    // decided before either potentially very large production file is touched.
    for name in [PRIVATE_EVIDENCE_FILE, COMMAND_FILE] {
        let path = root.join(name);
        fs::remove_file(&path)?;
        fs::create_dir(&path)?;
    }
    assert!(matches!(
        verify_stage7_inventory_recovery_evidence(root),
        Err(InventoryRecoveryEvidenceError::Incomplete)
    ));
    Ok(())
}

#[test]
fn referenced_private_payload_tampering_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let (temporary, _) = complete_ten_level_evidence()?;
    let private_path = temporary.path().join(PRIVATE_EVIDENCE_FILE);
    let original = fs::read_to_string(&private_path)?;
    let tampered = original.replacen(
        "signed-private-generation-2",
        "signed-private-generation-X",
        1,
    );
    if original == tampered {
        return Err("referenced private payload was not present".into());
    }
    fs::write(private_path, tampered)?;
    assert!(matches!(
        verify_stage7_inventory_recovery_evidence(temporary.path()),
        Err(InventoryRecoveryEvidenceError::PrivateReference)
    ));
    Ok(())
}

#[test]
fn verifier_rejects_missing_or_tampered_checkpoint_fill_fact()
-> Result<(), Box<dyn std::error::Error>> {
    let (temporary, report) = complete_ten_level_evidence()?;
    let root = temporary.path();
    let store = ProjectionStore::new(root.join(CHECKPOINT_FILE));
    let original = store
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;

    let mut variants = Vec::new();
    let mut missing = original.clone();
    missing.state.owned_fill_records.remove(&report.fill_id);
    variants.push(missing);

    let mut wrong_maker = original.clone();
    wrong_maker
        .state
        .owned_fill_records
        .get_mut(&report.fill_id)
        .ok_or("missing owned fill")?
        .maker = Some(false);
    variants.push(wrong_maker);

    let mut action_not_emitted = original.clone();
    action_not_emitted
        .state
        .owned_fill_records
        .get_mut(&report.fill_id)
        .ok_or("missing owned fill")?
        .grid_action_emitted = false;
    variants.push(action_not_emitted);

    let mut retired = original.clone();
    retired
        .state
        .owned_fill_records
        .get_mut(&report.fill_id)
        .ok_or("missing owned fill")?
        .retired_without_action = true;
    variants.push(retired);

    let mut wrong_generation = original.clone();
    wrong_generation
        .state
        .owned_fill_records
        .get_mut(&report.fill_id)
        .ok_or("missing owned fill")?
        .private_generation += 1;
    variants.push(wrong_generation);

    let mut wrong_price = original.clone();
    wrong_price
        .state
        .owned_fill_records
        .get_mut(&report.fill_id)
        .ok_or("missing owned fill")?
        .fill_price = Price::new(Decimal::new(999, 0))?;
    variants.push(wrong_price);

    for checkpoint in variants {
        store.save(&checkpoint)?;
        assert!(matches!(
            verify_stage7_inventory_recovery_evidence(root),
            Err(InventoryRecoveryEvidenceError::Transition)
        ));
    }
    Ok(())
}

#[test]
fn verifier_preserves_acceptance_after_later_epochs_but_rejects_final_epoch_drift()
-> Result<(), Box<dyn std::error::Error>> {
    let (temporary, _) = complete_ten_level_evidence()?;
    let root = temporary.path();
    let store = ProjectionStore::new(root.join(CHECKPOINT_FILE));
    let original = store
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;

    let mut wrong_anchor = original.clone();
    wrong_anchor
        .state
        .epoch
        .as_mut()
        .ok_or("missing epoch")?
        .anchor_price = Price::new(Decimal::new(999, 0))?;
    store.save(&wrong_anchor)?;
    assert!(matches!(
        verify_stage7_inventory_recovery_evidence(root),
        Err(InventoryRecoveryEvidenceError::Transition)
    ));

    let mut later_epoch = original.clone();
    let later = later_epoch.state.epoch.as_mut().ok_or("missing epoch")?;
    later.epoch += 1;
    later.anchor_price = Price::new(Decimal::new(102, 0))?;
    store.save(&later_epoch)?;
    verify_stage7_inventory_recovery_evidence(root)?;

    let mut rolled_back_epoch = original;
    rolled_back_epoch
        .state
        .epoch
        .as_mut()
        .ok_or("missing epoch")?
        .epoch -= 1;
    store.save(&rolled_back_epoch)?;
    assert!(matches!(
        verify_stage7_inventory_recovery_evidence(root),
        Err(InventoryRecoveryEvidenceError::Transition)
    ));
    Ok(())
}
