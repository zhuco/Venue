use crate::config::ExposureTakeProfitConfig;
use crate::{
    domain::{
        AccountBalance, Amount, Asset, CommandId, Instrument, MarketKind, OrderCommand, OrderOwner,
        OrderPurpose, OrderSide, PositionSide, Price,
    },
    exchange::grid::GridOrderFamilyReadback,
};

use super::*;

#[test]
fn stopped_order_recovery_accepts_a_durable_resetting_failure() {
    assert!(stopped_order_recovery_phase(GridPhase::ResettingGrid));
    assert!(!stopped_order_recovery_phase(GridPhase::Recovering));
    assert!(!stopped_order_recovery_phase(
        GridPhase::ReplenishingInventory
    ));
}

#[test]
fn confirmed_stopped_recovery_can_resolve_an_order_health_fence() {
    assert!(handoff_health_fence_allowed(false, false));
    assert!(handoff_health_fence_allowed(true, true));
    assert!(!handoff_health_fence_allowed(true, false));
}

#[test]
fn handoff_preserves_fill_history_while_exposure_settlement_is_pending() {
    assert!(!handoff_fill_history_window_can_advance(true));
    assert!(handoff_fill_history_window_can_advance(false));
}

#[test]
fn resolved_command_wal_is_sealed_by_source_hash_and_replaced_empty()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let command_path = temporary.path().join(COMMAND_FILE);
    let source = b"durable-command-wal\n";
    fs::write(&command_path, source)?;
    let source_sha256 = sha256_hex(source);

    archive_resolved_command_wal(temporary.path(), &command_path)?;

    assert_eq!(fs::read(&command_path)?, Vec::<u8>::new());
    assert_eq!(
        fs::read(
            temporary
                .path()
                .join(COMMAND_WAL_ARCHIVE_DIRECTORY)
                .join(format!("commands-{source_sha256}.jsonl"))
        )?,
        source
    );
    archive_resolved_command_wal(temporary.path(), &command_path)?;
    Ok(())
}

#[test]
fn stopped_handoff_refreshes_only_the_expired_same_scope_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let writer_scope = stage7_writer_scope(&binding()?);
    let authority = WriterLeaseAuthority::open(temporary.path().join("writer.json"), writer_scope)?;
    let predecessor = authority.register_initial(10, 20)?;
    let refreshed = refresh_expired_stopped_writer_after_signed_readback(
        &authority,
        &predecessor,
        21,
        predecessor.valid_until_ms,
    )?;

    assert_eq!(refreshed.generation, predecessor.generation);
    assert!(refreshed.revision > predecessor.revision);
    assert_eq!(refreshed.readback_generation, 21);
    assert!(refreshed.valid_until_ms > predecessor.valid_until_ms);
    assert!(writer_readback_is_not_ahead(21, &refreshed));
    assert!(!writer_readback_is_not_ahead(20, &refreshed));
    Ok(())
}

fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
    Ok(HedgedGridBinding {
        owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
        strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: "00000000-0000-4000-8000-000000000001".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        config_version: "stage7".to_owned(),
    })
}

fn capability_binding() -> CapabilityBinding {
    CapabilityBinding {
        exchange: "gate".to_owned(),
        account_binding: "usdt_futures_dual".to_owned(),
        symbol: "DOGE/USDT".to_owned(),
        api_key_sha256: "a".repeat(64),
    }
}

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    Ok(Instrument {
        symbol: "DOGE/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDT")?),
        generation: 7,
        price_tick: Price::new(Decimal::new(1, 5))?,
        quantity_step: Decimal::new(10, 0),
        minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ZERO),
    })
}

fn exposure(shadow: bool) -> ExposureTakeProfitConfig {
    ExposureTakeProfitConfig {
        enabled: true,
        shadow,
        position_equity_multiple: Decimal::new(3, 0),
        unrealized_pnl_equity_ratio: Decimal::new(5, 2),
        reduce_ratio: Decimal::new(30, 2),
        snapshot_interval_ms: 120_000,
        max_snapshot_age_ms: 3_000,
        rearm_clear_generations: 2,
    }
}

#[test]
fn stopped_recovery_fences_and_rejects_a_submitted_place_absent_from_signed_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let mut commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
    let binding = binding()?;
    let place = OrderCommand {
        time_in_force: Default::default(),
        command_id: CommandId::new("hgo_e46_long_open_l5_cmd")?,
        client_order_id: CommandId::new("hgo_e46_long_open_l5")?,
        owner: OrderOwner {
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(55, 0),
        limit_price: Price::new(Decimal::new(9245, 5))?,
        reduce_only: false,
    };
    commands.prepare_place(place.clone())?;
    commands.transition(&place.command_id, CommandState::Submitted)?;
    let readback = GridVenueReadback {
        raw_private_payloads: Vec::new(),
        order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
            Vec::new(),
            vec!["[]".to_owned()],
        )?),
        balance: AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(100, 0),
            available_balance: Decimal::new(100, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        },
        hedge_position: true,
        positions: Vec::new(),
        orders: Vec::new(),
        fills: Vec::new(),
    };

    settle_interrupted_wal_from_signed_readback(
        &mut commands,
        &HedgedGridState::new_with_params(
            binding.clone(),
            HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?,
        )?,
        &binding,
        &readback,
    )?;

    assert!(matches!(
        commands
            .receipt(&place.command_id)
            .map(|receipt| &receipt.state),
        Some(CommandState::Rejected { reason })
            if reason == "absent_from_complete_signed_orders_and_fill_history"
    ));
    assert!(!commands.has_unresolved());
    Ok(())
}

#[test]
fn promoted_admission_changes_only_executable_and_handoff_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let capability = capability_binding();
    let predecessor = Stage7LiveAdmissionEvidence::new(
        capability,
        binding()?,
        stage7_canary_support::stage7_parameter_release()?,
        instrument()?,
        Decimal::new(10, 0),
        "b".repeat(64),
        10,
        100,
        11,
        12,
    )?;
    let successor = predecessor.promote_for_executable_handoff(
        "c".repeat(64),
        "d".repeat(64),
        "e".repeat(64),
    )?;
    assert_eq!(successor.schema_version, 2);
    assert_eq!(successor.parameter_release, predecessor.parameter_release);
    assert_eq!(successor.instrument_rules, predecessor.instrument_rules);
    assert_eq!(successor.valid_until_ms, predecessor.valid_until_ms);
    assert_eq!(
        successor.predecessor_admission_sha256.as_deref(),
        Some(predecessor.admission_sha256.as_str())
    );
    assert_ne!(successor.admission_sha256, predecessor.admission_sha256);
    Ok(())
}

#[test]
fn handoff_promotion_content_addresses_the_successor_risk_release()
-> Result<(), Box<dyn std::error::Error>> {
    let predecessor = Stage7LiveAdmissionEvidence::new_with_exposure(
        capability_binding(),
        binding()?,
        stage7_canary_support::stage7_parameter_release()?,
        instrument()?,
        Decimal::new(10, 0),
        Some(exposure(true)),
        "b".repeat(64),
        10,
        100,
        11,
        12,
    )?;
    let successor_risk = exposure_release_digest(Some(exposure(false)))?;
    let successor = predecessor.promote_for_executable_handoff_with_exposure(
        "c".repeat(64),
        "d".repeat(64),
        "e".repeat(64),
        successor_risk.clone(),
    )?;

    assert_ne!(
        successor.configuration_sha256,
        predecessor.configuration_sha256
    );
    assert_eq!(successor.exposure_take_profit_sha256, successor_risk);
    assert_ne!(successor.admission_sha256, predecessor.admission_sha256);
    Ok(())
}

#[test]
fn handoff_chain_admits_only_the_successor_executable() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let capability = capability_binding();
    let predecessor = Stage7LiveAdmissionEvidence::new(
        capability.clone(),
        binding()?,
        stage7_canary_support::stage7_parameter_release()?,
        instrument()?,
        Decimal::new(10, 0),
        "b".repeat(64),
        10,
        100,
        11,
        12,
    )?;
    let manifest = Stage7ExecutableHandoffManifest {
        schema_version: HANDOFF_SCHEMA_VERSION,
        authorization: HANDOFF_AUTHORIZATION.to_owned(),
        exchange: "gate".to_owned(),
        symbol: "DOGE/USDT".to_owned(),
        canonical_root_sha256: canonical_root_sha256(temporary.path())?,
        predecessor_canonical_root_sha256: None,
        predecessor_executable_sha256: predecessor.executable_sha256.clone(),
        successor_executable_sha256: "c".repeat(64),
        predecessor_admission_sha256: predecessor.admission_sha256.clone(),
        authorized_at_ms: 15,
        valid_until_ms: 90,
    };
    let manifest_sha256 = canonical_digest(&manifest)?;
    let mut receipt = Stage7ExecutableHandoffReceipt {
        schema_version: HANDOFF_SCHEMA_VERSION,
        manifest,
        manifest_sha256: manifest_sha256.clone(),
        predecessor_admission: predecessor.clone(),
        writer_scope_sha256: "d".repeat(64),
        writer_generation: 1,
        writer_revision: 2,
        writer_readback_generation: 3,
        control_sha256: "e".repeat(64),
        checkpoint_sha256: "f".repeat(64),
        command_journal_sha256: "1".repeat(64),
        writer_state_sha256: "2".repeat(64),
        private_snapshot_sha256: "3".repeat(64),
        private_generation: 4,
        observed_at_ms: 20,
        long_quantity: Decimal::ONE,
        short_quantity: Decimal::ONE,
        hedge_position: true,
        orders_empty: true,
        wal_resolved: true,
        local_transactions_empty: true,
        order_health_clear: true,
        successor_exposure_release_bound: false,
        successor_exposure_take_profit_sha256: None,
        successor_configuration_sha256: None,
        private_evidence_recovery_manifest_sha256: None,
        private_evidence_journal_sha256: None,
        private_evidence_journal_bytes: None,
        handoff_sha256: String::new(),
    };
    receipt.handoff_sha256 = receipt.expected_handoff_sha256()?;
    let mut equal_signed_watermark = receipt.clone();
    equal_signed_watermark.private_generation = equal_signed_watermark.writer_readback_generation;
    equal_signed_watermark.handoff_sha256 = equal_signed_watermark.expected_handoff_sha256()?;
    equal_signed_watermark.validate_static()?;
    let mut legacy_digest_receipt = receipt.clone();
    legacy_digest_receipt.handoff_sha256 =
        legacy_digest_receipt.expected_legacy_handoff_sha256()?;
    assert_ne!(
        legacy_digest_receipt.handoff_sha256,
        legacy_digest_receipt.expected_handoff_sha256()?
    );
    legacy_digest_receipt.validate_static()?;
    let mut single_long = receipt.clone();
    single_long.short_quantity = Decimal::ZERO;
    single_long.handoff_sha256 = single_long.expected_handoff_sha256()?;
    single_long.validate_static()?;
    let mut single_short = receipt.clone();
    single_short.long_quantity = Decimal::ZERO;
    single_short.handoff_sha256 = single_short.expected_handoff_sha256()?;
    single_short.validate_static()?;
    let mut flat = receipt.clone();
    flat.long_quantity = Decimal::ZERO;
    flat.short_quantity = Decimal::ZERO;
    flat.handoff_sha256 = flat.expected_handoff_sha256()?;
    assert!(flat.validate_static().is_err());
    persist_receipt(temporary.path(), &receipt)?;
    let successor = predecessor.promote_for_executable_handoff(
        "c".repeat(64),
        manifest_sha256.clone(),
        receipt.handoff_sha256.clone(),
    )?;
    ProjectionStore::new(temporary.path().join(STAGE7_LIVE_ADMISSION_FILE)).save(&successor)?;

    let (_, recovered_predecessor) = validated_admission_predecessor(
        &capability,
        &instrument()?,
        Decimal::new(10, 0),
        temporary.path(),
        30,
        &"c".repeat(64),
    )?;
    assert_eq!(recovered_predecessor, predecessor);

    let destination = temporary.path().join("destination");
    fs::create_dir_all(&destination)?;
    persist_receipt(&destination, &receipt)?;
    let second_manifest = Stage7ExecutableHandoffManifest {
        schema_version: RELOCATION_HANDOFF_SCHEMA_VERSION,
        authorization: RELOCATION_HANDOFF_AUTHORIZATION.to_owned(),
        exchange: "gate".to_owned(),
        symbol: "DOGE/USDT".to_owned(),
        canonical_root_sha256: canonical_root_sha256(&destination)?,
        predecessor_canonical_root_sha256: Some(canonical_root_sha256(temporary.path())?),
        predecessor_executable_sha256: successor.executable_sha256.clone(),
        successor_executable_sha256: "4".repeat(64),
        predecessor_admission_sha256: successor.admission_sha256.clone(),
        authorized_at_ms: 31,
        valid_until_ms: 90,
    };
    let second_manifest_sha256 = canonical_digest(&second_manifest)?;
    let mut second_receipt = Stage7ExecutableHandoffReceipt {
        schema_version: HANDOFF_SCHEMA_VERSION,
        manifest: second_manifest,
        manifest_sha256: second_manifest_sha256.clone(),
        predecessor_admission: successor.clone(),
        writer_scope_sha256: "5".repeat(64),
        writer_generation: 2,
        writer_revision: 3,
        writer_readback_generation: 4,
        control_sha256: "6".repeat(64),
        checkpoint_sha256: "7".repeat(64),
        command_journal_sha256: "8".repeat(64),
        writer_state_sha256: "9".repeat(64),
        private_snapshot_sha256: "a".repeat(64),
        private_generation: 5,
        observed_at_ms: 35,
        long_quantity: Decimal::ONE,
        short_quantity: Decimal::ONE,
        hedge_position: true,
        orders_empty: true,
        wal_resolved: true,
        local_transactions_empty: true,
        order_health_clear: true,
        successor_exposure_release_bound: false,
        successor_exposure_take_profit_sha256: None,
        successor_configuration_sha256: None,
        private_evidence_recovery_manifest_sha256: None,
        private_evidence_journal_sha256: None,
        private_evidence_journal_bytes: None,
        handoff_sha256: String::new(),
    };
    second_receipt.handoff_sha256 = second_receipt.expected_handoff_sha256()?;
    persist_receipt(&destination, &second_receipt)?;
    let second_successor = successor.promote_for_executable_handoff(
        "4".repeat(64),
        second_manifest_sha256,
        second_receipt.handoff_sha256,
    )?;
    ProjectionStore::new(destination.join(STAGE7_LIVE_ADMISSION_FILE)).save(&second_successor)?;

    let (_, recovered_predecessor) = validated_admission_predecessor(
        &capability,
        &instrument()?,
        Decimal::new(10, 0),
        &destination,
        40,
        &"4".repeat(64),
    )?;
    assert_eq!(recovered_predecessor, predecessor);
    assert!(
        validated_admission_predecessor(
            &capability,
            &instrument()?,
            Decimal::new(10, 0),
            &destination,
            30,
            &"b".repeat(64),
        )
        .is_err()
    );
    assert!(
        validated_admission_predecessor(
            &capability,
            &instrument()?,
            Decimal::new(10, 0),
            &destination,
            40,
            &"c".repeat(64),
        )
        .is_err()
    );
    fs::remove_file(receipt_path(&destination, &manifest_sha256))?;
    assert!(
        validated_admission_predecessor(
            &capability,
            &instrument()?,
            Decimal::new(10, 0),
            &destination,
            40,
            &"4".repeat(64),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn nine_maintenance_handoffs_still_reach_the_original_canary()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let capability = capability_binding();
    let canary = Stage7LiveAdmissionEvidence::new(
        capability.clone(),
        binding()?,
        stage7_canary_support::stage7_parameter_release()?,
        instrument()?,
        Decimal::new(10, 0),
        "b".repeat(64),
        10,
        100,
        11,
        12,
    )?;
    let mut active = canary.clone();

    for release in 1..=9 {
        let successor_executable = format!("{release:064x}");
        let manifest = Stage7ExecutableHandoffManifest {
            schema_version: HANDOFF_SCHEMA_VERSION,
            authorization: HANDOFF_AUTHORIZATION.to_owned(),
            exchange: "gate".to_owned(),
            symbol: "DOGE/USDT".to_owned(),
            canonical_root_sha256: canonical_root_sha256(temporary.path())?,
            predecessor_canonical_root_sha256: None,
            predecessor_executable_sha256: active.executable_sha256.clone(),
            successor_executable_sha256: successor_executable.clone(),
            predecessor_admission_sha256: active.admission_sha256.clone(),
            authorized_at_ms: 15,
            valid_until_ms: 90,
        };
        let manifest_sha256 = canonical_digest(&manifest)?;
        let mut receipt = Stage7ExecutableHandoffReceipt {
            schema_version: HANDOFF_SCHEMA_VERSION,
            manifest,
            manifest_sha256: manifest_sha256.clone(),
            predecessor_admission: active.clone(),
            writer_scope_sha256: "d".repeat(64),
            writer_generation: release,
            writer_revision: release,
            writer_readback_generation: release,
            control_sha256: "e".repeat(64),
            checkpoint_sha256: "f".repeat(64),
            command_journal_sha256: "1".repeat(64),
            writer_state_sha256: "2".repeat(64),
            private_snapshot_sha256: "3".repeat(64),
            private_generation: release + 1,
            observed_at_ms: 20,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
            hedge_position: true,
            orders_empty: true,
            wal_resolved: true,
            local_transactions_empty: true,
            order_health_clear: true,
            successor_exposure_release_bound: false,
            successor_exposure_take_profit_sha256: None,
            successor_configuration_sha256: None,
            private_evidence_recovery_manifest_sha256: None,
            private_evidence_journal_sha256: None,
            private_evidence_journal_bytes: None,
            handoff_sha256: String::new(),
        };
        receipt.handoff_sha256 = receipt.expected_handoff_sha256()?;
        persist_receipt(temporary.path(), &receipt)?;
        active = active.promote_for_executable_handoff(
            successor_executable,
            manifest_sha256,
            receipt.handoff_sha256,
        )?;
    }

    ProjectionStore::new(temporary.path().join(STAGE7_LIVE_ADMISSION_FILE)).save(&active)?;
    let (_, recovered_canary) = validated_admission_predecessor(
        &capability,
        &instrument()?,
        Decimal::new(10, 0),
        temporary.path(),
        30,
        &active.executable_sha256,
    )?;
    assert_eq!(recovered_canary, canary);
    Ok(())
}
