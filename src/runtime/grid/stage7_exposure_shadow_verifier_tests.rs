use std::{collections::BTreeMap, fs};

use rust_decimal::Decimal;

use super::{
    stage7_canary_support::{STAGE7_LIVE_ADMISSION_FILE, Stage7LiveAdmissionEvidence},
    stage7_exposure_shadow_verifier::{
        ExposureShadowVerificationError, ExposureShadowVerifiedDecision,
        ExposureShadowVerifiedReason, verify_stage7_exposure_shadow_evidence,
    },
    *,
};
use crate::{
    config::ExposureTakeProfitConfig,
    domain::{
        AccountRiskSnapshot, Amount, Asset, Instrument, LegRiskSnapshot, MarketKind, PositionSide,
        Price, RiskSourceStatus,
    },
    execution::{CapabilityBinding, sha256_hex},
    runtime::hedged_grid::{
        EXPOSURE_SHADOW_EVIDENCE_FILE, ExposureShadowEvidenceJournal, RawRiskEvidenceRef,
        build_shadow_evidence,
    },
    storage::{PrivateEvidence, PrivateEvidenceJournal, ProjectionStore},
    strategy::hedged_grid::{
        ExposureGuardDecision, ExposureGuardParams, ExposureGuardState, GridPosition,
        HedgedGridBinding, HedgedGridParams, HedgedGridState, ReduceProfitableExposure,
    },
};

#[test]
fn verifier_reports_latest_lanes_release_and_exact_private_references()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = shadow_fixture()?;
    let before = root_files(fixture.path())?;
    let report = verify_stage7_exposure_shadow_evidence(fixture.path())?;
    assert_eq!(root_files(fixture.path())?, before);

    assert_eq!(report.release_id, "stage7_low_balance_hedged_grid_v1");
    assert_eq!(report.binding, binding()?);
    assert_eq!(
        report.long.decision,
        ExposureShadowVerifiedDecision::WouldReduce
    );
    assert_eq!(
        report.long.reason,
        ExposureShadowVerifiedReason::ThresholdBreached
    );
    assert_eq!(
        report.long.exposure_notional_threshold,
        Decimal::new(399, 0)
    );
    assert_eq!(report.long.unrealized_pnl_threshold, Decimal::new(665, 2));
    assert_eq!(
        report.short.decision,
        ExposureShadowVerifiedDecision::NoMutation
    );
    assert_eq!(report.short.reason, ExposureShadowVerifiedReason::FlatLeg);
    assert_eq!(report.long.raw_evidence, report.short.raw_evidence);
    assert_eq!(report.long.raw_evidence[0].sequence, 1);
    Ok(())
}

fn root_files(
    root: &std::path::Path,
) -> Result<BTreeMap<String, Vec<u8>>, Box<dyn std::error::Error>> {
    let mut files = BTreeMap::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            files.insert(
                entry.file_name().to_string_lossy().into_owned(),
                fs::read(entry.path())?,
            );
        }
    }
    Ok(files)
}

#[test]
fn verifier_rejects_cross_journal_hash_tamper_and_missing_private_record()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = shadow_fixture()?;
    let shadow_path = fixture.path().join(EXPOSURE_SHADOW_EVIDENCE_FILE);
    let encoded = fs::read_to_string(&shadow_path)?;
    let mut lines = encoded.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut first: serde_json::Value = serde_json::from_str(&lines[0])?;
    first["raw_evidence"][0]["payload_sha256"] = serde_json::Value::String("c".repeat(64));
    lines[0] = serde_json::to_string(&first)?;
    fs::write(&shadow_path, format!("{}\n", lines.join("\n")))?;
    assert!(matches!(
        verify_stage7_exposure_shadow_evidence(fixture.path()),
        Err(ExposureShadowVerificationError::PrivateReference)
    ));

    let fixture = shadow_fixture()?;
    fs::write(fixture.path().join(PRIVATE_EVIDENCE_FILE), b"")?;
    assert!(matches!(
        verify_stage7_exposure_shadow_evidence(fixture.path()),
        Err(ExposureShadowVerificationError::PrivateReference)
    ));
    Ok(())
}

#[test]
fn verifier_rejects_normalized_field_tamper_and_unbound_exposure_release()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = shadow_fixture_with_equity(Decimal::new(13_301, 2))?;
    assert!(matches!(
        verify_stage7_exposure_shadow_evidence(fixture.path()),
        Err(ExposureShadowVerificationError::SemanticReplay)
    ));

    let fixture = shadow_fixture()?;
    let admission_path = fixture.path().join(STAGE7_LIVE_ADMISSION_FILE);
    let mut admission: serde_json::Value = serde_json::from_slice(&fs::read(&admission_path)?)?;
    admission["exposure_release_bound"] = serde_json::Value::Bool(false);
    admission
        .as_object_mut()
        .ok_or("admission must be an object")?
        .remove("exposure_take_profit_sha256");
    fs::write(&admission_path, serde_json::to_vec(&admission)?)?;
    assert!(matches!(
        verify_stage7_exposure_shadow_evidence(fixture.path()),
        Err(ExposureShadowVerificationError::Release)
    ));
    Ok(())
}

fn shadow_fixture() -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    shadow_fixture_with_equity(Decimal::new(133, 0))
}

fn shadow_fixture_with_equity(
    account_equity: Decimal,
) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let binding = binding()?;
    let grid_params = HedgedGridParams::fixed_release(Asset::new("USDC")?, 10)?;
    persist_admission(root, &binding, &grid_params)?;
    let guard_params = ExposureGuardParams::fixed_release();
    let checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state: HedgedGridState::new_with_params(binding.clone(), grid_params)?,
        private_generation: 1,
        exposure_guard: Some(ExposureGuardState::new(
            binding.clone(),
            guard_params.clone(),
        )?),
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    ProjectionStore::new(root.join(CHECKPOINT_FILE)).save(&checkpoint)?;

    let mut private = PrivateEvidenceJournal::open(root.join(PRIVATE_EVIDENCE_FILE))?;
    let payloads = [
        r#"{"accountEquity":"133"}"#,
        r#"[{"symbol":"SOLUSDC","positionAmt":"4","positionSide":"LONG","markPrice":"100","notional":"400","unRealizedProfit":"7"}]"#,
        r#"{"dualSidePosition":true}"#,
        r#"{"canTrade":true}"#,
        r#"[{"asset":"USDC","assetIndexPrice":"0.999","time":1000}]"#,
    ];
    let raw = payloads
        .into_iter()
        .map(|payload| {
            let sequence = private.append(PrivateEvidence::new(1, 1_050, payload.to_owned())?)?;
            Ok(RawRiskEvidenceRef {
                sequence,
                generation: 1,
                payload_sha256: sha256_hex(payload.as_bytes()),
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let account = account(&binding, account_equity)?;
    let long = long_leg(&binding)?;
    let mut shadow = ExposureShadowEvidenceJournal::open(root.join(EXPOSURE_SHADOW_EVIDENCE_FILE))?;
    shadow.append_if_changed(build_shadow_evidence(
        &binding,
        &guard_params,
        &account,
        GridPosition::Long,
        Some(&long),
        &ExposureGuardDecision::ReduceProfitableExposure(ReduceProfitableExposure {
            risk_episode_id: "etp-l-0000000000000001".to_owned(),
            position: GridPosition::Long,
            trigger_generation: 1,
            position_quantity: long.quantity,
            position_notional: long.notional,
            account_equity: account.account_equity,
            unrealized_pnl: long.unrealized_pnl,
            reduce_ratio: guard_params.reduce_ratio,
            risk_currency: account.risk_currency.clone(),
        }),
        1_050,
        raw.clone(),
    )?)?;
    shadow.append_if_changed(build_shadow_evidence(
        &binding,
        &guard_params,
        &account,
        GridPosition::Short,
        None,
        &ExposureGuardDecision::Noop,
        1_050,
        raw,
    )?)?;
    Ok(temporary)
}

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

fn account(
    binding: &HedgedGridBinding,
    account_equity: Decimal,
) -> Result<AccountRiskSnapshot, Box<dyn std::error::Error>> {
    Ok(AccountRiskSnapshot {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        risk_currency: Asset::new("USD")?,
        account_equity,
        private_generation: 1,
        observed_at_ms: 1_050,
        source_status: RiskSourceStatus::Complete,
    })
}

fn long_leg(binding: &HedgedGridBinding) -> Result<LegRiskSnapshot, Box<dyn std::error::Error>> {
    Ok(LegRiskSnapshot {
        symbol: binding.symbol.clone(),
        position_side: PositionSide::Long,
        quantity: Decimal::new(4, 0),
        mark_price: Price::new(Decimal::new(100, 0))?,
        contract_multiplier: Decimal::new(999, 3),
        notional: Decimal::new(3996, 1),
        unrealized_pnl: Decimal::new(6993, 3),
        risk_currency: Asset::new("USD")?,
        private_generation: 1,
        observed_at_ms: 1_050,
    })
}

fn persist_admission(
    root: &std::path::Path,
    binding: &HedgedGridBinding,
    params: &HedgedGridParams,
) -> Result<(), Box<dyn std::error::Error>> {
    let guard = ExposureGuardParams::fixed_release();
    let admission = Stage7LiveAdmissionEvidence::new_with_exposure(
        CapabilityBinding {
            exchange: "binance".to_owned(),
            account_binding: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDC".to_owned(),
            api_key_sha256: "a".repeat(64),
        },
        binding.clone(),
        params.clone(),
        Instrument {
            symbol: binding.symbol.clone(),
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(Asset::new("USDC")?),
            generation: 9,
            price_tick: Price::new(Decimal::new(1, 3))?,
            quantity_step: Decimal::new(1, 2),
            minimum_notional: Amount::new(Asset::new("USDC")?, Decimal::ZERO),
        },
        Decimal::new(1, 2),
        Some(ExposureTakeProfitConfig {
            enabled: guard.enabled,
            shadow: true,
            position_equity_multiple: guard.position_equity_multiple,
            unrealized_pnl_equity_ratio: guard.unrealized_pnl_equity_ratio,
            reduce_ratio: guard.reduce_ratio,
            snapshot_interval_ms: 120_000,
            max_snapshot_age_ms: guard.max_snapshot_age_ms,
            rearm_clear_generations: guard.rearm_clear_generations,
        }),
        "b".repeat(64),
        10,
        100,
        1,
        1,
    )?;
    ProjectionStore::new(root.join(STAGE7_LIVE_ADMISSION_FILE)).save(&admission)?;
    Ok(())
}
