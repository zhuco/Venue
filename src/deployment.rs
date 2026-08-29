#[cfg(any(
    test,
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
use crate::cli::Cmd;

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
use crate::{Error, Result, cli::Cli, config::Config};

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
use crate::runtime::{
    HedgedGridControlTarget, Stage7CanaryRequest, Stage7ExecutableHandoffRequest,
    Stage7FlattenRequest, Stage7GridRequest, set_stage7_grid_control,
};
#[cfg(feature = "hedged-grid-binance")]
use crate::{
    exchange::binance::{PrivateCredentials, PrivateRest, PrivateStreamSocket},
    runtime::{
        BinanceLegacyStage7BridgeRequest, BinanceLegacyStage7StopRequest,
        Stage7ExternalAlgoCleanupRequest, Stage7PrivateEvidenceRecoveryRequest,
        Stage7PublicEvidenceRecoveryRequest, recover_stage7_private_evidence,
        recover_stage7_public_evidence, request_binance_legacy_stage7_stop,
        run_binance_legacy_stage7_bridge, run_binance_stage7_canary,
        run_binance_stage7_canary_recovery, run_binance_stage7_executable_handoff,
        run_binance_stage7_external_algo_cleanup, run_binance_stage7_flatten,
        run_binance_stage7_grid, run_binance_stage7_grid_canary,
    },
};
#[cfg(feature = "hedged-grid-bitget")]
use crate::{
    exchange::bitget::{BitgetCredentials, BitgetPrivateRest, BitgetPublicRest},
    runtime::{
        run_bitget_stage7_canary, run_bitget_stage7_canary_recovery,
        run_bitget_stage7_executable_handoff, run_bitget_stage7_flatten, run_bitget_stage7_grid,
        run_bitget_stage7_grid_canary,
    },
};
#[cfg(feature = "hedged-grid-gate")]
use crate::{
    exchange::gate::{GateCredentials, GatePrivateRest, GatePublicRest},
    runtime::{
        run_gate_stage7_canary, run_gate_stage7_canary_recovery,
        run_gate_stage7_executable_handoff, run_gate_stage7_flatten, run_gate_stage7_grid,
        run_gate_stage7_grid_canary,
    },
};

/// Fixed release composition: this entry references only the Binance adapter and Binance grid
/// runtime. Cargo requires the matching feature before the binary target can be built.
#[cfg(feature = "hedged-grid-binance")]
pub fn start_hedged_grid_binance_deployment(cli: Cli) -> Result<()> {
    let (cfg, command) = prepare(cli, "binance")?;
    match command {
        Cmd::Doctor {
            private: false,
            stream: false,
            record: None,
        } => {
            println!("ok symbol={}", cfg.symbol);
            Ok(())
        }
        Cmd::Doctor {
            private: true,
            stream: false,
            record: None,
        } => binance_private_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream: true,
            record: None,
        } => binance_stream_doctor(&cfg),
        Cmd::GridShadow {
            artifacts_root,
            max_turns,
        } => {
            let report = run_binance_stage7_grid(
                &cfg,
                Stage7GridRequest {
                    artifacts_root,
                    max_turns,
                    reset_on_start: false,
                    skip_inventory_replenishment_until_recovered: false,
                    confirm_mainnet_grid_mutations: false,
                    shadow_only: true,
                    stop_after_first_owned_fill: false,
                    wall_clock_deadline_ms: None,
                    force_order_health_check: false,
                },
            )?;
            print_binance_shadow_report(&report);
            Ok(())
        }
        Cmd::GridStart {
            artifacts_root,
            max_turns,
            reset_on_start,
            skip_inventory_replenishment_until_recovered,
            confirm_mainnet_grid_mutations,
        } => {
            if reset_on_start {
                set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Reset)?;
            }
            let report = run_binance_stage7_grid(
                &cfg,
                Stage7GridRequest {
                    artifacts_root,
                    max_turns,
                    reset_on_start,
                    skip_inventory_replenishment_until_recovered,
                    confirm_mainnet_grid_mutations,
                    shadow_only: false,
                    stop_after_first_owned_fill: false,
                    wall_clock_deadline_ms: None,
                    force_order_health_check: true,
                },
            )?;
            print_stage7_grid_report("hedged_grid", &report);
            Ok(())
        }
        Cmd::GridCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_binance_stage7_canary(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_canary(&report);
            Ok(())
        }
        Cmd::GridLifecycleCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_binance_stage7_grid_canary(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_lifecycle_canary(&report);
            Ok(())
        }
        Cmd::GridCanaryRecover {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_binance_stage7_canary_recovery(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_canary_recovery(&report);
            Ok(())
        }
        Cmd::GridExecutableHandoff {
            artifacts_root,
            release_manifest,
            confirm_mainnet_nonflat_executable_handoff,
            confirm_mainnet_stopped_order_recovery,
            archive_resolved_command_wal,
        } => {
            let report = run_binance_stage7_executable_handoff(
                &cfg,
                Stage7ExecutableHandoffRequest {
                    artifacts_root,
                    release_manifest,
                    confirm_mainnet_nonflat_executable_handoff,
                    confirm_mainnet_stopped_order_recovery,
                    archive_resolved_command_wal,
                },
            )?;
            print_handoff(&report);
            Ok(())
        }
        Cmd::GridExternalAlgoCancel {
            artifacts_root,
            expected_client_algo_id,
            expected_algo_id,
            confirm_mainnet_external_algo_cancel,
        } => {
            let report = run_binance_stage7_external_algo_cleanup(
                &cfg,
                Stage7ExternalAlgoCleanupRequest {
                    artifacts_root,
                    expected_client_algo_id,
                    expected_algo_id,
                    confirm_mainnet_external_algo_cancel,
                },
            )?;
            println!(
                "ok hedged_grid_external_algo_cancel exchange={} symbol={} client_algo_id={} algo_id={} regular_orders_preserved={} private_generation={} already_absent={} journal={}",
                report.exchange,
                report.symbol,
                report.client_algo_id,
                report.algo_id,
                report.regular_orders_preserved,
                report.private_generation,
                report.already_absent,
                report.cleanup_journal_path.display(),
            );
            Ok(())
        }
        Cmd::GridFlatten {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_binance_stage7_flatten(
                &cfg,
                Stage7FlattenRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_flatten(&report);
            Ok(())
        }
        Cmd::GridStop { artifacts_root } => {
            set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Stop)?;
            print_control("stop", &artifacts_root);
            Ok(())
        }
        Cmd::GridPrivateEvidenceRecover {
            artifacts_root,
            expected_source_sha256,
            expected_canonical_selection_sha256,
            expected_quarantine_selection_sha256,
            expected_coverage_sha256,
            expected_canonical_tail_sequence,
            expected_collision_count,
            confirm_private_evidence_forensic_recovery,
        } => {
            let report = recover_stage7_private_evidence(
                &cfg,
                Stage7PrivateEvidenceRecoveryRequest {
                    artifacts_root,
                    expected_source_sha256,
                    expected_canonical_selection_sha256,
                    expected_quarantine_selection_sha256,
                    expected_coverage_sha256,
                    expected_canonical_tail_sequence,
                    expected_collision_count,
                    confirm_private_evidence_forensic_recovery,
                },
            )?;
            println!(
                "ok stage7_private_evidence_recovery exchange={} symbol={} source_sha256={} source_records={} canonical_records={} collision_records={} canonical_selection_sha256={} quarantine_selection_sha256={} coverage_sha256={} recovered_journal_sha256={} manifest_sha256={}",
                report.exchange,
                report.symbol,
                report.source_sha256,
                report.source_records,
                report.canonical_records,
                report.collision_records,
                report.canonical_selection_sha256,
                report.quarantine_selection_sha256,
                report.coverage_sha256,
                report.recovered_journal_sha256,
                report.manifest_sha256,
            );
            Ok(())
        }
        Cmd::GridPublicEvidenceRecover {
            artifacts_root,
            expected_source_sha256,
            expected_canonical_selection_sha256,
            expected_quarantine_selection_sha256,
            expected_coverage_sha256,
            expected_canonical_tail_sequence,
            expected_collision_count,
            confirm_public_evidence_forensic_recovery,
        } => {
            let report = recover_stage7_public_evidence(
                &cfg,
                Stage7PublicEvidenceRecoveryRequest {
                    artifacts_root,
                    expected_source_sha256,
                    expected_canonical_selection_sha256,
                    expected_quarantine_selection_sha256,
                    expected_coverage_sha256,
                    expected_canonical_tail_sequence,
                    expected_collision_count,
                    confirm_public_evidence_forensic_recovery,
                },
            )?;
            println!(
                "ok stage7_public_evidence_recovery exchange={} symbol={} source_sha256={} source_records={} canonical_records={} collision_records={} canonical_selection_sha256={} quarantine_selection_sha256={} coverage_sha256={} recovered_journal_sha256={} manifest_sha256={}",
                report.exchange,
                report.symbol,
                report.source_sha256,
                report.source_records,
                report.canonical_records,
                report.collision_records,
                report.canonical_selection_sha256,
                report.quarantine_selection_sha256,
                report.coverage_sha256,
                report.recovered_journal_sha256,
                report.manifest_sha256,
            );
            Ok(())
        }
        Cmd::GridRestart { artifacts_root } => {
            set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Reset)?;
            print_control("restart", &artifacts_root);
            Ok(())
        }
        Cmd::GridLegacyBinanceStop {
            artifacts_root,
            confirm_mainnet_legacy_stop,
        } => {
            request_binance_legacy_stage7_stop(BinanceLegacyStage7StopRequest {
                artifacts_root: artifacts_root.clone(),
                confirm_mainnet_legacy_stop,
            })?;
            print_control("legacy_binance_stop", &artifacts_root);
            Ok(())
        }
        Cmd::GridLegacyBinanceBridge {
            artifacts_root,
            legacy_config_path,
            legacy_executable_path,
            successor_executable_path,
            expected_legacy_executable_sha256,
            expected_successor_executable_sha256,
            confirm_mainnet_nonflat_legacy_bridge,
        } => {
            let report = run_binance_legacy_stage7_bridge(
                &cfg,
                BinanceLegacyStage7BridgeRequest {
                    artifacts_root,
                    legacy_config_path,
                    legacy_executable_path,
                    successor_executable_path,
                    expected_legacy_executable_sha256,
                    expected_successor_executable_sha256,
                    confirm_mainnet_nonflat_legacy_bridge,
                },
            )?;
            println!(
                "ok hedged_grid_legacy_binance_bridge symbol={} private_generation={} writer_generation={} long_quantity={} short_quantity={} attestation_sha256={} receipt_sha256={} idempotent_replay={}",
                report.symbol,
                report.private_generation,
                report.writer_generation,
                report.long_quantity,
                report.short_quantity,
                report.attestation_sha256,
                report.receipt_sha256,
                report.idempotent_replay,
            );
            Ok(())
        }
        _ => Err(Error::HedgedGridDeploymentCommand),
    }
}

/// Fixed release composition: this entry references only the Gate adapter and shared grid runtime.
#[cfg(feature = "hedged-grid-gate")]
pub fn start_hedged_grid_gate_deployment(cli: Cli) -> Result<()> {
    let (cfg, command) = prepare(cli, "gate")?;
    start_stage7_gate(cfg, command)
}

/// Fixed release composition: this entry references only the Bitget adapter and shared grid
/// runtime.
#[cfg(feature = "hedged-grid-bitget")]
pub fn start_hedged_grid_bitget_deployment(cli: Cli) -> Result<()> {
    let (cfg, command) = prepare(cli, "bitget")?;
    start_stage7_bitget(cfg, command)
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn prepare(cli: Cli, expected: &'static str) -> Result<(Config, Cmd)> {
    let config = Config::load(&cli.config)?;
    let actual = configured_exchange(&config);
    if actual != expected {
        return Err(Error::HedgedGridDeploymentExchange { expected, actual });
    }
    if !is_grid_command(&cli.cmd)
        || (matches!(
            &cli.cmd,
            Cmd::GridPrivateEvidenceRecover { .. }
                | Cmd::GridPublicEvidenceRecover { .. }
                | Cmd::GridExternalAlgoCancel { .. }
                | Cmd::GridLegacyBinanceStop { .. }
                | Cmd::GridLegacyBinanceBridge { .. }
        ) && expected != "binance")
    {
        return Err(Error::HedgedGridDeploymentCommand);
    }
    crate::log::init(config.log)?;
    Ok((config, cli.cmd))
}

#[cfg(feature = "hedged-grid-gate")]
fn start_stage7_gate(cfg: Config, command: Cmd) -> Result<()> {
    match command {
        Cmd::Doctor {
            private: false,
            stream: false,
            record: None,
        } => gate_public_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream: false,
            record: None,
        } => gate_private_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream: true,
            record: None,
        } => gate_stream_doctor(&cfg),
        Cmd::GridShadow {
            artifacts_root,
            max_turns,
        } => run_gate_grid(
            &cfg,
            Stage7GridRequest {
                artifacts_root,
                max_turns,
                reset_on_start: false,
                skip_inventory_replenishment_until_recovered: false,
                confirm_mainnet_grid_mutations: false,
                shadow_only: true,
                stop_after_first_owned_fill: false,
                wall_clock_deadline_ms: None,
                force_order_health_check: false,
            },
        ),
        Cmd::GridStart {
            artifacts_root,
            max_turns,
            reset_on_start,
            skip_inventory_replenishment_until_recovered,
            confirm_mainnet_grid_mutations,
        } => {
            if reset_on_start {
                set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Reset)?;
            }
            run_gate_grid(
                &cfg,
                Stage7GridRequest {
                    artifacts_root,
                    max_turns,
                    reset_on_start,
                    skip_inventory_replenishment_until_recovered,
                    confirm_mainnet_grid_mutations,
                    shadow_only: false,
                    stop_after_first_owned_fill: false,
                    wall_clock_deadline_ms: None,
                    force_order_health_check: true,
                },
            )
        }
        Cmd::GridCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_gate_stage7_canary(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_canary(&report);
            Ok(())
        }
        Cmd::GridLifecycleCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_gate_stage7_grid_canary(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_lifecycle_canary(&report);
            Ok(())
        }
        Cmd::GridCanaryRecover {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_gate_stage7_canary_recovery(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_canary_recovery(&report);
            Ok(())
        }
        Cmd::GridExecutableHandoff {
            artifacts_root,
            release_manifest,
            confirm_mainnet_nonflat_executable_handoff,
            confirm_mainnet_stopped_order_recovery,
            archive_resolved_command_wal,
        } => {
            let report = run_gate_stage7_executable_handoff(
                &cfg,
                Stage7ExecutableHandoffRequest {
                    artifacts_root,
                    release_manifest,
                    confirm_mainnet_nonflat_executable_handoff,
                    confirm_mainnet_stopped_order_recovery,
                    archive_resolved_command_wal,
                },
            )?;
            print_handoff(&report);
            Ok(())
        }
        Cmd::GridFlatten {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_gate_stage7_flatten(
                &cfg,
                Stage7FlattenRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_flatten(&report);
            Ok(())
        }
        Cmd::GridStop { artifacts_root } => {
            set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Stop)?;
            print_control("stop", &artifacts_root);
            Ok(())
        }
        Cmd::GridRestart { artifacts_root } => {
            set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Reset)?;
            print_control("restart", &artifacts_root);
            Ok(())
        }
        _ => Err(Error::HedgedGridDeploymentCommand),
    }
}

#[cfg(feature = "hedged-grid-bitget")]
fn start_stage7_bitget(cfg: Config, command: Cmd) -> Result<()> {
    match command {
        Cmd::Doctor {
            private: false,
            stream: false,
            record: None,
        } => bitget_public_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream: false,
            record: None,
        } => bitget_private_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream: true,
            record: None,
        } => bitget_stream_doctor(&cfg),
        Cmd::GridShadow {
            artifacts_root,
            max_turns,
        } => run_bitget_grid(
            &cfg,
            Stage7GridRequest {
                artifacts_root,
                max_turns,
                reset_on_start: false,
                skip_inventory_replenishment_until_recovered: false,
                confirm_mainnet_grid_mutations: false,
                shadow_only: true,
                stop_after_first_owned_fill: false,
                wall_clock_deadline_ms: None,
                force_order_health_check: false,
            },
        ),
        Cmd::GridStart {
            artifacts_root,
            max_turns,
            reset_on_start,
            skip_inventory_replenishment_until_recovered,
            confirm_mainnet_grid_mutations,
        } => {
            if reset_on_start {
                set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Reset)?;
            }
            run_bitget_grid(
                &cfg,
                Stage7GridRequest {
                    artifacts_root,
                    max_turns,
                    reset_on_start,
                    skip_inventory_replenishment_until_recovered,
                    confirm_mainnet_grid_mutations,
                    shadow_only: false,
                    stop_after_first_owned_fill: false,
                    wall_clock_deadline_ms: None,
                    force_order_health_check: true,
                },
            )
        }
        Cmd::GridCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_bitget_stage7_canary(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_canary(&report);
            Ok(())
        }
        Cmd::GridLifecycleCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_bitget_stage7_grid_canary(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_lifecycle_canary(&report);
            Ok(())
        }
        Cmd::GridCanaryRecover {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_bitget_stage7_canary_recovery(
                &cfg,
                Stage7CanaryRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_canary_recovery(&report);
            Ok(())
        }
        Cmd::GridExecutableHandoff {
            artifacts_root,
            release_manifest,
            confirm_mainnet_nonflat_executable_handoff,
            confirm_mainnet_stopped_order_recovery,
            archive_resolved_command_wal,
        } => {
            let report = run_bitget_stage7_executable_handoff(
                &cfg,
                Stage7ExecutableHandoffRequest {
                    artifacts_root,
                    release_manifest,
                    confirm_mainnet_nonflat_executable_handoff,
                    confirm_mainnet_stopped_order_recovery,
                    archive_resolved_command_wal,
                },
            )?;
            print_handoff(&report);
            Ok(())
        }
        Cmd::GridFlatten {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let report = run_bitget_stage7_flatten(
                &cfg,
                Stage7FlattenRequest {
                    artifacts_root,
                    confirm_mainnet_grid_mutations,
                },
            )?;
            print_flatten(&report);
            Ok(())
        }
        Cmd::GridStop { artifacts_root } => {
            set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Stop)?;
            print_control("stop", &artifacts_root);
            Ok(())
        }
        Cmd::GridRestart { artifacts_root } => {
            set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Reset)?;
            print_control("restart", &artifacts_root);
            Ok(())
        }
        _ => Err(Error::HedgedGridDeploymentCommand),
    }
}

#[cfg(feature = "hedged-grid-gate")]
fn run_gate_grid(cfg: &Config, request: Stage7GridRequest) -> Result<()> {
    let report = run_gate_stage7_grid(cfg, request)?;
    print_stage7_grid_report("hedged_grid", &report);
    Ok(())
}

#[cfg(feature = "hedged-grid-bitget")]
fn run_bitget_grid(cfg: &Config, request: Stage7GridRequest) -> Result<()> {
    let report = run_bitget_stage7_grid(cfg, request)?;
    print_stage7_grid_report("hedged_grid", &report);
    Ok(())
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn configured_exchange(config: &Config) -> &'static str {
    if config.binance.is_some() {
        "binance"
    } else if config.gate.is_some() {
        "gate"
    } else {
        "bitget"
    }
}

#[cfg(any(
    test,
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn is_grid_command(command: &Cmd) -> bool {
    matches!(
        command,
        Cmd::Doctor { .. }
            | Cmd::GridStart { .. }
            | Cmd::GridShadow { .. }
            | Cmd::GridCanary { .. }
            | Cmd::GridLifecycleCanary { .. }
            | Cmd::GridCanaryRecover { .. }
            | Cmd::GridExecutableHandoff { .. }
            | Cmd::GridExternalAlgoCancel { .. }
            | Cmd::GridFlatten { .. }
            | Cmd::GridStop { .. }
            | Cmd::GridPrivateEvidenceRecover { .. }
            | Cmd::GridPublicEvidenceRecover { .. }
            | Cmd::GridRestart { .. }
            | Cmd::GridLegacyBinanceStop { .. }
            | Cmd::GridLegacyBinanceBridge { .. }
    )
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn print_stage7_grid_report(label: &str, report: &crate::runtime::Stage7GridReport) {
    println!(
        "ok {} exchange={} turns={} phase={:?} private_generation={} stopped={} private_stream_connected={} checkpoint={}",
        label,
        report.exchange,
        report.turns,
        report.phase,
        report.private_generation,
        report.stopped,
        report.private_stream_connected,
        report.checkpoint_path.display(),
    );
}

#[cfg(feature = "hedged-grid-binance")]
fn print_binance_shadow_report(report: &crate::runtime::Stage7GridReport) {
    println!(
        "ok hedged_grid_shadow exchange={} turns={} phase={:?} private_generation={} private_stream_connected={} checkpoint={}",
        report.exchange,
        report.turns,
        report.phase,
        report.private_generation,
        report.private_stream_connected,
        report.checkpoint_path.display(),
    );
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn print_canary(report: &crate::runtime::Stage7CanaryReport) {
    println!(
        "ok hedged_grid_canary exchange={} symbol={} private_generation={} capability_valid_until_ms={}",
        report.exchange, report.symbol, report.private_generation, report.capability_valid_until_ms,
    );
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn print_lifecycle_canary(report: &crate::runtime::Stage7GridCanaryReport) {
    println!(
        "ok hedged_grid_lifecycle_canary exchange={} symbol={} private_generation={} capability_valid_until_ms={}",
        report.exchange, report.symbol, report.private_generation, report.capability_valid_until_ms,
    );
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn print_canary_recovery(report: &crate::runtime::Stage7CanaryRecoveryReport) {
    println!(
        "ok hedged_grid_canary_recover exchange={} symbol={} private_generation={}",
        report.exchange, report.symbol, report.private_generation,
    );
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn print_handoff(report: &crate::runtime::Stage7ExecutableHandoffReport) {
    println!(
        "ok hedged_grid_executable_handoff exchange={} symbol={} predecessor_executable_sha256={} successor_executable_sha256={} private_generation={} writer_generation={} positions_preserved={} handoff_sha256={}",
        report.exchange,
        report.symbol,
        report.predecessor_executable_sha256,
        report.successor_executable_sha256,
        report.private_generation,
        report.writer_generation,
        report.positions_preserved,
        report.handoff_sha256,
    );
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn print_flatten(report: &crate::runtime::Stage7FlattenReport) {
    println!(
        "ok hedged_grid_flatten exchange={} symbol={} private_generation={} writer_generation={} recovered_after_retirement={}",
        report.exchange,
        report.symbol,
        report.private_generation,
        report.writer_generation,
        report.recovered_after_retirement,
    );
}

#[cfg(any(
    feature = "hedged-grid-binance",
    feature = "hedged-grid-gate",
    feature = "hedged-grid-bitget"
))]
fn print_control(action: &str, root: &std::path::Path) {
    println!("ok hedged_grid_{} root={}", action, root.display());
}

#[cfg(feature = "hedged-grid-binance")]
fn binance_private_doctor(cfg: &Config) -> Result<()> {
    let credentials = PrivateCredentials::from_environment()?;
    let client = PrivateRest::production(credentials, cfg.binance_config()?.account_binding)?;
    let readback = client.readback(&cfg.symbol)?;
    let risk = client.risk_readback(&cfg.symbol, 1, client.authoritative_now_ms()?, 3_000)?;
    let algos = crate::exchange::binance_private::parse_open_algo_orders(
        &client.open_algo_orders(&cfg.symbol)?,
        &cfg.symbol,
    )
    .map_err(crate::exchange::binance::PrivateReadbackError::Parse)?;
    let flat = readback
        .positions
        .iter()
        .all(|position| position.quantity.is_zero());
    println!(
        "ok private symbol={} can_trade={} one_way={} hedge={} flat={} balances={} positions={} orders={} algos={} fills={}",
        cfg.symbol,
        readback.capabilities.can_trade,
        readback.capabilities.one_way_position,
        readback.capabilities.hedge_position,
        flat,
        readback.balances.len(),
        readback.positions.len(),
        readback.orders.len(),
        algos.len(),
        readback.fills.len(),
    );
    for leg in &risk.legs {
        println!(
            "ok risk exchange=binance symbol={} position_side={:?} account_equity={} position_notional={} position_pnl={} risk_currency={}",
            leg.symbol,
            leg.position_side,
            risk.account.account_equity,
            leg.notional,
            leg.unrealized_pnl,
            leg.risk_currency,
        );
    }
    Ok(())
}

#[cfg(feature = "hedged-grid-binance")]
fn binance_stream_doctor(cfg: &Config) -> Result<()> {
    let credentials = PrivateCredentials::from_environment()?;
    let client = PrivateRest::production(credentials, cfg.binance_config()?.account_binding)?;
    let listen_key = client.create_user_stream()?;
    let socket = PrivateStreamSocket::connect(&listen_key)?;
    drop(socket);
    println!(
        "ok private_stream symbol={} connected=true local_disconnected=true account_stream_closed=false",
        cfg.symbol
    );
    Ok(())
}

#[cfg(feature = "hedged-grid-gate")]
fn gate_public_doctor(cfg: &Config) -> Result<()> {
    let public = GatePublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let (bid, ask) = public.best_bid_ask(&cfg.symbol)?;
    println!(
        "ok public exchange=gate symbol={} tick={} quantity_step={} bid={} ask={}",
        cfg.symbol,
        rules.instrument.price_tick.value(),
        rules.instrument.quantity_step,
        bid.value(),
        ask.value(),
    );
    Ok(())
}

#[cfg(feature = "hedged-grid-gate")]
fn gate_private_doctor(cfg: &Config) -> Result<()> {
    let public = GatePublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let private = GatePrivateRest::production(GateCredentials::from_environment()?)?;
    let readback = private.readback(&cfg.symbol, &rules)?;
    let risk = private.risk_readback(&cfg.symbol, &rules, "usdt_futures_dual", 1)?;
    if !readback.dual_position_mode {
        return Err(crate::exchange::gate::GateError::PositionMode.into());
    }
    print_private_readback(
        "gate",
        &cfg.symbol,
        readback.dual_position_mode,
        &readback.positions,
        readback.orders.len(),
        readback.fills.len(),
        &risk.account,
        &risk.legs,
    );
    Ok(())
}

#[cfg(feature = "hedged-grid-gate")]
fn gate_stream_doctor(cfg: &Config) -> Result<()> {
    let public = GatePublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let private = GatePrivateRest::production(GateCredentials::from_environment()?)?;
    let readback = private.readback(&cfg.symbol, &rules)?;
    if !readback.dual_position_mode {
        return Err(crate::exchange::gate::GateError::PositionMode.into());
    }
    let stream = private.connect_private_stream(&readback.user_id, &cfg.symbol)?;
    drop(stream);
    println!(
        "ok private_stream exchange=gate symbol={} connected=true local_disconnected=true",
        cfg.symbol
    );
    Ok(())
}

#[cfg(feature = "hedged-grid-bitget")]
fn bitget_public_doctor(cfg: &Config) -> Result<()> {
    let public = BitgetPublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let (bid, ask) = public.best_bid_ask(&cfg.symbol)?;
    println!(
        "ok public exchange=bitget symbol={} tick={} quantity_step={} bid={} ask={}",
        cfg.symbol,
        rules.instrument.price_tick.value(),
        rules.instrument.quantity_step,
        bid.value(),
        ask.value(),
    );
    Ok(())
}

#[cfg(feature = "hedged-grid-bitget")]
fn bitget_private_doctor(cfg: &Config) -> Result<()> {
    let public = BitgetPublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let private = BitgetPrivateRest::production(BitgetCredentials::from_environment()?)?;
    let readback = private.readback(&cfg.symbol, &rules, None)?;
    let risk = private.risk_readback(&cfg.symbol, "uta_usdt_futures_hedge", 1)?;
    if !readback.hedge_position {
        return Err(crate::exchange::bitget::BitgetError::PositionMode.into());
    }
    print_private_readback(
        "bitget",
        &cfg.symbol,
        readback.hedge_position,
        &readback.positions,
        readback.orders.len(),
        readback.fills.len(),
        &risk.account,
        &risk.legs,
    );
    Ok(())
}

#[cfg(feature = "hedged-grid-bitget")]
fn bitget_stream_doctor(cfg: &Config) -> Result<()> {
    let public = BitgetPublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let private = BitgetPrivateRest::production(BitgetCredentials::from_environment()?)?;
    let readback = private.readback(&cfg.symbol, &rules, None)?;
    if !readback.hedge_position {
        return Err(crate::exchange::bitget::BitgetError::PositionMode.into());
    }
    let stream = private.connect_private_stream(&cfg.symbol)?;
    drop(stream);
    println!(
        "ok private_stream exchange=bitget symbol={} connected=true local_disconnected=true",
        cfg.symbol
    );
    Ok(())
}

#[cfg(any(feature = "hedged-grid-gate", feature = "hedged-grid-bitget"))]
#[expect(
    clippy::too_many_arguments,
    reason = "the operator readback line deliberately prints one complete, explicit evidence tuple"
)]
fn print_private_readback(
    exchange: &str,
    symbol: &crate::domain::Symbol,
    hedge: bool,
    positions: &[crate::domain::Position],
    orders: usize,
    fills: usize,
    account: &crate::domain::AccountRiskSnapshot,
    legs: &[crate::domain::LegRiskSnapshot],
) {
    let flat = positions.iter().all(|position| position.quantity.is_zero());
    println!(
        "ok private exchange={} symbol={} hedge={} flat={} positions={} orders={} fills={}",
        exchange,
        symbol,
        hedge,
        flat,
        positions.len(),
        orders,
        fills,
    );
    for leg in legs {
        println!(
            "ok risk exchange={} symbol={} position_side={:?} account_equity={} position_notional={} position_pnl={} risk_currency={}",
            exchange,
            leg.symbol,
            leg.position_side,
            account.account_equity,
            leg.notional,
            leg.unrealized_pnl,
            leg.risk_currency,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn fixed_grid_artifact_rejects_unrelated_strategy_commands() {
        assert!(is_grid_command(&Cmd::GridStop {
            artifacts_root: PathBuf::from("grid-root"),
        }));
        assert!(is_grid_command(&Cmd::GridPrivateEvidenceRecover {
            artifacts_root: PathBuf::from("grid-root"),
            expected_source_sha256: "a".repeat(64),
            expected_canonical_selection_sha256: "b".repeat(64),
            expected_quarantine_selection_sha256: "c".repeat(64),
            expected_coverage_sha256: "d".repeat(64),
            expected_canonical_tail_sequence: 1,
            expected_collision_count: 1,
            confirm_private_evidence_forensic_recovery: true,
        }));
        assert!(is_grid_command(&Cmd::GridPublicEvidenceRecover {
            artifacts_root: PathBuf::from("grid-root"),
            expected_source_sha256: "a".repeat(64),
            expected_canonical_selection_sha256: "b".repeat(64),
            expected_quarantine_selection_sha256: "c".repeat(64),
            expected_coverage_sha256: "d".repeat(64),
            expected_canonical_tail_sequence: 1,
            expected_collision_count: 1,
            confirm_public_evidence_forensic_recovery: true,
        }));
        assert!(is_grid_command(&Cmd::Doctor {
            private: true,
            stream: false,
            record: None,
        }));
        assert!(!is_grid_command(&Cmd::Run));
    }
}
