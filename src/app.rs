use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Result,
    cli::{CanaryPhaseArg, CanarySideArg, Cli, Cmd, ControlTargetArg},
    config::Config,
    controller::ControlTarget,
    error::Error,
    exchange::binance::{
        PrivateCredentials, PrivateError, PrivateRest, PrivateStreamSocket, PublicRest,
        parse_instrument,
    },
    exchange::bitget::{BitgetCredentials, BitgetPrivateRest, BitgetPublicRest},
    exchange::gate::{GateCredentials, GatePrivateRest, GatePublicRest},
    execution::{
        Capability, CapabilityBinding, CapabilityEvidenceError, CapabilityEvidenceStore,
        CapabilityProbe, sha256_hex,
    },
    log,
    market::RawMarketRecorder,
    runtime::{
        BinanceAutoLiveRequest, BinanceAutoShadowRequest, BinanceCanaryPhase, BinanceCanaryRequest,
        BinanceLegacyStage7BridgeRequest, BinanceLegacyStage7StopRequest, HedgedGridControlTarget,
        ScalpingControlRequest, ScalpingCoreOwnerRiskCommitRequest, ScalpingCoreQuoteCommitRequest,
        ScalpingLiveResidentRequest, ScalpingShadowResidentRequest, Stage7CanaryRequest,
        Stage7ExecutableHandoffRequest, Stage7ExternalAlgoCleanupRequest, Stage7FlattenRequest,
        Stage7GridRequest, Stage7PrivateEvidenceRecoveryRequest,
        Stage7PublicEvidenceRecoveryRequest, apply_scalping_control,
        commit_scalping_core_owner_risk_page, commit_scalping_core_quote_receipt,
        recover_stage7_private_evidence, recover_stage7_public_evidence, replay_scalping_shadow,
        replay_scalping_shadow_with_evidence_and_risk_revaluation,
        replay_scalping_shadow_with_journal,
        replay_scalping_shadow_with_journal_and_risk_revaluation,
        request_binance_legacy_stage7_stop, run_binance_auto_live, run_binance_auto_shadow,
        run_binance_legacy_stage7_bridge, run_binance_shared_grid_shadow,
        run_binance_stage7_canary, run_binance_stage7_canary_recovery,
        run_binance_stage7_executable_handoff, run_binance_stage7_external_algo_cleanup,
        run_binance_stage7_flatten, run_binance_stage7_grid, run_binance_stage7_grid_canary,
        run_bitget_stage7_canary, run_bitget_stage7_canary_recovery,
        run_bitget_stage7_executable_handoff, run_bitget_stage7_flatten, run_bitget_stage7_grid,
        run_bitget_stage7_grid_canary, run_gate_stage7_canary, run_gate_stage7_canary_recovery,
        run_gate_stage7_executable_handoff, run_gate_stage7_flatten, run_gate_stage7_grid,
        run_gate_stage7_grid_canary, run_scalping_live_resident, run_scalping_shadow_resident,
        scan_binance_usdt_perpetuals, set_stage7_grid_control,
    },
    storage::{ScalpingEvidenceError, ScalpingEvidenceJournal},
    strategy::scalping::{
        ExposureState, ProtectionState, RiskRevaluation, SafetyProjection, ScalpingParams,
        StrategyBinding,
    },
};

const SHORT_EVIDENCE_TTL_MS: u64 = 5 * 60 * 1000;

pub fn start(cli: Cli) -> Result<()> {
    let cfg = Config::load(&cli.config)?;
    log::init(cfg.log)?;

    match cli.cmd {
        Cmd::AutoShadow {
            artifacts_root,
            initial_fill_recovery_from_ms,
            max_turns,
        } => {
            let report = run_binance_auto_shadow(
                &cfg,
                BinanceAutoShadowRequest {
                    artifacts_root,
                    initial_fill_recovery_from_ms,
                    max_turns,
                },
            )?;
            println!(
                "ok auto_shadow scan_sequence={} symbols={} run_root={}",
                report.scan_sequence,
                report.symbols.join(","),
                report.run_root.display(),
            );
            Ok(())
        }
        Cmd::AutoLive {
            artifacts_root,
            initial_fill_recovery_from_ms,
            max_turns,
            max_live_symbols,
            confirm_mainnet_strategy_mutations,
        } => {
            let report = run_binance_auto_live(
                &cfg,
                BinanceAutoLiveRequest {
                    artifacts_root,
                    initial_fill_recovery_from_ms,
                    max_turns,
                    max_live_symbols,
                    confirm_mainnet_strategy_mutations,
                },
            )?;
            println!(
                "ok auto_live scan_sequence={} symbols={} run_root={}",
                report.scan_sequence,
                report.symbols.join(","),
                report.run_root.display(),
            );
            Ok(())
        }
        Cmd::ScanBinance { artifacts_root } => {
            let report = scan_binance_usdt_perpetuals(&artifacts_root)?;
            let symbols = report
                .record
                .selection
                .selected
                .iter()
                .map(|selected| selected.sample.symbol.to_string())
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "ok binance_scan sequence={} inputs={} selected={} symbols={} journal={}",
                report.record.scan_sequence,
                report.record.input_count,
                report.record.selection.selected.len(),
                symbols,
                report.journal_path.display(),
            );
            Ok(())
        }
        Cmd::Doctor {
            private: false,
            stream: false,
            record: None,
        } => public_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream: false,
            record: None,
        } => private_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream: true,
            record: None,
        } => private_stream_doctor(&cfg),
        Cmd::Doctor {
            private: true,
            stream,
            record: Some(path),
        } => record_capability_evidence(&cfg, &path, stream),
        Cmd::Doctor {
            private: false,
            stream: true,
            record: _,
        } => Err(Error::PrivateStreamRequiresPrivate),
        Cmd::Doctor {
            private: false,
            stream: false,
            record: Some(_),
        } => Err(Error::PrivateStreamRequiresPrivate),
        Cmd::Replay {
            market,
            risk,
            evidence,
        } => shadow_replay(&cfg, &market, risk.as_deref(), evidence.as_deref()),
        Cmd::ShadowResident {
            artifacts_root,
            binding,
            initial_fill_recovery_from_ms,
            max_turns,
        } => {
            let report = run_scalping_shadow_resident(
                &cfg,
                ScalpingShadowResidentRequest {
                    artifacts_root,
                    binding_path: binding,
                    initial_fill_recovery_from_ms,
                    max_turns,
                },
            )?;
            println!(
                "ok shadow_resident turns={} worker_state={:?} disposition={:?} private_safe={} public_generation={} public_session={:?} public_feature={:?} deadline_pending={} public_in_flight={} pending_mark={} pending_preparation={} checkpoint={}",
                report.turns,
                report.worker_state,
                report.disposition,
                report.private_safe,
                report.public_generation,
                report.public_session_state,
                report.public_feature_state,
                report.deadline_pending,
                report.public_in_flight,
                report.pending_mark,
                report.pending_preparation,
                report.checkpoint_path.display(),
            );
            Ok(())
        }
        Cmd::LiveResident {
            artifacts_root,
            binding,
            initial_fill_recovery_from_ms,
            max_turns,
            confirm_mainnet_strategy_mutations,
        } => {
            let report = run_scalping_live_resident(
                &cfg,
                ScalpingLiveResidentRequest {
                    artifacts_root,
                    binding_path: binding,
                    initial_fill_recovery_from_ms,
                    max_turns,
                    confirm_mainnet_strategy_mutations,
                },
            )?;
            println!(
                "ok live_resident turns={} worker_state={:?} disposition={:?} private_safe={} public_generation={} public_session={:?} public_feature={:?} deadline_pending={} public_in_flight={} pending_mark={} pending_preparation={} checkpoint={}",
                report.turns,
                report.worker_state,
                report.disposition,
                report.private_safe,
                report.public_generation,
                report.public_session_state,
                report.public_feature_state,
                report.deadline_pending,
                report.public_in_flight,
                report.pending_mark,
                report.pending_preparation,
                report.checkpoint_path.display(),
            );
            Ok(())
        }
        Cmd::GridShadow {
            artifacts_root,
            max_turns,
        } => {
            let request = Stage7GridRequest {
                artifacts_root,
                max_turns,
                reset_on_start: false,
                skip_inventory_replenishment_until_recovered: false,
                confirm_mainnet_grid_mutations: false,
                shadow_only: true,
                stop_after_first_owned_fill: false,
                wall_clock_deadline_ms: None,
                force_order_health_check: false,
            };
            let report = match (
                cfg.binance.is_some(),
                cfg.gate.is_some(),
                cfg.bitget.is_some(),
            ) {
                (true, false, false) => run_binance_shared_grid_shadow(&cfg, request)?,
                (false, true, false) => run_gate_stage7_grid(&cfg, request)?,
                (false, false, true) => run_bitget_stage7_grid(&cfg, request)?,
                _ => return Err(Error::Disabled { cmd: "grid-shadow" }),
            };
            println!(
                "ok hedged_grid_shadow exchange={} turns={} phase={:?} private_generation={} private_stream_connected={} checkpoint={}",
                report.exchange,
                report.turns,
                report.phase,
                report.private_generation,
                report.private_stream_connected,
                report.checkpoint_path.display(),
            );
            Ok(())
        }
        Cmd::GridCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let request = Stage7CanaryRequest {
                artifacts_root,
                confirm_mainnet_grid_mutations,
            };
            let report = match (
                cfg.binance.is_some(),
                cfg.gate.is_some(),
                cfg.bitget.is_some(),
            ) {
                (true, false, false) => run_binance_stage7_canary(&cfg, request)?,
                (false, true, false) => run_gate_stage7_canary(&cfg, request)?,
                (false, false, true) => run_bitget_stage7_canary(&cfg, request)?,
                _ => return Err(Error::Disabled { cmd: "grid-canary" }),
            };
            println!(
                "ok hedged_grid_canary exchange={} symbol={} private_generation={} capability_valid_until_ms={}",
                report.exchange,
                report.symbol,
                report.private_generation,
                report.capability_valid_until_ms,
            );
            Ok(())
        }
        Cmd::GridLifecycleCanary {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let request = Stage7CanaryRequest {
                artifacts_root,
                confirm_mainnet_grid_mutations,
            };
            let report = match (
                cfg.binance.is_some(),
                cfg.gate.is_some(),
                cfg.bitget.is_some(),
            ) {
                (true, false, false) => run_binance_stage7_grid_canary(&cfg, request)?,
                (false, true, false) => run_gate_stage7_grid_canary(&cfg, request)?,
                (false, false, true) => run_bitget_stage7_grid_canary(&cfg, request)?,
                _ => {
                    return Err(Error::Disabled {
                        cmd: "grid-lifecycle-canary",
                    });
                }
            };
            println!(
                "ok hedged_grid_lifecycle_canary exchange={} symbol={} private_generation={} capability_valid_until_ms={}",
                report.exchange,
                report.symbol,
                report.private_generation,
                report.capability_valid_until_ms,
            );
            Ok(())
        }
        Cmd::GridCanaryRecover {
            artifacts_root,
            confirm_mainnet_grid_mutations,
        } => {
            let request = Stage7CanaryRequest {
                artifacts_root,
                confirm_mainnet_grid_mutations,
            };
            let report = match (
                cfg.binance.is_some(),
                cfg.gate.is_some(),
                cfg.bitget.is_some(),
            ) {
                (true, false, false) => run_binance_stage7_canary_recovery(&cfg, request)?,
                (false, true, false) => run_gate_stage7_canary_recovery(&cfg, request)?,
                (false, false, true) => run_bitget_stage7_canary_recovery(&cfg, request)?,
                _ => {
                    return Err(Error::Disabled {
                        cmd: "grid-canary-recover",
                    });
                }
            };
            println!(
                "ok hedged_grid_canary_recover exchange={} symbol={} private_generation={}",
                report.exchange, report.symbol, report.private_generation,
            );
            Ok(())
        }
        Cmd::GridExecutableHandoff {
            artifacts_root,
            release_manifest,
            confirm_mainnet_nonflat_executable_handoff,
            confirm_mainnet_stopped_order_recovery,
            archive_resolved_command_wal,
        } => {
            let request = Stage7ExecutableHandoffRequest {
                artifacts_root,
                release_manifest,
                confirm_mainnet_nonflat_executable_handoff,
                confirm_mainnet_stopped_order_recovery,
                archive_resolved_command_wal,
            };
            let report = match (
                cfg.binance.is_some(),
                cfg.gate.is_some(),
                cfg.bitget.is_some(),
            ) {
                (true, false, false) => run_binance_stage7_executable_handoff(&cfg, request)?,
                (false, true, false) => run_gate_stage7_executable_handoff(&cfg, request)?,
                (false, false, true) => run_bitget_stage7_executable_handoff(&cfg, request)?,
                _ => {
                    return Err(Error::Disabled {
                        cmd: "grid-executable-handoff",
                    });
                }
            };
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
            Ok(())
        }
        Cmd::GridExternalAlgoCancel {
            artifacts_root,
            expected_client_algo_id,
            expected_algo_id,
            confirm_mainnet_external_algo_cancel,
        } => {
            if cfg.binance.is_none() || cfg.gate.is_some() || cfg.bitget.is_some() {
                return Err(Error::Disabled {
                    cmd: "grid-external-algo-cancel",
                });
            }
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
            let request = Stage7FlattenRequest {
                artifacts_root,
                confirm_mainnet_grid_mutations,
            };
            let report = match (
                cfg.binance.is_some(),
                cfg.gate.is_some(),
                cfg.bitget.is_some(),
            ) {
                (true, false, false) => run_binance_stage7_flatten(&cfg, request)?,
                (false, true, false) => run_gate_stage7_flatten(&cfg, request)?,
                (false, false, true) => run_bitget_stage7_flatten(&cfg, request)?,
                _ => {
                    return Err(Error::Disabled {
                        cmd: "grid-flatten",
                    });
                }
            };
            println!(
                "ok hedged_grid_flatten exchange={} symbol={} private_generation={} writer_generation={} recovered_after_retirement={}",
                report.exchange,
                report.symbol,
                report.private_generation,
                report.writer_generation,
                report.recovered_after_retirement,
            );
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
            let request = Stage7GridRequest {
                artifacts_root,
                max_turns,
                reset_on_start,
                skip_inventory_replenishment_until_recovered,
                confirm_mainnet_grid_mutations,
                shadow_only: false,
                stop_after_first_owned_fill: false,
                wall_clock_deadline_ms: None,
                force_order_health_check: true,
            };
            let report = match (
                cfg.binance.is_some(),
                cfg.gate.is_some(),
                cfg.bitget.is_some(),
            ) {
                (true, false, false) => run_binance_stage7_grid(&cfg, request)?,
                (false, true, false) => run_gate_stage7_grid(&cfg, request)?,
                (false, false, true) => run_bitget_stage7_grid(&cfg, request)?,
                _ => return Err(Error::ExchangeConfiguration),
            };
            println!(
                "ok hedged_grid exchange={} turns={} phase={:?} private_generation={} stopped={} private_stream_connected={} checkpoint={}",
                report.exchange,
                report.turns,
                report.phase,
                report.private_generation,
                report.stopped,
                report.private_stream_connected,
                report.checkpoint_path.display(),
            );
            Ok(())
        }
        Cmd::GridStop { artifacts_root } => {
            set_stage7_grid_control(&cfg, &artifacts_root, HedgedGridControlTarget::Stop)?;
            println!("ok hedged_grid_stop root={}", artifacts_root.display());
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
            println!("ok hedged_grid_restart root={}", artifacts_root.display());
            Ok(())
        }
        Cmd::GridLegacyBinanceStop {
            artifacts_root,
            confirm_mainnet_legacy_stop,
        } => {
            if cfg.binance.is_none() || cfg.gate.is_some() || cfg.bitget.is_some() {
                return Err(Error::ExchangeConfiguration);
            }
            request_binance_legacy_stage7_stop(BinanceLegacyStage7StopRequest {
                artifacts_root: artifacts_root.clone(),
                confirm_mainnet_legacy_stop,
            })?;
            println!(
                "ok hedged_grid_legacy_binance_stop root={}",
                artifacts_root.display()
            );
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
            if cfg.binance.is_none() || cfg.gate.is_some() || cfg.bitget.is_some() {
                return Err(Error::ExchangeConfiguration);
            }
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
        Cmd::Control {
            artifacts_root,
            binding,
            target,
            command_id,
            idempotency_key,
            entry_expires_at_ms,
            confirm_entry_authority,
        } => {
            let report = apply_scalping_control(
                &cfg,
                ScalpingControlRequest {
                    artifacts_root,
                    binding_path: binding,
                    target: match target {
                        ControlTargetArg::Running => ControlTarget::Running,
                        ControlTargetArg::StopAndProtect => ControlTarget::StopAndProtect,
                        ControlTargetArg::FlattenAndStop => ControlTarget::FlattenAndStop,
                        ControlTargetArg::EmergencyStop => ControlTarget::EmergencyStop,
                    },
                    command_id,
                    idempotency_key,
                    entry_expires_at_ms,
                    confirm_entry_authority,
                },
            )?;
            println!(
                "ok control target={:?} revision={} changed={} expires_at_ms={}",
                report.target,
                report.revision,
                report.changed,
                report
                    .entry_expires_at_ms
                    .map_or_else(|| "none".to_owned(), |value| value.to_string()),
            );
            Ok(())
        }
        Cmd::CoreOwnerRiskCommit {
            artifacts_root,
            binding,
            page,
        } => {
            let report = commit_scalping_core_owner_risk_page(
                &cfg,
                ScalpingCoreOwnerRiskCommitRequest {
                    artifacts_root,
                    binding_path: binding,
                    page_path: page,
                },
            )?;
            println!(
                "ok core_owner_risk_commit sequence={} cursor_id={} inbox={}",
                report.sequence,
                report.cursor_id,
                report.inbox_path.display(),
            );
            Ok(())
        }
        Cmd::CoreQuoteCommit {
            artifacts_root,
            binding,
            receipt,
        } => {
            let report = commit_scalping_core_quote_receipt(
                &cfg,
                ScalpingCoreQuoteCommitRequest {
                    artifacts_root,
                    binding_path: binding,
                    receipt_path: receipt,
                },
            )?;
            println!(
                "ok core_quote_commit sequence={} quote_id={} receipts={}",
                report.sequence,
                report.quote_id,
                report.receipt_path.display(),
            );
            Ok(())
        }
        Cmd::Run => Err(Error::Disabled { cmd: "run" }),
        Cmd::Canary {
            phase,
            side,
            artifacts_root,
            confirm_mainnet_real_orders,
        } => {
            if !confirm_mainnet_real_orders {
                return Err(Error::CanaryConfirmation);
            }
            let report = crate::runtime::run_binance_canary(
                &cfg,
                BinanceCanaryRequest {
                    phase: match phase {
                        CanaryPhaseArg::PlaceCancel => BinanceCanaryPhase::PlaceCancel,
                        CanaryPhaseArg::Protection => BinanceCanaryPhase::Protection,
                    },
                    position_side: match side {
                        CanarySideArg::Long => crate::domain::PositionSide::Long,
                        CanarySideArg::Short => crate::domain::PositionSide::Short,
                    },
                    artifacts_root,
                },
            )?;
            println!(
                "ok canary phase={:?} symbol={} quantity={} notional={} {} terminal_flat={} evidence={}",
                report.phase,
                report.symbol,
                report.quantity,
                report.entry_notional.value,
                report.entry_notional.asset,
                report.terminal_flat,
                report.evidence_path.display(),
            );
            Ok(())
        }
        Cmd::CanaryRecover {
            artifacts_root,
            confirm_mainnet_private_readback,
            confirm_mainnet_recovery_mutations,
        } => {
            if !confirm_mainnet_private_readback {
                return Err(Error::CanaryRecoveryConfirmation);
            }
            let report = crate::runtime::run_binance_canary_recovery(
                &cfg,
                &artifacts_root,
                confirm_mainnet_recovery_mutations,
            )?;
            println!(
                "ok canary-recover symbol={} sealed_flat={} exact_cancel_required={} emergency_flatten_required={} remained_fenced={} mutation_attempts={}",
                report.symbol,
                report.sealed_flat.len(),
                report.exact_cancel_required.len(),
                report.emergency_flatten_required.len(),
                report.remained_fenced.len(),
                report.mutation_attempts,
            );
            Ok(())
        }
    }
}

/// Replays only durable public records. No private projection is manufactured here, so a command
/// line replay is stopped/fail-closed unless a richer runtime later supplies authoritative facts.
fn shadow_replay(
    cfg: &Config,
    market: &Path,
    risk: Option<&Path>,
    evidence: Option<&Path>,
) -> Result<()> {
    let records = RawMarketRecorder::open(market)?.recover()?.records;
    let asset = "USDT".parse().map_err(|_| Error::ShadowBinding)?;
    let binding = StrategyBinding {
        strategy_kind: crate::strategy::scalping::StrategyKind::Scalping,
        strategy_instance_id: "scalping_shadow_replay".to_owned(),
        run_id: "journal_replay".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: cfg.symbol.clone(),
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "scalping_shadow_replay:journal_replay".to_owned(),
        risk_budget: crate::domain::Amount::new(asset, rust_decimal::Decimal::new(5, 0)),
    };
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let safety = SafetyProjection {
        private_snapshot_ready: false,
        exposure: ExposureState::Unknown,
        execution_unknown: true,
        protection: ProtectionState::Unknown,
        owner_conflict: false,
        risk_budget_available: false,
    };
    let native_symbol = crate::exchange::binance::native_symbol(&cfg.symbol);
    let result = match (risk, evidence) {
        (Some(risk_path), Some(evidence_path)) => {
            let risk_revaluation = load_risk_revaluation(risk_path)?;
            let journal = ScalpingEvidenceJournal::open(evidence_path)?;
            replay_scalping_shadow_with_journal_and_risk_revaluation(
                &records,
                &native_symbol,
                binding,
                params,
                safety,
                &journal,
                &risk_revaluation,
            )?
        }
        (Some(risk_path), None) => {
            let risk_revaluation = load_risk_revaluation(risk_path)?;
            replay_scalping_shadow_with_evidence_and_risk_revaluation(
                &records,
                &native_symbol,
                binding,
                params,
                safety,
                &[],
                &risk_revaluation,
            )?
        }
        (None, Some(path)) => {
            let journal = ScalpingEvidenceJournal::open(path)?;
            replay_scalping_shadow_with_journal(
                &records,
                &native_symbol,
                binding,
                params,
                safety,
                &journal,
            )?
        }
        (None, None) => replay_scalping_shadow(&records, &native_symbol, binding, params, safety)?,
    };
    println!(
        "ok shadow_replay symbol={} records={} preparations={} intents={} safety=fail_closed",
        cfg.symbol,
        result.processed_records,
        result.preparations.len(),
        result.intents.len(),
    );
    Ok(())
}

/// Reads one complete, versioned risk proof. `RiskRevaluation` rejects unknown fields so a
/// replay cannot silently downgrade a richer risk input into an empty local ledger.
fn load_risk_revaluation(path: &Path) -> Result<RiskRevaluation> {
    let bytes = fs::read(path).map_err(|source| {
        Error::ScalpingEvidence(ScalpingEvidenceError::Io {
            path: path.to_path_buf(),
            source,
        })
    })?;
    serde_json::from_slice(&bytes)
        .map_err(|source| Error::ScalpingEvidence(ScalpingEvidenceError::Decode(source)))
}

/// Performs signed reads only. It emits capability and collection counts but never credentials or
/// account values, and it does not create a listen key or issue a mutation.
fn public_doctor(cfg: &Config) -> Result<()> {
    if cfg.gate.is_some() {
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
        return Ok(());
    }
    if cfg.bitget.is_some() {
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
        return Ok(());
    }
    println!("ok symbol={}", cfg.symbol);
    Ok(())
}

fn private_doctor(cfg: &Config) -> Result<()> {
    if cfg.gate.is_some() {
        return gate_private_doctor(cfg);
    }
    if cfg.bitget.is_some() {
        return bitget_private_doctor(cfg);
    }
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

/// Verifies that a PAPI private WebSocket can be opened. Only the probe's local socket is dropped;
/// the account-scoped listen key may be serving other symbol workers and is never remotely closed.
fn private_stream_doctor(cfg: &Config) -> Result<()> {
    if cfg.gate.is_some() {
        return gate_private_stream_doctor(cfg);
    }
    if cfg.bitget.is_some() {
        return bitget_private_stream_doctor(cfg);
    }
    let credentials = PrivateCredentials::from_environment()?;
    let client = PrivateRest::production(credentials, cfg.binance_config()?.account_binding)?;
    verify_private_stream(&client)?;
    println!(
        "ok private_stream symbol={} connected=true local_disconnected=true account_stream_closed=false",
        cfg.symbol
    );
    Ok(())
}

fn gate_private_doctor(cfg: &Config) -> Result<()> {
    let public = GatePublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let private = GatePrivateRest::production(GateCredentials::from_environment()?)?;
    let readback = private.readback(&cfg.symbol, &rules)?;
    let risk = private.risk_readback(&cfg.symbol, &rules, "usdt_futures_dual", 1)?;
    if !readback.dual_position_mode {
        return Err(crate::exchange::gate::GateError::PositionMode.into());
    }
    let flat = readback
        .positions
        .iter()
        .all(|position| position.quantity.is_zero());
    println!(
        "ok private exchange=gate symbol={} hedge={} flat={} positions={} orders={} fills={}",
        cfg.symbol,
        readback.dual_position_mode,
        flat,
        readback.positions.len(),
        readback.orders.len(),
        readback.fills.len(),
    );
    for leg in &risk.legs {
        println!(
            "ok risk exchange=gate symbol={} position_side={:?} account_equity={} position_notional={} position_pnl={} risk_currency={}",
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

fn bitget_private_doctor(cfg: &Config) -> Result<()> {
    let public = BitgetPublicRest::production()?;
    let rules = public.contract_rules(&cfg.symbol, 1)?;
    let private = BitgetPrivateRest::production(BitgetCredentials::from_environment()?)?;
    let readback = private.readback(&cfg.symbol, &rules, None)?;
    let risk = private.risk_readback(&cfg.symbol, "uta_usdt_futures_hedge", 1)?;
    if !readback.hedge_position {
        return Err(crate::exchange::bitget::BitgetError::PositionMode.into());
    }
    let flat = readback
        .positions
        .iter()
        .all(|position| position.quantity.is_zero());
    println!(
        "ok private exchange=bitget symbol={} hedge={} flat={} positions={} orders={} fills={}",
        cfg.symbol,
        readback.hedge_position,
        flat,
        readback.positions.len(),
        readback.orders.len(),
        readback.fills.len(),
    );
    for leg in &risk.legs {
        println!(
            "ok risk exchange=bitget symbol={} position_side={:?} account_equity={} position_notional={} position_pnl={} risk_currency={}",
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

fn gate_private_stream_doctor(cfg: &Config) -> Result<()> {
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

fn bitget_private_stream_doctor(cfg: &Config) -> Result<()> {
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

/// Runs only already-supported read-only probes, then appends their non-secret fingerprints as
/// one durable batch. No mutation capability is ever asserted by this doctor command.
fn record_capability_evidence(cfg: &Config, path: &Path, stream: bool) -> Result<()> {
    if cfg.binance.is_none() {
        return Err(Error::Disabled {
            cmd: "doctor --record",
        });
    }
    let api_key = crate::credential_env::required("BINANCE_API_KEY")
        .map_err(|_| Error::Private(PrivateError::Credentials))?;
    let binding = CapabilityBinding {
        exchange: "binance".to_owned(),
        account_binding: "portfolio_margin_um".to_owned(),
        symbol: cfg.symbol.to_string(),
        api_key_sha256: sha256_hex(api_key.as_bytes()),
    };
    let now_ms = wall_clock_ms()?;
    let valid_until_ms = now_ms
        .checked_add(SHORT_EVIDENCE_TTL_MS)
        .ok_or(Error::CapabilityEvidence(CapabilityEvidenceError::Invalid))?;

    let public = PublicRest::production()?;
    let exchange_info = public.exchange_info()?;
    parse_instrument(&exchange_info, cfg.symbol.clone(), 1)?;
    let depth = public.depth_snapshot(&cfg.symbol, 5)?;

    let credentials = PrivateCredentials::from_environment()?;
    let client = PrivateRest::production(credentials, cfg.binance_config()?.account_binding)?;
    let readback = client.readback(&cfg.symbol)?;
    if !readback.capabilities.can_trade || !readback.capabilities.hedge_position {
        return Err(Error::CapabilityEvidence(CapabilityEvidenceError::Invalid));
    }

    let mut probes = vec![
        CapabilityProbe::new(
            Capability::InstrumentRules,
            "binance_fapi_exchange_info_instrument_v1",
            sha256_hex(exchange_info.as_bytes()),
            valid_until_ms,
        )?,
        CapabilityProbe::new(
            Capability::PublicMarket,
            "binance_fapi_depth_snapshot_v1",
            sha256_hex(depth.as_bytes()),
            valid_until_ms,
        )?,
        CapabilityProbe::new(
            Capability::PrivateReadback,
            "binance_papi_um_signed_readback_v1",
            sha256_hex(format!(
                "can_trade={};hedge={};balances={};positions={};orders={};fills={}",
                readback.capabilities.can_trade,
                readback.capabilities.hedge_position,
                readback.balances.len(),
                readback.positions.len(),
                readback.orders.len(),
                readback.fills.len(),
            )),
            valid_until_ms,
        )?,
    ];
    if stream {
        verify_private_stream(&client)?;
        probes.push(CapabilityProbe::new(
            Capability::PrivateStream,
            "binance_papi_pm_websocket_connect_v1",
            sha256_hex("papi_pm_stream_connect_close_success_v1"),
            valid_until_ms,
        )?);
    }

    let mut store = CapabilityEvidenceStore::open(path)?;
    store.append_successes(&binding, now_ms, &probes)?;
    println!(
        "ok capability_evidence symbol={} recorded={} stream={}",
        cfg.symbol,
        probes.len(),
        stream,
    );
    Ok(())
}

fn verify_private_stream(client: &PrivateRest) -> Result<()> {
    let listen_key = client.create_user_stream()?;
    let socket = PrivateStreamSocket::connect(&listen_key)?;
    drop(socket);
    // PAPI has one account-scoped active key. A connectivity probe owns only this local socket;
    // remote DELETE would invalidate every resident strategy using the same account.
    Ok(())
}

fn wall_clock_ms() -> Result<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::CapabilityEvidenceClock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| Error::CapabilityEvidenceClock)
}
