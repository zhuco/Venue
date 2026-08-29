use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "venue", version, about = "Personal CTA runtime")]
pub struct Cli {
    #[arg(short, long, default_value = "venue.toml", global = true)]
    pub config: PathBuf,

    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
pub enum Cmd {
    Run,
    /// Persists one complete, read-only Binance USDT-perpetual candidate scan.
    ScanBinance {
        #[arg(long)]
        artifacts_root: PathBuf,
    },
    /// Scans Binance, prepares up to three isolated bindings, and runs mutation-free residents.
    AutoShadow {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        initial_fill_recovery_from_ms: u64,
        #[arg(long)]
        max_turns: Option<u64>,
    },
    /// Scans Binance and runs up to three selected phase-8 mainnet Live residents.
    AutoLive {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        initial_fill_recovery_from_ms: u64,
        #[arg(long)]
        max_turns: Option<u64>,
        /// Starts only the highest-ranked N symbols; hard-capped at three.
        #[arg(long, default_value_t = 1)]
        max_live_symbols: usize,
        #[arg(long, required = true)]
        confirm_mainnet_strategy_mutations: bool,
    },
    /// Replays an already-recorded public market journal through the mutation-free Shadow path.
    Replay {
        #[arg(long)]
        market: PathBuf,
        #[arg(long)]
        risk: Option<PathBuf>,
        #[arg(long)]
        evidence: Option<PathBuf>,
    },
    /// Runs the private-facts and safety portions of a mutation-free resident Shadow instance.
    ShadowResident {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        binding: PathBuf,
        #[arg(long)]
        initial_fill_recovery_from_ms: u64,
        #[arg(long)]
        max_turns: Option<u64>,
    },
    /// Runs the explicitly confirmed mainnet scalping resident with durable private reconciliation.
    LiveResident {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        binding: PathBuf,
        #[arg(long)]
        initial_fill_recovery_from_ms: u64,
        #[arg(long)]
        max_turns: Option<u64>,
        #[arg(long, required = true)]
        confirm_mainnet_strategy_mutations: bool,
    },
    /// Starts the configured hedged-grid deployment using [hedged_grid].grid_count.
    GridStart {
        #[arg(long)]
        artifacts_root: PathBuf,
        /// Bounded turns are for process supervision; omit for the resident actor.
        #[arg(long)]
        max_turns: Option<u64>,
        /// Atomically reset the current owned grid before entering the resident loop.
        #[arg(long)]
        reset_on_start: bool,
        /// Rebuild from current Hedge inventory without market top-up until inventory recovers.
        #[arg(long)]
        skip_inventory_replenishment_until_recovered: bool,
        #[arg(long, required = true)]
        confirm_mainnet_grid_mutations: bool,
    },
    /// Runs a mutation-free configured stage-7 hedged-grid private-facts Shadow instance.
    GridShadow {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        max_turns: Option<u64>,
    },
    /// Runs the bounded configured-exchange place/cancel plus hedge/reduce mainnet canary.
    GridCanary {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_grid_mutations: bool,
    },
    /// Verifies a real 3×3 grid lifecycle, then cancels owned orders and reduce-only flattens.
    GridLifecycleCanary {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_grid_mutations: bool,
    },
    /// Resumes only the cancellation and flattening path for an interrupted stage-7 Canary.
    GridCanaryRecover {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_grid_mutations: bool,
    },
    /// Authorizes one content-addressed executable upgrade after Stop, preserving both Hedge legs.
    GridExecutableHandoff {
        #[arg(long)]
        artifacts_root: PathBuf,
        /// Absolute operator-authored old/new executable hash manifest.
        #[arg(long)]
        release_manifest: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_nonflat_executable_handoff: bool,
        /// Allows the successor to settle WAL and cancel only exact owned orders when the
        /// predecessor failed closed after a durable Stop target.
        #[arg(long)]
        confirm_mainnet_stopped_order_recovery: bool,
        /// Seals the fully resolved predecessor command WAL after signed zero-order proof and
        /// starts the successor with an empty active WAL. The sealed source remains immutable.
        #[arg(long)]
        archive_resolved_command_wal: bool,
    },
    /// Cancels one sole, signed external Binance Algo without claiming grid ownership of it.
    GridExternalAlgoCancel {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        expected_client_algo_id: String,
        #[arg(long)]
        expected_algo_id: String,
        #[arg(long, required = true)]
        confirm_mainnet_external_algo_cancel: bool,
    },
    /// Cancels this Live binding's owned orders, reduce-only flattens both Hedge legs, and retires writer.
    GridFlatten {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_grid_mutations: bool,
    },
    /// Stops one hedged-grid symbol by cancelling only its owned grid orders.
    GridStop {
        #[arg(long)]
        artifacts_root: PathBuf,
    },
    /// Performs one operator-anchored, offline quarantine of a proven Stage-7 evidence fork.
    GridPrivateEvidenceRecover {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        expected_source_sha256: String,
        #[arg(long)]
        expected_canonical_selection_sha256: String,
        #[arg(long)]
        expected_quarantine_selection_sha256: String,
        #[arg(long)]
        expected_coverage_sha256: String,
        #[arg(long)]
        expected_canonical_tail_sequence: u64,
        #[arg(long)]
        expected_collision_count: u64,
        #[arg(long, required = true)]
        confirm_private_evidence_forensic_recovery: bool,
    },
    /// Performs one operator-anchored, offline quarantine of a proven Stage-7 public fork.
    GridPublicEvidenceRecover {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        expected_source_sha256: String,
        #[arg(long)]
        expected_canonical_selection_sha256: String,
        #[arg(long)]
        expected_quarantine_selection_sha256: String,
        #[arg(long)]
        expected_coverage_sha256: String,
        #[arg(long)]
        expected_canonical_tail_sequence: u64,
        #[arg(long)]
        expected_collision_count: u64,
        #[arg(long, required = true)]
        confirm_public_evidence_forensic_recovery: bool,
    },
    /// Rebuilds one hedged-grid symbol from a fresh private inventory snapshot.
    GridRestart {
        #[arg(long)]
        artifacts_root: PathBuf,
    },
    /// Requests a graceful Stop from the frozen Binance phase-1 grid runtime.
    GridLegacyBinanceStop {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_legacy_stop: bool,
    },
    /// Finalizes a mutation-free Binance phase-1 to shared Stage-7 non-flat handoff.
    GridLegacyBinanceBridge {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        legacy_config_path: PathBuf,
        #[arg(long)]
        legacy_executable_path: PathBuf,
        #[arg(long)]
        successor_executable_path: PathBuf,
        #[arg(long)]
        expected_legacy_executable_sha256: String,
        #[arg(long)]
        expected_successor_executable_sha256: String,
        #[arg(long, required = true)]
        confirm_mainnet_nonflat_legacy_bridge: bool,
    },
    /// Persists one bounded scalping controller target; it has no exchange or mutation capability.
    Control {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        binding: PathBuf,
        #[arg(long, value_enum)]
        target: ControlTargetArg,
        #[arg(long)]
        command_id: String,
        #[arg(long)]
        idempotency_key: String,
        /// Required only for `running`; absolute Unix time in milliseconds.
        #[arg(long)]
        entry_expires_at_ms: Option<u64>,
        /// Required only for `running`; authorizes a bounded future strategy entry window.
        #[arg(long)]
        confirm_entry_authority: bool,
    },
    /// Fsyncs one already-valued Core owner-risk page; it has no venue or mutation capability.
    CoreOwnerRiskCommit {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        binding: PathBuf,
        #[arg(long)]
        page: PathBuf,
    },
    /// Fsyncs one complete externally-valued Core quote receipt; it never derives a quote.
    CoreQuoteCommit {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long)]
        binding: PathBuf,
        #[arg(long)]
        receipt: PathBuf,
    },
    Doctor {
        #[arg(long)]
        private: bool,
        #[arg(long, requires = "private")]
        stream: bool,
        #[arg(long, requires = "private")]
        record: Option<PathBuf>,
    },
    /// Runs one explicitly confirmed, minimum-notional Binance mainnet Canary phase.
    Canary {
        #[arg(long, value_enum)]
        phase: CanaryPhaseArg,
        #[arg(long, value_enum)]
        side: CanarySideArg,
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_real_orders: bool,
    },
    /// Uses signed private readback to recover unfinished Canary runs; it never submits an entry.
    CanaryRecover {
        #[arg(long)]
        artifacts_root: PathBuf,
        #[arg(long, required = true)]
        confirm_mainnet_private_readback: bool,
        /// Allows only recovery-planned exact cancellation or full Hedge-leg IOC reduction.
        #[arg(long, requires = "confirm_mainnet_private_readback")]
        confirm_mainnet_recovery_mutations: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum CanaryPhaseArg {
    PlaceCancel,
    Protection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum CanarySideArg {
    Long,
    Short,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ControlTargetArg {
    Running,
    StopAndProtect,
    FlattenAndStop,
    EmergencyStop,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_doctor() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["venue", "doctor"])?;

        assert_eq!(
            cli.cmd,
            Cmd::Doctor {
                private: false,
                stream: false,
                record: None,
            }
        );
        assert_eq!(cli.config, PathBuf::from("venue.toml"));
        Ok(())
    }

    #[test]
    fn parses_read_only_binance_scan() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "scan-binance",
            "--artifacts-root",
            "C:\\venue-scan",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::ScanBinance {
                artifacts_root: PathBuf::from("C:\\venue-scan")
            }
        );
        Ok(())
    }

    #[test]
    fn parses_explicitly_confirmed_auto_live() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "auto-live",
            "--artifacts-root",
            "C:\\venue-live",
            "--initial-fill-recovery-from-ms",
            "123",
            "--confirm-mainnet-strategy-mutations",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::AutoLive {
                artifacts_root: PathBuf::from("C:\\venue-live"),
                initial_fill_recovery_from_ms: 123,
                max_turns: None,
                max_live_symbols: 1,
                confirm_mainnet_strategy_mutations: true,
            }
        );
        Ok(())
    }

    #[test]
    fn parses_bounded_auto_shadow() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "auto-shadow",
            "--artifacts-root",
            "C:\\venue-auto",
            "--initial-fill-recovery-from-ms",
            "100",
            "--max-turns",
            "5",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::AutoShadow {
                artifacts_root: PathBuf::from("C:\\venue-auto"),
                initial_fill_recovery_from_ms: 100,
                max_turns: Some(5),
            }
        );
        Ok(())
    }

    #[test]
    fn private_stream_doctor_requires_private_mode() {
        assert!(Cli::try_parse_from(["venue", "doctor", "--stream"]).is_err());
    }

    #[test]
    fn parses_private_stream_doctor() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["venue", "doctor", "--private", "--stream"])?;

        assert_eq!(
            cli.cmd,
            Cmd::Doctor {
                private: true,
                stream: true,
                record: None,
            }
        );
        Ok(())
    }

    #[test]
    fn capability_record_requires_private_mode() {
        assert!(
            Cli::try_parse_from(["venue", "doctor", "--record", "capabilities.jsonl"]).is_err()
        );
    }

    #[test]
    fn replay_requires_an_explicit_market_journal() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["venue", "replay", "--market", "market.jsonl"])?;
        assert_eq!(
            cli.cmd,
            Cmd::Replay {
                market: PathBuf::from("market.jsonl"),
                risk: None,
                evidence: None,
            }
        );
        Ok(())
    }

    #[test]
    fn replay_accepts_an_explicit_risk_revaluation_file() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "replay",
            "--market",
            "market.jsonl",
            "--risk",
            "risk.json",
            "--evidence",
            "shadow.jsonl",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::Replay {
                market: PathBuf::from("market.jsonl"),
                risk: Some(PathBuf::from("risk.json")),
                evidence: Some(PathBuf::from("shadow.jsonl")),
            }
        );
        Ok(())
    }

    #[test]
    fn resident_shadow_requires_explicit_artifacts_binding_and_fill_floor()
    -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "shadow-resident",
            "--artifacts-root",
            "C:\\venue-shadow",
            "--binding",
            "C:\\venue-shadow\\binding.json",
            "--initial-fill-recovery-from-ms",
            "100",
            "--max-turns",
            "5",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::ShadowResident {
                artifacts_root: PathBuf::from("C:\\venue-shadow"),
                binding: PathBuf::from("C:\\venue-shadow\\binding.json"),
                initial_fill_recovery_from_ms: 100,
                max_turns: Some(5),
            }
        );
        Ok(())
    }

    #[test]
    fn resident_live_requires_explicit_mutation_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "live-resident",
                "--artifacts-root",
                "C:\\venue-live",
                "--binding",
                "C:\\venue-live\\binding.json",
                "--initial-fill-recovery-from-ms",
                "100",
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_confirmed_live_resident() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "live-resident",
            "--artifacts-root",
            "C:\\venue-live",
            "--binding",
            "C:\\venue-live\\binding.json",
            "--initial-fill-recovery-from-ms",
            "100",
            "--max-turns",
            "5",
            "--confirm-mainnet-strategy-mutations",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::LiveResident {
                artifacts_root: PathBuf::from("C:\\venue-live"),
                binding: PathBuf::from("C:\\venue-live\\binding.json"),
                initial_fill_recovery_from_ms: 100,
                max_turns: Some(5),
                confirm_mainnet_strategy_mutations: true,
            }
        );
        Ok(())
    }

    #[test]
    fn core_commits_require_explicit_artifacts_binding_and_input() -> Result<(), clap::Error> {
        let risk = Cli::try_parse_from([
            "venue",
            "core-owner-risk-commit",
            "--artifacts-root",
            "C:\\venue-shadow",
            "--binding",
            "C:\\venue-shadow\\binding.json",
            "--page",
            "C:\\core\\page.json",
        ])?;
        assert_eq!(
            risk.cmd,
            Cmd::CoreOwnerRiskCommit {
                artifacts_root: PathBuf::from("C:\\venue-shadow"),
                binding: PathBuf::from("C:\\venue-shadow\\binding.json"),
                page: PathBuf::from("C:\\core\\page.json"),
            }
        );
        let quote = Cli::try_parse_from([
            "venue",
            "core-quote-commit",
            "--artifacts-root",
            "C:\\venue-shadow",
            "--binding",
            "C:\\venue-shadow\\binding.json",
            "--receipt",
            "C:\\core\\receipt.json",
        ])?;
        assert_eq!(
            quote.cmd,
            Cmd::CoreQuoteCommit {
                artifacts_root: PathBuf::from("C:\\venue-shadow"),
                binding: PathBuf::from("C:\\venue-shadow\\binding.json"),
                receipt: PathBuf::from("C:\\core\\receipt.json"),
            }
        );
        Ok(())
    }

    #[test]
    fn mainnet_canary_requires_explicit_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "canary",
                "--phase",
                "place-cancel",
                "--side",
                "long",
                "--artifacts-root",
                "C:\\venue-canary",
            ])
            .is_err()
        );
    }

    #[test]
    fn grid_flatten_requires_explicit_mainnet_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "grid-flatten",
                "--artifacts-root",
                "C:\\venue-grid",
            ])
            .is_err()
        );
    }

    #[test]
    fn external_algo_cancel_requires_explicit_target_and_confirmation() -> Result<(), clap::Error> {
        assert!(
            Cli::try_parse_from([
                "venue",
                "grid-external-algo-cancel",
                "--artifacts-root",
                "C:\\venue-grid",
                "--expected-client-algo-id",
                "external_algo",
                "--expected-algo-id",
                "42",
            ])
            .is_err()
        );
        let cli = Cli::try_parse_from([
            "venue",
            "grid-external-algo-cancel",
            "--artifacts-root",
            "C:\\venue-grid",
            "--expected-client-algo-id",
            "external_algo",
            "--expected-algo-id",
            "42",
            "--confirm-mainnet-external-algo-cancel",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::GridExternalAlgoCancel {
                artifacts_root: PathBuf::from("C:\\venue-grid"),
                expected_client_algo_id: "external_algo".to_owned(),
                expected_algo_id: "42".to_owned(),
                confirm_mainnet_external_algo_cancel: true,
            }
        );
        Ok(())
    }

    #[test]
    fn parses_confirmed_grid_flatten() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "grid-flatten",
            "--artifacts-root",
            "C:\\venue-grid",
            "--confirm-mainnet-grid-mutations",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::GridFlatten {
                artifacts_root: PathBuf::from("C:\\venue-grid"),
                confirm_mainnet_grid_mutations: true,
            }
        );
        Ok(())
    }

    #[test]
    fn executable_handoff_requires_explicit_nonflat_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "grid-executable-handoff",
                "--artifacts-root",
                "C:\\venue-grid",
                "--release-manifest",
                "C:\\venue-grid\\handoff.json",
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_confirmed_executable_handoff() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "grid-executable-handoff",
            "--artifacts-root",
            "C:\\venue-grid",
            "--release-manifest",
            "C:\\venue-grid\\handoff.json",
            "--confirm-mainnet-nonflat-executable-handoff",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::GridExecutableHandoff {
                artifacts_root: PathBuf::from("C:\\venue-grid"),
                release_manifest: PathBuf::from("C:\\venue-grid\\handoff.json"),
                confirm_mainnet_nonflat_executable_handoff: true,
                confirm_mainnet_stopped_order_recovery: false,
                archive_resolved_command_wal: false,
            }
        );
        Ok(())
    }

    #[test]
    fn parses_explicit_stopped_order_recovery_handoff() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "venue",
            "grid-executable-handoff",
            "--artifacts-root",
            "C:\\venue-grid",
            "--release-manifest",
            "C:\\venue-grid\\handoff.json",
            "--confirm-mainnet-nonflat-executable-handoff",
            "--confirm-mainnet-stopped-order-recovery",
            "--archive-resolved-command-wal",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::GridExecutableHandoff {
                artifacts_root: PathBuf::from("C:\\venue-grid"),
                release_manifest: PathBuf::from("C:\\venue-grid\\handoff.json"),
                confirm_mainnet_nonflat_executable_handoff: true,
                confirm_mainnet_stopped_order_recovery: true,
                archive_resolved_command_wal: true,
            }
        );
        Ok(())
    }

    #[test]
    fn private_evidence_recovery_requires_explicit_forensic_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "grid-private-evidence-recover",
                "--artifacts-root",
                "C:\\venue-grid",
                "--expected-source-sha256",
                &"a".repeat(64),
                "--expected-canonical-selection-sha256",
                &"b".repeat(64),
                "--expected-quarantine-selection-sha256",
                &"c".repeat(64),
                "--expected-coverage-sha256",
                &"d".repeat(64),
                "--expected-canonical-tail-sequence",
                "80451",
                "--expected-collision-count",
                "11",
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_confirmed_private_evidence_recovery() -> Result<(), clap::Error> {
        let source_sha256 = "a".repeat(64);
        let canonical_selection_sha256 = "b".repeat(64);
        let quarantine_selection_sha256 = "c".repeat(64);
        let coverage_sha256 = "d".repeat(64);
        let cli = Cli::try_parse_from([
            "venue",
            "grid-private-evidence-recover",
            "--artifacts-root",
            "C:\\venue-grid",
            "--expected-source-sha256",
            &source_sha256,
            "--expected-canonical-selection-sha256",
            &canonical_selection_sha256,
            "--expected-quarantine-selection-sha256",
            &quarantine_selection_sha256,
            "--expected-coverage-sha256",
            &coverage_sha256,
            "--expected-canonical-tail-sequence",
            "80451",
            "--expected-collision-count",
            "11",
            "--confirm-private-evidence-forensic-recovery",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::GridPrivateEvidenceRecover {
                artifacts_root: PathBuf::from("C:\\venue-grid"),
                expected_source_sha256: source_sha256,
                expected_canonical_selection_sha256: canonical_selection_sha256,
                expected_quarantine_selection_sha256: quarantine_selection_sha256,
                expected_coverage_sha256: coverage_sha256,
                expected_canonical_tail_sequence: 80_451,
                expected_collision_count: 11,
                confirm_private_evidence_forensic_recovery: true,
            }
        );
        Ok(())
    }

    #[test]
    fn parses_confirmed_public_evidence_recovery() -> Result<(), clap::Error> {
        let source_sha256 = "a".repeat(64);
        let canonical_selection_sha256 = "b".repeat(64);
        let quarantine_selection_sha256 = "c".repeat(64);
        let coverage_sha256 = "d".repeat(64);
        let cli = Cli::try_parse_from([
            "venue",
            "grid-public-evidence-recover",
            "--artifacts-root",
            "C:\\venue-grid",
            "--expected-source-sha256",
            &source_sha256,
            "--expected-canonical-selection-sha256",
            &canonical_selection_sha256,
            "--expected-quarantine-selection-sha256",
            &quarantine_selection_sha256,
            "--expected-coverage-sha256",
            &coverage_sha256,
            "--expected-canonical-tail-sequence",
            "1100905",
            "--expected-collision-count",
            "1",
            "--confirm-public-evidence-forensic-recovery",
        ])?;
        assert_eq!(
            cli.cmd,
            Cmd::GridPublicEvidenceRecover {
                artifacts_root: PathBuf::from("C:\\venue-grid"),
                expected_source_sha256: source_sha256,
                expected_canonical_selection_sha256: canonical_selection_sha256,
                expected_quarantine_selection_sha256: quarantine_selection_sha256,
                expected_coverage_sha256: coverage_sha256,
                expected_canonical_tail_sequence: 1_100_905,
                expected_collision_count: 1,
                confirm_public_evidence_forensic_recovery: true,
            }
        );
        Ok(())
    }

    #[test]
    fn canary_recovery_requires_explicit_private_readback_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "canary-recover",
                "--artifacts-root",
                "C:\\venue-canary",
            ])
            .is_err()
        );
    }

    #[test]
    fn recovery_mutation_confirmation_requires_private_readback_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "canary-recover",
                "--artifacts-root",
                "C:\\venue-canary",
                "--confirm-mainnet-recovery-mutations",
            ])
            .is_err()
        );
    }

    #[test]
    fn legacy_binance_stop_requires_explicit_confirmation() {
        assert!(
            Cli::try_parse_from([
                "venue",
                "grid-legacy-binance-stop",
                "--artifacts-root",
                "C:\\venue-grid",
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_confirmed_legacy_binance_bridge() -> Result<(), clap::Error> {
        let legacy_sha = "a".repeat(64);
        let successor_sha = "b".repeat(64);
        let cli = Cli::try_parse_from([
            "venue",
            "grid-legacy-binance-bridge",
            "--artifacts-root",
            "C:\\venue-grid",
            "--legacy-config-path",
            "C:\\release\\venue.grid.toml",
            "--legacy-executable-path",
            "C:\\release\\legacy.exe",
            "--successor-executable-path",
            "C:\\release\\successor.exe",
            "--expected-legacy-executable-sha256",
            &legacy_sha,
            "--expected-successor-executable-sha256",
            &successor_sha,
            "--confirm-mainnet-nonflat-legacy-bridge",
        ])?;
        assert!(matches!(
            cli.cmd,
            Cmd::GridLegacyBinanceBridge {
                confirm_mainnet_nonflat_legacy_bridge: true,
                ..
            }
        ));
        Ok(())
    }
}
