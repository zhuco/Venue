use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset, Symbol},
    market::{RawMarketRecord, RawMarketRecorder, RawSource},
    runtime::ScalpingOwnerRiskPage,
    storage::{ScalpingRiskBinding, ScalpingRiskCursor, ScalpingRiskFact},
    strategy::scalping::{RiskFact, RiskUnit, StrategyBinding, StrategyKind},
};

#[test]
fn doctor_validates_default_config() -> Result<(), Box<dyn std::error::Error>> {
    let out = venue("doctor")?;

    assert!(out.status.success());
    assert_eq!(String::from_utf8(out.stdout)?.trim(), "ok symbol=SOL/USDT");
    Ok(())
}

#[test]
fn private_doctor_fails_closed_without_process_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let out = Command::new(env!("CARGO_BIN_EXE_venue"))
        .current_dir(directory.path())
        .arg("--config")
        .arg(cfg)
        .arg("doctor")
        .arg("--private")
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .output()?;
    let err = String::from_utf8(out.stderr)?;

    assert!(!out.status.success());
    assert!(err.contains("private credentials are absent or invalid"));
    Ok(())
}

#[test]
fn private_stream_doctor_requires_private_flag() -> Result<(), Box<dyn std::error::Error>> {
    let out = venue_args(["doctor", "--stream"])?;
    let err = String::from_utf8(out.stderr)?;

    assert!(!out.status.success());
    assert!(err.contains("--private"));
    Ok(())
}

#[test]
fn private_stream_doctor_fails_closed_without_process_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let out = Command::new(env!("CARGO_BIN_EXE_venue"))
        .current_dir(directory.path())
        .arg("--config")
        .arg(cfg)
        .args(["doctor", "--private", "--stream"])
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .output()?;
    let err = String::from_utf8(out.stderr)?;

    assert!(!out.status.success());
    assert!(err.contains("private credentials are absent or invalid"));
    Ok(())
}

#[test]
fn canary_recovery_requires_credentials_but_never_uses_the_real_order_confirmation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let artifacts = directory.path().join("artifacts");
    fs::create_dir_all(artifacts.join("solusdt"))?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let out = Command::new(env!("CARGO_BIN_EXE_venue"))
        .current_dir(directory.path())
        .arg("--config")
        .arg(cfg)
        .arg("canary-recover")
        .arg("--artifacts-root")
        .arg(artifacts)
        .arg("--confirm-mainnet-private-readback")
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .output()?;
    let err = String::from_utf8(out.stderr)?;
    assert!(!out.status.success());
    assert!(err.contains("private credentials are absent or invalid"));
    Ok(())
}

#[test]
fn trading_commands_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let out = venue("run")?;
    let err = String::from_utf8(out.stderr)?;
    assert!(!out.status.success());
    assert!(err.contains("disabled in the current migration phase"));
    let replay = venue("replay")?;
    assert!(!replay.status.success());
    assert!(String::from_utf8(replay.stderr)?.contains("--market"));
    Ok(())
}

#[test]
fn replay_is_a_file_only_fail_closed_shadow_audit() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let raw_path = directory.path().join("market.jsonl");
    write_shadow_market_fixture(&raw_path)?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_venue"))
        .arg("--config")
        .arg(cfg)
        .args(["replay", "--market"])
        .arg(raw_path)
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("records=2"));
    assert!(stdout.contains("preparations=0"));
    assert!(stdout.contains("intents=0"));
    assert!(stdout.contains("safety=fail_closed"));
    Ok(())
}

#[test]
fn replay_with_evidence_but_without_risk_remains_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let raw_path = directory.path().join("market.jsonl");
    let evidence_path = directory.path().join("shadow.jsonl");
    write_shadow_market_fixture(&raw_path)?;
    fs::write(&evidence_path, b"")?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_venue"))
        .arg("--config")
        .arg(cfg)
        .args(["replay", "--market"])
        .arg(raw_path)
        .args(["--evidence"])
        .arg(evidence_path)
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("preparations=0"));
    assert!(stdout.contains("intents=0"));
    assert!(stdout.contains("safety=fail_closed"));
    Ok(())
}

#[test]
fn replay_accepts_a_complete_explicit_risk_revaluation_file()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let raw_path = directory.path().join("market.jsonl");
    let risk_path = directory.path().join("risk.json");
    write_shadow_market_fixture(&raw_path)?;
    write_complete_risk_fixture(&risk_path, "")?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_venue"))
        .arg("--config")
        .arg(cfg)
        .args(["replay", "--market"])
        .arg(raw_path)
        .args(["--risk"])
        .arg(risk_path)
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("records=2"));
    assert!(stdout.contains("preparations=0"));
    assert!(stdout.contains("intents=0"));
    assert!(stdout.contains("safety=fail_closed"));
    Ok(())
}

#[test]
fn replay_rejects_risk_files_with_unknown_fields() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let raw_path = directory.path().join("market.jsonl");
    let risk_path = directory.path().join("risk.json");
    write_shadow_market_fixture(&raw_path)?;
    write_complete_risk_fixture(&risk_path, ",\"unexpected\":true")?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_venue"))
        .arg("--config")
        .arg(cfg)
        .args(["replay", "--market"])
        .arg(raw_path)
        .args(["--risk"])
        .arg(risk_path)
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("unknown field `unexpected`"));
    Ok(())
}

#[test]
fn replay_rejects_quote_asset_as_a_logical_risk_unit() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let raw_path = directory.path().join("market.jsonl");
    let risk_path = directory.path().join("risk.json");
    write_shadow_market_fixture(&raw_path)?;
    write_risk_fixture(&risk_path, "", "USDT")?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_venue"))
        .arg("--config")
        .arg(cfg)
        .args(["replay", "--market"])
        .arg(raw_path)
        .args(["--risk"])
        .arg(risk_path)
        .output()?;
    assert!(!output.status.success());
    Ok(())
}

#[test]
fn core_owner_risk_commit_is_file_only_and_retries_the_exact_page()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let artifacts_root = directory.path().join("artifacts");
    let binding_path = directory.path().join("binding.json");
    let page_path = directory.path().join("page.json");
    let binding = core_binding()?;
    fs::write(&binding_path, serde_json::to_vec(&binding)?)?;
    fs::write(
        &page_path,
        serde_json::to_vec(&core_owner_risk_page(&binding)?)?,
    )?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");

    let command = || {
        Command::new(env!("CARGO_BIN_EXE_venue"))
            .arg("--config")
            .arg(&cfg)
            .arg("core-owner-risk-commit")
            .arg("--artifacts-root")
            .arg(&artifacts_root)
            .arg("--binding")
            .arg(&binding_path)
            .arg("--page")
            .arg(&page_path)
            .env_remove("BINANCE_API_KEY")
            .env_remove("BINANCE_API_SECRET")
            .output()
    };
    let first = command()?;
    let retry = command()?;
    assert!(first.status.success());
    assert!(retry.status.success());
    assert!(String::from_utf8(first.stdout)?.contains("sequence=1"));
    assert!(String::from_utf8(retry.stdout)?.contains("sequence=1"));
    assert!(artifacts_root.join("owner_risk_pages.jsonl").is_file());
    Ok(())
}

#[test]
fn core_quote_commit_rejects_malformed_external_input_without_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding_path = directory.path().join("binding.json");
    let receipt_path = directory.path().join("receipt.json");
    fs::write(&binding_path, serde_json::to_vec(&core_binding()?)?)?;
    fs::write(&receipt_path, b"{}")?;
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_venue"))
        .arg("--config")
        .arg(cfg)
        .args(["core-quote-commit", "--artifacts-root"])
        .arg(directory.path().join("artifacts"))
        .arg("--binding")
        .arg(binding_path)
        .arg("--receipt")
        .arg(receipt_path)
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("Core quote receipt JSON is invalid"));
    Ok(())
}

fn write_shadow_market_fixture(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "SOL/USDT".parse()?;
    let mut recorder = RawMarketRecorder::open(path)?;
    let _ = recorder.append(RawMarketRecord::new(
        RawSource::RestSnapshot,
        symbol.clone(),
        1,
        100,
        r#"{"lastUpdateId":10,"bids":[["99.997","100"]],"asks":[["100.003","1"]]}"#.to_owned(),
    )?)?;
    let _ = recorder.append(RawMarketRecord::new(
        RawSource::WebSocketTrade,
        symbol,
        1,
        101,
        r#"{"e":"aggTrade","E":101,"s":"SOLUSDT","a":7,"p":"100.001","q":"1","nq":"100.001","f":7,"l":7,"T":101,"m":false,"st":1}"#.to_owned(),
    )?)?;
    Ok(())
}

fn core_binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "core-cli".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "3f62c1d0-7d50-432c-802d-df76e3afdc92".to_owned(),
        symbol: "SOL/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "core-cli:shadow-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, rust_decimal::Decimal::new(5, 0)),
    })
}

fn core_owner_risk_page(
    binding: &StrategyBinding,
) -> Result<ScalpingOwnerRiskPage, Box<dyn std::error::Error>> {
    let risk_unit = RiskUnit::new("risk")?;
    let risk_binding = ScalpingRiskBinding {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        owner_scope: binding.owner_scope.clone(),
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        parameter_release_id: binding.parameter_release_id.clone(),
        symbol: binding.symbol.clone(),
        risk_unit: risk_unit.clone(),
        valuation_generation: 1,
    };
    let fact_id = "core-cli-fact".to_owned();
    Ok(ScalpingOwnerRiskPage {
        requested_after: None,
        facts: vec![ScalpingRiskFact {
            binding: risk_binding.clone(),
            fact: RiskFact {
                fact_id: fact_id.clone(),
                event_time_ms: 100,
                valuation_generation: 1,
                risk_unit,
                realized_pnl: rust_decimal::Decimal::ZERO,
            },
        }],
        cursor: ScalpingRiskCursor {
            cursor_id: "core-cli-cursor".to_owned(),
            binding: risk_binding,
            source_sequence: 1,
            complete_from_ms: 100,
            observed_through_ms: 100,
            has_more: false,
            source_fact_ids: vec![fact_id],
        },
    })
}

fn write_complete_risk_fixture(path: &Path, extra_fields: &str) -> std::io::Result<()> {
    write_risk_fixture(path, extra_fields, "risk")
}

fn write_risk_fixture(path: &Path, extra_fields: &str, risk_unit: &str) -> std::io::Result<()> {
    fs::write(
        path,
        format!(
            r#"{{"proof_id":"cli-risk-proof-1","target_generation":7,"risk_unit":"{risk_unit}","window_start_ms":0,"complete_through_ms":120000,"source_fact_ids":["cli-risk-fact-1"],"revalued_facts":[{{"fact_id":"cli-risk-fact-1","event_time_ms":1,"valuation_generation":7,"risk_unit":"{risk_unit}","realized_pnl":"0"}}]{extra_fields}}}"#
        ),
    )
}

fn venue(cmd: &str) -> std::io::Result<Output> {
    venue_args([cmd])
}

fn venue_args<const N: usize>(args: [&str; N]) -> std::io::Result<Output> {
    let cfg = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("venue.toml");
    Command::new(env!("CARGO_BIN_EXE_venue"))
        .arg("--config")
        .arg(cfg)
        .args(args)
        .output()
}
