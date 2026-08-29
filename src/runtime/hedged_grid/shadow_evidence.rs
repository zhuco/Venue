use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{AccountRiskSnapshot, LegRiskSnapshot},
    execution::sha256_hex,
    strategy::hedged_grid::{
        ExposureGuardDecision, ExposureGuardParams, GridPosition, HedgedGridBinding,
    },
};

use super::RiskSnapshotRuntimeError;

pub(crate) const EXPOSURE_SHADOW_EVIDENCE_FILE: &str = "exposure_shadow_evidence.jsonl";
const EXPOSURE_SHADOW_SCHEMA_VERSION: u16 = 1;
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawRiskEvidenceRef {
    pub sequence: u64,
    pub generation: u64,
    pub payload_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExposureShadowDecision {
    WouldReduce,
    NoMutation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExposureShadowReason {
    ThresholdBreached,
    FlatLeg,
    BelowExposureThreshold,
    PnlNotPositive,
    PnlNotStrictlyAboveThreshold,
    GuardDisabled,
    EpisodeSuppressed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExposureShadowEvidence {
    pub schema_version: u16,
    pub sequence: u64,
    pub binding: HedgedGridBinding,
    pub position: GridPosition,
    pub account: AccountRiskSnapshot,
    pub leg: Option<LegRiskSnapshot>,
    pub params: ExposureGuardParams,
    #[serde(with = "rust_decimal::serde::str")]
    pub exposure_notional_threshold: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl_threshold: Decimal,
    pub decision: ExposureShadowDecision,
    pub reason: ExposureShadowReason,
    pub risk_episode_id: Option<String>,
    pub validated_at_ms: u64,
    pub raw_evidence: Vec<RawRiskEvidenceRef>,
    pub semantic_sha256: String,
}

#[derive(Serialize)]
struct ExposureShadowSemantic<'a> {
    binding: &'a HedgedGridBinding,
    position: GridPosition,
    risk_currency: &'a crate::domain::Asset,
    #[serde(with = "rust_decimal::serde::str")]
    account_equity: &'a Decimal,
    leg: Option<ShadowLegSemantic<'a>>,
    params: &'a ExposureGuardParams,
    #[serde(with = "rust_decimal::serde::str")]
    exposure_notional_threshold: &'a Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    unrealized_pnl_threshold: &'a Decimal,
    decision: ExposureShadowDecision,
    reason: ExposureShadowReason,
}

#[derive(Serialize)]
struct ShadowLegSemantic<'a> {
    #[serde(with = "rust_decimal::serde::str")]
    quantity: &'a Decimal,
    mark_price: &'a crate::domain::Price,
    #[serde(with = "rust_decimal::serde::str")]
    contract_multiplier: &'a Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    notional: &'a Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    unrealized_pnl: &'a Decimal,
}

#[expect(
    clippy::too_many_arguments,
    reason = "the evidence constructor binds all independently verified risk surfaces at one boundary"
)]
pub(crate) fn build_shadow_evidence(
    binding: &HedgedGridBinding,
    params: &ExposureGuardParams,
    account: &AccountRiskSnapshot,
    position: GridPosition,
    leg: Option<&LegRiskSnapshot>,
    decision: &ExposureGuardDecision,
    validated_at_ms: u64,
    raw_evidence: Vec<RawRiskEvidenceRef>,
) -> Result<ExposureShadowEvidence, RiskSnapshotRuntimeError> {
    let exposure_notional_threshold = account
        .account_equity
        .checked_mul(params.position_equity_multiple)
        .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
    let unrealized_pnl_threshold = account
        .account_equity
        .checked_mul(params.unrealized_pnl_equity_ratio)
        .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
    let (shadow_decision, reason, risk_episode_id) = match decision {
        ExposureGuardDecision::ReduceProfitableExposure(action) => (
            ExposureShadowDecision::WouldReduce,
            ExposureShadowReason::ThresholdBreached,
            Some(action.risk_episode_id.clone()),
        ),
        ExposureGuardDecision::Noop => (
            ExposureShadowDecision::NoMutation,
            noop_reason(
                params,
                leg,
                exposure_notional_threshold,
                unrealized_pnl_threshold,
            ),
            None,
        ),
    };
    let mut evidence = ExposureShadowEvidence {
        schema_version: EXPOSURE_SHADOW_SCHEMA_VERSION,
        sequence: 0,
        binding: binding.clone(),
        position,
        account: account.clone(),
        leg: leg.cloned(),
        params: params.clone(),
        exposure_notional_threshold,
        unrealized_pnl_threshold,
        decision: shadow_decision,
        reason,
        risk_episode_id,
        validated_at_ms,
        raw_evidence,
        semantic_sha256: String::new(),
    };
    evidence.semantic_sha256 = semantic_digest(&evidence)?;
    validate_record(&evidence)?;
    Ok(evidence)
}

fn noop_reason(
    params: &ExposureGuardParams,
    leg: Option<&LegRiskSnapshot>,
    exposure_threshold: Decimal,
    pnl_threshold: Decimal,
) -> ExposureShadowReason {
    let Some(leg) = leg else {
        return ExposureShadowReason::FlatLeg;
    };
    if leg.notional < exposure_threshold {
        ExposureShadowReason::BelowExposureThreshold
    } else if leg.unrealized_pnl <= Decimal::ZERO {
        ExposureShadowReason::PnlNotPositive
    } else if leg.unrealized_pnl <= pnl_threshold {
        ExposureShadowReason::PnlNotStrictlyAboveThreshold
    } else if !params.enabled {
        ExposureShadowReason::GuardDisabled
    } else {
        ExposureShadowReason::EpisodeSuppressed
    }
}

pub(crate) struct ExposureShadowEvidenceJournal {
    path: PathBuf,
    archive_path: PathBuf,
    max_segment_bytes: u64,
    next_sequence: u64,
    last_semantic_by_position: BTreeMap<GridPosition, String>,
}

impl ExposureShadowEvidenceJournal {
    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<Self, RiskSnapshotRuntimeError> {
        Self::open_with_max_segment_bytes(path.into(), DEFAULT_MAX_SEGMENT_BYTES)
    }

    fn open_with_max_segment_bytes(
        path: PathBuf,
        max_segment_bytes: u64,
    ) -> Result<Self, RiskSnapshotRuntimeError> {
        if max_segment_bytes == 0 {
            return Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt);
        }
        let archive_path = archive_path(&path);
        let records = recover_segments(&archive_path, &path)?;
        let next_sequence = match records.last() {
            Some(record) => record
                .sequence
                .checked_add(1)
                .ok_or(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?,
            None => 1,
        };
        let mut last_semantic_by_position = BTreeMap::new();
        for record in records {
            last_semantic_by_position.insert(record.position, record.semantic_sha256);
        }
        Ok(Self {
            path,
            archive_path,
            max_segment_bytes,
            next_sequence,
            last_semantic_by_position,
        })
    }

    pub(crate) fn append_if_changed(
        &mut self,
        mut evidence: ExposureShadowEvidence,
    ) -> Result<bool, RiskSnapshotRuntimeError> {
        validate_record(&evidence)?;
        if self
            .last_semantic_by_position
            .get(&evidence.position)
            .is_some_and(|digest| digest == &evidence.semantic_sha256)
        {
            return Ok(false);
        }
        evidence.sequence = self.next_sequence;
        let mut encoded = serde_json::to_vec(&evidence)
            .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
        encoded.push(b'\n');
        self.rotate_if_needed(
            u64::try_from(encoded.len()).map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceIo)?,
        )?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceIo)?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_data())
            .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceIo)?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
        self.last_semantic_by_position
            .insert(evidence.position, evidence.semantic_sha256);
        Ok(true)
    }

    pub(crate) fn recover(&self) -> Result<Vec<ExposureShadowEvidence>, RiskSnapshotRuntimeError> {
        recover_segments(&self.archive_path, &self.path)
    }

    fn rotate_if_needed(&self, append_bytes: u64) -> Result<(), RiskSnapshotRuntimeError> {
        let current_bytes = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(_) => return Err(RiskSnapshotRuntimeError::ShadowEvidenceIo),
        };
        if current_bytes == 0
            || current_bytes.saturating_add(append_bytes) <= self.max_segment_bytes
        {
            return Ok(());
        }
        match fs::remove_file(&self.archive_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(RiskSnapshotRuntimeError::ShadowEvidenceIo),
        }
        fs::rename(&self.path, &self.archive_path)
            .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceIo)
    }
}

fn validate_record(record: &ExposureShadowEvidence) -> Result<(), RiskSnapshotRuntimeError> {
    record
        .binding
        .validate()
        .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
    record
        .params
        .validate()
        .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
    record
        .account
        .validate_at(record.validated_at_ms, record.params.max_snapshot_age_ms)
        .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
    if let Some(leg) = &record.leg {
        crate::domain::validate_risk_snapshot_pair(
            &record.account,
            leg,
            record.validated_at_ms,
            record.params.max_snapshot_age_ms,
        )
        .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
    }
    let exposure_notional_threshold = record
        .account
        .account_equity
        .checked_mul(record.params.position_equity_multiple)
        .ok_or(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
    let unrealized_pnl_threshold = record
        .account
        .account_equity
        .checked_mul(record.params.unrealized_pnl_equity_ratio)
        .ok_or(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
    let decision_is_consistent = match record.decision {
        ExposureShadowDecision::WouldReduce => {
            record.reason == ExposureShadowReason::ThresholdBreached
                && record
                    .risk_episode_id
                    .as_ref()
                    .is_some_and(|episode| !episode.is_empty())
                && record.params.enabled
                && record.leg.as_ref().is_some_and(|leg| {
                    leg.notional >= exposure_notional_threshold
                        && leg.unrealized_pnl > unrealized_pnl_threshold
                })
        }
        ExposureShadowDecision::NoMutation => {
            record.risk_episode_id.is_none()
                && record.reason
                    == noop_reason(
                        &record.params,
                        record.leg.as_ref(),
                        exposure_notional_threshold,
                        unrealized_pnl_threshold,
                    )
        }
    };
    if record.schema_version != EXPOSURE_SHADOW_SCHEMA_VERSION
        || record.account.exchange != record.binding.exchange
        || record.account.account != record.binding.account
        || record.exposure_notional_threshold != exposure_notional_threshold
        || record.unrealized_pnl_threshold != unrealized_pnl_threshold
        || record.raw_evidence.is_empty()
        || record.raw_evidence.iter().any(|reference| {
            reference.sequence == 0
                || reference.generation != record.account.private_generation
                || reference.payload_sha256.len() != 64
                || !reference
                    .payload_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        || record
            .raw_evidence
            .iter()
            .map(|reference| reference.sequence)
            .collect::<BTreeSet<_>>()
            .len()
            != record.raw_evidence.len()
        || record.leg.as_ref().is_some_and(|leg| {
            leg.symbol != record.binding.symbol
                || leg.private_generation != record.account.private_generation
                || leg.risk_currency != record.account.risk_currency
                || match record.position {
                    GridPosition::Long => leg.position_side != crate::domain::PositionSide::Long,
                    GridPosition::Short => leg.position_side != crate::domain::PositionSide::Short,
                }
        })
        || !decision_is_consistent
        || record.semantic_sha256 != semantic_digest(record)?
    {
        return Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt);
    }
    Ok(())
}

fn semantic_digest(record: &ExposureShadowEvidence) -> Result<String, RiskSnapshotRuntimeError> {
    let semantic = ExposureShadowSemantic {
        binding: &record.binding,
        position: record.position,
        risk_currency: &record.account.risk_currency,
        account_equity: &record.account.account_equity,
        leg: record.leg.as_ref().map(|leg| ShadowLegSemantic {
            quantity: &leg.quantity,
            mark_price: &leg.mark_price,
            contract_multiplier: &leg.contract_multiplier,
            notional: &leg.notional,
            unrealized_pnl: &leg.unrealized_pnl,
        }),
        params: &record.params,
        exposure_notional_threshold: &record.exposure_notional_threshold,
        unrealized_pnl_threshold: &record.unrealized_pnl_threshold,
        decision: record.decision,
        reason: record.reason,
    };
    serde_json::to_vec(&semantic)
        .map(sha256_hex)
        .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)
}

fn recover_segments(
    archive_path: &Path,
    path: &Path,
) -> Result<Vec<ExposureShadowEvidence>, RiskSnapshotRuntimeError> {
    let mut records = read_segment(archive_path)?;
    records.extend(read_segment(path)?);
    if records
        .windows(2)
        .any(|window| window[1].sequence != window[0].sequence.saturating_add(1))
    {
        return Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt);
    }
    Ok(records)
}

fn read_segment(path: &Path) -> Result<Vec<ExposureShadowEvidence>, RiskSnapshotRuntimeError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(RiskSnapshotRuntimeError::ShadowEvidenceIo),
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt);
    }
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let record: ExposureShadowEvidence = serde_json::from_slice(line)
                .map_err(|_| RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)?;
            if record.sequence == 0 {
                return Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt);
            }
            validate_record(&record)?;
            Ok(record)
        })
        .collect()
}

fn archive_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".1");
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::{Asset, PositionSide, Price, RiskSourceStatus},
        strategy::hedged_grid::{ExposureGuardParams, ReduceProfitableExposure},
    };

    use super::*;

    #[test]
    fn generation_only_changes_are_deduplicated_and_raw_secrets_are_not_copied()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join(EXPOSURE_SHADOW_EVIDENCE_FILE);
        let mut journal = ExposureShadowEvidenceJournal::open(&path)?;
        assert!(journal.append_if_changed(evidence(
            1,
            Decimal::new(20, 0),
            would_reduce(1)?,
            "api-secret-1"
        )?)?);
        assert!(!journal.append_if_changed(evidence(
            2,
            Decimal::new(20, 0),
            would_reduce(2)?,
            "api-secret-2"
        )?)?);

        let stored = fs::read_to_string(&path)?;
        assert!(!stored.contains("api-secret"));
        assert_eq!(journal.recover()?.len(), 1);
        Ok(())
    }

    #[test]
    fn decision_change_appends_and_truncated_tail_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join(EXPOSURE_SHADOW_EVIDENCE_FILE);
        let mut journal = ExposureShadowEvidenceJournal::open(&path)?;
        journal.append_if_changed(evidence(1, Decimal::new(20, 0), would_reduce(1)?, "one")?)?;
        journal.append_if_changed(evidence(
            2,
            Decimal::new(20, 0),
            ExposureGuardDecision::Noop,
            "two",
        )?)?;
        assert_eq!(journal.recover()?.len(), 2);

        OpenOptions::new()
            .append(true)
            .open(&path)?
            .write_all(b"{\"truncated\":true}")?;
        assert!(matches!(
            ExposureShadowEvidenceJournal::open(&path),
            Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)
        ));
        Ok(())
    }

    #[test]
    fn dedicated_segments_rotate_and_reopen_with_monotonic_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join(EXPOSURE_SHADOW_EVIDENCE_FILE);
        let mut journal =
            ExposureShadowEvidenceJournal::open_with_max_segment_bytes(path.clone(), 1)?;
        journal.append_if_changed(evidence(1, Decimal::new(20, 0), would_reduce(1)?, "one")?)?;
        journal.append_if_changed(evidence(
            2,
            Decimal::new(20, 0),
            ExposureGuardDecision::Noop,
            "two",
        )?)?;
        journal.append_if_changed(evidence(
            3,
            Decimal::new(21, 0),
            ExposureGuardDecision::Noop,
            "three",
        )?)?;

        assert!(archive_path(&path).exists());
        let reopened = ExposureShadowEvidenceJournal::open_with_max_segment_bytes(path, 1)?;
        let records = reopened.recover()?;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].sequence + 1, records[1].sequence);
        assert_eq!(records[1].sequence, 3);
        Ok(())
    }

    #[test]
    fn forged_semantic_hash_cannot_bypass_snapshot_or_threshold_invariants()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut forged = evidence(1, Decimal::new(20, 0), would_reduce(1)?, "one")?;
        forged.exposure_notional_threshold = Decimal::new(59, 0);
        forged.semantic_sha256 = semantic_digest(&forged)?;
        assert_eq!(
            validate_record(&forged),
            Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)
        );

        forged = evidence(1, Decimal::new(20, 0), would_reduce(1)?, "one")?;
        forged.account.observed_at_ms = forged.validated_at_ms.saturating_add(1);
        forged.semantic_sha256 = semantic_digest(&forged)?;
        assert_eq!(
            validate_record(&forged),
            Err(RiskSnapshotRuntimeError::ShadowEvidenceCorrupt)
        );
        Ok(())
    }

    fn evidence(
        generation: u64,
        equity: Decimal,
        decision: ExposureGuardDecision,
        raw_secret: &str,
    ) -> Result<ExposureShadowEvidence, Box<dyn std::error::Error>> {
        let binding = binding()?;
        let currency = Asset::new("USDT")?;
        let account = AccountRiskSnapshot {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            risk_currency: currency.clone(),
            account_equity: equity,
            private_generation: generation,
            observed_at_ms: generation * 100,
            source_status: RiskSourceStatus::Complete,
        };
        let leg = LegRiskSnapshot {
            symbol: binding.symbol.clone(),
            position_side: PositionSide::Long,
            quantity: Decimal::new(60, 0),
            mark_price: Price::new(Decimal::ONE)?,
            contract_multiplier: Decimal::ONE,
            notional: Decimal::new(60, 0),
            unrealized_pnl: Decimal::new(2, 0),
            risk_currency: currency,
            private_generation: generation,
            observed_at_ms: generation * 100,
        };
        build_shadow_evidence(
            &binding,
            &ExposureGuardParams::fixed_release(),
            &account,
            GridPosition::Long,
            Some(&leg),
            &decision,
            generation * 100,
            vec![RawRiskEvidenceRef {
                sequence: generation,
                generation,
                payload_sha256: sha256_hex(raw_secret.as_bytes()),
            }],
        )
        .map_err(Into::into)
    }

    fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            strategy_instance_id: "shadow_grid".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "gate".to_owned(),
            account: "usdt_futures".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            config_version: "shadow_v1".to_owned(),
            owner_scope: "shadow_grid_primary".to_owned(),
        })
    }

    fn would_reduce(generation: u64) -> Result<ExposureGuardDecision, Box<dyn std::error::Error>> {
        Ok(ExposureGuardDecision::ReduceProfitableExposure(
            ReduceProfitableExposure {
                risk_episode_id: format!("etp-l-{generation:016x}"),
                position: GridPosition::Long,
                trigger_generation: generation,
                position_quantity: Decimal::new(60, 0),
                position_notional: Decimal::new(60, 0),
                account_equity: Decimal::new(20, 0),
                unrealized_pnl: Decimal::new(2, 0),
                reduce_ratio: Decimal::new(30, 2),
                risk_currency: Asset::new("USDT")?,
            },
        ))
    }
}
