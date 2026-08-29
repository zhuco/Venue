use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    config::ExposureTakeProfitConfig,
    domain::{
        AccountRiskSnapshot, CommandError, CommandId, FieldState, Fill, Instrument,
        LegRiskSnapshot, MarketReduceCommand, OrderError, OrderOwner, OrderPurpose, OrderSide,
        PositionSide, Price,
    },
    exchange::grid::GridRiskReadback,
    execution::sha256_hex,
    risk::{MarketReduceApproval, RiskError, authorize_market_reduction},
    storage::ProjectionStore,
    strategy::hedged_grid::{
        ExposureGuardError, ExposureGuardParams, GridPosition, HedgedGridBinding,
        ReduceProfitableExposure,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExposureRuntimeSettings {
    pub guard: ExposureGuardParams,
    pub shadow: bool,
    pub snapshot_interval_ms: u64,
}

impl TryFrom<ExposureTakeProfitConfig> for ExposureRuntimeSettings {
    type Error = RiskSnapshotRuntimeError;

    fn try_from(config: ExposureTakeProfitConfig) -> Result<Self, Self::Error> {
        config
            .validate()
            .map_err(|_| RiskSnapshotRuntimeError::Config)?;
        let guard = ExposureGuardParams {
            enabled: config.enabled,
            position_equity_multiple: config.position_equity_multiple,
            unrealized_pnl_equity_ratio: config.unrealized_pnl_equity_ratio,
            reduce_ratio: config.reduce_ratio,
            max_snapshot_age_ms: config.max_snapshot_age_ms,
            rearm_clear_generations: config.rearm_clear_generations,
        };
        guard.validate()?;
        Ok(Self {
            guard,
            shadow: config.shadow,
            snapshot_interval_ms: config.snapshot_interval_ms,
        })
    }
}

#[cfg(test)]
pub(crate) fn exposure_guard_params(
    config: ExposureTakeProfitConfig,
) -> Result<ExposureGuardParams, RiskSnapshotRuntimeError> {
    Ok(ExposureRuntimeSettings::try_from(config)?.guard)
}

/// One complete account observation with at most one normalized LONG and SHORT for this binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BindingRiskSnapshot {
    pub account: AccountRiskSnapshot,
    pub long: Option<LegRiskSnapshot>,
    pub short: Option<LegRiskSnapshot>,
    /// Validation clock after reconciling bounded exchange/local clock skew.
    pub validated_at_ms: u64,
}

impl BindingRiskSnapshot {
    pub fn leg(&self, position: GridPosition) -> Option<&LegRiskSnapshot> {
        match position {
            GridPosition::Long => self.long.as_ref(),
            GridPosition::Short => self.short.as_ref(),
        }
    }
}

/// Selects only the target symbol and rejects duplicate or mixed-generation Hedge legs. A flat
/// side is represented by absence, never by manufacturing a zero-quantity leg.
pub(crate) fn select_binding_risk_snapshot(
    readback: &GridRiskReadback,
    binding: &HedgedGridBinding,
    now_ms: u64,
    max_snapshot_age_ms: u64,
) -> Result<BindingRiskSnapshot, RiskSnapshotRuntimeError> {
    if readback.account.exchange != binding.exchange || readback.account.account != binding.account
    {
        return Err(RiskSnapshotRuntimeError::Binding);
    }
    let observed_at_ms = readback.account.observed_at_ms;
    if observed_at_ms > now_ms && observed_at_ms.saturating_sub(now_ms) > max_snapshot_age_ms {
        return Err(crate::domain::RiskSnapshotError::ObservedAt.into());
    }
    // Gate and Bitget sign with their authoritative exchange clocks, which can lead the host by a
    // bounded amount. The adapter has already bounded the signed readback window; retain the
    // exchange timestamp and validate against the later clock instead of rewriting evidence.
    let validated_at_ms = now_ms.max(observed_at_ms);
    readback
        .account
        .validate_at(validated_at_ms, max_snapshot_age_ms)?;
    let mut selected = BindingRiskSnapshot {
        account: readback.account.clone(),
        long: None,
        short: None,
        validated_at_ms,
    };
    for leg in readback
        .legs
        .iter()
        .filter(|leg| leg.symbol == binding.symbol)
    {
        crate::domain::validate_risk_snapshot_pair(
            &readback.account,
            leg,
            validated_at_ms,
            max_snapshot_age_ms,
        )?;
        let slot = match leg.position_side {
            PositionSide::Long => &mut selected.long,
            PositionSide::Short => &mut selected.short,
            PositionSide::Net => return Err(RiskSnapshotRuntimeError::PositionSide),
        };
        if slot.replace(leg.clone()).is_some() {
            return Err(RiskSnapshotRuntimeError::DuplicateLeg);
        }
    }
    Ok(selected)
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the authorized plan crosses one rare risk boundary and boxing would spread through its callers"
)]
pub(crate) enum MarketReductionPlan {
    Authorized {
        command: MarketReduceCommand,
        approval: MarketReduceApproval,
    },
    SkippedBelowMinimum {
        requested_quantity: Decimal,
        aligned_quantity: Decimal,
    },
}

/// Durable orchestration envelope. `command=None` means the trigger was persisted before command
/// construction; `Some` freezes the exact generation-fenced command for UNKNOWN/restart recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExposureReductionPending {
    pub action: ReduceProfitableExposure,
    pub review_account: AccountRiskSnapshot,
    pub review_leg: LegRiskSnapshot,
    pub command: Option<MarketReduceCommand>,
}

impl ExposureReductionPending {
    pub fn validate_identity(&self) -> Result<(), RiskSnapshotRuntimeError> {
        let position = grid_position(self.review_leg.position_side)?;
        let structural_now_ms = self
            .review_account
            .observed_at_ms
            .max(self.review_leg.observed_at_ms);
        crate::domain::validate_risk_snapshot_pair(
            &self.review_account,
            &self.review_leg,
            structural_now_ms,
            u64::MAX,
        )?;
        let (position_equity_multiple, unrealized_pnl_equity_ratio) =
            release_thresholds(self.action.reduce_ratio)?;
        let exposure_boundary = self
            .action
            .account_equity
            .checked_mul(position_equity_multiple)
            .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
        let pnl_boundary = self
            .action
            .account_equity
            .checked_mul(unrealized_pnl_equity_ratio)
            .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
        if self.action.position != position
            || self.action.risk_episode_id.trim().is_empty()
            || self.action.trigger_generation == 0
            || self.action.trigger_generation > self.review_account.private_generation
            || self.action.position_quantity <= Decimal::ZERO
            || self.action.position_notional < exposure_boundary
            || self.action.account_equity <= Decimal::ZERO
            || self.action.unrealized_pnl <= pnl_boundary
            || self.action.risk_currency != self.review_account.risk_currency
            || self.review_account.risk_currency != self.review_leg.risk_currency
            || self.review_account.private_generation != self.review_leg.private_generation
        {
            return Err(RiskSnapshotRuntimeError::Settlement);
        }
        match &self.command {
            None => {
                if self.action.trigger_generation != self.review_account.private_generation
                    || self.action.position_quantity != self.review_leg.quantity
                    || self.action.position_notional != self.review_leg.notional
                    || self.action.account_equity != self.review_account.account_equity
                    || self.action.unrealized_pnl != self.review_leg.unrealized_pnl
                {
                    return Err(RiskSnapshotRuntimeError::Settlement);
                }
            }
            Some(command) => {
                command.validate()?;
                let maximum_quantity = self
                    .review_leg
                    .quantity
                    .checked_mul(self.action.reduce_ratio)
                    .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
                if command.command_id.as_str() != format!("cmd-{}", self.action.risk_episode_id)
                    || command.client_order_id.as_str()
                        != format!("ord-{}", self.action.risk_episode_id)
                    || command.risk_episode_id.as_str() != self.action.risk_episode_id
                    || command.position_side != self.review_leg.position_side
                    || command.position_generation != self.review_leg.private_generation
                    || command.owner.exchange != self.review_account.exchange
                    || command.owner.account != self.review_account.account
                    || command.owner.symbol != self.review_leg.symbol
                    || command.quantity > maximum_quantity
                {
                    return Err(RiskSnapshotRuntimeError::Settlement);
                }
            }
        }
        Ok(())
    }
}

/// Recomputes the release-bound ratio from the latest signed leg, rounds down, builds deterministic episode-scoped
/// identities, and passes the complete command through the execution risk gate.
pub(crate) fn plan_market_reduction(
    binding: &HedgedGridBinding,
    action: &ReduceProfitableExposure,
    account: &AccountRiskSnapshot,
    leg: &LegRiskSnapshot,
    instrument: &Instrument,
    now_ms: u64,
    max_snapshot_age_ms: u64,
) -> Result<MarketReductionPlan, RiskSnapshotRuntimeError> {
    let position = grid_position(leg.position_side)?;
    if action.position != position || release_thresholds(action.reduce_ratio).is_err() {
        return Err(RiskSnapshotRuntimeError::Action);
    }
    if instrument.quantity_step <= Decimal::ZERO {
        return Err(RiskSnapshotRuntimeError::QuantityStep);
    }
    let requested_quantity = leg
        .quantity
        .checked_mul(action.reduce_ratio)
        .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
    let aligned_quantity =
        (requested_quantity / instrument.quantity_step).floor() * instrument.quantity_step;
    if aligned_quantity <= Decimal::ZERO {
        return Ok(MarketReductionPlan::SkippedBelowMinimum {
            requested_quantity,
            aligned_quantity,
        });
    }
    let command = MarketReduceCommand {
        command_id: CommandId::new(format!("cmd-{}", action.risk_episode_id))?,
        client_order_id: CommandId::new(format!("ord-{}", action.risk_episode_id))?,
        owner: OrderOwner {
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::ExposureTakeProfit,
        },
        position_side: leg.position_side,
        side: match leg.position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
            PositionSide::Net => return Err(RiskSnapshotRuntimeError::PositionSide),
        },
        quantity: aligned_quantity,
        risk_episode_id: CommandId::new(action.risk_episode_id.clone())?,
        position_generation: leg.private_generation,
    };
    match authorize_market_reduction(
        &command,
        instrument,
        account,
        leg,
        now_ms,
        max_snapshot_age_ms,
    ) {
        Ok(approval) => Ok(MarketReductionPlan::Authorized { command, approval }),
        Err(RiskError::MinimumNotional) => Ok(MarketReductionPlan::SkippedBelowMinimum {
            requested_quantity,
            aligned_quantity,
        }),
        Err(error) => Err(error.into()),
    }
}

fn release_thresholds(
    reduce_ratio: Decimal,
) -> Result<(Decimal, Decimal), RiskSnapshotRuntimeError> {
    match reduce_ratio {
        ratio if ratio == Decimal::new(30, 2) => Ok((Decimal::new(3, 0), Decimal::new(5, 2))),
        ratio if ratio == Decimal::new(14, 3) => Ok((Decimal::new(3, 0), Decimal::new(1, 3))),
        _ => Err(RiskSnapshotRuntimeError::Action),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExposureReductionAudit {
    pub event: String,
    pub risk_episode_id: String,
    pub exchange: String,
    pub account: String,
    pub symbol: crate::domain::Symbol,
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub account_equity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub position_unrealized_pnl: Decimal,
    pub risk_currency: crate::domain::Asset,
    #[serde(with = "rust_decimal::serde::str")]
    pub position_notional_before: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub requested_reduce_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub executed_reduce_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub executed_reduce_notional: Decimal,
    pub average_fill_price: Price,
    pub trigger_generation: u64,
    pub settled_generation: u64,
}

const EXPOSURE_REDUCTION_AUDIT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExposureReductionAuditRecord {
    schema_version: u32,
    sequence: u64,
    previous_sha256: Option<String>,
    audit: ExposureReductionAudit,
    record_sha256: String,
}

#[derive(Serialize)]
struct ExposureReductionAuditRecordDigest<'a> {
    schema_version: u32,
    sequence: u64,
    previous_sha256: &'a Option<String>,
    audit: &'a ExposureReductionAudit,
}

impl ExposureReductionAuditRecord {
    fn new(
        sequence: u64,
        previous_sha256: Option<String>,
        audit: ExposureReductionAudit,
    ) -> Result<Self, RiskSnapshotRuntimeError> {
        let mut record = Self {
            schema_version: EXPOSURE_REDUCTION_AUDIT_SCHEMA_VERSION,
            sequence,
            previous_sha256,
            audit,
            record_sha256: String::new(),
        };
        record.record_sha256 = record.expected_sha256()?;
        Ok(record)
    }

    fn expected_sha256(&self) -> Result<String, RiskSnapshotRuntimeError> {
        serde_json::to_vec(&ExposureReductionAuditRecordDigest {
            schema_version: self.schema_version,
            sequence: self.sequence,
            previous_sha256: &self.previous_sha256,
            audit: &self.audit,
        })
        .map(sha256_hex)
        .map_err(|_| RiskSnapshotRuntimeError::ReceiptEvidence)
    }

    fn validate(
        &self,
        expected_sequence: u64,
        expected_previous: &Option<String>,
    ) -> Result<(), RiskSnapshotRuntimeError> {
        if self.schema_version != EXPOSURE_REDUCTION_AUDIT_SCHEMA_VERSION
            || self.sequence != expected_sequence
            || &self.previous_sha256 != expected_previous
            || self.record_sha256 != self.expected_sha256()?
        {
            return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExposureReductionAuditHead {
    schema_version: u32,
    sequence: u64,
    record_sha256: String,
    /// The exact record selected before the journal append. Keeping it in the atomic head lets a
    /// restart finish that decision even when a fresh signed readback advances settlement fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_record: Option<ExposureReductionAuditRecord>,
    head_sha256: String,
}

#[derive(Serialize)]
struct ExposureReductionAuditHeadDigest<'a> {
    schema_version: u32,
    sequence: u64,
    record_sha256: &'a str,
}

impl ExposureReductionAuditHead {
    fn new(record: &ExposureReductionAuditRecord) -> Result<Self, RiskSnapshotRuntimeError> {
        let mut head = Self {
            schema_version: EXPOSURE_REDUCTION_AUDIT_SCHEMA_VERSION,
            sequence: record.sequence,
            record_sha256: record.record_sha256.clone(),
            pending_record: Some(record.clone()),
            head_sha256: String::new(),
        };
        head.head_sha256 = head.expected_sha256()?;
        Ok(head)
    }

    fn expected_sha256(&self) -> Result<String, RiskSnapshotRuntimeError> {
        serde_json::to_vec(&ExposureReductionAuditHeadDigest {
            schema_version: self.schema_version,
            sequence: self.sequence,
            record_sha256: &self.record_sha256,
        })
        .map(sha256_hex)
        .map_err(|_| RiskSnapshotRuntimeError::ReceiptEvidence)
    }

    fn validate(&self) -> Result<(), RiskSnapshotRuntimeError> {
        if self.schema_version != EXPOSURE_REDUCTION_AUDIT_SCHEMA_VERSION
            || self.sequence == 0
            || self.head_sha256 != self.expected_sha256()?
        {
            return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
        }
        if let Some(record) = &self.pending_record
            && (record.sequence != self.sequence
                || record.record_sha256 != self.record_sha256
                || record.record_sha256 != record.expected_sha256()?)
        {
            return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
        }
        Ok(())
    }
}

fn reduction_audit_head_path(path: &Path) -> PathBuf {
    path.with_file_name("exposure_take_profit.head.json")
}

fn read_reduction_audit_records(
    path: &Path,
) -> Result<Vec<ExposureReductionAuditRecord>, RiskSnapshotRuntimeError> {
    let existing = match fs::read(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(RiskSnapshotRuntimeError::ReceiptIo),
    };
    if !existing.is_empty() && existing.last() != Some(&b'\n') {
        return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
    }
    let mut records = Vec::new();
    let mut previous = None;
    for line in existing.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let record: ExposureReductionAuditRecord =
            serde_json::from_slice(line).map_err(|_| RiskSnapshotRuntimeError::ReceiptEvidence)?;
        record.validate(records.len() as u64 + 1, &previous)?;
        if records
            .iter()
            .any(|existing: &ExposureReductionAuditRecord| {
                existing.audit.risk_episode_id == record.audit.risk_episode_id
            })
        {
            return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
        }
        previous = Some(record.record_sha256.clone());
        records.push(record);
    }
    Ok(records)
}

fn append_reduction_audit_record(
    path: &Path,
    record: &ExposureReductionAuditRecord,
) -> Result<(), RiskSnapshotRuntimeError> {
    let mut bytes =
        serde_json::to_vec(record).map_err(|_| RiskSnapshotRuntimeError::ReceiptEvidence)?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|_| RiskSnapshotRuntimeError::ReceiptIo)?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| RiskSnapshotRuntimeError::ReceiptIo)
}

fn equivalent_reduction_audit_retry(
    durable: &ExposureReductionAudit,
    retry: &ExposureReductionAudit,
) -> bool {
    if retry.settled_generation < durable.settled_generation {
        return false;
    }
    let mut durable = durable.clone();
    let mut retry = retry.clone();
    durable.settled_generation = 0;
    retry.settled_generation = 0;
    durable == retry
}

/// Appends one hash-chained success receipt per risk episode. The independently checksummed head
/// is persisted first: after a crash the same audit can finish that exact append, while journal
/// truncation, a conflicting pending append, or mutation of either artifact fails closed.
pub(crate) fn append_reduction_audit_once(
    path: &Path,
    audit: &ExposureReductionAudit,
) -> Result<bool, RiskSnapshotRuntimeError> {
    let records = read_reduction_audit_records(path)?;
    let expected_sequence = records.len() as u64 + 1;
    let expected_previous = records.last().map(|record| record.record_sha256.clone());
    let head_store = ProjectionStore::new(reduction_audit_head_path(path));
    let head = head_store
        .load::<ExposureReductionAuditHead>()
        .map_err(|_| RiskSnapshotRuntimeError::ReceiptEvidence)?;
    if let Some(head) = &head {
        head.validate()?;
        match records.last() {
            Some(last)
                if head.sequence == last.sequence && head.record_sha256 == last.record_sha256 =>
            {
                if head
                    .pending_record
                    .as_ref()
                    .is_some_and(|record| record != last)
                {
                    return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
                }
            }
            _ if head.sequence == expected_sequence => {
                if let Some(pending) = &head.pending_record {
                    pending.validate(expected_sequence, &expected_previous)?;
                    if !equivalent_reduction_audit_retry(&pending.audit, audit) {
                        return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
                    }
                    append_reduction_audit_record(path, pending)?;
                    // The durable pre-crash record was appended during this call. Report a new
                    // receipt even if only the retry's settled generation advanced, so the
                    // operator still receives the required one-time success log.
                    return Ok(true);
                }
                let next = ExposureReductionAuditRecord::new(
                    expected_sequence,
                    expected_previous.clone(),
                    audit.clone(),
                )?;
                if head.record_sha256 == next.record_sha256 {
                    append_reduction_audit_record(path, &next)?;
                    return Ok(true);
                }
                return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
            }
            _ => return Err(RiskSnapshotRuntimeError::ReceiptEvidence),
        }
    } else if !records.is_empty() {
        return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
    }

    for previous in &records {
        if previous.audit.risk_episode_id == audit.risk_episode_id {
            return if equivalent_reduction_audit_retry(&previous.audit, audit) {
                Ok(false)
            } else {
                Err(RiskSnapshotRuntimeError::ReceiptEvidence)
            };
        }
    }

    let next =
        ExposureReductionAuditRecord::new(expected_sequence, expected_previous, audit.clone())?;
    let next_head = ExposureReductionAuditHead::new(&next)?;
    head_store
        .save(&next_head)
        .map_err(|_| RiskSnapshotRuntimeError::ReceiptIo)?;
    append_reduction_audit_record(path, &next)?;
    Ok(true)
}

pub(crate) fn reduction_audit_for_episode(
    path: &Path,
    risk_episode_id: &str,
) -> Result<Option<ExposureReductionAudit>, RiskSnapshotRuntimeError> {
    let records = read_reduction_audit_records(path)?;
    let head_store = ProjectionStore::new(reduction_audit_head_path(path));
    let head = head_store
        .load::<ExposureReductionAuditHead>()
        .map_err(|_| RiskSnapshotRuntimeError::ReceiptEvidence)?;
    match (records.last(), head) {
        (None, None) => {}
        (Some(last), Some(head)) => {
            head.validate()?;
            if head.sequence != last.sequence
                || head.record_sha256 != last.record_sha256
                || head
                    .pending_record
                    .as_ref()
                    .is_some_and(|pending| pending != last)
            {
                return Err(RiskSnapshotRuntimeError::ReceiptEvidence);
            }
        }
        _ => return Err(RiskSnapshotRuntimeError::ReceiptEvidence),
    }
    Ok(records
        .into_iter()
        .find(|record| record.audit.risk_episode_id == risk_episode_id)
        .map(|record| record.audit))
}

/// A normalized execution becomes risk-episode evidence only after the runtime has matched its
/// exact native/client order identity to the durable `MarketReduceCommand`. Keeping that identity
/// in the value passed to settlement prevents an unrelated taker fill from being audited under an
/// active episode merely because symbol, side and quantity happen to match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExposureReductionFill {
    risk_episode_id: CommandId,
    fill: Fill,
}

pub(crate) fn associate_reduction_fill(
    command: &MarketReduceCommand,
    fill: Fill,
) -> ExposureReductionFill {
    ExposureReductionFill {
        risk_episode_id: command.risk_episode_id.clone(),
        fill,
    }
}

/// Aggregates already-associated private executions. The executed notional is always derived from
/// actual execution prices, never from the trigger mark or requested quantity.
pub(crate) fn summarize_reduction_fills(
    command: &MarketReduceCommand,
    action: &ReduceProfitableExposure,
    account: &AccountRiskSnapshot,
    leg: &LegRiskSnapshot,
    fills: &[ExposureReductionFill],
    settled_generation: u64,
) -> Result<ExposureReductionAudit, RiskSnapshotRuntimeError> {
    if fills.is_empty() {
        return Err(RiskSnapshotRuntimeError::EmptyFills);
    }
    if command.risk_episode_id.as_str() != action.risk_episode_id
        || command.position_side != leg.position_side
        || command.position_generation != leg.private_generation
        || settled_generation < command.position_generation
    {
        return Err(RiskSnapshotRuntimeError::Settlement);
    }
    let mut fill_ids = BTreeMap::new();
    let mut executed_quantity = Decimal::ZERO;
    let mut weighted_price = Decimal::ZERO;
    let mut executed_notional = Decimal::ZERO;
    for associated in fills {
        if associated.risk_episode_id != command.risk_episode_id {
            return Err(RiskSnapshotRuntimeError::FillEvidence);
        }
        let fill = &associated.fill;
        fill.validate()?;
        if let Some(previous) = fill_ids.get(&fill.fill_id) {
            if previous == fill {
                continue;
            }
            return Err(RiskSnapshotRuntimeError::FillEvidence);
        }
        fill_ids.insert(fill.fill_id.clone(), fill.clone());
        if fill.symbol != leg.symbol
            || fill.side != command.side
            || fill.position_side != FieldState::Known(leg.position_side)
            || fill.maker != FieldState::Known(false)
        {
            return Err(RiskSnapshotRuntimeError::FillEvidence);
        }
        executed_quantity = executed_quantity
            .checked_add(fill.quantity)
            .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
        let execution_value = fill
            .quantity
            .checked_mul(fill.price.value())
            .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
        weighted_price = weighted_price
            .checked_add(execution_value)
            .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
        executed_notional = executed_notional
            .checked_add(
                execution_value
                    .checked_mul(leg.contract_multiplier)
                    .ok_or(RiskSnapshotRuntimeError::Arithmetic)?,
            )
            .ok_or(RiskSnapshotRuntimeError::Arithmetic)?;
    }
    if executed_quantity <= Decimal::ZERO || executed_quantity > command.quantity {
        return Err(RiskSnapshotRuntimeError::Settlement);
    }
    let average_fill_price = Price::new(
        weighted_price
            .checked_div(executed_quantity)
            .ok_or(RiskSnapshotRuntimeError::Arithmetic)?,
    )
    .map_err(|_| RiskSnapshotRuntimeError::Arithmetic)?;
    Ok(ExposureReductionAudit {
        event: "grid_exposure_take_profit".to_owned(),
        risk_episode_id: action.risk_episode_id.clone(),
        exchange: account.exchange.clone(),
        account: account.account.clone(),
        symbol: leg.symbol.clone(),
        position_side: leg.position_side,
        account_equity: account.account_equity,
        position_unrealized_pnl: leg.unrealized_pnl,
        risk_currency: account.risk_currency.clone(),
        position_notional_before: leg.notional,
        requested_reduce_ratio: action.reduce_ratio,
        executed_reduce_quantity: executed_quantity,
        executed_reduce_notional: executed_notional,
        average_fill_price,
        trigger_generation: action.trigger_generation,
        settled_generation,
    })
}

fn grid_position(position_side: PositionSide) -> Result<GridPosition, RiskSnapshotRuntimeError> {
    match position_side {
        PositionSide::Long => Ok(GridPosition::Long),
        PositionSide::Short => Ok(GridPosition::Short),
        PositionSide::Net => Err(RiskSnapshotRuntimeError::PositionSide),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum RiskSnapshotRuntimeError {
    #[error("exposure take-profit configuration is not the fixed release")]
    Config,
    #[error("exposure guard configuration is invalid: {0}")]
    Guard(#[from] ExposureGuardError),
    #[error("risk snapshot is not authoritative: {0}")]
    Snapshot(#[from] crate::domain::RiskSnapshotError),
    #[error("risk readback does not match the deployment binding")]
    Binding,
    #[error("risk readback contains a NET leg")]
    PositionSide,
    #[error("risk readback contains duplicate Hedge legs")]
    DuplicateLeg,
    #[error("exposure action does not match the latest signed leg")]
    Action,
    #[error("instrument quantity step is invalid")]
    QuantityStep,
    #[error("risk reduction arithmetic overflowed")]
    Arithmetic,
    #[error("risk reduction command identity is invalid: {0}")]
    Command(#[from] CommandError),
    #[error("market reduction failed execution risk authorization: {0}")]
    Risk(#[from] RiskError),
    #[error("risk reduction settlement has no executions")]
    EmptyFills,
    #[error("risk reduction execution evidence is incomplete or conflicting")]
    FillEvidence,
    #[error("risk reduction execution settlement conflicts with the command")]
    Settlement,
    #[error("risk reduction receipt storage failed")]
    ReceiptIo,
    #[error("risk reduction receipt evidence is corrupt or conflicting")]
    ReceiptEvidence,
    #[error("exposure shadow evidence storage failed")]
    ShadowEvidenceIo,
    #[error("exposure shadow evidence is corrupt or inconsistent")]
    ShadowEvidenceCorrupt,
    #[error("risk reduction fill is invalid: {0}")]
    Fill(#[from] OrderError),
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::{Amount, Asset, MarketKind, RiskSourceStatus},
        exchange::grid::GridRiskReadback,
    };

    use super::*;

    fn config() -> ExposureTakeProfitConfig {
        ExposureTakeProfitConfig {
            enabled: true,
            shadow: false,
            position_equity_multiple: Decimal::new(3, 0),
            unrealized_pnl_equity_ratio: Decimal::new(5, 2),
            reduce_ratio: Decimal::new(30, 2),
            snapshot_interval_ms: 120_000,
            max_snapshot_age_ms: 3_000,
            rearm_clear_generations: 2,
        }
    }

    fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            strategy_instance_id: "grid_doge".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "bitget".to_owned(),
            account: "uta_usdt".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            config_version: "risk_v1".to_owned(),
            owner_scope: "grid_doge".to_owned(),
        })
    }

    fn account(generation: u64) -> Result<AccountRiskSnapshot, Box<dyn std::error::Error>> {
        Ok(AccountRiskSnapshot {
            exchange: "bitget".to_owned(),
            account: "uta_usdt".to_owned(),
            risk_currency: "USDT".parse()?,
            account_equity: Decimal::new(20, 0),
            private_generation: generation,
            observed_at_ms: 1_000,
            source_status: RiskSourceStatus::Complete,
        })
    }

    fn leg(
        generation: u64,
        side: PositionSide,
        quantity: Decimal,
    ) -> Result<LegRiskSnapshot, Box<dyn std::error::Error>> {
        Ok(LegRiskSnapshot {
            symbol: "DOGE/USDT".parse()?,
            position_side: side,
            quantity,
            mark_price: Price::new(Decimal::ONE)?,
            contract_multiplier: Decimal::new(2, 0),
            notional: quantity * Decimal::new(2, 0),
            unrealized_pnl: Decimal::new(2, 0),
            risk_currency: "USDT".parse()?,
            private_generation: generation,
            observed_at_ms: 1_000,
        })
    }

    fn instrument(step: Decimal) -> Result<Instrument, Box<dyn std::error::Error>> {
        let asset: Asset = "USDT".parse()?;
        Ok(Instrument {
            symbol: "DOGE/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(asset.clone()),
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 4))?,
            quantity_step: step,
            minimum_notional: Amount::new(asset, Decimal::ONE),
        })
    }

    fn action(
        side: GridPosition,
        quantity: Decimal,
    ) -> Result<ReduceProfitableExposure, Box<dyn std::error::Error>> {
        Ok(ReduceProfitableExposure {
            risk_episode_id: "etp-l-0000000000000001".to_owned(),
            position: side,
            trigger_generation: 7,
            position_quantity: quantity,
            position_notional: quantity * Decimal::new(2, 0),
            account_equity: Decimal::new(20, 0),
            unrealized_pnl: Decimal::new(2, 0),
            reduce_ratio: Decimal::new(30, 2),
            risk_currency: "USDT".parse()?,
        })
    }

    #[test]
    fn fixed_config_maps_to_guard_and_preserves_runtime_switches()
    -> Result<(), Box<dyn std::error::Error>> {
        let settings = ExposureRuntimeSettings::try_from(config())?;
        assert!(settings.guard.enabled);
        assert!(!settings.shadow);
        assert_eq!(settings.snapshot_interval_ms, 120_000);
        assert_eq!(exposure_guard_params(config())?, settings.guard);
        let mut invalid = config();
        invalid.reduce_ratio = Decimal::new(31, 2);
        assert_eq!(
            ExposureRuntimeSettings::try_from(invalid),
            Err(RiskSnapshotRuntimeError::Config)
        );
        Ok(())
    }

    #[test]
    fn readback_selects_one_same_generation_long_and_short()
    -> Result<(), Box<dyn std::error::Error>> {
        let account = account(7)?;
        let long = leg(7, PositionSide::Long, Decimal::new(109, 0))?;
        let short = leg(7, PositionSide::Short, Decimal::new(80, 0))?;
        let readback = GridRiskReadback {
            raw_private_payloads: Vec::new(),
            account: account.clone(),
            legs: vec![short.clone(), long.clone()],
        };
        let selected = select_binding_risk_snapshot(&readback, &binding()?, 2_000, 3_000)?;
        assert_eq!(selected.account, account);
        assert_eq!(selected.validated_at_ms, 2_000);
        assert_eq!(selected.leg(GridPosition::Long), Some(&long));
        assert_eq!(selected.leg(GridPosition::Short), Some(&short));

        let duplicate = GridRiskReadback {
            legs: vec![long.clone(), long],
            ..readback.clone()
        };
        assert_eq!(
            select_binding_risk_snapshot(&duplicate, &binding()?, 2_000, 3_000),
            Err(RiskSnapshotRuntimeError::DuplicateLeg)
        );
        let mixed = GridRiskReadback {
            legs: vec![leg(8, PositionSide::Long, Decimal::new(109, 0))?],
            ..readback
        };
        assert!(matches!(
            select_binding_risk_snapshot(&mixed, &binding()?, 2_000, 3_000),
            Err(RiskSnapshotRuntimeError::Snapshot(_))
        ));
        Ok(())
    }

    #[test]
    fn selector_accepts_only_bounded_exchange_clock_lead() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut account = account(7)?;
        account.observed_at_ms = 2_900;
        let mut long = leg(7, PositionSide::Long, Decimal::new(109, 0))?;
        long.observed_at_ms = 2_900;
        let readback = GridRiskReadback {
            raw_private_payloads: Vec::new(),
            account,
            legs: vec![long],
        };
        let selected = select_binding_risk_snapshot(&readback, &binding()?, 2_000, 1_000)?;
        assert_eq!(selected.validated_at_ms, 2_900);

        let mut too_far = readback;
        too_far.account.observed_at_ms = 3_001;
        too_far.legs[0].observed_at_ms = 3_001;
        assert!(matches!(
            select_binding_risk_snapshot(&too_far, &binding()?, 2_000, 1_000),
            Err(RiskSnapshotRuntimeError::Snapshot(
                crate::domain::RiskSnapshotError::ObservedAt
            ))
        ));
        Ok(())
    }

    #[test]
    fn reduction_rounds_thirty_percent_down_and_is_risk_authorized()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let account = account(7)?;
        let latest_leg = leg(7, PositionSide::Long, Decimal::new(109, 0))?;
        let plan = plan_market_reduction(
            &binding,
            &action(GridPosition::Long, latest_leg.quantity)?,
            &account,
            &latest_leg,
            &instrument(Decimal::new(10, 0))?,
            2_000,
            3_000,
        )?;
        let MarketReductionPlan::Authorized { command, approval } = plan else {
            return Err("expected authorized reduction".into());
        };
        assert_eq!(command.quantity, Decimal::new(30, 0));
        assert_eq!(command.side, OrderSide::Sell);
        assert_eq!(command.position_generation, 7);
        assert_eq!(approval.private_generation, 7);

        let tiny_leg = leg(7, PositionSide::Long, Decimal::ONE)?;
        assert!(matches!(
            plan_market_reduction(
                &binding,
                &action(GridPosition::Long, tiny_leg.quantity)?,
                &account,
                &tiny_leg,
                &instrument(Decimal::new(10, 0))?,
                2_000,
                3_000,
            )?,
            MarketReductionPlan::SkippedBelowMinimum { aligned_quantity, .. }
                if aligned_quantity == Decimal::ZERO
        ));
        Ok(())
    }

    #[test]
    fn audit_uses_actual_fill_prices_multiplier_and_rejects_duplicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let account = account(7)?;
        let leg = leg(7, PositionSide::Long, Decimal::new(100, 0))?;
        let action = action(GridPosition::Long, leg.quantity)?;
        let MarketReductionPlan::Authorized { command, .. } = plan_market_reduction(
            &binding,
            &action,
            &account,
            &leg,
            &instrument(Decimal::new(10, 0))?,
            2_000,
            3_000,
        )?
        else {
            return Err("expected command".into());
        };
        let pending = ExposureReductionPending {
            action: action.clone(),
            review_account: account.clone(),
            review_leg: leg.clone(),
            command: Some(command.clone()),
        };
        pending.validate_identity()?;
        let restored: ExposureReductionPending =
            serde_json::from_slice(&serde_json::to_vec(&pending)?)?;
        assert_eq!(restored, pending);
        restored.validate_identity()?;

        let make_fill = |id: &str, price: Decimal| -> Result<Fill, Box<dyn std::error::Error>> {
            Ok(Fill {
                execution_sequence: FieldState::Known(1),
                fill_id: id.to_owned(),
                order_id: "native-1".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                side: OrderSide::Sell,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: Decimal::new(10, 0),
                price: Price::new(price)?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(false),
                exchange_time_ms: Some(1_100),
            })
        };
        let fills = [
            associate_reduction_fill(&command, make_fill("fill-1", Decimal::ONE)?),
            associate_reduction_fill(&command, make_fill("fill-2", Decimal::new(2, 0))?),
        ];
        let audit = summarize_reduction_fills(&command, &action, &account, &leg, &fills, 8)?;
        assert_eq!(audit.executed_reduce_quantity, Decimal::new(20, 0));
        assert_eq!(audit.executed_reduce_notional, Decimal::new(60, 0));
        assert_eq!(audit.event, "grid_exposure_take_profit");
        assert_eq!(audit.average_fill_price.value(), Decimal::new(15, 1));
        let receipt = serde_json::to_value(&audit)?;
        for field in [
            "event",
            "exchange",
            "account",
            "symbol",
            "position_side",
            "account_equity",
            "position_unrealized_pnl",
            "risk_currency",
            "position_notional_before",
            "requested_reduce_ratio",
            "executed_reduce_quantity",
            "executed_reduce_notional",
            "average_fill_price",
            "risk_episode_id",
            "trigger_generation",
            "settled_generation",
        ] {
            assert!(receipt.get(field).is_some(), "missing audit field {field}");
        }
        let temporary = tempfile::tempdir()?;
        let receipt_path = temporary.path().join("exposure_take_profit.jsonl");
        assert!(append_reduction_audit_once(&receipt_path, &audit)?);
        assert_eq!(
            reduction_audit_for_episode(&receipt_path, &audit.risk_episode_id)?,
            Some(audit.clone())
        );
        assert_eq!(
            reduction_audit_for_episode(&receipt_path, "missing-episode")?,
            None
        );
        assert!(!append_reduction_audit_once(&receipt_path, &audit)?);
        assert_eq!(
            std::fs::read_to_string(&receipt_path)?.lines().count(),
            1,
            "a restart after receipt fsync must not duplicate the risk episode"
        );
        let mut later_settlement_retry = audit.clone();
        later_settlement_retry.settled_generation += 1;
        assert!(
            !append_reduction_audit_once(&receipt_path, &later_settlement_retry)?,
            "a later readback generation must reuse the durable episode receipt"
        );
        let mut conflicting_receipt = audit.clone();
        conflicting_receipt.executed_reduce_notional += Decimal::ONE;
        assert_eq!(
            append_reduction_audit_once(&receipt_path, &conflicting_receipt),
            Err(RiskSnapshotRuntimeError::ReceiptEvidence)
        );

        let duplicate = [fills[0].clone(), fills[0].clone()];
        let deduplicated =
            summarize_reduction_fills(&command, &action, &account, &leg, &duplicate, 8)?;
        assert_eq!(deduplicated.executed_reduce_quantity, Decimal::new(10, 0));
        let mut conflicting = fills[0].clone();
        conflicting.fill.price = Price::new(Decimal::new(2, 0))?;
        assert_eq!(
            summarize_reduction_fills(
                &command,
                &action,
                &account,
                &leg,
                &[fills[0].clone(), conflicting],
                8
            ),
            Err(RiskSnapshotRuntimeError::FillEvidence)
        );
        let mut wrong_episode = fills[0].clone();
        wrong_episode.risk_episode_id = CommandId::new("etp-l-wrong-episode")?;
        assert_eq!(
            summarize_reduction_fills(&command, &action, &account, &leg, &[wrong_episode], 8),
            Err(RiskSnapshotRuntimeError::FillEvidence)
        );
        Ok(())
    }

    #[test]
    fn reduction_audit_chain_rejects_truncation_and_tampering()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let account = account(7)?;
        let leg = leg(7, PositionSide::Long, Decimal::new(100, 0))?;
        let action = action(GridPosition::Long, leg.quantity)?;
        let MarketReductionPlan::Authorized { command, .. } = plan_market_reduction(
            &binding,
            &action,
            &account,
            &leg,
            &instrument(Decimal::new(10, 0))?,
            2_000,
            3_000,
        )?
        else {
            return Err("expected command".into());
        };
        let fill = associate_reduction_fill(
            &command,
            Fill {
                execution_sequence: FieldState::Known(1),
                fill_id: "chain-fill".to_owned(),
                order_id: "chain-native".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                side: OrderSide::Sell,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: Decimal::new(30, 0),
                price: Price::new(Decimal::ONE)?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(false),
                exchange_time_ms: Some(1_100),
            },
        );
        let first = summarize_reduction_fills(&command, &action, &account, &leg, &[fill], 8)?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("exposure_take_profit.jsonl");
        assert!(append_reduction_audit_once(&path, &first)?);

        let mut second = first.clone();
        second.risk_episode_id = "etp-l-0000000000000002".to_owned();
        second.trigger_generation = 9;
        second.settled_generation = 10;
        assert!(append_reduction_audit_once(&path, &second)?);
        assert_eq!(read_reduction_audit_records(&path)?.len(), 2);

        let original = fs::read(&path)?;
        let first_newline = original
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or("missing first record terminator")?;
        fs::write(&path, &original[..=first_newline])?;
        let mut third = second.clone();
        third.risk_episode_id = "etp-l-0000000000000003".to_owned();
        assert_eq!(
            append_reduction_audit_once(&path, &third),
            Err(RiskSnapshotRuntimeError::ReceiptEvidence),
            "the durable head must detect a removed journal tail"
        );

        fs::write(&path, &original)?;
        let mut tampered_records = read_reduction_audit_records(&path)?;
        tampered_records[0].audit.settled_generation += 1;
        let mut tampered_journal = Vec::new();
        for record in &tampered_records {
            tampered_journal.extend(serde_json::to_vec(record)?);
            tampered_journal.push(b'\n');
        }
        fs::write(&path, tampered_journal)?;
        assert_eq!(
            append_reduction_audit_once(&path, &third),
            Err(RiskSnapshotRuntimeError::ReceiptEvidence),
            "journal content mutation must fail its record digest"
        );

        fs::write(&path, &original)?;
        let head_path = reduction_audit_head_path(&path);
        let mut head: serde_json::Value = serde_json::from_slice(&fs::read(&head_path)?)?;
        head["record_sha256"] = serde_json::Value::String("0".repeat(64));
        fs::write(&head_path, serde_json::to_vec(&head)?)?;
        assert_eq!(
            append_reduction_audit_once(&path, &second),
            Err(RiskSnapshotRuntimeError::ReceiptEvidence),
            "an idempotent retry must still validate the sidecar head"
        );
        Ok(())
    }

    #[test]
    fn reduction_audit_chain_finishes_a_head_first_crash() -> Result<(), Box<dyn std::error::Error>>
    {
        let binding = binding()?;
        let account = account(7)?;
        let leg = leg(7, PositionSide::Short, Decimal::new(100, 0))?;
        let action = action(GridPosition::Short, leg.quantity)?;
        let MarketReductionPlan::Authorized { command, .. } = plan_market_reduction(
            &binding,
            &action,
            &account,
            &leg,
            &instrument(Decimal::new(10, 0))?,
            2_000,
            3_000,
        )?
        else {
            return Err("expected command".into());
        };
        let fill = associate_reduction_fill(
            &command,
            Fill {
                execution_sequence: FieldState::Known(1),
                fill_id: "crash-fill".to_owned(),
                order_id: "crash-native".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Short),
                quantity: Decimal::new(30, 0),
                price: Price::new(Decimal::ONE)?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(false),
                exchange_time_ms: Some(1_100),
            },
        );
        let audit = summarize_reduction_fills(&command, &action, &account, &leg, &[fill], 8)?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("exposure_take_profit.jsonl");
        let pending = ExposureReductionAuditRecord::new(1, None, audit.clone())?;
        ProjectionStore::new(reduction_audit_head_path(&path))
            .save(&ExposureReductionAuditHead::new(&pending)?)?;

        assert!(append_reduction_audit_once(&path, &audit)?);
        assert!(!append_reduction_audit_once(&path, &audit)?);
        assert_eq!(read_reduction_audit_records(&path)?, vec![pending]);

        let advanced_temporary = tempfile::tempdir()?;
        let advanced_path = advanced_temporary.path().join("exposure_take_profit.jsonl");
        let advanced_pending = ExposureReductionAuditRecord::new(1, None, audit.clone())?;
        ProjectionStore::new(reduction_audit_head_path(&advanced_path))
            .save(&ExposureReductionAuditHead::new(&advanced_pending)?)?;
        let mut conflicting_pending_retry = audit.clone();
        conflicting_pending_retry.executed_reduce_notional += Decimal::ONE;
        assert_eq!(
            append_reduction_audit_once(&advanced_path, &conflicting_pending_retry),
            Err(RiskSnapshotRuntimeError::ReceiptEvidence),
            "a material change must not replace the exact record frozen in the head"
        );
        assert!(!advanced_path.exists());
        let mut advanced_readback = audit;
        advanced_readback.settled_generation += 1;
        assert!(append_reduction_audit_once(
            &advanced_path,
            &advanced_readback
        )?);
        assert_eq!(
            read_reduction_audit_records(&advanced_path)?,
            vec![advanced_pending],
            "a later signed generation must not strand or replace the head-first audit"
        );
        Ok(())
    }

    #[test]
    fn pending_reduction_round_trip_preserves_episode_command_and_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let account = account(7)?;
        let leg = leg(7, PositionSide::Short, Decimal::new(109, 0))?;
        let action = action(GridPosition::Short, leg.quantity)?;
        let MarketReductionPlan::Authorized { command, .. } = plan_market_reduction(
            &binding,
            &action,
            &account,
            &leg,
            &instrument(Decimal::ONE)?,
            2_000,
            3_000,
        )?
        else {
            return Err("expected authorized reduction".into());
        };
        let pending = ExposureReductionPending {
            action,
            review_account: account,
            review_leg: leg,
            command: Some(command),
        };
        pending.validate_identity()?;
        let restored: ExposureReductionPending =
            serde_json::from_slice(&serde_json::to_vec(&pending)?)?;
        restored.validate_identity()?;
        assert_eq!(restored, pending);

        let mut conflicting = restored;
        if let Some(command) = conflicting.command.as_mut() {
            command.position_generation = 8;
        }
        assert_eq!(
            conflicting.validate_identity(),
            Err(RiskSnapshotRuntimeError::Settlement)
        );
        Ok(())
    }
}
