use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{controller::ControlTarget, domain::Symbol, strategy::scalping::StrategyKind};

pub const RUNTIME_RECOVERY_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryFactValue {
    True,
    False,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeRecoveryIdentity {
    pub strategy_kind: StrategyKind,
    pub strategy_instance_id: String,
    pub run_id: String,
    pub owner_scope: String,
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub authority_root_digest: String,
    pub generation: u64,
}

impl RuntimeRecoveryIdentity {
    fn validate(&self) -> Result<(), RuntimeRecoveryError> {
        if self.strategy_kind != StrategyKind::Scalping
            || [
                self.strategy_instance_id.as_str(),
                self.run_id.as_str(),
                self.owner_scope.as_str(),
                self.exchange.as_str(),
                self.account.as_str(),
            ]
            .iter()
            .any(|value| value.trim().is_empty())
            || !digest_is_valid(&self.authority_root_digest)
            || self.generation == 0
        {
            return Err(RuntimeRecoveryError::Identity);
        }
        Ok(())
    }

    fn same_authority(&self, other: &Self) -> bool {
        self.strategy_kind == other.strategy_kind
            && self.strategy_instance_id == other.strategy_instance_id
            && self.owner_scope == other.owner_scope
            && self.exchange == other.exchange
            && self.account == other.account
            && self.symbol == other.symbol
            && self.authority_root_digest == other.authority_root_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AnonymousProtectionCustody {
    pub episode_id: String,
    pub custody_fact_id: String,
    pub owner_scope: String,
    pub run_id: String,
    pub authority_root_digest: String,
    pub exposure_unit: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub remaining_exposure: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub protected_exposure: Decimal,
    pub custody_generation: u64,
    pub exit_group_generation: u64,
    pub supervisor_generation: u64,
    pub clock_generation: u64,
    pub valid_until_ms: u64,
}

impl AnonymousProtectionCustody {
    fn valid_for(&self, identity: &RuntimeRecoveryIdentity, observed_at_ms: u64) -> bool {
        !self.episode_id.trim().is_empty()
            && !self.custody_fact_id.trim().is_empty()
            && !self.exposure_unit.trim().is_empty()
            && self.owner_scope == identity.owner_scope
            && self.run_id == identity.run_id
            && self.authority_root_digest == identity.authority_root_digest
            && self.remaining_exposure > Decimal::ZERO
            && self.remaining_exposure == self.protected_exposure
            && self.custody_generation > 0
            && self.exit_group_generation > 0
            && self.supervisor_generation > 0
            && self.clock_generation > 0
            && self.valid_until_ms > observed_at_ms
    }

    fn continues(&self, prior: &Self) -> bool {
        self.episode_id == prior.episode_id
            && self.custody_fact_id == prior.custody_fact_id
            && self.owner_scope == prior.owner_scope
            && self.run_id == prior.run_id
            && self.authority_root_digest == prior.authority_root_digest
            && self.exposure_unit == prior.exposure_unit
            && self.custody_generation >= prior.custody_generation
            && self.exit_group_generation >= prior.exit_group_generation
            && self.supervisor_generation >= prior.supervisor_generation
            && self.clock_generation >= prior.clock_generation
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "coverage", content = "proof")]
pub enum TakeoverCoverage {
    StoppedFlat {
        instance_fact_id: String,
        open_permission_generation: u64,
    },
    StoppedProtected {
        custody: AnonymousProtectionCustody,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTakeoverReceipt {
    pub schema_version: u16,
    pub receipt_id: String,
    pub generation: u64,
    pub issued_at_ms: u64,
    pub valid_until_ms: u64,
    pub predecessor: RuntimeRecoveryIdentity,
    pub successor: RuntimeRecoveryIdentity,
    pub persistent_control_target: ControlTarget,
    pub coverage: TakeoverCoverage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeReconciliationFacts {
    pub fact_id: String,
    pub fact_generation: u64,
    pub observed_at_ms: u64,
    pub owner_scope: String,
    pub run_id: String,
    pub authority_root_digest: String,
    pub runtime_generation: u64,
    pub private_snapshot_ready: RecoveryFactValue,
    pub owner_conflict: RecoveryFactValue,
    pub execution_unknown: RecoveryFactValue,
    pub flat: RecoveryFactValue,
    pub entry_terminal: RecoveryFactValue,
    pub protection_terminal: RecoveryFactValue,
    pub custody: Option<AnonymousProtectionCustody>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeRecoveryPhase {
    Isolated,
    Reconciling,
    ProtectionOnly,
    LoweringRisk,
    StoppedFlat,
    StoppedProtected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeRecoveryDirective {
    ReconcileOnly,
    LowerRisk {
        cancel_entry: bool,
        repair_protection: bool,
        reduce_exposure: bool,
    },
    StoppedFlat,
    StoppedProtected,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeRecoveryState {
    pub schema_version: u16,
    pub active_identity: RuntimeRecoveryIdentity,
    #[serde(default)]
    pub pending_successor: Option<RuntimeRecoveryIdentity>,
    pub required_takeover_generation: u64,
    #[serde(default)]
    pub persistent_control_target: Option<ControlTarget>,
    pub phase: RuntimeRecoveryPhase,
    #[serde(default)]
    pub last_fact_id: Option<String>,
    #[serde(default)]
    pub last_fact_generation: Option<u64>,
    #[serde(default)]
    pub last_observed_at_ms: Option<u64>,
    #[serde(default)]
    pub custody_continuity: Option<AnonymousProtectionCustody>,
    #[serde(default)]
    pub last_takeover_receipt_id: Option<String>,
}

impl RuntimeRecoveryState {
    pub fn new(
        active_identity: RuntimeRecoveryIdentity,
        persistent_control_target: Option<ControlTarget>,
    ) -> Result<Self, RuntimeRecoveryError> {
        active_identity.validate()?;
        Ok(Self {
            schema_version: RUNTIME_RECOVERY_SCHEMA_VERSION,
            active_identity,
            pending_successor: None,
            required_takeover_generation: 1,
            persistent_control_target,
            phase: RuntimeRecoveryPhase::Reconciling,
            last_fact_id: None,
            last_fact_generation: None,
            last_observed_at_ms: None,
            custody_continuity: None,
            last_takeover_receipt_id: None,
        })
    }

    /// Restored terminal assertions are not fresh private facts. Same-run recovery re-enters
    /// reconciliation; an identity mismatch is isolated until an exact takeover receipt arrives.
    pub fn restore_for(
        mut persisted: Self,
        current_identity: RuntimeRecoveryIdentity,
    ) -> Result<Self, RuntimeRecoveryError> {
        persisted.validate_persisted()?;
        current_identity.validate()?;
        if current_identity == persisted.active_identity {
            if persisted.pending_successor.is_none() {
                persisted.phase = RuntimeRecoveryPhase::Reconciling;
            }
            persisted.last_fact_id = None;
            persisted.last_fact_generation = None;
            persisted.last_observed_at_ms = None;
            persisted.custody_continuity = None;
            return Ok(persisted);
        }
        if !persisted.active_identity.same_authority(&current_identity)
            || persisted.active_identity.run_id == current_identity.run_id
            || current_identity.generation <= persisted.active_identity.generation
        {
            return Err(RuntimeRecoveryError::Identity);
        }
        if persisted
            .pending_successor
            .as_ref()
            .is_some_and(|pending| pending != &current_identity)
        {
            return Err(RuntimeRecoveryError::Identity);
        }
        persisted.pending_successor = Some(current_identity);
        if persisted.phase != RuntimeRecoveryPhase::ProtectionOnly {
            persisted.phase = RuntimeRecoveryPhase::Isolated;
        }
        persisted.last_fact_id = None;
        persisted.last_fact_generation = None;
        persisted.last_observed_at_ms = None;
        persisted.custody_continuity = None;
        Ok(persisted)
    }

    #[must_use]
    pub fn effective_control_target(&self) -> ControlTarget {
        self.persistent_control_target
            .unwrap_or(ControlTarget::StopAndProtect)
    }

    #[must_use]
    pub const fn phase(&self) -> RuntimeRecoveryPhase {
        self.phase
    }

    /// A protected receipt preserves predecessor custody and keeps the successor isolated. Only a
    /// later, exactly generated flat receipt activates the successor identity.
    pub fn apply_takeover(
        &mut self,
        receipt: &RuntimeTakeoverReceipt,
        observed_at_ms: u64,
    ) -> Result<RuntimeRecoveryDirective, RuntimeRecoveryError> {
        if self.last_takeover_receipt_id.as_deref() == Some(receipt.receipt_id.as_str()) {
            return Ok(self.current_directive());
        }
        let successor = self
            .pending_successor
            .as_ref()
            .ok_or(RuntimeRecoveryError::Takeover)?;
        if receipt.schema_version != RUNTIME_RECOVERY_SCHEMA_VERSION
            || receipt.receipt_id.trim().is_empty()
            || receipt.generation != self.required_takeover_generation
            || receipt.issued_at_ms == 0
            || receipt.issued_at_ms > observed_at_ms
            || receipt.valid_until_ms <= observed_at_ms
            || receipt.predecessor != self.active_identity
            || &receipt.successor != successor
            || !receipt.predecessor.same_authority(&receipt.successor)
            || receipt.predecessor.run_id == receipt.successor.run_id
            || receipt.persistent_control_target != self.effective_control_target()
        {
            return Err(RuntimeRecoveryError::Takeover);
        }
        match &receipt.coverage {
            TakeoverCoverage::StoppedFlat {
                instance_fact_id,
                open_permission_generation,
            } if !instance_fact_id.trim().is_empty() && *open_permission_generation > 0 => {
                self.active_identity = receipt.successor.clone();
                self.pending_successor = None;
                self.phase = RuntimeRecoveryPhase::Reconciling;
                self.custody_continuity = None;
            }
            TakeoverCoverage::StoppedProtected { custody }
                if custody.valid_for(&receipt.predecessor, observed_at_ms) =>
            {
                self.phase = RuntimeRecoveryPhase::ProtectionOnly;
                self.custody_continuity = Some(custody.clone());
            }
            _ => return Err(RuntimeRecoveryError::Coverage),
        }
        self.last_takeover_receipt_id = Some(receipt.receipt_id.clone());
        self.required_takeover_generation = receipt
            .generation
            .checked_add(1)
            .ok_or(RuntimeRecoveryError::Takeover)?;
        Ok(self.current_directive())
    }

    pub fn project(
        &mut self,
        facts: &RuntimeReconciliationFacts,
    ) -> Result<RuntimeRecoveryDirective, RuntimeRecoveryError> {
        if facts.fact_id.trim().is_empty()
            || facts.fact_generation == 0
            || facts.observed_at_ms == 0
            || facts.owner_scope != self.active_identity.owner_scope
            || facts.run_id != self.active_identity.run_id
            || facts.authority_root_digest != self.active_identity.authority_root_digest
            || facts.runtime_generation != self.active_identity.generation
        {
            return Err(RuntimeRecoveryError::Facts);
        }
        if matches!(
            self.phase,
            RuntimeRecoveryPhase::Isolated | RuntimeRecoveryPhase::ProtectionOnly
        ) {
            return Ok(RuntimeRecoveryDirective::ReconcileOnly);
        }
        if self.last_fact_id.as_deref() == Some(facts.fact_id.as_str()) {
            return if self.last_fact_generation == Some(facts.fact_generation)
                && self.last_observed_at_ms == Some(facts.observed_at_ms)
            {
                Ok(self.current_directive())
            } else {
                Err(RuntimeRecoveryError::Facts)
            };
        }
        if self
            .last_fact_generation
            .is_some_and(|generation| facts.fact_generation <= generation)
            || self
                .last_observed_at_ms
                .is_some_and(|observed| facts.observed_at_ms < observed)
        {
            return Err(RuntimeRecoveryError::Facts);
        }
        self.last_fact_id = Some(facts.fact_id.clone());
        self.last_fact_generation = Some(facts.fact_generation);
        self.last_observed_at_ms = Some(facts.observed_at_ms);

        if [
            facts.private_snapshot_ready,
            facts.owner_conflict,
            facts.execution_unknown,
            facts.flat,
            facts.entry_terminal,
            facts.protection_terminal,
        ]
        .contains(&RecoveryFactValue::Unknown)
        {
            self.phase = RuntimeRecoveryPhase::Reconciling;
            self.custody_continuity = None;
            return Ok(RuntimeRecoveryDirective::ReconcileOnly);
        }
        let facts_safe = facts.private_snapshot_ready == RecoveryFactValue::True
            && facts.owner_conflict == RecoveryFactValue::False
            && facts.execution_unknown == RecoveryFactValue::False;
        if !facts_safe {
            self.phase = RuntimeRecoveryPhase::Reconciling;
            self.custody_continuity = None;
            return Ok(RuntimeRecoveryDirective::ReconcileOnly);
        }
        let flat = facts.flat == RecoveryFactValue::True
            && facts.entry_terminal == RecoveryFactValue::True
            && facts.protection_terminal == RecoveryFactValue::True
            && facts.custody.is_none();
        if flat {
            self.phase = RuntimeRecoveryPhase::StoppedFlat;
            self.custody_continuity = None;
            return Ok(RuntimeRecoveryDirective::StoppedFlat);
        }
        if facts.flat == RecoveryFactValue::False
            && facts.entry_terminal == RecoveryFactValue::True
            && matches!(
                self.effective_control_target(),
                ControlTarget::Running | ControlTarget::StopAndProtect
            )
            && facts.custody.as_ref().is_some_and(|custody| {
                custody.valid_for(&self.active_identity, facts.observed_at_ms)
                    && self
                        .custody_continuity
                        .as_ref()
                        .is_none_or(|prior| custody.continues(prior))
            })
        {
            self.phase = RuntimeRecoveryPhase::StoppedProtected;
            self.custody_continuity = facts.custody.clone();
            return Ok(RuntimeRecoveryDirective::StoppedProtected);
        }
        self.phase = RuntimeRecoveryPhase::LoweringRisk;
        self.custody_continuity = None;
        Ok(RuntimeRecoveryDirective::LowerRisk {
            cancel_entry: facts.entry_terminal != RecoveryFactValue::True,
            repair_protection: facts.flat != RecoveryFactValue::True,
            reduce_exposure: facts.flat == RecoveryFactValue::False
                && matches!(
                    self.effective_control_target(),
                    ControlTarget::FlattenAndStop | ControlTarget::EmergencyStop
                ),
        })
    }

    fn current_directive(&self) -> RuntimeRecoveryDirective {
        match self.phase {
            RuntimeRecoveryPhase::StoppedFlat => RuntimeRecoveryDirective::StoppedFlat,
            RuntimeRecoveryPhase::StoppedProtected => RuntimeRecoveryDirective::StoppedProtected,
            RuntimeRecoveryPhase::LoweringRisk => RuntimeRecoveryDirective::LowerRisk {
                cancel_entry: true,
                repair_protection: true,
                reduce_exposure: matches!(
                    self.effective_control_target(),
                    ControlTarget::FlattenAndStop | ControlTarget::EmergencyStop
                ),
            },
            RuntimeRecoveryPhase::Isolated
            | RuntimeRecoveryPhase::Reconciling
            | RuntimeRecoveryPhase::ProtectionOnly => RuntimeRecoveryDirective::ReconcileOnly,
        }
    }

    fn validate_persisted(&self) -> Result<(), RuntimeRecoveryError> {
        self.active_identity.validate()?;
        if self.schema_version != RUNTIME_RECOVERY_SCHEMA_VERSION
            || self.required_takeover_generation == 0
            || self.pending_successor.as_ref().is_some_and(|successor| {
                successor.validate().is_err()
                    || !self.active_identity.same_authority(successor)
                    || self.active_identity.run_id == successor.run_id
            })
            || self.last_fact_id.is_some() != self.last_fact_generation.is_some()
            || self.last_fact_id.is_some() != self.last_observed_at_ms.is_some()
            || self
                .last_takeover_receipt_id
                .as_ref()
                .is_some_and(|receipt_id| receipt_id.trim().is_empty())
            || matches!(
                self.phase,
                RuntimeRecoveryPhase::Isolated | RuntimeRecoveryPhase::ProtectionOnly
            ) != self.pending_successor.is_some()
            || self
                .custody_continuity
                .as_ref()
                .is_some_and(|custody| !custody.valid_for(&self.active_identity, 0))
        {
            return Err(RuntimeRecoveryError::Snapshot);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RuntimeRecoveryError {
    #[error("runtime recovery identity is incomplete or crosses authority roots")]
    Identity,
    #[error("runtime recovery snapshot is inconsistent")]
    Snapshot,
    #[error("takeover receipt is stale or not exactly bound")]
    Takeover,
    #[error("takeover coverage is incomplete, expired, or inconsistent")]
    Coverage,
    #[error("runtime reconciliation facts are stale or bound to another identity")]
    Facts,
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
