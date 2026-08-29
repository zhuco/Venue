use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    domain::{PositionSide, Symbol},
    execution::{CanaryRunBinding, CanaryRunPhase, CanaryRunState},
};

pub const CANARY_RECOVERY_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanaryRecoveryCandidate {
    pub binding: CanaryRunBinding,
    pub phase: CanaryRunPhase,
    pub frozen: bool,
}

/// Converts durable discovery into a stable recovery queue. Terminal runs are deliberately absent:
/// this queue can only fence and reduce risk for unfinished runs.
pub fn scan_unfinished<'a>(
    runs: impl IntoIterator<Item = &'a CanaryRunState>,
) -> Vec<CanaryRecoveryCandidate> {
    let mut candidates = runs
        .into_iter()
        .filter(|run| !run.is_terminal())
        .map(|run| CanaryRecoveryCandidate {
            binding: run.binding().clone(),
            phase: run.phase().clone(),
            frozen: run.is_frozen(),
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (
            left.binding.exchange.as_str(),
            left.binding.account.as_str(),
            &left.binding.symbol,
            left.binding.owner_scope.as_str(),
            left.binding.canary_id.as_str(),
        )
            .cmp(&(
                right.binding.exchange.as_str(),
                right.binding.account.as_str(),
                &right.binding.symbol,
                right.binding.owner_scope.as_str(),
                right.binding.canary_id.as_str(),
            ))
    });
    candidates
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "state",
    content = "value"
)]
pub enum HedgePositionReadback {
    Known {
        #[serde(with = "rust_decimal::serde::str")]
        long_quantity: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        short_quantity: Decimal,
    },
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryOrdinaryOrder {
    pub owner_scope: String,
    pub command_id: String,
    pub client_order_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryAlgoOrder {
    pub owner_scope: String,
    pub command_id: String,
    pub client_algo_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "state",
    content = "orders"
)]
pub enum OrdinaryOrderReadback {
    Known(Vec<RecoveryOrdinaryOrder>),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "state",
    content = "orders"
)]
pub enum AlgoOrderReadback {
    Known(Vec<RecoveryAlgoOrder>),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedCanaryReadback {
    pub schema_version: u16,
    pub readback_id: String,
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub signer_sha256: String,
    pub payload_sha256: String,
    pub signature_sha256: String,
    pub signature_verified: bool,
    pub positions: HedgePositionReadback,
    pub ordinary_orders: OrdinaryOrderReadback,
    pub algo_orders: AlgoOrderReadback,
}

impl SignedCanaryReadback {
    pub fn calculate_payload_sha256(&self) -> Result<String, serde_json::Error> {
        serde_json::to_vec(&(
            self.schema_version,
            &self.readback_id,
            &self.exchange,
            &self.account,
            &self.symbol,
            self.generation,
            self.observed_at_ms,
            &self.positions,
            &self.ordinary_orders,
            &self.algo_orders,
        ))
        .map(|payload| format!("{:x}", Sha256::digest(payload)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExactOrdinaryCancel {
    pub command_id: String,
    pub client_order_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExactAlgoCancel {
    pub command_id: String,
    pub client_algo_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EmergencyFlattenLeg {
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectionDebtState {
    Confirmed,
    AlgoPresent,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemainFencedReason {
    InvalidRun,
    InvalidReadback,
    BindingMismatch,
    GenerationMismatch,
    SignatureUnverified,
    DuplicateConfirmation,
    UnknownFacts,
    ForeignDebt,
    NeedSecondFlatConfirmation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "plan", content = "detail")]
pub enum CanaryRecoveryPlan {
    ExactCancel {
        binding: CanaryRunBinding,
        ordinary: Vec<ExactOrdinaryCancel>,
        algos: Vec<ExactAlgoCancel>,
    },
    EmergencyFlatten {
        binding: CanaryRunBinding,
        legs: Vec<EmergencyFlattenLeg>,
        protection_debt: ProtectionDebtState,
    },
    SealFlat {
        binding: CanaryRunBinding,
        generation: u64,
        first_payload_sha256: String,
        second_payload_sha256: String,
    },
    RemainFenced {
        binding: CanaryRunBinding,
        reason: RemainFencedReason,
    },
}

/// Produces a recovery-only plan from two independently signed account snapshots. The second
/// snapshot drives risk reduction; both snapshots must be exact and clean before flat is sealed.
#[must_use]
pub fn plan_canary_recovery(
    candidate: &CanaryRecoveryCandidate,
    expected_signer_sha256: &str,
    first: &SignedCanaryReadback,
    second: &SignedCanaryReadback,
) -> CanaryRecoveryPlan {
    let fenced = |reason| CanaryRecoveryPlan::RemainFenced {
        binding: candidate.binding.clone(),
        reason,
    };
    if !valid_candidate(candidate) {
        return fenced(RemainFencedReason::InvalidRun);
    }
    if !valid_readback(first) || !valid_readback(second) {
        return fenced(RemainFencedReason::InvalidReadback);
    }
    if !readback_matches(&candidate.binding, first) || !readback_matches(&candidate.binding, second)
    {
        return fenced(RemainFencedReason::BindingMismatch);
    }
    if first.generation != candidate.binding.readback_generation
        || second.generation != candidate.binding.readback_generation
        || first.generation != second.generation
    {
        return fenced(RemainFencedReason::GenerationMismatch);
    }
    if !digest_is_valid(expected_signer_sha256)
        || first.signer_sha256 != expected_signer_sha256
        || second.signer_sha256 != expected_signer_sha256
        || !first.signature_verified
        || !second.signature_verified
    {
        return fenced(RemainFencedReason::SignatureUnverified);
    }
    if first.readback_id == second.readback_id || second.observed_at_ms <= first.observed_at_ms {
        return fenced(RemainFencedReason::DuplicateConfirmation);
    }

    let ordinary = match exact_ordinary_cancels(&candidate.binding, &second.ordinary_orders) {
        Ok(value) => value,
        Err(RemainFencedReason::UnknownFacts) => Vec::new(),
        Err(reason) => return fenced(reason),
    };
    if !ordinary.is_empty() {
        return CanaryRecoveryPlan::ExactCancel {
            binding: candidate.binding.clone(),
            ordinary,
            algos: Vec::new(),
        };
    }

    let (long_quantity, short_quantity) = match &second.positions {
        HedgePositionReadback::Known {
            long_quantity,
            short_quantity,
        } if *long_quantity >= Decimal::ZERO && *short_quantity >= Decimal::ZERO => {
            (*long_quantity, *short_quantity)
        }
        HedgePositionReadback::Known { .. } => {
            return fenced(RemainFencedReason::InvalidReadback);
        }
        HedgePositionReadback::Unknown => return fenced(RemainFencedReason::UnknownFacts),
    };
    if long_quantity > Decimal::ZERO || short_quantity > Decimal::ZERO {
        let mut legs = Vec::with_capacity(2);
        if long_quantity > Decimal::ZERO {
            legs.push(EmergencyFlattenLeg {
                position_side: PositionSide::Long,
                quantity: long_quantity,
            });
        }
        if short_quantity > Decimal::ZERO {
            legs.push(EmergencyFlattenLeg {
                position_side: PositionSide::Short,
                quantity: short_quantity,
            });
        }
        return CanaryRecoveryPlan::EmergencyFlatten {
            binding: candidate.binding.clone(),
            legs,
            protection_debt: match &second.algo_orders {
                AlgoOrderReadback::Known(orders) if orders.is_empty() => {
                    ProtectionDebtState::Confirmed
                }
                AlgoOrderReadback::Known(_) => ProtectionDebtState::AlgoPresent,
                AlgoOrderReadback::Unknown => ProtectionDebtState::Unknown,
            },
        };
    }
    let algos = match exact_algo_cancels(&candidate.binding, &second.algo_orders) {
        Ok(value) => value,
        Err(reason) => return fenced(reason),
    };
    if !algos.is_empty() {
        return CanaryRecoveryPlan::ExactCancel {
            binding: candidate.binding.clone(),
            ordinary,
            algos,
        };
    }
    if !readback_is_clean_flat(second) {
        return fenced(RemainFencedReason::UnknownFacts);
    }
    if !readback_is_clean_flat(first) {
        return fenced(RemainFencedReason::NeedSecondFlatConfirmation);
    }
    CanaryRecoveryPlan::SealFlat {
        binding: candidate.binding.clone(),
        generation: second.generation,
        first_payload_sha256: first.payload_sha256.clone(),
        second_payload_sha256: second.payload_sha256.clone(),
    }
}

/// Verifies only the evidence needed to retire the exact local writer of an already terminal
/// Flat run. Terminal runs never re-enter the ordinary recovery queue, so this deliberately
/// reuses its read-only proof rules with a provisional non-terminal phase; callers must accept
/// `SealFlat` only and must still bind the active writer generation exactly.
#[must_use]
pub fn plan_terminal_flat_writer_retirement(
    candidate: &CanaryRecoveryCandidate,
    expected_signer_sha256: &str,
    first: &SignedCanaryReadback,
    second: &SignedCanaryReadback,
) -> CanaryRecoveryPlan {
    if !matches!(candidate.phase, CanaryRunPhase::Flat { .. }) {
        return CanaryRecoveryPlan::RemainFenced {
            binding: candidate.binding.clone(),
            reason: RemainFencedReason::InvalidRun,
        };
    }
    let provisional = CanaryRecoveryCandidate {
        binding: candidate.binding.clone(),
        phase: CanaryRunPhase::Prepared,
        frozen: false,
    };
    plan_canary_recovery(&provisional, expected_signer_sha256, first, second)
}

fn valid_candidate(candidate: &CanaryRecoveryCandidate) -> bool {
    let binding = &candidate.binding;
    !matches!(&candidate.phase, CanaryRunPhase::Flat { .. })
        && !binding.canary_id.trim().is_empty()
        && !binding.exchange.trim().is_empty()
        && !binding.account.trim().is_empty()
        && !binding.owner_scope.trim().is_empty()
        && !binding.release_id.trim().is_empty()
        && binding.position_side != PositionSide::Net
        && binding.writer_generation > 0
        && binding.readback_generation > 0
        && binding.valid_until_ms > 0
}

fn valid_readback(readback: &SignedCanaryReadback) -> bool {
    readback.schema_version == CANARY_RECOVERY_SCHEMA_VERSION
        && !readback.readback_id.trim().is_empty()
        && !readback.exchange.trim().is_empty()
        && !readback.account.trim().is_empty()
        && readback.generation > 0
        && readback.observed_at_ms > 0
        && digest_is_valid(&readback.signer_sha256)
        && digest_is_valid(&readback.payload_sha256)
        && digest_is_valid(&readback.signature_sha256)
        && readback
            .calculate_payload_sha256()
            .is_ok_and(|digest| digest == readback.payload_sha256)
}

fn readback_matches(binding: &CanaryRunBinding, readback: &SignedCanaryReadback) -> bool {
    readback.exchange == binding.exchange
        && readback.account == binding.account
        && readback.symbol == binding.symbol
}

fn exact_ordinary_cancels(
    binding: &CanaryRunBinding,
    readback: &OrdinaryOrderReadback,
) -> Result<Vec<ExactOrdinaryCancel>, RemainFencedReason> {
    let OrdinaryOrderReadback::Known(orders) = readback else {
        return Err(RemainFencedReason::UnknownFacts);
    };
    let mut identities = BTreeSet::new();
    let mut cancels = Vec::with_capacity(orders.len());
    for order in orders {
        if order.owner_scope != binding.owner_scope {
            return Err(RemainFencedReason::ForeignDebt);
        }
        if order.command_id.trim().is_empty()
            || order.client_order_id.trim().is_empty()
            || !identities.insert((order.command_id.as_str(), order.client_order_id.as_str()))
        {
            return Err(RemainFencedReason::InvalidReadback);
        }
        cancels.push(ExactOrdinaryCancel {
            command_id: order.command_id.clone(),
            client_order_id: order.client_order_id.clone(),
        });
    }
    cancels.sort_by(|left, right| {
        (&left.command_id, &left.client_order_id).cmp(&(&right.command_id, &right.client_order_id))
    });
    Ok(cancels)
}

fn exact_algo_cancels(
    binding: &CanaryRunBinding,
    readback: &AlgoOrderReadback,
) -> Result<Vec<ExactAlgoCancel>, RemainFencedReason> {
    let AlgoOrderReadback::Known(orders) = readback else {
        return Err(RemainFencedReason::UnknownFacts);
    };
    let mut identities = BTreeSet::new();
    let mut cancels = Vec::with_capacity(orders.len());
    for order in orders {
        if order.owner_scope != binding.owner_scope {
            return Err(RemainFencedReason::ForeignDebt);
        }
        if order.command_id.trim().is_empty()
            || order.client_algo_id.trim().is_empty()
            || !identities.insert((order.command_id.as_str(), order.client_algo_id.as_str()))
        {
            return Err(RemainFencedReason::InvalidReadback);
        }
        cancels.push(ExactAlgoCancel {
            command_id: order.command_id.clone(),
            client_algo_id: order.client_algo_id.clone(),
        });
    }
    cancels.sort_by(|left, right| {
        (&left.command_id, &left.client_algo_id).cmp(&(&right.command_id, &right.client_algo_id))
    });
    Ok(cancels)
}

fn readback_is_clean_flat(readback: &SignedCanaryReadback) -> bool {
    matches!(
        &readback.positions,
        HedgePositionReadback::Known {
            long_quantity,
            short_quantity,
        } if *long_quantity == Decimal::ZERO && *short_quantity == Decimal::ZERO
    ) && matches!(&readback.ordinary_orders, OrdinaryOrderReadback::Known(orders) if orders.is_empty())
        && matches!(&readback.algo_orders, AlgoOrderReadback::Known(orders) if orders.is_empty())
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
