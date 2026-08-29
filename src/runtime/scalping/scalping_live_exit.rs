use std::path::PathBuf;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{domain::CommandId, storage::ProjectionStore, strategy::scalping::StrategyBinding};

use super::{
    PrivateExposure, PrivateFactsProjectionInput, PrivateFactsReadiness, ProtectionProjection,
    ScalpingLiveGatewayError, ScalpingLiveSettlement, ScalpingProtectedGateway,
};

const LIVE_EXIT_SCHEMA_VERSION: u16 = 1;

/// One externally executed protected-writer operation. The state machine persists the phase
/// before returning it, so callers can restart without inventing a second exit submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalpingLiveExitAction {
    CleanupFlatKnown {
        protection_client_algo_id: CommandId,
        target_client_algo_id: Option<CommandId>,
    },
    CancelTarget {
        protection_client_algo_id: CommandId,
        target_client_algo_id: CommandId,
    },
    Reduce {
        client_algo_id: CommandId,
    },
    ReplaceStop {
        previous_client_algo_id: CommandId,
        hard_stop_distance_bps: Decimal,
    },
    CancelReplacedStop {
        previous_client_algo_id: CommandId,
        replacement_client_algo_id: CommandId,
    },
    CancelFlatStop {
        client_algo_id: CommandId,
    },
    RetireFlat,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ScalpingLiveExitCheckpoint {
    schema_version: u16,
    binding_digest: String,
    phase: ScalpingLiveExitPhase,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "phase")]
enum ScalpingLiveExitPhase {
    Idle,
    ReadyCleanupFlatKnown {
        protection_client_algo_id: CommandId,
        target_client_algo_id: Option<CommandId>,
    },
    ReadyCancelTarget {
        protection_client_algo_id: CommandId,
        target_client_algo_id: CommandId,
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
    },
    AwaitingTargetCancellation {
        protection_client_algo_id: CommandId,
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
        minimum_generation: u64,
    },
    ReadyReduce {
        client_algo_id: CommandId,
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
    },
    AwaitingReduction {
        client_algo_id: CommandId,
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
        minimum_generation: u64,
    },
    ReadyReplace {
        previous_client_algo_id: CommandId,
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
    },
    ReadyCancelReplaced {
        previous_client_algo_id: CommandId,
        replacement_client_algo_id: CommandId,
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
    },
    AwaitingReplacementCancellation {
        replacement_client_algo_id: CommandId,
        #[serde(with = "rust_decimal::serde::str")]
        hard_stop_distance_bps: Decimal,
        minimum_generation: u64,
    },
    ReadyCancelFlat {
        client_algo_id: CommandId,
    },
    AwaitingFlatCancellation {
        remaining_client_algo_id: Option<CommandId>,
        minimum_generation: u64,
    },
    ReadyRetireFlat,
}

/// Durable exit coordinator for one already-confirmed protected Algo identity. It never opens a
/// network client or submits a mutation; `ScalpingProtectedGateway` executes its returned action.
pub struct ScalpingLiveExitSettlement {
    store: ProjectionStore,
    checkpoint: ScalpingLiveExitCheckpoint,
}

/// Result of one physical exit-driver turn. `post_mutation_reconciliation` tells the resident to
/// revoke its current private worker readiness before it asks this driver for another action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingLiveExitDriveReport {
    pub action: Option<ScalpingLiveExitAction>,
    pub post_mutation_reconciliation: bool,
}

impl ScalpingLiveExitSettlement {
    pub fn open(
        artifacts_root: PathBuf,
        binding: &StrategyBinding,
    ) -> Result<Self, ScalpingLiveGatewayError> {
        if !artifacts_root.is_absolute() || binding.validate().is_err() {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        let store = ProjectionStore::new(artifacts_root.join("scalping_live_exit.json"));
        let digest = binding.digest();
        let checkpoint = match store.load::<ScalpingLiveExitCheckpoint>()? {
            Some(checkpoint)
                if checkpoint.schema_version == LIVE_EXIT_SCHEMA_VERSION
                    && checkpoint.binding_digest == digest =>
            {
                checkpoint
            }
            Some(_) => return Err(ScalpingLiveGatewayError::Settlement),
            None => ScalpingLiveExitCheckpoint {
                schema_version: LIVE_EXIT_SCHEMA_VERSION,
                binding_digest: digest,
                phase: ScalpingLiveExitPhase::Idle,
            },
        };
        Ok(Self { store, checkpoint })
    }

    /// Begins exactly one semantic exit. Duplicate recovery delivery is accepted only when the
    /// physical protection identity and hard-stop distance are identical.
    pub fn begin(
        &mut self,
        client_algo_id: CommandId,
        target_client_algo_id: Option<CommandId>,
        hard_stop_distance_bps: Decimal,
    ) -> Result<(), ScalpingLiveGatewayError> {
        if hard_stop_distance_bps <= Decimal::ZERO || client_algo_id.as_str().trim().is_empty() {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        let phase = match target_client_algo_id {
            Some(target_client_algo_id) => ScalpingLiveExitPhase::ReadyCancelTarget {
                protection_client_algo_id: client_algo_id,
                target_client_algo_id,
                hard_stop_distance_bps,
            },
            None => ScalpingLiveExitPhase::ReadyReduce {
                client_algo_id,
                hard_stop_distance_bps,
            },
        };
        match &self.checkpoint.phase {
            ScalpingLiveExitPhase::Idle => {
                self.checkpoint.phase = phase;
                self.persist()
            }
            current if current == &phase => Ok(()),
            _ => Err(ScalpingLiveGatewayError::Settlement),
        }
    }

    pub fn begin_flat_cleanup(
        &mut self,
        protection_client_algo_id: CommandId,
        target_client_algo_id: Option<CommandId>,
    ) -> Result<(), ScalpingLiveGatewayError> {
        if self.checkpoint.phase != ScalpingLiveExitPhase::Idle {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::ReadyCleanupFlatKnown {
            protection_client_algo_id,
            target_client_algo_id,
        };
        self.persist()
    }

    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.checkpoint.phase == ScalpingLiveExitPhase::Idle
    }

    #[must_use]
    pub fn needs_gateway(&self) -> bool {
        matches!(
            self.checkpoint.phase,
            ScalpingLiveExitPhase::ReadyCleanupFlatKnown { .. }
                | ScalpingLiveExitPhase::ReadyCancelTarget { .. }
                | ScalpingLiveExitPhase::ReadyReduce { .. }
                | ScalpingLiveExitPhase::ReadyReplace { .. }
                | ScalpingLiveExitPhase::ReadyCancelReplaced { .. }
                | ScalpingLiveExitPhase::ReadyCancelFlat { .. }
                | ScalpingLiveExitPhase::ReadyRetireFlat
        )
    }

    /// Only these phases follow an exchange mutation that has not yet been proven by a newer
    /// private-worker generation. The resident must withhold its old private fact in this window.
    #[must_use]
    pub fn awaits_private_reconciliation(&self) -> bool {
        matches!(
            self.checkpoint.phase,
            ScalpingLiveExitPhase::AwaitingTargetCancellation { .. }
                | ScalpingLiveExitPhase::AwaitingReduction { .. }
                | ScalpingLiveExitPhase::AwaitingReplacementCancellation { .. }
                | ScalpingLiveExitPhase::AwaitingFlatCancellation { .. }
        )
    }

    /// Emits at most one action. A post-mutation observation must be strictly newer than the
    /// persisted minimum generation; stale worker facts can never advance an exit phase.
    pub fn next(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<Option<ScalpingLiveExitAction>, ScalpingLiveGatewayError> {
        validate_projection_identity(readiness, projections)?;
        let action = match self.checkpoint.phase.clone() {
            ScalpingLiveExitPhase::Idle => None,
            ScalpingLiveExitPhase::ReadyCleanupFlatKnown {
                protection_client_algo_id,
                target_client_algo_id,
            } => Some(ScalpingLiveExitAction::CleanupFlatKnown {
                protection_client_algo_id,
                target_client_algo_id,
            }),
            ScalpingLiveExitPhase::ReadyCancelTarget {
                protection_client_algo_id,
                target_client_algo_id,
                ..
            } => Some(ScalpingLiveExitAction::CancelTarget {
                protection_client_algo_id,
                target_client_algo_id,
            }),
            ScalpingLiveExitPhase::ReadyReduce { client_algo_id, .. } => {
                Some(ScalpingLiveExitAction::Reduce { client_algo_id })
            }
            ScalpingLiveExitPhase::ReadyReplace {
                previous_client_algo_id,
                hard_stop_distance_bps,
            } => Some(ScalpingLiveExitAction::ReplaceStop {
                previous_client_algo_id,
                hard_stop_distance_bps,
            }),
            ScalpingLiveExitPhase::ReadyCancelReplaced {
                previous_client_algo_id,
                replacement_client_algo_id,
                ..
            } => Some(ScalpingLiveExitAction::CancelReplacedStop {
                previous_client_algo_id,
                replacement_client_algo_id,
            }),
            ScalpingLiveExitPhase::ReadyCancelFlat { client_algo_id } => {
                Some(ScalpingLiveExitAction::CancelFlatStop { client_algo_id })
            }
            ScalpingLiveExitPhase::ReadyRetireFlat => Some(ScalpingLiveExitAction::RetireFlat),
            ScalpingLiveExitPhase::AwaitingReduction {
                client_algo_id,
                hard_stop_distance_bps,
                minimum_generation,
            } => self.advance_after_readback(
                readiness,
                projections,
                client_algo_id,
                hard_stop_distance_bps,
                minimum_generation,
            )?,
            ScalpingLiveExitPhase::AwaitingTargetCancellation {
                protection_client_algo_id,
                hard_stop_distance_bps,
                minimum_generation,
            } => self.advance_after_target_cancellation(
                readiness,
                projections,
                protection_client_algo_id,
                hard_stop_distance_bps,
                minimum_generation,
            )?,
            ScalpingLiveExitPhase::AwaitingReplacementCancellation {
                replacement_client_algo_id,
                hard_stop_distance_bps,
                minimum_generation,
            } => self.advance_after_replacement_cancellation(
                readiness,
                projections,
                replacement_client_algo_id,
                hard_stop_distance_bps,
                minimum_generation,
            )?,
            ScalpingLiveExitPhase::AwaitingFlatCancellation {
                remaining_client_algo_id,
                minimum_generation,
            } => {
                if readiness.generation <= minimum_generation {
                    None
                } else if readiness.exposure == PrivateExposure::Flat
                    && !readiness.ordinary_order_debt
                    && readiness.algo_order_debt
                    && remaining_client_algo_id.is_some()
                {
                    let client_algo_id =
                        remaining_client_algo_id.ok_or(ScalpingLiveGatewayError::Settlement)?;
                    self.checkpoint.phase = ScalpingLiveExitPhase::ReadyCancelFlat {
                        client_algo_id: client_algo_id.clone(),
                    };
                    self.persist()?;
                    Some(ScalpingLiveExitAction::CancelFlatStop { client_algo_id })
                } else if readiness.exposure == PrivateExposure::Flat
                    && !readiness.ordinary_order_debt
                    && !readiness.algo_order_debt
                {
                    self.checkpoint.phase = ScalpingLiveExitPhase::ReadyRetireFlat;
                    self.persist()?;
                    Some(ScalpingLiveExitAction::RetireFlat)
                } else {
                    None
                }
            }
        };
        Ok(action)
    }

    pub fn record_reduce_sent(
        &mut self,
        client_algo_id: &CommandId,
        generation: u64,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let ScalpingLiveExitPhase::ReadyReduce {
            client_algo_id: expected,
            hard_stop_distance_bps,
        } = self.checkpoint.phase.clone()
        else {
            return Err(ScalpingLiveGatewayError::Settlement);
        };
        if &expected != client_algo_id || generation == 0 {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::AwaitingReduction {
            client_algo_id: expected,
            hard_stop_distance_bps,
            minimum_generation: generation,
        };
        self.persist()
    }

    pub fn record_target_cancel_sent(
        &mut self,
        protection_client_algo_id: &CommandId,
        target_client_algo_id: &CommandId,
        generation: u64,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let ScalpingLiveExitPhase::ReadyCancelTarget {
            protection_client_algo_id: expected_protection,
            target_client_algo_id: expected_target,
            hard_stop_distance_bps,
        } = self.checkpoint.phase.clone()
        else {
            return Err(ScalpingLiveGatewayError::Settlement);
        };
        if &expected_protection != protection_client_algo_id
            || &expected_target != target_client_algo_id
            || generation == 0
        {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::AwaitingTargetCancellation {
            protection_client_algo_id: expected_protection,
            hard_stop_distance_bps,
            minimum_generation: generation,
        };
        self.persist()
    }

    pub fn record_replacement_installed(
        &mut self,
        previous_client_algo_id: &CommandId,
        replacement_client_algo_id: CommandId,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let ScalpingLiveExitPhase::ReadyReplace {
            previous_client_algo_id: expected,
            hard_stop_distance_bps,
        } = self.checkpoint.phase.clone()
        else {
            return Err(ScalpingLiveGatewayError::Settlement);
        };
        if &expected != previous_client_algo_id {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::ReadyCancelReplaced {
            previous_client_algo_id: expected,
            replacement_client_algo_id,
            hard_stop_distance_bps,
        };
        self.persist()
    }

    pub fn record_replacement_cancel_sent(
        &mut self,
        previous_client_algo_id: &CommandId,
        replacement_client_algo_id: &CommandId,
        generation: u64,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let ScalpingLiveExitPhase::ReadyCancelReplaced {
            previous_client_algo_id: expected_previous,
            replacement_client_algo_id: expected_replacement,
            hard_stop_distance_bps,
        } = self.checkpoint.phase.clone()
        else {
            return Err(ScalpingLiveGatewayError::Settlement);
        };
        if &expected_previous != previous_client_algo_id
            || &expected_replacement != replacement_client_algo_id
            || generation == 0
        {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::AwaitingReplacementCancellation {
            replacement_client_algo_id: expected_replacement,
            hard_stop_distance_bps,
            minimum_generation: generation,
        };
        self.persist()
    }

    pub fn record_flat_cancel_sent(
        &mut self,
        client_algo_id: &CommandId,
        generation: u64,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let ScalpingLiveExitPhase::ReadyCancelFlat {
            client_algo_id: expected,
        } = self.checkpoint.phase.clone()
        else {
            return Err(ScalpingLiveGatewayError::Settlement);
        };
        if &expected != client_algo_id || generation == 0 {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::AwaitingFlatCancellation {
            remaining_client_algo_id: None,
            minimum_generation: generation,
        };
        self.persist()
    }

    pub fn record_flat_cleanup_sent(
        &mut self,
        cancelled_client_algo_id: &CommandId,
        remaining_client_algo_id: Option<CommandId>,
        generation: u64,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let ScalpingLiveExitPhase::ReadyCleanupFlatKnown {
            protection_client_algo_id,
            target_client_algo_id,
        } = self.checkpoint.phase.clone()
        else {
            return Err(ScalpingLiveGatewayError::Settlement);
        };
        let known = protection_client_algo_id == *cancelled_client_algo_id
            || target_client_algo_id.as_ref() == Some(cancelled_client_algo_id);
        if !known || generation == 0 {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::AwaitingFlatCancellation {
            remaining_client_algo_id,
            minimum_generation: generation,
        };
        self.persist()
    }

    pub fn record_flat_retired(&mut self) -> Result<(), ScalpingLiveGatewayError> {
        if self.checkpoint.phase != ScalpingLiveExitPhase::ReadyRetireFlat {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::Idle;
        self.persist()
    }

    /// Executes at most one durable exit action. The surrounding resident owns the private
    /// worker and must call `request_post_mutation_reconciliation` whenever this report says so.
    /// A flat retirement is local-only but still requires `ScalpingLiveSettlement` to consume the
    /// exact newer private projection before this phase is cleared.
    pub fn drive_gateway(
        &mut self,
        gateway: &mut ScalpingProtectedGateway,
        entry_settlement: &mut ScalpingLiveSettlement,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
        now_ms: u64,
    ) -> Result<ScalpingLiveExitDriveReport, ScalpingLiveGatewayError> {
        let Some(action) = self.next(readiness, projections)? else {
            return Ok(ScalpingLiveExitDriveReport {
                action: None,
                post_mutation_reconciliation: false,
            });
        };
        match &action {
            ScalpingLiveExitAction::CleanupFlatKnown {
                protection_client_algo_id,
                target_client_algo_id,
            } => {
                let (cancelled, remaining) = gateway.cancel_one_known_algo_after_flat(
                    protection_client_algo_id,
                    target_client_algo_id.as_ref(),
                    now_ms,
                )?;
                self.record_flat_cleanup_sent(&cancelled, remaining, readiness.generation)?;
                Ok(ScalpingLiveExitDriveReport {
                    action: Some(action),
                    post_mutation_reconciliation: true,
                })
            }
            ScalpingLiveExitAction::CancelTarget {
                protection_client_algo_id,
                target_client_algo_id,
            } => {
                gateway.cancel_target_before_exit(
                    protection_client_algo_id,
                    target_client_algo_id,
                    now_ms,
                )?;
                self.record_target_cancel_sent(
                    protection_client_algo_id,
                    target_client_algo_id,
                    readiness.generation,
                )?;
                Ok(ScalpingLiveExitDriveReport {
                    action: Some(action),
                    post_mutation_reconciliation: true,
                })
            }
            ScalpingLiveExitAction::Reduce { client_algo_id } => {
                gateway.reduce_open_position(now_ms)?;
                self.record_reduce_sent(client_algo_id, readiness.generation)?;
                Ok(ScalpingLiveExitDriveReport {
                    action: Some(action),
                    post_mutation_reconciliation: true,
                })
            }
            ScalpingLiveExitAction::ReplaceStop {
                previous_client_algo_id,
                hard_stop_distance_bps,
            } => {
                let replacement = gateway.install_replacement_stop(
                    previous_client_algo_id,
                    *hard_stop_distance_bps,
                    now_ms,
                )?;
                self.record_replacement_installed(previous_client_algo_id, replacement)?;
                Ok(ScalpingLiveExitDriveReport {
                    action: Some(action),
                    // The replacement is directly verified by the gateway. The next persisted
                    // action is its exact old-stop cancellation, so do not fence that bridge.
                    post_mutation_reconciliation: false,
                })
            }
            ScalpingLiveExitAction::CancelReplacedStop {
                previous_client_algo_id,
                replacement_client_algo_id,
            } => {
                gateway.cancel_replaced_stop(
                    previous_client_algo_id,
                    replacement_client_algo_id,
                    now_ms,
                )?;
                self.record_replacement_cancel_sent(
                    previous_client_algo_id,
                    replacement_client_algo_id,
                    readiness.generation,
                )?;
                Ok(ScalpingLiveExitDriveReport {
                    action: Some(action),
                    post_mutation_reconciliation: true,
                })
            }
            ScalpingLiveExitAction::CancelFlatStop { client_algo_id } => {
                gateway.cancel_known_algo_after_flat(client_algo_id, now_ms)?;
                self.record_flat_cancel_sent(client_algo_id, readiness.generation)?;
                Ok(ScalpingLiveExitDriveReport {
                    action: Some(action),
                    post_mutation_reconciliation: true,
                })
            }
            ScalpingLiveExitAction::RetireFlat => {
                entry_settlement.reconcile_flat_exit(readiness, projections)?;
                self.record_flat_retired()?;
                Ok(ScalpingLiveExitDriveReport {
                    action: Some(action),
                    post_mutation_reconciliation: false,
                })
            }
        }
    }

    fn advance_after_readback(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
        current_client_algo_id: CommandId,
        hard_stop_distance_bps: Decimal,
        minimum_generation: u64,
    ) -> Result<Option<ScalpingLiveExitAction>, ScalpingLiveGatewayError> {
        if readiness.generation <= minimum_generation {
            return Ok(None);
        }
        if readiness.exposure == PrivateExposure::Flat {
            self.checkpoint.phase = if readiness.algo_order_debt {
                ScalpingLiveExitPhase::ReadyCancelFlat {
                    client_algo_id: current_client_algo_id.clone(),
                }
            } else if !readiness.ordinary_order_debt {
                ScalpingLiveExitPhase::ReadyRetireFlat
            } else {
                return Ok(None);
            };
            self.persist()?;
            return self.next(readiness, projections);
        }
        if projections.protection.value == ProtectionProjection::Complete {
            self.checkpoint.phase = ScalpingLiveExitPhase::ReadyReduce {
                client_algo_id: current_client_algo_id,
                hard_stop_distance_bps,
            };
        } else {
            self.checkpoint.phase = ScalpingLiveExitPhase::ReadyReplace {
                previous_client_algo_id: current_client_algo_id,
                hard_stop_distance_bps,
            };
        }
        self.persist()?;
        self.next(readiness, projections)
    }

    fn advance_after_target_cancellation(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
        protection_client_algo_id: CommandId,
        hard_stop_distance_bps: Decimal,
        minimum_generation: u64,
    ) -> Result<Option<ScalpingLiveExitAction>, ScalpingLiveGatewayError> {
        if readiness.generation <= minimum_generation {
            return Ok(None);
        }
        if readiness.exposure == PrivateExposure::Flat {
            self.checkpoint.phase = if readiness.algo_order_debt {
                ScalpingLiveExitPhase::ReadyCancelFlat {
                    client_algo_id: protection_client_algo_id,
                }
            } else if !readiness.ordinary_order_debt {
                ScalpingLiveExitPhase::ReadyRetireFlat
            } else {
                return Ok(None);
            };
        } else if readiness.exposure == PrivateExposure::Open
            && readiness.algo_order_debt
            && projections.execution.value == super::ExecutionProjection::Known
            && projections.owner.value == super::OwnerProjection::Clear
        {
            // Phase-8 protection projection is intentionally Gap after the target is removed;
            // the still-visible stop identity remains the exact reduction guard.
            self.checkpoint.phase = ScalpingLiveExitPhase::ReadyReduce {
                client_algo_id: protection_client_algo_id,
                hard_stop_distance_bps,
            };
        } else {
            return Ok(None);
        }
        self.persist()?;
        self.next(readiness, projections)
    }

    fn advance_after_replacement_cancellation(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
        replacement_client_algo_id: CommandId,
        hard_stop_distance_bps: Decimal,
        minimum_generation: u64,
    ) -> Result<Option<ScalpingLiveExitAction>, ScalpingLiveGatewayError> {
        if readiness.generation <= minimum_generation {
            return Ok(None);
        }
        if readiness.exposure == PrivateExposure::Flat {
            self.checkpoint.phase = if readiness.algo_order_debt {
                ScalpingLiveExitPhase::ReadyCancelFlat {
                    client_algo_id: replacement_client_algo_id,
                }
            } else if !readiness.ordinary_order_debt {
                ScalpingLiveExitPhase::ReadyRetireFlat
            } else {
                return Ok(None);
            };
            self.persist()?;
            return self.next(readiness, projections);
        }
        if projections.protection.value != ProtectionProjection::Complete {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveExitPhase::ReadyReduce {
            client_algo_id: replacement_client_algo_id,
            hard_stop_distance_bps,
        };
        self.persist()?;
        self.next(readiness, projections)
    }

    fn persist(&self) -> Result<(), ScalpingLiveGatewayError> {
        self.store.save(&self.checkpoint)?;
        Ok(())
    }
}

fn validate_projection_identity(
    readiness: &PrivateFactsReadiness,
    projections: PrivateFactsProjectionInput,
) -> Result<(), ScalpingLiveGatewayError> {
    let identity = (readiness.generation, readiness.observed_at_ms);
    if readiness.generation == 0
        || readiness.observed_at_ms == 0
        || [
            (
                projections.execution.generation,
                projections.execution.observed_at_ms,
            ),
            (
                projections.owner.generation,
                projections.owner.observed_at_ms,
            ),
            (
                projections.protection.generation,
                projections.protection.observed_at_ms,
            ),
            (
                projections.risk_budget.generation,
                projections.risk_budget.observed_at_ms,
            ),
        ]
        .into_iter()
        .any(|projection| projection != identity)
    {
        return Err(ScalpingLiveGatewayError::Settlement);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::{
        domain::{Amount, CommandId},
        runtime::{ExecutionProjection, OwnerProjection, PrivateProjection, RiskBudgetProjection},
        strategy::scalping::StrategyKind,
    };

    use super::*;

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
            strategy_instance_id: "exit-test".to_owned(),
            run_id: "live-test".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            parameter_release_id: "direct-v1".to_owned(),
            owner_scope: "exit-test:live-test:SOL/USDT".to_owned(),
            risk_budget: Amount::new("USDT".parse()?, Decimal::new(10, 0)),
        })
    }

    fn facts(generation: u64, exposure: PrivateExposure, algo_debt: bool) -> PrivateFactsReadiness {
        PrivateFactsReadiness {
            generation,
            observed_at_ms: generation.saturating_mul(100),
            root_cause_fact_id: format!("private:{generation}"),
            exposure,
            ordinary_order_debt: false,
            algo_order_debt: algo_debt,
        }
    }

    fn projections(
        generation: u64,
        protection: ProtectionProjection,
    ) -> PrivateFactsProjectionInput {
        PrivateFactsProjectionInput {
            execution: PrivateProjection {
                generation,
                observed_at_ms: generation * 100,
                value: ExecutionProjection::Known,
            },
            owner: PrivateProjection {
                generation,
                observed_at_ms: generation * 100,
                value: OwnerProjection::Clear,
            },
            protection: PrivateProjection {
                generation,
                observed_at_ms: generation * 100,
                value: protection,
            },
            risk_budget: PrivateProjection {
                generation,
                observed_at_ms: generation * 100,
                value: RiskBudgetProjection::Available,
            },
        }
    }

    #[test]
    fn partial_exit_replaces_then_cancels_before_another_reduce()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let old = CommandId::new("vsa_old")?;
        let replacement = CommandId::new("vsp_replacement")?;
        let mut settlement =
            ScalpingLiveExitSettlement::open(directory.path().to_path_buf(), &binding)?;
        settlement.begin(old.clone(), None, Decimal::new(75, 0))?;

        assert_eq!(
            settlement.next(
                &facts(1, PrivateExposure::Open, true),
                projections(1, ProtectionProjection::Complete)
            )?,
            Some(ScalpingLiveExitAction::Reduce {
                client_algo_id: old.clone()
            })
        );
        settlement.record_reduce_sent(&old, 1)?;
        assert_eq!(
            settlement.next(
                &facts(2, PrivateExposure::Open, true),
                projections(2, ProtectionProjection::Gap)
            )?,
            Some(ScalpingLiveExitAction::ReplaceStop {
                previous_client_algo_id: old.clone(),
                hard_stop_distance_bps: Decimal::new(75, 0),
            })
        );
        settlement.record_replacement_installed(&old, replacement.clone())?;
        assert_eq!(
            settlement.next(
                &facts(2, PrivateExposure::Open, true),
                projections(2, ProtectionProjection::Gap)
            )?,
            Some(ScalpingLiveExitAction::CancelReplacedStop {
                previous_client_algo_id: old.clone(),
                replacement_client_algo_id: replacement.clone(),
            })
        );
        settlement.record_replacement_cancel_sent(&old, &replacement, 2)?;
        assert!(matches!(
            settlement.next(
                &facts(3, PrivateExposure::Open, true),
                projections(3, ProtectionProjection::Gap)
            ),
            Err(ScalpingLiveGatewayError::Settlement)
        ));
        assert_eq!(
            settlement.next(
                &facts(3, PrivateExposure::Open, true),
                projections(3, ProtectionProjection::Complete)
            )?,
            Some(ScalpingLiveExitAction::Reduce {
                client_algo_id: replacement.clone()
            })
        );
        settlement.record_reduce_sent(&replacement, 3)?;
        assert_eq!(
            settlement.next(
                &facts(4, PrivateExposure::Flat, true),
                projections(4, ProtectionProjection::Complete)
            )?,
            Some(ScalpingLiveExitAction::CancelFlatStop {
                client_algo_id: replacement.clone()
            })
        );
        settlement.record_flat_cancel_sent(&replacement, 4)?;
        assert_eq!(
            settlement.next(
                &facts(5, PrivateExposure::Flat, false),
                projections(5, ProtectionProjection::Complete)
            )?,
            Some(ScalpingLiveExitAction::RetireFlat)
        );
        settlement.record_flat_retired()?;
        assert_eq!(
            settlement.next(
                &facts(5, PrivateExposure::Flat, false),
                projections(5, ProtectionProjection::Complete)
            )?,
            None
        );
        Ok(())
    }
}
