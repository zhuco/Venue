use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    config::BinanceAccountBinding,
    domain::{
        Amount, CancelCommand, CommandId, Instrument, OrderCommand, OrderOwner, OrderPurpose,
        OrderSide, Position, PositionSide, Price, StopMarketFullPositionCommand,
        is_canonical_trading_account_id,
    },
    exchange::{
        binance::{
            PrivateCredentials, PrivateError, PrivateReadbackError, PrivateRest, PublicError,
            PublicRest, parse_depth_best_prices, parse_instrument,
        },
        binance_private,
    },
    execution::{
        AlgoProtectionCustodyInput, CommandJournal, ExecutionError, ExecutionReceipt, FlatReceipt,
        ProtectedReceipt, ProtectionEvidence, ProtectionPreflight, StrategyEntryPreflight,
        StrategyProtectionPreflight, StrategyReductionPreflight, WriterLeaseAuthority,
        WriterLeaseError, WriterScope, WriterSession, prove_algo_protection_custody, sha256_hex,
        submit_cancel, submit_stop_market_full_position, submit_strategy_limit_entry,
        submit_strategy_reduce, submit_strategy_stop_market_full_position,
        submit_strategy_take_profit_market_full_position,
    },
    risk::AccountRiskView,
    storage::{ProjectionStore, StorageError},
    strategy::scalping::{
        Direction, EntryStyle, PHASE8_ATR14_PARAMETER_RELEASE_ID, SemanticIntent, StrategyBinding,
    },
};

use super::{
    ExecutionProjection, OwnerProjection, PrivateExposure, PrivateFactsProjectionInput,
    PrivateFactsReadiness, ProtectionProjection,
};

const EXCHANGE: &str = "binance";
#[cfg(test)]
const TEST_ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
const CUSTODY_TTL_MS: u64 = 5_000;
const LIVE_SETTLEMENT_SCHEMA_VERSION: u16 = 2;

#[path = "scalping_live_gateway_recovery.rs"]
mod recovery;
pub use recovery::{recover_absent_unknown_scalping_entry, recover_unknown_scalping_cancels};

fn live_entry_target_usdt() -> Decimal {
    Decimal::new(5, 0)
}

/// Configuration for one exact strategy owner scope. The gateway is deliberately not a general
/// account trader: it opens one durable writer and never crosses this binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingLiveGatewayConfig {
    pub artifacts_root: PathBuf,
    pub binding: StrategyBinding,
    pub private_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalpingLiveEntryOutcome {
    NoFill {
        command_id: CommandId,
    },
    Protected {
        command_id: CommandId,
        position_side: PositionSide,
        quantity: Decimal,
        protection_strategy_id: String,
        protection_client_algo_id: CommandId,
    },
    ProtectedWithTarget {
        command_id: CommandId,
        position_side: PositionSide,
        quantity: Decimal,
        protection_strategy_id: String,
        protection_client_algo_id: CommandId,
        target_strategy_id: String,
        target_client_algo_id: CommandId,
    },
}

/// The only two terminal writer transitions that a newer, durable private-worker readback can
/// establish after a strategy entry attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalpingWriterReconciliation {
    RetiredFlat,
    ProtectionOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalpingLiveSettlementAction {
    RejectNoFill {
        intent_id: String,
    },
    ConfirmProtected {
        intent_id: String,
        client_algo_id: CommandId,
    },
    ConfirmProtectedWithTarget {
        intent_id: String,
        client_algo_id: CommandId,
        target_client_algo_id: CommandId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ScalpingLiveSettlementCheckpoint {
    schema_version: u16,
    binding_digest: String,
    phase: ScalpingLiveSettlementPhase,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "phase")]
enum ScalpingLiveSettlementPhase {
    Idle,
    Submitting {
        intent_id: String,
        client_algo_id: CommandId,
        #[serde(default)]
        target_client_algo_id: Option<CommandId>,
    },
    AwaitingNoFill {
        intent_id: String,
    },
    AwaitingProtected {
        intent_id: String,
        client_algo_id: CommandId,
        #[serde(default)]
        target_client_algo_id: Option<CommandId>,
    },
    ReadyNoFill {
        intent_id: String,
    },
    ReadyProtected {
        intent_id: String,
        client_algo_id: CommandId,
        #[serde(default)]
        target_client_algo_id: Option<CommandId>,
    },
    ProtectedActive {
        intent_id: String,
        client_algo_id: CommandId,
        #[serde(default)]
        target_client_algo_id: Option<CommandId>,
    },
}

/// Durable bridge between a one-shot gateway outcome and the strictly newer private-worker
/// proof that may change writer state. Restart never guesses whether an IOC filled or whether a
/// stop is protected: it resumes the persisted outcome phase until reconciliation succeeds.
pub struct ScalpingLiveSettlement {
    binding: StrategyBinding,
    artifacts_root: PathBuf,
    store: ProjectionStore,
    checkpoint: ScalpingLiveSettlementCheckpoint,
}

/// Mutation owner for an already protected strategy scope. It has no entry method: every call
/// first renews the protection-only predecessor and then takes the corresponding guard.
pub struct ScalpingProtectedGateway {
    binding: StrategyBinding,
    instrument: Instrument,
    private: PrivateRest,
    public: PublicRest,
    authority: WriterLeaseAuthority,
    writer: WriterSession,
    commands: CommandJournal,
}

/// Consumes the current strategy writer using only a newer private-worker readiness/projection
/// pair. It does not connect to Binance or create an entry. The caller must fence and refresh
/// the private worker after a gateway outcome before calling this function.
pub fn reconcile_scalping_writer(
    artifacts_root: PathBuf,
    binding: &StrategyBinding,
    readiness: &PrivateFactsReadiness,
    projections: PrivateFactsProjectionInput,
) -> Result<ScalpingWriterReconciliation, ScalpingLiveGatewayError> {
    validate_reconciliation_input(&artifacts_root, binding, readiness, projections)?;
    let authority =
        WriterLeaseAuthority::open(artifacts_root.join("writer.json"), writer_scope(binding))?;
    let writer = authority
        .active_session()?
        .ok_or(ScalpingLiveGatewayError::NoWriter)?;
    if readiness.generation <= writer.readback_generation {
        return Err(ScalpingLiveGatewayError::ReconciliationGeneration);
    }
    let summary = reconciliation_summary(readiness, projections);
    if readiness.exposure == PrivateExposure::Flat
        && !readiness.ordinary_order_debt
        && !readiness.algo_order_debt
        && projections.execution.value == ExecutionProjection::Known
        && projections.owner.value == OwnerProjection::Clear
        && projections.protection.value == ProtectionProjection::Complete
    {
        authority.retire_flat(&FlatReceipt {
            receipt_id: command_id("flat", &readiness.root_cause_fact_id)?
                .as_str()
                .to_owned(),
            predecessor: writer,
            scope: writer_scope(binding),
            readback_generation: readiness.generation,
            summary_sha256: summary,
        })?;
        return Ok(ScalpingWriterReconciliation::RetiredFlat);
    }
    if readiness.exposure == PrivateExposure::Open
        && projections.execution.value == ExecutionProjection::Known
        && projections.owner.value == OwnerProjection::Clear
        && projections.protection.value == ProtectionProjection::Complete
    {
        authority.retain_protected_predecessor(&ProtectedReceipt {
            predecessor: writer,
            scope: writer_scope(binding),
            readback_generation: readiness.generation,
            summary_sha256: summary,
        })?;
        return Ok(ScalpingWriterReconciliation::ProtectionOnly);
    }
    // A server-side exit can flatten the leg before the first post-entry reconciliation. Exact
    // owned Algo debt must remain cancellable, so demote the predecessor to protection-only;
    // the semantic episode is then confirmed and the normal flat-exit cursor removes the sibling.
    if readiness.exposure == PrivateExposure::Flat
        && !readiness.ordinary_order_debt
        && readiness.algo_order_debt
        && projections.execution.value == ExecutionProjection::Known
        && projections.owner.value == OwnerProjection::Clear
        && projections.protection.value == ProtectionProjection::Gap
    {
        authority.retain_protected_predecessor(&ProtectedReceipt {
            predecessor: writer,
            scope: writer_scope(binding),
            readback_generation: readiness.generation,
            summary_sha256: summary,
        })?;
        return Ok(ScalpingWriterReconciliation::ProtectionOnly);
    }
    Err(ScalpingLiveGatewayError::ReconciliationState)
}

impl ScalpingLiveSettlement {
    /// Opens the durable hand-off record. It is intentionally separate from strategy state: the
    /// record proves only which semantic transition becomes eligible after private reconciliation.
    pub fn open(
        artifacts_root: PathBuf,
        binding: StrategyBinding,
    ) -> Result<Self, ScalpingLiveGatewayError> {
        if !artifacts_root.is_absolute() || binding.validate().is_err() {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        std::fs::create_dir_all(&artifacts_root).map_err(|source| {
            ScalpingLiveGatewayError::Io {
                path: artifacts_root.clone(),
                source,
            }
        })?;
        let store = ProjectionStore::new(artifacts_root.join("scalping_live_settlement.json"));
        let expected_digest = binding.digest();
        let checkpoint = match store.load::<ScalpingLiveSettlementCheckpoint>()? {
            Some(checkpoint)
                if checkpoint.schema_version == LIVE_SETTLEMENT_SCHEMA_VERSION
                    && checkpoint.binding_digest == expected_digest =>
            {
                checkpoint
            }
            Some(_) => return Err(ScalpingLiveGatewayError::Settlement),
            None => ScalpingLiveSettlementCheckpoint {
                schema_version: LIVE_SETTLEMENT_SCHEMA_VERSION,
                binding_digest: expected_digest,
                phase: ScalpingLiveSettlementPhase::Idle,
            },
        };
        Ok(Self {
            binding,
            artifacts_root,
            store,
            checkpoint,
        })
    }

    /// Persists the semantic/physical hand-off before a gateway can create its writer or submit
    /// an IOC. A crash before the gateway outcome is therefore resolved by later private facts,
    /// never by retrying an entry merely because the process restarted.
    pub fn begin_entry(
        &mut self,
        intent_id: &str,
        idempotency_seed: &str,
    ) -> Result<(), ScalpingLiveGatewayError> {
        if intent_id.trim().is_empty() || idempotency_seed.trim().is_empty() {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        let digest = sha256_hex(idempotency_seed.as_bytes());
        let client_algo_id = CommandId::new(format!("vsa_{}", &digest[..28]))
            .map_err(|_| ScalpingLiveGatewayError::Settlement)?;
        let target_client_algo_id =
            if self.binding.parameter_release_id == PHASE8_ATR14_PARAMETER_RELEASE_ID {
                Some(
                    CommandId::new(format!("vta_{}", &digest[..28]))
                        .map_err(|_| ScalpingLiveGatewayError::Settlement)?,
                )
            } else {
                None
            };
        let phase = ScalpingLiveSettlementPhase::Submitting {
            intent_id: intent_id.to_owned(),
            client_algo_id,
            target_client_algo_id,
        };
        match &self.checkpoint.phase {
            ScalpingLiveSettlementPhase::Idle => {
                self.checkpoint.phase = phase;
                self.persist()
            }
            current if current == &phase => Ok(()),
            _ => Err(ScalpingLiveGatewayError::Settlement),
        }
    }

    /// Saves the gateway result before the caller fences the private worker. Repeated delivery
    /// of the exact same result is safe; conflicting results are rejected rather than guessed.
    pub fn record_entry_outcome(
        &mut self,
        intent_id: &str,
        outcome: &ScalpingLiveEntryOutcome,
    ) -> Result<(), ScalpingLiveGatewayError> {
        if intent_id.trim().is_empty() {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        let phase = match outcome {
            ScalpingLiveEntryOutcome::NoFill { .. } => {
                ScalpingLiveSettlementPhase::AwaitingNoFill {
                    intent_id: intent_id.to_owned(),
                }
            }
            ScalpingLiveEntryOutcome::Protected {
                protection_client_algo_id,
                ..
            } => ScalpingLiveSettlementPhase::AwaitingProtected {
                intent_id: intent_id.to_owned(),
                client_algo_id: protection_client_algo_id.clone(),
                target_client_algo_id: None,
            },
            ScalpingLiveEntryOutcome::ProtectedWithTarget {
                protection_client_algo_id,
                target_client_algo_id,
                ..
            } => ScalpingLiveSettlementPhase::AwaitingProtected {
                intent_id: intent_id.to_owned(),
                client_algo_id: protection_client_algo_id.clone(),
                target_client_algo_id: Some(target_client_algo_id.clone()),
            },
        };
        match (&self.checkpoint.phase, &phase) {
            (
                ScalpingLiveSettlementPhase::Submitting {
                    intent_id: expected,
                    ..
                },
                ScalpingLiveSettlementPhase::AwaitingNoFill { intent_id },
            ) if expected == intent_id => {
                self.checkpoint.phase = phase;
                self.persist()
            }
            (
                ScalpingLiveSettlementPhase::Submitting {
                    intent_id: expected_intent,
                    client_algo_id: expected_algo,
                    target_client_algo_id: expected_target,
                },
                ScalpingLiveSettlementPhase::AwaitingProtected {
                    intent_id,
                    client_algo_id,
                    target_client_algo_id,
                },
            ) if expected_intent == intent_id
                && expected_algo == client_algo_id
                && expected_target == target_client_algo_id =>
            {
                self.checkpoint.phase = phase;
                self.persist()
            }
            current if current.0 == current.1 => Ok(()),
            _ => Err(ScalpingLiveGatewayError::Settlement),
        }
    }

    /// Turns a durable private-worker proof into exactly one semantic action. The action stays
    /// durable until `acknowledge` succeeds, so a restart cannot lose the needed strategy update.
    pub fn reconcile(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<Option<ScalpingLiveSettlementAction>, ScalpingLiveGatewayError> {
        match &self.checkpoint.phase {
            ScalpingLiveSettlementPhase::Idle => return Ok(None),
            ScalpingLiveSettlementPhase::ProtectedActive { .. } => return Ok(None),
            ScalpingLiveSettlementPhase::ReadyNoFill { intent_id } => {
                return Ok(Some(ScalpingLiveSettlementAction::RejectNoFill {
                    intent_id: intent_id.clone(),
                }));
            }
            ScalpingLiveSettlementPhase::ReadyProtected {
                intent_id,
                client_algo_id,
                target_client_algo_id,
            } => {
                return Ok(Some(match target_client_algo_id {
                    Some(target_client_algo_id) => {
                        ScalpingLiveSettlementAction::ConfirmProtectedWithTarget {
                            intent_id: intent_id.clone(),
                            client_algo_id: client_algo_id.clone(),
                            target_client_algo_id: target_client_algo_id.clone(),
                        }
                    }
                    None => ScalpingLiveSettlementAction::ConfirmProtected {
                        intent_id: intent_id.clone(),
                        client_algo_id: client_algo_id.clone(),
                    },
                }));
            }
            ScalpingLiveSettlementPhase::Submitting { .. }
            | ScalpingLiveSettlementPhase::AwaitingNoFill { .. }
            | ScalpingLiveSettlementPhase::AwaitingProtected { .. } => {}
        }
        let reconciliation = reconcile_scalping_writer(
            self.artifacts_root.clone(),
            &self.binding,
            readiness,
            projections,
        )
        .or_else(|error| {
            if matches!(error, ScalpingLiveGatewayError::NoWriter)
                && complete_flat(readiness, projections)
            {
                Ok(ScalpingWriterReconciliation::RetiredFlat)
            } else {
                Err(error)
            }
        })?;
        self.checkpoint.phase = match self.checkpoint.phase.clone() {
            ScalpingLiveSettlementPhase::Submitting {
                intent_id,
                client_algo_id: _,
                target_client_algo_id: _,
            } if reconciliation == ScalpingWriterReconciliation::RetiredFlat => {
                ScalpingLiveSettlementPhase::ReadyNoFill { intent_id }
            }
            ScalpingLiveSettlementPhase::Submitting {
                intent_id,
                client_algo_id,
                target_client_algo_id,
            } if reconciliation == ScalpingWriterReconciliation::ProtectionOnly => {
                ScalpingLiveSettlementPhase::ReadyProtected {
                    intent_id,
                    client_algo_id,
                    target_client_algo_id,
                }
            }
            ScalpingLiveSettlementPhase::AwaitingNoFill { intent_id }
                if reconciliation == ScalpingWriterReconciliation::RetiredFlat =>
            {
                ScalpingLiveSettlementPhase::ReadyNoFill { intent_id }
            }
            ScalpingLiveSettlementPhase::AwaitingProtected {
                intent_id,
                client_algo_id,
                target_client_algo_id,
            } if reconciliation == ScalpingWriterReconciliation::ProtectionOnly => {
                ScalpingLiveSettlementPhase::ReadyProtected {
                    intent_id,
                    client_algo_id,
                    target_client_algo_id,
                }
            }
            _ => return Err(ScalpingLiveGatewayError::Settlement),
        };
        self.persist()?;
        self.reconcile(readiness, projections)
    }

    /// Acknowledges an action only when it is the currently persisted action, then returns to an
    /// empty checkpoint. This prevents a stale host from clearing another attempt's transition.
    pub fn acknowledge(
        &mut self,
        action: &ScalpingLiveSettlementAction,
    ) -> Result<(), ScalpingLiveGatewayError> {
        let matches = match (&self.checkpoint.phase, action) {
            (
                ScalpingLiveSettlementPhase::ReadyNoFill {
                    intent_id: expected,
                },
                ScalpingLiveSettlementAction::RejectNoFill { intent_id },
            ) => expected == intent_id,
            (
                ScalpingLiveSettlementPhase::ReadyProtected {
                    intent_id: expected_intent,
                    client_algo_id: expected_algo,
                    target_client_algo_id: None,
                },
                ScalpingLiveSettlementAction::ConfirmProtected {
                    intent_id,
                    client_algo_id,
                },
            ) => expected_intent == intent_id && expected_algo == client_algo_id,
            (
                ScalpingLiveSettlementPhase::ReadyProtected {
                    intent_id: expected_intent,
                    client_algo_id: expected_algo,
                    target_client_algo_id: Some(expected_target),
                },
                ScalpingLiveSettlementAction::ConfirmProtectedWithTarget {
                    intent_id,
                    client_algo_id,
                    target_client_algo_id,
                },
            ) => {
                expected_intent == intent_id
                    && expected_algo == client_algo_id
                    && expected_target == target_client_algo_id
            }
            _ => false,
        };
        if !matches {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = match action {
            ScalpingLiveSettlementAction::RejectNoFill { .. } => ScalpingLiveSettlementPhase::Idle,
            ScalpingLiveSettlementAction::ConfirmProtected {
                intent_id,
                client_algo_id,
            } => ScalpingLiveSettlementPhase::ProtectedActive {
                intent_id: intent_id.clone(),
                client_algo_id: client_algo_id.clone(),
                target_client_algo_id: None,
            },
            ScalpingLiveSettlementAction::ConfirmProtectedWithTarget {
                intent_id,
                client_algo_id,
                target_client_algo_id,
            } => ScalpingLiveSettlementPhase::ProtectedActive {
                intent_id: intent_id.clone(),
                client_algo_id: client_algo_id.clone(),
                target_client_algo_id: Some(target_client_algo_id.clone()),
            },
        };
        self.persist()
    }

    /// Returns the one durable protection identity for a semantically confirmed active episode.
    /// It deliberately survives the confirmation acknowledgement, so exit recovery never has to
    /// infer an Algo client ID from a strategy's semantic state.
    #[must_use]
    pub fn active_protection_client_algo_id(&self) -> Option<&CommandId> {
        match &self.checkpoint.phase {
            ScalpingLiveSettlementPhase::ProtectedActive { client_algo_id, .. } => {
                Some(client_algo_id)
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn active_target_client_algo_id(&self) -> Option<&CommandId> {
        match &self.checkpoint.phase {
            ScalpingLiveSettlementPhase::ProtectedActive {
                target_client_algo_id: Some(client_algo_id),
                ..
            } => Some(client_algo_id),
            _ => None,
        }
    }

    #[must_use]
    pub fn ready_protection_ids(&self) -> Option<(CommandId, Option<CommandId>)> {
        match &self.checkpoint.phase {
            ScalpingLiveSettlementPhase::ReadyProtected {
                client_algo_id,
                target_client_algo_id,
                ..
            } => Some((client_algo_id.clone(), target_client_algo_id.clone())),
            _ => None,
        }
    }

    pub fn recover_ready_protected_flat(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<bool, ScalpingLiveGatewayError> {
        if self.ready_protection_ids().is_none()
            || readiness.exposure != PrivateExposure::Flat
            || readiness.algo_order_debt
        {
            return Ok(false);
        }
        if reconcile_scalping_writer(
            self.artifacts_root.clone(),
            &self.binding,
            readiness,
            projections,
        )? != ScalpingWriterReconciliation::RetiredFlat
        {
            return Err(ScalpingLiveGatewayError::ReconciliationState);
        }
        self.checkpoint.phase = ScalpingLiveSettlementPhase::Idle;
        self.persist()?;
        Ok(true)
    }

    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.checkpoint.phase == ScalpingLiveSettlementPhase::Idle
    }

    /// A gateway outcome is durable but cannot be exposed to the semantic host until a newer
    /// private-worker generation has settled it.
    #[must_use]
    pub fn awaits_private_reconciliation(&self) -> bool {
        matches!(
            self.checkpoint.phase,
            ScalpingLiveSettlementPhase::Submitting { .. }
                | ScalpingLiveSettlementPhase::AwaitingNoFill { .. }
                | ScalpingLiveSettlementPhase::AwaitingProtected { .. }
        )
    }

    /// Clears a confirmed protected episode only after the usual newer private-worker proof has
    /// retired its writer as exact flat. A direct REST flat result can never clear this record.
    pub fn reconcile_flat_exit(
        &mut self,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<(), ScalpingLiveGatewayError> {
        if !matches!(
            self.checkpoint.phase,
            ScalpingLiveSettlementPhase::ProtectedActive { .. }
        ) {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        if reconcile_scalping_writer(
            self.artifacts_root.clone(),
            &self.binding,
            readiness,
            projections,
        )? != ScalpingWriterReconciliation::RetiredFlat
        {
            return Err(ScalpingLiveGatewayError::Settlement);
        }
        self.checkpoint.phase = ScalpingLiveSettlementPhase::Idle;
        self.persist()
    }

    /// Applies a ready settlement action through the resident's durable semantic host before
    /// clearing it locally. The source coordinator owns strategy checkpoint persistence; this
    /// gateway record owns only the external mutation/readback hand-off.
    pub fn reconcile_into_resident(
        &mut self,
        sources: &mut super::ScalpingResidentSources,
        readiness: &PrivateFactsReadiness,
        projections: PrivateFactsProjectionInput,
    ) -> Result<Option<ScalpingLiveSettlementAction>, ScalpingLiveGatewayError> {
        let Some(action) = self.reconcile(readiness, projections)? else {
            return Ok(None);
        };
        match &action {
            ScalpingLiveSettlementAction::RejectNoFill { intent_id } => {
                sources.reject_live_entry(intent_id, readiness.observed_at_ms)?;
            }
            ScalpingLiveSettlementAction::ConfirmProtected { intent_id, .. } => {
                sources.confirm_live_entry(intent_id, readiness.observed_at_ms)?;
            }
            ScalpingLiveSettlementAction::ConfirmProtectedWithTarget { intent_id, .. } => {
                sources.confirm_live_entry(intent_id, readiness.observed_at_ms)?;
            }
        }
        self.acknowledge(&action)?;
        Ok(Some(action))
    }

    fn persist(&self) -> Result<(), ScalpingLiveGatewayError> {
        self.store.save(&self.checkpoint)?;
        Ok(())
    }
}

impl ScalpingProtectedGateway {
    /// Reopens only a durable protection-only predecessor. A normal active writer, missing
    /// writer, or invalid account binding is rejected before credentials are used.
    pub fn open(
        artifacts_root: PathBuf,
        binding: StrategyBinding,
        account_binding: BinanceAccountBinding,
        now_ms: u64,
    ) -> Result<Self, ScalpingLiveGatewayError> {
        let config = ScalpingLiveGatewayConfig {
            artifacts_root: artifacts_root.clone(),
            binding: binding.clone(),
            private_generation: 1,
        };
        validate_config(&config, account_binding, now_ms)?;
        let authority =
            WriterLeaseAuthority::open(artifacts_root.join("writer.json"), writer_scope(&binding))?;
        let writer = authority
            .active_session()?
            .ok_or(ScalpingLiveGatewayError::NoWriter)?;
        let writer = authority.renew_protection(&writer, now_ms)?;
        let public = PublicRest::production()?;
        let exchange_info = public.exchange_info()?;
        let instrument = parse_instrument(
            &exchange_info,
            binding.symbol.clone(),
            writer.readback_generation,
        )?;
        let private =
            PrivateRest::production(PrivateCredentials::from_environment()?, account_binding)?;
        let commands = CommandJournal::open(artifacts_root.join("commands.jsonl"))?;
        Ok(Self {
            binding,
            instrument,
            private,
            public,
            authority,
            writer,
            commands,
        })
    }

    /// Reduces exactly the sole currently-open hedge leg using an IOC. A partial fill retains the
    /// old venue stop during the hand-off, but its quantity is no longer an exact custody proof;
    /// callers must obtain a newer private generation and replace it before any further exit.
    pub fn reduce_open_position(
        &mut self,
        now_ms: u64,
    ) -> Result<ExecutionReceipt, ScalpingLiveGatewayError> {
        if self.commands.has_unresolved() {
            return Err(ScalpingLiveGatewayError::UnresolvedCommand);
        }
        self.writer = self.authority.renew_protection(&self.writer, now_ms)?;
        let readback = self.private.readback(&self.binding.symbol)?;
        let position = exact_single_open_leg(&readback.positions, &self.binding)?;
        let (bid, ask) =
            parse_depth_best_prices(&self.public.depth_snapshot(&self.binding.symbol, 5)?)?;
        let price = match position.side {
            PositionSide::Long => bid,
            PositionSide::Short => ask,
            PositionSide::Net => return Err(ScalpingLiveGatewayError::Position),
        };
        let command = OrderCommand {
            time_in_force: Default::default(),
            command_id: command_id(
                "red",
                &format!(
                    "{}:{}:{}",
                    self.writer.token, self.writer.readback_generation, position.side as u8
                ),
            )?,
            client_order_id: command_id(
                "vrs",
                &format!(
                    "{}:{}:{}",
                    self.binding.owner_scope, self.writer.readback_generation, position.side as u8
                ),
            )?,
            owner: reduce_owner(&self.binding),
            side: close_side(position.side)?,
            position_side: position.side,
            quantity: position.quantity,
            limit_price: price,
            reduce_only: true,
        };
        let guard = self
            .authority
            .protection_dispatch_guard(&self.writer, now_ms)?;
        let receipt = submit_strategy_reduce(
            &mut self.commands,
            &self.private,
            command,
            &self.instrument,
            &position,
            StrategyReductionPreflight {
                binding: &self.binding,
                writer: &self.writer,
                now_ms,
                dispatch: &guard,
            },
        );
        drop(guard);
        receipt.map_err(Into::into)
    }

    /// Installs an exact stop beside the prior stop after a newer readback has exposed a partial
    /// reduction. The caller must cancel the old client Algo only after this returns, then fence
    /// the private worker and wait for a newer one-stop custody projection before another exit.
    /// Leaving the previous stop live until the replacement is directly visible avoids an
    /// unprotected cancellation window.
    pub fn install_replacement_stop(
        &mut self,
        previous_client_algo_id: &CommandId,
        hard_stop_distance_bps: Decimal,
        now_ms: u64,
    ) -> Result<CommandId, ScalpingLiveGatewayError> {
        if self.commands.has_unresolved() {
            return Err(ScalpingLiveGatewayError::UnresolvedCommand);
        }
        let previous = self
            .commands
            .stop_full_by_client_id(previous_client_algo_id)
            .cloned()
            .ok_or(ScalpingLiveGatewayError::ProtectionCommand)?;
        if previous.owner != protection_owner(&self.binding) {
            return Err(ScalpingLiveGatewayError::ProtectionCommand);
        }
        self.writer = self.authority.renew_protection(&self.writer, now_ms)?;
        let readback = self.private.readback(&self.binding.symbol)?;
        let position = exact_single_open_leg(&readback.positions, &self.binding)?;
        if position.side != previous.position_side || position.quantity >= previous.quantity {
            return Err(ScalpingLiveGatewayError::ProtectionCommand);
        }
        let open_algos = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&open_algos, &self.binding.symbol)?;
        if !algos
            .iter()
            .any(|algo| algo.client_algo_id == previous_client_algo_id.as_str())
        {
            return Err(ScalpingLiveGatewayError::ProtectionNotVisible);
        }
        let (best_bid, best_ask) =
            parse_depth_best_prices(&self.public.depth_snapshot(&self.binding.symbol, 5)?)?;
        let replacement = StopMarketFullPositionCommand {
            command_id: command_id(
                "rst",
                &format!(
                    "{}:{}:{}",
                    previous_client_algo_id.as_str(),
                    self.writer.readback_generation,
                    position.quantity
                ),
            )?,
            client_algo_id: command_id(
                "vsp",
                &format!(
                    "{}:{}:{}",
                    self.binding.owner_scope, self.writer.readback_generation, position.quantity
                ),
            )?,
            owner: protection_owner(&self.binding),
            side: close_side(position.side)?,
            position_side: position.side,
            quantity: position.quantity,
            trigger_price: stop_price(
                position.side,
                best_bid,
                best_ask,
                hard_stop_distance_bps,
                self.instrument.price_tick,
            )?,
            position_generation: self.writer.readback_generation,
        };
        let guard = self
            .authority
            .protection_dispatch_guard(&self.writer, now_ms)?;
        let receipt = submit_strategy_stop_market_full_position(
            &mut self.commands,
            &self.private,
            replacement.clone(),
            StrategyProtectionPreflight {
                binding: &self.binding,
                writer: &self.writer,
                now_ms,
                dispatch: &guard,
                protection: ProtectionPreflight {
                    instrument: &self.instrument,
                    position: &position,
                    private_generation: self.writer.readback_generation,
                    position_generation: self.writer.readback_generation,
                    account_can_trade: readback.capabilities.can_trade,
                    hedge_position: readback.capabilities.hedge_position,
                    mark_price_fresh: true,
                },
            },
        );
        drop(guard);
        match receipt? {
            ExecutionReceipt::ProtectedAlgo { .. } | ExecutionReceipt::AlreadyResolved { .. } => {}
            _ => return Err(ScalpingLiveGatewayError::Receipt),
        }
        let after = self.private.readback(&self.binding.symbol)?;
        let after_algos_payload = self.private.open_algo_orders(&self.binding.symbol)?;
        let after_algos =
            binance_private::parse_open_algo_orders(&after_algos_payload, &self.binding.symbol)?;
        let visible = after_algos
            .iter()
            .find(|algo| algo.client_algo_id == replacement.client_algo_id.as_str())
            .ok_or(ScalpingLiveGatewayError::ProtectionNotVisible)?;
        let fresh_position = exact_filled_leg(&after.positions, position.side)?;
        let valid_until_ms = now_ms
            .checked_add(CUSTODY_TTL_MS)
            .ok_or(ScalpingLiveGatewayError::Clock)?;
        prove_algo_protection_custody(AlgoProtectionCustodyInput {
            command: &replacement,
            position: &fresh_position,
            algo: visible,
            writer: &self.writer,
            evidence: ProtectionEvidence {
                private_generation: self.writer.readback_generation,
                readback_generation: self.writer.readback_generation,
                valid_until_ms,
                observed_at_ms: now_ms,
            },
            writer_role: crate::execution::CustodyWriterRole {
                predecessor_protected: true,
                protection_only: true,
            },
            now_ms,
        })?;
        Ok(replacement.client_algo_id)
    }

    /// Cancels the superseded stop only while a directly visible replacement protects the same
    /// currently-open Hedge leg. It deliberately leaves cancellation UNKNOWN until a new private
    /// worker generation proves the old Algo absent; a caller must not reduce again before that.
    pub fn cancel_replaced_stop(
        &mut self,
        previous_client_algo_id: &CommandId,
        replacement_client_algo_id: &CommandId,
        now_ms: u64,
    ) -> Result<ExecutionReceipt, ScalpingLiveGatewayError> {
        if self
            .commands
            .has_unresolved_cancel_for(previous_client_algo_id)
        {
            return Err(ScalpingLiveGatewayError::UnresolvedCommand);
        }
        let previous = self
            .commands
            .stop_full_by_client_id(previous_client_algo_id)
            .cloned()
            .ok_or(ScalpingLiveGatewayError::ProtectionCommand)?;
        let replacement = self
            .commands
            .stop_full_by_client_id(replacement_client_algo_id)
            .cloned()
            .ok_or(ScalpingLiveGatewayError::ProtectionCommand)?;
        if previous.owner != protection_owner(&self.binding)
            || replacement.owner != previous.owner
            || replacement.position_side != previous.position_side
        {
            return Err(ScalpingLiveGatewayError::ProtectionCommand);
        }
        let readback = self.private.readback(&self.binding.symbol)?;
        let position = exact_single_open_leg(&readback.positions, &self.binding)?;
        if position.side != replacement.position_side || position.quantity != replacement.quantity {
            return Err(ScalpingLiveGatewayError::PrivateState);
        }
        let open_algos = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&open_algos, &self.binding.symbol)?;
        let both_visible = [previous_client_algo_id, replacement_client_algo_id]
            .into_iter()
            .all(|client_id| {
                algos
                    .iter()
                    .any(|algo| algo.client_algo_id == client_id.as_str())
            });
        if !both_visible {
            return Err(ScalpingLiveGatewayError::ProtectionNotVisible);
        }
        self.writer = self.authority.renew_protection(&self.writer, now_ms)?;
        let guard = self
            .authority
            .protection_dispatch_guard(&self.writer, now_ms)?;
        let receipt = crate::execution::submit_cancel(
            &mut self.commands,
            &self.private,
            CancelCommand {
                command_id: command_id(
                    "rsc",
                    &format!(
                        "{}:{}:{}",
                        previous_client_algo_id.as_str(),
                        replacement_client_algo_id.as_str(),
                        self.writer.readback_generation
                    ),
                )?,
                owner: previous.owner,
                target_client_order_id: previous_client_algo_id.clone(),
            },
        );
        drop(guard);
        match receipt? {
            ExecutionReceipt::CancelAlgoPendingReadback
            | ExecutionReceipt::AlreadyResolved { .. } => {
                Ok(ExecutionReceipt::CancelAlgoPendingReadback)
            }
            _ => Err(ScalpingLiveGatewayError::Receipt),
        }
    }

    /// Cancels the exact phase-8 profit target before a strategy-driven reduction while the
    /// independently identified stop remains venue-visible for the full current Hedge leg.
    pub fn cancel_target_before_exit(
        &mut self,
        protection_client_algo_id: &CommandId,
        target_client_algo_id: &CommandId,
        now_ms: u64,
    ) -> Result<ExecutionReceipt, ScalpingLiveGatewayError> {
        if self
            .commands
            .has_unresolved_cancel_for(target_client_algo_id)
        {
            return Err(ScalpingLiveGatewayError::UnresolvedCommand);
        }
        let protection = self
            .commands
            .stop_full_by_client_id(protection_client_algo_id)
            .cloned()
            .ok_or(ScalpingLiveGatewayError::ProtectionCommand)?;
        let target = self
            .commands
            .stop_full_by_client_id(target_client_algo_id)
            .cloned()
            .ok_or(ScalpingLiveGatewayError::ProtectionCommand)?;
        if protection.owner != protection_owner(&self.binding)
            || target.owner != take_profit_owner(&self.binding)
            || target.position_side != protection.position_side
            || target.quantity != protection.quantity
        {
            return Err(ScalpingLiveGatewayError::ProtectionCommand);
        }
        let readback = self.private.readback(&self.binding.symbol)?;
        let position = exact_single_open_leg(&readback.positions, &self.binding)?;
        if position.side != protection.position_side || position.quantity != protection.quantity {
            return Err(ScalpingLiveGatewayError::PrivateState);
        }
        let open_algos = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&open_algos, &self.binding.symbol)?;
        if ![protection_client_algo_id, target_client_algo_id]
            .into_iter()
            .all(|client_id| {
                algos
                    .iter()
                    .any(|algo| algo.client_algo_id == client_id.as_str())
            })
        {
            return Err(ScalpingLiveGatewayError::ProtectionNotVisible);
        }
        self.writer = self.authority.renew_protection(&self.writer, now_ms)?;
        let guard = self
            .authority
            .protection_dispatch_guard(&self.writer, now_ms)?;
        let receipt = crate::execution::submit_cancel(
            &mut self.commands,
            &self.private,
            CancelCommand {
                command_id: command_id(
                    "tpc",
                    &format!(
                        "{}:{}",
                        target_client_algo_id.as_str(),
                        self.writer.readback_generation
                    ),
                )?,
                owner: target.owner,
                target_client_order_id: target_client_algo_id.clone(),
            },
        );
        drop(guard);
        match receipt? {
            ExecutionReceipt::CancelAlgoPendingReadback
            | ExecutionReceipt::AlreadyResolved { .. } => {
                Ok(ExecutionReceipt::CancelAlgoPendingReadback)
            }
            _ => Err(ScalpingLiveGatewayError::Receipt),
        }
    }

    /// Selects and cancels one of the exact phase-8 exits still visible after the position became
    /// flat. Unknown Algo debt is rejected; a second known exit is returned for the next fenced
    /// private generation.
    pub fn cancel_one_known_algo_after_flat(
        &mut self,
        protection_client_algo_id: &CommandId,
        target_client_algo_id: Option<&CommandId>,
        now_ms: u64,
    ) -> Result<(CommandId, Option<CommandId>), ScalpingLiveGatewayError> {
        let readback = self.private.readback(&self.binding.symbol)?;
        if !positions_are_flat(&readback.positions, &self.binding) || !readback.orders.is_empty() {
            return Err(ScalpingLiveGatewayError::PrivateState);
        }
        let payload = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&payload, &self.binding.symbol)?;
        let known = [Some(protection_client_algo_id), target_client_algo_id]
            .into_iter()
            .flatten()
            .filter(|id| algos.iter().any(|algo| algo.client_algo_id == id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if known.is_empty()
            || algos
                .iter()
                .any(|algo| !known.iter().any(|id| id.as_str() == algo.client_algo_id))
        {
            return Err(ScalpingLiveGatewayError::ProtectionCommand);
        }
        let cancelled = known[0].clone();
        let remaining = known.get(1).cloned();
        self.cancel_known_algo_after_flat(&cancelled, now_ms)?;
        Ok((cancelled, remaining))
    }

    /// Cancels exactly one known full-position Algo exit only after direct readback proves both
    /// hedge legs flat. The cancellation remains UNKNOWN until the private worker contributes a
    /// newer durable no-Algo generation, which is then consumed by `reconcile_scalping_writer`.
    pub fn cancel_known_algo_after_flat(
        &mut self,
        client_algo_id: &CommandId,
        now_ms: u64,
    ) -> Result<ExecutionReceipt, ScalpingLiveGatewayError> {
        if self.commands.has_unresolved_cancel_for(client_algo_id) {
            return Err(ScalpingLiveGatewayError::UnresolvedCommand);
        }
        let stop = self
            .commands
            .stop_full_by_client_id(client_algo_id)
            .cloned()
            .ok_or(ScalpingLiveGatewayError::ProtectionCommand)?;
        if stop.owner != protection_owner(&self.binding)
            && stop.owner != take_profit_owner(&self.binding)
        {
            return Err(ScalpingLiveGatewayError::ProtectionCommand);
        }
        let readback = self.private.readback(&self.binding.symbol)?;
        if !positions_are_flat(&readback.positions, &self.binding) || !readback.orders.is_empty() {
            return Err(ScalpingLiveGatewayError::PrivateState);
        }
        self.writer = self.authority.renew_protection(&self.writer, now_ms)?;
        let guard = self
            .authority
            .protection_dispatch_guard(&self.writer, now_ms)?;
        let receipt = crate::execution::submit_cancel(
            &mut self.commands,
            &self.private,
            CancelCommand {
                command_id: command_id(
                    "stc",
                    &format!(
                        "{}:{}",
                        client_algo_id.as_str(),
                        self.writer.readback_generation
                    ),
                )?,
                owner: stop.owner,
                target_client_order_id: client_algo_id.clone(),
            },
        );
        drop(guard);
        match receipt? {
            ExecutionReceipt::CancelAlgoPendingReadback
            | ExecutionReceipt::AlreadyResolved { .. } => {
                Ok(ExecutionReceipt::CancelAlgoPendingReadback)
            }
            _ => Err(ScalpingLiveGatewayError::Receipt),
        }
    }
}

/// A one-owner Binance PAPI UM execution gateway. A successful filled entry is not reported
/// until its exact Hedge leg has a venue-visible, custody-validated Algo stop.
pub struct ScalpingLiveGateway {
    binding: StrategyBinding,
    instrument: Instrument,
    private: PrivateRest,
    public: PublicRest,
    authority: WriterLeaseAuthority,
    writer: WriterSession,
    commands: CommandJournal,
    entry_closed: bool,
}

impl ScalpingLiveGateway {
    pub fn open(
        config: ScalpingLiveGatewayConfig,
        account_binding: BinanceAccountBinding,
        now_ms: u64,
    ) -> Result<Self, ScalpingLiveGatewayError> {
        validate_config(&config, account_binding, now_ms)?;
        std::fs::create_dir_all(&config.artifacts_root).map_err(|source| {
            ScalpingLiveGatewayError::Io {
                path: config.artifacts_root.clone(),
                source,
            }
        })?;
        let public = PublicRest::production()?;
        let exchange_info = public.exchange_info()?;
        let instrument = parse_instrument(
            &exchange_info,
            config.binding.symbol.clone(),
            config.private_generation,
        )?;
        let credentials = PrivateCredentials::from_environment()?;
        let private = PrivateRest::production(credentials, account_binding)?;
        let scope = writer_scope(&config.binding);
        let authority =
            WriterLeaseAuthority::open(config.artifacts_root.join("writer.json"), scope)?;
        let writer = authority.register_initial(now_ms, config.private_generation)?;
        let commands = CommandJournal::open(config.artifacts_root.join("commands.jsonl"))?;
        Ok(Self {
            binding: config.binding,
            instrument,
            private,
            public,
            authority,
            writer,
            commands,
            entry_closed: false,
        })
    }

    /// Submits one semantic marketable entry. The entry can only escape after a direct signed
    /// flat readback; an IOC no-fill retires this writer, while a fill must install and verify a
    /// full-position exchange stop before the writer is switched to protection-only.
    pub fn submit_intent(
        &mut self,
        intent: &SemanticIntent,
        now_ms: u64,
    ) -> Result<ScalpingLiveEntryOutcome, ScalpingLiveGatewayError> {
        if self.entry_closed {
            return Err(ScalpingLiveGatewayError::EntryClosed);
        }
        validate_intent(intent, &self.binding, now_ms)?;
        let readback = self.private.readback(&self.binding.symbol)?;
        let open_algos = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&open_algos, &self.binding.symbol)?;
        let account = validate_flat_entry_readback(&readback, &algos, &self.binding)?;
        if self.commands.has_unresolved() {
            return Err(ScalpingLiveGatewayError::UnresolvedCommand);
        }
        self.writer = self.authority.renew(&self.writer, now_ms)?;
        let (best_bid, best_ask) =
            parse_depth_best_prices(&self.public.depth_snapshot(&self.binding.symbol, 5)?)?;
        let price = entry_price(intent, best_bid, best_ask, self.instrument.price_tick)?;
        let quantity = quantity_for_intent(intent, &self.instrument, price)?;
        let rounded_entry_notional = quantity * price.value();
        let entry = OrderCommand {
            time_in_force: Default::default(),
            command_id: command_id("ent", &intent.intent_id)?,
            client_order_id: command_id("ve", &intent.idempotency_seed)?,
            owner: entry_owner(&self.binding),
            side: entry_side(intent.direction),
            position_side: position_side(intent.direction),
            quantity,
            limit_price: price,
            reduce_only: false,
        };
        let guard = self.authority.dispatch_guard(&self.writer, now_ms)?;
        let receipt = submit_strategy_limit_entry(
            &mut self.commands,
            &self.private,
            entry.clone(),
            StrategyEntryPreflight {
                intent,
                instrument: &self.instrument,
                account: &account,
                limits: &crate::risk::HardRiskLimits {
                    max_entry_notional: Amount::new(
                        "USDT"
                            .parse()
                            .map_err(|_| ScalpingLiveGatewayError::Config)?,
                        rounded_entry_notional,
                    ),
                },
                writer: &self.writer,
                now_ms,
                dispatch: &guard,
            },
        );
        drop(guard);
        let _order = accepted_order(receipt)?;
        let after_entry = if intent.entry_style == EntryStyle::PassiveMaker {
            let remaining_ms = intent.valid_until_ms.saturating_sub(now_ms);
            std::thread::sleep(std::time::Duration::from_millis(
                intent.entry_ttl_ms.min(remaining_ms),
            ));
            let observed = self.private.readback(&self.binding.symbol)?;
            if observed.orders.iter().any(|current| {
                current.client_order_id
                    == crate::domain::FieldState::Known(entry.client_order_id.as_str().to_owned())
            }) {
                let guard = self.authority.dispatch_guard(&self.writer, now_ms)?;
                let cancel = submit_cancel(
                    &mut self.commands,
                    &self.private,
                    CancelCommand {
                        command_id: command_id("ecn", &intent.intent_id)?,
                        owner: entry.owner.clone(),
                        target_client_order_id: entry.client_order_id.clone(),
                    },
                );
                drop(guard);
                match cancel? {
                    ExecutionReceipt::Cancelled { .. }
                    | ExecutionReceipt::CancelNotApplied { .. }
                    | ExecutionReceipt::AlreadyResolved { .. } => {}
                    _ => return Err(ScalpingLiveGatewayError::Receipt),
                }
                self.private.readback(&self.binding.symbol)?
            } else {
                observed
            }
        } else {
            self.private.readback(&self.binding.symbol)?
        };
        let open_algos = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&open_algos, &self.binding.symbol)?;
        if positions_are_flat(&after_entry.positions, &self.binding) {
            validate_flat_entry_readback(&after_entry, &algos, &self.binding)?;
            // A direct REST readback is not a durable private-worker generation.  Do not invent
            // one merely to retire the lease: retain the writer lock until the caller hands this
            // transition to the durable reconciliation path.
            self.entry_closed = true;
            return Ok(ScalpingLiveEntryOutcome::NoFill {
                command_id: entry.command_id,
            });
        }
        let position = exact_filled_leg(&after_entry.positions, position_side(intent.direction))?;
        let protection = self.install_protection(intent, &position, &after_entry, now_ms)?;
        if self.binding.parameter_release_id == PHASE8_ATR14_PARAMETER_RELEASE_ID {
            let target = self.install_take_profit(intent, &position, &after_entry, now_ms)?;
            self.entry_closed = true;
            return Ok(ScalpingLiveEntryOutcome::ProtectedWithTarget {
                command_id: entry.command_id,
                position_side: position.side,
                quantity: position.quantity,
                protection_strategy_id: protection,
                protection_client_algo_id: command_id("vsa", &intent.idempotency_seed)?,
                target_strategy_id: target,
                target_client_algo_id: command_id("vta", &intent.idempotency_seed)?,
            });
        }
        self.entry_closed = true;
        Ok(ScalpingLiveEntryOutcome::Protected {
            command_id: entry.command_id,
            position_side: position.side,
            quantity: position.quantity,
            protection_strategy_id: protection,
            protection_client_algo_id: command_id("vsa", &intent.idempotency_seed)?,
        })
    }

    fn install_protection(
        &mut self,
        intent: &SemanticIntent,
        position: &Position,
        readback: &binance_private::PrivateReadback,
        now_ms: u64,
    ) -> Result<String, ScalpingLiveGatewayError> {
        self.writer = self.authority.renew(&self.writer, now_ms)?;
        let (best_bid, best_ask) =
            parse_depth_best_prices(&self.public.depth_snapshot(&self.binding.symbol, 5)?)?;
        let stop = StopMarketFullPositionCommand {
            command_id: command_id("stp", &intent.intent_id)?,
            client_algo_id: command_id("vsa", &intent.idempotency_seed)?,
            owner: protection_owner(&self.binding),
            side: close_side(position.side)?,
            position_side: position.side,
            quantity: position.quantity,
            trigger_price: stop_price(
                position.side,
                best_bid,
                best_ask,
                intent.hard_stop_distance_bps,
                self.instrument.price_tick,
            )?,
            position_generation: self.writer.readback_generation,
        };
        let guard = self.authority.dispatch_guard(&self.writer, now_ms)?;
        let receipt = submit_stop_market_full_position(
            &mut self.commands,
            &self.private,
            stop.clone(),
            ProtectionPreflight {
                instrument: &self.instrument,
                position,
                private_generation: self.writer.readback_generation,
                position_generation: self.writer.readback_generation,
                account_can_trade: readback.capabilities.can_trade,
                hedge_position: readback.capabilities.hedge_position,
                // The stop trigger is derived from the just-read top-of-book and submitted in
                // this guarded call; a delayed or malformed book never reaches this point.
                mark_price_fresh: true,
            },
        );
        drop(guard);
        let strategy_id = protected_strategy_id(receipt)?;
        let after = self.private.readback(&self.binding.symbol)?;
        let algos_payload = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&algos_payload, &self.binding.symbol)?;
        let visible = algos
            .iter()
            .find(|algo| algo.client_algo_id == stop.client_algo_id.as_str())
            .ok_or(ScalpingLiveGatewayError::ProtectionNotVisible)?;
        let fresh_position = exact_filled_leg(&after.positions, position.side)?;
        let valid_until_ms = now_ms
            .checked_add(CUSTODY_TTL_MS)
            .ok_or(ScalpingLiveGatewayError::Clock)?;
        let custody = prove_algo_protection_custody(AlgoProtectionCustodyInput {
            command: &stop,
            position: &fresh_position,
            algo: visible,
            writer: &self.writer,
            evidence: ProtectionEvidence {
                private_generation: self.writer.readback_generation,
                readback_generation: self.writer.readback_generation,
                valid_until_ms,
                observed_at_ms: now_ms,
            },
            writer_role: crate::execution::CustodyWriterRole {
                predecessor_protected: false,
                protection_only: false,
            },
            now_ms,
        })?;
        // The custody proof is required before the fill can be returned.  Transitioning this
        // writer to protection-only additionally requires a *durably committed* newer private
        // generation, which is owned by the resident private-facts worker rather than this REST
        // gateway.  The caller must reconcile that transition; this gateway permits no re-entry.
        let _ = custody;
        Ok(strategy_id)
    }

    fn install_take_profit(
        &mut self,
        intent: &SemanticIntent,
        position: &Position,
        readback: &binance_private::PrivateReadback,
        now_ms: u64,
    ) -> Result<String, ScalpingLiveGatewayError> {
        self.writer = self.authority.renew(&self.writer, now_ms)?;
        let entry_price = position
            .entry_price
            .ok_or(ScalpingLiveGatewayError::PrivateState)?;
        let target = StopMarketFullPositionCommand {
            command_id: command_id("tgt", &intent.intent_id)?,
            client_algo_id: command_id("vta", &intent.idempotency_seed)?,
            owner: take_profit_owner(&self.binding),
            side: close_side(position.side)?,
            position_side: position.side,
            quantity: position.quantity,
            trigger_price: target_price(
                position.side,
                entry_price,
                intent.target_distance_bps,
                self.instrument.price_tick,
            )?,
            position_generation: self.writer.readback_generation,
        };
        let guard = self.authority.dispatch_guard(&self.writer, now_ms)?;
        let receipt = submit_strategy_take_profit_market_full_position(
            &mut self.commands,
            &self.private,
            target.clone(),
            StrategyProtectionPreflight {
                binding: &self.binding,
                writer: &self.writer,
                now_ms,
                dispatch: &guard,
                protection: ProtectionPreflight {
                    instrument: &self.instrument,
                    position,
                    private_generation: self.writer.readback_generation,
                    position_generation: self.writer.readback_generation,
                    account_can_trade: readback.capabilities.can_trade,
                    hedge_position: readback.capabilities.hedge_position,
                    mark_price_fresh: true,
                },
            },
        );
        drop(guard);
        let strategy_id = protected_strategy_id(receipt)?;
        let algos_payload = self.private.open_algo_orders(&self.binding.symbol)?;
        let algos = binance_private::parse_open_algo_orders(&algos_payload, &self.binding.symbol)?;
        let visible = algos
            .iter()
            .find(|algo| algo.client_algo_id == target.client_algo_id.as_str())
            .ok_or(ScalpingLiveGatewayError::ProtectionNotVisible)?;
        if visible.order_type != crate::domain::FieldState::Known("TAKE_PROFIT_MARKET".to_owned())
            || visible.side != crate::domain::FieldState::Known(target.side)
            || visible.position_side != crate::domain::FieldState::Known(target.position_side)
            || visible.quantity != crate::domain::FieldState::Known(target.quantity)
            || visible.trigger_price != crate::domain::FieldState::Known(target.trigger_price)
            || visible.working_type != crate::domain::FieldState::Known("MARK_PRICE".to_owned())
            || visible.reduce_only != crate::domain::FieldState::Known(true)
        {
            return Err(ScalpingLiveGatewayError::ProtectionNotVisible);
        }
        Ok(strategy_id)
    }
}

fn validate_config(
    config: &ScalpingLiveGatewayConfig,
    account_binding: BinanceAccountBinding,
    now_ms: u64,
) -> Result<(), ScalpingLiveGatewayError> {
    if !config.artifacts_root.is_absolute()
        || config.private_generation == 0
        || now_ms == 0
        || config.binding.validate().is_err()
        || config.binding.exchange != EXCHANGE
        || !is_canonical_trading_account_id(&config.binding.account)
        || config.binding.risk_budget.asset.as_str() != "USDT"
        || config.binding.risk_budget.value != live_entry_target_usdt()
        || account_binding != BinanceAccountBinding::PortfolioMarginUm
    {
        return Err(ScalpingLiveGatewayError::Config);
    }
    Ok(())
}

fn writer_scope(binding: &StrategyBinding) -> WriterScope {
    WriterScope {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        owner_scope: binding.owner_scope.clone(),
    }
}

fn validate_reconciliation_input(
    artifacts_root: &Path,
    binding: &StrategyBinding,
    readiness: &PrivateFactsReadiness,
    projections: PrivateFactsProjectionInput,
) -> Result<(), ScalpingLiveGatewayError> {
    let identity = (readiness.generation, readiness.observed_at_ms);
    let projections_match = [
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
    .all(|value| value == identity);
    if !artifacts_root.is_absolute()
        || binding.validate().is_err()
        || binding.exchange != EXCHANGE
        || !is_canonical_trading_account_id(&binding.account)
        || readiness.generation == 0
        || readiness.observed_at_ms == 0
        || readiness.root_cause_fact_id.trim().is_empty()
        || !projections_match
    {
        return Err(ScalpingLiveGatewayError::ReconciliationState);
    }
    Ok(())
}

fn complete_flat(
    readiness: &PrivateFactsReadiness,
    projections: PrivateFactsProjectionInput,
) -> bool {
    readiness.exposure == PrivateExposure::Flat
        && !readiness.ordinary_order_debt
        && !readiness.algo_order_debt
        && projections.execution.value == ExecutionProjection::Known
        && projections.owner.value == OwnerProjection::Clear
        && projections.protection.value == ProtectionProjection::Complete
}

fn reconciliation_summary(
    readiness: &PrivateFactsReadiness,
    projections: PrivateFactsProjectionInput,
) -> String {
    sha256_hex(
        format!(
            "{}:{}:{:?}:{:?}:{:?}:{:?}",
            readiness.generation,
            readiness.root_cause_fact_id,
            readiness.exposure,
            projections.execution.value,
            projections.owner.value,
            projections.protection.value,
        )
        .as_bytes(),
    )
}

fn validate_intent(
    intent: &SemanticIntent,
    binding: &StrategyBinding,
    now_ms: u64,
) -> Result<(), ScalpingLiveGatewayError> {
    let entry_style_valid = if binding.parameter_release_id == PHASE8_ATR14_PARAMETER_RELEASE_ID {
        intent.entry_style == EntryStyle::PassiveMaker
    } else {
        intent.entry_style == EntryStyle::MarketableLimit
    };
    if intent.symbol != binding.symbol
        || !entry_style_valid
        || intent.valid_until_ms <= now_ms
        || intent.target_quote.asset.as_str() != "USDT"
        || intent.target_quote != binding.risk_budget
        || intent.target_quote.value != live_entry_target_usdt()
    {
        return Err(ScalpingLiveGatewayError::Intent);
    }
    Ok(())
}

fn validate_flat_entry_readback(
    readback: &binance_private::PrivateReadback,
    algos: &[binance_private::AlgoOrderReadback],
    binding: &StrategyBinding,
) -> Result<AccountRiskView, ScalpingLiveGatewayError> {
    let positions_valid = readback.positions.len() == 2
        && readback.positions.iter().all(|position| {
            position.symbol == binding.symbol
                && matches!(position.side, PositionSide::Long | PositionSide::Short)
                && position.quantity.is_zero()
        });
    if !readback.capabilities.can_trade
        || !readback.capabilities.hedge_position
        || readback.capabilities.one_way_position
        || !positions_valid
        || !readback.orders.is_empty()
        || !algos.is_empty()
    {
        return Err(ScalpingLiveGatewayError::PrivateState);
    }
    let balance = readback
        .balances
        .iter()
        .find(|balance| balance.asset == binding.risk_budget.asset)
        .ok_or(ScalpingLiveGatewayError::PrivateState)?;
    AccountRiskView::from_balance(balance, 0).map_err(|_| ScalpingLiveGatewayError::PrivateState)
}

fn quantity_for_intent(
    intent: &SemanticIntent,
    instrument: &Instrument,
    price: Price,
) -> Result<Decimal, ScalpingLiveGatewayError> {
    let target = live_entry_target_usdt();
    if intent.target_quote.asset.as_str() != "USDT"
        || intent.target_quote.value != target
        || instrument.minimum_notional.asset.as_str() != "USDT"
        || instrument.minimum_notional.value > target
        || instrument.quantity_step <= Decimal::ZERO
    {
        return Err(ScalpingLiveGatewayError::Quantity);
    }
    let quantity = align_up(target / price.value(), instrument.quantity_step);
    let notional = quantity * price.value();
    let previous_quantity = quantity - instrument.quantity_step;
    if quantity <= Decimal::ZERO
        || notional < target
        || notional < instrument.minimum_notional.value
        || (previous_quantity > Decimal::ZERO && previous_quantity * price.value() >= target)
    {
        return Err(ScalpingLiveGatewayError::Quantity);
    }
    Ok(quantity)
}

fn entry_price(
    intent: &SemanticIntent,
    bid: Price,
    ask: Price,
    tick: Price,
) -> Result<Price, ScalpingLiveGatewayError> {
    if intent.entry_style == EntryStyle::MarketableLimit {
        return match intent.direction {
            Direction::Long => Ok(ask),
            Direction::Short => Ok(bid),
        };
    }
    let reference = intent.reference_price.value();
    // `target_distance_bps` is exactly 0.8 × ATR14 for the phase-8 release. Two ticks provide
    // the pair-specific lower bound when normalized ATR is exceptionally small.
    let tick_distance_bps = tick.value() * Decimal::new(20_000, 0) / reference;
    let distance_bps = intent.target_distance_bps.max(tick_distance_bps);
    if distance_bps <= Decimal::ZERO || distance_bps >= Decimal::new(10_000, 0) {
        return Err(ScalpingLiveGatewayError::Intent);
    }
    let ratio = distance_bps / Decimal::new(10_000, 0);
    let value = match intent.direction {
        Direction::Long => {
            let value = align_down(reference * (Decimal::ONE - ratio), tick.value());
            if value >= ask.value() {
                return Err(ScalpingLiveGatewayError::Intent);
            }
            value
        }
        Direction::Short => {
            let value = align_up(reference * (Decimal::ONE + ratio), tick.value());
            if value <= bid.value() {
                return Err(ScalpingLiveGatewayError::Intent);
            }
            value
        }
    };
    Price::new(value).map_err(|_| ScalpingLiveGatewayError::Intent)
}

fn stop_price(
    side: PositionSide,
    bid: Price,
    ask: Price,
    distance_bps: Decimal,
    tick: Price,
) -> Result<Price, ScalpingLiveGatewayError> {
    if distance_bps <= Decimal::ZERO || distance_bps >= Decimal::new(10_000, 0) {
        return Err(ScalpingLiveGatewayError::Intent);
    }
    let ratio = distance_bps / Decimal::new(10_000, 0);
    let value = match side {
        PositionSide::Long => align_down(bid.value() * (Decimal::ONE - ratio), tick.value()),
        PositionSide::Short => align_up(ask.value() * (Decimal::ONE + ratio), tick.value()),
        PositionSide::Net => return Err(ScalpingLiveGatewayError::PrivateState),
    };
    Price::new(value).map_err(|_| ScalpingLiveGatewayError::Intent)
}

fn target_price(
    side: PositionSide,
    entry: Price,
    distance_bps: Decimal,
    tick: Price,
) -> Result<Price, ScalpingLiveGatewayError> {
    if distance_bps <= Decimal::ZERO || distance_bps >= Decimal::new(10_000, 0) {
        return Err(ScalpingLiveGatewayError::Intent);
    }
    let ratio = distance_bps / Decimal::new(10_000, 0);
    let value = match side {
        PositionSide::Long => align_up(entry.value() * (Decimal::ONE + ratio), tick.value()),
        PositionSide::Short => align_down(entry.value() * (Decimal::ONE - ratio), tick.value()),
        PositionSide::Net => return Err(ScalpingLiveGatewayError::PrivateState),
    };
    Price::new(value).map_err(|_| ScalpingLiveGatewayError::Intent)
}

fn entry_owner(binding: &StrategyBinding) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose: OrderPurpose::Entry,
    }
}

fn protection_owner(binding: &StrategyBinding) -> OrderOwner {
    OrderOwner {
        purpose: OrderPurpose::Protection,
        ..entry_owner(binding)
    }
}

fn take_profit_owner(binding: &StrategyBinding) -> OrderOwner {
    OrderOwner {
        purpose: OrderPurpose::TakeProfit,
        ..entry_owner(binding)
    }
}

fn reduce_owner(binding: &StrategyBinding) -> OrderOwner {
    OrderOwner {
        purpose: OrderPurpose::Reduce,
        ..entry_owner(binding)
    }
}

fn position_side(direction: Direction) -> PositionSide {
    match direction {
        Direction::Long => PositionSide::Long,
        Direction::Short => PositionSide::Short,
    }
}

fn entry_side(direction: Direction) -> OrderSide {
    match direction {
        Direction::Long => OrderSide::Buy,
        Direction::Short => OrderSide::Sell,
    }
}

fn close_side(side: PositionSide) -> Result<OrderSide, ScalpingLiveGatewayError> {
    match side {
        PositionSide::Long => Ok(OrderSide::Sell),
        PositionSide::Short => Ok(OrderSide::Buy),
        PositionSide::Net => Err(ScalpingLiveGatewayError::PrivateState),
    }
}

fn exact_filled_leg(
    positions: &[Position],
    side: PositionSide,
) -> Result<Position, ScalpingLiveGatewayError> {
    positions
        .iter()
        .find(|position| position.side == side && position.quantity > Decimal::ZERO)
        .cloned()
        .ok_or(ScalpingLiveGatewayError::Position)
}

fn exact_single_open_leg(
    positions: &[Position],
    binding: &StrategyBinding,
) -> Result<Position, ScalpingLiveGatewayError> {
    if positions.len() != 2
        || positions.iter().any(|position| {
            position.symbol != binding.symbol
                || !matches!(position.side, PositionSide::Long | PositionSide::Short)
                || position.quantity.is_sign_negative()
        })
    {
        return Err(ScalpingLiveGatewayError::Position);
    }
    let mut open = positions
        .iter()
        .filter(|position| position.quantity > Decimal::ZERO);
    let position = open
        .next()
        .cloned()
        .ok_or(ScalpingLiveGatewayError::Position)?;
    if open.next().is_some() {
        return Err(ScalpingLiveGatewayError::Position);
    }
    Ok(position)
}

fn positions_are_flat(positions: &[Position], binding: &StrategyBinding) -> bool {
    positions.len() == 2
        && positions.iter().all(|position| {
            position.symbol == binding.symbol
                && matches!(position.side, PositionSide::Long | PositionSide::Short)
                && position.quantity.is_zero()
        })
}

fn accepted_order(
    receipt: Result<ExecutionReceipt, ExecutionError>,
) -> Result<crate::domain::Order, ScalpingLiveGatewayError> {
    match receipt? {
        ExecutionReceipt::Accepted { order, .. } => Ok(order),
        ExecutionReceipt::AlreadyResolved { .. }
        | ExecutionReceipt::AlreadyRejected { .. }
        | ExecutionReceipt::ProbeAccepted { .. }
        | ExecutionReceipt::Cancelled { .. }
        | ExecutionReceipt::CancelNotApplied { .. }
        | ExecutionReceipt::CancelledConditional { .. }
        | ExecutionReceipt::CancelAlgoPendingReadback
        | ExecutionReceipt::Reduced { .. }
        | ExecutionReceipt::Protected { .. }
        | ExecutionReceipt::ProtectedAlgo { .. } => Err(ScalpingLiveGatewayError::Receipt),
    }
}

fn protected_strategy_id(
    receipt: Result<ExecutionReceipt, ExecutionError>,
) -> Result<String, ScalpingLiveGatewayError> {
    match receipt? {
        ExecutionReceipt::ProtectedAlgo { algo_id } => Ok(algo_id),
        _ => Err(ScalpingLiveGatewayError::Receipt),
    }
}

fn command_id(prefix: &str, seed: &str) -> Result<CommandId, ScalpingLiveGatewayError> {
    let digest = sha256_hex(seed.as_bytes());
    CommandId::new(format!("{prefix}_{}", &digest[..28]))
        .map_err(|_| ScalpingLiveGatewayError::Intent)
}

fn align_down(value: Decimal, step: Decimal) -> Decimal {
    value - (value % step)
}

fn align_up(value: Decimal, step: Decimal) -> Decimal {
    let remainder = value % step;
    if remainder.is_zero() {
        value
    } else {
        value + (step - remainder)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingLiveGatewayError {
    #[error("live gateway configuration, binding, clock, or account mode is invalid")]
    Config,
    #[error("strategy intent is unsupported, expired, or is not exactly the 5 USDT live target")]
    Intent,
    #[error("fresh private state is not an empty, trade-enabled Hedge scope")]
    PrivateState,
    #[error(
        "entry quantity cannot satisfy the 5 USDT target with one minimal upward quantity-step alignment"
    )]
    Quantity,
    #[error("an exact filled Hedge leg was not found after the IOC entry")]
    Position,
    #[error("a protection response was accepted but is not yet visible in exact Algo readback")]
    ProtectionNotVisible,
    #[error("the requested protection command is absent or belongs to another strategy scope")]
    ProtectionCommand,
    #[error("gateway received an execution receipt outside its requested transition")]
    Receipt,
    #[error("system clock overflowed while forming custody evidence")]
    Clock,
    #[error("gateway artifact I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Binance public request failed: {0}")]
    Public(#[from] PublicError),
    #[error("Binance instrument payload failed validation: {0}")]
    Instrument(#[from] crate::exchange::binance::BinanceError),
    #[error("Binance private request failed: {0}")]
    Private(#[from] PrivateError),
    #[error("Binance private readback failed: {0}")]
    Readback(#[from] PrivateReadbackError),
    #[error("Binance private payload failed validation: {0}")]
    Parse(#[from] crate::exchange::binance_private::PrivateParseError),
    #[error("writer lease failed: {0}")]
    Writer(#[from] WriterLeaseError),
    #[error("execution journal failed: {0}")]
    Journal(#[from] crate::execution::CommandJournalError),
    #[error("execution boundary failed: {0}")]
    Execution(#[from] ExecutionError),
    #[error("protection custody failed: {0}")]
    Custody(#[from] crate::execution::ProtectionCustodyError),
    #[error("a prior command is unresolved")]
    UnresolvedCommand,
    #[error("this one-shot gateway has already attempted its only entry")]
    EntryClosed,
    #[error("no active scoped writer exists for private-worker reconciliation")]
    NoWriter,
    #[error("the private worker did not produce a generation newer than the active writer")]
    ReconciliationGeneration,
    #[error("private-worker reconciliation is not a complete flat or protected owner state")]
    ReconciliationState,
    #[error("live gateway settlement checkpoint is incompatible or out of order")]
    Settlement,
    #[error("live gateway settlement storage failed: {0}")]
    Storage(#[from] StorageError),
    #[error("live gateway could not persist the resident semantic settlement: {0}")]
    Sources(#[from] super::ScalpingResidentSourcesError),
}

#[cfg(test)]
include!("scalping_live_gateway_tests.rs");
