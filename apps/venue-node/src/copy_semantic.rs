use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use venue_control_protocol::{AccountDeliveryPayload, CopyLifecyclePolicy, CopyRelationRecord};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyExecutionRequest, CopyExecutionResult, CopyExecutionState,
    FollowerDeliveryManifest, TargetExposurePlan, plan_copy_execution,
};
use venue_domain::domain::{
    CommandId, ExecutionCommand, MarketReduceCommand, OrderCommand, OrderOwner, OrderPurpose,
    OrderSide, PositionSide, Price,
};
use venue_runtime::account::AccountRuntimeHost;
use venue_runtime::{
    AccountKey, AccountPhysicalGateway, CommandState, StrategyBinding, StrategyInstanceKey,
    StrategyKind,
};

use crate::ActorDeliveryTurn;

mod reconciliation;
mod signed_position;

#[derive(Debug, Deserialize)]
struct WireCopySemanticJob {
    target: TargetExposurePlan,
    leader_intent: Value,
}

/// Validated, mutation-free Copy work for one exact follower actor.
///
/// Control supplies immutable data only. Runtime must first durably apply it against its
/// recovered Actor/WAL state; only then may Node translate a fresh signed-fact request and ask
/// Runtime to admit the resulting command into the existing account lane.
#[derive(Clone, Debug)]
pub struct CopySemanticDelivery {
    manifest: FollowerDeliveryManifest,
    target: TargetExposurePlan,
    actor: StrategyBinding,
    owner: OrderOwner,
    delivery_digest: [u8; 32],
    durable_inbox_digest: [u8; 32],
    durable_inbox_sequence: u64,
    durable_inbox_root_digest: [u8; 32],
    recovery_only: bool,
}

/// The adapter-owned, fresh-rule portion of a Copy physical command.  Exposure is deliberately
/// not used as an exchange quantity: this fact is produced after the active adapter has applied
/// its current contract multiplier, lot size and position-mode rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshCopyCommandFacts {
    pub normalized_quantity: Decimal,
    pub rules_generation: u64,
    pub price_generation: u64,
    pub observed_at_ms: u64,
    pub fact_digest: [u8; 32],
    pub limit_price: Price,
}

impl CopySemanticDelivery {
    pub fn from_actor_turn(
        turn: &ActorDeliveryTurn,
        now_ms: u64,
    ) -> Result<Self, CopySemanticError> {
        Self::parse_actor_turn(turn, now_ms, false)
    }

    /// Expiry stops new execution, not read-only recovery of an already durable child.
    pub(crate) fn from_recovered_actor_turn(
        turn: &ActorDeliveryTurn,
        now_ms: u64,
    ) -> Result<Self, CopySemanticError> {
        Self::parse_actor_turn(turn, now_ms, true)
    }

    fn parse_actor_turn(
        turn: &ActorDeliveryTurn,
        now_ms: u64,
        recovery_only: bool,
    ) -> Result<Self, CopySemanticError> {
        let AccountDeliveryPayload::CopySemanticJob(job) = turn.payload() else {
            return Err(CopySemanticError::Kind);
        };
        if now_ms == 0
            || now_ms < job.created_at_ms
            || (!recovery_only && now_ms >= job.expires_at_ms)
        {
            return Err(CopySemanticError::Expired);
        }
        let manifest: FollowerDeliveryManifest = serde_json::from_value(job.manifest.clone())
            .map_err(|_| CopySemanticError::Manifest)?;
        manifest
            .validate(if recovery_only {
                manifest.issued_at_ms
            } else {
                now_ms
            })
            .map_err(|_| CopySemanticError::Manifest)?;
        let semantic: WireCopySemanticJob = serde_json::from_value(job.semantic_job.clone())
            .map_err(|_| CopySemanticError::SemanticJob)?;
        if semantic.leader_intent.is_null()
            || job.job_id != manifest.identities.job_id.to_string()
            || job.job_digest != manifest.plan_digest
            || job.symbol != manifest.binding.instrument.symbol
            || job.created_at_ms != manifest.issued_at_ms
            || job.expires_at_ms != manifest.expires_at_ms
            || semantic.target.snapshot_generation != manifest.snapshot_generation
            || manifest.binding.account_id != turn.lease().binding.trading_account_id
            || manifest.binding.instrument.symbol != turn.lease().binding.symbol
            || manifest.binding.follower_instance_id != turn.lease().binding.instance_id
        {
            return Err(CopySemanticError::Binding);
        }
        let account = AccountKey::new(
            turn.lease().binding.venue,
            turn.lease().binding.trading_account_id.clone(),
        )
        .map_err(|_| CopySemanticError::Binding)?;
        let key = StrategyInstanceKey::new(
            account,
            StrategyKind::Copy,
            turn.lease().binding.instance_id.clone(),
            turn.lease().binding.symbol.clone(),
        )
        .map_err(|_| CopySemanticError::Binding)?;
        let run_id = manifest.binding.follower_binding_id.to_string();
        let actor = StrategyBinding::new(key, run_id.clone(), encode_hex(&manifest.plan_digest))
            .map_err(|_| CopySemanticError::Binding)?;
        let owner = OrderOwner {
            strategy_instance_id: actor.key.instance_id.clone(),
            run_id,
            exchange: actor.key.account.exchange.as_str().to_owned(),
            account: actor.key.account.account.clone(),
            symbol: actor.key.symbol.clone(),
            purpose: owner_purpose(&semantic.target),
        };
        owner.validate().map_err(|_| CopySemanticError::Binding)?;
        Ok(Self {
            delivery_digest: manifest.delivery_digest(),
            manifest,
            target: semantic.target,
            actor,
            owner,
            durable_inbox_digest: turn.durable_inbox_digest(),
            durable_inbox_sequence: turn.durable_inbox_sequence(),
            durable_inbox_root_digest: turn.durable_inbox_root_digest(),
            recovery_only,
        })
    }

    #[must_use]
    pub const fn manifest(&self) -> &FollowerDeliveryManifest {
        &self.manifest
    }

    #[must_use]
    pub const fn target(&self) -> &TargetExposurePlan {
        &self.target
    }

    #[must_use]
    pub const fn actor(&self) -> &StrategyBinding {
        &self.actor
    }

    #[must_use]
    pub const fn owner(&self) -> &OrderOwner {
        &self.owner
    }

    /// A job's plan digest is not an Actor configuration. Resolve the existing configured actor
    /// and verify the latest durable relation instead of registering a new actor for every job.
    pub fn bind_registered_actor(
        &mut self,
        binding: &StrategyBinding,
        relation: &CopyRelationRecord,
    ) -> Result<(), CopySemanticError> {
        if self.recovery_only {
            return Err(CopySemanticError::Expired);
        }
        relation
            .validate()
            .map_err(|_| CopySemanticError::Binding)?;
        let follower = &relation.relation.follower;
        if binding.key.strategy_kind != StrategyKind::Copy
            || binding.key != self.actor.key
            || relation.relation.relation_id
                != self.manifest.binding.relation.relation_id.to_string()
            || relation.revision != self.manifest.binding.relation.revision
            || relation.relation.policy_digest() != self.manifest.binding.relation.policy_digest
            || relation.relation.lifecycle != CopyLifecyclePolicy::Active
            || follower.venue.as_str() != binding.key.account.exchange.as_str()
            || follower.trading_account_id != binding.key.account.account
            || follower.symbol != binding.key.symbol
            || follower.instance_id != binding.key.instance_id
        {
            return Err(CopySemanticError::Binding);
        }
        self.bind_recovery_actor(binding)
    }

    /// An old relation may be paused or superseded while its own submitted child still needs
    /// reconciliation. This binds only the configured owner; it cannot admit new execution.
    pub(crate) fn bind_recovery_actor(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), CopySemanticError> {
        if binding.key.strategy_kind != StrategyKind::Copy || binding.key != self.actor.key {
            return Err(CopySemanticError::Binding);
        }
        self.actor = binding.clone();
        self.owner.run_id = binding.run_id.clone();
        Ok(())
    }

    pub(crate) fn allow_cross_zero_continuation(
        &mut self,
        previous: &CopyExecutionRequest,
        now_ms: u64,
    ) -> Result<(), CopySemanticError> {
        self.validate_execution_request(previous)?;
        if previous.phase != venue_copy::CopyExecutionPhase::ReduceToZero {
            return Err(CopySemanticError::ExecutionRequest);
        }
        self.manifest
            .validate(now_ms)
            .map_err(|_| CopySemanticError::Expired)?;
        self.recovery_only = false;
        Ok(())
    }

    pub fn signed_position(
        &self,
        snapshot: &venue_runtime::SignedAccountSnapshot,
        now_ms: u64,
    ) -> Result<AuthoritativePositionSnapshot, CopySemanticError> {
        if snapshot.binding().venue.as_str() != self.actor.key.account.exchange.as_str() {
            return Err(CopySemanticError::Binding);
        }
        signed_position::position(&self.manifest, snapshot, now_ms)
    }

    pub fn limit_normalization_intent(
        &self,
        request: &CopyExecutionRequest,
    ) -> Result<venue_runtime::AccountLimitNormalizationIntent, CopySemanticError> {
        self.validate_execution_request(request)?;
        if request.requested_delta_exposure.value.is_zero() {
            return Err(CopySemanticError::ExecutionRequest);
        }
        let ids = CopyCommandIds::from_request(request)?;
        let current = request.current_exposure.value;
        let delta = request.requested_delta_exposure.value;
        let reduce_only =
            !current.is_zero() && current.is_sign_positive() != delta.is_sign_positive();
        if reduce_only && delta.abs() > current.abs() {
            return Err(CopySemanticError::ExecutionRequest);
        }
        let position_side = position_side_for(if reduce_only { current } else { delta })
            .ok_or(CopySemanticError::ExecutionRequest)?;
        Ok(venue_runtime::AccountLimitNormalizationIntent {
            command_id: ids.command_id,
            client_order_id: ids.client_order_id,
            owner: OrderOwner {
                purpose: if reduce_only {
                    OrderPurpose::ExposureTakeProfit
                } else {
                    OrderPurpose::Entry
                },
                ..self.owner.clone()
            },
            side: if delta.is_sign_positive() {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            position_side,
            quote_delta: delta.abs(),
            reduce_only,
        })
    }

    #[must_use]
    pub const fn delivery_digest(&self) -> [u8; 32] {
        self.delivery_digest
    }

    #[must_use]
    pub const fn durable_inbox_digest(&self) -> [u8; 32] {
        self.durable_inbox_digest
    }

    #[must_use]
    pub const fn durable_inbox_sequence(&self) -> u64 {
        self.durable_inbox_sequence
    }

    #[must_use]
    pub const fn durable_inbox_root_digest(&self) -> [u8; 32] {
        self.durable_inbox_root_digest
    }

    pub fn runtime_commitment(
        &self,
    ) -> Result<venue_runtime::account::CopyActorCommitment, CopySemanticError> {
        venue_runtime::account::CopyActorCommitment::new(
            self.delivery_digest,
            self.durable_inbox_digest,
            self.durable_inbox_sequence,
            self.durable_inbox_root_digest,
        )
        .map_err(|_| CopySemanticError::Binding)
    }

    /// Applies only the semantic Copy checkpoint through a recovered runtime. A missing real WAL
    /// head or an unready runtime fails closed; this method has no path to an execution command.
    pub fn apply_to_runtime(
        &self,
        runtime: &mut venue_runtime::account::AccountRuntime,
    ) -> Result<venue_runtime::account::CopyActorAppliedReceipt, CopySemanticError> {
        if self.recovery_only {
            return Err(CopySemanticError::Expired);
        }
        let commitment = self.runtime_commitment()?;
        runtime
            .apply_copy_actor_turn(&self.actor, commitment)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)
    }

    /// Produces a relation-bound semantic execution request from a fresh signed follower fact.
    /// It is intentionally not an `ExecutionCommand`: Node first applies fresh venue rules and
    /// durable identity allocation, then Runtime alone seals lane admission.
    pub fn execution_request(
        &self,
        position: &AuthoritativePositionSnapshot,
        now_ms: u64,
    ) -> Result<CopyExecutionRequest, CopySemanticError> {
        if self.recovery_only {
            return Err(CopySemanticError::Expired);
        }
        plan_copy_execution(&self.manifest, &self.target, position, now_ms)
            .map_err(|_| CopySemanticError::ExecutionRequest)
    }

    /// Converts a relation-bound target into the one canonical command shape accepted by the
    /// account lane.  The caller must supply a fresh adapter-normalized quantity; Copy never
    /// guesses contracts from quote exposure.  A reversal is split by `execution_request`: this
    /// method can only create the first reduce-to-zero command until a later signed zero fact
    /// produces a distinct `Adjust` request.
    pub fn execution_command(
        &self,
        request: &CopyExecutionRequest,
        facts: &FreshCopyCommandFacts,
    ) -> Result<ExecutionCommand, CopySemanticError> {
        self.validate_execution_request(request)?;
        if request.job_id != self.manifest.identities.job_id
            || request.delivery_digest != self.delivery_digest
            || request.binding != self.manifest.binding
            || request.position_generation == 0
            || facts.rules_generation == 0
            || facts.price_generation == 0
            || facts.observed_at_ms == 0
            || facts.fact_digest == [0; 32]
            || !facts.normalized_quantity.is_sign_positive()
            || facts.normalized_quantity.is_zero()
        {
            return Err(CopySemanticError::ExecutionCommand);
        }
        let command_ids = CopyCommandIds::from_request(request)?;
        let current = request.current_exposure.value;
        let delta = request.requested_delta_exposure.value;
        let is_reduction = match request.phase {
            venue_copy::CopyExecutionPhase::ReduceToZero => {
                if current.is_zero() || delta != -current {
                    return Err(CopySemanticError::ExecutionCommand);
                }
                true
            }
            venue_copy::CopyExecutionPhase::Adjust => {
                if delta.is_zero() {
                    return Err(CopySemanticError::ExecutionCommand);
                }
                !current.is_zero() && current.is_sign_positive() != delta.is_sign_positive()
            }
        };
        let position_side = if is_reduction {
            position_side_for(current).ok_or(CopySemanticError::ExecutionCommand)?
        } else {
            position_side_for(delta).ok_or(CopySemanticError::ExecutionCommand)?
        };
        let owner = OrderOwner {
            purpose: if is_reduction {
                OrderPurpose::ExposureTakeProfit
            } else {
                OrderPurpose::Entry
            },
            ..self.owner.clone()
        };
        let command = if is_reduction {
            ExecutionCommand::MarketReduce(MarketReduceCommand {
                command_id: command_ids.command_id,
                client_order_id: command_ids.client_order_id,
                owner,
                position_side,
                side: reduce_side(position_side)?,
                quantity: facts.normalized_quantity,
                risk_episode_id: command_ids.risk_episode_id,
                position_generation: request.position_generation,
            })
        } else {
            ExecutionCommand::PlaceLimit(OrderCommand {
                command_id: command_ids.command_id,
                client_order_id: command_ids.client_order_id,
                owner,
                position_side,
                side: open_side(position_side)?,
                quantity: facts.normalized_quantity,
                limit_price: facts.limit_price,
                reduce_only: false,
            })
        };
        command
            .validate()
            .map_err(|_| CopySemanticError::ExecutionCommand)?;
        Ok(command)
    }

    /// Host first writes the sole account WAL `Prepared` record, then Runtime validates the real
    /// actor receipt and admits that opaque proof to its lane.  Node cannot allocate an identity,
    /// dispatch a Host command, or retry an Unknown outcome itself.
    pub fn admit_execution_command<G: AccountPhysicalGateway>(
        &self,
        host: &mut AccountRuntimeHost<G>,
        runtime: &mut venue_runtime::account::AccountRuntime,
        applied: &venue_runtime::account::CopyActorAppliedReceipt,
        request: &CopyExecutionRequest,
        command: ExecutionCommand,
        observed_at_ms: u64,
    ) -> Result<CopyExecutionResult, CopySemanticError> {
        let expected_ids = CopyCommandIds::from_request(request)?;
        if observed_at_ms == 0
            || request.job_id != self.manifest.identities.job_id
            || request.delivery_digest != self.delivery_digest
            || request.binding != self.manifest.binding
            || host.account() != &self.actor.key.account
            || !same_copy_owner(command.mutation_owner(), &self.owner)
            || command.command_id() != &expected_ids.command_id
            || command.native_client_id() != Some(&expected_ids.client_order_id)
            || command.validate().is_err()
            || (matches!(request.phase, venue_copy::CopyExecutionPhase::ReduceToZero)
                && !is_reduce_only(&command))
        {
            return Err(CopySemanticError::ExecutionCommand);
        }
        let command_id = command.command_id().as_str().to_owned();
        if let Some(status) = host
            .command_status(command.command_id())
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?
        {
            return self.result_from_status(request, &status, observed_at_ms);
        }
        host.prepare_and_admit_copy_actor(
            runtime,
            &self.actor,
            applied,
            venue_runtime::account::AccountLanePriority::Normal,
            command,
        )
        .map_err(|_| CopySemanticError::RuntimeUnavailable)?;
        Ok(CopyExecutionResult {
            request: request.clone(),
            state: CopyExecutionState::Prepared,
            command_id: Some(command_id),
            fact_digest: [0; 32],
            reconciled_position: None,
            observed_at_ms,
        })
    }

    /// Runs the account's next eligible lane item exclusively through Runtime's Host bridge, then
    /// reads the same command WAL for this Copy delivery.  It never retries a command: an Unknown
    /// result remains frozen until `reconcile_execution_command` receives newer signed facts.
    pub fn dispatch_admitted_execution<G: AccountPhysicalGateway>(
        &self,
        host: &mut AccountRuntimeHost<G>,
        runtime: &mut venue_runtime::account::AccountRuntime,
        request: &CopyExecutionRequest,
        command: &ExecutionCommand,
        observed_at_ms: u64,
    ) -> Result<CopyExecutionResult, CopySemanticError> {
        if observed_at_ms == 0
            || host.account() != &self.actor.key.account
            || command.command_id() != &CopyCommandIds::from_request(request)?.command_id
        {
            return Err(CopySemanticError::ExecutionCommand);
        }
        runtime
            .dispatch_next_with_host(host)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?;
        let status = host
            .command_status(command.command_id())
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?
            .ok_or(CopySemanticError::RuntimeUnavailable)?;
        self.result_from_status(request, &status, observed_at_ms)
    }

    pub(crate) fn result_from_status(
        &self,
        request: &CopyExecutionRequest,
        status: &venue_runtime::AccountCommandStatus,
        observed_at_ms: u64,
    ) -> Result<CopyExecutionResult, CopySemanticError> {
        self.validate_execution_request(request)?;
        if status.binding().venue.as_str() != self.actor.key.account.exchange.as_str()
            || status.binding().trading_account_id != self.actor.key.account.account
            || observed_at_ms == 0
            || status.command_id() != &CopyCommandIds::from_request(request)?.command_id
            || status.record_sha256() == [0; 32]
        {
            return Err(CopySemanticError::RuntimeUnavailable);
        }
        let state = match status.state() {
            CommandState::Prepared => CopyExecutionState::Prepared,
            CommandState::Submitted => CopyExecutionState::Submitted,
            CommandState::Accepted { .. } => CopyExecutionState::Accepted,
            CommandState::Rejected { .. } => CopyExecutionState::Rejected,
            CommandState::Unknown { .. } => CopyExecutionState::Unknown,
        };
        Ok(CopyExecutionResult {
            request: request.clone(),
            state,
            command_id: Some(status.command_id().as_str().to_owned()),
            fact_digest: status.record_sha256(),
            reconciled_position: None,
            observed_at_ms,
        })
    }

    pub(crate) fn validate_execution_request(
        &self,
        request: &CopyExecutionRequest,
    ) -> Result<(), CopySemanticError> {
        use venue_copy::CopyExecutionPhase;
        let current = request.current_exposure.value;
        let desired = request.target_exposure.value;
        let cross_zero = !current.is_zero()
            && !desired.is_zero()
            && current.is_sign_positive() != desired.is_sign_positive();
        let expected = if cross_zero {
            -current
        } else {
            desired
                .checked_sub(current)
                .ok_or(CopySemanticError::ExecutionRequest)?
        };
        if request.job_id != self.manifest.identities.job_id
            || request.delivery_digest != self.delivery_digest
            || request.binding != self.manifest.binding
            || request.target_generation != self.manifest.snapshot_generation
            || request.position_generation == 0
            || request.target_exposure != self.target.target_exposure
            || [
                &request.target_exposure,
                &request.current_exposure,
                &request.requested_delta_exposure,
            ]
            .iter()
            .any(|amount| {
                amount.asset.as_str() != self.owner.symbol.quote()
                    || amount.value == Decimal::MIN
                    || amount.value == Decimal::MAX
            })
            || request.requested_delta_exposure.value != expected
            || (request.phase == CopyExecutionPhase::ReduceToZero) != cross_zero
        {
            return Err(CopySemanticError::ExecutionRequest);
        }
        Ok(())
    }

    #[must_use]
    pub const fn grants_gateway_capability(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_writer_lease(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_wal_authority(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_dispatch_permit(&self) -> bool {
        false
    }
}

struct CopyCommandIds {
    command_id: CommandId,
    client_order_id: CommandId,
    risk_episode_id: CommandId,
}

impl CopyCommandIds {
    fn from_request(request: &CopyExecutionRequest) -> Result<Self, CopySemanticError> {
        // New signed position generations cannot mint another child for a redelivered job.
        // Each immutable job has at most one reduce child and one adjust child; drift repair
        // requires a new job. Keep 128 digest bits within the 36-byte client-ID ceiling.
        let tag = request.delivery_digest[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let phase = match request.phase {
            venue_copy::CopyExecutionPhase::ReduceToZero => "r",
            venue_copy::CopyExecutionPhase::Adjust => "a",
        };
        let suffix = format!("{phase}{tag}");
        Ok(Self {
            command_id: CommandId::new(format!("cc{suffix}"))
                .map_err(|_| CopySemanticError::ExecutionCommand)?,
            client_order_id: CommandId::new(format!("co{suffix}"))
                .map_err(|_| CopySemanticError::ExecutionCommand)?,
            risk_episode_id: CommandId::new(format!("cr{suffix}"))
                .map_err(|_| CopySemanticError::ExecutionCommand)?,
        })
    }
}

fn position_side_for(value: Decimal) -> Option<PositionSide> {
    if value.is_sign_positive() && !value.is_zero() {
        Some(PositionSide::Long)
    } else if value.is_sign_negative() {
        Some(PositionSide::Short)
    } else {
        None
    }
}

fn open_side(position_side: PositionSide) -> Result<OrderSide, CopySemanticError> {
    match position_side {
        PositionSide::Long => Ok(OrderSide::Buy),
        PositionSide::Short => Ok(OrderSide::Sell),
        PositionSide::Net => Err(CopySemanticError::ExecutionCommand),
    }
}

fn reduce_side(position_side: PositionSide) -> Result<OrderSide, CopySemanticError> {
    match position_side {
        PositionSide::Long => Ok(OrderSide::Sell),
        PositionSide::Short => Ok(OrderSide::Buy),
        PositionSide::Net => Err(CopySemanticError::ExecutionCommand),
    }
}

fn owner_purpose(target: &TargetExposurePlan) -> OrderPurpose {
    let target_value = target.target_exposure.value;
    let delta_value = target.delta_exposure.value;
    if target_value.is_zero()
        || (!target_value.is_zero()
            && !delta_value.is_zero()
            && target_value.is_sign_positive() != delta_value.is_sign_positive())
    {
        OrderPurpose::Reduce
    } else {
        OrderPurpose::Entry
    }
}

fn same_copy_owner(actual: &OrderOwner, expected: &OrderOwner) -> bool {
    actual.strategy_instance_id == expected.strategy_instance_id
        && actual.run_id == expected.run_id
        && actual.exchange == expected.exchange
        && actual.account == expected.account
        && actual.symbol == expected.symbol
}

fn is_reduce_only(command: &ExecutionCommand) -> bool {
    match command {
        ExecutionCommand::PlaceLimit(order) => order.reduce_only,
        ExecutionCommand::MarketReduce(_) => true,
        _ => false,
    }
}

fn encode_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CopySemanticError {
    #[error("delivery is not a Copy semantic job")]
    Kind,
    #[error("Copy semantic job has expired")]
    Expired,
    #[error("Copy delivery manifest is invalid")]
    Manifest,
    #[error("Copy semantic job is invalid")]
    SemanticJob,
    #[error("Copy semantic job conflicts with its exact follower binding")]
    Binding,
    #[error("Copy runtime is not durably recovered and ready for this Actor turn")]
    RuntimeUnavailable,
    #[error("Copy execution request is not bound to fresh signed follower facts")]
    ExecutionRequest,
    #[error("Copy execution command is not a valid fresh-rule translation of this request")]
    ExecutionCommand,
}

pub(crate) fn copy_clock() -> Result<u64, CopySemanticError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| CopySemanticError::RuntimeUnavailable)?
        .as_millis()
        .try_into()
        .map_err(|_| CopySemanticError::RuntimeUnavailable)
}

#[cfg(test)]
mod tests {
    mod resident_e2e;
    use rust_decimal::Decimal;
    use venue_copy::{
        CopyAction, CopyExecutionPhase, CopyIdentityInput, DeliveryBinding, RelationCommitment,
        derive_copy_identities,
    };
    use venue_domain::domain::{Amount, Asset, InstrumentIdentity, MarketKind, Symbol};

    use super::*;

    pub(super) fn delivery_and_request(
        current: Decimal,
        target: Decimal,
        phase: CopyExecutionPhase,
        position_generation: u64,
    ) -> Result<(CopySemanticDelivery, CopyExecutionRequest), Box<dyn std::error::Error>> {
        let ids = derive_copy_identities(&CopyIdentityInput {
            event_id: [1; 16],
            source_event_id: [2; 16],
            follower_account_id: [3; 16],
            follower_binding_id: [4; 16],
            leader_order_id: [5; 16],
            revision: 1,
            action: CopyAction::New,
        })?;
        let relation = derive_copy_identities(&CopyIdentityInput {
            event_id: [6; 16],
            source_event_id: [7; 16],
            follower_account_id: [8; 16],
            follower_binding_id: [9; 16],
            leader_order_id: [10; 16],
            revision: 1,
            action: CopyAction::New,
        })?;
        let binding = DeliveryBinding {
            relation: RelationCommitment {
                relation_id: relation.job_id,
                revision: 1,
                policy_digest: [7; 32],
            },
            leader_id: relation.planning_snapshot_id,
            follower_id: relation.child_order_id,
            follower_binding_id: ids.planning_snapshot_id,
            follower_instance_id: "copy-instance".to_owned(),
            account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instrument: InstrumentIdentity {
                symbol: "DOGE/USDT".parse::<Symbol>()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
            },
            policy_id: relation.job_id,
        };
        let asset = Asset::new("USDT")?;
        let amount = |value| Amount::new(asset.clone(), value);
        let manifest = FollowerDeliveryManifest {
            identities: ids,
            binding: binding.clone(),
            plan_digest: [9; 32],
            snapshot_generation: 2,
            instrument_generation: 1,
            issued_at_ms: 100,
            expires_at_ms: 200,
        };
        let target_plan = TargetExposurePlan {
            snapshot_generation: 2,
            exposure_ratio: Decimal::ONE,
            safe_available_margin: amount(100.into()),
            effective_follower_capital: amount(100.into()),
            target_exposure: amount(target),
            delta_exposure: amount(target - current),
        };
        let actor = StrategyBinding::new(
            StrategyInstanceKey::new(
                AccountKey::new(venue_runtime::ExchangeId::Okx, binding.account_id.clone())?,
                StrategyKind::Copy,
                "copy-instance".to_owned(),
                binding.instrument.symbol.clone(),
            )?,
            binding.follower_binding_id.to_string(),
            "config-digest".to_owned(),
        )?;
        let owner = OrderOwner {
            strategy_instance_id: actor.key.instance_id.clone(),
            run_id: actor.run_id.clone(),
            exchange: "okx".to_owned(),
            account: binding.account_id.clone(),
            symbol: binding.instrument.symbol.clone(),
            purpose: OrderPurpose::Entry,
        };
        let request = CopyExecutionRequest {
            job_id: manifest.identities.job_id,
            delivery_digest: [11; 32],
            binding,
            target_generation: 2,
            position_generation,
            target_exposure: amount(target),
            current_exposure: amount(current),
            requested_delta_exposure: amount(if phase == CopyExecutionPhase::ReduceToZero {
                -current
            } else {
                target - current
            }),
            phase,
        };
        Ok((
            CopySemanticDelivery {
                manifest,
                target: target_plan,
                actor,
                owner,
                delivery_digest: [11; 32],
                durable_inbox_digest: [12; 32],
                durable_inbox_sequence: 1,
                durable_inbox_root_digest: [13; 32],
                recovery_only: false,
            },
            request,
        ))
    }

    pub(super) fn fresh_facts() -> Result<FreshCopyCommandFacts, Box<dyn std::error::Error>> {
        Ok(FreshCopyCommandFacts {
            normalized_quantity: Decimal::ONE,
            rules_generation: 3,
            price_generation: 4,
            observed_at_ms: 150,
            fact_digest: [14; 32],
            limit_price: Price::new(Decimal::ONE)?,
        })
    }

    #[test]
    fn reversal_is_deterministically_split_by_a_new_zero_position_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (delivery, reduce) = delivery_and_request(
            Decimal::from(20),
            Decimal::from(-10),
            CopyExecutionPhase::ReduceToZero,
            3,
        )?;
        let first = delivery.execution_command(&reduce, &fresh_facts()?)?;
        let repeated = delivery.execution_command(&reduce, &fresh_facts()?)?;
        assert_eq!(first, repeated);
        assert!(matches!(first, ExecutionCommand::MarketReduce(_)));

        let (delivery, open) = delivery_and_request(
            Decimal::ZERO,
            Decimal::from(-10),
            CopyExecutionPhase::Adjust,
            4,
        )?;
        let second = delivery.execution_command(&open, &fresh_facts()?)?;
        assert!(matches!(second, ExecutionCommand::PlaceLimit(_)));
        assert_ne!(first.command_id(), second.command_id());
        Ok(())
    }

    #[test]
    fn stale_or_unbound_rule_facts_cannot_create_a_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let (delivery, request) = delivery_and_request(
            Decimal::ZERO,
            Decimal::from(10),
            CopyExecutionPhase::Adjust,
            4,
        )?;
        let mut facts = fresh_facts()?;
        facts.fact_digest = [0; 32];
        assert_eq!(
            delivery.execution_command(&request, &facts),
            Err(CopySemanticError::ExecutionCommand)
        );
        Ok(())
    }

    #[test]
    fn repeated_generation_cannot_mint_a_new_copy_child() -> Result<(), Box<dyn std::error::Error>>
    {
        let (delivery, request) =
            delivery_and_request(Decimal::ZERO, 10.into(), CopyExecutionPhase::Adjust, 4)?;
        let original = delivery.limit_normalization_intent(&request)?;
        let mut later = request.clone();
        later.position_generation = 900;
        later.current_exposure.value = 3.into();
        later.requested_delta_exposure.value = 7.into();
        let repeated = delivery.limit_normalization_intent(&later)?;
        assert_eq!(original.command_id, repeated.command_id);
        assert_eq!(original.client_order_id, repeated.client_order_id);
        assert!(original.client_order_id.as_str().len() <= 36);
        Ok(())
    }

    #[test]
    fn altered_target_delta_currency_or_phase_is_not_the_original_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let (delivery, request) =
            delivery_and_request(Decimal::ZERO, 10.into(), CopyExecutionPhase::Adjust, 4)?;
        let mut bad = request.clone();
        bad.target_exposure.value = 11.into();
        assert!(delivery.validate_execution_request(&bad).is_err());
        bad = request.clone();
        bad.requested_delta_exposure.value = 11.into();
        assert!(delivery.validate_execution_request(&bad).is_err());
        bad = request.clone();
        bad.current_exposure.asset = Asset::new("USDC")?;
        assert!(delivery.validate_execution_request(&bad).is_err());
        bad = request.clone();
        bad.phase = CopyExecutionPhase::ReduceToZero;
        assert!(delivery.validate_execution_request(&bad).is_err());
        bad = request;
        bad.target_generation += 1;
        assert!(delivery.validate_execution_request(&bad).is_err());
        Ok(())
    }
}
