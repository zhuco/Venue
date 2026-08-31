use rust_decimal::Decimal;
use venue_control_protocol::CopyRelationRecord;
use venue_copy::{
    AuthoritativePositionSnapshot, CopyExecutionPhase, CopyExecutionRequest, CopyExecutionResult,
};
use venue_domain::domain::{Fill, Price};
use venue_runtime::{
    AccountPhysicalGateway, SignedAccountSnapshot, StrategyBinding,
    account::CopyActorAppliedReceipt,
};

use super::{ProductionResident, persist_actor_anchor};
use crate::copy_semantic::copy_clock as clock;
use crate::{CopySemanticDelivery, CopySemanticError, FreshCopyCommandFacts};

/// Semantic Applied and the same account WAL's current child state remain separate. An ACK
/// never turns this result into signed completion or a ledger update.
pub struct ResidentCopyResult {
    pub applied: CopyActorAppliedReceipt,
    pub request: CopyExecutionRequest,
    pub execution: Option<CopyExecutionResult>,
}

pub struct ResidentCopyReconciliation {
    pub execution: CopyExecutionResult,
    pub position: AuthoritativePositionSnapshot,
    pub fills: Vec<Fill>,
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    pub fn recover_copy_actor_applied(
        &self,
        mut delivery: CopySemanticDelivery,
        binding: &StrategyBinding,
    ) -> Result<Option<CopyActorAppliedReceipt>, CopySemanticError> {
        delivery.bind_recovery_actor(binding)?;
        self.runtime
            .recover_copy_actor_applied(binding, delivery.runtime_commitment()?)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)
    }

    /// Applies one immutable job against the configured Copy Actor and a current durable Control
    /// relation. The caller must retain the job for later signed reconciliation; this step only
    /// dispatches its deterministic child once through Runtime/Lane/Host.
    pub fn apply_copy_delivery<F>(
        &mut self,
        delivery: CopySemanticDelivery,
        binding: &StrategyBinding,
        relation: &CopyRelationRecord,
        before_execution: F,
    ) -> Result<ResidentCopyResult, CopySemanticError>
    where
        F: FnMut(&CopyExecutionRequest) -> Result<(), CopySemanticError>,
    {
        self.apply_copy_delivery_inner(delivery, binding, relation, false, before_execution)
    }

    fn apply_copy_delivery_inner<F>(
        &mut self,
        mut delivery: CopySemanticDelivery,
        binding: &StrategyBinding,
        relation: &CopyRelationRecord,
        require_zero_position: bool,
        mut before_execution: F,
    ) -> Result<ResidentCopyResult, CopySemanticError>
    where
        F: FnMut(&CopyExecutionRequest) -> Result<(), CopySemanticError>,
    {
        delivery.bind_registered_actor(binding, relation)?;
        let snapshot = self
            .refresh_signed_snapshot()
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?;
        let now_ms = clock()?;
        let position = delivery.signed_position(&snapshot, now_ms)?;
        let request = delivery.execution_request(&position, now_ms)?;
        // The account may change between the reduce readback and this new signed collection.
        // Do not reinterpret an immutable reduce child or prepare a second phase from nonzero.
        if require_zero_position
            && (!request.current_exposure.value.is_zero()
                || request.phase != CopyExecutionPhase::Adjust)
        {
            return Err(CopySemanticError::ExecutionRequest);
        }
        validate_current_capital(&delivery, &request, relation, &snapshot)?;
        // Persist the exact original request before either Actor Applied or command WAL. A
        // later signed position must not rewrite the meaning of this immutable child's ID.
        before_execution(&request)?;
        let applied = delivery.apply_to_runtime(&mut self.runtime)?;
        persist_actor_anchor(
            &self.artifacts_root,
            binding,
            &applied.actor_applied().anchor(),
        )
        .map_err(|_| CopySemanticError::RuntimeUnavailable)?;
        if request.requested_delta_exposure.value.is_zero() {
            return Ok(ResidentCopyResult {
                applied,
                request,
                execution: None,
            });
        }
        let intent = delivery.limit_normalization_intent(&request)?;
        // Check the stable child id before doing new normalization. A later snapshot changes
        // neither an Unknown child's identity nor the price/quantity already held in the WAL.
        if let Some(status) = self
            .host
            .command_status(&intent.command_id)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?
        {
            let execution = delivery.result_from_status(&request, &status, clock()?)?;
            return Ok(ResidentCopyResult {
                applied,
                request,
                execution: Some(execution),
            });
        }
        if request.phase == CopyExecutionPhase::ReduceToZero
            || request.target_exposure.value.is_zero()
        {
            let position = snapshot
                .positions()
                .iter()
                .find(|row| row.symbol == binding.key.symbol && !row.quantity.is_zero())
                .ok_or(CopySemanticError::ExecutionRequest)?;
            let mark = Price::new(
                position
                    .mark_price
                    .ok_or(CopySemanticError::ExecutionRequest)?,
            )
            .map_err(|_| CopySemanticError::ExecutionRequest)?;
            // A complete signed position already uses canonical base units. The adapter validates
            // the actual lot/contract rules and caps reduce-only against a fresh position again.
            let facts = FreshCopyCommandFacts {
                normalized_quantity: position.quantity.abs(),
                rules_generation: snapshot.rules_generation(),
                price_generation: snapshot.private_generation(),
                observed_at_ms: snapshot.observed_at_ms(),
                fact_digest: delivery.signed_position(&snapshot, now_ms)?.fact_digest,
                limit_price: mark,
            };
            let command = delivery.execution_command(&request, &facts)?;
            delivery.admit_execution_command(
                &mut self.host,
                &mut self.runtime,
                &applied,
                &request,
                command,
                now_ms,
            )?;
        } else {
            self.host
                .normalize_and_prepare_copy_limit(
                    &mut self.runtime,
                    binding,
                    &applied,
                    venue_runtime::account::AccountLanePriority::Normal,
                    &intent,
                )
                .map_err(|_| CopySemanticError::RuntimeUnavailable)?;
        }
        self.runtime
            .dispatch_next_with_host(&mut self.host)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?;
        let status = self
            .host
            .command_status(&intent.command_id)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?
            .ok_or(CopySemanticError::RuntimeUnavailable)?;
        let execution = delivery.result_from_status(&request, &status, clock()?)?;
        Ok(ResidentCopyResult {
            applied,
            request,
            execution: Some(execution),
        })
    }

    /// Recovery accepts expired or superseded deliveries only to read the original WAL child
    /// and current signed facts. It never normalizes, prepares, or dispatches a replacement.
    pub fn reconcile_copy_delivery(
        &mut self,
        mut delivery: CopySemanticDelivery,
        binding: &StrategyBinding,
        request: &CopyExecutionRequest,
        previous_fills: &[Fill],
    ) -> Result<ResidentCopyReconciliation, CopySemanticError> {
        delivery.bind_recovery_actor(binding)?;
        let result =
            delivery.reconcile_execution_command(&mut self.host, request, previous_fills)?;
        Ok(ResidentCopyReconciliation {
            execution: result.execution,
            position: result.position,
            fills: result.fills,
        })
    }

    /// Only the second child of this immutable cross-zero job may follow its reduce child.
    /// Re-check signed completion now; a persisted ACK, stale zero or changed relation is
    /// insufficient. The new Adjust request still must be journaled before Actor/WAL work.
    pub fn continue_cross_zero_copy_delivery<F>(
        &mut self,
        mut delivery: CopySemanticDelivery,
        binding: &StrategyBinding,
        relation: &CopyRelationRecord,
        previous: &CopyExecutionResult,
        previous_fills: &[Fill],
        before_execution: F,
    ) -> Result<ResidentCopyResult, CopySemanticError>
    where
        F: FnMut(&CopyExecutionRequest) -> Result<(), CopySemanticError>,
    {
        if previous.state != venue_copy::CopyExecutionState::Reconciled
            || previous.request.phase != CopyExecutionPhase::ReduceToZero
            || previous.fact_digest == [0; 32]
        {
            return Err(CopySemanticError::ExecutionRequest);
        }
        let current = self.reconcile_copy_delivery(
            delivery.clone(),
            binding,
            &previous.request,
            previous_fills,
        )?;
        if current.execution.state != venue_copy::CopyExecutionState::Reconciled
            || !current.position.exposure.value.is_zero()
        {
            return Err(CopySemanticError::ExecutionRequest);
        }
        delivery.allow_cross_zero_continuation(&previous.request, clock()?)?;
        self.apply_copy_delivery_inner(delivery, binding, relation, true, before_execution)
    }
}

fn validate_current_capital(
    delivery: &CopySemanticDelivery,
    request: &CopyExecutionRequest,
    relation: &CopyRelationRecord,
    snapshot: &SignedAccountSnapshot,
) -> Result<(), CopySemanticError> {
    let target = delivery.target();
    let quote = delivery.owner().symbol.quote();
    if [
        &target.safe_available_margin,
        &target.effective_follower_capital,
        &target.target_exposure,
        &target.delta_exposure,
    ]
    .iter()
    .any(|amount| amount.asset.as_str() != quote)
        || target.effective_follower_capital.value < Decimal::ZERO
        || target.safe_available_margin.value < Decimal::ZERO
    {
        return Err(CopySemanticError::ExecutionRequest);
    }
    let delta = request.requested_delta_exposure.value;
    let current = request.current_exposure.value;
    if delta.is_zero()
        || (!current.is_zero()
            && current.is_sign_positive() != delta.is_sign_positive()
            && delta.abs() <= current.abs())
    {
        return Ok(());
    }
    let balance = snapshot
        .balances()
        .iter()
        .find(|balance| balance.asset.as_str() == quote)
        .ok_or(CopySemanticError::ExecutionRequest)?;
    let available = balance
        .available_margin
        .filter(|value| *value >= Decimal::ZERO)
        .ok_or(CopySemanticError::ExecutionRequest)?;
    let reserve = Decimal::ONE
        .checked_sub(relation.relation.safety_reserve_rate)
        .ok_or(CopySemanticError::ExecutionRequest)?;
    let safe = available
        .checked_mul(reserve)
        .ok_or(CopySemanticError::ExecutionRequest)?;
    let capital = safe.min(relation.relation.allocated_capital);
    let cap = capital
        .checked_mul(relation.relation.risk.max_leverage)
        .ok_or(CopySemanticError::ExecutionRequest)?;
    if target.effective_follower_capital.value > capital
        || target.safe_available_margin.value > safe
        || target.target_exposure.value.abs() > cap
        || target.target_exposure.value.abs() > relation.relation.risk.max_total_notional
        || delta.abs() > relation.relation.risk.max_order_notional
        || target
            .exposure_ratio
            .checked_mul(target.effective_follower_capital.value)
            != Some(target.target_exposure.value)
    {
        return Err(CopySemanticError::ExecutionRequest);
    }
    Ok(())
}
