use venue_runtime::AccountPhysicalGateway;

use super::*;

/// Read-only Copy recovery work can close the Control delivery only after the original child has
/// an exact, newer signed proof. It never manufactures an execution result from local receipt
/// state alone.
impl<G: AccountPhysicalGateway> ControlResidentLoop<G> {
    pub(super) fn complete_copy_reconciliation_turn(
        &mut self,
        http_runtime: &tokio::runtime::Runtime,
        instance_id: &str,
        turn: crate::ReconciliationTurn,
        now: u64,
    ) -> Result<bool, ControlResidentLoopError> {
        let venue_control_protocol::AccountDeliveryPayload::CopySemanticJob(job) = turn.payload()
        else {
            return Err(ControlResidentLoopError::Copy);
        };
        let Some(durable) = self
            .copy_jobs
            .get(instance_id)
            .ok_or(ControlResidentLoopError::Config)?
            .jobs()
            .get(&job.job_id)
        else {
            return Ok(true);
        };
        let original = durable
            .turn
            .restore()
            .map_err(|_| ControlResidentLoopError::Copy)?;
        if original.payload() != turn.payload()
            || original.lease().delivery_id != turn.lease().delivery_id
            || original.lease().binding != turn.lease().binding
        {
            return Err(ControlResidentLoopError::Copy);
        }
        let delivery = CopySemanticDelivery::from_recovered_actor_turn(&original, now)
            .map_err(|_| ControlResidentLoopError::Copy)?;
        let Some(position) = durable.position.as_ref() else {
            return Ok(true);
        };
        let Some(execution) = durable.execution.as_ref().filter(|execution| {
            durable.request.as_ref() == Some(&execution.request)
                && execution.request.job_id.to_string() == job.job_id
                && delivery.manifest().identities.job_id == execution.request.job_id
                && delivery.manifest().binding == execution.request.binding
                && delivery.delivery_digest() == execution.request.delivery_digest
                && execution.request.phase == venue_copy::CopyExecutionPhase::Adjust
                && execution.state == venue_copy::CopyExecutionState::Reconciled
                && execution.fact_digest != [0; 32]
                && execution.reconciled_position.as_ref() == Some(position)
                && position.generation > execution.request.position_generation
                && position.fact_digest != [0; 32]
        }) else {
            return Ok(true);
        };
        let completion = turn.reconciled(
            now,
            execution.fact_digest,
            "copy physical child reconciled by newer signed account facts",
        )?;
        self.submit_reconciliation(http_runtime, instance_id, completion, now)
    }
}
