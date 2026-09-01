use venue_execution::AccountPhysicalGateway;

use crate::execution::AccountLaneFollowUp;

use crate::account::{AccountRuntimeHost, AccountRuntimeHostError};

use super::{AccountRuntime, AccountRuntimeError};

impl AccountRuntime {
    /// The only resident handoff: an admitted lane command reaches the one Host/WAL/writer.
    /// Host failures retain the lane occupancy so callers reconcile rather than resubmit.
    pub fn dispatch_next_with_host<G: AccountPhysicalGateway>(
        &mut self,
        host: &mut AccountRuntimeHost<G>,
    ) -> Result<Option<AccountLaneFollowUp>, AccountRuntimeHostError<G::Error>> {
        if host.account() != &self.account {
            return Err(AccountRuntimeHostError::Scope);
        }
        let command_id = {
            let candidate = self
                .next_execution_for_wal()
                .map_err(AccountRuntimeHostError::Runtime)?;
            let Some(candidate) = candidate else {
                return Ok(None);
            };
            candidate.command_id().clone()
        };
        if !host.has_prepared(&command_id) {
            return Err(AccountRuntimeHostError::PreparedProof);
        }
        let command = self.execution_lane.begin_host_dispatch().map_err(|error| {
            AccountRuntimeHostError::Runtime(AccountRuntimeError::ExecutionLane(error))
        })?;
        if command.command_id() != &command_id {
            return Err(AccountRuntimeHostError::PreparedProof);
        }
        let outcome = host.dispatch_prepared_for_lane(&command_id)?;
        let accepted_owner = matches!(
            outcome,
            venue_execution::AccountDispatchOutcome::Accepted { .. }
        )
        .then(|| command.mutation_owner().clone());
        self.advance_resident_wal_head(host.runtime_wal_head()?)
            .map_err(AccountRuntimeHostError::Runtime)?;
        let follow_up = self
            .execution_lane
            .record_host_outcome(&command_id, outcome)
            .map_err(|error| {
                AccountRuntimeHostError::Runtime(AccountRuntimeError::ExecutionLane(error))
            })?;
        if let Some(owner) = accepted_owner {
            self.hydrate_host_wal_routes(host.accepted_order_routes_for_owner(&owner))
                .map_err(AccountRuntimeHostError::Runtime)?;
        }
        Ok(Some(follow_up))
    }
}
