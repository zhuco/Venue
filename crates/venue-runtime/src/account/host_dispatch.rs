use venue_domain::domain::{ExecutionCommand, OrderOwner};
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
        // Only an accepted order-creating command can add a current Runtime route. Cancels,
        // especially the sealed legacy-v1 custody exception, retain their historical Owner and
        // must never hydrate that predecessor account identity into the successor router.
        let accepted_owner = accepted_route_owner(&command, &outcome);
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

fn accepted_route_owner(
    command: &ExecutionCommand,
    outcome: &venue_execution::AccountDispatchOutcome,
) -> Option<OrderOwner> {
    (matches!(
        outcome,
        venue_execution::AccountDispatchOutcome::Accepted { .. }
    ) && command.native_client_id().is_some())
    .then(|| command.mutation_owner().clone())
}

#[cfg(test)]
mod tests {
    use super::accepted_route_owner;
    use rust_decimal::Decimal;
    use venue_domain::domain::{
        CancelCommand, CommandId, ExecutionCommand, LimitTimeInForce, OrderCommand, OrderOwner,
        OrderPurpose, OrderSide, PositionSide, Price,
    };

    fn owner(account: &str) -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "grid-a".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "binance".to_owned(),
            account: account.to_owned(),
            symbol: "SOL/USDC".parse()?,
            purpose: OrderPurpose::Entry,
        })
    }

    #[test]
    fn accepted_cancel_has_no_route_identity_to_hydrate() -> Result<(), Box<dyn std::error::Error>>
    {
        let legacy = ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("legacy-cancel")?,
            owner: owner("portfolio_margin_um")?,
            target_client_order_id: CommandId::new("legacy-client")?,
        });
        let accepted = venue_execution::AccountDispatchOutcome::Accepted {
            venue_order_id: "accepted-native".to_owned(),
        };
        assert!(accepted_route_owner(&legacy, &accepted).is_none());

        let current = ExecutionCommand::PlaceLimit(OrderCommand {
            time_in_force: LimitTimeInForce::PostOnly,
            command_id: CommandId::new("current-place")?,
            client_order_id: CommandId::new("current-client")?,
            owner: owner("successor-account")?,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(5, 2),
            limit_price: Price::new(Decimal::from(100_u8))?,
            reduce_only: false,
        });
        assert_eq!(
            accepted_route_owner(&current, &accepted),
            Some(owner("successor-account")?)
        );
        Ok(())
    }
}
