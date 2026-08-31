use venue_execution::{CommandState, NativeOrderRoute};

use super::*;

impl AccountRuntime {
    /// Rebuilds only complete accepted native identities from the same Host command WAL while a
    /// matching actor is registering. This is an in-memory Router restoration, not a second
    /// owner journal or a new mutation authority; incomplete or ambiguous records are rejected.
    pub(crate) fn hydrate_host_wal_routes(
        &mut self,
        routes: Vec<NativeOrderRoute>,
    ) -> Result<(), AccountRuntimeError> {
        if !self.durable_recovery_complete {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        if routes.is_empty() {
            return Ok(());
        }
        let mut next_router = self.private_router.clone();
        for route in routes {
            let CommandState::Accepted { venue_order_id } = route.state else {
                return Err(AccountRuntimeError::OrderRouteReceipt);
            };
            if route.venue_order_id.as_deref() != Some(venue_order_id.as_str()) {
                return Err(AccountRuntimeError::OrderRouteReceipt);
            }
            self.ensure_supported_order_family(route.key.family)?;
            next_router.bind_order(
                route.key.family,
                route.key.client_id.as_str().to_owned(),
                Some(venue_order_id),
                route.command_id,
                route.owner,
                &self.registry,
            )?;
        }
        let next_revision = self
            .private_route_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::PrivateApplicationState)?;
        self.private_router = next_router;
        self.private_route_revision = next_revision;
        Ok(())
    }
}
