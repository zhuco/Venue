use super::*;
use crate::runtime::account::PersistedOrderRouteAppendReceipt;

impl AccountRuntime {
    pub(crate) fn install_order_route(
        &mut self,
        receipt: PersistedOrderRouteAppendReceipt,
    ) -> Result<(), AccountRuntimeError> {
        if !self.durable_recovery_complete {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        let (route, previous_root, next_root, append_sequence, record_sha256) =
            receipt.into_parts();
        let expected_append_sequence = self
            .owner_index_tail_sequence
            .checked_add(1)
            .ok_or(AccountRuntimeError::OrderRouteReceipt)?;
        let next_record_count = self
            .owner_index_record_count
            .checked_add(1)
            .ok_or(AccountRuntimeError::OrderRouteReceipt)?;
        if self.owner_index_root != Some(previous_root)
            || append_sequence != expected_append_sequence
            || record_sha256.iter().all(|byte| *byte == 0)
            || next_root.iter().all(|byte| *byte == 0)
        {
            return Err(AccountRuntimeError::OrderRouteReceipt);
        }
        let (family, command_id, client_order_id, venue_order_id, owner) = route.into_parts();
        self.ensure_supported_order_family(family)?;
        let mut next_router = self.private_router.clone();
        next_router.bind_order(
            family,
            client_order_id,
            venue_order_id,
            command_id,
            owner,
            &self.registry,
        )?;
        let next_revision = self
            .private_route_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::PrivateApplicationState)?;
        let current_roots = self
            .physical_authority_roots
            .as_ref()
            .ok_or(AccountRuntimeError::PhysicalRecoveryRequired)?;
        let next_physical_roots = current_roots
            .refreshed_owner(next_root)
            .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        self.private_router = next_router;
        self.private_route_revision = next_revision;
        self.owner_index_root = Some(next_root);
        self.owner_index_tail_sequence = append_sequence;
        self.owner_index_record_count = next_record_count;
        self.physical_authority_roots = Some(next_physical_roots);
        self.physical_durable_roots = None;
        if self.admitted_physical_recovery.is_some()
            || self.active_physical_recovery_session.is_some()
        {
            self.revoke_physical_authority();
        }
        Ok(())
    }
}
