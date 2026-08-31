use std::collections::BTreeMap;

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyExecutionPhase, CopyExecutionRequest, CopyExecutionResult,
    CopyExecutionState,
};
use venue_domain::domain::{ExecutionCommand, FieldState, Fill, NativeOrderFamily, PositionSide};
use venue_runtime::{
    AccountPhysicalGateway, CommandState, SignedAccountSnapshot, account::AccountRuntimeHost,
};

use super::{CopyCommandIds, CopySemanticDelivery, CopySemanticError, same_copy_owner};

/// Only normalized facts collected through the account Host. The caller journals the complete
/// result before projection; retained fills are needed when subsequent cursor pages overlap
/// only the last execution rather than replaying the whole child.
pub(crate) struct CopyReconciliation {
    pub execution: CopyExecutionResult,
    pub position: AuthoritativePositionSnapshot,
    pub fills: Vec<Fill>,
}

impl CopySemanticDelivery {
    pub(crate) fn reconcile_execution_command<G: AccountPhysicalGateway>(
        &self,
        host: &mut AccountRuntimeHost<G>,
        request: &CopyExecutionRequest,
        previous_fills: &[Fill],
    ) -> Result<CopyReconciliation, CopySemanticError> {
        self.validate_execution_request(request)?;
        if host.account() != &self.actor.key.account {
            return Err(CopySemanticError::Binding);
        }
        let ids = CopyCommandIds::from_request(request)?;
        // Never trust a caller-supplied command quantity or re-normalize a submitted child.
        let command = host
            .command_snapshot(&ids.command_id)
            .ok_or(CopySemanticError::RuntimeUnavailable)?;
        if command.native_client_id() != Some(&ids.client_order_id)
            || !same_copy_owner(command.mutation_owner(), &self.owner)
        {
            return Err(CopySemanticError::ExecutionCommand);
        }
        let status = host
            .reconcile_command_status(&ids.command_id)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?
            .ok_or(CopySemanticError::RuntimeUnavailable)?;
        let snapshot = host
            .refresh_signed_snapshot()
            .map_err(|_| CopySemanticError::RuntimeUnavailable)?;
        let now = super::copy_clock()?;
        let position = self.signed_position(&snapshot, now)?;
        let mut execution = self.result_from_status(request, &status, now)?;
        let mut fills = previous_fills.to_vec();
        if let CommandState::Accepted { venue_order_id } = status.state() {
            let facts = execution_facts(
                &command,
                venue_order_id,
                &snapshot,
                previous_fills,
                self.manifest.issued_at_ms,
                now,
            )?;
            let zero_required = request.phase == CopyExecutionPhase::ReduceToZero
                || request.target_exposure.value.is_zero();
            if facts.fully_filled
                && !facts.open
                && position.generation > request.position_generation
                && (!zero_required || position.exposure.value.is_zero())
            {
                let encoded = serde_json::to_vec(&(&command, request, &position, &facts.fills))
                    .map_err(|_| CopySemanticError::ExecutionRequest)?;
                let mut digest = Sha256::new();
                digest.update(b"venue.copy.signed-execution.v1");
                digest.update(encoded);
                execution.state = CopyExecutionState::Reconciled;
                execution.fact_digest = digest.finalize().into();
                execution.reconciled_position = Some(position.clone());
            }
            fills = facts.fills;
        }
        Ok(CopyReconciliation {
            execution,
            position,
            fills,
        })
    }
}

#[cfg(test)]
mod tests;

struct ExecutionFacts {
    fills: Vec<Fill>,
    fully_filled: bool,
    open: bool,
}

fn execution_facts(
    command: &ExecutionCommand,
    venue_order_id: &str,
    snapshot: &SignedAccountSnapshot,
    previous_fills: &[Fill],
    issued_at_ms: u64,
    now_ms: u64,
) -> Result<ExecutionFacts, CopySemanticError> {
    let (owner, client_id, side, position_side, quantity) = match command {
        ExecutionCommand::PlaceLimit(row) => (
            &row.owner,
            &row.client_order_id,
            row.side,
            row.position_side,
            row.quantity,
        ),
        ExecutionCommand::MarketReduce(row) => (
            &row.owner,
            &row.client_order_id,
            row.side,
            row.position_side,
            row.quantity,
        ),
        _ => return Err(CopySemanticError::ExecutionCommand),
    };
    if venue_order_id.is_empty() || quantity <= Decimal::ZERO {
        return Err(CopySemanticError::ExecutionCommand);
    }
    let mut collected = BTreeMap::<String, Fill>::new();
    for fill in previous_fills.iter().chain(
        snapshot
            .fills()
            .iter()
            .filter(|fill| fill.order_id == venue_order_id && fill.symbol == owner.symbol),
    ) {
        if fill.validate().is_err()
            || fill.order_id != venue_order_id
            || fill.symbol != owner.symbol
            || fill.side != side
            || matches!(fill.position_side, FieldState::Known(value)
                if value != PositionSide::Net && value != position_side)
            || fill
                .exchange_time_ms
                .is_none_or(|time| time < issued_at_ms || time > now_ms)
        {
            return Err(CopySemanticError::ExecutionRequest);
        }
        if let Some(existing) = collected.get(&fill.fill_id) {
            if existing != fill {
                return Err(CopySemanticError::ExecutionRequest);
            }
        } else {
            if collected.len() >= 4096 {
                return Err(CopySemanticError::ExecutionRequest);
            }
            collected.insert(fill.fill_id.clone(), fill.clone());
        }
    }
    let filled = collected.values().try_fold(Decimal::ZERO, |sum, fill| {
        sum.checked_add(fill.quantity)
            .ok_or(CopySemanticError::ExecutionRequest)
    })?;
    if filled > quantity {
        return Err(CopySemanticError::ExecutionRequest);
    }
    let mut open = false;
    for row in snapshot
        .open_orders()
        .iter()
        .filter(|row| row.family == NativeOrderFamily::UmOrder && row.symbol == owner.symbol)
    {
        let client_match = row.client_order_id == client_id.as_str();
        let venue_match = row.venue_order_id.as_deref() == Some(venue_order_id);
        if client_match || venue_match {
            // A signed identity conflict is not proof of absence.
            if !client_match || !venue_match || row.side != side {
                return Err(CopySemanticError::ExecutionRequest);
            }
            open = true;
        }
    }
    Ok(ExecutionFacts {
        fills: collected.into_values().collect(),
        fully_filled: filled == quantity,
        open,
    })
}
