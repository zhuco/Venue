use sha2::{Digest, Sha256};
use venue_domain::{
    CommandId, ExecutionCommand, MarketReduceCommand, NativeOrderFamily, OrderOwner, OrderPurpose,
    OrderSide, PositionSide,
};
use venue_runtime::{
    AccountPhysicalGateway, CommandState, StrategyBinding, account::AccountLanePriority,
};

use super::{NodeError, ProductionResident, ResidentReplay, persist_anchor};

/// Read-only signed evidence for one explicitly selected operator shutdown scope. Ownership on
/// orders has already been attached by the Host from the same account WAL; adapter ownership is
/// never accepted here.
#[derive(Clone, Debug)]
pub(crate) struct ControlShutdownSnapshot {
    pub connection_generation: u64,
    pub private_generation: u64,
    pub owned_open_orders: Vec<OwnedOpenOrder>,
    /// Positions are account facts, not order-owned facts. They are exposed only after the
    /// caller selected this exact registered symbol scope for an operator Flatten.
    pub symbol_legs: Vec<SignedPositionLeg>,
    /// An open order for this symbol that has no exact WAL route (or routes to another instance)
    /// makes cancellation and flattening fail closed instead of broadening the scope.
    pub has_scope_conflict: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct OwnedOpenOrder {
    pub family: NativeOrderFamily,
    pub client_order_id: CommandId,
    pub owner: OrderOwner,
}

#[derive(Clone, Debug)]
pub(crate) struct SignedPositionLeg {
    pub position_side: PositionSide,
    pub quantity: rust_decimal::Decimal,
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    /// Collects a complete signed account observation and narrows it to a registered strategy
    /// symbol. This method grants no mutation authority and deliberately leaves an unowned
    /// same-symbol order as a conflict.
    pub(crate) fn control_shutdown_snapshot(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<ControlShutdownSnapshot, NodeError> {
        let snapshot = self.refresh_signed_snapshot()?;
        let mut owned_open_orders = Vec::new();
        let mut has_scope_conflict = false;
        for order in snapshot
            .open_orders()
            .iter()
            .filter(|order| order.symbol == binding.key.symbol)
        {
            let Some(owner) = order.owner.as_ref() else {
                has_scope_conflict = true;
                continue;
            };
            if order.external || !binding.matches_owner(owner) {
                has_scope_conflict = true;
                continue;
            }
            let client_order_id = CommandId::new(order.client_order_id.clone())
                .map_err(|_| NodeError::ResidentRuntime)?;
            owned_open_orders.push(OwnedOpenOrder {
                family: order.family,
                client_order_id,
                owner: owner.clone(),
            });
        }
        let symbol_legs = snapshot
            .positions()
            .iter()
            .filter(|position| position.symbol == binding.key.symbol)
            .map(|position| SignedPositionLeg {
                position_side: position.position_side,
                quantity: position.quantity,
            })
            .collect();
        Ok(ControlShutdownSnapshot {
            connection_generation: snapshot.connection_generation(),
            private_generation: snapshot.private_generation(),
            owned_open_orders,
            symbol_legs,
            has_scope_conflict,
        })
    }

    /// The only physical shutdown handoff. It creates the normal durable resident semantic
    /// checkpoint, prepares the exact command in the shared Host WAL, and queues it as Critical
    /// reduction/cancellation work before using the one account writer.
    pub(crate) fn submit_control_shutdown_command(
        &mut self,
        binding: &StrategyBinding,
        command: ExecutionCommand,
    ) -> Result<CommandId, NodeError> {
        let command_id = command.command_id().clone();
        let replay = serde_json::to_vec(&ResidentReplay { command: &command })
            .map_err(|_| NodeError::ResidentRuntime)?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(super::resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        self.host
            .prepare_and_admit_operator(
                &mut self.runtime,
                binding,
                &applied,
                AccountLanePriority::Critical,
                command,
            )
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        let _follow_up = self
            .runtime
            .dispatch_next_with_host(&mut self.host)
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        Ok(command_id)
    }

    /// Re-reads only a command that already has a durable identity. Submitted or Unknown never
    /// become a new order from this path; Host asks the signed adapter to settle that same WAL
    /// entry instead.
    pub(crate) fn reconcile_control_shutdown_command(
        &mut self,
        command_id: &CommandId,
    ) -> Result<Option<CommandState>, NodeError> {
        self.host
            .reconcile_command_status(command_id)
            .map(|status| status.map(|status| status.state().clone()))
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })
    }
}

pub(crate) fn cancel_command(
    binding: &StrategyBinding,
    order: &OwnedOpenOrder,
    private_generation: u64,
) -> Result<ExecutionCommand, NodeError> {
    let target = format!(
        "{}:{}",
        std::str::from_utf8(family_tag(order.family)).map_err(|_| NodeError::ResidentRuntime)?,
        order.client_order_id.as_str()
    );
    let command_id = shutdown_id(
        "cancel",
        binding,
        private_generation.to_be_bytes(),
        target.as_bytes(),
    )?;
    Ok(ExecutionCommand::Cancel(venue_domain::CancelCommand {
        command_id,
        owner: order.owner.clone(),
        target_client_order_id: order.client_order_id.clone(),
    }))
}

pub(crate) fn reduce_command(
    binding: &StrategyBinding,
    private_generation: u64,
    leg: &SignedPositionLeg,
) -> Result<ExecutionCommand, NodeError> {
    if leg.quantity.is_zero() || private_generation == 0 {
        return Err(NodeError::ResidentRuntime);
    }
    let side = match leg.position_side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net if leg.quantity.is_sign_positive() => OrderSide::Sell,
        PositionSide::Net if leg.quantity.is_sign_negative() => OrderSide::Buy,
        PositionSide::Net => return Err(NodeError::ResidentRuntime),
    };
    let generation = private_generation.to_be_bytes();
    let side_tag = match leg.position_side {
        PositionSide::Long => b"long".as_slice(),
        PositionSide::Short => b"short".as_slice(),
        PositionSide::Net => b"net".as_slice(),
    };
    let command_id = shutdown_id("reduce", binding, generation, side_tag)?;
    let client_order_id = shutdown_id("reduce-client", binding, generation, side_tag)?;
    let risk_episode_id = shutdown_id("reduce-episode", binding, generation, side_tag)?;
    Ok(ExecutionCommand::MarketReduce(MarketReduceCommand {
        command_id,
        client_order_id,
        owner: OrderOwner {
            strategy_instance_id: binding.key.instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.key.account.exchange.as_str().to_owned(),
            account: binding.key.account.account.clone(),
            symbol: binding.key.symbol.clone(),
            purpose: OrderPurpose::ExposureTakeProfit,
        },
        position_side: leg.position_side,
        side,
        quantity: leg.quantity.abs(),
        risk_episode_id,
        position_generation: private_generation,
    }))
}

fn shutdown_id(
    kind: &str,
    binding: &StrategyBinding,
    generation: impl AsRef<[u8]>,
    target: impl AsRef<[u8]>,
) -> Result<CommandId, NodeError> {
    let mut digest = Sha256::new();
    digest.update(b"venue.node.control-shutdown.v1");
    for part in [
        kind.as_bytes(),
        binding.key.account.exchange.as_str().as_bytes(),
        binding.key.account.account.as_bytes(),
        binding.key.instance_id.as_bytes(),
        binding.key.symbol.to_string().as_bytes(),
        binding.run_id.as_bytes(),
        binding.config_digest.as_bytes(),
        generation.as_ref(),
        target.as_ref(),
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    use std::fmt::Write;
    let mut command_id = String::from("shutdown-");
    for byte in digest.finalize().iter().take(12) {
        write!(&mut command_id, "{byte:02x}").map_err(|_| NodeError::ResidentRuntime)?;
    }
    CommandId::new(command_id).map_err(|_| NodeError::ResidentRuntime)
}

const fn family_tag(family: NativeOrderFamily) -> &'static [u8] {
    match family {
        NativeOrderFamily::UmOrder => b"um_order",
        NativeOrderFamily::UmConditional => b"um_conditional",
        NativeOrderFamily::UmAlgo => b"um_algo",
    }
}
