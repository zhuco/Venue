use venue_domain::domain::{ExecutionCommand, OrderSide, PositionSide};
use venue_gateway_api::GatewayBinding;

use crate::{AccountGatewayResult, AccountPhysicalGateway};

/// Immutable database routing evidence for an exact cancel. Native IDs are never inferred from
/// price, symbol or whichever order happens to be visible in the current snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DurableExecutionContext {
    pub target_command: Option<ExecutionCommand>,
    pub target_native_order_id: Option<String>,
}

/// Adapter-normalized public rules and reference observation. All quantities are base units;
/// native contract lots and ticker names remain inside each adapter.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DurableMarketFacts {
    pub binding: GatewayBinding,
    pub metadata: venue_domain::domain::InstrumentMetadata,
    pub reference_price: venue_domain::domain::Price,
    pub observed_at_ms: u64,
    pub maximum_quantity: Option<rust_decimal::Decimal>,
    pub maximum_price: Option<venue_domain::domain::Price>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DurableOrderObservation {
    pub client_order_id: String,
    pub native_order_id: String,
    pub state: venue_domain::OrderState,
    pub filled_quantity: rust_decimal::Decimal,
}

/// Transport boundary for a command already committed as Sending in PostgreSQL. This does not
/// create a Host, writer lease or local journal. The caller owns account serialization and must
/// only reconcile the original identity after any uncertain send, including process restart.
pub trait DurableAccountGateway: AccountPhysicalGateway {
    fn execute_committed(&mut self, command: &ExecutionCommand) -> AccountGatewayResult;

    /// Queries the original native client identity and verifies the full immutable semantics.
    /// Accepted here means signed reconciliation, unlike an identity-only transport ACK.
    fn reconcile_committed(&mut self, _command: &ExecutionCommand) -> AccountGatewayResult {
        AccountGatewayResult::Unknown
    }

    fn execute_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        _context: &DurableExecutionContext,
    ) -> AccountGatewayResult {
        self.execute_committed(command)
    }

    fn reconcile_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        _context: &DurableExecutionContext,
    ) -> AccountGatewayResult {
        self.reconcile_committed(command)
    }
}

/// Checks all visible close reservations, including external orders. Native quantity/price rules
/// and permissions must still be checked by the adapter immediately before sending.
pub fn validate_signed_durable_command(
    binding: &GatewayBinding,
    command: &ExecutionCommand,
    snapshot: &crate::SignedAccountSnapshot,
    now_ms: u64,
) -> bool {
    use rust_decimal::Decimal;
    if !validate_durable_command(binding, command)
        || snapshot.binding() != binding
        || now_ms < snapshot.observed_at_ms()
        || now_ms.saturating_sub(snapshot.observed_at_ms()) > 5_000
    {
        return false;
    }
    let net = binding.venue == venue_gateway_api::VenueId::Hyperliquid;
    if (snapshot.position_mode() == crate::SignedAccountPositionMode::Net) != net {
        return false;
    }
    let (side, leg, quantity) = match command {
        ExecutionCommand::PlaceLimit(order) if order.reduce_only => {
            (order.side, order.position_side, order.quantity)
        }
        ExecutionCommand::MarketReduce(order) => (order.side, order.position_side, order.quantity),
        ExecutionCommand::StopMarketFullPosition(order) => {
            (order.side, order.position_side, order.quantity)
        }
        ExecutionCommand::PlaceLimit(order) if net => {
            return net_entry_allowed(snapshot, binding, order.side);
        }
        ExecutionCommand::PlaceMarket(order) if net => {
            return net_entry_allowed(snapshot, binding, order.side);
        }
        ExecutionCommand::PlaceLimit(_)
        | ExecutionCommand::PlaceMarket(_)
        | ExecutionCommand::Cancel(_) => return true,
        _ => return false,
    };
    let positions: Vec<_> = snapshot
        .positions()
        .iter()
        .filter(|p| p.symbol == binding.symbol && p.position_side == leg)
        .collect();
    let [position] = positions.as_slice() else {
        return false;
    };
    if quantity <= Decimal::ZERO
        || position.quantity.is_zero()
        || (net
            && ((position.quantity > Decimal::ZERO && side != OrderSide::Sell)
                || (position.quantity < Decimal::ZERO && side != OrderSide::Buy)))
    {
        return false;
    }
    // Native conditional reductions do not reserve executable regular-order quantity. Each must
    // independently be bounded by inventory and enforce native reduce-only when later triggered.
    if matches!(command, ExecutionCommand::StopMarketFullPosition(_)) {
        return quantity <= position.quantity.abs();
    }
    let mut reserved = Decimal::ZERO;
    for order in snapshot.open_orders().iter().filter(|o| {
        o.symbol == binding.symbol
            && o.position_side == leg
            && o.reduce_only
            && o.family == venue_domain::NativeOrderFamily::UmOrder
    }) {
        let Some(filled) = order.filled_quantity else {
            return false;
        };
        let Some(remaining) = order
            .quantity
            .checked_sub(filled)
            .filter(|q| *q >= Decimal::ZERO)
        else {
            return false;
        };
        let Some(next) = reserved.checked_add(remaining) else {
            return false;
        };
        reserved = next;
    }
    reserved
        .checked_add(quantity)
        .is_some_and(|total| total <= position.quantity.abs())
}

fn net_entry_allowed(
    snapshot: &crate::SignedAccountSnapshot,
    binding: &GatewayBinding,
    side: OrderSide,
) -> bool {
    let positions: Vec<_> = snapshot
        .positions()
        .iter()
        .filter(|p| p.symbol == binding.symbol)
        .collect();
    match positions.as_slice() {
        [] => true,
        [p] if p.position_side == PositionSide::Net => {
            p.quantity.is_zero() || (p.quantity.is_sign_positive() == (side == OrderSide::Buy))
        }
        _ => false,
    }
}

pub fn validate_durable_context(
    command: &ExecutionCommand,
    context: &DurableExecutionContext,
) -> bool {
    let ExecutionCommand::Cancel(cancel) = command else {
        return context.target_command.is_none() && context.target_native_order_id.is_none();
    };
    let Some(target) = &context.target_command else {
        return false;
    };
    let owner = target.mutation_owner();
    target.native_client_id() == Some(&cancel.target_client_order_id)
        && owner.account == cancel.owner.account
        && owner.exchange == cancel.owner.exchange
        && owner.symbol == cancel.owner.symbol
        && owner.strategy_instance_id == cancel.owner.strategy_instance_id
        && owner.run_id == cancel.owner.run_id
        && owner.purpose == cancel.owner.purpose
        && target.validate_persisted_shape().is_ok()
        && context
            .target_native_order_id
            .as_ref()
            .is_some_and(|id| !id.trim().is_empty())
}

/// Scope checking is repeated at the adapter boundary, independently of the database caller.
pub fn validate_durable_command(binding: &GatewayBinding, command: &ExecutionCommand) -> bool {
    let owner = command.mutation_owner();
    binding.validate().is_ok()
        && venue_domain::domain::CommandId::new(command.command_id().as_str()).is_ok()
        && command
            .native_client_id()
            .is_none_or(|id| venue_domain::domain::CommandId::new(id.as_str()).is_ok())
        && match command {
            ExecutionCommand::Cancel(cancel) => {
                venue_domain::domain::CommandId::new(cancel.target_client_order_id.as_str()).is_ok()
            }
            _ => true,
        }
        && command.validate_persisted_shape().is_ok()
        && owner.exchange == binding.venue.as_str()
        && owner.account == binding.trading_account_id
        && owner.symbol == binding.symbol
        && match command {
            ExecutionCommand::PlaceLimit(order) => {
                (binding.venue == venue_gateway_api::VenueId::Hyperliquid)
                    == (order.position_side == PositionSide::Net)
            }
            ExecutionCommand::MarketReduce(order) => {
                (binding.venue == venue_gateway_api::VenueId::Hyperliquid)
                    == (order.position_side == PositionSide::Net)
            }
            ExecutionCommand::PlaceMarket(order) => {
                (binding.venue == venue_gateway_api::VenueId::Hyperliquid)
                    == (order.position_side == PositionSide::Net)
            }
            ExecutionCommand::StopMarketFullPosition(order) => {
                (binding.venue == venue_gateway_api::VenueId::Hyperliquid)
                    == (order.position_side == PositionSide::Net)
            }
            ExecutionCommand::Cancel(_) => true,
            _ => false,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SignedAccountPositionFact, SignedAccountPositionMode, SignedAccountSnapshot};
    use rust_decimal::Decimal;
    use venue_domain::domain::{
        CommandId, LimitTimeInForce, OrderCommand, OrderOwner, OrderPurpose, Price, Symbol,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    fn close() -> Result<(GatewayBinding, ExecutionCommand), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            Symbol::new("BTC", "USDT")?,
        )?;
        let command = ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new("cmd1")?,
            client_order_id: CommandId::new("client1")?,
            owner: OrderOwner {
                strategy_instance_id: "strategy1".into(),
                run_id: "run1".into(),
                exchange: "bybit".into(),
                account: binding.trading_account_id.clone(),
                symbol: binding.symbol.clone(),
                purpose: OrderPurpose::Reduce,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::from(100))?,
            time_in_force: LimitTimeInForce::PostOnly,
            reduce_only: true,
        });
        Ok((binding, command))
    }
    #[test]
    fn wrong_account_or_venue_cannot_enter_durable_adapter()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut binding, command) = close()?;
        assert!(validate_durable_command(&binding, &command));
        binding.venue = VenueId::Gate;
        assert!(!validate_durable_command(&binding, &command));
        binding.venue = VenueId::Bybit;
        binding.trading_account_id = "00000000-0000-4000-8000-000000000002".into();
        assert!(!validate_durable_command(&binding, &command));
        Ok(())
    }
    #[test]
    fn reduction_requires_fresh_position_and_counts_external_reservations()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, command) = close()?;
        let position = SignedAccountPositionFact {
            symbol: binding.symbol.clone(),
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            entry_price: None,
            mark_price: None,
        };
        let make = |orders| {
            SignedAccountSnapshot::complete(
                binding.clone(),
                1000,
                1,
                1,
                1,
                SignedAccountPositionMode::Hedge,
                orders,
                vec![position.clone()],
                "cursor".into(),
                vec![],
            )
        };
        assert!(validate_signed_durable_command(
            &binding,
            &command,
            &make(vec![])?,
            1001
        ));
        assert!(!validate_signed_durable_command(
            &binding,
            &command,
            &make(vec![])?,
            7000
        ));
        let external = crate::SignedAccountOrderFact {
            client_order_id: "external".into(),
            venue_order_id: Some("1".into()),
            symbol: binding.symbol.clone(),
            family: venue_domain::domain::NativeOrderFamily::UmOrder,
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Some(Decimal::from(100)),
            time_in_force: Some(LimitTimeInForce::PostOnly),
            created_at_ms: Some(1),
            reduce_only: true,
            owner: None,
            external: true,
            state: Some(venue_domain::domain::OrderState::New),
            filled_quantity: Some(Decimal::ZERO),
        };
        assert!(!validate_signed_durable_command(
            &binding,
            &command,
            &make(vec![external])?,
            1001
        ));
        Ok(())
    }
}
