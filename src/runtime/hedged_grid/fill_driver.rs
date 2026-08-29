use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::{
    domain::{FieldState, Fill},
    strategy::hedged_grid::{
        GridAction, GridDecision, HedgedGridError, HedgedGridState, OwnedGridFill,
    },
};

/// Shared liquidity routing for every hedged-grid deployment. A venue order being post-only is
/// not evidence about an execution: only the normalized private fill may authorize maker-driven
/// strategy transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GridFillRoute {
    MakerDrive,
    TakerInventoryOnly,
    AwaitLiquidityEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GridFillProjection {
    SignedInventoryIncluded,
    ProjectStreamInventory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GridFillApplication {
    Noop,
    Rolling(Vec<GridAction>),
    ReanchorPending,
    TakerInventoryOnly,
    AwaitLiquidityEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum TerminalExecutionError {
    #[error("owned order has no execution evidence")]
    Empty,
    #[error("execution evidence has conflicting identity")]
    Identity,
    #[error("execution liquidity is not proven maker")]
    Liquidity,
    #[error("multiple executions lack a strict unique venue sequence")]
    Sequence,
    #[error("execution quantity is incomplete")]
    IncompleteQuantity,
    #[error("execution quantity conflicts with the owned order")]
    QuantityConflict,
    #[error("execution quantity arithmetic overflowed")]
    Arithmetic,
}

pub(crate) fn route_grid_fill(fill: &Fill) -> GridFillRoute {
    route_maker_evidence(&fill.maker)
}

pub(crate) fn route_maker_evidence(maker: &FieldState<bool>) -> GridFillRoute {
    match maker {
        FieldState::Known(true) => GridFillRoute::MakerDrive,
        FieldState::Known(false) => GridFillRoute::TakerInventoryOnly,
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => GridFillRoute::AwaitLiquidityEvidence,
    }
}

/// Selects the execution that completed one owned order. Multi-execution orders require a
/// venue-native monotonic sequence; timestamps and lexical fill ids are deliberately rejected as
/// ordering evidence. Identical duplicate rows are idempotent and do not inflate the quantity.
pub(crate) fn terminal_owned_execution<'a>(
    fills: &[&'a Fill],
    expected_quantity: Decimal,
) -> Result<&'a Fill, TerminalExecutionError> {
    if fills.is_empty() {
        return Err(TerminalExecutionError::Empty);
    }
    if !expected_quantity.is_sign_positive() || expected_quantity.is_zero() {
        return Err(TerminalExecutionError::QuantityConflict);
    }

    let mut unique = BTreeMap::<&str, &'a Fill>::new();
    for fill in fills {
        if route_grid_fill(fill) != GridFillRoute::MakerDrive {
            return Err(TerminalExecutionError::Liquidity);
        }
        match unique.get(fill.fill_id.as_str()) {
            None => {
                unique.insert(fill.fill_id.as_str(), fill);
            }
            Some(existing) if *existing == *fill => {}
            Some(_) => return Err(TerminalExecutionError::Identity),
        }
    }

    let first = unique
        .values()
        .next()
        .copied()
        .ok_or(TerminalExecutionError::Empty)?;
    let mut quantity = Decimal::ZERO;
    for fill in unique.values().copied() {
        if fill.order_id != first.order_id
            || fill.symbol != first.symbol
            || fill.side != first.side
            || fill.position_side != first.position_side
        {
            return Err(TerminalExecutionError::Identity);
        }
        quantity = quantity
            .checked_add(fill.quantity)
            .ok_or(TerminalExecutionError::Arithmetic)?;
    }
    if quantity < expected_quantity {
        return Err(TerminalExecutionError::IncompleteQuantity);
    }
    if quantity > expected_quantity {
        return Err(TerminalExecutionError::QuantityConflict);
    }
    if unique.len() == 1 {
        return Ok(first);
    }

    let mut sequences = BTreeSet::new();
    let mut terminal = None;
    for fill in unique.values().copied() {
        let FieldState::Known(sequence) = fill.execution_sequence else {
            return Err(TerminalExecutionError::Sequence);
        };
        if !sequences.insert(sequence) {
            return Err(TerminalExecutionError::Sequence);
        }
        if terminal.is_none_or(|(current, _): (u64, &Fill)| sequence > current) {
            terminal = Some((sequence, fill));
        }
    }
    terminal
        .map(|(_, fill)| fill)
        .ok_or(TerminalExecutionError::Sequence)
}

/// Applies the same reducer transition for the realtime and signed-recovery entrances. Liquidity
/// violations still consume the owned fill/inventory fact, but never produce rolling or reanchor
/// actions; the deployment shell decides how to fence and request signed reconciliation.
pub(crate) fn apply_owned_grid_fill(
    state: &mut HedgedGridState,
    fill: OwnedGridFill,
    projection: GridFillProjection,
) -> Result<GridFillApplication, HedgedGridError> {
    let route = route_maker_evidence(&fill.maker);
    let decision = match projection {
        GridFillProjection::SignedInventoryIncluded => state.observe_owned_fill(fill)?,
        GridFillProjection::ProjectStreamInventory => state.observe_stream_owned_fill(fill)?,
    };
    match route {
        GridFillRoute::TakerInventoryOnly => Ok(GridFillApplication::TakerInventoryOnly),
        GridFillRoute::AwaitLiquidityEvidence => Ok(GridFillApplication::AwaitLiquidityEvidence),
        GridFillRoute::MakerDrive => match decision {
            GridDecision::Noop => Ok(GridFillApplication::Noop),
            GridDecision::Blocked => Err(HedgedGridError::Phase),
            GridDecision::Actions(actions) => {
                if actions
                    .iter()
                    .any(|action| matches!(action, GridAction::ReanchorAtFill { .. }))
                {
                    if actions.len() != 1 {
                        return Err(HedgedGridError::Phase);
                    }
                    // The reducer has produced ReanchorPending. The deployment shell must fsync
                    // that state before it advances to Rebuilding under its writer guard.
                    Ok(GridFillApplication::ReanchorPending)
                } else if actions
                    .iter()
                    .all(|action| matches!(action, GridAction::Dispatch(_)))
                {
                    Ok(GridFillApplication::Rolling(actions))
                } else {
                    Err(HedgedGridError::Phase)
                }
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde_json::json;

    use crate::{
        domain::{FieldState, Fill, OrderSide, Price},
        exchange::{
            bitget::parse_private_fill_message,
            gate::{
                parse_contract_rules as parse_gate_contract_rules, parse_fill as parse_gate_fill,
                parse_fill_client_order_id,
            },
            grid::{GridPrivateEvent, GridVenueFill, binance_private_event},
        },
        runtime::hedged_grid_live::{client_order_id, parse_grid_client_order_id},
        strategy::hedged_grid::{
            GridEpoch, GridInventory, GridPosition, HedgedGridBinding, HedgedGridParams,
            InventoryRecoveryState,
        },
    };

    use super::*;

    fn fill(maker: FieldState<bool>) -> Result<Fill, Box<dyn std::error::Error>> {
        Ok(Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "fill_1".to_owned(),
            order_id: "order_1".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: FieldState::Missing,
            quantity: Decimal::ONE,
            price: Price::new(Decimal::ONE)?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker,
            exchange_time_ms: Some(1),
        })
    }

    fn execution(
        fill_id: &str,
        sequence: FieldState<u64>,
        quantity: Decimal,
        price: Decimal,
    ) -> Result<Fill, Box<dyn std::error::Error>> {
        let mut fill = fill(FieldState::Known(true))?;
        fill.fill_id = fill_id.to_owned();
        fill.execution_sequence = sequence;
        fill.quantity = quantity;
        fill.price = Price::new(price)?;
        Ok(fill)
    }

    fn grid_state(
        initial_quantity: Decimal,
        recovered_quantity: Option<Decimal>,
    ) -> Result<HedgedGridState, Box<dyn std::error::Error>> {
        let binding = HedgedGridBinding {
            strategy_instance_id: "grid".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "gate".to_owned(),
            account: "usdt_futures".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            config_version: "test".to_owned(),
            owner_scope: "grid_doge".to_owned(),
        };
        let mut state = HedgedGridState::new_with_params(
            binding,
            HedgedGridParams::fixed_release("USDT".parse()?, 3)?,
        )?;
        let inventory =
            |generation, quantity| -> Result<GridInventory, Box<dyn std::error::Error>> {
                Ok(GridInventory {
                    private_generation: generation,
                    private_observed_at_ms: generation * 100,
                    mark_price: Price::new(Decimal::new(100, 0))?,
                    long_quantity: quantity,
                    short_quantity: quantity,
                })
            };
        let _ = state.observe_inventory(inventory(1, initial_quantity)?)?;
        let _ = state.install_epoch(GridEpoch {
            epoch: 1,
            anchor_price: Price::new(Decimal::new(100, 0))?,
            step: Price::new(Decimal::new(2, 1))?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        })?;
        if let Some(quantity) = recovered_quantity {
            let _ = state.observe_inventory(inventory(2, quantity)?)?;
        }
        Ok(state)
    }

    #[test]
    fn only_known_maker_drives_grid() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            route_grid_fill(&fill(FieldState::Known(true))?),
            GridFillRoute::MakerDrive
        );
        assert_eq!(
            route_grid_fill(&fill(FieldState::Known(false))?),
            GridFillRoute::TakerInventoryOnly
        );
        assert_eq!(
            route_grid_fill(&fill(FieldState::Missing)?),
            GridFillRoute::AwaitLiquidityEvidence
        );
        assert_eq!(
            route_maker_evidence(&FieldState::Unavailable {
                reason: crate::domain::UnknownReason::Ambiguous,
            }),
            GridFillRoute::AwaitLiquidityEvidence
        );
        Ok(())
    }

    #[test]
    fn terminal_execution_uses_native_sequence_not_timestamp_or_lexical_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = execution(
            "9",
            FieldState::Known(9),
            Decimal::new(29, 0),
            Decimal::new(90, 0),
        )?;
        let mut terminal = execution(
            "10",
            FieldState::Known(10),
            Decimal::new(26, 0),
            Decimal::new(110, 0),
        )?;
        terminal.exchange_time_ms = first.exchange_time_ms;

        let selected = terminal_owned_execution(&[&terminal, &first, &first], Decimal::new(55, 0))?;
        assert_eq!(selected.fill_id, "10");
        assert_eq!(selected.price, terminal.price);
        Ok(())
    }

    #[test]
    fn multiple_executions_fail_closed_without_unique_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = execution(
            "opaque-a",
            FieldState::Unavailable {
                reason: crate::domain::UnknownReason::ParseFailure,
            },
            Decimal::new(2, 0),
            Decimal::new(90, 0),
        )?;
        let second = execution(
            "opaque-b",
            FieldState::Known(2),
            Decimal::new(3, 0),
            Decimal::new(110, 0),
        )?;
        assert_eq!(
            terminal_owned_execution(&[&first, &second], Decimal::new(5, 0)),
            Err(TerminalExecutionError::Sequence)
        );
        Ok(())
    }

    #[test]
    fn signed_and_stream_shells_converge_on_same_reanchor_price()
    -> Result<(), Box<dyn std::error::Error>> {
        let base = grid_state(Decimal::new(10, 2), Some(Decimal::new(15, 2)))?;
        assert!(matches!(
            base.inventory_recovery,
            InventoryRecoveryState::AwaitingNextOwnedFill { .. }
        ));
        let source = base
            .owned_orders
            .keys()
            .find(|key| key.position == GridPosition::Long)
            .cloned()
            .ok_or("missing owned order")?;
        let owned_fill = OwnedGridFill {
            fill_id: "10".to_owned(),
            private_generation: 3,
            source_order: source,
            fill_price: Price::new(Decimal::new(12345, 2))?,
            complete: true,
            maker: FieldState::Known(true),
        };
        let mut signed = base.clone();
        let mut stream = base;
        assert_eq!(
            apply_owned_grid_fill(
                &mut signed,
                owned_fill.clone(),
                GridFillProjection::SignedInventoryIncluded,
            )?,
            GridFillApplication::ReanchorPending
        );
        assert_eq!(
            apply_owned_grid_fill(
                &mut stream,
                owned_fill,
                GridFillProjection::ProjectStreamInventory,
            )?,
            GridFillApplication::ReanchorPending
        );
        assert_eq!(signed.inventory_recovery, stream.inventory_recovery);
        assert!(matches!(
            signed.inventory_recovery,
            InventoryRecoveryState::ReanchorPending { ref fill_price, .. }
                if fill_price.value() == Decimal::new(12345, 2)
        ));

        // A crash after fill acceptance must recover the exact pending identity/price before the
        // writer advances to Rebuilding; neither entrance may collapse these two durable states.
        signed = serde_json::from_str(&serde_json::to_string(&signed)?)?;
        stream = serde_json::from_str(&serde_json::to_string(&stream)?)?;
        signed.begin_reanchor_rebuild()?;
        stream.begin_reanchor_rebuild()?;
        assert!(matches!(
            signed.inventory_recovery,
            InventoryRecoveryState::Rebuilding { ref fill_price, .. }
                if fill_price.value() == Decimal::new(12345, 2)
        ));

        let authoritative = GridInventory {
            private_generation: 3,
            private_observed_at_ms: 300,
            mark_price: Price::new(Decimal::new(124, 0))?,
            long_quantity: Decimal::new(20, 2),
            short_quantity: Decimal::new(15, 2),
        };
        let _ = signed.observe_inventory(authoritative.clone())?;
        let _ = stream.observe_inventory(authoritative)?;
        assert_eq!(signed, stream);
        Ok(())
    }

    #[test]
    fn fill_in_arming_generation_rolls_and_only_later_generation_reanchors()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = grid_state(Decimal::new(10, 2), Some(Decimal::new(15, 2)))?;
        assert!(matches!(
            state.inventory_recovery,
            InventoryRecoveryState::AwaitingNextOwnedFill {
                armed_generation: 2
            }
        ));
        let first_source = state
            .owned_orders
            .keys()
            .find(|key| key.position == GridPosition::Long)
            .cloned()
            .ok_or("missing first owned order")?;
        let first = apply_owned_grid_fill(
            &mut state,
            OwnedGridFill {
                fill_id: "arming-fill".to_owned(),
                private_generation: 2,
                source_order: first_source,
                fill_price: Price::new(Decimal::new(101, 0))?,
                complete: true,
                maker: FieldState::Known(true),
            },
            GridFillProjection::SignedInventoryIncluded,
        )?;
        let GridFillApplication::Rolling(actions) = first else {
            return Err("arming-generation fill did not use ordinary rolling".into());
        };
        assert!(matches!(
            state.inventory_recovery,
            InventoryRecoveryState::AwaitingNextOwnedFill {
                armed_generation: 2
            }
        ));
        for action in actions {
            let GridAction::Dispatch(transaction) = action else {
                return Err("unexpected rolling action".into());
            };
            let _ = state.settle_transaction(&transaction.id, true)?;
        }

        let later_source = state
            .owned_orders
            .keys()
            .find(|key| key.position == GridPosition::Short)
            .cloned()
            .ok_or("missing later owned order")?;
        assert_eq!(
            apply_owned_grid_fill(
                &mut state,
                OwnedGridFill {
                    fill_id: "later-fill".to_owned(),
                    private_generation: 3,
                    source_order: later_source,
                    fill_price: Price::new(Decimal::new(102, 0))?,
                    complete: true,
                    maker: FieldState::Known(true),
                },
                GridFillProjection::SignedInventoryIncluded,
            )?,
            GridFillApplication::ReanchorPending
        );
        Ok(())
    }

    #[test]
    fn signed_and_stream_shells_preserve_normal_rolling_action_order_and_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let base = grid_state(Decimal::new(15, 2), None)?;
        assert_eq!(base.inventory_recovery, InventoryRecoveryState::Inactive);
        let source = base
            .owned_orders
            .keys()
            .find(|key| key.position == GridPosition::Long)
            .cloned()
            .ok_or("missing owned order")?;
        let owned_fill = OwnedGridFill {
            fill_id: "11".to_owned(),
            private_generation: 2,
            source_order: source,
            fill_price: Price::new(Decimal::new(998, 1))?,
            complete: true,
            maker: FieldState::Known(true),
        };
        let mut signed = base.clone();
        let mut stream = base;
        let signed_actions = match apply_owned_grid_fill(
            &mut signed,
            owned_fill.clone(),
            GridFillProjection::SignedInventoryIncluded,
        )? {
            GridFillApplication::Rolling(actions) => actions,
            other => return Err(format!("unexpected signed application: {other:?}").into()),
        };
        let stream_actions = match apply_owned_grid_fill(
            &mut stream,
            owned_fill,
            GridFillProjection::ProjectStreamInventory,
        )? {
            GridFillApplication::Rolling(actions) => actions,
            other => return Err(format!("unexpected stream application: {other:?}").into()),
        };
        assert_eq!(signed_actions, stream_actions);

        let authoritative = GridInventory {
            private_generation: 2,
            private_observed_at_ms: 200,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::new(20, 2),
            short_quantity: Decimal::new(15, 2),
        };
        let _ = signed.observe_inventory(authoritative.clone())?;
        let _ = stream.observe_inventory(authoritative)?;
        assert_eq!(signed, stream);
        Ok(())
    }

    #[test]
    fn concrete_adapters_converge_through_shared_fill_reducer()
    -> Result<(), Box<dyn std::error::Error>> {
        let base = grid_state(Decimal::new(15, 2), None)?;
        let source = base
            .owned_orders
            .keys()
            .find(|key| {
                key.position == GridPosition::Long
                    && key.role == crate::strategy::hedged_grid::GridOrderRole::Open
                    && key.level == 1
            })
            .cloned()
            .ok_or("missing shared source order")?;
        let source_intent = base
            .owned_orders
            .get(&source)
            .cloned()
            .ok_or("missing shared source intent")?;
        let owned_client_id = client_order_id(&source)?;
        assert_eq!(owned_client_id.as_str(), "hgo_e1_long_open_l1");

        // Binance now preserves the same normalized realtime fill contract as Gate and Bitget;
        // signed userTrades remains only the recovery entrance when the stream is incomplete.
        let binance_raw = r#"{"e":"ORDER_TRADE_UPDATE","E":1700000000000,"T":1700000000000,"o":{"s":"DOGEUSDT","c":"hgo_e1_long_open_l1","x":"TRADE","S":"BUY","ps":"LONG","t":10,"i":1,"l":"0.05","L":"99.8","m":true}}"#;
        let GridPrivateEvent::Fill {
            fill,
            client_order_id,
            ..
        } = binance_private_event(binance_raw.to_owned(), &"DOGE/USDT".parse()?)?
        else {
            return Err("missing Binance realtime fill".into());
        };
        let binance = GridVenueFill {
            fill,
            client_order_id,
        };

        let gate_rules = parse_gate_contract_rules(
            &json!({
                "name":"DOGE_USDT", "quanto_multiplier":"0.01",
                "order_size_min":"1", "order_price_round":"0.00001",
                "enable_decimal":false, "in_delisting":false, "status":"trading"
            }),
            "DOGE/USDT".parse()?,
            1,
        )?;
        let gate_row = json!({
            "id":"10", "order_id":"1", "contract":"DOGE_USDT", "size":"5",
            "price":"99.8", "role":"maker", "create_time_ms":"1700000000000",
            "text":"t-hgo_e1_long_open_l1"
        });
        let gate = GridVenueFill {
            fill: parse_gate_fill(&gate_row, &"DOGE/USDT".parse()?, &gate_rules)?,
            client_order_id: parse_fill_client_order_id(&gate_row)?,
        };

        let bitget_row = json!({
            "execId":"10", "orderId":"1", "clientOid":"hgo_e1_long_open_l1",
            "category":"USDT-FUTURES", "symbol":"DOGEUSDT", "side":"buy",
            "holdSide":"long", "execQty":"0.05", "execPrice":"99.8",
            "tradeScope":"maker", "updatedTime":"1700000000000"
        });
        let bitget = parse_private_fill_message(
            &json!({"arg":{"topic":"fill"},"data":[bitget_row]}).to_string(),
            &"DOGE/USDT".parse()?,
        )?
        .pop()
        .map(|fill| GridVenueFill {
            fill: fill.fill,
            client_order_id: fill.client_order_id,
        })
        .ok_or("missing Bitget fill event")?;

        assert_eq!(binance, gate);
        assert_eq!(gate, bitget);
        assert_eq!(binance.fill.price.value(), Decimal::new(998, 1));
        assert_eq!(binance.fill.maker, FieldState::Known(true));
        assert_eq!(
            binance.client_order_id,
            FieldState::Known(owned_client_id.as_str().to_owned())
        );

        let authoritative = GridInventory {
            private_generation: 2,
            private_observed_at_ms: 200,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::new(20, 2),
            short_quantity: Decimal::new(15, 2),
        };
        let mut outcomes = Vec::new();
        for (index, record) in [binance, gate, bitget].into_iter().enumerate() {
            let FieldState::Known(identity) = record.client_order_id else {
                return Err("missing normalized client identity".into());
            };
            assert_eq!(parse_grid_client_order_id(&identity)?, source);
            assert_eq!(
                terminal_owned_execution(&[&record.fill], source_intent.quantity)?,
                &record.fill
            );
            let mut state = base.clone();
            let projection = if index < 2 {
                let _ = state.observe_inventory(authoritative.clone())?;
                GridFillProjection::SignedInventoryIncluded
            } else {
                GridFillProjection::ProjectStreamInventory
            };
            let application = apply_owned_grid_fill(
                &mut state,
                OwnedGridFill {
                    fill_id: record.fill.fill_id,
                    private_generation: 2,
                    source_order: source.clone(),
                    fill_price: record.fill.price,
                    complete: true,
                    maker: record.fill.maker,
                },
                projection,
            )?;
            if index == 2 {
                let _ = state.observe_inventory(authoritative.clone())?;
            }
            outcomes.push((application, state));
        }
        assert_eq!(outcomes[0], outcomes[1]);
        assert_eq!(outcomes[1], outcomes[2]);
        Ok(())
    }
}
