use crate::{
    domain::{
        CancelCommand, Instrument, Order, OrderCommand, Position, PositionSide,
        StopMarketCloseAllCommand, StopMarketFullPositionCommand,
    },
    exchange::{binance::PrivateError, binance::PrivateRest, binance_private},
    risk::{
        AccountRiskView, HardRiskLimits, RiskApproval, RiskError, authorize_entry,
        authorize_reduction, authorize_stop_market_close_all, authorize_stop_market_full_position,
    },
};

use super::{
    CanaryRunState, CanaryRunStateError, CommandJournal, CommandJournalError, CommandState,
    DispatchGuard, EmergencyFlattenAuthorization, GateDecision, GateError, ProbeGateError,
    ProbeKind, ProbePermit, RecoveryCancelAuthorization, RecoveryDispatchGuard,
    RecoveryReduceAuthorization, RecoveryWriterError, WriterLeaseError, WriterSession,
    recovery_writer::{validate_recovery_cancel_dispatch, validate_recovery_reduce_dispatch},
    validate_canary_permit, validate_emergency_flatten_permit, validate_probe_permit,
};

/// Dispatches the first independently fenced place/cancel probe as GTX. The durable Canary run
/// state and command WAL advance before the network call because even a post-only order may fill.
pub struct PostOnlyProbePreflight<'a> {
    pub permit: ProbePermit,
    pub writer: &'a WriterSession,
    pub run: &'a mut CanaryRunState,
    pub now_ms: u64,
    pub dispatch: &'a DispatchGuard,
}

pub fn submit_post_only_probe(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: OrderCommand,
    preflight: PostOnlyProbePreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let _dispatch = preflight.dispatch;
    validate_probe_permit(
        preflight.permit,
        ProbeKind::PostOnlyPlaceCancel,
        &command,
        preflight.now_ms,
    )?;
    preflight
        .run
        .validate_entry_context(preflight.writer, &command, preflight.now_ms)?;
    match commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone())
    {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Submitted) | Some(CommandState::Unknown { .. }) => {
            return Err(ExecutionError::Pending);
        }
        Some(CommandState::Prepared) | None => {}
    }
    commands.prepare_place(command.clone())?;
    preflight
        .run
        .entry_submitted(preflight.permit.command_sha256_hex(), preflight.now_ms)?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    match client.place_limit_post_only(&command) {
        Ok(payload) => match binance_private::parse_order(&payload, &command.owner.symbol) {
            Ok(order) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Accepted {
                        venue_order_id: order.order_id.clone(),
                    },
                )?;
                Ok(ExecutionReceipt::ProbeAccepted { order })
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "probe_response_parse_failed".to_owned(),
                    },
                )?;
                preflight.run.freeze_unknown()?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            preflight.run.freeze_unknown()?;
            Err(ExecutionError::Venue(source))
        }
    }
}

/// Dispatches the deliberate minimum-size fill used to prove exchange-resident protection. Its
/// permit additionally requires independently recorded place, cancel, and reconciliation facts.
pub fn submit_protection_probe_entry(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: OrderCommand,
    preflight: PostOnlyProbePreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let _dispatch = preflight.dispatch;
    validate_probe_permit(
        preflight.permit,
        ProbeKind::ProtectionEntry,
        &command,
        preflight.now_ms,
    )?;
    preflight
        .run
        .validate_entry_context(preflight.writer, &command, preflight.now_ms)?;
    match commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone())
    {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Submitted) | Some(CommandState::Unknown { .. }) => {
            return Err(ExecutionError::Pending);
        }
        Some(CommandState::Prepared) | None => {}
    }
    commands.prepare_place(command.clone())?;
    preflight
        .run
        .entry_submitted(preflight.permit.command_sha256_hex(), preflight.now_ms)?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    match client.place_limit_immediate_or_cancel(&command) {
        Ok(payload) => match binance_private::parse_order(&payload, &command.owner.symbol) {
            Ok(order) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Accepted {
                        venue_order_id: order.order_id.clone(),
                    },
                )?;
                Ok(ExecutionReceipt::ProbeAccepted { order })
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "protection_probe_response_parse_failed".to_owned(),
                    },
                )?;
                preflight.run.freeze_unknown()?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            preflight.run.freeze_unknown()?;
            Err(ExecutionError::Venue(source))
        }
    }
}

/// Performs one limit-entry mutation after the command is durable and hard-risk approved.
/// It never retries a submitted mutation because that would create duplicate entry risk.
pub fn submit_limit_entry(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: OrderCommand,
    preflight: EntryPreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let existing_state = commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone());
    match existing_state {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Submitted) | Some(CommandState::Unknown { .. }) => {
            return Err(ExecutionError::Pending);
        }
        Some(CommandState::Prepared) | None => {}
    }

    let approval = prepare_limit_entry(commands, command.clone(), preflight)?;
    commands.transition(&command.command_id, CommandState::Submitted)?;

    match client.place_limit(&command) {
        Ok(payload) => match binance_private::parse_order(&payload, &command.owner.symbol) {
            Ok(order) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Accepted {
                        venue_order_id: order.order_id.clone(),
                    },
                )?;
                Ok(ExecutionReceipt::Accepted { approval, order })
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "accepted_response_parse_failed".to_owned(),
                    },
                )?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
    }
}

/// Generic strategy-writer boundary. It deliberately has no Canary, Core quote, Core risk,
/// calibration, or evidence-bundle dependency. The caller still must own an active writer lease,
/// pass fresh private account facts, and hold the lease's dispatch guard through the mutation.
#[derive(Clone, Copy, Debug)]
pub struct StrategyEntryPreflight<'a> {
    pub intent: &'a crate::strategy::scalping::SemanticIntent,
    pub instrument: &'a Instrument,
    pub account: &'a AccountRiskView,
    pub limits: &'a HardRiskLimits,
    pub writer: &'a WriterSession,
    pub now_ms: u64,
    pub dispatch: &'a DispatchGuard,
}

/// Journals and sends a bounded, semantic strategy entry. Marketable entries use IOC; passive
/// entries use GTX and remain owned by the caller's durable cancel/readback lifecycle.
pub fn submit_strategy_limit_entry(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: OrderCommand,
    preflight: StrategyEntryPreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let _dispatch = preflight.dispatch;
    match commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone())
    {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Submitted) | Some(CommandState::Unknown { .. }) => {
            return Err(ExecutionError::Pending);
        }
        Some(CommandState::Prepared) | None => {}
    }
    let approval = prepare_strategy_limit_entry(commands, command.clone(), preflight)?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    let submission = match preflight.intent.entry_style {
        crate::strategy::scalping::EntryStyle::MarketableLimit => {
            client.place_limit_immediate_or_cancel(&command)
        }
        crate::strategy::scalping::EntryStyle::PassiveMaker => {
            client.place_limit_post_only(&command)
        }
    };
    match submission {
        Ok(payload) => match binance_private::parse_order(&payload, &command.owner.symbol) {
            Ok(order) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Accepted {
                        venue_order_id: order.order_id.clone(),
                    },
                )?;
                Ok(ExecutionReceipt::Accepted { approval, order })
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "strategy_entry_response_parse_failed".to_owned(),
                    },
                )?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
    }
}

/// Protected-writer reduction boundary. The caller must obtain its `dispatch` through
/// `WriterLeaseAuthority::protection_dispatch_guard`, so this path can reduce verified exposure
/// but cannot create another entry after the writer has been fenced to protection-only.
#[derive(Clone, Copy, Debug)]
pub struct StrategyReductionPreflight<'a> {
    pub binding: &'a crate::strategy::scalping::StrategyBinding,
    pub writer: &'a WriterSession,
    pub now_ms: u64,
    pub dispatch: &'a DispatchGuard,
}

pub fn submit_strategy_reduce(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: OrderCommand,
    instrument: &Instrument,
    position: &Position,
    preflight: StrategyReductionPreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let _dispatch = preflight.dispatch;
    if preflight.binding.validate().is_err()
        || preflight.writer.valid_until_ms <= preflight.now_ms
        || preflight.writer.scope.exchange != preflight.binding.exchange
        || preflight.writer.scope.account != preflight.binding.account
        || preflight.writer.scope.symbol != preflight.binding.symbol
        || preflight.writer.scope.owner_scope != preflight.binding.owner_scope
        || command.owner.strategy_instance_id != preflight.binding.strategy_instance_id
        || command.owner.run_id != preflight.binding.run_id
        || command.owner.exchange != preflight.binding.exchange
        || command.owner.account != preflight.binding.account
        || command.owner.symbol != preflight.binding.symbol
        || command.owner.purpose != crate::domain::OrderPurpose::Reduce
        || !command.reduce_only
    {
        return Err(ExecutionError::StrategyReduction);
    }
    submit_reduce_limit(commands, client, command, instrument, position)
}

/// Protected-writer stop-maintenance boundary. It is intentionally separate from entry: an
/// already fenced predecessor may replace an exact stop for its current Hedge leg, but it can
/// never turn that permission into a new exposure-increasing order.
#[derive(Clone, Copy, Debug)]
pub struct StrategyProtectionPreflight<'a> {
    pub binding: &'a crate::strategy::scalping::StrategyBinding,
    pub writer: &'a WriterSession,
    pub now_ms: u64,
    pub dispatch: &'a DispatchGuard,
    pub protection: ProtectionPreflight<'a>,
}

pub fn submit_strategy_stop_market_full_position(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: StopMarketFullPositionCommand,
    preflight: StrategyProtectionPreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let _dispatch = preflight.dispatch;
    if preflight.binding.validate().is_err()
        || preflight.writer.valid_until_ms <= preflight.now_ms
        || preflight.writer.scope.exchange != preflight.binding.exchange
        || preflight.writer.scope.account != preflight.binding.account
        || preflight.writer.scope.symbol != preflight.binding.symbol
        || preflight.writer.scope.owner_scope != preflight.binding.owner_scope
        || command.owner != strategy_protection_owner(preflight.binding)
        || command.position_generation != preflight.writer.readback_generation
        || preflight.protection.private_generation != preflight.writer.readback_generation
        || preflight.protection.position_generation != preflight.writer.readback_generation
    {
        return Err(ExecutionError::StrategyProtection);
    }
    submit_stop_market_full_position(commands, client, command, preflight.protection)
}

/// Places the distinct exchange-side profit target under the same protected writer. The target
/// never counts as stop custody and cannot use the stop owner's identity.
pub fn submit_strategy_take_profit_market_full_position(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: StopMarketFullPositionCommand,
    preflight: StrategyProtectionPreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    let _dispatch = preflight.dispatch;
    if preflight.binding.validate().is_err()
        || preflight.writer.valid_until_ms <= preflight.now_ms
        || preflight.writer.scope.exchange != preflight.binding.exchange
        || preflight.writer.scope.account != preflight.binding.account
        || preflight.writer.scope.symbol != preflight.binding.symbol
        || preflight.writer.scope.owner_scope != preflight.binding.owner_scope
        || command.owner != strategy_take_profit_owner(preflight.binding)
        || command.position_generation != preflight.writer.readback_generation
        || preflight.protection.private_generation != preflight.writer.readback_generation
        || preflight.protection.position_generation != preflight.writer.readback_generation
    {
        return Err(ExecutionError::StrategyProtection);
    }
    submit_stop_market_full_position(commands, client, command, preflight.protection)
}

fn strategy_protection_owner(
    binding: &crate::strategy::scalping::StrategyBinding,
) -> crate::domain::OrderOwner {
    crate::domain::OrderOwner {
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose: crate::domain::OrderPurpose::Protection,
    }
}

fn strategy_take_profit_owner(
    binding: &crate::strategy::scalping::StrategyBinding,
) -> crate::domain::OrderOwner {
    crate::domain::OrderOwner {
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose: crate::domain::OrderPurpose::TakeProfit,
    }
}

/// Cancels exactly one previously journaled owner order. It cannot issue a symbol-wide cancel.
pub(crate) fn submit_cancel(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: CancelCommand,
) -> Result<ExecutionReceipt, ExecutionError> {
    command.validate().map_err(ExecutionError::Command)?;
    if command.owner.exchange != "binance" {
        return Err(ExecutionError::Owner);
    }
    let (target_owner, target_family) = commands
        .order_identity_by_client_id(&command.target_client_order_id)
        .map(|target| (target.owner.clone(), target.family))
        .ok_or(ExecutionError::Target)?;
    if target_owner != command.owner {
        return Err(ExecutionError::Owner);
    }
    match commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone())
    {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Prepared)
        | Some(CommandState::Submitted)
        | Some(CommandState::Unknown { .. }) => return Err(ExecutionError::Pending),
        None => {}
    }
    commands.prepare_cancel(command.clone())?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    let cancellation = match target_family {
        crate::domain::NativeOrderFamily::UmOrder => client.cancel_by_client_id(
            &command.owner.symbol,
            command.target_client_order_id.as_str(),
        ),
        crate::domain::NativeOrderFamily::UmConditional => client
            .cancel_conditional_by_client_strategy_id(
                &command.owner.symbol,
                command.target_client_order_id.as_str(),
            ),
        crate::domain::NativeOrderFamily::UmAlgo => {
            client.cancel_algo_by_client_algo_id(command.target_client_order_id.as_str())
        }
    };
    match cancellation {
        Ok(payload) => match binance_private::parse_order(&payload, &command.owner.symbol) {
            Ok(order) => match order.state {
                crate::domain::OrderState::Cancelled => {
                    commands.transition(
                        &command.command_id,
                        CommandState::Accepted {
                            venue_order_id: order.order_id.clone(),
                        },
                    )?;
                    Ok(ExecutionReceipt::Cancelled { order })
                }
                crate::domain::OrderState::New | crate::domain::OrderState::PartiallyFilled => {
                    commands.transition(
                        &command.command_id,
                        CommandState::Rejected {
                            reason: "cancel_response_target_still_open".to_owned(),
                        },
                    )?;
                    Ok(ExecutionReceipt::CancelNotApplied { order })
                }
                crate::domain::OrderState::Filled
                | crate::domain::OrderState::Expired
                | crate::domain::OrderState::Rejected => {
                    commands.transition(
                        &command.command_id,
                        CommandState::Accepted {
                            venue_order_id: order.order_id.clone(),
                        },
                    )?;
                    Ok(ExecutionReceipt::CancelNotApplied { order })
                }
                crate::domain::OrderState::Unknown => {
                    commands.transition(
                        &command.command_id,
                        CommandState::Unknown {
                            reason: "cancel_response_order_state_unknown".to_owned(),
                        },
                    )?;
                    Err(ExecutionError::UnexpectedOrderState)
                }
            },
            Err(_) if target_family == crate::domain::NativeOrderFamily::UmConditional => {
                match binance_private::parse_conditional_strategy_id(
                    &payload,
                    command.target_client_order_id.as_str(),
                ) {
                    Ok(strategy_id) => {
                        commands.transition(
                            &command.command_id,
                            CommandState::Accepted {
                                venue_order_id: strategy_id.clone(),
                            },
                        )?;
                        Ok(ExecutionReceipt::CancelledConditional { strategy_id })
                    }
                    Err(source) => {
                        commands.transition(
                            &command.command_id,
                            CommandState::Unknown {
                                reason: "conditional_cancel_response_parse_failed".to_owned(),
                            },
                        )?;
                        Err(ExecutionError::ReadbackRequired(source))
                    }
                }
            }
            Err(_) if target_family == crate::domain::NativeOrderFamily::UmAlgo => {
                let complete = serde_json::from_str::<serde_json::Value>(&payload)
                    .ok()
                    .and_then(|value| value.get("complete").and_then(serde_json::Value::as_bool));
                if complete == Some(true) {
                    commands.transition(
                        &command.command_id,
                        CommandState::Unknown {
                            reason: "algo_cancel_requires_signed_readback".to_owned(),
                        },
                    )?;
                    Ok(ExecutionReceipt::CancelAlgoPendingReadback)
                } else {
                    commands.transition(
                        &command.command_id,
                        CommandState::Unknown {
                            reason: "algo_cancel_response_parse_failed".to_owned(),
                        },
                    )?;
                    Err(ExecutionError::Pending)
                }
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "cancel_response_parse_failed".to_owned(),
                    },
                )?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
    }
}

/// Recovery cancellation cannot reach the raw cancel path without both its command-bound permit
/// and the recovery guard that owns the normal writer's OS lock.
pub(crate) fn submit_recovery_cancel(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    authorization: RecoveryCancelAuthorization,
    now_ms: u64,
    _dispatch: &RecoveryDispatchGuard,
) -> Result<ExecutionReceipt, ExecutionError> {
    validate_recovery_cancel_dispatch(&authorization, now_ms)?;
    submit_cancel(commands, client, authorization.command)
}

/// Sends a bounded reduce-only IOC order after durable journaling. Unlike a new entry this path
/// remains available while entry gates are closed, but it can only oppose a signed-readback
/// position and cannot exceed its current quantity; no unfilled remainder may rest on VPN delay.
pub(crate) fn submit_reduce_limit(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: OrderCommand,
    instrument: &Instrument,
    position: &Position,
) -> Result<ExecutionReceipt, ExecutionError> {
    let existing_state = commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone());
    match existing_state {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Submitted) | Some(CommandState::Unknown { .. }) => {
            return Err(ExecutionError::Pending);
        }
        Some(CommandState::Prepared) | None => {}
    }

    authorize_reduction(&command, instrument, position)?;
    commands.prepare_place(command.clone())?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    match client.place_limit_immediate_or_cancel(&command) {
        Ok(payload) => match binance_private::parse_order(&payload, &command.owner.symbol) {
            Ok(order) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Accepted {
                        venue_order_id: order.order_id.clone(),
                    },
                )?;
                Ok(ExecutionReceipt::Reduced { order })
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "reduce_response_parse_failed".to_owned(),
                    },
                )?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
    }
}

/// Recovery reduction consumes one exact Hedge leg from one signed observation. Partial IOC fills
/// are accepted as facts; the caller must take a new full readback before authorizing another leg.
pub(crate) fn submit_recovery_reduce(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    authorization: RecoveryReduceAuthorization,
    now_ms: u64,
    _dispatch: &RecoveryDispatchGuard,
) -> Result<ExecutionReceipt, ExecutionError> {
    validate_recovery_reduce_dispatch(&authorization, now_ms)?;
    submit_reduce_limit(
        commands,
        client,
        authorization.command,
        &authorization.instrument,
        &authorization.position,
    )
}

/// Executes only the exact full-position reduction carried by a short-lived emergency permit
/// while the caller retains the single-writer OS guard.
pub fn submit_emergency_flatten(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    authorization: EmergencyFlattenAuthorization,
    instrument: &Instrument,
    position: &Position,
    now_ms: u64,
    _dispatch: &DispatchGuard,
) -> Result<ExecutionReceipt, ExecutionError> {
    validate_emergency_flatten_permit(authorization.permit(), &authorization.command, now_ms)?;
    submit_reduce_limit(
        commands,
        client,
        authorization.command,
        instrument,
        position,
    )
}

/// Places a Hedge-side PAPI conditional close-all. It is only permitted for a fresh,
/// signed-readback position of the exact LONG or SHORT side.
pub fn submit_stop_market_close_all(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: StopMarketCloseAllCommand,
    preflight: ProtectionPreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    match commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone())
    {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Submitted) | Some(CommandState::Unknown { .. }) => {
            return Err(ExecutionError::Pending);
        }
        Some(CommandState::Prepared) | None => {}
    }
    if !preflight.account_can_trade || !preflight.hedge_position || !preflight.mark_price_fresh {
        return Err(ExecutionError::ProtectionPreflight);
    }
    if command.position_generation != preflight.position_generation
        || preflight.private_generation == 0
    {
        return Err(ExecutionError::ProtectionGeneration);
    }
    authorize_stop_market_close_all(&command, preflight.instrument, preflight.position)?;
    commands.prepare_stop_market_close_all(command.clone())?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    match client.place_stop_market_close_all(&command) {
        Ok(payload) => match binance_private::parse_conditional_strategy_id(
            &payload,
            command.client_strategy_id.as_str(),
        ) {
            Ok(strategy_id) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Accepted {
                        venue_order_id: strategy_id.clone(),
                    },
                )?;
                Ok(ExecutionReceipt::Protected { strategy_id })
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "protection_response_parse_failed".to_owned(),
                    },
                )?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
    }
}

/// Places one current PAPI UM Algo quantity-bound STOP_MARKET or TAKE_PROFIT_MARKET. The exact
/// quantity is validated against the fresh authoritative Hedge leg before WAL submission.
pub fn submit_stop_market_full_position(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command: StopMarketFullPositionCommand,
    preflight: ProtectionPreflight<'_>,
) -> Result<ExecutionReceipt, ExecutionError> {
    match commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone())
    {
        Some(CommandState::Accepted { venue_order_id }) => {
            return Ok(ExecutionReceipt::AlreadyResolved { venue_order_id });
        }
        Some(CommandState::Rejected { reason }) => {
            return Ok(ExecutionReceipt::AlreadyRejected { reason });
        }
        Some(CommandState::Submitted) | Some(CommandState::Unknown { .. }) => {
            return Err(ExecutionError::Pending);
        }
        Some(CommandState::Prepared) | None => {}
    }
    if !preflight.account_can_trade || !preflight.hedge_position || !preflight.mark_price_fresh {
        return Err(ExecutionError::ProtectionPreflight);
    }
    if command.position_generation != preflight.position_generation
        || preflight.private_generation == 0
    {
        return Err(ExecutionError::ProtectionGeneration);
    }
    authorize_stop_market_full_position(&command, preflight.instrument, preflight.position)?;
    commands.prepare_stop_market_full_position(command.clone())?;
    commands.transition(&command.command_id, CommandState::Submitted)?;
    match client.place_stop_market_full_position(&command) {
        Ok(payload) => match binance_private::parse_algo_order(
            &payload,
            &command.owner.symbol,
            command.client_algo_id.as_str(),
        ) {
            Ok(readback) if algo_readback_matches(&readback, &command, false) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Accepted {
                        venue_order_id: readback.algo_id.clone(),
                    },
                )?;
                Ok(ExecutionReceipt::ProtectedAlgo {
                    algo_id: readback.algo_id,
                })
            }
            Ok(_) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "algo_protection_response_mismatch".to_owned(),
                    },
                )?;
                Err(ExecutionError::Pending)
            }
            Err(source) => {
                commands.transition(
                    &command.command_id,
                    CommandState::Unknown {
                        reason: "algo_protection_response_parse_failed".to_owned(),
                    },
                )?;
                Err(ExecutionError::ReadbackRequired(source))
            }
        },
        Err(source @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            commands.transition(
                &command.command_id,
                CommandState::Rejected {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
        Err(source) => {
            commands.transition(
                &command.command_id,
                CommandState::Unknown {
                    reason: source.to_string(),
                },
            )?;
            Err(ExecutionError::Venue(source))
        }
    }
}

fn algo_readback_matches(
    readback: &binance_private::AlgoOrderReadback,
    command: &StopMarketFullPositionCommand,
    require_current_controls: bool,
) -> bool {
    let expected_type = match command.owner.purpose {
        crate::domain::OrderPurpose::Protection => "STOP_MARKET",
        crate::domain::OrderPurpose::TakeProfit => "TAKE_PROFIT_MARKET",
        _ => return false,
    };
    let core = readback.order_type == crate::domain::FieldState::Known(expected_type.to_owned())
        && readback.side == crate::domain::FieldState::Known(command.side)
        && readback.position_side == crate::domain::FieldState::Known(command.position_side)
        && readback.quantity == crate::domain::FieldState::Known(command.quantity)
        && readback.trigger_price == crate::domain::FieldState::Known(command.trigger_price)
        && readback.working_type == crate::domain::FieldState::Known("MARK_PRICE".to_owned())
        && readback.reduce_only == crate::domain::FieldState::Known(true);
    core && (!require_current_controls
        || matches!(
            readback.close_position,
            crate::domain::FieldState::Missing | crate::domain::FieldState::Null
        ))
}

/// Uses the WAL-recorded native family and exact client identity to settle one UNKNOWN command.
/// Ordinary orders become authoritative order facts; conditional strategies remain strategy
/// records and never pass through the ordinary-order parser.
pub fn resolve_unknown_order_by_readback(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    facts_journal: &mut crate::storage::Journal,
    reconciler: &mut super::Reconciler,
    command_id: &crate::domain::CommandId,
    generation: u64,
    received_at_ms: u64,
) -> Result<bool, ExecutionError> {
    if !matches!(
        commands.receipt(command_id).map(|receipt| &receipt.state),
        Some(CommandState::Unknown { .. })
    ) {
        return Ok(false);
    }
    if let Some(identity) = commands.cancel_target_identity(command_id).map(|identity| {
        (
            identity.owner.clone(),
            identity.family,
            identity.client_id.clone(),
        )
    }) {
        let mut context = UnknownReadbackContext {
            client,
            facts_journal,
            reconciler,
            generation,
            received_at_ms,
        };
        return resolve_unknown_cancel_by_readback(commands, &mut context, command_id, identity);
    }
    let identity = commands
        .order_identity(command_id)
        .map(|identity| {
            (
                identity.owner.clone(),
                identity.family,
                identity.client_id.clone(),
            )
        })
        .ok_or(ExecutionError::Target)?;
    match identity.1 {
        crate::domain::NativeOrderFamily::UmOrder => {
            let payload = client
                .order_by_client_id(&identity.0.symbol, identity.2.as_str())
                .map_err(ExecutionError::Venue)?;
            let order = binance_private::parse_order(&payload, &identity.0.symbol)
                .map_err(ExecutionError::ReadbackRequired)?;
            reconciler.accept_readback(
                facts_journal,
                super::ReadbackBatch {
                    generation,
                    received_at_ms,
                    balances: &[],
                    positions: &[],
                    orders: std::slice::from_ref(&order),
                    fills: &[],
                },
            )?;
            reconciler
                .resolve_unknown_place(commands, command_id, &order)
                .map_err(ExecutionError::Reconcile)
        }
        crate::domain::NativeOrderFamily::UmConditional => {
            let payload = client
                .conditional_order_by_client_strategy_id(&identity.0.symbol, identity.2.as_str())
                .map_err(ExecutionError::Venue)?;
            let readback =
                binance_private::parse_conditional_strategy(&payload, identity.2.as_str())
                    .map_err(ExecutionError::ReadbackRequired)?;
            resolve_unknown_conditional_place(commands, command_id, readback)
        }
        crate::domain::NativeOrderFamily::UmAlgo => resolve_unknown_algo_place(
            commands,
            client,
            command_id,
            &identity.0.symbol,
            &identity.2,
        ),
    }
}

fn resolve_unknown_algo_place(
    commands: &mut CommandJournal,
    client: &PrivateRest,
    command_id: &crate::domain::CommandId,
    symbol: &crate::domain::Symbol,
    client_algo_id: &crate::domain::CommandId,
) -> Result<bool, ExecutionError> {
    let command = match commands.receipt(command_id) {
        Some(crate::execution::CommandReceipt {
            command: crate::domain::ExecutionCommand::StopMarketFullPosition(command),
            state: CommandState::Unknown { .. },
            ..
        }) => command.clone(),
        _ => return Ok(false),
    };
    let readback = match client.algo_order_by_client_algo_id(client_algo_id.as_str()) {
        Ok(payload) => binance_private::parse_algo_order(&payload, symbol, client_algo_id.as_str())
            .map_err(ExecutionError::ReadbackRequired)?,
        Err(PrivateError::Rejected { .. }) => {
            let payload = match client.algo_order_history(symbol) {
                Ok(payload) => payload,
                Err(PrivateError::Rejected { .. }) => return Ok(false),
                Err(source) => return Err(ExecutionError::Venue(source)),
            };
            binance_private::parse_algo_order(&payload, symbol, client_algo_id.as_str())
                .map_err(ExecutionError::ReadbackRequired)?
        }
        Err(source) => return Err(ExecutionError::Venue(source)),
    };
    if !algo_readback_matches(&readback, &command, true) {
        return Ok(false);
    }
    let next_state = match readback.status {
        binance_private::ConditionalStrategyStatus::Rejected => CommandState::Rejected {
            reason: "signed_algo_readback_rejected".to_owned(),
        },
        binance_private::ConditionalStrategyStatus::Current
        | binance_private::ConditionalStrategyStatus::Cancelled
        | binance_private::ConditionalStrategyStatus::NonCancelledTerminal => {
            CommandState::Accepted {
                venue_order_id: readback.algo_id,
            }
        }
        binance_private::ConditionalStrategyStatus::Unknown => return Ok(false),
    };
    commands.transition(command_id, next_state)?;
    Ok(true)
}

fn resolve_unknown_conditional_place(
    commands: &mut CommandJournal,
    command_id: &crate::domain::CommandId,
    readback: binance_private::ConditionalStrategyReadback,
) -> Result<bool, ExecutionError> {
    let expected = match commands.receipt(command_id) {
        Some(crate::execution::CommandReceipt {
            command: crate::domain::ExecutionCommand::StopMarketCloseAll(command),
            state: CommandState::Unknown { .. },
            ..
        }) => (command.side, command.position_side, command.stop_price),
        _ => return Ok(false),
    };
    if !matches!(readback.side, crate::domain::FieldState::Known(side) if side == expected.0)
        || !matches!(
            readback.position_side,
            crate::domain::FieldState::Known(side) if side == expected.1
        )
        || !matches!(
            readback.stop_price,
            crate::domain::FieldState::Known(price) if price == expected.2
        )
        || readback.close_position != crate::domain::FieldState::Known(true)
    {
        return Ok(false);
    }
    let next_state = match readback.status {
        binance_private::ConditionalStrategyStatus::Rejected => CommandState::Rejected {
            reason: "signed_conditional_readback_rejected".to_owned(),
        },
        binance_private::ConditionalStrategyStatus::Current
        | binance_private::ConditionalStrategyStatus::Cancelled
        | binance_private::ConditionalStrategyStatus::NonCancelledTerminal => {
            CommandState::Accepted {
                venue_order_id: readback.strategy_id,
            }
        }
        binance_private::ConditionalStrategyStatus::Unknown => return Ok(false),
    };
    commands.transition(command_id, next_state)?;
    Ok(true)
}

/// A cancellation has no order identity of its own. Its recovery always follows the target family
/// recorded by the WAL; an active target proves cancellation failure, while all ambiguous target
/// states remain UNKNOWN.
struct UnknownReadbackContext<'a> {
    client: &'a PrivateRest,
    facts_journal: &'a mut crate::storage::Journal,
    reconciler: &'a mut super::Reconciler,
    generation: u64,
    received_at_ms: u64,
}

fn resolve_unknown_cancel_by_readback(
    commands: &mut CommandJournal,
    context: &mut UnknownReadbackContext<'_>,
    cancel_command_id: &crate::domain::CommandId,
    identity: (
        crate::domain::OrderOwner,
        crate::domain::NativeOrderFamily,
        crate::domain::CommandId,
    ),
) -> Result<bool, ExecutionError> {
    match identity.1 {
        crate::domain::NativeOrderFamily::UmOrder => {
            let payload = context
                .client
                .order_by_client_id(&identity.0.symbol, identity.2.as_str())
                .map_err(ExecutionError::Venue)?;
            let order = binance_private::parse_order(&payload, &identity.0.symbol)
                .map_err(ExecutionError::ReadbackRequired)?;
            context.reconciler.accept_readback(
                context.facts_journal,
                super::ReadbackBatch {
                    generation: context.generation,
                    received_at_ms: context.received_at_ms,
                    balances: &[],
                    positions: &[],
                    orders: std::slice::from_ref(&order),
                    fills: &[],
                },
            )?;
            match order.state {
                crate::domain::OrderState::Cancelled => context
                    .reconciler
                    .resolve_unknown_cancelled_target(commands, cancel_command_id, order.order_id)
                    .map_err(ExecutionError::Reconcile),
                crate::domain::OrderState::New | crate::domain::OrderState::PartiallyFilled => {
                    context
                        .reconciler
                        .resolve_unknown_cancel_open_target(commands, cancel_command_id)
                        .map_err(ExecutionError::Reconcile)
                }
                crate::domain::OrderState::Rejected
                | crate::domain::OrderState::Filled
                | crate::domain::OrderState::Expired
                | crate::domain::OrderState::Unknown => Ok(false),
            }
        }
        crate::domain::NativeOrderFamily::UmConditional => {
            match context
                .client
                .conditional_order_by_client_strategy_id(&identity.0.symbol, identity.2.as_str())
            {
                Ok(payload) => {
                    // A current endpoint response proves the strategy remains active. It remains
                    // a conditional strategy and is never parsed as a physical UM Order.
                    binance_private::parse_conditional_strategy(&payload, identity.2.as_str())
                        .map_err(ExecutionError::ReadbackRequired)?;
                    context
                        .reconciler
                        .resolve_unknown_cancel_open_target(commands, cancel_command_id)
                        .map_err(ExecutionError::Reconcile)
                }
                Err(PrivateError::Rejected { .. }) => resolve_unknown_conditional_cancel_history(
                    commands,
                    context,
                    cancel_command_id,
                    &identity.0.symbol,
                    identity.2.as_str(),
                ),
                Err(source) => Err(ExecutionError::Venue(source)),
            }
        }
        crate::domain::NativeOrderFamily::UmAlgo => resolve_unknown_algo_cancel(
            commands,
            context,
            cancel_command_id,
            &identity.0.symbol,
            &identity.2,
        ),
    }
}

fn resolve_unknown_algo_cancel(
    commands: &mut CommandJournal,
    context: &mut UnknownReadbackContext<'_>,
    cancel_command_id: &crate::domain::CommandId,
    symbol: &crate::domain::Symbol,
    client_algo_id: &crate::domain::CommandId,
) -> Result<bool, ExecutionError> {
    match context
        .client
        .algo_order_by_client_algo_id(client_algo_id.as_str())
    {
        Ok(payload) => {
            let current =
                binance_private::parse_algo_order(&payload, symbol, client_algo_id.as_str())
                    .map_err(ExecutionError::ReadbackRequired)?;
            match current.status {
                binance_private::ConditionalStrategyStatus::Cancelled => context
                    .reconciler
                    .resolve_unknown_cancelled_target(commands, cancel_command_id, current.algo_id)
                    .map_err(ExecutionError::Reconcile),
                binance_private::ConditionalStrategyStatus::NonCancelledTerminal
                | binance_private::ConditionalStrategyStatus::Rejected => context
                    .reconciler
                    .resolve_unknown_cancel_open_target(commands, cancel_command_id)
                    .map_err(ExecutionError::Reconcile),
                binance_private::ConditionalStrategyStatus::Current
                | binance_private::ConditionalStrategyStatus::Unknown => Ok(false),
            }
        }
        Err(PrivateError::Rejected { .. }) => {
            let payload = match context.client.algo_order_history(symbol) {
                Ok(payload) => payload,
                Err(PrivateError::Rejected { .. }) => return Ok(false),
                Err(source) => return Err(ExecutionError::Venue(source)),
            };
            let history =
                binance_private::parse_algo_order(&payload, symbol, client_algo_id.as_str())
                    .map_err(ExecutionError::ReadbackRequired)?;
            match history.status {
                binance_private::ConditionalStrategyStatus::Cancelled => context
                    .reconciler
                    .resolve_unknown_cancelled_target(commands, cancel_command_id, history.algo_id)
                    .map_err(ExecutionError::Reconcile),
                binance_private::ConditionalStrategyStatus::NonCancelledTerminal
                | binance_private::ConditionalStrategyStatus::Rejected => context
                    .reconciler
                    .resolve_unknown_cancel_open_target(commands, cancel_command_id)
                    .map_err(ExecutionError::Reconcile),
                binance_private::ConditionalStrategyStatus::Current
                | binance_private::ConditionalStrategyStatus::Unknown => Ok(false),
            }
        }
        Err(source) => Err(ExecutionError::Venue(source)),
    }
}

/// A missing current strategy is ambiguous until history returns the same client strategy ID and
/// a known terminal lifecycle. Missing or unrecognized history is deliberately still UNKNOWN.
fn resolve_unknown_conditional_cancel_history(
    commands: &mut CommandJournal,
    context: &mut UnknownReadbackContext<'_>,
    cancel_command_id: &crate::domain::CommandId,
    symbol: &crate::domain::Symbol,
    client_strategy_id: &str,
) -> Result<bool, ExecutionError> {
    let payload = match context
        .client
        .conditional_order_history_by_client_strategy_id(symbol, client_strategy_id)
    {
        Ok(payload) => payload,
        Err(PrivateError::Rejected { .. }) => return Ok(false),
        Err(source) => return Err(ExecutionError::Venue(source)),
    };
    let history = binance_private::parse_conditional_strategy(&payload, client_strategy_id)
        .map_err(ExecutionError::ReadbackRequired)?;
    match history.status {
        binance_private::ConditionalStrategyStatus::Cancelled => context
            .reconciler
            .resolve_unknown_cancelled_target(commands, cancel_command_id, history.strategy_id)
            .map_err(ExecutionError::Reconcile),
        binance_private::ConditionalStrategyStatus::NonCancelledTerminal => context
            .reconciler
            .resolve_unknown_cancel_open_target(commands, cancel_command_id)
            .map_err(ExecutionError::Reconcile),
        binance_private::ConditionalStrategyStatus::Rejected => context
            .reconciler
            .resolve_unknown_cancel_open_target(commands, cancel_command_id)
            .map_err(ExecutionError::Reconcile),
        binance_private::ConditionalStrategyStatus::Current
        | binance_private::ConditionalStrategyStatus::Unknown => Ok(false),
    }
}

/// Persists a command only after every deterministic hard-risk predicate succeeds.
pub fn prepare_limit_entry(
    commands: &mut CommandJournal,
    command: OrderCommand,
    preflight: EntryPreflight<'_>,
) -> Result<RiskApproval, ExecutionError> {
    validate_canary_permit(&preflight.permit, &command, preflight.now_ms)?;
    let risk_account = AccountRiskView {
        available_margin: preflight.account.available_margin.clone(),
        unresolved_commands: u32::from(commands.has_unresolved()),
    };
    let approval = authorize_entry(
        &command,
        preflight.instrument,
        &risk_account,
        preflight.limits,
    )?;
    commands.prepare_place(command)?;
    Ok(approval)
}

/// The generic path reuses only deterministic venue rules and the explicit local notional cap.
/// It never calls the Canary gate or any external strategy valuation source.
pub fn prepare_strategy_limit_entry(
    commands: &mut CommandJournal,
    command: OrderCommand,
    preflight: StrategyEntryPreflight<'_>,
) -> Result<RiskApproval, ExecutionError> {
    validate_strategy_entry(&command, preflight)?;
    let approval = authorize_entry(
        &command,
        preflight.instrument,
        preflight.account,
        preflight.limits,
    )?;
    commands.prepare_place(command)?;
    Ok(approval)
}

fn validate_strategy_entry(
    command: &OrderCommand,
    preflight: StrategyEntryPreflight<'_>,
) -> Result<(), ExecutionError> {
    use crate::strategy::scalping::{Direction, EntryStyle, SemanticPurpose};

    let intent = preflight.intent;
    if preflight.now_ms == 0
        || preflight.writer.valid_until_ms <= preflight.now_ms
        || preflight.writer.readback_generation == 0
        || intent.purpose != SemanticPurpose::Entry
        || intent.symbol != command.owner.symbol
        || intent.symbol != preflight.instrument.symbol
        || preflight.writer.scope.symbol != intent.symbol
        || preflight.writer.scope.exchange != command.owner.exchange
        || preflight.writer.scope.account != command.owner.account
        || command.owner.purpose != crate::domain::OrderPurpose::Entry
        || command.reduce_only
        || intent.target_quote.asset.as_str() != "USDT"
        || intent.target_quote.value <= rust_decimal::Decimal::ZERO
        || intent.target_quote.value > preflight.limits.max_entry_notional.value
        || preflight.limits.max_entry_notional.asset.as_str() != "USDT"
    {
        return Err(ExecutionError::StrategyEntry);
    }
    let expected_side = match intent.direction {
        Direction::Long => PositionSide::Long,
        Direction::Short => PositionSide::Short,
    };
    if command.position_side != expected_side {
        return Err(ExecutionError::StrategyEntry);
    }
    let reference = intent.reference_price.value();
    let passive_boundary = intent.target_distance_bps / rust_decimal::Decimal::new(10_000, 0);
    if intent.entry_style == EntryStyle::PassiveMaker
        && (intent.target_distance_bps <= rust_decimal::Decimal::ZERO
            || intent.target_distance_bps >= rust_decimal::Decimal::new(10_000, 0))
    {
        return Err(ExecutionError::StrategyEntry);
    }
    let price_ok = match (intent.entry_style, intent.direction) {
        (EntryStyle::MarketableLimit, Direction::Long) => {
            let boundary = intent.max_slippage_bps / rust_decimal::Decimal::new(10_000, 0);
            command.limit_price.value() <= reference * (rust_decimal::Decimal::ONE + boundary)
        }
        (EntryStyle::MarketableLimit, Direction::Short) => {
            let boundary = intent.max_slippage_bps / rust_decimal::Decimal::new(10_000, 0);
            command.limit_price.value() >= reference * (rust_decimal::Decimal::ONE - boundary)
        }
        (EntryStyle::PassiveMaker, Direction::Long) => {
            command.limit_price.value()
                <= reference * (rust_decimal::Decimal::ONE - passive_boundary)
        }
        (EntryStyle::PassiveMaker, Direction::Short) => {
            command.limit_price.value()
                >= reference * (rust_decimal::Decimal::ONE + passive_boundary)
        }
    };
    if !price_ok || command.limit_price.value() <= rust_decimal::Decimal::ZERO {
        return Err(ExecutionError::StrategyEntry);
    }
    Ok(())
}

/// The complete evidence set that must still be current at the exact WAL boundary.
#[derive(Clone, Copy, Debug)]
pub struct EntryPreflight<'a> {
    pub instrument: &'a Instrument,
    pub account: &'a AccountRiskView,
    pub limits: &'a HardRiskLimits,
    pub permit: GateDecision,
    pub now_ms: u64,
}

/// Fresh signed account and position evidence required to install exchange-side protection.
#[derive(Clone, Copy, Debug)]
pub struct ProtectionPreflight<'a> {
    pub instrument: &'a Instrument,
    pub position: &'a Position,
    pub private_generation: u64,
    pub position_generation: u64,
    pub account_can_trade: bool,
    pub hedge_position: bool,
    pub mark_price_fresh: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionReceipt {
    ProbeAccepted {
        order: Order,
    },
    Accepted {
        approval: RiskApproval,
        order: Order,
    },
    Cancelled {
        order: Order,
    },
    CancelNotApplied {
        order: Order,
    },
    CancelledConditional {
        strategy_id: String,
    },
    CancelAlgoPendingReadback,
    Reduced {
        order: Order,
    },
    Protected {
        strategy_id: String,
    },
    ProtectedAlgo {
        algo_id: String,
    },
    AlreadyResolved {
        venue_order_id: String,
    },
    AlreadyRejected {
        reason: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error(
        "generic strategy entry does not match its semantic intent, writer scope, or 10 USDT envelope"
    )]
    StrategyEntry,
    #[error("strategy reduction does not match its protected writer scope or exact owner")]
    StrategyReduction,
    #[error(
        "strategy stop maintenance does not match its protected writer scope or custody generation"
    )]
    StrategyProtection,
    #[error("cancel command is invalid: {0}")]
    Command(crate::domain::CommandError),
    #[error("hard risk rejected the command: {0}")]
    Risk(#[from] RiskError),
    #[error("Canary gate rejected the command: {0}")]
    Gate(#[from] GateError),
    #[error("first place/cancel probe gate rejected the command: {0}")]
    ProbeGate(#[from] ProbeGateError),
    #[error("durable Canary run state rejected the transition: {0}")]
    CanaryRun(#[from] CanaryRunStateError),
    #[error("emergency flatten gate rejected the reduction: {0}")]
    EmergencyFlatten(#[from] super::EmergencyFlattenError),
    #[error("recovery-only writer rejected the mutation: {0}")]
    RecoveryWriter(#[from] RecoveryWriterError),
    #[error("execution journal failed: {0}")]
    Journal(#[from] CommandJournalError),
    #[error("writer lease rejected the generic strategy dispatch: {0}")]
    Writer(#[from] WriterLeaseError),
    #[error("a submitted or unknown command requires exact signed readback before any retry")]
    Pending,
    #[error("cancel command does not own the target order")]
    Owner,
    #[error("cancel target is not a previously journaled entry order")]
    Target,
    #[error("Binance mutation failed: {0}")]
    Venue(PrivateError),
    #[error("Binance accepted a response that requires exact readback: {0}")]
    ReadbackRequired(binance_private::PrivateParseError),
    #[error("Binance returned an order whose lifecycle state is unknown")]
    UnexpectedOrderState,
    #[error("private fact reconciliation failed: {0}")]
    Reconcile(#[from] super::ReconciliationError),
    #[error("protection requires a trade-enabled Hedge account and fresh mark price")]
    ProtectionPreflight,
    #[error("protection command does not match the signed private position generation")]
    ProtectionGeneration,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_decimal::Decimal;

    use crate::{
        domain::{
            Amount, Asset, CommandId, FieldState, Instrument, MarketKind, OrderOwner, OrderPurpose,
            OrderSide, PositionSide, Price,
        },
        exchange::private_session::PrivateSessionState,
        execution::{Capability, CapabilityEvidence, GateInput, RunMode, evaluate_gate},
        strategy::scalping::{
            Direction, EntryStyle, ExitTemplate, Expert, RiskLimit, RiskPlan, RiskUnit,
            SemanticIntent, SemanticPurpose,
        },
    };

    use super::*;

    fn command() -> Result<
        (OrderCommand, Instrument, AccountRiskView, HardRiskLimits),
        Box<dyn std::error::Error>,
    > {
        let asset: Asset = "USDT".parse()?;
        let instrument = Instrument {
            symbol: "DOGE/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(asset.clone()),
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 5))?,
            quantity_step: Decimal::ONE,
            minimum_notional: Amount::new(asset.clone(), Decimal::new(5, 0)),
        };
        let command = OrderCommand {
            command_id: CommandId::new("canary_1")?,
            client_order_id: CommandId::new("venue_canary_1")?,
            owner: OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "canary_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: instrument.symbol.clone(),
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(50, 0),
            limit_price: Price::new(Decimal::new(1, 1))?,
            reduce_only: false,
        };
        let account = AccountRiskView {
            available_margin: Amount::new(asset.clone(), Decimal::new(5, 0)),
            unresolved_commands: 0,
        };
        let limits = HardRiskLimits {
            max_entry_notional: Amount::new(asset, Decimal::new(5, 0)),
        };
        Ok((command, instrument, account, limits))
    }

    fn permit(command: &OrderCommand) -> Result<GateDecision, Box<dyn std::error::Error>> {
        let capabilities: BTreeMap<_, _> = [
            Capability::InstrumentRules,
            Capability::PublicMarket,
            Capability::PrivateReadback,
            Capability::PrivateStream,
            Capability::PlaceLimit,
            Capability::Cancel,
            Capability::ReduceOnly,
            Capability::Reconciliation,
        ]
        .into_iter()
        .map(|capability| {
            (
                capability,
                CapabilityEvidence {
                    evidence_hash: format!("evidence_{capability:?}"),
                    generation: 1,
                    verified_at_ms: 1,
                    valid_until_ms: 10,
                },
            )
        })
        .collect();
        Ok(evaluate_gate(
            &GateInput {
                mode: RunMode::Canary,
                now_ms: 1,
                capabilities,
                private_session: PrivateSessionState::Ready,
                private_generation: 1,
                readback_generation: 1,
                private_readback_valid_until_ms: 10,
                instrument_generation: 1,
                binding_instrument_generation: 1,
                instrument_valid_until_ms: 10,
                account_readback_fresh: true,
                reconciliation_clean: true,
                reconciliation_valid_until_ms: 10,
                command_wal_clean: true,
                single_writer: true,
                writer_lease_valid_until_ms: 10,
                protection_verified: true,
                protection_valid_until_ms: 10,
                max_entry_notional: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
            },
            command,
        )?)
    }

    fn preflight<'a>(
        instrument: &'a Instrument,
        account: &'a AccountRiskView,
        limits: &'a HardRiskLimits,
        permit: GateDecision,
        now_ms: u64,
    ) -> EntryPreflight<'a> {
        EntryPreflight {
            instrument,
            account,
            limits,
            permit,
            now_ms,
        }
    }

    #[test]
    fn an_absent_or_expired_canary_permit_never_enters_the_wal()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        let (command, instrument, account, limits) = command()?;

        assert!(matches!(
            prepare_limit_entry(
                &mut journal,
                command.clone(),
                preflight(&instrument, &account, &limits, GateDecision::ShadowOnly, 1),
            ),
            Err(ExecutionError::Gate(GateError::Shadow))
        ));
        assert!(journal.receipt(&command.command_id).is_none());

        let expired = match permit(&command)? {
            GateDecision::CanaryPermit { command_sha256, .. } => GateDecision::CanaryPermit {
                command_sha256,
                valid_until_ms: 1,
            },
            GateDecision::ShadowOnly => {
                return Err("test gate unexpectedly issued ShadowOnly".into());
            }
        };

        assert!(matches!(
            prepare_limit_entry(
                &mut journal,
                command.clone(),
                preflight(&instrument, &account, &limits, expired, 1),
            ),
            Err(ExecutionError::Gate(GateError::PermitExpired))
        ));
        assert!(journal.receipt(&command.command_id).is_none());
        Ok(())
    }

    #[test]
    fn current_canary_permit_is_checked_before_wal_prepare()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        let (command, instrument, account, limits) = command()?;
        prepare_limit_entry(
            &mut journal,
            command.clone(),
            preflight(&instrument, &account, &limits, permit(&command)?, 1),
        )?;
        assert!(matches!(
            journal
                .receipt(&command.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Prepared)
        ));
        Ok(())
    }

    #[test]
    fn generic_strategy_writer_needs_no_canary_or_external_valuation_gate()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let (command, instrument, account, limits) = command()?;
        let scope = crate::execution::WriterScope {
            exchange: command.owner.exchange.clone(),
            account: command.owner.account.clone(),
            symbol: command.owner.symbol.clone(),
            owner_scope: "scalping_primary_run".to_owned(),
        };
        let authority = crate::execution::WriterLeaseAuthority::open(
            temporary.path().join("writer.json"),
            scope,
        )?;
        let writer = authority.register_initial(1, 1)?;
        let dispatch = authority.dispatch_guard(&writer, 1)?;
        let unit = RiskUnit::new("risk")?;
        let intent = SemanticIntent {
            intent_id: "strategy-intent-1".to_owned(),
            symbol: command.owner.symbol.clone(),
            direction: Direction::Long,
            purpose: SemanticPurpose::Entry,
            expert: Expert::TrendPullback,
            entry_style: EntryStyle::MarketableLimit,
            exit_template: ExitTemplate::TrendTrail,
            attempt_cap: 1,
            max_reprices: 0,
            risk_plan: RiskPlan {
                risk_per_episode: RiskLimit::new(unit.clone(), Decimal::ONE),
                quote_cap: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
                max_episode_loss: RiskLimit::new(unit, Decimal::ONE),
            },
            target_quote: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
            reference_price: command.limit_price,
            max_slippage_bps: Decimal::new(10, 0),
            valid_until_ms: 2,
            entry_ttl_ms: 1,
            hard_stop_distance_bps: Decimal::ONE,
            target_distance_bps: Decimal::ONE,
            max_hold_ms: 1,
            max_unprotected_ms: 1,
            requires_server_protection: true,
            opportunity_key: "strategy-opportunity-1".to_owned(),
            breakout_cursor: None,
            idempotency_seed: "strategy-seed-1".to_owned(),
        };
        let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        let approval = prepare_strategy_limit_entry(
            &mut journal,
            command.clone(),
            StrategyEntryPreflight {
                intent: &intent,
                instrument: &instrument,
                account: &account,
                limits: &limits,
                writer: &writer,
                now_ms: 1,
                dispatch: &dispatch,
            },
        )?;
        assert_eq!(approval.notional.value, Decimal::new(5, 0));
        assert!(matches!(
            journal
                .receipt(&command.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Prepared)
        ));
        Ok(())
    }

    #[test]
    fn conditional_unknown_requires_exact_hedge_side_and_close_all_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        let protection = StopMarketCloseAllCommand {
            command_id: CommandId::new("protect_1")?,
            client_strategy_id: CommandId::new("protect_client_1")?,
            owner: OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Protection,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            stop_price: Price::new(Decimal::new(9, 2))?,
            position_generation: 7,
        };
        journal.prepare_stop_market_close_all(protection.clone())?;
        journal.transition(&protection.command_id, CommandState::Submitted)?;
        journal.transition(
            &protection.command_id,
            CommandState::Unknown {
                reason: "timeout".to_owned(),
            },
        )?;
        let exact = binance_private::ConditionalStrategyReadback {
            strategy_id: "42".to_owned(),
            status: binance_private::ConditionalStrategyStatus::Current,
            side: FieldState::Known(OrderSide::Sell),
            position_side: FieldState::Known(PositionSide::Long),
            stop_price: FieldState::Known(protection.stop_price),
            close_position: FieldState::Known(true),
        };
        let mut wrong_side = exact.clone();
        wrong_side.position_side = FieldState::Known(PositionSide::Short);
        assert!(!resolve_unknown_conditional_place(
            &mut journal,
            &protection.command_id,
            wrong_side,
        )?);
        assert!(matches!(
            journal
                .receipt(&protection.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));
        assert!(resolve_unknown_conditional_place(
            &mut journal,
            &protection.command_id,
            exact,
        )?);
        assert!(matches!(
            journal
                .receipt(&protection.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Accepted { venue_order_id }) if venue_order_id == "42"
        ));
        Ok(())
    }
}
