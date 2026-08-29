use std::collections::BTreeSet;

use crate::{
    domain::{CommandId, Position, PositionSide},
    risk::AccountRiskView,
    runtime::{
        ExecutionProjection, OwnerProjection, PrivateExposure, PrivateFactsProjectionInput,
        PrivateFactsReadiness, PrivateProjection, ProtectionProjection, RiskBudgetProjection,
    },
    strategy::scalping::{PHASE8_ATR14_PARAMETER_RELEASE_ID, StrategyBinding},
};

use super::{AlgoProtectionCustody, CommandJournal, WriterSession};

/// Read-only inputs for turning one committed private-readback identity into the four anonymous
/// runtime projections. The resolver retains no balances, orders, positions, or custody proofs.
pub struct PrivateProjectionResolverInput<'a> {
    pub binding: &'a StrategyBinding,
    pub readiness: PrivateFactsReadiness,
    pub positions: &'a [Position],
    pub open_ordinary_client_ids: &'a [CommandId],
    pub open_algo_client_ids: &'a [CommandId],
    pub journal: &'a CommandJournal,
    pub writer: Option<&'a WriterSession>,
    pub algo_custodies: &'a [AlgoProtectionCustody],
    pub account_risk: Option<&'a AccountRiskView>,
    pub now_ms: u64,
}

/// Aggregates existing authoritative facts without persisting or copying any account, order, or
/// position state. Absent identity evidence is represented as `Unknown`, never as a ready fact.
#[must_use]
pub fn resolve_private_facts_projection(
    input: PrivateProjectionResolverInput<'_>,
) -> PrivateFactsProjectionInput {
    let identity = (input.readiness.generation, input.readiness.observed_at_ms);
    if !valid_input_identity(&input) {
        return unknown(identity);
    }

    let ordinary_ids = unique_ids(input.open_ordinary_client_ids);
    let algo_ids = unique_ids(input.open_algo_client_ids);
    let debts_are_exact = ordinary_ids.as_ref().is_some_and(|ordinary| {
        algo_ids
            .as_ref()
            .is_some_and(|algo| ordinary.is_disjoint(algo))
    }) && input.readiness.ordinary_order_debt
        != input.open_ordinary_client_ids.is_empty()
        && input.readiness.algo_order_debt != input.open_algo_client_ids.is_empty();
    let ids = match (&ordinary_ids, &algo_ids) {
        (Some(ordinary), Some(algo)) => ordinary.union(algo).cloned().collect(),
        _ => BTreeSet::new(),
    };
    let all_open_ids_resolve = debts_are_exact
        && ids
            .iter()
            .all(|client_id| input.journal.command_id_by_client_id(client_id).is_some());

    let execution = if debts_are_exact && all_open_ids_resolve && !input.journal.has_unresolved() {
        ExecutionProjection::Known
    } else {
        ExecutionProjection::Unknown
    };
    let owner = owner_projection(&input, &ids, debts_are_exact);
    let protection = protection_projection(&input, algo_ids.as_ref());
    let risk_budget = risk_budget_projection(&input);

    PrivateFactsProjectionInput {
        execution: projection(identity, execution),
        owner: projection(identity, owner),
        protection: projection(identity, protection),
        risk_budget: projection(identity, risk_budget),
    }
}

fn valid_input_identity(input: &PrivateProjectionResolverInput<'_>) -> bool {
    input.binding.validate().is_ok()
        && input.readiness.generation != 0
        && input.readiness.observed_at_ms != 0
        && input.readiness.observed_at_ms <= input.now_ms
        && positions_match_readiness(
            input.positions,
            &input.binding.symbol,
            input.readiness.exposure,
        )
}

fn unique_ids(ids: &[CommandId]) -> Option<BTreeSet<CommandId>> {
    let unique = ids.iter().cloned().collect::<BTreeSet<_>>();
    (unique.len() == ids.len()).then_some(unique)
}

fn owner_projection(
    input: &PrivateProjectionResolverInput<'_>,
    ids: &BTreeSet<CommandId>,
    debts_are_exact: bool,
) -> OwnerProjection {
    if !debts_are_exact {
        return OwnerProjection::Unknown;
    }
    if ids.is_empty() {
        return OwnerProjection::Clear;
    }
    let Some(writer) = input.writer else {
        // `OrderOwner` intentionally has no owner-scope field; an active scoped session is the
        // only supplied authority that can bind journal owners to the strategy owner scope.
        return OwnerProjection::Unknown;
    };
    if !writer_scope_matches(writer, input.binding) {
        return OwnerProjection::Conflict;
    }
    if ids.iter().all(|client_id| {
        input
            .journal
            .owner_by_client_id(client_id)
            .is_some_and(|owner| {
                owner.strategy_instance_id == input.binding.strategy_instance_id
                    && owner.run_id == input.binding.run_id
                    && owner.exchange == input.binding.exchange
                    && owner.account == input.binding.account
                    && owner.symbol == input.binding.symbol
            })
    }) {
        OwnerProjection::Clear
    } else {
        OwnerProjection::Conflict
    }
}

fn protection_projection(
    input: &PrivateProjectionResolverInput<'_>,
    algo_ids: Option<&BTreeSet<CommandId>>,
) -> ProtectionProjection {
    let Some(algo_ids) = algo_ids else {
        return ProtectionProjection::Unknown;
    };
    let Some(legs) = hedge_legs(input.positions, &input.binding.symbol) else {
        return ProtectionProjection::Unknown;
    };
    if legs.is_empty() {
        return if algo_ids.is_empty() && input.algo_custodies.is_empty() {
            ProtectionProjection::Complete
        } else {
            ProtectionProjection::Gap
        };
    }
    let Some(writer) = input.writer else {
        return ProtectionProjection::Unknown;
    };
    if !valid_protection_writer(
        writer,
        input.binding,
        input.readiness.generation,
        input.now_ms,
    ) {
        return ProtectionProjection::Unknown;
    }
    if input.algo_custodies.len() != legs.len() {
        return ProtectionProjection::Gap;
    }
    let mut custody_ids = BTreeSet::new();
    let mut covered = BTreeSet::new();
    for custody in input.algo_custodies {
        let client_id = match CommandId::new(&custody.client_algo_id) {
            Ok(client_id) => client_id,
            Err(_) => return ProtectionProjection::Gap,
        };
        let valid_custody = custody.symbol == input.binding.symbol
            && matches!(
                custody.position_side,
                PositionSide::Long | PositionSide::Short
            )
            && custody.private_generation == input.readiness.generation
            && custody.writer_generation == writer.generation
            && custody.valid_until_ms > input.now_ms
            && valid_digest(&custody.content_sha256)
            && !custody.venue_algo_id.trim().is_empty()
            && algo_ids.contains(&client_id)
            && CommandId::new(&custody.command_id).ok().as_ref()
                == input.journal.command_id_by_client_id(&client_id)
            && custody_ids.insert(client_id)
            && legs.contains(&(
                hedge_side(custody.position_side),
                custody.full_position_quantity,
            ));
        if !valid_custody
            || !covered.insert((
                hedge_side(custody.position_side),
                custody.full_position_quantity,
            ))
        {
            return ProtectionProjection::Gap;
        }
    }
    let target_ids = algo_ids
        .difference(&custody_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    let targets_valid = if input.binding.parameter_release_id == PHASE8_ATR14_PARAMETER_RELEASE_ID {
        target_ids.len() == legs.len()
            && target_ids.iter().all(|client_id| {
                input
                    .journal
                    .owner_by_client_id(client_id)
                    .is_some_and(|owner| {
                        owner.purpose == crate::domain::OrderPurpose::TakeProfit
                            && owner.symbol == input.binding.symbol
                            && owner.strategy_instance_id == input.binding.strategy_instance_id
                            && owner.run_id == input.binding.run_id
                    })
            })
    } else {
        target_ids.is_empty()
    };
    if covered == legs && targets_valid {
        ProtectionProjection::Complete
    } else {
        ProtectionProjection::Gap
    }
}

fn positions_match_readiness(
    positions: &[Position],
    symbol: &crate::domain::Symbol,
    exposure: PrivateExposure,
) -> bool {
    if positions.len() != 2 {
        return false;
    }
    let mut long = false;
    let mut short = false;
    let mut any_open = false;
    for position in positions {
        if position.symbol != *symbol || position.quantity.is_sign_negative() {
            return false;
        }
        match position.side {
            PositionSide::Long if !long => long = true,
            PositionSide::Short if !short => short = true,
            PositionSide::Long | PositionSide::Short | PositionSide::Net => return false,
        }
        any_open |= !position.quantity.is_zero();
    }
    long && short
        && matches!(
            (exposure, any_open),
            (PrivateExposure::Flat, false) | (PrivateExposure::Open, true)
        )
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn hedge_legs(
    positions: &[Position],
    symbol: &crate::domain::Symbol,
) -> Option<BTreeSet<(u8, rust_decimal::Decimal)>> {
    let mut legs = BTreeSet::new();
    for position in positions {
        if position.quantity.is_sign_negative() {
            return None;
        }
        if position.quantity.is_zero() {
            continue;
        }
        if position.symbol != *symbol
            || !matches!(position.side, PositionSide::Long | PositionSide::Short)
            || !legs.insert((hedge_side(position.side), position.quantity))
        {
            return None;
        }
    }
    Some(legs)
}

fn hedge_side(side: PositionSide) -> u8 {
    match side {
        PositionSide::Long => 1,
        PositionSide::Short => 2,
        PositionSide::Net => 0,
    }
}

fn risk_budget_projection(input: &PrivateProjectionResolverInput<'_>) -> RiskBudgetProjection {
    let Some(account_risk) = input.account_risk else {
        return RiskBudgetProjection::Unknown;
    };
    if account_risk.available_margin.asset != input.binding.risk_budget.asset
        || account_risk.available_margin.value.is_sign_negative()
    {
        return RiskBudgetProjection::Unknown;
    }
    if account_risk.unresolved_commands > 0
        || account_risk.available_margin.value < input.binding.risk_budget.value
    {
        RiskBudgetProjection::Unavailable
    } else {
        RiskBudgetProjection::Available
    }
}

fn writer_scope_matches(writer: &WriterSession, binding: &StrategyBinding) -> bool {
    writer.scope.exchange == binding.exchange
        && writer.scope.account == binding.account
        && writer.scope.symbol == binding.symbol
        && writer.scope.owner_scope == binding.owner_scope
}

fn valid_protection_writer(
    writer: &WriterSession,
    binding: &StrategyBinding,
    private_generation: u64,
    now_ms: u64,
) -> bool {
    writer_scope_matches(writer, binding)
        && writer.generation != 0
        && writer.revision != 0
        // The current signed private readback may be newer than the predecessor writer's
        // admission generation. That newer identity is precisely what allows the owner to
        // retain the predecessor as protection-only or retire it after an exact flat proof.
        && writer.readback_generation <= private_generation
        && !writer.token.trim().is_empty()
        // Protection projection never grants entry. An expired predecessor can therefore be
        // demoted to protection-only from fresh, exact exchange-resident Algo custody.
        && now_ms != 0
}

fn projection<T>(identity: (u64, u64), value: T) -> PrivateProjection<T> {
    PrivateProjection {
        generation: identity.0,
        observed_at_ms: identity.1,
        value,
    }
}

fn unknown(identity: (u64, u64)) -> PrivateFactsProjectionInput {
    PrivateFactsProjectionInput {
        execution: projection(identity, ExecutionProjection::Unknown),
        owner: projection(identity, OwnerProjection::Unknown),
        protection: projection(identity, ProtectionProjection::Unknown),
        risk_budget: projection(identity, RiskBudgetProjection::Unknown),
    }
}
