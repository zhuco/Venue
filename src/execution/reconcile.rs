use crate::{
    domain::{
        AccountBalance, DomainEvent, EventHeader, EventId, EventIdError, EventSource, FactRecord,
        Fill, Order, OrderState, Position,
    },
    exchange::private_session::{PrivateEvidenceSession, PrivateSessionError},
    execution::{CommandJournal, CommandJournalError, CommandState},
    storage::{AcceptOutcome, Journal, StorageError, TradingFacts},
};
use sha2::{Digest, Sha256};

/// Accepts a generation-fenced signed readback into the one authoritative fact stream.
#[derive(Debug, Default)]
pub struct Reconciler {
    facts: TradingFacts,
}

/// One coherent signed REST observation. All arrays must describe the same account/symbol scope.
#[derive(Clone, Copy, Debug)]
pub struct ReadbackBatch<'a> {
    pub generation: u64,
    pub received_at_ms: u64,
    pub balances: &'a [AccountBalance],
    pub positions: &'a [Position],
    pub orders: &'a [Order],
    pub fills: &'a [Fill],
}

#[derive(Clone, Copy, Debug)]
struct ReadbackContext {
    generation: u64,
    received_at_ms: u64,
}

impl Reconciler {
    /// Rebuilds the in-memory duplicate index from the one durable fact journal before accepting
    /// any new private readback after a process restart.
    pub fn recover(journal: &Journal) -> Result<Self, ReconciliationError> {
        let mut facts = TradingFacts::default();
        for entry in journal.recover()?.entries {
            facts.accept(entry.record);
        }
        Ok(Self { facts })
    }

    pub fn accept_readback(
        &mut self,
        journal: &mut Journal,
        batch: ReadbackBatch<'_>,
    ) -> Result<ReconciliationReport, ReconciliationError> {
        if batch.generation == 0 {
            return Err(ReconciliationError::Generation);
        }
        let context = ReadbackContext {
            generation: batch.generation,
            received_at_ms: batch.received_at_ms,
        };
        let mut report = ReconciliationReport::default();
        self.accept_all(
            journal,
            context,
            "balance",
            batch.balances.iter().cloned().map(DomainEvent::Balance),
            &mut report,
        )?;
        self.accept_all(
            journal,
            context,
            "position",
            batch.positions.iter().cloned().map(DomainEvent::Position),
            &mut report,
        )?;
        self.accept_all(
            journal,
            context,
            "order",
            batch.orders.iter().cloned().map(DomainEvent::Order),
            &mut report,
        )?;
        self.accept_all(
            journal,
            context,
            "fill",
            batch.fills.iter().cloned().map(DomainEvent::Fill),
            &mut report,
        )?;
        Ok(report)
    }

    pub fn resolve_unknown_place(
        &self,
        commands: &mut CommandJournal,
        command_id: &crate::domain::CommandId,
        order: &Order,
    ) -> Result<bool, ReconciliationError> {
        let expected = match commands.receipt(command_id) {
            Some(crate::execution::CommandReceipt {
                command: crate::domain::ExecutionCommand::PlaceLimit(command),
                state: CommandState::Unknown { .. },
                ..
            }) => command,
            _ => return Ok(false),
        };
        if !matches!(
            &order.client_order_id,
            crate::domain::FieldState::Known(readback_id)
                if readback_id == expected.client_order_id.as_str()
        ) || order.symbol != expected.owner.symbol
            || order.side != expected.side
            || !matches!(
                order.position_side,
                crate::domain::FieldState::Known(side) if side == expected.position_side
            )
            || order.quantity != expected.quantity
            || order.limit_price != Some(expected.limit_price)
            || order.reduce_only != expected.reduce_only
        {
            return Ok(false);
        }
        let next_state = match order.state {
            OrderState::Rejected => CommandState::Rejected {
                reason: "signed_order_readback_rejected".to_owned(),
            },
            OrderState::New
            | OrderState::PartiallyFilled
            | OrderState::Filled
            | OrderState::Cancelled
            | OrderState::Expired => CommandState::Accepted {
                venue_order_id: order.order_id.clone(),
            },
            OrderState::Unknown => return Ok(false),
        };
        commands.transition(command_id, next_state)?;
        Ok(true)
    }

    /// Settles an UNKNOWN cancel only when its exact target readback proves the target is no
    /// longer live. Other target states intentionally leave the cancel UNKNOWN: they cannot
    /// prove whether the cancellation raced a fill or was never received.
    pub fn resolve_unknown_cancelled_target(
        &self,
        commands: &mut CommandJournal,
        cancel_command_id: &crate::domain::CommandId,
        target_venue_id: String,
    ) -> Result<bool, ReconciliationError> {
        self.resolve_unknown_cancel(
            commands,
            cancel_command_id,
            CommandState::Accepted {
                venue_order_id: target_venue_id,
            },
        )
    }

    /// An exact current-order/current-strategy readback proves that the prior cancellation did
    /// not remove its target. This is a terminal rejection, not a retry permission.
    pub fn resolve_unknown_cancel_open_target(
        &self,
        commands: &mut CommandJournal,
        cancel_command_id: &crate::domain::CommandId,
    ) -> Result<bool, ReconciliationError> {
        self.resolve_unknown_cancel(
            commands,
            cancel_command_id,
            CommandState::Rejected {
                reason: "exact_target_readback_still_open".to_owned(),
            },
        )
    }

    /// Makes a private stream generation ready only after its full signed readback is durable.
    pub fn accept_private_readback(
        &mut self,
        facts_journal: &mut Journal,
        session: &mut PrivateEvidenceSession,
        batch: ReadbackBatch<'_>,
    ) -> Result<ReconciliationReport, ReconciliationError> {
        let generation = batch.generation;
        let guard = session.begin_readback_confirmation(generation)?;
        let report = self.accept_readback(facts_journal, batch)?;
        session.finish_readback_confirmation(guard)?;
        Ok(report)
    }

    pub fn facts(&self) -> &TradingFacts {
        &self.facts
    }

    fn resolve_unknown_cancel(
        &self,
        commands: &mut CommandJournal,
        cancel_command_id: &crate::domain::CommandId,
        next_state: CommandState,
    ) -> Result<bool, ReconciliationError> {
        if !matches!(
            commands.receipt(cancel_command_id),
            Some(crate::execution::CommandReceipt {
                command: crate::domain::ExecutionCommand::Cancel(_),
                state: CommandState::Unknown { .. },
                ..
            })
        ) {
            return Ok(false);
        }
        commands.transition(cancel_command_id, next_state)?;
        Ok(true)
    }

    fn accept_all(
        &mut self,
        journal: &mut Journal,
        context: ReadbackContext,
        kind: &str,
        events: impl IntoIterator<Item = DomainEvent>,
        report: &mut ReconciliationReport,
    ) -> Result<(), ReconciliationError> {
        for event in events {
            let event_id = fact_event_id(kind, &event)?;
            if self.facts.contains(&event_id) {
                report.duplicate += 1;
                continue;
            }
            let record = FactRecord {
                header: EventHeader {
                    schema_version: 1,
                    event_id,
                    source: EventSource::Readback,
                    source_sequence: None,
                    received_at_ms: context.received_at_ms,
                    generation: context.generation,
                },
                event,
            };
            journal.append(record.clone())?;
            match self.facts.accept(record) {
                AcceptOutcome::Accepted => report.accepted += 1,
                AcceptOutcome::Late => report.late += 1,
                AcceptOutcome::Duplicate => report.duplicate += 1,
            }
        }
        Ok(())
    }
}

/// A readback event's identity is derived from its normalized content, never from the poll
/// counter or session generation. Repeated snapshots therefore cannot create duplicate fills or
/// order facts, while a real state change produces a new immutable observation.
fn fact_event_id(kind: &str, event: &DomainEvent) -> Result<EventId, ReconciliationError> {
    let encoded = serde_json::to_vec(event).map_err(ReconciliationError::Encode)?;
    let digest = Sha256::digest(encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    EventId::new(format!("readback:{kind}:{digest}")).map_err(ReconciliationError::EventId)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconciliationReport {
    pub accepted: u32,
    pub late: u32,
    pub duplicate: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum ReconciliationError {
    #[error("readback generation must be positive")]
    Generation,
    #[error("normalized fact cannot be serialized for stable identity: {0}")]
    Encode(serde_json::Error),
    #[error("fact identity is invalid: {0}")]
    EventId(#[from] EventIdError),
    #[error("authoritative fact journal failed: {0}")]
    Journal(#[from] StorageError),
    #[error("execution journal failed: {0}")]
    Command(#[from] CommandJournalError),
    #[error("private evidence session failed: {0}")]
    PrivateSession(#[from] PrivateSessionError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        domain::{
            AccountBalance, Asset, CommandId, FieldState, Order, OrderPurpose, OrderSide,
            PositionSide, Price, Symbol,
        },
        exchange::private_session::{PrivateEvidenceSession, PrivateSessionState},
        execution::{CommandJournal, CommandState},
        storage::{Journal, PrivateEvidenceJournal},
    };

    use super::*;

    fn order(state: OrderState) -> Result<Order, Box<dyn std::error::Error>> {
        Ok(Order {
            order_id: "10".to_owned(),
            client_order_id: FieldState::Known("client_1".to_owned()),
            symbol: "DOGE/USDT".parse::<Symbol>()?,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            purpose: FieldState::Known(OrderPurpose::Entry),
            state,
            quantity: Decimal::ONE,
            filled_quantity: Decimal::ZERO,
            limit_price: Some(Price::new(Decimal::ONE)?),
            average_price: FieldState::Unavailable {
                reason: crate::domain::UnknownReason::NotYetObserved,
            },
            reduce_only: false,
        })
    }

    #[test]
    fn readback_is_journaled_before_it_is_accepted() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut facts_journal = Journal::open(directory.path().join("facts.jsonl"))?;
        let balance = AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(5, 0),
            available_balance: Decimal::new(5, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        };
        let mut reconciler = Reconciler::default();

        let orders = [order(OrderState::New)?];
        let report = reconciler.accept_readback(
            &mut facts_journal,
            ReadbackBatch {
                generation: 1,
                received_at_ms: 1,
                balances: &[balance],
                positions: &[],
                orders: &orders,
                fills: &[],
            },
        )?;
        assert_eq!(report.accepted, 2);
        assert_eq!(facts_journal.recover()?.entries.len(), 2);
        assert_eq!(reconciler.facts().records().len(), 2);
        Ok(())
    }

    #[test]
    fn repeated_signed_readback_never_duplicates_authoritative_facts()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut facts_journal = Journal::open(directory.path().join("facts.jsonl"))?;
        let balance = AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(5, 0),
            available_balance: Decimal::new(5, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        };
        let orders = [order(OrderState::New)?];
        let batch = ReadbackBatch {
            generation: 1,
            received_at_ms: 1,
            balances: &[balance],
            positions: &[],
            orders: &orders,
            fills: &[],
        };
        let mut reconciler = Reconciler::default();
        assert_eq!(
            reconciler
                .accept_readback(&mut facts_journal, batch)?
                .accepted,
            2
        );
        let duplicate = reconciler.accept_readback(&mut facts_journal, batch)?;
        assert_eq!(duplicate.duplicate, 2);
        assert_eq!(duplicate.accepted, 0);
        assert_eq!(facts_journal.recover()?.entries.len(), 2);
        assert_eq!(reconciler.facts().records().len(), 2);
        Ok(())
    }

    #[test]
    fn restart_rebuilds_readback_duplicate_identity() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("facts.jsonl");
        let mut facts_journal = Journal::open(&path)?;
        let balance = AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(5, 0),
            available_balance: Decimal::new(5, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        };
        let batch = ReadbackBatch {
            generation: 1,
            received_at_ms: 1,
            balances: &[balance],
            positions: &[],
            orders: &[],
            fills: &[],
        };
        let mut original = Reconciler::default();
        original.accept_readback(&mut facts_journal, batch)?;

        let mut restarted_journal = Journal::open(path)?;
        let mut restarted = Reconciler::recover(&restarted_journal)?;
        let report = restarted.accept_readback(&mut restarted_journal, batch)?;
        assert_eq!(report.duplicate, 1);
        assert_eq!(restarted_journal.recover()?.entries.len(), 1);
        Ok(())
    }

    #[test]
    fn only_exact_readback_resolves_unknown_place() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
        let command = crate::domain::OrderCommand {
            command_id: CommandId::new("command_1")?,
            client_order_id: CommandId::new("client_1")?,
            owner: crate::domain::OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        commands.prepare_place(command.clone())?;
        commands.transition(&command.command_id, CommandState::Submitted)?;
        commands.transition(
            &command.command_id,
            CommandState::Unknown {
                reason: "timeout".to_owned(),
            },
        )?;

        let mut wrong_hedge_side = order(OrderState::New)?;
        wrong_hedge_side.position_side = FieldState::Known(PositionSide::Short);
        assert!(!Reconciler::default().resolve_unknown_place(
            &mut commands,
            &command.command_id,
            &wrong_hedge_side,
        )?);
        assert!(matches!(
            commands
                .receipt(&command.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));

        assert!(Reconciler::default().resolve_unknown_place(
            &mut commands,
            &command.command_id,
            &order(OrderState::New)?,
        )?);
        assert!(matches!(
            commands
                .receipt(&command.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Accepted { .. })
        ));
        Ok(())
    }

    #[test]
    fn ordinary_order_reconciler_cannot_settle_conditional_protection()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
        let protection = crate::domain::StopMarketCloseAllCommand {
            command_id: CommandId::new("protect_1")?,
            client_strategy_id: CommandId::new("client_1")?,
            owner: crate::domain::OrderOwner {
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
            position_generation: 1,
        };
        commands.prepare_stop_market_close_all(protection.clone())?;
        commands.transition(&protection.command_id, CommandState::Submitted)?;
        commands.transition(
            &protection.command_id,
            CommandState::Unknown {
                reason: "timeout".to_owned(),
            },
        )?;

        assert!(!Reconciler::default().resolve_unknown_place(
            &mut commands,
            &protection.command_id,
            &order(OrderState::New)?,
        )?);
        assert!(matches!(
            commands
                .receipt(&protection.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));
        Ok(())
    }

    #[test]
    fn exact_cancelled_target_settles_unknown_cancel_but_open_target_rejects_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
        let order = crate::domain::OrderCommand {
            command_id: CommandId::new("order_1")?,
            client_order_id: CommandId::new("client_1")?,
            owner: crate::domain::OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        commands.prepare_place(order.clone())?;
        let cancelled = crate::domain::CancelCommand {
            command_id: CommandId::new("cancel_1")?,
            owner: order.owner.clone(),
            target_client_order_id: order.client_order_id.clone(),
        };
        commands.prepare_cancel(cancelled.clone())?;
        commands.transition(&cancelled.command_id, CommandState::Submitted)?;
        commands.transition(
            &cancelled.command_id,
            CommandState::Unknown {
                reason: "timeout".to_owned(),
            },
        )?;
        assert!(Reconciler::default().resolve_unknown_cancelled_target(
            &mut commands,
            &cancelled.command_id,
            "10".to_owned(),
        )?);
        assert!(matches!(
            commands
                .receipt(&cancelled.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Accepted { .. })
        ));

        let open = crate::domain::CancelCommand {
            command_id: CommandId::new("cancel_2")?,
            owner: order.owner.clone(),
            target_client_order_id: order.client_order_id,
        };
        commands.prepare_cancel(open.clone())?;
        commands.transition(&open.command_id, CommandState::Submitted)?;
        commands.transition(
            &open.command_id,
            CommandState::Unknown {
                reason: "timeout".to_owned(),
            },
        )?;
        assert!(
            Reconciler::default()
                .resolve_unknown_cancel_open_target(&mut commands, &open.command_id,)?
        );
        assert!(matches!(
            commands
                .receipt(&open.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Rejected { .. })
        ));
        Ok(())
    }

    #[test]
    fn conditional_target_cancel_uses_the_cancel_reconciler_not_an_order_fact()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
        let protection = crate::domain::StopMarketCloseAllCommand {
            command_id: CommandId::new("protect_1")?,
            client_strategy_id: CommandId::new("strategy_1")?,
            owner: crate::domain::OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Protection,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            stop_price: Price::new(Decimal::ONE)?,
            position_generation: 1,
        };
        commands.prepare_stop_market_close_all(protection.clone())?;
        let cancel = crate::domain::CancelCommand {
            command_id: CommandId::new("cancel_1")?,
            owner: protection.owner.clone(),
            target_client_order_id: protection.client_strategy_id.clone(),
        };
        commands.prepare_cancel(cancel.clone())?;
        commands.transition(&cancel.command_id, CommandState::Submitted)?;
        commands.transition(
            &cancel.command_id,
            CommandState::Unknown {
                reason: "timeout".to_owned(),
            },
        )?;

        assert!(
            Reconciler::default()
                .resolve_unknown_cancel_open_target(&mut commands, &cancel.command_id,)?
        );
        assert!(matches!(
            commands
                .receipt(&cancel.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Rejected { .. })
        ));
        Ok(())
    }

    #[test]
    fn private_session_becomes_ready_only_after_durable_fact_acceptance()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let evidence = PrivateEvidenceJournal::open(directory.path().join("evidence.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(evidence);
        session.ingest(1, r#"{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{}}"#.to_owned())?;
        let balance = AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(5, 0),
            available_balance: Decimal::new(5, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        };
        let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
        let mut reconciler = Reconciler::default();

        reconciler.accept_private_readback(
            &mut facts,
            &mut session,
            ReadbackBatch {
                generation: 1,
                received_at_ms: 2,
                balances: &[balance],
                positions: &[],
                orders: &[],
                fills: &[],
            },
        )?;
        assert_eq!(session.state(), PrivateSessionState::Ready);
        assert_eq!(facts.recover()?.entries.len(), 1);
        Ok(())
    }

    #[test]
    fn stale_private_readback_is_rejected_before_any_fact_is_journaled()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let evidence = PrivateEvidenceJournal::open(directory.path().join("evidence.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(evidence);
        session.on_disconnect()?;
        let mut facts = Journal::open(directory.path().join("facts.jsonl"))?;
        let mut reconciler = Reconciler::default();

        assert!(
            reconciler
                .accept_private_readback(
                    &mut facts,
                    &mut session,
                    ReadbackBatch {
                        generation: 1,
                        received_at_ms: 2,
                        balances: &[],
                        positions: &[],
                        orders: &[],
                        fills: &[],
                    },
                )
                .is_err()
        );
        assert!(facts.recover()?.entries.is_empty());
        Ok(())
    }
}
