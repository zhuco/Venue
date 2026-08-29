use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    domain::Symbol,
    storage::{PrivateEvidence, PrivateEvidenceError, PrivateEvidenceJournal},
};

use super::private_session_state::{
    DurablePrivateSessionError, DurablePrivateSessionState, DurablePrivateSessionStore,
    PrivateSessionStateGuard,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivateSessionState {
    NeedsReadback,
    Ready,
    Reconnecting,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrivateSessionBinding {
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
}

impl PrivateSessionBinding {
    fn valid(&self) -> bool {
        !self.exchange.trim().is_empty() && !self.account.trim().is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateSignal {
    ReadbackRequired,
    OrderLifecycleDebounced,
    RiskAlert,
    StreamExpired { generation: u64 },
}

enum InboundOutcome {
    Signal(PrivateSignal),
    InvalidPayload,
    UnsupportedEvent,
}

/// A private stream is evidence only. It becomes ready solely after a generation-matching signed
/// REST readback has been journaled into authoritative facts.
#[derive(Debug)]
pub struct PrivateEvidenceSession {
    journal: PrivateEvidenceJournal,
    generation: u64,
    state: PrivateSessionState,
    durable: Option<DurablePrivateSessionState>,
}

pub(crate) struct PrivateReadbackGuard {
    durable: Option<PrivateSessionStateGuard>,
    generation: u64,
    evidence_sequence: u64,
}

impl PrivateEvidenceSession {
    pub fn new(journal: PrivateEvidenceJournal) -> Self {
        Self {
            journal,
            generation: 1,
            state: PrivateSessionState::NeedsReadback,
            durable: None,
        }
    }

    /// Opens a deployment session by durably revoking any readiness held by a predecessor.
    /// A new socket and a same-generation signed readback are still required before Ready.
    pub fn open_durable(
        journal: PrivateEvidenceJournal,
        state_path: impl Into<PathBuf>,
        binding: PrivateSessionBinding,
    ) -> Result<Self, PrivateSessionError> {
        if !binding.valid() {
            return Err(PrivateSessionError::Binding);
        }
        let durable = DurablePrivateSessionStore::open(state_path, binding)
            .recover_and_fence(journal.last_sequence())?;
        Ok(Self {
            journal,
            generation: durable.snapshot().generation,
            state: durable.snapshot().state,
            durable: Some(durable),
        })
    }

    pub fn ingest(
        &mut self,
        received_at_ms: u64,
        payload: String,
    ) -> Result<PrivateSignal, PrivateSessionError> {
        if matches!(
            self.state,
            PrivateSessionState::Reconnecting | PrivateSessionState::Expired
        ) {
            return Err(PrivateSessionError::ReconnectRequired);
        }
        let evidence = PrivateEvidence::new(self.generation, received_at_ms, payload.clone())
            .map_err(PrivateSessionError::Evidence)?;
        let parsed = serde_json::from_str::<Value>(&payload).ok();
        let parsed_name = parsed
            .as_ref()
            .and_then(|event| event.get("e").and_then(Value::as_str).map(str::to_owned));
        let (generation, state, outcome) = match parsed_name.as_deref() {
            Some("ORDER_TRADE_UPDATE") if order_lifecycle_can_debounce(parsed.as_ref()) => (
                self.generation,
                PrivateSessionState::NeedsReadback,
                InboundOutcome::Signal(PrivateSignal::OrderLifecycleDebounced),
            ),
            Some(
                "ACCOUNT_UPDATE"
                | "ORDER_TRADE_UPDATE"
                | "CONDITIONAL_ORDER_TRADE_UPDATE"
                | "ACCOUNT_CONFIG_UPDATE"
                | "ALGO_UPDATE",
            ) => (
                self.generation,
                PrivateSessionState::NeedsReadback,
                InboundOutcome::Signal(PrivateSignal::ReadbackRequired),
            ),
            Some("MARGIN_CALL" | "RISK_LEVEL_CHANGE") => (
                self.generation,
                PrivateSessionState::NeedsReadback,
                InboundOutcome::Signal(PrivateSignal::RiskAlert),
            ),
            Some("listenKeyExpired") => {
                let generation = self.next_generation()?;
                (
                    generation,
                    PrivateSessionState::Expired,
                    InboundOutcome::Signal(PrivateSignal::StreamExpired { generation }),
                )
            }
            Some(_) => (
                self.next_generation()?,
                PrivateSessionState::Reconnecting,
                InboundOutcome::UnsupportedEvent,
            ),
            None => (
                self.next_generation()?,
                PrivateSessionState::Reconnecting,
                InboundOutcome::InvalidPayload,
            ),
        };
        self.append_and_persist(evidence, generation, state)?;
        match outcome {
            InboundOutcome::Signal(signal) => Ok(signal),
            InboundOutcome::InvalidPayload => Err(PrivateSessionError::Payload),
            InboundOutcome::UnsupportedEvent => Err(PrivateSessionError::Event),
        }
    }

    /// Revokes the current private-data generation before any reconnect attempt.
    /// Duplicate disconnect notifications do not advance the generation twice.
    pub fn on_disconnect(&mut self) -> Result<u64, PrivateSessionError> {
        let generation = if !matches!(
            self.state,
            PrivateSessionState::Reconnecting | PrivateSessionState::Expired
        ) {
            self.next_generation()?
        } else {
            self.generation
        };
        self.persist(
            generation,
            PrivateSessionState::Reconnecting,
            self.journal.last_sequence(),
        )?;
        Ok(self.generation)
    }

    /// Records that a new listenKey/socket is attached.  It never restores
    /// readiness: a signed readback for this exact generation is still needed.
    pub fn on_reconnect(&mut self) -> Result<u64, PrivateSessionError> {
        if self.state == PrivateSessionState::Ready {
            return Err(PrivateSessionError::ReconnectState);
        }
        self.persist(
            self.generation,
            PrivateSessionState::NeedsReadback,
            self.journal.last_sequence(),
        )?;
        Ok(self.generation)
    }

    /// Revokes a stale Ready projection while retaining the current live stream generation.
    /// Unlike reconnect, this is a local durable transition: the next signed REST readback must
    /// prove the same session is still safe before readiness can be restored.
    pub fn require_fresh_readback(&mut self) -> Result<(), PrivateSessionError> {
        if self.state != PrivateSessionState::Ready {
            return Err(PrivateSessionError::ReadbackNotRequired);
        }
        self.persist(
            self.generation,
            PrivateSessionState::NeedsReadback,
            self.journal.last_sequence(),
        )
    }

    /// Checks the generation before reconciliation writes any authoritative
    /// fact, so a stale readback cannot become durable after a disconnect.
    pub fn require_readback_generation(&self, generation: u64) -> Result<(), PrivateSessionError> {
        if generation != self.generation {
            return Err(PrivateSessionError::Generation);
        }
        if self.state != PrivateSessionState::NeedsReadback {
            return Err(PrivateSessionError::ReadbackNotRequired);
        }
        Ok(())
    }

    pub fn confirm_readback(&mut self, generation: u64) -> Result<(), PrivateSessionError> {
        let guard = self.begin_readback_confirmation(generation)?;
        self.finish_readback_confirmation(guard)
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn state(&self) -> PrivateSessionState {
        self.state
    }

    pub fn journal(&self) -> &PrivateEvidenceJournal {
        &self.journal
    }

    pub(crate) fn begin_readback_confirmation(
        &self,
        generation: u64,
    ) -> Result<PrivateReadbackGuard, PrivateSessionError> {
        self.require_readback_generation(generation)?;
        let durable = self
            .durable
            .as_ref()
            .map(DurablePrivateSessionState::begin_transition)
            .transpose()?;
        Ok(PrivateReadbackGuard {
            durable,
            generation,
            evidence_sequence: self.journal.last_sequence(),
        })
    }

    pub(crate) fn validate_readback_ticket(
        &self,
        generation: u64,
        evidence_sequence: u64,
    ) -> Result<(), PrivateSessionError> {
        self.require_readback_generation(generation)?;
        if evidence_sequence != self.journal.last_sequence() {
            return Err(PrivateSessionError::Generation);
        }
        let _guard = self
            .durable
            .as_ref()
            .map(DurablePrivateSessionState::begin_transition)
            .transpose()?;
        Ok(())
    }

    pub(crate) fn finish_readback_confirmation(
        &mut self,
        guard: PrivateReadbackGuard,
    ) -> Result<(), PrivateSessionError> {
        if guard.generation != self.generation
            || guard.evidence_sequence != self.journal.last_sequence()
            || self.state != PrivateSessionState::NeedsReadback
        {
            return Err(PrivateSessionError::Generation);
        }
        if let (Some(durable), Some(state_guard)) = (self.durable.as_mut(), guard.durable) {
            durable.finish_transition(
                state_guard,
                guard.generation,
                PrivateSessionState::Ready,
                guard.evidence_sequence,
            )?;
        }
        self.state = PrivateSessionState::Ready;
        Ok(())
    }

    pub(crate) fn fail_closed_in_memory(&mut self) {
        self.generation = self.generation.saturating_add(1).max(1);
        self.state = PrivateSessionState::Reconnecting;
    }

    fn next_generation(&self) -> Result<u64, PrivateSessionError> {
        self.generation
            .checked_add(1)
            .ok_or(PrivateSessionError::Generation)
    }

    fn persist(
        &mut self,
        generation: u64,
        state: PrivateSessionState,
        evidence_sequence: u64,
    ) -> Result<(), PrivateSessionError> {
        if let Some(durable) = self.durable.as_mut() {
            durable.commit(generation, state, evidence_sequence)?;
        }
        self.generation = generation;
        self.state = state;
        Ok(())
    }

    fn append_and_persist(
        &mut self,
        evidence: PrivateEvidence,
        generation: u64,
        state: PrivateSessionState,
    ) -> Result<(), PrivateSessionError> {
        let guard = self
            .durable
            .as_ref()
            .map(DurablePrivateSessionState::begin_transition)
            .transpose()?;
        let sequence = self
            .journal
            .append(evidence)
            .map_err(PrivateSessionError::Evidence)?;
        if let (Some(durable), Some(guard)) = (self.durable.as_mut(), guard)
            && let Err(error) = durable.finish_transition(guard, generation, state, sequence)
        {
            self.generation = generation;
            self.state = PrivateSessionState::Reconnecting;
            return Err(error.into());
        }
        self.generation = generation;
        self.state = state;
        Ok(())
    }
}

fn order_lifecycle_can_debounce(event: Option<&Value>) -> bool {
    matches!(
        event
            .and_then(|event| event.get("o"))
            .and_then(|order| order.get("x"))
            .and_then(Value::as_str),
        Some("NEW" | "CANCELED" | "EXPIRED" | "REJECTED" | "AMENDMENT")
    )
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateSessionError {
    #[error("private evidence journal failed: {0}")]
    Evidence(PrivateEvidenceError),
    #[error("durable private-session state failed")]
    Durable,
    #[error("private-session binding is invalid")]
    Binding,
    #[error("private stream payload is invalid")]
    Payload,
    #[error("private stream event is unsupported")]
    Event,
    #[error("private stream generation does not match a live readback session")]
    Generation,
    #[error("private stream must reconnect before it can accept evidence")]
    ReconnectRequired,
    #[error("private reconnect is only valid after readiness was revoked")]
    ReconnectState,
    #[error("a signed readback is not currently required for this private session")]
    ReadbackNotRequired,
}

impl From<DurablePrivateSessionError> for PrivateSessionError {
    fn from(_: DurablePrivateSessionError) -> Self {
        Self::Durable
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn push_is_evidence_until_matching_readback_confirms_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(journal);

        assert_eq!(
            session.ingest(
                1,
                r#"{"e":"ORDER_TRADE_UPDATE","E":1,"T":1,"o":{}}"#.to_owned()
            )?,
            PrivateSignal::ReadbackRequired
        );
        assert_eq!(session.state(), PrivateSessionState::NeedsReadback);
        session.confirm_readback(1)?;
        assert_eq!(session.state(), PrivateSessionState::Ready);
        assert_eq!(session.journal().recover()?.len(), 1);
        Ok(())
    }

    #[test]
    fn ordinary_order_lifecycle_events_debounce_but_trade_events_do_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(journal);
        session.confirm_readback(1)?;

        assert_eq!(
            session.ingest(
                1,
                r#"{"e":"ORDER_TRADE_UPDATE","o":{"x":"NEW","X":"NEW"}}"#.to_owned(),
            )?,
            PrivateSignal::OrderLifecycleDebounced
        );
        assert_eq!(session.state(), PrivateSessionState::NeedsReadback);
        assert_eq!(
            session.ingest(
                2,
                r#"{"e":"ORDER_TRADE_UPDATE","o":{"x":"TRADE","X":"FILLED"}}"#.to_owned(),
            )?,
            PrivateSignal::ReadbackRequired
        );
        assert_eq!(session.journal().recover()?.len(), 2);
        Ok(())
    }

    #[test]
    fn disconnected_stream_requires_reconnect_and_same_generation_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(journal);
        session.on_disconnect()?;
        assert_eq!(session.generation(), 2);
        assert!(session.confirm_readback(1).is_err());
        assert!(session.confirm_readback(2).is_err());
        session.on_reconnect()?;
        session.confirm_readback(2)?;
        assert_eq!(session.state(), PrivateSessionState::Ready);
        Ok(())
    }

    #[test]
    fn periodic_readback_revokes_ready_without_reconnecting()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(journal);
        session.on_reconnect()?;
        session.confirm_readback(1)?;

        session.require_fresh_readback()?;
        assert_eq!(session.generation(), 1);
        assert_eq!(session.state(), PrivateSessionState::NeedsReadback);
        session.confirm_readback(1)?;
        assert_eq!(session.state(), PrivateSessionState::Ready);
        Ok(())
    }

    #[test]
    fn durable_restart_fences_a_periodic_readback_before_it_can_restore_ready()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal_path = directory.path().join("private.jsonl");
        let state_path = directory.path().join("private_state.json");
        let binding = PrivateSessionBinding {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
        };
        let mut first = PrivateEvidenceSession::open_durable(
            PrivateEvidenceJournal::open(&journal_path)?,
            &state_path,
            binding.clone(),
        )?;
        first.on_reconnect()?;
        first.confirm_readback(1)?;
        first.require_fresh_readback()?;
        drop(first);

        let mut recovered = PrivateEvidenceSession::open_durable(
            PrivateEvidenceJournal::open(&journal_path)?,
            &state_path,
            binding,
        )?;
        assert_eq!(recovered.generation(), 2);
        assert_eq!(recovered.state(), PrivateSessionState::Reconnecting);
        assert!(recovered.confirm_readback(1).is_err());
        recovered.on_reconnect()?;
        recovered.confirm_readback(2)?;
        assert_eq!(recovered.state(), PrivateSessionState::Ready);
        Ok(())
    }

    #[test]
    fn expired_listen_key_rejects_old_stream_evidence_until_reconnected()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(journal);

        assert_eq!(
            session.ingest(1, r#"{"e":"listenKeyExpired"}"#.to_owned())?,
            PrivateSignal::StreamExpired { generation: 2 }
        );
        assert!(matches!(
            session.ingest(2, r#"{"e":"ORDER_TRADE_UPDATE","o":{}}"#.to_owned()),
            Err(PrivateSessionError::ReconnectRequired)
        ));
        session.on_reconnect()?;
        session.confirm_readback(2)?;
        assert_eq!(session.state(), PrivateSessionState::Ready);
        Ok(())
    }

    #[test]
    fn conditional_and_risk_events_revoke_private_readback_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(journal);

        assert_eq!(
            session.ingest(
                1,
                r#"{"e":"CONDITIONAL_ORDER_TRADE_UPDATE","E":1,"T":1,"o":{}}"#.to_owned()
            )?,
            PrivateSignal::ReadbackRequired
        );
        session.confirm_readback(1)?;
        assert_eq!(
            session.ingest(2, r#"{"e":"RISK_LEVEL_CHANGE","E":2}"#.to_owned())?,
            PrivateSignal::RiskAlert
        );
        assert_eq!(session.state(), PrivateSessionState::NeedsReadback);
        Ok(())
    }

    #[test]
    fn malformed_or_unsupported_inbound_evidence_fences_readiness()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let mut session = PrivateEvidenceSession::new(journal);
        session.confirm_readback(1)?;

        assert!(matches!(
            session.ingest(2, r#"{"missing_event_name":true}"#.to_owned()),
            Err(PrivateSessionError::Payload)
        ));
        assert_eq!(session.state(), PrivateSessionState::Reconnecting);
        assert_eq!(session.generation(), 2);
        assert_eq!(session.journal().recover()?.len(), 1);
        Ok(())
    }

    #[test]
    fn durable_restart_revokes_ready_and_advances_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal_path = directory.path().join("private.jsonl");
        let state_path = directory.path().join("private_state.json");
        let binding = PrivateSessionBinding {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
        };
        let journal = PrivateEvidenceJournal::open(&journal_path)?;
        let mut first =
            PrivateEvidenceSession::open_durable(journal, &state_path, binding.clone())?;
        assert_eq!(first.state(), PrivateSessionState::Reconnecting);
        assert_eq!(first.generation(), 1);
        first.on_reconnect()?;
        first.confirm_readback(1)?;
        assert_eq!(first.state(), PrivateSessionState::Ready);
        drop(first);

        let journal = PrivateEvidenceJournal::open(&journal_path)?;
        let mut second = PrivateEvidenceSession::open_durable(journal, &state_path, binding)?;
        assert_eq!(second.state(), PrivateSessionState::Reconnecting);
        assert_eq!(second.generation(), 2);
        assert!(second.confirm_readback(1).is_err());
        second.on_reconnect()?;
        second.confirm_readback(2)?;
        assert_eq!(second.state(), PrivateSessionState::Ready);
        Ok(())
    }

    #[test]
    fn stale_worker_cannot_append_after_a_new_worker_fences_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let journal_path = directory.path().join("private.jsonl");
        let state_path = directory.path().join("private_state.json");
        let binding = PrivateSessionBinding {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
        };
        let mut stale = PrivateEvidenceSession::open_durable(
            PrivateEvidenceJournal::open(&journal_path)?,
            &state_path,
            binding.clone(),
        )?;
        stale.on_reconnect()?;
        stale.confirm_readback(1)?;

        let mut current = PrivateEvidenceSession::open_durable(
            PrivateEvidenceJournal::open(&journal_path)?,
            &state_path,
            binding,
        )?;
        assert!(matches!(
            stale.ingest(
                2,
                r#"{"e":"ORDER_TRADE_UPDATE","E":2,"T":2,"o":{}}"#.to_owned()
            ),
            Err(PrivateSessionError::Durable)
        ));
        assert!(current.journal().recover()?.is_empty());

        current.on_reconnect()?;
        assert_eq!(
            current.ingest(
                3,
                r#"{"e":"ORDER_TRADE_UPDATE","E":3,"T":3,"o":{}}"#.to_owned()
            )?,
            PrivateSignal::ReadbackRequired
        );
        assert_eq!(current.journal().recover()?.len(), 1);
        Ok(())
    }
}
