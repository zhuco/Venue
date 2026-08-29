use std::{process, time::Duration};

use venue_domain::domain::{Fill, Order, Position};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateFactsWorkerState {
    Backoff,
    NeedsBootstrap,
    Ready,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateBootstrapScope {
    Account,
    Positions,
    PositionMode,
    AccountConfig,
    Orders,
    AlgoOrders,
    Fills,
}

impl PrivateBootstrapScope {
    const fn next(self) -> Option<Self> {
        match self {
            Self::Account => Some(Self::Positions),
            Self::Positions => Some(Self::PositionMode),
            Self::PositionMode => Some(Self::AccountConfig),
            Self::AccountConfig => Some(Self::Orders),
            Self::Orders => Some(Self::AlgoOrders),
            Self::AlgoOrders => Some(Self::Fills),
            Self::Fills => None,
        }
    }
}

/// Non-sensitive location of the most recent failed worker effect. Raw transport and exchange
/// payload errors stay in the adapter host and must not enter durable runtime state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateFactsFailureStage {
    Connect,
    ConnectCompletion,
    BootstrapTransport(PrivateBootstrapScope),
    BootstrapCompletion(PrivateBootstrapScope),
    FrameTransport,
    FrameCompletion,
    Keepalive,
    KeepaliveCompletion,
}

/// A readback authority is bound to both the private connection generation and the durable raw
/// evidence tail. The fields remain opaque so callers cannot construct an unproven ticket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateReadbackTicket {
    generation: u64,
    evidence_sequence: u64,
}

impl PrivateReadbackTicket {
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn evidence_sequence(self) -> u64 {
        self.evidence_sequence
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateFactsEffect {
    Connect {
        effect_id: u64,
    },
    Bootstrap {
        effect_id: u64,
        ticket: PrivateReadbackTicket,
        scope: PrivateBootstrapScope,
    },
    ReceiveFrame {
        effect_id: u64,
        generation: u64,
        next_sequence: u64,
    },
    Keepalive {
        effect_id: u64,
        generation: u64,
    },
}

impl PrivateFactsEffect {
    #[must_use]
    pub const fn effect_id(self) -> u64 {
        match self {
            Self::Connect { effect_id }
            | Self::Bootstrap { effect_id, .. }
            | Self::ReceiveFrame { effect_id, .. }
            | Self::Keepalive { effect_id, .. } => effect_id,
        }
    }
}

/// The only private-session identity that may cross into runtime admission. A host mints it only
/// after a durable, same-ticket signed readback; socket activity alone is never sufficient.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateFactsReadiness {
    pub generation: u64,
    pub observed_at_ms: u64,
    pub root_cause_fact_id: String,
    pub exposure: PrivateExposure,
    pub ordinary_order_debt: bool,
    pub algo_order_debt: bool,
}

/// Normalized private facts retained at the runtime boundary. Native symbols, raw protocol
/// fields, credentials, transports, and physical clients are intentionally not representable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateFactsSnapshot {
    pub generation: u64,
    pub observed_at_ms: u64,
    pub can_trade: bool,
    pub hedge_position: bool,
    pub positions: Vec<Position>,
    pub orders: Vec<Order>,
    pub fills: Vec<Fill>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateFactsClockRoot {
    pub observed_at_ms: u64,
    pub root_cause_fact_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateExposure {
    Flat,
    Open,
}

/// Exchange-independent scheduling policy. The host owns all transport and durable session I/O;
/// this policy only controls when the next evidence-producing effect may be requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateFactsSchedulePolicy {
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub keepalive_ms: u64,
    pub stream_burst_coalesce_ms: u64,
    pub frame_poll_ms: u64,
}

impl PrivateFactsSchedulePolicy {
    fn validate(self) -> Result<Self, PrivateFactsScheduleError> {
        if self.base_backoff_ms == 0
            || self.max_backoff_ms < self.base_backoff_ms
            || self.keepalive_ms == 0
            || self.stream_burst_coalesce_ms == 0
            || self.frame_poll_ms == 0
        {
            return Err(PrivateFactsScheduleError::Policy);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingEffect {
    effect_id: u64,
    kind: PendingEffectKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingEffectKind {
    Connect,
    Bootstrap {
        ticket: PrivateReadbackTicket,
        scope: PrivateBootstrapScope,
    },
    ReceiveFrame {
        generation: u64,
        sequence: u64,
    },
    Keepalive {
        generation: u64,
    },
}

/// Deterministic single-in-flight scheduler for a private-facts worker. It cannot open a socket,
/// parse a payload, persist evidence, issue a writer lease, or dispatch a mutation.
#[derive(Clone, Debug)]
pub struct PrivateFactsScheduler {
    policy: PrivateFactsSchedulePolicy,
    backoff_scope: String,
    state: PrivateFactsWorkerState,
    bootstrap_scope: PrivateBootstrapScope,
    pending: Option<PendingEffect>,
    next_effect_id: u64,
    next_retry_at_ms: u64,
    next_keepalive_at_ms: u64,
    next_refresh_at_ms: Option<u64>,
    stream_readback_due_at_ms: Option<u64>,
    periodic_readback_interval_ms: Option<u64>,
    consecutive_failures: u8,
    last_frame_sequence: u64,
    last_frame_poll_at_ms: u64,
    periodic_readback_in_progress: bool,
    last_failure_stage: Option<PrivateFactsFailureStage>,
}

impl PrivateFactsScheduler {
    pub fn new(
        policy: PrivateFactsSchedulePolicy,
        backoff_scope: impl Into<String>,
    ) -> Result<Self, PrivateFactsScheduleError> {
        let policy = policy.validate()?;
        let backoff_scope = backoff_scope.into();
        if backoff_scope.trim().is_empty() {
            return Err(PrivateFactsScheduleError::Policy);
        }
        Ok(Self {
            policy,
            backoff_scope,
            state: PrivateFactsWorkerState::Backoff,
            bootstrap_scope: PrivateBootstrapScope::Account,
            pending: None,
            next_effect_id: 1,
            next_retry_at_ms: 0,
            next_keepalive_at_ms: 0,
            next_refresh_at_ms: None,
            stream_readback_due_at_ms: None,
            periodic_readback_interval_ms: None,
            consecutive_failures: 0,
            last_frame_sequence: 0,
            last_frame_poll_at_ms: 0,
            periodic_readback_in_progress: false,
            last_failure_stage: None,
        })
    }

    #[must_use]
    pub const fn state(&self) -> PrivateFactsWorkerState {
        self.state
    }

    #[must_use]
    pub const fn periodic_readback_in_progress(&self) -> bool {
        self.periodic_readback_in_progress
    }

    #[must_use]
    pub const fn last_failure_stage(&self) -> Option<PrivateFactsFailureStage> {
        self.last_failure_stage
    }

    #[must_use]
    pub const fn pending_effect_id(&self) -> Option<u64> {
        match self.pending {
            Some(pending) => Some(pending.effect_id),
            None => None,
        }
    }

    #[must_use]
    pub const fn next_retry_at_ms(&self) -> u64 {
        self.next_retry_at_ms
    }

    pub fn record_failure(&mut self, stage: PrivateFactsFailureStage) {
        self.last_failure_stage = Some(stage);
    }

    pub fn clear_failure(&mut self) {
        self.last_failure_stage = None;
    }

    pub fn set_periodic_readback_interval(
        &mut self,
        interval_ms: u64,
    ) -> Result<(), PrivateFactsScheduleError> {
        if interval_ms == 0 {
            return Err(PrivateFactsScheduleError::Effect);
        }
        self.periodic_readback_interval_ms = Some(interval_ms);
        Ok(())
    }

    #[must_use]
    pub fn stream_readback_due(&self, now_ms: u64) -> bool {
        self.state == PrivateFactsWorkerState::Ready
            && self
                .stream_readback_due_at_ms
                .is_some_and(|due_at_ms| now_ms >= due_at_ms)
    }

    #[must_use]
    pub fn periodic_readback_due(&self, now_ms: u64) -> bool {
        self.state == PrivateFactsWorkerState::Ready
            && self.stream_readback_due_at_ms.is_none()
            && self.next_refresh_at_ms.is_some_and(|refresh_at_ms| {
                now_ms >= refresh_at_ms && self.last_frame_poll_at_ms >= refresh_at_ms
            })
    }

    pub fn next_effect(
        &mut self,
        now_ms: u64,
        generation: u64,
        evidence_sequence: u64,
    ) -> Result<Option<PrivateFactsEffect>, PrivateFactsScheduleError> {
        if generation == 0 {
            return Err(PrivateFactsScheduleError::Generation);
        }
        if self.pending.is_some() {
            return Ok(None);
        }
        let kind = match self.state {
            PrivateFactsWorkerState::Backoff if now_ms < self.next_retry_at_ms => return Ok(None),
            PrivateFactsWorkerState::Backoff => PendingEffectKind::Connect,
            PrivateFactsWorkerState::NeedsBootstrap => PendingEffectKind::Bootstrap {
                ticket: PrivateReadbackTicket {
                    generation,
                    evidence_sequence,
                },
                scope: self.bootstrap_scope,
            },
            PrivateFactsWorkerState::Ready => {
                if now_ms >= self.next_keepalive_at_ms {
                    PendingEffectKind::Keepalive { generation }
                } else {
                    self.last_frame_poll_at_ms = now_ms;
                    PendingEffectKind::ReceiveFrame {
                        generation,
                        sequence: self
                            .last_frame_sequence
                            .checked_add(1)
                            .ok_or(PrivateFactsScheduleError::Effect)?,
                    }
                }
            }
        };
        let effect_id = self.next_effect_id;
        self.next_effect_id = self
            .next_effect_id
            .checked_add(1)
            .ok_or(PrivateFactsScheduleError::Effect)?;
        self.pending = Some(PendingEffect { effect_id, kind });
        Ok(Some(match kind {
            PendingEffectKind::Connect => PrivateFactsEffect::Connect { effect_id },
            PendingEffectKind::Bootstrap { ticket, scope } => PrivateFactsEffect::Bootstrap {
                effect_id,
                ticket,
                scope,
            },
            PendingEffectKind::ReceiveFrame {
                generation,
                sequence,
            } => PrivateFactsEffect::ReceiveFrame {
                effect_id,
                generation,
                next_sequence: sequence,
            },
            PendingEffectKind::Keepalive { generation } => PrivateFactsEffect::Keepalive {
                effect_id,
                generation,
            },
        }))
    }

    pub fn complete_connect(&mut self, effect_id: u64) -> Result<(), PrivateFactsScheduleError> {
        self.begin_connect_completion(effect_id)?;
        self.finish_connect_completion()
    }

    /// Consumes only the exact connect effect. The host must finish its durable session
    /// transition before calling `finish_connect_completion`, so a failed reconnect retains the
    /// accumulated backoff history.
    pub fn begin_connect_completion(
        &mut self,
        effect_id: u64,
    ) -> Result<(), PrivateFactsScheduleError> {
        self.take_pending(effect_id, |kind| matches!(kind, PendingEffectKind::Connect))?;
        Ok(())
    }

    pub fn finish_connect_completion(&mut self) -> Result<(), PrivateFactsScheduleError> {
        if self.state != PrivateFactsWorkerState::Backoff || self.pending.is_some() {
            return Err(PrivateFactsScheduleError::State);
        }
        self.state = PrivateFactsWorkerState::NeedsBootstrap;
        self.bootstrap_scope = PrivateBootstrapScope::Account;
        self.consecutive_failures = 0;
        self.last_frame_sequence = 0;
        self.last_frame_poll_at_ms = 0;
        self.stream_readback_due_at_ms = None;
        self.periodic_readback_in_progress = false;
        Ok(())
    }

    pub fn complete_no_frame(&mut self, effect_id: u64) -> Result<(), PrivateFactsScheduleError> {
        self.take_pending(effect_id, |kind| {
            matches!(kind, PendingEffectKind::ReceiveFrame { .. })
        })?;
        Ok(())
    }

    pub fn complete_transport_failure(
        &mut self,
        effect_id: u64,
        _now_ms: u64,
    ) -> Result<(), PrivateFactsScheduleError> {
        self.take_pending(effect_id, |_| true)?;
        Ok(())
    }

    pub fn complete_keepalive(
        &mut self,
        effect_id: u64,
        generation: u64,
        now_ms: u64,
    ) -> Result<(), PrivateFactsScheduleError> {
        let pending = self.take_pending(effect_id, |kind| {
            matches!(kind, PendingEffectKind::Keepalive { .. })
        })?;
        let PendingEffectKind::Keepalive {
            generation: expected,
        } = pending.kind
        else {
            return Err(PrivateFactsScheduleError::Effect);
        };
        if expected != generation {
            return Err(PrivateFactsScheduleError::Generation);
        }
        self.next_keepalive_at_ms = now_ms.saturating_add(self.policy.keepalive_ms);
        Ok(())
    }

    pub fn complete_frame(
        &mut self,
        effect_id: u64,
        generation: u64,
        sequence: u64,
    ) -> Result<(), PrivateFactsScheduleError> {
        let pending = self.take_pending(effect_id, |kind| {
            matches!(kind, PendingEffectKind::ReceiveFrame { .. })
        })?;
        let PendingEffectKind::ReceiveFrame {
            generation: expected_generation,
            sequence: expected_sequence,
        } = pending.kind
        else {
            return Err(PrivateFactsScheduleError::Effect);
        };
        if generation != expected_generation || sequence != expected_sequence {
            return Err(PrivateFactsScheduleError::Generation);
        }
        self.last_frame_sequence = sequence;
        Ok(())
    }

    pub fn complete_bootstrap_scope(
        &mut self,
        effect_id: u64,
        ticket: PrivateReadbackTicket,
        scope: PrivateBootstrapScope,
    ) -> Result<(), PrivateFactsScheduleError> {
        let pending = self.take_pending(effect_id, |kind| {
            matches!(kind, PendingEffectKind::Bootstrap { .. })
        })?;
        let PendingEffectKind::Bootstrap {
            ticket: expected_ticket,
            scope: expected_scope,
        } = pending.kind
        else {
            return Err(PrivateFactsScheduleError::Effect);
        };
        if ticket != expected_ticket
            || scope != expected_scope
            || scope != self.bootstrap_scope
            || scope == PrivateBootstrapScope::Fills
        {
            return Err(PrivateFactsScheduleError::Generation);
        }
        self.bootstrap_scope = scope.next().ok_or(PrivateFactsScheduleError::Effect)?;
        Ok(())
    }

    pub fn complete_bootstrap(
        &mut self,
        effect_id: u64,
    ) -> Result<PrivateReadbackTicket, PrivateFactsScheduleError> {
        let pending = self.take_pending(effect_id, |kind| {
            matches!(kind, PendingEffectKind::Bootstrap { .. })
        })?;
        let PendingEffectKind::Bootstrap {
            ticket: expected_ticket,
            scope,
        } = pending.kind
        else {
            return Err(PrivateFactsScheduleError::Effect);
        };
        if scope != PrivateBootstrapScope::Fills
            || self.bootstrap_scope != PrivateBootstrapScope::Fills
        {
            return Err(PrivateFactsScheduleError::Effect);
        }
        Ok(expected_ticket)
    }

    pub fn begin_periodic_readback(&mut self) -> Result<(), PrivateFactsScheduleError> {
        if self.state != PrivateFactsWorkerState::Ready {
            return Err(PrivateFactsScheduleError::State);
        }
        self.begin_readback(true);
        Ok(())
    }

    pub fn begin_stream_readback(&mut self) -> Result<(), PrivateFactsScheduleError> {
        if self.state != PrivateFactsWorkerState::Ready {
            return Err(PrivateFactsScheduleError::State);
        }
        self.begin_readback(false);
        Ok(())
    }

    pub fn schedule_stream_readback(
        &mut self,
        now_ms: u64,
    ) -> Result<(), PrivateFactsScheduleError> {
        if self.state != PrivateFactsWorkerState::Ready {
            return Err(PrivateFactsScheduleError::State);
        }
        self.periodic_readback_in_progress = false;
        self.stream_readback_due_at_ms =
            Some(now_ms.saturating_add(self.policy.stream_burst_coalesce_ms));
        Ok(())
    }

    pub fn require_immediate_readback(&mut self) {
        self.state = PrivateFactsWorkerState::NeedsBootstrap;
        self.bootstrap_scope = PrivateBootstrapScope::Account;
        self.periodic_readback_in_progress = false;
        self.stream_readback_due_at_ms = None;
        self.next_refresh_at_ms = None;
    }

    pub fn mark_ready(&mut self, now_ms: u64, authority_interval_ms: Option<u64>) {
        self.state = PrivateFactsWorkerState::Ready;
        self.next_keepalive_at_ms = now_ms.saturating_add(self.policy.keepalive_ms);
        self.next_refresh_at_ms = authority_interval_ms
            .into_iter()
            .chain(self.periodic_readback_interval_ms)
            .min()
            .map(|interval| now_ms.saturating_add(interval));
        self.periodic_readback_in_progress = false;
        self.stream_readback_due_at_ms = None;
    }

    /// Fences all current scheduling authority and returns the next retry watermark.
    pub fn enter_backoff(&mut self, now_ms: u64) -> u64 {
        self.state = PrivateFactsWorkerState::Backoff;
        self.bootstrap_scope = PrivateBootstrapScope::Account;
        self.pending = None;
        self.last_frame_sequence = 0;
        self.last_frame_poll_at_ms = 0;
        self.stream_readback_due_at_ms = None;
        self.periodic_readback_in_progress = false;
        self.next_refresh_at_ms = None;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let delay = jittered_exponential_delay_ms(
            self.policy.base_backoff_ms,
            self.policy.max_backoff_ms,
            self.consecutive_failures,
            &self.backoff_scope,
            now_ms,
        );
        self.next_retry_at_ms = now_ms.saturating_add(delay);
        self.next_retry_at_ms
    }

    #[must_use]
    pub fn idle_wait(&self, now_ms: u64) -> Duration {
        let millis = match self.state {
            PrivateFactsWorkerState::Backoff => self
                .next_retry_at_ms
                .saturating_sub(now_ms)
                .clamp(1, self.policy.frame_poll_ms),
            PrivateFactsWorkerState::NeedsBootstrap | PrivateFactsWorkerState::Ready => 1,
        };
        Duration::from_millis(millis)
    }

    fn begin_readback(&mut self, periodic: bool) {
        self.state = PrivateFactsWorkerState::NeedsBootstrap;
        self.bootstrap_scope = PrivateBootstrapScope::Account;
        self.next_refresh_at_ms = None;
        self.periodic_readback_in_progress = periodic;
        self.stream_readback_due_at_ms = None;
    }

    fn take_pending(
        &mut self,
        effect_id: u64,
        accepts: impl FnOnce(PendingEffectKind) -> bool,
    ) -> Result<PendingEffect, PrivateFactsScheduleError> {
        let pending = self
            .pending
            .take()
            .ok_or(PrivateFactsScheduleError::Effect)?;
        if pending.effect_id != effect_id || !accepts(pending.kind) {
            self.pending = Some(pending);
            return Err(PrivateFactsScheduleError::Effect);
        }
        Ok(pending)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PrivateFactsScheduleError {
    #[error("private-facts schedule policy or scope is invalid")]
    Policy,
    #[error("private-facts scheduler issued or completed an invalid effect")]
    Effect,
    #[error("private-facts scheduler lifecycle is invalid")]
    State,
    #[error("private-facts effect belongs to another session generation")]
    Generation,
}

fn jittered_exponential_delay_ms(
    base_ms: u64,
    cap_ms: u64,
    failures: u8,
    scope: &str,
    entropy: u64,
) -> u64 {
    if base_ms == 0 || cap_ms < base_ms || failures == 0 {
        return 0;
    }
    let exponent = u32::from(failures.saturating_sub(1).min(15));
    let upper = base_ms.saturating_mul(1_u64 << exponent).min(cap_ms);
    let lower = (upper / 2).max(1);
    let span = upper.saturating_sub(lower).saturating_add(1);
    lower.saturating_add(jitter_hash(scope, failures, entropy) % span)
}

fn jitter_hash(scope: &str, failures: u8, entropy: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in scope
        .bytes()
        .chain(process::id().to_le_bytes())
        .chain([failures])
        .chain(entropy.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler() -> Result<PrivateFactsScheduler, PrivateFactsScheduleError> {
        PrivateFactsScheduler::new(
            PrivateFactsSchedulePolicy {
                base_backoff_ms: 250,
                max_backoff_ms: 30_000,
                keepalive_ms: 1_200_000,
                stream_burst_coalesce_ms: 100,
                frame_poll_ms: 1,
            },
            "account-a",
        )
    }

    #[test]
    fn effect_is_single_in_flight_and_ticket_binds_evidence_tail()
    -> Result<(), PrivateFactsScheduleError> {
        let mut scheduler = scheduler()?;
        let connect = scheduler
            .next_effect(1, 2, 7)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        assert!(matches!(connect, PrivateFactsEffect::Connect { .. }));
        assert_eq!(scheduler.next_effect(1, 2, 7)?, None);
        scheduler.complete_connect(connect.effect_id())?;
        let bootstrap = scheduler
            .next_effect(2, 2, 7)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        let PrivateFactsEffect::Bootstrap { ticket, scope, .. } = bootstrap else {
            return Err(PrivateFactsScheduleError::Effect);
        };
        assert_eq!(scope, PrivateBootstrapScope::Account);
        assert_eq!(ticket.generation(), 2);
        assert_eq!(ticket.evidence_sequence(), 7);
        Ok(())
    }

    #[test]
    fn stale_completion_does_not_consume_pending_effect() -> Result<(), PrivateFactsScheduleError> {
        let mut scheduler = scheduler()?;
        let connect = scheduler
            .next_effect(1, 1, 0)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        assert_eq!(
            scheduler.complete_connect(connect.effect_id().saturating_add(1)),
            Err(PrivateFactsScheduleError::Effect)
        );
        assert_eq!(scheduler.next_effect(1, 1, 0)?, None);
        scheduler.complete_connect(connect.effect_id())?;
        Ok(())
    }

    #[test]
    fn periodic_refresh_waits_for_a_poll_at_or_after_deadline()
    -> Result<(), PrivateFactsScheduleError> {
        let mut scheduler = scheduler()?;
        let connect = scheduler
            .next_effect(1, 1, 0)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        scheduler.complete_connect(connect.effect_id())?;
        scheduler.mark_ready(100, Some(100));
        assert!(!scheduler.periodic_readback_due(200));
        let poll = scheduler
            .next_effect(200, 1, 0)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        assert!(matches!(poll, PrivateFactsEffect::ReceiveFrame { .. }));
        scheduler.complete_no_frame(poll.effect_id())?;
        assert!(scheduler.periodic_readback_due(200));
        Ok(())
    }

    #[test]
    fn backoff_is_bounded_and_reconnect_resets_failures() -> Result<(), PrivateFactsScheduleError> {
        let mut scheduler = scheduler()?;
        for failure in 1..=12_u8 {
            let now = u64::from(failure) * 100_000;
            let upper = 250_u64
                .saturating_mul(1_u64 << u32::from(failure.saturating_sub(1).min(15)))
                .min(30_000);
            let retry = scheduler.enter_backoff(now);
            let delay = retry.saturating_sub(now);
            assert!((upper / 2).max(1) <= delay);
            assert!(delay <= upper);
        }
        let retry = scheduler.enter_backoff(2_000_000);
        let connect = scheduler
            .next_effect(retry, 9, 4)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        scheduler.complete_connect(connect.effect_id())?;
        assert_eq!(scheduler.state(), PrivateFactsWorkerState::NeedsBootstrap);
        Ok(())
    }

    #[test]
    fn stale_frame_generation_fails_closed_at_scheduler_boundary()
    -> Result<(), PrivateFactsScheduleError> {
        let mut scheduler = scheduler()?;
        let connect = scheduler
            .next_effect(1, 3, 0)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        scheduler.complete_connect(connect.effect_id())?;
        scheduler.mark_ready(1, None);
        let frame = scheduler
            .next_effect(2, 3, 0)?
            .ok_or(PrivateFactsScheduleError::Effect)?;
        let PrivateFactsEffect::ReceiveFrame {
            effect_id,
            next_sequence,
            ..
        } = frame
        else {
            return Err(PrivateFactsScheduleError::Effect);
        };
        assert_eq!(
            scheduler.complete_frame(effect_id, 4, next_sequence),
            Err(PrivateFactsScheduleError::Generation)
        );
        Ok(())
    }
}
