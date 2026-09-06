use venue_strategies::hedged_grid::GridResetPolicy;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ConvergenceProgress {
    pub pending_since_ms: Option<u64>,
    pub consecutive_failures: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProgressEvent {
    Pending,
    Failures(u32),
    Converged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProgressTransition {
    pub progress: ConvergenceProgress,
    pub pause: bool,
    pub timed_out: bool,
}

pub(crate) fn advance(
    current: ConvergenceProgress,
    event: ProgressEvent,
    policy: &GridResetPolicy,
    now_ms: u64,
) -> Option<ProgressTransition> {
    if now_ms == 0 || policy.convergence_timeout_ms == 0 || policy.failure_threshold == 0 {
        return None;
    }
    if event == ProgressEvent::Converged {
        return Some(ProgressTransition {
            progress: ConvergenceProgress::default(),
            pause: false,
            timed_out: false,
        });
    }
    let pending_since_ms = current.pending_since_ms.or(Some(now_ms));
    let consecutive_failures = match event {
        ProgressEvent::Failures(count) if count > 0 => {
            current.consecutive_failures.saturating_add(count)
        }
        _ => current.consecutive_failures,
    };
    let timed_out = pending_since_ms
        .is_some_and(|started| now_ms.saturating_sub(started) >= policy.convergence_timeout_ms);
    Some(ProgressTransition {
        progress: ConvergenceProgress {
            pending_since_ms,
            consecutive_failures,
        },
        pause: timed_out || consecutive_failures >= policy.failure_threshold,
        timed_out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> GridResetPolicy {
        GridResetPolicy {
            max_market_age_ms: 5_000,
            max_private_age_ms: 5_000,
            convergence_timeout_ms: 30_000,
            failure_threshold: 3,
        }
    }

    #[test]
    fn pending_timer_survives_turns_and_pauses_at_deadline() {
        let first = advance(
            ConvergenceProgress::default(),
            ProgressEvent::Pending,
            &policy(),
            1_000,
        )
        .expect("valid policy");
        assert_eq!(first.progress.pending_since_ms, Some(1_000));
        assert!(!first.pause);
        let before = advance(first.progress, ProgressEvent::Pending, &policy(), 30_999)
            .expect("valid policy");
        assert!(!before.pause);
        let deadline = advance(before.progress, ProgressEvent::Pending, &policy(), 31_000)
            .expect("valid policy");
        assert!(deadline.pause);
        assert!(deadline.timed_out);
    }

    #[test]
    fn failures_accumulate_until_stable_convergence_or_explicit_clear() {
        let mut progress = ConvergenceProgress::default();
        for expected in 1..=3 {
            let next = advance(
                progress,
                ProgressEvent::Failures(1),
                &policy(),
                1_000 + expected,
            )
            .expect("valid policy");
            assert_eq!(next.progress.consecutive_failures, expected as u32);
            assert_eq!(next.pause, expected == 3);
            progress = next.progress;
        }
        let cleared =
            advance(progress, ProgressEvent::Converged, &policy(), 2_000).expect("valid policy");
        assert_eq!(cleared.progress, ConvergenceProgress::default());
        assert!(!cleared.pause);
    }

    #[test]
    fn invalid_policy_never_synthesizes_progress() {
        let mut invalid = policy();
        invalid.failure_threshold = 0;
        assert!(
            advance(
                ConvergenceProgress::default(),
                ProgressEvent::Failures(1),
                &invalid,
                1
            )
            .is_none()
        );
    }
}
