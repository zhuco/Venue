//! A new signed baseline must confirm an apparent surface gap before reset.

#[derive(Clone, Copy, Debug)]
pub(super) struct SurfaceRecovery {
    generation: u64,
    requested_ms: u64,
    last_attempt_ms: u64,
}

impl SurfaceRecovery {
    pub(super) fn new(generation: u64, now: u64) -> Self {
        Self {
            generation,
            requested_ms: now,
            last_attempt_ms: 0,
        }
    }

    pub(super) fn confirmed(&self, generation: u64, observed_ms: u64, unresolved: bool) -> bool {
        !unresolved
            && generation != 0
            && generation != self.generation
            && observed_ms > self.requested_ms
    }

    pub(super) fn request_due(&mut self, now: u64) -> bool {
        if self.last_attempt_ms == 0 || now.saturating_sub(self.last_attempt_ms) >= 30_000 {
            self.last_attempt_ms = now;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_or_unknown_command_cannot_authorize_reset() {
        let gate = SurfaceRecovery::new(7, 100);
        assert!(!gate.confirmed(7, 200, false));
        assert!(!gate.confirmed(8, 99, false));
        assert!(!gate.confirmed(8, 200, true));
        assert!(!gate.confirmed(0, 200, false));
        assert!(gate.confirmed(8, 200, false));
        // A restarted private worker can install a lower, but newly signed generation.
        assert!(gate.confirmed(2, 200, false));
        let restarted = SurfaceRecovery::new(8, 201);
        assert!(!restarted.confirmed(8, 202, false));
    }

    #[test]
    fn repeated_cold_turns_do_not_flood_signed_recovery() {
        let mut gate = SurfaceRecovery::new(7, 100);
        assert!(gate.request_due(100));
        assert!(!gate.request_due(2_100));
        assert!(!gate.request_due(30_099));
        assert!(gate.request_due(30_100));
    }
}
