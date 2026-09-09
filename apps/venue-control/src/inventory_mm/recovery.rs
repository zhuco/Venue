use std::time::{Duration, Instant};

pub(super) struct ReadRecovery {
    first: Instant,
    retry_at: Instant,
    notice_at: Instant,
    failures: u8,
}

impl ReadRecovery {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            first: now,
            retry_at: now,
            notice_at: now,
            failures: 0,
        }
    }

    pub(super) fn ready(&self, now: Instant) -> bool {
        now >= self.retry_at
    }

    pub(super) fn failed(&mut self, now: Instant) -> bool {
        let delay = 2_u64
            .saturating_pow(u32::from(self.failures.min(4)))
            .min(10);
        self.failures = self.failures.saturating_add(1);
        self.retry_at = now + Duration::from_secs(delay);
        let notify = now >= self.notice_at;
        if notify {
            self.notice_at = now + Duration::from_secs(120);
        }
        notify
    }

    pub(super) fn elapsed(&self, now: Instant) -> Duration {
        now.duration_since(self.first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extended_outage_keeps_retrying_with_bounded_delay_and_rate_limited_notices() {
        let start = Instant::now();
        let mut recovery = ReadRecovery::new(start);
        assert!(recovery.failed(start));
        assert!(!recovery.ready(start + Duration::from_millis(999)));
        assert!(recovery.ready(start + Duration::from_secs(1)));
        assert!(!recovery.failed(start + Duration::from_secs(1)));
        for seconds in [3, 7, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115] {
            assert!(recovery.ready(start + Duration::from_secs(seconds)));
            assert!(!recovery.failed(start + Duration::from_secs(seconds)));
        }
        let later = start + Duration::from_secs(1800);
        assert!(recovery.ready(later));
        assert!(recovery.failed(later));
        assert!(recovery.ready(later + Duration::from_secs(10)));
        assert_eq!(recovery.elapsed(later), Duration::from_secs(1800));
    }
}
