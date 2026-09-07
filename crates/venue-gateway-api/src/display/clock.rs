//! Public-display time only. Never used for signing or private account freshness.
use parking_lot::RwLock;
use std::time::{Duration, Instant};

use super::Result;

const REFRESH_AFTER: Duration = Duration::from_secs(60);
const EXPIRES_AFTER: Duration = Duration::from_secs(300);
static CLOCK: RwLock<Option<Sample>> = RwLock::new(None);

struct Sample {
    upper_ms: u64,
    received: Instant,
}

impl Sample {
    fn at(&self, now: Instant) -> Result<u64> {
        let elapsed = now.saturating_duration_since(self.received);
        if elapsed >= EXPIRES_AFTER {
            return Err("public display clock expired; reconnecting time source".into());
        }
        self.upper_ms
            .checked_add(elapsed.as_millis() as u64)
            .ok_or_else(|| "public display clock overflow".into())
    }
}

pub fn received_ms() -> Result<u64> {
    CLOCK
        .read()
        .as_ref()
        .ok_or("public display clock not synchronized")?
        .at(Instant::now())
}

pub fn needs_refresh() -> bool {
    CLOCK
        .read()
        .as_ref()
        .is_none_or(|sample| sample.received.elapsed() >= REFRESH_AFTER)
}

fn upper_bound(server_ms: u64, round_trip: Duration) -> Result<u64> {
    if !(1_577_836_800_000..4_102_444_800_000).contains(&server_ms)
        || round_trip > Duration::from_secs(3)
    {
        return Err("public display clock sample outside bounds".into());
    }
    // The server sampled during this round trip. Using its time plus the full
    // RTT conservatively overestimates age, rather than making old data fresh.
    server_ms
        .checked_add(round_trip.as_millis() as u64 + 1)
        .ok_or_else(|| "public display clock overflow".into())
}

pub fn synchronize(server_ms: u64, started: Instant) -> Result<()> {
    let received = Instant::now();
    let mut upper_ms = upper_bound(server_ms, received.duration_since(started))?;
    let mut clock = CLOCK.write();
    if let Some(previous) = clock.as_ref().and_then(|sample| sample.at(received).ok()) {
        if previous.abs_diff(upper_ms) > 5_000 {
            return Err("public display clock source changed unexpectedly".into());
        }
        upper_ms = upper_ms.max(previous);
    }
    *clock = Some(Sample { upper_ms, received });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_time_uses_monotonic_elapsed_and_expires_without_wall_clock() -> Result<()> {
        let start = Instant::now();
        let sample = Sample {
            upper_ms: 1_788_749_000_000,
            received: start,
        };
        assert_eq!(
            sample.at(start + Duration::from_millis(731))?,
            1_788_749_000_731
        );
        assert!(sample.at(start + EXPIRES_AFTER).is_err());
        Ok(())
    }

    #[test]
    fn uncertainty_overestimates_age_and_rejects_slow_or_invalid_samples() -> Result<()> {
        assert_eq!(
            upper_bound(1_788_749_000_000, Duration::from_millis(700))?,
            1_788_749_000_701
        );
        assert!(upper_bound(1_788_749_000_000, Duration::from_millis(3001)).is_err());
        assert!(upper_bound(0, Duration::ZERO).is_err());
        assert!(upper_bound(u64::MAX, Duration::ZERO).is_err());
        Ok(())
    }
}
