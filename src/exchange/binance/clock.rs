use std::{
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use super::binance::PrivateError;

const TIME_SYNC_ATTEMPTS: usize = 6;

/// Process-local authoritative Binance clock. A signed request may use it only after a
/// midpoint-corrected server sample has been installed.
pub(crate) struct ServerClock {
    offset_ms: AtomicI64,
    synchronized: AtomicBool,
}

impl ServerClock {
    pub(crate) const fn new() -> Self {
        Self {
            offset_ms: AtomicI64::new(0),
            synchronized: AtomicBool::new(false),
        }
    }

    pub(crate) fn now_ms(&self) -> Result<u64, PrivateError> {
        if !self.synchronized.load(Ordering::Acquire) {
            return Err(PrivateError::Clock);
        }
        let local = local_now_ms()?;
        let offset = self.offset_ms.load(Ordering::Relaxed);
        if !self.synchronized.load(Ordering::Acquire) {
            return Err(PrivateError::Clock);
        }
        authoritative_timestamp(local, offset)
    }

    fn invalidate(&self) {
        self.synchronized.store(false, Ordering::Release);
    }

    fn install(&self, offset_ms: i64) {
        self.offset_ms.store(offset_ms, Ordering::Relaxed);
        self.synchronized.store(true, Ordering::Release);
    }
}

/// Synchronizes once at client construction and only again after Binance explicitly rejects the
/// timestamp. Signed mutations therefore do not pay an extra `/time` round trip.
pub(crate) fn synchronize(
    client: &reqwest::blocking::Client,
    base_url: &str,
    clock: &ServerClock,
) -> Result<(), PrivateError> {
    clock.invalidate();
    let mut best = None;
    let mut transport_failures = 0_usize;
    for _ in 0..TIME_SYNC_ATTEMPTS {
        let before = local_now_ms()?;
        let response = match client.get(format!("{base_url}/papi/v1/time")).send() {
            Ok(response) => response,
            Err(_) => {
                transport_failures = transport_failures.saturating_add(1);
                continue;
            }
        };
        if !response.status().is_success() {
            continue;
        }
        let Some(server_time) = response
            .text()
            .ok()
            .and_then(|payload| serde_json::from_str::<Value>(&payload).ok())
            .and_then(|value| value.get("serverTime").and_then(Value::as_u64))
        else {
            continue;
        };
        let after = local_now_ms()?;
        let round_trip_ms = after.saturating_sub(before);
        let midpoint = before.saturating_add(round_trip_ms / 2);
        let offset = time_offset_ms(server_time, midpoint);
        if best.is_none_or(|(best_rtt, _)| round_trip_ms < best_rtt) {
            best = Some((round_trip_ms, offset));
        }
    }
    let (_, offset) = best.ok_or(if transport_failures == TIME_SYNC_ATTEMPTS {
        PrivateError::Http
    } else {
        PrivateError::Clock
    })?;
    clock.install(offset);
    Ok(())
}

fn local_now_ms() -> Result<u64, PrivateError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PrivateError::Clock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| PrivateError::Clock)
}

fn authoritative_timestamp(local_ms: u64, offset_ms: i64) -> Result<u64, PrivateError> {
    if offset_ms >= 0 {
        local_ms
            .checked_add(offset_ms as u64)
            .ok_or(PrivateError::Clock)
    } else {
        local_ms
            .checked_sub(offset_ms.unsigned_abs())
            .ok_or(PrivateError::Clock)
    }
}

fn time_offset_ms(server_time_ms: u64, local_midpoint_ms: u64) -> i64 {
    let difference = i128::from(server_time_ms) - i128::from(local_midpoint_ms);
    difference.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn midpoint_offset_and_checked_adjustment_match_the_legacy_clock_contract() {
        assert_eq!(time_offset_ms(10_090, 10_050), 40);
        assert_eq!(authoritative_timestamp(20_000, 40).ok(), Some(20_040));
        assert_eq!(authoritative_timestamp(20_000, -40).ok(), Some(19_960));
        assert!(authoritative_timestamp(u64::MAX, 1).is_err());
        assert!(authoritative_timestamp(0, -1).is_err());
    }

    #[test]
    fn zero_offset_is_not_authoritative_before_successful_sync() {
        assert!(ServerClock::new().now_ms().is_err());
    }
}
