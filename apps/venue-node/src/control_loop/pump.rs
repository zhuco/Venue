use super::*;
use std::time::Instant;

const MARKET_IDLE_CADENCE: Duration = Duration::from_millis(5);

fn backoff(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(6);
    Duration::from_millis(100_u64.saturating_mul(1_u64 << exponent)).min(MAX_BACKOFF)
}

impl ControlResidentLoopError {
    fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Http(
                crate::ControlHttpClientError::Transport
                    | crate::ControlHttpClientError::Timeout
                    | crate::ControlHttpClientError::HttpStatus(_)
            ) | Self::Delivery(ControlDeliveryDriverError::Http(
                crate::ControlHttpClientError::Transport
                    | crate::ControlHttpClientError::Timeout
                    | crate::ControlHttpClientError::HttpStatus(_)
            )) | Self::Outbox(NodeProjectionOutboxError::Http(
                crate::ControlHttpClientError::Transport
                    | crate::ControlHttpClientError::Timeout
                    | crate::ControlHttpClientError::HttpStatus(_)
            ))
        )
    }
}

impl<G: AccountPhysicalGateway> ControlResidentLoop<G> {
    pub(super) fn run_with_private_pump<F>(mut self, mut pump: F) -> Result<(), NodeError>
    where
        F: FnMut(&mut ProductionResident<G>) -> Result<bool, NodeError>,
    {
        let http_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| NodeError::ResidentRuntime)?;
        let streamed = self.bindings.values().any(|binding| {
            matches!(
                binding.key.strategy_kind,
                StrategyKind::Scalping | StrategyKind::HedgedGrid
            )
        });
        let idle_cadence = if streamed {
            MARKET_IDLE_CADENCE
        } else {
            MIN_SIGNED_PRIVATE_REFRESH_INTERVAL
        };
        let mut next_control_at = Instant::now();
        let mut failures = 0_u32;
        loop {
            // The same thread remains the only account writer. Control backoff must not suspend
            // private/public acceptance; synchronous HTTP/signed-read latency is still additive,
            // so this cadence is not a wall-clock latency guarantee for a stalled transport.
            let progress = pump(&mut self.resident)?;
            if Instant::now() >= next_control_at {
                let now = now_ms().map_err(|_| NodeError::ResidentRuntime)?;
                let delay = match self.tick(&http_runtime, now) {
                    Ok(false) => return Ok(()),
                    Ok(true) => {
                        failures = 0;
                        self.poll_interval
                    }
                    Err(error) if error.retryable() => {
                        failures = failures.saturating_add(1);
                        backoff(failures)
                    }
                    Err(error) => {
                        return Err(NodeError::LiveHost {
                            venue: self.resident.runtime().account().exchange,
                            message: error.to_string(),
                        });
                    }
                };
                next_control_at = Instant::now()
                    .checked_add(delay)
                    .ok_or(NodeError::ResidentRuntime)?;
            }
            let wait = pump_wait(Instant::now(), next_control_at, idle_cadence, progress);
            if !wait_or_interrupt(&http_runtime, wait).map_err(|_| NodeError::ResidentRuntime)? {
                return Ok(());
            }
        }
    }
}

fn pump_wait(
    now: Instant,
    next_control_at: Instant,
    idle_cadence: Duration,
    progress: bool,
) -> Duration {
    if progress {
        Duration::ZERO
    } else {
        idle_cadence.min(next_control_at.saturating_duration_since(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_backoff_never_becomes_a_market_pump_sleep() {
        let now = Instant::now();
        let retry = now + backoff(1);
        for elapsed_ms in (0..100).step_by(5) {
            let current = now + Duration::from_millis(elapsed_ms);
            assert!(current < retry);
            assert_eq!(
                pump_wait(current, retry, MARKET_IDLE_CADENCE, false),
                MARKET_IDLE_CADENCE
            );
        }
        assert_eq!(
            pump_wait(now, retry, MARKET_IDLE_CADENCE, true),
            Duration::ZERO
        );
        assert_eq!(
            pump_wait(retry, retry, MARKET_IDLE_CADENCE, false),
            Duration::ZERO
        );
    }

    #[test]
    fn control_deadline_and_signed_refresh_remain_bounded_during_idle() {
        let now = Instant::now();
        let control = now + Duration::from_millis(2);
        assert_eq!(
            pump_wait(now, control, MARKET_IDLE_CADENCE, false),
            Duration::from_millis(2)
        );
        assert_eq!(
            pump_wait(
                now,
                now + MAX_BACKOFF,
                MIN_SIGNED_PRIVATE_REFRESH_INTERVAL,
                false
            ),
            MIN_SIGNED_PRIVATE_REFRESH_INTERVAL
        );
    }

    #[test]
    fn direct_control_http_failure_remains_retryable_at_capped_backoff() {
        let error = ControlResidentLoopError::Http(crate::ControlHttpClientError::HttpStatus(500));
        assert!(error.retryable());
        assert_eq!(backoff(u32::MAX), MAX_BACKOFF);
    }
}
