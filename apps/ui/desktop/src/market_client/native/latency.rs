use super::*;

#[derive(Default)]
pub(super) struct Probe {
    sequence: u64,
    pending: std::collections::VecDeque<(u64, Instant)>,
    sample: Option<(u64, Instant)>,
    latest_started: Option<Instant>,
}

impl Probe {
    pub(super) fn start(&mut self) -> Vec<u8> {
        self.sequence = self.sequence.wrapping_add(1);
        self.pending.retain(|(_, at)| at.elapsed() <= PONG_DEADLINE);
        if self.pending.len() >= 96 {
            self.pending.pop_front();
        }
        self.pending.push_back((self.sequence, Instant::now()));
        self.sequence.to_be_bytes().to_vec()
    }

    pub(super) fn finish(&mut self, payload: &[u8]) -> Option<u64> {
        self.finish_at(payload, Instant::now())
    }

    fn finish_at(&mut self, payload: &[u8], now: Instant) -> Option<u64> {
        let sequence = u64::from_be_bytes(payload.try_into().ok()?);
        let index = self.pending.iter().position(|(id, _)| *id == sequence)?;
        let (_, started) = self.pending.remove(index)?;
        if self.latest_started.is_some_and(|latest| started < latest) {
            return None;
        }
        self.latest_started = Some(started);
        let millis = now.saturating_duration_since(started).as_millis() as u64;
        self.sample = Some((millis, now));
        Some(millis)
    }

    fn recent(&self) -> Option<u64> {
        self.sample
            .filter(|(_, received)| received.elapsed() <= PONG_DEADLINE)
            .map(|(millis, _)| millis)
    }
}

pub(super) fn publish(
    public: &Probe,
    market: &Probe,
    generation: u64,
    selections: &[MarketSelection],
    emitter: &mut EventEmitter,
) -> Result<(), String> {
    // Both sockets serve the visible market. Report the slower measured path.
    let measured_at = match (public.sample, market.sample) {
        (Some((_, a)), Some((_, b))) => a.min(b),
        _ => return Ok(()),
    };
    let (Some(public), Some(market)) = (public.recent(), market.recent()) else {
        return Ok(());
    };
    for selection in selections {
        emitter.emit(LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
            generation,
            selection: selection.clone(),
            event_time_ms: 0,
            received_ms: 0,
            payload: MarketPayload::ConnectionRtt {
                millis: public.max(market),
                measured_at,
            },
        })))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_matching_pong_measures_monotonic_rtt_without_capping() {
        let mut probe = Probe::default();
        let old = probe.start();
        let current = probe.start();
        let Some((_, started)) = probe.pending.back().copied() else {
            return assert!(!probe.pending.is_empty());
        };
        assert_eq!(probe.finish_at(&[], started), None);
        assert_eq!(
            probe.finish_at(&current, started + Duration::from_millis(1501)),
            Some(1501)
        );
        assert_eq!(
            probe.finish_at(&old, started + Duration::from_millis(1600)),
            None
        );
        assert_eq!(probe.finish_at(&current, started), None);
    }

    #[test]
    fn frequent_probes_keep_slow_replies_and_bound_outstanding_messages() {
        let mut probe = Probe::default();
        let first = probe.start();
        let started = probe.pending[0].1;
        for _ in 0..4 {
            probe.start();
        }
        assert_eq!(
            probe.finish_at(&first, started + Duration::from_millis(1500)),
            Some(1500)
        );
        for _ in 0..200 {
            probe.start();
        }
        assert!(probe.pending.len() <= 96);
    }
}
