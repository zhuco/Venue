use std::{
    sync::{Arc, mpsc},
    thread,
};

use crate::exchange::grid::{GridRiskReadback, GridVenueError, HedgedGridRiskReadbackClient};

use super::Stage7GridError;

pub(super) struct Stage7RiskLane {
    client: Option<Arc<dyn HedgedGridRiskReadbackClient>>,
    pending: Option<PendingRiskReadback>,
}

struct PendingRiskReadback {
    private_generation: u64,
    receiver: mpsc::Receiver<Result<GridRiskReadback, GridVenueError>>,
}

pub(super) struct CompletedRiskReadback {
    pub(super) private_generation: u64,
    pub(super) result: Result<GridRiskReadback, GridVenueError>,
}

impl Stage7RiskLane {
    pub(super) fn new(client: Option<Arc<dyn HedgedGridRiskReadbackClient>>) -> Self {
        Self {
            client,
            pending: None,
        }
    }

    pub(super) fn pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Starts one request-only acquisition. The worker cannot mutate the exchange or any
    /// artifact; the resident remains the sole authority that may accept and persist its result.
    pub(super) fn start(
        &mut self,
        account: String,
        private_generation: u64,
    ) -> Result<(), Stage7GridError> {
        if private_generation == 0 || self.pending.is_some() {
            return Err(Stage7GridError::PrivateEvidence);
        }
        let client = self
            .client
            .as_ref()
            .cloned()
            .ok_or_else(|| Stage7GridError::Venue {
                reason: GridVenueError::RiskReadbackUnsupported.to_string(),
            })?;
        let (sender, receiver) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("stage7-risk-readback".to_owned())
            .spawn(move || {
                let _ = sender.send(client.risk_readback(&account, private_generation));
            })
            .map_err(|_| Stage7GridError::Dispatch)?;
        self.pending = Some(PendingRiskReadback {
            private_generation,
            receiver,
        });
        Ok(())
    }

    /// Never waits for the risk worker. A disconnected worker is a failed-closed lane result,
    /// not a reason to delay private fills.
    pub(super) fn poll(&mut self) -> Result<Option<CompletedRiskReadback>, Stage7GridError> {
        let Some(pending) = self.pending.as_ref() else {
            return Ok(None);
        };
        let result = match pending.receiver.try_recv() {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Empty) => return Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                self.pending = None;
                return Err(Stage7GridError::Dispatch);
            }
        };
        let private_generation = pending.private_generation;
        self.pending = None;
        Ok(Some(CompletedRiskReadback {
            private_generation,
            result,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Mutex, mpsc},
        time::{Duration, Instant},
    };

    use super::*;

    struct GatedRiskClient {
        gate: Mutex<mpsc::Receiver<()>>,
    }

    impl HedgedGridRiskReadbackClient for GatedRiskClient {
        fn risk_readback(
            &self,
            _account: &str,
            _private_generation: u64,
        ) -> Result<GridRiskReadback, GridVenueError> {
            self.gate
                .lock()
                .map_err(|_| GridVenueError::RiskReadbackUnsupported)?
                .recv()
                .map_err(|_| GridVenueError::RiskReadbackUnsupported)?;
            Err(GridVenueError::RiskReadbackUnsupported)
        }
    }

    #[test]
    fn pending_risk_request_never_blocks_resident_poll() -> Result<(), Stage7GridError> {
        let (release, gate) = mpsc::channel();
        let mut lane = Stage7RiskLane::new(Some(Arc::new(GatedRiskClient {
            gate: Mutex::new(gate),
        })));
        lane.start("account".to_owned(), 7)?;
        assert!(lane.pending());
        assert!(lane.poll()?.is_none());
        release.send(()).map_err(|_| Stage7GridError::Dispatch)?;
        let deadline = Instant::now() + Duration::from_secs(1);
        let completed = loop {
            if let Some(completed) = lane.poll()? {
                break completed;
            }
            if Instant::now() >= deadline {
                return Err(Stage7GridError::Dispatch);
            }
            thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(completed.private_generation, 7);
        assert!(matches!(
            completed.result,
            Err(GridVenueError::RiskReadbackUnsupported)
        ));
        assert!(!lane.pending());
        Ok(())
    }
}
