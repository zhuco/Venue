use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use tracing::warn;

use crate::backoff::jittered_exponential_delay_ms;
use crate::exchange::grid::{GridPublicPayloadSource, HedgedGridVenue};

use super::{HedgedGridBinding, Stage7GridError, stage7_public_evidence_path};
use crate::runtime::stage7_public_journal::{
    Stage7PublicBinding, Stage7PublicJournal, Stage7PublicSource,
};

const RETRY_BASE_MS: u64 = 1_000;
const RETRY_CAP_MS: u64 = 30_000;
// Gate depth can outpace a multi-endpoint signed private readback. Keep the socket drain bounded
// for stop responsiveness, but large enough that one durability batch catches up between those
// readbacks instead of letting locally queued, exchange-stale frames grow without bound.
const MAX_FRAMES_PER_TURN: usize = 4_096;
// Public capture yields quickly to private fills. A continuous depth stream must not delay the
// next private-event turn; batching and the frame cap still bound durable catch-up work.
const MAX_PUBLIC_DRAIN_DURATION: Duration = Duration::from_millis(5);

/// Owns public raw capture and connection backoff for exactly one stage-7 exchange/symbol root.
/// Every frame is fsynced before it is handed back to the exchange-specific normalizer.
pub(super) struct Stage7PublicRuntime {
    journal: Option<Stage7PublicJournal>,
    minimum_generation: u64,
    connected: bool,
    failures: u8,
    retry_at_ms: u64,
}

impl Stage7PublicRuntime {
    pub(super) fn open(
        artifacts_root: &Path,
        binding: &HedgedGridBinding,
    ) -> Result<Self, Stage7GridError> {
        fs::create_dir_all(artifacts_root).map_err(|source| Stage7GridError::Io {
            path: artifacts_root.to_path_buf(),
            source,
        })?;
        let journal = Stage7PublicJournal::open(
            stage7_public_evidence_path(artifacts_root, binding)?,
            Stage7PublicBinding {
                exchange: binding.exchange.clone(),
                symbol: binding.symbol.clone(),
            },
        )?;
        let minimum_generation = journal
            .max_generation()
            .checked_add(1)
            .ok_or(Stage7GridError::Clock)?;
        Ok(Self {
            journal: Some(journal),
            minimum_generation,
            connected: false,
            failures: 0,
            retry_at_ms: 0,
        })
    }

    /// Shadow may consume public frames for an in-memory reducer preview, but it must never
    /// create, repair, or append the live public evidence journal it is inspecting.
    pub(super) fn open_read_only(
        artifacts_root: &Path,
        binding: &HedgedGridBinding,
    ) -> Result<Self, Stage7GridError> {
        if !artifacts_root.is_dir() {
            return Err(Stage7GridError::ArtifactsRoot);
        }
        let minimum_generation = Stage7PublicJournal::max_generation_at_path(
            stage7_public_evidence_path(artifacts_root, binding)?,
            Stage7PublicBinding {
                exchange: binding.exchange.clone(),
                symbol: binding.symbol.clone(),
            },
        )?
        .checked_add(1)
        .ok_or(Stage7GridError::Clock)?;
        Ok(Self {
            journal: None,
            minimum_generation,
            connected: false,
            failures: 0,
            retry_at_ms: 0,
        })
    }

    /// A false result is a fail-closed no-mutation state. A later connection starts a new book
    /// generation after bounded exponential backoff; no stale book survives the reset.
    pub(super) fn drive<V: HedgedGridVenue>(
        &mut self,
        venue: &mut V,
        now_ms: u64,
    ) -> Result<bool, Stage7GridError> {
        if !self.connected {
            if now_ms < self.retry_at_ms {
                return Ok(false);
            }
            if let Err(error) = venue.seed_public_generation(self.minimum_generation) {
                warn!(
                    event = "stage7_public_generation_backoff",
                    exchange = venue.exchange(),
                    reason = %error,
                    "public generation recovery failed; blocking new grid intent"
                );
                self.fail(venue, now_ms);
                return Ok(false);
            }
            if let Err(error) = venue.connect_public_stream() {
                warn!(
                    event = "stage7_public_connect_backoff",
                    exchange = venue.exchange(),
                    reason = %error,
                    "public market connection failed; blocking new grid intent"
                );
                self.fail(venue, now_ms);
                return Ok(false);
            }
            self.connected = true;
            self.failures = 0;
        }
        // `next_public_payload` may spend time draining a busy socket.  A frame accepted near
        // the end of that drain can otherwise look like it arrived in the future relative to
        // the loop timestamp captured by the resident.  Advance only to durable frame times;
        // a following empty turn still evaluates normal wall-clock staleness.
        let mut freshness_now_ms = now_ms;
        let mut payloads = Vec::new();
        let drain_started_at = Instant::now();
        for _ in 0..MAX_FRAMES_PER_TURN {
            if drain_started_at.elapsed() >= MAX_PUBLIC_DRAIN_DURATION {
                break;
            }
            let payload = match venue.next_public_payload() {
                Ok(Some(payload)) => payload,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        event = "stage7_public_stream_backoff",
                        exchange = venue.exchange(),
                        reason = %error,
                        "public market stream failed; blocking new grid intent"
                    );
                    self.fail(venue, now_ms);
                    return Ok(false);
                }
            };
            payloads.push(payload);
        }
        if !payloads.is_empty()
            && let Some(journal) = self.journal.as_mut()
        {
            let durable_batch = payloads
                .iter()
                .map(|payload| {
                    (
                        payload.generation,
                        public_source(payload.source),
                        payload.received_at_ms,
                        payload.payload.clone(),
                    )
                })
                .collect();
            if let Err(error) = journal.append_batch(durable_batch) {
                warn!(
                    event = "stage7_public_journal_backoff",
                    exchange = venue.exchange(),
                    reason = %error,
                    "raw public frame was not durably committed; resetting public generation while private supervision continues"
                );
                self.fail(venue, now_ms);
                return Ok(false);
            }
        }
        for payload in payloads {
            freshness_now_ms = freshness_now_ms.max(payload.received_at_ms);
            let source = payload.source;
            let generation = payload.generation;
            if let Err(error) = venue.accept_public_payload(payload) {
                warn!(
                    event = "stage7_public_payload_backoff",
                    exchange = venue.exchange(),
                    source = ?source,
                    generation,
                    reason = %error,
                    "public market payload was rejected; blocking new grid intent"
                );
                self.fail(venue, now_ms);
                return Ok(false);
            }
        }
        Ok(venue.best_bid_ask(freshness_now_ms).is_ok())
    }

    fn fail<V: HedgedGridVenue>(&mut self, venue: &mut V, now_ms: u64) {
        venue.reset_public_stream();
        self.connected = false;
        self.failures = self.failures.saturating_add(1);
        let delay = jittered_exponential_delay_ms(
            RETRY_BASE_MS,
            RETRY_CAP_MS,
            self.failures,
            venue.exchange(),
            now_ms,
        );
        self.retry_at_ms = now_ms.saturating_add(delay);
    }
}

fn public_source(source: GridPublicPayloadSource) -> Stage7PublicSource {
    match source {
        GridPublicPayloadSource::RestSnapshot => Stage7PublicSource::RestSnapshot,
        GridPublicPayloadSource::RestTicker => Stage7PublicSource::RestTicker,
        GridPublicPayloadSource::WebSocketDepth => Stage7PublicSource::WebSocketDepth,
        GridPublicPayloadSource::WebSocketBbo => Stage7PublicSource::WebSocketBbo,
        GridPublicPayloadSource::WebSocketTrade => Stage7PublicSource::WebSocketTrade,
        GridPublicPayloadSource::WebSocketMark => Stage7PublicSource::WebSocketMark,
    }
}
