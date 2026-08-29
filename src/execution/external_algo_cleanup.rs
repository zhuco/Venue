use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{OrderSide, PositionSide, Price, Symbol},
    exchange::grid::HedgedGridMutationClient,
};

use super::recovery_writer::validate_external_algo_cancel_dispatch;
use super::{
    ExternalAlgoCancelAuthorization, RecoveryDispatchGuard, RecoveryWriterError,
    RecoveryWriterScope, sha256_hex,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalAlgoCustody {
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub algo_id: String,
    pub client_algo_id: String,
    pub order_type: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub trigger_price: Price,
    pub working_type: String,
    pub close_position: bool,
    pub reduce_only: bool,
}

impl ExternalAlgoCustody {
    pub(crate) fn validate(&self, scope: &RecoveryWriterScope) -> Result<(), RecoveryWriterError> {
        if self.exchange != "binance"
            || self.exchange != scope.exchange
            || self.account != scope.account
            || self.symbol != scope.symbol
            || self.algo_id.trim().is_empty()
            || self.client_algo_id.trim().is_empty()
            || self.order_type.trim().is_empty()
            || self.working_type.trim().is_empty()
            || self.position_side == PositionSide::Net
            || !self.quantity.is_sign_positive()
            || self.quantity.is_zero()
        {
            return Err(RecoveryWriterError::Identity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalAlgoCancelCommand {
    pub custody: ExternalAlgoCustody,
    pub signed_payload_sha256: String,
    pub observed_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "detail")]
pub enum ExternalAlgoCleanupState {
    Prepared,
    Submitted,
    ResponseObserved {
        response_sha256: String,
    },
    Unknown {
        error_sha256: String,
    },
    StillOpen {
        signed_payload_sha256: String,
        observed_at_ms: u64,
    },
    SettledAbsent {
        signed_payload_sha256: String,
        observed_at_ms: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalAlgoCleanupRecord {
    pub sequence: u64,
    pub attempt: u32,
    pub previous_record_sha256: Option<String>,
    pub command: ExternalAlgoCancelCommand,
    pub state: ExternalAlgoCleanupState,
    pub record_sha256: String,
}

#[derive(Serialize)]
struct RecordHashInput<'a> {
    sequence: u64,
    attempt: u32,
    previous_record_sha256: &'a Option<String>,
    command: &'a ExternalAlgoCancelCommand,
    state: &'a ExternalAlgoCleanupState,
}

#[derive(Debug)]
pub struct ExternalAlgoCleanupJournal {
    path: PathBuf,
    records: Vec<ExternalAlgoCleanupRecord>,
}

impl ExternalAlgoCleanupJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ExternalAlgoCleanupError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(ExternalAlgoCleanupError::Path);
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(source) => return Err(io_error(&path, source)),
        };
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            return Err(ExternalAlgoCleanupError::Truncated);
        }
        let mut records = Vec::<ExternalAlgoCleanupRecord>::new();
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let record: ExternalAlgoCleanupRecord =
                serde_json::from_slice(line).map_err(ExternalAlgoCleanupError::Decode)?;
            validate_recovered_record(records.last(), &record)?;
            records.push(record);
        }
        Ok(Self { path, records })
    }

    #[must_use]
    pub fn latest(&self) -> Option<&ExternalAlgoCleanupRecord> {
        self.records.last()
    }

    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.latest().is_some_and(|record| {
            matches!(record.state, ExternalAlgoCleanupState::SettledAbsent { .. })
        })
    }

    pub fn require_target(
        &self,
        expected_client_algo_id: &str,
        expected_algo_id: &str,
    ) -> Result<(), ExternalAlgoCleanupError> {
        let Some(record) = self.latest() else {
            return Ok(());
        };
        if record.command.custody.client_algo_id != expected_client_algo_id
            || record.command.custody.algo_id != expected_algo_id
        {
            return Err(ExternalAlgoCleanupError::Conflict);
        }
        Ok(())
    }

    pub fn mark_still_open(
        &mut self,
        signed_payload_sha256: String,
        observed_at_ms: u64,
    ) -> Result<(), ExternalAlgoCleanupError> {
        let previous = self
            .latest()
            .cloned()
            .ok_or(ExternalAlgoCleanupError::Transition)?;
        if matches!(
            previous.state,
            ExternalAlgoCleanupState::StillOpen { .. }
                | ExternalAlgoCleanupState::SettledAbsent { .. }
        ) {
            return Err(ExternalAlgoCleanupError::Transition);
        }
        self.append(
            previous.attempt,
            previous.command,
            ExternalAlgoCleanupState::StillOpen {
                signed_payload_sha256,
                observed_at_ms,
            },
        )
    }

    pub fn mark_settled_absent(
        &mut self,
        signed_payload_sha256: String,
        observed_at_ms: u64,
    ) -> Result<(), ExternalAlgoCleanupError> {
        let previous = self
            .latest()
            .cloned()
            .ok_or(ExternalAlgoCleanupError::Transition)?;
        if matches!(
            previous.state,
            ExternalAlgoCleanupState::SettledAbsent { .. }
        ) {
            return Ok(());
        }
        self.append(
            previous.attempt,
            previous.command,
            ExternalAlgoCleanupState::SettledAbsent {
                signed_payload_sha256,
                observed_at_ms,
            },
        )
    }

    fn prepare(
        &mut self,
        command: ExternalAlgoCancelCommand,
    ) -> Result<u32, ExternalAlgoCleanupError> {
        let attempt = match self.latest() {
            None => 1,
            Some(previous)
                if matches!(previous.state, ExternalAlgoCleanupState::StillOpen { .. })
                    && previous.command.custody == command.custody =>
            {
                previous
                    .attempt
                    .checked_add(1)
                    .ok_or(ExternalAlgoCleanupError::Sequence)?
            }
            Some(_) => return Err(ExternalAlgoCleanupError::Transition),
        };
        self.append(attempt, command, ExternalAlgoCleanupState::Prepared)?;
        Ok(attempt)
    }

    fn mark_submitted(&mut self) -> Result<(), ExternalAlgoCleanupError> {
        let previous = self
            .latest()
            .cloned()
            .ok_or(ExternalAlgoCleanupError::Transition)?;
        if !matches!(previous.state, ExternalAlgoCleanupState::Prepared) {
            return Err(ExternalAlgoCleanupError::Transition);
        }
        self.append(
            previous.attempt,
            previous.command,
            ExternalAlgoCleanupState::Submitted,
        )
    }

    fn mark_response(&mut self, response_sha256: String) -> Result<(), ExternalAlgoCleanupError> {
        let previous = self
            .latest()
            .cloned()
            .ok_or(ExternalAlgoCleanupError::Transition)?;
        if !matches!(previous.state, ExternalAlgoCleanupState::Submitted) {
            return Err(ExternalAlgoCleanupError::Transition);
        }
        self.append(
            previous.attempt,
            previous.command,
            ExternalAlgoCleanupState::ResponseObserved { response_sha256 },
        )
    }

    fn mark_unknown(&mut self, error_sha256: String) -> Result<(), ExternalAlgoCleanupError> {
        let previous = self
            .latest()
            .cloned()
            .ok_or(ExternalAlgoCleanupError::Transition)?;
        if !matches!(previous.state, ExternalAlgoCleanupState::Submitted) {
            return Err(ExternalAlgoCleanupError::Transition);
        }
        self.append(
            previous.attempt,
            previous.command,
            ExternalAlgoCleanupState::Unknown { error_sha256 },
        )
    }

    fn append(
        &mut self,
        attempt: u32,
        command: ExternalAlgoCancelCommand,
        state: ExternalAlgoCleanupState,
    ) -> Result<(), ExternalAlgoCleanupError> {
        let sequence = u64::try_from(self.records.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(ExternalAlgoCleanupError::Sequence)?;
        let previous_record_sha256 = self.latest().map(|record| record.record_sha256.clone());
        let record_sha256 =
            record_hash(sequence, attempt, &previous_record_sha256, &command, &state)?;
        let record = ExternalAlgoCleanupRecord {
            sequence,
            attempt,
            previous_record_sha256,
            command,
            state,
            record_sha256,
        };
        let encoded = serde_json::to_vec(&record).map_err(ExternalAlgoCleanupError::Encode)?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|source| io_error(&self.path, source))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| io_error(&self.path, source))?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_data())
            .map_err(|source| io_error(&self.path, source))?;
        self.records.push(record);
        Ok(())
    }
}

pub(crate) fn submit_external_algo_cancel(
    journal: &mut ExternalAlgoCleanupJournal,
    client: &dyn HedgedGridMutationClient,
    authorization: ExternalAlgoCancelAuthorization,
    now_ms: u64,
    _guard: &RecoveryDispatchGuard,
) -> Result<String, ExternalAlgoCleanupError> {
    validate_external_algo_cancel_dispatch(&authorization, now_ms)?;
    journal.prepare(authorization.command.clone())?;
    journal.mark_submitted()?;
    match client.cancel_algo_by_client_id(&authorization.command.custody.client_algo_id) {
        Ok(response) => {
            let response_sha256 = sha256_hex(response.as_bytes());
            journal.mark_response(response_sha256.clone())?;
            Ok(response_sha256)
        }
        Err(error) => {
            let error_sha256 = sha256_hex(error.to_string().as_bytes());
            journal.mark_unknown(error_sha256)?;
            Err(ExternalAlgoCleanupError::Venue {
                reason: error.to_string(),
            })
        }
    }
}

fn validate_recovered_record(
    previous: Option<&ExternalAlgoCleanupRecord>,
    record: &ExternalAlgoCleanupRecord,
) -> Result<(), ExternalAlgoCleanupError> {
    let expected_sequence = previous.map_or(1, |value| value.sequence.saturating_add(1));
    if record.sequence != expected_sequence
        || record.attempt == 0
        || record.previous_record_sha256 != previous.map(|value| value.record_sha256.clone())
        || record.record_sha256
            != record_hash(
                record.sequence,
                record.attempt,
                &record.previous_record_sha256,
                &record.command,
                &record.state,
            )?
    {
        return Err(ExternalAlgoCleanupError::HashChain);
    }
    if let Some(previous) = previous {
        if previous.command.custody != record.command.custody
            || !allowed_transition(previous, record)
        {
            return Err(ExternalAlgoCleanupError::Transition);
        }
    } else if record.attempt != 1 || !matches!(record.state, ExternalAlgoCleanupState::Prepared) {
        return Err(ExternalAlgoCleanupError::Transition);
    }
    Ok(())
}

fn allowed_transition(
    previous: &ExternalAlgoCleanupRecord,
    current: &ExternalAlgoCleanupRecord,
) -> bool {
    use ExternalAlgoCleanupState::{
        Prepared, ResponseObserved, SettledAbsent, StillOpen, Submitted, Unknown,
    };
    match (&previous.state, &current.state) {
        (Prepared, Submitted) => current.attempt == previous.attempt,
        (Submitted, ResponseObserved { .. } | Unknown { .. }) => {
            current.attempt == previous.attempt
        }
        (Prepared | Submitted | ResponseObserved { .. } | Unknown { .. }, StillOpen { .. }) => {
            current.attempt == previous.attempt
        }
        (
            Prepared | Submitted | ResponseObserved { .. } | Unknown { .. } | StillOpen { .. },
            SettledAbsent { .. },
        ) => current.attempt == previous.attempt,
        (StillOpen { .. }, Prepared) => current.attempt == previous.attempt.saturating_add(1),
        _ => false,
    }
}

fn record_hash(
    sequence: u64,
    attempt: u32,
    previous_record_sha256: &Option<String>,
    command: &ExternalAlgoCancelCommand,
    state: &ExternalAlgoCleanupState,
) -> Result<String, ExternalAlgoCleanupError> {
    let encoded = serde_json::to_vec(&RecordHashInput {
        sequence,
        attempt,
        previous_record_sha256,
        command,
        state,
    })
    .map_err(ExternalAlgoCleanupError::Encode)?;
    Ok(sha256_hex(encoded))
}

fn io_error(path: &Path, source: std::io::Error) -> ExternalAlgoCleanupError {
    ExternalAlgoCleanupError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExternalAlgoCleanupError {
    #[error("external Algo cleanup journal path must be absolute")]
    Path,
    #[error("external Algo cleanup journal has a truncated tail")]
    Truncated,
    #[error("external Algo cleanup journal sequence is exhausted")]
    Sequence,
    #[error("external Algo cleanup journal hash chain is invalid")]
    HashChain,
    #[error("external Algo cleanup journal transition is invalid")]
    Transition,
    #[error("external Algo cleanup target conflicts with durable custody")]
    Conflict,
    #[error("external Algo cleanup journal I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("external Algo cleanup journal encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("external Algo cleanup journal decoding failed: {0}")]
    Decode(serde_json::Error),
    #[error("external Algo cleanup writer authorization failed: {0}")]
    Writer(#[from] RecoveryWriterError),
    #[error("external Algo cleanup venue mutation is unresolved: {reason}")]
    Venue { reason: String },
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use crate::{
        domain::{CancelCommand, MarketOrderCommand, MarketReduceCommand, OrderCommand},
        exchange::grid::GridVenueError,
        execution::{
            ExternalAlgoCancelInput, RecoveryObservationProof, RecoveryWriterAuthority,
            authorize_external_algo_cancel,
        },
    };

    use super::*;

    struct PrewriteCheckingClient {
        journal_path: PathBuf,
        submitted_seen: Arc<AtomicBool>,
    }

    impl HedgedGridMutationClient for PrewriteCheckingClient {
        fn place_limit_post_only(&self, _command: &OrderCommand) -> Result<String, GridVenueError> {
            Err(GridVenueError::MutationUnsupported)
        }

        fn place_market(&self, _command: &MarketOrderCommand) -> Result<String, GridVenueError> {
            Err(GridVenueError::MutationUnsupported)
        }

        fn place_market_reduce(
            &self,
            _command: &MarketReduceCommand,
        ) -> Result<String, GridVenueError> {
            Err(GridVenueError::MutationUnsupported)
        }

        fn cancel_by_client_id(&self, _command: &CancelCommand) -> Result<String, GridVenueError> {
            Err(GridVenueError::MutationUnsupported)
        }

        fn cancel_algo_by_client_id(&self, client_algo_id: &str) -> Result<String, GridVenueError> {
            let submitted = ExternalAlgoCleanupJournal::open(&self.journal_path)
                .ok()
                .and_then(|journal| journal.latest().cloned())
                .is_some_and(|record| {
                    record.command.custody.client_algo_id == client_algo_id
                        && matches!(record.state, ExternalAlgoCleanupState::Submitted)
                });
            self.submitted_seen.store(submitted, Ordering::SeqCst);
            Ok("{\"clientAlgoId\":\"external_algo\",\"algoId\":\"42\"}".to_owned())
        }
    }

    #[test]
    fn dispatch_fsyncs_submitted_before_call_and_recovers_hash_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let journal_path = directory.path().join("external_algo_cleanup.jsonl");
        let writer_path = directory.path().join("writer.json");
        let scope = scope()?;
        let custody = custody()?;
        let proof = RecoveryObservationProof {
            generation: 7,
            observed_at_ms: 100,
            valid_until_ms: 30_100,
            payload_sha256: "a".repeat(64),
            signature_verified: true,
        };
        let authorization = authorize_external_algo_cancel(ExternalAlgoCancelInput {
            scope: &scope,
            custody: &custody,
            proof: &proof,
            now_ms: 100,
        })?;
        let authority = RecoveryWriterAuthority::open(writer_path, scope)?;
        let guard = authority.lock_external_algo_cleanup()?;
        let submitted_seen = Arc::new(AtomicBool::new(false));
        let client = PrewriteCheckingClient {
            journal_path: journal_path.clone(),
            submitted_seen: Arc::clone(&submitted_seen),
        };
        let mut journal = ExternalAlgoCleanupJournal::open(&journal_path)?;
        let _ = submit_external_algo_cancel(&mut journal, &client, authorization, 100, &guard)?;
        assert!(submitted_seen.load(Ordering::SeqCst));
        assert!(matches!(
            ExternalAlgoCleanupJournal::open(journal_path)?
                .latest()
                .map(|record| &record.state),
            Some(ExternalAlgoCleanupState::ResponseObserved { .. })
        ));
        Ok(())
    }

    #[test]
    fn durable_target_cannot_be_rebound() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let journal_path = directory.path().join("external_algo_cleanup.jsonl");
        let mut journal = ExternalAlgoCleanupJournal::open(journal_path)?;
        let command = ExternalAlgoCancelCommand {
            custody: custody()?,
            signed_payload_sha256: "b".repeat(64),
            observed_at_ms: 200,
        };
        let _ = journal.prepare(command)?;
        assert!(journal.require_target("different", "42").is_err());
        assert!(
            journal
                .require_target("external_algo", "different")
                .is_err()
        );
        Ok(())
    }

    fn scope() -> Result<RecoveryWriterScope, Box<dyn std::error::Error>> {
        Ok(RecoveryWriterScope {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDC".parse()?,
        })
    }

    fn custody() -> Result<ExternalAlgoCustody, Box<dyn std::error::Error>> {
        Ok(ExternalAlgoCustody {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDC".parse()?,
            algo_id: "42".to_owned(),
            client_algo_id: "external_algo".to_owned(),
            order_type: "TAKE_PROFIT_MARKET".to_owned(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(19, 1),
            trigger_price: Price::new(Decimal::new(105, 0))?,
            working_type: "CONTRACT_PRICE".to_owned(),
            close_position: false,
            reduce_only: false,
        })
    }
}
