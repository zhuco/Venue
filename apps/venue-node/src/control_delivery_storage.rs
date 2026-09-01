use std::path::PathBuf;

use venue_storage::{OpaqueJournal, OpaqueJournalError, StorageError};

use crate::control_delivery::{
    ControlDeliveryJournal, ControlDeliveryJournalError, ControlDeliveryJournalRecord,
};

/// The sole file-backed adapter for the Node control-delivery inbox.
///
/// All locking, hash chaining, incomplete-tail repair, sequence fencing, and sync behavior remains
/// owned by `venue_storage::OpaqueJournal`.
#[derive(Debug)]
pub struct OpaqueControlDeliveryJournal {
    inner: OpaqueJournal,
}

impl OpaqueControlDeliveryJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ControlDeliveryJournalError> {
        Ok(Self {
            inner: OpaqueJournal::open(path).map_err(map_error)?,
        })
    }
}

impl ControlDeliveryJournal for OpaqueControlDeliveryJournal {
    fn recover(
        &mut self,
    ) -> Result<Vec<ControlDeliveryJournalRecord>, ControlDeliveryJournalError> {
        self.inner
            .recover()
            .map(|records| {
                records
                    .into_iter()
                    .map(|record| ControlDeliveryJournalRecord {
                        sequence: record.sequence,
                        payload: record.payload,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    fn append(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
    ) -> Result<u64, ControlDeliveryJournalError> {
        self.inner
            .append(expected_sequence, payload)
            .map_err(map_error)
    }

    fn append_bounded(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
        maximum_file_bytes: u64,
    ) -> Result<u64, ControlDeliveryJournalError> {
        self.inner
            .append_bounded(expected_sequence, payload, maximum_file_bytes)
            .map_err(map_error)
    }

    fn storage_len(&self) -> Result<u64, ControlDeliveryJournalError> {
        self.inner.len().map_err(map_error)
    }

    fn compact(
        &mut self,
        expected_next_sequence: u64,
        payloads: &[Vec<u8>],
    ) -> Result<(), ControlDeliveryJournalError> {
        self.inner
            .compact(expected_next_sequence, payloads)
            .map_err(map_error)
    }

    fn compact_bounded(
        &mut self,
        expected_next_sequence: u64,
        payloads: &[Vec<u8>],
        maximum_file_bytes: u64,
    ) -> Result<(), ControlDeliveryJournalError> {
        self.inner
            .compact_bounded(expected_next_sequence, payloads, maximum_file_bytes)
            .map_err(map_error)
    }
}

fn map_error(error: OpaqueJournalError) -> ControlDeliveryJournalError {
    match error {
        OpaqueJournalError::SequenceConflict { .. } => {
            ControlDeliveryJournalError::SequenceConflict
        }
        OpaqueJournalError::Corrupt
        | OpaqueJournalError::Decode(_)
        | OpaqueJournalError::Storage(
            StorageError::Decode(_)
            | StorageError::Sequence
            | StorageError::TailRepair
            | StorageError::InvalidRecord(_),
        ) => ControlDeliveryJournalError::Corrupt,
        OpaqueJournalError::Encode(_)
        | OpaqueJournalError::SequenceExhausted
        | OpaqueJournalError::RecordTooLarge
        | OpaqueJournalError::Storage(
            StorageError::Io { .. } | StorageError::Encode(_) | StorageError::SequenceExhausted,
        ) => ControlDeliveryJournalError::Unavailable,
        OpaqueJournalError::FileLimitExceeded => ControlDeliveryJournalError::StorageLimit,
    }
}
