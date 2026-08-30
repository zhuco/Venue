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
    }
}
