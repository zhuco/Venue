use std::path::Path;

use crate::{
    storage::ProjectionStore,
    strategy::scalping::{ScalpingCheckpoint, ScalpingError},
};

/// Root owns durable projection I/O; the checkpoint DTO and reducer state remain pure strategy.
#[derive(Debug)]
pub struct ScalpingCheckpointStore {
    store: ProjectionStore,
}

impl ScalpingCheckpointStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            store: ProjectionStore::new(path.as_ref().to_path_buf()),
        }
    }

    pub fn load(&self) -> Result<Option<ScalpingCheckpoint>, ScalpingError> {
        self.store
            .load()
            .map_err(|error| ScalpingError::Persistence {
                detail: error.to_string(),
            })
    }

    pub fn save(&self, checkpoint: &ScalpingCheckpoint) -> Result<(), ScalpingError> {
        self.store
            .save(checkpoint)
            .map_err(|error| ScalpingError::Persistence {
                detail: error.to_string(),
            })
    }
}
