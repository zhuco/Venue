//! The OS owns lock lifetime, so an interrupted process cannot leave a stale owner.
use std::{
    fs::{File, OpenOptions, TryLockError},
    io,
    path::{Path, PathBuf},
};

pub fn acquire() -> io::Result<Option<File>> {
    let root = if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
    } else {
        std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share"))
    }
    .ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "VenueFlow user directory unavailable",
        )
    })?;
    let directory = root.join("VenueFlow");
    std::fs::create_dir_all(&directory)?;
    acquire_at(&directory.join("desktop.instance.lock"))
}

fn acquire_at(path: &Path) -> io::Result<Option<File>> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // Prevent deleting/replacing the locked inode while allowing other launchers to inspect it.
        options.share_mode(3);
    }
    let file = options.open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_lock_blocks_duplicates_and_releases_after_exit() -> io::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "venueflow-instance-test-{}.lock",
            std::process::id()
        ));
        let first = acquire_at(&path)?;
        assert!(first.is_some());
        let child = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "single_instance::tests::child_cannot_acquire_held_lock",
            ])
            .env("VENUEFLOW_TEST_INSTANCE_LOCK", &path)
            .status()?;
        assert!(child.success());
        drop(first);
        let reopened = acquire_at(&path)?;
        assert!(reopened.is_some());
        drop(reopened);
        std::fs::remove_file(path)
    }

    #[test]
    fn child_cannot_acquire_held_lock() -> io::Result<()> {
        if let Some(path) = std::env::var_os("VENUEFLOW_TEST_INSTANCE_LOCK") {
            assert!(acquire_at(Path::new(&path))?.is_none());
        }
        Ok(())
    }

    #[test]
    fn inaccessible_lock_is_an_error() {
        assert!(acquire_at(&std::env::temp_dir()).is_err());
    }
}
