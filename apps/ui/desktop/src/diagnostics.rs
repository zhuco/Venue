//! Detached desktop processes cannot rely on the launcher's stdout pipe being drained.
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::mpsc::{SyncSender, sync_channel},
};
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

const FILE_LIMIT: u64 = 2 * 1024 * 1024;
const LINE_LIMIT: usize = 16 * 1024;

struct LineWriter {
    sender: SyncSender<Vec<u8>>,
    bytes: Vec<u8>,
}
impl Write for LineWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let keep = bytes.len().min(LINE_LIMIT.saturating_sub(self.bytes.len()));
        self.bytes.extend_from_slice(&bytes[..keep]);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Drop for LineWriter {
    fn drop(&mut self) {
        // Diagnostics may be dropped; a slow disk must not block network or UI workers.
        if !self.bytes.is_empty() {
            let _ = self.sender.try_send(std::mem::take(&mut self.bytes));
        }
    }
}

struct LogFile {
    file: File,
    path: PathBuf,
    size: u64,
}
impl LogFile {
    fn open(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(1);
        }
        let mut file = options.open(&path)?;
        let size = file.metadata()?.len();
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file, path, size })
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.size + bytes.len() as u64 > FILE_LIMIT {
            self.file.flush()?;
            self.file
                .seek(SeekFrom::Start(self.size.saturating_sub(FILE_LIMIT)))?;
            let mut previous = File::create(self.path.with_extension("previous.log"))?;
            io::copy(&mut (&mut self.file).take(FILE_LIMIT), &mut previous)?;
            previous.flush()?;
            self.file.set_len(0)?;
            self.file.rewind()?;
            self.size = 0;
        }
        self.file.write_all(bytes)?;
        self.size += bytes.len() as u64;
        Ok(())
    }
}

pub fn init_diagnostics() {
    let path = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .map(|root| root.join("VenueFlow").join("desktop.log"));
    let (sender, receiver) = sync_channel::<Vec<u8>>(256);
    let worker = std::thread::Builder::new()
        .name("venueflow-diagnostics".into())
        .spawn(move || {
            let Some(mut log) = path.and_then(|path| LogFile::open(path).ok()) else {
                return;
            };
            while let Ok(bytes) = receiver.recv() {
                if log.append(&bytes).is_err() {
                    break;
                }
            }
        });
    let writer = if worker.is_ok() {
        BoxMakeWriter::new(move || LineWriter {
            sender: sender.clone(),
            bytes: Vec::new(),
        })
    } else {
        BoxMakeWriter::new(std::io::sink)
    };
    let layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_ansi(false)
        .with_writer(writer)
        .with_filter(
            tracing_subscriber::filter::Targets::new()
                .with_default(tracing::Level::WARN)
                .with_target("venueflow::terminal_latency", tracing::Level::INFO)
                .with_target("venueflow::account_projection", tracing::Level::INFO)
                .with_target("venueflow::chart_loading", tracing::Level::INFO),
        );
    let _ = tracing_subscriber::registry().with(layer).try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn saturated_log_queue_drops_bounded_records_without_waiting() -> io::Result<()> {
        let (sender, receiver) = sync_channel(1);
        for _ in 0..3 {
            let mut writer = LineWriter {
                sender: sender.clone(),
                bytes: Vec::new(),
            };
            writer.write_all(&vec![b'x'; LINE_LIMIT * 2])?;
        }
        assert_eq!(
            receiver.try_recv().map_err(io::Error::other)?.len(),
            LINE_LIMIT
        );
        assert!(receiver.try_recv().is_err());
        Ok(())
    }
    #[test]
    fn log_reopen_preserves_previous_run_and_rotation_is_bounded() -> io::Result<()> {
        let path =
            std::env::temp_dir().join(format!("venueflow-log-test-{}.log", std::process::id()));
        {
            let mut log = LogFile::open(path.clone())?;
            log.file.set_len(0)?;
            log.file.rewind()?;
            log.size = 0;
            log.append(b"previous run\n")?;
        }
        {
            let mut log = LogFile::open(path.clone())?;
            assert_eq!(log.size, 13);
            log.file.set_len(FILE_LIMIT)?;
            log.size = FILE_LIMIT;
            log.append(b"next\n")?;
            assert_eq!(log.size, 5);
            assert_eq!(
                std::fs::metadata(path.with_extension("previous.log"))?.len(),
                FILE_LIMIT
            );
        }
        std::fs::remove_file(path.with_extension("previous.log"))?;
        std::fs::remove_file(path)?;
        Ok(())
    }
}
