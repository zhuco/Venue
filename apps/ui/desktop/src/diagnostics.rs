//! Detached desktop processes cannot rely on the launcher's stdout pipe being drained.
use std::{fs::OpenOptions, path::PathBuf, sync::Mutex};
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_diagnostics() {
    let path = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .map(|root| root.join("VenueFlow").join("desktop.log"));
    let file = path.and_then(|path| {
        let parent = path.parent()?;
        std::fs::create_dir_all(parent).ok()?;
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .ok()
    });
    // Falling back to stdout could stall both network workers after the launcher exits.
    let writer = match file {
        Some(file) => BoxMakeWriter::new(Mutex::new(file)),
        None => BoxMakeWriter::new(std::io::sink),
    };
    let layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_ansi(false)
        .with_writer(writer)
        .with_filter(
            tracing_subscriber::filter::Targets::new()
                .with_default(tracing::Level::WARN)
                .with_target("venueflow::terminal_latency", tracing::Level::INFO),
        );
    let _ = tracing_subscriber::registry().with(layer).try_init();
}
