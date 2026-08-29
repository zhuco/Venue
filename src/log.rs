use tracing::Level;
use tracing_subscriber::util::SubscriberInitExt;

use crate::{Result, config::LogLevel, error::Error};

pub(crate) fn init(level: LogLevel) -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(to_level(level))
        .with_target(false)
        .compact()
        .finish()
        .try_init()
        .map_err(|err| Error::Log(err.to_string()))
}

const fn to_level(level: LogLevel) -> Level {
    match level {
        LogLevel::Error => Level::ERROR,
        LogLevel::Warn => Level::WARN,
        LogLevel::Info => Level::INFO,
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Trace => Level::TRACE,
    }
}
