//! Minimal fail-closed process configuration for the production Binance executor.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorLaunchConfig {
    pub database_url: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorLaunchConfigError {
    #[error("VENUE_EXECUTOR_DATABASE_URL is required")]
    DatabaseUrl,
    #[error("VENUE_EXECUTOR_MODE must be exactly LIVE")]
    Mode,
}

impl ExecutorLaunchConfig {
    pub fn from_environment() -> Result<Self, ExecutorLaunchConfigError> {
        let mode =
            std::env::var("VENUE_EXECUTOR_MODE").map_err(|_| ExecutorLaunchConfigError::Mode)?;
        let database_url = std::env::var("VENUE_EXECUTOR_DATABASE_URL")
            .map_err(|_| ExecutorLaunchConfigError::DatabaseUrl)?;
        Self::parse(&mode, database_url)
    }

    fn parse(mode: &str, database_url: String) -> Result<Self, ExecutorLaunchConfigError> {
        if mode != "LIVE" {
            return Err(ExecutorLaunchConfigError::Mode);
        }
        if database_url.trim() != database_url
            || !(database_url.starts_with("postgres://")
                || database_url.starts_with("postgresql://"))
        {
            return Err(ExecutorLaunchConfigError::DatabaseUrl);
        }
        Ok(Self { database_url })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_only_explicit_live_postgres_configuration() {
        assert!(
            ExecutorLaunchConfig::parse("LIVE", "postgres://user:pass@localhost/venue".into())
                .is_ok()
        );
        assert_eq!(
            ExecutorLaunchConfig::parse("live", "postgres://localhost/venue".into()),
            Err(ExecutorLaunchConfigError::Mode)
        );
        assert_eq!(
            ExecutorLaunchConfig::parse("LIVE", "https://example.test/venue".into()),
            Err(ExecutorLaunchConfigError::DatabaseUrl)
        );
    }
}
