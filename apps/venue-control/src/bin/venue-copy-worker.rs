//! PostgreSQL-backed semantic Copy planner. It produces LIVE-bound semantic delivery jobs only
//! and has no gateway, credential, Owner, WAL, writer, reconciliation, or mutation surface.

use std::{env, time::SystemTime};

use sqlx::postgres::PgPoolOptions;
use venue_control::{
    CopyObserverScope, CopyWorker, CopyWorkerConfig, MIGRATION_0001, MIGRATION_0002,
    MIGRATION_0003, MIGRATION_0004, MIGRATION_0005, MIGRATION_0006, MIGRATION_0007, MIGRATION_0008,
    MIGRATION_0009, MIGRATION_0010, MIGRATION_0011, MIGRATION_0012, MIGRATION_0013, MIGRATION_0014,
    PgControlRepository,
};
use venue_control_protocol::{GatewayMode, VenueId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = required("DATABASE_URL")?;
    let mode = parse_live_mode(&required("VENUE_COPY_MODE")?)?;
    let scope = CopyObserverScope {
        observer_id: required("VENUE_COPY_OBSERVER_ID")?,
        venue: required("VENUE_COPY_VENUE")?
            .parse::<VenueId>()
            .map_err(|_| "VENUE_COPY_VENUE is not a supported canonical venue")?,
        mode,
        trading_account_id: required("VENUE_COPY_TRADING_ACCOUNT_ID")?,
    };
    let worker_id = required("VENUE_COPY_WORKER_ID")?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await?;
    sqlx::raw_sql(MIGRATION_0001).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0002).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0003).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0004).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0005).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0006).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0007).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0008).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0009).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0010).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0011).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0012).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0013).execute(&pool).await?;
    sqlx::raw_sql(MIGRATION_0014).execute(&pool).await?;
    let worker = CopyWorker::new(
        PgControlRepository::new(pool),
        CopyWorkerConfig {
            mode,
            scope,
            worker_id,
            observer_lease_ms: 30_000,
            delivery_claim_ms: 30_000,
        },
    )?;
    let recovered_at = unix_time_ms()?;
    let recovered = worker.recover(recovered_at).await?;
    println!(
        "copy worker recovered cursor={} jobs={} ledger_entries={}",
        recovered.observer_cursor,
        recovered.jobs.len(),
        recovered.ledger_entries.len()
    );

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let now_ms = unix_time_ms()?;
                if let Some(rejected) = worker.project_next_rejected_delivery().await? {
                    println!("copy rejected delivery projection result={rejected:?}");
                }
                if let Some(projected) = worker.project_next_reconciled_ledger(now_ms).await? {
                    println!("copy ledger projection result={projected:?}");
                }
                if let Some(planned) = worker.plan_next(now_ms).await? {
                    println!(
                        "copy planner committed event={} job={}",
                        planned.observed.event_sequence,
                        planned.job.identities.job_id
                    );
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
        }
    }
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("{name} must be set for the local copy worker").into())
}

fn parse_live_mode(raw: &str) -> Result<GatewayMode, Box<dyn std::error::Error>> {
    if raw == "LIVE" {
        Ok(GatewayMode::Live)
    } else {
        Err("VENUE_COPY_MODE must be exactly LIVE".into())
    }
}

fn unix_time_ms() -> Result<u64, Box<dyn std::error::Error>> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis();
    Ok(u64::try_from(millis)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_mode_parser_accepts_only_the_exact_live_token() {
        assert_eq!(parse_live_mode("LIVE").ok(), Some(GatewayMode::Live));
        for raw in ["TEST", "live", " LIVE", "LIVE ", ""] {
            assert!(parse_live_mode(raw).is_err(), "accepted {raw:?}");
        }
    }
}
