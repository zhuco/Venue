//! PostgreSQL-backed TEST-only copy planner worker. It produces semantic delivery jobs only and
//! has no gateway, credential, Owner, WAL, writer, reconciliation, or mutation surface.

use std::{env, time::SystemTime};

use sqlx::postgres::PgPoolOptions;
use venue_control::{
    CopyObserverScope, CopyTestWorker, CopyTestWorkerConfig, MIGRATION_0001, MIGRATION_0002,
    MIGRATION_0003, PgControlRepository,
};
use venue_control_protocol::{GatewayMode, VenueId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = required("DATABASE_URL")?;
    let mode = required("VENUE_COPY_MODE")?
        .parse::<GatewayMode>()
        .map_err(|_| "VENUE_COPY_MODE must be exactly TEST; LIVE is disabled")?;
    let scope = CopyObserverScope {
        observer_id: required("VENUE_COPY_OBSERVER_ID")?,
        venue: required("VENUE_COPY_VENUE")?
            .parse::<VenueId>()
            .map_err(|_| "VENUE_COPY_VENUE is not a supported canonical venue")?,
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
    let worker = CopyTestWorker::new(
        PgControlRepository::new(pool),
        CopyTestWorkerConfig {
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
        "copy TEST worker recovered cursor={} jobs={} ledger_entries={}",
        recovered.observer_cursor,
        recovered.jobs.len(),
        recovered.ledger_entries.len()
    );

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Some(planned) = worker.plan_next(unix_time_ms()?).await? {
                    println!(
                        "copy TEST planner committed event={} job={}",
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
    env::var(name).map_err(|_| format!("{name} must be set for the local TEST copy worker").into())
}

fn unix_time_ms() -> Result<u64, Box<dyn std::error::Error>> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis();
    Ok(u64::try_from(millis)?)
}
