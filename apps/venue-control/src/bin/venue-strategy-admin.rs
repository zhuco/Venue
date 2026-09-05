//! Trusted operator API for independent strategies. Credentials enter through stdin only.
use sqlx::{Row, postgres::PgPoolOptions};
use std::io::Read;
use venue_control::{
    accounts::CredentialCipher,
    multi_venue_credentials::{StrategyCredentialStore, probe_strategy_account},
    multi_venue_exchange::StrategyCredentials,
    multi_venue_runtime::now_ms,
    multi_venue_store::MultiVenueStore,
    support_martingale::SupportMartingaleStore,
};
use venue_control_protocol::support_martingale::{
    SupportMartingaleCreateRequest, SupportMartingaleLifecycleRequest,
};
use venue_domain::domain::{ExecutionCommand, Symbol};
use zeroize::Zeroizing;

fn input<T: serde::de::DeserializeOwned>() -> Result<T, Box<dyn std::error::Error>> {
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin().take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 {
        return Err("input exceeds 64 KiB".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid JSON input".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let valid = matches!(args.as_slice(), [op] if op == "migrate")
        || matches!(args.as_slice(), [op, _, _, _, _] if op == "bind" || op == "bind-released")
        || matches!(args.as_slice(), [op, _] if op == "martingale-create" || op == "martingale-lifecycle")
        || matches!(args.as_slice(), [op, _, _] if matches!(op.as_str(), "submit"|"status"|"limits"|"grid-create"|"grid-status"|"probe"|"martingale-status"))
        || matches!(args.as_slice(), [op, _, _, _] if op == "snapshot" || op == "grid-lifecycle");
    if !valid {
        return Err("usage: venue-strategy-admin migrate | probe ACCOUNT SYMBOL | bind|bind-released USER ACCOUNT SYMBOL LABEL | limits USER CREDENTIAL | submit USER CREDENTIAL | snapshot USER CREDENTIAL SYMBOL | status USER COMMAND | grid-create USER CREDENTIAL | grid-status USER INSTANCE | grid-lifecycle USER INSTANCE start|pause|resume|stop|reset | martingale-create USER | martingale-status USER INSTANCE | martingale-lifecycle USER; structured input uses stdin".into());
    }
    if args[0] == "probe" {
        let credentials: StrategyCredentials = input()?;
        let probe =
            probe_strategy_account(&args[1], args[2].parse::<Symbol>()?, credentials).await?;
        println!("{}", serde_json::to_string(&probe)?);
        return Ok(());
    }
    let database = Zeroizing::new(
        std::env::var("VENUE_STRATEGY_ADMIN_DATABASE_URL")
            .map_err(|_| "VENUE_STRATEGY_ADMIN_DATABASE_URL is required")?,
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database)
        .await
        .map_err(|_| "strategy admin database unavailable")?;
    match args[0].as_str() {
        "limits" => {
            StrategyCredentialStore::new(pool, CredentialCipher::from_environment()?)
                .set_limits(&args[1], &args[2], input()?)
                .await?;
            println!("limits saved");
        }
        "grid-create" => {
            let id = venue_control::multi_venue_grid::StrategyGridStore::new(pool)
                .create(&args[1], &args[2], input()?, now_ms()?)
                .await?;
            println!("{id}");
        }
        "grid-status" => {
            let record = venue_control::multi_venue_grid::StrategyGridStore::new(pool)
                .get(&args[1], &args[2])
                .await?;
            println!("{}", serde_json::to_string(&record)?);
        }
        "grid-lifecycle" => {
            venue_control::multi_venue_grid::StrategyGridStore::new(pool)
                .lifecycle(&args[1], &args[2], &args[3], now_ms()?)
                .await?;
            println!("lifecycle intent saved");
        }
        "martingale-create" => {
            let request: SupportMartingaleCreateRequest = input()?;
            let id = SupportMartingaleStore::new(pool)
                .create(&args[1], request, now_ms()?)
                .await?;
            println!("{id}");
        }
        "martingale-status" => {
            let record = SupportMartingaleStore::new(pool)
                .get(&args[1], &args[2])
                .await?;
            println!("{}", serde_json::to_string(&record)?);
        }
        "martingale-lifecycle" => {
            let request: SupportMartingaleLifecycleRequest = input()?;
            let revision = SupportMartingaleStore::new(pool)
                .lifecycle(&args[1], request, now_ms()?)
                .await?;
            println!("{revision}");
        }
        "migrate" => venue_control::install_control_schema(&pool).await?,
        "bind" | "bind-released" => {
            let store = StrategyCredentialStore::new(pool, CredentialCipher::from_environment()?);
            let credentials: StrategyCredentials = input()?;
            let summary = if args[0] == "bind-released" {
                store
                    .bind_released_account(
                        &args[1],
                        &args[2],
                        &args[4],
                        args[3].parse::<Symbol>()?,
                        credentials,
                        now_ms()?,
                    )
                    .await?
            } else {
                store
                    .bind(
                        &args[1],
                        &args[2],
                        &args[4],
                        args[3].parse::<Symbol>()?,
                        credentials,
                        now_ms()?,
                    )
                    .await?
            };
            println!("{}", serde_json::to_string(&summary)?);
        }
        "submit" => {
            let command: ExecutionCommand = input()?;
            let result = MultiVenueStore::new(pool)
                .enqueue(&args[1], &args[2], command, now_ms()?)
                .await?;
            println!("{result:?}");
        }
        "snapshot" => {
            let store = StrategyCredentialStore::new(pool, CredentialCipher::from_environment()?);
            let snapshot = store
                .snapshot(&args[1], &args[2], args[3].parse::<Symbol>()?)
                .await?;
            println!("{}", serde_json::to_string(&snapshot)?);
        }
        "status" => {
            let row = sqlx::query("SELECT command_state,native_order_id,next_reconcile_ms FROM venue_binance_commands WHERE command_id=$1 AND owner_user_id=$2 AND strategy_command IS NOT NULL")
                .bind(&args[2]).bind(&args[1]).fetch_one(&pool).await.map_err(|_| "strategy command unavailable")?;
            println!(
                "{}",
                serde_json::json!({"state": row.try_get::<String,_>("command_state")?, "native_order_id": row.try_get::<Option<String>,_>("native_order_id")?, "next_reconcile_ms": row.try_get::<Option<i64>,_>("next_reconcile_ms")?})
            );
        }
        _ => return Err("unsupported operation".into()),
    }
    Ok(())
}
