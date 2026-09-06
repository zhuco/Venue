//! Operator inspection stays inside the executor credential boundary and never starts a writer.
use super::*;
use sqlx::{Row, postgres::PgPoolOptions};

pub async fn inspect_account(
    database_url: &str,
    credential_id: &str,
    symbol: venue_domain::Symbol,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET default_transaction_read_only=on")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .map_err(|_| "inspection database unavailable")?;
    let row = sqlx::query("SELECT user_id,trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND deleted_ms IS NULL AND venue='binance'")
        .bind(credential_id).fetch_one(&pool).await.map_err(|_| "inspection account unavailable")?;
    let owner: String = row.try_get("user_id")?;
    let account: String = row.try_get("trading_account_id")?;
    let secrets = crate::executor_secret::ExecutorSecretProvider::new(
        pool.clone(),
        crate::accounts::CredentialCipher::from_environment()?,
    );
    let credentials = secrets.load(credential_id, &owner).await?;
    let binding = venue_gateway_binance::GatewayBinding::new(
        venue_gateway_binance::VenueId::Binance,
        venue_gateway_binance::GatewayMode::Live,
        account.clone(),
        symbol,
    )?;
    let snapshot = tokio::task::spawn_blocking(move || {
        let limits = venue_gateway_binance::BinanceTransportLimits::new(
            std::time::Duration::from_secs(10),
            2 * 1024 * 1024,
        )
        .map_err(|error| error.to_string())?;
        let mut gateway =
            venue_gateway_binance::BinanceAccountGateway::connect_with_credentials_for_symbols(
                binding,
                std::collections::BTreeSet::new(),
                credentials,
                limits,
            )
            .map_err(|error| error.to_string())?;
        gateway
            .signed_projection_snapshot(None)
            .map(|snapshot| {
                serde_json::json!({
                    "observed_ms": snapshot.observed_at_ms(),
                    "positions": snapshot.positions(),
                    "open_orders": snapshot.open_orders(),
                    "conditional_orders": snapshot.conditional_orders(),
                    "fills": snapshot.fills(),
                })
            })
            .map_err(|error| error.to_string())
    })
    .await?;
    let store = PgExecutorStore::new(pool.clone());
    let mut exchange = crate::executor_exchange::BinanceExecutionRouter::new(
        venue_gateway_binance::BinanceTransportLimits::new(
            std::time::Duration::from_secs(10),
            2 * 1024 * 1024,
        )?,
    );
    let mut commands = Vec::new();
    for command in store.recover_nonterminal().await? {
        if command.credential_id != credential_id || command.owner_user_id != owner {
            continue;
        }
        let admission = crate::order_mirror::mirror_send_allowed(&store, &command, now_ms()?).await;
        let mut read = request(&command);
        store.prepare_mirror_request(&command, &mut read).await?;
        let result = exchange
            .readback(&read, secrets.load(credential_id, &owner).await?)
            .await;
        commands.push(serde_json::json!({
            "command_id": command.command_id,
            "stored_state": format!("{:?}", command.state),
            "admission": format!("{admission:?}"),
            "readback": format!("{result:?}"),
        }));
    }
    pool.close().await;
    Ok(
        serde_json::json!({"credential_id":credential_id,"account":account,"snapshot":snapshot,"commands":commands}),
    )
}
