use super::*;

#[tokio::test]
async fn mm_projection_readback_preserves_decision_across_heartbeat()
-> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") else {
        eprintln!("SKIP: VENUE_CONTROL_TEST_DATABASE_URL is not configured");
        return Ok(());
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let schema = format!("venue_mm_readback_{}_{}", std::process::id(), nonce);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    sqlx::raw_sql(&format!("CREATE SCHEMA {schema}; SET search_path TO {schema};
        CREATE TABLE venue_binance_commands(command_id TEXT, command_origin TEXT,
          credential_id TEXT, owner_user_id TEXT, trading_account_id TEXT,
          inventory_mm_private_generation BIGINT, inventory_mm_observed_ms BIGINT);
        CREATE TABLE venue_binance_account_projections(credential_id TEXT, owner_user_id TEXT,
          trading_account_id TEXT, private_generation BIGINT, observed_ms BIGINT, projection_json JSONB);"))
        .execute(&pool).await?;
    sqlx::raw_sql(crate::MIGRATION_0049).execute(&pool).await?;
    let original = json!({"stream_healthy":true,"fills_cursor":"fixture","projection":{
        "schema_version":1,"credential_id":"credential","trading_account_id":"account",
        "observed_ms":1000,"persisted_ms":1000,"private_generation":7,"position_mode":"hedge",
        "positions":[],"open_orders":[],"conditional_orders":[],"fills":[],"assets":[]}});
    sqlx::query("INSERT INTO venue_binance_account_projections VALUES('credential','owner','account',7,1000,$1)")
        .bind(&original).execute(&pool).await?;
    sqlx::query("INSERT INTO venue_binance_commands SELECT 'command','inventory_mm','credential','owner','account',7,1000,venue_mm_projection_digest(projection_json) FROM venue_binance_account_projections")
        .execute(&pool).await?;
    let store = PgExecutorStore::new(pool.clone());
    assert!(store.inventory_mm_projection("command").await?.is_some());
    let mut heartbeat = original.clone();
    heartbeat["projection"]["observed_ms"] = json!(1500);
    heartbeat["projection"]["persisted_ms"] = json!(1500);
    sqlx::query("UPDATE venue_binance_account_projections SET observed_ms=1500,projection_json=$1")
        .bind(&heartbeat)
        .execute(&pool)
        .await?;
    let readback = store
        .inventory_mm_projection("command")
        .await?
        .ok_or("heartbeat rejected")?;
    assert_eq!(
        readback.observed_ms, 1000,
        "physical dispatch keeps the original five-second deadline"
    );
    assert_eq!(readback.persisted_ms, 1500);
    for field in [
        "positions",
        "open_orders",
        "conditional_orders",
        "fills",
        "assets",
        "position_mode",
    ] {
        let mut changed = heartbeat.clone();
        changed["projection"][field] = json!([{"changed":true}]);
        sqlx::query("UPDATE venue_binance_account_projections SET projection_json=$1")
            .bind(changed)
            .execute(&pool)
            .await?;
        assert!(
            store.inventory_mm_projection("command").await?.is_none(),
            "changed {field}"
        );
    }
    sqlx::query("UPDATE venue_binance_account_projections SET projection_json=$1")
        .bind(&heartbeat)
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE venue_binance_commands SET inventory_mm_projection_digest=NULL")
        .execute(&pool)
        .await?;
    assert!(
        store.inventory_mm_projection("command").await?.is_none(),
        "legacy exact timestamp gate"
    );
    sqlx::query("UPDATE venue_binance_account_projections SET observed_ms=1000,projection_json=$1")
        .bind(&original)
        .execute(&pool)
        .await?;
    assert!(store.inventory_mm_projection("command").await?.is_some());
    sqlx::query("UPDATE venue_binance_account_projections SET private_generation=8")
        .execute(&pool)
        .await?;
    assert!(store.inventory_mm_projection("command").await?.is_none());
    sqlx::query("UPDATE venue_binance_account_projections SET private_generation=7,projection_json=jsonb_set(projection_json,'{stream_healthy}','false')").execute(&pool).await?;
    assert!(store.inventory_mm_projection("command").await?.is_none());
    sqlx::raw_sql(&format!(
        "SET search_path TO public; DROP SCHEMA {schema} CASCADE"
    ))
    .execute(&pool)
    .await?;
    pool.close().await;
    Ok(())
}
