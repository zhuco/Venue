use super::RECORD_MIRROR_FACT;
use sqlx::{Connection, Executor, PgConnection};

async fn record(
    connection: &mut PgConnection,
    command: &str,
    native: &str,
    quantity: &str,
    filled: &str,
) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(RECORD_MIRROR_FACT)
        .bind(native)
        .bind(filled)
        .bind(false)
        .bind(1000_i64)
        .bind(command)
        .bind(quantity)
        .execute(connection)
        .await?
        .rows_affected())
}

#[tokio::test]
async fn rounded_mirror_facts_are_idempotent_and_keep_legacy_and_cancel_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let url = match std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED").as_deref() == Ok("1") => {
            return Err("isolated PostgreSQL required".into());
        }
        Err(_) => {
            eprintln!("SKIP: isolated PostgreSQL for rounded mirror facts is not configured");
            return Ok(());
        }
    };
    let mut connection = PgConnection::connect(&url).await?;
    // Connection-local fixture tables exercise the exact production UPDATE without touching
    // permanent relations. Other integration tests validate the full schema and lifecycle.
    connection.execute("CREATE TEMP TABLE venue_order_mirrors (mirror_id TEXT PRIMARY KEY,child_native_order_id TEXT,child_quantity TEXT,filled_quantity TEXT,mirror_state TEXT,updated_ms BIGINT);
        CREATE TEMP TABLE venue_binance_commands (command_id TEXT PRIMARY KEY,mirror_order_id TEXT,command_phase TEXT,copy_risk JSONB);
        INSERT INTO venue_order_mirrors VALUES ('up',NULL,'54.878718033146745692020634398','0','pending',1),('old','old-native','54.878718033146745692020634398','0','live',1),('close',NULL,'54.8','0','pending',1);
        INSERT INTO venue_binance_commands VALUES ('up','up','open','{\"round_open_quantity_up\":true}'),('cancel','up','cancel','{\"round_open_quantity_up\":true}'),('old','old','open','{}'),('close','close','close','{\"round_open_quantity_up\":true}')").await?;
    assert_eq!(record(&mut connection, "up", "native", "55", "0").await?, 1);
    assert_eq!(record(&mut connection, "up", "native", "55", "0").await?, 1);
    let quantity: String =
        sqlx::query_scalar("SELECT child_quantity FROM venue_order_mirrors WHERE mirror_id='up'")
            .fetch_one(&mut connection)
            .await?;
    assert_eq!(quantity, "55");
    assert_eq!(record(&mut connection, "up", "native", "56", "0").await?, 0);
    assert_eq!(
        record(&mut connection, "up", "wrong-native", "55", "0").await?,
        0
    );
    assert_eq!(
        record(&mut connection, "up", "native", "55", "10").await?,
        1
    );
    assert_eq!(record(&mut connection, "up", "native", "55", "9").await?, 0);
    assert_eq!(
        record(&mut connection, "cancel", "native", "55", "10").await?,
        1
    );
    assert_eq!(
        record(&mut connection, "cancel", "native", "56", "10").await?,
        0
    );
    assert_eq!(
        record(&mut connection, "old", "old-native", "54", "0").await?,
        1
    );
    assert_eq!(
        record(&mut connection, "old", "old-native", "55", "0").await?,
        0
    );
    assert_eq!(
        record(&mut connection, "close", "close-native", "55", "0").await?,
        0
    );
    assert_eq!(
        record(&mut connection, "close", "close-native", "54", "0").await?,
        1
    );
    connection.close().await?;
    Ok(())
}
