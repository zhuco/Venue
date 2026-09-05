//! Trusted local permission administration; no exchange credentials or trading connection.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let migrate = args.as_slice() == ["migrate"];
    if !migrate && (args.len() != 4 || !matches!(args[0].as_str(), "grant" | "revoke")) {
        return Err("usage: venue-leader-bot-admin migrate | <grant|revoke> <KOL user UUID> <expected revision; 0 for first grant> <operator>; requires VENUE_CONTROL_ADMIN_DATABASE_URL".into());
    }
    let url = std::env::var("VENUE_CONTROL_ADMIN_DATABASE_URL")
        .map_err(|_| "administrator database URL is missing")?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .map_err(|_| "administrator database connection failed")?;
    if migrate {
        venue_control::install_control_schema(&pool).await?;
        println!("Control schema installed and verified");
        pool.close().await;
        return Ok(());
    }
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let revision = venue_control::leader_bot_admin::set_permission(
        &pool,
        &args[1],
        args[0] == "grant",
        args[2].parse()?,
        &args[3],
        now,
    )
    .await?;
    println!("Leader bot permission updated; revision={revision}");
    pool.close().await;
    Ok(())
}
