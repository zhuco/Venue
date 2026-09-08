use super::{InventoryMmStore, InventoryMmStoreError};
use crate::{
    executor_secret::ExecutorSecretProvider, private_projection::BinancePrivateProjectionStore,
};
use sqlx::PgPool;
use venue_control_protocol::inventory_mm::InventoryMmInstance;
use venue_gateway_binance::{
    BinanceGridMarketReader, BinanceTransportLimits, GatewayBinding, GatewayMode, VenueId,
};

/// Shared read-only admission for the authenticated service and trusted operational CLI.
/// Neither path may replace these authenticated leverage/margin facts with configured values.
pub async fn signed_gate(
    pool: PgPool,
    secrets: ExecutorSecretProvider,
    instance: &InventoryMmInstance,
) -> Result<(u8, u64), InventoryMmStoreError> {
    let unavailable = |_| InventoryMmStoreError::Unavailable;
    let projections = BinancePrivateProjectionStore::new(pool.clone());
    let projection = projections
        .load_healthy_owned(&instance.owner_user_id, &instance.credential_id)
        .await
        .map_err(unavailable)?
        .ok_or(InventoryMmStoreError::Conflict)?;
    let credentials = secrets
        .load_bound(
            &instance.credential_id,
            &instance.owner_user_id,
            &instance.trading_account_id,
        )
        .await
        .map_err(|_| InventoryMmStoreError::Conflict)?;
    let limits = BinanceTransportLimits::new(std::time::Duration::from_secs(10), 2 * 1024 * 1024)
        .map_err(|_| InventoryMmStoreError::Unavailable)?;
    let mut reader = BinanceGridMarketReader::new(
        GatewayBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: instance.trading_account_id.clone(),
            symbol: instance.config.symbol.clone(),
        },
        limits,
    )
    .map_err(|_| InventoryMmStoreError::Unavailable)?;
    let now =
        crate::multi_venue_runtime::now_ms().map_err(|_| InventoryMmStoreError::Unavailable)?;
    reader
        .refresh(now)
        .await
        .map_err(|_| InventoryMmStoreError::Unavailable)?;
    let (leverage, margin, conversion) = tokio::try_join!(
        reader.symbol_leverage(&credentials, projection.private_generation),
        reader.account_margin(&credentials, projection.private_generation),
        reader.quote_usd_evidence(projection.private_generation, 5_000)
    )
    .map_err(|_| InventoryMmStoreError::Unavailable)?;
    let (margin, margin_ms) = margin;
    let checked =
        crate::multi_venue_runtime::now_ms().map_err(|_| InventoryMmStoreError::Unavailable)?;
    let required_margin = instance
        .config
        .order_notional
        .checked_mul(rust_decimal::Decimal::TWO)
        .and_then(|v| v.checked_mul(conversion.usd_per_asset))
        .and_then(|v| {
            v.checked_div(rust_decimal::Decimal::from(
                instance.config.required_leverage,
            ))
        })
        .and_then(|v| v.checked_add(instance.config.min_available_margin))
        .ok_or(InventoryMmStoreError::Invalid)?;
    if leverage.0 != instance.config.required_leverage
        || leverage.1 > checked
        || checked - leverage.1 > 5_000
        || margin_ms > checked
        || checked - margin_ms > 5_000
        || conversion.private_generation != projection.private_generation
        || conversion.asset.as_str() != instance.config.symbol.quote()
        || conversion.source_time_ms > checked
        || checked - conversion.source_time_ms > 5_000
        || margin.available_balance < required_margin
    {
        return Err(InventoryMmStoreError::Conflict);
    }
    // Signed HTTP calls can cross a lifecycle/config change. Re-read revision and account
    // fences after the I/O; Start will repeat this under its credential lock before committing.
    let ready = InventoryMmStore::new(pool)
        .preflight(
            &instance.owner_user_id,
            &instance.instance_id,
            instance.revision,
            checked,
        )
        .await?;
    if !ready.ready {
        return Err(InventoryMmStoreError::Conflict);
    }
    Ok(leverage)
}
