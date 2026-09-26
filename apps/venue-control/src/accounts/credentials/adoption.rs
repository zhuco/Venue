use super::*;

pub(super) async fn adopt_deleted_personal(
    connection: &mut sqlx::PgConnection,
    principal: &Principal,
    credential: &str,
    prior_account: &str,
    prior_owner: &str,
    probe: &venue_gateway_binance::BinanceCredentialProbe,
) -> Result<Option<String>, AccountError> {
    // The authenticated managed verification path supplies the internal subject. Ownership
    // must lead back to that managed record's KOL, never merely to possession of an API key.
    let managed: Option<String> = sqlx::query_scalar(
        "SELECT m.managed_id FROM venue_managed_credentials m
         JOIN venue_kol_profiles p ON p.kol_user_id=m.kol_user_id
         JOIN venue_users u ON u.user_id=m.follower_user_id
         WHERE m.follower_user_id=$1 AND m.credential_id=$2 AND m.kol_user_id=$3
           AND m.delete_requested_ms IS NULL AND p.profile_state='enabled' AND NOT u.login_enabled
         FOR SHARE OF m,p",
    )
    .bind(&principal.user.user_id)
    .bind(credential)
    .bind(prior_owner)
    .fetch_optional(&mut *connection)
    .await
    .map_err(database_error)?;
    let Some(managed) = managed else {
        return Ok(None);
    };
    if probe.has_exposure {
        return Err(error(Code::AccountInUse));
    }
    let rows = sqlx::query("SELECT deleted_ms,octet_length(encrypted_credentials) AS secret_bytes FROM venue_api_credentials WHERE trading_account_id=$1 ORDER BY credential_id FOR UPDATE")
        .bind(prior_account).fetch_all(&mut *connection).await.map_err(database_error)?;
    if rows.is_empty()
        || rows.iter().any(|row| {
            row.try_get::<Option<i64>, _>("deleted_ms")
                .ok()
                .flatten()
                .is_none()
                || row.try_get::<i32, _>("secret_bytes").ok() != Some(0)
        })
    {
        return Ok(None);
    }
    // Deletion already required current signed flatness and command drain. Recheck local
    // custody under the original account row lock, using this new signed observation.
    deletion::check_current_custody(connection, prior_account, Some(probe.observed_ms)).await?;
    let source: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM venue_kol_profiles WHERE leader_trading_account_id=$1)",
    )
    .bind(prior_account)
    .fetch_one(&mut *connection)
    .await
    .map_err(database_error)?;
    if source {
        return Err(error(Code::AccountInUse));
    }
    let next = crypto::opaque_id()?;
    let changed = sqlx::query("UPDATE venue_user_trading_accounts SET retired_ms=$1,successor_account_id=$2,retired_by_managed_id=$3 WHERE trading_account_id=$4 AND user_id=$5 AND retired_ms IS NULL AND venue='binance'")
        .bind(ms(probe.observed_ms)?).bind(&next).bind(&managed).bind(prior_account).bind(prior_owner)
        .execute(&mut *connection).await.map_err(database_error)?;
    if changed.rows_affected() != 1 {
        return Err(error(Code::Conflict));
    }
    sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)")
        .bind(&next).bind(&principal.user.user_id).bind(probe.account_identity_hash.as_slice())
        .execute(&mut *connection).await.map_err(database_error)?;
    Ok(Some(next))
}
