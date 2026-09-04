//! Local administrator operation, never exposed to user HTTP routes. The caller must already
//! possess database administration access; supplying a username to a public API cannot grant it.
use sqlx::{PgPool, Row};

#[derive(Debug, thiserror::Error)]
pub enum LeaderBotAdminError {
    #[error("invalid permission request")]
    Invalid,
    #[error("KOL is absent or permission revision changed")]
    Conflict,
    #[error("permission database unavailable")]
    Unavailable,
}

pub async fn set_permission(
    pool: &PgPool,
    user: &str,
    enabled: bool,
    expected_revision: u64,
    operator: &str,
    now_ms: u64,
) -> Result<u64, LeaderBotAdminError> {
    use LeaderBotAdminError::*;
    if !venue_control_protocol::leader_bot::valid_id(user)
        || operator.trim().is_empty()
        || operator.chars().count() > 100
        || operator.chars().any(char::is_control)
        || now_ms == 0
    {
        return Err(Invalid);
    }
    let expected = i64::try_from(expected_revision).map_err(|_| Invalid)?;
    let next = expected.checked_add(1).ok_or(Invalid)?;
    let now = i64::try_from(now_ms).map_err(|_| Invalid)?;
    let mut tx = pool.begin().await.map_err(|_| Unavailable)?;
    sqlx::query("SELECT kol_user_id FROM venue_kol_profiles WHERE kol_user_id=$1 FOR UPDATE")
        .bind(user)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| Unavailable)?
        .ok_or(Conflict)?;
    let prior = sqlx::query(
        "SELECT revision FROM venue_leader_bot_permissions WHERE kol_user_id=$1 FOR UPDATE",
    )
    .bind(user)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|_| Unavailable)?;
    let revision = prior
        .map(|row| row.try_get::<i64, _>("revision"))
        .transpose()
        .map_err(|_| Unavailable)?
        .unwrap_or(0);
    if revision != expected {
        return Err(Conflict);
    }
    sqlx::query("INSERT INTO venue_leader_bot_permissions (kol_user_id,enabled,revision,updated_by,updated_ms) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(kol_user_id) DO UPDATE SET enabled=EXCLUDED.enabled,revision=EXCLUDED.revision,updated_by=EXCLUDED.updated_by,updated_ms=EXCLUDED.updated_ms")
        .bind(user).bind(enabled).bind(next).bind(operator).bind(now).execute(&mut *tx).await.map_err(|_| Unavailable)?;
    sqlx::query("INSERT INTO venue_leader_bot_permission_audit (kol_user_id,revision,enabled,operator,occurred_ms) VALUES ($1,$2,$3,$4,$5)")
        .bind(user).bind(next).bind(enabled).bind(operator).bind(now).execute(&mut *tx).await.map_err(|_| Unavailable)?;
    // Even a re-grant cannot resurrect a previous run or its pending orders.
    sqlx::query("UPDATE venue_leader_bots SET bot_state='draining',revision=revision+1,attention_code='permission_changed',updated_ms=$2 WHERE owner_user_id=$1 AND bot_state='running'")
        .bind(user).bind(now).execute(&mut *tx).await.map_err(|_| Unavailable)?;
    tx.commit().await.map_err(|_| Unavailable)?;
    u64::try_from(next).map_err(|_| Invalid)
}
