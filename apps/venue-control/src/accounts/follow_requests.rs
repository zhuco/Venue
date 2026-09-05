use super::{AccountError, Principal, crypto, database_error, error, ms};
use sqlx::{Postgres, Row, Transaction};
use venue_control_protocol::{accounts::AccountErrorCode as Code, kol::FollowRelationSummary};

pub(super) async fn lock_scope(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    managed: Option<(&str, &str)>,
    require_enabled: bool,
    credential: Option<&str>,
    now: u64,
) -> Result<(), AccountError> {
    // Keep profile -> user -> relation -> credential order shared with planning.
    let kol: Option<String> = match managed {
        Some((owner, _)) => Some(owner.to_owned()),
        None => {
            sqlx::query_scalar("SELECT kol_user_id FROM venue_user_kol_bindings WHERE user_id=$1")
                .bind(&principal.user.user_id)
                .fetch_optional(&mut **tx)
                .await
                .map_err(database_error)?
        }
    };
    let kol = kol.ok_or(error(Code::Forbidden))?;
    let enabled: bool = sqlx::query_scalar(
        "SELECT profile_state='enabled' FROM venue_kol_profiles WHERE kol_user_id=$1 FOR SHARE",
    )
    .bind(&kol)
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)?
    .ok_or(error(Code::Forbidden))?;
    if require_enabled && !enabled {
        return Err(error(Code::Forbidden));
    }
    let login_enabled: bool =
        sqlx::query_scalar("SELECT login_enabled FROM venue_users WHERE user_id=$1 FOR UPDATE")
            .bind(&principal.user.user_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(database_error)?
            .ok_or(error(Code::NotFound))?;
    match managed {
        Some((owner, id)) => {
            if login_enabled {
                return Err(error(Code::Forbidden));
            }
            let row = sqlx::query("SELECT credential_id FROM venue_managed_credentials WHERE managed_id=$1 AND kol_user_id=$2 AND follower_user_id=$3 FOR SHARE")
                .bind(id).bind(owner).bind(&principal.user.user_id).fetch_optional(&mut **tx).await.map_err(database_error)?.ok_or(error(Code::NotFound))?;
            let owned: String = row.try_get("credential_id").map_err(database_error)?;
            if credential.is_some_and(|id| id != owned) {
                return Err(error(Code::Conflict));
            }
            if credential.is_some() {
                sqlx::query("INSERT INTO venue_user_kol_bindings(user_id,kol_user_id,managed_id,bound_ms) VALUES($1,$2,$3,$4) ON CONFLICT(user_id) DO NOTHING")
                    .bind(&principal.user.user_id).bind(owner).bind(id).bind(ms(now)?).execute(&mut **tx).await.map_err(database_error)?;
            }
            let matches: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_user_kol_bindings WHERE user_id=$1 AND kol_user_id=$2 AND managed_id=$3 AND invite_id IS NULL)")
                .bind(&principal.user.user_id).bind(owner).bind(id).fetch_one(&mut **tx).await.map_err(database_error)?;
            if !matches {
                return Err(error(Code::Conflict));
            }
        }
        None if !login_enabled => return Err(error(Code::Forbidden)),
        None => {}
    }
    Ok(())
}

pub(super) fn digest(
    action: &str,
    request: &impl serde::Serialize,
) -> Result<Vec<u8>, AccountError> {
    Ok(crypto::fingerprint(
        &serde_json::to_vec(&(action, request)).map_err(|_| error(Code::InvalidInput))?,
    ))
}

pub(super) async fn replay(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    actor: &str,
    request_id: &str,
    hash: &[u8],
) -> Result<Option<FollowRelationSummary>, AccountError> {
    let row = sqlx::query("SELECT actor_user_id,request_hash,response_json FROM venue_follow_requests WHERE follower_user_id=$1 AND request_id=$2")
        .bind(&principal.user.user_id).bind(request_id).fetch_optional(&mut **tx).await.map_err(database_error)?;
    let Some(row) = row else { return Ok(None) };
    if row
        .try_get::<String, _>("actor_user_id")
        .map_err(database_error)?
        != actor
        || row
            .try_get::<Vec<u8>, _>("request_hash")
            .map_err(database_error)?
            != hash
    {
        return Err(error(Code::Conflict));
    }
    Ok(Some(
        serde_json::from_value(row.try_get("response_json").map_err(database_error)?)
            .map_err(|_| error(Code::Unavailable))?,
    ))
}

pub(super) async fn save(
    tx: &mut Transaction<'_, Postgres>,
    principal: &Principal,
    actor: &str,
    request_id: &str,
    hash: &[u8],
    response: &FollowRelationSummary,
    now: u64,
) -> Result<(), AccountError> {
    sqlx::query("INSERT INTO venue_follow_requests(follower_user_id,request_id,actor_user_id,request_hash,response_json,created_ms) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(&principal.user.user_id).bind(request_id).bind(actor).bind(hash)
        .bind(serde_json::to_value(response).map_err(|_| error(Code::Unavailable))?).bind(ms(now)?)
        .execute(&mut **tx).await.map_err(database_error)?;
    Ok(())
}
