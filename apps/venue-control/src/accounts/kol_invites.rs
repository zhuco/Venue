use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use sqlx::{Row, postgres::PgRow};
use venue_control_protocol::{accounts::AccountErrorCode as Code, kol_invites::*};

impl AccountService {
    pub async fn own_kol_invite(
        &self,
        principal: &Principal,
        now: u64,
    ) -> Result<Option<KolInviteSummary>, AccountError> {
        self.own_kol_profile(principal).await?;
        let row = sqlx::query(
            "SELECT * FROM venue_kol_invites WHERE kol_user_id=$1 AND invite_state='active'",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        row.as_ref()
            .map(|row| self.invite_summary(principal, row, now))
            .transpose()
    }

    pub async fn create_kol_invite(
        &self,
        principal: &Principal,
        request: KolInviteCreateRequest,
        now: u64,
    ) -> Result<KolInviteSummary, AccountError> {
        if !request.valid() {
            return Err(error(Code::InvalidInput));
        }
        self.rate_limit(&format!("kol-invite:{}", principal.user.user_id), 10, now)
            .await?;
        let request_hash = super::follow_requests::digest("kol-invite", &request)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let state: String = sqlx::query_scalar(
            "SELECT profile_state FROM venue_kol_profiles WHERE kol_user_id=$1 FOR UPDATE",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::Forbidden))?;
        if state != "enabled" {
            return Err(error(Code::Forbidden));
        }
        if let Some(row) =
            sqlx::query("SELECT * FROM venue_kol_invites WHERE invite_id=$1 AND kol_user_id=$2")
                .bind(&request.request_id)
                .bind(&principal.user.user_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(database_error)?
        {
            if row
                .try_get::<Option<Vec<u8>>, _>("request_hash")
                .map_err(database_error)?
                .as_ref()
                != Some(&request_hash)
            {
                return Err(error(Code::Conflict));
            }
            return self.invite_summary(principal, &row, now);
        }
        let current: Option<String> = sqlx::query_scalar("SELECT invite_id FROM venue_kol_invites WHERE kol_user_id=$1 AND invite_state='active'")
            .bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?;
        if current != request.expected_invite_id {
            return Err(error(Code::Conflict));
        }
        let code = match &request.invite_code {
            Some(code) => {
                venue_control_protocol::accounts::SecretValue::new(code.trim().to_owned())
            }
            None => new_invite_code()?,
        };
        let scope = format!(
            "kol-invite:{}:{}",
            principal.user.user_id, request.request_id
        );
        let envelope = self.cipher.encrypt(&scope, code.expose().as_bytes())?;
        sqlx::query("UPDATE venue_kol_invites SET invite_state='disabled',disabled_ms=$2,revision=revision+1 WHERE kol_user_id=$1 AND invite_state='active'")
            .bind(&principal.user.user_id).bind(ms(now)?).execute(&mut *tx).await.map_err(database_error)?;
        let row = sqlx::query("INSERT INTO venue_kol_invites(invite_id,kol_user_id,code_hash,invite_state,created_ms,code_envelope,replaced_invite_id,request_hash) VALUES($1,$2,$3,'active',$4,$5,$6,$7) RETURNING *")
            .bind(&request.request_id).bind(&principal.user.user_id).bind(crypto::fingerprint(code.expose().as_bytes())).bind(ms(now)?).bind(envelope).bind(&request.expected_invite_id).bind(request_hash)
            .fetch_one(&mut *tx).await.map_err(|cause| {
                if cause.as_database_error().is_some_and(|db| db.is_unique_violation()) { error(Code::Conflict) } else { database_error(cause) }
            })?;
        let summary = self.invite_summary(principal, &row, now)?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }

    fn invite_summary(
        &self,
        principal: &Principal,
        row: &PgRow,
        now: u64,
    ) -> Result<KolInviteSummary, AccountError> {
        let invite_id: String = row.try_get("invite_id").map_err(database_error)?;
        let envelope: Option<Vec<u8>> = row.try_get("code_envelope").map_err(database_error)?;
        let invite_code = envelope
            .map(|bytes| {
                let plain = self.cipher.decrypt(
                    &format!("kol-invite:{}:{}", principal.user.user_id, invite_id),
                    &bytes,
                )?;
                let code = std::str::from_utf8(&plain).map_err(|_| error(Code::Unavailable))?;
                let hash: Vec<u8> = row.try_get("code_hash").map_err(database_error)?;
                if crypto::fingerprint(code.as_bytes()) != hash {
                    return Err(error(Code::Unavailable));
                }
                Ok(code.to_owned())
            })
            .transpose()?;
        let now = ms(now)?;
        let expires: Option<i64> = row.try_get("expires_ms").map_err(database_error)?;
        Ok(KolInviteSummary {
            invite_id,
            invite_code,
            active: row
                .try_get::<String, _>("invite_state")
                .map_err(database_error)?
                == "active"
                && expires.is_none_or(|expires| expires > now),
            created_ms: u64::try_from(
                row.try_get::<i64, _>("created_ms")
                    .map_err(database_error)?,
            )
            .map_err(|_| error(Code::Unavailable))?,
        })
    }
}

fn new_invite_code() -> Result<venue_control_protocol::accounts::SecretValue, AccountError> {
    const ALPHABET: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut code = String::with_capacity(6);
    while code.len() < 6 {
        // Reject the incomplete range so every character has equal probability.
        for byte in crypto::random::<16>()? {
            if byte < 248 {
                code.push(char::from(ALPHABET[usize::from(byte % 62)]));
                if code.len() == 6 {
                    break;
                }
            }
        }
    }
    Ok(venue_control_protocol::accounts::SecretValue::new(code))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::test_support::{Fixture, login, now};
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn generated_invites_are_six_alphanumeric_characters() -> TestResult {
        for _ in 0..128 {
            let code = new_invite_code()?;
            assert_eq!(code.expose().len(), 6);
            assert!(
                code.expose()
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric())
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn invites_are_owned_encrypted_idempotent_and_rotate_without_rebinding() -> TestResult {
        let Some(f) = Fixture::create().await? else {
            return Ok(());
        };
        let time = now();
        let session = f.service.register(login("invite-kol"), time).await?;
        let owner = f.service.authenticate(session.token.expose(), time).await?;
        let account = crypto::opaque_id()?;
        sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)")
            .bind(&account).bind(&owner.user.user_id).bind(vec![7_u8;32]).execute(&f.pool).await?;
        sqlx::query("INSERT INTO venue_kol_profiles(kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES($1,$2,'KOL','Title','','100','enabled',1,$3,$3)")
            .bind(&owner.user.user_id).bind(account).bind(ms(time)?).execute(&f.pool).await?;
        assert!(f.service.own_kol_invite(&owner, time).await?.is_none());
        let request = KolInviteCreateRequest {
            request_id: crypto::opaque_id()?,
            expected_invite_id: None,
            invite_code: None,
        };
        let first = f
            .service
            .create_kol_invite(&owner, request.clone(), time)
            .await?;
        let code = first.invite_code.as_deref().ok_or("missing code")?;
        let encrypted: Vec<u8> =
            sqlx::query_scalar("SELECT code_envelope FROM venue_kol_invites WHERE invite_id=$1")
                .bind(&first.invite_id)
                .fetch_one(&f.pool)
                .await?;
        assert!(
            !encrypted
                .windows(code.len())
                .any(|value| value == code.as_bytes())
        );
        assert_eq!(
            f.service
                .own_kol_invite(&owner, time)
                .await?
                .ok_or("missing invite")?
                .invite_code,
            first.invite_code
        );
        assert_eq!(
            f.service
                .create_kol_invite(&owner, request.clone(), time)
                .await?
                .invite_code,
            first.invite_code
        );
        let follower = f
            .service
            .register_with_invite(
                venue_control_protocol::accounts::RegisterRequest {
                    username: "new-follower".into(),
                    password: venue_control_protocol::accounts::SecretValue::new(
                        "fixture-password".into(),
                    ),
                    invite_code: code.into(),
                },
                time,
            )
            .await?;
        let other = f
            .service
            .authenticate(follower.token.expose(), time)
            .await?;
        assert!(f.service.own_kol_invite(&other, time).await.is_err());
        assert!(
            f.service
                .create_kol_invite(&other, request.clone(), time)
                .await
                .is_err()
        );
        assert!(
            f.service
                .create_kol_invite(
                    &owner,
                    KolInviteCreateRequest {
                        request_id: crypto::opaque_id()?,
                        expected_invite_id: None,
                        invite_code: None
                    },
                    time
                )
                .await
                .is_err()
        );
        let next = f
            .service
            .create_kol_invite(
                &owner,
                KolInviteCreateRequest {
                    request_id: crypto::opaque_id()?,
                    expected_invite_id: Some(first.invite_id.clone()),
                    invite_code: Some("Ab12".into()),
                },
                time + 1,
            )
            .await?;
        assert!(next.active);
        assert!(f.service.resolve_invite(code, time + 1).await.is_err());
        assert!(
            f.service
                .resolve_invite(
                    next.invite_code.as_deref().ok_or("missing next code")?,
                    time + 1
                )
                .await
                .is_ok()
        );
        assert!(
            !f.service
                .create_kol_invite(&owner, request, time + 1)
                .await?
                .active
        );
        let bound: String =
            sqlx::query_scalar("SELECT kol_user_id FROM venue_user_kol_bindings WHERE user_id=$1")
                .bind(&other.user.user_id)
                .fetch_one(&f.pool)
                .await?;
        assert_eq!(bound, owner.user.user_id);
        let other_session = f.service.register(login("second-kol"), time).await?;
        let other_kol = f
            .service
            .authenticate(other_session.token.expose(), time)
            .await?;
        let other_account = crypto::opaque_id()?;
        sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)")
            .bind(&other_account).bind(&other_kol.user.user_id).bind(vec![8_u8;32]).execute(&f.pool).await?;
        sqlx::query("INSERT INTO venue_kol_profiles(kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES($1,$2,'KOL2','Title','','100','enabled',2,$3,$3)")
            .bind(&other_kol.user.user_id).bind(other_account).bind(ms(time)?).execute(&f.pool).await?;
        let duplicate = KolInviteCreateRequest {
            request_id: crypto::opaque_id()?,
            expected_invite_id: None,
            invite_code: next.invite_code.clone(),
        };
        assert!(matches!(
            f.service
                .create_kol_invite(&other_kol, duplicate, time + 2)
                .await,
            Err(AccountError {
                code: Code::Conflict
            })
        ));
        assert!(
            f.service
                .own_kol_invite(&other_kol, time + 2)
                .await?
                .is_none()
        );
        let left = KolInviteCreateRequest {
            request_id: crypto::opaque_id()?,
            expected_invite_id: Some(next.invite_id.clone()),
            invite_code: Some("RACE2026".into()),
        };
        let right = KolInviteCreateRequest {
            request_id: crypto::opaque_id()?,
            expected_invite_id: None,
            invite_code: Some("RACE2026".into()),
        };
        let (left_result, right_result) = tokio::join!(
            f.service.create_kol_invite(&owner, left, time + 3),
            f.service.create_kol_invite(&other_kol, right, time + 3)
        );
        assert_ne!(left_result.is_ok(), right_result.is_ok());
        if left_result.is_err() {
            assert_eq!(
                f.service
                    .own_kol_invite(&owner, time + 3)
                    .await?
                    .ok_or("lost old invitation")?
                    .invite_id,
                next.invite_id
            );
        }
        f.cleanup().await?;
        Ok(())
    }
}
