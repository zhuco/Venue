use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use sqlx::Row;
use venue_control_protocol::accounts::{
    AccountErrorCode as Code, LoginRequest, SecretValue, SessionResponse, UserSummary,
};

const SESSION_LIFETIME_MS: u64 = 12 * 60 * 60 * 1_000;

impl AccountService {
    pub async fn register(
        &self,
        request: LoginRequest,
        now_ms: u64,
    ) -> Result<SessionResponse, AccountError> {
        let username = request
            .normalized_username()
            .ok_or(error(Code::InvalidInput))?;
        if !request.valid_password() {
            return Err(error(Code::InvalidInput));
        }
        self.rate_limit("registration", 10, now_ms).await?;
        let hash = self.password_hash(request.password).await?;
        let user = UserSummary {
            user_id: crypto::opaque_id()?,
            username,
        };
        let result = sqlx::query("INSERT INTO venue_users (user_id, username, password_hash, created_ms) VALUES ($1,$2,$3,$4) ON CONFLICT (username) DO NOTHING")
            .bind(&user.user_id).bind(&user.username).bind(hash).bind(ms(now_ms)?)
            .execute(&self.pool).await.map_err(database_error)?;
        if result.rows_affected() != 1 {
            return Err(error(Code::UsernameUnavailable));
        }
        self.create_session(user, now_ms).await
    }

    pub async fn login(
        &self,
        request: LoginRequest,
        now_ms: u64,
    ) -> Result<SessionResponse, AccountError> {
        let username = request
            .normalized_username()
            .ok_or(error(Code::InvalidLogin))?;
        if request.password.expose().len() > 512 {
            return Err(error(Code::InvalidLogin));
        }
        self.rate_limit("login-global", 60, now_ms).await?;
        self.rate_limit(&format!("login:{username}"), 10, now_ms)
            .await?;
        let row = sqlx::query(
            "SELECT user_id, password_hash FROM venue_users WHERE username=$1 AND login_enabled",
        )
        .bind(&username)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        let hash = row
            .as_ref()
            .map(|row| row.try_get::<String, _>("password_hash"))
            .transpose()
            .map_err(database_error)?
            .unwrap_or_else(|| self.dummy_hash.clone());
        let valid = self.password_verify(request.password, hash).await?;
        let row = row.filter(|_| valid).ok_or(error(Code::InvalidLogin))?;
        self.create_session(
            UserSummary {
                user_id: row.try_get("user_id").map_err(database_error)?,
                username,
            },
            now_ms,
        )
        .await
    }

    async fn create_session(
        &self,
        user: UserSummary,
        now_ms: u64,
    ) -> Result<SessionResponse, AccountError> {
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        sqlx::query("SELECT user_id FROM venue_users WHERE user_id=$1 FOR UPDATE")
            .bind(&user.user_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(database_error)?;
        let session = self
            .create_session_in_transaction(&mut tx, user, now_ms)
            .await?;
        tx.commit().await.map_err(database_error)?;
        Ok(session)
    }

    pub(super) async fn create_session_in_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        user: UserSummary,
        now_ms: u64,
    ) -> Result<SessionResponse, AccountError> {
        let token = crypto::new_token()?;
        let expires_ms = now_ms
            .checked_add(SESSION_LIFETIME_MS)
            .ok_or(error(Code::InvalidInput))?;
        sqlx::query("DELETE FROM venue_user_sessions WHERE expires_ms <= $1 OR token_hash IN (SELECT token_hash FROM venue_user_sessions WHERE user_id=$2 ORDER BY expires_ms DESC OFFSET 9)")
            .bind(ms(now_ms)?).bind(&user.user_id).execute(&mut **tx).await.map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_user_sessions (token_hash,user_id,expires_ms) VALUES ($1,$2,$3)",
        )
        .bind(crypto::fingerprint(token.expose().as_bytes()))
        .bind(&user.user_id)
        .bind(ms(expires_ms)?)
        .execute(&mut **tx)
        .await
        .map_err(database_error)?;
        Ok(SessionResponse {
            user,
            token,
            expires_ms,
        })
    }

    pub async fn authenticate(&self, token: &str, now_ms: u64) -> Result<Principal, AccountError> {
        if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(error(Code::Unauthorized));
        }
        let token_hash = crypto::fingerprint(token.as_bytes());
        let row = sqlx::query("SELECT u.user_id,u.username,s.selected_credential_id FROM venue_user_sessions s JOIN venue_users u USING(user_id) WHERE s.token_hash=$1 AND s.expires_ms>$2 AND u.login_enabled")
            .bind(&token_hash).bind(ms(now_ms)?).fetch_optional(&self.pool).await.map_err(database_error)?
            .ok_or(error(Code::Unauthorized))?;
        Ok(Principal {
            user: UserSummary {
                user_id: row.try_get("user_id").map_err(database_error)?,
                username: row.try_get("username").map_err(database_error)?,
            },
            token_hash,
            selected_credential_id: row
                .try_get("selected_credential_id")
                .map_err(database_error)?,
        })
    }

    pub async fn logout(&self, principal: &Principal) -> Result<(), AccountError> {
        sqlx::query("DELETE FROM venue_user_sessions WHERE token_hash=$1")
            .bind(&principal.token_hash)
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        Ok(())
    }

    pub(super) async fn confirm_password(
        &self,
        principal: &Principal,
        password: SecretValue,
        now_ms: u64,
    ) -> Result<(), AccountError> {
        if password.expose().len() > 512 {
            return Err(error(Code::InvalidLogin));
        }
        self.rate_limit(&format!("confirm:{}", principal.user.user_id), 10, now_ms)
            .await?;
        let hash: String =
            sqlx::query_scalar("SELECT password_hash FROM venue_users WHERE user_id=$1")
                .bind(&principal.user.user_id)
                .fetch_one(&self.pool)
                .await
                .map_err(database_error)?;
        if self.password_verify(password, hash).await? {
            Ok(())
        } else {
            Err(error(Code::InvalidLogin))
        }
    }

    pub(super) async fn password_hash(
        &self,
        password: SecretValue,
    ) -> Result<String, AccountError> {
        let permit = self
            .password_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| error(Code::RateLimited))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            crypto::hash_password(&password)
        })
        .await
        .map_err(|_| error(Code::Unavailable))?
    }

    async fn password_verify(
        &self,
        password: SecretValue,
        hash: String,
    ) -> Result<bool, AccountError> {
        let permit = self
            .password_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| error(Code::RateLimited))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            crypto::verify_password(&password, &hash)
        })
        .await
        .map_err(|_| error(Code::Unavailable))?
    }

    pub(super) async fn rate_limit(
        &self,
        bucket: &str,
        limit: i32,
        now_ms: u64,
    ) -> Result<(), AccountError> {
        let now = ms(now_ms)?;
        sqlx::query("DELETE FROM venue_account_rate_limits WHERE window_ms < $1")
            .bind(now.saturating_sub(120_000))
            .execute(&self.pool)
            .await
            .map_err(database_error)?;
        let attempts: i32 = sqlx::query_scalar("INSERT INTO venue_account_rate_limits (bucket,window_ms,attempts) VALUES ($1,$2,1) ON CONFLICT (bucket) DO UPDATE SET window_ms=CASE WHEN venue_account_rate_limits.window_ms <= $2-60000 THEN $2 ELSE venue_account_rate_limits.window_ms END, attempts=CASE WHEN venue_account_rate_limits.window_ms <= $2-60000 THEN 1 ELSE LEAST(venue_account_rate_limits.attempts+1,10000) END RETURNING attempts")
            .bind(bucket).bind(now).fetch_one(&self.pool).await.map_err(database_error)?;
        if attempts > limit {
            Err(error(Code::RateLimited))
        } else {
            Ok(())
        }
    }
}
