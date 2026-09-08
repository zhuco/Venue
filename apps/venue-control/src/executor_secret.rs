//! Restricted executor-only access to encrypted user credentials.

use secrecy::SecretString;
use sqlx::{PgPool, Row};
use venue_control_protocol::accounts::BindCredentialRequest;
use venue_gateway_binance::BinanceCredentials;

use crate::accounts::{CredentialCipher, credential_scope};

#[derive(Clone)]
pub struct ExecutorSecretProvider {
    pool: PgPool,
    cipher: std::sync::Arc<CredentialCipher>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutorSecretError {
    #[error("executor credential is unavailable")]
    Unavailable,
    #[error("executor credential ownership was rejected")]
    Forbidden,
}

impl ExecutorSecretProvider {
    pub(crate) fn new_shared(pool: PgPool, cipher: std::sync::Arc<CredentialCipher>) -> Self {
        Self { pool, cipher }
    }
    #[must_use]
    pub fn new(pool: PgPool, cipher: CredentialCipher) -> Self {
        Self {
            pool,
            cipher: std::sync::Arc::new(cipher),
        }
    }

    /// Decrypts one credential only after the durable owner relation and verification state
    /// still match. The returned adapter container has no Debug or serde implementation and
    /// clears secret material on drop.
    pub async fn load(
        &self,
        credential_id: &str,
        owner_user_id: &str,
    ) -> Result<BinanceCredentials, ExecutorSecretError> {
        self.load_matching(credential_id, owner_user_id, None).await
    }

    /// Manual opens combine account binding and credential verification in the decryption read.
    pub(crate) async fn load_bound(
        &self,
        credential_id: &str,
        owner_user_id: &str,
        trading_account_id: &str,
    ) -> Result<BinanceCredentials, ExecutorSecretError> {
        self.load_matching(credential_id, owner_user_id, Some(trading_account_id))
            .await
    }

    async fn load_matching(
        &self,
        credential_id: &str,
        owner_user_id: &str,
        trading_account_id: Option<&str>,
    ) -> Result<BinanceCredentials, ExecutorSecretError> {
        let row = sqlx::query("SELECT encrypted_credentials FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL AND verification_json->>'verification'='verified' AND ($3::text IS NULL OR trading_account_id=$3)")
            .bind(credential_id).bind(owner_user_id).bind(trading_account_id).fetch_optional(&self.pool).await
            .map_err(|_| ExecutorSecretError::Unavailable)?
            .ok_or(ExecutorSecretError::Forbidden)?;
        let envelope: Vec<u8> = row
            .try_get("encrypted_credentials")
            .map_err(|_| ExecutorSecretError::Unavailable)?;
        let payload = self
            .cipher
            .decrypt(&credential_scope(owner_user_id, credential_id), &envelope)
            .map_err(|_| ExecutorSecretError::Unavailable)?;
        let request: BindCredentialRequest =
            serde_json::from_slice(&payload).map_err(|_| ExecutorSecretError::Unavailable)?;
        BinanceCredentials::from_secrets(
            SecretString::from(request.api_key.expose().to_owned()),
            SecretString::from(request.api_secret.expose().to_owned()),
        )
        .map_err(|_| ExecutorSecretError::Unavailable)
    }
}
