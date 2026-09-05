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
        let row = sqlx::query("SELECT encrypted_credentials FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL AND verification_json->>'verification'='verified'")
            .bind(credential_id).bind(owner_user_id).fetch_optional(&self.pool).await
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
