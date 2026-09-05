//! User authentication and credential administration, isolated from semantic command storage.
//! Verification only reads exchange state; no method grants a writer or sends an order.

mod credentials;
mod crypto;
mod follow_requests;
mod grid;
mod kol;
mod leader_bot;
mod managed_followers;
mod session;
mod terminal;
#[cfg(test)]
pub(crate) mod test_support;

use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Semaphore;
use venue_control_protocol::accounts::{AccountErrorCode, SecretValue, UserSummary};

pub(crate) use credentials::credential_scope;
pub use crypto::CredentialCipher;
pub const MIGRATION_0015: &str = include_str!("../../migrations/0015_accounts.sql");

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("account operation failed: {code:?}")]
pub struct AccountError {
    pub code: AccountErrorCode,
}

fn error(code: AccountErrorCode) -> AccountError {
    AccountError { code }
}
fn database_error(_: sqlx::Error) -> AccountError {
    error(AccountErrorCode::Unavailable)
}
fn ms(value: u64) -> Result<i64, AccountError> {
    i64::try_from(value).map_err(|_| error(AccountErrorCode::InvalidInput))
}

pub struct AccountService {
    pool: PgPool,
    cipher: CredentialCipher,
    password_slots: Arc<Semaphore>,
    dummy_hash: String,
    node_token_hash: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct Principal {
    pub user: UserSummary,
    token_hash: Vec<u8>,
    pub selected_credential_id: Option<String>,
}

impl AccountService {
    pub fn new(pool: PgPool, cipher: CredentialCipher) -> Result<Self, AccountError> {
        let node_token = std::env::var("VENUE_CONTROL_NODE_TOKEN")
            .ok()
            .map(SecretValue::new);
        Self::new_with_node_token(pool, cipher, node_token)
    }

    /// Production startup uses [`Self::new`] so the Node credential remains environment-sourced.
    /// This explicit boundary lets integration tests inject an isolated token without mutating
    /// process-wide environment state.
    pub fn new_with_node_token(
        pool: PgPool,
        cipher: CredentialCipher,
        node_token: Option<SecretValue>,
    ) -> Result<Self, AccountError> {
        Ok(Self {
            pool,
            cipher,
            password_slots: Arc::new(Semaphore::new(2)),
            node_token_hash: node_token
                .filter(|t| t.expose().len() >= 32)
                .map(|t| crypto::fingerprint(t.expose().as_bytes())),
            dummy_hash: crypto::hash_password(&SecretValue::new(
                "unavailable account dummy password".into(),
            ))?,
        })
    }

    pub fn node_authorized(&self, token: &str) -> bool {
        self.node_token_hash
            .as_ref()
            .is_some_and(|expected| &crypto::fingerprint(token.as_bytes()) == expected)
    }
}
