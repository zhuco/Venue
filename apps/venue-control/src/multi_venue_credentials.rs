use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use venue_control_protocol::accounts::{ApiVerificationState, CredentialSummary};
use venue_domain::domain::Symbol;
use venue_execution::{
    SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact,
};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use zeroize::Zeroizing;

use crate::accounts::{CredentialCipher, credential_scope};
use crate::multi_venue_exchange::{StrategyCredentials, StrategyExchangeError, StrategyGateway};

#[derive(Clone)]
pub struct StrategyCredentialStore {
    pool: PgPool,
    cipher: std::sync::Arc<CredentialCipher>,
}

/// Sanitized, read-only admission evidence. Fills are counted rather than emitted so an
/// operator probe cannot accidentally dump a large private history to the terminal.
#[derive(serde::Serialize)]
pub struct StrategyAccountProbe {
    pub venue: VenueId,
    pub identity_sha256: String,
    pub observed_at_ms: u64,
    pub position_mode: SignedAccountPositionMode,
    pub open_orders: Vec<SignedAccountOrderFact>,
    pub positions: Vec<SignedAccountPositionFact>,
    pub balances: Vec<SignedAccountBalance>,
    pub fill_count: usize,
    pub unknown_results: Vec<SignedUnknownFact>,
}

#[derive(Debug, thiserror::Error)]
pub enum StrategyProbeError {
    #[error("strategy probe binding rejected")]
    Binding,
    #[error("strategy probe network slot unavailable")]
    NetworkSlot,
    #[error("strategy probe gateway connection rejected: {0}")]
    Connect(String),
    #[error("strategy probe signed identity rejected")]
    Identity,
    #[error("strategy probe permissions rejected")]
    Permissions,
    #[error("strategy probe complete snapshot rejected")]
    Snapshot,
    #[error("strategy probe exact order observation rejected: {0}")]
    Observation(String),
    #[error("strategy probe worker unavailable")]
    Worker,
}

pub(crate) fn identity_hash(venue: VenueId, identity: &str) -> Vec<u8> {
    let mut hash = Sha256::new();
    hash.update(venue.as_str().as_bytes());
    hash.update([0]);
    if venue == VenueId::Hyperliquid {
        // Ethereum address casing cannot create a second nonce owner for the same API wallet.
        hash.update(identity.to_ascii_lowercase().as_bytes());
    } else {
        hash.update(identity.as_bytes());
    }
    hash.finalize().to_vec()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StrategyCredentialBindError {
    #[error("strategy exchange is unavailable or rejected its signed account contract")]
    Exchange,
    #[error("strategy account is already in use")]
    AccountInUse,
    #[error("strategy exchange identity belongs to another owner")]
    IdentityConflict,
}

enum BindFailure {
    Exchange(StrategyExchangeError),
    AccountInUse,
    IdentityConflict,
}

impl From<StrategyExchangeError> for BindFailure {
    fn from(error: StrategyExchangeError) -> Self {
        Self::Exchange(error)
    }
}

impl From<BindFailure> for StrategyCredentialBindError {
    fn from(error: BindFailure) -> Self {
        match error {
            BindFailure::Exchange(_) => Self::Exchange,
            BindFailure::AccountInUse => Self::AccountInUse,
            BindFailure::IdentityConflict => Self::IdentityConflict,
        }
    }
}

async fn resolve_identity_account(
    tx: &mut Transaction<'_, Postgres>,
    owner: &str,
    requested_account: &str,
    venue: VenueId,
    identity: &[u8],
) -> Result<String, BindFailure> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('venue-strategy-identity:' || $1 || ':' || encode($2,'hex'),0))")
        .bind(venue.as_str())
        .bind(identity)
        .execute(&mut **tx)
        .await
        .map_err(|_| StrategyExchangeError)?;
    let existing = sqlx::query(
        "SELECT trading_account_id,user_id FROM venue_user_trading_accounts WHERE venue=$1 AND exchange_identity_hash=$2 FOR UPDATE",
    )
    .bind(venue.as_str())
    .bind(identity)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| StrategyExchangeError)?;
    let canonical = if let Some(existing) = existing {
        let existing_owner: String = existing
            .try_get("user_id")
            .map_err(|_| StrategyExchangeError)?;
        if existing_owner != owner {
            return Err(BindFailure::IdentityConflict);
        }
        existing
            .try_get("trading_account_id")
            .map_err(|_| StrategyExchangeError)?
    } else {
        sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,$3,$4)")
            .bind(requested_account)
            .bind(owner)
            .bind(venue.as_str())
            .bind(identity)
            .execute(&mut **tx)
            .await
            .map_err(|_| StrategyExchangeError)?;
        requested_account.to_owned()
    };
    Ok(canonical)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(DIGITS[usize::from(byte >> 4)]));
        value.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    value
}

async fn verified_snapshot(
    account: &str,
    symbol: Symbol,
    credentials: StrategyCredentials,
) -> Result<(VenueId, String, SignedAccountSnapshot), StrategyProbeError> {
    let venue = credentials.venue();
    let binding = GatewayBinding::new(venue, GatewayMode::Live, account, symbol)
        .map_err(|_| StrategyProbeError::Binding)?;
    let _slot = crate::multi_venue_runtime::ACCOUNT_NETWORK_SLOTS
        .acquire()
        .await
        .map_err(|_| StrategyProbeError::NetworkSlot)?;
    tokio::task::spawn_blocking(move || {
        let mut gateway = StrategyGateway::connect_detailed(binding.clone(), credentials, 1)
            .map_err(StrategyProbeError::Connect)?;
        let identity = gateway
            .identity()
            .map_err(|_| StrategyProbeError::Identity)?;
        gateway
            .verify_permissions()
            .map_err(|_| StrategyProbeError::Permissions)?;
        gateway
            .verify_admission_symbol()
            .map_err(|_| StrategyProbeError::Permissions)?;
        let snapshot = gateway
            .snapshot(&binding)
            .map_err(|_| StrategyProbeError::Snapshot)?;
        Ok::<_, StrategyProbeError>((venue, identity, snapshot))
    })
    .await
    .map_err(|_| StrategyProbeError::Worker)?
}

/// Performs the exact signed read and permission checks used by `bind`, without writing the
/// database or granting command authority.
pub async fn probe_strategy_account(
    account: &str,
    symbol: Symbol,
    credentials: StrategyCredentials,
) -> Result<StrategyAccountProbe, StrategyProbeError> {
    let (venue, identity, snapshot) = verified_snapshot(account, symbol, credentials).await?;
    Ok(StrategyAccountProbe {
        venue,
        identity_sha256: hex(&identity_hash(venue, &identity)),
        observed_at_ms: snapshot.observed_at_ms(),
        position_mode: snapshot.position_mode(),
        open_orders: snapshot.open_orders().to_vec(),
        positions: snapshot.positions().to_vec(),
        balances: snapshot.balances().to_vec(),
        fill_count: snapshot.fills().len(),
        unknown_results: snapshot.unknown_results().to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bitget_identity_resolution_reuses_owner_and_serializes_concurrent_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let Some(fixture) = crate::accounts::test_support::Fixture::create().await? else {
            return Ok(());
        };
        sqlx::query("INSERT INTO venue_users(user_id,username,password_hash,created_ms) VALUES('owner1','bitget_owner1','fixture',1),('owner2','bitget_owner2','fixture',1)")
            .execute(&fixture.pool).await?;
        let hash = identity_hash(VenueId::Bitget, "signed-fixture-uid");
        let mut first = fixture.pool.begin().await?;
        let canonical =
            resolve_identity_account(&mut first, "owner1", "account1", VenueId::Bitget, &hash)
                .await
                .map_err(|_| "first resolution failed")?;
        assert_eq!(canonical, "account1");
        let concurrent_pool = fixture.pool.clone();
        let concurrent_hash = hash.clone();
        let second = tokio::spawn(async move {
            let mut tx = concurrent_pool.begin().await.map_err(|_| "begin failed")?;
            let account = resolve_identity_account(
                &mut tx,
                "owner1",
                "account2",
                VenueId::Bitget,
                &concurrent_hash,
            )
            .await
            .map_err(|_| "second resolution failed")?;
            tx.commit().await.map_err(|_| "commit failed")?;
            Ok::<_, &'static str>(account)
        });
        first.commit().await?;
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), second).await???,
            "account1"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_user_trading_accounts WHERE venue='bitget' AND exchange_identity_hash=$1")
            .bind(&hash).fetch_one(&fixture.pool).await?;
        assert_eq!(count, 1);
        let mut other_owner = fixture.pool.begin().await?;
        assert!(matches!(
            resolve_identity_account(
                &mut other_owner,
                "owner2",
                "account3",
                VenueId::Bitget,
                &hash
            )
            .await,
            Err(BindFailure::IdentityConflict)
        ));
        other_owner.rollback().await?;
        fixture.cleanup().await?;
        Ok(())
    }

    #[test]
    fn wallet_identity_is_case_insensitive_but_exchange_keys_are_not() {
        assert_eq!(
            identity_hash(VenueId::Hyperliquid, "0xAbCd"),
            identity_hash(VenueId::Hyperliquid, "0xabcd")
        );
        assert_ne!(
            identity_hash(VenueId::Bybit, "AbCd"),
            identity_hash(VenueId::Bybit, "abcd")
        );
    }
}

impl StrategyCredentialStore {
    /// Read-only exact client-ID observation for an already durable command. This never claims,
    /// sends or retries the command and is suitable for operator reconciliation evidence.
    pub async fn order_observation(
        &self,
        owner: &str,
        credential: &str,
        command: venue_domain::ExecutionCommand,
    ) -> Result<Option<venue_execution::DurableOrderObservation>, StrategyProbeError> {
        let account: String = sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL")
            .bind(credential).bind(owner).fetch_one(&self.pool).await.map_err(|_| StrategyProbeError::Binding)?;
        if command.mutation_owner().account != account {
            return Err(StrategyProbeError::Binding);
        }
        let (credentials, expected) = self
            .load(owner, credential, &account, false)
            .await
            .map_err(|_| StrategyProbeError::Permissions)?;
        let binding = GatewayBinding::new(
            credentials.venue(),
            GatewayMode::Live,
            account,
            command.mutation_owner().symbol.clone(),
        )
        .map_err(|_| StrategyProbeError::Binding)?;
        let _slot = crate::multi_venue_runtime::ACCOUNT_NETWORK_SLOTS
            .acquire()
            .await
            .map_err(|_| StrategyProbeError::NetworkSlot)?;
        tokio::task::spawn_blocking(move || {
            let mut gateway = StrategyGateway::connect_detailed(binding.clone(), credentials, 1)
                .map_err(StrategyProbeError::Connect)?;
            let identity = gateway
                .identity()
                .map_err(|_| StrategyProbeError::Identity)?;
            if identity_hash(binding.venue, &identity) != expected {
                return Err(StrategyProbeError::Identity);
            }
            gateway
                .order_observation_detailed(&command)
                .map_err(StrategyProbeError::Observation)
        })
        .await
        .map_err(|_| StrategyProbeError::Worker)?
    }

    /// Reads one bounded Bybit linear funding-settlement window. The adapter owns pagination,
    /// signed scope checks and duplicate rejection; no cross-venue funding semantics are guessed.
    pub async fn bybit_funding(
        &self,
        owner: &str,
        credential: &str,
        symbol: Symbol,
        query: venue_gateway_bybit::BybitFundingQuery,
    ) -> Result<venue_gateway_bybit::BybitFundingReadback, StrategyProbeError> {
        let account: String = sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL")
            .bind(credential).bind(owner).fetch_one(&self.pool).await.map_err(|_| StrategyProbeError::Binding)?;
        let (credentials, expected) = self
            .load(owner, credential, &account, false)
            .await
            .map_err(|_| StrategyProbeError::Permissions)?;
        if credentials.venue() != VenueId::Bybit {
            return Err(StrategyProbeError::Binding);
        }
        let binding = GatewayBinding::new(VenueId::Bybit, GatewayMode::Live, account, symbol)
            .map_err(|_| StrategyProbeError::Binding)?;
        let _slot = crate::multi_venue_runtime::ACCOUNT_NETWORK_SLOTS
            .acquire()
            .await
            .map_err(|_| StrategyProbeError::NetworkSlot)?;
        tokio::task::spawn_blocking(move || {
            let mut gateway = StrategyGateway::connect_detailed(binding.clone(), credentials, 1)
                .map_err(StrategyProbeError::Connect)?;
            let identity = gateway
                .identity()
                .map_err(|_| StrategyProbeError::Identity)?;
            if identity_hash(binding.venue, &identity) != expected {
                return Err(StrategyProbeError::Identity);
            }
            gateway
                .bybit_funding_detailed(&query)
                .map_err(StrategyProbeError::Observation)
        })
        .await
        .map_err(|_| StrategyProbeError::Worker)?
    }

    pub async fn set_limits(
        &self,
        owner: &str,
        credential: &str,
        limits: crate::multi_venue_risk::StrategyRiskLimits,
    ) -> Result<(), StrategyExchangeError> {
        if !limits.validate() {
            return Err(StrategyExchangeError);
        }
        let changed = sqlx::query("UPDATE venue_api_credentials SET strategy_limits=$1 WHERE credential_id=$2 AND user_id=$3 AND deleted_ms IS NULL AND verification_json->>'strategy_execution'='true'")
            .bind(serde_json::to_value(limits).map_err(|_| StrategyExchangeError)?).bind(credential).bind(owner)
            .execute(&self.pool).await.map_err(|_| StrategyExchangeError)?;
        if changed.rows_affected() != 1 {
            return Err(StrategyExchangeError);
        }
        Ok(())
    }

    pub(crate) async fn limits(
        &self,
        credential: &str,
    ) -> Result<Option<crate::multi_venue_risk::StrategyRiskLimits>, StrategyExchangeError> {
        let value: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT strategy_limits FROM venue_api_credentials WHERE credential_id=$1",
        )
        .bind(credential)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| StrategyExchangeError)?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| StrategyExchangeError)
    }

    pub(crate) async fn grid_facts(
        &self,
        owner: &str,
        credential: &str,
        symbol: Symbol,
        commands: Vec<venue_domain::ExecutionCommand>,
    ) -> Result<
        (
            venue_execution::SignedAccountSnapshot,
            venue_execution::DurableMarketFacts,
            Vec<venue_execution::DurableOrderObservation>,
        ),
        StrategyExchangeError,
    > {
        let account: String = sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL")
            .bind(credential).bind(owner).fetch_one(&self.pool).await.map_err(|_| StrategyExchangeError)?;
        let (credentials, expected) = self.load(owner, credential, &account, false).await?;
        let binding = GatewayBinding::new(credentials.venue(), GatewayMode::Live, account, symbol)
            .map_err(|_| StrategyExchangeError)?;
        let _slot = crate::multi_venue_runtime::ACCOUNT_NETWORK_SLOTS
            .acquire()
            .await
            .map_err(|_| StrategyExchangeError)?;
        tokio::task::spawn_blocking(move || {
            let mut gateway = StrategyGateway::connect(binding.clone(), credentials, 1)?;
            if identity_hash(binding.venue, &gateway.identity()?) != expected {
                return Err(StrategyExchangeError);
            }
            let mut observations = Vec::new();
            for command in &commands {
                let observation = gateway
                    .order_observation(command)?
                    .ok_or(StrategyExchangeError)?;
                observations.push(observation);
            }
            let market = gateway.market_facts()?;
            Ok((gateway.snapshot(&binding)?, market, observations))
        })
        .await
        .map_err(|_| StrategyExchangeError)?
    }
    pub fn new(pool: PgPool, cipher: CredentialCipher) -> Self {
        Self {
            pool,
            cipher: std::sync::Arc::new(cipher),
        }
    }

    pub(crate) fn new_shared(pool: PgPool, cipher: std::sync::Arc<CredentialCipher>) -> Self {
        Self { pool, cipher }
    }

    /// Rechecks the saved LIVE credential, native identity, current permissions and complete
    /// signed account facts without persisting a snapshot or granting command authority.
    pub(crate) async fn preflight_snapshot(
        &self,
        owner: &str,
        credential: &str,
        symbols: Vec<Symbol>,
    ) -> Result<venue_execution::SignedAccountSnapshot, StrategyExchangeError> {
        let symbol = symbols.first().cloned().ok_or(StrategyExchangeError)?;
        let account: String = sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL")
            .bind(credential).bind(owner).fetch_one(&self.pool).await.map_err(|_| StrategyExchangeError)?;
        let (credentials, expected) = self.load(owner, credential, &account, false).await?;
        let binding = GatewayBinding::new(credentials.venue(), GatewayMode::Live, account, symbol)
            .map_err(|_| StrategyExchangeError)?;
        let _slot = crate::multi_venue_runtime::ACCOUNT_NETWORK_SLOTS
            .acquire()
            .await
            .map_err(|_| StrategyExchangeError)?;
        tokio::task::spawn_blocking(move || {
            let mut gateway = StrategyGateway::connect(binding.clone(), credentials, 1)?;
            if identity_hash(binding.venue, &gateway.identity()?) != expected {
                return Err(StrategyExchangeError);
            }
            gateway.verify_permissions()?;
            gateway.verify_strategy_symbols(&symbols)?;
            gateway.snapshot(&binding)
        })
        .await
        .map_err(|_| StrategyExchangeError)?
    }

    /// Refreshes the same signed normalized facts consumed by an independently running strategy.
    /// This path is read-only at the exchange and never submits an order.
    pub async fn snapshot(
        &self,
        owner: &str,
        credential: &str,
        symbol: Symbol,
    ) -> Result<venue_execution::SignedAccountSnapshot, StrategyExchangeError> {
        let account: String = sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL")
            .bind(credential).bind(owner).fetch_one(&self.pool).await.map_err(|_| StrategyExchangeError)?;
        let (credentials, expected) = self.load(owner, credential, &account, false).await?;
        let binding = GatewayBinding::new(credentials.venue(), GatewayMode::Live, account, symbol)
            .map_err(|_| StrategyExchangeError)?;
        let _slot = crate::multi_venue_runtime::ACCOUNT_NETWORK_SLOTS
            .acquire()
            .await
            .map_err(|_| StrategyExchangeError)?;
        let snapshot = tokio::task::spawn_blocking(move || {
            let mut gateway = StrategyGateway::connect(binding.clone(), credentials, 1)?;
            if identity_hash(binding.venue, &gateway.identity()?) != expected {
                return Err(StrategyExchangeError);
            }
            gateway.snapshot(&binding)
        })
        .await
        .map_err(|_| StrategyExchangeError)??;
        sqlx::query("UPDATE venue_api_credentials SET strategy_snapshot=$1 WHERE credential_id=$2 AND user_id=$3")
            .bind(serde_json::to_value(&snapshot).map_err(|_| StrategyExchangeError)?).bind(credential).bind(owner)
            .execute(&self.pool).await.map_err(|_| StrategyExchangeError)?;
        Ok(snapshot)
    }

    /// Trusted Control/admin entry point. The account ID must be the existing inventory ID;
    /// supplying a new API key never changes a real account's durable ownership.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind(
        &self,
        owner: &str,
        account: &str,
        label: &str,
        symbol: Symbol,
        credentials: StrategyCredentials,
        now_ms: u64,
    ) -> Result<CredentialSummary, StrategyExchangeError> {
        self.bind_inner(
            owner,
            account,
            label,
            symbol,
            credentials,
            now_ms,
            false,
            false,
        )
        .await
        .map_err(|_| StrategyExchangeError)
    }

    /// Bitget's copy-trading key can prove the same signed UID as an existing UTA key.
    /// Resolve that UID to its canonical account row before admission; a second UUID would
    /// bypass the account queue and could put two strategy writers on one exchange account.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_bitget_copy(
        &self,
        owner: &str,
        account: &str,
        label: &str,
        symbol: Symbol,
        credentials: StrategyCredentials,
        now_ms: u64,
    ) -> Result<CredentialSummary, StrategyCredentialBindError> {
        if !matches!(&credentials, StrategyCredentials::BitgetCopy { .. }) {
            return Err(StrategyCredentialBindError::Exchange);
        }
        self.bind_inner(
            owner,
            account,
            label,
            symbol,
            credentials,
            now_ms,
            false,
            true,
        )
        .await
        .map_err(Into::into)
    }

    /// Operator entry after the old writer has been stopped and released through its own
    /// lifecycle. This never releases an old scope or imports/rewrites local recovery state.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_released_account(
        &self,
        owner: &str,
        account: &str,
        label: &str,
        symbol: Symbol,
        credentials: StrategyCredentials,
        now_ms: u64,
    ) -> Result<CredentialSummary, StrategyExchangeError> {
        self.bind_inner(
            owner,
            account,
            label,
            symbol,
            credentials,
            now_ms,
            true,
            false,
        )
        .await
        .map_err(|_| StrategyExchangeError)
    }

    #[allow(clippy::too_many_arguments)]
    async fn bind_inner(
        &self,
        owner: &str,
        account: &str,
        label: &str,
        symbol: Symbol,
        credentials: StrategyCredentials,
        now_ms: u64,
        retain_positions: bool,
        reuse_identity: bool,
    ) -> Result<CredentialSummary, BindFailure> {
        if label.trim().is_empty()
            || label.chars().count() > 64
            || label.chars().any(char::is_control)
        {
            return Err(BindFailure::Exchange(StrategyExchangeError));
        }
        let venue = credentials.venue();
        let payload =
            Zeroizing::new(serde_json::to_vec(&credentials).map_err(|_| StrategyExchangeError)?);
        let key_fingerprint = identity_hash(venue, credentials.key_identity());
        let masked_key = "••••".to_owned();
        let (_, native_identity, initial_snapshot) =
            verified_snapshot(account, symbol.clone(), credentials)
                .await
                .map_err(|_| StrategyExchangeError)?;
        let hash = identity_hash(venue, &native_identity);
        // Both admission paths require the old order network to be gone. Released accounts may
        // retain signed inventory; the new grid derives its own orders from those positions.
        let canonical_account = if reuse_identity {
            let mut resolution_tx = self.pool.begin().await.map_err(|_| StrategyExchangeError)?;
            let canonical =
                resolve_identity_account(&mut resolution_tx, owner, account, venue, &hash).await?;
            resolution_tx
                .rollback()
                .await
                .map_err(|_| StrategyExchangeError)?;
            canonical
        } else {
            account.to_owned()
        };
        let snapshot = if canonical_account == account {
            initial_snapshot
        } else {
            let canonical_credentials: StrategyCredentials =
                serde_json::from_slice(&payload).map_err(|_| StrategyExchangeError)?;
            let (canonical_venue, canonical_identity, snapshot) =
                verified_snapshot(&canonical_account, symbol, canonical_credentials)
                    .await
                    .map_err(|_| StrategyExchangeError)?;
            if canonical_venue != venue
                || identity_hash(canonical_venue, &canonical_identity) != hash
            {
                return Err(BindFailure::IdentityConflict);
            }
            snapshot
        };
        let has_exposure = snapshot.positions().iter().any(|p| !p.quantity.is_zero());
        if !snapshot.open_orders().is_empty() || (!retain_positions && has_exposure) {
            return Err(BindFailure::Exchange(StrategyExchangeError));
        }
        let id = crate::accounts::strategy_credential_id().map_err(|_| StrategyExchangeError)?;
        let mut tx = self.pool.begin().await.map_err(|_| StrategyExchangeError)?;
        let canonical_account = if reuse_identity {
            let resolved = resolve_identity_account(&mut tx, owner, account, venue, &hash).await?;
            if resolved != canonical_account {
                return Err(BindFailure::IdentityConflict);
            }
            resolved
        } else {
            account.to_owned()
        };
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:' || $1,0))",
        )
        .bind(&canonical_account)
        .execute(&mut *tx)
        .await
        .map_err(|_| StrategyExchangeError)?;
        sqlx::query("SELECT user_id FROM venue_users WHERE user_id=$1 FOR UPDATE")
            .bind(owner)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| StrategyExchangeError)?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM venue_api_credentials WHERE user_id=$1 AND deleted_ms IS NULL",
        )
        .bind(owner)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| StrategyExchangeError)?;
        if count >= 20 {
            return Err(BindFailure::Exchange(StrategyExchangeError));
        }
        let occupied: bool = if reuse_identity {
            sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE trading_account_id=$1)
                 OR EXISTS(SELECT 1 FROM venue_strategy_grids WHERE trading_account_id=$1 AND lifecycle IN ('running','pausing','paused','stopping','resetting'))
                 OR EXISTS(SELECT 1 FROM venue_support_martingale_instances WHERE trading_account_id=$1 AND lifecycle IN ('running','entry_paused','increase_paused','draining'))
                 OR EXISTS(SELECT 1 FROM venue_inventory_mm_instances WHERE trading_account_id=$1 AND instance_state <> 'stopped')",
            )
            .bind(&canonical_account)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| StrategyExchangeError)?
        } else {
            sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE trading_account_id=$1)",
            )
            .bind(&canonical_account)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| StrategyExchangeError)?
        };
        if occupied {
            return Err(if reuse_identity {
                BindFailure::AccountInUse
            } else {
                BindFailure::Exchange(StrategyExchangeError)
            });
        }
        let unfinished: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(&canonical_account).fetch_one(&mut *tx).await.map_err(|_| StrategyExchangeError)?;
        if unfinished {
            return Err(if reuse_identity {
                BindFailure::AccountInUse
            } else {
                BindFailure::Exchange(StrategyExchangeError)
            });
        }
        if !reuse_identity {
            sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
                .bind(&canonical_account).bind(owner).bind(venue.as_str()).bind(&hash).execute(&mut *tx).await.map_err(|_| StrategyExchangeError)?;
            let account_row = sqlx::query("SELECT trading_account_id,user_id FROM venue_user_trading_accounts WHERE venue=$1 AND exchange_identity_hash=$2 FOR UPDATE")
                .bind(venue.as_str()).bind(&hash).fetch_one(&mut *tx).await.map_err(|_| StrategyExchangeError)?;
            let stored_account: String = account_row
                .try_get("trading_account_id")
                .map_err(|_| StrategyExchangeError)?;
            let stored_owner: String = account_row
                .try_get("user_id")
                .map_err(|_| StrategyExchangeError)?;
            if stored_account != canonical_account || stored_owner != owner {
                return Err(BindFailure::Exchange(StrategyExchangeError));
            }
        }
        let encrypted = self
            .cipher
            .encrypt(&credential_scope(owner, &id), &payload)
            .map_err(|_| StrategyExchangeError)?;
        let summary = CredentialSummary {
            credential_id: id.clone(),
            label: label.trim().to_owned(),
            venue,
            masked_key,
            trading_account_id: Some(canonical_account.clone()),
            verification: ApiVerificationState::Verified,
            verified_ms: Some(snapshot.observed_at_ms()),
            expires_ms: None,
            api_reachable: true,
            dual_position: snapshot.position_mode() == SignedAccountPositionMode::Hedge,
            account_mode: Some(
                if venue == VenueId::Hyperliquid {
                    "Perpetual Net"
                } else {
                    "Perpetual Hedge"
                }
                .to_owned(),
            ),
            has_exposure: Some(has_exposure),
            equity: None,
            available_margin: None,
            balance_observed_ms: None,
        };
        let mut verification = serde_json::to_value(&summary).map_err(|_| StrategyExchangeError)?;
        verification["strategy_execution"] = serde_json::json!(true);
        let snapshot_json = serde_json::to_value(&snapshot).map_err(|_| StrategyExchangeError)?;
        sqlx::query("INSERT INTO venue_api_credentials(credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms,venue,strategy_snapshot) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
            .bind(&id).bind(owner).bind(&summary.label).bind(key_fingerprint).bind(&summary.masked_key)
            .bind(encrypted).bind(&canonical_account).bind(verification).bind(i64::try_from(now_ms).map_err(|_| StrategyExchangeError)?)
            .bind(venue.as_str()).bind(snapshot_json)
            .execute(&mut *tx).await.map_err(|_| StrategyExchangeError)?;
        tx.commit().await.map_err(|_| StrategyExchangeError)?;
        Ok(summary)
    }

    pub(crate) async fn load(
        &self,
        owner: &str,
        credential: &str,
        account: &str,
        reconcile: bool,
    ) -> Result<(StrategyCredentials, Vec<u8>), StrategyExchangeError> {
        let row = sqlx::query("SELECT c.encrypted_credentials,a.exchange_identity_hash,a.venue FROM venue_api_credentials c JOIN venue_user_trading_accounts a USING(trading_account_id) WHERE c.credential_id=$1 AND c.user_id=$2 AND a.user_id=$2 AND c.trading_account_id=$3 AND ($4 OR (c.verification_json->>'strategy_execution'='true' AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified'))")
            .bind(credential).bind(owner).bind(account).bind(reconcile).fetch_one(&self.pool).await.map_err(|_| StrategyExchangeError)?;
        let envelope: Vec<u8> = row
            .try_get("encrypted_credentials")
            .map_err(|_| StrategyExchangeError)?;
        let plaintext = self
            .cipher
            .decrypt(&credential_scope(owner, credential), &envelope)
            .map_err(|_| StrategyExchangeError)?;
        let credentials: StrategyCredentials =
            serde_json::from_slice(&plaintext).map_err(|_| StrategyExchangeError)?;
        let venue: String = row.try_get("venue").map_err(|_| StrategyExchangeError)?;
        if credentials.venue().as_str() != venue {
            return Err(StrategyExchangeError);
        }
        Ok((
            credentials,
            row.try_get("exchange_identity_hash")
                .map_err(|_| StrategyExchangeError)?,
        ))
    }
}
