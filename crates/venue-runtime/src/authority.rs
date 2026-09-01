use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::{ExchangeId, NativeOrderFamily, OrderOwner, Symbol};
use venue_storage::ActorAppliedReceipt;

const MAX_RUNTIME_ID_LEN: usize = 36;
const MAX_CONFIG_DIGEST_LEN: usize = 128;

/// Stable account identity for one process-owned exchange account. Symbols are deliberately
/// excluded so every strategy in the account shares one private stream and execution lane.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct AccountKey {
    pub exchange: ExchangeId,
    pub account: String,
}

impl AccountKey {
    pub fn new(
        exchange: ExchangeId,
        account: impl Into<String>,
    ) -> Result<Self, AccountModelError> {
        let account = account.into();
        validate_runtime_id(&account)?;
        Ok(Self { exchange, account })
    }

    #[must_use]
    pub fn matches_owner(&self, owner: &OrderOwner) -> bool {
        owner.exchange == self.exchange.as_str() && owner.account == self.account
    }
}

/// Closed account-level evidence for the native order families that one exchange adapter can
/// expose completely. The same value is retained by reconciliation and the execution lane so an
/// unsupported empty endpoint can never disagree with mutation admission or recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountOrderCapabilityEvidence {
    account: AccountKey,
    profile_version: u32,
    supported_families: BTreeSet<NativeOrderFamily>,
}

impl AccountOrderCapabilityEvidence {
    pub(crate) fn for_account(account: AccountKey) -> Self {
        let supported_families = match account.exchange {
            ExchangeId::Binance => BTreeSet::from([
                NativeOrderFamily::UmOrder,
                NativeOrderFamily::UmConditional,
                NativeOrderFamily::UmAlgo,
            ]),
            ExchangeId::Gate
            | ExchangeId::Bitget
            | ExchangeId::Bybit
            | ExchangeId::Hyperliquid
            | ExchangeId::Okx => BTreeSet::from([NativeOrderFamily::UmOrder]),
        };
        Self {
            account,
            profile_version: 1,
            supported_families,
        }
    }

    #[must_use]
    pub(crate) const fn account(&self) -> &AccountKey {
        &self.account
    }

    #[must_use]
    pub(crate) fn supports(&self, family: NativeOrderFamily) -> bool {
        self.supported_families.contains(&family)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    HedgedGrid,
    Scalping,
    /// A dormant terminal-owned actor. It emits no autonomous intent; explicit Control Trade
    /// deliveries still traverse the same Runtime, account lane, WAL, and physical writer.
    Manual,
    /// A follower-owned semantic Copy actor. It still has no exchange client or writer authority.
    Copy,
}

/// Stable logical ownership identity. A controlled process restart may replace `run_id` in its
/// binding, but it must recover and hand off all durable order ownership before doing so.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct StrategyInstanceKey {
    pub account: AccountKey,
    pub strategy_kind: StrategyKind,
    pub instance_id: String,
    pub symbol: Symbol,
}

impl StrategyInstanceKey {
    pub fn new(
        account: AccountKey,
        strategy_kind: StrategyKind,
        instance_id: impl Into<String>,
        symbol: Symbol,
    ) -> Result<Self, AccountModelError> {
        let instance_id = instance_id.into();
        validate_runtime_id(&instance_id)?;
        Ok(Self {
            account,
            strategy_kind,
            instance_id,
            symbol,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StrategyBinding {
    pub key: StrategyInstanceKey,
    pub run_id: String,
    pub config_digest: String,
}

/// Runtime-issued authority for exactly one actor input. It deliberately has no public
/// constructor or serde implementation, so a strategy cannot self-report connection, private or
/// configuration generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyTurnToken {
    target: StrategyInstanceKey,
    connection_generation: u64,
    private_generation: u64,
    config_digest: String,
    config_epoch: u64,
    turn_sequence: u64,
}

impl StrategyTurnToken {
    pub(crate) fn issue(
        target: StrategyInstanceKey,
        connection_generation: u64,
        private_generation: u64,
        config_digest: String,
        config_epoch: u64,
        turn_sequence: u64,
    ) -> Result<Self, AccountModelError> {
        if connection_generation == 0
            || config_epoch == 0
            || turn_sequence == 0
            || validate_config_digest(&config_digest).is_err()
        {
            return Err(AccountModelError::Authority);
        }
        Ok(Self {
            target,
            connection_generation,
            private_generation,
            config_digest,
            config_epoch,
            turn_sequence,
        })
    }

    #[must_use]
    pub const fn target(&self) -> &StrategyInstanceKey {
        &self.target
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub fn config_digest(&self) -> &str {
        &self.config_digest
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    #[must_use]
    pub const fn turn_sequence(&self) -> u64 {
        self.turn_sequence
    }
}

/// Receipt produced only after the actor inbox/checkpoint transaction is durable. AccountRuntime
/// accepts desired state and execution intents only when this receipt matches its active turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedStrategyTurnReceipt {
    token: StrategyTurnToken,
    actor_applied: Option<ActorAppliedReceipt>,
}

impl AppliedStrategyTurnReceipt {
    pub(crate) fn persisted(token: StrategyTurnToken, actor_applied: ActorAppliedReceipt) -> Self {
        Self {
            token,
            actor_applied: Some(actor_applied),
        }
    }

    #[must_use]
    pub(crate) const fn token(&self) -> &StrategyTurnToken {
        &self.token
    }

    #[must_use]
    pub(crate) const fn actor_applied(&self) -> Option<&ActorAppliedReceipt> {
        self.actor_applied.as_ref()
    }

    /// A serializable restart anchor for the already-durable actor checkpoint.  This is an
    /// integrity expectation only; it cannot create a turn, writer, or dispatch capability.
    #[must_use]
    pub fn actor_applied_anchor(&self) -> Option<venue_storage::ActorAppliedAnchor> {
        self.actor_applied.as_ref().map(ActorAppliedReceipt::anchor)
    }

    /// Read-only commitment to the exact actor checkpoint that made a semantic turn durable.
    /// It is suitable for a Control receipt, but cannot be used to reconstruct a turn or a
    /// physical dispatch permit.
    #[must_use]
    pub fn durable_fact_digest(&self) -> Option<[u8; 32]> {
        let anchor = self.actor_applied_anchor()?;
        let mut digest = Sha256::new();
        digest.update(b"venue.runtime.actor-applied.receipt.v1");
        digest.update(self.token.target().account.exchange.as_str().as_bytes());
        digest.update(self.token.target().account.account.as_bytes());
        digest.update(self.token.target().instance_id.as_bytes());
        digest.update(self.token.target().symbol.to_string().as_bytes());
        digest.update(self.token.config_epoch().to_le_bytes());
        digest.update(self.token.turn_sequence().to_le_bytes());
        digest.update(anchor.journal_root_sha256());
        digest.update(anchor.journal_tail_sequence().to_le_bytes());
        digest.update(anchor.checkpoint_sha256());
        Some(digest.finalize().into())
    }

    #[must_use]
    pub fn target(&self) -> &StrategyInstanceKey {
        self.token.target()
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.token.config_epoch()
    }

    #[cfg(test)]
    pub(crate) fn test_persisted(token: StrategyTurnToken) -> Self {
        Self {
            token,
            actor_applied: None,
        }
    }
}

impl StrategyBinding {
    pub fn new(
        key: StrategyInstanceKey,
        run_id: impl Into<String>,
        config_digest: impl Into<String>,
    ) -> Result<Self, AccountModelError> {
        let run_id = run_id.into();
        let config_digest = config_digest.into();
        validate_runtime_id(&run_id)?;
        validate_config_digest(&config_digest)?;
        Ok(Self {
            key,
            run_id,
            config_digest,
        })
    }

    #[must_use]
    pub fn matches_owner(&self, owner: &OrderOwner) -> bool {
        self.key.account.matches_owner(owner)
            && owner.strategy_instance_id == self.key.instance_id
            && owner.run_id == self.run_id
            && owner.symbol == self.key.symbol
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AccountModelError {
    #[error("runtime identifier must be 1 to 36 ASCII alphanumeric, dash, or underscore bytes")]
    Identifier,
    #[error("exchange must be Binance, Bitget, Bybit, Gate.io, Hyperliquid, or OKX")]
    Exchange,
    #[error("configuration digest is invalid")]
    ConfigDigest,
    #[error("strategy turn authority is invalid")]
    Authority,
}

fn validate_runtime_id(value: &str) -> Result<(), AccountModelError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_RUNTIME_ID_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    valid.then_some(()).ok_or(AccountModelError::Identifier)
}

pub(crate) fn validate_config_digest(value: &str) -> Result<(), AccountModelError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_CONFIG_DIGEST_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    valid.then_some(()).ok_or(AccountModelError::ConfigDigest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_resident_venues_admit_their_proven_regular_order_family() -> Result<(), AccountModelError>
    {
        for exchange in [ExchangeId::Bybit, ExchangeId::Hyperliquid, ExchangeId::Okx] {
            let account = AccountKey::new(exchange, "00000000-0000-0000-0000-000000000001")?;
            let evidence = AccountOrderCapabilityEvidence::for_account(account);

            assert!(evidence.supports(NativeOrderFamily::UmOrder));
            assert!(!evidence.supports(NativeOrderFamily::UmConditional));
            assert!(!evidence.supports(NativeOrderFamily::UmAlgo));
        }
        Ok(())
    }
}
