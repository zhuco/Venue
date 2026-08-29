use serde::{Deserialize, Serialize};

use crate::{HyperliquidError, credentials::valid_address};

/// Durable state for one Agent/API Wallet. The execution owner must persist the returned
/// checkpoint before exposing its nonce to a signer; the crate does not create a second WAL.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NonceCheckpoint {
    pub schema_version: u16,
    pub agent_address: String,
    pub last_nonce_ms: u64,
}

/// Storage boundary owned by the execution/WAL layer. `persist` must make the checkpoint durable
/// before it returns; this crate deliberately does not create a second nonce file or journal.
pub trait HyperliquidNonceStore {
    fn load(&mut self, agent_address: &str) -> Result<Option<NonceCheckpoint>, HyperliquidError>;

    fn persist(&mut self, checkpoint: &NonceCheckpoint) -> Result<(), HyperliquidError>;
}

/// A nonce whose exact checkpoint was durably persisted and read back. It is intentionally not
/// `Clone` or serializable; signing consumes it into one bounded exchange request.
#[derive(Debug, Eq, PartialEq)]
pub struct PersistedNonce {
    checkpoint: NonceCheckpoint,
}

impl PersistedNonce {
    #[must_use]
    pub const fn value(&self) -> u64 {
        self.checkpoint.last_nonce_ms
    }

    #[must_use]
    pub fn agent_address(&self) -> &str {
        &self.checkpoint.agent_address
    }
}

pub fn prepare_next_nonce(
    recovered: Option<&NonceCheckpoint>,
    agent_address: &str,
    now_ms: u64,
) -> Result<NonceCheckpoint, HyperliquidError> {
    if !valid_address(agent_address) || now_ms == 0 {
        return Err(HyperliquidError::Nonce);
    }
    let next = match recovered {
        None => now_ms,
        Some(state) => {
            if state.schema_version != 1
                || !state.agent_address.eq_ignore_ascii_case(agent_address)
                || state.last_nonce_ms == 0
            {
                return Err(HyperliquidError::Nonce);
            }
            now_ms.max(
                state
                    .last_nonce_ms
                    .checked_add(1)
                    .ok_or(HyperliquidError::Nonce)?,
            )
        }
    };
    Ok(NonceCheckpoint {
        schema_version: 1,
        agent_address: agent_address.to_ascii_lowercase(),
        last_nonce_ms: next,
    })
}

pub fn reserve_next_nonce<S: HyperliquidNonceStore>(
    store: &mut S,
    agent_address: &str,
    now_ms: u64,
) -> Result<PersistedNonce, HyperliquidError> {
    let recovered = store.load(agent_address)?;
    let checkpoint = prepare_next_nonce(recovered.as_ref(), agent_address, now_ms)?;
    store.persist(&checkpoint)?;
    if store.load(agent_address)?.as_ref() != Some(&checkpoint) {
        return Err(HyperliquidError::Nonce);
    }
    Ok(PersistedNonce { checkpoint })
}
