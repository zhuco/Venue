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
