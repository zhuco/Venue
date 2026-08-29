use secrecy::{ExposeSecret, SecretString};

use crate::HyperliquidError;

/// Request-scoped Hyperliquid Agent/API Wallet material. It intentionally has no
/// `Debug`, `Clone`, serialization, or secret accessor until the signing boundary exists.
pub struct HyperliquidCredentials {
    master_address: String,
    user_address: String,
    vault_address: Option<String>,
    agent_name: String,
    agent_address: String,
    _agent_private_key: SecretString,
}

impl HyperliquidCredentials {
    pub fn from_environment() -> Result<Self, HyperliquidError> {
        Self::from_values(
            std::env::var("HYPERLIQUID_MASTER_ADDRESS")
                .map_err(|_| HyperliquidError::Credentials)?,
            std::env::var("HYPERLIQUID_USER_ADDRESS").map_err(|_| HyperliquidError::Credentials)?,
            std::env::var("HYPERLIQUID_VAULT_ADDRESS").ok(),
            std::env::var("HYPERLIQUID_AGENT_NAME").map_err(|_| HyperliquidError::Credentials)?,
            std::env::var("HYPERLIQUID_AGENT_ADDRESS")
                .map_err(|_| HyperliquidError::Credentials)?,
            std::env::var("HYPERLIQUID_AGENT_PRIVATE_KEY")
                .map_err(|_| HyperliquidError::Credentials)?,
        )
    }

    pub(crate) fn from_values(
        master_address: impl Into<String>,
        user_address: impl Into<String>,
        vault_address: Option<String>,
        agent_name: impl Into<String>,
        agent_address: impl Into<String>,
        agent_private_key: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        let master_address = master_address.into();
        let user_address = user_address.into();
        let agent_name = agent_name.into();
        let agent_address = agent_address.into();
        let agent_private_key = SecretString::from(agent_private_key.into());
        let vault_valid = vault_address.as_deref().is_none_or(valid_address);
        let distinct_agent = !agent_address.eq_ignore_ascii_case(&master_address)
            && !agent_address.eq_ignore_ascii_case(&user_address)
            && vault_address
                .as_deref()
                .is_none_or(|vault| !agent_address.eq_ignore_ascii_case(vault));
        if !valid_address(&master_address)
            || !valid_address(&user_address)
            || !valid_address(&agent_address)
            || !vault_valid
            || !distinct_agent
            || agent_name.trim().is_empty()
            || agent_name.len() > 128
            || agent_name.chars().any(char::is_control)
            || !valid_private_key(agent_private_key.expose_secret())
        {
            return Err(HyperliquidError::Credentials);
        }
        Ok(Self {
            master_address,
            user_address,
            vault_address,
            agent_name,
            agent_address,
            _agent_private_key: agent_private_key,
        })
    }

    #[must_use]
    pub fn agent_address(&self) -> &str {
        &self.agent_address
    }

    #[must_use]
    pub fn public_binding(&self) -> (&str, &str, Option<&str>, &str) {
        (
            &self.master_address,
            &self.user_address,
            self.vault_address.as_deref(),
            &self.agent_name,
        )
    }
}

pub(crate) fn valid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
}

fn valid_private_key(value: &str) -> bool {
    let raw = value.strip_prefix("0x").unwrap_or(value);
    raw.len() == 64 && raw.as_bytes().iter().all(u8::is_ascii_hexdigit)
}
