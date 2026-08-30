use k256::{SecretKey, ecdsa::SigningKey};
use secrecy::{ExposeSecret, SecretString};
use sha3::{Digest, Keccak256};

use crate::HyperliquidError;

/// Request-scoped Hyperliquid API Wallet material. It intentionally has no `Debug`,
/// `Clone`, serialization, or public secret accessor.
pub struct HyperliquidCredentials {
    account_address: String,
    user_address: String,
    vault_address: Option<String>,
    api_wallet_address: String,
    api_wallet_private_key: SecretString,
}

impl HyperliquidCredentials {
    pub fn from_environment() -> Result<Self, HyperliquidError> {
        let account_address = std::env::var("HYPERLIQUID_ACCOUNT_ADDRESS")
            .map_err(|_| HyperliquidError::Credentials)?;
        let vault_address = std::env::var("HYPERLIQUID_VAULT_ADDRESS")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let api_wallet_address = std::env::var("HYPERLIQUID_API_WALLET_ADDRESS")
            .map_err(|_| HyperliquidError::Credentials)?;
        let api_wallet_private_key = SecretString::from(
            std::env::var("HYPERLIQUID_API_WALLET_PRIVATE_KEY")
                .map_err(|_| HyperliquidError::Credentials)?,
        );
        Self::from_secrets(
            account_address,
            vault_address,
            api_wallet_address,
            api_wallet_private_key,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_values(
        account_address: impl Into<String>,
        vault_address: Option<String>,
        api_wallet_address: impl Into<String>,
        api_wallet_private_key: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        Self::from_secrets(
            account_address.into(),
            vault_address,
            api_wallet_address.into(),
            SecretString::from(api_wallet_private_key.into()),
        )
    }

    fn from_secrets(
        account_address: String,
        vault_address: Option<String>,
        api_wallet_address: String,
        api_wallet_private_key: SecretString,
    ) -> Result<Self, HyperliquidError> {
        let account_address = normalize_address(&account_address)?;
        let vault_address = vault_address
            .as_deref()
            .map(normalize_address)
            .transpose()?;
        let user_address = vault_address
            .as_deref()
            .unwrap_or(&account_address)
            .to_owned();
        let api_wallet_address = normalize_address(&api_wallet_address)?;
        let signing_key = signing_key(api_wallet_private_key.expose_secret())?;
        let derived_api_wallet_address = address_from_signing_key(&signing_key)?;
        let distinct_api_wallet = api_wallet_address != account_address
            && api_wallet_address != user_address
            && vault_address
                .as_ref()
                .is_none_or(|vault| vault != &api_wallet_address);
        if api_wallet_address != derived_api_wallet_address || !distinct_api_wallet {
            return Err(HyperliquidError::Credentials);
        }
        Ok(Self {
            account_address,
            user_address,
            vault_address,
            api_wallet_address,
            api_wallet_private_key,
        })
    }

    #[must_use]
    pub fn account_address(&self) -> &str {
        &self.account_address
    }

    #[must_use]
    pub fn user_address(&self) -> &str {
        &self.user_address
    }

    #[must_use]
    pub fn vault_address(&self) -> Option<&str> {
        self.vault_address.as_deref()
    }

    #[must_use]
    pub fn api_wallet_address(&self) -> &str {
        &self.api_wallet_address
    }

    #[must_use]
    pub fn public_binding(&self) -> (&str, &str, Option<&str>, &str) {
        (
            &self.account_address,
            &self.user_address,
            self.vault_address.as_deref(),
            &self.api_wallet_address,
        )
    }

    pub(crate) fn signing_key(&self) -> Result<SigningKey, HyperliquidError> {
        signing_key(self.api_wallet_private_key.expose_secret())
    }
}

pub(crate) fn valid_address(value: &str) -> bool {
    address_bytes(value).is_ok()
}

pub(crate) fn address_bytes(value: &str) -> Result<[u8; 20], HyperliquidError> {
    let raw = value
        .strip_prefix("0x")
        .ok_or(HyperliquidError::Credentials)?;
    if raw.len() != 40 {
        return Err(HyperliquidError::Credentials);
    }
    let mut decoded = [0_u8; 20];
    let (pairs, remainder) = raw.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(HyperliquidError::Credentials);
    }
    for (index, pair) in pairs.iter().enumerate() {
        decoded[index] = (hex_nibble(pair[0]).ok_or(HyperliquidError::Credentials)? << 4)
            | hex_nibble(pair[1]).ok_or(HyperliquidError::Credentials)?;
    }
    if decoded == [0; 20] {
        return Err(HyperliquidError::Credentials);
    }
    Ok(decoded)
}

fn normalize_address(value: &str) -> Result<String, HyperliquidError> {
    address_bytes(value)?;
    Ok(value.to_ascii_lowercase())
}

fn signing_key(value: &str) -> Result<SigningKey, HyperliquidError> {
    let raw = value.strip_prefix("0x").unwrap_or(value);
    if raw.len() != 64 || !raw.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(HyperliquidError::Credentials);
    }
    let mut decoded = [0_u8; 32];
    let (pairs, remainder) = raw.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err(HyperliquidError::Credentials);
    }
    for (index, pair) in pairs.iter().enumerate() {
        decoded[index] = (hex_nibble(pair[0]).ok_or(HyperliquidError::Credentials)? << 4)
            | hex_nibble(pair[1]).ok_or(HyperliquidError::Credentials)?;
    }
    let secret = SecretKey::from_slice(&decoded);
    decoded.fill(0);
    let secret = secret.map_err(|_| HyperliquidError::Credentials)?;
    Ok(SigningKey::from(secret))
}

fn address_from_signing_key(signing_key: &SigningKey) -> Result<String, HyperliquidError> {
    let point = signing_key.verifying_key().to_encoded_point(false);
    let point = point.as_bytes();
    if point.len() != 65 || point.first() != Some(&4) {
        return Err(HyperliquidError::Credentials);
    }
    let hash = Keccak256::digest(&point[1..]);
    let mut output = String::with_capacity(42);
    output.push_str("0x");
    for byte in &hash[12..] {
        push_hex_byte(&mut output, *byte);
    }
    Ok(output)
}

fn push_hex_byte(output: &mut String, value: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(char::from(HEX[usize::from(value >> 4)]));
    output.push(char::from(HEX[usize::from(value & 0x0f)]));
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
