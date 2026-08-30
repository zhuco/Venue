use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;

use crate::BybitError;

pub struct BybitCredentials {
    pub(crate) api_key: SecretString,
    pub(crate) api_secret: SecretString,
}

impl BybitCredentials {
    pub fn from_environment() -> Result<Self, BybitError> {
        Self::from_values(
            std::env::var("BYBIT_API_KEY").map_err(|_| BybitError::Credentials)?,
            std::env::var("BYBIT_API_SECRET").map_err(|_| BybitError::Credentials)?,
        )
    }

    pub(crate) fn from_values(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Result<Self, BybitError> {
        let api_key = SecretString::from(api_key.into());
        let api_secret = SecretString::from(api_secret.into());
        if api_key.expose_secret().trim().is_empty() || api_secret.expose_secret().trim().is_empty()
        {
            return Err(BybitError::Credentials);
        }
        Ok(Self {
            api_key,
            api_secret,
        })
    }

    pub(crate) fn recovery_namespace_hmac(
        &self,
        scope_bytes: &[u8],
    ) -> Result<[u8; 32], BybitError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.expose_secret().as_bytes())
            .map_err(|_| BybitError::Credentials)?;
        mac.update(b"venue-bybit-recovery-credential-namespace-v1");
        mac.update(b"BYBIT_API_KEY\0BYBIT_API_SECRET");
        mac.update(&(self.api_key.expose_secret().len() as u64).to_be_bytes());
        mac.update(self.api_key.expose_secret().as_bytes());
        mac.update(scope_bytes);
        Ok(mac.finalize().into_bytes().into())
    }
}
