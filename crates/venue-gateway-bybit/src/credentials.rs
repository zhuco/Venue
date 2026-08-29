use secrecy::{ExposeSecret, SecretString};

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
}
