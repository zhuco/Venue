use secrecy::{ExposeSecret, SecretString};

use crate::BinanceAuthError;

const API_KEY_ENV: &str = "BINANCE_API_KEY";
const API_SECRET_ENV: &str = "BINANCE_API_SECRET";

/// Credentials are owned by zeroizing secrecy containers before any validation occurs.
pub struct BinanceCredentials {
    pub(crate) api_key: SecretString,
    pub(crate) api_secret: SecretString,
}

impl BinanceCredentials {
    pub fn from_environment() -> Result<Self, BinanceAuthError> {
        let api_key = SecretString::from(
            std::env::var(API_KEY_ENV).map_err(|_| BinanceAuthError::Credentials)?,
        );
        let api_secret = SecretString::from(
            std::env::var(API_SECRET_ENV).map_err(|_| BinanceAuthError::Credentials)?,
        );
        Self::from_secrets(api_key, api_secret)
    }

    #[cfg(test)]
    pub(crate) fn from_values(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Result<Self, BinanceAuthError> {
        Self::from_secrets(
            SecretString::from(api_key.into()),
            SecretString::from(api_secret.into()),
        )
    }

    fn from_secrets(
        api_key: SecretString,
        api_secret: SecretString,
    ) -> Result<Self, BinanceAuthError> {
        if api_key.expose_secret().trim().is_empty() || api_secret.expose_secret().trim().is_empty()
        {
            return Err(BinanceAuthError::Credentials);
        }
        Ok(Self {
            api_key,
            api_secret,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_owned_before_empty_value_rejection() {
        assert!(matches!(
            BinanceCredentials::from_values("key", ""),
            Err(BinanceAuthError::Credentials)
        ));
        assert!(BinanceCredentials::from_values("key", "secret").is_ok());
    }
}
