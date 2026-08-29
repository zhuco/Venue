use secrecy::{ExposeSecret, SecretString};

use crate::OkxError;

pub struct OkxCredentials {
    pub(crate) api_key: SecretString,
    pub(crate) api_secret: SecretString,
    pub(crate) passphrase: SecretString,
}

impl OkxCredentials {
    pub fn from_environment() -> Result<Self, OkxError> {
        let api_key =
            SecretString::from(std::env::var("OKX_API_KEY").map_err(|_| OkxError::Credentials)?);
        let api_secret =
            SecretString::from(std::env::var("OKX_API_SECRET").map_err(|_| OkxError::Credentials)?);
        let passphrase = SecretString::from(
            std::env::var("OKX_API_PASSPHRASE").map_err(|_| OkxError::Credentials)?,
        );
        Self::from_secrets(api_key, api_secret, passphrase)
    }

    #[cfg(test)]
    pub(crate) fn from_values(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        passphrase: impl Into<String>,
    ) -> Result<Self, OkxError> {
        Self::from_secrets(
            SecretString::from(api_key.into()),
            SecretString::from(api_secret.into()),
            SecretString::from(passphrase.into()),
        )
    }

    fn from_secrets(
        api_key: SecretString,
        api_secret: SecretString,
        passphrase: SecretString,
    ) -> Result<Self, OkxError> {
        if api_key.expose_secret().trim().is_empty()
            || api_secret.expose_secret().trim().is_empty()
            || passphrase.expose_secret().trim().is_empty()
        {
            return Err(OkxError::Credentials);
        }
        Ok(Self {
            api_key,
            api_secret,
            passphrase,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_credentials_fail_after_secrecy_takes_ownership() {
        assert!(OkxCredentials::from_values("key", "", "passphrase").is_err());
        assert!(OkxCredentials::from_values("key", "secret", "passphrase").is_ok());
    }
}
