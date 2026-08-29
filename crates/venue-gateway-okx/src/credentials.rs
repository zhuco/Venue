use secrecy::SecretString;

use crate::OkxError;

pub struct OkxCredentials {
    pub(crate) api_key: SecretString,
    pub(crate) api_secret: SecretString,
    pub(crate) passphrase: SecretString,
}

impl OkxCredentials {
    pub fn from_environment() -> Result<Self, OkxError> {
        Self::from_values(
            std::env::var("OKX_API_KEY").map_err(|_| OkxError::Credentials)?,
            std::env::var("OKX_API_SECRET").map_err(|_| OkxError::Credentials)?,
            std::env::var("OKX_API_PASSPHRASE").map_err(|_| OkxError::Credentials)?,
        )
    }

    pub(crate) fn from_values(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        passphrase: impl Into<String>,
    ) -> Result<Self, OkxError> {
        let api_key = api_key.into();
        let api_secret = api_secret.into();
        let passphrase = passphrase.into();
        if api_key.trim().is_empty() || api_secret.trim().is_empty() || passphrase.trim().is_empty()
        {
            return Err(OkxError::Credentials);
        }
        Ok(Self {
            api_key: SecretString::from(api_key),
            api_secret: SecretString::from(api_secret),
            passphrase: SecretString::from(passphrase),
        })
    }
}
