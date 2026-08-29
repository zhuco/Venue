use secrecy::{ExposeSecret, SecretString};

use crate::BitgetError;

pub struct BitgetCredentials {
    pub(crate) api_key: SecretString,
    pub(crate) api_secret: SecretString,
    pub(crate) passphrase: SecretString,
}

impl BitgetCredentials {
    pub fn from_environment() -> Result<Self, BitgetError> {
        let api_key = SecretString::from(
            std::env::var("BITGET_API_KEY").map_err(|_| BitgetError::Credentials)?,
        );
        let api_secret = SecretString::from(
            std::env::var("BITGET_API_SECRET").map_err(|_| BitgetError::Credentials)?,
        );
        let passphrase = select_passphrase(
            optional_environment("BITGET_API_PASSPHRASE")?,
            optional_environment("BITGET_PASSPHRASE")?,
        )?;
        Self::from_secrets(api_key, api_secret, SecretString::from(passphrase))
    }

    #[cfg(test)]
    pub(crate) fn from_values(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        passphrase: impl Into<String>,
    ) -> Result<Self, BitgetError> {
        // Secrets are owned by zeroizing containers before validation, including failure paths.
        let api_key = SecretString::from(api_key.into());
        let api_secret = SecretString::from(api_secret.into());
        let passphrase = SecretString::from(passphrase.into());
        Self::from_secrets(api_key, api_secret, passphrase)
    }

    fn from_secrets(
        api_key: SecretString,
        api_secret: SecretString,
        passphrase: SecretString,
    ) -> Result<Self, BitgetError> {
        if api_key.expose_secret().trim().is_empty()
            || api_secret.expose_secret().trim().is_empty()
            || passphrase.expose_secret().trim().is_empty()
        {
            return Err(BitgetError::Credentials);
        }
        Ok(Self {
            api_key,
            api_secret,
            passphrase,
        })
    }
}

fn optional_environment(name: &str) -> Result<Option<String>, BitgetError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(BitgetError::Credentials),
    }
}

fn select_passphrase(
    canonical: Option<String>,
    legacy: Option<String>,
) -> Result<String, BitgetError> {
    match (canonical, legacy) {
        (Some(canonical), Some(legacy)) if canonical != legacy => Err(BitgetError::Credentials),
        (Some(canonical), _) => Ok(canonical),
        (None, Some(legacy)) => Ok(legacy),
        (None, None) => Err(BitgetError::Credentials),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_reject_empty_values_without_exposing_them() {
        assert!(BitgetCredentials::from_values("key", "", "pass").is_err());
        let credentials = BitgetCredentials::from_values("key", "secret", "pass");
        assert!(credentials.is_ok());
    }

    #[test]
    fn passphrase_aliases_must_not_conflict() {
        assert_eq!(
            select_passphrase(Some("primary".to_owned()), Some("legacy".to_owned())),
            Err(BitgetError::Credentials)
        );
        assert_eq!(
            select_passphrase(Some("same".to_owned()), Some("same".to_owned())),
            Ok("same".to_owned())
        );
        assert_eq!(
            select_passphrase(None, Some("legacy".to_owned())),
            Ok("legacy".to_owned())
        );
    }
}
