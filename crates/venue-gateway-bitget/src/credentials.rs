use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};

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
        Self::from_secrets(api_key, api_secret, passphrase)
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

    pub(crate) fn identity_commitment(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"venue-bitget-credential-identity-v1");
        for value in [
            self.api_key.expose_secret(),
            self.api_secret.expose_secret(),
            self.passphrase.expose_secret(),
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        digest.finalize().into()
    }
}

fn optional_environment(name: &str) -> Result<Option<SecretString>, BitgetError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(SecretString::from(value))),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(BitgetError::Credentials),
    }
}

fn select_passphrase(
    canonical: Option<SecretString>,
    legacy: Option<SecretString>,
) -> Result<SecretString, BitgetError> {
    match (canonical, legacy) {
        (Some(canonical), Some(legacy)) if canonical.expose_secret() != legacy.expose_secret() => {
            Err(BitgetError::Credentials)
        }
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
        let conflict = select_passphrase(
            Some(SecretString::from("primary".to_owned())),
            Some(SecretString::from("legacy".to_owned())),
        );
        assert!(matches!(conflict, Err(BitgetError::Credentials)));

        let same = select_passphrase(
            Some(SecretString::from("same".to_owned())),
            Some(SecretString::from("same".to_owned())),
        );
        assert!(same.is_ok());
        if let Ok(same) = same {
            assert_eq!(same.expose_secret(), "same");
        }

        let legacy = select_passphrase(None, Some(SecretString::from("legacy".to_owned())));
        assert!(legacy.is_ok());
        if let Ok(legacy) = legacy {
            assert_eq!(legacy.expose_secret(), "legacy");
        }
    }
}
