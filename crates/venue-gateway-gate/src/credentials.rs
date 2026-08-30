use secrecy::ExposeSecret;
use secrecy::SecretString;
use sha2::{Digest, Sha256};

use crate::GateProtocolError;

/// Request-signing credentials retained behind the workspace secrecy boundary.
pub struct GateCredentials {
    pub(crate) api_key: SecretString,
    pub(crate) api_secret: SecretString,
}

impl GateCredentials {
    pub fn from_environment() -> Result<Self, GateProtocolError> {
        Self::from_values(
            std::env::var("GATEIO_API_KEY").map_err(|_| GateProtocolError::Credentials)?,
            std::env::var("GATEIO_API_SECRET").map_err(|_| GateProtocolError::Credentials)?,
        )
    }

    pub(crate) fn from_values(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Result<Self, GateProtocolError> {
        let api_key = api_key.into();
        let api_secret = api_secret.into();
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(GateProtocolError::Credentials);
        }
        Ok(Self {
            api_key: SecretString::from(api_key),
            api_secret: SecretString::from(api_secret),
        })
    }

    pub(crate) fn identity_commitment(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"venue-gate-credential-identity-v2");
        for value in [
            self.api_key.expose_secret().as_bytes(),
            self.api_secret.expose_secret().as_bytes(),
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
        digest.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::GateCredentials;

    #[test]
    fn credential_commitment_binds_key_and_secret_without_exposing_either()
    -> Result<(), Box<dyn std::error::Error>> {
        let baseline = GateCredentials::from_values("key", "secret")?.identity_commitment();
        assert_ne!(
            baseline,
            GateCredentials::from_values("other", "secret")?.identity_commitment()
        );
        assert_ne!(
            baseline,
            GateCredentials::from_values("key", "other")?.identity_commitment()
        );
        Ok(())
    }
}
