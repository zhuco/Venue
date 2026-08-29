use secrecy::SecretString;

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
}
