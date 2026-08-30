use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;

use crate::{OkxConfig, OkxCredentials, OkxError};

pub(crate) struct SignedHeaders {
    entries: [(String, SecretString); 5],
    simulated_trading: bool,
}

impl SignedHeaders {
    #[must_use]
    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        if name.eq_ignore_ascii_case("x-simulated-trading") {
            return self.simulated_trading.then_some("1");
        }
        self.entries
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.expose_secret())
    }
}

pub(crate) fn sign(
    credentials: &OkxCredentials,
    config: &OkxConfig,
    timestamp: &str,
    method: &str,
    request_path: &str,
    body: &[u8],
) -> Result<SignedHeaders, OkxError> {
    if timestamp.trim().is_empty()
        || method.is_empty()
        || !method.bytes().all(|byte| byte.is_ascii_uppercase())
        || !request_path.starts_with('/')
    {
        return Err(OkxError::SigningInput);
    }
    let api_key = credentials.api_key.expose_secret();
    let passphrase = credentials.passphrase.expose_secret();
    let mut mac = Hmac::<Sha256>::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| OkxError::SigningInput)?;
    mac.update(timestamp.as_bytes());
    mac.update(method.as_bytes());
    mac.update(request_path.as_bytes());
    mac.update(body);
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    Ok(SignedHeaders {
        entries: [
            (
                "OK-ACCESS-KEY".to_owned(),
                SecretString::from(api_key.to_owned()),
            ),
            ("OK-ACCESS-SIGN".to_owned(), SecretString::from(signature)),
            (
                "OK-ACCESS-TIMESTAMP".to_owned(),
                SecretString::from(timestamp.to_owned()),
            ),
            (
                "OK-ACCESS-PASSPHRASE".to_owned(),
                SecretString::from(passphrase.to_owned()),
            ),
            (
                "Content-Type".to_owned(),
                SecretString::from("application/json".to_owned()),
            ),
        ],
        simulated_trading: config.simulated_trading(),
    })
}
