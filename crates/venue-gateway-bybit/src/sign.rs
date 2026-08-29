use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;
use venue_gateway_api::GatewayBinding;

use crate::{BybitCredentials, BybitError, BybitGatewayBinding};

pub struct SignedHeaders {
    entries: [(String, SecretString); 5],
}

impl SignedHeaders {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.expose_secret())
    }
}

pub fn sign(
    credentials: &BybitCredentials,
    binding: &BybitGatewayBinding,
    request_binding: &GatewayBinding,
    timestamp_ms: u64,
    payload: &[u8],
) -> Result<SignedHeaders, BybitError> {
    binding.validate_request_binding(request_binding)?;
    let recv_window_ms = binding.config().recv_window_ms();
    if timestamp_ms == 0 || recv_window_ms == 0 || recv_window_ms > 10_000 {
        return Err(BybitError::SigningInput);
    }
    let timestamp = timestamp_ms.to_string();
    let recv_window = recv_window_ms.to_string();
    let api_key = credentials.api_key.expose_secret();
    let mut mac = Hmac::<Sha256>::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| BybitError::SigningInput)?;
    mac.update(timestamp.as_bytes());
    mac.update(api_key.as_bytes());
    mac.update(recv_window.as_bytes());
    mac.update(payload);
    let signature = hex(&mac.finalize().into_bytes());
    Ok(SignedHeaders {
        entries: [
            (
                "X-BAPI-API-KEY".to_owned(),
                SecretString::from(api_key.to_owned()),
            ),
            ("X-BAPI-SIGN".to_owned(), SecretString::from(signature)),
            (
                "X-BAPI-SIGN-TYPE".to_owned(),
                SecretString::from("2".to_owned()),
            ),
            ("X-BAPI-TIMESTAMP".to_owned(), SecretString::from(timestamp)),
            (
                "X-BAPI-RECV-WINDOW".to_owned(),
                SecretString::from(recv_window),
            ),
        ],
    })
}

pub(crate) fn ws_auth_signature(
    credentials: &BybitCredentials,
    expires_at_ms: u64,
) -> Result<SecretString, BybitError> {
    if expires_at_ms == 0 {
        return Err(BybitError::SigningInput);
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| BybitError::SigningInput)?;
    mac.update(b"GET/realtime");
    mac.update(expires_at_ms.to_string().as_bytes());
    Ok(SecretString::from(hex(&mac.finalize().into_bytes())))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
