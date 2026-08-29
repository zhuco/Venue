use hmac::{Hmac, Mac};
use secrecy::ExposeSecret;
use sha2::Sha256;

use crate::{BybitCredentials, BybitError};

pub struct SignedHeaders {
    entries: [(String, String); 5],
}

impl SignedHeaders {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    }
}

pub fn sign(
    credentials: &BybitCredentials,
    timestamp_ms: u64,
    recv_window_ms: u64,
    payload: &[u8],
) -> Result<SignedHeaders, BybitError> {
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
            ("X-BAPI-API-KEY".to_owned(), api_key.to_owned()),
            ("X-BAPI-SIGN".to_owned(), signature),
            ("X-BAPI-SIGN-TYPE".to_owned(), "2".to_owned()),
            ("X-BAPI-TIMESTAMP".to_owned(), timestamp),
            ("X-BAPI-RECV-WINDOW".to_owned(), recv_window),
        ],
    })
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
