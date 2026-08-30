use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;

use crate::{BitgetConfig, BitgetCredentials, BitgetError};

pub struct SignInput<'a> {
    pub timestamp_ms: u64,
    pub method: &'a str,
    pub request_path: &'a str,
    pub query: &'a str,
    pub body: &'a [u8],
}

pub struct SignedHeaders {
    entries: [(String, SecretString); 6],
}

impl SignedHeaders {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.expose_secret())
    }
}

pub fn prehash(input: &SignInput<'_>) -> Result<Vec<u8>, BitgetError> {
    if input.timestamp_ms == 0
        || input.method.is_empty()
        || !input.method.bytes().all(|byte| byte.is_ascii_alphabetic())
        || !input.request_path.starts_with('/')
        || input.request_path.contains('?')
        || input.query.starts_with('?')
    {
        return Err(BitgetError::SigningInput);
    }
    let mut value = input.timestamp_ms.to_string().into_bytes();
    value.extend(input.method.bytes().map(|byte| byte.to_ascii_uppercase()));
    value.extend_from_slice(input.request_path.as_bytes());
    if !input.query.is_empty() {
        value.push(b'?');
        value.extend_from_slice(input.query.as_bytes());
    }
    value.extend_from_slice(input.body);
    Ok(value)
}

pub fn sign(
    credentials: &BitgetCredentials,
    _config: &BitgetConfig,
    input: &SignInput<'_>,
) -> Result<SignedHeaders, BitgetError> {
    let prehash = prehash(input)?;
    let api_key = credentials.api_key.expose_secret();
    let passphrase = credentials.passphrase.expose_secret();
    let mut mac = Hmac::<Sha256>::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| BitgetError::SigningInput)?;
    mac.update(&prehash);
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    Ok(SignedHeaders {
        entries: [
            (
                "ACCESS-KEY".to_owned(),
                SecretString::from(api_key.to_owned()),
            ),
            ("ACCESS-SIGN".to_owned(), SecretString::from(signature)),
            (
                "ACCESS-TIMESTAMP".to_owned(),
                SecretString::from(input.timestamp_ms.to_string()),
            ),
            (
                "ACCESS-PASSPHRASE".to_owned(),
                SecretString::from(passphrase.to_owned()),
            ),
            (
                "Content-Type".to_owned(),
                SecretString::from("application/json".to_owned()),
            ),
            ("locale".to_owned(), SecretString::from("en-US".to_owned())),
        ],
    })
}

/// Private WebSocket login uses seconds and the fixed `GET/user/verify` prehash.
pub fn ws_sign(credentials: &BitgetCredentials, timestamp_s: u64) -> Result<String, BitgetError> {
    if timestamp_s == 0 {
        return Err(BitgetError::SigningInput);
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| BitgetError::SigningInput)?;
    mac.update(format!("{timestamp_s}GET/user/verify").as_bytes());
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use venue_gateway_api::GatewayMode;

    use super::*;

    fn credentials() -> Result<BitgetCredentials, BitgetError> {
        BitgetCredentials::from_values("key", "secret", "pass")
    }

    #[test]
    fn rest_signature_matches_the_reviewed_fixed_vector() -> Result<(), BitgetError> {
        let input = SignInput {
            timestamp_ms: 1_627_366_780_545,
            method: "get",
            request_path: "/api/v3/account/fee-rate",
            query: "category=SPOT&symbol=BTCUSDT",
            body: b"",
        };
        assert_eq!(
            prehash(&input)?,
            b"1627366780545GET/api/v3/account/fee-rate?category=SPOT&symbol=BTCUSDT"
        );
        let headers = sign(
            &credentials()?,
            &BitgetConfig::for_mode(GatewayMode::Live),
            &input,
        )?;
        assert_eq!(
            headers.get("ACCESS-SIGN"),
            Some("Cnzpvm2X8kzdlPnV+DENDS3HIuU/jZq4eJknd1s0vfQ=")
        );
        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert!(headers.get("paptrading").is_none());
        Ok(())
    }

    #[test]
    fn websocket_signature_matches_the_reviewed_fixed_vector() -> Result<(), BitgetError> {
        assert_eq!(
            ws_sign(&credentials()?, 1_700_000_000)?,
            "asp8h2LSGzNFWF9BshQJj0WiZA5uDIWsAk9FCfz2Ilk="
        );
        Ok(())
    }

    #[test]
    fn ambiguous_or_incomplete_signing_inputs_fail_closed() {
        for input in [
            SignInput {
                timestamp_ms: 0,
                method: "GET",
                request_path: "/api/v3/account/info",
                query: "",
                body: b"",
            },
            SignInput {
                timestamp_ms: 1,
                method: "GET",
                request_path: "/api/v3/account/info?coin=USDT",
                query: "coin=USDT",
                body: b"",
            },
            SignInput {
                timestamp_ms: 1,
                method: "GET",
                request_path: "/api/v3/account/info",
                query: "?coin=USDT",
                body: b"",
            },
        ] {
            assert_eq!(prehash(&input), Err(BitgetError::SigningInput));
        }
    }
}
