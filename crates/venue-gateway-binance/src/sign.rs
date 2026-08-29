use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;
use venue_gateway_api::GatewayBinding;

use crate::{BinanceAuthError, BinanceConfig, BinanceCredentials};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceHttpMethod {
    Get,
    Post,
    Delete,
    Put,
}

impl BinanceHttpMethod {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Delete => "DELETE",
            Self::Put => "PUT",
        }
    }
}

pub struct BinanceRestSignInput<'a> {
    pub binding: &'a GatewayBinding,
    pub method: BinanceHttpMethod,
    pub path: &'a str,
    pub parameters: &'a [(&'a str, &'a str)],
    pub recv_window_ms: u32,
    pub timestamp_ms: u64,
}

/// Opaque signed material. It deliberately implements neither `Debug` nor `Display`; only a
/// crate-local transport may expose the API key and signed query to an HTTP library.
pub struct SignedBinanceRestRequest {
    method: BinanceHttpMethod,
    origin: &'static str,
    path: String,
    api_key: SecretString,
    query: SecretString,
}

impl SignedBinanceRestRequest {
    #[must_use]
    pub const fn method(&self) -> BinanceHttpMethod {
        self.method
    }

    #[must_use]
    pub const fn origin(&self) -> &'static str {
        self.origin
    }

    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Confirms that the opaque authorization material survived construction without returning
    /// either the API key or signed query to callers outside this adapter.
    #[must_use]
    pub fn authentication_material_is_present(&self) -> bool {
        !self.api_key.expose_secret().is_empty()
            && self
                .query
                .expose_secret()
                .rsplit_once("&signature=")
                .is_some_and(|(payload, signature)| !payload.is_empty() && signature.len() == 64)
    }

    pub(crate) fn api_key(&self) -> &str {
        self.api_key.expose_secret()
    }

    pub(crate) fn query(&self) -> &str {
        self.query.expose_secret()
    }
}

pub fn sign_rest(
    credentials: &BinanceCredentials,
    config: &BinanceConfig,
    input: &BinanceRestSignInput<'_>,
) -> Result<SignedBinanceRestRequest, BinanceAuthError> {
    config.validate_binding(input.binding)?;
    validate_input(input)?;

    let mut pairs = Vec::with_capacity(input.parameters.len().saturating_add(2));
    for (key, value) in input.parameters {
        pairs.push(format!(
            "{}={}",
            encode_component(key),
            encode_component(value)
        ));
    }
    pairs.push(format!("recvWindow={}", input.recv_window_ms));
    pairs.push(format!("timestamp={}", input.timestamp_ms));
    let payload = pairs.join("&");
    let mut mac = HmacSha256::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| BinanceAuthError::SigningInput)?;
    mac.update(payload.as_bytes());
    let signature = hex(&mac.finalize().into_bytes());
    Ok(SignedBinanceRestRequest {
        method: input.method,
        origin: config.portfolio_rest_origin(),
        path: input.path.to_owned(),
        api_key: SecretString::from(credentials.api_key.expose_secret().to_owned()),
        query: SecretString::from(format!("{payload}&signature={signature}")),
    })
}

fn validate_input(input: &BinanceRestSignInput<'_>) -> Result<(), BinanceAuthError> {
    if input.timestamp_ms == 0
        || !(1..=60_000).contains(&input.recv_window_ms)
        || !input.path.starts_with("/papi/")
        || input.path.contains('?')
        || input.path.contains('#')
        || !input.path.is_ascii()
    {
        return Err(BinanceAuthError::SigningInput);
    }
    let mut keys = std::collections::BTreeSet::new();
    for (key, _) in input.parameters {
        if key.is_empty()
            || matches!(*key, "recvWindow" | "timestamp" | "signature")
            || !keys.insert(*key)
        {
            return Err(BinanceAuthError::SigningInput);
        }
    }
    Ok(())
}

fn encode_component(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

#[cfg(test)]
mod tests {
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;
    use crate::BinanceAccountBinding;

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Binance,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "LTC/BTC".parse()?,
        )?)
    }

    #[test]
    fn rest_hmac_matches_the_reviewed_binance_fixed_vector()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Live)?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let credentials = BinanceCredentials::from_values(
            "vmPUZE6mv8Yx48nmP9b1fBr5m3hjV8o2pHKZr9B7L6wK4C6zZ9U6rA5bM4rJ8eG",
            "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
        )?;
        let parameters = [
            ("symbol", "LTCBTC"),
            ("side", "BUY"),
            ("type", "LIMIT"),
            ("timeInForce", "GTC"),
            ("quantity", "1"),
            ("price", "0.1"),
        ];
        let request = sign_rest(
            &credentials,
            &config,
            &BinanceRestSignInput {
                binding: &binding,
                method: BinanceHttpMethod::Post,
                path: "/papi/v1/um/order",
                parameters: &parameters,
                recv_window_ms: 5_000,
                timestamp_ms: 1_499_827_319_559,
            },
        )?;
        assert_eq!(request.method(), BinanceHttpMethod::Post);
        assert_eq!(request.origin(), "https://papi.binance.com");
        assert_eq!(request.path(), "/papi/v1/um/order");
        assert!(request.authentication_material_is_present());
        assert_eq!(
            request.api_key.expose_secret(),
            credentials.api_key.expose_secret()
        );
        assert_eq!(
            request.query.expose_secret(),
            "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559&signature=c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
        Ok(())
    }

    #[test]
    fn signing_percent_encodes_exact_bytes_and_preserves_parameter_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Test)?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let parameters = [("client", "a/b c"), ("symbol", "LTCBTC")];
        let request = sign_rest(
            &credentials,
            &config,
            &BinanceRestSignInput {
                binding: &binding,
                method: BinanceHttpMethod::Get,
                path: "/papi/v1/um/order",
                parameters: &parameters,
                recv_window_ms: 1,
                timestamp_ms: 2,
            },
        )?;
        assert!(
            request
                .query
                .expose_secret()
                .starts_with("client=a%2Fb%20c&symbol=LTCBTC&recvWindow=1&timestamp=2&signature=")
        );
        Ok(())
    }

    #[test]
    fn signing_rejects_cross_binding_and_ambiguous_inputs() -> Result<(), Box<dyn std::error::Error>>
    {
        let binding = binding(GatewayMode::Live)?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let other_account = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000002",
            "LTC/BTC".parse()?,
        )?;
        let other_mode = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Test,
            "00000000-0000-4000-8000-000000000001",
            "LTC/BTC".parse()?,
        )?;
        let other_symbol = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        let reserved = [("timestamp", "1")];
        for (scope, path, parameters, recv_window_ms, timestamp_ms, expected) in [
            (
                &other_account,
                "/papi/v1/um/order",
                &[][..],
                5_000,
                1,
                BinanceAuthError::Binding,
            ),
            (
                &other_mode,
                "/papi/v1/um/order",
                &[][..],
                5_000,
                1,
                BinanceAuthError::Binding,
            ),
            (
                &other_symbol,
                "/papi/v1/um/order",
                &[][..],
                5_000,
                1,
                BinanceAuthError::Binding,
            ),
            (
                &binding,
                "/fapi/v1/order",
                &[][..],
                5_000,
                1,
                BinanceAuthError::SigningInput,
            ),
            (
                &binding,
                "/papi/v1/um/order?x=1",
                &[][..],
                5_000,
                1,
                BinanceAuthError::SigningInput,
            ),
            (
                &binding,
                "/papi/v1/um/order",
                &reserved[..],
                5_000,
                1,
                BinanceAuthError::SigningInput,
            ),
            (
                &binding,
                "/papi/v1/um/order",
                &[][..],
                60_001,
                1,
                BinanceAuthError::SigningInput,
            ),
            (
                &binding,
                "/papi/v1/um/order",
                &[][..],
                5_000,
                0,
                BinanceAuthError::SigningInput,
            ),
        ] {
            let result = sign_rest(
                &credentials,
                &config,
                &BinanceRestSignInput {
                    binding: scope,
                    method: BinanceHttpMethod::Get,
                    path,
                    parameters,
                    recv_window_ms,
                    timestamp_ms,
                },
            );
            assert!(matches!(result, Err(error) if error == expected));
        }
        Ok(())
    }
}
