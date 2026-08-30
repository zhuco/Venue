use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha512};

use crate::{GateCredentials, GateProtocolError, endpoints};

pub struct GateRestSignedHeaders {
    entries: [(String, SecretString); 3],
}

impl GateRestSignedHeaders {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.expose_secret())
    }
}

/// Signs the exact bytes sent by a Gate REST request.
pub fn sign_rest(
    credentials: &GateCredentials,
    timestamp_sec: i64,
    method: &str,
    endpoint: &str,
    query: &str,
    body: &[u8],
) -> Result<GateRestSignedHeaders, GateProtocolError> {
    if timestamp_sec <= 0
        || method.is_empty()
        || !method.bytes().all(|byte| byte.is_ascii_uppercase())
        || query.starts_with('?')
    {
        return Err(GateProtocolError::SigningInput);
    }
    let path = endpoints::canonical_rest_path(endpoint)?;
    let body_hash = hex(&Sha512::digest(body));
    let canonical = format!("{method}\n{path}\n{query}\n{body_hash}\n{timestamp_sec}");
    let signature = signature(credentials, canonical.as_bytes())?;
    Ok(GateRestSignedHeaders {
        entries: [
            (
                "KEY".to_owned(),
                SecretString::from(credentials.api_key.expose_secret().to_owned()),
            ),
            (
                "Timestamp".to_owned(),
                SecretString::from(timestamp_sec.to_string()),
            ),
            ("SIGN".to_owned(), SecretString::from(signature)),
        ],
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatePrivateChannel {
    Orders,
    UserTrades,
    Positions,
    Balances,
}

impl GatePrivateChannel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Orders => "futures.orders",
            Self::UserTrades => "futures.usertrades",
            Self::Positions => "futures.positions",
            Self::Balances => "futures.balances",
        }
    }
}

pub struct GateWebSocketAuth {
    api_key: SecretString,
    signature: SecretString,
}

impl GateWebSocketAuth {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        if name.eq_ignore_ascii_case("KEY") {
            Some(self.api_key.expose_secret())
        } else if name.eq_ignore_ascii_case("SIGN") {
            Some(self.signature.expose_secret())
        } else if name.eq_ignore_ascii_case("method") {
            Some("api_key")
        } else {
            None
        }
    }
}

/// Signs one authenticated USDT-futures subscription without constructing a socket message.
pub fn sign_websocket_subscription(
    credentials: &GateCredentials,
    channel: GatePrivateChannel,
    timestamp_sec: i64,
) -> Result<GateWebSocketAuth, GateProtocolError> {
    if timestamp_sec <= 0 {
        return Err(GateProtocolError::SigningInput);
    }
    let canonical = format!(
        "channel={}&event=subscribe&time={timestamp_sec}",
        channel.as_str()
    );
    Ok(GateWebSocketAuth {
        api_key: SecretString::from(credentials.api_key.expose_secret().to_owned()),
        signature: SecretString::from(signature(credentials, canonical.as_bytes())?),
    })
}

fn signature(credentials: &GateCredentials, canonical: &[u8]) -> Result<String, GateProtocolError> {
    let mut mac = Hmac::<Sha512>::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| GateProtocolError::SigningInput)?;
    mac.update(canonical);
    Ok(hex(&mac.finalize().into_bytes()))
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

#[cfg(test)]
mod tests {
    use venue_gateway_api::GatewayMode;

    use super::*;
    use crate::{GateConfig, GateProductScope, endpoints};

    #[test]
    fn live_selects_current_official_production_origins() {
        let live = GateConfig::for_mode(GatewayMode::Live);
        assert_eq!(live.rest_origin(), "https://api.gateio.ws/api/v4");
        assert_eq!(live.usdt_futures_ws(), "wss://fx-ws.gateio.ws/v4/ws/usdt");
        assert_eq!(live.mode(), GatewayMode::Live);
        assert_eq!(
            live.rest_url(endpoints::FUTURES_ORDER),
            Ok("https://api.gateio.ws/api/v4/futures/usdt/orders".to_owned())
        );
    }

    #[test]
    fn only_usdt_perpetual_scope_is_admitted() -> Result<(), GateProtocolError> {
        let scope = GateProductScope::usdt_perpetual("usdt", false)?;
        assert_eq!(scope.settlement(), "usdt");
        assert_eq!(
            GateProductScope::usdt_perpetual("btc", false),
            Err(GateProtocolError::ProductScope)
        );
        assert_eq!(
            GateProductScope::usdt_perpetual("usdt", true),
            Err(GateProtocolError::ProductScope)
        );
        assert!(endpoints::FUTURES_ORDER.starts_with("/futures/usdt/"));
        assert!(endpoints::FUTURES_FILLS.starts_with("/futures/usdt/"));
        Ok(())
    }

    #[test]
    fn rest_signing_preserves_the_gate_official_fixed_vector() -> Result<(), GateProtocolError> {
        let credentials = GateCredentials::from_values("key", "secret")?;
        let headers = sign_rest(
            &credentials,
            1_541_993_715,
            "GET",
            "/futures/orders",
            "contract=BTC_USD&status=finished&limit=50",
            &[],
        )?;
        assert_eq!(
            headers.get("SIGN"),
            Some(
                "55f84ea195d6fe57ce62464daaa7c3c02fa9d1dde954e4c898289c9a2407a3d6fb3faf24deff16790d726b66ac9f74526668b13bd01029199cc4fcc522418b8a"
            )
        );
        assert_eq!(headers.get("KEY"), Some("key"));
        assert_eq!(headers.get("Timestamp"), Some("1541993715"));
        Ok(())
    }

    #[test]
    fn websocket_signing_preserves_a_fixed_subscription_vector() -> Result<(), GateProtocolError> {
        let credentials = GateCredentials::from_values("key", "secret")?;
        let auth =
            sign_websocket_subscription(&credentials, GatePrivateChannel::Orders, 1_541_993_715)?;
        assert_eq!(auth.get("method"), Some("api_key"));
        assert_eq!(auth.get("KEY"), Some("key"));
        assert_eq!(
            auth.get("SIGN"),
            Some(
                "4cdab02f21aba635fce8684a050806325cb4aa74a93d00c39f2084da73614d2e1d25878ca7c9ebcbde9541cddfc5ae36b1ccde10982eb82fd09f7a30a6d43d84"
            )
        );
        Ok(())
    }

    #[test]
    fn signing_rejects_noncanonical_inputs_and_empty_credentials() {
        assert!(GateCredentials::from_values("", "secret").is_err());
        let credentials = GateCredentials::from_values("key", "secret");
        if let Ok(credentials) = credentials {
            assert_eq!(
                sign_rest(&credentials, 1, "get", "/futures/orders", "", &[]).err(),
                Some(GateProtocolError::SigningInput)
            );
            assert_eq!(
                sign_rest(&credentials, 1, "GET", "/api/v4/futures/orders", "", &[]).err(),
                Some(GateProtocolError::SigningInput)
            );
            assert_eq!(
                sign_websocket_subscription(&credentials, GatePrivateChannel::Orders, 0).err(),
                Some(GateProtocolError::SigningInput)
            );
        }
    }
}
