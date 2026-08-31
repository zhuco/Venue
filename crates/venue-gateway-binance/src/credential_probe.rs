//! Bounded credential administration probe. Only signed GETs to fixed production origins are
//! possible here. Its result is not a private-stream session or an execution capability.

use crate::{
    BinanceCredentials, endpoints, private::parse_account_capabilities, sign::signature_for_payload,
};
use rust_decimal::Decimal;
use secrecy::ExposeSecret;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceProbeError {
    #[error("exchange credentials were rejected")]
    Credentials,
    #[error("required API permissions are absent or withdrawal permission is enabled")]
    Permissions,
    #[error("Binance Portfolio Margin UM with Hedge position mode is required")]
    AccountMode,
    #[error("exchange credential verification is unavailable")]
    Unavailable,
    #[error("exchange returned incomplete verification evidence")]
    Incomplete,
}

#[derive(Clone, Debug)]
pub struct BinanceCredentialProbe {
    /// Stable digest of the signed exchange identity, independent of the API key.
    pub account_identity_hash: [u8; 32],
    pub observed_ms: u64,
    pub has_exposure: bool,
}

const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_BYTES: usize = 512 * 1024;

pub async fn probe_credentials(
    credentials: &BinanceCredentials,
) -> Result<BinanceCredentialProbe, BinanceProbeError> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| BinanceProbeError::Unavailable)?;
    tokio::time::timeout(
        PROBE_TIMEOUT,
        probe(|surface| read(&client, credentials, surface)),
    )
    .await
    .map_err(|_| BinanceProbeError::Unavailable)?
}

async fn probe<F, Fut>(mut read_surface: F) -> Result<BinanceCredentialProbe, BinanceProbeError>
where
    F: FnMut(Surface) -> Fut,
    Fut: std::future::Future<Output = Result<Zeroizing<String>, BinanceProbeError>>,
{
    let permissions = read_surface(Surface::Permissions).await?;
    validate_permissions(&permissions)?;
    let identity = read_surface(Surface::Identity).await?;
    let identity_hash = identity_hash(&identity)?;
    let account = read_surface(Surface::Account).await?;
    let account: Value =
        serde_json::from_str(&account).map_err(|_| BinanceProbeError::Incomplete)?;
    if account.get("accountStatus").and_then(Value::as_str) != Some("NORMAL") {
        return Err(BinanceProbeError::AccountMode);
    }
    let config = read_surface(Surface::Config).await?;
    let mode = read_surface(Surface::PositionMode).await?;
    let capabilities =
        parse_account_capabilities(&config, &mode).map_err(|_| BinanceProbeError::Incomplete)?;
    if !capabilities.can_trade {
        return Err(BinanceProbeError::Permissions);
    }
    if !capabilities.hedge_position {
        return Err(BinanceProbeError::AccountMode);
    }
    let positions = read_surface(Surface::Positions).await?;
    let orders = read_surface(Surface::Orders).await?;
    let algos = read_surface(Surface::Algos).await?;
    let has_exposure = positions_have_exposure(&positions)?
        | orders_have_exposure(&orders)?
        | orders_have_exposure(&algos)?;
    Ok(BinanceCredentialProbe {
        account_identity_hash: identity_hash,
        observed_ms: now_ms()?,
        has_exposure,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Surface {
    Permissions,
    Identity,
    Account,
    Config,
    PositionMode,
    Positions,
    Orders,
    Algos,
}
impl Surface {
    fn target(&self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Permissions => (
                "https://api.binance.com",
                "/sapi/v1/account/apiRestrictions",
                "",
            ),
            Self::Identity => (
                "https://api.binance.com",
                "/api/v3/account",
                "omitZeroBalances=true&",
            ),
            Self::Account => ("https://papi.binance.com", endpoints::ACCOUNT, ""),
            Self::Config => ("https://papi.binance.com", endpoints::ACCOUNT_CONFIG, ""),
            Self::PositionMode => ("https://papi.binance.com", endpoints::POSITION_MODE, ""),
            Self::Positions => ("https://papi.binance.com", endpoints::POSITIONS, ""),
            Self::Orders => ("https://papi.binance.com", endpoints::OPEN_ORDERS, ""),
            Self::Algos => (
                "https://papi.binance.com",
                endpoints::OPEN_ALGO_ORDERS,
                "algoType=CONDITIONAL&",
            ),
        }
    }
}

async fn read(
    client: &reqwest::Client,
    credentials: &BinanceCredentials,
    surface: Surface,
) -> Result<Zeroizing<String>, BinanceProbeError> {
    let (origin, path, parameters) = surface.target();
    let payload = format!("{parameters}recvWindow=5000&timestamp={}", now_ms()?);
    let signature = Zeroizing::new(
        signature_for_payload(credentials, &payload).map_err(|_| BinanceProbeError::Credentials)?,
    );
    let url = Zeroizing::new(format!(
        "{origin}{path}?{payload}&signature={}",
        signature.as_str()
    ));
    let mut response = client
        .get(url.as_str())
        .header("X-MBX-APIKEY", credentials.api_key.expose_secret())
        .send()
        .await
        .map_err(|_| BinanceProbeError::Unavailable)?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|n| n > MAX_BYTES as u64)
    {
        return Err(BinanceProbeError::Incomplete);
    }
    let mut bytes = Zeroizing::new(Vec::new());
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| BinanceProbeError::Unavailable)?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_BYTES {
            return Err(BinanceProbeError::Incomplete);
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        let code = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|v| v.get("code").and_then(Value::as_i64));
        return Err(match code {
            Some(-2014 | -2015 | -1022) => BinanceProbeError::Credentials,
            _ if status.as_u16() == 403 => BinanceProbeError::Permissions,
            _ => BinanceProbeError::Unavailable,
        });
    }
    std::str::from_utf8(&bytes)
        .map(|v| Zeroizing::new(v.to_owned()))
        .map_err(|_| BinanceProbeError::Incomplete)
}

fn validate_permissions(payload: &str) -> Result<(), BinanceProbeError> {
    let v: Value = serde_json::from_str(payload).map_err(|_| BinanceProbeError::Incomplete)?;
    if v.get("enableReading").and_then(Value::as_bool) != Some(true)
        || v.get("enablePortfolioMarginTrading")
            .and_then(Value::as_bool)
            != Some(true)
        || v.get("enableWithdrawals").and_then(Value::as_bool) != Some(false)
    {
        return Err(BinanceProbeError::Permissions);
    }
    Ok(())
}

fn identity_hash(payload: &str) -> Result<[u8; 32], BinanceProbeError> {
    let v: Value = serde_json::from_str(payload).map_err(|_| BinanceProbeError::Incomplete)?;
    let uid = v
        .get("uid")
        .and_then(Value::as_u64)
        .filter(|v| *v > 0)
        .ok_or(BinanceProbeError::Incomplete)?;
    Ok(Sha256::digest(format!("binance-account:{uid}").as_bytes()).into())
}

fn positions_have_exposure(payload: &str) -> Result<bool, BinanceProbeError> {
    let v: Value = serde_json::from_str(payload).map_err(|_| BinanceProbeError::Incomplete)?;
    let rows = v.as_array().ok_or(BinanceProbeError::Incomplete)?;
    let mut nonzero = false;
    for row in rows {
        let amount = row
            .get("positionAmt")
            .and_then(Value::as_str)
            .ok_or(BinanceProbeError::Incomplete)?;
        nonzero |= !Decimal::from_str(amount)
            .map_err(|_| BinanceProbeError::Incomplete)?
            .is_zero();
    }
    Ok(nonzero)
}

fn orders_have_exposure(payload: &str) -> Result<bool, BinanceProbeError> {
    let v: Value = serde_json::from_str(payload).map_err(|_| BinanceProbeError::Incomplete)?;
    v.as_array()
        .map(|rows| !rows.is_empty())
        .ok_or(BinanceProbeError::Incomplete)
}

fn now_ms() -> Result<u64, BinanceProbeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .filter(|t| *t > 0)
        .ok_or(BinanceProbeError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(surface: Surface) -> &'static str {
        match surface {
            Surface::Permissions => {
                r#"{"enableReading":true,"enablePortfolioMarginTrading":true,"enableWithdrawals":false}"#
            }
            Surface::Identity => r#"{"uid":123456}"#,
            Surface::Account => r#"{"accountStatus":"NORMAL"}"#,
            Surface::Config => r#"{"canTrade":true}"#,
            Surface::PositionMode => r#"{"dualSidePosition":true}"#,
            Surface::Positions | Surface::Orders | Surface::Algos => "[]",
        }
    }
    #[tokio::test]
    async fn complete_probe_checks_every_signed_surface_and_never_accepts_partial_evidence()
    -> Result<(), BinanceProbeError> {
        let mut called = Vec::new();
        let result = probe(|surface| {
            called.push(surface);
            std::future::ready(Ok(Zeroizing::new(fixture(surface).into())))
        })
        .await?;
        assert_eq!(
            called,
            vec![
                Surface::Permissions,
                Surface::Identity,
                Surface::Account,
                Surface::Config,
                Surface::PositionMode,
                Surface::Positions,
                Surface::Orders,
                Surface::Algos
            ]
        );
        assert!(!result.has_exposure);
        for failed in called {
            let result = probe(|surface| {
                std::future::ready(if surface == failed {
                    Err(BinanceProbeError::Unavailable)
                } else {
                    Ok(Zeroizing::new(fixture(surface).into()))
                })
            })
            .await;
            assert_eq!(result.err(), Some(BinanceProbeError::Unavailable));
        }
        let result = probe(|surface| {
            std::future::ready(Ok(Zeroizing::new(
                if surface == Surface::PositionMode {
                    r#"{"dualSidePosition":false}"#
                } else {
                    fixture(surface)
                }
                .into(),
            )))
        })
        .await;
        assert_eq!(result.err(), Some(BinanceProbeError::AccountMode));
        for exposed in [Surface::Positions, Surface::Orders, Surface::Algos] {
            let result = probe(|surface| {
                std::future::ready(Ok(Zeroizing::new(
                    if surface == exposed {
                        r#"[{"positionAmt":"1","orderId":42}]"#
                    } else {
                        fixture(surface)
                    }
                    .into(),
                )))
            })
            .await?;
            assert!(result.has_exposure);
        }
        Ok(())
    }
    #[test]
    fn permissions_are_explicit_and_identity_is_not_the_key() -> Result<(), BinanceProbeError> {
        assert!(validate_permissions(r#"{"enableReading":true,"enablePortfolioMarginTrading":true,"enableWithdrawals":false}"#).is_ok());
        assert!(validate_permissions(r#"{"enableReading":true}"#).is_err());
        assert!(validate_permissions(r#"{"enableReading":true,"enablePortfolioMarginTrading":true,"enableWithdrawals":true}"#).is_err());
        assert_eq!(
            identity_hash(r#"{"uid":123,"other":1}"#)?,
            identity_hash(r#"{"uid":123}"#)?
        );
        assert_ne!(
            identity_hash(r#"{"uid":123}"#)?,
            identity_hash(r#"{"uid":124}"#)?
        );
        assert!(identity_hash(r#"{}"#).is_err());
        Ok(())
    }
    #[test]
    fn unknown_or_nonzero_exposure_never_proves_safe_deletion() -> Result<(), BinanceProbeError> {
        assert!(!positions_have_exposure(r#"[{"positionAmt":"0"}]"#)?);
        assert!(positions_have_exposure(r#"[{"positionAmt":"-0.001"}]"#)?);
        assert!(positions_have_exposure(r#"[{"positionAmt":"0"},{}]"#).is_err());
        assert!(orders_have_exposure("{}").is_err());
        assert!(orders_have_exposure("[{\"orderId\":1}]")?);
        Ok(())
    }
}
