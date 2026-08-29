mod binding;
mod config;
mod credentials;
mod models;
mod nonce;

use std::str::FromStr;

use rust_decimal::Decimal;
use venue_domain::domain::{
    Amount, Asset, FieldState, Fill, OrderSide, Price, Symbol, UnknownReason,
};
use venue_gateway_api::CapabilityFlags;

pub use binding::{HyperliquidGatewayBinding, HyperliquidGatewayBindingError};
pub use config::{HyperliquidConfig, endpoints};
pub use credentials::HyperliquidCredentials;
pub use nonce::{NonceCheckpoint, prepare_next_nonce};

use models::{EventEnvelope, UserFillRow, UserFillsData};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidUserFills {
    pub user_address: String,
    pub is_snapshot: bool,
    pub fills: Vec<HyperliquidFill>,
}

/// No account capability is advertised until authenticated readback, private stream,
/// EIP-712 signing, writer ownership, WAL, and UNKNOWN reconciliation are all connected.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

pub fn parse_private_user_fills(
    payload: &[u8],
    symbol: &Symbol,
    native_coin: &str,
    expected_user_address: &str,
) -> Result<HyperliquidUserFills, HyperliquidError> {
    if native_coin.is_empty() || !credentials::valid_address(expected_user_address) {
        return Err(HyperliquidError::Binding);
    }
    let events: Vec<EventEnvelope> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    let mut matching = events
        .into_iter()
        .filter(|event| event.channel == "userFills");
    let event = matching.next().ok_or(HyperliquidError::Payload)?;
    if matching.next().is_some() {
        return Err(HyperliquidError::Payload);
    }
    let data: UserFillsData =
        serde_json::from_value(event.data).map_err(|_| HyperliquidError::Payload)?;
    if !data.user.eq_ignore_ascii_case(expected_user_address) {
        return Err(HyperliquidError::Binding);
    }
    let fills = data
        .fills
        .into_iter()
        .map(|row| normalize_fill(row, symbol, native_coin, &data.user))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(HyperliquidUserFills {
        user_address: data.user.to_ascii_lowercase(),
        is_snapshot: data.is_snapshot,
        fills,
    })
}

fn normalize_fill(
    row: UserFillRow,
    symbol: &Symbol,
    native_coin: &str,
    user_address: &str,
) -> Result<HyperliquidFill, HyperliquidError> {
    if row.coin != native_coin || row.coin != symbol.base() || symbol.quote() != "USDC" {
        return Err(HyperliquidError::Binding);
    }
    if row.oid == 0 || row.tid == 0 || row.time == 0 {
        return Err(HyperliquidError::Payload);
    }
    let side = match row.side.as_str() {
        "B" => OrderSide::Buy,
        "A" => OrderSide::Sell,
        _ => return Err(HyperliquidError::Payload),
    };
    let quantity = decimal(&row.sz)?;
    if !quantity.is_sign_positive() || quantity.is_zero() {
        return Err(HyperliquidError::Payload);
    }
    let fee_asset = Asset::new(&row.fee_token).map_err(|_| HyperliquidError::Payload)?;
    let fee = Amount::new(fee_asset.clone(), decimal(&row.fee)?.abs());
    let realized_pnl = Amount::new(fee_asset, decimal(&row.closed_pnl)?);
    let fill = Fill {
        fill_id: format!(
            "hl:{}:{native_coin}:{}",
            user_address.to_ascii_lowercase(),
            row.tid
        ),
        execution_sequence: FieldState::Known(row.tid),
        order_id: row.oid.to_string(),
        symbol: symbol.clone(),
        side,
        position_side: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        quantity,
        price: Price::new(decimal(&row.px)?).map_err(|_| HyperliquidError::Payload)?,
        fee: FieldState::Known(fee),
        realized_pnl: FieldState::Known(realized_pnl),
        maker: row
            .crossed
            .map(|crossed| FieldState::Known(!crossed))
            .unwrap_or(FieldState::Missing),
        exchange_time_ms: Some(row.time),
    };
    fill.validate().map_err(|_| HyperliquidError::Payload)?;
    Ok(HyperliquidFill {
        fill,
        client_order_id: row
            .cloid
            .filter(|value| !value.is_empty())
            .map(FieldState::Known)
            .unwrap_or(FieldState::Missing),
    })
}

fn decimal(value: &str) -> Result<Decimal, HyperliquidError> {
    Decimal::from_str(value).map_err(|_| HyperliquidError::Payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HyperliquidError {
    #[error("Hyperliquid named Agent credentials are unavailable or invalid")]
    Credentials,
    #[error("Hyperliquid nonce state is invalid, mismatched, or exhausted")]
    Nonce,
    #[error("Hyperliquid private payload is invalid or incomplete")]
    Payload,
    #[error("Hyperliquid payload does not match the fixed account or instrument binding")]
    Binding,
    #[error(
        "Hyperliquid signing and mutation are unavailable until protocol dependencies and safety gates are approved"
    )]
    SigningUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayApiError, GatewayBinding, GatewayMode, VenueId};

    const PRIVATE_EVENTS: &[u8] = include_bytes!("../fixtures/private-account-events.json");
    const USER: &str = "0x0000000000000000000000000000000000000001";
    const AGENT: &str = "0x2222222222222222222222222222222222222222";

    fn binding(
        venue: VenueId,
        mode: GatewayMode,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            venue,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)
    }

    #[test]
    fn binding_and_config_accept_only_hyperliquid_test_or_live()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!("SHADOW".parse::<GatewayMode>().is_err());
        let test =
            HyperliquidGatewayBinding::new(binding(VenueId::Hyperliquid, GatewayMode::Test)?)?;
        let live =
            HyperliquidGatewayBinding::new(binding(VenueId::Hyperliquid, GatewayMode::Live)?)?;
        let test_config = HyperliquidConfig::for_binding(&test);
        let live_config = HyperliquidConfig::for_binding(&live);
        assert_eq!(test_config.mode(), GatewayMode::Test);
        assert_eq!(live_config.mode(), GatewayMode::Live);
        assert_eq!(
            test_config.rest_origin(),
            "https://api.hyperliquid-testnet.xyz"
        );
        assert_eq!(live_config.rest_origin(), "https://api.hyperliquid.xyz");
        assert_ne!(test_config.websocket(), live_config.websocket());
        assert_eq!(test.gateway_binding().symbol.to_string(), "BTC/USDC");
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn binding_rejects_wrong_venue_and_account() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            HyperliquidGatewayBinding::new(binding(VenueId::Bybit, GatewayMode::Live)?),
            Err(HyperliquidGatewayBindingError::Venue)
        );
        let invalid_account = GatewayBinding {
            venue: VenueId::Hyperliquid,
            mode: GatewayMode::Live,
            trading_account_id: "owner-address-is-not-an-account-uuid".to_owned(),
            symbol: "BTC/USDC".parse()?,
        };
        assert_eq!(
            HyperliquidGatewayBinding::new(invalid_account),
            Err(HyperliquidGatewayBindingError::Gateway(
                GatewayApiError::TradingAccountId
            ))
        );
        Ok(())
    }

    #[test]
    fn named_agent_credentials_reject_an_owner_declared_as_the_agent() {
        let result = HyperliquidCredentials::from_values(
            USER,
            USER,
            None,
            "venue-agent",
            USER,
            "11".repeat(32),
        );
        assert!(matches!(result, Err(HyperliquidError::Credentials)));
        let credential = HyperliquidCredentials::from_values(
            USER,
            USER,
            None,
            "venue-agent",
            AGENT,
            "11".repeat(32),
        );
        assert!(credential.is_ok());
        let vault_owner = HyperliquidCredentials::from_values(
            USER,
            USER,
            Some(AGENT.to_owned()),
            "venue-agent",
            AGENT.to_ascii_uppercase().replace("0X", "0x"),
            "11".repeat(32),
        );
        assert!(matches!(vault_owner, Err(HyperliquidError::Credentials)));
    }

    #[test]
    fn nonce_checkpoint_is_monotonic_and_bound_to_one_agent() -> Result<(), HyperliquidError> {
        let first = prepare_next_nonce(None, AGENT, 1_700_000_000_000)?;
        let recovered = serde_json::from_slice::<NonceCheckpoint>(
            &serde_json::to_vec(&first).map_err(|_| HyperliquidError::Nonce)?,
        )
        .map_err(|_| HyperliquidError::Nonce)?;
        let second = prepare_next_nonce(Some(&recovered), AGENT, 1_699_999_999_000)?;
        assert_eq!(second.last_nonce_ms, first.last_nonce_ms + 1);
        assert_eq!(
            prepare_next_nonce(Some(&second), USER, 1_700_000_000_002),
            Err(HyperliquidError::Nonce)
        );
        Ok(())
    }

    #[test]
    fn private_fill_fixture_preserves_identity_and_unknown_position_side()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol = "BTC/USDC".parse()?;
        let page = parse_private_user_fills(PRIVATE_EVENTS, &symbol, "BTC", USER)?;
        assert!(!page.is_snapshot);
        assert_eq!(page.fills.len(), 1);
        let item = &page.fills[0];
        assert_eq!(
            item.fill.fill_id,
            "hl:0x0000000000000000000000000000000000000001:BTC:5001"
        );
        assert_eq!(item.fill.order_id, "101");
        assert_eq!(item.fill.execution_sequence, FieldState::Known(5001));
        assert!(matches!(
            item.fill.position_side,
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted
            }
        ));
        assert_eq!(
            item.client_order_id,
            FieldState::Known("0x00000000000000000000000000000001".to_owned())
        );
        Ok(())
    }

    #[test]
    fn wrong_user_or_native_coin_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let symbol = "BTC/USDC".parse()?;
        assert_eq!(
            parse_private_user_fills(
                PRIVATE_EVENTS,
                &symbol,
                "BTC",
                "0x3333333333333333333333333333333333333333"
            ),
            Err(HyperliquidError::Binding)
        );
        assert_eq!(
            parse_private_user_fills(PRIVATE_EVENTS, &symbol, "ETH", USER),
            Err(HyperliquidError::Binding)
        );
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }
}
