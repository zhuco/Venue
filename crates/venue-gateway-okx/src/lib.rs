mod binding;
mod config;
mod credentials;
mod models;
mod sign;

use std::str::FromStr;

use rust_decimal::Decimal;
use venue_domain::domain::{
    Amount, Asset, FieldState, Fill, OrderSide, PositionSide, Price, Symbol,
};
use venue_gateway_api::CapabilityFlags;

pub use binding::{OkxGatewayBinding, OkxGatewayBindingError};
pub use config::{OkxConfig, endpoints};
pub use credentials::OkxCredentials;
pub use sign::{SignedHeaders, request_path, sign};

use models::{Envelope, FillRow};

/// No account capability is advertised until authenticated readback, private stream, writer,
/// WAL, and UNKNOWN reconciliation are all connected.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxPositionMode {
    Net,
    LongShort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

pub fn parse_fill_page(
    payload: &[u8],
    symbol: &Symbol,
    native_symbol: &str,
    position_mode: OkxPositionMode,
    base_quantity_per_contract: Decimal,
) -> Result<Vec<OkxFill>, OkxError> {
    if native_symbol.is_empty()
        || !base_quantity_per_contract.is_sign_positive()
        || base_quantity_per_contract.is_zero()
    {
        return Err(OkxError::Binding);
    }
    let envelope: Envelope<FillRow> =
        serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if envelope.code != "0" {
        return Err(OkxError::Rejected);
    }
    envelope
        .data
        .into_iter()
        .map(|row| {
            normalize_fill(
                row,
                symbol,
                native_symbol,
                position_mode,
                base_quantity_per_contract,
            )
        })
        .collect()
}

fn normalize_fill(
    row: FillRow,
    symbol: &Symbol,
    native_symbol: &str,
    position_mode: OkxPositionMode,
    base_quantity_per_contract: Decimal,
) -> Result<OkxFill, OkxError> {
    if row.inst_type != "SWAP" || row.inst_id != native_symbol {
        return Err(OkxError::Binding);
    }
    if row.bill_id.is_empty() || row.ord_id.is_empty() {
        return Err(OkxError::Payload);
    }
    let side = match row.side.as_str() {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        _ => return Err(OkxError::Payload),
    };
    let position_side = match (position_mode, row.pos_side.as_str()) {
        (OkxPositionMode::Net, "net") => PositionSide::Net,
        (OkxPositionMode::LongShort, "long") => PositionSide::Long,
        (OkxPositionMode::LongShort, "short") => PositionSide::Short,
        _ => return Err(OkxError::PositionMode),
    };
    let native_quantity = decimal(&row.fill_sz)?;
    if !native_quantity.is_sign_positive() || native_quantity.is_zero() {
        return Err(OkxError::Payload);
    }
    let quantity = native_quantity * base_quantity_per_contract;
    let price = Price::new(decimal(&row.fill_px)?).map_err(|_| OkxError::Payload)?;
    let fee = Amount::new(
        Asset::new(&row.fee_ccy).map_err(|_| OkxError::Payload)?,
        decimal(&row.fee)?.abs(),
    );
    let exchange_time_ms = u64::from_str(&row.ts).map_err(|_| OkxError::Payload)?;
    if exchange_time_ms == 0 {
        return Err(OkxError::Payload);
    }
    let fill = Fill {
        fill_id: row.bill_id,
        execution_sequence: FieldState::Missing,
        order_id: row.ord_id,
        symbol: symbol.clone(),
        side,
        position_side: FieldState::Known(position_side),
        quantity,
        price,
        fee: FieldState::Known(fee),
        realized_pnl: FieldState::Missing,
        maker: FieldState::Missing,
        exchange_time_ms: Some(exchange_time_ms),
    };
    fill.validate().map_err(|_| OkxError::Payload)?;
    let client_order_id = if row.cl_ord_id.is_empty() {
        FieldState::Missing
    } else {
        FieldState::Known(row.cl_ord_id)
    };
    Ok(OkxFill {
        fill,
        client_order_id,
    })
}

fn decimal(value: &str) -> Result<Decimal, OkxError> {
    Decimal::from_str(value).map_err(|_| OkxError::Payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OkxError {
    #[error("OKX credentials are unavailable or empty")]
    Credentials,
    #[error("OKX signing input is invalid")]
    SigningInput,
    #[error("OKX response payload is invalid or incomplete")]
    Payload,
    #[error("OKX rejected the request")]
    Rejected,
    #[error("OKX response does not match the fixed gateway binding")]
    Binding,
    #[error("OKX response position side does not match the verified account mode")]
    PositionMode,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    const FILL_FIXTURE: &[u8] = include_bytes!("../fixtures/fills-history-page.json");

    fn config(mode: GatewayMode) -> Result<OkxConfig, Box<dyn std::error::Error>> {
        Ok(OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?)
    }

    #[test]
    fn one_binding_selects_only_its_test_or_live_transport()
    -> Result<(), Box<dyn std::error::Error>> {
        let test = config(GatewayMode::Test)?;
        let live = config(GatewayMode::Live)?;
        assert!(test.simulated_trading());
        assert!(!live.simulated_trading());
        assert!(test.private_ws().contains("wspap.okx.com"));
        assert!(live.private_ws().contains("ws.okx.com"));
        assert_eq!(test.gateway_binding().mode, GatewayMode::Test);
        assert_eq!(live.gateway_binding().mode, GatewayMode::Live);
        assert_eq!(test.gateway_binding().symbol.to_string(), "BTC/USDT");
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn signing_preserves_the_okx_fixed_vector() -> Result<(), OkxError> {
        let credentials = OkxCredentials::from_values("key", "mysecret", "pass")?;
        let config = config(GatewayMode::Test).map_err(|_| OkxError::Binding)?;
        let headers = sign(
            &credentials,
            &config,
            "2020-12-08T09:08:57.715Z",
            "GET",
            "/api/v5/account/balance",
            &[],
        )?;
        assert_eq!(
            headers.get("OK-ACCESS-SIGN"),
            Some("7dqjFHmbJfEEOQc+0wMh6KyqlUAh5C2x6vqL7qZTilE=")
        );
        assert_eq!(headers.get("x-simulated-trading"), Some("1"));
        Ok(())
    }

    #[test]
    fn live_signing_omits_simulated_trading_header() -> Result<(), OkxError> {
        let credentials = OkxCredentials::from_values("key", "mysecret", "pass")?;
        let config = config(GatewayMode::Live).map_err(|_| OkxError::Binding)?;
        let headers = sign(
            &credentials,
            &config,
            "2020-12-08T09:08:57.715Z",
            "GET",
            "/api/v5/account/balance",
            &[],
        )?;
        assert_eq!(headers.get("x-simulated-trading"), None);
        Ok(())
    }

    #[test]
    fn signed_fill_fixture_preserves_ids_and_converts_contracts()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol = "BTC/USDT".parse()?;
        let fills = parse_fill_page(
            FILL_FIXTURE,
            &symbol,
            "BTC-USDT-SWAP",
            OkxPositionMode::LongShort,
            Decimal::new(1, 2),
        )?;
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].fill.fill_id, "9002");
        assert_eq!(fills[0].fill.quantity, Decimal::new(2, 2));
        assert_eq!(
            fills[0].fill.position_side,
            FieldState::Known(PositionSide::Long)
        );
        assert_eq!(
            fills[0].client_order_id,
            FieldState::Known("0123456789abcdef0123456789abcdef".to_owned())
        );
        assert_eq!(
            fills[1].fill.position_side,
            FieldState::Known(PositionSide::Short)
        );
        Ok(())
    }

    #[test]
    fn fill_fixture_rejects_wrong_binding_or_account_mode() -> Result<(), Box<dyn std::error::Error>>
    {
        let symbol = "BTC/USDT".parse()?;
        assert_eq!(
            parse_fill_page(
                FILL_FIXTURE,
                &symbol,
                "ETH-USDT-SWAP",
                OkxPositionMode::LongShort,
                Decimal::ONE,
            ),
            Err(OkxError::Binding)
        );
        assert_eq!(
            parse_fill_page(
                FILL_FIXTURE,
                &symbol,
                "BTC-USDT-SWAP",
                OkxPositionMode::Net,
                Decimal::ONE,
            ),
            Err(OkxError::PositionMode)
        );
        Ok(())
    }
}
