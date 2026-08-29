mod config;
mod credentials;
mod models;
mod sign;

use std::str::FromStr;

use rust_decimal::Decimal;
use venue_domain::domain::{Amount, Asset, FieldState, Fill, OrderSide, Price, Symbol};

pub use config::{BybitConfig, endpoints};
pub use credentials::BybitCredentials;
pub use sign::{SignedHeaders, sign};

use models::{Envelope, ExecutionRow, Page};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitExecutionPage {
    pub next_cursor: Option<String>,
    pub fills: Vec<BybitFill>,
}

pub fn parse_execution_page(
    payload: &[u8],
    symbol: &Symbol,
    native_symbol: &str,
) -> Result<BybitExecutionPage, BybitError> {
    if native_symbol.is_empty() {
        return Err(BybitError::Binding);
    }
    let envelope: Envelope<Page<ExecutionRow>> =
        serde_json::from_slice(payload).map_err(|_| BybitError::Payload)?;
    if envelope.ret_code != 0 || envelope.result.category != "linear" {
        return Err(BybitError::Rejected);
    }
    let fills = envelope
        .result
        .list
        .into_iter()
        .map(|row| normalize_execution(row, symbol, native_symbol))
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor =
        (!envelope.result.next_page_cursor.is_empty()).then_some(envelope.result.next_page_cursor);
    Ok(BybitExecutionPage { next_cursor, fills })
}

fn normalize_execution(
    row: ExecutionRow,
    symbol: &Symbol,
    native_symbol: &str,
) -> Result<BybitFill, BybitError> {
    if row.symbol != native_symbol {
        return Err(BybitError::Binding);
    }
    if row.exec_type != "Trade" || row.order_id.is_empty() || row.exec_id.is_empty() {
        return Err(BybitError::Payload);
    }
    let side = match row.side.as_str() {
        "Buy" => OrderSide::Buy,
        "Sell" => OrderSide::Sell,
        _ => return Err(BybitError::Payload),
    };
    let quantity = decimal(&row.exec_qty)?;
    if !quantity.is_sign_positive() || quantity.is_zero() {
        return Err(BybitError::Payload);
    }
    let price = Price::new(decimal(&row.exec_price)?).map_err(|_| BybitError::Payload)?;
    let fee = if row.fee_currency.is_empty() {
        FieldState::Missing
    } else {
        FieldState::Known(Amount::new(
            Asset::new(&row.fee_currency).map_err(|_| BybitError::Payload)?,
            decimal(&row.exec_fee)?,
        ))
    };
    let exchange_time_ms = u64::from_str(&row.exec_time).map_err(|_| BybitError::Payload)?;
    if exchange_time_ms == 0 {
        return Err(BybitError::Payload);
    }
    let fill = Fill {
        fill_id: row.exec_id,
        execution_sequence: FieldState::Missing,
        order_id: row.order_id,
        symbol: symbol.clone(),
        side,
        position_side: FieldState::Missing,
        quantity,
        price,
        fee,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Missing,
        exchange_time_ms: Some(exchange_time_ms),
    };
    fill.validate().map_err(|_| BybitError::Payload)?;
    let client_order_id = if row.order_link_id.is_empty() {
        FieldState::Missing
    } else {
        FieldState::Known(row.order_link_id)
    };
    Ok(BybitFill {
        fill,
        client_order_id,
    })
}

fn decimal(value: &str) -> Result<Decimal, BybitError> {
    Decimal::from_str(value).map_err(|_| BybitError::Payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitError {
    #[error("Bybit credentials are unavailable or empty")]
    Credentials,
    #[error("Bybit signing input is invalid")]
    SigningInput,
    #[error("Bybit response payload is invalid or incomplete")]
    Payload,
    #[error("Bybit rejected the request")]
    Rejected,
    #[error("Bybit response does not match the fixed gateway binding")]
    Binding,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::GatewayMode;

    const EXECUTION_FIXTURE: &[u8] = include_bytes!("../fixtures/execution-trade-page.json");

    #[test]
    fn modes_select_only_testnet_or_live_endpoints() {
        let test = BybitConfig::for_mode(GatewayMode::Test);
        let live = BybitConfig::for_mode(GatewayMode::Live);
        assert_eq!(test.rest_origin, "https://api-testnet.bybit.com");
        assert_eq!(live.rest_origin, "https://api.bybit.com");
        assert_ne!(test.private_ws, live.private_ws);
    }

    #[test]
    fn signing_preserves_the_bybit_v5_fixed_vector() -> Result<(), BybitError> {
        let credentials = BybitCredentials::from_values("test", "secret")?;
        let headers = sign(
            &credentials,
            1_670_000_000_000,
            5_000,
            b"accountType=UNIFIED",
        )?;
        assert_eq!(
            headers.get("X-BAPI-SIGN"),
            Some("8ed52aa3777e158a21222a41d3f0d807d97753d6add49376c12241e0e77a2c9e")
        );
        assert_eq!(headers.get("X-BAPI-SIGN-TYPE"), Some("2"));
        Ok(())
    }

    #[test]
    fn signed_execution_fixture_preserves_all_identities() -> Result<(), Box<dyn std::error::Error>>
    {
        let symbol = "BTC/USDT".parse()?;
        let page = parse_execution_page(EXECUTION_FIXTURE, &symbol, "BTCUSDT")?;
        assert_eq!(page.next_cursor.as_deref(), Some("page=2"));
        assert_eq!(page.fills.len(), 3);
        assert_eq!(page.fills[0].fill.fill_id, "a");
        assert_eq!(
            page.fills[0].client_order_id,
            FieldState::Known("MANAGED_CLIENT_ID".to_owned())
        );
        assert_eq!(page.fills[2].fill.side, OrderSide::Sell);
        assert_eq!(page.fills[2].fill.position_side, FieldState::Missing);
        Ok(())
    }

    #[test]
    fn execution_fixture_rejects_a_different_native_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol = "BTC/USDT".parse()?;
        assert_eq!(
            parse_execution_page(EXECUTION_FIXTURE, &symbol, "ETHUSDT"),
            Err(BybitError::Binding)
        );
        Ok(())
    }
}
