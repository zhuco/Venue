use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::{Map, Value};

use venue_domain::domain::{
    AccountBalance, AccountRiskSnapshot, Asset, LegRiskSnapshot, Position, PositionSide, Price,
    RiskSourceStatus, Symbol, validate_risk_snapshot_pair,
};

use crate::private::{PrivateAccountCapabilities, PrivateParseError};

const POSITION_NOTIONAL_SCALE: u32 = 8;

/// Signed/authoritative quote-to-USD evidence required because Portfolio Margin account equity is
/// USD while UM position values are reported in the contract quote asset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsdConversionEvidence {
    pub asset: Asset,
    pub usd_per_asset: Decimal,
    pub private_generation: u64,
    pub observed_at_ms: u64,
    pub source_time_ms: u64,
}

/// Parses Binance's Portfolio Margin asset-index response and binds its native timestamp to the
/// same local observation used by the signed account/position generation.
pub fn parse_usd_conversion_evidence(
    payload: &str,
    asset: Asset,
    private_generation: u64,
    observed_at_ms: u64,
    max_age_ms: u64,
) -> Result<UsdConversionEvidence, PrivateParseError> {
    let root: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let item = match &root {
        Value::Object(item) => item,
        Value::Array(items) if items.len() == 1 => {
            items[0].as_object().ok_or(PrivateParseError::Payload)?
        }
        Value::Array(_) => return Err(PrivateParseError::CurrencyEvidence),
        _ => return Err(PrivateParseError::Payload),
    };
    if private_generation == 0 || observed_at_ms == 0 {
        return Err(PrivateParseError::CurrencyEvidence);
    }
    let source_time_ms = item
        .get("time")
        .and_then(Value::as_u64)
        .ok_or(PrivateParseError::CurrencyEvidence)?;
    if source_time_ms == 0
        || source_time_ms > observed_at_ms
        || observed_at_ms.saturating_sub(source_time_ms) > max_age_ms
    {
        return Err(PrivateParseError::CurrencyEvidence);
    }
    // The current USD-M endpoint identifies a price as `SYMBOLUSD` and calls its value `index`.
    // Keep the older PAPI `asset`/`assetIndexPrice` shape readable for frozen risk replay only;
    // a public risk read below uses the documented first branch.
    let expected_symbol = format!("{}USD", asset.as_str());
    let usd_per_asset =
        if item.get("symbol").and_then(Value::as_str) == Some(expected_symbol.as_str()) {
            decimal(item, "index")?
        } else if item.get("asset").and_then(Value::as_str) == Some(asset.as_str()) {
            decimal(item, "assetIndexPrice")?
        } else {
            return Err(PrivateParseError::CurrencyEvidence);
        };
    if usd_per_asset <= Decimal::ZERO {
        return Err(PrivateParseError::CurrencyEvidence);
    }
    Ok(UsdConversionEvidence {
        asset,
        usd_per_asset,
        private_generation,
        observed_at_ms,
        source_time_ms,
    })
}

/// PAPI exposes UM trading permission and position mode through separate signed account reads.
/// They are both required because neither one can safely be inferred from the other.
pub fn capabilities(
    account_config_payload: &str,
    position_mode_payload: &str,
) -> Result<PrivateAccountCapabilities, PrivateParseError> {
    crate::private::parse_account_capabilities(account_config_payload, position_mode_payload)
}

/// PAPI's account-wide risk values are the authority for a Portfolio Margin deployment. The UM
/// account payload supplies positions but does not carry an equivalent available-balance field.
pub fn parse_account_balance(payload: &str) -> Result<AccountBalance, PrivateParseError> {
    let root: Value = serde_json::from_str(payload).map_err(|_| PrivateParseError::Payload)?;
    let item = root.as_object().ok_or(PrivateParseError::Payload)?;
    let result = AccountBalance {
        asset: "USDT"
            .parse::<Asset>()
            .map_err(|_| PrivateParseError::Payload)?,
        wallet_balance: decimal(item, "accountEquity")?,
        available_balance: decimal(item, "totalAvailableBalance")?,
        initial_margin: decimal(item, "accountInitialMargin")?,
        maintenance_margin: decimal(item, "accountMaintMargin")?,
    };
    result.validate().map_err(PrivateParseError::Account)?;
    Ok(result)
}

/// A symbol-scoped position-risk response omits flat Hedge legs. After signed Hedge-mode
/// verification, absence inside that exact symbol scope is authoritative zero exposure.
pub fn complete_scoped_positions(
    mut positions: Vec<Position>,
    symbol: &Symbol,
    hedge_position: bool,
) -> Vec<Position> {
    if hedge_position {
        for side in [PositionSide::Long, PositionSide::Short] {
            if !positions.iter().any(|position| position.side == side) {
                positions.push(Position {
                    symbol: symbol.clone(),
                    side,
                    quantity: Decimal::ZERO,
                    entry_price: None,
                    mark_price: None,
                });
            }
        }
    }
    positions
}

/// Produces one same-generation account/leg risk observation. Stablecoin parity is never assumed:
/// SOL/USDC and USDT contracts require explicit quote-to-USD evidence.
#[expect(
    clippy::too_many_arguments,
    reason = "the signed payloads, binding scope, generation, clock, and currency evidence form one indivisible risk tuple"
)]
pub fn parse_risk_snapshots(
    account_payload: &str,
    positions_payload: &str,
    symbol: &Symbol,
    account: &str,
    capabilities: PrivateAccountCapabilities,
    private_generation: u64,
    observed_at_ms: u64,
    conversion: Option<&UsdConversionEvidence>,
) -> Result<(AccountRiskSnapshot, Vec<LegRiskSnapshot>), PrivateParseError> {
    if !capabilities.can_trade || !capabilities.hedge_position {
        return Err(PrivateParseError::Capability);
    }
    let usd: Asset = "USD".parse().map_err(|_| PrivateParseError::Payload)?;
    let quote: Asset = symbol
        .quote()
        .parse()
        .map_err(|_| PrivateParseError::Payload)?;
    let usd_per_quote = if quote.as_str() == "USD" {
        Decimal::ONE
    } else {
        let evidence = conversion.ok_or(PrivateParseError::CurrencyEvidence)?;
        if evidence.asset != quote
            || evidence.usd_per_asset <= Decimal::ZERO
            || evidence.private_generation != private_generation
            || evidence.observed_at_ms != observed_at_ms
        {
            return Err(PrivateParseError::CurrencyEvidence);
        }
        evidence.usd_per_asset
    };
    let account_root: Value =
        serde_json::from_str(account_payload).map_err(|_| PrivateParseError::Payload)?;
    let account_item = account_root.as_object().ok_or(PrivateParseError::Payload)?;
    let account_snapshot = AccountRiskSnapshot {
        exchange: "binance".to_owned(),
        account: account.to_owned(),
        risk_currency: usd.clone(),
        account_equity: decimal(account_item, "accountEquity")?,
        private_generation,
        observed_at_ms,
        source_status: RiskSourceStatus::Complete,
    };
    let positions: Value =
        serde_json::from_str(positions_payload).map_err(|_| PrivateParseError::Payload)?;
    let positions = positions.as_array().ok_or(PrivateParseError::Payload)?;
    let mut legs = Vec::new();
    for value in positions {
        let item = value.as_object().ok_or(PrivateParseError::Payload)?;
        if item.get("symbol").and_then(Value::as_str) != Some(crate::native_symbol(symbol).as_str())
        {
            continue;
        }
        let quantity = decimal(item, "positionAmt")?.abs();
        if quantity.is_zero() {
            continue;
        }
        let position_side = match item.get("positionSide").and_then(Value::as_str) {
            Some("LONG") => PositionSide::Long,
            Some("SHORT") => PositionSide::Short,
            _ => return Err(PrivateParseError::Position),
        };
        let mark_price =
            Price::new(decimal(item, "markPrice")?).map_err(|_| PrivateParseError::Payload)?;
        let quote_notional = quantity * mark_price.value();
        let reported_quote_notional = decimal(item, "notional")?.abs();
        // PAPI serializes position notional at eight decimal places. Preserve that signed field as
        // consistency evidence, then publish the exact normalized product required by the domain
        // snapshot instead of carrying venue rounding into risk comparisons.
        if reported_quote_notional != quote_notional.round_dp(POSITION_NOTIONAL_SCALE) {
            return Err(PrivateParseError::RiskSnapshot);
        }
        let leg = LegRiskSnapshot {
            symbol: symbol.clone(),
            position_side,
            quantity,
            mark_price,
            contract_multiplier: usd_per_quote,
            notional: quote_notional * usd_per_quote,
            unrealized_pnl: decimal(item, "unRealizedProfit")? * usd_per_quote,
            risk_currency: usd.clone(),
            private_generation,
            observed_at_ms,
        };
        validate_risk_snapshot_pair(&account_snapshot, &leg, observed_at_ms, 0)
            .map_err(|_| PrivateParseError::RiskSnapshot)?;
        legs.push(leg);
    }
    // Equity validity is still required when both legs are flat and therefore no pair exists.
    account_snapshot
        .validate_at(observed_at_ms, 0)
        .map_err(|_| PrivateParseError::RiskSnapshot)?;
    Ok((account_snapshot, legs))
}

fn decimal(item: &Map<String, Value>, field: &str) -> Result<Decimal, PrivateParseError> {
    let raw = item
        .get(field)
        .and_then(Value::as_str)
        .ok_or(PrivateParseError::Payload)?;
    Decimal::from_str(raw).map_err(|_| PrivateParseError::Payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_index_conversion_requires_one_fresh_positive_match()
    -> Result<(), Box<dyn std::error::Error>> {
        let asset: Asset = "USDT".parse()?;
        let payload = r#"{"asset":"USDT","assetIndexPrice":"0.9998","time":1000}"#;
        let evidence = parse_usd_conversion_evidence(payload, asset.clone(), 7, 1_050, 100)?;
        assert_eq!(evidence.usd_per_asset, Decimal::new(9_998, 4));
        assert_eq!(evidence.private_generation, 7);
        assert_eq!(evidence.source_time_ms, 1_000);
        let array_payload = r#"[{"asset":"USDT","assetIndexPrice":"0.9998","time":1000}]"#;
        assert_eq!(
            parse_usd_conversion_evidence(array_payload, asset.clone(), 7, 1_050, 100,)?,
            evidence
        );

        assert!(matches!(
            parse_usd_conversion_evidence(payload, asset.clone(), 7, 1_101, 100),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        assert!(matches!(
            parse_usd_conversion_evidence(payload, asset.clone(), 7, 999, 100),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        assert!(matches!(
            parse_usd_conversion_evidence(
                r#"{"asset":"USDC","assetIndexPrice":"1","time":1000}"#,
                asset.clone(),
                7,
                1_050,
                100,
            ),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        assert!(matches!(
            parse_usd_conversion_evidence(
                r#"{"asset":"USDT","assetIndexPrice":"0","time":1000}"#,
                asset.clone(),
                7,
                1_050,
                100,
            ),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        assert!(matches!(
            parse_usd_conversion_evidence(r#"[]"#, asset.clone(), 7, 1_050, 100,),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        assert!(matches!(
            parse_usd_conversion_evidence(
                r#"[
                    {"asset":"USDT","assetIndexPrice":"1","time":1000},
                    {"asset":"USDC","assetIndexPrice":"1","time":1000}
                ]"#,
                asset.clone(),
                7,
                1_050,
                100,
            ),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        assert!(matches!(
            parse_usd_conversion_evidence(
                r#"[
                    {"asset":"USDT","assetIndexPrice":"1","time":1000},
                    {"asset":"USDT","assetIndexPrice":"1","time":1000}
                ]"#,
                asset,
                7,
                1_050,
                100,
            ),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        Ok(())
    }

    #[test]
    fn documented_usd_m_asset_index_uses_asset_usd_symbol_and_index()
    -> Result<(), Box<dyn std::error::Error>> {
        let usdc: Asset = "USDC".parse()?;
        let evidence = parse_usd_conversion_evidence(
            r#"{"symbol":"USDCUSD","time":1000,"index":"0.9975"}"#,
            usdc,
            7,
            1_050,
            100,
        )?;
        assert_eq!(evidence.usd_per_asset, Decimal::new(9_975, 4));
        assert!(
            parse_usd_conversion_evidence(
                r#"{"symbol":"USDTUSD","time":1000,"index":"1"}"#,
                "USDC".parse()?,
                7,
                1_050,
                100,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn portfolio_account_uses_aggregate_risk_values() -> Result<(), Box<dyn std::error::Error>> {
        let balance = parse_account_balance(
            r#"{"accountEquity":"12","totalAvailableBalance":"8","accountInitialMargin":"3","accountMaintMargin":"0.5"}"#,
        )?;
        assert_eq!(balance.available_balance, Decimal::new(8, 0));
        assert!(capabilities(r#"{"canTrade":true}"#, r#"{"dualSidePosition":false}"#,)?.can_trade);
        Ok(())
    }

    #[test]
    fn portfolio_risk_requires_explicit_usdc_to_usd_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDC".parse()?;
        let account = r#"{"accountEquity":"133","totalAvailableBalance":"100","accountInitialMargin":"3","accountMaintMargin":"0.5"}"#;
        let positions = r#"[{"symbol":"SOLUSDC","positionAmt":"4","positionSide":"LONG","markPrice":"100","notional":"400","unRealizedProfit":"7"}]"#;
        let capabilities = PrivateAccountCapabilities {
            can_trade: true,
            one_way_position: false,
            hedge_position: true,
        };
        assert!(matches!(
            parse_risk_snapshots(
                account,
                positions,
                &symbol,
                "portfolio_margin_um",
                capabilities,
                9,
                1_000,
                None,
            ),
            Err(PrivateParseError::CurrencyEvidence)
        ));
        let evidence = UsdConversionEvidence {
            asset: "USDC".parse()?,
            usd_per_asset: Decimal::new(999, 3),
            private_generation: 9,
            observed_at_ms: 1_000,
            source_time_ms: 1_000,
        };
        let (account, legs) = parse_risk_snapshots(
            account,
            positions,
            &symbol,
            "portfolio_margin_um",
            capabilities,
            9,
            1_000,
            Some(&evidence),
        )?;
        assert_eq!(account.account_equity, Decimal::new(133, 0));
        assert_eq!(legs[0].notional, Decimal::new(3996, 1));
        assert_eq!(legs[0].unrealized_pnl, Decimal::new(6993, 3));
        assert_eq!(legs[0].risk_currency, "USD".parse()?);
        Ok(())
    }

    #[test]
    fn portfolio_position_notional_accepts_only_binance_eight_decimal_rounding()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDC".parse()?;
        let account = r#"{"accountEquity":"118.00836498"}"#;
        let positions = r#"[
            {"symbol":"SOLUSDC","positionAmt":"-5.48","positionSide":"SHORT","markPrice":"100.18264123","notional":"-549.00087394","unRealizedProfit":"-4.33280119"},
            {"symbol":"SOLUSDC","positionAmt":"1.17","positionSide":"LONG","markPrice":"100.18264123","notional":"117.21369024","unRealizedProfit":"-1.18334591"}
        ]"#;
        let capabilities = PrivateAccountCapabilities {
            can_trade: true,
            one_way_position: false,
            hedge_position: true,
        };
        let evidence = UsdConversionEvidence {
            asset: "USDC".parse()?,
            usd_per_asset: Decimal::new(9_999, 4),
            private_generation: 18,
            observed_at_ms: 1_000,
            source_time_ms: 1_000,
        };

        let (_, legs) = parse_risk_snapshots(
            account,
            positions,
            &symbol,
            "portfolio_margin_um",
            capabilities,
            18,
            1_000,
            Some(&evidence),
        )?;
        assert_eq!(legs.len(), 2);
        assert_eq!(
            legs[0].notional,
            Decimal::new(548, 2) * Decimal::new(10_018_264_123, 8) * Decimal::new(9_999, 4)
        );
        assert_eq!(
            legs[1].notional,
            Decimal::new(117, 2) * Decimal::new(10_018_264_123, 8) * Decimal::new(9_999, 4)
        );

        let inconsistent = positions.replace("549.00087394", "549.00087393");
        assert!(matches!(
            parse_risk_snapshots(
                account,
                &inconsistent,
                &symbol,
                "portfolio_margin_um",
                capabilities,
                18,
                1_000,
                Some(&evidence),
            ),
            Err(PrivateParseError::RiskSnapshot)
        ));
        Ok(())
    }
}
