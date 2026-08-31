use rust_decimal::Decimal;
use serde_json::Value;

use crate::domain::{AccountRiskSnapshot, Asset, Instrument, LegRiskSnapshot, Symbol};

use super::{binance_portfolio, bitget, gate};

/// Immutable context needed to replay one venue risk readback without transport or credentials.
pub(crate) struct RiskReplayRequest<'a> {
    pub exchange: &'a str,
    pub account: &'a str,
    pub symbol: &'a Symbol,
    pub instrument: &'a Instrument,
    pub minimum_quantity: Decimal,
    pub private_generation: u64,
    pub observed_at_ms: u64,
    pub max_age_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplayedRiskSnapshot {
    pub account: AccountRiskSnapshot,
    pub legs: Vec<LegRiskSnapshot>,
}

/// Replays the exact ordered raw payload tuple emitted by each venue's signed risk readback.
/// Payload count and order are protocol evidence; missing, surplus, or reordered values fail closed.
pub(crate) fn replay_private_risk_payloads(
    request: &RiskReplayRequest<'_>,
    payloads: &[String],
) -> Result<ReplayedRiskSnapshot, RiskReplayError> {
    validate_request(request)?;
    match request.exchange {
        "binance" => replay_binance(request, payloads),
        "bitget" => replay_bitget(request, payloads),
        "gate" => replay_gate(request, payloads),
        _ => Err(RiskReplayError::Exchange),
    }
}

fn validate_request(request: &RiskReplayRequest<'_>) -> Result<(), RiskReplayError> {
    if request.account.is_empty()
        || request.symbol != &request.instrument.symbol
        || request.minimum_quantity <= Decimal::ZERO
        || request.private_generation == 0
        || request.observed_at_ms == 0
        || request.max_age_ms == 0
    {
        return Err(RiskReplayError::Context);
    }
    request
        .instrument
        .validate()
        .map_err(|_| RiskReplayError::Context)
}

fn replay_binance(
    request: &RiskReplayRequest<'_>,
    payloads: &[String],
) -> Result<ReplayedRiskSnapshot, RiskReplayError> {
    let [
        account,
        positions,
        position_mode,
        account_config,
        conversion,
    ] = payloads
    else {
        return Err(RiskReplayError::PayloadTuple);
    };
    let capabilities = binance_portfolio::capabilities(account_config, position_mode)
        .map_err(|_| RiskReplayError::Payload)?;
    let quote: Asset = request
        .symbol
        .quote()
        .parse()
        .map_err(|_| RiskReplayError::Context)?;
    let conversion = binance_portfolio::parse_usd_conversion_evidence(
        conversion,
        quote,
        request.private_generation,
        request.observed_at_ms,
        request.max_age_ms,
    )
    .map_err(|_| RiskReplayError::Payload)?;
    let (account, legs) = binance_portfolio::parse_risk_snapshots(
        account,
        positions,
        request.symbol,
        request.account,
        capabilities,
        request.private_generation,
        request.observed_at_ms,
        Some(&conversion),
    )
    .map_err(|_| RiskReplayError::Payload)?;
    Ok(ReplayedRiskSnapshot { account, legs })
}

fn replay_bitget(
    request: &RiskReplayRequest<'_>,
    payloads: &[String],
) -> Result<ReplayedRiskSnapshot, RiskReplayError> {
    let [assets, settings, positions] = payloads else {
        return Err(RiskReplayError::PayloadTuple);
    };
    let assets = parse_json(assets)?;
    let settings = parse_json(settings)?;
    let positions = parse_json(positions)?;
    let assets = bitget::bitget_data(&assets).map_err(|_| RiskReplayError::Payload)?;
    let settings = bitget::bitget_data(&settings)
        .map_err(|_| RiskReplayError::Payload)?
        .as_object()
        .ok_or(RiskReplayError::Payload)?;
    if settings.get("holdMode").and_then(Value::as_str) != Some("hedge_mode") {
        return Err(RiskReplayError::Payload);
    }
    let positions =
        bitget::list_data(bitget::bitget_data(&positions).map_err(|_| RiskReplayError::Payload)?)
            .map_err(|_| RiskReplayError::Payload)?;
    let (account, legs) = bitget::parse_risk_snapshots(
        assets,
        positions,
        request.symbol,
        request.account,
        request.private_generation,
        request.observed_at_ms,
    )
    .map_err(|_| RiskReplayError::Payload)?;
    Ok(ReplayedRiskSnapshot { account, legs })
}

fn replay_gate(
    request: &RiskReplayRequest<'_>,
    payloads: &[String],
) -> Result<ReplayedRiskSnapshot, RiskReplayError> {
    if payloads.len() != 2 && payloads.len() != 4 {
        return Err(RiskReplayError::PayloadTuple);
    }
    let account_value = parse_json(&payloads[0])?;
    let position_value = parse_json(&payloads[1])?;
    let positions = position_value.as_array().ok_or(RiskReplayError::Payload)?;
    let quantity_step = request.instrument.quantity_step;
    if quantity_step <= Decimal::ZERO || request.minimum_quantity % quantity_step != Decimal::ZERO {
        return Err(RiskReplayError::Context);
    }
    let rules = gate::GateContractRules {
        native_symbol: gate::native_symbol(request.symbol).map_err(|_| RiskReplayError::Context)?,
        instrument: request.instrument.clone(),
        quanto_multiplier: quantity_step,
        minimum_contracts: request.minimum_quantity / quantity_step,
        // Historical risk evidence does not prove a current per-order ceiling.
        maximum_contracts: None,
        // This field controls order conversion only and is not consulted by the risk parser.
        decimal_contracts: true,
    };
    let needs_unified = gate::gate_risk::requires_unified_single_currency(&account_value)
        .map_err(|_| RiskReplayError::Payload)?;
    let (_, account, legs) = match (needs_unified, payloads) {
        (false, [_, _]) => gate::parse_risk_snapshots(
            &account_value,
            positions,
            request.symbol,
            &rules,
            request.account,
            request.private_generation,
            request.observed_at_ms,
        ),
        (true, [_, _, mode, unified]) => {
            let mode = parse_json(mode)?;
            let unified = parse_json(unified)?;
            gate::gate_risk::parse_risk_snapshots_with_unified(
                &account_value,
                &mode,
                &unified,
                positions,
                request.symbol,
                &rules,
                request.account,
                request.private_generation,
                request.observed_at_ms,
            )
        }
        _ => return Err(RiskReplayError::PayloadTuple),
    }
    .map_err(|_| RiskReplayError::Payload)?;
    Ok(ReplayedRiskSnapshot { account, legs })
}

fn parse_json(payload: &str) -> Result<Value, RiskReplayError> {
    serde_json::from_str(payload).map_err(|_| RiskReplayError::Payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum RiskReplayError {
    #[error("unsupported risk replay exchange")]
    Exchange,
    #[error("risk replay context is incomplete or inconsistent")]
    Context,
    #[error("risk replay payload tuple has the wrong count or order")]
    PayloadTuple,
    #[error("risk replay payload is invalid for the admitted exchange")]
    Payload,
}

#[cfg(test)]
mod tests {
    use crate::domain::{Amount, MarketKind, PositionSide, Price};

    use super::*;

    fn instrument(symbol: &str, step: Decimal) -> Result<Instrument, Box<dyn std::error::Error>> {
        let symbol: Symbol = symbol.parse()?;
        Ok(Instrument {
            settlement_asset: Some(symbol.quote().parse()?),
            minimum_notional: Amount::new(symbol.quote().parse()?, Decimal::ZERO),
            symbol,
            market: MarketKind::LinearPerpetual,
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 4))?,
            quantity_step: step,
        })
    }

    #[test]
    fn replays_binance_portfolio_tuple_and_rejects_order_or_field_tampering()
    -> Result<(), Box<dyn std::error::Error>> {
        let instrument = instrument("SOL/USDC", Decimal::new(1, 2))?;
        let request = RiskReplayRequest {
            exchange: "binance",
            account: "portfolio_margin_um",
            symbol: &instrument.symbol,
            instrument: &instrument,
            minimum_quantity: Decimal::new(1, 2),
            private_generation: 9,
            observed_at_ms: 1_050,
            max_age_ms: 100,
        };
        let payloads = vec![
            r#"{"accountEquity":"133"}"#.to_owned(),
            r#"[{"symbol":"SOLUSDC","positionAmt":"4","positionSide":"LONG","markPrice":"100","notional":"400","unRealizedProfit":"7"}]"#.to_owned(),
            r#"{"dualSidePosition":true}"#.to_owned(),
            r#"{"canTrade":true}"#.to_owned(),
            r#"[{"asset":"USDC","assetIndexPrice":"0.999","time":1000}]"#.to_owned(),
        ];
        let replay = replay_private_risk_payloads(&request, &payloads)?;
        assert_eq!(replay.account.account_equity, Decimal::new(133, 0));
        assert_eq!(replay.legs[0].position_side, PositionSide::Long);
        assert_eq!(replay.legs[0].notional, Decimal::new(3996, 1));

        let mut reordered = payloads.clone();
        reordered.swap(2, 3);
        assert_eq!(
            replay_private_risk_payloads(&request, &reordered),
            Err(RiskReplayError::Payload)
        );
        let mut tampered = payloads;
        tampered[4] = tampered[4].replace("USDC", "USDT");
        assert_eq!(
            replay_private_risk_payloads(&request, &tampered),
            Err(RiskReplayError::Payload)
        );
        Ok(())
    }

    #[test]
    fn replays_bitget_uta_envelopes_and_rejects_missing_or_reordered_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let instrument = instrument("DOGE/USDT", Decimal::ONE)?;
        let request = RiskReplayRequest {
            exchange: "bitget",
            account: "uta_usdt_futures_hedge",
            symbol: &instrument.symbol,
            instrument: &instrument,
            minimum_quantity: Decimal::ONE,
            private_generation: 8,
            observed_at_ms: 1_000,
            max_age_ms: 100,
        };
        let payloads = vec![
            r#"{"code":"00000","data":{"usdtEquity":"20"}}"#.to_owned(),
            r#"{"code":"00000","data":{"holdMode":"hedge_mode"}}"#.to_owned(),
            r#"{"code":"00000","data":{"list":[{"symbol":"DOGEUSDT","marginCoin":"USDT","holdMode":"hedge_mode","posSide":"long","total":"600","markPrice":"0.1","unrealisedPnl":"1.1"}]}}"#.to_owned(),
        ];
        let replay = replay_private_risk_payloads(&request, &payloads)?;
        assert_eq!(replay.account.account_equity, Decimal::new(20, 0));
        assert_eq!(replay.legs[0].notional, Decimal::new(60, 0));

        assert_eq!(
            replay_private_risk_payloads(&request, &payloads[..2]),
            Err(RiskReplayError::PayloadTuple)
        );
        let mut reordered = payloads;
        reordered.swap(0, 1);
        assert_eq!(
            replay_private_risk_payloads(&request, &reordered),
            Err(RiskReplayError::Payload)
        );
        Ok(())
    }

    #[test]
    fn replays_gate_classic_and_unified_tuples_strictly() -> Result<(), Box<dyn std::error::Error>>
    {
        let instrument = instrument("DOGE/USDT", Decimal::new(1, 1))?;
        let request = RiskReplayRequest {
            exchange: "gate",
            account: "usdt_futures_dual",
            symbol: &instrument.symbol,
            instrument: &instrument,
            minimum_quantity: Decimal::new(1, 1),
            private_generation: 7,
            observed_at_ms: 3_000,
            max_age_ms: 100,
        };
        let classic = vec![
            r#"{"position_mode":"dual","total":"20","unrealised_pnl":"2"}"#.to_owned(),
            r#"[{"contract":"DOGE_USDT","mode":"dual_short","size":"-7","mark_price":"0.1","value":"0.07","unrealised_pnl":"1.2"}]"#.to_owned(),
        ];
        let replay = replay_private_risk_payloads(&request, &classic)?;
        assert_eq!(replay.account.account_equity, Decimal::new(22, 0));
        assert_eq!(replay.legs[0].position_side, PositionSide::Short);

        let unified = vec![
            r#"{"position_mode":"dual","margin_mode":3,"total":"0","unrealised_pnl":"-1"}"#.to_owned(),
            classic[1].clone(),
            r#"{"mode":"single_currency"}"#.to_owned(),
            r#"{"mode":"single_currency","locked":false,"balances":{"USDT":{"margin_balance":"22.5"}}}"#.to_owned(),
        ];
        assert_eq!(
            replay_private_risk_payloads(&request, &unified)?
                .account
                .account_equity,
            Decimal::new(225, 1)
        );
        let mut reordered = unified;
        reordered.swap(2, 3);
        assert_eq!(
            replay_private_risk_payloads(&request, &reordered),
            Err(RiskReplayError::Payload)
        );
        Ok(())
    }
}
