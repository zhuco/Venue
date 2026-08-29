use super::*;

#[test]
fn signed_private_readback_uses_one_symbol_across_every_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let result = private_readback_from_payloads(
        &symbol,
        r#"{"accountEquity":"5","totalAvailableBalance":"5","accountInitialMargin":"0","accountMaintMargin":"0"}"#,
        "[]",
        r#"{"canTrade":true}"#,
        r#"{"dualSidePosition":true}"#,
        "[]",
        "[]",
    )?;

    assert_eq!(result.balances.len(), 1);
    assert!(result.capabilities.can_trade);
    assert_eq!(result.positions.len(), 2);
    assert!(
        result
            .positions
            .iter()
            .all(|position| position.quantity.is_zero())
    );
    assert!(
        result
            .positions
            .iter()
            .any(|position| position.side == crate::domain::PositionSide::Long)
    );
    assert!(
        result
            .positions
            .iter()
            .any(|position| position.side == crate::domain::PositionSide::Short)
    );
    assert!(result.orders.is_empty());
    assert!(result.fills.is_empty());
    Ok(())
}
