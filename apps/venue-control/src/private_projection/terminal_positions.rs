use super::*;
use crate::executor_exchange::TerminalPositionSettlement;

impl BinancePrivateProjectionStore {
    pub(super) async fn apply_terminal_position_refresh(
        &self,
        owner: &str,
        projection: &mut TerminalAccountProjection,
    ) -> Result<(), PrivateProjectionError> {
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT DISTINCT ON (c.symbol) m.settlement_json FROM venue_terminal_position_commands m JOIN venue_binance_commands c ON c.command_id=m.command_id WHERE c.owner_user_id=$1 AND c.credential_id=$2 AND c.trading_account_id=$3 AND m.settlement_json IS NOT NULL AND (m.settlement_json->>'observed_ms')::bigint>$4 ORDER BY c.symbol,(m.settlement_json->>'observed_ms')::bigint DESC,c.command_id DESC")
            .bind(owner).bind(&projection.credential_id).bind(&projection.trading_account_id).bind(ms(projection.observed_ms)?)
            .fetch_all(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        for row in rows {
            let settlement: TerminalPositionSettlement =
                serde_json::from_value(row).map_err(|_| PrivateProjectionError::Unavailable)?;
            merge_positions(projection, settlement);
        }
        Ok(())
    }
}

fn merge_positions(
    projection: &mut TerminalAccountProjection,
    settlement: TerminalPositionSettlement,
) {
    // A symbol-only REST read must never freshen assets, orders, or the account stream clock.
    if settlement.observed_ms <= projection.observed_ms {
        return;
    }
    let Some(symbol) = settlement.positions.first().map(|p| p.symbol.clone()) else {
        return;
    };
    projection.positions.retain(|p| p.symbol != symbol);
    projection.positions.extend(settlement.positions);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refresh_replaces_only_its_symbol_and_does_not_freshen_other_surfaces()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = TerminalAccountProjection {
            schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: "00000000-0000-4000-8000-000000000001".into(),
            trading_account_id: "00000000-0000-4000-8000-000000000002".into(),
            observed_ms: 100,
            persisted_ms: 100,
            private_generation: 1,
            position_mode: TerminalPositionMode::Hedge,
            positions: Vec::new(),
            open_orders: Vec::new(),
            fills: Vec::new(),
            position_history: Vec::new(),
            assets: Vec::new(),
        };
        let row = TerminalPosition {
            symbol: "SOL/USDC".parse()?,
            position_side: PositionSide::Long,
            quantity: rust_decimal::Decimal::ONE,
            entry_price: None,
            mark_price: None,
        };
        projection.positions.push(row.clone());
        let mut zero = row.clone();
        zero.quantity = rust_decimal::Decimal::ZERO;
        merge_positions(
            &mut projection,
            TerminalPositionSettlement {
                executed_quantity: rust_decimal::Decimal::ONE,
                observed_ms: 101,
                positions: vec![zero.clone()],
            },
        );
        assert_eq!(projection.positions, vec![zero]);
        assert_eq!(projection.observed_ms, 100);
        assert_eq!(projection.persisted_ms, 100);
        merge_positions(
            &mut projection,
            TerminalPositionSettlement {
                executed_quantity: rust_decimal::Decimal::ONE,
                observed_ms: 99,
                positions: vec![row],
            },
        );
        assert!(projection.positions[0].quantity.is_zero());
        Ok(())
    }
}
