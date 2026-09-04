use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::{PositionSide, Symbol, is_canonical_trading_account_id};

pub const TERMINAL_POSITION_ACTION_PATH: &str = "/v2/kol/terminal/positions/action";
pub const TERMINAL_POSITION_ACTION_SCHEMA: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionAction {
    Close,
    Reverse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalPositionActionRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
    pub symbol: Symbol,
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub action: PositionAction,
    pub market_risk_confirmed: bool,
}

impl TerminalPositionActionRequest {
    pub fn validate(&self) -> Result<(), crate::kol::KolProtocolError> {
        if self.schema_version != TERMINAL_POSITION_ACTION_SCHEMA
            || !is_canonical_trading_account_id(&self.request_id)
            || !is_canonical_trading_account_id(&self.credential_id)
            || !matches!(self.position_side, PositionSide::Long | PositionSide::Short)
            || self.quantity <= Decimal::ZERO
            || self.quantity == Decimal::MAX
            || !self.market_risk_confirmed
        {
            return Err(crate::kol::KolProtocolError::TerminalOrder);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_actions_require_exact_hedge_side_positive_quantity_and_confirmation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut request = TerminalPositionActionRequest {
            schema_version: TERMINAL_POSITION_ACTION_SCHEMA,
            request_id: "00000000-0000-4000-8000-000000000001".into(),
            credential_id: "00000000-0000-4000-8000-000000000002".into(),
            symbol: "SOL/USDC".parse()?,
            position_side: PositionSide::Long,
            quantity: Decimal::new(276, 2),
            action: PositionAction::Reverse,
            market_risk_confirmed: true,
        };
        request.validate()?;
        let wire = serde_json::to_string(&request)?;
        assert_eq!(
            serde_json::from_str::<TerminalPositionActionRequest>(&wire)?,
            request
        );
        request.market_risk_confirmed = false;
        assert!(request.validate().is_err());
        request.market_risk_confirmed = true;
        request.position_side = PositionSide::Net;
        assert!(request.validate().is_err());
        request.position_side = PositionSide::Short;
        request.quantity = Decimal::ZERO;
        assert!(request.validate().is_err());
        Ok(())
    }
}
