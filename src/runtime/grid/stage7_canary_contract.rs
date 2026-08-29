use crate::{
    domain::MarketReduceCommand,
    exchange::grid::{
        BinanceGridVenue, BitgetGridVenue, GateGridVenue, GridVenueError, HedgedGridVenue,
    },
    execution::CapabilityBinding,
};

pub(crate) trait Stage7CanaryVenue: HedgedGridVenue {
    fn capability_binding(&self) -> CapabilityBinding;
    fn place_market_reduce(
        &mut self,
        command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError>;
}

impl Stage7CanaryVenue for BinanceGridVenue {
    fn capability_binding(&self) -> CapabilityBinding {
        BinanceGridVenue::capability_binding(self)
    }

    fn place_market_reduce(
        &mut self,
        command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        BinanceGridVenue::place_market_reduce(self, command)
    }
}

impl Stage7CanaryVenue for GateGridVenue {
    fn capability_binding(&self) -> CapabilityBinding {
        GateGridVenue::capability_binding(self)
    }

    fn place_market_reduce(
        &mut self,
        command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        GateGridVenue::place_market_reduce(self, command)
    }
}

impl Stage7CanaryVenue for BitgetGridVenue {
    fn capability_binding(&self) -> CapabilityBinding {
        BitgetGridVenue::capability_binding(self)
    }

    fn place_market_reduce(
        &mut self,
        command: &MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        BitgetGridVenue::place_market_reduce(self, command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_production_grid_venues_implement_the_same_canary_contract() {
        fn assert_contract<T: Stage7CanaryVenue>() {}
        assert_contract::<BinanceGridVenue>();
        assert_contract::<GateGridVenue>();
        assert_contract::<BitgetGridVenue>();
    }
}
