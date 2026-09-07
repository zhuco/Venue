use super::CopyRiskRejection;

/// Only fixed classifications cross into the ledger; never persist raw payloads or credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreDispatchRejection {
    Binding,
    Identity,
    Quantity,
    Price,
    Direction,
    OrderType,
    CloseReservation,
    CancelTarget,
    Scope,
    InstrumentRules,
    CatalogueUnavailable,
    ClockUnavailable,
    Signing,
    Intent,
    OrderRules,
    Position,
    Payload,
    AccountFacts,
}

impl PreDispatchRejection {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Binding => "not_dispatched_binding",
            Self::Identity => "not_dispatched_identity",
            Self::Quantity => "not_dispatched_quantity",
            Self::Price => "not_dispatched_price",
            Self::Direction => "not_dispatched_direction",
            Self::OrderType => "not_dispatched_order_type",
            Self::CloseReservation => "not_dispatched_close_reservation",
            Self::CancelTarget => "not_dispatched_cancel_target",
            Self::Scope => "not_dispatched_scope",
            Self::InstrumentRules => "not_dispatched_instrument_rules",
            Self::CatalogueUnavailable => "not_dispatched_catalogue_unavailable",
            Self::ClockUnavailable => "not_dispatched_clock_unavailable",
            Self::Signing => "not_dispatched_signing",
            Self::Intent => "not_dispatched_intent",
            Self::OrderRules => "not_dispatched_order_rules",
            Self::Position => "not_dispatched_position",
            Self::Payload => "not_dispatched_payload",
            Self::AccountFacts => "not_dispatched_account_facts",
        }
    }
}

impl From<venue_gateway_binance::BinanceExecutionError> for BinanceExecutionError {
    fn from(error: venue_gateway_binance::BinanceExecutionError) -> Self {
        use venue_gateway_binance::BinanceExecutionError as GatewayError;
        let reason = match error {
            GatewayError::Binding => PreDispatchRejection::Scope,
            GatewayError::Intent => PreDispatchRejection::Intent,
            GatewayError::Rules => PreDispatchRejection::OrderRules,
            GatewayError::Position => PreDispatchRejection::Position,
            GatewayError::Payload => PreDispatchRejection::Payload,
            GatewayError::Readback => PreDispatchRejection::AccountFacts,
            GatewayError::UnsupportedCommand => PreDispatchRejection::OrderType,
            // This conversion is only for request preparation, never a transport response.
            GatewayError::VenueRejected => return Self::Invalid,
        };
        Self::PreDispatch(reason)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceExecutionError {
    #[error("Binance execution is unavailable")]
    Unavailable,
    #[error("Binance execution request is invalid")]
    Invalid,
    #[error("Binance pre-dispatch rejection: {0:?}")]
    PreDispatch(PreDispatchRejection),
    #[error("Manual opening quantity rounds down to zero")]
    OpenQuantityZero,
    #[error("Binance copy risk check rejected the order")]
    Risk(CopyRiskRejection),
}

impl BinanceExecutionError {
    /// `submit` only returns these errors before calling the physical mutation transport. Once a
    /// POST is attempted, uncertainty is represented by `ExecutionReadback::Unknown` instead.
    #[must_use]
    pub const fn not_dispatched_code(self) -> &'static str {
        match self {
            Self::PreDispatch(reason) => reason.code(),
            Self::Invalid => "not_dispatched_invalid",
            Self::OpenQuantityZero => "not_dispatched_quantity_zero",
            Self::Unavailable => "not_dispatched_unavailable",
            Self::Risk(reason) => reason.code(),
        }
    }
}
