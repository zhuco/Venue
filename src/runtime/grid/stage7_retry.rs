use crate::exchange::grid::GridVenueError;

pub(super) fn is_transient_readback_error(error: &GridVenueError) -> bool {
    matches!(
        error,
        GridVenueError::Gate(
            crate::exchange::gate::GateError::Http
                | crate::exchange::gate::GateError::RateLimited
                | crate::exchange::gate::GateError::PrivateReadbackRejected { .. }
        ) | GridVenueError::Bitget(
            crate::exchange::bitget::BitgetError::Http
                | crate::exchange::bitget::BitgetError::RateLimited
                | crate::exchange::bitget::BitgetError::Payload
                | crate::exchange::bitget::BitgetError::Readback(_)
                | crate::exchange::bitget::BitgetError::ReadbackShape { .. }
                | crate::exchange::bitget::BitgetError::Pagination
        )
    )
}

pub(super) fn is_transient_instrument_rule_error(error: &GridVenueError) -> bool {
    matches!(error, GridVenueError::InstrumentRulesUnavailable)
}

pub(super) fn is_transient_venue_startup_error(error: &GridVenueError) -> bool {
    is_transient_readback_error(error) || is_transient_instrument_rule_error(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::gate::GateError;

    #[test]
    fn gate_private_readback_rejection_keeps_the_mutation_gate_closed_for_retry() {
        let error = GridVenueError::Gate(GateError::PrivateReadbackRejected {
            label: "INVALID_PARAM_VALUE".to_owned(),
        });
        assert!(is_transient_readback_error(&error));
    }

    #[test]
    fn gate_mutation_rejection_is_not_misclassified_as_a_readback_retry() {
        let error = GridVenueError::Gate(GateError::Rejected {
            label: "POC_FILL_IMMEDIATELY".to_owned(),
        });
        assert!(!is_transient_readback_error(&error));
    }

    #[test]
    fn incomplete_bitget_private_surfaces_retry_with_the_mutation_gate_closed() {
        for error in [
            GridVenueError::Bitget(crate::exchange::bitget::BitgetError::Payload),
            GridVenueError::Bitget(crate::exchange::bitget::BitgetError::Readback("fills")),
            GridVenueError::Bitget(crate::exchange::bitget::BitgetError::Pagination),
        ] {
            assert!(is_transient_readback_error(&error));
        }
    }
}
