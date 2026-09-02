use super::*;

pub(super) fn dispatch_catalog_permit(
    gateway: &mut BinanceAccountGateway,
    permit: AccountDispatchPermit,
) -> AccountGatewayResult {
    if permit.binding() != gateway.config.gateway_binding() {
        return rejected("binance_permit_binding");
    }
    if gateway.refresh_private().is_err() {
        return rejected("binance_preflight_failed");
    }
    let command_symbol = &permit.command().mutation_owner().symbol;
    let Some(rules) = gateway.rules_by_symbol.get(command_symbol).cloned() else {
        return rejected("binance_unconfigured_symbol");
    };
    let binding = match binding_for_symbol(gateway.config.gateway_binding(), command_symbol.clone())
    {
        Ok(value) => value,
        Err(_) => return rejected("binance_symbol_binding"),
    };
    let config =
        match BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding) {
            Ok(value) => value,
            Err(_) => return rejected("binance_symbol_binding"),
        };
    // This is an ephemeral read scope over the same credentials and account Host, not a second
    // gateway or writer. The selected rule binding is required by Binance's exact readback.
    let mut transport = match BinanceHttpTransport::new(
        config.clone(),
        rules.instrument.generation,
        gateway.private_generation,
        gateway.transport.recovery_limits(),
    ) {
        Ok(value) => value,
        Err(_) => return rejected("binance_symbol_transport"),
    };
    if transport
        .inherit_synchronized_clock(&gateway.transport)
        .is_err()
    {
        return rejected("binance_clock");
    }
    let private = if uses_refreshed_anchor_private(gateway.config.gateway_binding(), command_symbol)
    {
        gateway.private.clone()
    } else {
        let attempt = match gateway.take_attempt_id() {
            Ok(value) => value,
            Err(_) => return rejected("binance_attempt"),
        };
        match gateway.runtime.block_on(fetch_private(
            &transport,
            &gateway.credentials,
            &config,
            &rules,
            gateway.private_generation,
            attempt,
        )) {
            Ok(value) => value,
            Err(_) => return rejected("binance_symbol_preflight"),
        }
    };
    let prepared = match prepare_execution_command(&rules, &private, permit.command()) {
        Ok(value) => value,
        Err(_) => return rejected("binance_intent_rejected"),
    };
    let timestamp = match transport.signing_timestamp_ms() {
        Ok(value) => value,
        Err(_) => return rejected("binance_clock"),
    };
    match gateway
        .runtime
        .block_on(transport.dispatch_then_exact_readback(
            &gateway.credentials,
            private.scope(),
            &prepared,
            timestamp,
        )) {
        BinancePhysicalMutationOutcome::ReadBack { ack, readback } => {
            let venue_order_id = ack.order_id.clone();
            if settle_mutation_ack(&ack, *readback).is_ok() {
                AccountGatewayResult::Accepted { venue_order_id }
            } else {
                AccountGatewayResult::Unknown
            }
        }
        BinancePhysicalMutationOutcome::DispatchFailed {
            error: BinanceTransportError::HttpStatus(status),
        } if (400..500).contains(&status) => rejected("binance_venue_rejected"),
        BinancePhysicalMutationOutcome::DispatchFailed { .. }
        | BinancePhysicalMutationOutcome::AckedReadbackUnknown { .. }
        | BinancePhysicalMutationOutcome::DispatchUnknown { .. } => AccountGatewayResult::Unknown,
    }
}

pub(super) fn uses_refreshed_anchor_private(
    account_binding: &GatewayBinding,
    command_symbol: &Symbol,
) -> bool {
    &account_binding.symbol == command_symbol
}
