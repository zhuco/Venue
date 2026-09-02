use super::*;

const ROLLING_DISPATCH_CACHE_TTL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq)]
struct BinanceRollingDispatchKey {
    binding: GatewayBinding,
    symbol: Symbol,
    batch_sha256: [u8; 32],
    signed_private_generation: u64,
    child_count: u8,
}

#[derive(Debug)]
pub(super) struct BinanceRollingDispatchCache {
    key: BinanceRollingDispatchKey,
    next_child_index: u8,
    adapter_private_generation: u64,
    rules_generation: u64,
    expires_at: Instant,
}

impl BinanceRollingDispatchCache {
    fn new(
        key: BinanceRollingDispatchKey,
        adapter_private_generation: u64,
        rules_generation: u64,
        now: Instant,
    ) -> Self {
        Self {
            key,
            next_child_index: 0,
            adapter_private_generation,
            rules_generation,
            expires_at: now + ROLLING_DISPATCH_CACHE_TTL,
        }
    }

    fn is_usable(
        &self,
        key: &BinanceRollingDispatchKey,
        child_index: u8,
        adapter_private_generation: u64,
        rules_generation: u64,
        now: Instant,
    ) -> bool {
        self.key == *key
            && self.next_child_index == child_index
            && self.adapter_private_generation == adapter_private_generation
            && self.rules_generation == rules_generation
            && now < self.expires_at
    }

    fn accepted_child(mut self) -> Option<Self> {
        self.next_child_index = self.next_child_index.checked_add(1)?;
        (self.next_child_index < self.key.child_count).then_some(self)
    }
}

pub(super) fn dispatch_catalog_permit(
    gateway: &mut BinanceAccountGateway,
    permit: AccountDispatchPermit,
) -> AccountGatewayResult {
    if permit.binding() != gateway.config.gateway_binding() {
        return rejected("binance_permit_binding");
    }
    let command_symbol = &permit.command().mutation_owner().symbol;
    let rolling = rolling_dispatch_key(&permit, command_symbol);
    let current_rules_generation = gateway
        .rules_by_symbol
        .get(command_symbol)
        .map(|rules| rules.instrument.generation);
    let mut rolling_cache = gateway.rolling_dispatch_cache.take().filter(|cache| {
        rolling.as_ref().is_some_and(|(key, child_index)| {
            current_rules_generation.is_some_and(|rules_generation| {
                cache.is_usable(
                    key,
                    *child_index,
                    gateway.private_generation,
                    rules_generation,
                    Instant::now(),
                )
            })
        })
    });
    let reused_rolling_preflight = rolling_cache.is_some();
    if !reused_rolling_preflight && gateway.refresh_private().is_err() {
        return rejected("binance_preflight_failed");
    }
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
    if rolling_cache.is_none()
        && let Some((key, 0)) = rolling
        && uses_refreshed_anchor_private(gateway.config.gateway_binding(), command_symbol)
    {
        rolling_cache = Some(BinanceRollingDispatchCache::new(
            key,
            gateway.private_generation,
            rules.instrument.generation,
            Instant::now(),
        ));
    }
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
                gateway.rolling_dispatch_cache =
                    rolling_cache.and_then(BinanceRollingDispatchCache::accepted_child);
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

fn rolling_dispatch_key(
    permit: &AccountDispatchPermit,
    command_symbol: &Symbol,
) -> Option<(BinanceRollingDispatchKey, u8)> {
    let rolling = permit.managed_grid_rolling_batch()?;
    let owner = permit.command().mutation_owner();
    let child_shape = match rolling.child_index() {
        0 | 1 => matches!(permit.command(), ExecutionCommand::PlaceLimit(order)
            if order.time_in_force == LimitTimeInForce::PostOnly),
        2 => matches!(permit.command(), ExecutionCommand::Cancel(_)),
        _ => false,
    };
    if rolling.binding() != permit.binding()
        || rolling.symbol() != command_symbol
        || command_symbol != &permit.binding().symbol
        || owner.exchange != permit.binding().venue.as_str()
        || owner.account != permit.binding().trading_account_id
        || rolling.signed_private_generation() == 0
        || rolling.child_count() != 3
        || rolling.batch_sha256().iter().all(|byte| *byte == 0)
        || !child_shape
    {
        return None;
    }
    Some((
        BinanceRollingDispatchKey {
            binding: rolling.binding().clone(),
            symbol: rolling.symbol().clone(),
            batch_sha256: rolling.batch_sha256(),
            signed_private_generation: rolling.signed_private_generation(),
            child_count: rolling.child_count(),
        },
        rolling.child_index(),
    ))
}

pub(super) fn uses_refreshed_anchor_private(
    account_binding: &GatewayBinding,
    command_symbol: &Symbol,
) -> bool {
    &account_binding.symbol == command_symbol
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};

    fn cache_key(batch_byte: u8) -> Result<BinanceRollingDispatchKey, Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDC".parse()?;
        Ok(BinanceRollingDispatchKey {
            binding: GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                "00000000-0000-4000-8000-000000000001",
                symbol.clone(),
            )?,
            symbol,
            batch_sha256: [batch_byte; 32],
            signed_private_generation: 7,
            child_count: 3,
        })
    }

    #[test]
    fn rolling_cache_requires_exact_order_generation_digest_and_ttl()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = Instant::now();
        let key = cache_key(1)?;
        let cache = BinanceRollingDispatchCache::new(key.clone(), 9, 11, now);
        assert!(cache.is_usable(&key, 0, 9, 11, now));
        assert!(!cache.is_usable(&cache_key(2)?, 0, 9, 11, now));
        assert!(!cache.is_usable(&key, 1, 9, 11, now));
        assert!(!cache.is_usable(&key, 0, 10, 11, now));
        assert!(!cache.is_usable(&key, 0, 9, 12, now));
        assert!(!cache.is_usable(&key, 0, 9, 11, now + ROLLING_DISPATCH_CACHE_TTL));
        Ok(())
    }

    #[test]
    fn rolling_cache_expires_after_exactly_three_accepted_children()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = Instant::now();
        let key = cache_key(3)?;
        let first = BinanceRollingDispatchCache::new(key.clone(), 9, 11, now);
        let second = first.accepted_child().ok_or("second child")?;
        assert!(second.is_usable(&key, 1, 9, 11, now));
        let third = second.accepted_child().ok_or("third child")?;
        assert!(third.is_usable(&key, 2, 9, 11, now));
        assert!(third.accepted_child().is_none());
        Ok(())
    }
}
