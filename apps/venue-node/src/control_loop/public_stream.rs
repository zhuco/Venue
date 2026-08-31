use super::*;
#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
mod pending;
#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
use pending::PendingPublicFacts;
// Only the fixed adapter entry can choose a receiver. No config or Control request can supply
// a socket, endpoint, or public-read capability for another venue.
#[cfg(any(feature = "bybit", feature = "okx", feature = "hyperliquid"))]
macro_rules! normalized_public_runner {
    ($gateway:path, $receiver:path, $method:ident, $venue:ident, $publish:ident) => {
        impl ControlResidentLoop<$gateway> {
            pub fn $method(self) -> Result<(), NodeError> {
                let runtime = public_runtime()?;
                let mut receivers = Vec::new();
                for binding in self.scalping_bindings()? {
                    let receiver = runtime
                        .block_on(<$receiver>::connect(
                            public_gateway_binding(&binding)?,
                            Duration::from_secs(10),
                            2 * 1024 * 1024,
                        ))
                        .map_err(|error| NodeError::LiveHost {
                            venue: venue_gateway_api::VenueId::$venue,
                            message: error.to_string(),
                        })?;
                    receivers.push((binding, receiver, PendingPublicFacts::default()));
                }
                let mut last_refresh_ms = None;
                let mut next_receiver = 0;
                self.run_with_private_pump(move |resident| {
                    let private = refresh_signed_private_if_due(resident, &mut last_refresh_ms)?;
                    let public =
                        pump_public_batch(receivers.len(), &mut next_receiver, |index, wait| {
                            let (binding, receiver, pending) = &mut receivers[index];
                            if pending.is_empty() {
                                let events =
                                    runtime.block_on(receiver.next(wait)).map_err(|error| {
                                        NodeError::LiveHost {
                                            venue: venue_gateway_api::VenueId::$venue,
                                            message: error.to_string(),
                                        }
                                    })?;
                                pending.install(&binding.key.symbol, events)?;
                            }
                            if let Some((received_at_ms, event)) = pending.pop() {
                                match event {
                                    event @ (venue_domain::MarketEvent::Snapshot(_)
                                    | venue_domain::MarketEvent::Delta(_)) => {
                                        resident.$publish(binding, received_at_ms, event)?;
                                    }
                                    fact => {
                                        resident.publish_scalping_stream_fact(
                                            binding,
                                            received_at_ms,
                                            fact,
                                        )?;
                                    }
                                }
                                return Ok(true);
                            }
                            Ok(false)
                        })?;
                    Ok(private || public)
                })
            }
        }
    };
}

/// Drain queued frames fairly, instead of reading one frame per slow Control poll and allowing
/// a fast book to fall permanently behind. The frame/time budgets yield back to private work.
#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
fn pump_public_batch(
    count: usize,
    next_receiver: &mut usize,
    mut poll: impl FnMut(usize, Duration) -> Result<bool, NodeError>,
) -> Result<bool, NodeError> {
    if count == 0 {
        return Ok(false);
    }
    let started = std::time::Instant::now();
    let mut idle = 0;
    let mut progress = false;
    for _ in 0..256 {
        let remaining = Duration::from_millis(5).saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }
        let index = *next_receiver % count;
        *next_receiver = (index + 1) % count;
        if poll(index, remaining.min(Duration::from_millis(1)))? {
            progress = true;
            idle = 0;
        } else {
            idle += 1;
            if idle == count {
                break;
            }
        }
    }
    Ok(progress)
}

#[cfg(all(
    test,
    any(
        feature = "bitget",
        feature = "gate",
        feature = "bybit",
        feature = "okx",
        feature = "hyperliquid"
    )
))]
mod tests {
    use super::*;

    #[test]
    fn public_batch_round_robins_drains_backlog_and_stops_after_idle_round() -> Result<(), NodeError>
    {
        let mut cursor = 1;
        let mut pending = [4_u32, 2, 1];
        let mut visited = Vec::new();
        assert!(pump_public_batch(3, &mut cursor, |index, wait| {
            assert!(wait <= Duration::from_millis(1));
            visited.push(index);
            let ready = pending[index] != 0;
            pending[index] = pending[index].saturating_sub(1_u32);
            Ok(ready)
        })?);
        assert_eq!(pending, [0, 0, 0]);
        assert_eq!(&visited[..3], &[1, 2, 0]);
        assert!(visited.len() <= 15);
        assert!(!pump_public_batch(0, &mut cursor, |_, _| Err(
            NodeError::ResidentRuntime
        ))?);
        assert!(pump_public_batch(1, &mut cursor, |_, _| Err(NodeError::ResidentRuntime)).is_err());
        Ok(())
    }
}

#[cfg(feature = "bybit")]
normalized_public_runner!(
    venue_gateway_bybit::BybitAccountGateway,
    venue_gateway_bybit::BybitScalpingPublicReceiver,
    run_bybit,
    Bybit,
    publish_full_snapshot_scalping_book
);
#[cfg(feature = "okx")]
normalized_public_runner!(
    venue_gateway_okx::OkxAccountGateway,
    venue_gateway_okx::OkxScalpingPublicReceiver,
    run_okx,
    Okx,
    publish_sequenced_scalping_book
);
#[cfg(feature = "hyperliquid")]
normalized_public_runner!(
    venue_gateway_hyperliquid::HyperliquidAccountGateway,
    venue_gateway_hyperliquid::HyperliquidScalpingPublicReceiver,
    run_hyperliquid,
    Hyperliquid,
    publish_full_snapshot_scalping_book
);
#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
use venue_gateway_api::{GatewayBinding, GatewayMode};

#[cfg(feature = "binance")]
impl ControlResidentLoop<venue_gateway_binance::BinanceAccountGateway> {
    /// Binance alone currently supplies the bounded private Grid bridge. Keep that adapter-only
    /// pump out of the generic node loop so other venue processes cannot acquire a Binance read.
    pub fn run_binance(mut self) -> Result<(), NodeError> {
        let grid_bindings = self
            .bindings
            .values()
            .filter(|binding| binding.key.strategy_kind == StrategyKind::HedgedGrid)
            .cloned()
            .collect::<Vec<_>>();
        // Initial installation is one startup transaction, not a market-data retry loop. Its
        // own signed readback leaves a failed/unknown account Paused; retrying here could create
        // a second epoch or physical child after an indeterminate gateway outcome.
        for binding in grid_bindings {
            if self.resident.take_grid_bootstrap_request(&binding) {
                self.resident.bootstrap_binance_grid_once(&binding)?;
            }
        }
        self.run_with_private_pump(|resident| {
            // Both bounded reads are adapter-normalized facts.  A public feed cannot bypass the
            // same account Runtime/MarketHub, and a private fill remains first for Grid custody.
            let private_progress = resident.poll_binance_grid_private_once()?;
            let public_progress = resident.poll_binance_scalping_public_once()?;
            Ok(private_progress || public_progress)
        })
    }
}

#[cfg(feature = "bitget")]
impl ControlResidentLoop<venue_gateway_bitget::BitgetAccountGateway> {
    /// Bitget's resident owns the public socket for every configured Scalping actor.  The socket
    /// yields adapter-validated public facts; the existing resident bridge keeps its
    /// snapshot hidden until a covering update proves a contiguous book.
    pub fn run_bitget(mut self) -> Result<(), NodeError> {
        let runtime = public_runtime()?;
        let limits = venue_gateway_bitget::BitgetTransportLimits::new(
            Duration::from_secs(10),
            2 * 1024 * 1024,
        )
        .map_err(|_| NodeError::ResidentRuntime)?;
        let scalping = self.scalping_bindings()?;
        let mut receivers = Vec::with_capacity(scalping.len());
        for binding in scalping {
            let receiver = runtime
                .block_on(venue_gateway_bitget::BitgetScalpingPublicReceiver::connect(
                    public_gateway_binding(&binding)?,
                    limits,
                ))
                .map_err(|error| NodeError::LiveHost {
                    venue: venue_gateway_api::VenueId::Bitget,
                    message: error.to_string(),
                })?;
            self.resident
                .register_bitget_scalping_book_bridge(&binding)?;
            receivers.push((binding, receiver, PendingPublicFacts::default()));
        }
        let mut last_refresh_ms = None;
        let mut next_receiver = 0;
        self.run_with_private_pump(move |resident| {
            let private = refresh_signed_private_if_due(resident, &mut last_refresh_ms)?;
            let public = pump_public_batch(receivers.len(), &mut next_receiver, |index, wait| {
                use venue_gateway_bitget::BitgetScalpingPublicFrame as Frame;
                let (binding, receiver, pending) = &mut receivers[index];
                if pending.is_empty() {
                    match runtime.block_on(receiver.next(wait)) {
                        Ok(Some(Frame::Books(message))) => {
                            resident.ingest_bitget_scalping_book(binding, message)?;
                            return Ok(true);
                        }
                        Ok(Some(Frame::Trades(batch))) => pending.install(
                            &binding.key.symbol,
                            batch.trades.into_iter().map(|value| {
                                (
                                    value.trade.received_at_ms,
                                    venue_domain::MarketEvent::Trade(value.trade),
                                )
                            }),
                        )?,
                        Ok(Some(Frame::ClosedBars(batch))) => pending.install(
                            &binding.key.symbol,
                            batch.bars.into_iter().map(|bar| {
                                (bar.received_at_ms, venue_domain::MarketEvent::Bar(bar))
                            }),
                        )?,
                        Ok(None) | Err(venue_gateway_bitget::BitgetPublicWsError::Idle) => {
                            return Ok(false);
                        }
                        Err(error) => {
                            return Err(NodeError::LiveHost {
                                venue: venue_gateway_api::VenueId::Bitget,
                                message: error.to_string(),
                            });
                        }
                    }
                }
                if let Some((time, event)) = pending.pop() {
                    resident.publish_scalping_stream_fact(binding, time, event)?;
                    return Ok(true);
                }
                Ok(false)
            })?;
            Ok(private || public)
        })
    }
}

#[cfg(feature = "gate")]
impl ControlResidentLoop<venue_gateway_gate::GateAccountGateway> {
    /// Gate's socket is subscribed before its REST baseline is fetched. The resident therefore
    /// observes the existing snapshot-plus-delta bridge, never an unsequenced REST book.
    pub fn run_gate(mut self) -> Result<(), NodeError> {
        let runtime = public_runtime()?;
        let limits =
            venue_gateway_gate::GateTransportLimits::new(Duration::from_secs(10), 2 * 1024 * 1024)
                .map_err(|_| NodeError::ResidentRuntime)?;
        let scalping = self.scalping_bindings()?;
        let mut receivers = Vec::with_capacity(scalping.len());
        for binding in scalping {
            let receiver = runtime
                .block_on(venue_gateway_gate::GateScalpingPublicReceiver::connect(
                    public_gateway_binding(&binding)?,
                    limits,
                ))
                .map_err(|error| NodeError::LiveHost {
                    venue: venue_gateway_api::VenueId::Gate,
                    message: error.to_string(),
                })?;
            let bridge = receiver
                .new_book_bridge()
                .map_err(|error| NodeError::LiveHost {
                    venue: venue_gateway_api::VenueId::Gate,
                    message: error.to_string(),
                })?;
            self.resident
                .register_gate_scalping_book_bridge(&binding, bridge)?;
            receivers.push((binding, receiver, PendingPublicFacts::default()));
        }
        let mut last_refresh_ms = None;
        let mut next_receiver = 0;
        self.run_with_private_pump(move |resident| {
            let private = refresh_signed_private_if_due(resident, &mut last_refresh_ms)?;
            let public = pump_public_batch(receivers.len(), &mut next_receiver, |index, wait| {
                use venue_gateway_gate::GateScalpingPublicFrame as Frame;
                let (binding, receiver, pending) = &mut receivers[index];
                if pending.is_empty() {
                    match runtime.block_on(receiver.next(wait)) {
                        Ok(Some(Frame::Snapshot(snapshot))) => {
                            resident.ingest_gate_scalping_snapshot(binding, snapshot)?;
                            return Ok(true);
                        }
                        Ok(Some(Frame::Delta(delta))) => {
                            resident.ingest_gate_scalping_delta(binding, delta)?;
                            return Ok(true);
                        }
                        Ok(Some(Frame::Trades(batch))) => pending.install(
                            &binding.key.symbol,
                            batch.trades.into_iter().map(|trade| {
                                (
                                    trade.received_at_ms,
                                    venue_domain::MarketEvent::Trade(trade),
                                )
                            }),
                        )?,
                        Ok(Some(Frame::ClosedBars(batch))) => pending.install(
                            &binding.key.symbol,
                            batch.bars.into_iter().map(|bar| {
                                (bar.received_at_ms, venue_domain::MarketEvent::Bar(bar))
                            }),
                        )?,
                        Ok(None) | Err(venue_gateway_gate::GatePublicWsError::Idle) => {
                            return Ok(false);
                        }
                        Err(error) => {
                            return Err(NodeError::LiveHost {
                                venue: venue_gateway_api::VenueId::Gate,
                                message: error.to_string(),
                            });
                        }
                    }
                }
                if let Some((time, event)) = pending.pop() {
                    resident.publish_scalping_stream_fact(binding, time, event)?;
                    return Ok(true);
                }
                Ok(false)
            })?;
            Ok(private || public)
        })
    }
}

#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
impl<G: AccountPhysicalGateway> ControlResidentLoop<G> {
    fn scalping_bindings(&self) -> Result<Vec<StrategyBinding>, NodeError> {
        Ok(self
            .bindings
            .values()
            .filter(|binding| binding.key.strategy_kind == StrategyKind::Scalping)
            .cloned()
            .collect())
    }
}

#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
fn public_runtime() -> Result<tokio::runtime::Runtime, NodeError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| NodeError::ResidentRuntime)
}

#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
fn public_gateway_binding(binding: &StrategyBinding) -> Result<GatewayBinding, NodeError> {
    GatewayBinding::new(
        binding.key.account.exchange,
        GatewayMode::Live,
        binding.key.account.account.clone(),
        binding.key.symbol.clone(),
    )
    .map_err(|_| NodeError::ResidentRuntime)
}

#[cfg(any(
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
fn refresh_signed_private_if_due<G: AccountPhysicalGateway>(
    resident: &mut ProductionResident<G>,
    last_refresh_ms: &mut Option<u64>,
) -> Result<bool, NodeError> {
    let now = now_ms().map_err(|_| NodeError::ResidentRuntime)?;
    if last_refresh_ms.is_some_and(|previous| {
        now.saturating_sub(previous) < MIN_SIGNED_PRIVATE_REFRESH_INTERVAL.as_millis() as u64
    }) {
        return Ok(false);
    }
    let refreshed = resident.refresh_signed_snapshot()?;
    *last_refresh_ms = Some(now);
    Ok(refreshed.private_generation() != 0)
}
