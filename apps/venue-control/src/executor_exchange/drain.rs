use super::*;

pub type LimitAbsenceFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Option<venue_gateway_binance::BinanceAbsentLimitOrder>,
                    BinanceExecutionError,
                >,
            > + Send
            + 'a,
    >,
>;

pub(super) fn confirm(
    request: &ExecutionRequest,
    created_ms: u64,
    credentials: BinanceCredentials,
    limits: venue_gateway_binance::BinanceTransportLimits,
) -> LimitAbsenceFuture<'_> {
    Box::pin(async move {
        if request.origin != venue_control_protocol::kol::ExecutorCommandOrigin::Copy
            || request.known_native_order_id.is_some()
            || !matches!(
                request.order_kind,
                ExecutionOrderKind::Limit {
                    reducing: false,
                    ..
                }
            )
        {
            return Ok(None);
        }
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            request.trading_account_id.clone(),
            request.symbol.clone(),
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        let client = request.client_order_id.clone();
        tokio::task::spawn_blocking(move || {
            let mut gateway = BinanceAccountGateway::connect_with_credentials_for_symbols(
                binding,
                BTreeSet::new(),
                credentials,
                limits,
            )
            .map_err(|_| BinanceExecutionError::Unavailable)?;
            gateway
                .confirm_recent_absent_limit_order(&client, created_ms)
                .map_err(|_| BinanceExecutionError::Unavailable)
        })
        .await
        .map_err(|_| BinanceExecutionError::Unavailable)?
    })
}
