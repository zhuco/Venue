use super::*;
mod crossed_orders;

pub(super) fn apply(
    model: &mut AppModel,
    workspaces: &mut Workspaces,
    server: crate::model::MarketServer,
    generation: u64,
    event: LocalMarketClientEvent,
    context: &egui::Context,
) {
    // The receiver belongs to one worker lifetime; catalogs, quotes and errors have
    // the same source identity as its market envelopes, even across A -> B -> A.
    if model.market_worker_failed
        || server != model.preferences.market_server
        || generation != model.market_generation
    {
        return;
    }
    match event {
        LocalMarketClientEvent::History { request, result } => {
            if request.generation != model.local_markets.generation()
                || request.selection.binding.venue != server.venue()
            {
                return;
            }
            match model.local_markets.finish_history(&request, result) {
                Ok(added) => workspaces.history_prepended(
                    &request.selection,
                    added,
                    &model.preferences.selected_symbol,
                ),
                Err(error) => {
                    let _ = model
                        .local_markets
                        .finish_history(&request, Err(error.to_string()));
                    model.notice(format!("History page rejected: {error}"));
                }
            }
        }
        LocalMarketClientEvent::Market(envelope) => {
            if envelope.generation != model.local_markets.generation()
                || envelope.selection.binding.venue != server.venue()
                || !model
                    .local_markets
                    .selections()
                    .any(|selection| selection == &envelope.selection)
                || (server != crate::model::MarketServer::Binance
                    && !model
                        .local_symbols
                        .contains(&envelope.selection.binding.symbol.to_string()))
            {
                return;
            }
            // The active subscription is the scope boundary. Binance catalog and history load in
            // parallel; a delayed catalog must not discard the only initial history response.
            let trade = match &envelope.payload {
                crate::market::MarketPayload::Trade(trade) => {
                    Some((envelope.selection.binding.symbol.clone(), trade.clone()))
                }
                _ => None,
            };
            let history = matches!(
                &envelope.payload,
                crate::market::MarketPayload::RestHistory { .. }
            )
            .then(|| (envelope.selection.clone(), envelope.generation));
            if let Err(error) = model.local_markets.apply(*envelope) {
                if matches!(
                    error,
                    crate::market::LocalMarketError::Indicator(
                        venue_indicators::chart::ChartIndicatorError::DiscontinuousBar
                    )
                ) {
                    // The next frame resubscribes with a fresh generation and reloads history.
                    // Never promote a partial forming candle to a confirmed close to bridge a gap.
                    model.history_requests.clear();
                    if let Err(reset_error) = model.local_markets.replace([]) {
                        model.notice(format!("Market resync failed: {reset_error}"));
                    }
                    model.notice("K 线数据不连续，正在重新同步");
                    context.request_repaint();
                    return;
                }
                model.notice(format!("Ignored invalid local market event: {error}"));
            } else if let Some((selection, generation)) = history {
                let bars = model
                    .local_markets
                    .view(&selection)
                    .map_or(0, |view| view.bars.len());
                tracing::info!(target: "venueflow::chart_loading", generation, symbol = %selection.binding.symbol, interval = selection.interval.label(), bars, "Chart initial history accepted");
                context.request_repaint();
            } else if let Some((symbol, trade)) = trade {
                crossed_orders::observe(model, &symbol, &trade, context);
            }
        }
        LocalMarketClientEvent::Catalog(symbols) => {
            model.apply_local_catalog(symbols);
            if !model
                .local_symbols
                .contains(&model.preferences.selected_symbol)
            {
                let fallback = model
                    .local_symbols
                    .iter()
                    .find(|s| s.starts_with("BTC/"))
                    .or(model.local_symbols.first())
                    .cloned();
                if let Some(symbol) = fallback {
                    model.clear_trading_intent();
                    model.select_symbol(symbol);
                    workspaces.reset_chart_viewports();
                }
            }
        }
        LocalMarketClientEvent::Quotes(mut quotes) => {
            quotes.retain(|q| model.local_symbols.contains(&q.symbol));
            model.apply_local_quotes(quotes);
        }
        LocalMarketClientEvent::QuotesUnavailable(error) => {
            model.notice(format!("Local Binance 24h quotes unavailable: {error}"));
        }
        LocalMarketClientEvent::CatalogUnavailable(error) => {
            model.local_catalog_error = Some(error.clone());
            model.notice(format!("Local Binance symbol catalog unavailable: {error}"));
        }
        LocalMarketClientEvent::ProxyDetected(detected) => {
            model.local_proxy_detected = detected;
        }
        LocalMarketClientEvent::RepaintRequested => context.request_repaint(),
        LocalMarketClientEvent::WorkerFailed(error) => {
            model.market_worker_failed = true;
            let _ = model.local_markets.replace([]);
            model.local_quotes.clear();
            model.local_precisions.clear();
            model.local_symbols.clear();
            model.history_requests.clear();
            model.clear_trading_intent();
            model.local_catalog_error = Some(error);
        }
    }
}

#[cfg(test)]
mod tests;
