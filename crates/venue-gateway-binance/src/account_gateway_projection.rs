use venue_execution::{
    AccountPhysicalGateway, AccountRecoveryRequest, AccountSymbolSet, SignedAccountSnapshot,
};

use super::{
    AccountHostValidationError, BTreeSet, BinanceSnapshotFillsRequest, Fill, RecentFillsCursor,
    USER_TRADES_PAGE_LIMIT, Value, build_fills_for_native_symbol_request, json_rows_snapshot,
    snapshot_rules, snapshot_u64, str,
};
use super::{BinanceAccountGateway, BinanceAccountGatewayError};

pub(super) async fn snapshot_fills_cursor(
    request: BinanceSnapshotFillsRequest<'_>,
) -> Result<(String, Vec<Fill>), AccountHostValidationError> {
    let BinanceSnapshotFillsRequest {
        transport,
        credentials,
        scope,
        symbols,
        previous,
        observed_at_ms,
        catalogue,
        generation,
    } = request;
    let default_start = observed_at_ms
        .checked_sub(1)
        .filter(|value| *value > 0)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    let mut fills = Vec::new();
    let mut fill_ids = BTreeSet::new();
    let mut next = previous;
    for native in symbols {
        let mut cursor = next
            .by_native_symbol
            .get(native)
            .copied()
            .unwrap_or(RecentFillsCursor {
                observed_through_ms: default_start,
                last_trade_id: None,
                last_event_time_ms: None,
            });
        let start = cursor.observed_through_ms;
        let mut terminal = false;
        for page_index in 1..=crate::BINANCE_PRIVATE_MAX_PAGES {
            let page_index = u32::try_from(page_index)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let request = build_fills_for_native_symbol_request(
                scope,
                native,
                page_index,
                cursor,
                start,
                observed_at_ms,
            )
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let page = transport
                .execute_read(
                    credentials,
                    &request,
                    transport
                        .signing_timestamp_ms()
                        .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                )
                .await
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let rows = json_rows_snapshot(&page.payload)
                .map_err(|_| AccountHostValidationError::SignedSnapshotStage("fills_rows"))?;
            advance_snapshot_fill_cursor(&mut cursor, &rows, start).map_err(|error| {
                eprintln!(
                    "Signed fills cursor rejected: symbol={native} page={page_index} rows={}",
                    rows.len()
                );
                error
            })?;
            let rules = snapshot_rules(catalogue, native, generation)
                .map_err(|_| AccountHostValidationError::SignedSnapshotStage("fills_rules"))?;
            let payload = str::from_utf8(&page.payload)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            for fill in crate::private::parse_fills(payload, &rules.instrument.symbol)
                .map_err(|_| AccountHostValidationError::SignedSnapshotStage("fills_normalize"))?
            {
                if !fill_ids.insert((fill.symbol.clone(), fill.fill_id.clone())) {
                    return Err(AccountHostValidationError::SignedSnapshot);
                }
                fills.push(fill);
            }
            if rows.len() < usize::from(USER_TRADES_PAGE_LIMIT) {
                terminal = true;
                break;
            }
        }
        if !terminal {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        cursor.observed_through_ms = cursor.observed_through_ms.max(observed_at_ms);
        next.by_native_symbol.insert(native.clone(), cursor);
    }
    Ok((next.encode(), fills))
}

pub(super) fn advance_snapshot_fill_cursor(
    cursor: &mut RecentFillsCursor,
    rows: &[serde_json::Map<String, Value>],
    start: u64,
) -> Result<(), AccountHostValidationError> {
    if rows.len() > usize::from(USER_TRADES_PAGE_LIMIT) {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    for row in rows {
        let id = snapshot_u64(row, "id")?;
        let event_time = snapshot_u64(row, "time")?;
        if cursor.last_trade_id.is_some_and(|previous| id <= previous)
            || cursor
                .last_event_time_ms
                .is_some_and(|previous| event_time < previous)
            // fromId resumes by native identity, without a startTime parameter. An unseen
            // fill may precede the prior REST observation while remaining after the last fill.
            || (cursor.last_trade_id.is_none() && event_time < start)
        {
            eprintln!(
                "Signed fills chronology mismatch: id={id} time={event_time} prior_id={:?} prior_time={:?} start={start}",
                cursor.last_trade_id, cursor.last_event_time_ms
            );
            return Err(AccountHostValidationError::SignedSnapshotStage(
                "fills_cursor_order",
            ));
        }
        cursor.last_trade_id = Some(id);
        cursor.last_event_time_ms = Some(event_time);
    }
    Ok(())
}

/// One bounded, read-only snapshot job. Its HTTP awaits do not borrow the live user stream.
pub struct BinanceProjectionRead {
    transport: crate::BinanceHttpTransport,
    credentials: crate::BinanceCredentials,
    config: crate::BinanceConfig,
    rules: crate::BinanceInstrumentRules,
    connection_generation: u64,
    prior_generation: u64,
    generation: u64,
    attempt: u64,
    request: AccountRecoveryRequest,
}

pub struct BinanceCompletedProjection {
    read: BinanceProjectionRead,
    snapshot: SignedAccountSnapshot,
}

impl BinanceProjectionRead {
    pub async fn collect(self) -> Result<BinanceCompletedProjection, BinanceAccountGatewayError> {
        let snapshot = super::fetch_account_wide_snapshot(super::BinanceSnapshotCollection {
            transport: &self.transport,
            credentials: &self.credentials,
            config: &self.config,
            selected_rules: &self.rules,
            connection_generation: self.connection_generation,
            private_generation: self.generation,
            rules_generation: self.rules.instrument.generation,
            attempt_id: self.attempt,
            recovery: &self.request,
        })
        .await
        .map_err(|_| BinanceAccountGatewayError::Readback)?;
        Ok(BinanceCompletedProjection {
            read: self,
            snapshot,
        })
    }
}

impl BinanceCompletedProjection {
    pub fn snapshot(&self) -> &SignedAccountSnapshot {
        &self.snapshot
    }
}

impl BinanceAccountGateway {
    pub fn prepare_projection_read(
        &mut self,
        cursor: Option<String>,
    ) -> Result<BinanceProjectionRead, BinanceAccountGatewayError> {
        let symbols = AccountSymbolSet::new(
            self.config.gateway_binding(),
            self.projection_symbols.iter().cloned(),
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let request = AccountRecoveryRequest::read_only(
            self.config.gateway_binding().clone(),
            symbols,
            cursor,
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let generation = self.next_private_generation()?;
        Ok(BinanceProjectionRead {
            transport: self.transport_for_private_generation(generation)?,
            credentials: crate::BinanceCredentials::from_secrets(
                self.credentials.api_key.clone(),
                self.credentials.api_secret.clone(),
            )
            .map_err(|_| BinanceAccountGatewayError::Credentials)?,
            config: self.config.clone(),
            rules: self.rules.clone(),
            connection_generation: self.connection_generation,
            prior_generation: self.private_generation,
            generation,
            attempt: self.take_attempt_id()?,
            request,
        })
    }

    /// Called only after the consumer has committed this exact signed baseline. A result from a
    /// replaced adapter or concurrent generation must never relabel buffered user-stream facts.
    pub fn accept_projection_read(
        &mut self,
        completed: BinanceCompletedProjection,
    ) -> Result<(), BinanceAccountGatewayError> {
        if completed.read.connection_generation != self.connection_generation
            || completed.read.prior_generation != self.private_generation
            || completed.read.config.gateway_binding() != self.config.gateway_binding()
        {
            return Err(BinanceAccountGatewayError::Binding);
        }
        for symbol in completed
            .snapshot
            .open_orders()
            .iter()
            .map(|order| &order.symbol)
            .chain(
                completed
                    .snapshot
                    .positions()
                    .iter()
                    .map(|position| &position.symbol),
            )
            .chain(completed.snapshot.fills().iter().map(|fill| &fill.symbol))
        {
            self.projection_symbols.insert(symbol.clone());
        }
        self.transport = completed.read.transport;
        self.private_generation = completed.read.generation;
        self.rolling_dispatch_cache = None;
        Ok(())
    }
    /// Rewinds only affected symbols for authenticated historical repair. No execution permit
    /// or account state is changed; the normal signed pagination and durable dedup still apply.
    pub fn replay_projection_fills_from(
        previous: Option<&str>,
        from: &std::collections::BTreeMap<venue_domain::Symbol, u64>,
    ) -> Result<Option<String>, BinanceAccountGatewayError> {
        if from.is_empty() {
            return Ok(previous.map(str::to_owned));
        }
        let mut cursor = super::parse_snapshot_fills_cursor(previous)
            .map_err(|_| BinanceAccountGatewayError::Binding)?;
        for (symbol, time) in from {
            let start = time
                .checked_sub(1)
                .filter(|time| *time > 0)
                .ok_or(BinanceAccountGatewayError::Binding)?;
            cursor.by_native_symbol.insert(
                crate::native_symbol(symbol),
                super::RecentFillsCursor {
                    observed_through_ms: start,
                    last_trade_id: None,
                    last_event_time_ms: None,
                },
            );
        }
        Ok(Some(cursor.encode()))
    }

    /// Collects a complete, normalized signed account snapshot for a secret-free durable read
    /// model. It grants no dispatch permit and never exposes raw PAPI payloads or credentials.
    pub fn signed_projection_snapshot(
        &mut self,
        previous_fills_cursor: Option<String>,
    ) -> Result<SignedAccountSnapshot, BinanceAccountGatewayError> {
        let configured_symbols = AccountSymbolSet::new(
            self.config.gateway_binding(),
            self.projection_symbols.iter().cloned(),
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let request = AccountRecoveryRequest::read_only(
            self.config.gateway_binding().clone(),
            configured_symbols,
            previous_fills_cursor,
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        <Self as AccountPhysicalGateway>::signed_account_snapshot(self, &request)
            .map_err(BinanceAccountGatewayError::SignedSnapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_cursor_rewinds_only_affected_symbol() -> Result<(), Box<dyn std::error::Error>> {
        let previous = "binance-fills-v1|BTCUSDT,1000,42,999;SOLUSDC,2000,87,1999";
        let from = [("SOL/USDC".parse()?, 1500)].into_iter().collect();
        assert_eq!(
            BinanceAccountGateway::replay_projection_fills_from(Some(previous), &from)?,
            Some("binance-fills-v1|BTCUSDT,1000,42,999;SOLUSDC,1499,,".to_owned())
        );
        assert!(
            BinanceAccountGateway::replay_projection_fills_from(Some("broken"), &from).is_err()
        );
        let invalid = [("SOL/USDC".parse()?, 0)].into_iter().collect();
        assert!(
            BinanceAccountGateway::replay_projection_fills_from(Some(previous), &invalid).is_err()
        );
        Ok(())
    }
}
