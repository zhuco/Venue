use super::*;

pub(super) async fn snapshot_market_order_facts(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    scope: &BinancePrivateReadScope,
    catalogue: &str,
    generation: u64,
    fills: &[Fill],
) -> Result<Vec<SignedMarketOrderFact>, AccountHostValidationError> {
    let mut first_by_order = BTreeMap::<(String, String), &Fill>::new();
    for fill in fills {
        let native = native_symbol(&fill.symbol);
        let key = (native, fill.order_id.clone());
        match first_by_order.get(&key) {
            Some(first) => {
                if first.symbol != fill.symbol
                    || first.side != fill.side
                    || first.position_side != fill.position_side
                {
                    return Err(AccountHostValidationError::SignedSnapshot);
                }
                let first_key = (
                    first.exchange_time_ms.unwrap_or(u64::MAX),
                    sequence(&first.execution_sequence),
                );
                let candidate_key = (
                    fill.exchange_time_ms.unwrap_or(u64::MAX),
                    sequence(&fill.execution_sequence),
                );
                if candidate_key < first_key {
                    first_by_order.insert(key, fill);
                }
            }
            None => {
                first_by_order.insert(key, fill);
            }
        }
    }

    let mut facts = Vec::new();
    for ((native, native_order_id), first) in first_by_order {
        let page = signed_snapshot_page(
            transport,
            credentials,
            build_exact_order_by_native_id_request(scope, &native, &native_order_id),
        )
        .await?;
        let value: Value = serde_json::from_slice(&page.payload)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let row = value
            .as_object()
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        if snapshot_identifier(row, "orderId")? != native_order_id
            || snapshot_text(row, "symbol")? != native
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        if snapshot_text(row, "type")? != "MARKET" {
            continue;
        }
        let rules = snapshot_rules(catalogue, &native, generation)?;
        let position_side = match &first.position_side {
            FieldState::Known(value) => *value,
            _ => return Err(AccountHostValidationError::SignedSnapshot),
        };
        let quantity = snapshot_decimal(row, "origQty")?;
        let created_at_ms = first
            .exchange_time_ms
            .filter(|value| *value > 0)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        if rules.instrument.symbol != first.symbol
            || snapshot_side(row)? != first.side
            || snapshot_position_side(row)? != position_side
            || quantity <= Decimal::ZERO
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        facts.push(SignedMarketOrderFact {
            client_order_id: snapshot_text(row, "clientOrderId")?.to_owned(),
            venue_order_id: native_order_id,
            symbol: first.symbol.clone(),
            side: first.side,
            position_side,
            quantity,
            reference_price: first.price.value(),
            created_at_ms,
        });
    }
    Ok(facts)
}

fn sequence(value: &FieldState<u64>) -> u64 {
    match value {
        FieldState::Known(value) => *value,
        _ => u64::MAX,
    }
}
