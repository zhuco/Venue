use super::*;

pub(super) async fn fetch_account_wide_risk(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    config: &BinanceConfig,
    selected_rules: &BinanceInstrumentRules,
    private_generation: u64,
    attempt_id: u64,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    let stage = AccountHostValidationError::RiskEvidenceStage;
    let observed_at_ms = now_ms().map_err(|_| stage("clock_start"))?;
    let scope = BinancePrivateReadScope::new(
        config,
        selected_rules,
        private_generation,
        attempt_id,
        observed_at_ms,
    )
    .map_err(|_| stage("scope"))?;
    let catalogue = transport
        .fetch_usd_m_exchange_info()
        .await
        .map_err(|_| stage("exchange_info_read"))?;
    let catalogue = str::from_utf8(&catalogue.payload).map_err(|_| stage("exchange_info_utf8"))?;
    let account_config = signed_page(transport, credentials, build_account_config_request(&scope))
        .await
        .map_err(|_| stage("account_config_read"))?;
    let position_mode = signed_page(transport, credentials, build_position_mode_request(&scope))
        .await
        .map_err(|_| stage("position_mode_read"))?;
    let positions = signed_page(
        transport,
        credentials,
        build_account_wide_positions_request(&scope),
    )
    .await
    .map_err(|_| stage("positions_read"))?;
    let regular = signed_page(
        transport,
        credentials,
        build_account_wide_regular_orders_request(&scope),
    )
    .await
    .map_err(|_| stage("regular_orders_read"))?;
    let algo = signed_page(
        transport,
        credentials,
        build_account_wide_algo_orders_request(&scope),
    )
    .await
    .map_err(|_| stage("algo_orders_read"))?;
    let account_config =
        str::from_utf8(&account_config.payload).map_err(|_| stage("account_config_utf8"))?;
    let position_mode =
        str::from_utf8(&position_mode.payload).map_err(|_| stage("position_mode_utf8"))?;
    let capabilities = crate::portfolio::capabilities(account_config, position_mode)
        .map_err(|_| stage("capabilities_parse"))?;
    if !capabilities.can_trade || !capabilities.hedge_position {
        return Err(stage("capabilities_value"));
    }
    let positions = account_position_notionals(catalogue, &positions.payload, private_generation)
        .map_err(|_| stage("positions_normalize"))?;
    let orders = account_entry_order_notionals(
        catalogue,
        &regular.payload,
        &algo.payload,
        private_generation,
    )
    .map_err(|_| stage("orders_normalize"))?;
    let mut quote_assets = positions
        .iter()
        .chain(orders.iter())
        .map(|amount| amount.asset.clone())
        .collect::<BTreeSet<_>>();
    // The selected binding is the only candidate this gateway can normalize. Include its quote
    // even when the signed account is flat, otherwise a valid SOL/USDC entry could not be
    // valued while preserving the complete all-symbol account totals above.
    quote_assets.insert(
        Asset::new(config.gateway_binding().symbol.quote()).map_err(|_| stage("selected_quote"))?,
    );
    let rates = fetch_quote_to_usdt_rates(transport, &quote_assets, private_generation)
        .await
        .map_err(|_| stage("quote_rates"))?;
    AccountRiskEvidence::complete_with_usdt_valuation(
        config.gateway_binding().clone(),
        now_ms().map_err(|_| stage("clock_finish"))?,
        private_generation,
        positions,
        orders,
        rates,
    )
    .map_err(|_| stage("evidence_complete"))?
    .with_earliest_observation(observed_at_ms)
    .map_err(|_| stage("evidence_window"))
}

/// Public asset-index evidence converts every native quote asset from the complete signed
/// position/order collection. USDT is the sole identity; every other quote uses its own USD
/// index divided by the independently observed USDT USD index.
pub(super) async fn fetch_quote_to_usdt_rates(
    transport: &BinanceHttpTransport,
    quote_assets: &BTreeSet<Asset>,
    private_generation: u64,
) -> Result<Vec<AccountQuoteToUsdtRate>, AccountHostValidationError> {
    let usdt = Asset::new("USDT").map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let non_usdt = quote_assets
        .iter()
        .filter(|asset| *asset != &usdt)
        .cloned()
        .collect::<Vec<_>>();
    if non_usdt.is_empty() {
        return Ok(Vec::new());
    }
    if non_usdt.len() >= MAX_ACCOUNT_RISK_QUOTE_ASSETS {
        return Err(AccountHostValidationError::RiskEvidence);
    }

    let mut required = non_usdt.clone();
    required.push(usdt.clone());
    let mut usd_per_asset = BTreeMap::new();
    for asset in required {
        let response = transport
            .fetch_usd_m_asset_index(&asset)
            .await
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let payload = str::from_utf8(&response.payload)
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let evidence = crate::portfolio::parse_usd_conversion_evidence(
            payload,
            asset.clone(),
            private_generation,
            response.received_at_ms,
            MAX_ACCOUNT_RISK_RATE_AGE_MS,
        )
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        if usd_per_asset.insert(asset, evidence).is_some() {
            return Err(AccountHostValidationError::RiskEvidence);
        }
    }
    quote_to_usdt_rates(quote_assets, &usd_per_asset, private_generation)
}

pub(super) fn quote_to_usdt_rates(
    quote_assets: &BTreeSet<Asset>,
    usd_per_asset: &BTreeMap<Asset, crate::portfolio::UsdConversionEvidence>,
    private_generation: u64,
) -> Result<Vec<AccountQuoteToUsdtRate>, AccountHostValidationError> {
    let usdt = Asset::new("USDT").map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let non_usdt = quote_assets
        .iter()
        .filter(|asset| *asset != &usdt)
        .cloned()
        .collect::<Vec<_>>();
    if non_usdt.is_empty() {
        return Ok(Vec::new());
    }
    if non_usdt.len() >= MAX_ACCOUNT_RISK_QUOTE_ASSETS {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    let usdt_usd = usd_per_asset
        .get(&usdt)
        .filter(|evidence| {
            evidence.private_generation == private_generation
                && evidence.observed_at_ms > 0
                && evidence.usd_per_asset > Decimal::ZERO
        })
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    non_usdt
        .into_iter()
        .map(|asset| {
            let quote_usd = usd_per_asset
                .get(&asset)
                .filter(|evidence| {
                    evidence.private_generation == private_generation
                        && evidence.observed_at_ms > 0
                        && evidence.usd_per_asset > Decimal::ZERO
                })
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            let usdt_per_asset = quote_usd
                .usd_per_asset
                .checked_div(usdt_usd.usd_per_asset)
                .filter(|rate| *rate > Decimal::ZERO)
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            Ok(AccountQuoteToUsdtRate {
                asset,
                usdt_per_asset,
                observed_at_ms: quote_usd.source_time_ms.min(usdt_usd.source_time_ms),
                private_generation,
            })
        })
        .collect()
}

pub(super) async fn signed_page(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    request: Result<crate::BinancePrivateReadRequest, crate::BinanceReadbackError>,
) -> Result<crate::BinanceRawPrivatePage, AccountHostValidationError> {
    let request = request.map_err(|_| AccountHostValidationError::RiskEvidence)?;
    transport
        .execute_read(
            credentials,
            &request,
            transport
                .signing_timestamp_ms()
                .map_err(|_| AccountHostValidationError::RiskEvidence)?,
        )
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}

pub(super) fn account_position_notionals(
    catalogue: &str,
    payload: &[u8],
    generation: u64,
) -> Result<Vec<AccountRiskAmount>, AccountHostValidationError> {
    let rows = json_rows(payload)?;
    let mut notionals = Vec::new();
    for row in rows {
        let quantity = decimal_field(&row, "positionAmt")?;
        if quantity.is_zero() {
            continue;
        }
        let rules = account_rules(catalogue, text_field(&row, "symbol")?, generation)?;
        let mark = decimal_field(&row, "markPrice")?;
        let reported = decimal_field(&row, "notional")?.abs();
        let computed = quantity
            .abs()
            .checked_mul(mark)
            .ok_or(AccountHostValidationError::Notional)?;
        if mark <= Decimal::ZERO || reported != computed.round_dp(8) {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        validate_quantity(&rules, quantity.abs())?;
        notionals.push(AccountRiskAmount {
            asset: quote_asset(&rules)?,
            value: reported,
        });
    }
    Ok(notionals)
}

pub(super) fn account_entry_order_notionals(
    catalogue: &str,
    regular: &[u8],
    algo: &[u8],
    generation: u64,
) -> Result<Vec<AccountRiskAmount>, AccountHostValidationError> {
    let mut notionals = Vec::new();
    for row in json_rows(regular)? {
        let reduce_only = bool_field(&row, "reduceOnly")?;
        let rules = account_rules(catalogue, text_field(&row, "symbol")?, generation)?;
        let quantity = decimal_field(&row, "origQty")?;
        let filled = decimal_field(&row, "executedQty")?;
        let remaining = quantity
            .checked_sub(filled)
            .ok_or(AccountHostValidationError::RiskEvidence)?;
        let price = decimal_field(&row, "price")?;
        if remaining <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        validate_quantity(&rules, remaining)?;
        if price % rules.instrument.price_tick.value() != Decimal::ZERO {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        if !reduce_only {
            notionals.push(AccountRiskAmount {
                asset: quote_asset(&rules)?,
                value: remaining
                    .checked_mul(price)
                    .ok_or(AccountHostValidationError::Notional)?,
            });
        }
    }
    for row in json_rows(algo)? {
        // Conditional family has several wire shapes. A non-reduce strategy that is not fully
        // normalized must reserve no guessed value: it closes entry admission until reconciled.
        if !bool_field(&row, "reduceOnly")? && !bool_field(&row, "closePosition")? {
            return Err(AccountHostValidationError::RiskEvidence);
        }
    }
    Ok(notionals)
}

pub(super) fn account_rules(
    catalogue: &str,
    native: &str,
    generation: u64,
) -> Result<BinanceInstrumentRules, AccountHostValidationError> {
    let rules = parse_native_instrument_rules(catalogue, native, generation)
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    Ok(rules)
}

pub(super) fn quote_asset(
    rules: &BinanceInstrumentRules,
) -> Result<Asset, AccountHostValidationError> {
    Asset::new(rules.instrument.symbol.quote())
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}
