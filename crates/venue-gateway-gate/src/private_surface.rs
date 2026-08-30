use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use venue_domain::domain::{AccountBalance, Position, PositionSide};
use venue_gateway_api::GatewayBinding;

use crate::{
    GATE_PRIVATE_PAGE_LIMIT, GATE_STAGE7_ORDER_PROFILE_VERSION, GateContractRules,
    GateFillsReadback, GateGatewayBinding, GateOrderPayloadError, GatePrivatePayloadError,
    GateStage7OrderFamilyCandidate, GateStage7OrderFamilyEvidence, GateStage7OrderFamilyScope,
    GateStage7UnsupportedEvidence, collect_fill_pages, collect_regular_order_pages, endpoints,
    parse_account_balance, parse_dual_position_mode, parse_position,
    validate_stage7_order_families,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatePrivateReadSource {
    Account,
    DualPositions,
    RegularOrders,
    Fills,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GateFillsCursor {
    last_native_id: Option<String>,
}

impl GateFillsCursor {
    pub fn new(last_native_id: Option<String>) -> Result<Self, GatePrivateReadError> {
        if last_native_id
            .as_deref()
            .is_some_and(|value| !valid_native_id(value))
        {
            return Err(GatePrivateReadError::Cursor);
        }
        Ok(Self { last_native_id })
    }

    #[must_use]
    pub fn last_native_id(&self) -> Option<&str> {
        self.last_native_id.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatePreparedPrivateRead {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt: u64,
    pub source: GatePrivateReadSource,
    pub endpoint: String,
    pub query: String,
    pub cursor_before: Option<String>,
}

impl GatePreparedPrivateRead {
    pub(crate) fn validate(
        &self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
    ) -> Result<(), GatePrivateReadError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| GatePrivateReadError::Binding)?;
        validate_rules(binding, rules, self.generation)?;
        if self.attempt == 0 {
            return Err(GatePrivateReadError::Binding);
        }
        let expected = prepare_private_read(
            binding,
            rules,
            self.generation,
            self.attempt,
            self.source,
            GateFillsCursor::new(self.cursor_before.clone())?,
        )?;
        if &expected != self {
            return Err(GatePrivateReadError::Binding);
        }
        Ok(())
    }
}

pub fn prepare_private_read(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    generation: u64,
    attempt: u64,
    source: GatePrivateReadSource,
    cursor: GateFillsCursor,
) -> Result<GatePreparedPrivateRead, GatePrivateReadError> {
    validate_rules(binding, rules, generation)?;
    if attempt == 0 {
        return Err(GatePrivateReadError::Binding);
    }
    let cursor_before = match source {
        GatePrivateReadSource::RegularOrders | GatePrivateReadSource::Fills => {
            cursor.last_native_id
        }
        GatePrivateReadSource::Account | GatePrivateReadSource::DualPositions => {
            if cursor.last_native_id.is_some() {
                return Err(GatePrivateReadError::Cursor);
            }
            None
        }
    };
    let (endpoint, query) = match source {
        GatePrivateReadSource::Account => (endpoints::FUTURES_ACCOUNT.to_owned(), String::new()),
        GatePrivateReadSource::DualPositions => (
            format!(
                "{}/{}",
                endpoints::DUAL_POSITIONS_PREFIX,
                rules.native_symbol
            ),
            "holding=false".to_owned(),
        ),
        GatePrivateReadSource::RegularOrders => {
            let mut query = format!(
                "contract={}&limit={GATE_PRIVATE_PAGE_LIMIT}&status=open",
                rules.native_symbol
            );
            append_cursor(&mut query, cursor_before.as_deref());
            (endpoints::FUTURES_OPEN_ORDERS.to_owned(), query)
        }
        GatePrivateReadSource::Fills => {
            let mut query = format!(
                "contract={}&limit={GATE_PRIVATE_PAGE_LIMIT}",
                rules.native_symbol
            );
            append_cursor(&mut query, cursor_before.as_deref());
            (endpoints::FUTURES_FILLS.to_owned(), query)
        }
    };
    Ok(GatePreparedPrivateRead {
        binding: binding.gateway_binding().clone(),
        generation,
        attempt,
        source,
        endpoint,
        query,
        cursor_before,
    })
}

#[derive(Clone, Eq, PartialEq)]
pub struct GateRawPrivateResponse {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt: u64,
    pub source: GatePrivateReadSource,
    pub endpoint: String,
    pub query: String,
    pub cursor_before: Option<String>,
    pub requested_at_ms: u64,
    pub received_at_ms: u64,
    pub payload: String,
    pub payload_sha256: [u8; 32],
}

impl std::fmt::Debug for GateRawPrivateResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GateRawPrivateResponse")
            .field("binding", &self.binding)
            .field("generation", &self.generation)
            .field("attempt", &self.attempt)
            .field("source", &self.source)
            .field("endpoint", &self.endpoint)
            .field("query", &self.query)
            .field("cursor_before", &self.cursor_before)
            .field("requested_at_ms", &self.requested_at_ms)
            .field("received_at_ms", &self.received_at_ms)
            .field("payload", &"[REDACTED SIGNED PRIVATE PAYLOAD]")
            .field("payload_sha256", &self.payload_sha256)
            .finish()
    }
}

impl GateRawPrivateResponse {
    pub fn from_response(
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        request: &GatePreparedPrivateRead,
        requested_at_ms: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, GatePrivateReadError> {
        request.validate(binding, rules)?;
        if requested_at_ms == 0 || received_at_ms < requested_at_ms || payload.is_empty() {
            return Err(GatePrivateReadError::Payload);
        }
        let payload_sha256 = Sha256::digest(payload.as_bytes()).into();
        Ok(Self {
            binding: request.binding.clone(),
            generation: request.generation,
            attempt: request.attempt,
            source: request.source,
            endpoint: request.endpoint.clone(),
            query: request.query.clone(),
            cursor_before: request.cursor_before.clone(),
            requested_at_ms,
            received_at_ms,
            payload,
            payload_sha256,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatePrivateReadbackCandidate {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub attempt: u64,
    pub observed_at_ms: u64,
    pub user_id: String,
    pub balance: AccountBalance,
    pub positions: [Position; 2],
    pub order_families: GateStage7OrderFamilyCandidate,
    pub fills: GateFillsReadback,
    pub fills_cursor_before: GateFillsCursor,
    pub fills_cursor_after: GateFillsCursor,
    pub raw_payload_digests: Vec<[u8; 32]>,
}

pub fn validate_private_readback<I>(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    profile_version: u64,
    expires_at_ms: u64,
    validated_at_ms: u64,
    responses: I,
) -> Result<GatePrivateReadbackCandidate, GatePrivateReadError>
where
    I: IntoIterator<Item = GateRawPrivateResponse>,
{
    if profile_version != GATE_STAGE7_ORDER_PROFILE_VERSION {
        return Err(GatePrivateReadError::Profile);
    }
    let responses = responses.into_iter().collect::<Vec<_>>();
    let first = responses
        .first()
        .ok_or(GatePrivateReadError::MissingSurface)?;
    validate_rules(binding, rules, first.generation)?;
    if first.attempt == 0
        || responses.iter().any(|raw| {
            raw.binding != *binding.gateway_binding()
                || raw.generation != first.generation
                || raw.attempt != first.attempt
                || raw.requested_at_ms == 0
                || raw.received_at_ms < raw.requested_at_ms
                || Sha256::digest(raw.payload.as_bytes()).as_slice() != raw.payload_sha256
        })
    {
        return Err(GatePrivateReadError::Binding);
    }
    for raw in &responses {
        let expected = prepare_private_read(
            binding,
            rules,
            raw.generation,
            raw.attempt,
            raw.source,
            GateFillsCursor::new(raw.cursor_before.clone())?,
        )?;
        if raw.binding != expected.binding
            || raw.endpoint != expected.endpoint
            || raw.query != expected.query
            || raw.cursor_before != expected.cursor_before
        {
            return Err(GatePrivateReadError::Cursor);
        }
    }
    let observed_at_ms = responses
        .iter()
        .map(|raw| raw.received_at_ms)
        .max()
        .ok_or(GatePrivateReadError::MissingSurface)?;
    let started_at_ms = responses
        .iter()
        .map(|raw| raw.requested_at_ms)
        .min()
        .ok_or(GatePrivateReadError::MissingSurface)?;
    if expires_at_ms <= observed_at_ms
        || validated_at_ms < observed_at_ms
        || validated_at_ms >= expires_at_ms
        || observed_at_ms.saturating_sub(started_at_ms) > 3_000
    {
        return Err(GatePrivateReadError::Freshness);
    }

    let account = one(&responses, GatePrivateReadSource::Account)?;
    let positions = one(&responses, GatePrivateReadSource::DualPositions)?;
    let regular = pages(&responses, GatePrivateReadSource::RegularOrders)?;
    let fills = pages(&responses, GatePrivateReadSource::Fills)?;
    if account.cursor_before.is_some() || positions.cursor_before.is_some() {
        return Err(GatePrivateReadError::Cursor);
    }
    let account_value: Value =
        serde_json::from_str(&account.payload).map_err(|_| GatePrivateReadError::Payload)?;
    if !parse_dual_position_mode(&account_value).map_err(GatePrivateReadError::Risk)? {
        return Err(GatePrivateReadError::Positions);
    }
    let balance = parse_account_balance(&account_value).map_err(GatePrivateReadError::Private)?;
    let (user_id, positions) = parse_two_legs(&positions.payload, rules)?;

    validate_page_chain(&regular, None)?;
    let regular_readback = collect_regular_order_pages(
        regular.iter().map(|raw| raw.payload.as_str()),
        &rules.instrument.symbol,
        rules,
    )
    .map_err(GatePrivateReadError::Orders)?;
    let fills_cursor_before = GateFillsCursor::new(
        fills
            .first()
            .ok_or(GatePrivateReadError::MissingSurface)?
            .cursor_before
            .clone(),
    )?;
    validate_page_chain(&fills, fills_cursor_before.last_native_id())?;
    let fills_readback = collect_fill_pages(
        fills.iter().map(|raw| raw.payload.as_str()),
        &rules.instrument.symbol,
        rules,
    )
    .map_err(GatePrivateReadError::Orders)?;
    let fills_cursor_after = GateFillsCursor::new(
        fills_readback
            .last_native_id
            .clone()
            .or_else(|| fills_cursor_before.last_native_id.clone()),
    )?;
    let scope = GateStage7OrderFamilyScope {
        binding: binding.gateway_binding().clone(),
        profile_version,
        attempt: first.attempt,
        generation: first.generation,
        observed_at_ms,
        expires_at_ms,
    };
    let order_families = validate_stage7_order_families(
        scope,
        rules,
        validated_at_ms,
        [
            GateStage7OrderFamilyEvidence::Regular(regular_readback),
            GateStage7OrderFamilyEvidence::Unsupported(GateStage7UnsupportedEvidence::conditional(
                profile_version,
            )),
            GateStage7OrderFamilyEvidence::Unsupported(GateStage7UnsupportedEvidence::algo(
                profile_version,
            )),
        ],
    )
    .map_err(|_| GatePrivateReadError::Profile)?;
    Ok(GatePrivateReadbackCandidate {
        binding: binding.gateway_binding().clone(),
        generation: first.generation,
        attempt: first.attempt,
        observed_at_ms,
        user_id,
        balance,
        positions,
        order_families,
        fills: fills_readback,
        fills_cursor_before,
        fills_cursor_after,
        raw_payload_digests: responses.iter().map(|raw| raw.payload_sha256).collect(),
    })
}

fn parse_two_legs(
    payload: &str,
    rules: &GateContractRules,
) -> Result<(String, [Position; 2]), GatePrivateReadError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| GatePrivateReadError::Payload)?;
    let rows = value.as_array().ok_or(GatePrivateReadError::Payload)?;
    if rows.len() != 2 {
        return Err(GatePrivateReadError::Positions);
    }
    let mut user_id = None;
    let mut long = None;
    let mut short = None;
    for row in rows {
        let object = row.as_object().ok_or(GatePrivateReadError::Payload)?;
        if object.get("contract").and_then(Value::as_str) != Some(&rules.native_symbol) {
            return Err(GatePrivateReadError::Positions);
        }
        let user = identifier(object.get("user")).ok_or(GatePrivateReadError::Positions)?;
        if user_id.as_ref().is_some_and(|expected| expected != &user) {
            return Err(GatePrivateReadError::Positions);
        }
        user_id = Some(user);
        let position = parse_position(row, &rules.instrument.symbol, rules)
            .map_err(GatePrivateReadError::Private)?;
        let slot = match position.side {
            PositionSide::Long => &mut long,
            PositionSide::Short => &mut short,
            PositionSide::Net => return Err(GatePrivateReadError::Positions),
        };
        if slot.replace(position).is_some() {
            return Err(GatePrivateReadError::Positions);
        }
    }
    Ok((
        user_id.ok_or(GatePrivateReadError::Positions)?,
        [
            long.ok_or(GatePrivateReadError::Positions)?,
            short.ok_or(GatePrivateReadError::Positions)?,
        ],
    ))
}

fn validate_page_chain(
    pages: &[&GateRawPrivateResponse],
    expected_start: Option<&str>,
) -> Result<(), GatePrivateReadError> {
    let mut expected = expected_start.map(str::to_owned);
    let mut seen = BTreeSet::new();
    let mut terminal = false;
    for page in pages {
        if terminal || page.cursor_before != expected {
            return Err(GatePrivateReadError::Cursor);
        }
        let value: Value =
            serde_json::from_str(&page.payload).map_err(|_| GatePrivateReadError::Payload)?;
        let rows = value.as_array().ok_or(GatePrivateReadError::Payload)?;
        if rows.len() > GATE_PRIVATE_PAGE_LIMIT {
            return Err(GatePrivateReadError::Cursor);
        }
        let mut last = None;
        for row in rows {
            let object = row.as_object().ok_or(GatePrivateReadError::Payload)?;
            let id = identifier(object.get("id")).ok_or(GatePrivateReadError::Payload)?;
            if !seen.insert(id.clone()) {
                return Err(GatePrivateReadError::Cursor);
            }
            last = Some(id);
        }
        terminal = rows.len() < GATE_PRIVATE_PAGE_LIMIT;
        if !terminal {
            expected = last;
        }
    }
    if pages.is_empty() || !terminal {
        return Err(GatePrivateReadError::Cursor);
    }
    Ok(())
}

fn one(
    responses: &[GateRawPrivateResponse],
    source: GatePrivateReadSource,
) -> Result<&GateRawPrivateResponse, GatePrivateReadError> {
    let mut matching = responses.iter().filter(|raw| raw.source == source);
    let value = matching
        .next()
        .ok_or(GatePrivateReadError::MissingSurface)?;
    if matching.next().is_some() {
        return Err(GatePrivateReadError::DuplicateSurface);
    }
    Ok(value)
}

fn pages(
    responses: &[GateRawPrivateResponse],
    source: GatePrivateReadSource,
) -> Result<Vec<&GateRawPrivateResponse>, GatePrivateReadError> {
    let pages = responses
        .iter()
        .filter(|raw| raw.source == source)
        .collect::<Vec<_>>();
    if pages.is_empty() {
        return Err(GatePrivateReadError::MissingSurface);
    }
    Ok(pages)
}

fn validate_rules(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    generation: u64,
) -> Result<(), GatePrivateReadError> {
    if generation == 0
        || binding.gateway_binding().symbol != rules.instrument.symbol
        || generation != rules.instrument.generation
        || rules.native_symbol.trim().is_empty()
        || rules.instrument.validate().is_err()
        || rules.quanto_multiplier <= Decimal::ZERO
    {
        return Err(GatePrivateReadError::Binding);
    }
    Ok(())
}

fn append_cursor(query: &mut String, cursor: Option<&str>) {
    if let Some(cursor) = cursor {
        query.push_str("&last_id=");
        query.push_str(cursor);
    }
}

fn valid_native_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn identifier(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) if valid_native_id(value) => Some(value.clone()),
        Some(Value::Number(value)) if valid_native_id(&value.to_string()) => {
            Some(value.to_string())
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatePrivateReadError {
    #[error("Gate private read binding, generation, or request scope is invalid")]
    Binding,
    #[error("Gate private read cursor is invalid, overlapping, or incomplete")]
    Cursor,
    #[error("Gate private read payload is invalid or was modified")]
    Payload,
    #[error("Gate private read is missing a required signed surface")]
    MissingSurface,
    #[error("Gate private read repeats a singleton signed surface")]
    DuplicateSurface,
    #[error("Gate private read is stale, future-dated, or too wide")]
    Freshness,
    #[error("Gate private read does not prove exactly one long and one short leg")]
    Positions,
    #[error("Gate private read profile evidence is invalid")]
    Profile,
    #[error(transparent)]
    Private(#[from] GatePrivatePayloadError),
    #[error(transparent)]
    Orders(#[from] GateOrderPayloadError),
    #[error(transparent)]
    Risk(#[from] crate::GateRiskError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{Amount, Instrument, MarketKind, Price};
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

    fn facts() -> Result<(GateGatewayBinding, GateContractRules), Box<dyn std::error::Error>> {
        let binding = GateGatewayBinding::new(GatewayBinding::new(
            VenueId::Gate,
            GatewayMode::Live,
            ACCOUNT_ID,
            "DOGE/USDT".parse()?,
        )?)?;
        let rules = GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                symbol: "DOGE/USDT".parse()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some("USDT".parse()?),
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            decimal_contracts: false,
        };
        Ok((binding, rules))
    }

    fn raw(
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        source: GatePrivateReadSource,
        cursor: Option<&str>,
        payload: &str,
    ) -> Result<GateRawPrivateResponse, Box<dyn std::error::Error>> {
        let request = prepare_private_read(
            binding,
            rules,
            7,
            11,
            source,
            GateFillsCursor::new(cursor.map(str::to_owned))?,
        )?;
        Ok(GateRawPrivateResponse::from_response(
            binding,
            rules,
            &request,
            1_000,
            1_100,
            payload.to_owned(),
        )?)
    }

    fn complete(
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
    ) -> Result<Vec<GateRawPrivateResponse>, Box<dyn std::error::Error>> {
        Ok(vec![
            raw(
                binding,
                rules,
                GatePrivateReadSource::Account,
                None,
                r#"{"position_mode":"dual","total":"10","available":"9"}"#,
            )?,
            raw(
                binding,
                rules,
                GatePrivateReadSource::DualPositions,
                None,
                r#"[{"user":42,"contract":"DOGE_USDT","mode":"dual_long","size":"0","entry_price":"0","mark_price":"0"},{"user":42,"contract":"DOGE_USDT","mode":"dual_short","size":"2","entry_price":"0.1","mark_price":"0.11"}]"#,
            )?,
            raw(
                binding,
                rules,
                GatePrivateReadSource::RegularOrders,
                None,
                include_str!("../tests/fixtures/regular_orders.json"),
            )?,
            raw(
                binding,
                rules,
                GatePrivateReadSource::Fills,
                Some("227262265"),
                include_str!("../tests/fixtures/fills.json"),
            )?,
        ])
    }

    #[test]
    fn complete_attempt_binds_account_two_legs_regular_profile_and_fill_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let candidate = validate_private_readback(
            &binding,
            &rules,
            GATE_STAGE7_ORDER_PROFILE_VERSION,
            2_000,
            1_200,
            complete(&binding, &rules)?,
        )?;
        assert_eq!(candidate.user_id, "42");
        assert_eq!(candidate.positions[0].side, PositionSide::Long);
        assert_eq!(candidate.positions[1].side, PositionSide::Short);
        assert_eq!(candidate.order_families.regular().orders.len(), 2);
        assert_eq!(
            candidate.fills_cursor_before.last_native_id(),
            Some("227262265")
        );
        assert_eq!(
            candidate.fills_cursor_after.last_native_id(),
            Some("227262267")
        );
        Ok(())
    }

    #[test]
    fn missing_leg_cross_attempt_and_cursor_overlap_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let mut missing = complete(&binding, &rules)?;
        missing[1].payload =
            r#"[{"user":42,"contract":"DOGE_USDT","mode":"dual_long","size":"0"}]"#.to_owned();
        missing[1].payload_sha256 = Sha256::digest(missing[1].payload.as_bytes()).into();
        assert_eq!(
            validate_private_readback(&binding, &rules, 1, 2_000, 1_200, missing),
            Err(GatePrivateReadError::Positions)
        );

        let mut crossed = complete(&binding, &rules)?;
        crossed[2].attempt = 12;
        assert_eq!(
            validate_private_readback(&binding, &rules, 1, 2_000, 1_200, crossed),
            Err(GatePrivateReadError::Binding)
        );

        let mut cursor = complete(&binding, &rules)?;
        cursor[3].cursor_before = Some("9".to_owned());
        assert_eq!(
            validate_private_readback(&binding, &rules, 1, 2_000, 1_200, cursor),
            Err(GatePrivateReadError::Cursor)
        );
        Ok(())
    }
}
