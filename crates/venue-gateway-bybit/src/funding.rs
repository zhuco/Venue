use std::{collections::BTreeSet, str::FromStr};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{Amount, Asset, Symbol};
use venue_gateway_api::GatewayBinding;

use crate::{
    BybitError, BybitGatewayBinding, BybitHistoryWindow, BybitPrivateSource,
    BybitRawPrivatePayload, linear_native_symbol,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BybitFundingQuery {
    pub start_ms: u64,
    pub end_ms: u64,
    pub cursor: Option<String>,
}

impl BybitFundingQuery {
    pub fn new(start_ms: u64, end_ms: u64, cursor: Option<String>) -> Result<Self, BybitError> {
        BybitHistoryWindow::new(start_ms, end_ms)?;
        if cursor.as_deref().is_some_and(|value| !valid_cursor(value)) {
            return Err(BybitError::Pagination);
        }
        Ok(Self {
            start_ms,
            end_ms,
            cursor,
        })
    }

    pub(crate) fn window(&self) -> Result<BybitHistoryWindow, BybitError> {
        BybitHistoryWindow::new(self.start_ms, self.end_ms)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BybitFundingSettlement {
    pub settlement_id: String,
    pub symbol: Symbol,
    pub settled_at_ms: u64,
    pub funding: Amount,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BybitFundingReadback {
    pub binding: GatewayBinding,
    pub observed_at_ms: u64,
    pub requested_cursor: Option<String>,
    pub settlements: Vec<BybitFundingSettlement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BybitFundingPage {
    pub raw: BybitRawPrivatePayload,
    pub next_cursor: Option<String>,
    pub settlements: Vec<BybitFundingSettlement>,
}

pub(crate) fn parse_funding_page(
    binding: &BybitGatewayBinding,
    raw: &BybitRawPrivatePayload,
) -> Result<BybitFundingPage, BybitError> {
    raw.validate(binding, BybitPrivateSource::FundingTransactions)?;
    let envelope: FundingEnvelope =
        serde_json::from_slice(&raw.payload).map_err(|_| BybitError::Payload)?;
    if envelope.ret_code != 0 || envelope.ret_msg != "OK" || envelope.result.list.len() > 50 {
        return Err(BybitError::Rejected);
    }
    let window = raw.history_window.as_ref().ok_or(BybitError::Clock)?;
    let native_symbol =
        linear_native_symbol(&raw.binding.symbol).map_err(|_| BybitError::Binding)?;
    let quote = raw.binding.symbol.quote();
    let mut ids = BTreeSet::new();
    let mut previous_time = None;
    let settlements = envelope
        .result
        .list
        .into_iter()
        .map(|row| {
            let settled_at_ms = row
                .transaction_time
                .parse::<u64>()
                .ok()
                .filter(|value| {
                    *value >= window.start_ms
                        && *value <= window.end_ms
                        && *value <= raw.received_at_ms
                })
                .ok_or(BybitError::Clock)?;
            if row.id.is_empty()
                || row.id.len() > 128
                || !row.id.bytes().all(|byte| byte.is_ascii_graphic())
                || !ids.insert(row.id.clone())
                || row.symbol != native_symbol
                || row.category != "linear"
                || row.transaction_type != "SETTLEMENT"
                || row.currency != quote
                || previous_time.is_some_and(|previous| settled_at_ms > previous)
            {
                return Err(BybitError::Payload);
            }
            previous_time = Some(settled_at_ms);
            let funding = Decimal::from_str(&row.funding).map_err(|_| BybitError::Payload)?;
            Ok(BybitFundingSettlement {
                settlement_id: row.id,
                symbol: raw.binding.symbol.clone(),
                settled_at_ms,
                funding: Amount::new(
                    Asset::new(&row.currency).map_err(|_| BybitError::Payload)?,
                    funding,
                ),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = match envelope.result.next_page_cursor.unwrap_or_default() {
        value if value.is_empty() => None,
        value if valid_cursor(&value) => Some(value),
        _ => return Err(BybitError::Pagination),
    };
    Ok(BybitFundingPage {
        raw: raw.clone(),
        next_cursor,
        settlements,
    })
}

pub(crate) fn complete_funding_pages(
    binding: &BybitGatewayBinding,
    pages: &[BybitFundingPage],
    requested_cursor: Option<String>,
) -> Result<BybitFundingReadback, BybitError> {
    let first = pages.first().ok_or(BybitError::Pagination)?;
    if pages.len() > crate::BYBIT_PRIVATE_MAX_PAGES {
        return Err(BybitError::Pagination);
    }
    let mut expected_cursor = requested_cursor.clone();
    let mut ids = BTreeSet::new();
    let mut previous_time = None;
    let mut settlements = Vec::new();
    for (index, page) in pages.iter().enumerate() {
        if parse_funding_page(binding, &page.raw)? != *page
            || page.raw.binding != first.raw.binding
            || page.raw.generation != first.raw.generation
            || page.raw.attempt_id != first.raw.attempt_id
            || page.raw.history_window != first.raw.history_window
            || usize::try_from(page.raw.page_index).map_err(|_| BybitError::Pagination)? != index
            || page.raw.request_cursor != expected_cursor
            || (page.settlements.is_empty() && page.next_cursor.is_some())
        {
            return Err(BybitError::Pagination);
        }
        for settlement in &page.settlements {
            if !ids.insert(settlement.settlement_id.clone())
                || previous_time.is_some_and(|previous| settlement.settled_at_ms > previous)
            {
                return Err(BybitError::Pagination);
            }
            previous_time = Some(settlement.settled_at_ms);
            settlements.push(settlement.clone());
        }
        expected_cursor = page.next_cursor.clone();
    }
    if expected_cursor.is_some() {
        return Err(BybitError::Pagination);
    }
    Ok(BybitFundingReadback {
        binding: first.raw.binding.clone(),
        observed_at_ms: pages
            .iter()
            .map(|page| page.raw.received_at_ms)
            .max()
            .ok_or(BybitError::Pagination)?,
        requested_cursor,
        settlements,
    })
}

fn valid_cursor(value: &str) -> bool {
    (1..=2_048).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'&' | b'?' | b'#'))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingEnvelope {
    ret_code: i64,
    ret_msg: String,
    result: FundingResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingResult {
    #[serde(default)]
    next_page_cursor: Option<String>,
    list: Vec<FundingRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRow {
    id: String,
    symbol: String,
    category: String,
    transaction_time: String,
    #[serde(rename = "type")]
    transaction_type: String,
    currency: String,
    funding: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

    fn binding() -> Result<BybitGatewayBinding, Box<dyn std::error::Error>> {
        Ok(BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            ACCOUNT_ID,
            "SOL/USDT".parse()?,
        )?)?)
    }

    fn raw(
        binding: &BybitGatewayBinding,
        page_index: u32,
        cursor: Option<&str>,
        payload: &[u8],
    ) -> Result<BybitRawPrivatePayload, Box<dyn std::error::Error>> {
        let request = crate::prepare_private_request(
            binding,
            7,
            11,
            page_index,
            BybitPrivateSource::FundingTransactions,
            cursor,
            Some(BybitHistoryWindow::new(1_000, 2_000)?),
            None,
        )?;
        Ok(BybitRawPrivatePayload::from_response(
            binding,
            &request,
            1_500,
            2_000,
            payload.to_vec(),
        )?)
    }

    #[test]
    fn query_is_account_symbol_time_and_cursor_bound() -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let request = crate::prepare_private_request(
            &binding,
            7,
            11,
            1,
            BybitPrivateSource::FundingTransactions,
            Some("next-token"),
            Some(BybitHistoryWindow::new(1_000, 2_000)?),
            None,
        )?;
        assert_eq!(request.path, crate::endpoints::TRANSACTION_LOG);
        assert_eq!(
            request.query,
            "accountType=UNIFIED&category=linear&currency=USDT&baseCoin=SOL&type=SETTLEMENT&startTime=1000&endTime=2000&limit=50&cursor=next-token"
        );
        let continuation = crate::prepare_private_request(
            &binding,
            7,
            12,
            0,
            BybitPrivateSource::FundingTransactions,
            Some("next-token"),
            Some(BybitHistoryWindow::new(1_000, 2_000)?),
            None,
        )?;
        assert_eq!(continuation.request_cursor.as_deref(), Some("next-token"));
        assert!(BybitFundingQuery::new(1_000, 2_000, Some("bad&cursor".into())).is_err());
        Ok(())
    }

    #[test]
    fn pages_preserve_signed_funding_asset_and_reject_duplicate_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let first = br#"{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"next","list":[{"id":"fund-2","symbol":"SOLUSDT","category":"linear","transactionTime":"1900","type":"SETTLEMENT","currency":"USDT","funding":"0.02"}]},"time":2000}"#;
        let second = br#"{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"","list":[{"id":"fund-1","symbol":"SOLUSDT","category":"linear","transactionTime":"1800","type":"SETTLEMENT","currency":"USDT","funding":"-0.01"}]},"time":2000}"#;
        let pages = [
            parse_funding_page(&binding, &raw(&binding, 0, None, first)?)?,
            parse_funding_page(&binding, &raw(&binding, 1, Some("next"), second)?)?,
        ];
        let readback = complete_funding_pages(&binding, &pages, None)?;
        assert_eq!(readback.settlements.len(), 2);
        assert_eq!(readback.settlements[0].funding.asset, Asset::new("USDT")?);
        assert_eq!(readback.settlements[1].funding.value, Decimal::new(-1, 2));

        let duplicate = String::from_utf8(second.to_vec())?.replace("fund-1", "fund-2");
        let duplicate_pages = [
            pages[0].clone(),
            parse_funding_page(
                &binding,
                &raw(&binding, 1, Some("next"), duplicate.as_bytes())?,
            )?,
        ];
        assert!(complete_funding_pages(&binding, &duplicate_pages, None).is_err());

        let empty = br#"{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":null,"list":[]},"time":2000}"#;
        let empty_pages = [parse_funding_page(
            &binding,
            &raw(&binding, 0, None, empty)?,
        )?];
        assert!(
            complete_funding_pages(&binding, &empty_pages, None)?
                .settlements
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn wrong_symbol_type_or_currency_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let valid = r#"{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"","list":[{"id":"fund-1","symbol":"SOLUSDT","category":"linear","transactionTime":"1800","type":"SETTLEMENT","currency":"USDT","funding":"-0.01"}]},"time":2000}"#;
        for payload in [
            valid.replace("SOLUSDT", "ETHUSDT"),
            valid.replace("SETTLEMENT", "TRADE"),
            valid.replace("USDT", "USDC"),
        ] {
            assert!(
                parse_funding_page(&binding, &raw(&binding, 0, None, payload.as_bytes())?).is_err()
            );
        }
        Ok(())
    }
}
