use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct Response {
    code: String,
    data: Vec<TradingPair>,
}

#[derive(Deserialize)]
struct TradingPair {
    symbol: String,
}

#[cfg(test)]
fn contains_symbol(payload: &[u8], native_symbol: &str) -> bool {
    allowed_symbols(payload).is_some_and(|symbols| symbols.contains(native_symbol))
}

pub(crate) fn allowed_symbols(payload: &[u8]) -> Option<BTreeSet<String>> {
    let response = serde_json::from_slice::<Response>(payload).ok()?;
    if response.code != "00000" || response.data.is_empty() || response.data.len() > 10_000 {
        return None;
    }
    let mut symbols = BTreeSet::new();
    for pair in response.data {
        if pair.symbol.is_empty()
            || pair.symbol.len() > 128
            || !pair.symbol.chars().all(char::is_alphanumeric)
            || !symbols.insert(pair.symbol)
        {
            return None;
        }
    }
    Some(symbols)
}

#[cfg(test)]
mod tests {
    use super::contains_symbol;

    #[test]
    fn signed_scope_requires_success_exact_membership_and_unique_symbols() {
        let valid =
            br#"{"code":"00000","data":[{"symbol":"ADAUSDT","leverage":"50","marginDetails":[]}]}"#;
        assert!(contains_symbol(valid, "ADAUSDT"));
        assert!(!contains_symbol(valid, "BTCUSDT"));
        let unicode =
            "{\"code\":\"00000\",\"data\":[{\"symbol\":\"牛来USDT\"},{\"symbol\":\"ADAUSDT\"}]}";
        assert!(contains_symbol(unicode.as_bytes(), "ADAUSDT"));
        for invalid in [
            br#"{"code":"40014","data":[{"symbol":"ADAUSDT"}]}"#.as_slice(),
            br#"{"code":"00000","data":[]}"#,
            br#"{"code":"00000","data":[{"symbol":"ADAUSDT"},{"symbol":"ADAUSDT"}]}"#,
            br#"{"code":"00000","data":[{"symbol":"ADAUSDT"},{}]}"#,
            br#"{"code":"00000","data":[{"symbol":"adausdt"}]}"#,
        ] {
            assert!(!contains_symbol(invalid, "ADAUSDT"));
        }
    }
}
