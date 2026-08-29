const TRADING_ACCOUNT_ID_LEN: usize = 36;

/// Canonical stable trading-account identity used across configuration, runtime, and gateways.
#[must_use]
pub fn is_canonical_trading_account_id(value: &str) -> bool {
    value.len() == TRADING_ACCOUNT_ID_LEN
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_identity_accepts_only_the_canonical_uuid_shape() {
        assert!(is_canonical_trading_account_id(
            "00000000-0000-4000-8000-000000000001"
        ));
        assert!(!is_canonical_trading_account_id("portfolio_margin_um"));
        assert!(!is_canonical_trading_account_id(
            "000000000000-4000-8000-000000000001"
        ));
    }
}
