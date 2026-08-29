pub const INSTRUMENTS: &str = "/api/v3/market/instruments";
pub const ACCOUNT_SETTINGS: &str = "/api/v3/account/settings";
pub const ACCOUNT_INFO: &str = "/api/v3/account/info";
pub const BALANCES: &str = "/api/v3/account/assets";
pub const POSITIONS: &str = "/api/v3/position/current-position";
pub const PLACE_ORDER: &str = "/api/v3/trade/place-order";
pub const PLACE_REALITY_ORDER: &str = "/api/v3/trade/place-reality-order";
pub const AMEND_ORDER: &str = "/api/v3/trade/modify-order";
pub const CANCEL_ORDER: &str = "/api/v3/trade/cancel-order";
pub const CANCEL_REALITY_ORDER: &str = "/api/v3/trade/cancel-reality-order";
pub const OPEN_ORDERS: &str = "/api/v3/trade/unfilled-orders";
pub const ORDER_DETAIL: &str = "/api/v3/trade/order-info";
pub const FILLS: &str = "/api/v3/trade/fills";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_uses_only_reviewed_v3_paths() {
        for path in [
            INSTRUMENTS,
            ACCOUNT_SETTINGS,
            ACCOUNT_INFO,
            BALANCES,
            POSITIONS,
            PLACE_ORDER,
            PLACE_REALITY_ORDER,
            AMEND_ORDER,
            CANCEL_ORDER,
            CANCEL_REALITY_ORDER,
            OPEN_ORDERS,
            ORDER_DETAIL,
            FILLS,
        ] {
            assert!(path.starts_with("/api/v3/"));
        }
    }
}
