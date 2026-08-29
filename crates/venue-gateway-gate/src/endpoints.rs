//! Exact Gate REST paths for the admitted USDT perpetual account surface.
//!
//! These constants are data only. They do not create a transport or grant mutation authority.

pub const TIME: &str = "/spot/time";
pub const ACCOUNT_DETAIL: &str = "/account/detail";
pub const ACCOUNT_MAIN_KEYS: &str = "/account/main_keys";
pub const FUTURES_CONTRACTS: &str = "/futures/usdt/contracts";
pub const FUTURES_ACCOUNT: &str = "/futures/usdt/accounts";
pub const UNIFIED_ACCOUNT: &str = "/unified/accounts";
pub const FUTURES_DUAL_MODE: &str = "/futures/usdt/dual_mode";
pub const POSITIONS: &str = "/futures/usdt/positions";
pub const FUTURES_ORDER: &str = "/futures/usdt/orders";
pub const FUTURES_OPEN_ORDERS: &str = "/futures/usdt/orders";
pub const FUTURES_FILLS: &str = "/futures/usdt/my_trades_timerange";
