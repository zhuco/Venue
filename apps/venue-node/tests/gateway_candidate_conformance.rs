use std::{ffi::OsString, path::Path};

use venue_gateway_api::{GatewayMode, VenueId};
use venue_node::NodeLaunch;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000041";
const VENUES: [(VenueId, &str); 6] = [
    (VenueId::Binance, "BTC/USDT"),
    (VenueId::Bitget, "BTC/USDT"),
    (VenueId::Bybit, "BTC/USDT"),
    (VenueId::Gate, "DOGE/USDT"),
    (VenueId::Hyperliquid, "BTC/USDC"),
    (VenueId::Okx, "BTC/USDT"),
];

fn arguments(mode: &str, symbol: &str) -> Vec<OsString> {
    vec![
        "venue-node-contract".into(),
        "--mode".into(),
        mode.into(),
        "--trading-account-id".into(),
        ACCOUNT.into(),
        "--symbol".into(),
        symbol.into(),
        "--artifacts-base".into(),
        std::env::temp_dir()
            .join("venue-gateway-candidate-contract")
            .into_os_string(),
    ]
}

#[test]
fn all_six_bindings_keep_test_and_live_roots_disjoint() -> Result<(), Box<dyn std::error::Error>> {
    for (venue, symbol) in VENUES {
        let test = NodeLaunch::try_parse_from(venue, arguments("TEST", symbol))?;
        let live = NodeLaunch::try_parse_from(venue, arguments("LIVE", symbol))?;

        assert_eq!(test.binding().venue, venue);
        assert_eq!(test.binding().mode, GatewayMode::Test);
        assert_eq!(live.binding().venue, venue);
        assert_eq!(live.binding().mode, GatewayMode::Live);
        assert_ne!(test.artifacts_root(), live.artifacts_root());
        assert!(test.artifacts_root().ends_with(Path::new(ACCOUNT)));
        assert!(live.artifacts_root().ends_with(Path::new(ACCOUNT)));

        for invalid in ["test", "live", "SHADOW", " LIVE "] {
            assert!(NodeLaunch::try_parse_from(venue, arguments(invalid, symbol)).is_err());
        }
    }
    Ok(())
}
