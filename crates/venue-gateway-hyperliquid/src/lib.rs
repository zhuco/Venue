mod binding;
mod config;
mod credentials;
mod models;
mod nonce;
mod protocol;

use venue_gateway_api::CapabilityFlags;

pub use binding::{
    HyperliquidGatewayBinding, HyperliquidGatewayBindingError, HyperliquidReadBinding,
};
pub use config::{HyperliquidConfig, endpoints};
pub use credentials::HyperliquidCredentials;
pub use nonce::{NonceCheckpoint, prepare_next_nonce};
pub use protocol::{
    HyperliquidAccountSnapshot, HyperliquidBbo, HyperliquidFill, HyperliquidFillCursor,
    HyperliquidFillPage, HyperliquidFillQuery, HyperliquidInfoRequest, HyperliquidOpenOrder,
    HyperliquidOpenOrdersSnapshot, HyperliquidOrderLookup, HyperliquidOrderStatus,
    HyperliquidPayloadScope, HyperliquidPerpMeta, HyperliquidUserFills,
    build_clearinghouse_state_request, build_l2_book_request, build_meta_request,
    build_open_orders_request, build_order_status_request, build_user_fills_by_time_request,
    parse_clearinghouse_snapshot, parse_l2_book_bbo, parse_open_orders_snapshot,
    parse_order_status, parse_perp_meta, parse_private_user_fills, parse_user_fills_page,
    parse_ws_bbo,
};

/// No account capability is advertised until authenticated readback, private stream,
/// EIP-712 signing, writer ownership, WAL, and UNKNOWN reconciliation are all connected.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HyperliquidError {
    #[error("Hyperliquid named Agent credentials are unavailable or invalid")]
    Credentials,
    #[error("Hyperliquid nonce state is invalid, mismatched, or exhausted")]
    Nonce,
    #[error("Hyperliquid private payload is invalid or incomplete")]
    Payload,
    #[error("Hyperliquid payload does not match the fixed account or instrument binding")]
    Binding,
    #[error(
        "Hyperliquid signing and mutation are unavailable until protocol dependencies and safety gates are approved"
    )]
    SigningUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_domain::domain::{FieldState, OrderState, PositionSide, UnknownReason};
    use venue_gateway_api::{GatewayApiError, GatewayBinding, GatewayMode, VenueId};

    const PRIVATE_EVENTS: &[u8] = include_bytes!("../fixtures/private-account-events.json");
    const META: &[u8] = include_bytes!("../fixtures/perp-meta.json");
    const BOOK: &[u8] = include_bytes!("../fixtures/l2-book.json");
    const CLEARINGHOUSE: &[u8] = include_bytes!("../fixtures/clearinghouse-state.json");
    const OPEN_ORDERS: &[u8] = include_bytes!("../fixtures/open-orders.json");
    const FILLS_PAGE: &[u8] = include_bytes!("../fixtures/fills-page.json");
    const ORDER_STATUS: &[u8] = include_bytes!("../fixtures/order-status.json");
    const USER: &str = "0x0000000000000000000000000000000000000001";
    const AGENT: &str = "0x2222222222222222222222222222222222222222";

    fn binding(
        venue: VenueId,
        mode: GatewayMode,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            venue,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)
    }

    fn read_binding(
        mode: GatewayMode,
    ) -> Result<HyperliquidReadBinding, Box<dyn std::error::Error>> {
        Ok(HyperliquidReadBinding::new(
            HyperliquidGatewayBinding::new(binding(VenueId::Hyperliquid, mode)?)?,
            USER,
        )?)
    }

    fn meta(mode: GatewayMode) -> Result<HyperliquidPerpMeta, Box<dyn std::error::Error>> {
        Ok(parse_perp_meta(META, &read_binding(mode)?)?)
    }

    #[test]
    fn binding_and_config_accept_only_hyperliquid_test_or_live()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!("SHADOW".parse::<GatewayMode>().is_err());
        let test =
            HyperliquidGatewayBinding::new(binding(VenueId::Hyperliquid, GatewayMode::Test)?)?;
        let live =
            HyperliquidGatewayBinding::new(binding(VenueId::Hyperliquid, GatewayMode::Live)?)?;
        let test_config = HyperliquidConfig::for_binding(&test);
        let live_config = HyperliquidConfig::for_binding(&live);
        assert_eq!(test_config.mode(), GatewayMode::Test);
        assert_eq!(live_config.mode(), GatewayMode::Live);
        assert_eq!(
            test_config.rest_origin(),
            "https://api.hyperliquid-testnet.xyz"
        );
        assert_eq!(live_config.rest_origin(), "https://api.hyperliquid.xyz");
        assert_ne!(test_config.websocket(), live_config.websocket());
        assert_eq!(test.gateway_binding().symbol.to_string(), "BTC/USDC");
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn binding_rejects_wrong_venue_and_account() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            HyperliquidGatewayBinding::new(binding(VenueId::Bybit, GatewayMode::Live)?),
            Err(HyperliquidGatewayBindingError::Venue)
        );
        let invalid_account = GatewayBinding {
            venue: VenueId::Hyperliquid,
            mode: GatewayMode::Live,
            trading_account_id: "owner-address-is-not-an-account-uuid".to_owned(),
            symbol: "BTC/USDC".parse()?,
        };
        assert_eq!(
            HyperliquidGatewayBinding::new(invalid_account),
            Err(HyperliquidGatewayBindingError::Gateway(
                GatewayApiError::TradingAccountId
            ))
        );
        Ok(())
    }

    #[test]
    fn named_agent_credentials_reject_an_owner_declared_as_the_agent() {
        let result = HyperliquidCredentials::from_values(
            USER,
            USER,
            None,
            "venue-agent",
            USER,
            "11".repeat(32),
        );
        assert!(matches!(result, Err(HyperliquidError::Credentials)));
        let credential = HyperliquidCredentials::from_values(
            USER,
            USER,
            None,
            "venue-agent",
            AGENT,
            "11".repeat(32),
        );
        assert!(credential.is_ok());
        let vault_owner = HyperliquidCredentials::from_values(
            USER,
            USER,
            Some(AGENT.to_owned()),
            "venue-agent",
            AGENT.to_ascii_uppercase().replace("0X", "0x"),
            "11".repeat(32),
        );
        assert!(matches!(vault_owner, Err(HyperliquidError::Credentials)));
    }

    #[test]
    fn nonce_checkpoint_is_monotonic_and_bound_to_one_agent() -> Result<(), HyperliquidError> {
        let first = prepare_next_nonce(None, AGENT, 1_700_000_000_000)?;
        let recovered = serde_json::from_slice::<NonceCheckpoint>(
            &serde_json::to_vec(&first).map_err(|_| HyperliquidError::Nonce)?,
        )
        .map_err(|_| HyperliquidError::Nonce)?;
        let second = prepare_next_nonce(Some(&recovered), AGENT, 1_699_999_999_000)?;
        assert_eq!(second.last_nonce_ms, first.last_nonce_ms + 1);
        assert_eq!(
            prepare_next_nonce(Some(&second), USER, 1_700_000_000_002),
            Err(HyperliquidError::Nonce)
        );
        Ok(())
    }

    #[test]
    fn meta_and_books_bind_native_coin_user_symbol_and_mode()
    -> Result<(), Box<dyn std::error::Error>> {
        let meta = meta(GatewayMode::Test)?;
        assert_eq!(meta.scope.native_coin(), "BTC");
        assert_eq!(meta.scope.user_address(), USER);
        assert_eq!(meta.scope.symbol().to_string(), "BTC/USDC");
        assert_eq!(meta.scope.mode(), GatewayMode::Test);
        assert_eq!(meta.asset_index, 0);
        assert_eq!(meta.size_decimals, 5);
        let bbo = parse_l2_book_bbo(BOOK, &meta)?;
        assert_eq!(bbo.exchange_time_ms, 1_754_450_974_231);
        assert_eq!(bbo.bid.price.value(), Decimal::new(1_133_770, 1));
        assert_eq!(bbo.ask.price.value(), Decimal::new(1_133_970, 1));
        let ws = br#"{"channel":"bbo","data":{"coin":"BTC","time":1754450974232,"bbo":[{"px":"113377.0","sz":"1.0","n":1},{"px":"113397.0","sz":"2.0","n":2}]}}"#;
        assert_eq!(parse_ws_bbo(ws, &meta)?.exchange_time_ms, 1_754_450_974_232);
        Ok(())
    }

    #[test]
    fn account_and_orders_are_fixed_binding_snapshots() -> Result<(), Box<dyn std::error::Error>> {
        let meta = meta(GatewayMode::Live)?;
        let account = parse_clearinghouse_snapshot(CLEARINGHOUSE, &meta)?;
        assert_eq!(account.scope.mode(), GatewayMode::Live);
        assert_eq!(
            account.balance.wallet_balance,
            Decimal::new(13_109_482_328, 6)
        );
        let position = account.position.ok_or("position missing")?;
        assert_eq!(position.side, PositionSide::Short);
        assert_eq!(position.quantity, Decimal::new(335, 4));

        let orders = parse_open_orders_snapshot(OPEN_ORDERS, &meta, 1_700_000_000_010)?;
        assert_eq!(orders.orders.len(), 1);
        assert_eq!(orders.orders[0].order.state, OrderState::PartiallyFilled);
        assert_eq!(orders.orders[0].order.filled_quantity, Decimal::new(2, 0));
        assert!(orders.orders[0].order.reduce_only);
        Ok(())
    }

    #[test]
    fn private_fill_fixture_preserves_composite_identity_without_fake_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let meta = meta(GatewayMode::Test)?;
        let page = parse_private_user_fills(PRIVATE_EVENTS, &meta)?;
        assert!(!page.is_snapshot);
        assert_eq!(page.fills.len(), 1);
        let item = &page.fills[0];
        assert_eq!(
            item.fill.fill_id,
            "hl:0x0000000000000000000000000000000000000001:1700000000002:BTC:5001"
        );
        assert_eq!(item.fill.order_id, "101");
        assert_eq!(
            item.fill.execution_sequence,
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted
            }
        );
        assert!(matches!(
            item.fill.position_side,
            FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted
            }
        ));
        assert_eq!(
            item.client_order_id,
            FieldState::Known("0x00000000000000000000000000000001".to_owned())
        );
        Ok(())
    }

    #[test]
    fn fill_page_is_sorted_filtered_and_explicitly_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let selected = meta(GatewayMode::Test)?;
        let first = parse_user_fills_page(
            FILLS_PAGE,
            &selected,
            &HyperliquidFillQuery::new(&selected, 1_700_000_000_001, 1_700_000_000_002, 1, None)?,
        )?;
        assert!(!first.complete);
        assert_eq!(first.fills.len(), 1);
        let cursor = first.next_cursor.ok_or("cursor missing")?;
        assert_eq!(cursor.page_coin(), "BTC");
        assert_eq!(cursor.trade_id(), 6001);
        let second = parse_user_fills_page(
            FILLS_PAGE,
            &selected,
            &HyperliquidFillQuery::new(
                &selected,
                1_700_000_000_001,
                1_700_000_000_002,
                10,
                Some(cursor),
            )?,
        )?;
        assert!(second.complete);
        assert_eq!(second.fills.len(), 1);
        assert_eq!(second.fills[0].fill.fill_id.ends_with(":BTC:6002"), true);
        Ok(())
    }

    #[test]
    fn wrong_user_coin_or_malformed_book_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let wrong_user = HyperliquidReadBinding::new(
            HyperliquidGatewayBinding::new(binding(VenueId::Hyperliquid, GatewayMode::Test)?)?,
            "0x3333333333333333333333333333333333333333",
        )?;
        let wrong_user_meta = parse_perp_meta(META, &wrong_user)?;
        assert_eq!(
            parse_private_user_fills(PRIVATE_EVENTS, &wrong_user_meta),
            Err(HyperliquidError::Binding)
        );
        let eth_binding = GatewayBinding::new(
            VenueId::Hyperliquid,
            GatewayMode::Test,
            "00000000-0000-4000-8000-000000000001",
            "ETH/USDC".parse()?,
        )?;
        let eth = HyperliquidReadBinding::new(HyperliquidGatewayBinding::new(eth_binding)?, USER)?;
        let eth_meta = parse_perp_meta(META, &eth)?;
        assert_eq!(
            parse_l2_book_bbo(BOOK, &eth_meta),
            Err(HyperliquidError::Binding)
        );
        let crossed = br#"{"coin":"BTC","time":1,"levels":[[{"px":"11","sz":"1","n":1}],[{"px":"10","sz":"1","n":1}]]}"#;
        assert_eq!(
            parse_l2_book_bbo(crossed, &meta(GatewayMode::Live)?),
            Err(HyperliquidError::Payload)
        );
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn duplicate_fill_cursor_and_missing_required_maker_fact_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let meta = meta(GatewayMode::Test)?;
        let mut duplicate: Vec<serde_json::Value> = serde_json::from_slice(FILLS_PAGE)?;
        duplicate.push(duplicate[1].clone());
        assert_eq!(
            parse_user_fills_page(
                &serde_json::to_vec(&duplicate)?,
                &meta,
                &HyperliquidFillQuery::new(&meta, 1_700_000_000_001, 1_700_000_000_002, 10, None,)?
            ),
            Err(HyperliquidError::Payload)
        );

        let mut events: Vec<serde_json::Value> = serde_json::from_slice(PRIVATE_EVENTS)?;
        events[1]["data"]["fills"][0]
            .as_object_mut()
            .ok_or("fill object missing")?
            .remove("crossed");
        assert_eq!(
            parse_private_user_fills(&serde_json::to_vec(&events)?, &meta),
            Err(HyperliquidError::Payload)
        );
        Ok(())
    }

    #[test]
    fn info_request_bodies_are_exact_and_keep_test_live_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let test = meta(GatewayMode::Test)?;
        let live = meta(GatewayMode::Live)?;
        let meta_request = build_meta_request(test.scope.binding())?;
        assert_eq!(meta_request.mode(), GatewayMode::Test);
        assert_eq!(
            meta_request.rest_origin(),
            "https://api.hyperliquid-testnet.xyz"
        );
        assert_eq!(meta_request.endpoint(), "/info");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(meta_request.body())?,
            serde_json::json!({"type":"meta"})
        );
        let live_book = build_l2_book_request(&live)?;
        assert_eq!(live_book.mode(), GatewayMode::Live);
        assert_eq!(live_book.rest_origin(), "https://api.hyperliquid.xyz");
        assert_eq!(live_book.binding(), live.scope.binding());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(live_book.body())?,
            serde_json::json!({"type":"l2Book","coin":"BTC"})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                build_clearinghouse_state_request(&test)?.body()
            )?,
            serde_json::json!({"type":"clearinghouseState","user":USER})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(build_open_orders_request(&test)?.body())?,
            serde_json::json!({"type":"openOrders","user":USER})
        );

        let query =
            HyperliquidFillQuery::new(&test, 1_700_000_000_001, 1_700_000_000_002, 1, None)?;
        let request = build_user_fills_by_time_request(&query)?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(request.body())?,
            serde_json::json!({
                "type":"userFillsByTime",
                "user":USER,
                "startTime":1_700_000_000_001_u64,
                "endTime":1_700_000_000_002_u64,
                "aggregateByTime":false
            })
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(request.body())?
                .get("limit")
                .is_none()
        );
        assert!(HyperliquidFillQuery::new(&test, 0, 1, 1, None).is_err());
        assert!(HyperliquidFillQuery::new(&test, 2, 1, 1, None).is_err());
        assert!(HyperliquidFillQuery::new(&test, 1, 2, 0, None).is_err());
        assert!(HyperliquidFillQuery::new(&test, 1, 2, 2_001, None).is_err());
        Ok(())
    }

    #[test]
    fn inclusive_fill_cursor_and_order_status_recovery_are_bound_and_strict()
    -> Result<(), Box<dyn std::error::Error>> {
        let selected = meta(GatewayMode::Test)?;
        let first = parse_user_fills_page(
            FILLS_PAGE,
            &selected,
            &HyperliquidFillQuery::new(&selected, 1_700_000_000_001, 1_700_000_000_002, 1, None)?,
        )?;
        let cursor = first.next_cursor.ok_or("cursor missing")?;
        let next = HyperliquidFillQuery::new(
            &selected,
            1_700_000_000_001,
            1_700_000_000_002,
            10,
            Some(cursor.clone()),
        )?;
        let body: serde_json::Value =
            serde_json::from_slice(build_user_fills_by_time_request(&next)?.body())?;
        assert_eq!(body["startTime"], cursor.time_ms());
        assert_eq!(
            HyperliquidFillQuery::new(
                &meta(GatewayMode::Live)?,
                1_700_000_000_001,
                1_700_000_000_002,
                10,
                Some(cursor)
            ),
            Err(HyperliquidError::Payload)
        );

        let lookup = HyperliquidOrderLookup::order_id(91_490_942)?;
        let request = build_order_status_request(&selected, &lookup)?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(request.body())?,
            serde_json::json!({"type":"orderStatus","user":USER,"oid":91_490_942_u64})
        );
        let cloid = HyperliquidOrderLookup::client_order_id("0x00000000000000000000000000000001")?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                build_order_status_request(&selected, &cloid)?.body()
            )?,
            serde_json::json!({
                "type":"orderStatus",
                "user":USER,
                "oid":"0x00000000000000000000000000000001"
            })
        );
        assert!(matches!(
            parse_order_status(ORDER_STATUS, &selected, &cloid)?,
            HyperliquidOrderStatus::Known { .. }
        ));
        assert!(matches!(
            parse_order_status(ORDER_STATUS, &selected, &lookup)?,
            HyperliquidOrderStatus::Known {
                order_id: 91_490_942,
                state: OrderState::Filled,
                exchange_time_ms: 1_724_361_546_645,
                ..
            }
        ));
        assert!(matches!(
            parse_order_status(br#"{"status":"unknownOid"}"#, &selected, &lookup)?,
            HyperliquidOrderStatus::Unknown { .. }
        ));
        assert_eq!(
            parse_order_status(
                ORDER_STATUS,
                &selected,
                &HyperliquidOrderLookup::order_id(91_490_943)?
            ),
            Err(HyperliquidError::Binding)
        );
        assert_eq!(
            HyperliquidOrderLookup::client_order_id("0x1234"),
            Err(HyperliquidError::Payload)
        );
        Ok(())
    }
}
