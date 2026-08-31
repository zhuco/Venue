use super::*;
use crate::{GatePublicError, collect_regular_order_pages, parse_regular_order};
use venue_domain::domain::{CommandId, OrderOwner, OrderPurpose};

const CATALOGUE: &str = r#"[{
    "name":"DOGE_USDT","in_delisting":false,"status":"trading",
    "quanto_multiplier":"0.1","order_size_min":"1","order_size_max":"1000",
    "order_price_round":"0.0001","enable_decimal":false
},{
    "name":"BTC_USDT","in_delisting":false,"status":"trading",
    "quanto_multiplier":"0.001","order_size_min":"1","order_size_max":"1000",
    "order_price_round":"0.1","enable_decimal":false
}]"#;

const BBO: &str = include_str!("../tests/fixtures/limit-bbo.json");

fn rules() -> Result<GateContractRules, Box<dyn std::error::Error>> {
    let contract: Value = serde_json::from_str(CATALOGUE)?;
    Ok(parse_contract_rules(&contract[0], "DOGE/USDT".parse()?, 7)?)
}

fn limit_intent(
    purpose: OrderPurpose,
    side: OrderSide,
    position_side: PositionSide,
    reduce_only: bool,
    quote_delta: Decimal,
) -> Result<AccountLimitNormalizationIntent, Box<dyn std::error::Error>> {
    Ok(AccountLimitNormalizationIntent {
        command_id: CommandId::new("gate_limit_command_1")?,
        client_order_id: CommandId::new("gate_limit_client_1")?,
        owner: OrderOwner {
            strategy_instance_id: "grid_1".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "gate".to_owned(),
            account: "account_1".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose,
        },
        side,
        position_side,
        quote_delta,
        reduce_only,
    })
}

#[test]
fn signed_snapshot_policy_preserves_missing_and_unrepresented_values() {
    assert_eq!(snapshot_limit_time_in_force(None), Ok(None));
    assert_eq!(
        snapshot_limit_time_in_force(Some(&Value::String("ioc".to_owned()))),
        Ok(None)
    );
    assert_eq!(
        snapshot_limit_time_in_force(Some(&Value::String("poc".to_owned()))),
        Ok(Some(LimitTimeInForce::PostOnly))
    );
    assert!(snapshot_limit_time_in_force(Some(&Value::Bool(true))).is_err());
}

#[test]
fn cancel_lookup_uses_the_parser_canonical_client_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let orders = collect_regular_order_pages(
        [include_str!("../tests/fixtures/regular_orders.json")],
        &rules.instrument.symbol,
        &rules,
    )?;
    assert_eq!(
        regular_venue_order_id_for_client_id(&orders.orders, "hgo_e7_long_open_l1"),
        Some("9001".to_owned())
    );
    assert_eq!(
        regular_venue_order_id_for_client_id(&orders.orders, "t-hgo_e7_long_open_l1"),
        None
    );
    Ok(())
}

#[test]
fn priced_limit_uses_explicit_gtc_price_and_caps_quantity() -> Result<(), Box<dyn std::error::Error>>
{
    let priced = AccountPricedLimitIntent {
        intent: limit_intent(
            OrderPurpose::Entry,
            OrderSide::Buy,
            PositionSide::Long,
            false,
            Decimal::from(10),
        )?,
        limit_price: Price::new(Decimal::new(1, 1))?,
        time_in_force: LimitTimeInForce::Gtc,
        maximum_quantity: Some(Decimal::new(15, 1)),
    };
    let ExecutionCommand::PlaceLimit(command) = normalize_priced_limit(&priced, &rules()?)? else {
        return Err("expected limit".into());
    };
    assert_eq!(command.limit_price, priced.limit_price);
    assert_eq!(command.time_in_force, LimitTimeInForce::Gtc);
    assert_eq!(command.quantity, Decimal::new(15, 1));
    assert!(command.quantity * command.limit_price.value() <= priced.intent.quote_delta);

    let mut unaligned = priced.clone();
    unaligned.limit_price = Price::new(Decimal::new(10_001, 5))?;
    assert_eq!(
        normalize_priced_limit(&unaligned, &rules()?),
        Err(AccountHostValidationError::Command)
    );
    Ok(())
}

#[test]
fn unknown_limit_readback_requires_the_original_policy() -> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let intent = limit_intent(
        OrderPurpose::Entry,
        OrderSide::Buy,
        PositionSide::Long,
        false,
        Decimal::from(10),
    )?;
    let mut command = normalize_limit_from_bbo(
        &intent,
        &rules,
        Decimal::new(1000, 4),
        Decimal::new(1001, 4),
    )?;
    let orders: Value =
        serde_json::from_str(include_str!("../tests/fixtures/regular_orders.json"))?;
    let mut order = parse_regular_order(&orders[0], &rules.instrument.symbol, &rules)?;
    let ExecutionCommand::PlaceLimit(place) = &command else {
        return Err("limit required".into());
    };
    order.client_order_id = FieldState::Known(place.client_order_id.as_str().to_owned());
    order.side = place.side;
    order.position_side = FieldState::Known(place.position_side);
    order.quantity = place.quantity;
    order.limit_price = Some(place.limit_price);
    order.time_in_force = FieldState::Known(place.time_in_force);
    order.reduce_only = place.reduce_only;
    assert!(readback_policy_matches_command(&command, &order));
    let ExecutionCommand::PlaceLimit(place) = &mut command else {
        return Err("limit required".into());
    };
    place.time_in_force = LimitTimeInForce::Gtc;
    assert!(!readback_policy_matches_command(&command, &order));
    order.time_in_force = FieldState::Known(LimitTimeInForce::Gtc);
    assert!(readback_policy_matches_command(&command, &order));
    order.time_in_force = FieldState::Missing;
    assert!(!readback_policy_matches_command(&command, &order));
    Ok(())
}

fn public_binding(rules: &GateContractRules) -> Result<GatePublicBinding, GatePublicError> {
    GatePublicBinding::new(
        rules.instrument.symbol.clone(),
        rules.native_symbol.clone(),
        rules.quanto_multiplier,
    )
}

#[test]
fn configured_catalog_is_exact_and_routes_owner_symbol_rules()
-> Result<(), Box<dyn std::error::Error>> {
    let doge: Symbol = "DOGE/USDT".parse()?;
    let btc: Symbol = "BTC/USDT".parse()?;
    let catalog = catalogue_rules(
        CATALOGUE,
        7,
        [doge.clone(), btc.clone()].into_iter().collect(),
    )?;
    assert_eq!(catalog_rule(&catalog, &doge)?.native_symbol, "DOGE_USDT");
    let btc_rules = catalog_rule(&catalog, &btc)?;
    assert_eq!(btc_rules.native_symbol, "BTC_USDT");
    assert!(matches!(
        catalog_rule(&catalog, &"ETH/USDT".parse()?),
        Err(GateAccountGatewayError::Rules)
    ));

    let mut intent = limit_intent(
        OrderPurpose::Entry,
        OrderSide::Buy,
        PositionSide::Long,
        false,
        Decimal::from(10),
    )?;
    intent.owner.symbol = btc;
    let normalized = normalize_limit_from_bbo(
        &intent,
        &btc_rules,
        Decimal::from(10_000),
        Decimal::from(10_001),
    )?;
    assert!(
        matches!(normalized, ExecutionCommand::PlaceLimit(command) if command.owner.symbol == intent.owner.symbol)
    );
    Ok(())
}

#[test]
fn limit_normalization_uses_fresh_bbo_contract_steps_and_preserves_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let intent = limit_intent(
        OrderPurpose::Entry,
        OrderSide::Buy,
        PositionSide::Long,
        false,
        Decimal::ONE,
    )?;
    let (bid, ask) = parse_fresh_limit_bbo(
        &rules,
        public_binding(&rules)?,
        BBO.to_owned(),
        1_000_000,
        1_000_200,
    )?;
    let command = normalize_limit_from_bbo(&intent, &rules, bid, ask)?;
    let ExecutionCommand::PlaceLimit(command) = command else {
        return Err("Gate normalizer did not emit PlaceLimit".into());
    };
    assert_eq!(command.command_id, intent.command_id);
    assert_eq!(command.client_order_id, intent.client_order_id);
    assert_eq!(command.owner, intent.owner);
    assert_eq!(command.side, OrderSide::Buy);
    assert_eq!(command.position_side, PositionSide::Long);
    assert!(!command.reduce_only);
    assert_eq!(command.limit_price.value(), Decimal::new(1, 1));
    assert_eq!(command.quantity, Decimal::from(10));
    Ok(())
}

#[test]
fn limit_normalization_rejects_wrong_symbol_crossed_stale_empty_and_bad_precision()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let binding = public_binding(&rules)?;
    let intent = limit_intent(
        OrderPurpose::Entry,
        OrderSide::Buy,
        PositionSide::Long,
        false,
        Decimal::ONE,
    )?;
    let wrong_symbol = BBO.replace(
        "\"current\":1000",
        "\"contract\":\"BTC_USDT\",\"current\":1000",
    );
    assert!(
        parse_fresh_limit_bbo(&rules, binding.clone(), wrong_symbol, 1_000_000, 1_000_200).is_err()
    );
    let crossed = BBO.replace(
        "\"asks\":[{\"p\":\"0.1010\",\"s\":\"20\"}]",
        "\"asks\":[{\"p\":\"0.1000\",\"s\":\"20\"}]",
    );
    assert!(parse_fresh_limit_bbo(&rules, binding.clone(), crossed, 1_000_000, 1_000_200).is_err());
    let empty = BBO.replace("[{\"p\":\"0.1000\",\"s\":\"20\"}]", "[]");
    assert!(parse_fresh_limit_bbo(&rules, binding.clone(), empty, 1_000_000, 1_000_200).is_err());
    assert!(
        parse_fresh_limit_bbo(
            &rules,
            binding.clone(),
            BBO.to_owned(),
            1_000_000,
            1_004_001
        )
        .is_err()
    );
    let bad_price = normalize_limit_from_bbo(
        &intent,
        &rules,
        Decimal::new(10005, 5),
        Decimal::new(101, 3),
    );
    assert_eq!(bad_price, Err(AccountHostValidationError::Command));
    let mut wrong_symbol_intent = intent.clone();
    wrong_symbol_intent.owner.symbol = "BTC/USDT".parse()?;
    assert_eq!(
        normalize_limit_from_bbo(
            &wrong_symbol_intent,
            &rules,
            Decimal::new(1, 1),
            Decimal::new(101, 3)
        ),
        Err(AccountHostValidationError::Command)
    );
    Ok(())
}

#[test]
fn limit_normalization_fails_instead_of_increasing_to_gate_minimum_or_relaxing_side()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let too_small = limit_intent(
        OrderPurpose::Entry,
        OrderSide::Buy,
        PositionSide::Long,
        false,
        Decimal::new(9, 3),
    )?;
    assert_eq!(
        normalize_limit_from_bbo(&too_small, &rules, Decimal::new(1, 1), Decimal::new(101, 3)),
        Err(AccountHostValidationError::Command)
    );
    let wrong_side = limit_intent(
        OrderPurpose::Entry,
        OrderSide::Sell,
        PositionSide::Long,
        false,
        Decimal::ONE,
    )?;
    assert_eq!(
        normalize_limit_from_bbo(
            &wrong_side,
            &rules,
            Decimal::new(1, 1),
            Decimal::new(101, 3)
        ),
        Err(AccountHostValidationError::Command)
    );
    let wrong_reduce = limit_intent(
        OrderPurpose::Entry,
        OrderSide::Buy,
        PositionSide::Long,
        true,
        Decimal::ONE,
    )?;
    assert_eq!(
        normalize_limit_from_bbo(
            &wrong_reduce,
            &rules,
            Decimal::new(1, 1),
            Decimal::new(101, 3)
        ),
        Err(AccountHostValidationError::Command)
    );
    Ok(())
}

#[test]
fn account_wide_risk_uses_real_contract_multiplier_for_positions_and_orders() {
    let positions = account_position_notionals(
        CATALOGUE,
        r#"[{"contract":"DOGE_USDT","size":"10","mark_price":"0.1"}]"#,
        7,
    );
    assert_eq!(positions, Ok(vec![Decimal::new(1, 1)]));
    let orders = account_entry_order_notionals(
        CATALOGUE,
        &[serde_json::json!({
            "contract":"DOGE_USDT", "is_reduce_only":false,
            "size":"10", "left":"5", "price":"0.2"
        })],
        7,
    );
    assert_eq!(orders, Ok(vec![Decimal::new(1, 1)]));
}

#[test]
fn account_wide_risk_rejects_incomplete_or_non_usdt_facts() {
    assert!(
        account_entry_order_notionals(
            CATALOGUE,
            &[serde_json::json!({"contract":"DOGE_USDT", "is_reduce_only":false})],
            7,
        )
        .is_err()
    );
    assert!(symbol_for_contract(Some(&Value::String("DOGE_USDC".to_owned()))).is_err());
}

#[test]
fn signed_snapshot_partial_regular_order_keeps_original_quantity_and_filled_amount()
-> Result<(), Box<dyn std::error::Error>> {
    let positions = vec![serde_json::json!({
        "contract":"DOGE_USDT", "mode":"dual_long", "size":"10",
        "entry_price":"0.1", "mark_price":"0.11"
    })];
    let facts = snapshot_position_facts(CATALOGUE, &positions, 7)?;
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].quantity, Decimal::ONE);
    let orders = vec![serde_json::json!({
        "id":"9001", "contract":"DOGE_USDT", "size":"10", "left":"5",
        "is_reduce_only":false, "status":"open", "price":"0.2", "text":"foreign",
        "create_time":"1700000000.125", "update_time":"1700000001"
    })];
    let order_facts = snapshot_regular_order_facts(CATALOGUE, &orders, 7)?;
    assert_eq!(order_facts.len(), 1);
    assert_eq!(order_facts[0].quantity, Decimal::ONE);
    assert_eq!(order_facts[0].filled_quantity, Some(Decimal::new(5, 1)));
    assert_eq!(order_facts[0].state, Some(OrderState::PartiallyFilled));
    assert_eq!(order_facts[0].family, NativeOrderFamily::UmOrder);
    assert_eq!(order_facts[0].created_at_ms, Some(1_700_000_000_125));
    Ok(())
}

#[test]
fn signed_snapshot_normalizes_only_the_exact_gate_managed_text_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let orders = vec![
        serde_json::json!({
            "id":"9001", "contract":"DOGE_USDT", "size":"10", "left":"5",
            "is_reduce_only":false, "status":"open", "price":"0.2", "text":"t-gate_limit_client_1",
            "create_time":"1700000000.125"
        }),
        serde_json::json!({
            "id":"9002", "contract":"DOGE_USDT", "size":"-10", "left":"-5",
            "is_reduce_only":false, "status":"open", "price":"0.2", "text":"t-foreign space",
            "create_time":"1700000000.126"
        }),
    ];
    let facts = snapshot_regular_order_facts(CATALOGUE, &orders, 7)?;
    assert_eq!(facts[0].client_order_id, "gate_limit_client_1");
    assert_eq!(facts[1].client_order_id, "t-foreign space");
    Ok(())
}

#[test]
fn signed_snapshot_rejects_duplicate_fills_and_bad_position_family()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        snapshot_fills_cursor(
            &[serde_json::json!({"id":"1"}), serde_json::json!({"id":"1"})],
            &["[]".to_owned()],
            None,
        )
        .is_err()
    );
    assert!(
        snapshot_position_facts(
            CATALOGUE,
            &[serde_json::json!({
                "contract":"DOGE_USDT", "mode":"single", "size":"1",
                "entry_price":"0.1", "mark_price":"0.1"
            })],
            7,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn signed_snapshot_fill_cursor_is_native_and_legacy_digest_fails_closed() {
    assert_eq!(
        parse_snapshot_fills_cursor(Some("gate-fills-v1|123")),
        Ok(Some("123".to_owned()))
    );
    assert!(
        parse_snapshot_fills_cursor(Some(
            "4983f3d75db0d72aeb1e68c57d9f171d981edc3ef31b8ca16c4d5f1caa26dce5"
        ))
        .is_err()
    );
    assert_eq!(
        snapshot_fills_cursor(
            &[serde_json::json!({"id":"124"})],
            &["[]".to_owned()],
            Some("123".to_owned())
        ),
        Ok("gate-fills-v1|124".to_owned())
    );
}

#[test]
fn signed_snapshot_exports_normalized_account_fill_facts() -> Result<(), Box<dyn std::error::Error>>
{
    let fills = snapshot_fill_facts(
        CATALOGUE,
        &[serde_json::json!({
            "id":"9", "order_id":"3", "contract":"DOGE_USDT",
            "size":"-10", "price":"0.2"
        })],
        7,
    )?;
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity, Decimal::ONE);
    assert_eq!(fills[0].side, OrderSide::Sell);
    Ok(())
}
