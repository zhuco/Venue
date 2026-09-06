use super::*;

#[test]
fn never_traded_sentinels_are_only_valid_for_empty_positions() {
    assert_eq!(position_sequence("-1", true), Ok(0));
    assert_eq!(position_updated_at("0", true), Ok(0));
    assert_eq!(position_unrealized_pnl("", true), Ok(Decimal::ZERO));
    assert_eq!(position_sequence("7", false), Ok(7));
    assert!(position_sequence("-1", false).is_err());
    assert!(position_updated_at("0", false).is_err());
    assert!(position_unrealized_pnl("", false).is_err());
}

#[test]
fn native_limit_policy_is_projected_without_inventing_unsupported_values() {
    assert_eq!(
        canonical_limit_time_in_force("Limit", "PostOnly"),
        FieldState::Known(LimitTimeInForce::PostOnly)
    );
    assert_eq!(
        canonical_limit_time_in_force("Limit", "GTC"),
        FieldState::Known(LimitTimeInForce::Gtc)
    );
    assert!(matches!(
        canonical_limit_time_in_force("Limit", "IOC"),
        FieldState::Unavailable {
            reason: UnknownReason::Ambiguous
        }
    ));
}
