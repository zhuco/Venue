use super::{planner::same_terms, store::unavailable};
use crate::kol_executor::BinanceCommandLedgerError as Error;
use rust_decimal::Decimal;
use sqlx::PgConnection;
use venue_control_protocol::kol::{TerminalFill, TerminalOpenOrder};

pub(super) fn recorded_complete(order: &TerminalOpenOrder) -> bool {
    order.quantity > Decimal::ZERO && order.filled_quantity == Some(order.quantity)
}

// Fills are authenticated, account-scoped and deduplicated by native trade identity on insert.
// Absence alone is never used as evidence that the source filled.
pub(super) async fn source_complete(
    connection: &mut PgConnection,
    account: &str,
    order: &TerminalOpenOrder,
) -> Result<bool, Error> {
    if recorded_complete(order) {
        return Ok(true);
    }
    let latest: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT order_json FROM venue_binance_order_observations WHERE trading_account_id=$1 AND client_order_id=$2 ORDER BY observed_ms DESC LIMIT 1",
    )
    .bind(account)
    .bind(&order.client_order_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(unavailable)?;
    if let Some(latest) = latest {
        let latest: TerminalOpenOrder =
            serde_json::from_value(latest).map_err(|_| Error::Conflict)?;
        if !same_terms(&latest, order) {
            return Ok(false);
        }
    }
    Ok(filled_quantity(connection, account, order).await? == order.quantity)
}

pub(super) async fn filled_quantity(
    connection: &mut PgConnection,
    account: &str,
    order: &TerminalOpenOrder,
) -> Result<Decimal, Error> {
    let native = order.native_order_id.as_deref().ok_or(Error::Conflict)?;
    let rows: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT fill_json FROM venue_binance_account_fills WHERE trading_account_id=$1 AND symbol=$2 AND fill_json->>'native_order_id'=$3",
    )
    .bind(account)
    .bind(order.symbol.to_string())
    .bind(native)
    .fetch_all(connection)
    .await
    .map_err(unavailable)?;
    matched_fill_total(order, rows)
}

fn matched_fill_total(
    order: &TerminalOpenOrder,
    rows: Vec<serde_json::Value>,
) -> Result<Decimal, Error> {
    let native = order.native_order_id.as_deref().ok_or(Error::Conflict)?;
    let mut total = Decimal::ZERO;
    for row in rows {
        let fill: TerminalFill = serde_json::from_value(row).map_err(|_| Error::Conflict)?;
        if fill.native_order_id != native
            || fill.symbol != order.symbol
            || fill.position_side != order.position_side
            || fill.order_side != order.order_side
            || fill.quantity <= Decimal::ZERO
        {
            return Err(Error::Conflict);
        }
        total = total.checked_add(fill.quantity).ok_or(Error::Conflict)?;
    }
    if total > order.quantity {
        return Err(Error::Conflict);
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_completion_requires_exact_full_quantity_and_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut order: TerminalOpenOrder = serde_json::from_value(serde_json::json!({
            "client_order_id":"source", "native_order_id":"123", "symbol":"DOGE/USDC",
            "order_side":"buy", "position_side":"long", "quantity":"245",
            "filled_quantity":"0", "limit_price":"0.08959", "post_only":true,
            "time_in_force":"post_only", "reduce_only":false, "state":"new", "created_ms":1
        }))?;
        let fill = |quantity: &str| {
            serde_json::json!({
                "native_trade_id":quantity, "native_order_id":"123", "symbol":"DOGE/USDC",
                "order_side":"buy", "position_side":"long", "quantity":quantity,
                "price":"0.08959", "maker":true, "occurred_ms":2
            })
        };
        assert_eq!(matched_fill_total(&order, vec![])?, Decimal::ZERO);
        assert_eq!(
            matched_fill_total(&order, vec![fill("122")])?,
            Decimal::from(122)
        );
        assert_eq!(
            matched_fill_total(&order, vec![fill("122"), fill("123")])?,
            order.quantity
        );
        assert!(matched_fill_total(&order, vec![fill("246")]).is_err());
        let mut wrong = fill("245");
        wrong["position_side"] = "short".into();
        assert!(matched_fill_total(&order, vec![wrong]).is_err());
        let mut wrong = fill("245");
        wrong["native_order_id"] = "124".into();
        assert!(matched_fill_total(&order, vec![wrong]).is_err());
        assert!(!recorded_complete(&order));
        order.filled_quantity = Some(order.quantity);
        assert!(recorded_complete(&order));
        order.filled_quantity = Some(Decimal::from(122));
        assert!(!recorded_complete(&order));
        Ok(())
    }
}
