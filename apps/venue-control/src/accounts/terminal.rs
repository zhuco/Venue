//! Authenticated terminal command admission. It only writes the singleton Executor ledger.

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use sqlx::Row;
use venue_control_protocol::{
    accounts::AccountErrorCode as Code,
    kol::{
        ExecutorCommandOrigin, ExecutorCommandPhase, ExecutorCommandState, ExecutorCommandSummary,
        ExecutorOrderKind, TerminalAction, TerminalOrderKind, TerminalOrderRequest,
    },
};
use venue_domain::domain::{OrderSide, PositionSide};

use super::{AccountError, AccountService, Principal, database_error, error, ms};

const MAX_TERMINAL_PROJECTION_AGE_MS: u64 = 15_000;

impl AccountService {
    pub async fn enqueue_terminal_order(
        &self,
        principal: &Principal,
        request: TerminalOrderRequest,
        now_ms: u64,
    ) -> Result<ExecutorCommandSummary, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        let projection =
            crate::private_projection::BinancePrivateProjectionStore::new(self.pool.clone())
                .load_owned(&principal.user.user_id, &request.credential_id)
                .await
                .map_err(|_| error(Code::Unavailable))?
                .ok_or(error(Code::VerificationRequired))?;
        if projection.observed_ms > now_ms
            || now_ms.saturating_sub(projection.observed_ms) > MAX_TERMINAL_PROJECTION_AGE_MS
        {
            return Err(error(Code::VerificationRequired));
        }
        let position_side = request.action.position_side();
        let reducing = request.action.is_close();
        let side = order_side_for(request.action);
        let position_quantity = projection
            .positions
            .iter()
            .find(|position| {
                position.symbol == request.symbol && position.position_side == position_side
            })
            .map_or(Decimal::ZERO, |position| {
                position.quantity.max(Decimal::ZERO)
            });
        let requested_quantity = match request.order_kind {
            TerminalOrderKind::Market => request
                .close_quantity_cap
                .map(|cap| cap.min(position_quantity))
                .filter(|quantity| *quantity > Decimal::ZERO)
                .ok_or(error(Code::Conflict))?,
            TerminalOrderKind::LimitPostOnly => {
                let price = request.limit_price.ok_or(error(Code::InvalidInput))?;
                let quantity = request
                    .quote_notional
                    .checked_div(price)
                    .ok_or(error(Code::InvalidInput))?;
                if reducing {
                    quantity
                        .min(
                            request
                                .close_quantity_cap
                                .ok_or(error(Code::InvalidInput))?,
                        )
                        .min(position_quantity)
                } else {
                    quantity
                }
            }
        };
        if requested_quantity <= Decimal::ZERO {
            return Err(error(Code::Conflict));
        }
        // A legacy account-node scope is a fail-closed ownership fence. It is deliberately not
        // age-expired here: only an explicit, audited migration may release an old writer after
        // its WAL, Unknown outcomes and positions have converged.
        let legacy_owned: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_control_strategy_scopes \
             WHERE venue='binance' AND mode='LIVE' AND trading_account_id=$1)",
        )
        .bind(&projection.trading_account_id)
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)?;
        if legacy_owned {
            return Err(error(Code::Conflict));
        }
        let digest: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&request).map_err(|_| error(Code::InvalidInput))?)
                .into();
        let command_id = super::crypto::opaque_id()?;
        let client_order_id =
            terminal_client_order_id(&principal.user.user_id, &request.request_id);
        let phase = if reducing { "close" } else { "open" };
        let order_kind = match request.order_kind {
            TerminalOrderKind::Market => "market",
            TerminalOrderKind::LimitPostOnly => "limit_post_only",
        };
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let inserted = sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,limit_price,rule_version,client_order_id,command_state,source_digest,created_ms,updated_ms) VALUES ($1,'terminal',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'pending',$15,$16,$16) ON CONFLICT (owner_user_id,request_id) WHERE command_origin='terminal' DO NOTHING")
            .bind(&command_id).bind(&request.request_id).bind(&principal.user.user_id)
            .bind(&projection.trading_account_id).bind(&request.credential_id).bind(request.symbol.to_string())
            .bind(position_side_name(position_side)).bind(phase).bind(order_kind).bind(order_side_name(side))
            .bind(requested_quantity.normalize().to_string()).bind(request.limit_price.map(|price| price.normalize().to_string()))
            .bind(format!("binance-pm-um-projection-{}", projection.private_generation)).bind(client_order_id)
            .bind(digest.as_slice()).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(database_error)?;
        let row = sqlx::query("SELECT command_id,request_id,command_origin,command_phase,order_kind,order_side,requested_quantity,limit_price,trading_account_id,symbol,position_side,command_state,native_order_id,created_ms,updated_ms,sanitized_error_code,source_digest FROM venue_binance_commands WHERE owner_user_id=$1 AND request_id=$2 AND command_origin='terminal' FOR SHARE")
            .bind(&principal.user.user_id).bind(&request.request_id).fetch_one(&mut *tx).await.map_err(database_error)?;
        let durable_digest: Option<Vec<u8>> =
            row.try_get("source_digest").map_err(database_error)?;
        if inserted.rows_affected() == 0 && durable_digest.as_deref() != Some(digest.as_slice()) {
            return Err(error(Code::Conflict));
        }
        let summary = command_summary(&row)?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }

    pub async fn terminal_executions(
        &self,
        principal: &Principal,
    ) -> Result<Vec<ExecutorCommandSummary>, AccountError> {
        let rows = sqlx::query("SELECT command_id,request_id,command_origin,command_phase,order_kind,order_side,requested_quantity,limit_price,trading_account_id,symbol,position_side,command_state,native_order_id,created_ms,updated_ms,sanitized_error_code FROM venue_binance_commands WHERE owner_user_id=$1 ORDER BY created_ms DESC,command_id DESC LIMIT 200")
            .bind(&principal.user.user_id).fetch_all(&self.pool).await.map_err(database_error)?;
        rows.iter().map(command_summary).collect()
    }
}

fn command_summary(row: &sqlx::postgres::PgRow) -> Result<ExecutorCommandSummary, AccountError> {
    let origin = match text(row, "command_origin")?.as_str() {
        "copy" => ExecutorCommandOrigin::Copy,
        "terminal" => ExecutorCommandOrigin::Terminal,
        _ => return Err(error(Code::Unavailable)),
    };
    let phase = match text(row, "command_phase")?.as_str() {
        "open" => ExecutorCommandPhase::Open,
        "close" => ExecutorCommandPhase::Close,
        "cancel" => ExecutorCommandPhase::Cancel,
        _ => return Err(error(Code::Unavailable)),
    };
    let order_kind = match text(row, "order_kind")?.as_str() {
        "market" => ExecutorOrderKind::Market,
        "limit_post_only" => ExecutorOrderKind::LimitPostOnly,
        "cancel_exact" => ExecutorOrderKind::CancelExact,
        _ => return Err(error(Code::Unavailable)),
    };
    let state = match text(row, "command_state")?.as_str() {
        "pending" => ExecutorCommandState::Pending,
        "sending" => ExecutorCommandState::Sending,
        "accepted" => ExecutorCommandState::Accepted,
        "rejected" => ExecutorCommandState::Rejected,
        "reconcile_required" => ExecutorCommandState::ReconcileRequired,
        "reconciled" => ExecutorCommandState::Reconciled,
        "cancelled" => ExecutorCommandState::Cancelled,
        _ => return Err(error(Code::Unavailable)),
    };
    let summary = ExecutorCommandSummary {
        command_id: text(row, "command_id")?,
        request_id: row.try_get("request_id").map_err(database_error)?,
        origin,
        phase,
        trading_account_id: text(row, "trading_account_id")?,
        symbol: text(row, "symbol")?
            .parse()
            .map_err(|_| error(Code::Unavailable))?,
        position_side: optional_position_side(row)?,
        order_side: optional_order_side(row)?,
        order_kind,
        requested_quantity: optional_decimal(row, "requested_quantity")?,
        limit_price: optional_decimal(row, "limit_price")?,
        state,
        native_order_id: row.try_get("native_order_id").map_err(database_error)?,
        created_ms: unsigned(row.try_get("created_ms").map_err(database_error)?)?,
        updated_ms: unsigned(row.try_get("updated_ms").map_err(database_error)?)?,
        sanitized_error_code: row
            .try_get("sanitized_error_code")
            .map_err(database_error)?,
    };
    summary.validate().map_err(|_| error(Code::Unavailable))?;
    Ok(summary)
}

fn order_side_for(action: TerminalAction) -> OrderSide {
    match action {
        TerminalAction::OpenLong | TerminalAction::CloseShort => OrderSide::Buy,
        TerminalAction::CloseLong | TerminalAction::OpenShort => OrderSide::Sell,
    }
}

fn terminal_client_order_id(owner: &str, request_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(owner.as_bytes());
    hasher.update([0]);
    hasher.update(request_id.as_bytes());
    let hex = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("t{}", &hex[..35])
}

fn text(row: &sqlx::postgres::PgRow, field: &str) -> Result<String, AccountError> {
    row.try_get(field).map_err(database_error)
}

fn optional_decimal(
    row: &sqlx::postgres::PgRow,
    field: &str,
) -> Result<Option<Decimal>, AccountError> {
    row.try_get::<Option<String>, _>(field)
        .map_err(database_error)?
        .map(|value| value.parse().map_err(|_| error(Code::Unavailable)))
        .transpose()
}

fn optional_position_side(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<PositionSide>, AccountError> {
    row.try_get::<Option<String>, _>("position_side")
        .map_err(database_error)?
        .map(|value| match value.as_str() {
            "long" => Ok(PositionSide::Long),
            "short" => Ok(PositionSide::Short),
            _ => Err(error(Code::Unavailable)),
        })
        .transpose()
}

fn optional_order_side(row: &sqlx::postgres::PgRow) -> Result<Option<OrderSide>, AccountError> {
    row.try_get::<Option<String>, _>("order_side")
        .map_err(database_error)?
        .map(|value| match value.as_str() {
            "buy" => Ok(OrderSide::Buy),
            "sell" => Ok(OrderSide::Sell),
            _ => Err(error(Code::Unavailable)),
        })
        .transpose()
}

fn order_side_name(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn position_side_name(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
        PositionSide::Net => "net",
    }
}

fn unsigned(value: i64) -> Result<u64, AccountError> {
    u64::try_from(value).map_err(|_| error(Code::Unavailable))
}
