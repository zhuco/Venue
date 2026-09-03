use rust_decimal::Decimal;
use serde::Serialize;
use venue_control_protocol::grid::{
    GridInstanceState, GridInstanceSummary, GridOrderRole, GridOrderSemanticKey,
};
use venue_control_protocol::kol::{
    ExecutorCommandOrigin, ExecutorCommandPhase, ExecutorCommandState, ExecutorOrderKind,
};
use venue_domain::{PositionSide, Symbol};

pub const MAX_GRID_DESIRED_ORDERS: usize = 200;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum GridCommandIntent {
    LimitPostOnly {
        key: GridOrderSemanticKey,
        #[serde(with = "rust_decimal::serde::str")]
        quantity: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        limit_price: Decimal,
    },
    Market {
        position_side: PositionSide,
        role: GridOrderRole,
        #[serde(with = "rust_decimal::serde::str")]
        quantity: Decimal,
    },
    Cancel {
        target_client_order_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GridLedgerCommand {
    pub command_id: String,
    pub client_order_id: String,
    pub instance_id: String,
    pub config_revision: u64,
    pub plan_revision: u64,
    pub semantic_key: String,
    pub rule_version: String,
    pub source_digest: [u8; 32],
    pub intent: GridCommandIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridLedgerCommandRecord {
    pub command_id: String,
    pub client_order_id: String,
    pub instance_id: String,
    pub config_revision: u64,
    pub plan_revision: u64,
    pub semantic_key: String,
    pub owner_user_id: String,
    pub trading_account_id: String,
    pub credential_id: String,
    pub symbol: Symbol,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridCommandStatus {
    pub command_id: String,
    pub client_order_id: String,
    pub semantic_key: String,
    pub phase: ExecutorCommandPhase,
    pub order_kind: ExecutorOrderKind,
    pub state: ExecutorCommandState,
    pub native_order_id: Option<String>,
    pub selected_native_order_id: Option<String>,
    pub target_client_order_id: Option<String>,
    pub sanitized_error_code: Option<String>,
    pub updated_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridReduceReservation {
    pub command_id: String,
    pub origin: ExecutorCommandOrigin,
    pub grid_instance_id: Option<String>,
    pub client_order_id: String,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub state: ExecutorCommandState,
    pub updated_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridRuntimeRecord {
    pub owner_user_id: String,
    pub instance: GridInstanceSummary,
    /// Tail of the durable projected-plan chain. It is internal execution metadata and is not
    /// exposed through the public Grid DTO.
    pub tail_batch_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridConvergenceUpdate {
    pub instance_id: String,
    pub expected_instance_revision: u64,
    pub expected_state: GridInstanceState,
    pub expected_plan_revision: u64,
    pub next_plan_revision: u64,
    pub desired_digest: [u8; 32],
    pub dirty: bool,
    pub consecutive_failures: u16,
    pub last_facts_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GridDesiredOrder {
    pub key: GridOrderSemanticKey,
    pub client_order_id: String,
    pub quantity: Decimal,
    pub limit_price: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridDesiredSurface {
    pub instance_id: String,
    pub symbol: Symbol,
    pub config_revision: u64,
    pub plan_revision: u64,
    pub desired_digest: [u8; 32],
    pub orders: Vec<GridDesiredOrder>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GridOwnedOrderState {
    Working,
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GridOrderOwnership {
    pub instance_id: String,
    pub trading_account_id: String,
    pub config_revision: u64,
    pub plan_revision: u64,
    pub key: GridOrderSemanticKey,
    pub place_command_id: String,
    pub client_order_id: String,
    pub symbol: Symbol,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub filled_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub limit_price: Decimal,
    pub native_order_id: Option<String>,
    #[serde(skip)]
    pub state: GridOwnedOrderState,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GridFillAllocation {
    pub instance_id: String,
    pub trading_account_id: String,
    pub config_revision: u64,
    pub client_order_id: String,
    pub native_trade_id: String,
    pub symbol: Symbol,
    pub position_side: PositionSide,
    pub role: GridOrderRole,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    pub maker: Option<bool>,
    pub occurred_ms: Option<u64>,
    pub observed_ms: u64,
}
