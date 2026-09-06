//! Durable control-plane state for support martingale instances.
mod planner;
pub use planner::{NoopReason, Plan, PlannerInput, TakeProfitOrder, plan, plan_take_profit_only};
mod reference_market;
mod runtime;
mod stop_loss;
mod store;
pub use reference_market::{
    BinanceReferenceClient, ReferenceMarketError, ReferenceSnapshot, SymbolReference,
};
pub use runtime::SupportMartingaleRuntime;
pub use store::{
    SupportMartingaleCommandKind, SupportMartingaleDatabasePreflight,
    SupportMartingaleRuntimeState, SupportMartingaleRuntimeSymbolState, SupportMartingaleStore,
    SupportMartingaleStoreError,
};
