import type { ExecutionFacts } from "../lib/types";

// Synthetic projection for layout/state QA only; never served by the deployed BFF.
export function projectedFacts(account: string): ExecutionFacts {
  const now = Date.now();
  const binding = { venue: "Binance", mode: "LIVE", trading_account_id: account,
    symbol: "BTC/USDT", instance_id: "copy-btc", config_epoch: 7 };
  const fact = { binding, observed_ms: now, signed_generation: 42, fact_digest: Array(32).fill(1) };
  const relation = { relation_id: "00000000-0000-4000-8000-000000000010", relation_revision: 1,
    job_id: "layout-job-000001" };
  return {
    schema_version: 2, generated_ms: now,
    orders: Array.from({ length: 24 }, (_, index) => ({ ...fact, fact_digest: Array(32).fill(index + 1),
      order_id: `layout-order-${String(index).padStart(5, "0")}`, client_order_id: `layout-client-${index}`,
      state: "partially_filled", side: "buy", position_side: "long", quantity: "0.00000000000000000001",
      filled_quantity: index === 0 ? null : "0", limit_price: "9007199254740993.01", reduce_only: false })),
    positions: [{ ...fact, position_side: "long", quantity: "0.00000000000000000001",
      entry_price: "9007199254740993.01", mark_price: "9007199254740993.02" }],
    fills: [{ ...fact, fill_id: "layout-fill-000001", order_id: "layout-order-00000", side: "buy",
      position_side: "long", quantity: "0.00000000000000000001", price: "9007199254740993.01",
      execution_sequence: 1, occurred_ms: now }],
    reconciliation: [{ ...fact, reconciled_ms: now, complete_order_families: true, complete_position_legs: true }],
    copy_ledger: [{ ...fact, ...relation, ledger_sequence: null, managed_exposure: "0.00000000000000000001" }],
    drift: [{ ...fact, ...relation, target_exposure: "0.00000000000000000001", actual_exposure: "0", repair_pending: true }],
    execution: (["semantic_applied", "rejected", "unknown", "reconciled"] as const).map((state, index) => ({
      ...fact, ...relation, fact_digest: Array(32).fill(index + 25), job_id: `layout-job-${state}`,
      command_id: `layout-command-${state}`, state })),
    risk: [{ venue: "Binance", mode: "LIVE", trading_account_id: account, observed_ms: now,
      signed_generation: 42, absolute_position_notional: "0.00009", open_entry_notional: "0.001",
      reserved_entry_notional: "5.00", max_total_notional: "10.00", accepts_new_risk: false,
      fact_digest: Array(32).fill(1) }],
    health: [{ venue: "Binance", mode: "LIVE", trading_account_id: account, observed_ms: now,
      private_generation: 42, last_reconciled_ms: now, health: "degraded", fact_digest: Array(32).fill(1) }],
  };
}
