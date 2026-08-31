export type Role = "viewer" | "operator" | "admin";
export type Connection = "LIVE" | "DEGRADED" | "OFFLINE";
export type WriteState = "loading" | "readonly" | "recovering" | "ready";

export interface AccountBalance { asset: string; equity: string; available_margin: string | null; }
export interface Account { venue: string; mode: "LIVE"; trading_account_id: string; health: string; equity: string | null; available_margin: string | null; unrealized_pnl: string | null; balances?: AccountBalance[]; private_generation: number; writer_generation: number; last_reconciled_ms: number; }
export interface Strategy { instance_id: string; kind: string; venue: string; mode: "LIVE"; trading_account_id: string; symbol: string; lifecycle: string; config_epoch: number; open_orders: number; long_quantity: string; short_quantity: string; realized_pnl: string | null; unrealized_pnl: string | null; last_receipt_ms: number; attention: string | null; }
export interface Relation { relation_id: string; revision: number; leader_id: string; follower_instance_id: string; symbol: string; target_exposure: string; actual_exposure: string; drift: string; status: string; last_applied_job: string | null; }
export interface Ledger { receipt_id: string; instance_id: string; occurred_ms: number; action: string; state: string; detail: string; }
export interface Snapshot { schema_version: 2; generated_ms: number; connection: Connection; accounts: Account[]; strategies: Strategy[]; copy_relations: Relation[]; markets: unknown[]; ledger: Ledger[]; }
export interface RelationBinding { venue: string; mode: "LIVE"; trading_account_id: string; instance_id: string; symbol: string; }
export interface RelationRecord { relation: { relation_id: string; leader: RelationBinding; follower: RelationBinding; allocated_capital: string; multiplier: string; safety_reserve_rate: string; risk: { max_total_notional: string; max_order_notional: string; max_leverage: string; }; lifecycle: "active" | "paused"; }; revision: number; }
export interface RelationCandidate { binding: RelationBinding; lifecycle: string; config_epoch: number; }
export interface Session { subject: string; role: Role; account_scope: string[]; csrf: string; expires_ms: number; writable: boolean; }
export interface Receipt { schema_version: 2; request_id: string; state: "accepted" | "applied" | "rejected" | "unknown"; receipt_id: string; observed_ms: number; detail: string; }
export type Fact = Record<string, unknown>;
export interface ExecutionFacts { schema_version: 2; generated_ms: number; orders: Fact[]; positions: Fact[]; fills: Fact[]; reconciliation: Fact[]; copy_ledger: Fact[]; drift: Fact[]; execution: Array<Fact & { state: "semantic_applied" | "prepared" | "submitted" | "accepted" | "rejected" | "unknown" | "reconciled"; }>; risk: Fact[]; health: Fact[]; }
