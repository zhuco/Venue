/** Browser views of the owned DTOs in venue-control-protocol::{accounts,kol,leader_bot}. */
export type Credential = { credential_id: string; label: string; masked_key: string; verification: string; trading_account_id: string | null; dual_position: boolean; account_mode: string | null };
export type CustomerOverview = { user: { user_id: string; username: string }; credentials: Credential[]; selected_credential_id: string | null; csrf: string };
export type FollowSizing = { mode: "proportional" } | { mode: "fixed_notional"; notional: string };
export type FollowSettings = { credential_id: string; sizing?: FollowSizing; allocated_capital: string; multiplier: string; max_order_notional: string; max_total_notional: string; max_deviation_bps: number; allowed_symbols: string[] };
export type ManagedFollowSettings = Omit<FollowSettings, "credential_id">;
export type ManagedFollowRelation = { managed_id: string; relation_id: string; state: string; revision: number; settings: ManagedFollowSettings; activation_requested: boolean };
export type FollowRelation = { relation_id: string; state: string; revision: number; settings: FollowSettings; activation_requested: boolean };
export type LeaderAccess = { can_use: boolean; permission_revision: number; bot: { bot_id: string; trading_account_id: string; credential_id: string; state: string; revision: number; active_followers: number; pending_orders: number; attention_code: string | null } | null };
export type MirrorOrder = { mirror_id: string; symbol: string; source_order_id: string; child_client_order_id: string; state: string; requested_quantity: string; filled_quantity: string; attention_code: string | null };
export type Invite = { profile: { name: string; title: string; description: string } };
