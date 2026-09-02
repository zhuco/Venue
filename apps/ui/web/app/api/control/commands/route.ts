import { NextRequest, NextResponse } from "next/server";
import { allowWrite, control, jsonHeaders, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

type ControlAction = "pause" | "resume" | "stop" | "flatten" | "trade";
type TradeAction = "open_long" | "open_short" | "close_long" | "close_short" | "cancel_selected_order" | "cancel_all_orders";
type TradeIntent = { action: TradeAction; quote_asset: string; order_type: "limit"; time_in_force: "gtc"; post_only: boolean; reduce_only: boolean; selected_price: string | null; quote_notional: string | null; close_quantity_cap: string | null; selected_order_id: string | null; };
type Input = { request_id: string; venue: string; mode: "LIVE"; trading_account_id: string; instance_id: string; symbol: string; action: ControlAction; trade?: TradeIntent; expected_config_epoch: number; confirmation?: string; };
const actions = new Set<ControlAction>(["pause", "resume", "stop", "flatten", "trade"]);
const tradeActions = new Set<TradeAction>(["open_long", "open_short", "close_long", "close_short", "cancel_selected_order", "cancel_all_orders"]);

function confirmation(input: Input): string {
  return `${input.action.toUpperCase()} venue=${input.venue} mode=${input.mode} trading_account_id=${input.trading_account_id} symbol=${input.symbol} instance_id(${input.instance_id.length})=${input.instance_id} expected_config_epoch=${input.expected_config_epoch}`;
}

export async function POST(request: NextRequest) {
  const input = await request.json().catch(() => undefined) as Partial<Input> | undefined;
  const epoch = input?.expected_config_epoch;
  if (!validCommand(input, epoch)) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const command = input as Input;
  const granted = allowWrite(request, command.trading_account_id); if (granted instanceof Response) return granted;
  if ((command.action === "stop" || command.action === "flatten") && command.confirmation !== confirmation(command)) return NextResponse.json({ error: "confirmation_rejected" }, { status: 400, headers: noStore() });
  const envelope = { schema_version: 2, ...command };
  const upstream = await control("/v2/control/commands", { method: "POST", body: JSON.stringify(envelope) });
  return new NextResponse(upstream.body, { status: upstream.status, headers: { ...jsonHeaders(), "X-Venue-Audit": `${granted.session.subject}:${granted.session.role}:${envelope.request_id}:${Date.now()}` } });
}

function validCommand(input: Partial<Input> | undefined, epoch: unknown): input is Input {
  if (!input || typeof input.request_id !== "string" || !uuid(input.request_id) || input.mode !== "LIVE" || typeof input.venue !== "string" || !input.venue || typeof input.trading_account_id !== "string" || !input.trading_account_id || typeof input.instance_id !== "string" || !input.instance_id || typeof input.symbol !== "string" || !canonicalSymbol(input.symbol) || !actions.has(input.action as ControlAction) || !Number.isSafeInteger(epoch) || !epoch || (epoch as number) < 1) return false;
  if (input.action !== "trade") return input.trade === undefined;
  return validTrade(input.trade, input.symbol);
}

function validTrade(value: unknown, symbol: string): value is TradeIntent {
  if (!value || typeof value !== "object") return false;
  const trade = value as Partial<TradeIntent>;
  const isClose = trade.action === "close_long" || trade.action === "close_short";
  const isOrder = trade.action === "open_long" || trade.action === "open_short" || isClose;
  const quote = symbol.split("/")[1];
  if (!quote || !tradeActions.has(trade.action as TradeAction) || trade.quote_asset !== quote || trade.order_type !== "limit" || trade.time_in_force !== "gtc" || typeof trade.post_only !== "boolean" || typeof trade.reduce_only !== "boolean" || trade.reduce_only !== isClose) return false;
  if (isOrder) return positiveDecimal(trade.selected_price) && positiveDecimal(trade.quote_notional) && trade.selected_order_id === null && (isClose ? positiveDecimal(trade.close_quantity_cap) : trade.close_quantity_cap === null);
  if (trade.selected_price !== null || trade.quote_notional !== null || trade.close_quantity_cap !== null) return false;
  return trade.action === "cancel_all_orders" ? trade.selected_order_id === null : typeof trade.selected_order_id === "string" && trade.selected_order_id.trim().length > 0 && trade.selected_order_id.length <= 256;
}

function positiveDecimal(value: unknown): value is string { return typeof value === "string" && value.length <= 128 && /^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value) && /[1-9]/.test(value); }
function canonicalSymbol(value: string): boolean { return /^[A-Z0-9]+\/[A-Z0-9]+$/.test(value); }
function uuid(value: string): boolean { return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value); }
