import { NextRequest, NextResponse } from "next/server";
import { allowWrite, control, jsonHeaders, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

type Input = { request_id: string; venue: string; mode: "LIVE"; trading_account_id: string; instance_id: string; symbol: string; action: "pause" | "resume" | "stop" | "flatten"; expected_config_epoch: number; confirmation?: string; };
const actions = new Set<Input["action"]>(["pause", "resume", "stop", "flatten"]);

function confirmation(input: Input): string {
  return `${input.action.toUpperCase()} venue=${input.venue} mode=${input.mode} trading_account_id=${input.trading_account_id} symbol=${input.symbol} instance_id(${input.instance_id.length})=${input.instance_id} expected_config_epoch=${input.expected_config_epoch}`;
}

export async function POST(request: NextRequest) {
  const input = await request.json().catch(() => undefined) as Partial<Input> | undefined;
  const epoch = input?.expected_config_epoch;
  if (!input || typeof input.request_id !== "string" || !uuid(input.request_id) || input.mode !== "LIVE" || !input.venue || !input.trading_account_id || !input.instance_id || !input.symbol || !actions.has(input.action as Input["action"]) || !Number.isSafeInteger(epoch) || !epoch || epoch < 1) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const command: Input = { request_id: input.request_id, venue: input.venue, mode: input.mode, trading_account_id: input.trading_account_id, instance_id: input.instance_id, symbol: input.symbol, action: input.action as Input["action"], expected_config_epoch: epoch, confirmation: input.confirmation };
  const granted = allowWrite(request, command.trading_account_id); if (granted instanceof Response) return granted;
  if ((command.action === "stop" || command.action === "flatten") && command.confirmation !== confirmation(command)) return NextResponse.json({ error: "confirmation_rejected" }, { status: 400, headers: noStore() });
  const envelope = { schema_version: 2, ...command };
  const upstream = await control("/v2/control/commands", { method: "POST", body: JSON.stringify(envelope) });
  return new NextResponse(upstream.body, { status: upstream.status, headers: { ...jsonHeaders(), "X-Venue-Audit": `${granted.session.subject}:${granted.session.role}:${envelope.request_id}:${Date.now()}` } });
}

function uuid(value: string): boolean { return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value); }
