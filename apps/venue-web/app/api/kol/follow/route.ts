import { NextRequest, NextResponse } from "next/server";
import { allowKolWrite, getKolSession, jsonHeaders, kolControl, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const session = getKolSession(request);
  if (!session) return NextResponse.json({ error: "session_required" }, { status: 401, headers: noStore() });
  const upstream = await kolControl("/v2/kol/follow/settings", { method: "GET" }, session.token);
  const body = await upstream.json().catch(() => undefined);
  return NextResponse.json(body ?? { error: "follow_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}

export async function POST(request: NextRequest) {
  const granted = allowKolWrite(request); if (granted instanceof Response) return granted;
  const input = await request.json().catch(() => undefined);
  if (!validSettings(input)) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const upstream = await kolControl("/v2/kol/follow/settings", { method: "POST", body: JSON.stringify(input) }, granted.session.token);
  const body = await upstream.json().catch(() => undefined);
  return NextResponse.json(body ?? { error: "follow_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}

function validSettings(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const request = value as Record<string, unknown>; const settings = request.settings;
  if (request.schema_version !== 1 || !canonicalId(request.request_id) || (request.expected_revision !== null && request.expected_revision !== undefined && (!Number.isSafeInteger(request.expected_revision) || Number(request.expected_revision) < 1))) return false;
  if (!settings || typeof settings !== "object") return false;
  const risk = settings as Record<string, unknown>;
  return canonicalId(risk.credential_id) && decimal(risk.allocated_capital) && decimal(risk.multiplier) && decimal(risk.max_order_notional) && decimal(risk.max_total_notional)
    && Number.isSafeInteger(risk.max_deviation_bps) && Number(risk.max_deviation_bps) >= 0 && Number(risk.max_deviation_bps) <= 10_000
    && Array.isArray(risk.allowed_symbols) && risk.allowed_symbols.length > 0 && risk.allowed_symbols.length <= 20 && risk.allowed_symbols.every((symbol) => typeof symbol === "string" && /^[A-Z0-9]{1,20}\/[A-Z0-9]{1,20}$/.test(symbol));
}

function canonicalId(value: unknown): boolean { return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value); }
function decimal(value: unknown): boolean { return typeof value === "string" && /^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,18})?$/.test(value) && Number(value) > 0; }
