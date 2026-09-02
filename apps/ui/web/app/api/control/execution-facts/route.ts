import { NextRequest, NextResponse } from "next/server";
import { allowRead, control, jsonHeaders } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const granted = allowRead(request); if (granted instanceof Response) return granted;
  const upstream = await control("/v2/ui/execution-facts");
  const body = await upstream.json().catch(() => undefined) as Record<string, unknown> | undefined;
  if (!upstream.ok || !body) return NextResponse.json({ error: "facts_unavailable" }, { status: 503, headers: jsonHeaders() });
  const scoped = granted.session.account_scope;
  const arrays = ["orders", "positions", "fills", "reconciliation", "copy_ledger", "drift", "execution", "risk", "health"];
  for (const field of arrays) body[field] = Array.isArray(body[field]) ? body[field].filter((item) => inScope(item, scoped)) : [];
  return NextResponse.json(body, { headers: jsonHeaders() });
}

function inScope(value: unknown, scope: string[]): boolean { if (!value || typeof value !== "object") return false; const fact = value as { trading_account_id?: unknown; binding?: { trading_account_id?: unknown } }; const account = typeof fact.trading_account_id === "string" ? fact.trading_account_id : fact.binding?.trading_account_id; return typeof account === "string" && scope.includes(account); }
