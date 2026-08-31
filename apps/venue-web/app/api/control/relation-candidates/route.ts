import { NextRequest, NextResponse } from "next/server";
import { allowRead, control, jsonHeaders } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const granted = allowRead(request); if (granted instanceof Response) return granted;
  const upstream = await control("/v2/copy/relation-candidates"); const body = await upstream.json().catch(() => undefined);
  if (!upstream.ok || !Array.isArray(body)) return NextResponse.json({ error: "candidates_unavailable" }, { status: 503, headers: jsonHeaders() });
  return NextResponse.json(body.filter((item) => { const account = item && typeof item === "object" ? (item as { binding?: { trading_account_id?: unknown } }).binding?.trading_account_id : undefined; return typeof account === "string" && granted.session.account_scope.includes(account); }), { headers: jsonHeaders() });
}
