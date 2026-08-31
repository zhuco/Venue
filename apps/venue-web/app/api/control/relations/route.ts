import { NextRequest, NextResponse } from "next/server";
import { allowRead, allowWrite, control, jsonHeaders, noStore } from "@/lib/server";
import { relationInScope } from "@/lib/projection-scope";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const granted = allowRead(request); if (granted instanceof Response) return granted;
  const upstream = await control("/v2/copy/relations");
  const body = await upstream.json().catch(() => undefined);
  if (!upstream.ok || !Array.isArray(body)) return NextResponse.json({ error: "relations_unavailable" }, { status: 503, headers: jsonHeaders() });
  return NextResponse.json(body.filter((item) => relationInScope(item, granted.session.account_scope)), { headers: jsonHeaders() });
}

export async function POST(request: NextRequest) {
  const body: unknown = await request.json().catch(() => undefined);
  const relation = typeof body === "object" && body !== null && "relation" in body ? (body as { relation?: { follower?: { trading_account_id?: unknown }; leader?: unknown } }).relation : undefined;
  const accountId = relation?.follower?.trading_account_id;
  if (typeof accountId !== "string") return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const granted = allowWrite(request, accountId); if (granted instanceof Response) return granted;
  const candidates = await control("/v2/copy/relation-candidates").then((response) => response.json().catch(() => undefined));
  if (!Array.isArray(candidates) || !bindingAllowed(relation?.leader, candidates, granted.session.account_scope) || !bindingAllowed(relation?.follower, candidates, granted.session.account_scope)) return NextResponse.json({ error: "candidate_rejected" }, { status: 403, headers: noStore() });
  const upstream = await control("/v2/copy/relations", { method: "POST", body: JSON.stringify(body) });
  return new NextResponse(upstream.body, { status: upstream.status, headers: jsonHeaders() });
}

export function bindingAllowed(binding: unknown, candidates: unknown[], scope: string[]): boolean { if (!binding || typeof binding !== "object") return false; const input = binding as { venue?: unknown; mode?: unknown; trading_account_id?: unknown; instance_id?: unknown; symbol?: unknown }; return typeof input.trading_account_id === "string" && scope.includes(input.trading_account_id) && candidates.some((candidate) => { const value = candidate && typeof candidate === "object" ? (candidate as { binding?: typeof input }).binding : undefined; return typeof value?.trading_account_id === "string" && scope.includes(value.trading_account_id) && value.venue === input.venue && value.mode === input.mode && value.trading_account_id === input.trading_account_id && value.instance_id === input.instance_id && value.symbol === input.symbol; }); }
