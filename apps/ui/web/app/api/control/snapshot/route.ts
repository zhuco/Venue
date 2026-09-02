import { NextRequest, NextResponse } from "next/server";
import { allowRead, control, jsonHeaders } from "@/lib/server";
import { scopedSnapshot } from "@/lib/projection-scope";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const granted = allowRead(request); if (granted instanceof Response) return granted;
  const [upstream, relationResponse] = await Promise.all([control("/v2/ui/snapshot"), control("/v2/copy/relations")]);
  const body = await upstream.json().catch(() => undefined) as Record<string, unknown> | undefined;
  const relations: unknown = await relationResponse.json().catch(() => undefined);
  if (!upstream.ok || !body || !relationResponse.ok || !Array.isArray(relations)) return NextResponse.json({ error: "snapshot_unavailable" }, { status: 503, headers: jsonHeaders() });
  const scoped = granted.session.account_scope;
  const upstreamMs = upstream.headers.get("x-venue-bff-control-ms");
  return NextResponse.json(scopedSnapshot(body, relations, scoped), { headers: { ...jsonHeaders(), ...(upstreamMs ? { "Server-Timing": `bff-control;dur=${upstreamMs}` } : {}) } });
}
