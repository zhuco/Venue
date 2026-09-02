import { NextRequest, NextResponse } from "next/server";
import { allowKolWrite, jsonHeaders, kolControl, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  const granted = allowKolWrite(request); if (granted instanceof Response) return granted;
  const input = await request.json().catch(() => undefined);
  if (!validLifecycle(input)) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const upstream = await kolControl("/v2/kol/follow/lifecycle", { method: "POST", body: JSON.stringify(input) }, granted.session.token);
  const body = await upstream.json().catch(() => undefined);
  return NextResponse.json(body ?? { error: "follow_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}

function validLifecycle(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const request = value as Record<string, unknown>;
  const canonical = (id: unknown) => typeof id === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id);
  return request.schema_version === 1 && canonical(request.request_id) && canonical(request.relation_id)
    && Number.isSafeInteger(request.expected_revision) && Number(request.expected_revision) > 0
    && ((request.action === "activate" && request.risk_confirmed === true) || (request.action === "pause" && request.risk_confirmed === false));
}
