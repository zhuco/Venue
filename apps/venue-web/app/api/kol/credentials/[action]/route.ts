import { NextRequest, NextResponse } from "next/server";
import { allowKolWrite, jsonHeaders, kolControl, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";
const routes = { verify: "/v2/account/credentials/verify", select: "/v2/account/select", delete: "/v2/account/credentials/delete" } as const;

export async function POST(request: NextRequest, context: { params: Promise<{ action: string }> }) {
  const granted = allowKolWrite(request); if (granted instanceof Response) return granted;
  const { action } = await context.params;
  if (!(action in routes)) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const input = await request.json().catch(() => undefined);
  if (!input || typeof input !== "object") return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const upstream = await kolControl(routes[action as keyof typeof routes], { method: "POST", body: JSON.stringify(input) }, granted.session.token);
  const body = await upstream.json().catch(() => undefined);
  return NextResponse.json(body ?? { error: "credentials_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}
