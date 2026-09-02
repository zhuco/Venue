import { NextRequest, NextResponse } from "next/server";
import { allowKolWrite, getKolSession, jsonHeaders, kolControl, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const session = getKolSession(request);
  if (!session) return NextResponse.json({ error: "session_required" }, { status: 401, headers: noStore() });
  const upstream = await kolControl("/v2/kol/profile", { method: "GET" }, session.token);
  const payload = await upstream.json().catch(() => undefined);
  return NextResponse.json(payload ?? { error: "profile_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}

export async function POST(request: NextRequest) {
  const granted = allowKolWrite(request); if (granted instanceof Response) return granted;
  const body = await request.json().catch(() => undefined);
  if (!body || typeof body !== "object") return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const upstream = await kolControl("/v2/kol/profile", { method: "POST", body: JSON.stringify(body) }, granted.session.token);
  const payload = await upstream.json().catch(() => undefined);
  return NextResponse.json(payload ?? { error: "profile_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}
