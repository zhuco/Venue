import { NextRequest, NextResponse } from "next/server";
import { getKolSession, jsonHeaders, kolControl, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const session = getKolSession(request);
  if (!session) return NextResponse.json({ error: "session_required" }, { status: 401, headers: noStore() });
  const upstream = await kolControl("/v2/account/session", { method: "GET" }, session.token);
  const body = await upstream.json().catch(() => undefined);
  if (!upstream.ok || !body) return NextResponse.json({ error: "session_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
  return NextResponse.json({ overview: body, csrf: session.csrf }, { headers: jsonHeaders() });
}
