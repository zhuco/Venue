import { NextRequest, NextResponse } from "next/server";
import { allowKolWrite, clearKolSession, jsonHeaders, kolControl } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  const granted = allowKolWrite(request); if (granted instanceof Response) return granted;
  const upstream = await kolControl("/v2/account/logout", { method: "POST", body: "{}" }, granted.session.token);
  const response = NextResponse.json({ ok: upstream.ok }, { status: upstream.ok ? 200 : 503, headers: jsonHeaders() });
  clearKolSession(response);
  return response;
}
