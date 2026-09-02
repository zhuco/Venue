import { NextRequest, NextResponse } from "next/server";
import { allowedOrigin, issueKolSession, jsonHeaders, kolControl, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

type Input = { username?: unknown; password?: unknown; };
type Login = { token: string; user: { user_id: string; username: string }; expires_ms: number; };

export async function POST(request: NextRequest) {
  const input = await request.json().catch(() => undefined) as Input | undefined;
  if (!allowedOrigin(request) || !input || typeof input.username !== "string" || typeof input.password !== "string" || input.username.length > 64 || input.password.length > 512) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const upstream = await kolControl("/v2/account/login", { method: "POST", body: JSON.stringify(input) });
  const body = await upstream.json().catch(() => undefined) as Partial<Login> | undefined;
  if (!upstream.ok || !validLogin(body)) return NextResponse.json(body ?? { error: "login_failed" }, { status: upstream.status, headers: jsonHeaders() });
  const response = NextResponse.json({ user: body.user, expires_ms: body.expires_ms }, { headers: jsonHeaders() });
  if (!issueKolSession(response, body)) return NextResponse.json({ error: "session_unavailable" }, { status: 503, headers: noStore() });
  return response;
}

function validLogin(value: Partial<Login> | undefined): value is Login {
  return !!value && typeof value.token === "string" && /^[0-9a-f]{64}$/i.test(value.token)
    && !!value.user && typeof value.user.user_id === "string" && typeof value.user.username === "string"
    && typeof value.expires_ms === "number" && Number.isSafeInteger(value.expires_ms) && value.expires_ms > Date.now();
}
