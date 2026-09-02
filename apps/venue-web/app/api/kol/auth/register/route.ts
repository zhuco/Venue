import { NextRequest, NextResponse } from "next/server";
import { issueKolSession, jsonHeaders, kolControl, noStore, allowedOrigin } from "@/lib/server";

export const dynamic = "force-dynamic";

type Input = { username?: unknown; password?: unknown; invite_code?: unknown; };
type Login = { token: string; user: { user_id: string; username: string }; expires_ms: number; };

export async function POST(request: NextRequest) {
  const input = await request.json().catch(() => undefined) as Input | undefined;
  if (!allowedOrigin(request) || !valid(input)) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const upstream = await kolControl("/v2/account/register", { method: "POST", body: JSON.stringify(input) });
  const body = await upstream.json().catch(() => undefined) as Partial<Login> | undefined;
  if (!upstream.ok || !validLogin(body)) return NextResponse.json(body ?? { error: "registration_failed" }, { status: upstream.status, headers: jsonHeaders() });
  const response = NextResponse.json({ user: body.user, expires_ms: body.expires_ms }, { headers: jsonHeaders() });
  if (!issueKolSession(response, body)) return NextResponse.json({ error: "session_unavailable" }, { status: 503, headers: noStore() });
  return response;
}

function valid(input: Input | undefined): input is { username: string; password: string; invite_code: string } {
  return !!input && typeof input.username === "string" && input.username.length >= 3 && input.username.length <= 64
    && typeof input.password === "string" && input.password.length >= 8 && input.password.length <= 512
    && typeof input.invite_code === "string" && /^[A-Za-z0-9_-]{24,64}$/.test(input.invite_code);
}
function validLogin(value: Partial<Login> | undefined): value is Login {
  return !!value && typeof value.token === "string" && /^[0-9a-f]{64}$/i.test(value.token)
    && !!value.user && typeof value.user.user_id === "string" && typeof value.user.username === "string"
    && typeof value.expires_ms === "number" && Number.isSafeInteger(value.expires_ms) && value.expires_ms > Date.now();
}
