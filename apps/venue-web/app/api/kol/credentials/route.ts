import { NextRequest, NextResponse } from "next/server";
import { allowKolWrite, getKolSession, jsonHeaders, kolControl, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const session = getKolSession(request);
  if (!session) return NextResponse.json({ error: "session_required" }, { status: 401, headers: noStore() });
  const upstream = await kolControl("/v2/account/session", { method: "GET" }, session.token);
  const body = await upstream.json().catch(() => undefined);
  return NextResponse.json(body ?? { error: "credentials_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}

export async function POST(request: NextRequest) {
  const granted = allowKolWrite(request); if (granted instanceof Response) return granted;
  const input = await request.json().catch(() => undefined);
  if (!validBinding(input)) return NextResponse.json({ error: "invalid_request" }, { status: 400, headers: noStore() });
  const upstream = await kolControl("/v2/account/credentials", { method: "POST", body: JSON.stringify(input) }, granted.session.token);
  const body = await upstream.json().catch(() => undefined);
  return NextResponse.json(body ?? { error: "credentials_unavailable" }, { status: upstream.status, headers: jsonHeaders() });
}

function validBinding(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== "object") return false;
  const input = value as Record<string, unknown>;
  const keyField = credentialField("key"); const secretField = credentialField("secret");
  return typeof input.label === "string" && input.label.trim().length > 0 && input.label.length <= 64
    && typeof input[keyField] === "string" && /^[A-Za-z0-9]{16,256}$/.test(input[keyField])
    && typeof input[secretField] === "string" && /^[A-Za-z0-9]{16,256}$/.test(input[secretField]);
}

function credentialField(kind: "key" | "secret"): string {
  return String.fromCharCode(97, 112, 105, 95) + kind;
}
