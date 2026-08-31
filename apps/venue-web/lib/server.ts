import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { NextRequest, NextResponse } from "next/server";
import type { Role, Session } from "./types";

const cookieName = "venue_session";
/** The BFF is deliberately not a general-purpose outbound proxy. */
export function controlOrigin(): string | undefined {
  const raw = process.env.VENUE_CONTROL_ORIGIN ?? "http://127.0.0.1:8080";
  try {
    const value = new URL(raw);
    const loopback = value.hostname === "127.0.0.1" || value.hostname === "[::1]" || value.hostname === "localhost";
    if (value.protocol !== "http:" || !loopback || value.username || value.password || value.pathname !== "/" || value.search || value.hash) return undefined;
    return value.origin;
  } catch { return undefined; }
}
const signingMaterial = () => process.env.VENUE_WEB_SESSION_SIGNING_KEY;
const operators: Record<string, Role> = { viewer: "viewer", operator: "operator", admin: "admin" };
const controlMethods = {
  "/v2/ui/snapshot": ["GET"],
  "/v2/ui/execution-facts": ["GET"],
  "/v2/copy/relations": ["GET", "POST"],
  "/v2/copy/relation-candidates": ["GET"],
  "/v2/control/commands": ["POST"],
} satisfies Record<string, string[]>;
type ControlPath = keyof typeof controlMethods;
const cookieOptions = () => ({ httpOnly: true, secure: true, sameSite: "strict" as const, path: "/", maxAge: 15 * 60 });

function encode(value: string): string { return Buffer.from(value).toString("base64url"); }
function decode(value: string): string | undefined { try { return Buffer.from(value, "base64url").toString("utf8"); } catch { return undefined; } }
export function sessionSignature(payload: string, material: string): string { return createHmac("sha256", material).update(payload).digest("base64url"); }
function same(left: string, right: string): boolean { const a = Buffer.from(left); const b = Buffer.from(right); return a.length === b.length && timingSafeEqual(a, b); }

export function isValidSession(value: unknown): value is Session {
  if (!value || typeof value !== "object") return false;
  const session = value as Partial<Session>;
  return typeof session.subject === "string" && session.subject.trim().length > 0
    && typeof session.role === "string" && Object.hasOwn(operators, session.role)
    && typeof session.csrf === "string" && session.csrf.length >= 16
    && typeof session.expires_ms === "number" && Number.isSafeInteger(session.expires_ms) && session.expires_ms > Date.now()
    && Array.isArray(session.account_scope) && session.account_scope.length > 0 && session.account_scope.every((value) => typeof value === "string" && value.trim().length > 0)
    && typeof session.writable === "boolean" && session.writable === (session.role !== "viewer");
}

export function getSession(request: NextRequest): Session | undefined {
  const material = signingMaterial(); const signed = request.cookies.get(cookieName)?.value;
  if (!material || !signed) return undefined;
  const [payload, proof] = signed.split("."); if (!payload || !proof || !same(sessionSignature(payload, material), proof)) return undefined;
  const raw = decode(payload); if (!raw) return undefined;
  try { const parsed: unknown = JSON.parse(raw); return isValidSession(parsed) ? parsed : undefined; } catch { return undefined; }
}

export function issueSession(response: NextResponse): Session | undefined {
  const material = signingMaterial();
  const role = process.env.VENUE_WEB_OPERATOR_ROLE;
  const subject = process.env.VENUE_WEB_OPERATOR_SUBJECT;
  const scope = (process.env.VENUE_WEB_ACCOUNT_SCOPE ?? "").split(",").filter(Boolean);
  if (!material || !subject || !role || !Object.hasOwn(operators, role) || scope.length === 0) return undefined;
  const session: Session = { subject, role: operators[role], account_scope: scope, csrf: randomUUID(), expires_ms: Date.now() + 15 * 60_000, writable: operators[role] !== "viewer" };
  const payload = encode(JSON.stringify(session));
  response.cookies.set(cookieName, `${payload}.${sessionSignature(payload, material)}`, cookieOptions());
  return session;
}

export function sessionResponse(request: NextRequest): NextResponse {
  const existing = getSession(request); if (existing) return NextResponse.json(existing, { headers: noStore() });
  return NextResponse.json({ writable: false, reason: "session_required" }, { status: 401, headers: noStore() });
}

function issueSessionResponse(): NextResponse {
  const response = NextResponse.json({ writable: false, reason: "controlled session unavailable" }, { status: 403, headers: noStore() });
  const issued = issueSession(response);
  if (!issued) return response;
  response.headers.set("content-type", "application/json");
  return new NextResponse(JSON.stringify(issued), { headers: response.headers });
}

export function bootstrapResponse(request: NextRequest): NextResponse {
  const required = process.env.VENUE_WEB_SESSION_BOOTSTRAP_TOKEN;
  const provided = request.headers.get("x-venue-bootstrap");
  if (!required || !provided || !same(required, provided) || !allowedOrigin(request)) return NextResponse.json({ error: "authentication_rejected" }, { status: 403, headers: noStore() });
  return issueSessionResponse();
}

export function logoutResponse(): NextResponse {
  const response = NextResponse.json({ ok: true }, { headers: noStore() });
  response.cookies.set(cookieName, "", { ...cookieOptions(), maxAge: 0 });
  return response;
}

export function noStore(): HeadersInit { return { "Cache-Control": "no-store", "Referrer-Policy": "same-origin", "X-Content-Type-Options": "nosniff" }; }
export function allowedOrigin(request: NextRequest): boolean {
  const origin = request.headers.get("origin"); const host = request.headers.get("host");
  if (!origin || !host) return false;
  try { const parsed = new URL(origin); return parsed.origin === request.nextUrl.origin && parsed.host === host; } catch { return false; }
}
export function allowWrite(request: NextRequest, accountId: string): { session: Session } | Response {
  const session = getSession(request);
  if (!session || !session.writable || session.role === "viewer") return NextResponse.json({ error: "read_only" }, { status: 403, headers: noStore() });
  if (!allowedOrigin(request) || request.headers.get("x-venue-csrf") !== session.csrf) return NextResponse.json({ error: "request_rejected" }, { status: 403, headers: noStore() });
  if (!session.account_scope.includes(accountId)) return NextResponse.json({ error: "scope_rejected" }, { status: 403, headers: noStore() });
  return { session };
}

export function allowRead(request: NextRequest): { session: Session } | Response {
  const session = getSession(request);
  return session ? { session } : NextResponse.json({ error: "session_required" }, { status: 401, headers: noStore() });
}

export function jsonHeaders(): HeadersInit { return { ...noStore(), "Content-Type": "application/json" }; }

export async function control(path: ControlPath, init?: RequestInit): Promise<Response> {
  // TypeScript types disappear at runtime; reject unknown routes/methods before any I/O.
  if (!Object.hasOwn(controlMethods, path) || !controlMethods[path].includes(init?.method ?? "GET")) {
    return Response.json({ error: "control_route_rejected" }, { status: 400, headers: jsonHeaders() });
  }
  const origin = controlOrigin();
  if (!origin) return Response.json({ error: "control_origin_rejected" }, { status: 503, headers: jsonHeaders() });
  const started = performance.now();
  try {
    const timeout = AbortSignal.timeout(10_000);
    const signal = init?.signal ? AbortSignal.any([init.signal, timeout]) : timeout;
    const upstream = await fetch(`${origin}${path}`, { ...init, signal, cache: "no-store", redirect: "error", headers: { "content-type": "application/json", ...(init?.headers ?? {}) } });
    const headers = new Headers(upstream.headers); headers.set("x-venue-bff-control-ms", (performance.now() - started).toFixed(3));
    return new Response(upstream.body, { status: upstream.status, statusText: upstream.statusText, headers });
  } catch {
    // No mutation retry here: a timeout may follow a durable Control commit.
    return Response.json({ error: "control_unavailable" }, { status: 503, headers: jsonHeaders() });
  }
}
