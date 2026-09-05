import { createCipheriv, createDecipheriv, createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { NextRequest, NextResponse } from "next/server";
import { allowedOrigin, controlOrigin, noStore } from "./server";

const cookieName = "venue_customer";
type CustomerSession = { token: string; csrf: string; expires_ms: number };
const routes: Record<string, { path: string; methods: string[]; public?: boolean }> = {
  "managed-followers": { path: "/v2/kol/managed-followers", methods: ["GET", "POST"] },
  "managed-verify": { path: "/v2/kol/managed-followers/verify", methods: ["POST"] },
  "managed-settings": { path: "/v2/kol/managed-followers/follow/settings", methods: ["POST"] },
  "managed-follow": { path: "/v2/kol/managed-followers/follow/lifecycle", methods: ["POST"] },
  "managed-status": { path: "/v2/kol/managed-followers/follow/status", methods: ["POST"] },
  login: { path: "/v2/account/login", methods: ["POST"], public: true },
  register: { path: "/v2/account/register", methods: ["POST"], public: true },
  session: { path: "/v2/account/session", methods: ["GET"] },
  logout: { path: "/v2/account/logout", methods: ["POST"] },
  credentials: { path: "/v2/account/credentials", methods: ["GET", "POST"] },
  verify: { path: "/v2/account/credentials/verify", methods: ["POST"] },
  select: { path: "/v2/account/select", methods: ["POST"] },
  delete: { path: "/v2/account/credentials/delete", methods: ["POST"] },
  settings: { path: "/v2/kol/follow/settings", methods: ["GET", "POST"] },
  follow: { path: "/v2/kol/follow/lifecycle", methods: ["POST"] },
  leader: { path: "/v2/kol/leader-bot", methods: ["GET", "POST"] },
  "leader-lifecycle": { path: "/v2/kol/leader-bot/lifecycle", methods: ["POST"] },
  "mirror-orders": { path: "/v2/kol/follow/orders", methods: ["GET"] },
};
const response = (body: unknown, status = 200) => NextResponse.json(body, { status, headers: noStore() });
const cookieOptions = { httpOnly: true, secure: true, sameSite: "strict" as const, path: "/" };
function key(): Buffer | undefined {
  const material = process.env.VENUE_WEB_SESSION_SIGNING_KEY;
  return material && material.length >= 32 ? createHash("sha256").update("venue-customer-v1\0").update(material).digest() : undefined;
}
export function sealCustomerSession(session: CustomerSession): string | undefined {
  const material = key(); if (!material) return undefined;
  const iv = randomBytes(12); const cipher = createCipheriv("aes-256-gcm", material, iv);
  cipher.setAAD(Buffer.from(cookieName));
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(session), "utf8"), cipher.final()]);
  return Buffer.concat([iv, cipher.getAuthTag(), ciphertext]).toString("base64url");
}
export function customerSession(request: NextRequest): CustomerSession | undefined {
  const material = key(); const raw = request.cookies.get(cookieName)?.value;
  if (!material || !raw || raw.length > 4096) return undefined;
  try {
    const bytes = Buffer.from(raw, "base64url"); if (bytes.length < 29) return undefined;
    const decipher = createDecipheriv("aes-256-gcm", material, bytes.subarray(0, 12));
    decipher.setAAD(Buffer.from(cookieName)); decipher.setAuthTag(bytes.subarray(12, 28));
    const value = JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]).toString("utf8"));
    return value && typeof value.token === "string" && value.token.length >= 16 && value.token.length <= 512
      && typeof value.csrf === "string" && value.csrf.length === 36 && Number.isSafeInteger(value.expires_ms)
      && value.expires_ms > Date.now() ? value : undefined;
  } catch { return undefined; }
}
function same(a: string, b: string): boolean {
  const left = Buffer.from(a); const right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}
async function boundedJson(body: ReadableStream<Uint8Array> | null, limit: number): Promise<unknown> {
  if (!body) throw new Error("missing_body");
  const reader = body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
  try {
    while (true) {
      const next = await reader.read(); if (next.done) break;
      size += next.value.length; if (size > limit) throw new Error("oversized_body");
      chunks.push(next.value);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } finally { await reader.cancel().catch(() => undefined); reader.releaseLock(); }
}
type ObjectValue = Record<string, unknown>;
function object(value: unknown): ObjectValue {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid_object");
  return value as ObjectValue;
}
function pick(value: unknown, fields: string[]): ObjectValue {
  const source = object(value); const result: ObjectValue = {};
  for (const field of fields) if (Object.hasOwn(source, field)) result[field] = source[field];
  return result;
}
const userFields = ["user_id", "username"];
const credentialFields = ["credential_id", "label", "venue", "masked_key", "trading_account_id", "verification", "verified_ms", "expires_ms", "api_reachable", "dual_position", "account_mode", "has_exposure"];
const riskFields = ["credential_id", "allocated_capital", "multiplier", "max_order_notional", "max_total_notional", "max_deviation_bps", "allowed_symbols"];
function publicRisk(raw: unknown, managed = false): ObjectValue {
  const value = object(raw);
  const result = pick(value, managed ? riskFields.filter(field => field !== "credential_id") : riskFields);
  if (value.sizing !== undefined) {
    const sizing = object(value.sizing);
    if (sizing.mode === "proportional") result.sizing = { mode: "proportional" };
    else if (sizing.mode === "fixed_notional" && typeof sizing.notional === "string" && /^\d+(\.\d+)?$/.test(sizing.notional)) result.sizing = { mode: "fixed_notional", notional: sizing.notional };
    else throw new Error("invalid_sizing");
  }
  return result;
}
// Every response crosses an explicit DTO boundary. Control session tokens and exchange
// secrets are never passed through to a browser response, including unexpected fields.
export function customerPublicValue(action: string, method: string, raw: unknown): unknown {
  if (raw === null) return null;
  if (["managed-settings", "managed-follow", "managed-status"].includes(action)) {
    const value = object(raw);
    return { ...pick(value, ["managed_id", "relation_id", "state", "revision", "activation_requested"]), settings: publicRisk(value.settings, true) };
  }
  if (action === "managed-followers" || action === "managed-verify") {
    const fields = ["managed_id", "label", "masked_key", "verification", "verified_ms"];
    if (action === "managed-followers" && method === "GET") {
      const value = object(raw);
      if (!Array.isArray(value.accounts) || typeof value.can_manage !== "boolean") throw new Error("invalid_managed_accounts");
      return { can_manage: value.can_manage, accounts: value.accounts.map(v => pick(v, fields)) };
    }
    return pick(raw, fields);
  }
  if (action === "session" || (action === "credentials" && method === "GET")) {
    const value = object(raw);
    if (!Array.isArray(value.credentials)) throw new Error("invalid_overview");
    return { user: pick(value.user, userFields), credentials: value.credentials.map(v => pick(v, credentialFields)), selected_credential_id: value.selected_credential_id };
  }
  if (["credentials", "verify"].includes(action)) return pick(raw, credentialFields);
  if (action === "select" || action === "delete") {
    // Selection/deletion return the full owned overview in Control.
    return customerPublicValue("session", "GET", raw);
  }
  if (action === "settings" || action === "follow") {
    const value = object(raw);
    return { ...pick(value, ["relation_id", "state", "revision", "activation_requested"]), settings: publicRisk(value.settings) };
  }
  if (action === "leader" || action === "leader-lifecycle") {
    const value = object(raw);
    return { ...pick(value, ["schema_version", "can_use", "permission_revision"]), bot: value.bot === null ? null : pick(value.bot, ["bot_id", "trading_account_id", "credential_id", "state", "revision", "active_followers", "pending_orders", "attention_code"]) };
  }
  if (action === "mirror-orders") {
    if (!Array.isArray(raw)) throw new Error("invalid_orders");
    return raw.map(value => pick(value, ["mirror_id", "symbol", "source_order_id", "child_client_order_id", "state", "requested_quantity", "filled_quantity", "attention_code"]));
  }
  if (action === "invite") return { schema_version: object(raw).schema_version, profile: pick(object(raw).profile, ["kol_id", "name", "title", "description", "state", "revision"]) };
  if (action === "logout") return null;
  throw new Error("invalid_action");
}
export async function customerResponse(request: NextRequest, action: string): Promise<NextResponse> {
  const invite = action === "invite";
  const route = Object.hasOwn(routes, action) ? routes[action] : undefined;
  if ((!route || !route.methods.includes(request.method)) && !(invite && request.method === "GET")) return response({ code: "not_found" }, 404);
  let path = route?.path ?? "";
  if (invite) {
    const code = request.nextUrl.searchParams.get("code") ?? "";
    if (!/^[A-Za-z0-9_-]{24,64}$/.test(code)) return response({ code: "invalid_input" }, 400);
    path = `/v2/public/kol/invites/${code}`;
  } else if (request.nextUrl.search) return response({ code: "invalid_input" }, 400);
  const session = customerSession(request);
  if (!invite && !route?.public && !session) return response({ code: "unauthorized" }, 401);
  if (request.method === "POST" && (!allowedOrigin(request) || request.headers.get("content-type")?.split(";")[0].trim() !== "application/json"
    || (!route?.public && (!session || !same(request.headers.get("x-venue-csrf") ?? "", session.csrf))))) return response({ code: "forbidden" }, 403);
  const origin = controlOrigin(); if (!origin || !key()) return response({ code: "unavailable" }, 503);
  let body: string | undefined;
  if (request.method === "POST") {
    try {
      const raw = object(await boundedJson(request.body, 16_384));
      if (action === "managed-followers") {
        if (Object.keys(raw).some(k => !["request_id", "label", "key", "secret"].includes(k)) || [raw.request_id, raw.label, raw.key, raw.secret].some(v => typeof v !== "string")) throw new Error("invalid_managed_credentials");
        body = JSON.stringify({ request_id: raw.request_id, credential: { label: raw.label, api_key: raw.key, api_secret: raw.secret } });
      } else if (action === "credentials") {
        if (Object.keys(raw).some(k => !["label", "key", "secret"].includes(k)) || [raw.label, raw.key, raw.secret].some(v => typeof v !== "string")) throw new Error("invalid_credentials");
        body = JSON.stringify({ label: raw.label, api_key: raw.key, api_secret: raw.secret });
      } else { body = JSON.stringify(raw); }
    } catch { return response({ code: "invalid_input" }, 400); }
  }
  try {
    const headers = new Headers({ "Accept": "application/json" });
    if (body) headers.set("Content-Type", "application/json");
    if (session && !route?.public && !invite) headers.set("Authorization", `Bearer ${session.token}`);
    const upstream = await fetch(`${origin}${path}`, { method: request.method, headers, body, redirect: "error", cache: "no-store", signal: AbortSignal.timeout(30_000) });
    const raw = await boundedJson(upstream.body, 2_097_152);
    if (!upstream.ok) {
      const code = object(raw).code;
      const allowed = ["invalid_input", "invalid_login", "username_unavailable", "unauthorized", "forbidden", "not_found", "conflict", "verification_required", "account_in_use", "rate_limited", "unavailable"];
      return response({ code: typeof code === "string" && allowed.includes(code) ? code : "unavailable" }, upstream.status >= 400 && upstream.status <= 599 ? upstream.status : 502);
    }
    if (route?.public) {
      const value = object(raw);
      if (typeof value.token !== "string" || value.token.length < 16 || value.token.length > 512 || !Number.isSafeInteger(value.expires_ms) || (value.expires_ms as number) <= Date.now()) throw new Error("invalid_session");
      const fresh: CustomerSession = { token: value.token, csrf: randomUUID(), expires_ms: value.expires_ms as number };
      const sealed = sealCustomerSession(fresh); if (!sealed) throw new Error("unavailable");
      const result = response({ user: pick(value.user, userFields), csrf: fresh.csrf, expires_ms: fresh.expires_ms });
      result.cookies.set(cookieName, sealed, { ...cookieOptions, expires: new Date(fresh.expires_ms) }); return result;
    }
    const value = customerPublicValue(action, request.method, raw);
    const result = response(action === "session" ? { ...object(value), csrf: session?.csrf } : value);
    if (action === "logout") result.cookies.set(cookieName, "", { ...cookieOptions, maxAge: 0 });
    return result;
  } catch { return response({ code: "unavailable" }, 503); }
}
