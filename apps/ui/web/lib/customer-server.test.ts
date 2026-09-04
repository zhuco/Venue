import assert from "node:assert/strict";
import test from "node:test";
import { NextRequest } from "next/server";
import { customerPublicValue, customerResponse, customerSession, sealCustomerSession } from "./customer-server";

const material = "customer-cookie-test-material-at-least-32-characters";
const session = () => ({ token: "owned-control-session-token", csrf: "00000000-0000-4000-8000-000000000001", expires_ms: Date.now() + 60_000 });
function request(action: string, options: { body?: unknown; cookie?: string; origin?: string; csrf?: string } = {}) {
  const headers = new Headers({ host: "venue.example", authorization: "Bearer forged-browser-token" });
  if (options.cookie) headers.set("cookie", `venue_customer=${options.cookie}`);
  if (options.body !== undefined) { headers.set("content-type", "application/json"); headers.set("origin", options.origin ?? "https://venue.example"); headers.set("x-venue-csrf", options.csrf ?? session().csrf); }
  return new NextRequest(`https://venue.example/api/customer/${action}`, { method: options.body === undefined ? "GET" : "POST", headers, body: options.body === undefined ? undefined : JSON.stringify(options.body) });
}
test("customer sessions are encrypted, authenticated, expiring, and separate from operator cookies", () => {
  const previous = process.env.VENUE_WEB_SESSION_SIGNING_KEY;
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = material;
    const own = session(); const sealed = sealCustomerSession(own); assert.ok(sealed);
    assert.equal(Buffer.from(sealed, "base64url").includes(Buffer.from(own.token)), false);
    assert.deepEqual(customerSession(request("session", { cookie: sealed })), own);
    const damaged = Buffer.from(sealed, "base64url"); damaged[30] ^= 1;
    assert.equal(customerSession(request("session", { cookie: damaged.toString("base64url") })), undefined);
    assert.equal(customerSession(request("session", { cookie: sealCustomerSession({ ...own, expires_ms: Date.now() - 1 }) })), undefined);
    assert.equal(customerSession(new NextRequest("https://venue.example", { headers: { cookie: `venue_session=${sealed}` } })), undefined);
    delete process.env.VENUE_WEB_SESSION_SIGNING_KEY;
    assert.equal(sealCustomerSession(own), undefined);
  } finally { if (previous === undefined) delete process.env.VENUE_WEB_SESSION_SIGNING_KEY; else process.env.VENUE_WEB_SESSION_SIGNING_KEY = previous; }
});
test("customer route rejects forged authority, cross-origin writes, missing CSRF and proxy paths before I/O", async () => {
  const prior = process.env.VENUE_WEB_SESSION_SIGNING_KEY; const fetch = globalThis.fetch; let calls = 0;
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = material;
    globalThis.fetch = async () => { calls++; throw new Error("unexpected_io"); };
    const cookie = sealCustomerSession(session()); assert.ok(cookie);
    assert.equal((await customerResponse(request("leader"), "leader")).status, 401);
    assert.equal((await customerResponse(request("leader", { cookie, body: {}, origin: "https://attacker.example" }), "leader")).status, 403);
    assert.equal((await customerResponse(request("leader", { cookie, body: {}, csrf: "wrong" }), "leader")).status, 403);
    assert.equal((await customerResponse(request("grant", { cookie, body: { enabled: true } }), "grant")).status, 404);
    assert.equal((await customerResponse(request("session?token=forged", { cookie }), "session")).status, 400);
    assert.equal((await customerResponse(request("invite?code=../../account/session"), "invite")).status, 400);
    assert.equal(calls, 0);
  } finally { globalThis.fetch = fetch; if (prior === undefined) delete process.env.VENUE_WEB_SESSION_SIGNING_KEY; else process.env.VENUE_WEB_SESSION_SIGNING_KEY = prior; }
});
test("login cookie keeps the Control token out of JSON; writes use only the owned customer token", async () => {
  const keys = ["VENUE_WEB_SESSION_SIGNING_KEY", "VENUE_WEB_CONTROL_SESSION_TOKEN", "VENUE_CONTROL_ORIGIN"] as const;
  const old = keys.map(key => process.env[key]); const fetch = globalThis.fetch;
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = material;
    process.env.VENUE_WEB_CONTROL_SESSION_TOKEN = "operator-token-not-customer";
    process.env.VENUE_CONTROL_ORIGIN = "http://127.0.0.1:39180";
    globalThis.fetch = async (url, init) => {
      assert.equal(String(url), "http://127.0.0.1:39180/v2/account/login");
      assert.equal(new Headers(init?.headers).get("authorization"), null);
      assert.equal(init?.redirect, "error");
      return Response.json({ ...session(), user: { user_id: "alice", username: "alice" }, unexpected: "secret" });
    };
    const login = await customerResponse(request("login", { body: { username: "alice", password: "password-fixture" } }), "login");
    assert.equal(login.status, 200);
    const loginBody = await login.json(); assert.equal(loginBody.token, undefined); assert.equal(loginBody.unexpected, undefined);
    const setCookie = login.headers.get("set-cookie"); assert.ok(setCookie); assert.match(setCookie, /HttpOnly/); assert.match(setCookie, /Secure/); assert.match(setCookie, /SameSite=strict/);
    const cookie = /^venue_customer=([^;]+)/.exec(setCookie)?.[1]; assert.ok(cookie);
    globalThis.fetch = async (url, init) => {
      assert.equal(String(url), "http://127.0.0.1:39180/v2/account/credentials");
      assert.equal(new Headers(init?.headers).get("authorization"), `Bearer ${session().token}`);
      assert.deepEqual(JSON.parse(String(init?.body)), { label: "owned", api_key: "read-trade-key-fixture", api_secret: "secret-fixture" });
      return Response.json({ credential_id: "owned", label: "owned", masked_key: "••••ture", api_key: "read-trade-key-fixture", api_secret: "secret-fixture", token: "other-secret" });
    };
    const bound = await customerResponse(request("credentials", { cookie, csrf: loginBody.csrf, body: { label: "owned", key: "read-trade-key-fixture", secret: "secret-fixture" } }), "credentials");
    assert.equal(bound.status, 200); const text = await bound.text(); assert.equal(text.includes("secret"), false); assert.equal(text.includes("read-trade-key-fixture"), false);
  } finally { globalThis.fetch = fetch; keys.forEach((key, index) => { if (old[index] === undefined) delete process.env[key]; else process.env[key] = old[index]; }); }
});
test("public leader DTO preserves server denial and exposes only own aggregate state", () => {
  assert.deepEqual(customerPublicValue("leader", "GET", { schema_version: 1, can_use: false, permission_revision: 0, bot: null, followers: [{ secret: "foreign" }], admin: true }), { schema_version: 1, can_use: false, permission_revision: 0, bot: null });
  assert.deepEqual(customerPublicValue("mirror-orders", "GET", [{ mirror_id: "owned", state: "pending", owner: "foreign", api_secret: "secret" }]), [{ mirror_id: "owned", state: "pending" }]);
});
