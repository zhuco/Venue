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
      assert.deepEqual(JSON.parse(String(init?.body)), { credential: { label: "owned", api_key: "read-trade-key-fixture", api_secret: "secret-fixture" }, authorization: { sizing: { mode: "proportional" }, multiplier: "1" } });
      return Response.json({ credential_id: "owned", label: "owned", masked_key: "••••ture", api_key: "read-trade-key-fixture", api_secret: "secret-fixture", token: "other-secret" });
    };
    const bound = await customerResponse(request("credentials", { cookie, csrf: loginBody.csrf, body: { label: "owned", key: "read-trade-key-fixture", secret: "secret-fixture", authorization: { sizing: { mode: "proportional" }, multiplier: "1" } } }), "credentials");
    assert.equal(bound.status, 200); const text = await bound.text(); assert.equal(text.includes("secret"), false); assert.equal(text.includes("read-trade-key-fixture"), false);
  } finally { globalThis.fetch = fetch; keys.forEach((key, index) => { if (old[index] === undefined) delete process.env[key]; else process.env[key] = old[index]; }); }
});
test("public leader DTO preserves server denial and exposes only own aggregate state", () => {
  assert.deepEqual(customerPublicValue("leader", "GET", { schema_version: 1, can_use: false, permission_revision: 0, bot: null, followers: [{ secret: "foreign" }], admin: true }), { schema_version: 1, can_use: false, permission_revision: 0, bot: null });
  assert.deepEqual(customerPublicValue("mirror-orders", "GET", [{ mirror_id: "owned", state: "pending", owner: "foreign", api_secret: "secret" }]), [{ mirror_id: "owned", state: "pending" }]);
});

test("managed save forwards an owned request once and strips all secret and identity fields", async () => {
  const keys = ["VENUE_WEB_SESSION_SIGNING_KEY", "VENUE_CONTROL_ORIGIN"] as const;
  const old = keys.map(key => process.env[key]); const fetch = globalThis.fetch; let calls = 0;
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = material;
    process.env.VENUE_CONTROL_ORIGIN = "http://127.0.0.1:39180";
    const cookie = sealCustomerSession(session()); assert.ok(cookie);
    globalThis.fetch = async (url, init) => {
      calls++;
      assert.equal(String(url), "http://127.0.0.1:39180/v2/kol/managed-followers");
      assert.equal(new Headers(init?.headers).get("authorization"), `Bearer ${session().token}`);
      assert.deepEqual(JSON.parse(String(init?.body)), { request_id: session().csrf, credential: { label: "托管", api_key: "K".repeat(32), api_secret: "S".repeat(32) }, authorization: { sizing: { mode: "proportional" }, multiplier: "1" } });
      return Response.json({ managed_id: "owned", label: "托管", masked_key: "••••KKKK", verification: "unverified", verified_ms: null, api_key: "K".repeat(32), api_secret: "S".repeat(32), follower_user_id: "hidden", credential_id: "hidden", trading_account_id: "hidden" });
    };
    const body = { request_id: session().csrf, label: "托管", key: "K".repeat(32), secret: "S".repeat(32), authorization: { sizing: { mode: "proportional" }, multiplier: "1" } };
    const denied = await customerResponse(request("managed-followers", { cookie, body: { ...body, kol_user_id: "forged" } }), "managed-followers");
    assert.equal(denied.status,400); assert.equal(calls,0);
    assert.equal((await customerResponse(request("managed-verify", { cookie, csrf:"invalid", body:{managed_id:"owned"} }),"managed-verify")).status,403);
    const saved = await customerResponse(request("managed-followers", { cookie, body }), "managed-followers");
    assert.equal(saved.status,200); assert.equal(calls,1);
    const value = await saved.json(); assert.deepEqual(Object.keys(value).sort(),["label","managed_id","masked_key","verification","verified_ms"]);
    assert.equal(JSON.stringify(value).includes("S".repeat(32)),false);
    globalThis.fetch = async () => { calls++; throw new Error("timeout_with_secret"); };
    const failed = await customerResponse(request("managed-followers", {cookie,body}),"managed-followers");
    assert.equal(failed.status,503); assert.deepEqual(await failed.json(),{code:"unavailable"}); assert.equal(calls,2);
  } finally { globalThis.fetch = fetch; keys.forEach((key,index) => { if(old[index] === undefined) delete process.env[key]; else process.env[key]=old[index]; }); }
});

test("managed list and verification disclose only the narrow summary", () => {
  const raw = { managed_id:"one", label:"one", masked_key:"••••1234", verification:"verified", verified_ms:1, account_identity:"hidden", api_secret:"secret" };
  const clean = customerPublicValue("managed-verify","POST",raw);
  assert.deepEqual(customerPublicValue("managed-followers","GET",{can_manage:false,accounts:[raw],token:"secret"}),{can_manage:false,accounts:[clean]});
  assert.equal(JSON.stringify(clean).includes("secret"),false);
});

test("managed deletion needs only the owned managed id", async () => {
  const keys = ["VENUE_WEB_SESSION_SIGNING_KEY", "VENUE_CONTROL_ORIGIN"] as const;
  const old = keys.map(key => process.env[key]); const fetch = globalThis.fetch; let calls = 0;
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = material;
    process.env.VENUE_CONTROL_ORIGIN = "http://127.0.0.1:39180";
    const cookie = sealCustomerSession(session()); assert.ok(cookie);
    globalThis.fetch = async (url, init) => {
      calls++;
      assert.equal(String(url), "http://127.0.0.1:39180/v2/kol/managed-followers/delete");
      assert.deepEqual(JSON.parse(String(init?.body)), { managed_id: "owned" });
      return Response.json({ can_manage: true, accounts: [] });
    };
    const denied = await customerResponse(request("managed-delete", { cookie, body: { managed_id: "owned", password: "legacy" } }), "managed-delete");
    assert.equal(denied.status, 400); assert.equal(calls, 0);
    const deleted = await customerResponse(request("managed-delete", { cookie, body: { managed_id: "owned" } }), "managed-delete");
    assert.equal(deleted.status, 200); assert.equal(calls, 1);
    assert.deepEqual(await deleted.json(), { can_manage: true, accounts: [] });
  } finally { globalThis.fetch = fetch; keys.forEach((key,index) => { if(old[index] === undefined) delete process.env[key]; else process.env[key]=old[index]; }); }
});

test("managed sizing responses preserve each mode without leaking internal credentials", () => {
  const settings = { sizing: { mode:"fixed_notional", notional:"5.5", api_secret:"hidden" }, allocated_capital:"55", multiplier:"1", max_order_notional:"5.5", max_total_notional:"55", max_deviation_bps:100, allowed_symbols:["DASH/USDT"], credential_id:"hidden" };
  for (const action of ["managed-settings", "managed-follow", "managed-status"]) {
    const raw = { managed_id:"one", relation_id:"relation", state:"paused", revision:1, activation_requested:false, settings, follower_user_id:"hidden" };
    const clean = customerPublicValue(action,"POST",raw) as {settings: {sizing: object}};
    assert.deepEqual(clean.settings.sizing,{mode:"fixed_notional",notional:"5.5"});
    assert.equal(JSON.stringify(clean).includes("hidden"),false);
    const proportional = customerPublicValue(action,"POST",{...raw,settings:{...settings,sizing:{mode:"proportional"}}}) as typeof clean;
    assert.deepEqual(proportional.settings.sizing,{mode:"proportional"});
    assert.equal(customerPublicValue(action,"POST",null),null);
    assert.throws(()=>customerPublicValue(action,"POST",{...raw,settings:{...settings,sizing:{mode:"unexpected"}}}));
  }
});

test("owned account equity remains exact while credential internals are stripped", () => {
  const credential = { credential_id: "owned", equity: "9007199254740993.010000000000000001", balance_observed_ms: 123, api_secret: "hidden", ciphertext: "hidden" };
  const clean = { credential_id: credential.credential_id, equity: credential.equity, balance_observed_ms: 123 };
  assert.deepEqual(customerPublicValue("verify", "POST", credential), clean);
  assert.deepEqual(customerPublicValue("session", "GET", { user: { user_id: "u", username: "user" }, credentials: [credential], selected_credential_id: "owned" }), { user: { user_id: "u", username: "user" }, credentials: [clean], selected_credential_id: "owned" });
});

test("public follower registration requires an invite and cannot request elevated roles", async () => {
  const keys = ["VENUE_WEB_SESSION_SIGNING_KEY", "VENUE_CONTROL_ORIGIN"] as const;
  const old = keys.map(key => process.env[key]); const originalFetch = globalThis.fetch; let calls = 0;
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = material;
    process.env.VENUE_CONTROL_ORIGIN = "http://127.0.0.1:39180";
    const body = { username: "follower", password: "password-fixture", invite_code: "Ab12" };
    globalThis.fetch = async (url, init) => {
      calls++;
      assert.equal(String(url), "http://127.0.0.1:39180/v2/account/register");
      assert.deepEqual(JSON.parse(String(init?.body)), body);
      assert.equal(new Headers(init?.headers).get("authorization"), null);
      return Response.json({ ...session(), user: { user_id: "follower", username: "follower" } });
    };
    for (const raw of [{ username: body.username, password: body.password }, { ...body, invite_code: "" }, { ...body, invite_code: "Ab1" }, { ...body, invite_code: "AB_1" }, { ...body, invite_code: "AB-1" }, { ...body, role: "kol" }, { ...body, kol_user_id: "forged" }]) {
      assert.equal((await customerResponse(request("register", { body: raw }), "register")).status, 400);
    }
    assert.equal(calls, 0);
    const result = await customerResponse(request("register", { body }), "register");
    assert.equal(result.status, 200); assert.equal(calls, 1);
    assert.equal((await result.json()).token, undefined);
    assert.match(result.headers.get("set-cookie") ?? "", /HttpOnly/);
  } finally {
    globalThis.fetch = originalFetch;
    keys.forEach((key, index) => { if (old[index] === undefined) delete process.env[key]; else process.env[key] = old[index]; });
  }
});

test("KOL invite responses expose only the owned share code, never database envelopes", () => {
  assert.deepEqual(customerPublicValue("kol-invite", "GET", { invite_id: "id", invite_code: "KOL2026", active: true, created_ms: 100, code_envelope: "secret", code_hash: "hash", request_hash: "hash", kol_user_id: "hidden" }), { invite_id: "id", invite_code: "KOL2026", active: true, created_ms: 100 });
  assert.deepEqual(customerPublicValue("kol-profile", "GET", { kol_id: "own", state: "enabled", api_secret: "hidden" }), { kol_id: "own", state: "enabled" });
});


test("configured leader creation forwards capital and the same request identity without leaking internals", async () => {
  const keys = ["VENUE_WEB_SESSION_SIGNING_KEY", "VENUE_CONTROL_ORIGIN"] as const;
  const old = keys.map(key => process.env[key]); const originalFetch = globalThis.fetch;
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = material;
    process.env.VENUE_CONTROL_ORIGIN = "http://127.0.0.1:39180";
    const cookie = sealCustomerSession(session()); assert.ok(cookie);
    const body = { schema_version: 2, request_id: session().csrf, credential_id: "owned", config: { name: "KOL", description: "", strategy_capital: "100.25" } };
    let calls = 0;
    globalThis.fetch = async (url, init) => {
      calls++;
      assert.equal(String(url), "http://127.0.0.1:39180/v2/kol/leader-bots");
      assert.equal(new Headers(init?.headers).get("authorization"), `Bearer ${session().token}`);
      assert.deepEqual(JSON.parse(String(init?.body)), body);
      if (calls === 1) return Response.json({ code: "unavailable" }, { status: 503 });
      return Response.json({ schema_version: 2, can_use: true, permission_revision: 1, bots: [{ bot_id: "created", state: "stopped", revision: 1, owner: "hidden", api_secret: "hidden" }] });
    };
    assert.equal((await customerResponse(request("leader-create", { cookie, body }), "leader-create")).status, 503);
    assert.equal(calls, 1);
    const result = await customerResponse(request("leader-create", { cookie, body }), "leader-create");
    assert.equal(result.status, 200);
    assert.deepEqual(await result.json(), { schema_version: 2, can_use: true, permission_revision: 1, bots: [{ bot_id: "created", state: "stopped", revision: 1 }] });
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = originalFetch;
    keys.forEach((key, index) => { if (old[index] === undefined) delete process.env[key]; else process.env[key] = old[index]; });
  }
});
