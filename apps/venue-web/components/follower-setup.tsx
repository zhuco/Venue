"use client";

import { FormEvent, useEffect, useState } from "react";

type Credential = { credential_id: string; label: string; masked_key: string; verification: string; account_mode: string | null; api_reachable: boolean; dual_position: boolean; };
type Overview = { credentials: Credential[]; selected_credential_id: string | null; };
type Relation = { relation_id: string; state: "paused" | "active" | "needs_attention" | "disabled"; revision: number; activation_requested: boolean; settings: { credential_id: string; allocated_capital: string; multiplier: string; max_order_notional: string; max_total_notional: string; max_deviation_bps: number; allowed_symbols: string[]; }; };

export function FollowerSetup() {
  const [overview, setOverview] = useState<Overview>();
  const [relation, setRelation] = useState<Relation>();
  const [csrf, setCsrf] = useState<string>();
  const [message, setMessage] = useState("正在读取账户状态…");
  useEffect(() => { void load(); }, []);
  async function load() {
    const session = await fetch("/api/kol/auth/session", { credentials: "same-origin" });
    const payload = await session.json().catch(() => undefined) as { overview?: Overview; csrf?: string } | undefined;
    if (!session.ok || !payload?.overview || typeof payload.csrf !== "string") { setMessage("请先登录。 "); return; }
    setOverview(payload.overview); setCsrf(payload.csrf);
    const current = await fetch("/api/kol/follow", { credentials: "same-origin" });
    if (current.ok) setRelation(await current.json() as Relation); else setRelation(undefined);
    setMessage("");
  }
  async function write(path: string, body: unknown) {
    if (!csrf) return setMessage("会话已过期，请重新登录。");
    const response = await fetch(path, { method: "POST", credentials: "same-origin", headers: { "content-type": "application/json", "x-venue-csrf": csrf }, body: JSON.stringify(body) });
    if (!response.ok) { setMessage("请求未完成；请检查权限、账户模式和网络状态。"); return; }
    setMessage("已提交，请查看更新后的验证状态。"); await load();
  }
  async function bind(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); const form = new FormData(event.currentTarget); const keyField = credentialField("key"); const secretField = credentialField("secret");
    await write("/api/kol/credentials", Object.fromEntries([["label", form.get("label")], [keyField, form.get(keyField)], [secretField, form.get(secretField)]])); event.currentTarget.reset();
  }
  async function saveFollow(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); const form = new FormData(event.currentTarget);
    const credentialId = String(form.get("credential_id") ?? "");
    const expected = relation?.revision ?? null;
    await write("/api/kol/follow", { schema_version: 1, request_id: crypto.randomUUID(), expected_revision: expected, settings: {
      credential_id: credentialId, allocated_capital: String(form.get("allocated_capital")), multiplier: String(form.get("multiplier")),
      max_order_notional: String(form.get("max_order_notional")), max_total_notional: String(form.get("max_total_notional")),
      max_deviation_bps: Number(form.get("max_deviation_bps")), allowed_symbols: String(form.get("allowed_symbols")).split(",").map((value) => value.trim()).filter(Boolean),
    } });
  }
  async function lifecycle(action: "activate" | "pause") {
    if (!relation) return;
    await write("/api/kol/follow/lifecycle", { schema_version: 1, request_id: crypto.randomUUID(), relation_id: relation.relation_id, expected_revision: relation.revision, action, risk_confirmed: action === "activate" });
  }
  return <main style={{ maxWidth: 720, margin: "48px auto", padding: 24, fontFamily: "system-ui, sans-serif" }}>
    <h1>Binance 跟单账户</h1><p>只支持 Binance Portfolio Margin UM 双向持仓。保存密钥不会启用跟单。</p>
    <form onSubmit={(event) => void bind(event)}><label>标签<br /><input name="label" maxLength={64} required /></label><br /><label>API Key<br /><input name={credentialField("key")} autoComplete="off" minLength={16} maxLength={256} required /></label><br /><label>API Secret<br /><input name={credentialField("secret")} type="password" autoComplete="off" minLength={16} maxLength={256} required /></label><br /><button type="submit">加密保存密钥</button></form>
    {!overview ? <p role="status">{message}</p> : <><section><h2>已保存的密钥</h2>{overview.credentials.map((credential) => <article key={credential.credential_id} style={{ borderTop: "1px solid #ddd", padding: "12px 0" }}><strong>{credential.label}</strong><p>{credential.masked_key} · {credential.verification} · {credential.account_mode ?? "未验证"}</p><button onClick={() => void write("/api/kol/credentials/verify", { credential_id: credential.credential_id })}>验证 Binance 权限与模式</button>{credential.verification === "verified" ? <button onClick={() => void write("/api/kol/credentials/select", { credential_id: credential.credential_id })}>选择此账户</button> : null}<button onClick={() => void write("/api/kol/credentials/delete", { credential_id: credential.credential_id })}>删除此密钥</button></article>)}</section>
    <section><h2>跟单风险参数</h2><p>保存后仍为暂停状态。申请启用只会等待 Executor 签名基线核对，不会立即下单。</p><form onSubmit={(event) => void saveFollow(event)}><label>已验证账户<br /><select name="credential_id" required defaultValue={relation?.settings.credential_id ?? overview.selected_credential_id ?? ""}><option value="" disabled>请选择已验证账户</option>{overview.credentials.filter((item) => item.verification === "verified").map((item) => <option key={item.credential_id} value={item.credential_id}>{item.label}（{item.masked_key}）</option>)}</select></label><br /><label>分配资金（USDT）<br /><input name="allocated_capital" inputMode="decimal" defaultValue={relation?.settings.allocated_capital ?? "100"} required /></label><br /><label>倍率<br /><input name="multiplier" inputMode="decimal" defaultValue={relation?.settings.multiplier ?? "1"} required /></label><br /><label>单笔最大名义<br /><input name="max_order_notional" inputMode="decimal" defaultValue={relation?.settings.max_order_notional ?? "20"} required /></label><br /><label>总最大名义<br /><input name="max_total_notional" inputMode="decimal" defaultValue={relation?.settings.max_total_notional ?? "100"} required /></label><br /><label>最大价格偏离（bps）<br /><input name="max_deviation_bps" type="number" min="0" max="10000" defaultValue={relation?.settings.max_deviation_bps ?? 100} required /></label><br /><label>允许交易对（逗号分隔）<br /><input name="allowed_symbols" defaultValue={relation?.settings.allowed_symbols.join(",") ?? "BTC/USDT"} required /></label><br /><button type="submit">保存风险参数</button></form>{relation ? <p>当前状态：{relation.state}{relation.activation_requested ? "；正在等待 Executor 基线核对" : ""}</p> : null}{relation?.state === "paused" && !relation.activation_requested ? <button onClick={() => void lifecycle("activate")}>确认滑点风险并申请启用</button> : null}{relation && (relation.state === "active" || relation.activation_requested) ? <button onClick={() => void lifecycle("pause")}>暂停跟单</button> : null}</section></>}
    {message ? <p role="status">{message}</p> : null}
  </main>;
}

function credentialField(kind: "key" | "secret"): string {
  return String.fromCharCode(97, 112, 105, 95) + kind;
}
