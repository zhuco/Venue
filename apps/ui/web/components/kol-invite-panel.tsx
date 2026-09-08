"use client";
import { useEffect, useRef, useState } from "react";
import { api, messages, RequestError } from "@/lib/customer-api";

type Invite = { invite_id: string; invite_code: string | null; active: boolean; created_ms: number };
type Request = { request_id: string; expected_invite_id: string | null; invite_code: string | null };
export function KolInvitePanel({ csrf, enabled }: { csrf: string; enabled: boolean }) {
  const [invite, setInvite] = useState<Invite | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [pending, setPending] = useState<Request | null>(null);
  const [error, setError] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [copied, setCopied] = useState("");
  const [custom, setCustom] = useState("");
  const [origin, setOrigin] = useState("");
  const gate = useRef(false);
  useEffect(() => {
    let alive = true; setOrigin(window.location.origin);
    api<Invite | null>("kol-invite").then(value => { if (alive) { setInvite(value); setLoaded(true); } }).catch(cause => { if (alive) setError(cause instanceof Error ? cause.message : messages.unavailable); });
    return () => { alive = false; };
  }, []);
  async function refresh() {
    if (gate.current) return;
    gate.current = true; setBusy(true); setError("");
    try { setInvite(await api<Invite | null>("kol-invite")); setLoaded(true); }
    catch (cause) { setLoaded(false); setError(cause instanceof Error ? cause.message : messages.unavailable); }
    finally { gate.current = false; setBusy(false); }
  }
  async function generate(manual = false) {
    if (manual && !/^[A-Za-z0-9]{4,64}$/.test(custom.trim())) { setError("请输入 4–64 位英文字母和数字，邀请码区分大小写。"); return; }
    if (gate.current || !loaded || !enabled || (!pending && invite && !confirmed)) return;
    const request = pending ?? { request_id: crypto.randomUUID(), expected_invite_id: invite?.invite_id ?? null, invite_code: manual ? custom.trim() : null };
    gate.current = true; setBusy(true); setError(""); setCopied("");
    try { setInvite(await api<Invite>("kol-invite", csrf, request)); setPending(null); setConfirmed(false); }
    catch (cause) {
      const uncertain = !(cause instanceof RequestError) || cause.status >= 500 || cause.status === 408 || cause.status === 429;
      setPending(uncertain ? request : null);
      setError(uncertain ? "生成结果待确认，请重试原请求；不会重复更换邀请码。" : cause instanceof RequestError && cause.status === 409 ? "邀请码已被使用或状态已变化，请换一个邀请码或刷新后重试。" : cause instanceof Error ? cause.message : messages.unavailable);
    } finally { gate.current = false; setBusy(false); }
  }
  const code = invite?.active ? invite.invite_code : null;
  const link = code && origin ? `${origin}/join/${code}` : "";
  async function copy(value: string) {
    try { await navigator.clipboard.writeText(value); setCopied("已复制"); }
    catch { setError("复制失败，请选中下方文本手动复制。"); }
  }
  return <section className="panel" aria-label="邀请跟单用户"><h2>邀请跟单用户</h2><a href="/help/kol#invite" target="_blank" rel="noreferrer">如何邀请用户并开始跟单？</a>
    <p>用户通过邀请链接注册，或在注册页填写邀请码，注册后固定归属于你。</p>
    {!enabled && <p className="notice">KOL 尚未启用，暂不能生成或使用邀请码。</p>}
    {error && <p role="alert" className="notice error">{error}</p>}
    {loaded && !invite && <p>尚未生成邀请码。</p>}
    {loaded && invite && !code && <p>当前邀请码已过期，或旧记录仅保存哈希，无法显示原文。可生成新邀请码。</p>}
    {code && <div className="customer-grid"><label>邀请码<input readOnly value={code} /></label><label>邀请链接<input readOnly value={link} /></label></div>}
    {invite && <label className="customer-confirm"><input type="checkbox" checked={confirmed} disabled={busy || Boolean(pending)} onChange={event => setConfirmed(event.target.checked)} />我确认更换后旧邀请链接不能再注册，已有跟单归属不变。</label>}
    <label>自定义邀请码（可选）<input value={custom} onChange={event => setCustom(event.target.value)} disabled={busy || Boolean(pending)} minLength={4} maxLength={64} placeholder="例如 KOL2026" /><small>4–64 位英文字母和数字，区分大小写；所有 KOL 的邀请码不能重复。</small></label>
    <div className="buttons"><button disabled={busy} onClick={() => void refresh()}>刷新邀请码</button>
      <button className="primary" disabled={busy || !loaded || !enabled || (!pending && Boolean(invite) && !confirmed)} onClick={() => void generate()}>{pending ? "重试原请求" : invite ? "生成新邀请码" : "生成邀请码"}</button>
      <button disabled={busy || !loaded || !enabled || Boolean(pending) || !custom.trim() || (Boolean(invite) && !confirmed)} onClick={() => void generate(true)}>使用自定义邀请码</button>
      {code && <><button onClick={() => void copy(code)}>复制邀请码</button><button onClick={() => void copy(link)}>复制邀请链接</button></>}
    </div>{copied && <p role="status">{copied}</p>}
  </section>;
}
