"use client";

import { useCallback, useEffect, useState } from "react";
import { api, RequestError } from "@/lib/customer-api";
import type { Credential } from "@/lib/customer-types";

type Source = { trading_account_id: string | null; revision: number; can_change: boolean };
export function KolSourcePanel({ csrf, credentials, onSource }: { csrf: string; credentials: Credential[]; onSource: (account: string | null) => void }) {
  const [source, setSource] = useState<Source | null>(null);
  const [choice, setChoice] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [saved, setSaved] = useState(false);
  const refresh = useCallback(async () => {
    try { setSource(await api<Source>("kol-source")); }
    catch { setSource(null); setError("无法读取带单账户，请刷新后重试。"); }
  }, []);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => { onSource(source?.trading_account_id ?? null); }, [source, onSource]);
  const current = source?.trading_account_id ? credentials.find(c => c.trading_account_id === source.trading_account_id) : undefined;
  return <section className="panel" aria-label="带单账户"><h2>带单账户（只能选择一个）</h2><a href="/help/kol#source" target="_blank" rel="noreferrer">查看设置步骤</a>
    <p>KOL 和跟单帐户都必须是币安统一帐户，且开通 U 本位合约，在交易设置中修改为双向持仓。API 设置需要开启统一账户交易。</p>
    <p>当前带单账户：<strong>{current?.label ?? source?.trading_account_id ?? "尚未指定"}</strong></p>
    {error && <p role="alert">{error}</p>}
    {saved && <p role="status">带单账户已保存。保存不会启动带单。</p>}
    {source && !source.can_change ? <p className="muted">已有带单机器人或跟单关系，当前带单账户已锁定，不能直接更换。</p> : <form onSubmit={async event => {
      event.preventDefault(); if (!source || busy) return;
      setBusy(true); setError(""); setSaved(false);
      try { setSource(await api<Source>("kol-source", csrf, { credential_id: choice, expected_revision: source.revision })); setChoice(""); setSaved(true); }
      catch (cause) { setError(cause instanceof RequestError ? cause.message : "保存未确认，请刷新核对当前带单账户。"); await refresh(); }
      finally { setBusy(false); }
    }}><label>指定带单账户<select required value={choice} disabled={!source || busy} onChange={event => { setChoice(event.target.value); setSaved(false); }}><option value="">选择本人已验证的账户</option>{credentials.filter(c => c.verification === "verified" && c.dual_position).map(c => <option key={c.credential_id} value={c.credential_id}>{c.label}</option>)}</select></label><div className="buttons"><button disabled={!source || busy || !choice}>{busy ? "正在保存…" : "保存带单账户"}</button></div></form>}
    <p className="muted">此设置对所有登录端生效。验证通过并指定账户后自动开通带单，无需额外审批；保存不会启动带单。</p>
    <button disabled={busy} onClick={() => { setError(""); setSaved(false); void refresh(); }}>刷新带单账户</button>
  </section>;
}
