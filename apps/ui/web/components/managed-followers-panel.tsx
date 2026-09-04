"use client";

import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { api, messages, RequestError } from "@/lib/customer-api";

type Account = { managed_id: string; label: string; masked_key: string; verification: string; verified_ms: number | null };
type Overview = { can_manage: boolean; accounts: Account[] };
type Draft = { id: string; label: string; key: string; secret: string; status: "editing" | "uncertain" | "saved"; message: string };
const draft = (): Draft => ({ id: crypto.randomUUID(), label: "", key: "", secret: "", status: "editing", message: "" });
const verification: Record<string, string> = { unverified: "未验证", verified: "验证通过", invalid_credentials: "密钥无效", permission_denied: "权限不符", mode_mismatch: "账户模式不符", network_unavailable: "验证暂不可用，可稍后重试", account_conflict: "账户已绑定其他身份" };

export function ManagedFollowersPanel({ csrf }: { csrf: string }) {
  const [overview, setOverview] = useState<Overview | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [rows, setRows] = useState<Draft[]>([]);
  const gate = useRef(false);
  const alive = useRef(true);
  const dialog = useRef<HTMLDialogElement>(null);
  const refresh = useCallback(async () => {
    try { const value = await api<Overview>("managed-followers"); if (alive.current) { setOverview(value); setError(""); } }
    catch (cause) { if (alive.current) { setError(cause instanceof Error ? cause.message : messages.unavailable); setOverview(previous => previous ? { ...previous, can_manage: false } : null); } }
  }, []);
  useEffect(() => { alive.current = true; void refresh(); return () => { alive.current = false; }; }, [refresh]);
  function close() { if (!gate.current) { dialog.current?.close(); setRows([]); } }
  function edit(id: string, field: "label" | "key" | "secret", value: string) {
    setRows(previous => previous.map(row => row.id === id ? { ...row, [field]: value, message: "" } : row));
  }
  async function save(event: FormEvent) {
    event.preventDefault(); if (gate.current || !overview?.can_manage) return;
    gate.current = true; setBusy(true); setError(""); setNotice("");
    let count = 0;
    try {
      for (const row of rows.filter(row => row.status !== "saved")) {
        try {
          await api<Account>("managed-followers", csrf, { request_id: row.id, label: row.label.trim(), key: row.key, secret: row.secret });
          count++;
          if (!alive.current) return;
          setRows(previous => previous.map(item => item.id === row.id ? { ...item, key: "", secret: "", status: "saved", message: "已加密保存" } : item));
        } catch (cause) {
          if (!alive.current) return;
          const uncertain = !(cause instanceof RequestError) || cause.status >= 500 || cause.status === 408;
          setRows(previous => previous.map(item => item.id === row.id ? { ...item, status: uncertain ? "uncertain" : "editing", message: uncertain ? "结果待确认，请重试原内容；不会重复创建。" : cause instanceof RequestError && cause.status === 409 ? "密钥已保存或请求冲突，请刷新列表核对。" : cause instanceof Error ? cause.message : messages.unavailable } : item));
          // Do not continue a batch after an unknown result or authorization failure.
          break;
        }
      }
      if (count && alive.current) setNotice(`已保存 ${count} 个托管账户。可在列表中手动验证权限。`);
      await refresh();
    } finally { gate.current = false; if (alive.current) setBusy(false); }
  }
  async function verify(account: Account) {
    if (gate.current) return;
    gate.current = true; setBusy(true); setError(""); setNotice("");
    try {
      const result = await api<Account>("managed-verify", csrf, { managed_id: account.managed_id });
      if (alive.current) setNotice(`${account.label}：${verification[result.verification] ?? "状态未知"}`);
      await refresh();
    } catch (cause) { if (alive.current) setError(cause instanceof Error ? cause.message : messages.unavailable); }
    finally { gate.current = false; if (alive.current) setBusy(false); }
  }
  if (overview && !overview.can_manage && overview.accounts.length === 0 && !error) return null;
  return <section className="panel" aria-label="托管跟单账户">
    <div className="heading"><h2>托管跟单账户</h2><span className="pill">{overview ? `${overview.accounts.length} / 200` : "正在读取"}</span></div>
    <p>为交由你管理的 Binance 账户保存 API 密钥，并逐个验证。保存后跟单保持关闭。</p>
    {error && <p role="alert" className="notice error">{error}</p>}
    {notice && <p role="status" className="notice">{notice}</p>}
    <div className="buttons"><button className="primary" disabled={busy || !overview?.can_manage || overview.accounts.length >= 200} onClick={() => { setRows([draft()]); dialog.current?.showModal(); }}>添加托管 API Key</button><button disabled={busy} onClick={() => void refresh()}>刷新托管账户</button></div>
    {overview?.accounts.length === 0 && <p className="muted">尚未添加托管账户。点击上方按钮开始保存。</p>}
    {overview && overview.accounts.length > 0 && <div className="table"><table><thead><tr><th>账户标签</th><th>API Key</th><th>验证状态</th><th>跟单</th><th>操作</th></tr></thead><tbody>{overview.accounts.map(account => <tr key={account.managed_id}><td>{account.label}</td><td>{account.masked_key}</td><td>{verification[account.verification] ?? "状态未知"}</td><td>未启用</td><td><button disabled={busy || !overview.can_manage} onClick={() => void verify(account)}>验证权限</button></td></tr>)}</tbody></table></div>}
    <dialog ref={dialog} className="managed-dialog" aria-labelledby="managed-title" onCancel={event => { event.preventDefault(); close(); }}>
      <h2 id="managed-title">添加托管 API Key</h2><p>填写标签、API Key 和 API Secret。支持一次添加多个账户，最多 10 个。</p>
      <p className="muted">仅支持 Binance Portfolio Margin · UM · 双向持仓；开启读取及 UM 交易权限，关闭提现。密钥加密保存后只显示掩码。</p>
      {error && <div className="notice error" role="alert">{error}<button type="button" disabled={busy} onClick={() => void refresh()}>刷新连接</button></div>}
      <form onSubmit={event => void save(event)}>
        <div className="managed-rows">{rows.map((row, index) => <fieldset key={row.id} disabled={busy || row.status !== "editing"} className="managed-row"><legend>账户 {index + 1}{row.status === "saved" ? " · 已保存" : ""}</legend>
          <label>账户标签<input aria-label={`账户 ${index + 1} 标签`} value={row.label} onChange={e => edit(row.id,"label",e.target.value)} required maxLength={64} autoComplete="off" /></label>
          <div className="customer-grid"><label>API Key<input aria-label={`账户 ${index + 1} API Key`} type="password" value={row.key} onChange={e => edit(row.id,"key",e.target.value)} required minLength={16} maxLength={256} pattern="[A-Za-z0-9]+" autoComplete="off" spellCheck={false} /></label>
          <label>API Secret<input aria-label={`账户 ${index + 1} API Secret`} type="password" value={row.secret} onChange={e => edit(row.id,"secret",e.target.value)} required minLength={16} maxLength={256} pattern="[A-Za-z0-9]+" autoComplete="off" spellCheck={false} /></label></div>
          {row.message && <p role="status">{row.message}</p>}
          {rows.length > 1 && row.status === "editing" && <button type="button" onClick={() => setRows(previous => previous.filter(item => item.id !== row.id))}>移除此行</button>}
        </fieldset>)}</div>
        <div className="buttons managed-actions"><button type="button" disabled={busy || rows.length >= 10 || rows.some(row => row.status === "uncertain")} onClick={() => setRows(previous => [...previous,draft()])}>再添加一个账户</button>
          <button type="submit" className="primary" disabled={busy || !overview?.can_manage || rows.every(row => row.status === "saved")}>{busy ? "正在保存…" : rows.some(row => row.status === "uncertain") ? "确认并重试保存" : "加密保存账户"}</button>
          <button type="button" disabled={busy} onClick={close}>{rows.some(row => row.status === "saved") ? "完成" : "取消"}</button></div>
      </form>
    </dialog>
  </section>;
}
