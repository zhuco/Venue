"use client";
import { useRef, useState, type FormEvent } from "react";
import { api, messages, RequestError } from "@/lib/customer-api";
import type { ManagedFollowRelation, ManagedFollowSettings } from "@/lib/customer-types";
import { FollowSizingFields, sizingFromForm } from "./follow-sizing-fields";

export function ManagedFollowSettingsPanel({ managedId, label, csrf, canManage }: { managedId: string; label: string; csrf: string; canManage: boolean }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const gate = useRef(false);
  const [loaded, setLoaded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [relation, setRelation] = useState<ManagedFollowRelation | null>(null);
  const [pending, setPending] = useState<{ action: string; body: object } | null>(null);
  const [confirmed, setConfirmed] = useState(false);
  async function refresh() {
    if (gate.current) return;
    gate.current = true; setBusy(true); setError("");
    try { setRelation(await api<ManagedFollowRelation | null>("managed-status", csrf, { managed_id: managedId })); setLoaded(true); }
    catch (cause) { setLoaded(false); setError(cause instanceof Error ? cause.message : messages.unavailable); }
    finally { gate.current = false; setBusy(false); }
  }
  async function submit(action: string, body: object) {
    if (gate.current) return;
    gate.current = true; setBusy(true); setError("");
    try { setRelation(await api<ManagedFollowRelation>(action, csrf, body)); setLoaded(true); setPending(null); setConfirmed(false); }
    catch (cause) {
      const uncertain = !(cause instanceof RequestError) || cause.status >= 500 || cause.status === 408;
      if (uncertain) setPending({ action, body });
      else setPending(null);
      setError(uncertain ? "结果待确认。请核对状态，或用原请求重试。" : cause instanceof Error ? cause.message : messages.unavailable);
    } finally { gate.current = false; setBusy(false); }
  }
  function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending) return;
    const data = new FormData(event.currentTarget);
    const text = (name: string) => String(data.get(name) ?? "").trim();
    const settings: ManagedFollowSettings = {
      sizing: sizingFromForm(data), allocated_capital: text("capital"), multiplier: text("multiplier"),
      max_order_notional: text("orderLimit"), max_total_notional: text("totalLimit"),
      max_deviation_bps: Number(text("deviation")), allowed_symbols: text("symbols").split(/[,，\s]+/).filter(Boolean),
    };
    void submit("managed-settings", { managed_id: managedId, request_id: crypto.randomUUID(), expected_revision: relation?.revision ?? null, settings });
  }
  const current = relation?.settings;
  const state = !loaded ? "尚未读取" : !relation ? "尚未设置" : relation.activation_requested ? "正在校验激活条件" : ({ paused: "已暂停", active: "跟单中", needs_attention: "需要处理", disabled: "已停用" }[relation.state] ?? "状态未知");
  return <>
    <button onClick={() => { dialog.current?.showModal(); void refresh(); }}>跟单设置</button>
    <dialog ref={dialog} className="managed-dialog" style={{ whiteSpace: "normal", overflowWrap: "anywhere" }} aria-label={`${label} 跟单设置`} onCancel={event => { if (busy || pending) event.preventDefault(); }}>
      <h2>{label} · 跟单设置</h2><p role="status">{state}</p>
      {error && <p role="alert" className="notice error">{error}</p>}
      {pending && <button disabled={busy} onClick={() => void submit(pending.action, pending.body)}>重试原请求</button>}
      <button disabled={busy} onClick={() => void refresh()}>刷新跟单状态</button>
      {loaded && <form key={`${managedId}:${relation?.revision ?? 0}`} onSubmit={save}>
        <fieldset disabled={busy || Boolean(pending) || !canManage || relation?.state === "active" || relation?.activation_requested}>
          <div className="customer-grid"><FollowSizingFields value={current?.sizing} />
            <label>分配资金（报价币）<input name="capital" inputMode="decimal" defaultValue={current?.allocated_capital ?? ""} required /></label>
            <label>定比跟单倍数<input name="multiplier" inputMode="decimal" defaultValue={current?.multiplier ?? "1"} required /></label>
            <label>单笔名义上限（报价币）<input name="orderLimit" inputMode="decimal" defaultValue={current?.max_order_notional ?? ""} required /></label>
            <label>总名义上限（报价币）<input name="totalLimit" inputMode="decimal" defaultValue={current?.max_total_notional ?? ""} required /></label>
            <label>价格偏离限制（基点）<input name="deviation" type="number" min="0" max="5000" defaultValue={current?.max_deviation_bps ?? ""} required /></label>
            <label>允许交易对<input name="symbols" defaultValue={current?.allowed_symbols.join(", ") ?? ""} placeholder="DASH/USDT" required /></label>
          </div><button type="submit">保存设置</button>
        </fieldset>
      </form>}
      {loaded && relation && <>
        <label className="customer-confirm"><input type="checkbox" checked={confirmed} onChange={event => setConfirmed(event.target.checked)} disabled={busy || Boolean(pending)} />已核对账户、金额和风险参数，确认启用跟单。</label>
        <div className="buttons"><button disabled={busy || Boolean(pending) || !canManage || !confirmed || relation.state !== "paused" || relation.activation_requested} onClick={() => void submit("managed-follow", { managed_id: managedId, request_id: crypto.randomUUID(), relation_id: relation.relation_id, expected_revision: relation.revision, action: "activate", risk_confirmed: true })}>启用跟单</button>
          <button disabled={busy || Boolean(pending)} onClick={() => void submit("managed-follow", { managed_id: managedId, request_id: crypto.randomUUID(), relation_id: relation.relation_id, expected_revision: relation.revision, action: "pause", risk_confirmed: false })}>暂停并撤销同步挂单</button></div>
      </>}
      <p className="muted">保存不自动启用。激活需完成空仓、无挂单和权限验证；暂停保留已有仓位。</p>
      <button disabled={busy || Boolean(pending)} onClick={() => { dialog.current?.close(); setConfirmed(false); }}>关闭</button>
    </dialog>
  </>;
}
