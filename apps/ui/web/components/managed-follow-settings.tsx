"use client";
import { useRef, useState, type FormEvent } from "react";
import { api, messages, RequestError } from "@/lib/customer-api";
import type { ManagedFollowRelation, ManagedFollowSettings } from "@/lib/customer-types";
import { FollowSizingFields, hasFollowEquity, sizingFromForm } from "./follow-sizing-fields";

export function ManagedFollowSettingsPanel({ managedId, label, csrf, canManage, equity }: { managedId: string; label: string; csrf: string; canManage: boolean; equity: string | null }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const gate = useRef(false);
  const [loaded, setLoaded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [relation, setRelation] = useState<ManagedFollowRelation | null>(null);
  const [pending, setPending] = useState<{ action: string; body: object } | null>(null);
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
    try { setRelation(await api<ManagedFollowRelation>(action, csrf, body)); setLoaded(true); setPending(null); }
    catch (cause) {
      const uncertain = !(cause instanceof RequestError) || cause.status >= 500 || cause.status === 408;
      if (uncertain) setPending({ action, body });
      else setPending(null);
      setError(uncertain ? "结果待确认。请核对状态，或用原请求重试。" : cause instanceof Error ? cause.message : messages.unavailable);
    } finally { gate.current = false; setBusy(false); }
  }
  function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending || !relation || !hasFollowEquity(equity)) return;
    const data = new FormData(event.currentTarget);
    const text = (name: string) => String(data.get(name) ?? "").trim();
    const sizing = sizingFromForm(data);
    const settings: ManagedFollowSettings = {
      sizing, allocated_capital: equity, multiplier: text("multiplier"),
      max_order_notional: sizing.mode === "fixed_notional" ? sizing.notional : equity, max_total_notional: sizing.mode === "fixed_notional" ? sizing.notional : equity,
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
      {(relation?.state === "active" || relation?.activation_requested) && <p className="notice">跟单中或正在申请激活，当前参数仅供查看。请先暂停，待同步挂单撤销和对账完成后修改，再重新申请跟单。暂停不会自动平仓。</p>}
      {error && <p role="alert" className="notice error">{error}</p>}
      {pending && <button disabled={busy} onClick={() => void submit(pending.action, pending.body)}>重试原请求</button>}
      <div className="buttons"><button disabled={busy} onClick={() => void refresh()}>刷新跟单状态</button></div>
      {loaded && (!relation || !hasFollowEquity(equity)) && <p className="notice">请先在账户列表验证权限，读取全部权益并初始化跟单设置。</p>}
      {loaded && <form key={`${managedId}:${relation?.revision ?? 0}`} onSubmit={save}>
        <fieldset disabled={busy || Boolean(pending) || !canManage || !relation || !hasFollowEquity(equity) || relation?.state === "active" || relation?.activation_requested}>
          <FollowSizingFields value={current?.sizing} multiplier={current?.multiplier} equity={equity} />
          <details><summary>高级设置（可选）</summary><div className="customer-grid">
            <label>价格偏离限制（基点）<input name="deviation" type="number" min="0" max="5000" defaultValue={current?.max_deviation_bps ?? ""} required /></label>
            <label>允许交易对（留空允许全部）<input name="symbols" defaultValue={current?.allowed_symbols.join(", ") ?? ""} placeholder="留空允许全部" /></label>
          </div></details><div className="buttons"><button className="primary" type="submit">保存设置</button></div>
        </fieldset>
      </form>}
      {loaded && relation && <>
        <div className="buttons"><button disabled={busy || Boolean(pending) || !canManage || relation.state !== "paused" || relation.activation_requested} onClick={() => void submit("managed-follow", { managed_id: managedId, request_id: crypto.randomUUID(), relation_id: relation.relation_id, expected_revision: relation.revision, action: "activate", risk_confirmed: true })}>重新申请跟单</button>
          <button disabled={busy || Boolean(pending)} onClick={() => void submit("managed-follow", { managed_id: managedId, request_id: crypto.randomUUID(), relation_id: relation.relation_id, expected_revision: relation.revision, action: "pause", risk_confirmed: false })}>暂停并撤销同步挂单</button></div>
      </>}
      <p className="muted">保存 API 即授权跟单；验证成功后自动申请激活。系统仍校验空仓、无挂单、权限和单一执行器；暂停保留已有仓位。</p>
      <div className="buttons"><button disabled={busy || Boolean(pending)} onClick={() => dialog.current?.close()}>关闭</button></div>
    </dialog>
  </>;
}
