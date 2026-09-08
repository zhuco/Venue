"use client";

import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { api, messages, RequestError, verificationFeedback, verificationMessages } from "@/lib/customer-api";
import { KolSourcePanel } from "./kol-source-panel";
import { KolInvitePanel } from "./kol-invite-panel";
import { ManagedFollowersPanel } from "./managed-followers-panel";
import { FollowAuthorizationFields, FollowSizingFields, hasFollowEquity, authorizationFromForm, sizingFromForm } from "./follow-sizing-fields";
import type { Credential, CustomerOverview, FollowRelation, FollowSettings, Invite, LeaderAccess, MirrorOrder } from "@/lib/customer-types";

const states: Record<string, string> = { stopped: "已停止", running: "运行中", draining: "正在撤销同步挂单", needs_attention: "需要处理", paused: "已暂停", active: "跟单中", disabled: "已禁用", pending: "待执行", live: "已挂单", cancelling: "撤单中", terminal: "已结束", blocked: "未下单", verified: "验证通过", unverified: "未验证" };
const field = (form: FormData, name: string) => String(form.get(name) ?? "");
type Pending = { action: string; body: object };

export function CustomerConsole({ inviteCode, registration = false }: { inviteCode?: string; registration?: boolean }) {
  const registering = registration || Boolean(inviteCode);
  const [overview, setOverview] = useState<CustomerOverview | null>(null);
  const [kolProfile, setKolProfile] = useState<{ state: string } | null>(null);
  const [sourceAccount, setSourceAccount] = useState<string | null>(null);
  const [leader, setLeader] = useState<LeaderAccess | null>(null);
  const [relation, setRelation] = useState<FollowRelation | null>(null);
  const [orders, setOrders] = useState<MirrorOrder[]>([]);
  const [invite, setInvite] = useState<Invite | null>(null);
  const [error, setError] = useState(""); const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(true); const [busy, setBusy] = useState(false);
  const [fresh, setFresh] = useState(false); const [confirmed, setConfirmed] = useState(false);
  const [pending, setPending] = useState<Pending | null>(null);
  const mutating = useRef(false); const version = useRef(0); const hasSession = useRef(true);
  const refresh = useCallback(async () => {
    const current = ++version.current;
    try {
      const account = await api<CustomerOverview>("session");
      const results = await Promise.all([api<LeaderAccess>("leader"), api<FollowRelation | null>("settings").catch(cause => { if (cause instanceof RequestError && cause.status === 404) return null; throw cause; }), api<MirrorOrder[]>("mirror-orders"), api<{ state: string } | null>("kol-profile").catch(cause => { if (cause instanceof RequestError && (cause.status === 403 || cause.status === 404)) return null; throw cause; })]);
      if (version.current !== current) return;
      hasSession.current = true; setOverview(account); setLeader(results[0]); setRelation(results[1]); setOrders(results[2]); setKolProfile(results[3]); setFresh(true);
    } catch (cause) {
      if (version.current !== current) return;
      setFresh(false);
      if (cause instanceof RequestError && cause.status === 401) { hasSession.current = false; setOverview(null); setLeader(null); setKolProfile(null); setRelation(null); setOrders([]); setPending(null); setConfirmed(false); }
      else setError(cause instanceof Error ? cause.message : messages.unavailable);
    } finally { if (version.current === current) setLoading(false); }
  }, []);
  useEffect(() => { void refresh(); const timer = setInterval(() => { if (hasSession.current && !mutating.current) void refresh(); }, 5000); return () => { clearInterval(timer); version.current++; }; }, [refresh]);
  useEffect(() => {
    if (!inviteCode) return;
    let disposed = false;
    api<Invite>(`invite?code=${encodeURIComponent(inviteCode)}`).then(value => { if (!disposed) setInvite(value); }).catch(cause => { if (!disposed) setError(cause instanceof Error ? cause.message : messages.unavailable); });
    return () => { disposed = true; };
  }, [inviteCode]);
  async function mutate(action: string, body: object, retryable = false) {
    if (mutating.current) return;
    mutating.current = true; setBusy(true); setError(""); setMessage(""); setFresh(false); version.current++;
    if (retryable) setPending({ action, body });
    try {
      const result = await api<Partial<Credential>>(action, overview?.csrf, body); setPending(null); setConfirmed(false);
      if (action === "logout") { hasSession.current = false; setOverview(null); setLeader(null); setKolProfile(null); setRelation(null); setOrders([]); }
      else {
        if (action === "verify") { const feedback = verificationFeedback(result.verification); if (feedback.success) setMessage(feedback.message); else setError(feedback.message); }
        else setMessage("请求已处理。");
        await refresh();
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : messages.unavailable);
      if (cause instanceof RequestError && cause.status >= 400 && cause.status < 500 && cause.status !== 408 && cause.status !== 429) setPending(null);
      await refresh();
    } finally { mutating.current = false; setBusy(false); }
  }
  const selected = sourceAccount ? overview?.credentials.find(c => c.trading_account_id === sourceAccount) : undefined;
  const locked = busy || !fresh || pending !== null;
  const bot = leader?.bot;
  return <main className="customer-page"><div className="customer-stack">
    <header className="customer-header"><div><p className="eyebrow">VENUE · BINANCE LIVE</p><h1>{kolProfile ? "KOL 管理后台" : "跟单账户"}</h1><p>同步 KOL 的限价单、市价单和止损单，使用自己的交易账户执行。</p></div>{overview && <div className="buttons">{kolProfile && <a className="guide-help-link" href="/help/kol">KOL 使用指南</a>}<span>{overview.user.username}</span><button disabled={busy || pending !== null} onClick={() => void mutate("logout", {})}>退出登录</button></div>}</header>
    {error && <div role="alert" className="notice error">{error}</div>}
    {message && <div role="status" className="notice">{message}</div>}
    {pending && <div className="notice"><span>上次请求结果待确认。重试会使用同一个请求编号。</span><div className="buttons"><button disabled={busy} onClick={() => void mutate(pending.action, pending.body, true)}>查询并重试原请求</button></div></div>}
    {loading ? <p role="status">正在读取账户…</p> : !overview ? <section className="panel customer-auth"><h2>{registering ? "注册跟单账户" : "登录账户"}</h2>{invite && <div className="customer-invite"><strong>{invite.profile.name} · {invite.profile.title}</strong><p>{invite.profile.description}</p></div>}
      <form onSubmit={(event: FormEvent<HTMLFormElement>) => { event.preventDefault(); const data = new FormData(event.currentTarget); const body = { username: field(data, "username"), password: field(data, "password"), ...(registering ? { invite_code: inviteCode ?? field(data, "inviteCode").trim() } : {}) }; event.currentTarget.reset(); void mutate(registering ? "register" : "login", body); }}>
        {registering && !inviteCode && <label>KOL 邀请码<input name="inviteCode" required minLength={6} maxLength={64} pattern="[A-Za-z0-9_-]+" autoComplete="off" /><small>向你要跟随的 KOL 获取邀请码。注册后归属固定，不会自动换绑。</small></label>}
        <label>用户名<input name="username" autoComplete="username" required minLength={3} maxLength={64} /></label>
        <label>密码<input name="password" type="password" autoComplete={registering ? "new-password" : "current-password"} required minLength={8} maxLength={128} /></label>
        <div className="buttons"><button className="primary" disabled={busy || Boolean(inviteCode && !invite)}>{registering ? "注册跟单账户" : "登录"}</button>{registering ? <a href="/login">已有账户，前往登录</a> : <a href="/register">注册跟单账户</a>}</div>
      </form><p className="muted">注册后仅管理自己的跟单账户，添加账户时选择定比或定额，不开通带单权限。跟单注册需要有效的 KOL 邀请。已有跟单绑定保持不变。历史表现不保证未来收益；主从账户独立成交，可能产生亏损。</p></section> : <>
      {!fresh && <div className="notice">账户状态尚未刷新，启动操作暂不可用。<button disabled={busy} onClick={() => void refresh()}>刷新状态</button></div>}
      <section className="panel"><h2>{kolProfile ? "交易账户" : "我的跟单账户"}</h2><p>必须使用币安统一账户（Portfolio Margin），并开启 U 本位合约双向持仓。密钥须具备读取和 UM 交易权限，关闭提现。</p>
        {overview.credentials.length === 0 ? <p className="muted">尚未绑定交易账户。</p> : overview.credentials.map(credential => <CredentialRow key={credential.credential_id} credential={credential} selected={selected?.credential_id === credential.credential_id} disabled={locked} mutate={mutate} canLead={Boolean(kolProfile)} relation={relation?.settings.credential_id === credential.credential_id ? relation : null} />)}
        <details><summary>{kolProfile ? "添加交易账户" : "添加跟单账户"}</summary><form onSubmit={(event: FormEvent<HTMLFormElement>) => { event.preventDefault(); const data = new FormData(event.currentTarget); const body = { label: field(data, "label"), key: field(data, "key"), secret: field(data, "secret"), authorization: kolProfile ? { sizing: { mode: "proportional" }, multiplier: "1" } : authorizationFromForm(data) }; event.currentTarget.reset(); void mutate("credentials", body); }}>
          <label>账户名称<input name="label" required maxLength={64} autoComplete="off" /></label><label>API密钥<input name="key" type="password" required minLength={16} maxLength={256} autoComplete="off" spellCheck={false} /></label><label>密钥<input name="secret" type="password" required minLength={16} maxLength={256} autoComplete="off" spellCheck={false} /></label>
          {!kolProfile && <FollowAuthorizationFields />}
          <div className="buttons"><button disabled={locked}>{kolProfile ? "绑定交易账户" : "绑定并授权跟单"}</button></div>
        </form><p className="muted">{kolProfile ? "绑定并验证成功后，请在下方指定唯一带单账户。" : "默认定比跟单 1 倍，定额跟单需填写每笔名义金额。验证成功后自动申请跟单。"}密钥仅由服务器加密保存。</p></details>
      </section>
      {kolProfile && <KolSourcePanel csrf={overview.csrf} credentials={overview.credentials} onSource={setSourceAccount} />}
      {(leader?.can_use || bot) && <section className="panel" aria-label="带单机器人"><div className="heading"><h2>带单机器人</h2><span className="pill">{bot ? states[bot.state] ?? bot.state : "尚未创建"}</span></div>
        {!leader?.can_use && <p>带单权限已撤销。已有实例仍可查看和停止。</p>}
        {bot ? <><p>主账户：{bot.trading_account_id}</p><p>跟单账户 {bot.active_followers} · 待处理挂单 {bot.pending_orders}</p>{bot.attention_code && <p role="status">需处理：{bot.attention_code}</p>}</> : <p>将已验证的 KOL 主账户设为带单源。</p>}
        {leader?.can_use && bot?.state === "stopped" && <label className="customer-confirm"><input type="checkbox" checked={confirmed} onChange={e => setConfirmed(e.target.checked)} />我确认启动后，符合条件的新挂单将同步到启用跟单的账户。</label>}
        <div className="buttons">{!bot && leader?.can_use && <button disabled={locked || selected?.verification !== "verified"} onClick={() => void mutate("leader", { schema_version: 1, request_id: crypto.randomUUID(), credential_id: selected?.credential_id }, true)}>创建带单机器人</button>}
          {bot && leader?.can_use && bot.state === "stopped" && <button className="primary" disabled={locked || !confirmed} onClick={() => void mutate("leader-lifecycle", { schema_version: 1, request_id: crypto.randomUUID(), bot_id: bot.bot_id, expected_revision: bot.revision, action: "start", risk_confirmed: true }, true)}>启动带单</button>}
          {bot && bot.state !== "stopped" && <button disabled={locked || bot.state === "draining"} onClick={() => void mutate("leader-lifecycle", { schema_version: 1, request_id: crypto.randomUUID(), bot_id: bot.bot_id, expected_revision: bot.revision, action: "stop", risk_confirmed: false }, true)}>停止并撤销同步挂单</button>}
        </div><p className="muted">停止只撤销程序创建的同步挂单，已有仓位不会自动平仓。</p>
      </section>}
      {kolProfile && <KolInvitePanel key={`invite:${overview.user.user_id}`} csrf={overview.csrf} enabled={kolProfile.state === "enabled"} />}
      <ManagedFollowersPanel key={`managed:${overview.user.user_id}`} csrf={overview.csrf} />
      {!kolProfile && <section className="panel"><h2>我的同步订单</h2><p className="muted">主账户和跟单账户独立成交；显示最近 500 条订单记录。</p>{orders.length === 0 ? <p>暂无同步订单。</p> : <div className="table"><table><thead><tr><th>交易对</th><th>来源订单</th><th>委托数量</th><th>已成交</th><th>状态</th></tr></thead><tbody>{orders.map(order => <tr key={order.mirror_id}><td>{order.symbol}</td><td>{order.source_order_id}</td><td>{order.requested_quantity}</td><td>{order.filled_quantity}</td><td>{states[order.state] ?? order.state}{order.attention_code && <small> · {order.attention_code}</small>}</td></tr>)}</tbody></table></div>}</section>}
    </>}
  </div></main>;
}

type Mutate = (action: string, body: object, retryable?: boolean) => Promise<void>;
function CredentialRow({ credential, selected, disabled, mutate, relation, canLead }: { credential: Credential; selected: boolean; disabled: boolean; mutate: Mutate; relation: FollowRelation | null; canLead: boolean }) {
  return <div className="customer-credential"><div><strong>{credential.label}</strong> <span>{credential.masked_key}</span><p>{verificationMessages[credential.verification] ?? credential.verification}{selected && canLead ? " · 带单账户" : ""}</p></div><div className="buttons"><button disabled={disabled} onClick={() => void mutate("verify", { credential_id: credential.credential_id })}>验证权限</button>{relation && <FollowPanel relation={relation} credential={credential} disabled={disabled} mutate={mutate} />}</div><details><summary>删除绑定</summary><form onSubmit={event => { event.preventDefault(); const data = new FormData(event.currentTarget); const body = { credential_id: credential.credential_id, password: field(data, "password") }; event.currentTarget.reset(); void mutate("delete", body); }}><label>输入登录密码确认<input name="password" type="password" required autoComplete="current-password" /></label><div className="buttons"><button disabled={disabled}>确认删除绑定</button></div></form></details></div>;
}
function FollowPanel({ relation, credential, disabled, mutate }: { relation: FollowRelation; credential: Credential; disabled: boolean; mutate: Mutate }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const current = relation?.settings;
  const equity = credential.equity;
  const editing = disabled || !relation || !hasFollowEquity(equity) || relation?.state === "active" || relation?.activation_requested;
  return <><button disabled={disabled} onClick={() => dialog.current?.showModal()}>跟单设置</button><dialog ref={dialog} className="managed-dialog" aria-label={`${credential.label} 跟单设置`} onCancel={event => { if (disabled) event.preventDefault(); }}><div className="heading"><h2>{credential.label} · 跟单设置</h2><span className="pill">{relation?.activation_requested ? "正在校验激活条件" : relation ? states[relation.state] ?? relation.state : "尚未设置"}</span></div><p>仅同步启用后的新挂单，不追补已有订单或仓位。</p>{(relation.state === "active" || relation.activation_requested) && <p className="notice">跟单中或正在申请激活，当前参数仅供查看。请先暂停，待同步挂单撤销和对账完成后修改，再重新申请跟单。暂停不会自动平仓。</p>}{(!relation || !hasFollowEquity(equity)) && <p className="notice">请先验证账户权限，读取全部权益并初始化跟单设置。</p>}
    <form key={`${relation?.relation_id ?? "new"}:${relation?.revision ?? 0}`} onSubmit={event => { event.preventDefault(); const data = new FormData(event.currentTarget); if (!hasFollowEquity(equity) || !relation) return; const sizing = sizingFromForm(data); const settings: FollowSettings = { credential_id: field(data, "credential"), sizing, allocated_capital: equity, multiplier: field(data, "multiplier"), max_order_notional: sizing.mode === "fixed_notional" ? sizing.notional : equity, max_total_notional: sizing.mode === "fixed_notional" ? sizing.notional : equity, max_deviation_bps: Number(field(data, "deviation")), allowed_symbols: field(data, "symbols").split(/[,，\s]+/).filter(Boolean) }; void mutate("settings", { schema_version: 1, request_id: crypto.randomUUID(), expected_revision: relation?.revision ?? null, settings }, true); }}>
      <fieldset disabled={Boolean(editing)}><div className="customer-grid"><input type="hidden" name="credential" value={credential.credential_id} />
        <FollowSizingFields value={current?.sizing} multiplier={current?.multiplier} equity={equity} /></div><details><summary>高级设置（可选）</summary><div className="customer-grid"><label>价格偏离限制（基点）<input name="deviation" type="number" min="0" max="5000" defaultValue={current?.max_deviation_bps ?? 100} required /></label><label className="customer-wide">允许交易对（留空允许全部；多项以逗号分隔）<input name="symbols" defaultValue={current?.allowed_symbols.join(", ") ?? ""} placeholder="留空允许全部" /></label></div></details><div className="buttons"><button>保存跟单设置</button></div></fieldset>
    </form>{relation && <div className="buttons"><button className="primary" disabled={disabled || relation.state === "active" || relation.activation_requested} onClick={() => void mutate("follow", { schema_version: 1, request_id: crypto.randomUUID(), relation_id: relation.relation_id, expected_revision: relation.revision, action: "activate", risk_confirmed: true }, true)}>重新申请跟单</button><button disabled={disabled || (relation.state === "paused" && !relation.activation_requested)} onClick={() => void mutate("follow", { schema_version: 1, request_id: crypto.randomUUID(), relation_id: relation.relation_id, expected_revision: relation.revision, action: "pause", risk_confirmed: false }, true)}>暂停并撤销同步挂单</button></div>}
    <p className="muted">保存 API 时即授权跟单，验证成功后自动申请激活；系统仍校验空仓、无挂单、权限和单一执行器。主订单结束时撤销子订单剩余量。</p>
    <div className="buttons"><button disabled={disabled} onClick={() => dialog.current?.close()}>关闭</button></div>
  </dialog></>;
}
