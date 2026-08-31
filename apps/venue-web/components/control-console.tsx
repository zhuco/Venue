"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";
import { AppShell } from "./app-shell";
import { freshProjection, safeControlEvent } from "@/lib/realtime";
import { sumDecimals } from "@/lib/decimal";
import type {
  ExecutionFacts,
  Fact,
  Receipt,
  RelationCandidate,
  RelationRecord,
  Session,
  Snapshot,
  Strategy,
  WriteState,
} from "@/lib/types";

const fmt = (value: string | number) => String(value);
const when = (value: number) =>
  new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(value);

async function load<T>(path: string): Promise<T> {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) throw new Error(`请求失败 (${response.status})`);
  return response.json() as Promise<T>;
}

export function ControlConsole() {
  const [page, setPage] = useState(0);
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [facts, setFacts] = useState<ExecutionFacts>();
  const [relations, setRelations] = useState<RelationRecord[]>([]);
  const [candidates, setCandidates] = useState<RelationCandidate[]>([]);
  const [session, setSession] = useState<Session>();
  const [state, setState] = useState<WriteState>("loading");
  const [error, setError] = useState<string>();
  const [receipt, setReceipt] = useState<Receipt>();
  const [selected, setSelected] = useState<Strategy>();
  const [connectionRevision, setConnectionRevision] = useState(0);
  const pending = useRef<Record<string, string>>({});
  const realtimeReady = useRef(false);
  const reloadSequence = useRef(0);
  const reload = async (fromRealtime = false) => {
    const sequence = ++reloadSequence.current;
    try {
      const nextSession = await load<Session>("/api/session").catch(
        () => undefined,
      );
      if (!nextSession) {
        if (sequence !== reloadSequence.current) return;
        setSession(undefined);
        setState("readonly");
        return;
      }
      if (sequence !== reloadSequence.current) return;
      setSession(nextSession);
      const [nextSnapshot, nextFacts, nextRelations, nextCandidates] =
        await Promise.all([
          load<Snapshot>("/api/control/snapshot"),
          load<ExecutionFacts>("/api/control/execution-facts"),
          load<RelationRecord[]>("/api/control/relations"),
          load<RelationCandidate[]>("/api/control/relation-candidates"),
        ]);
      if (!freshProjection(nextSnapshot) || !freshProjection(nextFacts))
        throw new Error("快照版本或时效无效，写入已关闭");
      if (sequence !== reloadSequence.current) return;
      setSnapshot(nextSnapshot);
      setFacts(nextFacts);
      setRelations(nextRelations);
      setCandidates(nextCandidates);
      setSession(nextSession);
      setState(
        nextSession.writable
          ? fromRealtime &&
            realtimeReady.current &&
            navigator.onLine &&
            nextSession.expires_ms > Date.now()
            ? "ready"
            : "recovering"
          : "readonly",
      );
      setError(undefined);
    } catch (reason) {
      if (sequence !== reloadSequence.current) return;
      setState("recovering");
      setError(reason instanceof Error ? reason.message : "快照不可用");
    }
  };
  useEffect(() => {
    void reload();
  }, []);
  const activeStrategy = selected ? snapshot?.strategies.find(item =>
    item.venue === selected.venue && item.trading_account_id === selected.trading_account_id
      && item.instance_id === selected.instance_id) : snapshot?.strategies[0];
  useEffect(() => {
    if (!session || !activeStrategy) return;
    const scope = new URLSearchParams({
      venue: activeStrategy.venue.toLowerCase(),
      trading_account_id: activeStrategy.trading_account_id,
    });
    let stream: EventSource | undefined;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;
    let failures = 0;
    const reconnect = () => {
      if (stopped) return;
      realtimeReady.current = false;
      setState("recovering");
      stream?.close();
      stream = undefined;
      if (retry) return;
      retry = setTimeout(
        () => {
          retry = undefined;
          void reload();
          connect();
        },
        Math.min(10_000, 500 * 2 ** Math.min(failures++, 5)),
      );
    };
    const connect = () => {
      if (stopped) return;
      let cursor = 0;
      const currentStream = new EventSource(`/api/events?${scope}`);
      stream = currentStream;
      currentStream.addEventListener("control", (event) => {
        if (stopped || stream !== currentStream) return;
        const message = event as MessageEvent<string>;
        const id = Number(message.lastEventId);
        const value = safeControlEvent(message.data);
        if (
          !value ||
          !Number.isSafeInteger(id) ||
          id !== value.cursor ||
          value.previous_cursor !== cursor ||
          value.scope.trading_account_id !==
            activeStrategy.trading_account_id ||
          value.scope.venue !== activeStrategy.venue.toLowerCase()
        ) {
          reconnect();
          return;
        }
        cursor = id;
        failures = 0;
        realtimeReady.current = true;
        void reload(true);
      });
      currentStream.onerror = () => {
        if (!stopped && stream === currentStream) reconnect();
      };
    };
    connect();
    return () => {
      stopped = true;
      realtimeReady.current = false;
      reloadSequence.current += 1;
      if (retry) clearTimeout(retry);
      stream?.close();
    };
  }, [
    session?.writable,
    session?.subject,
    activeStrategy?.trading_account_id,
    activeStrategy?.venue,
    connectionRevision,
  ]);
  useEffect(() => {
    const offline = () => {
      realtimeReady.current = false;
      setState("recovering");
    };
    const online = () => {
      setConnectionRevision(value => value + 1);
      void reload();
    };
    window.addEventListener("offline", offline);
    window.addEventListener("online", online);
    return () => {
      window.removeEventListener("offline", offline);
      window.removeEventListener("online", online);
    };
  }, []);
  useEffect(() => {
    const timer = setInterval(() => {
      if (!session) return;
      if (session.expires_ms <= Date.now()) {
        realtimeReady.current = false;
        setSession(undefined);
        setState("readonly");
      } else if (
        !freshProjection(snapshot) ||
        !freshProjection(facts) ||
        !navigator.onLine
      ) {
        realtimeReady.current = false;
        setState("recovering");
      }
    }, 1_000);
    return () => clearInterval(timer);
  }, [session, snapshot, facts]);
  const writable =
    state === "ready" &&
    Boolean(session?.writable) &&
    Boolean(session && session.expires_ms > Date.now()) &&
    freshProjection(snapshot) &&
    freshProjection(facts);
  const totals = useMemo(
    () => ({
      drift: sumDecimals(snapshot?.copy_relations.map((item) => item.drift) ?? [], true),
    }),
    [snapshot],
  );
  const command = async (action: "pause" | "resume" | "stop" | "flatten") => {
    if (!activeStrategy || !session || !writable) return;
    const input = {
      venue: activeStrategy.venue,
      mode: "LIVE" as const,
      trading_account_id: activeStrategy.trading_account_id,
      instance_id: activeStrategy.instance_id,
      symbol: activeStrategy.symbol,
      action,
      expected_config_epoch: activeStrategy.config_epoch,
    };
    const confirmation =
      action === "stop" || action === "flatten"
        ? `${action.toUpperCase()} venue=${input.venue} mode=${input.mode} trading_account_id=${input.trading_account_id} symbol=${input.symbol} instance_id(${input.instance_id.length})=${input.instance_id} expected_config_epoch=${input.expected_config_epoch}`
        : undefined;
    if (
      confirmation &&
      !window.confirm(`确认提交以下不可逆 LIVE 控制命令：\n${confirmation}`)
    )
      return;
    const key = `${input.trading_account_id}:${action}:${input.instance_id}:${input.expected_config_epoch}`;
    const request_id = pending.current[key] ?? crypto.randomUUID();
    pending.current[key] = request_id;
    setState("recovering");
    try {
    const response = await fetch("/api/control/commands", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-venue-csrf": session.csrf,
      },
      body: JSON.stringify({ ...input, request_id, confirmation }),
    });
    const body = (await response.json()) as Receipt | { error: string };
    if (!response.ok || "error" in body) {
      setError("命令被拒绝，写入门已关闭。");
      return;
    }
    setReceipt(body);
    if (
      body.state === "applied" ||
      body.state === "rejected" ||
      body.state === "unknown"
    )
      delete pending.current[key];
    } catch {
      setError("命令响应中断，结果尚未确认；保留原请求编号，请先恢复连接核对回执。不会自动重发。");
    }
  };
  const logout = async () => {
    await fetch("/api/session", { method: "DELETE" });
    setSession(undefined);
    setState("readonly");
  };
  const login = async (token: string) => {
    try {
    const response = await fetch("/api/session", {
      method: "POST",
      headers: { "x-venue-bootstrap": token },
    });
    if (!response.ok) {
      setError("受控登录被拒绝，写入保持关闭。");
      return;
    }
    await reload(true);
    } catch {
      setError("登录连接失败，请重试；写入保持关闭。");
    }
  };
  const saveRelation = async (
    relation: RelationRecord["relation"],
    expected_revision?: number,
  ) => {
    if (!session || !writable) return;
    const revision = expected_revision ?? 0;
    const key =
      expected_revision === undefined
        ? `relation:create:${relation.follower.venue}:${relation.follower.trading_account_id}:${relation.follower.instance_id}:${relation.follower.symbol}`
        : `relation:update:${relation.relation_id}:${revision}`;
    const request_id = pending.current[key] ?? crypto.randomUUID();
    pending.current[key] = request_id;
    setState("recovering");
    try {
    const response = await fetch("/api/control/relations", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-venue-csrf": session.csrf,
      },
      body: JSON.stringify({
        schema_version: 2,
        request_id,
        expected_revision: revision,
        relation,
      }),
    });
    const receipt = (await response.json().catch(() => undefined)) as
      | { state?: string }
      | undefined;
    if (!response.ok)
      setError("关系更新被拒绝（可能是 revision 冲突），写入门已关闭。");
    // Retain the request id for this exact create binding or relation revision. A delayed second
    // click after the first response must remain a database-idempotent replay, not a new write.
    void receipt;
    await reload(true);
    } catch {
      setError("关系更新响应中断，结果尚未确认；请恢复连接后核对 revision，不会自动重发。");
    }
  };
  return (
    <AppShell
      active={page}
      onLogout={() => void logout()}
      onPage={setPage}
      state={state}
    >
      {error && (
        <div className="notice error" role="alert">
          <span>{error}</span>
          <div className="buttons">
            <button
              type="button"
              onClick={() => void reload(realtimeReady.current)}
            >
              重试连接
            </button>
          </div>
        </div>
      )}
      {!session && <Login onLogin={login} />}
      {!snapshot || !facts ? (
        <section className="loading">
          <span>
            {error
              ? "快照暂不可用；写入保持关闭，可重试连接。"
              : "正在获取无缓存快照…"}
          </span>
        </section>
      ) : (
        <>
          {page === 0 && (
            <Overview
              snapshot={snapshot}
              totals={totals}
              onSelect={setSelected}
            />
          )}
          {page === 1 && (
            <Relations
              candidates={candidates}
              records={relations}
              summary={snapshot.copy_relations}
              writable={writable}
              save={saveRelation}
            />
          )}
          {page === 2 && (
            <Accounts
              accounts={snapshot.accounts}
              strategies={snapshot.strategies}
            />
          )}
          {page === 3 && <Execution facts={facts} />}
          {page === 4 && (
            <Receipts snapshot={snapshot} receipt={receipt} facts={facts} />
          )}
          {page === 5 && (
            <Controls
              strategy={activeStrategy}
              writable={writable}
              command={command}
            />
          )}
        </>
      )}
    </AppShell>
  );
}

function Login({
  onLogin,
}: Readonly<{ onLogin: (token: string) => Promise<void> }>) {
  const [token, setToken] = useState("");
  return (
    <section className="notice login">
      <label htmlFor="bootstrap-token">
        受控登录令牌
        <input
          autoComplete="off"
          id="bootstrap-token"
          onChange={(event) => setToken(event.target.value)}
          type="password"
          value={token}
        />
      </label>
      <button
        disabled={!token}
        onClick={() => {
          const value = token;
          setToken("");
          void onLogin(value);
        }}
        type="button"
      >
        建立受控会话
      </button>
      <span>令牌仅随本次同源请求发送，不写入浏览器存储。</span>
    </section>
  );
}

function Overview({
  snapshot,
  totals,
  onSelect,
}: Readonly<{
  snapshot: Snapshot;
  totals: { drift: string };
  onSelect: (strategy: Strategy) => void;
}>) {
  const [view, setView] = useState<"follower" | "kol" | "ops">("ops");
  return (
    <section className="stack">
      <header>
        <p className="eyebrow">schema v2 · same-origin BFF</p>
        <h1>控制面总览</h1>
        <p>
          最后快照：{when(snapshot.generated_ms)}
          。控制回执与交易所签名事实明确分层。
        </p>
      </header>
      <div className="metrics">
        <Metric label="账户权益" value="—（不跨币种汇总）" />
        <Metric label="活跃关系" value={snapshot.copy_relations.length} />
        <Metric label="累计漂移" value={fmt(totals.drift)} />
      </div>
      <section className="panel">
        <h2>工作台视图</h2>
        <div className="buttons">
          <button
            aria-pressed={view === "follower"}
            onClick={() => setView("follower")}
            type="button"
          >
            Follower
          </button>
          <button
            aria-pressed={view === "kol"}
            onClick={() => setView("kol")}
            type="button"
          >
            KOL
          </button>
          <button
            aria-pressed={view === "ops"}
            onClick={() => setView("ops")}
            type="button"
          >
            OPS
          </button>
        </div>
        <p className="muted">
          视图只组织已授权 schema v2 事实；不改变会话角色或写入权限。
        </p>
        {view === "follower" && (
          <dl className="facts">
            <div>
              <dt>已分配资金</dt>
              <dd>
                {snapshot.accounts.flatMap((item) => (item.balances ?? []).map((balance) => `${balance.asset} ${balance.equity}`)).join(" · ") || "—"}
              </dd>
            </div>
            <div>
              <dt>跟单风险</dt>
              <dd>漂移 {fmt(totals.drift)}</dd>
            </div>
            <div>
              <dt>账户范围</dt>
              <dd>
                {snapshot.accounts
                  .map((item) => item.trading_account_id)
                  .join(" · ")}
              </dd>
            </div>
            <div>
              <dt>订单事实</dt>
              <dd>请到订单与持仓查看签名投影</dd>
            </div>
          </dl>
        )}
        {view === "kol" && (
          <dl className="facts">
            <div>
              <dt>Leader 策略</dt>
              <dd>
                {snapshot.strategies
                  .map((item) => item.instance_id)
                  .join(" · ")}
              </dd>
            </div>
            <div>
              <dt>下游关系</dt>
              <dd>
                {snapshot.copy_relations
                  .map((item) => item.follower_instance_id)
                  .join(" · ") || "—"}
              </dd>
            </div>
            <div>
              <dt>执行事实</dt>
              <dd>仅在订单与持仓呈现签名事实</dd>
            </div>
            <div>
              <dt>风险边界</dt>
              <dd>不因展示而取得写权限</dd>
            </div>
          </dl>
        )}
        {view === "ops" && (
          <p className="muted">
            OPS 使用控制、回执与风险页；任何 mutation
            均经受控会话、CSRF、Origin、账户范围、连续事件与明确确认门控。
          </p>
        )}
      </section>
      <section className="panel">
        <h2>策略状态</h2>
        <div className="list">
          {snapshot.strategies.map((item) => (
            <button
              className="row action-row"
              key={item.instance_id}
              onClick={() => onSelect(item)}
              type="button"
            >
              <strong>{item.instance_id}</strong>
              <span>
                {item.venue} · {item.symbol} · epoch {item.config_epoch}
              </span>
              <em>{item.lifecycle}</em>
            </button>
          ))}
        </div>
      </section>
      <section className="panel">
        <h2>网关健康</h2>
        {snapshot.accounts.map((item) => (
          <div className="row" key={item.trading_account_id}>
            <strong>{item.venue}</strong>
            <span>
              {item.health} · private generation {item.private_generation}
            </span>
            <em>LIVE</em>
          </div>
        ))}
      </section>
    </section>
  );
}
function Relations({
  candidates,
  records,
  summary,
  writable,
  save,
}: Readonly<{
  candidates: RelationCandidate[];
  records: RelationRecord[];
  summary: Snapshot["copy_relations"];
  writable: boolean;
  save: (
    relation: RelationRecord["relation"],
    expected_revision?: number,
  ) => Promise<void>;
}>) {
  return (
    <section className="stack">
      <header>
        <p className="eyebrow">Copy relation</p>
        <h1>跟单关系</h1>
        <p>
          仅可选择 Control 投影的策略绑定；每次写入带稳定
          request_id、revision、CSRF、Origin 与账户范围。
        </p>
      </header>
      <RelationForm
        candidates={candidates}
        disabled={!writable}
        onSave={save}
      />
      {records.length === 0 ? (
        <Empty />
      ) : (
        records.map((record) => {
          const status = summary.find(
            (item) => item.relation_id === record.relation.relation_id,
          );
          return (
            <section className="panel" key={record.relation.relation_id}>
              <div className="heading">
                <div>
                  <h2>{record.relation.follower.instance_id}</h2>
                  <p>
                    {record.relation.leader.instance_id} →{" "}
                    {record.relation.follower.symbol}
                  </p>
                </div>
                <b className="pill">revision {record.revision}</b>
              </div>
              <dl className="facts">
                <div>
                  <dt>目标 / 实际</dt>
                  <dd>
                    {status?.target_exposure ?? "—"} /{" "}
                    {status?.actual_exposure ?? "—"}
                  </dd>
                </div>
                <div>
                  <dt>漂移</dt>
                  <dd>{status?.drift ?? "—"}</dd>
                </div>
                <div>
                  <dt>最大总名义</dt>
                  <dd>{record.relation.risk.max_total_notional}</dd>
                </div>
                <div>
                  <dt>生命周期</dt>
                  <dd>{record.relation.lifecycle}</dd>
                </div>
              </dl>
              <details className="relation-editor">
                <summary>编辑 revision {record.revision}</summary>
                <RelationForm
                  candidates={candidates}
                  disabled={!writable}
                  initial={record}
                  onSave={save}
                />
              </details>
            </section>
          );
        })
      )}
    </section>
  );
}
function RelationForm({
  candidates,
  disabled,
  initial,
  onSave,
}: Readonly<{
  candidates: RelationCandidate[];
  disabled: boolean;
  initial?: RelationRecord;
  onSave: (
    relation: RelationRecord["relation"],
    expected_revision?: number,
  ) => Promise<void>;
}>) {
  // Keep the relation identity stable across duplicate clicks and network retries. The BFF and
  // PostgreSQL can then treat the second submission as the same durable request.
  const draftRelationId = useRef(crypto.randomUUID());
  const helpId = useId();
  const [leader, setLeader] = useState(0);
  const [follower, setFollower] = useState(
    Math.min(1, Math.max(candidates.length - 1, 0)),
  );
  const [capital, setCapital] = useState(
    initial?.relation.allocated_capital ?? "100.00",
  );
  const [multiplier, setMultiplier] = useState(
    initial?.relation.multiplier ?? "1.00",
  );
  const [reserve, setReserve] = useState(
    initial?.relation.safety_reserve_rate ?? "0.10",
  );
  const [total, setTotal] = useState(
    initial?.relation.risk.max_total_notional ?? "1000.00",
  );
  const [order, setOrder] = useState(
    initial?.relation.risk.max_order_notional ?? "100.00",
  );
  const [leverage, setLeverage] = useState(
    initial?.relation.risk.max_leverage ?? "3.00",
  );
  const [lifecycle, setLifecycle] = useState<"active" | "paused">(
    initial?.relation.lifecycle ?? "paused",
  );
  const choose = (index: number) => candidates[index]?.binding;
  const submit = () => {
    const nextLeader = initial?.relation.leader ?? choose(leader);
    const nextFollower = initial?.relation.follower ?? choose(follower);
    if (
      !nextLeader ||
      !nextFollower ||
      nextLeader.instance_id === nextFollower.instance_id
    )
      return;
    void onSave(
      {
        relation_id: initial?.relation.relation_id ?? draftRelationId.current,
        leader: nextLeader,
        follower: nextFollower,
        allocated_capital: capital,
        multiplier,
        safety_reserve_rate: reserve,
        risk: {
          max_total_notional: total,
          max_order_notional: order,
          max_leverage: leverage,
        },
        lifecycle,
      },
      initial?.revision,
    );
  };
  return (
    <form
      className="panel relation-form"
      onSubmit={(event) => {
        event.preventDefault();
        submit();
      }}
    >
      <h2>{initial ? "编辑关系" : "创建关系"}</h2>
      {!initial && (
        <>
          <label>
            带单策略（Leader）
            <select
              aria-label="Leader 策略"
              onChange={(event) => setLeader(Number(event.target.value))}
              value={leader}
            >
              {candidates.map((item, index) => (
                <option key={item.binding.instance_id} value={index}>
                  {item.binding.instance_id} · {item.binding.symbol}
                </option>
              ))}
            </select>
          </label>
          <label>
            跟单策略（Follower）
            <select
              aria-label="Follower 策略"
              onChange={(event) => setFollower(Number(event.target.value))}
              value={follower}
            >
              {candidates.map((item, index) => (
                <option key={item.binding.instance_id} value={index}>
                  {item.binding.instance_id} · {item.binding.symbol}
                </option>
              ))}
            </select>
          </label>
        </>
      )}
      <label>
        分配资金（报价币）
        <input
          aria-label="分配资金"
          aria-describedby={`${helpId}-capital`}
          inputMode="decimal"
          onChange={(event) => setCapital(event.target.value)}
          value={capital}
        />
        <small id={`${helpId}-capital`}>以跟单交易对的报价币计价，不代表账户余额。</small>
      </label>
      <label>
        跟单倍率
        <input
          aria-label="跟单倍率"
          aria-describedby={`${helpId}-multiplier`}
          inputMode="decimal"
          onChange={(event) => setMultiplier(event.target.value)}
          value={multiplier}
        />
        <small id={`${helpId}-multiplier`}>1.00 表示按分配资金同比例跟随。</small>
      </label>
      <label>
        保证金预留比例
        <input
          aria-label="保证金预留比例"
          aria-describedby={`${helpId}-reserve`}
          inputMode="decimal"
          onChange={(event) => setReserve(event.target.value)}
          value={reserve}
        />
        <small id={`${helpId}-reserve`}>填写小数，例如 0.10 表示预留 10%。</small>
      </label>
      <label>
        关系累计名义上限（报价币）
        <input
          aria-label="关系累计名义上限"
          aria-describedby={`${helpId}-total`}
          inputMode="decimal"
          onChange={(event) => setTotal(event.target.value)}
          value={total}
        />
        <small id={`${helpId}-total`}>还须满足账户级风险上限，不能在此放宽。</small>
      </label>
      <label>
        单笔名义上限（报价币）
        <input
          aria-label="单笔名义上限"
          aria-describedby={`${helpId}-order`}
          inputMode="decimal"
          onChange={(event) => setOrder(event.target.value)}
          value={order}
        />
        <small id={`${helpId}-order`}>限制每笔交易名义金额，不是保证金金额。</small>
      </label>
      <label>
        策略敞口倍率上限
        <input
          aria-label="策略敞口倍率上限"
          aria-describedby={`${helpId}-leverage`}
          inputMode="decimal"
          onChange={(event) => setLeverage(event.target.value)}
          value={leverage}
        />
        <small id={`${helpId}-leverage`}>只限制策略目标，不修改交易所账户杠杆。</small>
      </label>
      <label>
        生命周期
        <select
          onChange={(event) =>
            setLifecycle(event.target.value as "active" | "paused")
          }
          value={lifecycle}
        >
          <option value="active">运行（受风控约束）</option>
          <option value="paused">暂停（不生成新增目标）</option>
        </select>
      </label>
      <button disabled={disabled || candidates.length < 2} type="submit">
        {initial ? "保存 revision 编辑" : "创建受控关系"}
      </button>
    </form>
  );
}
function Accounts({
  accounts,
  strategies,
}: Readonly<{ accounts: Snapshot["accounts"]; strategies: Strategy[] }>) {
  return (
    <section className="stack">
      <header>
        <p className="eyebrow">account scope</p>
        <h1>交易账户</h1>
        <p>所有余额和金额显示为十进制字符串，不在浏览器进行交易数值计算。</p>
      </header>
      <section className="panel table">
        <table>
          <thead>
            <tr>
              <th>Venue</th>
              <th>账户</th>
              <th>权益</th>
              <th>可用保证金</th>
              <th>对账</th>
            </tr>
          </thead>
          <tbody>
            {accounts.map((item) => (
              <tr key={item.trading_account_id}>
                <td>{item.venue}</td>
                <td>{item.trading_account_id}</td>
                <td>{(item.balances ?? []).map((balance) => `${balance.asset} ${balance.equity}`).join(" · ") || "—"}</td>
                <td>{(item.balances ?? []).map((balance) => balance.available_margin === null ? `${balance.asset} —` : `${balance.asset} ${balance.available_margin}`).join(" · ") || "—"}</td>
                <td>{when(item.last_reconciled_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="panel">
        <h2>账户实例</h2>
        {strategies.map((item) => (
          <div className="row" key={item.instance_id}>
            <strong>{item.instance_id}</strong>
            <span>
              {item.symbol} · 长 {item.long_quantity} · 短 {item.short_quantity}
            </span>
            <em>
              已实现 {quoteAmount(item.symbol, item.realized_pnl)} · 未实现{" "}
              {quoteAmount(item.symbol, item.unrealized_pnl)} · {item.open_orders} orders
            </em>
          </div>
        ))}
      </section>
    </section>
  );
}
function Execution({ facts }: Readonly<{ facts: ExecutionFacts }>) {
  return (
    <section className="stack">
      <header>
        <p className="eyebrow">signed execution facts</p>
        <h1>订单、持仓与成交</h1>
        <p>
          仅显示账户节点已签名并上传的事实；控制回执和 execution
          阶段绝不作为成交推断。
        </p>
      </header>
      <FactTable
        title="签名订单"
        rows={facts.orders}
        fields={[
          "binding.venue",
          "binding.trading_account_id",
          "binding.symbol",
          "order_id",
          "state",
          "quantity",
          "filled_quantity",
          "observed_ms",
        ]}
      />
      <FactTable
        title="签名持仓"
        rows={facts.positions}
        fields={[
          "binding.venue",
          "binding.trading_account_id",
          "binding.symbol",
          "position_side",
          "quantity",
          "entry_price",
          "mark_price",
          "observed_ms",
        ]}
      />
      <FactTable
        title="签名成交"
        rows={facts.fills}
        fields={[
          "binding.venue",
          "binding.trading_account_id",
          "fill_id",
          "order_id",
          "quantity",
          "price",
          "occurred_ms",
        ]}
      />
      <FactTable
        title="签名对账"
        rows={facts.reconciliation}
        fields={[
          "binding.venue",
          "binding.symbol",
          "signed_generation",
          "complete_order_families",
          "complete_position_legs",
          "reconciled_ms",
        ]}
      />
      <FactTable
        title="执行阶段（非成交）"
        rows={facts.execution}
        fields={[
          "binding.venue",
          "binding.trading_account_id",
          "relation_id",
          "job_id",
          "state",
          "observed_ms",
        ]}
      />
    </section>
  );
}
function Receipts({
  snapshot,
  receipt,
  facts,
}: Readonly<{
  snapshot: Snapshot;
  receipt: Receipt | undefined;
  facts: ExecutionFacts;
}>) {
  return (
    <section className="stack">
      <header>
        <p className="eyebrow">durable control receipt</p>
        <h1>回执与风险</h1>
        <p>
          Accepted 或 Applied 控制回执不等于成交；Unknown 必须由签名事实收敛。
        </p>
      </header>
      {receipt && (
        <section className="notice">
          <strong>刚收到 {receipt.state}</strong>
          <span>{receipt.detail || receipt.receipt_id}</span>
        </section>
      )}
      <FactTable
        title="控制回执"
        rows={snapshot.ledger}
        fields={["occurred_ms", "instance_id", "action", "state", "detail"]}
      />
      <FactTable
        title="账户风险"
        rows={facts.risk}
        fields={[
          "venue",
          "trading_account_id",
          "absolute_position_notional",
          "open_entry_notional",
          "reserved_entry_notional",
          "max_total_notional",
          "accepts_new_risk",
          "observed_ms",
        ]}
      />
      <FactTable
        title="账户健康"
        rows={facts.health}
        fields={[
          "venue",
          "trading_account_id",
          "health",
          "private_generation",
          "last_reconciled_ms",
          "observed_ms",
        ]}
      />
      <FactTable
        title="跟单账本"
        rows={facts.copy_ledger}
        fields={[
          "binding.venue",
          "relation_id",
          "job_id",
          "ledger_sequence",
          "managed_exposure",
          "observed_ms",
        ]}
      />
      <FactTable
        title="跟单偏差"
        rows={facts.drift}
        fields={[
          "binding.venue",
          "relation_id",
          "target_exposure",
          "actual_exposure",
          "repair_pending",
          "observed_ms",
        ]}
      />
    </section>
  );
}
function FactTable({
  title,
  rows,
  fields,
}: Readonly<{ title: string; rows: object[]; fields: string[] }>) {
  const [pageIndex, setPageIndex] = useState(0);
  const pageCount = Math.max(1, Math.ceil(rows.length / 5));
  const currentPage = Math.min(pageIndex, pageCount - 1);
  const visibleRows = rows.slice(currentPage * 5, (currentPage + 1) * 5);
  return (
    <section className="panel table fact-table">
      <h2>{title}</h2>
      {rows.length === 0 ? (
        <p className="muted">当前无权威投影；页面保持空态，不推断交易事实。</p>
      ) : (
        <table>
          <thead>
            <tr>
              {fields.map((field) => (
                <th key={field}>{fieldLabels[field] ?? field}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, index) => (
              <tr key={String(read(row, "fact_digest") ?? index)}>
                {fields.map((field) => (
                  <td key={field} data-label={fieldLabels[field] ?? field}>{display(read(row, field), field)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {pageCount > 1 && (
        <nav className="buttons table-pages" aria-label={`${title}分页`}>
          <button type="button" disabled={currentPage === 0} onClick={() => setPageIndex(currentPage - 1)}>上一页</button>
          <span>{currentPage + 1} / {pageCount} · 共 {rows.length} 条</span>
          <button type="button" disabled={currentPage + 1 >= pageCount} onClick={() => setPageIndex(currentPage + 1)}>下一页</button>
        </nav>
      )}
    </section>
  );
}
function read(row: object, field: string): unknown {
  return field
    .split(".")
    .reduce<unknown>(
      (value, key) =>
        value && typeof value === "object" ? (value as Fact)[key] : undefined,
      row,
    );
}
const fieldLabels: Record<string, string> = {
  "binding.venue": "交易所", "binding.trading_account_id": "账户", "binding.symbol": "交易对",
  venue: "交易所", trading_account_id: "账户", order_id: "订单 ID", fill_id: "成交 ID",
  state: "状态", quantity: "数量", filled_quantity: "已成交数量", observed_ms: "观测时间",
  position_side: "持仓方向", entry_price: "入场价", mark_price: "标记价", price: "成交价",
  occurred_ms: "发生时间", relation_id: "关系 ID", job_id: "任务 ID", instance_id: "实例",
  action: "动作", detail: "说明", absolute_position_notional: "持仓名义价值",
  open_entry_notional: "增险挂单", reserved_entry_notional: "预留风险", max_total_notional: "名义上限",
  accepts_new_risk: "允许增险", health: "健康状态", private_generation: "私流代际",
  last_reconciled_ms: "最近对账", managed_exposure: "受管敞口", target_exposure: "目标敞口",
  actual_exposure: "实际敞口", repair_pending: "修复任务", signed_generation: "签名代际",
  complete_order_families: "订单族完整", complete_position_legs: "持仓腿完整", reconciled_ms: "对账时间",
  ledger_sequence: "账本序号",
};
const fieldValueLabels: Record<string, Record<string, string>> = {
  accepts_new_risk: { true: "允许增险", false: "禁止增险" },
  repair_pending: { true: "修复任务待处理", false: "暂无待处理修复任务" },
  health: {
    healthy: "健康",
    recovering: "恢复中",
    needs_attention: "需关注",
    stopped: "已停止",
    unknown: "未知",
    degraded: "降级",
  },
};
function quoteAmount(symbol: string, value: string | null | undefined): string {
  const [base, quote, ...rest] = symbol.split("/");
  return value === null || value === undefined || !base || !quote || rest.length > 0
    ? "—"
    : `${quote} ${value}`;
}
function display(value: unknown, field: string): string {
  if (field.endsWith("_ms") && typeof value === "number" && Number.isSafeInteger(value) && value > 0)
    return when(value);
  return value === undefined || value === null
    ? "—"
    : typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
      ? fieldValueLabels[field]?.[String(value)] ??
        (fieldValueLabels[field] ? "—" : String(value))
      : "—";
}
function Controls({
  strategy,
  writable,
  command,
}: Readonly<{
  strategy: Strategy | undefined;
  writable: boolean;
  command: (action: "pause" | "resume" | "stop" | "flatten") => Promise<void>;
}>) {
  if (!strategy) return <Empty />;
  return (
    <section className="stack">
      <header>
        <p className="eyebrow">semantic control</p>
        <h1>暂停、停止与平仓</h1>
        <p>操作对象精确绑定 venue、LIVE、账户、交易对、实例和 config epoch。</p>
      </header>
      <section className="panel danger">
        <dl className="facts">
          <div>
            <dt>venue / mode</dt>
            <dd>{strategy.venue} / LIVE</dd>
          </div>
          <div>
            <dt>account</dt>
            <dd>{strategy.trading_account_id}</dd>
          </div>
          <div>
            <dt>symbol / instance</dt>
            <dd>
              {strategy.symbol} / {strategy.instance_id}
            </dd>
          </div>
          <div>
            <dt>config epoch</dt>
            <dd>{strategy.config_epoch}</dd>
          </div>
        </dl>
        <p>
          Stop 仅停止新增意图并撤自有订单；Flatten
          还会请求将更新签名持仓降至零。两者均由服务端重建并校验确认文本。
        </p>
        <div className="buttons">
          <button
            disabled={!writable}
            onClick={() => void command("pause")}
            type="button"
          >
            暂停
          </button>
          <button
            disabled={!writable}
            onClick={() => void command("resume")}
            type="button"
          >
            恢复
          </button>
          <button
            className="danger-button"
            disabled={!writable}
            onClick={() => void command("stop")}
            type="button"
          >
            停止
          </button>
          <button
            className="danger-button"
            disabled={!writable}
            onClick={() => void command("flatten")}
            type="button"
          >
            平仓
          </button>
        </div>
        {!writable && (
          <p className="muted">
            写入门关闭：等待受控会话、最新快照和连续 SSE 事件。
          </p>
        )}
      </section>
    </section>
  );
}
function Metric({
  label,
  value,
}: Readonly<{ label: string; value: string | number }>) {
  return (
    <article className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}
function Empty() {
  return (
    <section className="panel empty">
      <strong>暂无可显示的投影</strong>
      <p>该状态保持只读，等待 Control 生成权威快照。</p>
    </section>
  );
}
function Unavailable({
  title,
  detail,
}: Readonly<{ title: string; detail: string }>) {
  return (
    <section className="panel empty">
      <strong>{title}</strong>
      <p>{detail}</p>
    </section>
  );
}
