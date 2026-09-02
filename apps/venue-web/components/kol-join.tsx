"use client";

import { FormEvent, useState } from "react";

type Profile = { name: string; title: string; description: string; };

export function KolJoin({ inviteCode, profile }: { inviteCode: string; profile: Profile }) {
  const [message, setMessage] = useState<string>();
  const [busy, setBusy] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>, route: "register" | "login") {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const body = route === "register"
      ? { username: form.get("username"), password: form.get("password"), invite_code: inviteCode }
      : { username: form.get("username"), password: form.get("password") };
    setBusy(true); setMessage(undefined);
    try {
      const response = await fetch(`/api/kol/auth/${route}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), credentials: "same-origin" });
      if (!response.ok) { setMessage("无法完成请求。请检查输入，或稍后重试。"); return; }
      const session = await response.json() as { user?: { username?: string } };
      setMessage(`已安全登录为 ${session.user?.username ?? "用户"}。尚未绑定 API，交易未启用。`);
      event.currentTarget.reset();
    } catch { setMessage("服务暂时不可用，请稍后重试。"); }
    finally { setBusy(false); }
  }

  return <main style={{ maxWidth: 720, margin: "48px auto", padding: 24, fontFamily: "system-ui, sans-serif" }}>
    <p>VENUE · Binance KOL 跟单</p>
    <h1>{profile.name}</h1>
    <h2>{profile.title}</h2>
    {profile.description ? <p style={{ whiteSpace: "pre-wrap" }}>{profile.description}</p> : null}
    <section aria-label="risk disclosure" style={{ borderLeft: "4px solid #b45309", padding: "12px 16px", background: "#fffbeb", margin: "24px 0" }}>
      <strong>风险提示</strong><p>合约交易具有高风险。KOL 的内容由其本人提供，历史表现不代表未来结果。注册或登录不会自动启用跟单，也不会创建任何交易。</p>
    </section>
    <section>
      <h2>通过此邀请注册</h2>
      <form onSubmit={(event) => void submit(event, "register")}>
        <label>用户名<br /><input name="username" autoComplete="username" minLength={3} maxLength={64} required disabled={busy} /></label><br />
        <label>密码<br /><input name="password" type="password" autoComplete="new-password" minLength={8} maxLength={128} required disabled={busy} /></label><br />
        <button type="submit" disabled={busy}>{busy ? "正在提交…" : "注册并继续"}</button>
      </form>
    </section>
    <section style={{ marginTop: 32 }}>
      <h2>已有 VENUE 账号</h2>
      <p>登录不会改变你已有的 KOL 归属。</p>
      <form onSubmit={(event) => void submit(event, "login")}>
        <label>用户名<br /><input name="username" autoComplete="username" minLength={3} maxLength={64} required disabled={busy} /></label><br />
        <label>密码<br /><input name="password" type="password" autoComplete="current-password" minLength={8} maxLength={128} required disabled={busy} /></label><br />
        <button type="submit" disabled={busy}>{busy ? "正在提交…" : "登录"}</button>
      </form>
    </section>
    {message ? <p role="status" style={{ marginTop: 24 }}>{message}</p> : null}
  </main>;
}
