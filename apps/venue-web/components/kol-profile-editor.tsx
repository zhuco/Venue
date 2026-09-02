"use client";

import { FormEvent, useEffect, useState } from "react";

type Profile = { name: string; title: string; description: string; revision: number; };

export function KolProfileEditor() {
  const [profile, setProfile] = useState<Profile>();
  const [csrf, setCsrf] = useState<string>();
  const [message, setMessage] = useState("正在载入 KOL 资料…");

  useEffect(() => { void load(); }, []);
  async function load() {
    const session = await fetch("/api/kol/auth/session", { credentials: "same-origin" });
    const overview = await session.json().catch(() => undefined) as { overview?: { user?: unknown }; csrf?: string } | undefined;
    if (!session.ok || !overview?.overview?.user || typeof overview.csrf !== "string" || overview.csrf.length < 16) { setMessage("请先通过邀请页登录。"); return; }
    const response = await fetch("/api/kol/profile", { credentials: "same-origin" });
    const value = await response.json().catch(() => undefined) as Profile | undefined;
    if (!response.ok || !value || typeof value.revision !== "number") { setMessage("当前账号不是启用的 KOL，或资料暂不可用。"); return; }
    setCsrf(overview.csrf); setProfile(value); setMessage("");
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); if (!profile) return;
    if (!csrf) { setMessage("请重新打开此页面以建立安全会话。"); return; }
    const form = new FormData(event.currentTarget);
    const request = { schema_version: 1, request_id: crypto.randomUUID(), name: form.get("name"), title: form.get("title"), description: form.get("description"), expected_revision: profile.revision };
    const response = await fetch("/api/kol/profile", { method: "POST", headers: { "content-type": "application/json", "x-venue-csrf": csrf }, body: JSON.stringify(request), credentials: "same-origin" });
    const value = await response.json().catch(() => undefined) as Profile | undefined;
    if (!response.ok || !value || typeof value.revision !== "number") { setMessage("保存失败：资料可能已被其他操作更新，请重新载入。"); return; }
    setProfile(value); setMessage("资料已更新。");
  }

  return <main style={{ maxWidth: 720, margin: "48px auto", padding: 24, fontFamily: "system-ui, sans-serif" }}>
    <h1>KOL 公开页面</h1><p>固定风险提示由平台维护，不能在此修改。</p>
    {!profile ? <p role="status">{message}</p> : <form onSubmit={(event) => void submit(event)}>
      <label>公开名称<br /><input name="name" defaultValue={profile.name} minLength={1} maxLength={40} required /></label><br />
      <label>页面标题<br /><input name="title" defaultValue={profile.title} minLength={1} maxLength={80} required /></label><br />
      <label>说明<br /><textarea name="description" defaultValue={profile.description} maxLength={2000} rows={8} /></label><br />
      <button type="submit">保存公开资料</button>{message ? <p role="status">{message}</p> : null}
    </form>}
  </main>;
}
