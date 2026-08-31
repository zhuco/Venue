"use client";

import { useEffect, useRef, useState } from "react";

const pages = ["总览", "跟单关系", "账户", "订单与持仓", "回执与风险", "控制"];

export function AppShell({ active, children, state, onLogout, onPage }: Readonly<{ active: number; children: React.ReactNode; state: string; onLogout: () => void; onPage: (index: number) => void; }>) {
  const [open, setOpen] = useState(false); const button = useRef<HTMLButtonElement>(null); const nav = useRef<HTMLElement>(null);
  useEffect(() => { if (!open) return; const prior = document.body.style.overflow; document.body.style.overflow = "hidden"; nav.current?.querySelector<HTMLButtonElement>("button")?.focus(); return () => { document.body.style.overflow = prior; button.current?.focus(); }; }, [open]);
  useEffect(() => { if (!open) return; const close = (event: KeyboardEvent) => { if (event.key === "Escape") setOpen(false); }; window.addEventListener("keydown", close); return () => window.removeEventListener("keydown", close); }, [open]);
  const connection = state === "ready" ? "已同步 · 可受控写入" : state === "readonly" ? "只读 · 未获得受控会话" : "恢复中 · 写入已关闭";
  return <div className="shell"><a className="skip" href="#main">跳到主要内容</a><aside aria-label="主导航" className={open ? "side open" : "side"} id="main-nav" ref={nav}><div className="brand"><b>V</b><span>Venue Control</span></div><p>受控实盘操作面</p><nav>{pages.map((page, index) => <button aria-current={active === index ? "page" : undefined} className={active === index ? "selected" : ""} key={page} onClick={() => { onPage(index); setOpen(false); }} type="button">{page}</button>)}<button className="logout" onClick={() => { onLogout(); setOpen(false); }} type="button">退出受控会话</button></nav><small>浏览器不保存账户凭证，也不直接访问 Control。</small></aside><button aria-controls="main-nav" aria-expanded={open} aria-label="切换导航" className="menu" onClick={() => setOpen(!open)} ref={button} type="button"><i /><i /></button>{open && <button aria-label="关闭导航" className="scrim" onClick={() => setOpen(false)} type="button" />}<main id="main" tabIndex={-1}><div className={`connection ${state}`}><strong>{connection}</strong>{state === "ready" && <span>Control 写入就绪，不代表允许开仓。</span>}</div>{children}</main></div>;
}
