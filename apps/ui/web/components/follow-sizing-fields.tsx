"use client";
import { useState } from "react";
import type { FollowSizing } from "@/lib/customer-types";

export function FollowSizingFields({ value }: { value?: FollowSizing }) {
  const [mode, setMode] = useState(value?.mode ?? "proportional");
  return <>
    <label>跟单方式<select name="sizingMode" value={mode} onChange={event => setMode(event.target.value as FollowSizing["mode"])}><option value="proportional">定比跟单</option><option value="fixed_notional">定额跟单</option></select></label>
    {mode === "fixed_notional" ? <label>每笔跟单名义金额（报价币）<input name="fixedNotional" inputMode="decimal" defaultValue={value?.mode === "fixed_notional" ? value.notional : ""} required /><small>按源单价格换算数量并向下取整，实际名义金额可能略低；不乘跟单倍数。</small></label> : <p className="muted">数量按分配资金 / KOL 策略资本 × 跟单倍数计算。</p>}
  </>;
}

export function sizingFromForm(data: FormData): FollowSizing {
  return data.get("sizingMode") === "fixed_notional"
    ? { mode: "fixed_notional", notional: String(data.get("fixedNotional") ?? "").trim() }
    : { mode: "proportional" };
}
