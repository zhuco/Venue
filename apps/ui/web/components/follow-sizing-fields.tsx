"use client";
import { useState } from "react";
import type { FollowSizing } from "@/lib/customer-types";

export function FollowSizingFields({ value }: { value?: FollowSizing }) {
  const [mode, setMode] = useState(value?.mode ?? "proportional");
  return <>
    <label>跟单方式<select name="sizingMode" value={mode} onChange={event => setMode(event.target.value as FollowSizing["mode"])}><option value="proportional">定比跟单</option><option value="fixed_notional">定额跟单</option></select></label>
    {mode === "fixed_notional" ? <label>单笔跟单合约金额（非保证金金额）<input name="fixedNotional" inputMode="decimal" defaultValue={value?.mode === "fixed_notional" ? value.notional : ""} required /><small>按源单价格换算；不足交易所数量或名义额限制时向上进位，不乘跟单倍数。</small></label> : <p className="muted">默认 1 倍；数量按账户权益、KOL 策略资本和跟单倍数计算。</p>}
  </>;
}

export function sizingFromForm(data: FormData): FollowSizing {
  return data.get("sizingMode") === "fixed_notional"
    ? { mode: "fixed_notional", notional: String(data.get("fixedNotional") ?? "").trim() }
    : { mode: "proportional" };
}

export function FollowAuthorizationFields() {
  const [mode, setMode] = useState<FollowSizing["mode"]>("proportional");
  return <>
    <label>跟单方式<select name="sizingMode" value={mode} onChange={event => setMode(event.target.value as FollowSizing["mode"])}><option value="proportional">按比例</option><option value="fixed_notional">按单笔金额</option></select></label>
    {mode === "fixed_notional"
      ? <label>每笔金额（报价币）<input name="fixedNotional" inputMode="decimal" required /><small>不足交易所最小数量或名义额时向上进位，并计入总额度。</small></label>
      : <label>比例倍数<input name="authorizationMultiplier" inputMode="decimal" defaultValue="1" required /></label>}
  </>;
}
FollowAuthorizationFields
export function authorizationFromForm(data: FormData) {
  const sizing = sizingFromForm(data);
  return { sizing, multiplier: sizing.mode === "fixed_notional" ? "1" : String(data.get("authorizationMultiplier") ?? "").trim() };
}
