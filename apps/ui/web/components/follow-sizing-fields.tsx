"use client";
import { useState } from "react";
import type { FollowSizing } from "@/lib/customer-types";

export function FollowSizingFields({ value, multiplier = "1", multiplierName = "multiplier", equity }: { value?: FollowSizing; multiplier?: string; multiplierName?: string; equity?: string | null }) {
  const [mode, setMode] = useState(value?.mode ?? "proportional");
  return <div className="follow-sizing customer-wide">
    <div className="follow-mode" role="group" aria-label="跟单方式">
      <button type="button" aria-pressed={mode === "proportional"} onClick={() => setMode("proportional")}>定比跟单</button>
      <button type="button" aria-pressed={mode === "fixed_notional"} onClick={() => setMode("fixed_notional")}>定额跟单</button>
    </div>
    <input type="hidden" name="sizingMode" value={mode} />
    <p className="muted">{mode === "proportional" ? "按账户全部权益与带单策略资金的比例跟单，默认 1 倍。" : "每笔开仓使用相同的名义金额，不乘跟单倍数。名义金额是订单价值，不是保证金。"}</p>
    {mode === "fixed_notional" ? <><label>每笔跟单名义金额（报价币）<input key="fixed" name="fixedNotional" inputMode="decimal" defaultValue={value?.mode === "fixed_notional" ? value.notional : ""} required /><small>USDT 合约按 USDT 填写，USDC 合约按 USDC 填写；低于交易所最低名义额时会补足。</small></label><input type="hidden" name={multiplierName} value="1" /></> : <label>跟单倍数<input key="proportional" name={multiplierName} inputMode="decimal" defaultValue={multiplier} required /><small>1 倍表示使用相同的资金比例，2 倍表示该比例的两倍。</small></label>}
    <p className="muted">跟单资金默认使用验证时的账户全部权益{hasFollowEquity(equity) ? `（${equity} USD）` : ""}，无需填写总跟单金额。此为验证快照；定比数量仍按此快照计算。软件不设置单笔或总名义金额上限，能否开仓由币安账户保证金及交易规则决定。</p>
  </div>;
}

export function sizingFromForm(data: FormData): FollowSizing {
  return data.get("sizingMode") === "fixed_notional"
    ? { mode: "fixed_notional", notional: String(data.get("fixedNotional") ?? "").trim() }
    : { mode: "proportional" };
}

export function FollowAuthorizationFields() {
  return <FollowSizingFields multiplierName="authorizationMultiplier" />;
}

export function authorizationFromForm(data: FormData) {
  const sizing = sizingFromForm(data);
  return { sizing, multiplier: sizing.mode === "fixed_notional" ? "1" : String(data.get("authorizationMultiplier") ?? "").trim() };
}

export function hasFollowEquity(value: string | null | undefined): value is string {
  return typeof value === "string" && value.length <= 128 && /^\d+(?:\.\d+)?$/.test(value) && /[1-9]/.test(value);
}
