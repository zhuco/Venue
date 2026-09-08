"use client";
import { useState } from "react";
export function CopyIp({ value }: { value: string }) {
  const [message, setMessage] = useState("");
  return <div className="guide-copy"><code>{value}</code><button type="button" onClick={async () => { try { await navigator.clipboard.writeText(value); setMessage("IP 已复制"); } catch { setMessage("复制未完成，请选中左侧 IP 手动复制"); } }}>复制 IP</button><span role="status">{message}</span></div>;
}
