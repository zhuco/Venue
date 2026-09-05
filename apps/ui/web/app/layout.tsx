import type { Metadata } from "next";
import "./globals.css";
import "./adjustments.css";
import "./customer.css";

export const metadata: Metadata = {
  title: "Venue · 跟单与带单机器人",
  description: "Binance KOL 挂单同步与账户管理",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="zh-CN"><body>{children}</body></html>;
}
