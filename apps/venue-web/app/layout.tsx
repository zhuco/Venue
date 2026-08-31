import type { Metadata } from "next";
import "./globals.css";
import "./adjustments.css";

export const metadata: Metadata = {
  title: "Venue Control",
  description: "Controlled Venue operations surface",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="zh-CN"><body>{children}</body></html>;
}
