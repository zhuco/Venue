import type { NextConfig } from "next";

const isDev = process.env.NODE_ENV === "development";

export const securityHeaders = [
  { key: "Content-Security-Policy", value: `default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; font-src 'self' data:; script-src 'self' 'unsafe-inline'${isDev ? " 'unsafe-eval'" : ""}; style-src 'self' 'unsafe-inline'` },
  { key: "X-Frame-Options", value: "DENY" },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "Referrer-Policy", value: "same-origin" },
  { key: "Permissions-Policy", value: "camera=(), geolocation=(), microphone=(), payment=(), usb=()" },
  { key: "Strict-Transport-Security", value: "max-age=31536000; includeSubDomains" },
];

const config: NextConfig = {
  output: "standalone",
  poweredByHeader: false,
  typedRoutes: true,
  async headers() { return [{ source: "/:path*", headers: securityHeaders }]; },
};

export default config;
