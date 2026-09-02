import { defineConfig, devices } from "@playwright/test";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

const qaDir = process.env.VENUE_WEB_QA_DIR ?? join(
  process.platform === "win32" ? "G:/Build/Venue" : tmpdir(),
  "venue-web-qa",
  `local-${process.pid}`,
);
if (!isAbsolute(qaDir)) throw new Error("VENUE_WEB_QA_DIR must be an absolute build-artifact path");
const screenshotDir = process.env.VENUE_WEB_SCREENSHOT_DIR ?? join(qaDir, "screenshots");
if (!isAbsolute(screenshotDir)) throw new Error("VENUE_WEB_SCREENSHOT_DIR must be an absolute build-artifact path");
process.env.VENUE_WEB_SCREENSHOT_DIR = screenshotDir;
const browserExecutable = process.env.VENUE_WEB_BROWSER_EXECUTABLE;

// Readiness checks run in Playwright's parent process too. A workstation proxy must not turn
// an unused loopback port into a successful proxy response or receive isolated QA requests.
process.env.NO_PROXY = [
  process.env.NO_PROXY ?? process.env.no_proxy ?? "",
  "127.0.0.1",
  "localhost",
].filter(Boolean).join(",");
process.env.no_proxy = process.env.NO_PROXY;

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  outputDir: qaDir,
  use: {
    baseURL: "http://localhost:3216",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    launchOptions: {
      ...(browserExecutable ? { executablePath: browserExecutable } : {}),
      args: ["--no-proxy-server"],
    },
  },
  webServer: [
    {
      command: "node e2e/control-fixture-server.mjs",
      url: "http://127.0.0.1:38080/v2/ui/snapshot",
      reuseExistingServer: false,
    },
    {
      command: "npm run start",
      url: "http://localhost:3216",
      reuseExistingServer: false,
      env: {
        PORT: "3216",
        HOSTNAME: "127.0.0.1",
        NO_PROXY: "127.0.0.1,localhost",
        no_proxy: "127.0.0.1,localhost",
        VENUE_CONTROL_ORIGIN: "http://127.0.0.1:38080",
        VENUE_WEB_SESSION_SIGNING_KEY: "e2e-signing-material",
        VENUE_WEB_SESSION_BOOTSTRAP_TOKEN: "e2e-bootstrap-token",
        VENUE_WEB_OPERATOR_ROLE: "operator",
        VENUE_WEB_OPERATOR_SUBJECT: "e2e-operator",
        VENUE_WEB_ACCOUNT_SCOPE: "00000000-0000-4000-8000-000000000001",
      },
    },
  ],
  projects: [
    {
      name: "desktop",
      use: {
        ...devices["Desktop Chrome"],
        browserName: "chromium",
        viewport: { width: 1440, height: 900 },
      },
    },
    {
      name: "mobile",
      use: {
        ...devices["iPhone 13"],
        browserName: "chromium",
        viewport: { width: 390, height: 844 },
      },
    },
    {
      name: "landscape",
      use: {
        ...devices["iPhone 13 landscape"],
        browserName: "chromium",
        viewport: { width: 844, height: 390 },
      },
    },
    {
      name: "tablet",
      use: {
        ...devices["iPad (gen 7)"],
        browserName: "chromium",
        viewport: { width: 768, height: 1024 },
      },
    },
    {
      name: "wide",
      use: {
        ...devices["Desktop Chrome"],
        browserName: "chromium",
        viewport: { width: 1920, height: 1080 },
      },
    },
  ],
});
