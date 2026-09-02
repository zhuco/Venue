import { existsSync, readFileSync, readdirSync } from "node:fs";
import { extname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const rules = [
  ["legacy Control endpoint", /\/v1(?:\/|["'`?])/],
  ["non-LIVE gateway mode", /\b(?:TESTNET|DEMO)\b/],
  ["exchange credential field", /\b(?:api[_-]?(?:key|secret|passphrase)|wallet[_-]?private[_-]?key|(?:BINANCE|BYBIT|BITGET|GATEIO|OKX)_API_(?:KEY|SECRET|PASSPHRASE)|HYPERLIQUID_API_WALLET_PRIVATE_KEY)\b/i],
  ["direct exchange API", /https?:\/\/(?:api|fapi|papi|dapi)\.(?:binance\.com|bybit\.com|bitget\.com|gateio\.ws|hyperliquid\.xyz)|https?:\/\/www\.okx\.com\/api\//i],
  ["URL credential", /[?&](?:access_token|token|api_key|secret)=/i],
];

export function boundaryViolations(source) {
  return rules.filter(([, pattern]) => pattern.test(source)).map(([label]) => label);
}

function filesBelow(root) {
  return readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const path = join(root, entry.name);
    if (entry.isSymbolicLink()) throw new Error("Web boundary scan refuses symbolic links");
    return entry.isDirectory() ? filesBelow(path) : [path];
  });
}

export function verifyBoundary(root) {
  const sourceFiles = ["app", "components", "lib", "public"]
    .map((name) => join(root, name))
    .filter(existsSync)
    .flatMap(filesBelow)
    .filter((path) => [".ts", ".tsx", ".js", ".mjs"].includes(extname(path)) && !path.endsWith(".test.ts"));
  const staticRoot = join(root, ".next", "static");
  if (!existsSync(staticRoot)) throw new Error("Build Web before checking its browser bundle");
  const bundleFiles = filesBelow(staticRoot).filter((path) => extname(path) === ".js");
  if (sourceFiles.length === 0 || bundleFiles.length === 0) throw new Error("Web boundary scan found no source or browser JavaScript");
  const violations = [...sourceFiles, ...bundleFiles].flatMap((path) =>
    boundaryViolations(readFileSync(path, "utf8")).map((reason) => `${relative(root, path)}: ${reason}`),
  );
  // Only filenames and rule names are printed; offending text may itself contain a secret.
  if (violations.length) throw new Error(violations.join("\n"));
  return { sourceFiles: sourceFiles.length, bundleFiles: bundleFiles.length };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = verifyBoundary(fileURLToPath(new URL("../", import.meta.url)));
  console.log(`Web boundary verified: ${result.sourceFiles} source files, ${result.bundleFiles} browser chunks`);
}
