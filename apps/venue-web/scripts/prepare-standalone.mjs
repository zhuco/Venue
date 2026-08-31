import { cpSync, existsSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const standalone = join(root, ".next", "standalone");
if (!existsSync(join(standalone, "server.js"))) {
  throw new Error("Next standalone server was not built");
}
// Only public build assets join the traced server; no environment file is packaged.
cpSync(join(root, ".next", "static"), join(standalone, ".next", "static"), {
  recursive: true,
});
if (existsSync(join(root, "public"))) {
  cpSync(join(root, "public"), join(standalone, "public"), { recursive: true });
}
