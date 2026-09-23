// First publications may be completed interactively before OIDC is configured.
// A rerun skips only an identical tarball already present at this exact version.
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const registry = pkg.publishConfig?.registry ?? "https://registry.npmjs.org/";
const url = new URL(`${encodeURIComponent(pkg.name)}/${encodeURIComponent(pkg.version)}`, registry);
const response = await fetch(url);
if (response.ok) {
  const published = await response.json();
  if (
    published.name !== pkg.name ||
    published.version !== pkg.version ||
    typeof published.dist?.integrity !== "string"
  )
    throw new Error("Registry returned invalid exact-version metadata");
  const dir = mkdtempSync(join(tmpdir(), "verify-published-"));
  try {
    const packed = JSON.parse(
      execFileSync("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", dir], {
        encoding: "utf8",
      }),
    );
    if (published.dist.integrity !== packed[0]?.integrity)
      throw new Error(
        "Existing registry version differs from the release source; refusing to skip publication",
      );
    console.log("Exact release tarball is already published; registry integrity matches.");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
} else if (response.status === 404) {
  execFileSync("npm", ["publish", "--provenance"], { stdio: "inherit" });
} else throw new Error(`Cannot verify registry publication state: HTTP ${response.status}`);
