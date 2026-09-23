// First publications may be completed interactively before OIDC is configured.
// A rerun skips only an identical tarball already present at this exact version.
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const result = spawnSync(
  "npm",
  ["view", `${pkg.name}@${pkg.version}`, "dist.integrity", "--json"],
  { encoding: "utf8" },
);
if (result.status === 0) {
  const dir = mkdtempSync(join(tmpdir(), "verify-published-"));
  try {
    const packed = JSON.parse(
      execFileSync("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", dir], {
        encoding: "utf8",
      }),
    );
    if (JSON.parse(result.stdout) !== packed[0]?.integrity)
      throw new Error(
        "Existing registry version differs from the release source; refusing to skip publication",
      );
    console.log("Exact release tarball is already published; registry integrity matches.");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
} else if (result.stderr.includes("E404")) {
  execFileSync("npm", ["publish", "--provenance"], { stdio: "inherit" });
} else throw new Error("Cannot verify registry publication state");
