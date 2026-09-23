import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { it } from "node:test";
import assert from "node:assert/strict";
const script = fileURLToPath(new URL("./publish-package.mjs", import.meta.url));
for (const mode of ["missing", "same", "different", "unavailable"])
  it(`publication ${mode} preserves the exact-version boundary`, () => {
    const dir = mkdtempSync(join(tmpdir(), "publish-test-"));
    try {
      writeFileSync(
        join(dir, "package.json"),
        JSON.stringify({ name: "@shpitdev/test-release", version: "0.1.0" }),
      );
      writeFileSync(
        join(dir, "npm"),
        `#!/usr/bin/env node\nconst fs=require("node:fs");const action=process.argv[2];if(action==="view"){if(process.env.TEST_MODE==="missing"){process.stderr.write("E404");process.exit(1)}if(process.env.TEST_MODE==="unavailable"){process.stderr.write("E503");process.exit(1)}process.stdout.write(JSON.stringify(process.env.TEST_MODE==="same"?"sha512-exact":"sha512-different"))}else if(action==="pack")process.stdout.write(JSON.stringify([{integrity:"sha512-exact"}]));else if(action==="publish")fs.writeFileSync("published",JSON.stringify(process.argv.slice(2)));`,
        { mode: 0o755 },
      );
      const result = spawnSync(process.execPath, [script], {
        cwd: dir,
        env: { ...process.env, PATH: dir + ":" + process.env.PATH, TEST_MODE: mode },
        encoding: "utf8",
      });
      assert.equal(result.status, mode === "missing" || mode === "same" ? 0 : 1);
      assert.equal(existsSync(join(dir, "published")), mode === "missing");
      if (mode === "missing")
        assert.deepEqual(JSON.parse(readFileSync(join(dir, "published"), "utf8")), [
          "publish",
          "--provenance",
        ]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
