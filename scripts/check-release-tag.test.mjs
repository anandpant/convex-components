import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const script = fileURLToPath(new URL("./check-release-tag.mjs", import.meta.url));
const packageJson = JSON.parse(
  readFileSync(
    new URL("../packages/openrouter-observability/package.json", import.meta.url),
    "utf8",
  ),
);
const expectedTag = `openrouter-observability-v${packageJson.version}`;

function check(tag) {
  return spawnSync(process.execPath, [script], {
    encoding: "utf8",
    env: { ...process.env, RELEASE_TAG: tag },
  });
}

describe("release tag check", () => {
  it("accepts the package version tag", () => {
    assert.equal(check(expectedTag).status, 0);
  });

  it("rejects unrelated tags", () => {
    const result = check("v0.1.0");
    assert.notEqual(result.status, 0);
    assert.match(
      result.stderr,
      new RegExp(`Release tag must be ${expectedTag.replaceAll(".", "\\.")}`),
    );
  });
});
