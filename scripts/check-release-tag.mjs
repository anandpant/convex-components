import { readFile } from "node:fs/promises";

const packageJson = JSON.parse(
  await readFile(
    new URL("../packages/openrouter-observability/package.json", import.meta.url),
    "utf8",
  ),
);
const expectedTag = `openrouter-observability-v${packageJson.version}`;

if (process.env.RELEASE_TAG !== expectedTag) {
  throw new Error(
    `Release tag must be ${expectedTag}; received ${process.env.RELEASE_TAG ?? "none"}`,
  );
}
