import { readFile, appendFile } from "node:fs/promises";
const names = ["openrouter-observability", "cliproxy-observability"];
const name = names.find((name) => process.env.RELEASE_TAG?.startsWith(`${name}-v`)) ?? names[0];
const packageJson = JSON.parse(
  await readFile(new URL(`../packages/${name}/package.json`, import.meta.url), "utf8"),
);
const expectedTag = `${name}-v${packageJson.version}`;
if (process.env.RELEASE_TAG !== expectedTag)
  throw new Error(
    `Release tag must be ${expectedTag}; received ${process.env.RELEASE_TAG ?? "none"}`,
  );
if (process.env.GITHUB_OUTPUT)
  await appendFile(
    process.env.GITHUB_OUTPUT,
    `package-directory=packages/${name}\npackage-name=${packageJson.name}\nversion=${packageJson.version}\n`,
  );
