import { execFileSync } from "node:child_process";
import {
  copyFileSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packDirectory = join(packageRoot, ".pack");
const tarballs = readdirSync(packDirectory).filter((file) => file.endsWith(".tgz"));
if (tarballs.length !== 1)
  throw new Error(`Expected one packed artifact, found ${tarballs.length}`);

const tarball = join(packDirectory, tarballs[0]);
const entries = execFileSync("tar", ["-tzf", tarball], { encoding: "utf8" });
for (const required of [
  "package/LICENSE",
  "package/CHANGELOG.md",
  "package/dist/client.js",
  "package/dist/client.d.ts",
  "package/dist/content.js",
  "package/dist/component/convex.config.js",
  "package/dist/component/_generated/component.d.ts",
  "package/src/test.ts",
  "package/src/component/schema.ts",
]) {
  if (!entries.includes(required)) throw new Error(`Packed artifact is missing ${required}`);
}
const license = execFileSync("tar", ["-xOzf", tarball, "package/LICENSE"], {
  encoding: "utf8",
});
if (!license.includes("Copyright 2026 Anand Pant")) {
  throw new Error("Packed artifact is missing the package copyright notice");
}
if (entries.includes(".test."))
  throw new Error("Packed artifact contains test implementation files");

const installDirectory = mkdtempSync(join(tmpdir(), "convex-openrouter-pack-"));
try {
  copyFileSync(tarball, join(installDirectory, "package.tgz"));
  writeFileSync(
    join(installDirectory, "package.json"),
    JSON.stringify({
      private: true,
      type: "module",
      dependencies: {
        "@shpitdev/convex-openrouter-observability": "file:./package.tgz",
        convex: "1.42.2",
      },
      devDependencies: {
        "convex-test": "0.0.54",
        typescript: "npm:@typescript/typescript6@^6.0.2",
        vite: "^8.2.2",
        vitest: "^4.1.11",
      },
    }),
  );
  writeFileSync(
    join(installDirectory, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        module: "ESNext",
        moduleResolution: "Bundler",
        noEmit: true,
        strict: true,
        target: "ESNext",
      },
      include: ["usage.ts"],
    }),
  );
  writeFileSync(
    join(installDirectory, "usage.ts"),
    `${readFileSync(join(packageRoot, "example/convex/convex.config.ts"), "utf8")}
import { decodeOpenRouterInput, OpenRouterObservability, type OpenRouterObservabilityComponent } from "@shpitdev/convex-openrouter-observability";
import componentTest from "@shpitdev/convex-openrouter-observability/test";
import type { GenericDataModel, GenericQueryCtx } from "convex/server";
declare const component: OpenRouterObservabilityComponent;
declare const ctx: Pick<GenericQueryCtx<GenericDataModel>, "runQuery">;
const observability = new OpenRouterObservability(component);
void observability.pageTraceSummaries(ctx, { traceId: "trace", limit: 6 });
void observability.pageCorrelationSummaries(ctx, { correlation: { kind: "user", userId: "user" } });
void decodeOpenRouterInput('{"messages":[]}');
void componentTest;
`,
  );
  writeFileSync(
    join(installDirectory, "helper.test.ts"),
    `import { expect, test } from "vitest";
import helper from "@shpitdev/convex-openrouter-observability/test";

test("loads executable component source modules", async () => {
  expect(Object.keys(helper.modules)).toContain("./component/queries.ts");
  expect(Object.keys(helper.modules).some((path) => path.endsWith(".d.ts"))).toBe(false);
  await expect(helper.modules["./component/crons.ts"]()).resolves.toBeDefined();
});
`,
  );
  execFileSync("pnpm", ["install", "--ignore-scripts", "--frozen-lockfile=false"], {
    cwd: installDirectory,
    stdio: "pipe",
  });
  execFileSync("pnpm", ["exec", "tsc6", "--noEmit"], {
    cwd: installDirectory,
    stdio: "pipe",
  });
  execFileSync("pnpm", ["exec", "vitest", "run"], {
    cwd: installDirectory,
    stdio: "pipe",
  });
} finally {
  rmSync(installDirectory, { recursive: true, force: true });
  rmSync(packDirectory, { recursive: true, force: true });
}

console.log("Packed artifact installs and typechecks without workspace source imports.");
