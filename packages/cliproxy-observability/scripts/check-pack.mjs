import { execFileSync } from "node:child_process";
import { copyFileSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
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
  "package/dist/capture/index.js",
  "package/dist/protocols/index.js",
  "package/dist/model-call/index.js",
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

const installDirectory = mkdtempSync(join(tmpdir(), "convex-cliproxy-pack-"));
try {
  copyFileSync(tarball, join(installDirectory, "package.tgz"));
  writeFileSync(
    join(installDirectory, "package.json"),
    JSON.stringify({
      private: true,
      type: "module",
      dependencies: {
        "@shpitdev/convex-cliproxy-observability": "file:./package.tgz",
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
    `import { defineApp } from "convex/server";
import config from "@shpitdev/convex-cliproxy-observability/convex.config";
import { CliproxyObservability, handleCliproxyCaptureRequest, projectPendingSegments, type CliproxyObservabilityComponent, type PrivateCaptureStorage } from "@shpitdev/convex-cliproxy-observability";
import { fromOpenRouterSpan } from "@shpitdev/convex-cliproxy-observability/model-call";
import { SSEReader } from "@shpitdev/convex-cliproxy-observability/protocols";
import componentTest from "@shpitdev/convex-cliproxy-observability/test";
import type { GenericActionCtx, GenericDataModel } from "convex/server";
const app = defineApp(); app.use(config);
declare const component: CliproxyObservabilityComponent;
declare const ctx: GenericActionCtx<GenericDataModel>;
declare const storage: PrivateCaptureStorage;
const client = new CliproxyObservability(component);
void client.pageRecentSummaries(ctx, {destinationId:"dev"});
void handleCliproxyCaptureRequest(ctx, new Request("https://example.test"), {client, storage, destinationId:"dev", deploymentId:"dev-deployment", environment:"dev", instanceIds:["host"], tokens:["token"], scheduleProjection:async()=>{}});
void projectPendingSegments(ctx, {client, storage, destinationId:"dev", callId:"call"});
void fromOpenRouterSpan({_id:"span",receivedAt:1});
void new SSEReader(); void componentTest;

`,
  );
  writeFileSync(
    join(installDirectory, "helper.test.ts"),
    `import { expect, test } from "vitest";
import helper from "@shpitdev/convex-cliproxy-observability/test";

test("loads executable component source modules", async () => {
  expect(Object.keys(helper.modules)).toContain("./component/queries.ts");
  expect(Object.keys(helper.modules).some((path) => path.endsWith(".d.ts"))).toBe(false);
  await expect(helper.modules["./component/queries.ts"]()).resolves.toBeDefined();
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
