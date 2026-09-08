/// <reference types="vite/client" />

import { makeFunctionReference } from "convex/server";
import { convexTest } from "convex-test";
import { describe, expect, it } from "vitest";
import exampleSchema from "../example/convex/schema.js";
import componentTest from "../src/test.js";

const exampleModules = import.meta.glob("../example/convex/**/*.ts");
const generatedModules = import.meta.glob("../src/component/_generated/*.ts");
const hostModules = {
  ...exampleModules,
  ...Object.fromEntries(
    Object.entries(generatedModules).map(([path, load]) => [
      path.replace("../src/component", "../example/convex"),
      load,
    ]),
  ),
};
const requestSpanSummaries = makeFunctionReference<"query">("traces:requestSpanSummaries");

describe("internal CLI query example", () => {
  it("keeps the component read behind an internal host function", async () => {
    const backend = convexTest(exampleSchema, hostModules);
    componentTest.register(backend);
    await expect(
      backend.query(requestSpanSummaries, { requestId: "request-123" }),
    ).resolves.toMatchObject({ status: "ready", page: [], done: true });
  });
});
