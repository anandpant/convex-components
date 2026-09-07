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
const byRequest = makeFunctionReference<"query">("traces:byRequest");

describe("host authorization example", () => {
  it("denies unauthenticated and non-owner reads, then allows the owner's component read", async () => {
    const backend = convexTest(exampleSchema, hostModules);
    componentTest.register(backend);
    const requestRecordId = await backend.run(async (ctx) =>
      ctx.db.insert("traceRequests", {
        ownerSubject: "owner",
        observabilityRequestId: "request-123",
      }),
    );
    await expect(backend.query(byRequest, { requestRecordId })).rejects.toThrow("Unauthorized");
    await expect(
      backend.withIdentity({ subject: "other" }).query(byRequest, { requestRecordId }),
    ).rejects.toThrow("Forbidden");
    await expect(
      backend.withIdentity({ subject: "owner" }).query(byRequest, { requestRecordId }),
    ).resolves.toEqual([]);
  });
});
