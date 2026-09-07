import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  traceRequests: defineTable({
    ownerSubject: v.string(),
    observabilityRequestId: v.string(),
  }),
});
