import { defineComponent } from "convex/server";
import { v } from "convex/values";

export default defineComponent("openrouterObservability", {
  env: {
    WEBHOOK_TOKEN: v.optional(v.string()),
    RETENTION_DAYS: v.optional(v.string()),
  },
});
