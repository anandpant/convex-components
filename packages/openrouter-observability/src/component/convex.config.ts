import { defineComponent } from "convex/server";
import { v } from "convex/values";

export default defineComponent("openrouterObservability", {
  env: {
    RETENTION_DAYS: v.optional(v.string()),
  },
});
