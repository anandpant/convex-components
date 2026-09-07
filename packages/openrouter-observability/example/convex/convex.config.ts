import openrouterObservability from "@shpitdev/convex-openrouter-observability/convex.config.js";
import { defineApp } from "convex/server";
import { v } from "convex/values";

const app = defineApp({
  env: { OPENROUTER_OBSERVABILITY_TOKEN: v.optional(v.string()) },
});

app.use(openrouterObservability, {
  name: "openrouterObservability",
  httpPrefix: "/openrouter/",
  env: {
    WEBHOOK_TOKEN: app.env.OPENROUTER_OBSERVABILITY_TOKEN,
    RETENTION_DAYS: "30",
  },
});

export default app;
