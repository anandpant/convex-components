import openrouterObservability from "@shpitdev/convex-openrouter-observability/convex.config.js";
import { defineApp } from "convex/server";
const app = defineApp();

app.use(openrouterObservability, {
  name: "openrouterObservability",
  env: {
    RETENTION_DAYS: "30",
  },
});

export default app;
