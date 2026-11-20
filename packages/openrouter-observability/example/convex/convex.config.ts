import openrouterObservability from "@shpitdev/convex-openrouter-observability/convex.config.js";
import { defineApp } from "convex/server";
const app = defineApp();

app.use(openrouterObservability, { name: "openrouterObservability" });

export default app;
