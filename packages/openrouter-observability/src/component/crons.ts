import { cronJobs } from "convex/server";
import { internal } from "./_generated/api.js";

const crons = cronJobs();

crons.daily(
  "delete expired OpenRouter observability data",
  { hourUTC: 4, minuteUTC: 7 },
  internal.retention.start,
);

export default crons;
