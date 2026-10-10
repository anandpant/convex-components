import { v } from "convex/values";

/** Local durable ACK counts. They do not claim remote persistence or projection. */
export const contentHealthValidator = v.object({
  observedBytes: v.number(),
  committedBytes: v.number(),
  lostBytes: v.number(),
  unconfirmedBytes: v.number(),
  unknownLosses: v.number(),
  streamCallbacks: v.number(),
  incompleteCalls: v.number(),
  capacityRejections: v.number(),
  retainedChargedBytes: v.number(),
  peakChargedBytes: v.number(),
});

export function validContentHealth(value: unknown): boolean {
  if (value === undefined) return true;
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const counters = value as Record<string, unknown>;
  return (
    Object.keys(counters).length === Object.keys(contentHealthValidator.fields).length &&
    Object.keys(contentHealthValidator.fields).every((key) => {
      const n = counters[key];
      return typeof n === "number" && Number.isSafeInteger(n) && n >= 0;
    }) &&
    (counters.retainedChargedBytes as number) <= (counters.peakChargedBytes as number)
  );
}
