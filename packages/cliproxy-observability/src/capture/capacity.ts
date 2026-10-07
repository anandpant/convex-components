import { v } from "convex/values";

export const capacityValidator = v.object({
  budgetBytes: v.number(),
  allocatedBytes: v.number(),
  reusableBytes: v.number(),
  remainingBytes: v.number(),
  pendingBytes: v.number(),
  acknowledgedEvents: v.number(),
  observedAt: v.string(),
  growthBytesPerSecond: v.optional(v.number()),
  estimatedSecondsToCeiling: v.optional(v.number()),
});

export function validCapacity(value: unknown): boolean {
  if (value === undefined) return true;
  if (value === null || typeof value !== "object") return false;
  const c = value as Record<string, unknown>;
  return (
    [
      c.budgetBytes,
      c.allocatedBytes,
      c.reusableBytes,
      c.remainingBytes,
      c.pendingBytes,
      c.acknowledgedEvents,
    ].every((n) => typeof n === "number" && Number.isSafeInteger(n) && n >= 0) &&
    typeof c.observedAt === "string" &&
    Number.isFinite(Date.parse(c.observedAt)) &&
    (c.remainingBytes as number) <= (c.budgetBytes as number) &&
    [c.growthBytesPerSecond, c.estimatedSecondsToCeiling].every(
      (n) => n === undefined || (typeof n === "number" && Number.isFinite(n) && n >= 0),
    ) &&
    Object.keys(c).every((key) => Object.hasOwn(capacityValidator.fields, key))
  );
}
