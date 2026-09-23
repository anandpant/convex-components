import type { RecordValue } from "./types.js";
export const object = (value: unknown): RecordValue =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as RecordValue)
    : {};
export const list = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
export const string = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;
export const identity = (value: unknown) => {
  const text = string(value);
  return text && text.length <= 256 ? text : undefined;
};
export const count = (value: unknown) =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
export function nanoTime(value: string | undefined) {
  if (!value) return;
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/.exec(
    value,
  );
  if (!match) return;
  const seconds = Date.parse(`${match[1]}${match[3]}`);
  return Number.isFinite(seconds)
    ? String(BigInt(seconds) * 1_000_000n + BigInt((match[2] ?? "").padEnd(9, "0")))
    : undefined;
}
export function parseObject(raw: string): RecordValue {
  const parsed: unknown = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
    throw new Error("Object required");
  return parsed as RecordValue;
}

export function append(target: RecordValue, key: string, value: unknown) {
  if (typeof value === "string") target[key] = (string(target[key]) ?? "") + value;
}
