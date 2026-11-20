import type { Infer } from "convex/values";
import {
  sha256Hex,
  TraceContentInvalidError,
  type TraceBlobReader,
  type TraceBlobRef,
  type TraceBlobStorage,
} from "./blobContent.js";
import { storedCacheWriteTokens, type ParsedOpenRouterSpan } from "./component/parser.js";
import type { slimSpan, spanPayloadRef } from "./component/spanValues.js";

export const DEFAULT_SPAN_PREFIX = "openrouter-observability/spans/v1";
const encoder = new TextEncoder();
const PAYLOAD_FIELDS = new Set([
  "input",
  "output",
  "attributes",
  "resourceAttributes",
  "eventsJson",
  "linksJson",
  "statusJson",
]);
type PayloadField =
  | "input"
  | "output"
  | "attributes"
  | "resourceAttributes"
  | "eventsJson"
  | "linksJson"
  | "statusJson";

export type SlimSpan = Infer<typeof slimSpan>;
export type SpanPayloadRef = Infer<typeof spanPayloadRef>;

function utf8Bytes(value: string | undefined) {
  return value === undefined ? undefined : encoder.encode(value).byteLength;
}

/** The archived object's blob reference: UTF-8 JSON at the row's recorded key and digest. */
export function spanPayloadBlobRef(payload: SpanPayloadRef): TraceBlobRef {
  return {
    kind: "trace_blob",
    key: payload.key,
    sha256: payload.sha256,
    byteLength: payload.byteLength,
    contentType: "application/json",
    encoding: "utf8",
  };
}

/**
 * Write a prepared span's full content to the host's storage and return the slim row that
 * points at it. The key ends in the content digest, so a redelivery never replaces bytes an
 * admitted row already verified.
 */
export async function archiveSpan(
  span: ParsedOpenRouterSpan,
  storage: TraceBlobStorage,
  prefix = DEFAULT_SPAN_PREFIX,
): Promise<SlimSpan> {
  const { input, output, attributes, resourceAttributes = [] } = span;
  const scalars = Object.fromEntries(
    Object.entries(span).filter(([field]) => !PAYLOAD_FIELDS.has(field)),
  ) as Omit<ParsedOpenRouterSpan, PayloadField>;
  const bytes = encoder.encode(JSON.stringify(span));
  const sha256 = await sha256Hex(bytes);
  const key = [prefix, span.traceId, span.spanId]
    .map((part, index) => (index === 0 ? part : encodeURIComponent(part)))
    .concat(`${sha256}.json`)
    .join("/");
  const payload = { key, sha256, byteLength: bytes.byteLength };
  await storage.put({ ...spanPayloadBlobRef(payload), bytes });
  return {
    ...scalars,
    inputUtf8Bytes: utf8Bytes(input),
    outputUtf8Bytes: utf8Bytes(output),
    attributeCount: attributes.length,
    resourceAttributeCount: resourceAttributes.length,
    cacheCreationInputTokens: storedCacheWriteTokens(attributes),
    payload,
  };
}

type StoredSpanRow = { payload?: SpanPayloadRef } & Record<string, unknown>;

/**
 * A stored span with its full content: the archived object merged under the row, or the row
 * itself when it still holds its content inline. Returns null when the object is missing.
 */
export async function loadFullSpan<Row extends StoredSpanRow>(
  reader: TraceBlobReader,
  row: Row,
  options: { maxBytes: number },
): Promise<(Row & ParsedOpenRouterSpan) | null> {
  if (row.payload === undefined) return row as Row & ParsedOpenRouterSpan;
  const reference = spanPayloadBlobRef(row.payload);
  if (reference.byteLength > options.maxBytes) {
    throw new TraceContentInvalidError("archived span exceeds the caller's byte limit");
  }
  const result = await reader.get(reference, options);
  if (result === null) return null;
  if (
    result.bytes.byteLength !== reference.byteLength ||
    (await sha256Hex(result.bytes)) !== reference.sha256
  ) {
    throw new TraceContentInvalidError("archived span does not match its reference");
  }
  const content = JSON.parse(
    new TextDecoder("utf-8", { fatal: true }).decode(result.bytes),
  ) as ParsedOpenRouterSpan;
  return { ...content, ...row };
}
