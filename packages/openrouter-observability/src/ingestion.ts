import type { GenericActionCtx, GenericDataModel } from "convex/server";
import type { ComponentApi } from "./component/_generated/component.js";
import {
  externalizeTraceValue,
  exactJsonDocumentWhenNeeded,
  parseJsonPreservingUnsafeNumbers,
  resolveTraceBlobLimits,
  sha256Hex,
  TraceContentBoundExceededError,
  TraceContentInvalidError,
  type RemoteTraceBlobMapper,
  type TraceBlobLimits,
  type TraceBlobStorage,
} from "./blobContent.js";
import {
  InvalidOtlpDeliveryError,
  OtlpBoundExceededError,
  parseOpenRouterOtlpDelivery,
  storedOpenRouterSpanSize,
} from "./component/parser.js";

type ActionContext = Pick<GenericActionCtx<GenericDataModel>, "runMutation">;
const BEARER_PREFIX = "Bearer ";
const encoder = new TextEncoder();

export type OpenRouterTraceHandlerOptions = {
  component: ComponentApi;
  bearerToken: string | undefined;
  blobStorage: TraceBlobStorage;
  blobPrefix?: string;
  limits?: TraceBlobLimits;
  mapRemoteUrl?: RemoteTraceBlobMapper;
};

async function constantTimeEqual(left: string, right: string) {
  const [leftDigest, rightDigest] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(left)),
    crypto.subtle.digest("SHA-256", encoder.encode(right)),
  ]);
  const leftBytes = new Uint8Array(leftDigest);
  const rightBytes = new Uint8Array(rightDigest);
  let difference = 0;
  for (let index = 0; index < leftBytes.length; index += 1) {
    difference |= leftBytes[index] ^ rightBytes[index];
  }
  return difference === 0;
}

async function authorized(request: Request, token: string | undefined) {
  const authorization = request.headers.get("authorization");
  if (!token || !authorization?.startsWith(BEARER_PREFIX)) return false;
  const supplied = authorization.slice(BEARER_PREFIX.length);
  return supplied.length > 0 && (await constantTimeEqual(supplied, token));
}

function isApplicationJson(request: Request) {
  return (
    request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() ===
    "application/json"
  );
}

function isTestConnection(request: Request) {
  return request.headers.get("x-test-connection")?.trim().toLowerCase() === "true";
}

function declaredBodySize(request: Request) {
  const value = request.headers.get("content-length");
  if (value === null || !/^\d+$/.test(value)) return undefined;
  const size = Number(value);
  return Number.isSafeInteger(size) ? size : undefined;
}

export async function readBoundedTraceBody(request: Request, maximumBytes: number) {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1) {
    throw new Error("maximumBytes must be a positive integer");
  }
  if ((declaredBodySize(request) ?? 0) > maximumBytes) {
    await request.body?.cancel("Payload too large");
    return { kind: "too_large" } as const;
  }
  const reader = request.body?.getReader();
  if (!reader) return { kind: "body", bytes: new Uint8Array() } as const;
  const chunks: Uint8Array[] = [];
  let byteLength = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      byteLength += value.byteLength;
      if (byteLength > maximumBytes) {
        await reader.cancel("Payload too large");
        return { kind: "too_large" } as const;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { kind: "body", bytes } as const;
}

function emptyEnvelope(value: unknown) {
  return (
    typeof value === "object" &&
    value !== null &&
    Array.isArray((value as Record<string, unknown>).resourceSpans) &&
    ((value as Record<string, unknown>).resourceSpans as unknown[]).length === 0
  );
}

async function prepareDelivery(bytes: Uint8Array, options: OpenRouterTraceHandlerOptions) {
  let delivery: unknown;
  try {
    delivery = parseJsonPreservingUnsafeNumbers(
      new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    );
  } catch {
    throw new TraceContentInvalidError("Invalid JSON");
  }
  const deliveryDigest = await sha256Hex(bytes);
  const parsedSpans = parseOpenRouterOtlpDelivery(delivery, { enforceStoredSpanBound: false });
  const content = parsedSpans.map((span) => ({
    input: contentString(span.input),
    output: contentString(span.output),
    attributes: span.attributes.map((attribute) =>
      parseJsonPreservingUnsafeNumbers(attribute.valueJson),
    ),
    resourceAttributes: (span.resourceAttributes ?? []).map((attribute) =>
      parseJsonPreservingUnsafeNumbers(attribute.valueJson),
    ),
    events: contentString(span.eventsJson),
    links: contentString(span.linksJson),
    status: contentString(span.statusJson),
  }));
  const externalized = await externalizeTraceValue(content, {
    deliveryDigest,
    prefix: options.blobPrefix,
    limits: options.limits,
    mapRemoteUrl: options.mapRemoteUrl,
  });
  if (!Array.isArray(externalized.value)) {
    throw new TraceContentInvalidError("prepared trace content is invalid");
  }
  const externalizedValues = externalized.value;
  const spans = parsedSpans.map((span, index) => {
    const prepared = externalizedValues[index];
    if (!isPreparedContent(prepared)) {
      throw new TraceContentInvalidError("prepared trace content is invalid");
    }
    return {
      ...span,
      input: restoreContentString(prepared.input),
      output: restoreContentString(prepared.output),
      attributes: span.attributes.map((attribute, attributeIndex) => ({
        key: attribute.key,
        valueJson: JSON.stringify(prepared.attributes[attributeIndex]),
      })),
      resourceAttributes: (span.resourceAttributes ?? []).map((attribute, attributeIndex) => ({
        key: attribute.key,
        valueJson: JSON.stringify(prepared.resourceAttributes[attributeIndex]),
      })),
      eventsJson: restoreContentString(prepared.events),
      linksJson: restoreContentString(prepared.links),
      statusJson: restoreContentString(prepared.status),
    };
  });
  for (const span of spans) {
    if (storedOpenRouterSpanSize(span) > 900 * 1024) {
      throw new OtlpBoundExceededError("stored span exceeds the limit of 921600 bytes");
    }
  }
  return { delivery, spans, planned: externalized.planned };
}

function contentString(raw: string | undefined) {
  if (raw === undefined) return { kind: "absent" } as const;
  const exactDocument = exactJsonDocumentWhenNeeded(raw);
  if (exactDocument) return { kind: "json", value: exactDocument } as const;
  try {
    return { kind: "json", value: parseJsonPreservingUnsafeNumbers(raw) } as const;
  } catch {
    return { kind: "raw", value: raw } as const;
  }
}

function isPreparedContent(value: unknown): value is {
  input: unknown;
  output: unknown;
  attributes: unknown[];
  resourceAttributes: unknown[];
  events: unknown;
  links: unknown;
  status: unknown;
} {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return Array.isArray(record.attributes) && Array.isArray(record.resourceAttributes);
}

function restoreContentString(value: unknown) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TraceContentInvalidError("prepared content wrapper is invalid");
  }
  const record = value as Record<string, unknown>;
  if (record.kind === "absent") return undefined;
  if (record.kind === "raw" && typeof record.value === "string") return record.value;
  if (record.kind === "raw" || record.kind === "json") return JSON.stringify(record.value);
  throw new TraceContentInvalidError("prepared content wrapper is invalid");
}

function responseForAdmission(admission: { kind: string; admitted?: number }) {
  if (admission.kind === "rejected")
    return new Response("Prepared content rejected", { status: 413 });
  if (admission.kind === "unavailable")
    return new Response("Admission unavailable", { status: 503 });
  return new Response(null, { status: (admission.admitted ?? 0) === 0 ? 204 : 202 });
}

export async function handleOpenRouterTraceRequest(
  ctx: ActionContext,
  request: Request,
  options: OpenRouterTraceHandlerOptions,
) {
  if (!(await authorized(request, options.bearerToken))) {
    return new Response("Unauthorized", { status: 401 });
  }
  if (!isApplicationJson(request)) {
    return new Response("Content-Type must be application/json", { status: 415 });
  }
  const limits = resolveTraceBlobLimits(options.limits);
  const body = await readBoundedTraceBody(request, limits.maxRequestBytes);
  if (body.kind === "too_large") return new Response("Payload too large", { status: 413 });

  try {
    const prepared = await prepareDelivery(body.bytes, options);
    if (isTestConnection(request) && emptyEnvelope(prepared.delivery)) {
      return new Response(null, { status: 204 });
    }
    for (const object of prepared.planned) await options.blobStorage.put(object);
    const admission = await ctx.runMutation(options.component.ingest.admitPrepared, {
      spans: prepared.spans,
    });
    return responseForAdmission(admission);
  } catch (error) {
    if (
      error instanceof TraceContentBoundExceededError ||
      error instanceof OtlpBoundExceededError
    ) {
      return new Response(error.message, { status: 413 });
    }
    if (error instanceof TraceContentInvalidError || error instanceof InvalidOtlpDeliveryError) {
      return new Response(error.message, { status: 400 });
    }
    console.error("OpenRouter trace admission failed", error);
    return new Response("Admission unavailable", { status: 503 });
  }
}
