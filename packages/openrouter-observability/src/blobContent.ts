const DEFAULT_BLOB_PREFIX = "openrouter-observability/v1";
const DEFAULT_MAX_BLOB_BYTES = 6 * 1024 * 1024;
const DEFAULT_MAX_REQUEST_BYTES = 8 * 1024 * 1024;
const DEFAULT_TEXT_INLINE_BYTES = 64 * 1024;
const encoder = new TextEncoder();

export const TRACE_BLOB_MARKER = "$openrouterObservabilityBlob";
export const UNSAFE_JSON_NUMBER_MARKER = "$openrouterObservabilityJsonNumber";

export type TraceBlobRef = {
  kind: "trace_blob";
  key: string;
  sha256: string;
  byteLength: number;
  contentType: string;
  encoding: "binary" | "utf8";
};

export type TraceBlobMarker = { [TRACE_BLOB_MARKER]: TraceBlobRef };

export class ExactJsonDocument {
  constructor(readonly raw: string) {}
}

export type TraceBlobPut = TraceBlobRef & { bytes: Uint8Array };

export type TraceBlobStorage = {
  put: (object: TraceBlobPut) => Promise<void>;
};

export type TraceBlobReader = {
  get: (
    reference: TraceBlobRef,
    options: { maxBytes: number },
  ) => Promise<{ bytes: Uint8Array; contentType?: string } | null>;
};

export type TraceBlobLimits = {
  maxRequestBytes?: number;
  maxBlobBytes?: number;
  inlineTextBytes?: number;
};

export type RemoteTraceBlobMapper = (
  url: string,
) => Promise<TraceBlobRef | undefined> | TraceBlobRef | undefined;

export class TraceContentInvalidError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TraceContentInvalidError";
  }
}

export class TraceContentBoundExceededError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TraceContentBoundExceededError";
  }
}

export function resolveTraceBlobLimits(limits: TraceBlobLimits = {}) {
  const maxRequestBytes = limits.maxRequestBytes ?? DEFAULT_MAX_REQUEST_BYTES;
  const resolved = {
    maxRequestBytes,
    maxBlobBytes: limits.maxBlobBytes ?? Math.min(DEFAULT_MAX_BLOB_BYTES, maxRequestBytes),
    inlineTextBytes: limits.inlineTextBytes ?? DEFAULT_TEXT_INLINE_BYTES,
  };
  for (const [name, value] of Object.entries(resolved)) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new Error(`${name} must be a positive integer`);
    }
  }
  if (resolved.maxBlobBytes > resolved.maxRequestBytes) {
    throw new Error("maxBlobBytes cannot exceed maxRequestBytes");
  }
  if (resolved.maxRequestBytes > DEFAULT_MAX_REQUEST_BYTES) {
    throw new Error(`maxRequestBytes cannot exceed ${DEFAULT_MAX_REQUEST_BYTES}`);
  }
  if (resolved.maxBlobBytes > DEFAULT_MAX_BLOB_BYTES) {
    throw new Error(`maxBlobBytes cannot exceed ${DEFAULT_MAX_BLOB_BYTES}`);
  }
  if (resolved.inlineTextBytes > DEFAULT_TEXT_INLINE_BYTES) {
    throw new Error(`inlineTextBytes cannot exceed ${DEFAULT_TEXT_INLINE_BYTES}`);
  }
  return resolved;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isTraceBlobRef(value: unknown): value is TraceBlobRef {
  return (
    isRecord(value) &&
    value.kind === "trace_blob" &&
    typeof value.key === "string" &&
    value.key.length > 0 &&
    value.key.length <= 1024 &&
    typeof value.sha256 === "string" &&
    /^[0-9a-f]{64}$/.test(value.sha256) &&
    typeof value.byteLength === "number" &&
    Number.isSafeInteger(value.byteLength) &&
    value.byteLength >= 0 &&
    typeof value.contentType === "string" &&
    value.contentType.length > 0 &&
    value.contentType.length <= 255 &&
    (value.encoding === "binary" || value.encoding === "utf8")
  );
}

export function traceBlobRefFromMarker(value: unknown) {
  if (!isRecord(value) || Object.keys(value).length !== 1) return undefined;
  const reference = value[TRACE_BLOB_MARKER];
  return isTraceBlobRef(reference) ? reference : undefined;
}

function bytesToHex(bytes: Uint8Array) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function sha256Hex(bytes: Uint8Array) {
  const input =
    bytes.buffer instanceof ArrayBuffer &&
    bytes.byteOffset === 0 &&
    bytes.byteLength === bytes.buffer.byteLength
      ? bytes.buffer
      : bytes.slice().buffer;
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", input)));
}

const JSON_NUMBER = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;

function unsafeJsonNumbers(raw: string) {
  const matches: Array<{ start: number; end: number; source: string }> = [];
  let index = 0;
  let inString = false;
  let escaped = false;
  while (index < raw.length) {
    const character = raw[index] as string;
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      index += 1;
      continue;
    }
    if (character === '"') {
      inString = true;
      index += 1;
      continue;
    }
    if (character === "-" || (character >= "0" && character <= "9")) {
      JSON_NUMBER.lastIndex = index;
      const source = JSON_NUMBER.exec(raw)?.[0];
      if (source) {
        const value = Number(source);
        if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
          matches.push({ start: index, end: index + source.length, source });
        }
        index += source.length;
        continue;
      }
    }
    index += 1;
  }
  return matches;
}

export function parseJsonPreservingUnsafeNumbers(raw: string) {
  const matches = unsafeJsonNumbers(raw);
  if (matches.length === 0) return JSON.parse(raw) as unknown;
  const parts: string[] = [];
  let cursor = 0;
  for (const match of matches) {
    parts.push(
      raw.slice(cursor, match.start),
      JSON.stringify({ [UNSAFE_JSON_NUMBER_MARKER]: match.source }),
    );
    cursor = match.end;
  }
  parts.push(raw.slice(cursor));
  return JSON.parse(parts.join("")) as unknown;
}

export function exactJsonDocumentWhenNeeded(raw: string) {
  return unsafeJsonNumbers(raw).length === 0 ? undefined : new ExactJsonDocument(raw);
}

function decodeBase64(value: string, path: string) {
  if (value.length === 0 || value.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(value)) {
    throw new TraceContentInvalidError(`${path} contains malformed base64`);
  }
  try {
    const decoded = atob(value);
    const bytes = new Uint8Array(decoded.length);
    for (let index = 0; index < decoded.length; index += 1) {
      bytes[index] = decoded.charCodeAt(index);
    }
    return bytes;
  } catch {
    throw new TraceContentInvalidError(`${path} contains malformed base64`);
  }
}

function parseDataUrl(value: string, path: string) {
  const separator = value.indexOf(",");
  if (separator === -1) throw new TraceContentInvalidError(`${path} contains a malformed data URL`);
  const metadata = value.slice(5, separator);
  const payload = value.slice(separator + 1);
  const segments = metadata.split(";");
  const contentType = segments[0] || "text/plain;charset=US-ASCII";
  if (contentType.length > 255 || /[\r\n]/.test(contentType)) {
    throw new TraceContentInvalidError(`${path} contains an invalid media type`);
  }
  if (segments.at(-1)?.toLowerCase() !== "base64") {
    throw new TraceContentInvalidError(`${path} data URL must use base64 encoding`);
  }
  return { bytes: decodeBase64(payload, path), contentType };
}

function looksLikeDataUrl(value: string) {
  return /^data:[^,\s]*,/.test(value) || /^data:(?:image|audio|video|application)\//i.test(value);
}

function recognizedBase64Field(key: string, parent: Record<string, unknown>) {
  if (["b64_json", "image_base64", "audio_base64"].includes(key)) return true;
  if (key !== "data") return false;
  const declaredType = [parent.type, parent.media_type, parent.mime_type]
    .filter((value): value is string => typeof value === "string")
    .join(" ")
    .toLowerCase();
  return declaredType.includes("base64") || /(^|\s)(image|audio)\//.test(declaredType);
}

type ExternalizeOptions = {
  deliveryDigest: string;
  prefix?: string;
  limits?: TraceBlobLimits;
  mapRemoteUrl?: RemoteTraceBlobMapper;
};

export async function externalizeTraceValue(value: unknown, options: ExternalizeOptions) {
  const limits = resolveTraceBlobLimits(options.limits);
  const planned: TraceBlobPut[] = [];
  let ordinal = 0;
  let plannedBytes = 0;

  const externalize = async (
    bytes: Uint8Array,
    contentType: string,
    encoding: "binary" | "utf8",
  ): Promise<TraceBlobMarker> => {
    if (bytes.byteLength > limits.maxBlobBytes) {
      throw new TraceContentBoundExceededError(
        `one trace blob exceeds the limit of ${limits.maxBlobBytes} bytes`,
      );
    }
    plannedBytes += bytes.byteLength;
    if (plannedBytes > limits.maxRequestBytes) {
      throw new TraceContentBoundExceededError(
        `externalized trace content exceeds the limit of ${limits.maxRequestBytes} bytes`,
      );
    }
    const sha256 = await sha256Hex(bytes);
    const key = `${options.prefix ?? DEFAULT_BLOB_PREFIX}/${options.deliveryDigest}/${ordinal}-${sha256}`;
    ordinal += 1;
    const reference: TraceBlobRef = {
      kind: "trace_blob",
      key,
      sha256,
      byteLength: bytes.byteLength,
      contentType,
      encoding,
    };
    planned.push({ ...reference, bytes });
    return { [TRACE_BLOB_MARKER]: reference };
  };

  const visit = async (
    current: unknown,
    path: string,
    parent?: Record<string, unknown>,
    key = "",
  ): Promise<unknown> => {
    if (current instanceof ExactJsonDocument) {
      return await externalize(encoder.encode(current.raw), "application/json", "utf8");
    }
    if (typeof current === "string") {
      if (looksLikeDataUrl(current)) {
        const parsed = parseDataUrl(current, path);
        return await externalize(parsed.bytes, parsed.contentType, "binary");
      }
      if (parent && recognizedBase64Field(key, parent)) {
        return await externalize(decodeBase64(current, path), "application/octet-stream", "binary");
      }
      if (/^https?:\/\//.test(current) && options.mapRemoteUrl) {
        const mapped = await options.mapRemoteUrl(current);
        if (mapped !== undefined) {
          if (!isTraceBlobRef(mapped) || mapped.byteLength > limits.maxBlobBytes) {
            throw new TraceContentInvalidError("mapRemoteUrl returned an invalid trace blob ref");
          }
          return { [TRACE_BLOB_MARKER]: mapped };
        }
      }
      const bytes = encoder.encode(current);
      return bytes.byteLength > limits.inlineTextBytes
        ? await externalize(bytes, "text/plain;charset=utf-8", "utf8")
        : current;
    }
    if (Array.isArray(current)) {
      const result = [];
      for (let index = 0; index < current.length; index += 1) {
        result.push(await visit(current[index], `${path}[${index}]`));
      }
      return result;
    }
    if (isRecord(current)) {
      if (
        Object.keys(current).length === 1 &&
        typeof current[UNSAFE_JSON_NUMBER_MARKER] === "string"
      ) {
        return await externalize(
          encoder.encode(current[UNSAFE_JSON_NUMBER_MARKER]),
          "application/json",
          "utf8",
        );
      }
      if (TRACE_BLOB_MARKER in current) {
        return await externalize(
          encoder.encode(JSON.stringify(current)),
          "application/json;charset=utf-8",
          "utf8",
        );
      }
      const result: Record<string, unknown> = {};
      for (const [childKey, child] of Object.entries(current)) {
        result[childKey] = await visit(child, `${path}.${childKey}`, current, childKey);
      }
      return result;
    }
    return current;
  };

  return { value: await visit(value, "delivery"), planned };
}

function collectMarkers(value: unknown, references: TraceBlobRef[]) {
  const reference = traceBlobRefFromMarker(value);
  if (reference) {
    references.push(reference);
    return;
  }
  if (Array.isArray(value)) {
    for (const child of value) collectMarkers(child, references);
  } else if (isRecord(value)) {
    for (const child of Object.values(value)) collectMarkers(child, references);
  }
}

export function findTraceBlobRefs(storedSpan: unknown) {
  if (!isRecord(storedSpan)) return [];
  const references: TraceBlobRef[] = [];
  const jsonFields = ["input", "output", "eventsJson", "linksJson", "statusJson"];
  for (const field of jsonFields) {
    const raw = storedSpan[field];
    if (typeof raw !== "string") continue;
    try {
      collectMarkers(JSON.parse(raw) as unknown, references);
    } catch {
      // Legacy invalid content can contain no package-created references.
    }
  }
  for (const field of ["attributes", "resourceAttributes"]) {
    const attributes = storedSpan[field];
    if (!Array.isArray(attributes)) continue;
    for (const attribute of attributes) {
      if (!isRecord(attribute) || typeof attribute.valueJson !== "string") continue;
      try {
        collectMarkers(JSON.parse(attribute.valueJson) as unknown, references);
      } catch {
        // Stored OTLP attributes are JSON, but legacy rows may predate that invariant.
      }
    }
  }
  return references;
}

export async function resolveTraceBlob(
  reader: TraceBlobReader,
  storedSpan: unknown,
  reference: TraceBlobRef,
  options: { maxBytes: number },
) {
  if (!Number.isSafeInteger(options.maxBytes) || options.maxBytes < 1) {
    throw new Error("maxBytes must be a positive integer");
  }
  if (reference.byteLength > options.maxBytes) {
    throw new TraceContentBoundExceededError("trace blob exceeds the caller's byte limit");
  }
  const storedReference = findTraceBlobRefs(storedSpan).find(
    (candidate) =>
      candidate.key === reference.key &&
      candidate.sha256 === reference.sha256 &&
      candidate.byteLength === reference.byteLength &&
      candidate.contentType === reference.contentType &&
      candidate.encoding === reference.encoding,
  );
  if (!storedReference) {
    throw new TraceContentInvalidError("trace blob reference does not belong to the stored span");
  }
  const result = await reader.get(storedReference, options);
  if (result === null) return null;
  if (
    result.bytes.byteLength !== reference.byteLength ||
    result.bytes.byteLength > options.maxBytes
  ) {
    throw new TraceContentInvalidError("trace blob byte length does not match its reference");
  }
  if ((await sha256Hex(result.bytes)) !== reference.sha256) {
    throw new TraceContentInvalidError("trace blob digest does not match its reference");
  }
  return { bytes: result.bytes, contentType: result.contentType ?? storedReference.contentType };
}
