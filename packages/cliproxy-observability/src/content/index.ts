import { sha256 } from "../capture/index.js";
import type { PrivateContentReference } from "../model-call/index.js";
export type PrivateCaptureStorage = {
  put: (reference: PrivateContentReference, bytes: Uint8Array) => Promise<void>;
  get: (key: string) => Promise<ReadableStream<Uint8Array>>;
};
/** The host must obtain manifest from the authorized selected call, never from model content. */
export async function resolveCallBlob(
  storage: Pick<PrivateCaptureStorage, "get">,
  manifest: readonly PrivateContentReference[],
  selected: PrivateContentReference,
  maxBytes = 2 * 1024 * 1024,
): Promise<Uint8Array> {
  if (
    !manifest.some(
      (ref) =>
        ref.key === selected.key &&
        ref.sha256 === selected.sha256 &&
        ref.byteLength === selected.byteLength &&
        ref.contentType === selected.contentType,
    ) ||
    !Number.isSafeInteger(selected.byteLength) ||
    selected.byteLength < 0 ||
    selected.byteLength > maxBytes
  )
    throw new Error("unowned or oversized content reference");
  const reader = (await storage.get(selected.key)).getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > selected.byteLength || size > maxBytes) {
        await reader.cancel();
        throw new Error("stored content bound exceeded");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  if (size !== selected.byteLength || (await sha256(body)) !== selected.sha256)
    throw new Error("stored content integrity failure");
  return body;
}
