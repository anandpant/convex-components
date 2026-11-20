import type { TraceBlobPut, TraceBlobRef } from "../src/client.js";

export function memoryStorage() {
  const objects = new Map<string, TraceBlobPut>();
  return {
    objects,
    adapter: {
      put: async (object: TraceBlobPut) => {
        objects.set(object.key, object);
      },
    },
    reader: {
      get: async (reference: TraceBlobRef, options: { maxBytes: number }) => {
        const object = objects.get(reference.key);
        if (!object || object.bytes.byteLength > options.maxBytes) return null;
        return { bytes: object.bytes, contentType: object.contentType };
      },
    },
  };
}
