import {
  MAX_BODY_PARTS,
  MAX_CONTENT_BYTES,
  continuesBodyPart,
  sha256,
  type CaptureObservationV1,
} from "../capture/index.js";
import { expandStockHookBlock } from "./stock-block.js";

/**
 * Reassembles native 0.2.5 split bodies from validated observations read in
 * sequence order. `push` returns a whole observation's body unchanged, nothing
 * for an earlier part, and the exact original body at the final part once its
 * length and SHA-256 match. Interleaved, missing or reordered parts throw.
 */
export class BodyPartAssembler {
  #open?: { previous: CaptureObservationV1; body: Uint8Array; written: number };
  /** `maxBytes` bounds the buffer one split body may allocate. */
  constructor(readonly maxBytes = MAX_BODY_PARTS * MAX_CONTENT_BYTES) {}
  /** A split body has started but its final part has not been pushed. */
  get pending(): boolean {
    return this.#open !== undefined;
  }
  async push(o: CaptureObservationV1, body: Uint8Array): Promise<Uint8Array | undefined> {
    const part = o.bodyPart;
    const open = this.#open;
    if (!part) {
      if (open) throw new Error("body part interrupted");
      return body;
    }
    if (part.index === 0) {
      if (open) throw new Error("body part interrupted");
      if (part.totalBytes > this.maxBytes) throw new Error("body part read limit");
      this.#open = { previous: o, body: new Uint8Array(part.totalBytes), written: 0 };
    } else if (!open || !continuesBodyPart(open.previous, o)) {
      throw new Error("body part order mismatch");
    }
    const current = this.#open!;
    if (part.offset !== current.written || body.byteLength > part.totalBytes - current.written)
      throw new Error("body part offset mismatch");
    current.body.set(body, part.offset);
    current.written += body.byteLength;
    current.previous = o;
    if (part.index < part.count - 1) return undefined;
    this.#open = undefined;
    if (current.written !== part.totalBytes || (await sha256(current.body)) !== part.totalSha256)
      throw new Error("body part digest mismatch");
    return current.body;
  }
}

/** Stock callbacks in a block, or the one callback a split stream body carries. */
export function stockHookPieces(
  o: Pick<CaptureObservationV1, "bodyPart" | "stockHookChunkLengths">,
  body: Uint8Array,
): readonly Uint8Array[] {
  return o.bodyPart ? [body] : expandStockHookBlock(body, o.stockHookChunkLengths ?? []);
}
