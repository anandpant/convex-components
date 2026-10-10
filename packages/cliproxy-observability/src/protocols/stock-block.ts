/** Recover stock parser boundaries without altering the captured bytes. */
export function expandStockHookBlock(
  body: Uint8Array,
  lengths: readonly number[],
): readonly Uint8Array[] {
  if (!Array.isArray(lengths) || lengths.length === 0 || lengths.length > 1024)
    throw new Error("invalid stock hook block lengths");
  const pieces: Uint8Array[] = [];
  let offset = 0;
  for (const length of lengths) {
    if (!Number.isSafeInteger(length) || length < 0 || length > body.byteLength - offset)
      throw new Error("invalid stock hook block length");
    pieces.push(body.subarray(offset, offset + length));
    offset += length;
  }
  if (offset !== body.byteLength) throw new Error("stock hook block byte mismatch");
  return pieces;
}
