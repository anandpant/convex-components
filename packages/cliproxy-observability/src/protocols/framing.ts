/** Incremental SSE framing. Times belong to the observation containing the final byte. */
export type SSEFrame = { data: string; event?: string; observedAt?: string };
export type SSECheckpoint = { pendingBase64: string };
export class SSEReader {
  private pending: Uint8Array;
  constructor(
    checkpoint: SSECheckpoint = { pendingBase64: "" },
    private maxBytes = 1024 * 1024,
  ) {
    this.pending = Uint8Array.from(atob(checkpoint.pendingBase64), (c) => c.charCodeAt(0));
    if (this.pending.length > maxBytes) throw new Error("SSE frame limit");
  }
  feed(bytes: Uint8Array, observedAt?: string): SSEFrame[] {
    const combined = new Uint8Array(this.pending.length + bytes.length);
    combined.set(this.pending);
    combined.set(bytes, this.pending.length);
    const frames: SSEFrame[] = [];
    let start = 0;
    for (let i = 0; i < combined.length; i++) {
      const delimiter =
        combined[i] === 13 &&
        combined[i + 1] === 10 &&
        combined[i + 2] === 13 &&
        combined[i + 3] === 10
          ? 4
          : combined[i] === 10 && combined[i + 1] === 10
            ? 2
            : combined[i] === 13 && combined[i + 1] === 13
              ? 2
              : 0;
      if (i - start > this.maxBytes) throw new Error("SSE frame limit");
      if (!delimiter) continue;
      const block = new TextDecoder("utf-8", { fatal: true }).decode(combined.subarray(start, i));
      const data: string[] = [];
      let event: string | undefined;
      for (const line of block.split(/\r\n|\r|\n/)) {
        if (line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon < 0 ? line : line.slice(0, colon);
        const value = (colon < 0 ? "" : line.slice(colon + 1)).replace(/^ /, "");
        if (field === "data") data.push(value);
        else if (field === "event") event = value;
      }
      if (data.length) frames.push({ data: data.join("\n"), event, observedAt });
      i += delimiter - 1;
      start = i + 1;
    }
    this.pending = combined.slice(start);
    if (this.pending.length > this.maxBytes) throw new Error("SSE frame limit");
    return frames;
  }
  finish(): { truncated: boolean } {
    return {
      truncated: new TextDecoder("utf-8", { fatal: true }).decode(this.pending).trim().length > 0,
    };
  }
  checkpoint(): SSECheckpoint {
    let raw = "";
    for (const byte of this.pending) raw += String.fromCharCode(byte);
    return { pendingBase64: btoa(raw) };
  }
}
