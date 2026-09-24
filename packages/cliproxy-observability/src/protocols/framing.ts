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
  /** Derive frames from pinned stock pre-framer callbacks; stored bytes stay untouched. */
  feedStock(bytes: Uint8Array, protocol?: string, observedAt?: string): SSEFrame[] {
    const decoder = new TextDecoder("utf-8", { fatal: true });
    if (protocol === "chat_completions" && (this.pending[0] ?? bytes[0]) === 123) {
      if (this.pending.length + bytes.length > this.maxBytes) throw new Error("SSE frame limit");
      const combined = new Uint8Array(this.pending.length + bytes.length);
      combined.set(this.pending);
      combined.set(bytes, this.pending.length);
      this.pending = combined;
      try {
        const data = decoder.decode(combined);
        JSON.parse(data);
        this.pending = new Uint8Array();
        return [{ data, observedAt }];
      } catch {
        return [];
      }
    }
    if (protocol === "responses" && this.pending.length && bytes.length) {
      // Only an event-only field can gain a missing separator. A data: prefix
      // inside fragmented JSON is content, not a new line.
      const event = new TextDecoder().decode(this.pending);
      if (
        /^event: *[a-zA-Z0-9][a-zA-Z0-9._:-]{0,126} *$/.test(event) &&
        bytes[0] === 100 &&
        bytes[1] === 97 &&
        bytes[2] === 116 &&
        bytes[3] === 97 &&
        bytes[4] === 58
      ) {
        const line = new Uint8Array(bytes.length + 1);
        line[0] = 10;
        line.set(bytes, 1);
        bytes = line;
      }
    }
    const frames = this.feed(bytes, observedAt);
    if (protocol !== "responses") return frames;
    let candidate: string;
    try {
      candidate = decoder.decode(this.pending);
    } catch {
      return frames;
    }
    if (!candidate.trim()) {
      this.pending = new Uint8Array();
      return frames;
    }
    const data: string[] = [];
    let event: string | undefined;
    for (const line of candidate.replaceAll("\r\n", "\n").replaceAll("\r", "\n").split("\n")) {
      if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
      else if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line !== "" && !line.startsWith(":")) return frames;
    }
    if (!data.length) return frames;
    const payload = data.join("\n");
    if (payload !== "[DONE]") {
      try {
        JSON.parse(payload);
      } catch {
        return frames;
      }
    }
    this.pending = new Uint8Array();
    frames.push({ data: payload, event, observedAt });
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
