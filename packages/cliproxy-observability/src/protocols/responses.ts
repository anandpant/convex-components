import type { RecordValue } from "./types.js";
import { append, count, list, object } from "./values.js";

/** Validate the captured metadata extension without interpreting provider-specific payload keys.
 * Keep identity bounded because the incremental checkpoint stores it across segments.
 */
export function observeResponsesEnvelope(
  state: { responseId?: string; invalid?: boolean },
  event: RecordValue,
) {
  if (event.type === "keepalive" && count(event.sequence_number) === undefined)
    state.invalid = true;
  const metadata = event.type === "response.metadata";
  if (
    metadata &&
    (count(event.sequence_number) === undefined ||
      event.metadata === null ||
      typeof event.metadata !== "object" ||
      Array.isArray(event.metadata))
  )
    state.invalid = true;
  const id = metadata ? event.response_id : object(event.response).id;
  if (metadata || id !== undefined) {
    if (
      typeof id !== "string" ||
      !id.length ||
      id.length > 256 ||
      (state.responseId !== undefined && state.responseId !== id)
    )
      state.invalid = true;
    else state.responseId = id;
  }
}

/** Reconstruct only emitted Responses content; a completed item is not a completed call. */
export function assembleResponsesStream(events: RecordValue[]) {
  const response: RecordValue = {};
  const items = new Map<number, RecordValue>();
  const envelope: { responseId?: string; invalid?: boolean } = {};
  const metadataEvents: RecordValue[] = [];
  let terminal = false;
  let terminalUsage = false;
  let terminalSnapshot = false;
  let recognized = false;
  let invalid = false;
  let unsupported = false;
  for (const event of events) {
    const type = String(event.type);
    observeResponsesEnvelope(envelope, event);
    if (type === "response.metadata") {
      recognized = true;
      // Preserve every update, including its identity and sequence, separately from
      // response.metadata in lifecycle snapshots (a distinct provider field).
      metadataEvents.push(event);
      continue;
    }
    if (type === "keepalive") {
      // This control frame carries no response content, identity or final usage.
      recognized = true;
      if (count(event.sequence_number) === undefined) invalid = true;
      continue;
    }
    if (
      [
        "response.created",
        "response.queued",
        "response.in_progress",
        "response.completed",
        "response.failed",
        "response.incomplete",
      ].includes(type)
    ) {
      recognized = true;
      const snapshot = object(event.response);
      if (snapshot.object !== "response" || typeof snapshot.id !== "string") {
        invalid = true;
        continue;
      }
      if (["response.completed", "response.failed", "response.incomplete"].includes(type)) {
        if (snapshot.status !== type.slice("response.".length) || !Array.isArray(snapshot.output)) {
          invalid = true;
          continue;
        }
        terminal = true;
        terminalSnapshot = true;
        terminalUsage = snapshot.usage !== undefined && snapshot.usage !== null;
      }
      Object.assign(response, snapshot);
      continue;
    }
    if (type === "error") {
      recognized = true;
      terminal = true;
      response.status = "failed";
      response.error = event.error ?? {
        type: event.type,
        code: event.code,
        message: event.message,
      };
      terminalUsage = false;
      continue;
    }
    if (type === "response.output_item.added" || type === "response.output_item.done") {
      recognized = true;
      const index = count(event.output_index);
      const item = object(event.item);
      if (index === undefined || typeof item.type !== "string") invalid = true;
      else items.set(index, item);
      continue;
    }
    if (
      [
        "response.content_part.added",
        "response.content_part.done",
        "response.output_text.delta",
        "response.output_text.done",
        "response.output_text.annotation.added",
        "response.refusal.delta",
        "response.refusal.done",
        "response.function_call_arguments.delta",
        "response.function_call_arguments.done",
        "response.reasoning_summary_part.added",
        "response.reasoning_summary_part.done",
        "response.reasoning_summary_text.delta",
        "response.reasoning_summary_text.done",
      ].includes(type)
    ) {
      recognized = true;
      const index = count(event.output_index);
      const item = index === undefined ? undefined : items.get(index);
      if (!item || item.id !== event.item_id) {
        invalid = true;
        continue;
      }
      if (type.startsWith("response.function_call_arguments.")) {
        if (type.endsWith(".delta")) append(item, "arguments", event.delta);
        else item.arguments = event.arguments;
        continue;
      }
      const summary = type.startsWith("response.reasoning_summary_");
      const partIndex = count(summary ? event.summary_index : event.content_index);
      if (partIndex === undefined) {
        invalid = true;
        continue;
      }
      const field = summary ? "summary" : "content";
      const parts = list(item[field]);
      // Keep sparse provider indices in a map-like object rather than allocating a huge array.
      if (partIndex > parts.length) {
        invalid = true;
        continue;
      }
      if (type.includes("_part.")) parts[partIndex] = object(event.part);
      else {
        const refusal = type.startsWith("response.refusal.");
        const part = object(parts[partIndex]);
        part.type ??= summary ? "summary_text" : refusal ? "refusal" : "output_text";
        const key = refusal ? "refusal" : "text";
        if (type === "response.output_text.annotation.added") {
          const annotations = list(part.annotations);
          const annotationIndex = count(event.annotation_index);
          if (annotationIndex === undefined || annotationIndex > annotations.length) {
            invalid = true;
            continue;
          }
          annotations[annotationIndex] = event.annotation;
          part.annotations = annotations;
        } else if (type.endsWith(".delta")) append(part, key, event.delta);
        else part[key] = event[key];
        parts[partIndex] = part;
      }
      item[field] = parts;
      continue;
    }
    if (
      [
        "response.web_search_call.in_progress",
        "response.web_search_call.searching",
        "response.web_search_call.completed",
        "response.file_search_call.in_progress",
        "response.file_search_call.searching",
        "response.file_search_call.completed",
        "response.code_interpreter_call.in_progress",
        "response.code_interpreter_call.interpreting",
        "response.code_interpreter_call.completed",
        "response.image_generation_call.in_progress",
        "response.image_generation_call.generating",
        "response.image_generation_call.completed",
        "response.mcp_call.in_progress",
        "response.mcp_call.completed",
        "response.mcp_call.failed",
        "response.mcp_list_tools.in_progress",
        "response.mcp_list_tools.completed",
        "response.mcp_list_tools.failed",
      ].includes(type)
    ) {
      // Tool lifecycle frames carry no content or call usage. Keep them in the raw
      // transcript; only a response terminal event can establish final usage.
      recognized = true;
      continue;
    }
    // Retained in the raw transcript. Do not pretend an unknown protocol frame was decoded.
    unsupported = true;
  }
  const emitted = [...items.entries()].toSorted(([a], [b]) => a - b).map(([, item]) => item);
  if (!terminalSnapshot || !Array.isArray(response.output)) response.output = emitted;
  else if (response.status === "failed" || response.status === "incomplete") {
    const snapshotIds = new Set(response.output.map((item) => object(item).id));
    // Failed terminal snapshots may omit unfinished items. Preserve emitted
    // evidence while preferring terminal entries with the same exact item ID.
    response.output = [
      ...response.output,
      ...emitted.filter((item) => typeof item.id !== "string" || !snapshotIds.has(item.id)),
    ];
  }
  return {
    response,
    terminal,
    terminalUsage,
    recognized,
    invalid: invalid || !!envelope.invalid,
    unsupported,
    metadataEvents,
  };
}

export function responsesMessages(output: unknown): RecordValue[] {
  return list(output).map((raw) => {
    const item = object(raw);
    if (item.type === "function_call")
      return {
        role: "assistant",
        tool_calls: [
          {
            id: item.call_id,
            type: "function",
            function: { name: item.name, arguments: item.arguments },
          },
        ],
      };
    return { ...item, role: item.role ?? "assistant" };
  });
}
