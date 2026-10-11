import type { GenericActionCtx, GenericDataModel } from "convex/server";
import type { CliproxyObservability } from "./client.js";
import { resolveCallBlob, type PrivateCaptureStorage } from "./content/index.js";
import { decodeBody, sha256, type CaptureObservationV1 } from "./capture/index.js";
import { applyObservation, type ProjectionCheckpoint } from "./protocols/checkpoint.js";
import { BodyPartAssembler } from "./protocols/body-parts.js";
import type { ModelCallV1 } from "./model-call/index.js";
type ProjectionContext = Pick<GenericActionCtx<GenericDataModel>, "runQuery" | "runMutation">;
type ProjectionOptions = {
  client: CliproxyObservability;
  storage: PrivateCaptureStorage;
  destinationId: string;
  callId: string;
};
const segmentLines = (content: Uint8Array) =>
  new TextDecoder("utf-8", { fatal: true }).decode(content).split("\n").filter(Boolean);
/**
 * A split body spans several immutable segments and projection pages. Re-read
 * its owned segments from the first part and return the exact verified body.
 */
async function reassembleSplitBody(
  ctx: ProjectionContext,
  options: ProjectionOptions,
  fromSegment: number,
  final: CaptureObservationV1,
): Promise<Uint8Array> {
  const parts = new BodyPartAssembler();
  const first = final.sequence - final.bodyPart!.index;
  let after = fromSegment - 1;
  while (after < final.sequence) {
    const page = await ctx.runQuery(options.client.component.queries.pageEventSegments, {
      destinationId: options.destinationId,
      callId: options.callId,
      afterSequence: after,
      limit: 8,
    });
    if (!page.segments.length || page.segments[0]!.sequence !== after + 1)
      throw new Error("split body segment unavailable");
    for (const segment of page.segments) {
      if (segment.sequence > final.sequence) break;
      const content = await resolveCallBlob(
        options.storage,
        [segment.reference],
        segment.reference,
      );
      for (const line of segmentLines(content)) {
        const o = JSON.parse(line) as CaptureObservationV1;
        if (o.sequence < first || o.sequence > final.sequence) continue;
        const whole = await parts.push(o, decodeBody(o));
        if (whole) return whole;
      }
      after = segment.throughSequence;
    }
  }
  throw new Error("split body incomplete");
}
/** Project at most two immutable segments. Host schedules the next page when `more` is true. */
export async function projectPendingSegments(ctx: ProjectionContext, options: ProjectionOptions) {
  const { client, destinationId, callId } = options;
  const state = await ctx.runQuery(client.component.queries.getProcessingState, {
    destinationId,
    callId,
  });
  if (!state) return { progressed: false, more: false };
  try {
    const page = await ctx.runQuery(client.component.queries.pageEventSegments, {
      destinationId,
      callId,
      afterSequence: state.projectedThroughSequence,
      limit: 2,
    });
    const call = JSON.parse(state.summaryJson) as ModelCallV1;
    call.capture.projectedThroughSequence = state.projectedThroughSequence;
    const checkpoint = JSON.parse(state.checkpointJson) as ProjectionCheckpoint;
    if (checkpoint.sseBlob) {
      const stored = await resolveCallBlob(
        options.storage,
        [checkpoint.sseBlob],
        checkpoint.sseBlob,
      );
      checkpoint.sse = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(stored));
      delete checkpoint.sseBlob;
    }
    let through = state.projectedThroughSequence;
    let waitingForGap = false;
    for (const segment of page.segments) {
      if (segment.sequence !== through + 1) {
        waitingForGap = true;
        break;
      }
      const content = await resolveCallBlob(
        options.storage,
        [segment.reference],
        segment.reference,
      );
      for (const line of segmentLines(content)) {
        const o = JSON.parse(line) as CaptureObservationV1;
        const part = o.bodyPart;
        if (part?.index === 0) checkpoint.bodyPartSegment = segment.sequence;
        if (part && part.index === part.count - 1) {
          if (checkpoint.bodyPartSegment === undefined) throw new Error("split body start missing");
          const whole = await reassembleSplitBody(ctx, options, checkpoint.bodyPartSegment, o);
          applyObservation(call, checkpoint, o, whole);
          delete checkpoint.bodyPartSegment;
        } else applyObservation(call, checkpoint, o);
      }
      through = segment.throughSequence;
    }
    if (through === state.projectedThroughSequence)
      return { progressed: false, more: false, waitingForGap };
    // A frame can exceed Convex's checkpoint budget. Persist pending framing bytes
    // privately; the bounded component checkpoint owns the exact reference.
    if (checkpoint.sse && checkpoint.sse.pendingBase64.length > 16 * 1024) {
      const bytes = new TextEncoder().encode(JSON.stringify(checkpoint.sse));
      const digest = await sha256(bytes);
      const ref = {
        key: `observability/cliproxy/v2/${destinationId}/${state.instanceId}/${callId}/parser/${digest}.json`,
        sha256: digest,
        byteLength: bytes.length,
        contentType: "application/json",
      };
      await options.storage.put(ref, bytes);
      checkpoint.sseBlob = ref;
      delete checkpoint.sse;
    }
    const committed = await ctx.runMutation(client.component.ingest.commitProjection, {
      destinationId,
      callId,
      expectedRevision: state.revision,
      throughSequence: through,
      summaryJson: JSON.stringify(call),
      checkpointJson: JSON.stringify(checkpoint),
    });
    return {
      progressed: committed.committed,
      more: !committed.committed || (!page.done && !waitingForGap),
      waitingForGap,
    };
  } catch (error) {
    await ctx.runMutation(client.component.ingest.recordProjectionFailure, {
      destinationId,
      callId,
      expectedRevision: state.revision,
    });
    throw error;
  }
}
