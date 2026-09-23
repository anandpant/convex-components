import type { GenericActionCtx, GenericDataModel } from "convex/server";
import type { CliproxyObservability } from "./client.js";
import { resolveCallBlob, type PrivateCaptureStorage } from "./content/index.js";
import type { CaptureObservationV1 } from "./capture/index.js";
import { applyObservation, type ProjectionCheckpoint } from "./protocols/checkpoint.js";
import type { ModelCallV1 } from "./model-call/index.js";
/** Project at most two immutable segments. Host schedules the next page when `more` is true. */
export async function projectPendingSegments(
  ctx: Pick<GenericActionCtx<GenericDataModel>, "runQuery" | "runMutation">,
  options: {
    client: CliproxyObservability;
    storage: PrivateCaptureStorage;
    destinationId: string;
    callId: string;
  },
) {
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
      for (const line of new TextDecoder("utf-8", { fatal: true })
        .decode(content)
        .split("\n")
        .filter(Boolean))
        applyObservation(call, checkpoint, JSON.parse(line) as CaptureObservationV1);
      through = segment.throughSequence;
    }
    if (through === state.projectedThroughSequence)
      return { progressed: false, more: false, waitingForGap };
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
