import type {
  GenericActionCtx,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
  FunctionArgs,
} from "convex/server";
import type { ComponentApi } from "./component/_generated/component.js";

export {
  decodeOpenRouterInput,
  decodeOpenRouterOutput,
  type ContentDecodeOutcome,
  type DecodedInputContent,
  type DecodedContentPart,
  type DecodedText,
  type ImageContentPart,
  type DecodedMessage,
  type DecodedMessageContent,
  type DecodedOutputContent,
  type EmittedToolCall,
  type OpaqueContentPart,
  type TextContentPart,
  type ToolDefinition,
} from "./content.js";
export {
  isTraceBlobRef,
  findTraceBlobRefs,
  resolveTraceBlob,
  traceBlobRefFromMarker,
  TRACE_BLOB_MARKER,
  UNSAFE_JSON_NUMBER_MARKER,
  TraceContentBoundExceededError,
  TraceContentInvalidError,
  type RemoteTraceBlobMapper,
  type TraceBlobLimits,
  type TraceBlobMarker,
  type TraceBlobPut,
  type TraceBlobReader,
  type TraceBlobRef,
  type TraceBlobStorage,
} from "./blobContent.js";
export {
  handleOpenRouterTraceRequest,
  readBoundedTraceBody,
  type OpenRouterTraceHandlerOptions,
} from "./ingestion.js";

export {
  archiveSpan,
  loadFullSpan,
  spanPayloadBlobRef,
  DEFAULT_SPAN_PREFIX,
  type SlimSpan,
  type SpanPayloadRef,
} from "./spanArchive.js";
export type { ParsedOpenRouterSpan, StoredAttribute } from "./component/parser.js";

type QueryContext = Pick<GenericQueryCtx<GenericDataModel>, "runQuery">;
type MutationContext = Pick<GenericMutationCtx<GenericDataModel>, "runQuery">;
type ActionContext = Pick<GenericActionCtx<GenericDataModel>, "runQuery">;
type ReadContext = QueryContext | MutationContext | ActionContext;

export type OpenRouterObservabilityComponent = ComponentApi;
export type SpanCursor = { receivedAt: number; _creationTime: number };
export type SummaryPageOptions = { limit?: number; cursor?: SpanCursor };
export type SpanDocumentId = FunctionArgs<ComponentApi["queries"]["getSpan"]>["spanDocumentId"];
export type SpanCorrelation =
  | { kind: "request"; requestId: string }
  | { kind: "run"; runId: string }
  | { kind: "job"; jobId: string };

export class OpenRouterObservability {
  constructor(private readonly component: ComponentApi) {}

  pageCorrelationSummaries(
    ctx: ReadContext,
    args: SummaryPageOptions & { correlation: SpanCorrelation },
  ) {
    return ctx.runQuery(this.component.queries.pageCorrelationSummaries, args);
  }

  /** The stored row; pass it to `loadFullSpan` in an action for its content. */
  getSpan(ctx: ReadContext, args: { spanDocumentId: SpanDocumentId }) {
    return ctx.runQuery(this.component.queries.getSpan, args);
  }
}
