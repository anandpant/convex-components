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
  type DecodedMessage,
  type DecodedMessageContent,
  type DecodedOutputContent,
  type EmittedToolCall,
  type TextContentPart,
  type ToolDefinition,
} from "./content.js";

type QueryContext = Pick<GenericQueryCtx<GenericDataModel>, "runQuery">;
type MutationContext = Pick<GenericMutationCtx<GenericDataModel>, "runQuery">;
type ActionContext = Pick<GenericActionCtx<GenericDataModel>, "runQuery">;
type ReadContext = QueryContext | MutationContext | ActionContext;

export type OpenRouterObservabilityComponent = ComponentApi;
export type SpanCursor = { receivedAt: number; _creationTime: number };
export type SummaryPageOptions = { limit?: number; cursor?: SpanCursor };
export type TraceSummaryCursor = FunctionArgs<
  ComponentApi["queries"]["pageTraceSummaries"]
>["cursor"];
export type SpanDocumentId = FunctionArgs<
  ComponentApi["queries"]["exportFullSpan"]
>["spanDocumentId"];
export type TraceSummaryPageOptions = { limit?: number; cursor?: TraceSummaryCursor };
export type SpanCorrelation =
  | { kind: "request"; requestId: string }
  | { kind: "session"; sessionId: string }
  | { kind: "user"; userId: string }
  | { kind: "entity"; entityType: string; entityId: string }
  | { kind: "run"; runId: string }
  | { kind: "job"; jobId: string }
  | { kind: "rootExecution"; rootExecutionId: string }
  | { kind: "opencodeSession"; opencodeSessionId: string };

export class OpenRouterObservability {
  constructor(private readonly component: ComponentApi) {}

  pageTraceSummaries(ctx: ReadContext, args: TraceSummaryPageOptions & { traceId: string }) {
    return ctx.runQuery(this.component.queries.pageTraceSummaries, args);
  }

  pageCorrelationSummaries(
    ctx: ReadContext,
    args: SummaryPageOptions & { correlation: SpanCorrelation },
  ) {
    return ctx.runQuery(this.component.queries.pageCorrelationSummaries, args);
  }

  pageRecentSummaries(ctx: ReadContext, args: SummaryPageOptions = {}) {
    return ctx.runQuery(this.component.queries.pageRecentSummaries, args);
  }

  exportFullSpan(ctx: ReadContext, args: { spanDocumentId: SpanDocumentId }) {
    return ctx.runQuery(this.component.queries.exportFullSpan, args);
  }

  getCorrelationProjectionCoverage(ctx: ReadContext) {
    return ctx.runQuery(this.component.queries.getCorrelationProjectionCoverage, {});
  }
}
