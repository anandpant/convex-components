import type {
  GenericActionCtx,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
} from "convex/server";
import type { ComponentApi } from "./component/_generated/component.js";

type QueryContext = Pick<GenericQueryCtx<GenericDataModel>, "runQuery">;
type MutationContext = Pick<GenericMutationCtx<GenericDataModel>, "runQuery">;
type ActionContext = Pick<GenericActionCtx<GenericDataModel>, "runQuery">;
type ReadContext = QueryContext | MutationContext | ActionContext;

export type OpenRouterObservabilityComponent = ComponentApi;
export type SpanCursor = { receivedAt: number; _creationTime: number };
export type ListOptions = { limit?: number; before?: SpanCursor };

export class OpenRouterObservability {
  constructor(private readonly component: ComponentApi) {}

  getTrace(ctx: ReadContext, args: { traceId: string; limit?: number }) {
    return ctx.runQuery(this.component.queries.getTrace, args);
  }

  getSpan(ctx: ReadContext, args: { traceId: string; spanId: string }) {
    return ctx.runQuery(this.component.queries.getSpan, args);
  }

  listBySession(ctx: ReadContext, args: ListOptions & { sessionId: string }) {
    return ctx.runQuery(this.component.queries.listBySession, args);
  }

  listByUser(ctx: ReadContext, args: ListOptions & { userId: string }) {
    return ctx.runQuery(this.component.queries.listByUser, args);
  }

  listByRequest(ctx: ReadContext, args: ListOptions & { requestId: string }) {
    return ctx.runQuery(this.component.queries.listByRequest, args);
  }

  listByEntity(ctx: ReadContext, args: ListOptions & { entityType: string; entityId: string }) {
    return ctx.runQuery(this.component.queries.listByEntity, args);
  }

  listRecent(ctx: ReadContext, args: ListOptions = {}) {
    return ctx.runQuery(this.component.queries.listRecent, args);
  }
}
