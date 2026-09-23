/** Native CLIProxy application-log projection. Upstream attempts are never usage sources. */
export type ProjectionState = "absent" | "invalid" | "unsupported" | "truncated" | "decoded";
export type RecordValue = Record<string, unknown>;
export type CliproxyProjection = {
  version: 1;
  protocol: "chat_completions" | "anthropic_messages" | "responses" | "unknown";
  requestState: ProjectionState;
  responseState: ProjectionState;
  input?: RecordValue;
  output?: RecordValue;
  scalars: {
    requestModel?: string;
    responseModel?: string;
    generationId?: string;
    streamed?: boolean;
    startTimeUnixNano?: string;
    finishReason?: string;
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
    reasoningTokens?: number;
    cachedInputTokens?: number;
    statusJson?: string;
  };
  metadata: RecordValue;
};
