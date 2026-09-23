import type { CliproxyProjection, RecordValue } from "./types.js";
import { count, object } from "./values.js";
export function extractUsage(projection: CliproxyProjection, raw: RecordValue) {
  // Anthropic reports disjoint uncached/cache creation/cache read input counts.
  // Never manufacture totals, costs, or token counts from byte/text lengths.
  projection.metadata.usage = raw;
  const anthropic = projection.protocol === "anthropic_messages";

  const responses = projection.protocol === "responses";

  const nativeInput = count(raw.input_tokens);
  const cacheRead = count(raw.cache_read_input_tokens);
  const cacheCreated = count(raw.cache_creation_input_tokens);
  const anthropicInput =
    nativeInput !== undefined && cacheRead !== undefined && cacheCreated !== undefined
      ? count(nativeInput + cacheRead + cacheCreated)
      : undefined;
  Object.assign(projection.scalars, {
    inputTokens: anthropic ? anthropicInput : responses ? nativeInput : count(raw.prompt_tokens),
    outputTokens: count(raw[anthropic || responses ? "output_tokens" : "completion_tokens"]),
    totalTokens: count(raw.total_tokens),
    cachedInputTokens: count(
      anthropic
        ? raw.cache_read_input_tokens
        : object(raw[responses ? "input_tokens_details" : "prompt_tokens_details"]).cached_tokens,
    ),
    reasoningTokens: count(
      object(raw[anthropic || responses ? "output_tokens_details" : "completion_tokens_details"])[
        anthropic ? "thinking_tokens" : "reasoning_tokens"
      ],
    ),
  });
  projection.metadata.tokenSemantics = anthropic
    ? "input_is_sum_of_reported_input_cache_creation_cache_read; cached_input_is_cache_read; missing_components_mean_unknown; no_synthesized_total"
    : "input_includes_reported_cached_input; terminal_usage_snapshot";
}
