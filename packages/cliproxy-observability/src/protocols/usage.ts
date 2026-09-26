import { TOKEN_FIELDS, type TokenField } from "../model-call/index.js";
import type { CliproxyProjection, RecordValue } from "./types.js";
import { count } from "./values.js";
/** Names the rule below in each usage measurement. */
const USAGE_SEMANTICS = "client_protocol_usage_v2";
/**
 * The one token normalization rule, read from terminal native usage (`details.field` for
 * nested counts). `first` takes the first listed field reported. `base` needs its field and
 * adds each `plus` field, counting an absent one as 0. Input includes cache reads and writes,
 * as OpenAI-style usage reports it. Anthropic reports them apart from `input_tokens` and omits
 * them when no cache is used, as do CLIProxy's translators, so its input is the sum.
 * `totalTokens` is only ever the reported `total_tokens`; Anthropic reports none and it is
 * never summed. Nothing is manufactured from byte or text lengths.
 */
const TOKEN_NORMALIZATION: Record<
  Exclude<CliproxyProjection["protocol"], "unknown">,
  Record<TokenField, { first: readonly string[] } | { base: string; plus: readonly string[] }>
> = {
  anthropic_messages: {
    inputTokens: {
      base: "input_tokens",
      plus: ["cache_read_input_tokens", "cache_creation_input_tokens"],
    },
    outputTokens: { first: ["output_tokens"] },
    totalTokens: { first: ["total_tokens"] },
    reasoningTokens: { first: ["output_tokens_details.thinking_tokens"] },
    cachedInputTokens: { first: ["cache_read_input_tokens"] },
    cacheCreationInputTokens: { first: ["cache_creation_input_tokens"] },
  },
  responses: {
    inputTokens: { first: ["input_tokens"] },
    outputTokens: { first: ["output_tokens"] },
    totalTokens: { first: ["total_tokens"] },
    reasoningTokens: { first: ["output_tokens_details.reasoning_tokens"] },
    cachedInputTokens: { first: ["input_tokens_details.cached_tokens"] },
    cacheCreationInputTokens: {
      first: [
        "input_tokens_details.cache_write_tokens",
        "input_tokens_details.cache_creation_tokens",
      ],
    },
  },
  chat_completions: {
    inputTokens: { first: ["prompt_tokens"] },
    outputTokens: { first: ["completion_tokens"] },
    totalTokens: { first: ["total_tokens"] },
    reasoningTokens: { first: ["completion_tokens_details.reasoning_tokens"] },
    cachedInputTokens: { first: ["prompt_tokens_details.cached_tokens"] },
    cacheCreationInputTokens: {
      first: [
        "prompt_tokens_details.cache_write_tokens",
        "prompt_tokens_details.cached_creation_tokens",
        "prompt_tokens_details.cache_creation_tokens",
      ],
    },
  },
};
export function normalizeUsage(
  protocol: keyof typeof TOKEN_NORMALIZATION,
  reported: ReadonlyMap<string, number>,
): Partial<Record<TokenField, number>> {
  const out: Partial<Record<TokenField, number>> = {};
  for (const field of TOKEN_FIELDS) {
    const rule = TOKEN_NORMALIZATION[protocol][field];
    if ("first" in rule) {
      out[field] = rule.first
        .map((name) => reported.get(name))
        .find((value) => value !== undefined);
      continue;
    }
    const base = reported.get(rule.base);
    out[field] =
      base === undefined
        ? undefined
        : count(rule.plus.reduce((total, name) => total + (reported.get(name) ?? 0), base));
  }
  return out;
}
/** Native counts keyed as usage measurements name them. */
function reportedCounts(raw: RecordValue): Map<string, number> {
  const reported = new Map<string, number>();
  for (const [key, value] of Object.entries(raw)) {
    if (count(value) !== undefined) reported.set(key, value as number);
    else if (value && typeof value === "object" && !Array.isArray(value))
      for (const [field, nested] of Object.entries(value))
        if (count(nested) !== undefined) reported.set(`${key}.${field}`, nested as number);
  }
  return reported;
}
export function extractUsage(projection: CliproxyProjection, raw: RecordValue) {
  projection.metadata.usage = raw;
  projection.metadata.tokenSemantics = USAGE_SEMANTICS;
  if (projection.protocol !== "unknown")
    Object.assign(projection.scalars, normalizeUsage(projection.protocol, reportedCounts(raw)));
}
