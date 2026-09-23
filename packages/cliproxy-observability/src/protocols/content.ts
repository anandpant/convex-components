import { z } from "zod";
const toolCall = z.object({
  id: z.string(),
  type: z.literal("function"),
  function: z.object({ name: z.string(), arguments: z.string() }),
});
const message = z
  .object({
    role: z.string().optional(),
    content: z.unknown().optional(),
    tool_call_id: z.string().optional(),
    tool_calls: z.array(toolCall).optional(),
  })
  .loose();
const output = z.object({
  format: z.literal("cliproxy_response_v1"),
  protocol: z.enum(["chat_completions", "anthropic_messages", "responses"]),
  completion: z.string(),
  reasoning: z.string(),
  messages: z.array(message),
  native: z.record(z.string(), z.unknown()),
});
export type CliproxyOutput = z.infer<typeof output>;
export function decodeCliproxyOutput(
  raw?: string,
):
  | { state: "absent" | "invalid" | "unsupported" | "truncated" }
  | { state: "decoded"; content: CliproxyOutput } {
  if (raw === undefined) return { state: "absent" };
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return { state: "invalid" };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return { state: "unsupported" };
  if (!("format" in value) || value.format !== "cliproxy_response_v1")
    return { state: "unsupported" };
  const parsed = output.safeParse(value);
  return parsed.success ? { state: "decoded", content: parsed.data } : { state: "invalid" };
}
