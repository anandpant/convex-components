export const metadata = {
  type: "response.metadata",
  response_id: "response-fixture",
  sequence_number: 1,
  metadata: { routing: { lane: "fixture" }, flags: ["synthetic"] },
};
export const terminal = {
  type: "response.completed",
  sequence_number: 2,
  response: {
    object: "response",
    id: "response-fixture",
    status: "completed",
    metadata: { request_label: "fixture" },
    output: [
      {
        type: "message",
        id: "message-fixture",
        content: [{ type: "output_text", text: "Built." }],
      },
      {
        type: "reasoning",
        id: "reasoning-fixture",
        summary: [{ type: "summary_text", text: "Checked." }],
      },
      {
        type: "function_call",
        id: "tool-fixture",
        call_id: "call-fixture",
        name: "build",
        arguments: '{"size":2}',
      },
    ],
    usage: {
      input_tokens: 60,
      output_tokens: 5,
      total_tokens: 65,
      input_tokens_details: { cached_tokens: 20, cache_write_tokens: 30 },
      output_tokens_details: { reasoning_tokens: 2 },
    },
  },
};
export const streamBytes = (events: object[]) =>
  Buffer.from(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""));
