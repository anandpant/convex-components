import { httpRouter } from "convex/server";
import { internal } from "./_generated/api.js";
import { env, httpAction } from "./_generated/server.js";

const http = httpRouter();
const MAX_BODY_BYTES = 900 * 1024;
const BEARER_PREFIX = "Bearer ";
const encoder = new TextEncoder();

async function sha256(value: string) {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(value)));
}

async function constantTimeEqual(left: string, right: string) {
  const [leftDigest, rightDigest] = await Promise.all([sha256(left), sha256(right)]);
  let difference = 0;
  for (let index = 0; index < leftDigest.length; index += 1) {
    difference |= leftDigest[index] ^ rightDigest[index];
  }
  return difference === 0;
}

function suppliedBearerToken(request: Request) {
  const authorization = request.headers.get("authorization");
  if (!authorization?.startsWith(BEARER_PREFIX)) return "";
  return authorization.slice(BEARER_PREFIX.length);
}

async function hasValidBearerToken(request: Request) {
  const suppliedToken = suppliedBearerToken(request);
  if (!env.WEBHOOK_TOKEN || suppliedToken.length === 0) return false;
  return await constantTimeEqual(suppliedToken, env.WEBHOOK_TOKEN);
}

function isApplicationJson(request: Request) {
  const contentType = request.headers.get("content-type");
  return contentType?.split(";", 1)[0]?.trim().toLowerCase() === "application/json";
}

function isTestConnection(request: Request) {
  return request.headers.get("x-test-connection")?.trim().toLowerCase() === "true";
}

function declaredBodySize(request: Request) {
  const value = request.headers.get("content-length");
  if (value === null || !/^\d+$/.test(value)) return undefined;
  const size = Number(value);
  return Number.isSafeInteger(size) ? size : undefined;
}

export async function readBoundedBody(request: Request) {
  if ((declaredBodySize(request) ?? 0) > MAX_BODY_BYTES) {
    await request.body?.cancel("Payload too large");
    return { kind: "too_large" } as const;
  }

  const reader = request.body?.getReader();
  if (!reader) return { kind: "body", text: "" } as const;
  const decoder = new TextDecoder();
  const chunks: string[] = [];
  let byteLength = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      byteLength += value.byteLength;
      if (byteLength > MAX_BODY_BYTES) {
        await reader.cancel("Payload too large");
        return { kind: "too_large" } as const;
      }
      chunks.push(decoder.decode(value, { stream: true }));
    }
    chunks.push(decoder.decode());
    return { kind: "body", text: chunks.join("") } as const;
  } finally {
    reader.releaseLock();
  }
}

function admissionResponse(
  admission:
    | { kind: "accepted"; admitted: number }
    | { kind: "test_connection" }
    | { kind: "unavailable" }
    | { kind: "rejected"; status: number; message: string },
) {
  if (admission.kind === "rejected") {
    return new Response(admission.message, { status: admission.status });
  }
  if (admission.kind === "unavailable") {
    return new Response("Admission unavailable", { status: 503 });
  }
  if (admission.kind === "test_connection" || admission.admitted === 0) {
    return new Response(null, { status: 204 });
  }
  return new Response(null, { status: 202 });
}

const captureTraces = httpAction(async (ctx, request) => {
  if (!(await hasValidBearerToken(request))) {
    return new Response("Unauthorized", { status: 401 });
  }
  if (!isApplicationJson(request)) {
    return new Response("Content-Type must be application/json", { status: 415 });
  }

  const body = await readBoundedBody(request);
  if (body.kind === "too_large") {
    return new Response("Payload too large", { status: 413 });
  }

  try {
    const admission = await ctx.runMutation(internal.ingest.admit, {
      isTestConnection: isTestConnection(request),
      rawBody: body.text,
    });
    return admissionResponse(admission);
  } catch (error) {
    console.error("OpenRouter trace admission failed", error);
    return new Response("Admission unavailable", { status: 503 });
  }
});

for (const method of ["POST", "PUT"] as const) {
  http.route({ path: "/traces", method, handler: captureTraces });
}

http.route({
  path: "/health",
  method: "GET",
  handler: httpAction(async () => new Response("ok", { status: 200 })),
});

export default http;
