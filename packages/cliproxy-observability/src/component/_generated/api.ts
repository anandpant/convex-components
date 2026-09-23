/* eslint-disable */

import type * as ingest from "../ingest.js";
import type * as queries from "../queries.js";
import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";
import { anyApi, componentsGeneric } from "convex/server";

const fullApi: ApiFromModules<{
  ingest: typeof ingest;
  queries: typeof queries;
}> = anyApi as any;

export const api: FilterApi<typeof fullApi, FunctionReference<any, "public">> = anyApi as any;
export const internal: FilterApi<typeof fullApi, FunctionReference<any, "internal">> = anyApi as any;
export const components = componentsGeneric() as unknown as {};
