/* eslint-disable */

import type { FunctionReference, RegisteredMutation, RegisteredQuery } from "convex/server";
import type * as ingest from "../ingest.js";
import type * as queries from "../queries.js";

type ComponentQuery<Registered, Name extends string | undefined> =
  Registered extends RegisteredQuery<any, infer Args, infer Returns>
    ? FunctionReference<"query", "internal", Args, Awaited<Returns>, Name>
    : never;

type PublicQueries<Module, Name extends string | undefined> = {
  [Key in keyof Module as Module[Key] extends RegisteredQuery<"public", any, any>
    ? Key
    : never]: ComponentQuery<Module[Key], Name>;
};

type ComponentMutation<Registered, Name extends string | undefined> =
  Registered extends RegisteredMutation<any, infer Args, infer Returns>
    ? FunctionReference<"mutation", "internal", Args, Awaited<Returns>, Name>
    : never;

type PublicMutations<Module, Name extends string | undefined> = {
  [Key in keyof Module as Module[Key] extends RegisteredMutation<"public", any, any>
    ? Key
    : never]: ComponentMutation<Module[Key], Name>;
};

export type ComponentApi<Name extends string | undefined = string | undefined> = {
  ingest: PublicMutations<typeof ingest, Name>;
  queries: PublicQueries<typeof queries, Name>;
};
