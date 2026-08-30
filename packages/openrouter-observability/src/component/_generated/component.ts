/* eslint-disable */

import type { FunctionReference, RegisteredQuery } from "convex/server";
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

export type ComponentApi<Name extends string | undefined = string | undefined> = {
  queries: PublicQueries<typeof queries, Name>;
};
