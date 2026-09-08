import { httpRouter } from "convex/server";

// Ingestion belongs to the host HTTP router because blob credentials and
// storage lifetime are host concerns. This empty router prevents older mounted
// component routes from accepting content without the host storage adapter.
export default httpRouter();
