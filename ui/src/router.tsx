import {
  createRootRoute,
  createRoute,
  createRouter,
  Navigate,
} from "@tanstack/react-router";
import { z } from "zod";
import { RootLayout } from "./routes/root";
import { EventsPage } from "./routes/EventsPage";
import { TraceView } from "./features/traces/TraceView";
import { querySearchSchema } from "./lib/query";

// Trace route accepts an optional ?span=<id> to deep-link into a specific
// span — e.g. clicking the trace link on a log row opens the waterfall
// with that log's emitting span pre-selected and scrolled into view.
const traceDetailSearchSchema = z.object({
  span: z.string().optional(),
});

const rootRoute = createRootRoute({
  component: RootLayout,
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => <Navigate to="/events" search={{}} replace />,
});

// /events — unified wide-events view. The URL-level search carries a
// `dataset` field that picks an optional signal_type preset filter
// (spans / logs / metrics) or runs across all signals (events). Everything
// else on the page (Define, Chart, Explore, waterfall navigation) is
// dataset-agnostic.
export const eventsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/events",
  validateSearch: querySearchSchema,
  component: EventsPage,
});

// Legacy redirects. Old /traces|/logs|/metrics URLs land on /events with
// the equivalent dataset preset so shared links keep working.
const tracesRedirect = createRoute({
  getParentRoute: () => rootRoute,
  path: "/traces",
  component: () => <Navigate to="/events" search={{ dataset: "spans" }} replace />,
});
const logsRedirect = createRoute({
  getParentRoute: () => rootRoute,
  path: "/logs",
  component: () => <Navigate to="/events" search={{ dataset: "logs" }} replace />,
});
const metricsRedirect = createRoute({
  getParentRoute: () => rootRoute,
  path: "/metrics",
  component: () => <Navigate to="/events" search={{ dataset: "metrics" }} replace />,
});

// Trace-detail waterfall — specialised view, kept outside the unified page.
const traceDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/traces/$traceId",
  validateSearch: traceDetailSearchSchema,
  component: function TraceDetailRoute() {
    const { traceId } = traceDetailRoute.useParams();
    const { span } = traceDetailRoute.useSearch();
    return <TraceView traceID={traceId} initialSpanID={span ?? null} />;
  },
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  eventsRoute,
  tracesRedirect,
  logsRedirect,
  metricsRedirect,
  traceDetailRoute,
]);

export const router = createRouter({
  routeTree,
  defaultPreload: "intent",
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
