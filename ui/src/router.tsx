import {
  createRootRoute,
  createRoute,
  createRouter,
  Navigate,
} from "@tanstack/react-router";
import { z } from "zod";
import { RootLayout } from "./routes/root";
import { EventsPage } from "./routes/EventsPage";
import { HistoryPage } from "./routes/HistoryPage";
import { TraceView } from "./features/traces/TraceView";
import { querySearchSchema } from "./lib/query";

// Trace route accepts:
//   ?span=<id>   — deep-link into a specific span (clicking a log row's
//                  trace link pre-selects the emitting span).
//   ?from=<hex>  — content-hash of the originating /events query
//                  (query_history). The "filter by" affordance on span
//                  attributes uses this to round-trip back to the source
//                  query with the new filter merged in. 64-char lower
//                  hex (SHA-256). Optional — deep-links from outside
//                  (e.g. shared trace URLs) just don't have it, and the
//                  filter button falls back to a default /events view.
//   ?tab=<id>    — originating /events tab (Overview/Traces/Explore/
//                  Tail). Round-tripped purely so filter-by lands the
//                  user back on the same tab they clicked from. The
//                  tab isn't part of the Query AST stored in
//                  query_history, so we pass it alongside `from`
//                  rather than inside it.
const traceDetailSearchSchema = z.object({
  span: z.string().optional(),
  from: z.string().regex(/^[0-9a-f]{64}$/).optional(),
  tab: z.enum(["overview", "traces", "explore", "tail"]).optional(),
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

// Query history — dedup'd list of recent queries, rehydrate + re-run.
const historyRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/history",
  component: HistoryPage,
});

// Trace-detail waterfall — specialised view, kept outside the unified page.
const traceDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/traces/$traceId",
  validateSearch: traceDetailSearchSchema,
  component: function TraceDetailRoute() {
    const { traceId } = traceDetailRoute.useParams();
    const { span, from, tab } = traceDetailRoute.useSearch();
    return (
      <TraceView
        traceID={traceId}
        initialSpanID={span ?? null}
        fromHash={from ?? null}
        fromTab={tab ?? null}
      />
    );
  },
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  eventsRoute,
  tracesRedirect,
  logsRedirect,
  metricsRedirect,
  historyRoute,
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
