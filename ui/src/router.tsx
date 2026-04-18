import {
  createRootRoute,
  createRoute,
  createRouter,
  Navigate,
} from "@tanstack/react-router";
import { RootLayout } from "./routes/root";
import { TracesPage } from "./routes/TracesPage";
import { LogsPage } from "./routes/LogsPage";
import { MetricsPage } from "./routes/MetricsPage";
import { TraceView } from "./features/traces/TraceView";
import { querySearchSchema } from "./lib/query";

const rootRoute = createRootRoute({
  component: RootLayout,
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => <Navigate to="/traces" search={{}} replace />,
});

// /traces — trace list with query-header. Honeycomb-style URL persistence:
// everything in the query builder (filters, group-by, time range) serializes
// to the URL so a shared link reproduces the view.
export const tracesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/traces",
  validateSearch: querySearchSchema,
  component: TracesPage,
});

const traceDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/traces/$traceId",
  component: function TraceDetailRoute() {
    const { traceId } = traceDetailRoute.useParams();
    return <TraceView traceID={traceId} />;
  },
});

// /logs — same idea as /traces but with an extra FTS search input.
export const logsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/logs",
  validateSearch: querySearchSchema,
  component: LogsPage,
});

// /metrics — metric-name picker up top; same Define/Chart/Explore
// skeleton as the other datasets, query engine running against the
// metric_points + metric_series join.
export const metricsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/metrics",
  validateSearch: querySearchSchema,
  component: MetricsPage,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  tracesRoute,
  traceDetailRoute,
  logsRoute,
  metricsRoute,
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
