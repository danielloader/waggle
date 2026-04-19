import {
  createRootRoute,
  createRoute,
  createRouter,
  Navigate,
} from "@tanstack/react-router";
import { RootLayout } from "./routes/root";
import { EventsPage } from "./routes/EventsPage";
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

// Four peer routes that all render the same EventsPage component. Route
// path IS the dataset — no `?dataset=` param — so the URL stays canonical
// and the sidebar nav items map cleanly to the page you land on.
//
// Switching dataset from the pill inside the Define panel navigates
// between the routes, preserving the rest of the search state.
//
// Honeycomb draws the same lines: Traces / Logs / Metrics are distinct
// surfaces because the signals have different source topologies and
// different "detail" shapes. The /events surface sits alongside them as
// a cross-signal power-user view.
export const tracesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/traces",
  validateSearch: querySearchSchema,
  component: () => <EventsPage dataset="spans" path="/traces" />,
});
export const logsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/logs",
  validateSearch: querySearchSchema,
  component: () => <EventsPage dataset="logs" path="/logs" />,
});
export const metricsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/metrics",
  validateSearch: querySearchSchema,
  component: () => <EventsPage dataset="metrics" path="/metrics" />,
});
export const eventsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/events",
  validateSearch: querySearchSchema,
  component: () => <EventsPage dataset="events" path="/events" />,
});

// Trace-detail waterfall — specialised view, kept outside the unified page.
const traceDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/traces/$traceId",
  component: function TraceDetailRoute() {
    const { traceId } = traceDetailRoute.useParams();
    return <TraceView traceID={traceId} />;
  },
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  tracesRoute,
  logsRoute,
  metricsRoute,
  eventsRoute,
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
