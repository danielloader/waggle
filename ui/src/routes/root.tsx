import { useEffect, useState } from "react";
import { Link, Outlet, useLocation } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  History,
  PanelLeftClose,
  PanelLeft,
} from "lucide-react";
import clsx from "clsx";

const STORAGE_KEY = "waggle.sidebar.collapsed";

interface HealthResponse {
  ok: boolean;
  listen_addr?: string;
}

export function RootLayout() {
  const { pathname } = useLocation();
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return window.localStorage.getItem(STORAGE_KEY) === "1";
  });

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEY, collapsed ? "1" : "0");
  }, [collapsed]);

  // Health probe — reads back the server's real listen address so the
  // footer doesn't lie when waggle runs on a non-default --addr.
  const health = useQuery<HealthResponse>({
    queryKey: ["health"],
    queryFn: async () => {
      const res = await fetch("/api/health");
      if (!res.ok) throw new Error(String(res.status));
      return (await res.json()) as HealthResponse;
    },
    staleTime: 60_000,
    refetchOnWindowFocus: false,
  });
  // addressPort extracts "[::]:4318" → ":4318" so the footer renders as
  // a short port tag rather than repeating "127.0.0.1".
  const addressPort = (() => {
    const addr = health.data?.listen_addr ?? "";
    const colon = addr.lastIndexOf(":");
    return colon >= 0 ? addr.slice(colon) : addr;
  })();

  const navItem = (
    to: string,
    label: string,
    icon: React.ReactNode,
    active: boolean,
  ) => (
    <Link
      to={to}
      className={clsx(
        "flex items-center rounded-md text-sm",
        collapsed ? "justify-center p-2" : "gap-2 px-3 py-2",
        active
          ? "bg-[var(--color-accent)] text-white"
          : "text-[var(--color-ink)] hover:bg-[var(--color-border)]/50",
      )}
      title={collapsed ? label : undefined}
    >
      <span className="w-4 h-4 flex items-center justify-center">{icon}</span>
      {!collapsed && <span>{label}</span>}
    </Link>
  );

  return (
    <div className="h-full flex">
      <aside
        className={clsx(
          "border-r flex flex-col transition-[width] duration-150",
          collapsed ? "w-14" : "w-56",
        )}
        style={{
          background: "var(--color-card)",
          borderColor: "var(--color-border)",
        }}
      >
        <div
          className={clsx(
            "flex items-center border-b",
            collapsed ? "justify-center px-2 py-3" : "justify-between px-3 py-3",
          )}
          style={{ borderColor: "var(--color-border)" }}
        >
          {!collapsed && (
            <span className="flex items-center gap-1.5 font-semibold">
              <img src="/favicon.svg" alt="" className="w-4 h-4" />
              waggle
            </span>
          )}
          <button
            type="button"
            onClick={() => setCollapsed((v) => !v)}
            className="p-1 rounded hover:bg-[var(--color-border)]/50"
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? (
              <PanelLeft className="w-4 h-4" />
            ) : (
              <PanelLeftClose className="w-4 h-4" />
            )}
          </button>
        </div>
        <nav className="p-2 flex flex-col gap-1 flex-1">
          {navItem(
            "/events",
            "Events",
            <Activity />,
            pathname === "/" || pathname.startsWith("/events") || pathname === "/traces" || pathname === "/logs" || pathname === "/metrics",
          )}
          {navItem(
            "/history",
            "History",
            <History />,
            pathname.startsWith("/history"),
          )}
        </nav>
        <div
          className={clsx(
            "text-xs border-t flex items-center",
            collapsed ? "justify-center px-2 py-2" : "gap-2 px-3 py-2.5",
          )}
          style={{
            color: "var(--color-ink-muted)",
            borderColor: "var(--color-border)",
          }}
          title={
            health.isError
              ? "Waggle isn't responding"
              : `Waggle is listening on ${health.data?.listen_addr ?? ""} for OTLP/HTTP`
          }
        >
          <span
            className="inline-block w-1.5 h-1.5 rounded-full shrink-0"
            style={{
              background: health.isError
                ? "var(--color-error)"
                : "var(--color-ok)",
            }}
          />
          {!collapsed && (
            <span className="truncate">
              {health.isError
                ? "offline"
                : `OTLP/HTTP on ${addressPort || "…"}`}
            </span>
          )}
        </div>
      </aside>
      <main className="flex-1 overflow-auto min-w-0">
        <Outlet />
      </main>
    </div>
  );
}
