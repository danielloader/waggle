import { useEffect, useState } from "react";
import { Link, Outlet, useLocation } from "@tanstack/react-router";
import { Activity, ScrollText, PanelLeftClose, PanelLeft } from "lucide-react";
import clsx from "clsx";

const STORAGE_KEY = "waggle.sidebar.collapsed";

export function RootLayout() {
  const { pathname } = useLocation();
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return window.localStorage.getItem(STORAGE_KEY) === "1";
  });

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEY, collapsed ? "1" : "0");
  }, [collapsed]);

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
          background: "var(--color-surface)",
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
          {!collapsed && <span className="font-semibold">waggle</span>}
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
          {navItem("/traces", "Traces", <Activity />, pathname.startsWith("/traces"))}
          {navItem("/logs", "Logs", <ScrollText />, pathname.startsWith("/logs"))}
        </nav>
        {!collapsed && (
          <div
            className="p-3 text-xs border-t"
            style={{
              color: "var(--color-ink-muted)",
              borderColor: "var(--color-border)",
            }}
          >
            Listening on :4318 for OTLP/HTTP
          </div>
        )}
      </aside>
      <main className="flex-1 overflow-auto min-w-0">
        <Outlet />
      </main>
    </div>
  );
}
