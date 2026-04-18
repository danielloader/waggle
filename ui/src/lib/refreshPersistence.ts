import { useEffect } from "react";
import {
  REFRESH_RATES,
  type QuerySearch,
  type RefreshRate,
} from "./query";

const STORAGE_KEY = "waggle.refresh";

/**
 * Persist the auto-refresh cadence across page loads.
 *
 * URL wins for shared links — if the current URL explicitly carries a
 * `refresh` param, we leave it alone and simply mirror it into storage.
 * Otherwise, on mount, we seed from localStorage so a user's preferred
 * cadence survives a hard reload / bookmark open that doesn't carry the
 * param in its query string.
 *
 * The same key is used on both /traces and /logs so "I always want 30s"
 * applies uniformly rather than being per-dataset.
 */
export function useRefreshPersistence(
  search: QuerySearch,
  setSearch: (next: QuerySearch) => void,
) {
  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams(window.location.search);
    if (params.has("refresh")) return;
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (!stored || !isRefreshRate(stored)) return;
    if (stored === search.refresh) return;
    setSearch({ ...search, refresh: stored });
    // Run once on mount — we're seeding from storage, not reacting to
    // subsequent changes. The write-back effect below handles those.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(STORAGE_KEY, search.refresh);
  }, [search.refresh]);
}

function isRefreshRate(s: string): s is RefreshRate {
  return (REFRESH_RATES as readonly string[]).includes(s);
}
