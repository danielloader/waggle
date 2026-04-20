/** Thin typed fetch helpers against the Go backend. */

export interface ServiceSummary {
  service: string;
  span_count: number;
  error_count: number;
  error_rate: number;
}

export interface TraceSummary {
  trace_id: string;
  root_service: string;
  root_name: string;
  start_ns: number;
  duration_ns: number;
  span_count: number;
  has_error: boolean;
}

export interface SpanOut {
  trace_id: string;
  span_id: string;
  parent_span_id?: string;
  resource_id: number;
  service_name: string;
  name: string;
  /**
   * OTel SpanKind enum name, lowercased — "server" | "client" | "internal" |
   * "producer" | "consumer" | "" (unspecified). Mirrors meta.span_kind but
   * exposed as a top-level field for convenience.
   */
  kind: string;
  start_ns: number;
  end_ns: number;
  duration_ns: number;
  status_code: number;
  status_message?: string;
  attributes: string;
  events?: { time_ns: number; name: string; attributes: string }[];
  links?: { linked_trace_id: string; linked_span_id: string; attributes?: string }[];
}

export interface TraceResource {
  ID: number;
  ServiceName: string;
  ServiceNamespace?: string;
  ServiceVersion?: string;
  ServiceInstanceID?: string;
  SDKName?: string;
  SDKLanguage?: string;
  SDKVersion?: string;
  AttributesJSON: string;
  FirstSeenNS?: number;
  LastSeenNS?: number;
}

export interface TraceDetail {
  trace_id: string;
  spans: SpanOut[];
  resources: Record<string, TraceResource>;
}

export interface FieldInfo {
  key: string;
  type: string;
  count: number;
}

export interface LogOut {
  log_id: number;
  time_ns: number;
  service: string;
  severity: string;
  severity_number: number;
  body: string;
  trace_id?: string;
  span_id?: string;
  attributes: string;
}

/**
 * HttpError carries the status code alongside the parsed body so callers
 * can switch on 404/400 without string-matching the message.
 */
export class HttpError extends Error {
  status: number;
  statusText: string;
  body: string;
  constructor(status: number, statusText: string, body: string) {
    super(`${status} ${statusText}: ${body}`);
    this.status = status;
    this.statusText = statusText;
    this.body = body;
  }
}

async function getJSON<T>(url: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(url, { signal });
  if (!res.ok) {
    throw new HttpError(res.status, res.statusText, await res.text());
  }
  return (await res.json()) as T;
}

export const api = {
  listServices: (signal?: AbortSignal) =>
    getJSON<{ services: ServiceSummary[] }>("/api/services", signal),

  listTraces: (params: URLSearchParams, signal?: AbortSignal) =>
    getJSON<{ traces: TraceSummary[]; next_cursor: string }>(
      `/api/traces?${params.toString()}`,
      signal,
    ),

  getTrace: (traceID: string, signal?: AbortSignal) =>
    getJSON<TraceDetail>(`/api/traces/${traceID}`, signal),

  listFields: (params: URLSearchParams, signal?: AbortSignal) =>
    getJSON<{ fields: FieldInfo[] }>(`/api/fields?${params.toString()}`, signal),

  listFieldValues: (key: string, params: URLSearchParams, signal?: AbortSignal) =>
    getJSON<{ values: string[] }>(
      `/api/fields/${encodeURIComponent(key)}/values?${params.toString()}`,
      signal,
    ),

  listSpanNames: (params: URLSearchParams, signal?: AbortSignal) =>
    getJSON<{ names: string[] }>(`/api/span-names?${params.toString()}`, signal),

  searchLogs: (params: URLSearchParams, signal?: AbortSignal) =>
    getJSON<{ logs: LogOut[]; next_cursor: string }>(
      `/api/logs/search?${params.toString()}`,
      signal,
    ),

  listHistory: (limit: number, signal?: AbortSignal) =>
    getJSON<{ entries: QueryHistoryEntry[] }>(
      `/api/history?limit=${limit}`,
      signal,
    ),
};

export interface QueryHistoryEntry {
  id: number;
  dataset: string;
  query_json: string;
  display_text: string;
  run_count: number;
  first_run_ns: number;
  last_run_ns: number;
}
