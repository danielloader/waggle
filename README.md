# waggle

Local OpenTelemetry viewer inspired by Honeycomb — named for the
[waggle dance](https://en.wikipedia.org/wiki/Waggle_dance) bees use to share
locations. Run it next to your service, point any OTLP/HTTP exporter at
`http://localhost:4318`, and browse a Honeycomb-style trace waterfall, log
explorer, and structured query builder in the same tab.

- Single static binary — pure Go, no CGO, no Docker required, no Node at runtime.
- OTLP/HTTP ingest (protobuf + JSON) on `POST /v1/traces` and `POST /v1/logs`.
- Persists to a single SQLite file (WAL mode; FTS5 for log search).
- Embedded React UI served from the same port.

## Screenshots

Honeycomb-style query builder over traces — filters, group-by, aggregates,
time range, all serialized to the URL:

![Trace list with query header](docs/traces-list.png)

Query builder in action — the screenshot below breaks down every HTTP
operation in the last 6 hours by status code and span name. The Define
row reads:

- **Select** `COUNT` — how many spans match.
- **Where** `http.response.status_code exists` — keep only spans that
  carry an HTTP status, filtering out unrelated internal / DB work
  *before* aggregation.
- **Group by** `http.response.status_code, name` — split the count into
  one row per (status × operation) pair so 200 POST /checkout and 500
  GET /reports appear separately.
- **Order by** `count desc` — noisiest combinations float to the top.
- **Limit** `1000` — cap the result set.

The chart timeseries renders one line per group so error codes stand
out against the 200s at a glance, and the Overview tab lists the raw
rows ordered by volume.

![Aggregation with group-by](docs/query-builder.png)

Trace waterfall with per-span detail:

![Trace waterfall](docs/trace-waterfall.png)

Log explorer with FTS5 search:

![Log explorer](docs/logs.png)

## Install

**Binary** — grab a release archive from
[Releases](https://github.com/danielloader/waggle/releases), extract, and run:

```sh
./waggle
```

**Docker** — images are published to GitHub Container Registry:

```sh
docker run --rm -p 4318:4318 -v $(pwd)/data:/data \
  ghcr.io/danielloader/waggle:latest
```

**From source** — requires Go 1.26+ and Node 22+:

```sh
go tool task build
./bin/waggle
```

Once running, open `http://localhost:4318` and point any OTLP/HTTP exporter
(OpenTelemetry SDK defaults work) at the same URL.

## Usage

The UI has two views:

- `/traces` — trace list with a Honeycomb-style query builder (filters,
  group-by, aggregates, time range — all serialized to the URL so shared
  links reproduce the view). Click a trace to see the waterfall and span
  detail.
- `/logs` — FTS5-backed log search with the same query-builder surface plus
  a free-text search box.

## Config

All flags have matching environment variables. Flags take precedence.

| Flag | Env | Default | Notes |
| --- | --- | --- | --- |
| `--db-path` | `WAGGLE_DB` | `./waggle.db` | SQLite file path. |
| `--addr` | `WAGGLE_ADDR` | `127.0.0.1:4318` | Bind address for UI, API, and OTLP ingest. |
| `--ingest-addr` | `WAGGLE_INGEST_ADDR` | — | Override to split OTLP ingest onto its own listener. |
| `--ui-addr` | `WAGGLE_UI_ADDR` | — | Override to split the UI + API onto its own listener. |
| `--no-open-browser` | `WAGGLE_NO_OPEN` | `false` | Skip the browser auto-open on startup. |
| `--retention` | `WAGGLE_RETENTION` | `24h` | Drop data older than this (Go duration; `0` disables). |
| `--log-level` | `WAGGLE_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |
| `--dev` | — | `false` | Dev mode: do not serve embedded UI, do not open browser. |

When `--ingest-addr` and `--ui-addr` differ, waggle binds two HTTP listeners;
otherwise a single listener serves everything on `--addr`.

## Development

```sh
# Go (hot-reload via air) + Vite dev server, concurrently.
# Go listens on :4318, Vite on :5173 with /v1 and /api proxied to Go.
go tool task dev
```

One-time prerequisites:

```sh
go install github.com/air-verse/air@latest
(cd ui && npm install)
```

Tasks are defined in `Taskfile.yml` and run via
[go-task](https://taskfile.dev), which is pinned as a module-local tool in
`go.mod` — no system install needed. Run `go tool task` with no arguments to
list every target.

Useful targets:

| Task | What it does |
| --- | --- |
| `go tool task build` | Build the UI and compile a single static binary into `bin/waggle`. |
| `go tool task test` | Run Go and UI tests. |
| `go tool task typecheck` | `tsc --noEmit` + `go vet`. |
| `go tool task fmt` | `gofmt` + `goimports` on Go sources. |
| `go tool task loadgen -- --rate 20` | Stream realistic OTel traces at a running waggle — see `cmd/loadgen`. |
| `go tool task release:snapshot` | Local goreleaser snapshot (archives + Docker image, no publish). |

## Project layout

```text
cmd/
  waggle/       # server entry point
  loadgen/      # OTel trace load generator (real OTel SDK)
internal/
  api/          # JSON API for the UI (/api/*)
  config/       # flag + env parsing
  ingest/       # OTLP/HTTP decode + buffered writer
  otlp/         # OTLP -> internal model transform
  query/        # structured query builder (validates + compiles to SQL)
  server/       # HTTP wiring (ingest + UI/API listeners)
  store/        # storage seam + SQLite implementation (schema, queries)
  ui/           # embedded React build (//go:embed all:dist)
ui/             # Vite + React + TanStack Router + Tailwind source
```
