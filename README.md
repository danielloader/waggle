# waggle

Local OTel viewer inspired by Honeycomb — named for the [waggle dance](https://en.wikipedia.org/wiki/Waggle_dance) bees use to share locations. Run it next to your service, point the OTel SDK at `http://localhost:4318`, and get a Honeycomb-style trace waterfall + log explorer + structured query builder on `http://localhost:4318` in the browser.

- Single static binary (pure-Go; no CGO, no Docker, no Node at runtime).
- Accepts OTLP/HTTP (protobuf + JSON) on `/v1/traces` and `/v1/logs`.
- Persists to one SQLite file; WAL mode; FTS5 for log search.

See the design notes and the SQLite schema rationale in the plan file.

## Quick start

```sh
go tool task build
./bin/waggle
```

Then point an OTel SDK at `http://localhost:4318` with the default
OTLP/HTTP exporter.

## Development

```sh
# Run Go (hot-reload via air) + Vite dev server concurrently.
# Go listens on :4318, Vite on :5173 with /v1 and /api proxied back.
go tool task dev
```

Install prerequisites once:

```sh
go install github.com/air-verse/air@latest
(cd ui && npm install)
```

Tasks are defined in `Taskfile.yml` and run via [go-task](https://taskfile.dev),
which is pinned as a module-local tool in `go.mod`. Run `go tool task` with no
arguments to see all targets.

## Config

| Flag | Env | Default |
|---|---|---|
| `--db-path` | `WAGGLE_DB` | `./waggle.db` |
| `--addr` | `WAGGLE_ADDR` | `127.0.0.1:4318` |
| `--ingest-addr` / `--ui-addr` | … | same as `--addr` |
| `--no-open-browser` | `WAGGLE_NO_OPEN` | `false` |
| `--retention` | `WAGGLE_RETENTION` | `24h` |
| `--log-level` | `WAGGLE_LOG_LEVEL` | `info` |
| `--dev` | — | `false` |
