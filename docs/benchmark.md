# Query Performance Benchmark

Baseline timings for a 10 million event SQLite database, covering the main
query shapes the UI and API emit. Run on a MacBook (Apple Silicon, NVMe SSD,
`modernc.org/sqlite` pure-Go driver, WAL mode).

## Dataset

| Signal | Rows | Ratio |
|--------|------|-------|
| Spans  | 6,000,000 | 60% |
| Logs   | 1,000,000 | 10% |
| Metrics | 3,000,000 | 30% |
| **Total** | **10,000,000** | |

- **DB size on disk:** 4.42 GB (post-checkpoint, WAL truncated)
- **Schema:** v3 — `events` (spans+logs) + `metric_events` (folded Honeycomb-style)
- **Services:** 8 × 2 instances = 16 resources, timestamps spread over 24 h
- **Generation time:** 18m31s with `cmd/loadgen-bulk`

## Reproduction

### 1. Generate the test database

```bash
# Creates ./waggle-test.db (~4.5 GB, takes ~20 min)
go tool task loadgen-bulk -- --db ./waggle-test.db --total 10000000
```

Use `--seed N` for a reproducible dataset, otherwise the seed is time-based.
Full flag reference: `go run ./cmd/loadgen-bulk --help`.

### 2. Run the benchmark

```bash
# 3 runs per query, reports min/median/max
go tool task bench-queries -- --db ./waggle-test.db --runs 3
```

## Results (3-run median, April 2026)

All timings are the median of 3 consecutive runs against a warm OS page cache.
`Rows` is the number of rows returned by the query.

| # | Query | Rows | Min | Median | Max |
|---|-------|------|-----|--------|-----|
| 1 | LIST services (span count + error rate per service) | 8 | 9.148s | 9.184s | 9.207s |
| 2 | LIST traces (root spans + span\_count correlated subquery, LIMIT 50) | 50 | 1ms | 2ms | 3ms |
| 3 | COUNT(\*) all spans | 1 | 506ms | 526ms | 547ms |
| 4 | COUNT(\*) all logs | 1 | 75ms | 75ms | 84ms |
| 5 | COUNT(\*) all metric events | 1 | 3ms | 4ms | 26ms |
| 6 | COUNT(\*) spans GROUP BY service\_name | 8 | 8.886s | 8.947s | 9.158s |
| 7 | COUNT(\*) spans GROUP BY http\_route | 24 | 10.006s | 10.14s | 10.144s |
| 8 | COUNT(\*) spans GROUP BY span\_kind | 3 | 12.107s | 12.171s | 12.184s |
| 9 | Error rate per service (status\_code=2) | 8 | 9.055s | 9.073s | 9.165s |
| 10 | P50/P95/P99 duration\_ns by service (percentile UDF) | 8 | 11.942s | 12.022s | 12.083s |
| 11 | COUNT(\*) spans bucketed by 1h over full window | 25 | 2.993s | 3.011s | 3.093s |
| 12 | COUNT(\*) spans bucketed 1h + GROUP BY service (multi-series) | 200 | 12.003s | 12.054s | 12.296s |
| 13 | P95 duration\_ns bucketed 1h (percentile time-series) | 25 | 9.357s | 9.424s | 9.543s |
| 14 | Raw span rows LIMIT 500 ORDER BY time DESC | 500 | 1ms | 1ms | 2ms |
| 15 | COUNT(\*) spans filtered by service + quarter-window time range | 1 | 37ms | 38ms | 42ms |
| 16 | COUNT(\*) spans WHERE http\_route = '/users' | 1 | 9.456s | 9.533s | 9.649s |
| 17 | GET single trace (all spans for one trace\_id) | 8 | <1ms | <1ms | <1ms |
| 18 | COUNT(\*) logs GROUP BY severity\_text | 4 | 2.559s | 2.563s | 2.582s |
| 19 | Raw log rows LIMIT 500 ORDER BY time DESC | 500 | 1ms | 1ms | 2ms |
| 20 | FTS log search: body MATCH 'payment' | 100 | 370ms | 371ms | 372ms |
| 21 | COUNT(\*) metric\_events GROUP BY service\_name | 8 | 250ms | 253ms | 254ms |
| 22 | MAX(requests.total) metric GROUP BY service\_name (json\_extract) | 8 | 11.987s | 12.021s | 12.108s |
| 23 | AVG(cpu.utilization) metric bucketed 1h (json\_extract) | 25 | 13.039s | 13.061s | 13.099s |
| 24 | COUNT(\*) spans WHERE json\_extract(attributes, '$.component') = 'middleware' | 1 | 9.511s | 9.686s | 9.713s |
| 25 | COUNT(DISTINCT trace\_id) — trace cardinality | 1 | 9.496s | 9.635s | 9.752s |
| 26 | COUNT(\*) root spans only (partial index on parent\_span\_id IS NULL) | 1 | 5.432s | 5.436s | 5.455s |
| 27 | Top-100 slowest spans (ORDER BY duration\_ns DESC) | 100 | 6.093s | 6.103s | 6.113s |
| 28 | COUNT(\*) + AVG duration of ERROR spans (status\_code=2) by service | 8 | 5.563s | 5.568s | 5.597s |
| 29 | LIST attribute\_keys for spans (field catalog) | 88 | <1ms | <1ms | <1ms |
| 30 | COUNT(\*) entire events table (all signals) | 1 | 125ms | 125ms | 125ms |
