# Relational query fields

Status: **Shelved** — picked up later. This file is the design-of-record so
future work can start without re-deriving the semantics or SQL shape.

## 1. What this feature is

Honeycomb lets you write span-level queries that actually filter at
**trace-level** by using prefixed field names. The prefixes each express a
relationship between the span the predicate applies to and the span set
of its trace. Reference:
[Honeycomb Relational Columns](https://docs.honeycomb.io/investigate/query/relational-fields/).

Our query engine (see `internal/query/`) is currently span-scoped and
log-scoped only. This plan adds trace-scoped mode, which kicks in whenever
a query carries at least one prefixed (relational) filter.

### Prefix cheatsheet

| Prefix | Meaning | Example (our syntax) |
|---|---|---|
| `root.<field>` | Predicate applies to the trace's root span (`parent_span_id IS NULL`) | `root.http.route = "/checkout"` |
| `parent.<field>` | Predicate applies to the **direct parent** of a span that *would otherwise match* the rest of the clause; i.e. constrains the parent | `parent.db.system = "postgresql"` |
| `child.<field>` | Predicate applies to a **direct child** of a span that would otherwise match | `child.status_code = 2` |
| `any.<field>` | **Exists** a span in the trace matching | `any.http.route = "/checkout"` |
| `any2.<field>` | Independent any-group; must match a **different** span from `any.` | `any.rpc.service = "Fraud" AND any2.db.system = "postgresql"` |
| `any3.<field>` | Third independent any-group (Honeycomb caps at 3) | … |
| `none.<field>` | **Not exists** a span in the trace matching | `none.db.system = "mysql"` |

### The property that makes this non-trivial

Within a single `any.` (or `any2.`, `any3.`) group, **all predicates with
that prefix must be satisfied by the same span**. This is why Honeycomb
has `any`, `any2`, `any3` rather than just `any` — so you can assert
"some span is POST and 500" vs "some span is POST and separately some span
is 500":

```
where: any.http.method = "POST" AND any.http.status_code = 500
  → one span that is POST AND 500

where: any.http.method = "POST" AND any2.http.status_code = 500
  → one POST span, separately one 500 span
```

## 2. User-facing examples

All payloads below go to `POST /api/query` — the wire shape is unchanged;
the `field` strings just carry the relational prefix.

**A. Traces that hit `/checkout` and had an error anywhere:**

```json
{
  "dataset": "spans",
  "time_range": {"from": "...", "to": "..."},
  "select": [{"op": "count"}],
  "where": [
    {"field": "root.http.route", "op": "=", "value": "/checkout"},
    {"field": "any.status_code",  "op": "=", "value": 2}
  ]
}
```

**B. p95 of root span duration, grouped by root service, for traces that
touched Postgres but not Redis:**

```json
{
  "dataset": "spans",
  "time_range": {"from": "...", "to": "..."},
  "select": [{"op": "p95", "field": "duration_ns"}],
  "where": [
    {"field": "any.db.system",  "op": "=", "value": "postgresql"},
    {"field": "none.db.system", "op": "=", "value": "redis"}
  ],
  "group_by": ["root.service.name"]
}
```

**C. Time-series trace count broken down by root http method:**

```json
{
  "dataset": "spans",
  "time_range": {"from": "...", "to": "..."},
  "select": [{"op": "count"}],
  "group_by": ["root.http.request.method"],
  "bucket_ms": 60000
}
```

## 3. What changes in the query engine — overview

1. **Parsing** — the validator/parser peels `root.`, `any.`, `any2.`,
   `any3.`, `none.` (and eventually `parent.`, `child.`) off each filter,
   normalizing the underlying field name and collecting the clauses into
   typed buckets.
2. **Trace-scoped mode** — when any relational clause exists, the builder
   switches to CTE form. `count()` becomes `COUNT(DISTINCT trace_id)`. The
   outer SELECT runs over root spans of qualifying traces.
3. **Non-relational predicates** — apply to root spans of qualifying
   traces by default (see §5 decision 2).
4. **GROUP BY** — resolved against root spans. `GROUP BY root.foo` is
   equivalent to `GROUP BY foo` in the outer query. `GROUP BY any.foo` is
   rejected (ambiguous — which matching span?).

## 4. SQL shape — full CTE template

### 4.1 Without `parent.`/`child.` (Phase A)

```sql
WITH qualifying_traces AS (
    SELECT trace_id
    FROM spans
    WHERE start_time_ns >= ?            -- time range (from)
      AND start_time_ns <  ?            -- time range (to)
    GROUP BY trace_id
    HAVING
        -- root. clauses: exactly one root span (parent_span_id IS NULL)
        -- must satisfy all root. predicates together.
        MAX(CASE WHEN parent_span_id IS NULL
                      AND <root-pred-1>
                      AND <root-pred-2>
                      AND ...
                  THEN 1 ELSE 0 END) = 1

        -- any. group: one span must satisfy every "any." predicate.
        AND MAX(CASE WHEN <any1-pred-1>
                          AND <any1-pred-2>
                          AND ...
                      THEN 1 ELSE 0 END) = 1

        -- any2. group: one span must satisfy every "any2." predicate.
        AND MAX(CASE WHEN <any2-preds> THEN 1 ELSE 0 END) = 1

        -- none.: no span in the trace may satisfy the none. predicate.
        AND MAX(CASE WHEN <none-pred> THEN 1 ELSE 0 END) = 0
),
root_spans AS (
    SELECT s.*
    FROM spans s
    JOIN qualifying_traces t ON s.trace_id = t.trace_id
    WHERE s.parent_span_id IS NULL
)
SELECT <outer aggregations>
FROM root_spans
WHERE <non-relational filters>
GROUP BY <group keys>
ORDER BY ... LIMIT ...
```

Notes:
- The `HAVING MAX(CASE … THEN 1 ELSE 0 END) = 1` idiom is how we express
  "exists a row satisfying X" in a single-table GROUP BY — cheaper than
  subqueries and friendly to SQLite's planner.
- `= 0` on the `none.` variant is the cheap negation.
- Each `any` group becomes one `MAX(CASE WHEN … THEN 1 ELSE 0 END) = 1`
  clause with all that group's predicates AND-ed inside the CASE. That's
  where "same span must match" is enforced: a single row either satisfies
  all of them or doesn't.

### 4.2 With time-series (`bucket_ms`)

Same CTE, the outer query carries the bucket expression exactly as today:

```sql
-- outer SELECT
SELECT
    (start_time_ns / <bucket_ns>) * <bucket_ns> AS bucket_ns,
    <group keys>,
    COUNT(DISTINCT trace_id) AS count
FROM root_spans
WHERE ...
GROUP BY bucket_ns, <group keys>
ORDER BY bucket_ns ASC
```

Time bucketing uses the root span's `start_time_ns` — matches Honeycomb
and avoids double-counting.

### 4.3 `parent.` and `child.` (Phase B)

Don't fit the single-pass GROUP BY template. Use correlated `EXISTS`:

```sql
-- child.http.status_code = 500 at span level:
--   "spans whose direct child returned 500"
EXISTS (
    SELECT 1 FROM spans c
    WHERE c.trace_id = s.trace_id
      AND c.parent_span_id = s.span_id
      AND c.http_status_code = 500
)
```

Plug that into the CTE's HAVING (for trace-level existence) or into the
outer WHERE (for span-level "where the span has a child with …"). See §7.

## 5. Open decisions to resolve on pickup

Number these; answering them unblocks Phase A.

1. **Phase A only, or A + B?**
   Phase A covers ~80% of usage (`root.` / `any.` / `any2.` / `any3.` /
   `none.`). Phase B adds `parent.` / `child.`, which need self-joins.
   Recommend ship Phase A first.

2. **Non-relational WHERE scoping when relational clauses exist.**
   Options:
   - **(a)** Apply them to root spans of qualifying traces. Simple and
     matches our outer query shape. *Recommended.*
   - **(b)** Treat bare `service.name = "x"` as an implicit
     `any.service.name = "x"`. More Honeycomb-like.
   - **(c)** Reject bare predicates when relational is present; force
     the user to pick a prefix.

3. **`count()` semantics in relational mode.**
   Recommend: auto-rewrite to `COUNT(DISTINCT trace_id)`. No user-visible
   syntax change. Other ops (`p95(duration_ns)`, `avg(field)`) run against
   root spans of qualifying traces by default. To aggregate over *all*
   matching spans rather than roots, users can opt in via a future
   `@all-spans` or `@matching-spans` modifier on the aggregation — defer.

4. **Reject `GROUP BY any.*`.** Recommended — it's ambiguous. `GROUP BY
   root.*` and bare `GROUP BY <field>` (root-scoped when relational) are
   fine.

5. **UI affordance.**
   - **(a)** User types `root.http.route` into the FilterChip field
     manually. Matches Honeycomb exactly. Simplest.
   - **(b)** Add a prefix dropdown to the FilterChip editor
     (`[no prefix] / root. / any. / any2. / any3. / none.`) that
     prepends the prefix. Explicit, more discoverable.
   - **(c)** Autocomplete offers both prefixed and raw forms.
   Recommend (b), with the field-name input still accepting a full
   `root.http.route` typed string that updates the prefix dropdown.

## 6. AST changes

The wire format **doesn't change**. Clients still send:

```json
{"field": "root.http.route", "op": "=", "value": "/x"}
```

Internally, the parser splits prefixed filters into typed groups. File:
`internal/query/types.go` and `internal/query/validate.go`.

```go
// internal/query/types.go — additions

type Relational struct {
    Root []Filter      // all "root." filters, stripped of prefix
    Any  [3][]Filter   // any[0] = "any.", any[1] = "any2.", any[2] = "any3."
    None []Filter      // "none." filters
    // Phase B:
    // Parent []Filter
    // Child  []Filter
}

type Query struct {
    // ... existing fields ...

    // Relational is populated by Validate() after splitting out prefixed
    // entries from Where. Not present in the JSON wire shape; derived.
    Relational Relational `json:"-"`
}

// IsTraceScoped reports whether any relational clause is present, which
// triggers CTE-based SQL generation in the builder.
func (q *Query) IsTraceScoped() bool {
    return len(q.Relational.Root) > 0 ||
        len(q.Relational.None) > 0 ||
        len(q.Relational.Any[0]) > 0 ||
        len(q.Relational.Any[1]) > 0 ||
        len(q.Relational.Any[2]) > 0
}
```

Parsing rule (in validate.go, before the normal filter checks):

```go
// Strip relational prefixes. Order matters: longer prefixes first.
var relationalPrefixes = []struct {
    prefix string
    target func(*Relational) *[]Filter
}{
    {"root.",  func(r *Relational) *[]Filter { return &r.Root }},
    {"any3.",  func(r *Relational) *[]Filter { return &r.Any[2] }},
    {"any2.",  func(r *Relational) *[]Filter { return &r.Any[1] }},
    {"any.",   func(r *Relational) *[]Filter { return &r.Any[0] }},
    {"none.",  func(r *Relational) *[]Filter { return &r.None }},
}
```

Similar stripping for `group_by` entries prefixed with `root.`:

```go
// Strip "root." from group_by entries; "any.*" in group_by is rejected.
for i, g := range q.GroupBy {
    switch {
    case strings.HasPrefix(g, "root."):
        q.GroupBy[i] = strings.TrimPrefix(g, "root.")
    case hasAnyPrefix(g, "any.", "any2.", "any3."):
        return fmt.Errorf("group_by[%d]: %q is ambiguous; group_by cannot use any.* prefix", i, g)
    case strings.HasPrefix(g, "none."):
        return fmt.Errorf("group_by[%d]: %q is invalid; group_by cannot use none. prefix", i, g)
    }
}
```

## 7. Builder changes

File: `internal/query/builder.go`.

Add a new entry path:

```go
func Build(q *Query) (Compiled, error) {
    if q.IsTraceScoped() {
        return buildTraceScoped(q)
    }
    return buildSpanScoped(q) // the existing build() logic renamed
}
```

`buildTraceScoped` generates the CTE shell. Rough shape:

```go
func buildTraceScoped(q *Query) (Compiled, error) {
    b := newBuilder(q)

    // 1. Compile the HAVING clauses for the qualifying_traces CTE.
    havings := []string{}

    if len(q.Relational.Root) > 0 {
        rootExpr := "parent_span_id IS NULL"
        for _, f := range q.Relational.Root {
            clause, err := b.clauseForResolvedField(f) // see below
            if err != nil { return Compiled{}, err }
            rootExpr += " AND " + clause
        }
        havings = append(havings,
            fmt.Sprintf("MAX(CASE WHEN %s THEN 1 ELSE 0 END) = 1", rootExpr))
    }

    for _, group := range q.Relational.Any {
        if len(group) == 0 { continue }
        parts := []string{}
        for _, f := range group {
            clause, err := b.clauseForResolvedField(f)
            if err != nil { return Compiled{}, err }
            parts = append(parts, clause)
        }
        havings = append(havings,
            fmt.Sprintf("MAX(CASE WHEN %s THEN 1 ELSE 0 END) = 1",
                strings.Join(parts, " AND ")))
    }

    if len(q.Relational.None) > 0 {
        parts := []string{}
        for _, f := range q.Relational.None {
            clause, err := b.clauseForResolvedField(f)
            if err != nil { return Compiled{}, err }
            parts = append(parts, clause)
        }
        havings = append(havings,
            fmt.Sprintf("MAX(CASE WHEN %s THEN 1 ELSE 0 END) = 0",
                strings.Join(parts, " AND ")))
    }

    // 2. Build outer SELECT against root_spans CTE.
    //    count → COUNT(DISTINCT trace_id). Other aggregations run against
    //    root_spans as written.
    ...

    // 3. Emit:
    //    WITH qualifying_traces AS (SELECT trace_id FROM spans
    //         WHERE <time range> GROUP BY trace_id HAVING <havings>),
    //         root_spans AS (SELECT s.* FROM spans s JOIN qualifying_traces t
    //                        ON s.trace_id = t.trace_id WHERE parent_span_id IS NULL)
    //    SELECT ... FROM root_spans WHERE <non-relational> GROUP BY ...
}
```

Additions to the existing aggregation codegen:

- `aggregation(Aggregation{Op: OpCount})` in trace-scoped mode emits
  `COUNT(DISTINCT trace_id)` instead of `COUNT(*)`.

### `clauseForResolvedField`

Extract the existing `resolveField` + `clauseFor` composition into a helper
that can be reused inside the CTE. Important: the attribute JSON path
expression is the same whether we're in the CTE or outer query (the
attribute column is on the spans table in both cases), so no changes to
`resolveField`.

## 8. Executor / Store changes

None. `Store.RunQuery` still takes `(sql, args, columns, hasBucket,
groupKeys, rates)` and returns rows. The CTE is transparent to it.

## 9. Schema / index changes

None required for Phase A.

For Phase B (`parent.` / `child.`) add:

```sql
CREATE INDEX IF NOT EXISTS idx_spans_parent
    ON spans(trace_id, parent_span_id);
```

This covers the self-join. Ship together with Phase B or in a small
migration ahead of it.

## 10. Frontend changes

File: `ui/src/features/query/FilterChip.tsx` + `AddFilterButton.tsx`.

Minimum viable:
- Accept a `root.`, `any.`, `any2.`, `any3.`, `none.` prefix in the
  FilterChip field input. The existing field validation already permits
  the `.` character so no schema change is needed.
- The chip's visual shows the prefix (already happens since we render
  `filter.field` verbatim).

Better (design decision 5b):
- Add a prefix dropdown to the chip editor (defaults to "no prefix").
- When the user changes the dropdown, prepend the prefix to the field on
  save; when editing, derive the dropdown value by splitting the field
  string.
- Autocomplete under `prefix + partial` — e.g. typing `http.` with
  prefix `root.` fetches `/api/fields?prefix=http.` and prepends
  `root.` on selection.

Routing/search-param impact:
- `querySearchSchema` in `ui/src/lib/query.ts` doesn't need changes;
  filter values are unchanged.

## 11. Test plan

### 11.1 Unit tests — `internal/query/builder_test.go`

- `TestRelational_ParsesPrefixes` — given a `Query` with mixed bare and
  prefixed filters, `Validate()` partitions them into `Relational` and
  strips prefixes from the underlying `Field` names.
- `TestRelational_AnyGroupsAreIndependent` — `any.` and `any2.` each
  produce their own `MAX(CASE…)=1` HAVING clause.
- `TestRelational_NoneEmitsZero` — `none.` produces `MAX(CASE…)=0`.
- `TestRelational_RejectsAmbiguousGroupBy` — `group_by: ["any.x"]`
  fails validation.
- `TestRelational_CountRewritesToDistinctTraceID` — in trace-scoped mode
  the SQL has `COUNT(DISTINCT trace_id)`, not `COUNT(*)`.
- `TestRelational_TimeRangeAppliedAtCTELevel` — the time filter is in
  the CTE's WHERE, not the outer query.

### 11.2 End-to-end — `internal/store/sqlite/relational_test.go` (new)

Seed three traces with known shapes:

```
trace-A: root GET /checkout     → child db(postgresql) → child rpc(Fraud)
trace-B: root GET /checkout     → child db(mysql)
trace-C: root GET /health       → child db(postgresql)
```

Assertions:

| Query | Expected trace count |
|---|---|
| `root.http.route = "/checkout"` | 2 (A, B) |
| `root.http.route = "/checkout" AND any.db.system = "postgresql"` | 1 (A) |
| `any.db.system = "postgresql" AND none.db.system = "mysql"` | 2 (A, C) |
| `any.db.system = "postgresql" AND any2.rpc.service = "Fraud"` | 1 (A) |
| `any.db.system = "postgresql" AND any.rpc.service = "Fraud"` (same span must satisfy both) | 0 |

Also verify `EXPLAIN QUERY PLAN` for a typical relational query uses
`idx_spans_roots` or `idx_spans_time` inside the CTE and the PK for the
outer join.

### 11.3 API-level — `internal/api/router_test.go`

Add `TestAPI_Query_Relational` that POSTs one of the above payloads over
HTTP and asserts the row count + columns.

## 12. Edge cases & gotchas

- **Empty trace:** if a trace has only non-root spans (never happens in
  well-formed OTLP but defensive code is worth it), the root. predicate
  silently drops it. That's correct.
- **Root is inferred**, not tagged: we use `parent_span_id IS NULL`. If
  an SDK sends malformed traces with multiple "roots", our root-sensitive
  queries will treat each as a candidate root. Honeycomb handles this
  similarly; no action needed.
- **Time range on incomplete traces:** a trace whose root falls outside
  the range but whose children fall inside is excluded from trace-scoped
  queries. This matches Honeycomb behaviour. Flag in UI later — not a
  v1 concern.
- **Parameter count:** CTE has many predicates. SQLite's default
  parameter limit is 999 — plenty of headroom for typical queries.
- **`any.` group with 0 predicates:** treat as no-op, don't emit a
  clause. Already handled by `if len(group) == 0 { continue }`.
- **Nested JSON attributes:** `root.http.response.status_code` resolves
  via the same `resolveField` path as the non-relational case — no
  additional work.
- **`kind = 1` root heuristic:** we use `parent_span_id IS NULL` to
  identify roots. OTel `SpanKind` isn't reliable here (e.g. a server
  span can have a parent from an upstream service via traceparent).
  Stick with `parent_span_id IS NULL`.

## 13. Scope boundaries

**In Phase A:** `root.`, `any.` / `any2.` / `any3.`, `none.`.

**In Phase B:** `parent.`, `child.`.

**Not planned:**
- Arbitrary-depth `ancestor.` / `descendant.` (not a Honeycomb feature).
- More than 3 `any` groups (Honeycomb caps at 3; we match).
- Per-aggregation span-scoping modifiers (`@all-spans` / `@root`) — a
  future idea if users need span-level aggregations while also filtering
  at trace level. Not required for v1.

## 14. Files touched (cheat sheet)

Phase A:

- `internal/query/types.go` — add `Relational` struct + `IsTraceScoped()`
- `internal/query/validate.go` — prefix stripping + new rejections
- `internal/query/builder.go` — split into `buildSpanScoped` + `buildTraceScoped`; CTE generator
- `internal/query/builder_test.go` — parser and builder unit tests
- `internal/store/sqlite/relational_test.go` (new) — end-to-end tests with seeded traces
- `internal/api/router_test.go` — one HTTP-level test
- `ui/src/features/query/FilterChip.tsx` — prefix dropdown in chip editor
- `ui/src/features/query/AddFilterButton.tsx` — prefix dropdown in add form
- `ui/src/lib/query.ts` — optionally add a `RELATIONAL_PREFIXES` constant for the dropdown

Phase B (later):

- `internal/store/sqlite/schema.sql` — add `idx_spans_parent`
- `internal/query/builder.go` — `parent.` / `child.` via `EXISTS` clauses

## 15. Pickup checklist

When this comes off the shelf:

1. Confirm decisions 1–5 in §5 (Phase scope, WHERE scoping, count
   semantics, GROUP BY rejection, UI affordance).
2. Start at §14 Phase A files in order.
3. Land the `TestRelational_*` unit tests first — they pin the contract.
4. Wire the end-to-end test (`relational_test.go`) next.
5. Frontend last.
6. Consider whether to surface a mode indicator in the UI when the query
   has become trace-scoped (e.g. a subtle "traces" pill next to the
   query header) so the count-of-traces semantics isn't surprising.
