---
title: "From Exploration to Production: Analysis That Knows Where It Came From"
description: "How ZeaOS bridges the gap between interactive DuckDB analysis and production dbt on MotherDuck — without losing provenance at the handoff."
---
{% raw %}
# From Exploration to Production: Analysis That Knows Where It Came From

*How ZeaOS bridges the gap between interactive DuckDB analysis and production dbt on MotherDuck — without losing provenance at the handoff.*

---

## The Gap Nobody Talks About

Every data team has a version of this problem.

An analyst opens a DuckDB session, loads a Parquet file, runs a dozen queries, finds something interesting, and builds a clean summary table. Then someone asks: "Can we get this into our dbt project?" And the reconstruction begins.

Which filter did you apply? What was the exact `GROUP BY`? Was that `ROUND(AVG(...), 2)` or `ROUND(AVG(...), 4)`? The analyst goes back to their terminal history, pieces it together, writes the SQL again from scratch, tests it, and eventually a dbt model exists that approximately matches what was explored.

The provenance — the chain of decisions from raw data to finished table — is gone. What survives is a SQL file with no memory of how it got there.

This is the exploration-to-production gap. It is not a DuckDB problem or a dbt problem. It is a handoff problem. The tools that are best for exploration have no concept of lineage. The tools that manage lineage (dbt, Airflow, Dagster) are too heavyweight for exploration. The gap lives in between.

ZeaOS is built to close that gap.

---

## What ZeaOS Is

ZeaOS is an Arrow-native data REPL powered by DuckDB. It looks like a shell, feels like a notebook, and keeps track of everything.

```
ZeaOS> trips = load https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
→ trips: 2,964,624 rows × 19 cols

ZeaOS> long_trips = trips | where trip_distance > 2.0
→ long_trips: 1,987,443 rows × 19 cols

ZeaOS> zone_revenue = zeaql "SELECT PULocationID, COUNT(*) AS trips,
         ROUND(SUM(fare_amount), 2) AS total_fare,
         ROUND(AVG(tip_amount), 2) AS avg_tip
         FROM long_trips GROUP BY PULocationID ORDER BY total_fare DESC"
→ zone_revenue: 254 rows × 4 cols
```

Every assignment — every `=` — records what happened: where the data came from, what operation produced it, what the parent table was. That record is the lineage chain.

When the session ends, the chain survives. When the session resumes, the chain is restored. This is not a scratchpad. It is a persistent analytical record.

---

## The Lineage Chain as a First-Class Artifact

The session's lineage is stored in `~/.zeaos/session.json`. For the NYC Taxi analysis above it looks like this:

```
trips        → load(https://...yellow_tripdata_2024-01.parquet)
long_trips   → where(trip_distance > 2.0)   parent: trips
zone_revenue → sql                           parent: (resolved via SQL reference to long_trips)
avg_tip      → sql                           parent: (resolved via SQL reference to trips)
```

This chain is what makes everything else possible. It is what lets ZeaOS answer the question "where did `zone_revenue` come from?" without you having to remember — and what lets it generate accurate dbt models, source declarations, and provenance metadata automatically.

Without the chain, you have query results. With the chain, you have a reproducible analytical artifact.

---

## Promoting to dbt

When analysis is ready to share, `model promote` marks a table for export:

```
ZeaOS> model promote zone_revenue as zone_revenue_by_pickup model
ZeaOS> model promote avg_tip as avg_tip_by_payment model
```

`model validate` checks portability before anything is pushed:

```
ZeaOS> model validate zone_revenue_by_pickup
Validating zone_revenue_by_pickup (from zone_revenue)...
  ✓ SQL structure valid
  ✓ 254 rows × 4 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/...
  ✓ portable: duckdb dialect, no non-standard functions detected
```

Then `model publish` generates a complete, working dbt Core project and pushes it to GitHub:

```
ZeaOS> model publish --repo myorg/nyc-taxi-dbt --new
  created models/zone_revenue_by_pickup.sql
  created models/zone_revenue_by_pickup.yml
  created models/avg_tip_by_payment.sql
  created models/avg_tip_by_payment.yml
  created models/long_trips.sql (intermediate)
  created models/zea_sources.yml
  created macros/stage_zea_sources.sql
  created dbt_project.yml
  created profiles.yml
  created zea_export.json
✓ Published to https://github.com/myorg/nyc-taxi-dbt
```

Three things in that output are worth pausing on.

**`models/long_trips.sql (intermediate)`** — ZeaOS detected that `zone_revenue_by_pickup` references `long_trips` via `{{ ref('long_trips') }}`, and that `long_trips` is not promoted. Rather than generating a broken dbt project, it auto-generated an intermediate model for `long_trips` from the lineage chain. A dbt user writing models by hand would have had to notice this dependency manually.

**`macros/stage_zea_sources.sql`** — This is a dbt macro that runs before any model and creates a DuckDB schema + view pointing at the HTTPS Parquet source:

```sql
{% macro stage_zea_sources() %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS zea_http") %}
  {% do run_query("CREATE OR REPLACE VIEW zea_http.yellow_tripdata_2024_01
    AS SELECT * FROM read_parquet('https://...')") %}
{% endmacro %}
```

This is the mechanism that makes the dbt project work end-to-end without a warehouse. DuckDB reads the Parquet directly from HTTPS at `dbt run` time. Without this macro — which is not obvious to write and requires knowing both the DuckDB dialect and dbt's `on-run-start` hook — `dbt run` fails with a source-not-found compilation error. ZeaOS generates it because it knows the source URI from the lineage chain.

**`zea_export.json`** — Records the complete provenance: session ID, source URIs, transformation history, row counts, portability status, and — after push — target locations and timestamps. It is not read by dbt. It is metadata for humans and tools that want to understand where a model came from.

---

## Running in dbt — No Warehouse Required

Clone the published repo and run:

```bash
git clone https://github.com/myorg/nyc-taxi-dbt
cd nyc-taxi-dbt
dbt run
```

```
Found 4 models, 1 operation, 7 tests, 1 source

1 of 1 START hook: zea_export.on-run-start.0 ......... [OK in 1.64s]
1 of 4 START sql table model main.long_trips ......... [OK in 4.1s]
2 of 4 START sql table model main.avg_tip_by_payment . [OK in 0.7s]
3 of 4 START sql table model main.zone_revenue_by_pickup [OK in 0.2s]

Finished running 4 models in 4.11 seconds.
All 4 models created successfully.
```

The staging hook runs first, creating the DuckDB view that resolves the HTTPS source. `long_trips` runs next, materializing the filtered dataset. The two promoted models run after, with `zone_revenue_by_pickup` depending on `long_trips` and `avg_tip_by_payment` depending on the source view directly.

DuckDB fetches the source Parquet from the NYC TLC CloudFront URL on first run. No warehouse account. No credentials. No ETL. Anyone with Python 3.12 and `pip install dbt-duckdb` can reproduce this exactly.

```bash
dbt test   # 7 generated column tests — all pass
dbt docs generate && dbt docs serve   # browsable data catalog with full DAG
```

---

## Pushing to MotherDuck

The local dbt-duckdb path is a complete local development environment. When you're ready for persistent, shareable, production-grade storage, `push` materializes the source data into MotherDuck:

```
ZeaOS> push --target md:my_db
Connecting to MotherDuck (md:my_db)...
Connected to MotherDuck.
Pushing 4 table(s) to md:my_db...
  ✓ trips          → zea_exports.trips          2,964,624 rows
  ✓ long_trips     → zea_exports.long_trips      1,987,443 rows
  ✓ zone_revenue   → zea_exports.zone_revenue    254 rows
  ✓ avg_tip        → zea_exports.avg_tip         5 rows
Push complete.
```

On first connection, MotherDuck's DuckDB extension opens a browser window for OAuth. After authentication, the token is cached at `~/.motherduck/token` and all future connections — from ZeaOS, from dbt-duckdb, from the DuckDB CLI — are automatic.

The push is recorded in `session.json`:

```json
"push_records": [
  {
    "target": "md:my_db",
    "schema": "zea_exports",
    "table_name": "trips",
    "pushed_at": "2026-04-06T20:00:00Z",
    "row_count": 2964624,
    "source_uri": "https://d37ci6vzurychx.cloudfront.net/..."
  }
]
```

This record is what enables drift detection. If someone modifies the MotherDuck table directly, or if the source data updates:

```
ZeaOS> push sync --target md:my_db
  ✓ trips          2,964,624 rows  in sync
  ✓ long_trips     1,987,443 rows  in sync
  ✓ zone_revenue   254 rows        in sync
  ✓ avg_tip        5 rows          in sync
All pushed tables are in sync.
```

---

## Running dbt Against MotherDuck

With the source tables materialized in MotherDuck, add a prod target to `profiles.yml`:

```yaml
zea_local:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'local.duckdb'
      threads: 4
    prod:
      type: duckdb
      path: 'md:my_db'
      threads: 4
```

Update the staging macro to skip HTTPS re-fetch when targeting MotherDuck:

```sql
{% macro stage_zea_sources() %}
  {% if target.type == 'duckdb' and 'md:' not in (target.path | default('')) %}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS zea_http") %}
    {% do run_query("CREATE OR REPLACE VIEW zea_http.yellow_tripdata_2024_01
      AS SELECT * FROM read_parquet('https://...')") %}
  {% endif %}
{% endmacro %}
```

Then:

```bash
dbt run --target prod
```

The models run against MotherDuck. The source tables are already there — no HTTPS fetch, no staging delay. The same SQL that ran locally in 4 seconds runs in MotherDuck against the same data.

The transformation SQL — `zone_revenue_by_pickup.sql`, `avg_tip_by_payment.sql` — never changed. The same model that worked in local DuckDB works in MotherDuck prod. That portability is not accidental. It is what ZeaOS's portability validation at the `promote` step is checking for.

---

## What This Path Actually Provides

Stepping back, here is what the full ZeaOS → dbt-duckdb → MotherDuck path delivers that the individual tools alone do not:

**Provenance from source to production.** The lineage chain in `session.json` traces every table back to its origin. The push records in `session.json` trace every MotherDuck table back to the session that created it and the source URI it came from. The `zea_export.json` in the published repo captures the full picture for anyone who clones it. A reviewer can audit the complete chain — HTTPS source → filter → aggregation → dbt model → MotherDuck table — without running anything.

**A working dbt project generated from analysis, not written alongside it.** The dbt project is not scaffolded by hand. It is derived from the session lineage. The intermediate model auto-generation, the staging macro, the sources YAML placement, the `on-run-start` hook — all of these reflect actual choices that a dbt practitioner would have to make manually and correctly. ZeaOS makes them from the lineage chain.

**Local dev that is identical in structure to prod.** The same dbt project works locally with HTTPS and in production with MotherDuck. Switching targets is a `--target prod` flag. The staging macro handles the difference transparently. There is no separate "dev project" and "prod project."

**Drift detection across the full pipeline.** Because the push was attributed to a session entry with a timestamp and row count, `push sync` can surface divergence between the local analytical record and the MotherDuck copy. This is the beginning of a data contract between the exploration environment and production storage.

---

## The Bigger Picture

The pattern this describes — interactive exploration that produces auditable, production-ready artifacts — is not specific to the NYC Taxi dataset or to MotherDuck. It applies whenever:

- An analyst explores data interactively and a data engineer needs to operationalize the findings
- A model needs to be understood by someone who did not write it
- Source data lives at an HTTP URL, an S3 path, or a local file that needs to be promoted to shared storage
- The analysis evolves across multiple sessions and the lineage needs to survive

The DuckDB ecosystem — DuckDB CLI, dbt-duckdb, MotherDuck — provides excellent tools for each layer of this stack. ZeaOS provides the thread that runs through all of them: the session as a persistent analytical record that knows where every table came from, what produced it, and where it went.

Analysis that knows where it came from is analysis that can be trusted, reproduced, and extended. That is what the exploration-to-production path is actually for.

---

## Getting Started

```bash
# Install ZeaOS
brew install open-tempest-labs/tap/zeaos

# Run the NYC Taxi analysis end-to-end
zeaos
ZeaOS> zearun taxi-dbt-publish --repo yourname/nyc-taxi-dbt --new

# Clone and run in dbt
git clone https://github.com/yourname/nyc-taxi-dbt
cd nyc-taxi-dbt
pip install dbt-duckdb   # requires Python 3.12
dbt run

# Push to MotherDuck (requires free account at motherduck.com)
zeaos
ZeaOS> push --target md:my_db
```

Full documentation: [open-tempest-labs.github.io/zeaos](https://open-tempest-labs.github.io/zeaos)

Source: [github.com/open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos)

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs).*
{% endraw %}
