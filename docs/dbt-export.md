---
title: "ZeaOS → dbt Export"
---
{% raw %}
# ZeaOS → dbt Export

*Zea is the local, Arrow-native exploration and promotion layer that feeds high-quality, validated artifacts into dbt. dbt owns modeling, orchestration, documentation, and production governance.*

---

## The Boundary

| Layer | Owns |
|-------|------|
| **ZeaOS** | Exploration, session state, lineage, local validation, export |
| **dbt** | Modeling conventions, orchestration, testing, documentation, production governance |

The workflow: explore freely in ZeaOS, promote the tables worth keeping, export a self-contained dbt project bundle, then hand it to dbt to run and govern.

---

## Commands

```
model promote <table> [as <name>] [model|semantic]   mark a table for export
model list                                            show promoted artifacts
model validate [<name>]                              check portability
model export [<name>] [-o <dir>]                     write dbt project bundle
```

`model promote` and `model list` are target-agnostic — they record intent without caring about the destination format. `model validate` and `model export` apply dbt-specific rules and generate dbt-specific files.

---

## Worked Example: NYC Taxi Revenue by Zone

This picks up from where the [NYC Taxi tutorial](tutorial-nyc-taxi) leaves off. We already have a session with `trips` loaded and some derived tables. Now we want to promote the revenue analysis to a production dbt model.

### Step 1 — Explore and build the tables

```
ZeaOS> trips = load https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet ...
→ trips: 2_964_624 rows × 19 cols

ZeaOS> long_trips = trips | where trip_distance > 2.0
→ long_trips: 1_847_203 rows × 19 cols

ZeaOS> zone_revenue = zeaql "SELECT PULocationID, COUNT(*) AS trips, ROUND(SUM(fare_amount), 2) AS total_fare, ROUND(AVG(tip_amount), 2) AS avg_tip FROM long_trips GROUP BY PULocationID ORDER BY total_fare DESC"
→ zone_revenue: 262 rows × 4 cols
```

### Step 2 — Promote

```
ZeaOS> model promote zone_revenue as zone_revenue_by_pickup model
promoted zone_revenue → zone_revenue_by_pickup (model)
```

The export name must be a valid dbt model name: lowercase, alphanumeric and underscores, starting with a letter.

Check what's been promoted:

```
ZeaOS> model list
Export Name               Kind        Source Table          Promoted At
────────────────────────────────────────────────────────────────────────
zone_revenue_by_pickup    model       zone_revenue          2026-04-05 10:30:00
```

### Step 3 — Validate

```
ZeaOS> model validate zone_revenue_by_pickup
Validating zone_revenue_by_pickup (from zone_revenue)...
  ✓ SQL parses correctly
  ✓ 262 rows × 4 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
  ✓ portable: duckdb dialect, no non-standard functions detected
```

Validation checks:
- SQL parses correctly in DuckDB (using `EXPLAIN`)
- Source URIs are recorded and resolvable
- No non-deterministic functions (`random()`, `now()`, etc.)
- No DuckDB-specific functions that won't translate to other adapters

### Step 4 — Export

```
ZeaOS> model export zone_revenue_by_pickup -o ./taxi-dbt-project
  created taxi-dbt-project/models/zone_revenue_by_pickup.sql
  created taxi-dbt-project/models/zone_revenue_by_pickup.yml
  created taxi-dbt-project/sources/zea_sources.yml
  created taxi-dbt-project/dbt_project.yml
  created taxi-dbt-project/profiles.yml
  created taxi-dbt-project/zea_export.json

Exported 1 artifact(s) to taxi-dbt-project/
Next steps:
  cd taxi-dbt-project
  pip install dbt-duckdb   # if not already installed
  dbt debug --profiles-dir .
  dbt run
```

---

## The Generated Bundle

### `models/zone_revenue_by_pickup.sql`

```sql
{{ config(materialized='table') }}

SELECT PULocationID, COUNT(*) AS trips, ROUND(SUM(fare_amount), 2) AS total_fare, ROUND(AVG(tip_amount), 2) AS avg_tip FROM {{ source('zea_http', 'long_trips') }} GROUP BY PULocationID ORDER BY total_fare DESC
```

ZeaOS substitutes session table references with dbt `{{ source() }}` and `{{ ref() }}` Jinja calls. Tables that were loaded from a URI become `source()` references; derived session tables become `ref()` references.

### `models/zone_revenue_by_pickup.yml`

```yaml
version: 2

models:
  - name: zone_revenue_by_pickup
    description: "Promoted from ZeaOS session (table: zone_revenue)"
    columns:
      - name: PULocationID
        description: ""
        tests:
          - not_null
      - name: trips
        description: ""
        tests:
          - not_null
      - name: total_fare
        description: ""
        tests:
          - not_null
      - name: avg_tip
        description: ""
        tests:
          - not_null
```

Column definitions and `not_null` tests are generated automatically from the Arrow schema. Edit descriptions and add further tests (`unique`, `accepted_values`, `relationships`) directly in the YAML before committing to your dbt project.

### `sources/zea_sources.yml`

```yaml
version: 2

sources:
  - name: zea_http
    description: "ZeaOS data sources"
    tables:
      - name: long_trips
        description: "Loaded from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
```

### `dbt_project.yml`

```yaml
name: 'zea_export'
version: '1.0.0'
config-version: 2

profile: 'zea_local'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  zea_export:
    materialized: table
```

### `profiles.yml`

```yaml
zea_local:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'local.duckdb'
      threads: 4
```

A local DuckDB profile is generated automatically so the bundle runs immediately with `dbt run` and no additional configuration. Swap the profile for your production adapter (BigQuery, Snowflake, Redshift) when promoting to a shared project.

### `zea_export.json`

```json
{
  "version": "0.2.0",
  "session_id": "zea-2026-04-05-1030",
  "exported": "2026-04-05T10:30:00Z",
  "user": "larry",
  "artifacts": [
    {
      "name": "zone_revenue_by_pickup",
      "kind": "model",
      "source_uris": [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
      ],
      "transformations": [
        { "type": "load", "uri": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" },
        { "type": "where", "expr": "trip_distance > 2.0" },
        { "type": "sql",   "expr": "SELECT PULocationID, COUNT(*) AS trips ..." }
      ],
      "columns": 4,
      "row_count": 262,
      "dialect": "duckdb",
      "portable": true
    }
  ]
}
```

The manifest records the full lineage — every load, filter, and SQL transformation — along with row counts and portability status. This is the artifact you commit alongside the model files to give reviewers a complete picture of where the model came from.

---

## Running in dbt

```bash
cd taxi-dbt-project
pip install dbt-duckdb

dbt debug --profiles-dir .   # verify connection
dbt run                       # materialise the model
dbt test                      # run generated column tests
```

---

## Exporting Multiple Tables

`export` without a name exports all promoted artifacts:

```
ZeaOS> model promote avg_tip as avg_tip_by_payment model
ZeaOS> model export -o ./taxi-dbt-project
```

Each artifact gets its own `.sql` and `.yml` file. Source definitions are deduplicated — if two models share a source URI it appears once in `zea_sources.yml`.

---

## Portability Notes

| Operation | Exportable | Notes |
|-----------|-----------|-------|
| `load <file>` | ✅ | Becomes `{{ source(...) }}` |
| `where EXPR` | ✅ | Standard SQL WHERE |
| `select COLS` | ✅ | Standard SQL SELECT |
| `group COL` | ✅ | Standard SQL GROUP BY |
| `zeaql "SQL"` | ✅ | Session table refs rewritten to `source()`/`ref()` |
| `pivot COL→VAL` | ⚠️ | DuckDB PIVOT syntax — flagged as non-portable |
| `zearun <plugin>` | ❌ | Non-portable, marked in manifest |
| `random()`, `now()` | ❌ | Non-deterministic, flagged by validate |

The `model validate` command reports any portability issues before you export. Non-portable warnings appear in the model YAML `meta.zea_warnings` field so they're visible to dbt model reviewers.

---

## Merging Into an Existing dbt Project

The bundle is designed to be merged into an existing project:

```bash
cp taxi-dbt-project/models/*.sql      my-existing-project/models/zea/
cp taxi-dbt-project/models/*.yml      my-existing-project/models/zea/
cp taxi-dbt-project/sources/*.yml     my-existing-project/models/zea/
# dbt_project.yml and profiles.yml are only needed for standalone use
```

Replace the auto-generated `{{ source('zea_http', ...) }}` references with `{{ ref(...) }}` references to your project's existing source or staging models as appropriate.
{% endraw %}
