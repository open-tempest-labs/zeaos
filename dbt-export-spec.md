# ZeaOS dbt Export Specification v0.2.0 (Revised)

## Overview

ZeaOS exports interactive exploration sessions into **dbt-compatible project artifacts** that preserve transformation lineage, semantic intent, and validation metadata. The goal is to promote exploratory analysis into durable, reproducible dbt models or semantic assets while maintaining a clean boundary between local exploration (ZeaOS) and production orchestration (dbt).

## Design Principles

1. **Intent over data**: Export transformation lineage and SQL, not raw tables
2. **dbt-native**: Generate standard dbt project structure and conventions
3. **Deterministic**: Every exported model must be reproducible in dbt
4. **Progressive**: Support both dbt Core and dbt Fusion workflows
5. **Honest**: Clearly mark non-exportable operations

## Command Syntax

```
# Mark table for promotion (adds to export context)
dbt promote t3 [as] clicks_by_day [semantic|model]

# Export current session to dbt project
dbt export [NAME] [--target DIR] [--format core|fusion]

# Export single promoted artifact
dbt export clicks_by_day [--target DIR]

# Validate export without writing files
dbt validate [NAME]

# List exportable tables
dbt list
exportable
```

## File Formats

### Exported Bundle Structure

```
zea-dbt-export/
├── dbt_project.yml              # Minimal project config
├── profiles.yml                 # Local DuckDB profile
├── models/
│   ├── clicks_by_day.sql        # Generated model SQL
│   └── clicks_by_day.yml        # Model metadata + semantics
├── sources/
│   └── zea_sources.yml          # Source definitions from zea:// URIs
├── zea_export.json              # ZeaOS lineage + validation manifest
├── README.md                    # Import instructions
└── seeds/                       # Optional: reference data
```

### Core Artifacts

#### 1. Model SQL (`models/clicks_by_day.sql`)
```sql
{{ config(materialized='table') }}

SELECT 
  customer_id,
  date_trunc('day', event_ts) as event_day,
  count(*) as event_count
FROM {{ source('zea_raw', 'events') }}
WHERE event_type = 'click'
GROUP BY 1, 2
```

#### 2. Model YAML (`models/clicks_by_day.yml`)
```yaml
version: 2

models:
  - name: clicks_by_day
    description: "Daily click events by customer (promoted from ZeaOS session)"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
        tests:
          - not_null
          - unique
      - name: event_day
        description: "Event date truncated to day"
        tests:
          - not_null
      - name: event_count
        description: "Number of click events for customer on event_day"
        tests:
          - dbt_utils.expression_is_true:
              expression: "event_count >= 0"
    tests:
      - dbt_utils.expression_is_true:
          expression: "event_count > 0"

semantic_models:
  - name: clicks_by_day
    description: "Customer click analytics"
    entities:
      - name: customer_id
        type: primary
        expr: customer_id
    measures:
      - name: click_count
        agg: sum
        expr: event_count
    dimensions:
      - name: event_day
        type: time
        time_granularity: day
        expr: event_day
```

#### 3. Sources YAML (`sources/zea_sources.yml`)
```yaml
version: 2

sources:
  - name: zea_raw
    schema: raw
    tables:
      - name: events
        description: "Raw event data from zea://raw/events.parquet"
        freshness:
          warn_after: {count: 7, period: day}
        loaded_at_field: event_ts
```

#### 4. Zea Export Manifest (`zea_export.json`)
```json
{
  "version": "0.2.0",
  "session_id": "analysis-2026-04-05-0950",
  "exported": "2026-04-05T10:05:00Z",
  "user": "larry-mccay",
  "artifacts": [
    {
      "name": "clicks_by_day",
      "kind": "model",
      "source_uris": ["zea://raw/events.parquet"],
      "transformations": [
        {"type": "filter", "expr": "event_type = 'click'"},
        {"type": "group_by", "keys": ["customer_id", "date_trunc('day', event_ts)"]},
        {"type": "aggregate", "expr": "count(*) as event_count"}
      ],
      "columns": 3,
      "row_count": 12500,
      "dialect": "duckdb",
      "portable": true,
      "validated": true,
      "validation": {
        "local_rows": 12500,
        "local_checksum": "sha256:abc123...",
        "dialect_version": "DuckDB 1.2.0"
      }
    }
  ]
}
```

## What Gets Captured

### Required Metadata
| Field | Type | Description |
|-------|------|-------------|
| `source_uris` | array | zea:// URIs for all inputs |
| `transformations` | array | Filter/group/join/aggregate steps |
| `columns` | array | Final column names + inferred types |
| `grain` | array | Primary grouping keys (if identifiable) |
| `dialect` | string | Validated SQL dialect |
| `portable` | boolean | Can run in target dbt adapter |

### Transformation Mapping
```
ZeaOS Operation          → dbt SQL
─────────────────────────┬─────────────────
t1 = load zea://events   → {{ source('zea_raw', 'events') }}
t2 = t1 | where type=.. → WHERE event_type = 'click'
t3 = t2 | group cust,.. → GROUP BY customer_id, event_day
t4 = t3 | count(*)      → COUNT(*) as event_count
```

## Validation Rules

### Exportable Operations
```
✅ load zea://path              → source reference
✅ where EXPR                   → WHERE clause  
✅ select COL1, COL2            → column projection
✅ group COL1, COL2             → GROUP BY
✅ aggregate count(*), sum(COL) → aggregate functions
✅ date_trunc(), extract()      → SQL date functions
✅ join t1 t2 on COL1=COL2      → SQL JOIN
```

### Non-Exportable Operations
```
❌ t1 | zeaplugin custom       → Custom plugin (non-portable)
❌ session-only temp tables     → Ephemeral session state
❌ non-deterministic ops        → random(), now()
❌ engine-specific functions    → DuckDB-only UDFs
```

## Command Workflow

```
# 1. Explore and promote
zea> t1 = load zea://raw/events.parquet
zea> t2 = t1 | where event_type = 'click'  
zea> t3 = t2 | group customer_id, event_day count(*) as event_count
zea> dbt promote t3 as clicks_by_day model

# 2. Validate export
zea> dbt validate clicks_by_day
✓ SQL parses correctly
✓ 3 columns match expected schema  
✓ Source URIs resolved
✓ Dialect: duckdb ✓ portable ✓

# 3. Export
zea> dbt export clicks_by_day --target ./my-dbt-project/
Created: ./my-dbt-project/models/clicks_by_day.sql
Created: ./my-dbt-project/models/clicks_by_day.yml
Created: ./my-dbt-project/sources/zea_sources.yml

# 4. Test in dbt (user runs this)
cd my-dbt-project
dbt debug --profiles-dir .
dbt run --select clicks_by_day
```

## Testing Without dbt Installed

### Stage 1: ZeaOS Self-Validation
```
zea> dbt validate clicks_by_day --dry-run
```
Checks SQL syntax, column contracts, source resolution, lineage consistency.

### Stage 2: Local DuckDB Execution
```
zea> dbt test clicks_by_day
✓ Source data accessible: zea://raw/events.parquet
✓ Generated SQL produces 12,500 rows (matches session)
✓ Column types match: customer_id:string, event_day:date, event_count:bigint
✓ Checksum matches session result
```

### Stage 3: dbt Core + DuckDB (minimal install)

```bash
# On test machine
pip install dbt-duckdb
mkdir test-dbt && cd test-dbt
zea-export-bundle/ → ./
dbt deps
dbt debug
dbt run --select clicks_by_day
dbt test --select clicks_by_day
```

**Minimal `profiles.yml`** (auto-generated):
```yaml
zea_local:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'local.duckdb'
      threads: 4
```

## Sample dbt_project.yml (auto-generated)

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

## Error Handling

```
Non-exportable table t4:
❌ contains zeaplugin custom-transform (non-portable)
❌ uses session-only table t_temp
→ Mark as "exploration-only" in zea_export.json

Partial export t5:
⚠  rewrote DuckDB-specific date_trunc() → standard SQL
⚠  dropped non-portable window UDF
→ Warned in validation + README.md
```

## Success Criteria

An export is successful when:
- ✅ SQL parses in DuckDB
- ✅ Row count matches ZeaOS session result (±1%)
- ✅ Column schema matches exactly
- ✅ Sources resolve to zea:// URIs  
- ✅ Lineage is fully reproducible
- ✅ Passes basic dbt tests (not_null, unique where applicable)

## Future Extensions

```
v0.3.0: dbt Fusion YAML spec support
v0.4.0: Multi-table dependency graphs  
v0.5.0: dbt Metrics Layer export
v1.0.0: Remote dbt Cloud project push
```

**All `export` commands now use the safe `dbt` subcommand namespace.** No shell collisions, clear intent, dbt-community friendly. 🚀
