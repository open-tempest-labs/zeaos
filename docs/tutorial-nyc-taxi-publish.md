---
title: "Publishing NYC Taxi Analysis as Data Models via GitHub"
---
{% raw %}
# Publishing NYC Taxi Analysis as Data Models via GitHub

*This tutorial picks up from the [NYC Taxi Analysis](tutorial-nyc-taxi) walkthrough and takes it all the way to a running dbt project. By the end you will have promoted the session's key analytical tables as named model artifacts, published a dbt Core project to GitHub, cloned it, and run `dbt run` to materialise the models — no warehouse, no cloud account, no data loading required.*

---

## Why This Works Without a Warehouse

The source data for this analysis is a public Parquet file served over HTTPS by the NYC Taxi and Limousine Commission. DuckDB can read HTTPS Parquet files directly — so when dbt runs the generated models, DuckDB fetches the file at query time. No pre-loading, no ETL, no external tables.

This is the reason the local dbt-DuckDB path is genuinely end-to-end: the source URL is recorded in the ZeaOS session, written into `sources/zea_sources.yml`, and resolved by DuckDB at `dbt run` time. Anyone who clones the published repo and has Python installed can run the full pipeline.

> **Portability note:** This zero-setup path works when source data comes from a public HTTPS or S3 URL. If your ZeaOS session loaded data from a local file, ZeaOS will warn you at publish time and suggest saving to a ZeaDrive S3 backend first.

---

## Where We Left Off

After the NYC Taxi tutorial the session holds nine tables:

```
ZeaOS — Zero-copy Data REPL from Open Tempest Labs
Tables: [trips, cc_trips, jfk_trips, long_trips, zone_revenue, top_zones,
         zone_payment, payment_pivot, avg_tip]
```

The session persisted to `~/.zeaos/tables/`. Start ZeaOS and it resumes automatically.

---

## Quick Path: One-Command Plugin

The entire workflow — load, analyse, promote, validate, publish — is packaged as a ZeaOS script plugin. Install it once:

```bash
mkdir -p ~/.zeaos/plugins
cp plugins/taxi-dbt-publish.zea ~/.zeaos/plugins/
```

Then from inside any ZeaOS session:

```
ZeaOS> zearun taxi-dbt-publish --repo lmccay/nyc-taxi-dbt --new
```

That single command loads the taxi data, builds the analysis tables, promotes and validates two model artifacts, and publishes the bundle to GitHub. Skip to [Running in dbt](#running-in-dbt) to see what happens next.

The rest of this tutorial walks through each step manually.

---

## Step 1 — Decide What to Promote

Not every session table belongs in a published model bundle:

- **Promote** finished analytical views — the tables a downstream consumer would actually query.
- **Skip** intermediate tables (`long_trips`, `zone_payment`) — these are captured as `{{ source() }}` or `{{ ref() }}` references inside the promoted models.
- **Flag** non-portable operations — PIVOT uses DuckDB-specific syntax and will need rewriting for other adapters.

For the taxi analysis:

| Session table | Model name | Why |
|---|---|---|
| `zone_revenue` | `zone_revenue_by_pickup` | Revenue analysis by pickup zone |
| `avg_tip` | `avg_tip_by_payment` | Tip behaviour by payment type |
| `payment_pivot` | `payment_mix_by_zone` | Payment mix by zone (DuckDB-only PIVOT) |

---

## Step 2 — Promote

```
ZeaOS> model promote zone_revenue as zone_revenue_by_pickup model
promoted zone_revenue → zone_revenue_by_pickup (model)

ZeaOS> model promote avg_tip as avg_tip_by_payment model
promoted avg_tip → avg_tip_by_payment (model)

ZeaOS> model promote payment_pivot as payment_mix_by_zone model
promoted payment_pivot → payment_mix_by_zone (model)

ZeaOS> model list
Export Name               Kind        Source Table          Promoted At
────────────────────────────────────────────────────────────────────────
zone_revenue_by_pickup    model       zone_revenue          2026-04-11 09:00:00
avg_tip_by_payment        model       avg_tip               2026-04-11 09:00:01
payment_mix_by_zone       model       payment_pivot         2026-04-11 09:00:02
```

Promotions persist across restarts. To remove one: `model unpromote <name>`.

---

## Step 3 — Validate

`model validate` checks SQL structure, row counts, source URIs, and portability before anything is pushed:

```
ZeaOS> model validate zone_revenue_by_pickup
Validating zone_revenue_by_pickup (from zone_revenue)...
  ✓ SQL structure valid (source refs resolve at export runtime)
  ✓ 254 rows × 4 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
  ✓ portable: standard SQL, no non-standard functions detected

ZeaOS> model validate avg_tip_by_payment
Validating avg_tip_by_payment (from avg_tip)...
  ✓ SQL structure valid (source refs resolve at export runtime)
  ✓ 5 rows × 3 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
  ✓ portable: standard SQL, no non-standard functions detected

ZeaOS> model validate payment_mix_by_zone
Validating payment_mix_by_zone (from payment_pivot)...
  ✓ SQL structure valid (source refs resolve at export runtime)
  ✓ 10 rows × 7 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
  ⚠  PIVOT uses DuckDB-specific syntax — may not run on other adapters
```

The source URI check is what makes the dbt-DuckDB path work end-to-end. Because the source is a public HTTPS URL, the `✓ source URIs` line means anyone who clones the repo can run `dbt run` immediately.

---

## Step 4 — Publish to GitHub

If `gh` is already authenticated, no token setup is needed — ZeaOS picks it up automatically:

```
ZeaOS> model publish --repo lmccay/nyc-taxi-dbt --new
Generating export bundle...
  created models/zone_revenue_by_pickup.sql
  created models/zone_revenue_by_pickup.yml
  created models/avg_tip_by_payment.sql
  created models/avg_tip_by_payment.yml
  created models/payment_mix_by_zone.sql
  created models/payment_mix_by_zone.yml
  created sources/zea_sources.yml
  created dbt_project.yml
  created profiles.yml
  created zea_export.json
  created README.md
  created .gitignore
Creating GitHub repo lmccay/nyc-taxi-dbt...
  created: https://github.com/lmccay/nyc-taxi-dbt
commit: ZeaOS v1.0.0: initial export — zone_revenue_by_pickup, avg_tip_by_payment, payment_mix_by_zone
✓ Published to https://github.com/lmccay/nyc-taxi-dbt
```

---

## What Lands in the Repo

```
nyc-taxi-dbt/
├── macros/
│   └── stage_zea_sources.sql
├── models/
│   ├── zone_revenue_by_pickup.sql
│   ├── zone_revenue_by_pickup.yml
│   ├── avg_tip_by_payment.sql
│   ├── avg_tip_by_payment.yml
│   ├── long_trips.sql            ← auto-generated intermediate model
│   ├── payment_mix_by_zone.sql
│   ├── payment_mix_by_zone.yml
│   └── zea_sources.yml           ← source declarations (must be in model-paths)
├── dbt_project.yml
├── profiles.yml
├── zea_export.json
├── README.md
└── .gitignore
```

### `models/zone_revenue_by_pickup.sql`

```sql
{{ config(materialized='table') }}

SELECT PULocationID, COUNT(*) AS trips,
       ROUND(SUM(fare_amount), 2) AS total_fare,
       ROUND(AVG(tip_amount), 2) AS avg_tip
FROM {{ ref('long_trips') }}
GROUP BY PULocationID
ORDER BY total_fare DESC
```

ZeaOS rewrites session table references to dbt Jinja calls: HTTP-loaded source tables become `{{ source() }}`, derived session tables become `{{ ref() }}`. Because `long_trips` is a derived pipe table (not loaded directly from HTTP), it becomes `{{ ref('long_trips') }}` and ZeaOS auto-generates `models/long_trips.sql` as an intermediate model. The full lineage chain — `trips → long_trips → zone_revenue` — is preserved in `zea_export.json`.

### `macros/stage_zea_sources.sql`

```sql
{% macro stage_zea_sources() %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS zea_http") %}
  {% do run_query("CREATE OR REPLACE VIEW zea_http.yellow_tripdata_2024_01 AS SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')") %}
{% endmacro %}
```

This macro runs before any model via `on-run-start` in `dbt_project.yml`. It creates the DuckDB schema and view that `{{ source('zea_http', 'yellow_tripdata_2024_01') }}` compiles to. DuckDB's built-in HTTP client reads the Parquet directly from the CloudFront URL at query time — no data staging required.

### `models/zea_sources.yml`

```yaml
# generated by ZeaOS — safe to overwrite
version: 2

sources:
  - name: zea_http
    schema: zea_http
    description: "ZeaOS data sources"
    tables:
      - name: yellow_tripdata_2024_01
        description: "Loaded from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
```

Declares the HTTP source so dbt can resolve `{{ source('zea_http', 'yellow_tripdata_2024_01') }}` at compile time. The `schema: zea_http` line tells dbt to look in the `zea_http` DuckDB schema, which the staging macro creates before models run. This file lives in `models/` because dbt only scans directories listed in `model-paths` for source declarations.

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

Configures dbt to use DuckDB with a local database file. This is for local development only — dbt Cloud manages its own connections.

### `zea_export.json`

Records every load, filter, and SQL transformation in the session lineage, plus source URIs, row counts, and portability status. This is how a reviewer understands where a model came from without having to re-run the pipeline.

---

## Running in dbt

Clone the published repo and run with dbt-DuckDB.

> **Python version:** dbt requires Python 3.12. It does not yet support Python 3.13 or 3.14. If `python3 --version` shows 3.13+ (common on recent macOS Homebrew), install 3.12 explicitly.

On macOS the cleanest path is `pipx`:

```bash
brew install pipx python@3.12
pipx install dbt-duckdb --python $(brew --prefix python@3.12)/bin/python3.12
pipx ensurepath   # adds ~/.local/bin to PATH; restart your shell once after this
```

Or with a virtual environment:

```bash
brew install python@3.12
python3.12 -m venv .venv
source .venv/bin/activate
pip install dbt-duckdb
```

Then:

```bash
git clone https://github.com/lmccay/nyc-taxi-dbt
cd nyc-taxi-dbt
dbt run
```

DuckDB fetches the source Parquet directly from the NYC TLC CloudFront URL at query time. No data loading step, no warehouse, no credentials. Expected output:

```
Running with dbt=1.8.x
Found 4 models, 8 tests, 1 source

Concurrency: 4 threads

1 of 4 START sql table model main.long_trips ...................... [RUN]
1 of 4 OK created sql table model main.long_trips ................. [OK in 4.1s]
2 of 4 START sql table model main.zone_revenue_by_pickup .......... [RUN]
2 of 4 OK created sql table model main.zone_revenue_by_pickup ..... [OK in 0.2s]
3 of 4 START sql table model main.avg_tip_by_payment .............. [RUN]
3 of 4 OK created sql table model main.avg_tip_by_payment ......... [OK in 3.8s]
4 of 4 START sql table model main.payment_mix_by_zone ............. [RUN]
4 of 4 OK created sql table model main.payment_mix_by_zone ........ [OK in 0.1s]

Finished running 4 models in 0:00:08.4 seconds.
All 4 models were created successfully.
```

`long_trips` runs first because `zone_revenue_by_pickup` depends on it (`{{ ref('long_trips') }}`). The first HTTP fetch takes a few seconds as DuckDB downloads the source Parquet and caches it locally. Subsequent runs are faster.

Run the generated column tests:

```bash
dbt test
```

```
Found 3 models, 8 tests, 1 source

1 of 8 PASS not_null_zone_revenue_by_pickup_PULocationID ........... [PASS]
2 of 8 PASS not_null_zone_revenue_by_pickup_trips .................. [PASS]
3 of 8 PASS not_null_zone_revenue_by_pickup_total_fare ............. [PASS]
4 of 8 PASS not_null_zone_revenue_by_pickup_avg_tip ................ [PASS]
5 of 8 PASS not_null_avg_tip_by_payment_payment_type ............... [PASS]
6 of 8 PASS not_null_avg_tip_by_payment_avg_tip .................... [PASS]
7 of 8 PASS not_null_avg_tip_by_payment_trips ...................... [PASS]
8 of 8 PASS not_null_payment_mix_by_zone_PULocationID .............. [PASS]

Finished running 8 tests.
All 8 passed.
```

Generate the dbt docs site:

```bash
dbt docs generate
dbt docs serve
```

This produces a browsable data catalog showing the full model DAG, column descriptions, test results, and source lineage — all from the files ZeaOS generated.

---

## What dbt Does With the Repo

To be precise about the end-to-end flow:

1. **dbt reads `dbt_project.yml`** — finds the project name, model paths, materialization defaults, and the `on-run-start` hook.
2. **dbt parses `models/*.sql`** — builds a DAG from the `{{ source() }}` and `{{ ref() }}` Jinja calls.
3. **dbt resolves `sources/zea_sources.yml`** — maps `source('zea_http', 'yellow_tripdata_2024_01')` to the `zea_http` schema, verifying the source declaration exists at compile time.
4. **`on-run-start` runs `{{ stage_zea_sources() }}`** — the macro in `macros/stage_zea_sources.sql` executes `CREATE SCHEMA IF NOT EXISTS zea_http` and `CREATE OR REPLACE VIEW zea_http.yellow_tripdata_2024_01 AS SELECT * FROM read_parquet('https://...')` in DuckDB before any model runs.
5. **dbt compiles the SQL** — substitutes Jinja calls with actual table references for the target adapter.
6. **DuckDB executes** — `long_trips` first (it has no upstream models), then `zone_revenue_by_pickup` and `avg_tip_by_payment` in parallel. DuckDB reads the source Parquet directly from HTTPS.
7. **Models are materialised** as tables in `local.duckdb`.

`zea_export.json` is not read by dbt — it is metadata for humans and tools. It tells a reviewer the complete provenance of each model: which session produced it, what transformations were applied, what the source URLs are, and whether the SQL is portable to other adapters.

---

## Next Steps

**Add more tests** — the generated YAML has `not_null` for every column. Add `unique` tests for IDs, `accepted_values` for `payment_type`, and `dbt_utils.expression_is_true` for numeric range checks before sharing with a team.

**Fill in descriptions** — column descriptions are blank placeholders. Add business context before the models go into a shared project.

**Moving to a cloud warehouse** — when you're ready to run on BigQuery, Snowflake, or Redshift, the source data needs to land in the warehouse first (via Fivetran, Airbyte, or a custom load). Replace the `{{ source('zea_http', ...) }}` references with references to your warehouse sources. The transformation SQL stays the same.

**Saving a default repo** — set a default so you never need to type `--repo` again:

```
ZeaOS> model publish set-repo lmccay/nyc-taxi-dbt
```

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs). Source: [github.com/open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos)*
{% endraw %}
