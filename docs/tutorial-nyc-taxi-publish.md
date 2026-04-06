---
title: "Publishing NYC Taxi Analysis to dbt via GitHub"
---

# Publishing NYC Taxi Analysis to dbt via GitHub

*This tutorial picks up directly from the [NYC Taxi Analysis](tutorial-nyc-taxi) walkthrough. By the end you will have promoted the session's key analytical tables, validated their portability, published a complete dbt Core project to GitHub, and run it locally with `dbt run`.*

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

## Deciding What to Promote

Not every table in a session belongs in a dbt project. The rule of thumb:

- **Promote** derived tables that represent finished analytical views — the tables a downstream consumer would actually query.
- **Skip** intermediate scratch tables (`long_trips`, `zone_payment`) — these become `{{ source() }}` or `{{ ref() }}` references inside the promoted models, not standalone models themselves.
- **Flag** tables with non-portable operations (PIVOT uses DuckDB-specific syntax). Promote them with a warning; the reviewer decides whether to ship them.

For the taxi analysis the right set is:

| Session table | Export name | Why |
|---|---|---|
| `zone_revenue` | `zone_revenue_by_pickup` | Core revenue analysis by pickup zone |
| `avg_tip` | `avg_tip_by_payment` | Tip behaviour by payment type |
| `payment_pivot` | `payment_mix_by_zone` | Payment breakdown across top zones (non-portable — PIVOT) |

---

## Option A: Manual Promote → Validate → Publish

### Step 1 — Promote

```
ZeaOS> promote zone_revenue as zone_revenue_by_pickup model
promoted zone_revenue → zone_revenue_by_pickup (model)

ZeaOS> promote avg_tip as avg_tip_by_payment model
promoted avg_tip → avg_tip_by_payment (model)

ZeaOS> promote payment_pivot as payment_mix_by_zone model
promoted payment_pivot → payment_mix_by_zone (model)
```

Check the promotions list:

```
ZeaOS> list --type=promotions
Export Name               Kind        Source Table          Promoted At
────────────────────────────────────────────────────────────────────────
zone_revenue_by_pickup    model       zone_revenue          2026-04-06 09:00:00
avg_tip_by_payment        model       avg_tip               2026-04-06 09:00:01
payment_mix_by_zone       model       payment_pivot         2026-04-06 09:00:02
```

### Step 2 — Validate

Run `validate --target=dbt` against each artifact to catch portability issues before they land in the repo:

```
ZeaOS> validate zone_revenue_by_pickup --target=dbt
Validating zone_revenue_by_pickup (from zone_revenue)...
  ✓ SQL parses correctly
  ✓ 262 rows × 4 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
  ✓ portable: duckdb dialect, no non-standard functions detected

ZeaOS> validate avg_tip_by_payment --target=dbt
Validating avg_tip_by_payment (from avg_tip)...
  ✓ SQL parses correctly
  ✓ 5 rows × 3 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
  ✓ portable: duckdb dialect, no non-standard functions detected

ZeaOS> validate payment_mix_by_zone --target=dbt
Validating payment_mix_by_zone (from payment_pivot)...
  ✓ SQL parses correctly
  ✓ 10 rows × 7 cols
  ✓ source URIs: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
  ⚠  PIVOT uses DuckDB-specific syntax — may not run on other adapters
```

Two models are fully portable. `payment_mix_by_zone` works on DuckDB but will need manual rewriting for BigQuery, Snowflake, or Redshift. The warning is recorded in the model YAML's `meta.zea_warnings` field so it's visible to reviewers.

### Step 3 — Publish to GitHub

Create a new GitHub repository and push the bundle in one command. If `gh` is already authenticated, no token setup is needed — ZeaOS picks it up automatically:

```
ZeaOS> publish --repo lmccay/nyc-taxi-dbt --new
Generating export bundle...
creating repository lmccay/nyc-taxi-dbt...
initialising repo with dbt scaffold...
  wrote models/zone_revenue_by_pickup.sql
  wrote models/zone_revenue_by_pickup.yml
  wrote models/avg_tip_by_payment.sql
  wrote models/avg_tip_by_payment.yml
  wrote models/payment_mix_by_zone.sql
  wrote models/payment_mix_by_zone.yml
  wrote sources/zea_sources.yml
  wrote dbt_project.yml
  wrote profiles.yml
  wrote zea_export.json
  wrote README.md
  wrote .gitignore
commit: ZeaOS v0.2.0: initial export — zone_revenue_by_pickup, avg_tip_by_payment, payment_mix_by_zone
pushing to lmccay/nyc-taxi-dbt main...
✓ Published to https://github.com/lmccay/nyc-taxi-dbt
```

To push as a PR to an existing team project instead:

```
ZeaOS> publish --repo acme-data/dbt-main --pr
```

---

## Option B: One-Shot with `--auto-promote`

If you want ZeaOS to decide what to promote and push everything in a single command:

```
ZeaOS> publish --repo lmccay/nyc-taxi-dbt --new --auto-promote
Auto-promoting session tables...
  promoted trips
  promoted cc_trips
  promoted jfk_trips
  promoted long_trips
  promoted zone_revenue
  promoted top_zones
  promoted zone_payment
  promoted avg_tip
  promoted payment_pivot (⚠  non-portable: PIVOT uses DuckDB-specific syntax)
Generating export bundle...
creating repository lmccay/nyc-taxi-dbt...
  ...
✓ Published to https://github.com/lmccay/nyc-taxi-dbt
```

`--auto-promote` promotes every eligible session table that isn't already promoted. It skips tables with names that start with `_` (internal scratch) and tables where SQL reconstruction fails. Non-portable tables are promoted with a warning, not silently dropped.

For a first exploration this is a good way to get everything into a repo quickly and then prune from there. For a production handoff the selective approach in Option A produces a cleaner result.

---

## What Lands in the Repo

```
nyc-taxi-dbt/
├── models/
│   ├── zone_revenue_by_pickup.sql
│   ├── zone_revenue_by_pickup.yml
│   ├── avg_tip_by_payment.sql
│   ├── avg_tip_by_payment.yml
│   ├── payment_mix_by_zone.sql
│   └── payment_mix_by_zone.yml
├── sources/
│   └── zea_sources.yml
├── dbt_project.yml
├── profiles.yml
├── zea_export.json
├── README.md
└── .gitignore
```

### `models/zone_revenue_by_pickup.sql`

```sql
{{ config(materialized='table') }}

SELECT PULocationID, COUNT(*) AS trips, ROUND(SUM(fare_amount), 2) AS total_fare, ROUND(AVG(tip_amount), 2) AS avg_tip
FROM {{ source('zea_http', 'long_trips') }}
GROUP BY PULocationID
ORDER BY total_fare DESC
```

Session table references are rewritten to `{{ source() }}` (for HTTP-loaded tables) and `{{ ref() }}` (for derived session tables). The full lineage chain — `trips → long_trips → zone_revenue` — is preserved in `zea_export.json`.

### `sources/zea_sources.yml`

```yaml
version: 2

# generated by ZeaOS — safe to overwrite

sources:
  - name: zea_http
    description: "ZeaOS HTTP data sources"
    tables:
      - name: long_trips
        description: "Filtered trips from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet (where trip_distance > 2.0)"
```

The "generated by ZeaOS" marker lets `publish` merge safely into the file on subsequent pushes without overwriting sources added by other team members.

### `zea_export.json`

Records the full lineage for each artifact: every load, filter, and SQL transformation, plus row counts, source URIs, portability status, and the session ID. This is the artifact reviewers use to understand where a model came from without having to run the pipeline themselves.

---

## Running in dbt

```bash
cd nyc-taxi-dbt
pip install dbt-duckdb

dbt debug --profiles-dir .   # verify connection
dbt run                       # materialise the three models
dbt test                      # run generated column tests
```

Expected output:

```
Running with dbt=1.8.x
Found 3 models, 12 tests, 1 source

Concurrency: 4 threads

1 of 3 START sql table model main.zone_revenue_by_pickup .......... [RUN]
1 of 3 OK created sql table model main.zone_revenue_by_pickup ..... [OK in 2.1s]
2 of 3 START sql table model main.avg_tip_by_payment .............. [RUN]
2 of 3 OK created sql table model main.avg_tip_by_payment ......... [OK in 0.3s]
3 of 3 START sql table model main.payment_mix_by_zone ............. [RUN]
3 of 3 OK created sql table model main.payment_mix_by_zone ........ [OK in 0.2s]

Finished running 3 models in 0:00:02.6 seconds.
All 3 models were created successfully.
```

The `dbt test` pass validates the `not_null` column tests generated from the Arrow schema. Edit the `.yml` files to add `unique`, `accepted_values`, or relationship tests before promoting to a shared project.

---

## Next Steps After Publishing

Once the bundle is in GitHub:

1. **Replace `{{ source() }}` refs** with `{{ ref() }}` pointers to your project's existing staging models where appropriate — `long_trips` in the source becomes a reference to your `stg_trips` model.

2. **Fill in column descriptions** in the `.yml` files. ZeaOS generates `description: ""` placeholders; add business-context text before merging.

3. **Add more tests.** The generated YAML has `not_null` tests for every column. Add `unique` tests for IDs, `accepted_values` for payment types, and `dbt_utils.expression_is_true` for numeric range checks.

4. **Configure a production profile.** The generated `profiles.yml` targets DuckDB local. Swap it for BigQuery, Snowflake, or Redshift as needed — but note that `payment_mix_by_zone` uses PIVOT and will need adapter-specific rewriting.

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs). Source: [github.com/open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos)*
