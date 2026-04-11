---
title: "Getting Started with ZeaOS and MinIO"
---

# Getting Started with ZeaOS and MinIO

*Load, explore, and publish data as Apache Iceberg — no cloud account, no pipeline code, no warehouse required. This guide uses Docker Compose to run ZeaOS alongside a local MinIO instance. The same ZeaOS configuration works against any existing MinIO cluster you already have.*

---

## What You'll Build

By the end of this guide you will have:

- Loaded a public dataset (NYC taxi trips, ~2.9M rows) into a ZeaOS session
- Run interactive transforms: filters, aggregations, pivots
- Loaded a file from your own machine via a mounted data directory
- Published the results as Apache Iceberg v2 tables to MinIO
- Queried the Iceberg tables back with DuckDB's `iceberg_scan`

Everything runs locally. The only prerequisites are Docker Desktop and a terminal.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Mac, Windows, or Linux)
- Git (to clone the repo) or the ZeaOS release archive

> **Windows note:** Docker Desktop on Windows uses Linux containers. ZeaOS runs fully in this mode. The only feature not available in containers is ZeaDrive FUSE cloud mounting — all other operations including S3 push, Iceberg write/read, and local file access work normally.

---

## Step 1 — Clone and Start

```bash
git clone https://github.com/open-tempest-labs/zeaos
cd zeaos

docker compose up -d minio minio-init
docker compose run --rm zeaos
```

The first run builds the ZeaOS image and downloads Go dependencies — this takes a few minutes. Subsequent starts are fast.

You should see:

```
ZeaOS — Zero-copy Data REPL from Open Tempest Labs
Tables: []   Drive: ~/zeadrive [sdk — no mount]   v1.0.0
ZeaOS>
```

`[sdk — no mount]` means ZeaOS is talking to MinIO directly via the S3 SDK — no FUSE mount needed in the container.

---

## Step 2 — Load a Public Dataset

Load the NYC Yellow Taxi trip data for January 2024 directly from HTTPS — no download required ahead of time:

```
ZeaOS> trips = load https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet ...
→ trips: 2_964_624 rows × 19 cols
```

Take a look at the schema:

```
ZeaOS> describe trips
Table:   trips
Rows:    2964624
Columns: 19

Column                          Type
──────────────────────────────────────────────────
VendorID                        int32
tpep_pickup_datetime            timestamp[us]
tpep_dropoff_datetime           timestamp[us]
passenger_count                 double
trip_distance                   double
RatecodeID                      double
store_and_fwd_flag              large_utf8
PULocationID                    int32
DOLocationID                    int32
payment_type                    int64
fare_amount                     double
extra                           double
mta_tax                         double
tip_amount                      double
tolls_amount                    double
improvement_surcharge           double
total_amount                    double
congestion_surcharge            double
airport_fee                     double
```

---

## Step 3 — Load Your Own Data

Drop any Parquet, CSV, or JSON file into the `data/` directory on your host machine:

```bash
# In a separate terminal on your host
cp ~/Downloads/sales_q4.csv ./data/
```

Inside the ZeaOS REPL, load it from `/data/` — the directory is mounted directly into the container:

```
ZeaOS> sales = load /data/sales_q4.csv
→ sales: 84_203 rows × 12 cols
```

The `data/` directory persists on your host between container restarts. To use a different directory, copy `.env.example` to `.env` and set `ZEA_DATA_DIR`:

```bash
cp .env.example .env
# edit .env and set ZEA_DATA_DIR=/your/datasets
docker compose run --rm zeaos
```

---

## Step 4 — Explore and Transform

Filter, aggregate, and pivot using the pipe syntax or raw SQL. All operations run in Arrow memory — nothing is written to disk until you push or save.

```
ZeaOS> long_trips = trips | where trip_distance > 5
→ long_trips: 1_048_412 rows × 19 cols

ZeaOS> zone_revenue = long_trips | group PULocationID sum(fare_amount)
→ zone_revenue: 254 rows × 2 cols

ZeaOS> top_zones = zone_revenue | top 10
→ top_zones: 10 rows × 2 cols

ZeaOS> zeaview top_zones
```

The viewer opens in the terminal — sort with `s`, filter with `f`, export with `e`.

For more complex analysis, drop into SQL:

```
ZeaOS> avg_tip = zeaql "SELECT payment_type, COUNT(*) AS trips, ROUND(AVG(tip_amount), 2) AS avg_tip FROM trips GROUP BY payment_type ORDER BY trips DESC"
→ avg_tip: 5 rows × 3 cols

ZeaOS> payment_pivot = zeaql "PIVOT (SELECT PULocationID, payment_type, fare_amount FROM long_trips LIMIT 5000) ON payment_type USING first(fare_amount)"
→ payment_pivot: 254 rows × 7 cols
```

Check your session at any point:

```
ZeaOS> status
```

---

## Step 5 — Push to MinIO as Apache Iceberg

Push `zone_revenue` to MinIO as an Apache Iceberg v2 table:

```
ZeaOS> push zone_revenue --target zea://s3-data/analytics --iceberg

Push to: zea://s3-data/analytics [Y/n]: Y
Schema name [analytics]: 

Pushing 1 table(s) to zea://s3-data/analytics...
  ✓ zone_revenue → analytics.zone_revenue  254 rows
Push complete. Run 'push status' to review.
```

ZeaOS writes the full Iceberg table structure to MinIO: Parquet data files with embedded field IDs, manifest files, a metadata JSON snapshot, and a `version-hint.text` file. No Spark, no Glue catalog, no warehouse.

Push the other tables:

```
ZeaOS> push avg_tip payment_pivot --target zea://s3-data/analytics --iceberg

Pushing 2 table(s) to zea://s3-data/analytics...
  ✓ avg_tip → analytics.avg_tip  5 rows
  ✓ payment_pivot → analytics.payment_pivot  254 rows
Push complete.
```

Verify the snapshots landed correctly:

```
ZeaOS> iceberg verify zone_revenue avg_tip payment_pivot
zea://s3-data/analytics/analytics/zone_revenue
  Snapshot             Status      Rows          Change          Session
  ──────────────────────────────────────────────────────────────────────────────────────
  1744000000000        ✓ verified  254           first verify    .../zeaos

  ✓ 1 snapshot(s) verified, 1 new baseline(s) established

zea://s3-data/analytics/analytics/avg_tip
  Snapshot             Status      Rows          Change          Session
  ──────────────────────────────────────────────────────────────────────────────────────
  1744000001000        ✓ verified  5             first verify    .../zeaos

  ✓ 1 snapshot(s) verified, 1 new baseline(s) established

zea://s3-data/analytics/analytics/payment_pivot
  Snapshot             Status      Rows          Change          Session
  ──────────────────────────────────────────────────────────────────────────────────────
  1744000002000        ✓ verified  254           first verify    .../zeaos

  ✓ 1 snapshot(s) verified, 1 new baseline(s) established
```

---

## Step 6 — Query Iceberg Tables Back with DuckDB

Read the Iceberg tables back from MinIO inside the ZeaOS session:

```
ZeaOS> t = zeaql "SELECT * FROM iceberg_scan('s3://zeaos-data/analytics/zone_revenue') ORDER BY sum_fare_amount DESC LIMIT 5"
→ t: 5 rows × 2 cols

ZeaOS> zeaview t
```

Or open a DuckDB shell on your host machine and point it at MinIO directly:

```bash
# In a separate terminal on your host
duckdb
```

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

SET s3_endpoint='localhost:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';

SELECT * FROM iceberg_scan('s3://zeaos-data/analytics/zone_revenue')
ORDER BY sum_fare_amount DESC
LIMIT 10;
```

The Iceberg tables written by ZeaOS are standard Apache Iceberg v2 — any tool with an Iceberg reader can open them. DuckDB, PyIceberg, Spark, and Trino all work against the same files.

---

## Step 7 — Save Your Session

ZeaOS sessions persist automatically across container restarts via the `zeaos-session` Docker volume. Tables are spilled to Parquet in `~/.zeaos/tables/` and reloaded next time you start:

```
ZeaOS> exit

# Later...
docker compose run --rm zeaos
```

```
ZeaOS — Zero-copy Data REPL from Open Tempest Labs
Tables: [trips, long_trips, zone_revenue, top_zones, avg_tip, payment_pivot]
ZeaOS>
```

To save a specific table to your local `data/` directory:

```
ZeaOS> save zone_revenue /data/zone_revenue.parquet
saved zone_revenue → /data/zone_revenue.parquet
```

---

## Connecting an Existing MinIO Cluster

If you already have a MinIO instance, skip the docker-compose MinIO service entirely. Run ZeaOS directly:

```bash
docker run -it \
  -e ZEA_TEST_S3_ENDPOINT=https://minio.yourorg.com \
  -e ZEA_TEST_S3_BUCKET=your-bucket \
  -e AWS_ACCESS_KEY_ID=your-access-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret-key \
  -v /your/local/data:/data \
  zeaos:latest
```

Or install ZeaOS natively (see [installation](installation.md)) and run `enable-s3` to configure the connection interactively:

```
ZeaOS> enable-s3
```

The TUI form accepts any S3-compatible endpoint — MinIO, Cloudflare R2, Backblaze B2, Wasabi, or standard AWS S3. The same `push --iceberg` and `iceberg_scan` workflow applies to all of them.

---

## What's Next

**Promote tables as named models** — mark analytical views for downstream use and publish them as dbt-compatible SQL to a GitHub repository:

```
ZeaOS> model promote zone_revenue as zone_revenue_by_pickup model
ZeaOS> model publish --repo yourname/nyc-taxi-models --new
```

See the [NYC Taxi publish tutorial](tutorial-nyc-taxi-publish.md) for the full walkthrough.

**Use MotherDuck as your warehouse** — push tables to MotherDuck for persistent SQL access from anywhere:

```
ZeaOS> push --target md:my_database
```

**Run tests in CI** — the same docker-compose stack runs the full test suite:

```bash
make docker-test
```

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs). Source: [github.com/open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos)*
