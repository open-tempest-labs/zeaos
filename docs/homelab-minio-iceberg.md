---
title: "Your Personal Data Lakehouse with ZeaOS and MinIO"
---

# Your Personal Data Lakehouse with ZeaOS and MinIO

*If you already run a MinIO instance at home — on a Proxmox node, a Raspberry Pi, a NAS, or an old workstation — you have everything you need to build a serious data lakehouse. ZeaOS turns your MinIO bucket into an Apache Iceberg warehouse: load data from anywhere, transform it interactively, push it as a versioned Iceberg table, and query it with any standard tool. No Spark. No AWS. No recurring bill.*

---

## What a Homelab Data Lakehouse Looks Like

The pattern is simple:

1. **Load** raw data from a source — a Home Assistant CSV export, a Proxmox log file, a pfSense traffic export, a downloaded dataset
2. **Transform** it interactively in ZeaOS — filter, aggregate, join, pivot
3. **Push** the result to MinIO as an Apache Iceberg table
4. The next day, **load new data and push again** — ZeaOS appends a new snapshot to the existing table, preserving the full history

After a month, you have a time-series Iceberg table where each snapshot is one day's worth of data. DuckDB, PyIceberg, or any Iceberg-capable tool can read it from your MinIO endpoint. The data never leaves your network.

---

## Prerequisites

- A running MinIO instance (any version — on bare metal, in Docker, or on a NAS)
- Docker installed on the machine you'll run ZeaOS from
- Your MinIO access key, secret key, bucket name, and endpoint URL

If you don't have MinIO running yet, the [Getting Started with Docker](getting-started-docker.md) guide spins one up locally in five minutes.

---

## Step 1 — Connect ZeaOS to Your MinIO Instance

Run the ZeaOS container and point it at your MinIO endpoint. Set the environment variables for your cluster:

```bash
docker run -it \
  -e ZEA_TEST_S3_ENDPOINT=http://minio.homelab.local:9000 \
  -e ZEA_TEST_S3_BUCKET=analytics \
  -e AWS_ACCESS_KEY_ID=your-access-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret-key \
  -e AWS_REGION=us-east-1 \
  -v /mnt/nas/datasets:/data \
  --name zeaos \
  ghcr.io/open-tempest-labs/zeaos:latest
```

Or use the docker-compose stack from the getting-started guide, replacing the MinIO environment variables with your cluster's values in a `.env` file:

```bash
# .env
ZEA_TEST_S3_ENDPOINT=http://minio.homelab.local:9000
ZEA_TEST_S3_BUCKET=analytics
MINIO_USER=your-access-key
MINIO_PASSWORD=your-secret-key
ZEA_DATA_DIR=/mnt/nas/datasets
```

You should see:

```
ZeaOS — Zero-copy Data REPL from Open Tempest Labs
Tables: []   Drive: ~/zeadrive [sdk — no mount]   v1.0.0
ZeaOS>
```

`[sdk — no mount]` confirms ZeaOS is talking to MinIO directly via the S3 SDK.

---

## Step 2 — Day 1: Load and Push Your First Snapshot

This guide includes sample data files so you can follow along without any Home Assistant setup. Generate them by running the script included in the `data/` directory on your host machine:

```bash
python3 data/generate-sample-data.py
# wrote 3600 rows → data/home_assistant_2025_01.csv
# wrote 3600 rows → data/home_assistant_2025_02.csv
```

This produces two days of simulated readings from five energy sensors (energy total, solar production, HVAC, kitchen, and washer) at 2-minute intervals. If you do run Home Assistant, replace these files with your own exports from **Settings → System → Storage → Download data** and the same workflow applies.

Inside the ZeaOS REPL, load day one:

```
ZeaOS> energy = load /data/home_assistant_2025_01.csv
→ energy: 3_600 rows × 6 cols

ZeaOS> describe energy
Table:   energy
Rows:    3600
Columns: 6

Column           Type
────────────────────────────────
entity_id        large_utf8
state            large_utf8
last_changed     large_utf8
last_updated     large_utf8
attributes       large_utf8
domain           large_utf8
```

Clean it up — cast timestamps and numeric state values:

```
ZeaOS> energy_clean = zeaql "
  SELECT
    entity_id,
    CAST(last_changed AS TIMESTAMP) AS recorded_at,
    CAST(state AS DOUBLE) AS kwh
  FROM energy
  WHERE domain = 'sensor'
    AND TRY_CAST(state AS DOUBLE) IS NOT NULL
  ORDER BY recorded_at
"
→ energy_clean: 3_600 rows × 3 cols
```

Push it to MinIO as an Iceberg table:

```
ZeaOS> push energy_clean --target zea://s3-data/analytics --iceberg

Push to: zea://s3-data/analytics [Y/n]: Y
Schema name [analytics]: home

Pushing 1 table(s) to zea://s3-data/analytics...
  ✓ energy_clean → home.energy_clean  3600 rows
Push complete. Run 'push status' to review.
```

ZeaOS writes the full Iceberg v2 structure to your MinIO bucket: a Parquet data file with embedded field IDs, a manifest, a manifest list, a metadata JSON snapshot, and a `version-hint.text` pointer. Snapshot 1 is live.

---

## Step 3 — Day 2: Append a New Snapshot

Load the second sample file and transform it into the same session table name — `energy_clean`. Pushing the same name to the same schema is what triggers the append path:

```
ZeaOS> energy_raw = load /data/home_assistant_2025_02.csv
→ energy_raw: 3_600 rows × 6 cols

ZeaOS> energy_clean = zeaql "
  SELECT
    entity_id,
    CAST(last_changed AS TIMESTAMP) AS recorded_at,
    CAST(state AS DOUBLE) AS kwh
  FROM energy_raw
  WHERE domain = 'sensor'
    AND TRY_CAST(state AS DOUBLE) IS NOT NULL
  ORDER BY recorded_at
"
→ energy_clean: 3_600 rows × 3 cols

ZeaOS> push energy_clean --target zea://s3-data/analytics --iceberg

Push to: zea://s3-data/analytics [Y/n]: Y
Schema name [analytics]: home

Pushing 1 table(s) to zea://s3-data/analytics...
  ✓ energy_clean → home.energy_clean  3600 rows
Push complete.
```

ZeaOS detected that `home.energy_clean` already exists in your MinIO bucket and appended a second snapshot instead of recreating the table. The existing snapshot 1 data is untouched. Snapshot 2 is a new Parquet file in `s3://analytics/home/energy_clean/data/`. The metadata JSON at version 2 references both snapshots.

After 31 days of this, your table has 31 snapshots — the full month of readings, immutable, versioned, queryable.

---

## Step 4 — Verify Snapshot Integrity

Verify that every snapshot's data file matches its recorded hash:

```
ZeaOS> iceberg verify energy_clean
home.energy_clean
  Snapshot             Status      Rows          Change          Session
  ──────────────────────────────────────────────────────────────────────────────────────
  1735689600000        ✓ verified  3600          first verify    .../zeaos
  1735776000000        ✓ verified  3600          first verify    .../zeaos

  ✓ 2 snapshot(s) verified, 2 new baseline(s) established
```

Run `iceberg verify` as a daily cron job to detect accidental mutations or storage corruption early.

---

## Step 5 — Query Across All Snapshots with DuckDB

From any machine with DuckDB and access to your MinIO endpoint:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

SET s3_endpoint='minio.homelab.local:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id='your-access-key';
SET s3_secret_access_key='your-secret-key';

-- Query all data across both snapshots
SELECT
  entity_id,
  DATE_TRUNC('day', recorded_at) AS day,
  SUM(kwh) AS total_kwh
FROM iceberg_scan('s3://analytics/home/energy_clean')
GROUP BY 1, 2
ORDER BY 1, 2;
```

DuckDB reads Parquet data files from both snapshots and returns the full month.

You can also query from inside a running ZeaOS session:

```
ZeaOS> monthly = zeaql "
  SELECT
    entity_id,
    DATE_TRUNC('day', recorded_at) AS day,
    SUM(kwh) AS total_kwh
  FROM iceberg_scan('s3://analytics/home/energy_clean')
  GROUP BY 1, 2
  ORDER BY 1, 2
"
→ monthly: 248 rows × 3 cols

ZeaOS> zeaview monthly
```

---

## More Homelab Data Sources

The same load → transform → push → append workflow applies to any structured data your homelab produces.

**Proxmox node statistics** — export via the Proxmox API or `pvesh` CLI:
```
ZeaOS> proxmox = load /data/proxmox_nodes_2025_01.csv
ZeaOS> push proxmox --target zea://s3-data/analytics --iceberg
```

**Pi-hole query log** — Pi-hole writes a SQLite database at `/etc/pihole/pihole-FTL.db`; export with:
```bash
sqlite3 /etc/pihole/pihole-FTL.db \
  "SELECT * FROM queries WHERE timestamp > strftime('%s','now','-1 day')" \
  > /mnt/nas/datasets/pihole_2025_01.csv
```
Then in ZeaOS:
```
ZeaOS> dns = load /data/pihole_2025_01.csv
ZeaOS> push dns --target zea://s3-data/analytics --iceberg
```

**pfSense traffic export** — Enable NetFlow export to a collector like ntopng, or use the pfSense Diagnostics → Traffic Graph CSV export.

**Weather station data** — If you run a personal weather station that logs to a CSV or a local InfluxDB instance, export a day's readings and push them.

---

## Automating Daily Appends

Once the manual workflow is familiar, automate it with a shell script and a cron job on your host machine:

```bash
#!/bin/bash
# /usr/local/bin/zeaos-daily-push.sh

set -e
DATE=$(date +%Y_%m_%d)
EXPORT_DIR=/mnt/nas/datasets

# Export today's Home Assistant data
ha_client export --output "$EXPORT_DIR/energy_${DATE}.csv"

# Run ZeaOS non-interactively
docker run --rm \
  -e ZEA_TEST_S3_ENDPOINT=http://minio.homelab.local:9000 \
  -e ZEA_TEST_S3_BUCKET=analytics \
  -e AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
  -e AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  -v "$EXPORT_DIR:/data" \
  -v zeaos-session:/root/.zeaos \
  ghcr.io/open-tempest-labs/zeaos:latest \
  zeaos << 'EOF'
energy = load /data/energy_${DATE}.csv
energy_clean = zeaql "SELECT entity_id, CAST(last_changed AS TIMESTAMP) AS recorded_at, CAST(state AS DOUBLE) AS kwh FROM energy WHERE domain = 'sensor' AND TRY_CAST(state AS DOUBLE) IS NOT NULL"
push energy_clean --target zea://s3-data/analytics --iceberg --schema home
iceberg verify energy_clean
exit
EOF
```

Add it to cron:

```bash
# crontab -e
0 2 * * * /usr/local/bin/zeaos-daily-push.sh >> /var/log/zeaos-push.log 2>&1
```

After 30 days of automated pushes, your table has a month of history. After a year, it has 365 snapshots — a time-series dataset that any analytics tool can read from your MinIO endpoint.

---

## What You've Built

A personal data lakehouse with:

- **Immutable, versioned snapshots** — every push adds a snapshot; nothing is overwritten
- **Standard open format** — Apache Iceberg v2, readable by DuckDB, PyIceberg, Spark, Trino
- **Zero egress cost** — all data stays on your MinIO instance on your network
- **Cryptographic integrity** — `iceberg verify` checks SHA-256 of every data file against its recorded hash
- **No catalog service** — Iceberg metadata lives directly in your MinIO bucket as JSON files; no Hive Metastore, no Glue, no REST catalog required

Your data, your infrastructure, your warehouse.

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs). Source: [github.com/open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos)*
