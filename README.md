# ZeaOS

**The interactive data shell that takes you from raw file to Apache Iceberg warehouse — no Spark, no Glue, no pipeline code.**

```
ZZZZZZZZZZZZZZZZZZZ                                        OOOOOOOOO        SSSSSSSSSSSSSSS
Z:::::::::::::::::Z                                      OO:::::::::OO    SS:::::::::::::::S
Z:::::::::::::::::Z                                    OO:::::::::::::OO S:::::SSSSSS::::::S
Z:::ZZZZZZZZ:::::Z                                    O:::::::OOO:::::::OS:::::S     SSSSSSS
ZZZZZ     Z:::::Z      eeeeeeeeeeee    aaaaaaaaaaaaa  O::::::O   O::::::OS:::::S
        Z:::::Z      ee::::::::::::ee  a::::::::::::a O:::::O     O:::::OS:::::S
       Z:::::Z      e::::::eeeee:::::eeaaaaaaaaa:::::aO:::::O     O:::::O S::::SSSS
      Z:::::Z      e::::::e     e:::::e         a::::aO:::::O     O:::::O  SS::::::SSSSS
     Z:::::Z       e:::::::eeeee::::::e  aaaaaaa:::::aO:::::O     O:::::O    SSS::::::::SS
    Z:::::Z        e:::::::::::::::::e aa::::::::::::aO:::::O     O:::::O       SSSS::::SS
   Z:::::Z         e::::::eeeeeeeeeee a::::aaaa::::::aO:::::O     O:::::O            S:::::S
ZZZ:::::Z     ZZZZZe:::::::e         a::::a    a:::::aO::::::O   O::::::O            S:::::S
Z::::::ZZZZZZZZ:::Ze::::::::e        a::::a    a:::::aO:::::::OOO:::::::OSSSSSSS     S:::::S
Z:::::::::::::::::Z e::::::::eeeeeeeea:::::aaaa::::::a OO:::::::::::::OO S::::::SSSSSS:::::S
Z:::::::::::::::::Z  ee:::::::::::::e a::::::::::aa:::a  OO:::::::::OO   S:::::::::::::::SS
ZZZZZZZZZZZZZZZZZZZ    eeeeeeeeeeee    aaaaaaaaaa  aaaa    OOOOOOOOO      SSSSSSSSSSSSSSS
```

---

## What problem does ZeaOS solve?

**Getting data into a warehouse today requires either a Spark cluster, a managed catalog service, or bespoke Python that you write and maintain yourself.**

ZeaOS is the missing interactive layer. You load a file, explore it with pipe transforms and SQL, and publish it — as a fully compliant Apache Iceberg v2 table, to MotherDuck, or as flat Parquet — without writing a pipeline.

Three things that typically require custom code or heavy infrastructure, now in one shell:

- **Iceberg without Spark.** `push --iceberg` produces a DuckDB-ready Apache Iceberg v2 table on S3 — correct field IDs, sequence numbers, manifest format, and version hint. The spec compliance details that silently break hand-crafted tables are handled automatically.
- **Zero-copy interactive exploration.** Session tables live as Apache Arrow records in memory. Filter operations run via Arrow compute — no file re-reads, no intermediate Parquet. For million-row datasets the difference is instant vs. waiting.
- **Session persistence with embedded lineage.** Your named tables, their derivation chain, and their source URIs survive restarts. When you push, that lineage travels into the Iceberg snapshot — provenance you'd otherwise have to build separately.

---

## Quick Start

**Option A — Native install (macOS)**

```sh
brew tap open-tempest-labs/zeaos
brew install zeaos
zeaos
```

**Option B — Docker Compose (macOS, Linux, Windows)**

No install required. Spins up ZeaOS alongside a local MinIO instance for Iceberg push:

```sh
git clone https://github.com/open-tempest-labs/zeaos
cd zeaos
docker compose up -d minio minio-init
docker compose run --rm zeaos
```

See [Getting Started with Docker](docs/getting-started-docker.md) for the full walkthrough.

---

Once you have a REPL prompt:

```
ZeaOS> trips = load https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
downloading ...
→ trips: 2_964_624 rows × 19 cols

ZeaOS> long_trips = trips | where trip_distance > 5.0
→ long_trips: 1_219_108 rows × 19 cols

ZeaOS> avg_tip = zeaql "SELECT payment_type, AVG(tip_amount) AS avg_tip, COUNT(*) AS trips FROM trips GROUP BY payment_type"
→ avg_tip: 5 rows × 3 cols

ZeaOS> push --target zea://s3-data --iceberg
Push to: zea://s3-data/prod/nyc_taxi [Y/n] y
Pushing 3 table(s) to zea://s3-data...
  ✓ trips       → prod.nyc_taxi.trips       2964624 rows
  ✓ long_trips  → prod.nyc_taxi.long_trips  1219108 rows
  ✓ avg_tip     → prod.nyc_taxi.avg_tip     5 rows
Push complete.
```

Those tables are now Apache Iceberg v2 tables on S3, readable by DuckDB, Athena, Spark, and Trino with no catalog required:

```sql
-- In DuckDB / MotherDuck
SELECT * FROM iceberg_scan('s3://your-bucket/prod/nyc_taxi/avg_tip');
```

---

## Apache Iceberg

ZeaOS treats Iceberg as a first-class publish target. `push --iceberg` produces fully spec-compliant Apache Iceberg v2 tables that work out of the box with DuckDB's `iceberg_scan`, AWS Athena, Spark, and Trino.

Spec compliance details handled automatically — each one a real failure mode when crafting Iceberg tables by hand:

| Detail | Why it matters |
|--------|---------------|
| `PARQUET:field_id` in every column | DuckDB matches columns by field ID, not name. Missing field IDs cause `iceberg_scan` to return all NULLs with no warning. |
| `version-hint.text` as a bare integer | DuckDB constructs the metadata filename from this file's raw content. Any trailing newline or whitespace produces a wrong filename and a cryptic error. |
| `last-sequence-number` starting at 1 | DuckDB rejects manifests with sequence number 0. |
| Filesystem-resolvable manifest paths | Catalog URIs (`s3://`, `zea://`) in the manifest cannot be opened by DuckDB directly. ZeaOS stores the FUSE-resolved path. |
| Lineage in snapshot summary | Source URIs, SQL derivation chain, and session ID are embedded in the snapshot's summary properties — provenance that travels with the table. |

### Verifying an Iceberg table

```
ZeaOS> iceberg verify avg_tip
```

ZeaOS recomputes the SHA-256 of each data file and compares it against the hash recorded in the snapshot summary at push time.

### Reading pushed tables

```sql
-- DuckDB
SELECT * FROM iceberg_scan('/path/to/prod/nyc_taxi/avg_tip');

-- MotherDuck (after attaching the S3 path)
SELECT * FROM iceberg_scan('s3://bucket/prod/nyc_taxi/avg_tip');
```

---

## MotherDuck

Push session tables directly to [MotherDuck](https://motherduck.com) as native DuckDB tables:

```
ZeaOS> push --target md:my_db
```

On first use you will be prompted for a schema name. The target and schema are saved to `~/.zeaos/config.json` so subsequent pushes require no flags.

```
ZeaOS> push status
Table                     Target                     Pushed At             Format    Rows
────────────────────────────────────────────────────────────────────────────────────────────
avg_tip                   md:my_db/nyc_taxi.avg_tip  2026-04-11 10:30:00   parquet   5
trips                     md:my_db/nyc_taxi.trips    2026-04-11 10:30:01   parquet   2964624
```

Check for drift and re-push if the remote is stale:

```
ZeaOS> push sync --target md:my_db
```

---

## dbt Integration

ZeaOS can generate a dbt project bundle from promoted session tables — model SQL files and `sources.yml` — ready to drop into an existing dbt project.

### Promotions

`model promote` marks a session table as a named export artifact. There are two kinds:
- `model` — a dbt SQL model (the default)
- `semantic` — a semantic layer metric or entity

```
ZeaOS> model promote avg_tip as avg_tip_by_payment model
ZeaOS> model list
Export Name               Kind        Source Table          Promoted At
────────────────────────────────────────────────────────────────────────
avg_tip_by_payment        model       avg_tip               2026-04-11 10:30:00
```

**Promotions affect `push` behavior.** Once any promotion exists, a bare `push` (no explicit table names) will only push the source load-node tables in the promoted artifacts' lineage — not all session tables. This is intentional: it scopes the data push to exactly what the promoted models need.

To push a specific table regardless of promotions: `push <table> --target ...`

To see active promotions at any time: `model list` (also shown as a footer on `list`).

To remove a promotion: `model unpromote <name>`. When no promotions remain, bare `push` reverts to pushing all session tables.

Promotions persist across restarts.

### Generating the dbt bundle

```
ZeaOS> model validate                 # check SQL portability
ZeaOS> model export -o ./zea-dbt-export
```

The exported bundle contains model `.sql` files and `sources.yml` referencing the tables by their MotherDuck location. Push the data first, then run dbt against it.

---

## Commands

### Loading Data

```
t = load <file>                    # Parquet, CSV, TSV, JSON, JSONL
t = load https://host/file.parquet # Remote file — downloaded then loaded
t = load zea://data/file.parquet   # Via ZeaDrive (local or cloud)
t = zeaql "SELECT ..."             # Arbitrary SQL over session tables
```

### Pipe Transforms

Transforms chain with `|` and produce a new named table.

```
t2 = t1 | where <expr>              # Filter rows         e.g. amount > 100
t2 = t1 | select <cols>             # Pick columns        e.g. id, name, amount
t2 = t1 | top <n>                   # First N rows
t2 = t1 | group <col>               # Count by column
t2 = t1 | group <col> sum(<col2>)   # Aggregate
t2 = t1 | pivot <col>→<val>         # Pivot table
```

Chains compose naturally:

```
result = raw | where status = "active" | group region sum(revenue) | top 10
```

### Publishing

`push` always pushes all session tables (or named tables) to a destination. It is never affected by model promotion state — use `model push` for that.

```
push --target md:my_db              # Push all session tables to MotherDuck
push --target zea://s3-data         # Push as flat Parquet to ZeaDrive
push --target zea://s3-data --iceberg  # Push as Apache Iceberg v2
push <table> --target md:my_db      # Push a specific table
push status                         # Show push history
push sync --target md:my_db         # Re-push tables that have drifted
```

On first push without `--schema`, ZeaOS prompts interactively:

```
Push to: my_db.default [Y/n] n
  Schema [default]: nyc_taxi
Push to: my_db.nyc_taxi [Y/n] y
```

### Session Management

```
list                         # List session tables
drop <table>                 # Remove table from session
save <table> <path>          # Write to file (.parquet / .csv / .json / zea://)
hist                         # Table lineage DAG (TUI)
status                       # Session state: tables, drive, memory (TUI)
describe <table>             # Schema, row/col counts, lineage
```

### Viewing Data

```
zeaview <table>              # TUI viewer
                             #   s  sort    f  filter
                             #   g  graph   e  export
                             #   d  schema  ?  help
```

### ZeaDrive

`zea://` paths work everywhere. Local storage requires no setup. Cloud backends (S3-compatible) are added with `enable-s3` and backed by Volumez FUSE.

```
t = load zea://data/sales.parquet        # local: ~/.zeaos/local/data/
t = load zea://s3-data/warehouse/sales.parquet  # cloud: mounted S3 backend
save trips zea://data/trips.parquet      # write back to ZeaDrive

enable-s3                                # TUI form: configure S3 backend
zeadrive status                          # show backends and mount state
zeadrive ls [zea://path]                 # list files at a zea:// path (local, FUSE, or S3)
```

### Model

`model` manages named model artifacts — promote session tables, validate SQL portability, generate export bundles, and publish to downstream tooling. Run `model` with no arguments for the full subcommand reference.

```
model promote <table> [as <name>] [model|semantic]
model unpromote <name>...
model list
model validate [<name>]
model export [-o DIR]
model push --target <dest>          # push source data for promoted models only
model publish [<name>]              # publish model SQL to Git
```

### Iceberg

```
iceberg verify <table>       # Verify data file integrity against recorded hash
iceberg repair <table>       # Re-copy metadata to remote after a failed push
```

---

## Architecture

```
zeaos (REPL)
├── readline          input + history
├── Parser            shorthand pipe syntax → SQL
├── Arrow registry    session state (retained Arrow records)
├── arrowfilter       WHERE via Arrow compute (zero-copy, no DuckDB round-trip)
├── DuckDB CGO        GROUP BY, PIVOT, file I/O, SQL engine
├── ZeaShell libs     TUI viewer, expression parser, plugin runtime
├── zeaberg           Apache Iceberg v2 writer (zeaberg-go module)
└── ZeaDrive
    ├── zea://         → ~/.zeaos/local/  (always available, no setup)
    └── zea://<name>/  → ~/zeadrive/<name>/  (Volumez FUSE, cloud backends)
```

**Zero-copy design.** Session tables live as Apache Arrow record batches. Filter operations (`where`) use Arrow compute directly — DuckDB never touches the predicate. Projection and aggregation go through DuckDB's Arrow C Data Interface, reading in-place where possible. No intermediate Parquet files during a session.

**Session persistence.** On exit, tables are spilled to `~/.zeaos/tables/` as Parquet and restored on next launch. The session registry — including table names, row counts, source URIs, SQL derivation, and parent relationships — is saved to `~/.zeaos/session.json`.

**Iceberg compliance.** The `zeaberg` module (a separate Go module in this workspace) handles all Iceberg v2 metadata: manifest files, manifest lists, snapshot summaries, field ID embedding, and version hint format. It is intentionally decoupled so it can be used independently of the ZeaOS REPL.

---

## Building from Source

Requires Go 1.22+, a C compiler (for DuckDB CGO), and macFUSE (for ZeaDrive cloud backends).

```sh
git clone https://github.com/open-tempest-labs/zeaos
cd zeaos
make build
```

> **Note:** the `-tags duckdb_arrow` build tag is required for Arrow C Data Interface support. `make build` handles this automatically. Building with `go build` directly will produce a binary without Arrow integration.

Run all tests:

```sh
make test
```

---

## Ecosystem

| Repo | Description |
|------|-------------|
| [open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos) | This repo — the REPL and Iceberg push |
| [open-tempest-labs/zeashell](https://github.com/open-tempest-labs/zeashell) | Shell library, TUI viewer, plugin runtime |
| [open-tempest-labs/volumez](https://github.com/open-tempest-labs/volumez) | FUSE volume driver backing ZeaDrive |

---

## Further Reading

- [Getting Started with Docker and MinIO](docs/getting-started-docker.md)
- [ZeaOS → dbt Export](docs/dbt-export.md)
- [From Exploration to Production: Analysis That Knows Where It Came From](docs/whitepaper-exploration-to-production.md)
- [Home Energy Monitoring with ZeaOS, MinIO, and Iceberg](docs/homelab-minio-iceberg.md)
- [NYC Taxi Tutorial](docs/tutorial-nyc-taxi.md)

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs).*
