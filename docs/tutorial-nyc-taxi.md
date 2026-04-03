# End-to-End Data Analysis with ZeaOS: NYC Taxi Trips

*A walkthrough of ZeaOS's Arrow-native data pipeline — from raw Parquet to cloud storage — including the technical gaps we had to close to make DuckDB and Apache Arrow work together reliably at the query layer.*

<img width="764" height="425" alt="image" src="https://github.com/user-attachments/assets/b3d66cd1-aa55-453b-9009-76cdedb9dd97" />

---

## The Setup

ZeaOS is an interactive data REPL built on Apache Arrow and DuckDB. Every table you load lives in memory as retained Arrow record batches. DuckDB provides the SQL engine — GROUP BY, PIVOT, aggregations — while Arrow provides the zero-copy in-memory format. In theory these two fit together perfectly. In practice there are sharp edges in DuckDB's Arrow C Data Interface integration that block real analytical workloads. More on that below.

For this walkthrough we'll use the NYC Yellow Taxi trip data for January 2024 — a publicly available Parquet file from the NYC Taxi and Limousine Commission with roughly 2.9 million rows and 19 columns. It's a good test: it has integer columns (payment type, location IDs, passenger count), floats (fare, tip, distance), and enough rows to make the difference between correct and incorrect predicate evaluation obvious.

Start ZeaOS:

```
zeaos
```

---

## Loading the Data

The NYC TLC publishes monthly Parquet files directly over HTTPS — no account or API key required. ZeaOS downloads the file, hands it to DuckDB, and retains the result as Arrow record batches, all in one step:

```
ZeaOS> trips = load https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet ...
→ trips: 2_964_624 rows × 19 cols
```

The file is about 50 MB on the wire. On a typical broadband connection it loads in under a minute. ZeaOS streams it to a temp file, passes the path to DuckDB's `read_parquet`, pulls the result into Arrow record batches, and deletes the temp file. From this point forward, `trips` lives entirely in Arrow memory — nothing is written to disk during the session.

Get a quick look at the schema:

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
passenger_count                 int64
trip_distance                   double
RatecodeID                      int64
store_and_fwd_flag              large_utf8
PULocationID                    int64
DOLocationID                    int64
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

Notice that `payment_type`, `PULocationID`, `DOLocationID`, and `passenger_count` are all `int64`. This matters more than it should — read on.

---

## Filtering: Where Integer Columns Get Interesting

Payment type 1 is credit card. Let's pull out just those trips:

```
ZeaOS> cc_trips = trips | where payment_type = 1
→ cc_trips: 2_183_049 rows × 19 cols
```

That's about 73% of all trips — reasonable for New York.

Now let's look at long-haul trips from JFK (location ID 132):

```
ZeaOS> jfk_trips = trips | where PULocationID = 132
→ jfk_trips: 47_821 rows × 19 cols
```

These work correctly. But they only work correctly because ZeaOS evaluates `where` predicates using **Apache Arrow compute** rather than passing them to DuckDB.

> ### Technical Aside: DuckDB's Predicate Pushdown Bug with Arrow Integer Columns
>
> DuckDB reads Arrow data via the Arrow C Data Interface — specifically `duckdb_arrow_scan`, which registers an `ArrowArrayStream` as a scannable table. When DuckDB builds a query plan for something like `WHERE payment_type = 1`, it pushes the predicate down to the scan level, telling the Arrow stream to filter before returning rows.
>
> The problem: the stream doesn't actually apply the predicate. It returns all rows. DuckDB trusts that the scan already filtered and applies no additional filter of its own. The result is that integer column predicates silently return every row — not an error, just wrong data.
>
> String (VARCHAR) columns are not affected because DuckDB handles those through a different code path.
>
> This is a known bug in DuckDB's Arrow C Data Interface integration (present in go-duckdb v1.8.5 / DuckDB 1.1.x). It directly impacts any system using Arrow as an in-memory format with DuckDB as the compute engine — which is exactly the ADBC (Arrow Database Connectivity) use case.
>
> **ZeaOS's fix:** `where` operations are evaluated entirely in Go using Apache Arrow's compute package (`arrow/compute.FilterRecordBatch`) and zeashell's expression parser. DuckDB never sees the predicate. The filter runs directly on Arrow record batches, row by row, with zero data copy. This is the correct behaviour for an Arrow-native system regardless of what DuckDB does.
>
> *This aside will be removed or revised once the underlying DuckDB issue is resolved.*

---

## Aggregating: How Many Trips by Payment Type?

```
ZeaOS> by_payment = trips | group payment_type
→ by_payment: 5 rows × 2 cols

ZeaOS> zeaview by_payment
```

| payment_type | _count |
|---|---|
| 1 | 2183049 |
| 2 | 757341 |
| 0 | 22098 |
| 4 | 1538 |
| 5 | 598 |

Credit card (1) dominates. Cash (2) is about a quarter of trips. The small counts for 0, 4, and 5 represent no-charge and unknown categories. Payment type 3 (dispute) has no recorded trips in this month.

Now let's see average tip by payment type — this requires a SQL expression since we want `AVG` rather than `COUNT`:

```
ZeaOS> avg_tip = zeaql "SELECT payment_type, ROUND(AVG(tip_amount), 2) AS avg_tip, COUNT(*) AS trips FROM trips GROUP BY payment_type ORDER BY avg_tip DESC"
→ avg_tip: 5 rows × 3 cols

ZeaOS> zeaview avg_tip
```

| payment_type | avg_tip | trips |
|---|---|---|
| 1 | 3.84 | 2183049 |
| 0 | 0.00 | 22098 |
| 4 | 0.00 | 1538 |
| 5 | 0.00 | 598 |
| 2 | 0.00 | 757341 |

Credit card passengers tip an average of $3.84. Cash passengers tip $0.00 — almost certainly because cash tips don't get recorded in the system, not because New York cab riders are unusually stingy.

> ### Technical Aside: DuckDB GROUP BY and PIVOT Over Arrow C Streams
>
> Aggregation on Arrow data via DuckDB's Arrow C Data Interface is more fragile than filtering. When DuckDB executes a GROUP BY or PIVOT against a table backed by an `ArrowArrayStream` (registered via `RegisterView`), it crashes with a SIGSEGV in `duckdb_execute_prepared_arrow` — specifically at `addr=0x4`, a null pointer dereference in DuckDB's C++ result export code.
>
> The crash occurs regardless of thread count (`SET threads=1` does not help) and regardless of whether the output is collected via the Arrow or regular SQL path. The Arrow C Data Interface stream callbacks are invoked from DuckDB's GROUP BY executor in a way that corrupts internal state.
>
> This is a second critical gap for ADBC adoption: you can get Arrow data into DuckDB, but you cannot run the most common analytical operations — GROUP BY and PIVOT — against it without crashing the process. For any system trying to use Arrow as a zero-copy interchange format with DuckDB as the compute backend, this is a fundamental blocker.
>
> **ZeaOS's fix:** For GROUP BY and PIVOT operations, ZeaOS copies the source Arrow records into a native DuckDB in-memory table first (a simple `CREATE TABLE AS SELECT *` — no aggregation, which the Arrow stream handles fine), runs the aggregation against the native table, then reads the result back as Arrow. Only the source records for that specific operation are copied; the aggregated result is small. The working data for the session remains in Arrow throughout.
>
> The `zeaql` path (used for `AVG` above) works correctly because it reads from Parquet-originated or natively materialized DuckDB tables, bypassing the Arrow stream entirely.
>
> *This aside will be removed or revised once the underlying DuckDB issue is resolved.*

---

## Building an Analysis Pipeline

Let's chain operations into a more useful summary. We want the top 10 pickup zones by total revenue, but only for trips longer than 2 miles:

```
ZeaOS> long_trips = trips | where trip_distance > 2.0
→ long_trips: 1_847_203 rows × 19 cols
```

```
ZeaOS> zone_revenue = zeaql "SELECT PULocationID, COUNT(*) AS trips, ROUND(SUM(fare_amount), 2) AS total_fare, ROUND(AVG(tip_amount), 2) AS avg_tip FROM long_trips GROUP BY PULocationID ORDER BY total_fare DESC"
→ zone_revenue: 262 rows × 4 cols
```

```
ZeaOS> top_zones = zone_revenue | top 10
→ top_zones: 10 rows × 4 cols

ZeaOS> zeaview top_zones
```

Inside `zeaview`, press `g` to open the graph panel. Select `total_fare` as the value column. The bar chart shows JFK, LaGuardia, and Midtown at the top — exactly what you'd expect.

Press `e` to export the current view as CSV if you want to share the raw numbers.

---

## Pivoting: Payment Mix by Top Zones

Now let's look at how payment type breaks down across the top pickup zones. First build the source:

```
ZeaOS> zone_payment = zeaql "SELECT PULocationID, payment_type, COUNT(*) AS trips FROM long_trips WHERE PULocationID IN (SELECT PULocationID FROM top_zones) GROUP BY PULocationID, payment_type"
→ zone_payment: 54 rows × 3 cols
```

```
ZeaOS> payment_pivot = zone_payment | pivot payment_type→trips
→ payment_pivot: 10 rows × 7 cols

ZeaOS> zeaview payment_pivot
```

The pivot reshapes the data so each payment type becomes a column, making it easy to compare the credit-card-vs-cash split across zones. Airport zones (JFK, LaGuardia) skew heavily toward credit card — a pattern that emerges clearly in the pivoted view.

---

## Checking Lineage

At this point we have several derived tables. `hist` shows the full lineage:

```
ZeaOS> hist
```

```
session
└── trips  [2_964_624 rows × 19 cols]  → load(yellow_tripdata_2024-01.parquet)
    ├── cc_trips  [2_183_049 rows × 19 cols]  → where(payment_type = 1)
    ├── jfk_trips  [47_821 rows × 19 cols]  → where(PULocationID = 132)
    ├── long_trips  [1_847_203 rows × 19 cols]  → where(trip_distance > 2.0)
    │   ├── zone_revenue  [262 rows × 4 cols]  → sql
    │   │   └── top_zones  [10 rows × 4 cols]  → top(10)
    │   └── zone_payment  [54 rows × 3 cols]  → sql
    │       └── payment_pivot  [10 rows × 7 cols]  → pivot(payment_type→trips)
    └── avg_tip  [5 rows × 3 cols]  → sql
```

Every transformation is tracked. Nothing was written to disk.

---

## Saving Results to ZeaDrive

The `top_zones` and `payment_pivot` tables are the outputs worth keeping. Use `save` to write them to ZeaDrive local storage — ZeaOS creates the directory automatically:

```
ZeaOS> save top_zones zea://taxi-analysis/top_zones.parquet
saved top_zones → zea://taxi-analysis/top_zones.parquet

ZeaOS> save payment_pivot zea://taxi-analysis/payment_pivot.parquet
saved payment_pivot → zea://taxi-analysis/payment_pivot.parquet
```

`save` infers the format from the extension — `.parquet`, `.csv`, and `.json` are all supported. The `zea://` prefix routes to `~/.zeaos/local/` with no setup required.

To push to a cloud S3 backend (requires `enable-s3` configured):

```
ZeaOS> save top_zones zea://s3-data/taxi-analysis/top_zones.parquet
ZeaOS> save payment_pivot zea://s3-data/taxi-analysis/payment_pivot.parquet
```

On any other machine with ZeaOS and the same S3 backend configured, these files are immediately accessible via `zea://s3-data/taxi-analysis/`.

---

## Session Persistence

Exit ZeaOS and come back:

```
ZeaOS> exit

Goodbye.
```

```
zeaos
```

```
ZeaOS — Zero-copy Data REPL from Open Tempest Labs
Tables: [trips, cc_trips, jfk_trips, long_trips, zone_revenue, top_zones, zone_payment, payment_pivot, avg_tip]
```

All nine tables are restored from `~/.zeaos/tables/`. The session picks up exactly where it left off.

---

## What This Demonstrates

ZeaOS closes two critical gaps in the Arrow + DuckDB integration that block real ADBC adoption:

**Gap 1 — Integer predicate pushdown:** Arrow C Data Interface scans silently ignore integer WHERE predicates pushed by DuckDB's query planner. ZeaOS evaluates all `where` operations at the Arrow compute layer, bypassing DuckDB entirely for filtering.

**Gap 2 — Aggregation over Arrow streams:** GROUP BY and PIVOT operations against Arrow C Data Interface scans crash DuckDB's process. ZeaOS materializes source records into native DuckDB tables for aggregation operations only, then reads the result back as Arrow.

Both workarounds preserve the core promise: session data lives as Arrow record batches in memory with no unnecessary copies. The source dataset — 2.9 million taxi trips — is loaded once and stays in Arrow throughout, regardless of how many derived tables are produced from it.

When these bugs are fixed upstream in DuckDB's Arrow C Data Interface, the workarounds can be removed and the pipeline becomes fully zero-copy end to end.

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs). Source: [github.com/open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos)*
