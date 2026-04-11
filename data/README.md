# data/

Drop your local datasets here — Parquet, CSV, JSON, or TSV files.

This directory is mounted to `/data` inside the ZeaOS Docker container. Files placed here are immediately accessible from the REPL:

```
ZeaOS> t = load /data/myfile.parquet
ZeaOS> t = load /data/sales.csv
```

Files in this directory are gitignored. To use a different directory, set `ZEA_DATA_DIR` before starting:

```bash
ZEA_DATA_DIR=/your/datasets docker compose run --rm zeaos
```
