# ZeaOS

**An Arrow-Native Data OS powered by DuckDB**

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
    Z:::::Z        e:::::::::::::::::e aa::::::::::::aO:::::O     O:::::O       SSSSSS::::S
   Z:::::Z         e::::::eeeeeeeeeee a::::aaaa::::::aO:::::O     O:::::O            S:::::S
ZZZ:::::Z     ZZZZZe:::::::e         a::::a    a:::::aO::::::O   O::::::O            S:::::S
Z::::::ZZZZZZZZ:::Ze::::::::e        a::::a    a:::::aO:::::::OOO:::::::OSSSSSSS     S:::::S
Z:::::::::::::::::Z e::::::::eeeeeeeea:::::aaaa::::::a OO:::::::::::::OO S::::::SSSSSS:::::S
Z:::::::::::::::::Z  ee:::::::::::::e a::::::::::aa:::a  OO:::::::::OO   S:::::::::::::::SS
ZZZZZZZZZZZZZZZZZZZ    eeeeeeeeeeeeee  aaaaaaaaaa  aaaa    OOOOOOOOO      SSSSSSSSSSSSSSS
```

ZeaOS is an interactive data REPL that treats your data the way an operating system treats processes — as first-class, named, living things with lineage, sessions that persist across restarts, and a filesystem you can mount.

Under the hood it is built on three pillars:

| Pillar | Role |
|--------|------|
| **[ZeaShell](https://github.com/open-tempest-labs/zeashell)** | TUI viewer, expression parser, plugin runtime |
| **[Apache Arrow](https://arrow.apache.org/)** | Zero-copy in-memory columnar format for all session tables |
| **[DuckDB](https://duckdb.org/)** | Embedded OLAP engine for SQL, GROUP BY, PIVOT, and file I/O |

---

## The Vision

Most data tools force you to choose between the comfort of a spreadsheet and the power of a query engine. ZeaOS is neither — it is a **data shell**.

You load files. You pipe them through transforms. You name the results. You look at lineage. You come back tomorrow and your session is still there. You mount a remote volume and your paths just work. You write a plugin and share it.

Everything in memory is Arrow. Arrow is the lingua franca of the modern data stack — zero-copy interop with DuckDB, pandas, Polars, ADBC, Flight. ZeaOS is the interactive layer that sits on top of that.

---

## Components

### ZeaOS (this repo)
The REPL itself. Manages a session registry of Arrow tables, dispatches commands through a pipe parser that compiles shorthand syntax to SQL, and hands results back as retained Arrow records.

### ZeaShell
The library layer. Provides the TUI table viewer (`zeaview`), the WHERE expression parser used by the Arrow-native filter path, and the plugin execution runtime (`zea run`). [→ github.com/open-tempest-labs/zeashell](https://github.com/open-tempest-labs/zeashell)

### ZeaDrive
A unified data path layer accessible via the `zea://` URL scheme. Works out of the box with no setup — `zea://` resolves to local storage at `~/.zeaos/local/`. Cloud backends (S3-compatible) can be added with `enable-s3` and are accessed via `zea://s3-data/` (or whatever name you configure), backed by [Volumez](https://github.com/open-tempest-labs/volumez) FUSE. [→ github.com/open-tempest-labs/volumez](https://github.com/open-tempest-labs/volumez)

---

## Installation

```sh
brew tap open-tempest-labs/zeaos
brew install zeaos
```

### ZeaDrive

`zea://` paths work immediately after install — no additional setup needed for local storage:

```sh
ZeaOS> t = load zea://mydata/sales.parquet    # ~/.zeaos/local/mydata/sales.parquet
ZeaOS> ls zea://mydata/                        # browse via shell
```

To add an S3-compatible cloud backend, run `enable-s3` inside ZeaOS. This opens a configuration form and writes `~/.zeaos/volumez.json`. Cloud paths then mount automatically on first access via [Volumez](https://github.com/open-tempest-labs/volumez).

Cloud backends require **macFUSE** and **Volumez** to be installed. See the [Installation Guide](docs/installation.md) for details including the macFUSE kernel extension approval process on macOS and Apple Silicon.

---

## Quick Start

```
zeaos
```

```
ZeaOS> t = load ~/data/earthquakes.parquet
→ t: 1_847_204 rows × 12 cols

ZeaOS> big = t | where magnitude > 7.0
→ big: 412 rows × 12 cols

ZeaOS> by_region = big | group region
→ by_region: 18 rows × 2 cols

ZeaOS> zeaview by_region
```

---

## Command Reference

### Loading Data

```
t = load <file>              # Parquet, CSV, TSV, JSON, JSONL
t = zea sql "SELECT ..."     # Arbitrary SQL over session tables
t = load zea://data/file.parquet   # Via mounted ZeaDrive
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

### Session Management

```
t2 = t1                      # Alias / copy
drop <table>                 # Remove table from session
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

`zea://` paths work everywhere with no mount required for local storage:

```
t = load zea://data/sales.parquet        # local: ~/.zeaos/local/data/sales.parquet
ls zea://data/                           # shell commands work too
cp ~/downloads/file.csv zea://data/      # copy files in
```

To add a cloud backend:

```
enable-s3                                # opens TUI form, writes ~/.zeaos/volumez.json
```

Once configured, cloud paths mount automatically on first access:

```
t = load zea://s3-data/warehouse/sales.parquet   # mounts Volumez, then loads
zeadrive status                                   # show local path, backends, mount state
zeadrive mount                                    # explicit mount (optional)
zeadrive unmount                                  # unmount cloud backends
```

### Plugins

```
zearun <name> [args]                # Run plugin, stream output to terminal
t = zearun <name> [args]            # Run plugin, capture CSV output as table

zeaplugin                           # List all available plugins
zeaplugin list                      # List all available plugins
zeaplugin <name> --help             # Show help for a specific plugin
```

Plugins are discovered from `~/.zea/plugins/`. Use `zea pluginate` from zeashell to create new plugins from your shell history.

### Other

```
exit / quit                  # Exit ZeaOS
? or help                    # Command reference
```

---

## Architecture

```
zeaos (REPL)
├── readline          input + history
├── Parser            shorthand pipe syntax → SQL
├── Arrow registry    session state (retained Arrow records)
├── arrowfilter       WHERE evaluation via Arrow compute (zero-copy)
├── DuckDB CGO        GROUP BY, PIVOT, file I/O, SQL engine
├── ZeaShell libs     TUI viewer, expression parser, plugin runtime
└── ZeaDrive
    ├── zea://         → ~/.zeaos/local/  (always available, no setup)
    └── zea://<name>/  → ~/zeadrive/<name>/  (Volumez FUSE, cloud backends)
```

**Zero-copy design.** Session tables live as Apache Arrow record batches. Filter operations (`where`) use Arrow compute directly — DuckDB never sees the predicate. Projection and aggregation go through DuckDB's Arrow C Data Interface, reading in-place where possible. No intermediate Parquet files during a session.

**Session persistence.** On exit, tables are spilled to `~/.zeaos/tables/` as Parquet and restored on next launch. The session registry is saved to `~/.zeaos/session.json`.

---

## Building from Source

Requires Go 1.21+, a C compiler (for DuckDB CGO), and FUSE (for ZeaDrive).

```sh
git clone https://github.com/open-tempest-labs/zeaos
cd zeaos
go build -o zeaos ./cmd/zeaos
```

---

## Ecosystem

| Repo | Description |
|------|-------------|
| [open-tempest-labs/zeaos](https://github.com/open-tempest-labs/zeaos) | This repo — the REPL |
| [open-tempest-labs/zeashell](https://github.com/open-tempest-labs/zeashell) | Shell library, TUI viewer, plugin runtime |
| [open-tempest-labs/volumez](https://github.com/open-tempest-labs/volumez) | FUSE volume driver backing ZeaDrive |

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs).*
