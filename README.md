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

### ZeaDrive (powered by Volumez)
A FUSE-mounted volume accessible via the `zea://` URL scheme. Mount once, reference data from anywhere in your session without knowing the actual mount path. Regular POSIX shell commands (`ls`, `cp`, `rm`) work against the mounted path once the drive is up. [→ github.com/open-tempest-labs/volumez](https://github.com/open-tempest-labs/volumez)

---

## Installation

```sh
brew tap open-tempest-labs/zeaos
brew install zeaos
```

### ZeaDrive (macFUSE required)

ZeaDrive mounts a Volumez-backed volume at `zea://`, enabling cross-machine session portability — load your tables on one machine, sync to a ZeaDrive volume, and resume on any other machine with ZeaOS installed.

ZeaDrive requires **macFUSE**, which involves approving a system kernel extension in **System Settings → Privacy & Security** and a reboot. On Apple Silicon you may also need to reduce the startup security policy in Recovery Mode to permit third-party kernel extensions.

**→ See the full [Installation Guide](docs/installation.md) for step-by-step macFUSE setup including Apple Silicon instructions.**

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

```
zeadrive mount [path]        # Mount volume (default ~/zeadrive)
zeadrive unmount             # Unmount
zeadrive status              # Show mount path and status
```

Reference mounted files with `zea://` from anywhere — load commands, shell commands piped through the REPL, anywhere a path appears:

```
ZeaOS> t = load zea://datasets/sales_2025.parquet
ZeaOS> ls zea://datasets/
```

### Plugins

```
zeaplugin <name> [args]             # Run plugin, stream output to terminal
zeaplugin <name> [args] → <table>   # Capture plugin CSV output as table
```

Plugins are discovered from `~/.zea/plugins/`. Use `zea run --help` to list available plugins and their arguments.

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
└── ZeaDrive          zea:// volume mount via Volumez FUSE
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
