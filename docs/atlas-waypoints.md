---
title: "Navigating the Zea: Atlas, Waypoints, and Session Orientation"
---

# Navigating the Zea: Atlas, Waypoints, and Session Orientation

When you work with ZeaOS, every named table you create is a **waypoint** — a landmark in your analytical journey. You loaded a raw dataset, filtered it, grouped it, pivoted it. Each step has a name and a position in the map of your session. The `atlas` command is how you navigate that map.

---

## What Is the Atlas?

The atlas is ZeaOS's session navigation TUI. Type `atlas` at the REPL prompt and the terminal opens an interactive lineage tree:

```
ZeaOS> atlas
```

<!-- screenshot: atlas TUI showing trips → long_trips → zone_revenue tree -->

The left panel shows your session waypoints as a tree. Each table is nested under the source it was derived from — a `where` filter indented under its parent, an aggregation under the filtered set, and so on. You can see the shape of your analytical journey at a glance.

The right panel shows detail for the currently selected waypoint: schema, row and column counts, the derivation chain that produced it, and push history if the table has been published.

---

## Waypoints

Every named table in ZeaOS is a waypoint — a position in the Zea you can return to, navigate from, or hand off. Waypoints are created by assignment:

```
ZeaOS> trips = load https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
→ trips: 2_964_624 rows × 19 cols

ZeaOS> fares = trips | select passenger_count, trip_distance, fare_amount, tip_amount, total_amount
→ fares: 2_964_624 rows × 5 cols

ZeaOS> long_fares = fares | where trip_distance > 5
→ long_fares: 1_219_108 rows × 5 cols

ZeaOS> by_zone = long_fares | group PULocationID sum(fare_amount)
→ by_zone: 254 rows × 2 cols

ZeaOS> top_zones = by_zone | top 10
→ top_zones: 10 rows × 2 cols
```

Open the atlas and you'll see this as a tree:

```
trips
└── fares
    └── long_fares
        └── by_zone
            └── top_zones
```

Each node is a waypoint. The tree shows not just what you have, but how you got there.

### Waypoints persist across sessions

When you exit ZeaOS, all waypoints are saved to `~/.zeaos/`. The Parquet data is spilled to `~/.zeaos/tables/`, and the registry — names, row counts, lineage, push history — is written to `~/.zeaos/session.json`. Next time you start ZeaOS, your atlas is exactly where you left it.

```
ZeaOS> exit

# Later...
docker compose run --rm zeaos

ZeaOS — Zero-copy Data REPL from Open Tempest Labs
Tables: [trips, fares, long_fares, by_zone, top_zones]   Drive: ~/zeadrive [sdk]   v1.1.0
ZeaOS>
```

Your waypoints are waiting.

---

## Navigating the Atlas

### Moving between waypoints

Use `↑` and `↓` to move the selection. The right panel updates live with the selected waypoint's details.

### Expanding and collapsing the tree

Press `Space` to expand or collapse a node. For sessions with many derived tables, collapsing branches you're not currently working in keeps the map readable.

### Opening a waypoint in the viewer

Press `Enter` to open the selected waypoint in `zeaview` — the full-screen TUI table viewer. Sort columns with `s`, filter rows with `f`, view the schema with `d`, or export with `e`. Press `q` to close the viewer and return to the atlas.

For large tables (over 500k rows), the atlas will prompt before opening the viewer:

```
trips has 2,964,624 rows.
┌───────────────────────────────────┐
│  Open first 500k rows             │
│  Open all rows (may be slow)      │
│  Cancel                           │
└───────────────────────────────────┘
```

### Quickview

Press `d` to open a quickview panel for the selected waypoint — an inline summary without leaving the atlas:

```
trips
──────────────────────────────
Rows:     2,964,624
Columns:  19
Created:  2026-05-02 14:03:11

Schema:
  VendorID              int32
  tpep_pickup_datetime  timestamp[us]
  trip_distance         double
  fare_amount           double
  ...

Pushed:   (none)
```

Press `Esc` to dismiss the quickview and return to the tree.

### Copying a waypoint name

Press `c` to copy the selected table name to the clipboard. Useful when composing a `zeaql` query or `push` command without retyping.

### Closing the atlas

Press `q` or `Esc` to close the atlas and return to the REPL prompt.

---

## Describing a Waypoint

Outside the atlas, `describe` gives you the same detail for any waypoint directly in the REPL:

```
ZeaOS> describe long_fares
Table:   long_fares
Rows:    1219108
Columns: 5
Parent:  fares
Ops:     where trip_distance > 5

Column            Type
────────────────────────────────
passenger_count   double
trip_distance     double
fare_amount       double
tip_amount        double
total_amount      double
```

`Parent` and `Ops` together are the waypoint's breadcrumb trail — the derivation chain that produced it from the source.

---

## Split View: Two Waypoints at Once

Sometimes you want to compare two waypoints directly. `zeaview` accepts multiple table names and opens them in a split-pane view:

```
ZeaOS> zeaview by_zone top_zones
```

<!-- screenshot: split view showing by_zone (254 rows) stacked above top_zones (10 rows) -->

The default layout stacks panes vertically. For a side-by-side comparison:

```
ZeaOS> zeaview by_zone top_zones --orientation=left-right
```

<!-- screenshot: split view side-by-side -->

Press `Tab` to move focus between panes. All viewer key bindings (`s` sort, `f` filter, `d` schema) apply to the focused pane independently. Press `q` to close both panes and return to the REPL.

Split view works with any in-memory waypoints. You can open a source table alongside a derived aggregate, two different filter results, or any combination of waypoints in your session.

---

## The Atlas as a Working Tool

The atlas is not just a history log — it's an active navigation tool for your session. A few patterns that work well in practice:

**Orientation after a break.** If you return to a long session, open `atlas` first. The lineage tree shows you the shape of what you built and where you left off, faster than reading a list of table names.

**Pre-push review.** Before pushing tables, open the atlas and press `d` on each waypoint you plan to push. Confirm row counts and schema match your expectations without leaving the TUI.

**Exploring a restored session.** When ZeaOS restores waypoints from a previous session, the atlas shows them exactly as they were — tree structure, lineage, push history. If a waypoint's data has drifted from the original source, `iceberg verify` can confirm integrity.

**Naming intermediate steps.** Because waypoints persist, it's worth naming intermediate steps even if you don't intend to push them. `by_zone` is a more useful waypoint than an anonymous intermediate — it shows up in the tree, can be re-examined later, and its derivation is self-documenting.

---

## Quick Reference

| Command | What it does |
|---------|-------------|
| `atlas` | Open the session atlas TUI |
| `hist` | Alias for atlas |
| `describe <table>` | Schema, lineage, row count in the REPL |
| `list` | Flat list of all session waypoints |
| `zeaview <table>` | Open a single waypoint in the viewer |
| `zeaview t1 t2` | Split view — two waypoints stacked |
| `zeaview t1 t2 --orientation=left-right` | Split view — side-by-side |

**Atlas key bindings:**

| Key | Action |
|-----|--------|
| `↑` / `↓` | Move selection |
| `Enter` | Open in zeaview |
| `d` | Quickview (schema, stats, push history) |
| `c` | Copy table name to clipboard |
| `Space` | Expand / collapse node |
| `q` / `Esc` | Close atlas |

---

*ZeaOS is developed by [Open Tempest Labs](https://github.com/open-tempest-labs).*
