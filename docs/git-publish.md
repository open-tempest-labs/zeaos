---
title: GitHub Publishing
---

# GitHub Publishing

ZeaOS can publish dbt export bundles directly to GitHub repositories. Use `model publish` to push promoted artifacts to an existing dbt project, create a new repo, or open a pull request.

---

## Setup

### Store a GitHub PAT

```
ZeaOS> model publish token add personal --pat ghp_xxxxxxxxxxxxx
token 'personal' saved

ZeaOS> model publish token list
NAME        DEFAULT
personal    ✓
```

The token is stored in `~/.zeaos/github/tokens.json` (mode 0600). Tokens need `repo` scope (or `public_repo` for public repos only).

If `gh` is already authenticated on your machine, ZeaOS picks it up automatically — no token setup required.

---

## Commands

```
model publish [<name>] --repo OWNER/REPO [--branch BRANCH] [--new] [--pr] [--token NAME]
model publish --session SESSION_ID --repo OWNER/REPO [--branch BRANCH] [--pr] [--token NAME]

model publish token add <name> --pat <token>
model publish token list
model publish token remove <name>
```

---

## Worked Example: NYC Taxi Revenue

Continuing from the [dbt Export](dbt-export) workflow, we have a promoted artifact `zone_revenue_by_pickup` that has passed `model validate`. Now we publish it.

### Push to an existing dbt project

```
ZeaOS> model publish zone_revenue_by_pickup --repo acme-data/dbt-main
cloning acme-data/dbt-main...
  merging models/zone_revenue_by_pickup.sql
  merging models/zone_revenue_by_pickup.yml
  merging sources/zea_sources.yml
commit: ZeaOS v1.0.0: promote zone_revenue_by_pickup from session zea-2026-04-11-1030
pushing to acme-data/dbt-main main...
✓ Published to acme-data/dbt-main
```

### Open a pull request instead of pushing directly

```
ZeaOS> model publish zone_revenue_by_pickup --repo acme-data/dbt-main --pr
cloning acme-data/dbt-main...
  merging models/zone_revenue_by_pickup.sql
  merging models/zone_revenue_by_pickup.yml
  merging sources/zea_sources.yml
commit: ZeaOS v1.0.0: promote zone_revenue_by_pickup from session zea-2026-04-11-1030
pushing to acme-data/dbt-main zea/zone_revenue_by_pickup-20260411...
✓ PR created: https://github.com/acme-data/dbt-main/pull/42
```

### Create a new repository

```
ZeaOS> model publish zone_revenue_by_pickup --repo acme-data/taxi-analysis --new
creating repository acme-data/taxi-analysis...
initialising repo with dbt scaffold...
  wrote models/zone_revenue_by_pickup.sql
  wrote models/zone_revenue_by_pickup.yml
  wrote sources/zea_sources.yml
  wrote dbt_project.yml
  wrote profiles.yml
  wrote zea_export.json
  wrote README.md
  wrote .gitignore
commit: ZeaOS v1.0.0: initial export — zone_revenue_by_pickup
pushing to acme-data/taxi-analysis main...
✓ Published to https://github.com/acme-data/taxi-analysis
```

### Publish all promoted artifacts

```
ZeaOS> model publish --repo acme-data/dbt-main --pr
exporting 3 promoted artifact(s)...
  merging models/zone_revenue_by_pickup.sql
  merging models/avg_tip_by_payment.sql
  merging models/long_trips.sql
  merging sources/zea_sources.yml
commit: ZeaOS v1.0.0: promote 3 artifact(s) from session zea-2026-04-11-1030
✓ PR created: https://github.com/acme-data/dbt-main/pull/43
```

---

## File Merge Rules

When publishing to an existing repository:

| File | Behaviour |
|------|-----------|
| `models/*.sql` | Overwrite (or prompt if content came from another tool) |
| `models/*.yml` | Overwrite |
| `sources/zea_sources.yml` | Merge — new sources appended; existing ZeaOS-generated file replaced; non-ZeaOS file skipped with a warning |
| `dbt_project.yml` | Create only if missing |
| `profiles.yml` | Create only if missing |
| `zea_export.json` | Always overwrite |
| `README.md` | Create only if missing |
| `.gitignore` | Create only if missing |

---

## Token Resolution

When no `--token` flag is given, the default token is used. Set a token as default:

```
ZeaOS> model publish token add work --pat ghp_yyy
ZeaOS> model publish token add personal --pat ghp_xxx
```

The first token added becomes the default. To change it, remove and re-add, or use `--token work` per publish call.

---

## Local Cache

Cloned repositories are cached under `~/.zeaos/github/repos/OWNER/REPO/`. Subsequent publishes to the same repo do a `git pull` rather than a full clone. Recently published repos are tracked in `~/.zeaos/github/recent.json`.

---

## Error Handling

| Situation | Behaviour |
|-----------|-----------|
| No token configured and `gh` not authenticated | `error: no GitHub token — run: model publish token add <name> --pat <token>` |
| No write access to repo | `error: push failed — do you need --pr to open a pull request instead?` |
| Model name already in repo (foreign content) | Prompt: overwrite or skip |
| No promoted tables in session | `nothing to publish — run 'model promote <table> as <name> model' first` |
| `--new` but repo already exists | `error: repository acme-data/taxi-analysis already exists (omit --new to push to it)` |
