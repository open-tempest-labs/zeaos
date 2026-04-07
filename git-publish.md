
You are an expert Go developer building CLI tools with GitHub API experience.

Implement **ZeaOS GitHub publishing** for dbt exports. Assume `zea promote`, `zea export`, `zea validate`, `zea list` already exist and work per previous spec.

## ORGANIZATION

github.com/open-tempest-labs/zeaos

```
## NEW COMMAND: `publish`

```
publish [NAME] --repo OWNER/REPO [--branch BRANCH] [--new] [--pr]
publish --session ID --repo OWNER/REPO
```

**Examples**:
```
publish clicks_by_day --repo team/my-dbt-project
publish clicks_by_day --repo zea-clicks-analysis --new
publish --session analysis-2026-04-05 --repo team/dbt-main --pr
```

## BEHAVIOR

### 1. Single artifact publish
```
publish clicks_by_day --repo team/my-dbt-project
```
```
1. zea export clicks_by_day → temp bundle (models/*.sql, *.yml, etc.)
2. git clone/pull target repo (or create new with --new)
3. Merge bundle into repo structure (models/, sources/)
4. git commit -m "ZeaOS: promote clicks_by_day [session: abc123]"
5. git push origin main
```

### 2. Session publish
```
publish --session analysis-uuid --repo team/dbt-main
```
Exports all promotable tables from session into single commit.

### 3. New repo (--new)
```
publish clicks_by_day --repo zea-clicks-analysis --new
```
Creates new repo with minimal dbt_project.yml + exported artifacts.

### 4. PR creation (--pr)
```
publish clicks_by_day --repo team/dbt-main --pr
```
Creates PR instead of direct push.

## GENERATED FILES (from zea export)

**models/clicks_by_day.sql**:
```sql
{{ config(materialized='table') }}

SELECT 
  customer_id,
  date_trunc('day', event_ts) as event_day,
  count(*) as event_count
FROM {{ source('zea_raw', 'events') }}
WHERE event_type = 'click'
GROUP BY 1, 2
```

**models/clicks_by_day.yml**:
```yaml
version: 2

models:
  - name: clicks_by_day
    description: "Daily click events by customer (promoted from ZeaOS session)"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
        tests:
          - not_null
          - unique
      - name: event_day
        description: "Event date truncated to day"
        tests:
          - not_null
      - name: event_count
        description: "Number of click events for customer on event_day"
        tests:
          - dbt_utils.expression_is_true:
              expression: "event_count >= 0"

semantic_models:
  - name: clicks_by_day
    description: "Customer click analytics"
    entities:
      - name: customer_id
        type: primary
        expr: customer_id
    measures:
      - name: click_count
        agg: sum
        expr: event_count
    dimensions:
      - name: event_day
        type: time
        time_granularity: day
        expr: event_day
```

**sources/zea_sources.yml**:
```yaml
version: 2

sources:
  - name: zea_raw
    schema: raw
    tables:
      - name: events
        description: "Raw event data from zea://raw/events.parquet"
        freshness:
          warn_after: {count: 7, period: day}
        loaded_at_field: event_ts
```

## DEPENDENCIES
```go
import (
    "github.com/google/go-github/v62/github"     // GitHub API
    "golang.org/x/oauth2"                        // OAuth2
    "github.com/go-git/go-git/v5"                // Git operations
    "github.com/open-tempest-labs/zeaos/export"  // Existing export logic
)
```

## IMPLEMENTATION

### Storage (`~/.zeaos/github/`)
```
~/.zeaos/github/
├── tokens/           # Encrypted GitHub PATs (github.com/PAT_NAME)
├── repos/            # Local repo cache (owner/repo/)
└── recent.json       # Recently published repos
```

### PAT Management
```
publish token add personal --pat ghp_xxx
publish token list
publish token use personal --repo team/my-dbt-project
```

### Repo Resolution
```
1. --repo flag
2. ~/.zeaos/default-repo
3. Recent repos (interactive picker)
4. zea-[session-name] (auto-new)
```

### Branch Resolution
```
1. --branch flag
2. main (default)
3. develop
```

## FILE MERGE LOGIC

```
Target repo structure:
├── models/
│   ├── staging/
│   └── marts/
├── sources/
└── dbt_project.yml

ZeaOS bundle → models/clicks_by_day.sql + .yml (create if missing)
            → sources/zea_sources.yml (MERGE existing sources)
            → profiles.yml (only if missing)
            → dbt_project.yml (only if missing)

Commit message:
"ZeaOS v0.2.0: promote clicks_by_day from session abc123"
```

## NEW REPO TEMPLATE (--new)
```
dbt_project.yml (minimal config)
profiles.yml (DuckDB local)
models/clicks_by_day.sql
models/clicks_by_day.yml  
sources/zea_sources.yml
zea_export.json
README.md ("Import into dbt Cloud...")
.gitignore (dbt standard)
```

## ERROR HANDLING
```
❌ No write access → "Create fork/PR?"
❌ Model name conflict → "Rename or overwrite?"
❌ No promotable tables → "Nothing to publish"
✅ Success → "Published to team/my-dbt-project#abc123"
```

## SUCCESS CRITERIA
```
✅ zea promote t3 clicks_by_day ✓
✅ zea validate clicks_by_day ✓
✅ publish clicks_by_day --repo test/repo ✓
✅ Repo contains 6 files ✓
✅ Commit pushed ✓
✅ No shell 'export' collision ✓
```

## CONFIG (~/.zeaos/config.json)
```json
{
  "github": {
    "default_repo": "team/my-dbt-project",
    "default_branch": "main",
    "auto_clone_timeout": "5m"
  }
}
```

**Cross-platform. Production-ready. Tab completion. PAT encryption. Repo caching. Ready for `go build`.**
```

Now the prompt contains the **exact file templates** Claude needs to merge into repos, with all the concrete examples restored and the correct `open-tempest-labs` organization. The GitHub publishing logic is fully specified and ready for implementation. 🚀
