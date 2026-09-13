#!/usr/bin/env bash
# Fixture: a dg project with dagster-dbt + dbt-duckdb installed, and a minimal dbt project beside it.
set -euo pipefail
uvx create-dagster@latest project warehouse --no-uv-sync >/dev/null 2>&1
cd warehouse
uv add -q "dagster-dbt" "dbt-duckdb" >/dev/null 2>&1
uv sync -q
mkdir -p dbt/jaffle/models
cat > dbt/jaffle/dbt_project.yml <<'YML'
name: jaffle
version: "1.0.0"
profile: jaffle
model-paths: ["models"]
YML
cat > dbt/jaffle/profiles.yml <<'YML'
jaffle:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: jaffle.duckdb
YML
cat > dbt/jaffle/models/customers.sql <<'SQL'
select 1 as customer_id, 'ada' as name
union all
select 2, 'grace'
SQL
cat > dbt/jaffle/models/customer_count.sql <<'SQL'
select count(*) as n from {{ ref('customers') }}
SQL
echo "fixture ready: $(pwd)"
