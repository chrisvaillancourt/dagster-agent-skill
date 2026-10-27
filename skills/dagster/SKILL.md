---
name: dagster
description: Builds, tests, automates, operates and deploys Dagster data pipelines using the current toolchain (dg CLI, create-dagster, defs/ autoloading, components, declarative automation, Pipes, Dagster+). Use for any task that mentions Dagster, dg, dagster-*, assets or materialization, defs.yaml or components, AutomationCondition, schedules or sensors in Dagster, asset checks, partitions and backfills, IO managers, resources, dbt/dlt/Sling/Fivetran with Dagster, dagster.yaml, code locations, the Dagster daemon or webserver, Dagster+ Serverless or Hybrid, or migrating legacy Dagster code (repository, SourceAsset, AutoMaterializePolicy) to modern APIs.
compatibility: Python 3.9+; uv recommended (Dagster's default toolchain; run dg as `uv run dg`). scripts/dagster_docs.py needs network access to docs.dagster.io and raw.githubusercontent.com.
metadata:
  author: chrisvaillancourt
  version: "0.1.0"
  dagster-release-line: "1.13"
  source: https://github.com/chrisvaillancourt/dagster-agent-skill
  docs-index: https://docs.dagster.io/llms.txt
---

# Dagster

Expert guidance for Dagster work. `SKILL.md` is the map: rules, mental model, a task router, canonical patterns, and the reference files to open. Open exactly the reference(s) the router names; fetch anything else live with `scripts/dagster_docs.py`.

## Rules

1. **Never answer API, CLI or YAML details from memory.** Open the reference file the router names, or run `scripts/dagster_docs.py <docs-path>` / `--api <page>`. Docs beat memory; Dagster's surface changed a lot (the `dg` CLI and components are the default now).
2. **Modern toolchain only**, unless the user's repo is legacy and they ask to stay there: `uvx create-dagster@latest project`, `dg scaffold defs`, `dg check defs`, `dg dev`, `dg launch`. `dagster dev -m` and `@repository` are superseded, and a single `create-dagster` project needs no `workspace.yaml` (`dg dev` plus `defs/` autoloading). `workspace.yaml` with `grpc_server` (or `python_module`) entries is still the current wiring for OSS deployments that run separate code servers; see `references/deployment-oss.md`.
3. **Assets first.** Model outputs as `@dg.asset` (or components that generate assets). Reach for `@dg.op`/`@dg.job` only for non-asset work or dynamic fan-out.
4. **`import dagster as dg`** and the `dg.` prefix in all code, as the docs do.
5. **Verify every change** with the loop in [Verification loop](#verification-loop); do not report success without running it. Run `scripts/check_project.sh` when in a project.
6. **Config and secrets** go through `dg.Config` / `dg.ConfigurableResource` fields and `dg.EnvVar("NAME")`, never `os.getenv` at import time and never literals in code, metadata or YAML.
7. **Legacy code**: before editing an existing project, run `scripts/find_legacy_patterns.py .` and modernize per `references/migration-and-legacy.md` only when asked or when the legacy API blocks the task.
8. **Do not invent** decorator arguments, CLI flags or `defs.yaml` keys. If a reference and the live docs disagree, the live docs win; note the discrepancy.

## Mental model

- A **Dagster project** is a Python package (`src/<pkg>/`) with a `definitions.py` entry point that calls `dg.load_from_defs_folder(...)`. Everything under `src/<pkg>/defs/` is **autoloaded**: decorated assets, checks, schedules, sensors, jobs; `dg.Definitions` returned by a `@dg.definitions` function (that is how resources are bound); and `defs.yaml` **component** instances. One rule: a module contributes *either* its module-scope decorated objects *or* the `Definitions` it returns, never both, and nothing warns when the former are dropped.
- A deployed project is a **code location**. The **webserver** serves the UI/GraphQL; the **daemon** runs schedules, sensors, the run queue, backfills and automation conditions; **run launchers/executors** run the work. `dg dev` starts all of that locally.
- **Assets** carry keys, deps, metadata, partitions, checks and an optional `automation_condition`. **Resources** are typed clients injected by parameter name. **IO managers** move return values between assets; with `deps=` you move data yourself.
- **Automation**: prefer `dg.AutomationCondition` (eager / on_cron / on_missing) on assets; use schedules for fixed cadence and sensors for external events.
- **Components** turn YAML into definitions (`type: dagster_dbt.DbtProjectComponent`); custom components subclass `dg.Component` and are scaffolded with `dg scaffold component`.
- **Pipes** run external code (subprocess, K8s, Databricks, Lambda…) as assets with logs and metadata streamed back.

## Task router

| Task | Open | Verify with |
| --- | --- | --- |
| New project, project layout, `dg` commands, `.env`, workspaces | `references/project-setup-and-cli.md` | `dg check defs` · `dg list defs` |
| Define assets, deps, `multi_asset`, metadata/tags/kinds, asset selection, factories, external assets | `references/assets.md` | `dg list defs --assets '<sel>'` · `dg launch --assets` |
| Partitions, partition mappings, backfills, dynamic partitions | `references/partitions-and-backfills.md` | `dg launch --assets X --partition K` |
| Schedules, sensors, asset/run-status sensors, jobs as targets | `references/schedules-and-sensors.md` | `dg check defs` · `build_schedule_context` tests |
| AutomationCondition (eager/on_cron/on_missing, custom) | `references/declarative-automation.md` | `dg.evaluate_automation_conditions` test |
| Resources, `Config`, `RunConfig`, `EnvVar`, secrets, run config YAML | `references/resources-and-config.md` | `dg check defs` · `dg list envs` |
| IO managers (built-in, custom, DuckDB/Snowflake/BigQuery/S3) | `references/io-managers.md` | `dg.materialize` test |
| Use or create components, `defs.yaml`, templating, state-backed components, migrate to components | `references/components.md` | `dg check yaml` → `dg check defs` |
| Unit tests, `materialize`, asset checks, freshness checks | `references/testing-and-asset-checks.md` | `uv run pytest` |
| Ops, graphs, jobs, dynamic fan-out, hooks, retries, executors | `references/ops-jobs-and-graphs.md` | `job.execute_in_process()` |
| `dagster.yaml`, daemon, concurrency/pools, run retries/monitoring, logging, debugging | `references/operations-and-debugging.md` | `dagster instance info` |
| Deploy OSS: Docker, Kubernetes/Helm, ECS, code servers, images | `references/deployment-oss.md` | `dg check defs` in CI |
| Dagster+ Serverless/Hybrid, agents, CI/CD, `dg plus`, `dg api`, alerts, Insights, MCP | `references/dagster-plus.md` | `dg plus deploy` · `dg api ... --json` |
| External execution: subprocess, K8s, Databricks, Spark, Lambda/Glue/EMR (Pipes) | `references/pipes-external-execution.md` | `dg launch --assets` |
| Which integration exists; Snowflake/BigQuery/DuckDB/S3/Slack/BI/ML libs | `references/integrations-catalog.md` | `dg list components` |
| dbt, dlt, Sling, Fivetran, Airbyte in depth | `references/integrations-dbt-and-elt.md` | `dg check defs` · `dg list defs` |
| Exact signature or symbol name | `references/api-quick-reference.md` (grep it) | `dagster_docs.py --api <page>` |
| Old code: `@repository`, `SourceAsset`, `AutoMaterializePolicy`, step launchers, Airflow migration, upgrades | `references/migration-and-legacy.md` | `scripts/find_legacy_patterns.py` |
| An error message or stuck run/schedule/sensor | `references/troubleshooting.md` | `scripts/check_project.sh` |
| Review Dagster code: reimplemented built-ins (env settings in a `resource_fn`, per-location failure sensors, uncalled teardown methods) | `references/reviewing-dagster-code.md` | the grep commands in that file |

Grep a reference before reading it whole when you need one fact: `grep -n "partition_mapping" references/partitions-and-backfills.md`.

## Verification loop

Run after every change to definitions, in the project directory:

```bash
uv run dg check yaml          # defs.yaml schema errors (skip if no components)
uv run dg check defs          # loads every definition on a real instance; exit 1 on error
uv run dg list defs           # confirm keys, groups, deps, schedules appeared as intended
uv run dg launch --assets my_asset            # execute locally (add --partition K); ephemeral instance unless DAGSTER_HOME is set
uv run pytest tests -q                        # unit tests (uv add --dev pytest first: create-dagster ships tests/ but no runner)
```

`scripts/check_project.sh [--launch "<selection>"] [--tests]` runs this loop and reports the first failure. Interactive check: `uv run dg dev` then open http://localhost:3000.

## Canonical patterns

**Entry point** (`src/my_project/definitions.py`, generated by `create-dagster`):

```python
from pathlib import Path
import dagster as dg

@dg.definitions
def defs():
    return dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
```

**Asset with dependency, resource, config and metadata** (`src/my_project/defs/assets.py`):

```python
import dagster as dg

class OrdersConfig(dg.Config):
    min_amount: float = 0.0

@dg.asset(group_name="sales", kinds={"duckdb"}, deps=["raw_orders"])
def orders(context: dg.AssetExecutionContext, config: OrdersConfig, warehouse: WarehouseResource) -> dg.MaterializeResult:
    n = warehouse.execute(f"create table orders as select * from raw_orders where amount >= {config.min_amount}")
    context.log.info("loaded %s rows", n)
    return dg.MaterializeResult(metadata={"dagster/row_count": n})
```

Use a parameter named after the upstream asset instead of `deps=` when an IO manager should pass its value.

**Resources bound in `defs/`** (`src/my_project/defs/resources.py`, a module holding nothing else; template in `assets/templates/resources_defs.py`):

```python
@dg.definitions
def resources():
    return dg.Definitions(resources={"warehouse": WarehouseResource(conn=dg.EnvVar("WAREHOUSE_URL"))})
```

**Automation**: `@dg.asset(automation_condition=dg.AutomationCondition.on_cron("0 6 * * *"))` for cadence after upstream updates; `.eager()` to follow upstream; `.on_missing()` to fill new partitions. Enable the `default_automation_condition_sensor` for the code location (UI toggle) or define `dg.AutomationConditionSensorDefinition(..., default_status=dg.DefaultSensorStatus.RUNNING)`.

**Schedule over a job**:

```python
daily_job = dg.define_asset_job("daily_job", selection=dg.AssetSelection.groups("sales"))
daily_schedule = dg.ScheduleDefinition(job=daily_job, cron_schedule="0 6 * * *", execution_timezone="UTC")
```

**Asset check**:

```python
@dg.asset_check(asset=orders, blocking=True)
def orders_have_rows(warehouse: WarehouseResource) -> dg.AssetCheckResult:
    n = warehouse.scalar("select count(*) from orders")
    return dg.AssetCheckResult(passed=n > 0, metadata={"rows": n}, severity=dg.AssetCheckSeverity.ERROR)
```

**Component instance** (`src/my_project/defs/jdbt/defs.yaml`, created by `dg scaffold defs dagster_dbt.DbtProjectComponent jdbt --project-path dbt/jdbt`):

```yaml
type: dagster_dbt.DbtProjectComponent
attributes:
  project: "{{ context.project_root }}/dbt/jdbt"   # what the scaffolder writes; `{{ project_root }}` also resolves
  select: "fqn:*"                                   # optional; other attributes: exclude, op, cli_args
  translation: { group_name: jdbt }                 # so schedules/jobs can target group:jdbt (default group is "default")
```

Discover types and schemas with `dg list components` and `dg utils inspect-component <Type>`; validate with `dg check yaml`.

**Test** (`tests/test_assets.py`):

```python
def test_orders():
    result = dg.materialize([raw_orders, orders], resources={"warehouse": FakeWarehouse()},
                            run_config=dg.RunConfig(ops={"orders": OrdersConfig(min_amount=10)}))
    assert result.success
```

## dg CLI essentials

```bash
uvx create-dagster@latest project my-project && cd my-project && uv sync   # `project .` names the package after the CWD
uv run dg scaffold defs dagster.asset assets/orders.py        # path is relative to src/<pkg>/defs/ from any CWD; also: dagster.multi_asset, dagster.schedule, dagster.sensor, dagster.job, dagster.resources
uv run dg scaffold defs dagster.asset_check checks/orders.py --asset-key orders   # asset_check requires --asset-key
uv run dg scaffold defs <module.ComponentType> <name> [flags] # component instance (defs.yaml); --format python for component.py
uv run dg scaffold component MyComponent                      # custom component class under src/<pkg>/components/
uv run dg list defs [--json] [--assets '<selection>']          # what is loaded
uv run dg list components [--package dagster_dbt]              # component types available in this env
uv run dg utils inspect-component dagster_dbt.DbtProjectComponent   # a component's defs.yaml schema and scaffold flags
uv run dg check yaml && uv run dg check defs                   # validate YAML, then load every definition (also: dg check toml)
uv run dg dev [-p 3000]                                        # UI + daemon locally
uv run dg launch --assets 'group:sales' [--partition 2025-01-01] [--config-json '{...}']
uv run dg launch --job daily_job [-c run_config.yaml]
uv run dg list envs                                            # EnvVars referenced by the project (.env is auto-loaded)
```

Prefer `--json` output when you consume the result programmatically. Dagster+ operations: `dg plus login|deploy|pull env`, `dg api <object> <verb> --json` (see `references/dagster-plus.md`). If a Dagster+ MCP server is connected, prefer its tools over `dg api` for deployed state (runs, logs, assets, alerts).

## Scripts

- `scripts/dagster_docs.py <docs-path|URL>` prints any docs page as Markdown with code snippets inlined (source: docs repo). `--search <terms>` searches the docs index; `--api "<page>"` prints an API reference page (`assets`, `asset checks`, `partitions`, `schedules and sensors`, `dagster-dbt library`…); `--list-api` lists them. Run with `uv run python` (or `python3`); stdlib only, cached 24h. Use it whenever a reference file lacks the detail you need.
- `scripts/check_project.sh [dir] [--launch "<selection>"] [--tests]` runs the verification loop with the project's own environment. Exit 0 = all passed.
- `scripts/find_legacy_patterns.py [path] [--strict] [--json]` lists superseded APIs/files with the replacement and docs page. Run it before modifying an unfamiliar Dagster repo.

## Templates (`assets/templates/`)

`pyproject-tool-dg.toml` (the `[tool.dg]` block), `dagster.yaml` (production instance config), `Dockerfile` (uv-based code-location image), `ci-github-actions.yml` (OSS CI: `dg check defs` + pytest), `resources_defs.py` (resource binding in `defs/`), `test_defs.py` (definition-load and materialize smoke tests). Copy, then replace `my_project`.

## Common mistakes to avoid

- Using `dagster dev -m …`, `workspace.yaml` or `@repository` in a single `create-dagster` project. Use `dg dev`; definitions autoload from `defs/`. Do not strip `workspace.yaml` from an OSS deployment that points the webserver and daemon at separate code servers; that is its current job.
- Reading secrets with `os.getenv` at import or hard-coding them; use `dg.EnvVar` in resource config.
- Putting resources in `defs.yaml`. Resources are not components; return them from a `@dg.definitions` function in `defs/`.
- Adding a `@dg.definitions` function (or a module-level `Definitions` object) to a `defs/` module that also holds decorated assets. The loader then takes only the returned `Definitions` and silently drops the module-scope assets. Keep such functions in their own module.
- Forgetting `can_subset=True` on a `@dg.multi_asset` that users will select partially.
- Expecting `AutomationCondition` to do anything while the automation condition sensor is off.
- Sensors that take longer than the evaluation timeout or forget `run_key`, producing timeouts or duplicate runs.
- Mixing partition definitions across a dependency without a `partition_mapping`.
- Returning a plain value or reading `context.partition_key` in an asset with `BackfillPolicy.single_run()`; a range run has no single key and the default I/O manager cannot store one output for a range. Read `context.partition_keys` / `partition_time_window`, write the data yourself, return `dg.MaterializeResult`.
- Pinning `dagster-<integration>` libraries to the core version. Core packages (`dagster`, `dagster-webserver`, `dagster-graphql`) share one 1.x version; integration libraries are released alongside on their own 0.y.z track, so install core pinned and let the resolver pick the matching library release.
- Using concurrency pools without `run_monitoring.free_slots_after_run_end_seconds`; a cancelled run then holds its slot until someone frees it in the UI.
- Reporting "done" without `dg check defs` passing.
