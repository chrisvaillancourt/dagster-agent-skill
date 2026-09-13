# Project setup and the `dg` CLI

How to create, lay out, configure, and drive a Dagster project with the modern toolchain: `create-dagster` for scaffolding, a `defs/` folder that autoloads, and `dg` as the day-to-day CLI. Open this file first when starting a project or orienting in an unfamiliar one. Authoring component classes is in `components.md`; resource and config patterns are in `resources-and-config.md`; the Dagster+ CLI (`dg plus`) is in `dagster-plus.md`; production deployment is in `deployment-oss.md`; converting an older `@repository`/`workspace.yaml` project is in `migration-and-legacy.md`.

## Contents
- [Create a project](#create-a-project)
- [Project layout](#project-layout)
- [How `defs/` autoloading works](#how-defs-autoloading-works)
- [Configuration: `[tool.dg]` and `dg.toml`](#configuration-tooldg-and-dgtoml)
- [Workspaces: multiple projects](#workspaces-multiple-projects)
- [Organizing larger projects](#organizing-larger-projects)
- [`dg` command cheat sheet](#dg-command-cheat-sheet)
- [Environment variables and `.env`](#environment-variables-and-env)
- [Running a local instance](#running-a-local-instance)
- [The verification loop](#the-verification-loop)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Create a project

Use when starting anything new. `create-dagster` is the only recommended entry point; do not hand-roll the package layout.

```bash
uvx create-dagster@latest project my-project   # answer y to the "Run uv sync?" prompt
cd my-project
source .venv/bin/activate                      # .venv\Scripts\activate on Windows
dg --version
```

The prompt runs `uv sync`, which creates `.venv` and `uv.lock`. Pass `--uv-sync` / `--no-uv-sync` to answer it non-interactively (useful in scripts and CI).

Escape hatch without uv: `pip install dagster dagster-webserver dagster-dg-cli`, then `create-dagster project my-project`, `python -m venv .venv`, activate, and `pip install --editable . --group dev` (`--group` needs pip 25.1+; otherwise drop it and `pip install dagster-dg-cli` separately). `create-dagster` is also installable via `brew install dagster-io/tap/create-dagster` or the `https://dg.dagster.io/create-dagster/install.sh` script.

Running `dg` afterwards: either activate `.venv` and call `dg ...`, or prefix every call with `uv run dg ...`. The prefix just forces execution inside the project virtualenv; the two are equivalent. Prefer `uv run dg` in scripts and CI where no shell activation has happened.

Requires Python 3.10+.

Gotchas:

- `dg` resolves a project's interpreter at `.venv/bin/python` relative to the project root. If your venv lives elsewhere, set `DG_PROJECT_PYTHON_EXECUTABLE` in a `.env` file **at the project root**. It is read only from that file; exporting it in the shell has no effect.
- `--use-active-venv` (on `dg dev` and `dg check defs`) makes `dg` use `$VIRTUAL_ENV` for all projects instead of resolving per-project venvs.

## Project layout

`create-dagster project` produces a standard `src`-layout Python package plus two Dagster-specific directories:

```
my-project/
├── pyproject.toml          # packaging metadata + [tool.dg] settings
├── README.md
├── uv.lock
├── src/
│   └── my_project/
│       ├── __init__.py
│       ├── definitions.py  # entry point: the top-level Definitions object
│       ├── defs/           # autoloaded definitions and component instances
│       │   └── __init__.py
│       └── components/     # custom Component classes (registry module)
│           └── __init__.py
└── tests/
    └── __init__.py
```

`tests/` is a separate top-level package, outside `src/`. `src/my_project/components/` holds custom component types created with `dg scaffold component`; it is also a reasonable home for helper code shared across definitions.

The generated `definitions.py` is the loader, and you rarely edit it:

```python
# src/my_project/definitions.py  (as generated; other examples here use `import dagster as dg`)
from pathlib import Path

from dagster import definitions, load_from_defs_folder


@definitions
def defs():
    return load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)
```

`project_root` points at the directory containing the project config file (`pyproject.toml` with `[tool.dg]`, or `dg.toml`) — three levels up from `src/<pkg>/definitions.py`. The API reference names this parameter `path_within_project` and describes it as "a path within the dg project directory"; docs examples use both spellings, so match whatever the scaffold emitted rather than renaming it.

`@dg.definitions` marks a zero-argument function that returns a `Definitions`. Dagster calls it at load time.

## How `defs/` autoloading works

Use when adding anything new: put the file under `defs/` and it is picked up. No registration, no import in `definitions.py`.

`load_from_defs_folder` reads the project config to find the defs module, then recursively walks it. Each subdirectory behaves as a `dagster.DefsFolderComponent` and discovers:

| What is found | How it is loaded |
| --- | --- |
| `.py` files containing Dagster definitions | Module-scoped `@dg.asset`, `@dg.asset_check`, `@dg.schedule`, `@dg.sensor`, `@dg.job`, `@dg.multi_asset` objects |
| A function decorated with `@dg.definitions` returning `dg.Definitions` | The returned `Definitions` is merged in (this is how resources are registered) |
| Subdirectories with a `defs.yaml` | A component instance, resolved from its `type:` and `attributes:` |
| Nested subdirectories | Recursed into, arbitrarily deep |

`__pycache__` and hidden directories (leading `.`) are skipped.

Add resources by returning a `Definitions` with a `resources` mapping from anywhere inside `defs/`. Scaffold it with `dg scaffold defs dagster.resources resources.py`:

```python
# src/my_project/defs/resources.py
from dagster_duckdb import DuckDBResource

import dagster as dg


@dg.definitions
def resources():
    return dg.Definitions(resources={"duckdb": DuckDBResource(database="/tmp/data.duckdb")})
```

Resource keys bind by parameter name: an asset with a `duckdb: DuckDBResource` parameter gets that instance. See `resources-and-config.md` for `EnvVar`, `ConfigurableResource`, and run config.

Anti-pattern: do not put a resource in `defs/<name>/defs.yaml`. YAML defs require a `Component` class; components produce assets, resources are dependencies of them. Use a `resources/` Python module or a `defs.py` returning `Definitions(resources=...)`.

To mix component instances with hand-written Python definitions that live **outside** `defs/`, merge them in `definitions.py`:

```python
# src/my_project/definitions.py
from pathlib import Path

from my_project.assets.analytics import customer_segmentation, revenue_forecast
from my_project.resources.warehouse import get_warehouse_resource

import dagster as dg


@dg.definitions
def defs():
    component_defs = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    pythonic_defs = dg.Definitions(
        assets=[customer_segmentation, revenue_forecast],
        resources={"warehouse": get_warehouse_resource()},
    )
    return dg.Definitions.merge(component_defs, pythonic_defs)
```

Resources bound here are available to both the component-produced and the Python-authored assets. Prefer a dedicated `resources/` module when resource construction is non-trivial or needs its own unit tests; prefer a `defs/.../defs.py` when the resource is simple and mostly serves components.

## Configuration: `[tool.dg]` and `dg.toml`

`dg` identifies a directory as a project or workspace purely by config file. Three kinds exist, merged in this precedence order: user config < project/workspace config < command line.

Project config lives in `pyproject.toml` under `tool.dg.*` (the usual case), or in a standalone `dg.toml` with the same keys at top level (use this when the package still uses `setup.py`).

```toml
# pyproject.toml
[tool.dg]
directory_type = "project"            # required; marks the directory as a dg project

[tool.dg.project]
root_module = "my_project"            # required
defs_module = "my_project.defs"       # default: <root_module>.defs
code_location_target_module = "my_project.definitions"  # default: <root_module>.definitions
code_location_name = "my-project"     # default: the project name
registry_modules = ["my_project.alternate_components.*"]  # default: []

[tool.dg.cli]                         # application-level settings; DISALLOWED inside a workspace
```

`code_location_target_module` is the setting to change if your top-level `Definitions` is not in `<root_module>/definitions.py`. `registry_modules` accepts `*` wildcards that match whole module segments only (`foo.*.bar` matches `foo.baz.bar`; `foo.*bar` is invalid); `dg scaffold component` adds entries automatically.

Custom component types are exposed to `dg` through a Python entry point, conventionally `<root_module>.components`:

```toml
[project.entry-points]
"dagster_dg_cli.registry_modules" = { my_project = "my_project.components" }
```

Entry points are registered at install time. After adding or changing one, reinstall the package — `uv sync` will not do it; use `uv pip install -e .` (or `pip install -e .`). Confirm with `dg list registry-modules`.

User config is optional, at `~/.config/dg.toml` (`%APPDATA%/dg/dg.toml` on Windows), and may contain only a `[cli]` section: `verbose`, `suppress_warnings`, `[cli.telemetry] enabled`, and `[cli.plus]` settings (`organization`, `default_deployment`, `user_token`) that replace the older `.dagster_cloud_cli.yaml`.

Validate any config change with `dg check toml`.

## Workspaces: multiple projects

Use when teams need separate projects, or when dependency conflicts force separate Python environments. One project is the default; do not reach for a workspace early.

```bash
uvx create-dagster@latest workspace dagster-workspace && cd dagster-workspace
source deployments/local/.venv/bin/activate     # re-activate in every new shell
uvx create-dagster@latest project projects/project-1
uvx create-dagster@latest project projects/project-2
dg list projects
dg dev                                          # loads every project in the workspace
```

```
dagster-workspace/
├── dg.toml                    # directory_type = "workspace"
├── deployments/
│   └── local/                 # venv used to run dg against the whole workspace
│       ├── pyproject.toml
│       └── uv.lock
└── projects/
    ├── project-1/             # own pyproject.toml, src/, tests/, .venv, uv.lock
    └── project-2/
```

```toml
# dg.toml
directory_type = "workspace"

[workspace]

[[workspace.projects]]
path = "projects/project-1"

[[workspace.projects]]
path = "projects/project-2"
```

Running `create-dagster project` inside a workspace appends the project to `[[workspace.projects]]` automatically. Each project keeps its own `.venv`; `dg` spawns one subprocess per project and talks to it over an API layer. `dg dev -w path/to/dg.toml` loads a workspace from elsewhere.

Gotcha: application-level `[cli]` settings must live in the workspace `dg.toml`, not in a member project's `pyproject.toml`. `dg` warns with `cli_config_in_workspace_project` if you do the latter.

Assets can depend on assets in a sibling project; they become separate code locations in one UI.

## Organizing larger projects

The flat `defs/` folder stops scaling once you have many assets. Pick one of two axes and stay consistent.

**By technology** — best when the team thinks in tools:

```
src/my_project/defs/
├── dbt/{assets.py, resources.py}
└── dlt/{assets.py, resources.py, pipelines/{github.py, hubspot.py}}
```

**By concept** — best when readers are not fluent in the underlying tools:

```
src/my_project/defs/
├── ingestion/dlt/{assets.py, resources.py}
└── transformation/{adhoc/…, dbt/{assets.py, partitions.py, resources.py}}
```

Keep external tool projects **outside** the Dagster Python package. A dbt project, Sling replication, or notebook tree belongs at the repository root as a sibling of the Dagster project, referenced by path from a component or resource:

```
.
├── dbt_project/{dbt_project.yml, models/, macros/, tests/}
└── my-project/
```

Use a workspace, not deeper nesting, when the real problem is conflicting dependencies.

## `dg` command cheat sheet

Every command below accepts `--verbose` and `--target-path <dir>` (load context from a directory containing `dg.toml`/`pyproject.toml` instead of the CWD).

| Command | Purpose and useful flags |
| --- | --- |
| `dg scaffold defs <type> <path>` | Create a definition under `defs/`. Path is always relative to `src/<pkg>/defs/`, regardless of CWD. `--format python` emits a `component.py` instead of a `defs.yaml` for component types. |
| `dg scaffold component <Name>` | Create a custom component class in `src/<pkg>/components/` and register it. See `components.md`. |
| `dg scaffold build-artifacts` | Scaffold a Dockerfile. Kept for compatibility; prefer `dg plus deploy configure`. |
| `dg check toml` | Validate `dg.toml` / `pyproject.toml` config. |
| `dg check yaml [PATHS]` | Validate `defs.yaml` files against component schemas. `--watch` revalidates on change; `--validate-requirements` checks declared env var requirements. |
| `dg check defs` | Load and validate all definitions. Exit code 1 on error. `--check-yaml/--no-check-yaml`, `--use-active-venv`. |
| `dg list defs` | List loaded definitions. `--json`, `-p/--path`, `-a/--assets <selection>`, `-c/--columns key,group,deps,kinds,description,tags,cron,is_executable`. |
| `dg list components` | Component types available in the environment. `-p/--package`, `--json`. |
| `dg list component-tree` | Hierarchy of component instances under `defs/`. `--output-file`. |
| `dg list envs` | Env vars required or used by components, and whether each is set. |
| `dg list projects` | Projects in the workspace, or the current project directory. |
| `dg list registry-modules` | Registered plugin/registry modules and the objects they expose. |
| `dg dev` | Start webserver + daemon locally. `-p/--port`, `-h/--host`, `--check-yaml/--no-check-yaml`, `--log-level`, `-w/--workspace`, `--use-active-venv`. |
| `dg launch` | Launch a run. `--assets <selection>`, `--job <name>`, `--partition <key>`, `--partition-range <start>...<end>`, `--config-json '<json>'`, `-c/--config <file>` (repeatable; earlier files win). |
| `dg utils inspect-component <Type>` | Component details. `--description`, `--defs-yaml-schema` (LLM-oriented annotated YAML template), `--defs-yaml-example-values`, `--defs-yaml-json-schema`, `--scaffold-params-schema`. |
| `dg utils configure-editor vscode\|cursor` | Install an extension supplying JSON schemas for `defs.yaml`. |
| `dg utils refresh-defs-state` | Refresh state for state-backed components. `--defs-state-key`, `--management-type`. |

Built-in scaffold types are `dagster.asset`, `dagster.asset_check`, `dagster.multi_asset`, `dagster.schedule`, `dagster.sensor`, `dagster.job`, and `dagster.resources`, plus any registered component such as `dagster.PythonScriptComponent`, `dagster.TemplatedSqlComponent`, `dagster_dbt.DbtProjectComponent`, or `<your_pkg>.components.MyComponent`. `dg list components` enumerates the registered *component* types only; the seven built-ins above are not listed there, and `dg scaffold defs --help` does not print them either. Confirm one with `dg scaffold defs <type> --help`.

```bash
dg scaffold defs dagster.asset assets/my_asset.py
dg scaffold defs dagster.resources resources.py
dg scaffold defs dagster_dbt.DbtProjectComponent jdbt --project-path dbt/jdbt
dg launch --assets 'key:"customers"+ and kind:"dbt"'
dg launch --job daily_sync --partition 2024-01-01
```

Asset selection strings accept `key:`, `group:`, `kind:`, `tag:`, `owner:`, `status:`, `+` upstream/downstream operators, and `roots()`/`sinks()`. See `assets.md`.

## Environment variables and `.env`

`dg` commands load a `.env` file from the project root automatically, so local secrets stay out of source control and out of `defs.yaml`. Keep `.env` untracked.

```bash
dg list envs            # every env var components require, with a ✓ when set
echo 'SNOWFLAKE_PASSWORD=...' >> .env
dg list envs            # confirm it is now set
```

Component YAML reads them through the Jinja `env` function (`"{{ env.SNOWFLAKE_PASSWORD }}"`), and `dg check yaml` will require you to declare those dependencies explicitly in the file. Python code reads them through `dg.EnvVar` — see `resources-and-config.md`.

Gotchas:

- The webserver picks up `.env` from the directory it was launched in. After editing `.env`, reload the code location in the UI or restart `dg dev`; a running workspace will not see the change.
- `dg list env` (singular) appears in older guides and behaves the same as `dg list envs`.
- `DG_PROJECT_PYTHON_EXECUTABLE` is the one variable read *only* from `.env`, never from the shell.

## Running a local instance

`dg dev` starts the webserver and daemon together on `localhost:3000`. It is for development only: no authentication, no web security, single webserver process, no daemon auto-restart.

By default the instance is **ephemeral** — storage goes to a temp directory (`Using temporary directory /…/.tmp_dagster_home_<uuid> for storage`) and every run and materialization disappears when you stop it. For a persistent local instance:

```bash
mkdir -p ~/.dagster_home
export DAGSTER_HOME=~/.dagster_home
dg dev
```

Instance settings (run concurrency, storage, run launcher, retention) go in `$DAGSTER_HOME/dagster.yaml`. Full key reference is in `deployment-oss.md`.

Two environment variables let code branch on execution context:

| Variable | Set by | Typical use |
| --- | --- | --- |
| `DAGSTER_IS_DEV_CLI=1` | `dg dev` | Start schedules/sensors `RUNNING` in production but `STOPPED` locally; point resources at local stores. |
| `DAGSTER_IS_DEFS_VALIDATION_CLI=1` | `dg check defs` | Skip expensive network calls or state fetches during validation. |

```python
import os

if os.getenv("DAGSTER_IS_DEV_CLI"):
    ...  # local-only behavior
```

## The verification loop

Run this after every change. Each step is cheap and fails loudly; do not claim a change works until `dg check defs` passes.

```bash
dg check yaml                 # component YAML matches its schema
dg check defs                 # all definitions import and resolve; exit 1 on error
dg list defs                  # the new/changed object actually appears
dg launch --assets 'key:"my_asset"'   # or: dg dev, then materialize in the UI
pytest tests                  # unit tests
```

`dg check defs` runs `dg check yaml` first unless you pass `--no-check-yaml`, so in CI `dg check defs` alone is usually sufficient. A successful run prints `All component YAML validated successfully.` and `All definitions loaded successfully.`

Common failure and its cause: a newly scaffolded component type does not appear in `dg list components`, or a custom component is not found. The entry point was added but the package was not reinstalled — run `uv pip install -e .`.

Testing patterns, fixtures, and asset checks are in `testing-and-asset-checks.md`.

## Legacy and superseded patterns

| Superseded | Use instead |
| --- | --- |
| `dagster` CLI (`dagster dev -m my_project.definitions`, `dagster asset materialize`, `dagster job execute`, `dagster definitions validate`) | `dg dev`, `dg launch --assets`, `dg launch --job`, `dg check defs` |
| `workspace.yaml` with `load_from:` entries | `dg.toml` with `[[workspace.projects]]`, or a project's `pyproject.toml` `[tool.dg]` |
| `@repository` | `Definitions` (usually produced by `@dg.definitions` + `load_from_defs_folder`) |
| `dagster-webserver` / `dagster-daemon run` launched separately in development | `dg dev` (launches both) |
| `dg scaffold build-artifacts`, `dg scaffold github-actions` | `dg plus deploy configure [serverless\|hybrid]` — see `dagster-plus.md` |

The `dagster` CLI still exists and remains the right tool for production process entry points (`dagster-webserver`, `dagster-daemon run`, `dagster api grpc`) — see `deployment-oss.md`. Converting an existing `@repository`/`workspace.yaml` project step by step is covered in `migration-and-legacy.md`.

## Sources
- https://docs.dagster.io/getting-started/installation — install paths for `create-dagster`, `dg`, and verification
- https://docs.dagster.io/getting-started/concepts — code location and `Definitions` definitions
- https://docs.dagster.io/dagster-basics-tutorial/projects — scaffolding walkthrough and default `definitions.py`
- https://docs.dagster.io/guides/build/projects — project vs. workspace terminology
- https://docs.dagster.io/guides/build/projects/creating-projects — `create-dagster project` steps and next steps
- https://docs.dagster.io/guides/build/projects/project-structure/project-overview — file and directory reference
- https://docs.dagster.io/guides/build/projects/project-structure/organizing-dagster-projects — by-technology vs. by-concept, external projects
- https://docs.dagster.io/guides/build/projects/project-structure/combining-components-with-pythonic-definitions — `Definitions.merge`, resource placement anti-patterns
- https://docs.dagster.io/guides/build/projects/workspaces/creating-workspaces — workspace scaffolding and per-project venvs
- https://docs.dagster.io/guides/build/projects/workspaces/dg-toml — `dg.toml` reference
- https://docs.dagster.io/guides/build/projects/moving-to-components/migrating-project — autoloading, registry module entry points, reinstall requirement
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/adding-component-definitions — `dg scaffold defs`, `--format python`, `defs.yaml` shape
- https://docs.dagster.io/guides/build/components/using-environment-variables-in-components — `dg list env`, `.env` workflow
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-configuration — config file kinds, precedence, `DG_PROJECT_PYTHON_EXECUTABLE`
- https://docs.dagster.io/api/clis/create-dagster — `create-dagster project` / `workspace` options and layouts
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference — `dg scaffold`, `dev`, `check`, `list`, `launch` flags
- https://docs.dagster.io/api/dagster/components — `DefsFolderComponent` discovery rules, `load_from_defs_folder`
- https://docs.dagster.io/deployment/oss/deployment-options/running-dagster-locally — ephemeral vs. `DAGSTER_HOME`, `DAGSTER_IS_DEV_CLI`
- https://docs.dagster.io/guides/operate/configuration/using-environment-variables-and-secrets — `.env` loading rules and reload caveat
- https://docs.dagster.io/api/clis/cli — superseded `dagster` CLI
