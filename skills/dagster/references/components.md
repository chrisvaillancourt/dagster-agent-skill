# Components

Components are the modern default for structuring a Dagster project: a `Component` subclass is a factory that builds `dg.Definitions`, and an instance of it is declared in a `defs.yaml` (or `component.py`) directory under `defs/`. This file covers using shipped components, templating `defs.yaml`, authoring and registering custom components, state-backed components, and migrating an existing project. Project scaffolding, `dg` installation and general CLI usage are in `project-setup-and-cli.md`. Integration-specific component attributes (dbt, Sling, Fivetran) are in `integrations-dbt-and-elt.md` and `integrations-catalog.md`. Asset selection syntax used by `post_processing` targets is in `assets.md`.

## Contents
- [Mental model](#mental-model)
- [Using an existing component](#using-an-existing-component)
- [defs.yaml structure](#defsyaml-structure)
- [Templating in defs.yaml](#templating-in-defsyaml)
- [Post-processing](#post-processing)
- [Partitions in components](#partitions-in-components)
- [Environment variables](#environment-variables)
- [Validating and testing component instances](#validating-and-testing-component-instances)
- [Authoring a custom component](#authoring-a-custom-component)
- [Registering components](#registering-components)
- [Subclassing an existing component](#subclassing-an-existing-component)
- [Testing custom components](#testing-custom-components)
- [State-backed components](#state-backed-components)
- [Migrating an existing project](#migrating-an-existing-project)
- [Managing components in the Dagster+ UI](#managing-components-in-the-dagster-ui)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Mental model

Use when definitions should be generated from configuration rather than hand-written per asset, or when an integration ships one. A component is a class implementing `build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions`. Classes live in `src/<pkg>/components/`, the registry module. An *instance* is a directory `src/<pkg>/defs/<name>/` holding a `defs.yaml` whose `type` is the fully-qualified Python path of the class, plus whatever files it references. `src/<pkg>/definitions.py` is a single `dg.load_from_defs_folder` call, which walks `defs/`, loads every instance and every plain `.py` module of definitions, and merges the results.

Directories under `defs/` with no `defs.yaml` are implicitly a `dagster.DefsFolderComponent`, which recursively discovers `defs.yaml` files, `.py` modules holding definitions, and nested folders, skipping `__pycache__` and dot-directories.

Built-in component types, always available:

| Type | Purpose |
|---|---|
| `dagster.DefsFolderComponent` | Container for a directory of child components; holds `post_processing` |
| `dagster.DefinitionsComponent` | An arbitrary set of Dagster definitions |
| `dagster.PythonScriptComponent` | Runs a Python script via Pipes with your `python` executable |
| `dagster.UvRunComponent` | Same, but launched with `uv run` |
| `dagster.FunctionComponent` | Wraps a Python function returning `MaterializeResult` / `AssetCheckResult` |
| `dagster.TemplatedSqlComponent` | Executes templated SQL from a string or file (preview) |

`dagster.SqlComponent` and `dagster.TemplatedSqlComponent` are preview APIs and may break in patch releases.

## Using an existing component

```bash
dg list components                        # every type in the environment; --package, --json
dg utils inspect-component dagster.PythonScriptComponent --defs-yaml-schema
dg scaffold defs dagster_dbt.DbtProjectComponent jdbt --project-path dbt/jdbt
```

`inspect-component` also takes `--description`, `--scaffold-params-schema`, `--defs-yaml-json-schema` and `--defs-yaml-example-values`. Prefer `--defs-yaml-schema`: it emits an annotated YAML template with Required/Optional markers and inline field docs, the right thing to read before writing a `defs.yaml`. `dg dev` serves the same as a **Docs** tab per code location. If a type is missing, install its library with `uv add dagster-dbt`.

For `dg scaffold defs`, the first argument is the component type and the second a path *relative to `defs/`*; run it from anywhere and output still lands under `src/<pkg>/defs/`. Flags after the path go to that component's scaffolder, listed by `--scaffold-params-schema`. `dg utils configure-editor vscode` installs an editor extension supplying JSON schemas for `defs.yaml`.

Pass `--format python` to get a `component.py` holding a `@dg.component_instance` function instead of a `defs.yaml`:

```python
# src/my_project/defs/jdbt/component.py
@dg.component_instance
def load(context: dg.ComponentLoadContext) -> DbtProjectComponent:
    return DbtProjectComponent(project=...)
```

Prefer YAML. Reach for `@dg.component_instance` only when the instance needs Python at load time, such as computing attributes from a live query or wiring an object with no YAML resolver.

## defs.yaml structure

Top-level keys:

| Key | Meaning |
|---|---|
| `type` | Fully-qualified path to the component class, e.g. `my_project.components.shell_command.ShellCommand` |
| `attributes` | Mapping validated against the component's derived pydantic schema |
| `post_processing` | Asset-level overrides applied after `build_defs` |
| `template_vars_module` | Module supplying `@dg.template_var` functions |
| `requirements` | Declares external requirements, currently `requirements.env` |

Multiple instances in one file are separated by `---`:

```yaml
# src/my_project/defs/generate_revenue_report/defs.yaml
type: dagster.PythonScriptComponent
attributes:
  execution: {path: process_sales_data.py}
  assets: [{key: sales_revenue_report, group_name: analytics}]
---   # second instance in the same file
type: dagster.PythonScriptComponent
attributes: {execution: {path: process_customer_data.py},
             assets: [{key: customer_summary_stats, deps: [sales_revenue_report]}]}
```

Relative paths inside `attributes` resolve against the component's own directory, so `scripts/process_sales_data.py` reaches a script in a subdirectory.

## Templating in defs.yaml

Every string value in a `Resolvable` is rendered by Jinja2 before resolution, and the result may be an arbitrary Python object, not just a string. Default scope:

| Expression | Yields |
|---|---|
| `{{ env.MY_VAR }}` | Value of the environment variable |
| `{{ env('MY_VAR', 'default') }}` | Same, with a fallback |
| `{{ project_root }}` / `{{ context.project_root }}` | Absolute path to the project root |
| `{{ context.load_component('sibling_dir') }}` | The component instance at that `defs/`-relative path |
| `{{ automation_condition.on_cron('@daily') }}` | Any static constructor of `dg.AutomationCondition` |

`context.load_component` accepts relative paths such as `'../duckdb_connection'`, which is how a SQL component reaches a connection component in a sibling folder. Values resolve to real Python objects, so `automation_condition: "{{ automation_condition.eager() }}"` passes an `AutomationCondition`, not its repr. Do not reach for `{% if %}` / `{% for %}`. Push branching and iteration into a template UDF instead: unit-testable, type-checked, and it keeps the YAML flat.

### Custom template variables

A `@dg.template_var` function adds a name to the Jinja scope. Two valid signatures only: zero parameters, or a single `dg.ComponentLoadContext`. Anything else fails at load. A template var returning a callable is a UDF.

```python
# src/my_project/defs/my_component/template_vars.py
import os
from collections.abc import Callable
import dagster as dg

@dg.template_var
def table_prefix(context: dg.ComponentLoadContext) -> str:
    return "warehouse" if os.getenv("IS_PROD") else f"staging_{context.path.name}"

@dg.template_var
def compliance_tags() -> Callable[[str, bool], dict[str, str]]:
    def _tags(classification: str, has_pii: bool = False) -> dict[str, str]:
        base = {"public": 30, "internal": 90}.get(classification, 90)
        return {"retention_days": str(base * (2 if has_pii else 1))}
    return _tags
```

```yaml
type: my_project.components.DataProcessor
template_vars_module: .template_vars   # leading dot = relative to this component's module
attributes:
  table_name: "{{ table_prefix }}_data"
  tags: "{{ compliance_tags('internal', has_pii=false) }}"
```

`template_vars_module` takes a relative path resolved against the component's module, or an absolute one such as `my_project.shared.template_vars`. For a reusable component, declare the same functions as `@staticmethod` + `@dg.template_var` on the class; every instance then gets them with no `template_vars_module`.

## Post-processing

Use when you want to change attributes of the assets a component produced without touching the component's code. Only assets are supported; other definition types are not.

```yaml
# src/my_project/defs/my_assets/defs.yaml
type: dagster.DefsFolderComponent
post_processing:
  assets:
    - attributes: {kinds: ["some_kind"]}      # no target: applies to every asset
    - {target: foo, attributes: {group_name: "foo_group"}}
    - {target: "tag:defs_tag=true", attributes: {group_name: "tagged_group"}}
```

`target` accepts the full asset selection syntax (`*`, `tag:k=v`, `key:`). Settable attributes: `deps`, `description`, `metadata`, `group_name`, `skippable`, `code_version`, `owners`, `tags`, `kinds`, `automation_condition`, `partitions_def`. Post-processors are evaluated in order, so a later targeted entry refines a broad earlier one; verify with `dg list defs`. A `defs.yaml` carrying only `post_processing` for a folder should declare `type: dagster.DefsFolderComponent` and nothing else.

## Partitions in components

In YAML, expose the `PartitionsDefinition` from a `@dg.template_var` named `the_daily_partitions_def`, then apply it through `post_processing`:

```yaml
template_vars_module: .template_vars
post_processing:
  assets:
    - target: "*"
      attributes: {partitions_def: "{{ the_daily_partitions_def }}"}
```

In Python, subclass the component and map over its specs with `super().build_defs(context).map_asset_specs(fn)`, where `fn` takes an `AssetSpec` and returns `spec.replace_attributes(partitions_def=...)`.

Keep one partitions definition per component: each execution step maps to a single partitions definition, so mixing them breaks any integration that groups several assets into one step. Execution logic reads `context.partition_key` or `context.partition_time_window`; see `partitions-and-backfills.md`.

## Environment variables

Reference variables with `{{ env.VAR }}`, then declare them under `requirements.env` so the dependency is explicit:

```yaml
type: dagster_sling.SlingReplicationCollectionComponent
attributes:
  connections:
    SNOWFLAKE: {account: "{{ env.SNOWFLAKE_ACCOUNT }}", password: "{{ env.SNOWFLAKE_PASSWORD }}"}
requirements:
  env: [SNOWFLAKE_ACCOUNT, SNOWFLAKE_PASSWORD]
```

`dg check yaml --validate-requirements` fails with the list of variables used in templates but absent from `requirements.env`. `dg list env` (spelled `dg list envs` in the CLI reference) shows every variable referenced by components and whether it has a value; populate a `.env` file at the project root for local runs. On Dagster+, `dg plus create env --from-local-env` pushes them to a deployment scope.

## Validating and testing component instances

```bash
dg check yaml                  # schema-validate every defs.yaml (fast, no import)
dg check yaml --watch          # re-validate on save
dg check defs                  # fully load definitions through a Dagster instance
dg list defs                   # confirm what was actually produced
```

Run `dg check yaml` first: it catches attribute typos and type errors without importing your code. `dg check defs` is slower, exits 1 on error, sets `DAGSTER_IS_DEFS_VALIDATION_CLI=1` while running, and takes `--no-check-yaml` to skip the YAML pass. To assert on a real instance in pytest, load it through `dg.ComponentTree.for_project`:

```python
# tests/test_components.py
def test_function_component() -> None:
    tree = dg.ComponentTree.for_project(Path(__file__).parent.parent)
    assert isinstance(tree.load_component("path/to/fn"), dg.FunctionComponent)
    defs = tree.build_defs_at_path("path/to/fn")
    assert dg.materialize(defs.get_assets_def("some_asset")).success
```

The path is relative to the `defs` folder; the project root is the directory holding `pyproject.toml` or `setup.py`.

## Authoring a custom component

Use when several definitions share a shape that differs only by configuration. Map a component to one technology: `SnowflakeQueryComponent`, `DockerScriptComponent`.

```bash
dg scaffold component ShellCommand          # -> src/my_project/components/shell_command.py
dg scaffold component ShellCommand --no-model   # pythonic interface, no YAML schema
```

```python
# src/my_project/components/shell_command.py
import subprocess
from collections.abc import Sequence
from pathlib import Path
import dagster as dg

class ShellCommand(dg.Component, dg.Model, dg.Resolvable):
    """Models a shell script as a Dagster asset."""

    script_path: str
    asset_specs: Sequence[dg.ResolvedAssetSpec]

    @classmethod
    def get_spec(cls) -> dg.ComponentTypeSpec:      # owners/tags shown in the UI Docs tab
        return dg.ComponentTypeSpec(owners=["team@example.com"], tags=["shell"])

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resolved = Path(context.path, self.script_path).absolute()

        @dg.multi_asset(name=Path(self.script_path).stem, specs=self.asset_specs)
        def _asset(context: dg.AssetExecutionContext):
            self.execute(resolved, context)

        return dg.Definitions(assets=[_asset])

    def execute(self, path: Path, context: dg.AssetExecutionContext):
        return subprocess.run(["sh", str(path)], check=True)
```

- `dg.Model` + `dg.Resolvable` gives the class a pydantic model and a derived YAML schema. `Model` disallows extra fields, so a misspelled YAML key errors instead of being ignored. `pydantic.BaseModel` and `@dataclass` also work, so existing classes can be adopted unchanged.
- `context.path` is the instance directory; resolve user-supplied relative paths against it.
- Put execution in a method named `execute` so subclasses override it without reimplementing `build_defs`.
- `@dg.multi_asset(specs=...)` when one invocation covers several assets; a single-asset component takes `asset_spec: dg.ResolvedAssetSpec` instead of a `Sequence`.

### The Resolved framework

`dg.Resolvable` derives the YAML schema from the class's fields or annotated `__init__`. Subclasses must be a pydantic model, a `@dataclass`, a plain class with an annotated `__init__`, or a `@record`. Annotated aliases resolve common Dagster types from YAML:

| Alias | Resolves to |
|---|---|
| `dg.ResolvedAssetSpec` | `dg.AssetSpec` |
| `dg.ResolvedAssetKey` | `dg.AssetKey` |
| `dg.ResolvedAssetCheckSpec` | `dg.AssetCheckSpec` |
| `dg.AssetAttributesModel` | All settable attributes of an `AssetSpec` |

For a type with no built-in schema, attach a `dg.Resolver` to the annotation. The resolve function takes a `dg.ResolutionContext` plus the raw model value and calls `context.resolve_value` to render nested templates:

```python
def resolve_api_key(context: dg.ResolutionContext, api_key: str) -> MyApiClient:
    return MyApiClient(api_key=context.resolve_value(api_key, as_type=str))

@dataclass
class MyComponent(dg.Component, dg.Resolvable):
    api_client: Annotated[
        MyApiClient,
        dg.Resolver(resolve_api_key, model_field_name="api_key", model_field_type=str),
    ]
```

`model_field_type` is the type the YAML author writes; `model_field_name` renames the YAML key relative to the Python attribute. It is also the hook that lets a test pass a mock client while YAML passes a key. Nested `Resolvable` models compose, so a component can accept a list of sub-models. Add component-specific Jinja names with a `get_additional_scope` classmethod returning a mapping such as `{"daily_partitions": dg.DailyPartitionsDefinition(start_date="2024-01-01")}`, then write `partitions_def: '{{ daily_partitions }}'` in YAML.

### Custom scaffolders

By default `dg scaffold defs` creates a directory with an empty `defs.yaml`. Attach a `dg.Scaffolder` with `@dg.scaffold_with` to emit a populated `defs.yaml` plus companion files:

```python
class ShellCommandScaffolder(dg.Scaffolder):
    def scaffold(self, request: dg.ScaffoldRequest) -> None:
        dg.scaffold_component(request, {"script_path": "script.sh",
                                        "asset_specs": [{"key": "my_asset"}]})
        (Path(request.target_path) / "script.sh").write_text("#!/bin/bash\necho hi")

@dg.scaffold_with(ShellCommandScaffolder)
class ShellCommand(dg.Component, dg.Resolvable): ...
```

`dg.scaffold_component` writes the `defs.yaml`; `request.target_path` is the instance directory and `request.type_name` the fully-qualified string for `type:`.

## Registering components

A component class is discoverable when its module is a *registry module*. Two mechanisms:

1. **Project config.** `[tool.dg.project] registry_modules` in `pyproject.toml` (or the top-level `project` table in `dg.toml`) lists module names or `*`-patterns. `<root_module>.components` is the conventional location, and `dg scaffold component` adds the module to the list if not already covered. `*` matches whole segments only: `foo.*.bar` is valid, `foo.*bar` is not.
2. **Entry point.** For components shipped from an installed package, declare `dagster_dg_cli.registry_modules` in the package metadata:

```toml
# pyproject.toml
[project.entry-points]
"dagster_dg_cli.registry_modules" = { my_existing_project = "my_existing_project.components" }
```

Entry points register at *install* time. After adding one to an editable install you must reinstall with `uv pip install -e .`; `uv sync` does not reinstall the project itself and the entry point stays invisible. Confirm with `dg list registry-modules`, then `dg list components`. The key it prints is the full dotted path including the defining module, for example `my_project.components.shell_command.ShellCommand`, and that exact string goes in `type:`.

### Inline components

Use when a component is genuinely single-use and should not appear in the registry.

```bash
dg scaffold defs inline-component --typename AssetWithSchedule assets_with_schedules
```

This writes both the class (`asset_with_schedule.py`) and a `defs.yaml` into `defs/assets_with_schedules/`. Fill in the class as usual, then declare instances against its full path, `my_project.defs.assets_with_schedules.asset_with_schedule.AssetWithSchedule`. Nothing is special about `type:` here; the class simply never enters the registry.

## Subclassing an existing component

Use when `defs.yaml` cannot express the customization. Put the subclass in `component.py` next to the instance for a one-off, or in `components/` for something reusable, then point `type:` at it.

```python
# src/my_project/defs/my_sling_sync/component.py
from dagster_sling import SlingReplicationCollectionComponent, SlingReplicationSpecModel, SlingResource
import dagster as dg

class CustomSlingReplicationComponent(SlingReplicationCollectionComponent):
    def execute(self, context: dg.AssetExecutionContext, sling: SlingResource,
                replication_spec_model: SlingReplicationSpecModel):
        context.log.info("custom execution")
        return sling.replicate(context=context, debug=True)
```

Then set `type: my_project.defs.my_sling_sync.component.CustomSlingReplicationComponent` in `defs.yaml`.

For components that translate external metadata into assets, override the spec builder rather than the executor. `DbtProjectComponent` exposes `get_asset_spec(self, manifest, unique_id, project) -> dg.AssetSpec`; call `super()`, then `replace_attributes` / `merge_attributes`. This replaces a custom `DagsterDbtTranslator`, whose `get_asset_key`, `get_group_name`, `get_description` and `get_metadata` hooks all collapse into `get_asset_spec`. A subclass is also where you add YAML scope for your team, via `get_additional_scope` returning, say, a `custom_cron` function that builds an `AutomationCondition`.

## Testing custom components

`dg.components.testing.create_defs_folder_sandbox` yields a `DefsFolderSandbox` backed by a temporary `defs` folder, exercising the scaffolder and the loader the way a user would.

```python
def test_scaffold_and_load():
    with dg.components.testing.create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.scaffold_component(
            component_cls=MyComponent,
            defs_yaml_contents={"type": "my_project.components.MyComponent",
                                "attributes": {"asset_key": "my_asset"}},
        )
        with sandbox.load_component_and_build_defs(defs_path=defs_path) as (component, defs):
            assert defs.get_assets_def("my_asset").key == dg.AssetKey("my_asset")
```

`scaffold_component` takes `component_cls`, `defs_path`, `scaffold_params`, `scaffold_format` and `defs_yaml_contents`; assert on custom scaffolder output with `(defs_path / "replication.yaml").exists()`. Scaffold several components into one sandbox and call `sandbox.build_all_defs()` to cover cross-component references such as `{{ context.load_component('...') }}`. The sandbox puts its `src` on `sys.path` and evicts loaded modules on exit.

## State-backed components

Use when definitions depend on an external system: a Fivetran connector list, a compiled dbt manifest, a Tableau site. `dg.StateBackedComponent` splits `build_defs` into an expensive fetch and a cheap load.

```python
from dagster.components import DefsStateConfig, DefsStateConfigArgs, ResolvedDefsStateConfig

@dataclass
class MyStateBackedComponent(dg.StateBackedComponent):
    base_url: str
    defs_state: ResolvedDefsStateConfig = DefsStateConfigArgs.local_filesystem()

    @property
    def defs_state_config(self) -> DefsStateConfig:
        return DefsStateConfig.from_args(self.defs_state, default_key=f"MyComponent[{self.base_url}]")

    def write_state_to_path(self, state_path: Path) -> None:
        state_path.write_text(json.dumps(fetch_tables(self.base_url)))

    def build_defs_from_state(self, context, state_path: Optional[Path]) -> dg.Definitions:
        return dg.Definitions() if state_path is None else build(json.loads(state_path.read_text()))
```

`write_state_to_path` runs at most once per code location load. In CI it is driven by `dg utils refresh-defs-state`, which takes `--defs-state-key` and `--management-type` and operates on one project at a time; `workspace.yaml` plays no role, so run it once per project directory with the project fully installed. Configure the strategy under `attributes.defs_state`:

| Field | Values | Notes |
|---|---|---|
| `management_type` | `LOCAL_FILESYSTEM`, `VERSIONED_STATE_STORAGE`, `LEGACY_CODE_SERVER_SNAPSHOTS` | Bundled components default to `LOCAL_FILESYSTEM` |
| `key` | string | Defaults to `ClassName[discriminator]`; identical keys share state |
| `refresh_if_dev` | bool, default `true` | Auto-refresh under `dagster dev` and `dg` commands |

`LOCAL_FILESYSTEM` writes to a gitignored `defs/.local_defs_state/<key>/state`. Refresh in CI before `docker build` so the directory ships inside the image; never commit it.

`VERSIONED_STATE_STORAGE` writes UUID-versioned objects to blob storage. On OSS, configure `defs_state_storage` in `dagster.yaml` with `UPathDefsStateStorage` and an S3/GCS/Azure `base_path`, and set `DAGSTER_HOME` for the refresh step; every code location reload then picks up the latest version. On Dagster+ it is fully managed and each load is *pinned* to specific versions, so a plain reload does not advance them: use **Refresh definitions state** in the code location dropdown or the `refreshDefsState` GraphQL mutation. The per-component **Refresh** action is preview, gated behind the **Enable experimental component instance UI** flag.

`LEGACY_CODE_SERVER_SNAPSHOTS` refreshes on every load and keeps state in memory. Superseded: an unavailable upstream API takes the whole location down, there is no rollback, and run artifacts grow. Do not use it in new components. Two components may legitimately share a key, such as several `DbtProjectComponent` instances selecting different models from one project, but an accidental collision silently overwrites state, so set `key` only deliberately.

## Migrating an existing project

Use when an existing package has a hand-built top-level `Definitions` and you want autoloading. Install `dagster-dg-cli`, then add `dg` config to `pyproject.toml` (or a `dg.toml` if the project uses `setup.py`):

```toml
[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "my_existing_project"
code_location_target_module = "my_existing_project.definitions"   # defaults to <root>.definitions
```

Create `src/<pkg>/components/__init__.py`, add the `dagster_dg_cli.registry_modules` entry point, reinstall the package, and create the `src/<pkg>/defs/` submodule. Then merge autoloaded definitions into the existing object:

```python
# src/my_existing_project/definitions.py
from pathlib import Path
from my_existing_project.assets import my_asset
import dagster as dg

defs = dg.Definitions.merge(
    dg.Definitions(assets=[my_asset]),
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent.parent),
)
```

Move one module at a time into `defs/` with `git mv`, delete its import from `definitions.py`, and run `dg list defs`. When everything has moved, `definitions.py` is a single `load_from_defs_folder` call. To use a component *without* restructuring, keep your layout and merge one instance in with `dg.build_defs_for_component(component=SlingReplicationCollectionComponent(...))`.

Resources are not components. Put them in `defs/<name>/defs.py` returning a `Definitions` with a `resources` mapping, or in a `resources/` module bound in `definitions.py`. A `defs/snowflake/defs.yaml` for a resource is an anti-pattern: YAML instances require a `Component` class. See `resources-and-config.md`.

## Managing components in the Dagster+ UI

Preview, Dagster+ only, gated behind the per-user **Enable experimental component instance UI** flag. The code location's **Components** tab has a **Library** sub-tab of installed types and an **Instances** sub-tab. Instances added here are **app-managed**: their configuration lives in Dagster+ and is never written back to your source files. Components defined in code show as **Code-backed** and are read-only. If a change breaks the location, the UI offers to revert it.

## Gotchas

- The `type:` value is the fully-qualified path including the defining module, not `package.ClassName`. Copy it from `dg list components`.
- `dg.Model` forbids extra fields. A misspelled attribute is a pydantic validation error, not a silently ignored key.
- Run `dg check yaml` before `dg check defs`; the first is fast and needs no import.
- Environment variables referenced in templates must exist at *load* time, not just run time, and must be listed in `requirements.env` to pass `dg check yaml --validate-requirements`.
- Adding an entry point to an editable install requires a reinstall; `uv sync` will not do it.
- `post_processing` applies to assets only, and a folder-level `defs.yaml` using it must declare `type: dagster.DefsFolderComponent`.
- Keep one partitions definition per component instance.
- `dg scaffold defs` always writes under `src/<pkg>/defs/` regardless of your working directory.
- Jinja values resolve to real Python objects, so `partitions_def: "{{ the_daily_partitions_def }}"` passes a `PartitionsDefinition`, not its repr. Quote the template so YAML does not try to parse the braces as a flow mapping.

## Legacy and superseded patterns

| Old | New |
|---|---|
| `@repository`, `workspace.yaml`, `dagster` CLI | `dg` CLI, `defs/` autoloading, `load_from_defs_folder` |
| Hand-written `Definitions(...)` listing every asset | `dg.load_from_defs_folder(path_within_project=...)` merged with any remainder |
| Custom `DagsterDbtTranslator` subclass | Subclass `DbtProjectComponent` and override `get_asset_spec` |
| `defs_state` `LEGACY_CODE_SERVER_SNAPSHOTS` | `LOCAL_FILESYSTEM` or `VERSIONED_STATE_STORAGE` |

## Sources
- https://docs.dagster.io/guides/build/components — components overview
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/adding-component-definitions — scaffolding instances, defs.yaml, multi-instance YAML
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/post-processing-components — post_processing schema, targets, testing-component-definitions
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/using-template-variables — template_vars_module, static template vars
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/using-partitions — partitions via post_processing and map_asset_specs
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/python-script-component-tutorial — PythonScriptComponent worked example
- https://docs.dagster.io/guides/build/components/using-environment-variables-in-components — env templating, requirements.env
- https://docs.dagster.io/guides/build/components/creating-new-components/creating-and-registering-a-component — Component/Model/Resolvable, build_defs, get_spec, inline components
- https://docs.dagster.io/guides/build/components/creating-new-components/component-customization — Resolver, get_additional_scope, custom scaffolder
- https://docs.dagster.io/guides/build/components/creating-new-components/subclassing-components — local vs global subclasses, execute override
- https://docs.dagster.io/guides/build/components/creating-new-components/testing-your-component — create_defs_folder_sandbox
- https://docs.dagster.io/guides/build/components/state-backed-components — state strategies and defs state keys
- https://docs.dagster.io/guides/build/components/state-backed-components/configuring-state-backed-components — defs_state fields
- https://docs.dagster.io/guides/build/components/state-backed-components/managing-state-in-ci-cd — refresh-defs-state in CI, versioned state storage config
- https://docs.dagster.io/guides/build/components/managing-components-in-the-ui — app-managed vs code-backed instances
- https://docs.dagster.io/guides/build/projects/moving-to-components/migrating-project — dg config, entry point, Definitions.merge, build_defs_for_component
- https://docs.dagster.io/guides/build/projects/project-structure/combining-components-with-pythonic-definitions — where resources belong
- https://docs.dagster.io/api/dagster/components — Component, StateBackedComponent, Resolvable, Resolver, template_var, built-ins, testing utilities
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-configuration — registry_modules and project settings
