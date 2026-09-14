# dbt and ELT integrations

Deep coverage of the five most-used data integrations: dbt (Core and Cloud), dlt, Sling, Fivetran, Airbyte. Each gets the Component path (preferred), the Pythonic escape hatch, translation, and dependency wiring to non-integration assets. Generic component mechanics (`defs.yaml` shape, template vars, `post_processing`, subclassing) are in `components.md`; only integration-specific keys appear here. Other libraries are catalogued in `integrations-catalog.md`; partition mappings in `partitions-and-backfills.md`; automation conditions in `declarative-automation.md`.

## Contents

- [Choosing a path](#choosing-a-path)
- [dbt: Component path](#dbt-component-path)
- [dbt: Pythonic path](#dbt-pythonic-path)
- [dbt: manifest lifecycle and deployment](#dbt-manifest-lifecycle-and-deployment)
- [dbt: tests as asset checks](#dbt-tests-as-asset-checks)
- [dbt: dependencies with non-dbt assets](#dbt-dependencies-with-non-dbt-assets)
- [dbt: partitions and incremental models](#dbt-partitions-and-incremental-models)
- [dbt Cloud](#dbt-cloud)
- [dlt](#dlt)
- [Sling](#sling)
- [Fivetran](#fivetran)
- [Airbyte](#airbyte)
- [Cross-cutting patterns](#cross-cutting-patterns)
- [Troubleshooting](#troubleshooting)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Choosing a path

All five ship a Component (YAML) and a Pythonic factory. Default to the Component; it covers any customization expressible as key, group, metadata, or automation overrides. Drop to `@dbt_assets` only for two `PartitionsDefinition`s over one dbt project, since one component carries one. For custom Python around execution or translation logic too complex for Jinja, subclass the Component (`execute`, `get_asset_spec`) rather than abandoning it: you keep YAML config, scaffolding, and state management. Verify with `dg check defs` and `dg list defs`, then run `dg launch --assets '*'`.

## dbt: Component path

```bash
uv add dagster-dbt dbt-duckdb  # swap in your adapter
dg scaffold defs dagster_dbt.DbtProjectComponent dbt_ingest --project-path dbt
```

Recommended layout: dbt project beside the Dagster package (`dbt/` at the project root, component at `src/<project>/defs/dbt_ingest/defs.yaml`). `DbtProjectComponent` is state-backed: it compiles and caches the manifest.

```yaml
# src/my_project/defs/dbt_ingest/defs.yaml
type: dagster_dbt.DbtProjectComponent
attributes:
  project: '{{ context.project_root }}/dbt'
  select: 'tag:daily'
  cli_args: ['build']
  include_metadata: [column_metadata, row_count]
  translation: { group_name: dbt_models, description: 'dbt model {{ node.name }}' }
```
Set `translation.group_name` whenever a schedule, job, or `dg launch --assets 'group:...'` must select exactly the dbt models; without it they land in the `default` group alongside everything else. A `profiles.yml` sitting beside `dbt_project.yml` needs no `profiles_dir`; a successful `dg launch` build is the proof that dbt found it.

| Key | Meaning |
| --- | --- |
| `project` | Path string, or mapping: `project_dir`, or `repo_url` + `repo_relative_path` + `token` for a remote dbt repo; `prepare_project_cli_args` overrides manifest generation |
| `select` / `exclude` / `selector` | dbt selection strings; `select` defaults to `fqn:*`, `selector` names an entry in `selectors.yml` |
| `cli_args` | The dbt command as a list |
| `translation` | Per-node asset attribute overrides, Jinja over `node` |
| `op` | Op settings; `pool` for concurrency |
| `include_metadata` | Any of `column_metadata`, `row_count`, `insights` |
| `template_vars_module` | Top-level, sibling of `attributes` (e.g. `.template_vars`) |
| `post_processing` | Top-level; applies attributes such as `partitions_def` to generated specs |

A remote dbt project is cloned into component state; scaffold it with `--git-url <url> --project-path <subdir>`. `prepare_project_cli_args` defaults to `["parse", "--quiet"]`; use `["compile", "--quiet"]` when translations need `node.compiled_code`. `selector` is present in the 1.13 component schema, but the docs still describe the component as not reading `selectors.yml` and prescribe workarounds: tag the models and use `select: 'tag:x'`, select by `path:models/marts/`, or move that slice to `@dbt_assets`.

Translation templates see `node.name`, `node.fqn` (a list, e.g. `["jaffle_shop", "staging", "stg_customers"]`), `node.original_file_path`, `node.tags`, `node.config`. Register a `@dg.template_var` in a `template_vars.py` beside `defs.yaml` to derive groups from directory layout, then reference it: `group_name: '{{ group_from_fqn()(node.fqn) }}'`.

### Subclassing

```python
# src/my_project/lib/custom_dbt_component.py
class CustomDbtConfig(dg.Config):
    full_refresh: bool = False


class CustomDbtProjectComponent(DbtProjectComponent):
    @property
    def op_config_schema(self) -> type[dg.Config]:
        return CustomDbtConfig

    def get_asset_spec(self, manifest, unique_id: str, project) -> dg.AssetSpec:
        props = self.get_resource_props(manifest, unique_id)
        return super().get_asset_spec(manifest, unique_id, project).merge_attributes(
            metadata={"dbt_model_name": props["name"]}
        )

    def execute(self, context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
        args = ["build", "--full-refresh"] if context.op_config.get("full_refresh") else ["build"]
        yield from dbt.cli(args, context=context).stream()
```

Point `type:` at the subclass; supply op config at launch under `ops: {<op_name>: {config: {full_refresh: true}}}`.

## dbt: Pythonic path

```python
# src/my_project/defs/dbt.py
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

import dagster as dg

dbt_project = DbtProject(project_dir=Path(__file__).absolute().parent / "dbt")
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_row_counts()


defs = dg.Definitions(resources={"dbt": DbtCliResource(project_dir=dbt_project)})
```

`DbtProject(project_dir=, target_path="target", profiles_dir=, profile=, target=, packaged_project_dir=, state_path=, prepare_project_cli_args=)`. `packaged_project_dir` receives a copy of the project plus `manifest.json` at build time, which PEX-style deployments need. `DbtCliResource(project_dir=, global_config_flags=, profiles_dir=, profile=, target=, dbt_executable="dbt", state_path=)`.

`@dbt_assets(manifest=, select="fqn:*", exclude=, selector=, name=, io_manager_key=, partitions_def=, dagster_dbt_translator=, backfill_policy=, op_tags=, required_resource_keys=, project=, retry_policy=, pool=)`. `selector` cannot combine with `select`/`exclude`. Pass `project=` so code references link assets to their `.sql` files. Several `@dbt_assets` can target one manifest if their selections do not overlap. `dbt.cli(args, context=context)` returns a `DbtCliInvocation`: `.stream()` yields Dagster events, `.wait()` blocks without emitting them, `.get_artifact("run_results.json")` reads an artifact afterwards, `.stream_raw_events()` exposes raw dbt events. `.fetch_row_counts()` and `.fetch_column_metadata()` chain onto `.stream()` and fetch in parallel with model execution.

### Translator

```python
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        return super().get_asset_key(dbt_resource_props).with_prefix("snowflake")


# pass as dagster_dbt_translator= to @dbt_assets
translator = CustomDagsterDbtTranslator(DagsterDbtTranslatorSettings(enable_code_references=True))
```

Current override points: `get_asset_key`, `get_group_name`, `get_description`, `get_metadata`, `get_tags`, `get_owners`, `get_automation_condition`, `get_code_version`, `get_partition_mapping`. `get_auto_materialize_policy` is superseded by `get_automation_condition`.

`DagsterDbtTranslatorSettings` defaults: `enable_asset_checks=True`, `enable_source_tests_as_checks=False`, `enable_code_references=False`, `enable_duplicate_source_asset_keys=False`, `enable_dbt_selection_by_name=False`, `enable_source_metadata=True`, `enable_dbt_views_as_virtual_assets=False`.

For one-off overrides, dbt `meta` config beats writing a translator. Under a model's `config.meta.dagster`, set `group`, `asset_key`, or `owners` (a list, e.g. `['owner@company.com', 'team:data@company.com']`).

Defaults: asset key is `<schema>/<model_name>` for models, seeds, and snapshots (bare name when no schema) and `<source_name>/<table_name>` for sources; group is the dbt group; owner is that group's email; `code_version` is a hash of the model SQL, which drives the "Unsynced" indicator.

## dbt: manifest lifecycle and deployment

`dagster-dbt` needs `manifest.json`: generate it at build time in production, at load time in development.

- Development: `DbtProject.prepare_if_dev()` regenerates it under `dg dev` and is a no-op elsewhere. The component does this itself.
- CI/CD: `dagster-dbt project prepare-and-package --file path/to/project.py` runs `dbt deps` and `dbt parse`, then packages the project. For components, pass `--components path/to/project-root`. In Docker, `RUN cd /path/to/dbt/project && dbt deps && dbt parse` during the image build does the same job.

`dbt parse` needs a `profiles.yml` with credentials present, but never connects, so `env_var` dummy defaults suffice. When dbt lives in its own repository, clone it as a subdirectory in CI, supply the dbt secrets to the CI environment, and add a dispatch trigger from the dbt repo to redeploy the code location. For `dbt defer` in branch deployments, pass `state_path` to `DbtProject` (relative to the dbt project directory, created if absent) and splat `*dbt.get_defer_args()` into the command: `dbt.cli(["build", *dbt.get_defer_args()], context=context).stream()`. Defer requires `DAGSTER_BUILD_STATEDIR` in CI.

In CI, `dg plus integrations dbt manage-manifest` uploads the manifest from the source deployment (default `prod`) and downloads it in branch deployments, replacing `dagster-cloud ci dagster-dbt project manage-state`. `dg plus integrations dbt download-manifest` pulls it for local development. Both take `--components <dir>` or `--file <py>`. Prod must run first to create the baseline.

## dbt: tests as asset checks

On by default: dbt tests on models load as asset checks, generic and singular alike, keyed like `customers:not_null_customers_customer_id`.

- `DagsterDbtTranslatorSettings(enable_asset_checks=False)` disables the mapping. Tests still run and emit `AssetObservation` events.
- Source tests are excluded by default; `enable_source_tests_as_checks=True` loads them. A `dbt build` runs them either way.
- A singular test spanning several models needs `meta.dagster.ref` naming the target, or Dagster runs it without ingesting it as a check: `{{ config(meta={'dagster': {'ref': {'name': 'customers', 'package': 'my_dbt_assets'}}}) }}`.
- Indirect selection: Dagster leaves `DBT_INDIRECT_SELECTION` unset so its selection matches dbt's, and overrides it to `empty` only when tests must be selected explicitly (materializing assets while excluding their checks, or the reverse).
- `build_dbt_asset_selection` with a tag filters *models*, not tests. To run tagged tests including source tests, invoke the CLI through `DbtCliResource` with dbt selection syntax.
- `build_freshness_checks_from_dbt_assets` builds freshness checks from dbt metadata; check semantics live in `testing-and-asset-checks.md`.

## dbt: dependencies with non-dbt assets

**Upstream (a Dagster asset feeds a dbt model).** Declare the table as a dbt source and stamp the Dagster asset key on it. This is the pattern for Sling, Fivetran, dlt, and plain Python producers, and it works across code locations.

```yaml
# dbt/models/sources.yml
version: 2
sources:
  - name: raw
    tables:
      - name: raw_customers
        meta: { dagster: { asset_key: ['raw_customers'] } }
```

Any model selecting `{{ source('raw', 'raw_customers') }}` then depends on the asset keyed `raw_customers`. For a scheduling-only edge with no data read, put `-- depends_on: {{ source('raw','raw_customers') }}` in the model as a comment. Without the `meta` block, `get_asset_key_for_source([my_dbt_assets], "jaffle_shop")` returns the generated key for a single-table source, and `get_asset_keys_by_output_name_for_source` handles a multi-table source feeding a `@multi_asset`.

**Downstream (a Dagster asset reads a dbt model).** Use `@dg.asset(deps=[get_asset_key_for_model([my_dbt_assets], "my_dbt_model")])`, or resolve the key in YAML from another component:

```yaml
type: dagster.PythonScriptComponent
attributes:
  execution: { path: export_customers.py }
  assets:
    - key: customers_export
      deps:
        - "{{ context.load_component('dbt_ingest').asset_key_for_model('customers') }}"
```

## dbt: partitions and incremental models

Two batching models, two sets of flags. Confusing them fails silently.

**Regular incremental.** You filter the rows; dbt runs the model once for the whole window. Pass `--vars` and read them inside `{% if is_incremental() %}`.

Put the non-incremental models in a companion `@dbt_assets` with `exclude="config.materialized:incremental"`.

```python
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="config.materialized:incremental",
    partitions_def=dg.DailyPartitionsDefinition(start_date="2023-01-01"),
)
def incremental_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    tw = context.partition_time_window
    dbt_vars = {"start_date": tw.start.strftime("%Y-%m-%d"), "end_date": tw.end.strftime("%Y-%m-%d")}
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()
```

**Microbatch.** dbt's engine subdivides the window itself, so pass `--event-time-start` / `--event-time-end`. `--vars` only injects values into the SQL template and does not control batch scheduling, so a microbatch model driven by `--vars` silently reprocesses every batch from `begin` to now. Select these with `config.incremental_strategy:microbatch`. Match the `PartitionsDefinition` to the model's `batch_size` (`day`/`month`/`year` to daily/monthly/yearly) and its `start_date` to the model's `begin`.

In the Component, partitions come from `post_processing` and the command from `cli_args`, which sees `partition_key` and `partition_time_window`:

```yaml
type: dagster_dbt.DbtProjectComponent
template_vars_module: .template_vars
attributes:
  project: '{{ context.project_root }}/dbt'
  select: 'config.materialized:incremental'
  cli_args:
    - build
    - --vars:
        start_date: "{{ partition_time_window.start.strftime('%Y-%m-%d') }}"
        end_date: "{{ partition_time_window.end.strftime('%Y-%m-%d') }}"
post_processing:
  assets: [{ target: '*', attributes: { partitions_def: '{{ daily_partitions_def }}' } }]
```

Dagster JSON-encodes the `--vars` mapping for the CLI. For microbatch, replace those entries with `--event-time-start` and `--event-time-end` as separate list items. One component carries one `PartitionsDefinition`; use a second component with a different `select` for a second scheme. Snapshots must never run concurrently. Split them into their own component with `select: 'resource_type:snapshot'` and `op: {pool: dbt-snapshots}`, exclude them from the models component, and cap the pool with `dagster instance concurrency set dbt-snapshots 1`.

## dbt Cloud

`DbtCloudComponent` for YAML, `@dbt_cloud_assets` for Python. Execution is remote, so code references to local SQL are unavailable.

```yaml
type: dagster_dbt.DbtCloudComponent
attributes:
  workspace:
    account_id: 123456
    token: '{{ env.DBT_CLOUD_TOKEN }}'
    project_id: 11111
    environment_id: 22222
  select: 'tag:dagster'
```

Component attributes: `workspace` (required), `select` (default `fqn:*`), `exclude`, `defs_state`. Workspace fields: `account_id`, `token`, `project_id`, `environment_id` required; `access_url` defaults to `https://cloud.getdbt.com`; `adhoc_job_name`, `request_max_retries` (3), `request_retry_delay` (0.25), `request_timeout` (15) optional.

```python
from dagster_dbt.cloud_v2.asset_decorator import dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace
from dagster_dbt.cloud_v2.sensor_builder import build_dbt_cloud_polling_sensor

creds = DbtCloudCredentials(account_id=..., access_url=..., token=...)
workspace = DbtCloudWorkspace(credentials=creds, project_id=..., environment_id=...)
polling_sensor = build_dbt_cloud_polling_sensor(workspace=workspace)


@dbt_cloud_assets(workspace=workspace)
def my_dbt_cloud_assets(context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace):
    yield from dbt_cloud.cli(args=["build"], context=context).wait()
```

`load_dbt_cloud_asset_specs(workspace=...)` gives observation-only specs; pair either with the polling sensor so externally triggered dbt Cloud runs reach the event log. `@dbt_cloud_assets` also takes `select`, `exclude`, `selector`, `name`, `group_name`, `dagster_dbt_translator`, `partitions_def`, `backfill_policy`; a `TimeWindowPartitionsDefinition` defaults to a single-run backfill policy. dbt Cloud allows one concurrent run per job, so raise `adhoc_job_pool_size` above 1 for concurrent partitioned backfills and set `adhoc_job_pool_mode` to `overflow` (default), `wait`, or `fail`.

## dlt

```bash
uv add dagster-dlt
dg scaffold defs dagster_dlt.DltLoadCollectionComponent github_snowflake_ingest \
  --source github --destination snowflake
```

Scaffolding writes `defs.yaml` plus a `loads.py` holding the dlt source and pipeline objects, which stay runnable under plain dlt. Each `loads` entry pairs a source with a pipeline by fully scoped Python identifier. Translation templates see both the `DltResource` (`resource`) and the `Pipeline` (`pipeline`).

```yaml
type: dagster_dlt.DltLoadCollectionComponent
attributes:
  loads:
    - source: .loads.dlthub_dlt_stargazers_source
      pipeline: .loads.dlthub_dlt_stargazers_pipeline
      translation:
        group_name: github_data
        metadata: { resource_name: '{{ resource.name }}', pipeline: '{{ pipeline.pipeline_name }}' }
```

Subclass and override `get_asset_spec(data)` or `execute(context, dlt_pipeline_resource)` for logic beyond Jinja. The Pythonic form:

```python
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData


class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        return super().get_asset_spec(data).replace_attributes(key=dg.AssetKey(data.resource.name))


@dlt_assets(
    dlt_source=github_reactions("dagster-io", "dagster", max_items=250),
    dlt_pipeline=dlt.pipeline(pipeline_name="github_issues", destination="snowflake"),
    name="github",
    group_name="github",
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def dagster_github_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
```

`@dlt_assets` also takes `partitions_def`, `backfill_policy`, `op_tags`, `pool`. `build_dlt_asset_specs(dlt_source=, dlt_pipeline=, dagster_dlt_translator=)` returns specs with no materialization body, which is how you attach metadata to the external upstream assets dlt generates, or reshape `deps`. Credentials come from dlt's own environment variables, not Dagster resources: `SOURCES__GITHUB__ACCESS_TOKEN`, `DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD`, and so on. Gotchas: dlt manages pipeline state itself, so partitioned dlt assets need a concurrency limit to avoid state corruption, and the partition key must be threaded back into the source (`dlt.run(context=context, dlt_source=my_source(color=context.partition_key))`). With the `sql_database` source, set `defer_table_reflect=True` so definition reloads stop querying the database.

## Sling

```bash
uv add dagster-sling duckdb
dg scaffold defs dagster_sling.SlingReplicationCollectionComponent sling_ingest
```

Scaffolding writes `defs.yaml` and an empty `replication.yaml` (`source: {}` / `streams: {}` / `target: {}`). Names under `connections` must match the `source` and `target` keys in the replication config; translation templates see `stream_definition`.

```yaml
# src/my_project/defs/sling_ingest/defs.yaml
type: dagster_sling.SlingReplicationCollectionComponent
attributes:
  connections:
    DUCKDB: { type: duckdb, instance: /tmp/my_project.duckdb }
  replications:
    - path: ./replication.yaml
      translation: { group_name: sling_data, description: '{{ stream_definition.name }}' }
```

```yaml
# src/my_project/defs/sling_ingest/replication.yaml
source: LOCAL
target: DUCKDB
defaults: { mode: full-refresh, object: '{stream_table}' }
streams:
  file://raw_customers.csv:
    object: 'main.raw_customers'
  public.users: # per-stream override, no translator needed
    meta: { dagster: { asset_key: 'mydb_users', deps: 'sourcedb_users' } }
```

```python
from dagster_sling import SlingConnectionResource, SlingResource, sling_assets

sling_resource = SlingResource(
    connections=[
        SlingConnectionResource(
            name="MY_POSTGRES", type="postgres", connection_string=dg.EnvVar("POSTGRES_URL")
        ),
        SlingConnectionResource(name="MY_SNOWFLAKE", type="snowflake", role="REPORTING"),
    ]
)


@sling_assets(replication_config="path/to/replication.yaml")
def my_assets(context, sling: SlingResource):
    yield from sling.replicate(context=context)
```

`replication_config` takes a path or an inline dict. `@sling_assets` also takes `dagster_sling_translator`, `name`, `partitions_def`, `backfill_policy`, `op_tags`, `pool`. `SlingConnectionResource` takes either `connection_string` or type-specific keyword arguments, and `sling.stream_raw_logs()` yields raw Sling log lines.

`DagsterSlingTranslator.get_asset_spec(stream_definition)` is the one current override point; `get_asset_key`, `get_deps_asset_key`, `get_description`, `get_group_name`, `get_metadata`, `get_tags`, `get_kinds`, and `get_auto_materialize_policy` are superseded by it. Default keys prepend the translator's `target_prefix`: stream `public.accounts` becomes `target_public_accounts` depending on `public_accounts`. Gotcha: `dagster-sling` ships the Sling Python CLI but not the Sling binary, which it downloads at runtime. Where that download is blocked, fetch the release tarball at build time and set `SLING_BINARY` to its path.

## Fivetran

Connectors must have synced at least once before they appear in Dagster.

```bash
uv add dagster-fivetran
dg scaffold defs dagster_fivetran.FivetranAccountComponent fivetran_ingest \
  --account-id test_account --api-key "{{ env.FIVETRAN_API_KEY }}" \
  --api-secret "{{ env.FIVETRAN_API_SECRET }}"
```

```yaml
type: dagster_fivetran.FivetranAccountComponent
attributes:
  workspace:
    account_id: test_account
    api_key: '{{ env.FIVETRAN_API_KEY }}'
    api_secret: '{{ env.FIVETRAN_API_SECRET }}'
  connector_selector: { by_name: [salesforce_warehouse_sync] }
  polling_sensor: true
  translation: { group_name: fivetran_data, description: 'Fivetran {{ props.name }}' }
```

`FivetranAccountComponent` is state-backed and caches workspace metadata; translation templates see `props`, a `FivetranConnectorTableProps`.

```python
from dagster_fivetran import (
    FivetranWorkspace,
    build_fivetran_assets_definitions,
    build_fivetran_polling_sensor,
)

workspace = FivetranWorkspace(
    account_id=dg.EnvVar("FIVETRAN_ACCOUNT_ID"),
    api_key=dg.EnvVar("FIVETRAN_API_KEY"),
    api_secret=dg.EnvVar("FIVETRAN_API_SECRET"),
)
all_fivetran_assets = build_fivetran_assets_definitions(workspace=workspace)
polling_sensor = build_fivetran_polling_sensor(workspace=workspace)
```

| API | Use |
| --- | --- |
| `load_fivetran_asset_specs(workspace, ...)` | Observation only: specs with no materialization body |
| `build_fivetran_assets_definitions(workspace, ...)` | Materializable definitions for every connector |
| `@fivetran_assets(connector_id=, workspace=, ...)` | One connector, custom Python around `fivetran.sync_and_poll(context=context)` |
| `build_fivetran_polling_sensor(workspace, ...)` | Emits materializations for syncs Fivetran ran on its own schedule |

All three asset APIs accept `dagster_fivetran_translator` and `connector_selector_fn` (`Callable[[FivetranConnector], bool]`). Fivetran assets have no upstream deps by default; set them by overriding `DagsterFivetranTranslator.get_asset_spec` and calling `replace_attributes(deps=[...])`. Chain `.fetch_column_metadata()` onto `sync_and_poll()` for column schema and lineage. `dagster-fivetran snapshot --python-module <pkg> --output-path snapshot.snap` captures a workspace snapshot so large deployments load without hitting the API.

Behavioral gotchas, all set on `FivetranWorkspace` or the component's `workspace` block:

- The first Dagster-triggered sync sets the connector's schedule to "manual", disabling Fivetran auto-scheduling. `disable_schedule_on_trigger=False` keeps both active; the polling sensor then deduplicates events. A quota-based reschedule raises `RetryRequested` by default; `retry_on_reschedule=False` keeps polling in the same step instead.
- Syncing any table in a connector materializes every Dagster asset for that connector. `FivetranSyncConfig(resync=True, resync_parameters={"schema": ["table"]})` passed to `sync_and_poll` performs a historical resync; omit `resync_parameters` to resync the whole connector. Both are expensive.

## Airbyte

One component covers Cloud and OSS. It is state-backed; translation templates see `props`, an `AirbyteConnectionTableProps`.

```bash
uv add dagster-airbyte
dg scaffold defs dagster_airbyte.AirbyteWorkspaceComponent airbyte_ingest \
  --workspace-id test_workspace --client-id "{{ env.AIRBYTE_CLIENT_ID }}" \
  --client-secret "{{ env.AIRBYTE_CLIENT_SECRET }}"
```

```yaml
type: dagster_airbyte.AirbyteWorkspaceComponent
attributes:
  workspace: # omit both base URLs for Airbyte Cloud
    rest_api_base_url: http://localhost:8000/api/public/v1
    configuration_api_base_url: http://localhost:8000/api/v1
    workspace_id: your-workspace-id
    client_id: '{{ env.AIRBYTE_CLIENT_ID }}'
    client_secret: '{{ env.AIRBYTE_CLIENT_SECRET }}'
  connection_selector: { by_name: [salesforce_to_snowflake] }
  translation: { group_name: airbyte_data, description: 'Airbyte {{ props.connection_name }}' }
```

```python
from dagster_airbyte import AirbyteCloudWorkspace, AirbyteWorkspace, build_airbyte_assets_definitions

oss = AirbyteWorkspace(  # OSS: both base URLs required, unlike Cloud
    rest_api_base_url="http://localhost:8000/api/public/v1",
    configuration_api_base_url="http://localhost:8000/api/v1",
    workspace_id=dg.EnvVar("AIRBYTE_WORKSPACE_ID"),
    client_id=dg.EnvVar("AIRBYTE_CLIENT_ID"),
    client_secret=dg.EnvVar("AIRBYTE_CLIENT_SECRET"),
)
all_airbyte_assets = build_airbyte_assets_definitions(workspace=oss)
```

`AirbyteWorkspace` supports OAuth client credentials, basic auth (`username`/`password`), or no auth. `load_airbyte_asset_specs(workspace, dagster_airbyte_translator=, connection_selector_fn=)` gives observation-only specs; `@airbyte_assets(connection_id=, workspace=, ...)` handles a single connection; `load_airbyte_cloud_asset_specs` is superseded by `load_airbyte_asset_specs`. The Airbyte resource APIs are marked beta.

## Cross-cutting patterns

- **Kinds tags are automatic.** Assets carry the tool plus the destination technology (`dbt` + `duckdb`, `airbyte` + `snowflake`); no manual `compute_kind`.
- **Chaining.** Downstream: `@dg.asset(deps=[dg.AssetKey(["customers"])])`, the integration's key helper when the key is derived, or `deps:` on another component's asset entry in YAML. Upstream: dbt reads edges from `sources.yml`; Fivetran, Airbyte, and dlt set deps through their translator's `get_asset_spec`; Sling also accepts `meta.dagster.deps`.
- **Scheduling.** Prefer automation conditions on the translated specs over per-integration job factories: set `automation_condition` under `translation` in YAML, or `map_asset_specs(...)` in Python, and run an `AutomationConditionSensorDefinition`. The `default_automation_condition_sensor` must be enabled for conditions to evaluate. `dg list defs` shows generated keys, groups, deps, and kinds before anything runs, and `dg check defs` catches duplicate definitions, the usual symptom of a half-finished migration from a Pythonic definition to a component.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `DagsterDbtManifestNotFoundError: .../target/manifest.json does not exist` | CI deployed without generating the manifest. Add `dbt deps && dbt parse` (or `dagster-dbt project prepare-and-package`) before the deploy step, or bake it into the image |
| `ValidationError ... DbtCliResource project_dir ... does not exist` from the Launchpad | Stale absolute path saved in Launchpad config after a redeploy. Delete the `resources` block (or just the `dbt` key) and relaunch; never store dbt paths there |
| Duplicate definition errors after adopting a component | The old `@dbt_assets` or `DbtCliResource` still exists. Delete them, then `dg check defs` |
| Microbatch model reprocesses all history | `--vars` passed instead of `--event-time-start` / `--event-time-end` |
| `AutomationCondition.code_version_changed()` never fires after a macro edit | dbt code versions derive from each model's `raw_code`; macros and ephemeral models are not in the asset graph. Materialize affected models manually or use a time-based trigger |
| Corrupted snapshot tables | Concurrent snapshot runs. Isolate snapshots into their own component and cap the pool at 1 |
| Snapshot SQL compilation errors everywhere after `dbt deps` | A transitive package changed a macro. Run `dbt clean && dbt deps`, then pin versions in `packages.yml` |
| Fivetran connector missing from the asset graph | It has never completed a sync |
| Sling binary download blocked | Pre-install the binary at build time and set `SLING_BINARY` |
| Run killed mid-`dbt build` by a node eviction | Retry with the `FROM_ASSET_FAILURE` strategy and `dagster/retry_on_asset_or_op_failure` set to `false`, so retries skip already-materialized assets without persisted dbt artifacts. See `operations-and-debugging.md` |

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| `load_assets_from_dbt_project` / `load_assets_from_dbt_manifest` | `DbtProjectComponent`, or `@dbt_assets` |
| `DagsterDbtTranslator.get_auto_materialize_policy` | `get_automation_condition` |
| `load_assets_from_dbt_cloud_job`, `dbt_cloud_resource`, `DbtCloudClientResource`, `dbt_cloud_run_op` | `DbtCloudComponent`, `DbtCloudWorkspace`, `@dbt_cloud_assets` |
| `dagster-cloud ci dagster-dbt project manage-state` | `dg plus integrations dbt manage-manifest` |
| `dagster-dbt project scaffold` for a new project; `dagster_embedded_elt.sling` / `.dlt` imports | `create-dagster project` + `dg scaffold defs ...`; `dagster_sling` / `dagster_dlt` (`dagster-embedded-elt` is deprecated) |
| Per-attribute getters on `DagsterSlingTranslator` (`get_asset_key`, `get_deps_asset_key`, `get_description`, `get_group_name`, `get_metadata`, `get_tags`, `get_kinds`) and `DagsterDltTranslator` (`get_asset_key`, `get_deps_asset_keys`, `get_automation_condition`, `get_auto_materialize_policy`) | `get_asset_spec(...)`, then read the attribute off the returned spec |
| `AirbyteResource`, `AirbyteCloudResource`, `load_assets_from_airbyte_instance`, `build_airbyte_assets`, `airbyte_sync_op` | `AirbyteWorkspace` / `AirbyteCloudWorkspace` + `build_airbyte_assets_definitions` |
| `load_airbyte_cloud_asset_specs`; `fivetran_resource`, `FivetranResource`, `load_assets_from_fivetran_instance` | `load_airbyte_asset_specs`; `FivetranWorkspace` + `build_fivetran_assets_definitions` (removal slated for 0.30) |

## Sources

- https://docs.dagster.io/integrations/libraries/dbt and .../dbt/dbt-pythonic — component walkthrough, translation, `DbtProject`, `DbtCliResource`, `@dbt_assets`, subclassing
- https://docs.dagster.io/integrations/libraries/dbt/reference — key/group/owner defaults, asset checks, metadata fetching, dependencies, deployment, defer
- https://docs.dagster.io/integrations/libraries/dbt/dbt-patterns and .../dbt/dbt-migration-components — snapshot concurrency, microbatch comparison, tagged tests, macro code versions, Pythonic-to-component migration
- https://docs.dagster.io/integrations/libraries/dbt/dbt-cloud — `DbtCloudComponent`, `DbtCloudWorkspace`, polling sensor, partitioned dbt Cloud assets
- https://docs.dagster.io/integrations/libraries/dbt/using-dbt-with-dagster-plus/dbt-selectors — selector limits and workarounds, cross-code-location deps, column lineage
- https://docs.dagster.io/integrations/libraries/dlt, .../dlt/dlt-pythonic, .../embedded-elt — `DltLoadCollectionComponent`, `@dlt_assets`, translator, secrets, partitions, `dagster-embedded-elt` deprecation
- https://docs.dagster.io/integrations/libraries/sling and .../sling/sling-pythonic — component, `replication.yaml`, `@sling_assets`, connections, Sling binary
- https://docs.dagster.io/integrations/libraries/fivetran and .../fivetran/fivetran-pythonic — component, workspace, factories, polling sensor, resync, snapshots
- https://docs.dagster.io/integrations/libraries/airbyte/airbyte-component, .../airbyte/airbyte-oss, .../airbyte/migration-guide — component, workspace URLs and auth, legacy migration
- https://docs.dagster.io/deployment/troubleshooting/dbtprojectcomponent-missing-manifest-json and .../dbt-project-pydantic-validation-errors — missing manifest in CI/CD, stale Launchpad `project_dir`
