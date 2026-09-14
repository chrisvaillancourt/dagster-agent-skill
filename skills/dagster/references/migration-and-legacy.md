# Migration and legacy patterns

Reading Dagster's API lifecycle markers, upgrading a deployment, modernizing an old codebase to the `dg`
components layout, and migrating into Dagster from Airflow or plain Python. Replacement APIs are named
here, not explained: see `components.md`, `project-setup-and-cli.md`, `declarative-automation.md`,
`pipes-external-execution.md`, `resources-and-config.md`, `integrations-dbt-and-elt.md`, `dagster-plus.md`.

## Contents

- [API lifecycle and compatibility](#api-lifecycle-and-compatibility)
- [Upgrading a Dagster deployment](#upgrading-a-dagster-deployment)
- [Old to new API map](#old-to-new-api-map)
- [Modernizing a legacy project](#modernizing-a-legacy-project)
- [Sensors to Declarative Automation](#sensors-to-declarative-automation)
- [Step launchers to Pipes](#step-launchers-to-pipes)
- [Airflow to Dagster with dagster-airlift](#airflow-to-dagster-with-dagster-airlift)
- [Airflow operator equivalents](#airflow-operator-equivalents)
- [Python scripts to Dagster](#python-scripts-to-dagster)
- [Dagster+ and region migrations](#dagster-and-region-migrations)
- [Detection greps](#detection-greps)
- [Sources](#sources)

## API lifecycle and compatibility

Use when an import emits a warning, or you must decide whether an API is safe to build on.

| Stage | Breaks when | Lifetime | Warning class |
| --- | --- | --- | --- |
| Preview | patch releases | until design complete or canceled | `dagster.PreviewWarning` |
| Beta | minor releases (behavior in patch) | at most two 1.x releases, then GA or back to preview | `dagster.BetaWarning` |
| Generally Available | minimal risk | until at least 2.0 | none |
| Superseded | no break; better alternative exists | until at least 2.0 | `dagster.SupersessionWarning` |
| Deprecated | will be removed | removed in a minor release; the warning names it | built-in `DeprecationWarning` |

Decision rule: Superseded means keep existing code and write new code the new way. Deprecated means
schedule a migration. Preview/Beta means do not depend on the signature.

```python
import warnings

from dagster import BetaWarning, PreviewWarning, SupersessionWarning

warnings.filterwarnings("ignore", category=BetaWarning)
warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=SupersessionWarning)
# DeprecationWarning is a Python built-in
warnings.filterwarnings("ignore", category=DeprecationWarning)
```

`dagster` core follows semver: public non-preview, non-beta APIs do not break within 1.x. Public
means top-level exports of `dagster` plus `@public` methods in the API reference. Integration
libraries sit on a separate pre-1.0 track (0.y.z), compatible with `dagster` 1.x, but a minor bump
there (0.29 to 0.30) may break.

## Upgrading a Dagster deployment

Use when moving between minor versions.

1. Read `https://docs.dagster.io/migration/upgrading` for the target version. It lists per-release
   breaking changes and deprecations with before/after code.
2. Bump core and let the resolver pick library versions. Do **not** pin `dagster-*` integration
   libraries to the core version number — they use the 0.y.z track, so `dagster-dbt==1.13.0` does
   not resolve. Install `dagster==<version>` plus the bare library name, or
   `uv lock --upgrade-package dagster --upgrade-package dagster-dbt`.
3. Migrate storage when the minor version changed. `dagster instance migrate` applies schema and
   data migrations; `dagster instance reindex` rebuilds indexes over historical runs. Some releases
   require it explicitly (1.9 added columns to `runs` and `bulk_actions` plus `backfill_tags`).
4. Restart services. Documented Kubernetes order: back up Postgres, `helm repo update`,
   `helm upgrade`, scale webserver and daemon deployments to 0 replicas, run `dagster instance
   migrate` as a Job (`--set migrate.enabled=true --show-only
   templates/job-instance-migrate.yaml`), verify the pod completed, scale both back to their
   recorded replica counts. Scaling down first is what stops runs writing mid-migration.
5. Dagster+ Hybrid: upgrade the agent too. See `dagster-plus.md`.

Gotchas from the upgrade guide:

- A `FreshnessDaemon` runs by default from 1.12. Disable with `freshness: {enabled: false}` in
  `dagster.yaml` if you do not use freshness policies.
- From 1.9, `AssetExecutionContext` is not a subclass of `OpExecutionContext`. Helpers annotated
  `OpExecutionContext` and `isinstance` checks against it break on asset code.
- From 1.8, conditions are evaluated by a per-code-location `default_automation_condition_sensor`,
  not one global pass; the `AssetDaemon` must still run. Old behavior:
  `auto_materialize: {use_sensors: true}`.
- From 1.8, public APIs return stdlib `datetime`, not Pendulum, so `.add()` / `.subtract()` fail.
  From 1.12, custom `Executor` subclasses must handle resource-init failures or runs hang.

## Old to new API map

Every row is supported by the docs or the upgrade guide. Removed means the symbol is gone; deprecated
means it warns and is scheduled for removal; superseded means it works indefinitely but is no longer
recommended.

### Project structure and CLI

| Legacy | Replacement | Notes |
| --- | --- | --- |
| `dagster` CLI | `dg` CLI | Whole CLI superseded; `dg` is the default for new projects. |
| `dagster dev -f/-m` | `dg dev` | `dg` locates the project via TOML config, not a file path. |
| `workspace.yaml` | `dg.toml` with `directory_type = "workspace"` and `[[workspace.projects]]` | `workspace.yaml` still loads; docs recommend migrating. Both found in cwd, or via `-w`. |
| `@repository` | `Definitions`, or `@dg.definitions` | One workspace may mix both during migration. |
| Manual `Definitions(assets=[...])` listing | `dg.load_from_defs_folder(path_within_project=Path(__file__).parent)` | Autoloads everything under `<root_module>/defs`. |
| `dagit` package and executable | `dagster-webserver` | Deprecated in 1.4, drop-in. Helm `dagit:` / `ingress.dagit` became `dagsterWebserver:` / `ingress.dagsterWebserver`. |
| `dagster-cloud` CLI | `dg plus` and `dg api` | Superseded. |

### Assets and automation

| Legacy | Replacement | Notes |
| --- | --- | --- |
| `SourceAsset`, `AssetsDefinition.to_source_assets()` | `AssetSpec` | Deprecated, removal in 2.0. For an I/O manager key: `AssetSpec(...).with_io_manager_key(...)`. |
| `external_assets_from_specs`, `external_asset_from_spec` | pass `AssetSpec`s straight to `Definitions(assets=[...])` | Removed in 1.13. |
| `non_argument_deps={...}` | `deps=[...]` | Deprecated since 1.4; set becomes a list, pass asset symbols. |
| `deps=AssetKey("x")` unwrapped | `deps=[dg.AssetDep("x")]` | Removed in 1.13. `AssetDep` carries partition mappings. |
| `AutoMaterializePolicy`, `AutoMaterializeRule`, `auto_materialize_policy=` | `AutomationCondition`, `automation_condition=` | Deprecated in 1.8, no longer in the core API reference. The two interoperate, so migrate incrementally. |
| `AutoMaterializeSensorDefinition` | `AutomationConditionSensorDefinition` | Renamed in 1.8; `asset_selection=` became `target=` in 1.9. |
| `@multi_asset_sensor`, `MultiAssetSensorDefinition` | Declarative Automation | Superseded; keep only for side effects or when a specific job must be targeted. |
| `build_asset_reconciliation_sensor` (removed 1.4), `@freshness_policy_sensor` (removed 1.9) | `AutomationCondition` on the asset; freshness policies | The latter went with the legacy `FreshnessPolicy` it depended on. |
| `AssetSelection.keys(...)` / `.sources(...)` | `AssetSelection.assets(...)` / `.roots(...)` | Deprecated, removal in 2.0. |
| `define_asset_job(partitions_def=...)` | omit it | Deprecated in 1.8; inferred from targeted assets. |
| `@observable_source_asset(auto_observe_interval_minutes=...)` | `automation_condition=` plus a schedule or sensor | Removed in 1.13. |

### Freshness

The freshness story moved twice. Read column by column before touching freshness code.

| Legacy | Replacement | Notes |
| --- | --- | --- |
| pre-1.11 `FreshnessPolicy` | `LegacyFreshnessPolicy` | Renamed in 1.11. `from dagster import FreshnessPolicy` now resolves to the *new* class; the old one is `from dagster.deprecated import FreshnessPolicy`. |
| `freshness_policy=` on `@asset`, `@asset_check`, `AssetOut`, `AssetsDefinition.from_graph/from_op`, `AssetSpec.replace_attributes/merge_attributes` | `legacy_freshness_policy=` (transitional), then new policies | Removed from `AssetOut`, `AssetsDefinition`, `@observable_source_asset` and the `load_assets_from_*` helpers in 1.13. |
| freshness checks (`build_last_update_freshness_checks`, `build_time_partition_freshness_checks`, `build_sensor_for_freshness_checks`) | attach `dg.FreshnessPolicy` objects to assets | Superseded. New policies are still under active development, so expect gaps. |
| `dagster_dbt.build_freshness_checks_from_dbt_assets` | override `get_asset_spec` in a `DagsterDbtTranslator`, or set `translation` on `DbtProjectComponent` | Superseded. |
| `from dagster.preview.freshness import ...` | `from dagster import FreshnessPolicy, apply_freshness_policy` | Moved to top level in 1.12. |

### Execution, resources, context

| Legacy | Replacement | Notes |
| --- | --- | --- |
| `StepLauncher`, `databricks_pyspark_step_launcher`, `emr_pyspark_step_launcher` | Dagster Pipes | Superseded and deprecated (1.9). No removal planned before 2.0, but no new development either. |
| `dagster_k8s.execute_k8s_job` | `dagster_k8s.PipesK8sClient` | Superseded. |
| `dagster-shell` (`create_shell_command_op`, `create_shell_script_op`) | `dg.PipesSubprocessClient` | Deprecated in 1.8. |
| `@resource` / `@io_manager` functions with `config_schema=` | `ConfigurableResource` / `ConfigurableIOManager` with typed attributes | Docs define resources by subclassing `ConfigurableResource`; `IOManagerFactory` is the escape hatch for stateful managers. |
| `fs_io_manager`; `ConfigurablePickledObject{S3,ADLS2,GCS}IOManager` | `FilesystemIOManager`; `S3PickleIOManager` / `ADLS2PickleIOManager` / `GCSPickleIOManager` | Superseded; the pickled-object ones are deprecated with removal in 2.0. |
| `context.asset_partition_key_for_output(...)` and siblings | `context.partition_key`, `.partition_keys`, `.partition_key_range`, `.partition_time_window` | Deprecated on both context classes. |
| `context.get_tag(k)` / `has_tag(k)`, `op_config`, `get_mapping_key`, `selected_output_names` in assets | `context.run.tags.get(k)` / `k in context.run.tags`; otherwise `context.op_execution_context.<same>` | Deprecated; prefer typed `Config` params. |
| `OpExecutionContext` annotation on asset helpers | `AssetExecutionContext` | Not a subclass since 1.9. |
| `os.getenv` for secrets in resources | `dg.EnvVar("NAME")` | `os.getenv` resolves at code-location load and **is** visible in the UI; `EnvVar` resolves at run launch and is not. |
| `@solid`, `@pipeline`, `@composite_solid`, `ModeDefinition`, `PresetDefinition`, `InputDefinition`, `OutputDefinition`, `execute_pipeline`, `dagster pipeline` CLI, `AssetGroup`, `build_assets_job` | `@op`, `@graph`, `@job`, `ins`/`outs`, `Config`, `execute_in_process` / `execute_job`, `dagster job`, `group_name=`, `define_asset_job` | All removed in 1.0. |

### Integrations

| Legacy | Replacement | Notes |
| --- | --- | --- |
| `dagster-embedded-elt` / `dagster_embedded_elt` | `dagster-sling` / `dagster-dlt` | Deprecated. Rewrite `dagster_embedded_elt.sling` to `dagster_sling`, `.dlt` to `dagster_dlt`. |
| `load_assets_from_dbt_project`, `load_assets_from_dbt_manifest`, `dbt_cli_resource`, `DbtCliClientResource`, `dbt_run_op` and friends | `@dbt_assets`, `DbtCliResource`, `DbtProject` | Removed in 1.8. |
| `key_prefix` / `node_info_to_*_fn` args on the dbt loaders, `KeyPrefixDagsterDbtTranslator` | a custom `DagsterDbtTranslator` overriding `get_asset_spec` | Loaders are gone; translators are the extension point. |
| `AirbyteResource`, `load_assets_from_airbyte_instance` | see `/integrations/libraries/airbyte/migration-guide` | Superseded for Airbyte 1.6.0+. |
| `fivetran_resource`, `FivetranResource`, `build_fivetran_assets`, `load_assets_from_fivetran_instance`, `fivetran_sync_op` | `FivetranWorkspace` + `@fivetran_assets` / `build_fivetran_assets_definitions` | Deprecated, removal in library 0.30. |
| `PowerBIWorkspace.build_defs()`, `SigmaOrganization.build_defs()`, `DagsterLookerResource.build_defs()` | `load_powerbi_asset_specs(...)` / `load_sigma_asset_specs(...)` / `load_looker_asset_specs(...)` into `Definitions` | Removed in 1.13. These loaders now take a translator **instance**, not a class. |
| Translator `get_asset_key` / `get_group_name` / `get_metadata` / `get_tags` / `get_deps_*` overrides (dbt, dlt, sling, Looker) | override `get_asset_spec(...)` and read fields off the returned spec | Superseded across libraries. |
| `dagster-github` (`GithubResource`, `GithubClient`) and the `dagster-aws` CloudWatch logger | write your own resource | Whole integrations marked deprecated. |
| `ComponentLoadContext.load_component_at_path` / `.build_defs_at_path`; `Definitions.get_all_asset_specs()` | `context.load_component(...)` / `context.build_defs(...)`; `Definitions.resolve_all_asset_specs()` | Removed in 1.13. |

## Modernizing a legacy project

Use when handed a repo that predates `dg`. Order matters: get `dg` recognizing the project before moving any code. Verify after each step with `dg list defs` and `dg check defs`.

1. Run the [detection greps](#detection-greps) to size the job.
2. Create and activate a venv in the project root (`uv venv`, `source .venv/bin/activate`).
3. `uv add dagster-dg-cli`.
4. Add `dg` config. With `pyproject.toml`, append the two sections; with `setup.py` only, create a
   `dg.toml` holding the same keys at top level and under `[project]`.

   ```toml
   # pyproject.toml
   [tool.dg]
   directory_type = "project"

   [tool.dg.project]
   root_module = "my_existing_project"
   code_location_target_module = "my_existing_project.definitions"
   code_location_name = "my_existing_project"   # the name workspace.yaml / [tool.dagster] gave the location
   ```

   `directory_type` and `root_module` are required. `code_location_target_module` defaults to `<root_module>.definitions`; set it only if the `Definitions` object lives elsewhere.
   Pin `code_location_name` to the existing name: schedule and sensor on/off state and run history are keyed by it, and `dagster definitions validate` warns when the name changes.
5. `dg list defs` must now print your existing assets. If it does not, the config is wrong. Save this baseline before touching any code (`dg list defs --json > /tmp/before.json`) and diff it against the same command at the end: keys, deps, groups, automation labels, jobs and schedule crons must match exactly.
   Default statuses are not in that output, so also record `defs.resolve_schedule_def("all_job_schedule").default_status` and `defs.resolve_sensor_def("default_automation_condition_sensor").default_status` (that sensor exists whenever any asset has an `automation_condition`).
   Keep derived names intact: a `ScheduleDefinition` without `name=` is called `<job_name>_schedule`, and renaming it resets its on/off state.
6. `mkdir my_existing_project/components && touch my_existing_project/components/__init__.py`.
7. Declare the entry point so `dg` sees custom component types. With `setup.py`, put this in a new
   `setup.cfg` instead; `setup.py` cannot be read reliably.

   ```toml
   # pyproject.toml
   [project.entry-points]
   "dagster_dg_cli.registry_modules" = { my_existing_project = "my_existing_project.components" }
   ```
8. `uv pip install --editable .`. Mandatory: `uv sync` does **not** reinstall, and entry points are
   registered only at install time. `dg list registry-modules` should then list the new module.
9. `mkdir my_existing_project/defs`, then merge autoloading into the existing `Definitions`:

   ```python
   # my_existing_project/definitions.py
   from pathlib import Path

   from my_existing_project.assets import my_asset

   import dagster as dg

   defs = dg.Definitions.merge(
       dg.Definitions(assets=[my_asset]),
       dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
   )
   ```
10. Move one legacy module at a time: `mv my_existing_project/elt/* my_existing_project/defs/elt`,
    then drop that module's imports and entries from the explicit `Definitions(...)` call. Re-run
    `dg list defs` after each move.
11. Repeat until `definitions.py` is one line:
    `defs = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)`.
12. Delete `workspace.yaml`, add a workspace `dg.toml` (`directory_type = "workspace"` plus a
    `[[workspace.projects]]` entry per project), then run `dg dev`.
13. Work through the [old to new map](#old-to-new-api-map) for the symbols the greps found, and
    convert modules to components where one exists (see `components.md`).

Escape hatch: to use a single component without restructuring, keep your `Definitions` and merge in `dg.build_defs_for_component(component=SomeComponent(...))`. No `defs/` folder, no entry point.

## Sensors to Declarative Automation

Use when a sensor exists only to launch downstream work after upstream assets materialize. Condition language is in `declarative-automation.md`.

| Sensor pattern | Condition |
| --- | --- |
| `@asset_sensor` on one upstream asset | `dg.AutomationCondition.eager()` |
| multi-asset sensor waiting for **all** monitored assets | `dg.AutomationCondition.on_cron("0 * * * *")` — **not** `eager()` |
| multi-asset sensor firing on **any** monitored asset | `dg.AutomationCondition.eager()` |
| sensor that ignores some upstreams | `eager().ignore(dg.AssetSelection.keys("static_config"))`, or `.allow(...)` |

Gotchas:

- `eager()` fires when *any* dependency updates. The cron tick in `on_cron` is the synchronization
  barrier that makes "all deps updated" safe; a hand-rolled `asset_matches(...).since(...)`
  condition desynchronizes permanently after one mid-cycle backfill.
- Keep the sensor when it performs side effects, needs materialization metadata to decide, builds
  per-event `run_config`, or must count events. Conditions are boolean per tick, not counters.
- Metadata-gated triggers become an `@asset_check` plus a condition that holds while upstream checks
  are failing. Per-run config moves into the asset body, or becomes fixed `run_tags` on the sensor.
- A sensor polling an external system splits in two: model the tables as `AssetSpec`s, keep a sensor
  that only *reports* materializations, let downstream assets use conditions.
- Conditions are evaluated by an `AutomationConditionSensorDefinition`. A default one exists per code
  location and must be toggled on in the UI under **Automation**.

## Step launchers to Pipes

Use when the codebase contains `databricks_pyspark_step_launcher`, `emr_pyspark_step_launcher`, or a `StepLauncher` subclass.

1. Build and ship the Spark runtime yourself in CI. In the PySpark job's dependencies, drop
   `dagster` and add `dagster-pipes`.
2. Split each step-launcher asset into a Dagster asset plus an external script. The asset invokes a
   Pipes client (`PipesEMRClient`, `PipesDatabricksClient`, `PipesK8sClient`).
3. Pipes jobs cannot use I/O managers or resources. Return a path from the asset so the I/O manager
   stores it, or pass values in through Pipes `extras` and read them with `PipesContext`. Metadata
   logged from the script is readable from upstream materializations.

State the trade-off first: Dagster stops managing the Spark runtime, and resources and I/O managers no
longer reach the compute. If the pipeline leans on those, staying put is supported — step launchers are superseded, not scheduled for removal.

## Airflow to Dagster with dagster-airlift

Two routes; pick by DAG count.

**Small migration: rewrite.** Each `@task` becomes a `@dg.asset`; XCom becomes `deps` plus a storage
layer or I/O manager; `default_args` retries become `dg.RetryPolicy` on the asset; DAG tags become
`group_name` / `tags`; `@dag(schedule=...)` becomes `@dg.schedule` over a `define_asset_job`.
`catchup=True` has no equivalent — trigger backfills explicitly.

**Larger migration: `dagster-airlift`.** Four phases, each independently valuable and reversible.

| Phase | What happens | Airflow code changes |
| --- | --- | --- |
| Peer | Each DAG appears in Dagster as an asset; a sensor polls the REST API and records a materialization per DAG run. | none |
| Observe | Map DAGs or tasks to real `AssetSpec`s so Dagster shows true lineage; the same sensor materializes them. | none |
| Migrate | Flip tasks (or a whole DAG) to `proxied: true`; Airflow still schedules, Dagster executes. | one call at the end of the DAG file |
| Decommission | Delete the DAG, drop the mappings, attach the assets to a `ScheduleDefinition`. | DAG removed |

Recommended entry point for new work is the component, not the raw functions:

```bash
dg scaffold defs dagster_airlift.core.components.AirflowInstanceComponent airflow \
  --name my_airflow --auth-type basic_auth
```

```yaml
# src/my_project/defs/airflow/defs.yaml
type: dagster_airlift.core.components.AirflowInstanceComponent

attributes:
  name: my_airflow
  auth:
    type: basic_auth
    webserver_url: '{{ env("AIRFLOW_WEBSERVER_URL") }}'
    username: '{{ env("AIRFLOW_USERNAME") }}'
    password: '{{ env("AIRFLOW_PASSWORD") }}'
  mappings:
    - dag_id: upload_source_data
      task_mappings:
        - task_id: upload_orders
          assets:
            - spec: {key: order_data}
        - task_id: aggregate_user_data
          assets:
            - spec: {key: aggregated_user_data, deps: [order_data]}
```

Swap `task_mappings:` for a flat `assets:` list under the `dag_id` to map at DAG level. The component supports basic auth only, and creates a sensor named `<instance>__airflow_monitoring_job_sensor`.

The Python API (`dagster-airlift[core]`, beta) is what you need for MWAA, multiple instances, or
custom event handling:

```python
from dagster_airlift.core import (
    AirflowBasicAuthBackend,
    AirflowInstance,
    assets_with_task_mappings,
    build_defs_from_airflow_instance,
)

import dagster as dg

defs = build_defs_from_airflow_instance(
    airflow_instance=AirflowInstance(
        auth_backend=AirflowBasicAuthBackend(
            webserver_url="http://localhost:8080", username="admin", password="admin"
        ),
        name="warehouse",
    ),
    defs=dg.Definitions(
        assets=assets_with_task_mappings(
            dag_id="rebuild_customers_list",
            task_mappings={"load_raw_customers": [dg.AssetSpec("raw_customers")]},
        )
    ),
)
```

Other symbols: `dagster_airlift.mwaa.MwaaSessionAuthBackend` (`dagster-airlift[mwaa]`, boto3 MWAA
session plus `env_name`); `build_defs_from_airflow_instance` also takes
`sensor_minimum_interval_seconds`, `event_transformer_fn`, `dag_selector_fn`,
`source_code_retrieval_enabled`, `default_sensor_status`; `assets_with_dag_mappings`;
`assets_with_multiple_task_mappings` when one asset is produced by tasks in several DAGs;
`load_airflow_dag_asset_specs` for federation-style observation without the defs builder.

Proxying, on the Airflow side (`dagster-airlift[in-airflow]`):

```python
# airflow_dags/my_dag.py — at the very end of the file
from pathlib import Path

from dagster_airlift.in_airflow import proxying_to_dagster
from dagster_airlift.in_airflow.proxied_state import load_proxied_state_from_yaml

proxying_to_dagster(
    proxied_state=load_proxied_state_from_yaml(Path(__file__).parent / "proxied_state"),
    global_vars=globals(),
)
```

`proxied_state/` holds one YAML per DAG named `<dag_id>.yaml`; scaffold with `AIRFLOW_HOME=...
dagster-airlift proxy scaffold`. Task-level form is a `tasks:` list of `{id, proxied}`; DAG-level
form is a single `proxied: true`. That file is the source of truth, so rolling back is a one-line
edit. Proxied tasks become `DefaultProxyTaskToDagsterOperator` (DAG-level:
`DefaultProxyDAGToDagsterOperator`), calling Dagster's GraphQL API. For Dagster+ or custom auth,
subclass `BaseProxyTaskToDagsterOperator` / `BaseProxyDAGToDagsterOperator` implementing
`get_dagster_session`, `get_dagster_url` and `build_from_task` / `build_from_dag`, then pass
`build_from_task_fn` / `build_from_dag_fn`.

Choose DAG-level mapping when the DAG uses dynamic task mapping, or when you intend to restructure
rather than mirror the task graph; it collapses the Airflow graph into one task. For federation, call
`build_defs_from_airflow_instance` per instance, combine with `dg.Definitions.merge`, add a
cross-instance dependency with `AssetSpec.replace_attributes`, make the downstream DAG asset
executable with a `@dg.multi_asset` calling `airflow_instance.trigger_dag(...)` then
`wait_for_run_completion(...)`, and drive it with `AutomationCondition.eager()`.

Airlift gotchas:

- Airflow's REST API is required: stable from Airflow 2.0, MWAA needs 2.4.3+.
- Airflow state is cached in the Dagster database, so adding, removing or restructuring a DAG
  surfaces as missing materializations or a failing sensor until you reload the code location
  (**Deployment** then **Redeploy**, or via the GraphQL client from CI).
- The polling sensor must stay on; default tick is 30s, `minimum_interval_seconds` can go to 1s.
- Partitioned assets must be time-window partitioned, and each Airflow `logical_date` must correspond
  exactly to one Dagster partition.
- Keep the Airflow and Dagster deployments in separate packages, and add asset checks during the peer
  phase — they act as acceptance tests and outlive the DAG.

## Airflow operator equivalents

| Airflow | Dagster |
| --- | --- |
| `BashOperator` (general) | `@dg.asset` invoking `dg.PipesSubprocessClient` |
| `BashOperator` running dbt | `@dbt_assets` + `DbtCliResource` |
| `PythonOperator` | `@dg.asset` or `@dg.multi_asset` calling a shared package function |
| `KubernetesPodOperator` | `dagster_k8s.PipesK8sClient` |
| `ExternalTaskSensor`, `FileSensor`, `HttpSensor` | `@dg.sensor` yielding `RunRequest` |
| XCom | `deps` for ordering, plus storage or an I/O manager for data |
| Connections, Variables | resources (`ConfigurableResource`) |
| `default_args={"retries": ...}` | `dg.RetryPolicy` on the asset |
| DAG tags | `group_name`, `tags` |
| `schedule="0 9 * * *"` on `@dag` | `@dg.schedule` over `define_asset_job` |
| AWS CLI / S3 sync in bash | `dagster_aws` resources such as `S3Resource` |

For `PythonOperator`, factor the callable into a third package installed by both the Airflow and Dagster
environments, import it from both sides, and only then proxy. That keeps dependency resolution tractable during the overlap.

`KubernetesPodOperator` parameters map onto `PipesK8sClient` as `in_cluster` to
`load_incluster_config`, `cluster_context` to `kube_context`, `config_file` to `kubeconfig_file`.
Everything else goes through `base_pod_spec`, its nested `containers` key, or `base_pod_meta`; the
operator-migration doc has the full field-by-field list.

## Python scripts to Dagster

Use for cron-scheduled scripts. The script does not need rewriting: wrap it in an asset that shells
out with `subprocess`, capture stdout and the return code as asset metadata, and move command-line
arguments into a `dg.Config` subclass supplied through `RunConfig`.

```python
import dagster as dg


class ScriptConfig(dg.Config):
    script_path: str
    extra_args: list[str] = []


job = dg.define_asset_job(
    "my_script_job",
    selection=[run_python_script],
    config=dg.RunConfig(
        ops={"run_python_script": ScriptConfig(script_path="/opt/scripts/extract_data.py")}
    ),
)
```

The crontab entry becomes a `@dg.schedule`; a `--date` argument becomes a daily partition; the
subprocess inherits the environment, so env-var secrets keep working. An asset that requires config
cannot be materialized with one click — supply config in the launchpad, or pin it in a job. General
sequencing for any source system: run both schedulers in parallel, migrate leaf pipelines first,
bridge handoffs with a `@dg.sensor`, disable the old schedule last.

## Dagster+ and region migrations

Pointers only; details in `dagster-plus.md`.

- **OSS to Dagster+**: create the organization, pick Hybrid or Serverless, repoint CI/CD at the
  Dagster+ deploy flow, then either start metadata fresh at cutover or import history with the
  `oss-metadata-to-plus` example in the Dagster repo.
- **Serverless to Hybrid**: in **Deployment** then **Agents**, use **Switch to Hybrid**, then stand
  up your own agent. History and metadata survive, but there is downtime with no agent attached.
- **US to EU**: separate control planes, so this is a new organization plus re-configured agents,
  CI/CD and SSO. Pipeline code is unchanged. Run history, schedules and sensor state do **not**
  transfer; asset materialization metadata can be moved manually.

## Detection greps

Run at the repo root before planning the work.

```bash
grep -rn "@repository\|RepositoryDefinition\|AssetGroup\|@solid\|@pipeline\|ModeDefinition" --include="*.py" .
grep -rn "SourceAsset(\|to_source_assets\|external_assets_from_specs\|non_argument_deps" --include="*.py" .
grep -rn "AutoMaterializePolicy\|AutoMaterializeRule\|auto_materialize_policy" --include="*.py" .
grep -rn "multi_asset_sensor\|build_asset_reconciliation_sensor\|freshness_policy_sensor" --include="*.py" .
grep -rn "FreshnessPolicy(\|freshness_policy=\|build_last_update_freshness_checks" --include="*.py" .
grep -rn "_step_launcher\|StepLauncher\|execute_k8s_job\|fs_io_manager" --include="*.py" .
grep -rn "@io_manager\|@resource\b\|config_schema=\|context.op_config\|OpExecutionContext" --include="*.py" .
grep -rn "load_assets_from_dbt\|dbt_cli_resource\|DbtCliClientResource" --include="*.py" .
grep -rn "dagster_embedded_elt\|load_assets_from_airbyte_instance\|fivetran_resource" --include="*.py" .
find . -name workspace.yaml -o -name setup.py
grep -rn "dagit\|dagster-cloud " Dockerfile* *.yaml .github/workflows/*.yml 2>/dev/null
```

Confirm the target state: `dg check defs` passes and `dg list defs` shows every definition the old `Definitions` object exposed.

## Sources

- https://docs.dagster.io/api/api-lifecycle and /api/api-lifecycle/filtering-api-lifecycle-warnings — stages, guarantees, warning classes
- https://docs.dagster.io/about/releases — semver promise and library versioning track
- https://docs.dagster.io/about/changelog — per-release changelog
- https://docs.dagster.io/migration/upgrading — per-version breaking changes and deprecations
- https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/migrating-while-upgrading — instance migration procedure
- https://docs.dagster.io/guides/build/projects/moving-to-components — converting a project to `dg`, moving definitions into `defs/`, `build_defs_for_component`
- https://docs.dagster.io/guides/build/projects/workspaces/dg-toml and /workspace-yaml — workspace config, old and new
- https://docs.dagster.io/guides/automate/declarative-automation/migrating-from-sensors — sensor to condition mappings
- https://docs.dagster.io/integrations/external-pipelines/migrating-from-step-launchers-to-pipes — step launcher migration
- https://docs.dagster.io/migration/migrating-to-dagster and /migration/python-to-dagster — incremental strategy, wrapping cron scripts
- https://docs.dagster.io/migration/airflow-to-dagster/basic-migration — manual DAG rewrite
- https://docs.dagster.io/migration/airflow-to-dagster/airflow-component-tutorial — `AirflowInstanceComponent`
- https://docs.dagster.io/migration/airflow-to-dagster/airlift-v1 — peer, observe, migrate, decommission, plus task-level, DAG-level, federation and migration-reference subguides
- https://docs.dagster.io/migration/airflow-to-dagster/airflow-operator-migration — operator equivalents
- https://docs.dagster.io/integrations/libraries/airlift — REST API compatibility matrix
- https://docs.dagster.io/integrations/libraries/embedded-elt — `dagster-embedded-elt` replacement
- https://docs.dagster.io/migration/oss-to-dagster-plus, /migration/serverless-to-hybrid, /migration/dagster-plus-us-to-eu — Dagster+ moves
- https://docs.dagster.io/guides/operate/configuration/using-environment-variables-and-secrets — `EnvVar` vs `os.getenv`
