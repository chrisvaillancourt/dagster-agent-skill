### Q1
`dg check yaml` (with `--validate-requirements`, which is on by default) flags `API_TOKEN` as an env var that is used in the template but not declared under `requirements: env: [API_TOKEN]` in defs.yaml, and it fails. It does not check whether the variable is actually set. `dg check defs` still loads the definitions. The template resolves to the unset value, so the description becomes `"token="` or `"token=None"`; I lean towards `"token=None"` (moderate confidence). Defaults with `{{ env('API_TOKEN', 'x') }}` avoid this.

### Q2
When a module has a `@dg.definitions` function, that function's returned `Definitions` is used for the module. Loose module-level `@dg.asset` objects in the same module are ignored, so only the assets from the `Definitions` object appear. I believe the loose asset is dropped silently with no warning, but I'm only about 50% sure; some versions may raise or warn. Put the loose asset inside the returned `Definitions` to be safe.

### Q3
The default `operation` is `merge`, so you get kinds `{a, b}` and tags `{x: '1', y: '2'}`. Use `operation: replace` to overwrite instead. `dg check yaml` validates `post_processing` against its schema (`target`, `operation`, and the `attributes` fields), so typos in those keys are caught. Selection strings in `target` are only checked when the defs load. Fairly confident.

### Q4
The default is a pydantic model instance, which is unhashable. `@dataclass` rejects it as a mutable default with `ValueError: mutable default ... use default_factory` at class-definition time. That breaks import of your project, so every dg command that loads it errors. Fix: `defs_state: ResolvedDefsStateConfig = field(default_factory=DefsStateConfigArgs.local_filesystem)`. Also make sure the class is Resolvable/Model-compatible as the base expects. High confidence on the cause.

### Q5
Yes. Entry points are read from installed package metadata, so you must reinstall after changing them. Run `uv sync` (which reinstalls the editable project) or `uv pip install -e .`. `[tool.dg.project].registry_modules` keeps working and is combined with the entry-point modules. It is the recommended no-reinstall mechanism for in-project components, so for local components you can drop the entry point and just use `registry_modules`.

### Q6
`DAGSTER_IS_DEV_CLI` is set by the dev and validation entry points so that libraries like dagster-dbt run dev-only behaviour (for example `prepare_if_dev()`, which parses the manifest at load). Production code servers don't set it, so your branch diverges. It is set by `dg dev` / `dagster dev` and by `dg check defs` (via `dagster definitions validate`). Moderate confidence that `dg list defs` also sets it. Not by deployed code servers or `dg launch`. Don't branch business logic on it.

### Q7
For specs belonging to executable `AssetsDefinition`s, changing `partitions_def` via `map_asset_specs` raises a `DagsterInvalidDefinitionError` (cannot change partitions_def on an existing assets definition). It only works for plain external `AssetSpec`s. Set `partitions_def` when the asset is built instead, for example via component attributes or `post_processing`. About 65% confident on the exact 1.13.28 behaviour.

### Q8
`dg launch` executes the run synchronously in the local process using the project's environment, against the instance resolved from `DAGSTER_HOME`. If `DAGSTER_HOME` is unset, it uses an ephemeral instance. The run only appears in your `dg dev` UI if both processes point at the same persistent `DAGSTER_HOME`. By default `dg dev` uses its own temp home, so you usually won't see it.

### Q9
No. A `--partition-range` launch is a single run over a partition range, so every selected asset must have `backfill_policy=dg.BackfillPolicy.single_run()`. Otherwise it errors. With the default multi-run policy, launch one partition at a time with `--partition`, or do a backfill from the UI. Fairly confident.

### Q10
`context.partition_key` raises because the run covers a range, not a single key. Use `context.partition_key_range`, `context.partition_time_window`, or `context.partition_keys` and process the whole range. Also, the default UPath-based I/O manager expects a multi-partition output as a `dict[partition_key, obj]`, so either return that or use an I/O manager that writes the time window itself, as warehouse I/O managers do.

### Q11
It runs `2024-05-19`, the most recently completed daily partition. Things that change the answer:
- `end_offset` on the partitions def (`end_offset=1` → `2024-05-20`).
- The partitions def's `timezone`, which the schedule inherits.
- The partitions def's own `hour_offset`/`minute_offset`, since 01:30 must fall after the partition boundary.
- `day_offset` on the schedule builder for coarser partitions.

### Q12
The run launches; Dagster doesn't gate on upstream existence. It then fails at load time: `UPathIOManager.load_input` raises because 21 partition files are missing. Add `ins={"up": dg.AssetIn(metadata={"allow_missing_partitions": True})}` and the dict will contain only the 3 existing hourly keys.

### Q13
- `context.definition_metadata` is the metadata on the input itself (`AssetIn(metadata=...)`).
- `context.upstream_output.definition_metadata` is the upstream asset's definition metadata, which is where your table name lives.
- `context.upstream_output.output_metadata` is not the runtime `MaterializeResult` metadata in `load_input`; it is empty or unavailable there.

For runtime values, query the instance, for example `context.instance.get_latest_materialization_event(context.asset_key).asset_materialization.metadata`. Fairly confident.

### Q14
Not reliably. A plain `return None` is generally treated as an output value of `None`, so the I/O manager stores it and a materialization is recorded. To skip, write the function as a generator: `if nothing_new: return`, otherwise `yield dg.Output(value)` or `yield dg.MaterializeResult(...)`. Then no output and no materialization are emitted, and downstream assets skip. About 70% confident.

### Q15
Weekly partitions start on Sunday by default (`day_offset=0`), so the first key is `2024-05-05`, and May 1–4 are not covered. To cover May 1 onward, either:
- Use `day_offset=3` (Wednesday) so the first key is `2024-05-01`, or
- Set `start_date="2024-04-28"` so the week starting April 28 includes May 1–4.

### Q16
Pass the path relative to the `defs` folder: `dg scaffold defs dagster.asset sales/assets.py`. That creates `src/my_proj/defs/sales/assets.py`. If you pass `src/my_proj/defs/sales/assets.py`, dg still treats it as relative to `defs/`, so you get a nested `src/my_proj/defs/src/my_proj/defs/sales/assets.py`. Confidence: high.

### Q17
No. `dg launch` executes the run locally in-process, like `dagster job execute`. It does not submit to the Dagster+ Hybrid agent or run launcher. It blocks until the run finishes instead of returning immediately. To run on the deployment, launch the run from the Dagster+ UI, API or GraphQL, or use a schedule or sensor. Confidence: medium-high.

### Q18
`dg scaffold defs dagster.job jobs.py`. The path is relative to `src/myproj/defs/`. Confidence: high.

### Q19
Use the tick's scheduled time, not wall-clock time:

```python
with dg.partition_loading_context(effective_dt=context.scheduled_execution_time):
    key = partitions_def.get_last_partition_key()
```

The simpler option is `dg.build_schedule_from_partitioned_job(job)`, which does this for you.

Don't pass:
- `datetime.now()`, or today's date string. Today's daily partition doesn't exist until the day ends, so the latest key at a midnight tick is yesterday.
- The deprecated `current_time=` and `dynamic_partitions_store=` kwargs on partition methods. The context manager replaces them.

Confidence: medium.

### Q20
No. Once `monitored_jobs` is set, the sensor watches only what you list, and its own location is no longer implied. Add `dg.CodeLocationSelector(location_name="alerts")` to the list as well. Alternatively, use `monitor_all_code_locations=True` to watch everything. Confidence: high.

### Q21
Use `AutomationCondition.on_cron("...")`. Its "all deps updated since the last cron tick" clause makes it wait until every upstream has updated in that cycle. You can also build it yourself with `all_deps_updated_since_cron`.

The common wrong choice is `AutomationCondition.eager()`, which fires as soon as any one dep updates. That means up to three materializations per cycle. Confidence: high.

### Q22
Arbitrary Python condition subclasses aren't serializable, so the daemon can't evaluate them. Evaluate them in the code server instead:

```python
dg.AutomationConditionSensorDefinition("my_sensor", target=..., use_user_code_server=True)
```

`evaluate(self, context)` must return a `dg.AutomationResult(context=context, true_subset=...)`. Also give the condition a `description`.

Limits:
- It's experimental.
- It runs only under a sensor with `use_user_code_server=True`, not under the default daemon sensor.
- Evaluation is slower and costs code-server load.
- Cursor and state support is limited compared to built-ins.

Confidence: medium.

### Q23
Probably zero. `eager()` includes `in_latest_time_window()`, so only the most recent partition is ever eligible. It also triggers on `newly_missing()`, not on partitions that are already missing, so at most it requests the latest one, and only when that partition becomes missing.

There are two ways to cover the older partitions:
- **One-time fix:** run a backfill.
- **Ongoing automation:** use a custom condition such as `AutomationCondition.in_latest_time_window(timedelta(days=30)) & AutomationCondition.missing() & ~AutomationCondition.in_progress()`, or swap the window with `eager().replace(...)`. Pair it with a `BackfillPolicy` if you want batched runs.

Confidence: medium-high.

### Q24
Without an explicit `out=`, Dagster infers a single output, `result`, typed as the whole tuple. A tuple of `Output` objects then doesn't match that type, so the type check fails.

Declare the outputs explicitly with `@dg.op(out={"num": dg.Out(int), "s": dg.Out(str)})`. Then either `return 1, "a"`, or yield/return `dg.Output(1, output_name="num")` and `dg.Output("a", output_name="s")`. Confidence: medium.

### Q25
- **`Failure(..., allow_retries=False)`:** the body runs once. The retry policy is bypassed.
- **Plain `Failure`:** `allow_retries` defaults to True, so it runs 1 + 3 = 4 times.

Confidence: high.

### Q26
No, `dg check defs` passes. `EnvVar` is resolved lazily, when the resource is initialized at execution time. The first failure appears when a run that uses the resource starts. The resource-init step fails with an error saying the environment variable `DB_PASSWORD` is not set. In Dagster+ or a deployed code server, what matters is the env of the run worker, not your shell.

An empty string counts as set, so `password=""` passes through silently and you get an auth failure at connect time. Confidence: medium on the empty-string behavior.

### Q27
No. `setup_for_execution` and `teardown_after_execution` are only invoked by `ConfigurableResource`'s own resource-definition path, which Dagster uses when you pass the instance directly. A raw `ResourceDefinition(resource_fn=lambda ...)` just returns the object.

For cleanup, Dagster honors these and nothing else:
- On a `ConfigurableResource` passed directly: `yield_for_execution` (a context-manager override).
- With a raw `ResourceDefinition`: a `resource_fn` that is a generator (code after `yield`) or that returns a context manager (`__enter__`/`__exit__`).

It never calls `close()`, `dispose()` or `__del__`. Confidence: medium.

### Q28
There are two problems:
- `materialize_to_memory` forbids supplying resources for IO-manager keys and raises a `DagsterInvariantViolationError`, because it forces an in-memory IO manager.
- `up` isn't in the selection, so loading `down`'s input fails. Nothing materialized `up` in memory.

To fix it, either:
- include `up` with `materialize_to_memory([up, down])`,
- call `down(up_value)` directly, or
- use `dg.materialize([down], resources=...)` with a stubbed IO manager that serves `up`.

With `deps=[up]` there is no data dependency, so `materialize_to_memory([down])` works on its own, with no IO manager needed. Confidence: medium-high.

### Q29
**Blocking ERROR check:** the blocking check with the default ERROR severity fails its step. Downstream assets of `orders` in the same run are skipped and the run fails.

**WARN check:** a failing check with `severity=WARN` is recorded as a failed check but does not block, even if it is marked `blocking=True`. Only ERROR blocks.

**`dg.materialize(...)`:** with the default `raise_on_error=True`, it raises (`DagsterExecutionStepExecutionError` / run failure) because of the blocking failure. With `raise_on_error=False`, you get a result where `result.success` is False and the downstream step was skipped. Confidence: medium.

### Q30
`AssetSelection.assets(...)` includes the asset's checks by default, so exclude them and target the checks separately:

```python
asset_job = dg.define_asset_job("j", dg.AssetSelection.assets(orders).without_checks())
checks_job = dg.define_asset_job("orders_checks", dg.AssetSelection.checks_for_assets(orders))
checks_schedule = dg.ScheduleDefinition(job=checks_job, cron_schedule="0 * * * *")
```

Confidence: high.

### Q31
Dagster 1.11+ replaced the class: the old `FreshnessPolicy` (with `maximum_lag_minutes`) is now `LegacyFreshnessPolicy`, and `FreshnessPolicy` is the new API with `.time_window(fail_window=..., warn_window=...)` and `.cron(...)`. You don't need a sensor or schedule, because the daemon evaluates freshness. In OSS you must enable it in `dagster.yaml` with `freshness: {enabled: true}` and run `dagster-daemon`. Freshness only reports PASS/WARN/FAIL state and does not trigger materializations. *(Medium confidence on the exact yaml key.)*

### Q32
Subclass `dg.ConfigurableResourceFactory[GitHub]`, declare `token: str` (or `dg.EnvVar`) as a field, and return `GitHub(self.token)` from `create_resource(self, context)`. In the asset, annotate the parameter as `github: dg.ResourceParam[GitHub]` so it receives the built client rather than the factory. *(High confidence on the factory and `ResourceParam`; I'm not confident `dg.Resource[T]` is a distinct public annotation, so use `ResourceParam`.)*

### Q33
Write the enum as the member **name** (e.g. `color: RED`), not its value. A discriminated union becomes a selector keyed by the discriminator value: `pet: {cat: {meows: 3}}`, with no `pet_type` key. Config is strict, so unknown keys fail run-config validation before the run starts. *(Medium-high confidence.)*

### Q34
`dg` resolves the project's Python per project (`[tool.dg.project.python_environment]`, e.g. `active = true` or `uv_managed = true`). Its subprocesses read env from the project's `.env`, so set `DG_PROJECT_PYTHON_EXECUTABLE` there, at the project root. A shell export may be overridden or ignored when `dg` re-resolves the project environment, so don't rely on it. *(Low confidence; check `dg --help` or the docs for your exact version.)*

### Q35
The default is `QueuedRunCoordinator` (the default since 1.x) with no `max_concurrent_runs` limit. All 25 runs are enqueued as QUEUED and the daemon's run queue dequeues and launches them, effectively all at once. If `dagster-daemon` isn't running, they sit in QUEUED forever. *(Medium-high confidence.)*

### Q36
`table_schema` expects a dict with a `columns` key, so a bare list fails metadata normalization on the orchestration side. You get a Pipes error or warning, and the materialization fails or loses that metadata. Use `raw_value: {"columns": [{"name": "id", "type": "int"}]}`. *(Medium-low confidence on whether it raises or only warns.)*

### Q37
`concurrency.runs.max_concurrent_runs` is typed as `IntSource`, so `{env: NAME}` loads. I believe `run_retries.max_retries` is a plain `Int`, so `{env: ...}` there would fail instance config validation. *(Low-medium confidence; the quick test is `dagster instance info`.)*

### Q38
`dg launch` executes the run in-process in your CLI process, like `dagster asset materialize`. It bypasses the run coordinator queue and `K8sRunLauncher`, so there is no k8s pod and no `max_concurrent_runs` limit. Pool limits are instance-level slot claims made during step execution, so they still apply if the CLI points at the same `DAGSTER_HOME` instance. *(Medium confidence.)*

### Q39
It does not throttle. A pool with no limit and no `concurrency.pools.default_limit` is unlimited. When a run holding a slot is cancelled, its slots are released as the run worker tears down. If the worker died, the daemon reclaims slots held by runs in a terminal state; run monitoring helps here. You can also free them manually in the UI's concurrency page or with `dagster instance concurrency`. *(Medium confidence.)*

### Q40
Sensors are evaluated in the `grpc_server` code-location process, not the daemon, so `EnvVar('API_KEY')` resolves to missing. The tick then fails with a missing env var error. Set it on the code location container, and also on the launched run containers if runs use the resource. *(High confidence.)*

### Q41
Outside a Dagster orchestration process, `open_dagster_pipes()` emits a `DagsterPipesWarning` that the process wasn't launched by Dagster. All Pipes calls, including `report_asset_materialization`, become no-ops, so the script runs normally and nothing is reported. *(High confidence.)*

### Q42
Both processes fail at startup when loading the instance. They raise a `DagsterHomeException` or similar saying `$DAGSTER_HOME` must be an absolute path to an existing directory. Dagster does not auto-create it. *(Medium-high confidence.)*

### Q43
Run `dg plus deploy start` (with `--deployment` and `--deployment-type`), then `dg plus deploy build-and-push`, then `dg plus deploy finish`. Use `dg plus deploy set-build-output` if you built the image yourself. A bare `dg plus deploy` does the full pipeline in one shot: it starts a new session, builds and pushes again, and finishes. It doesn't complete your staged session, so you'd rebuild and push a second time. *(Medium confidence.)*

### Q44
`dg` writes to the user-level `dg` config file, `~/.dg.toml`, under `[cli.plus]`. If a legacy `~/.dagster_cloud_cli/config` also has Plus settings, the two sources can conflict. `dg` may read the new file while `dagster-cloud` and other tooling use the old one, so you hit the wrong org or deployment or a stale token. Remove or align the legacy file. *(Low-medium confidence on precedence.)*

### Q45
These are two settings at different levels. The per-PR limit (5) is deployment-level `run_queue.max_concurrent_runs` applied to branch deployments; setting it requires Admin on the deployment. The cap across all branch deployments (100) is an organization setting (`max_concurrent_branch_deployment_runs`), which requires Organization Admin. So `dg api deployment settings set` covers only the per-PR limit. *(Low-medium confidence on the exact key names.)*

### Q46
Set `auto_materialize: use_sensors: false` in `dagster.yaml`. The AssetDaemon is still required in both modes. With this setting it runs the single global evaluation tick. With sensors enabled, it is the process that evaluates the `AutomationConditionSensorDefinition`s. Either way, run `dagster-daemon run` and turn on "Auto-materializing" or the sensors in the UI. Confidence: high.

### Q47
- **`freshness_policy=LegacyFreshnessPolicy(...)` and `AssetOut(legacy_freshness_policy=...)`:** Both attach the old lag-based freshness policy. That policy is now effectively metadata only. Freshness-based auto-materialize was removed, so it drives no scheduling, and it is not the new freshness/alerting system. Migrate to the new `FreshnessPolicy.time_window(fail_window=..., warn_window=...)` or `FreshnessPolicy.cron(...)`.
- **`auto_observe_interval_minutes`:** This asked the asset daemon to run the observation function every N minutes. It is deprecated. Prefer `automation_condition=AutomationCondition.cron_tick_passed("*/10 * * * *")`, or a schedule or sensor that runs an observation job.

Confidence: medium.

### Q48
These constructs are deprecated or removed:
- `SourceAsset(...)`
- `external_asset_from_spec` / `external_assets_from_specs`
- Old `@observable_source_asset`-centric patterns and `auto_observe_interval_minutes`

The replacement is to declare external assets as plain `dg.AssetSpec(key, ...)` and pass them in `Definitions(assets=[...])`, or let `dg` autoload them from `defs`. Report their state with:
- `AssetMaterialization` / `AssetObservation` events from sensors or ops
- The REST endpoint `/report_asset_materialization/`
- `instance.report_runless_asset_event`

Confidence: medium. Some of these may still exist with deprecation warnings rather than hard failures.

### Q49
No, it won't resolve. Integration libraries use the 0.x line, so `dagster==1.13.28` pairs with `dagster-dbt==0.29.28` and `dagster-snowflake==0.29.28`, and no 1.13.28 version of them exists. Pin `dagster==1.13.28` and either pin the libraries to `0.29.28` or leave them unpinned or ranged (`>=0.29,<0.30`). Each library pins its matching `dagster` version, so the resolver keeps them in lockstep. Then run `uv lock --upgrade-package dagster` (or the equivalent). Confidence: high.

### Q50
Entry points are package metadata, so they only appear once the project is installed as a package, and installed after the edit. Common reasons it fails:
- `uv sync` didn't reinstall your package after the pyproject change.
- The project isn't a buildable package: no `[build-system]`, or `tool.uv.package = false`.
- The entry point value doesn't point to an importable module.

Use `[project.entry-points."dagster_dg_cli.registry_modules"] my_project = "my_project.components"` and make sure a `[build-system]` exists. Then run `uv sync --reinstall-package my_project` (or `uv pip install -e .`), and run `dg list registry-modules` in that venv. Confidence: medium-high.

### Q51
**Non-blocking check:**
- The run does not fail.
- The check is recorded as failed with ERROR severity, which the UI shows and alerts can use.
- The step succeeds, so `b` still materializes.

**With `blocking=True`:** A failed check with ERROR severity makes the step fail after emitting the result. The run fails and `b` is skipped. A WARN-severity failure would still not block. Confidence: high.

### Q52
With the default translator and no `object`/meta overrides:
- **Asset key:** `AssetKey(["target", "public", "accounts"])`. The stream name is sanitized and split on `.`, then prefixed with `target_prefix`.
- **Upstream dep:** `AssetKey(["public", "accounts"])`, the source stream.

Reference it downstream with `@dg.asset(deps=[dg.AssetKey(["target", "public", "accounts"])])`. Confidence: medium.

### Q53
The YAML exposes a `translation_settings:` field that maps to `DagsterDbtTranslatorSettings`:
```yaml
attributes:
  project: '{{ project_root }}/dbt'
  translation_settings:
    enable_source_tests_as_checks: true
```
Yes, I believe `DbtProjectComponent` defaults to `enable_code_references=True`, unlike the bare Python settings, which default to False. Confidence: medium on the code-references default, high on the field.

### Q54
Set `enable_semantic_model_refresh: true` in the `PowerBIWorkspaceComponent` attributes. It also accepts a list of semantic model names if you want to limit which ones become refreshable. The component then builds materializable semantic-model assets that trigger a refresh and poll for completion. Confidence: medium.

### Q55
The default `dbt_executable` is `"dbt"`, resolved on `PATH`, typically the venv's dbt-core. When that executable is the Fusion engine, `.fetch_row_counts()` isn't supported. It is skipped with a warning rather than adding row-count metadata, because it relies on dbt-core adapter queries. Confidence: medium. It may be a no-op or a warning depending on the version.

### Q56
Pass `--event-time-start` and `--event-time-end` from `context.partition_time_window` as ISO timestamps, for example `["build", "--event-time-start", start.isoformat(), "--event-time-end", end.isoformat()]`. If you pass `--vars` with start_date/end_date instead, microbatch ignores them. dbt then computes batches from the current time plus `lookback`, so every partition or backfill processes the latest window instead of its own. Confidence: high.

### Q57
By default (`disable_schedule_on_trigger=True`), Dagster switches the connector's schedule to manual when it triggers a sync, so Fivetran stops syncing on its own schedule. The two `FivetranWorkspace` settings that change this are:
- `disable_schedule_on_trigger`: set it to `False` to keep the Fivetran schedule.
- `retry_on_reschedule` (I'm less sure of this one): controls whether a sync Fivetran reschedules is retried rather than failed.

Confidence: high on the first, low-medium on the second.

### Q58
No, that's not right. `materialize_to_memory` is not deprecated and is still the recommended helper for in-memory unit tests. It forces an in-memory IO manager for all outputs. If you pass an `io_manager` resource, it raises a `DagsterInvariantViolationError` instead of using it. Confidence: medium-high.

