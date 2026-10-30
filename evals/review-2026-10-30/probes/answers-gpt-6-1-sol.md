### Q1
`dg check yaml --validate-requirements` checks YAML/schema requirements; it does not guarantee that referenced environment variables exist. My best understanding is that `dg check defs` resolves the template and the missing value renders as an empty string, producing `token=` rather than an error. **Low confidence** on the missing-variable behavior in 1.13.x.

### Q2
The definitions returned by the `@dg.definitions` function take precedence; the standalone module-level asset is not automatically added unless the function includes it. My understanding is that discovery warns about the ignored module-level definitions. **Medium confidence** on the warning.

### Q3
My best answer is that post-processing merges these attributes: kinds become `{a, b}`, and tags include both `x: '1'` and `y: '2'`, plus Dagster’s kind tags. I believe `dg check yaml` does not fully validate the post-processing attribute structure because that processing is handled separately from the component’s schema. **Low confidence** on both details; do not rely on replacement-versus-merge semantics without checking this version.

### Q4
`DefsStateConfigArgs.local_filesystem()` produces a mutable default that a dataclass cannot accept directly. Use `from dataclasses import field` and `defs_state: ResolvedDefsStateConfig = field(default_factory=DefsStateConfigArgs.local_filesystem)`. The failure happens while importing the class, so every command that discovers that component can fail.

### Q5
With a packaged uv project, `uv run dg ...` normally synchronizes and installs the project editably, including updated entry-point metadata; a separate `uv pip install -e .` is unnecessary. The entry point replaces the need to configure those same modules through `[tool.dg.project].registry_modules`; remove the redundant list rather than assuming uv rewrites it. A non-package project or execution with synchronization disabled changes the installation answer.

### Q6
`DAGSTER_IS_DEV_CLI` identifies a CLI loading environment, not reliably “a developer’s machine”; CI definition checks can therefore take the development branch. It is set by `dg dev` and definition-inspection/checking paths such as `dg check defs` and `dg list defs`; I am **less confident** about the complete command list, including `dg launch`. Use an explicit deployment/environment setting for production-dependent definitions instead.

### Q7
My best understanding is that this maps the asset specs and assigns `pdef`, including specs belonging to executable assets; being inside `Component.build_defs` does not itself prohibit the operation. A multi-asset must still satisfy Dagster’s partition-definition consistency constraints. **Low confidence** on the exact behavior of 1.13.28; I cannot confidently rule out a version-specific restriction.

### Q8
`dg launch` executes locally rather than submitting through your production run launcher; my understanding is that its execution is synchronous/in-process. A separate `dg dev` session does not automatically share its instance storage: runs appear there only when both commands use the same persistent instance, typically through the same `DAGSTER_HOME`. **Medium confidence** on the exact launcher implementation.

### Q9
Not with the default backfill policy. CLI partition-range execution requires the selected asset to support a single run covering that range, normally through `backfill_policy=dg.BackfillPolicy.single_run()`. Otherwise, launch individual partitions or submit a backfill through Dagster’s backfill machinery.

### Q10
`context.partition_key` raises when the run covers multiple partition keys: there is no single key to return. Use `context.partition_key_range`, `context.partition_keys`, or `context.partition_time_window` and compute the whole requested range. Returning one DataFrame also requires an I/O manager that can correctly write a multi-partition output; changing the context accessor alone is insufficient.

### Q11
With ordinary daily partitions, default `end_offset=0`, and aligned UTC time zones, the May 20 tick runs partition `2024-05-19`, the most recently completed day. Partition availability settings such as `end_offset`, the partition time-zone/boundary configuration, and the schedule’s effective execution time can change that answer. `hour_of_day=1, minute_of_hour=30` sets the tick time, not the partition label.

### Q12
The default behavior is failure when the I/O manager tries to load a missing required hourly partition; a dictionary annotation does not make missing partitions optional. Set `metadata={"allow_missing_partitions": True}` on the downstream `dg.AssetIn` to let a `UPathIOManager` skip absent partitions. The resulting dictionary contains the three available partitions, so downstream logic must handle incomplete input.

### Q13
`context.definition_metadata` is metadata on the downstream input definition, such as its `AssetIn`; `context.upstream_output.definition_metadata` is static metadata on the upstream output/asset definition. Neither is a historical lookup of metadata reported by `MaterializeResult`. Do not rely on `context.upstream_output.output_metadata` for persisted runtime metadata; query the upstream materialization event through `context.instance`, selecting the appropriate partition and event.

### Q14
No. Returning `None` still produces an output and can materialize the asset, even with `output_required=False`. Make the function a generator and yield `dg.Output(value)` only when there is new data; yielding nothing skips the output and dependent execution.

### Q15
The first partition key is `2024-05-05`: default weekly partitions start on Sunday, and Dagster advances to the first boundary at or after the supplied start date. To partition from Wednesday, May 1, use `WeeklyPartitionsDefinition(start_date="2024-05-01", day_offset=3)`. If Sunday-aligned weeks are required instead, start at `2024-04-28` and handle the partial first week explicitly.
### Q16
Pass `sales/assets.py`: `dg scaffold defs dagster.asset sales/assets.py`. The path is relative to the project’s configured defs module, not the project root. Passing `src/my_proj/defs/sales/assets.py` nests that entire path under `src/my_proj/defs/` instead.

### Q17
No. `dg launch` executes locally and synchronously; configuring a Dagster+ Hybrid deployment does not redirect it to that deployment’s run launcher. Use `dg plus deployment launch` to submit a remote run.

### Q18
Pass `jobs.py`: `dg scaffold defs dagster.job jobs.py`. The scaffolder resolves it under the configured defs module, producing `src/myproj/defs/jobs.py`.

### Q19
Use the job’s partitions definition: `partitions_def.get_last_partition_key(current_time=context.scheduled_execution_time)`, then pass the resulting string to `RunRequest(partition_key=...)`. This respects the partition definition’s timezone, availability boundaries, and `end_offset`; handle `None` if no partition is available. Do not pass the schedule context or a datetime itself as `partition_key`, and do not substitute wall-clock “now” for the scheduled tick time.

### Q20
No. An explicit `monitored_jobs` list replaces the default monitoring scope; it does not augment the jobs in the sensor’s own location. Add another `CodeLocationSelector(location_name="alerts")` or explicit local job selections if both locations should trigger it.

### Q21
For cron-defined cycles, use `dg.AutomationCondition.on_cron(cron_schedule)`, which waits for all upstream dependencies to update since the relevant cron tick; its dependency predicate is `all_deps_updated_since_cron(...)`. The common wrong choice is `eager()` or `any_deps_updated()`: neither imposes an all-upstreams-per-cycle barrier. A cycle without a cron boundary needs an explicitly defined reset condition rather than an assumed “all updated” interpretation.

### Q22
Run custom Python conditions through an `AutomationConditionSensorDefinition(..., use_user_code_server=True)` so evaluation occurs where the implementation is available. Implement the condition’s required interface, including its name and an `evaluate()` returning `AutomationResult`; conditions serialized across process boundaries also need supported serialization, typically internal `@whitelist_for_serdes`. Arbitrary subclasses are not automatically portable to daemon-side or Dagster+-managed evaluation; confidence is moderate on the precise 1.13.x serialization requirements.

### Q23
By default, `eager()` restricts time-partitioned assets to the latest time window: it requests the latest eligible missing partition, not all 30 historical ones. Remove that restriction with `dg.AutomationCondition.eager().without(dg.AutomationCondition.in_latest_time_window())` to allow older partitions to qualify. Other eager predicates, including dependency availability, still apply; use a backfill for explicitly bounded historical work.

### Q24
`tuple[Output[int], Output[str]]` is not a declaration of two independently named outputs; without an explicit multi-output definition, Dagster can treat the return as one tuple-valued output and type-check its elements against the inferred tuple type. Declare `out={"number": dg.Out(int), "text": dg.Out(str)}` and return `(1, "a")` with a raw-value return annotation, or yield named `dg.Output` objects. Do not use `Output` wrappers as the tuple’s ordinary data values.

### Q25
With `allow_retries=False`, the body runs once: the failure overrides the retry policy. With plain `dg.Failure("bad input")`, it runs up to four times—one initial attempt plus three retries—assuming every attempt fails.

### Q26
Normally, `dg check defs` succeeds: `EnvVar` defers lookup, and definitions validation does not initialize the resource. The missing variable fails when execution resolves the resource configuration, before its setup or consuming op runs, with an environment-variable/config-postprocessing error. An existing variable containing `""` resolves to an empty string and satisfies a plain `str` field unless you add stricter validation.

### Q27
No. That plain `ResourceDefinition` returns a Python object; it does not automatically activate the returned `ConfigurableResource`’s lifecycle. Register `MyResource(...)` directly, or explicitly enter its lifecycle from a yielding resource; supported lifecycle customization includes `yield_for_execution()` with cleanup in `finally`. Dagster does not automatically discover and call arbitrary `close()`, `cleanup()`, or `dispose()` methods.

### Q28
`materialize_to_memory` installs in-memory IO managers and rejects a supplied resource under an IO-manager key, including `"io_manager"`; use `dg.materialize` to exercise `FilesystemIOManager`. Separately, `def down(up)` needs a resolvable upstream asset/input and an IO manager able to load its value—supplying an IO manager alone does not supply the missing upstream definition. With `deps=[up]` and no `up` parameter, the dependency establishes ordering but does not load an upstream value, so a downstream-only materialization can run without that value.

### Q29
The `orders` materialization and its checks run before the downstream asset. A failed blocking check with default `ERROR` severity prevents downstream execution; a failed blocking check with `WARN` severity does not. With default `raise_on_error=True`, `dg.materialize(...)` raises on the blocking ERROR failure, whereas a failed WARN check alone does not make execution fail.

### Q30
Use `dg.define_asset_job("j", dg.AssetSelection.assets(orders).without_checks())`; selecting an asset otherwise includes its associated checks. Define a separate checks-only job with `dg.AssetSelection.checks_for_assets(orders)` and schedule that job independently. Those separately scheduled checks no longer gate the earlier materialization job’s downstream execution.
### Q31
Attach `freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24))` to the asset; it describes freshness, not when to materialize the asset. My understanding is that automatic evaluation of this newer policy requires Dagster+ rather than an OSS sensor or schedule; for OSS freshness checks, use `build_last_update_freshness_checks` with `build_sensor_for_freshness_checks`. The older, automation-oriented policy became `LegacyFreshnessPolicy`. Confidence: medium on the 1.13 availability boundary.

### Q32
Subclass `dg.ConfigurableResourceFactory[GitHub]`, declare `token: str`, and implement `create_resource(self, context: dg.InitResourceContext) -> GitHub`. Register the factory in `Definitions(resources={"github": ...})`; annotate the asset argument as `github: dg.Resource[GitHub]` to receive its constructed client. Older examples use `ResourceParam[GitHub]`; confidence: medium on the exact 1.13 `Resource` spelling.

### Q33
Use the enum **member name**, not its Python `.value`; for `Color.RED = "red"`, YAML uses `color: RED`. The discriminated union becomes a selector, such as `pet: {cat: {name: Whiskers}}`, rather than `pet: {pet_type: cat, name: Whiskers}`. Unknown keys normally fail run-config validation; permissive config must be explicitly modeled with `PermissiveConfig`.

### Q34
My best understanding is that `DG_PROJECT_PYTHON_EXECUTABLE` must be in the project-root `.env` file—for example, `DG_PROJECT_PYTHON_EXECUTABLE=../.venvs/my-project/bin/python`—because `dg` reads this override during project-environment discovery. Merely exporting it in the invoking shell does not activate that particular override. Confidence: low; this is implementation-specific behavior I cannot verify here.

### Q35
My best answer for 1.13.28 is that the default is `QueuedRunCoordinator`, not the older immediate-launch `DefaultRunCoordinator`. The sensor’s 25 runs enter the queue, and the daemon launches them subject to concurrency configuration; the default maximum is 10 concurrent runs, so the remainder wait. Confidence: medium on the version-specific default; queued runs require the daemon to drain them.

### Q36
The list is the wrong shape for `table_schema`; Dagster rejects it while decoding the reported metadata, rather than constructing a valid table schema. Use `raw_value: {"columns": [{"name": "id", "type": "int"}]}`. The producer-side report may be emitted successfully before the orchestration process encounters the decoding error.

### Q37
My understanding is that neither field accepts `{env: NAME}` in 1.13.28: their configuration schemas require literal integers rather than `IntSource` values. Render the environment values into integer YAML before Dagster loads the instance configuration. Confidence: medium; environment-variable support is field-specific, not a universal `dagster.yaml` feature.

### Q38
`dg launch` executes locally; it is not a submission to the deployment’s `K8sRunLauncher`. Do not rely on it to enforce the deployment’s queued-run or pool limits; submit through the deployment’s UI, API, schedule, or sensor for deployment-managed execution. Confidence: medium on the exact local executor behavior in 1.13.

### Q39
`pool="db"` alone does not throttle unless the instance has an explicit limit for `db` or a configured default pool limit. For a limited pool, normal execution cleanup releases claimed slots; the daemon’s concurrency-slot cleanup handles slots left behind by terminated runs. Cancellation must actually terminate the execution—requesting cancellation alone is not proof that the slot is free.

### Q40
The sensor evaluates in the code-location process, so a resource resolving `dg.EnvVar("API_KEY")` needs that variable in the `grpc_server` container. Setting it only on the daemon does not propagate it over gRPC; resource initialization fails when the sensor tick needs that resource. Set it in the code-location container’s environment, and separately in run-worker environments if run resources also need it.

### Q41
Without Dagster’s Pipes bootstrap parameters, `open_dagster_pipes()` warns and returns an inactive context so the script can run independently. Materialization reports do not create Dagster asset events in that mode. Code that requires injected extras or other orchestration-provided values can still fail when those values are absent.

### Q42
Startup fails with `DagsterInvariantViolationError`: an explicitly set `DAGSTER_HOME` must point to an existing directory. Dagster does not silently create that home directory or fall back to an ephemeral instance. Create `/opt/dagster/home` and make it accessible to both processes before starting them.

### Q43
My best recollection is `dg plus deploy start`, then `dg plus deploy build-and-push`, then `dg plus deploy finish` to apply the staged deployment. Bare `dg plus deploy` is the command group, not the finalization operation, so it does not update code locations. Confidence: low on these exact current subcommand names; I would verify their help before putting them into CI.

### Q44
My best recollection is that the newer CLI writes Plus credentials/settings to `~/.dg.toml`, rather than the legacy `~/.dagster_cloud_cli/config`. Conflicting legacy configuration can cause commands that still consult it to target a different organization or deployment, so do not maintain contradictory settings in both places. Confidence: low on the exact current path and precedence; I cannot safely assert the precise failure mode without checking the implementation.

### Q45
These are organization-level branch-deployment concurrency controls, not an ordinary deployment setting applied with `dg api deployment settings set`. Configure the per-branch limit as 5 and the organization-wide branch-run limit as 100; changing organization settings requires an Organization Admin role. Confidence: medium on the settings scope and role; I do not know the exact 1.13 CLI field names confidently enough to supply a command.
### Q46
Set `auto_materialize: {use_sensors: false}` in `dagster.yaml` to use the global AssetDaemon evaluation loop instead of automation-condition sensors. The AssetDaemon is still required when using those sensors: it performs automation-condition evaluation on their behalf; the sensor daemon alone does not replace it.

### Q47
My best recollection is that these legacy freshness-policy arguments are no longer supported in 1.13, rather than becoming the new freshness policies; they should not be carried forward as working automation configuration. `auto_observe_interval_minutes` historically scheduled observations, not materializations, and depended on the AssetDaemon. **Low confidence on the exact 1.13 removal-versus-ignored behavior of these three signatures.**

### Q48
Use `AssetSpec` to describe an external asset’s key, dependencies, metadata, and other attributes; including it in `Definitions(assets=[...])` does not give Dagster a materialization function. My best answer is that older `SourceAsset` and observable-source-asset APIs are the incompatible constructs targeted by this migration, but **I’m not confident about the exact 1.13 removal boundary**.

### Q49
No: those integration packages use the **0.29.x** version series alongside Dagster **1.13.x**, not matching `1.13.x` package versions. For Dagster `1.13.28`, use `dagster-dbt==0.29.28` and `dagster-snowflake==0.29.28`, then regenerate the lockfile.

### Q50
An entry-point declaration is discoverable only after the project is installed as a Python distribution; `uv sync` does not necessarily install a project without a build system. Add an appropriate `[build-system]`, ensure the components module is included in the built package, and run `uv sync` again. The entry point’s value must name the importable registry module, not merely its directory.

### Q51
A failed **non-blocking** check does not itself fail the run, even with `severity=AssetCheckSeverity.ERROR`; `b` can still materialize. With `blocking=True`, Dagster waits for the check before executing downstream assets, and a failed `ERROR` check prevents `b` from executing and fails the run. A blocking check reporting only `WARN` does not enforce that failure.

### Q52
My recollection is that the default translator sanitizes `public.accounts` to `public_accounts`, producing `AssetKey(["target", "public_accounts"])` with upstream dependency `AssetKey(["public_accounts"])`. Reference the generated asset with `@dg.asset(deps=[dg.AssetKey(["target", "public_accounts"])])`; this declares a dependency without requiring a loaded input value. **Moderate confidence on the default key sanitization.**

### Q53
The relevant translator setting is `enable_source_tests_as_checks: true`; it enables checks for dbt source tests, not just model tests. **I don’t confidently recall the 1.13 component’s YAML nesting for that setting**, so I would not invent a `translator.settings` or `translation` path. My recollection is that `DbtProjectComponent` enables code references by default, unlike the plain translator’s historical default.

### Q54
Set `enable_refresh_semantic_models: true` on the Power BI component’s attributes. This makes semantic models executable, refreshable assets instead of merely external asset specifications, without requiring hand-written Python refresh wrappers. **Moderate confidence on the exact component field name.**

### Q55
`DbtCliResource` defaults to the executable named `dbt`, resolved from the execution environment; that name does not guarantee the Python dbt Core implementation rather than Fusion. My best recollection is that `.fetch_row_counts()` is unsupported with Fusion and skips enrichment with a warning, rather than obtaining row counts. **Low confidence on whether 0.29.28 warns/skips or raises for that unsupported operation.**

### Q56
Pass `--event-time-start` and `--event-time-end`, using the partition’s `context.partition_time_window.start` and `.end` as timestamp values. `--vars` containing `start_date` and `end_date` does not configure dbt’s microbatch event-time window unless custom project code explicitly interprets those variables; dbt may therefore process a different window than the Dagster partition.

### Q57
By default, triggering a sync through Dagster disables the connector’s Fivetran-managed schedule by switching it to manual scheduling. `FivetranWorkspace.disable_schedule_on_trigger` controls that behavior. **I do not confidently recall the second 1.13-era workspace setting’s name or semantics**, so I cannot reliably supply the requested pair from memory.

### Q58
No: `dg.materialize_to_memory` is not deprecated merely because you are using Dagster 1.13. It executes assets with in-memory IO managers, including assets that declare their own IO-manager keys; supplying an IO-manager resource yourself is rejected rather than silently honored. `dg.materialize(..., resources={"io_manager": ...})` is not an equivalent replacement when assets use other IO-manager keys.
