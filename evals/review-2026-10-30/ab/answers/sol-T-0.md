### Q1
`uv run dg check yaml --validate-requirements` fails if `API_TOKEN` is not declared in the component’s `requirements.env`; it checks declarations, not whether environment variables have values. Declaring `requirements: {env: [API_TOKEN]}` satisfies that check even when the variable is unset. `uv run dg check defs` disables requirements validation in its YAML precheck and does not fail merely because this variable is unset: `env.API_TOKEN` resolves to `None`, so the asset description becomes `"token=None"`. To fail on an unset variable, use `env('API_TOKEN')` instead of dot access.

### Q5
Entry points are registered in installed package metadata, so the project must be installed or refreshed after adding the entry point. `uv pip install -e .` does that explicitly, but is not additionally required when `uv sync` correctly installs the project as a package. Once the entry point is declared, dg ignores `[tool.dg.project].registry_modules` rather than merging it with entry-point discovery; remove that obsolete list.

### Q10
`BackfillPolicy.single_run()` executes the asset once for a partition range, so `context.partition_key` raises `DagsterInvariantViolationError` when the range contains several days. Use `context.partition_time_window`, `context.partition_key_range`, or `context.partition_keys` and process the entire range. A returned DataFrame is one output, not automatically split into daily outputs; use a range-aware IO manager or write the range yourself and return `dg.MaterializeResult`. Alternatively, use `dg.BackfillPolicy.multi_run(max_partitions_per_run=1)` to preserve single-partition execution and IO behavior.

### Q19
Inside the schedule, enter `with dg.partition_loading_context(effective_dt=context.scheduled_execution_time):` and compute `key = partitions_def.get_last_partition_key()`, then return `dg.RunRequest(partition_key=key)` if the key is not `None`. This respects the partition definition’s timezone, exclusions, and `end_offset` as of that tick. Do not pass `current_time=` or the tick datetime positionally to `get_last_partition_key()`—in 1.13.28 that method takes no arguments.

### Q23
`dg.AutomationCondition.eager()` considers at most the latest daily partition, not all 30 missing partitions, and dependency/in-progress guards can reduce requests to zero. In particular, its initial evaluation does not request an already-missing historical backlog; a newly eligible daily partition can be requested on a later tick. Widen the scope with `.without(dg.AutomationCondition.in_latest_time_window())` or replace that condition with `in_latest_time_window(lookback_delta=timedelta(days=30))`. For an existing backlog, also replace `"newly_missing"` with `dg.AutomationCondition.missing()` so it remains eligible on subsequent evaluations, or launch an explicit backfill.

### Q32
Use the public `dg.ConfigurableResource` base class, add a config field such as `token: str`, and override `create_resource(self, context: dg.InitResourceContext) -> GitHub` to return `GitHub(self.token)`. Annotate the asset parameter as `github: dg.ResourceParam[GitHub]` and register the configured wrapper under `Definitions(resources={"github": GitHubResource(...)})`. In 1.13.28, neither `dg.ConfigurableResourceFactory` nor `dg.Resource[T]` is exported from the top-level `dagster` API.

### Q36
The Pipes SDK can emit that envelope, but Dagster rejects it while decoding the materialization metadata because `table_schema` requires a mapping, not a list. Use `raw_value={"columns": [{"name": "id", "type": "int"}]}`. The metadata key `"schema"` is valid; `"dagster/column_schema"` is the conventional key for column-schema metadata.

### Q44
With no existing configuration, both commands write `~/.config/dg.toml` under `[cli.plus]` on macOS/Linux, subject to `XDG_CONFIG_HOME` or the `DG_CLI_CONFIG` override. If only the legacy `~/.dagster_cloud_cli/config` exists, the CLI reuses that file rather than automatically migrating it. If both files contain nonempty Plus configuration, configuration loading raises “Found Dagster Plus config in both … Please consolidate your config files,” rather than choosing precedence.

### Q50
`uv sync` can succeed while installing only dependencies: a legacy project without package/build configuration, or with `tool.uv.package = false`, may never install its own entry-point metadata. Additionally, declaring the entry point makes dg ignore `[tool.dg.project].registry_modules`, so that fallback cannot rescue an uninstalled entry point. Configure a valid `[build-system]` and package discovery, then run `uv sync --reinstall-package <project-name>` or `uv pip install -e .` in the environment dg uses. Alternatively, remove the entry-point declaration and use `registry_modules` in the active dg project configuration.

### Q55
In [dagster-dbt 0.29.28](https://github.com/dagster-io/dagster/blob/1.13.28/python_modules/libraries/dagster-dbt/dagster_dbt/core/resource.py#L38-L45), the default is `"dbtf"` if `shutil.which("dbtf")` succeeds at module initialization, otherwise `"dbt"`; `dbt_executable=` overrides it. When the invocation uses Fusion, [`.fetch_row_counts()`](https://github.com/dagster-io/dagster/blob/1.13.28/python_modules/libraries/dagster-dbt/dagster_dbt/core/dbt_event_iterator.py#L221-L241) logs a warning that Fusion does not provide the dbt Core adapter and passes events through without adding row-count metadata. It does not raise an unsupported-operation exception or discard the event stream.
