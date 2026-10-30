### Q1
`uv run dg check yaml --validate-requirements` checks declaration, not environment availability: it fails if `API_TOKEN` is absent from `requirements.env`, but passes if declared even when unset. `uv run dg check defs` does not enable that requirements check and, assuming otherwise valid definitions, succeeds. Dot access to an unset variable returns `None`, so the asset description becomes `"token=None"`; unlike dot access, `{{ env('API_TOKEN') }}` raises when unset without a default.

### Q5
Yes—after adding the entry point, run `uv pip install -e .` to refresh installed package metadata; do not rely on `uv sync` alone to register it. When the project declares a `dagster_dg_cli.registry_modules` entry point, `[tool.dg.project].registry_modules` is ignored for normal discovery, not merged with the entry-point modules or automatically deleted. Move any still-needed registrations into the entry-point discovery structure and verify with `uv run dg list registry-modules`.

### Q10
A multi-day single-run backfill has a partition range, so `context.partition_key` raises `DagsterInvariantViolationError`. Use `context.partition_keys`, `context.partition_key_range`, or `context.partition_time_window` to process the entire range. Returning a DataFrame then exposes a second issue: the default filesystem IO manager cannot store one output across multiple partitions, so write the data yourself and return `dg.MaterializeResult`, supply a range-capable IO manager, or use one partition per run.

### Q19
Compute the key inside `with dg.partition_loading_context(effective_dt=context.scheduled_execution_time):` using `key = partitions_def.get_last_partition_key()`, then return `dg.RunRequest(partition_key=key)`. Handle `key is None`, for example with `dg.SkipReason`, when no partition is available. Do not pass `current_time=` or `dynamic_partitions_store=` to `get_last_partition_key()` in 1.13; those inputs belong on `partition_loading_context`.

### Q23
Default `dg.AutomationCondition.eager()` considers only the latest daily partition—at most one, not all 30—and existing missing partitions alone produce **zero requests on the initial evaluation** because initial evaluation counts as handled. To expand historical eligibility, use `.without(dg.AutomationCondition.in_latest_time_window())`, or `.replace(...)` that operand with `dg.AutomationCondition.in_latest_time_window(lookback_delta=timedelta(days=30))`. To include partitions already missing when automation starts, also use `.replace("handled", dg.AutomationCondition.newly_requested() | dg.AutomationCondition.newly_updated())`; dependency and in-progress guards still apply. For a one-time historical fill, launch an explicit backfill instead.

### Q32
Use the public generic base `dg.ConfigurableResource[GitHub]`, declare `token: str`, and override `create_resource(self, context: dg.InitResourceContext) -> GitHub` to return `GitHub(self.token)`. Annotate the asset parameter as `github: dg.ResourceParam[GitHub]` and bind the factory under the `"github"` resource key. In 1.13.28, `ConfigurableResourceFactory` exists internally but is not exported as `dg.ConfigurableResourceFactory`, and `dg.Resource[T]` is not a public annotation.

### Q36
The external Pipes context accepts and serializes that list-shaped `raw_value`, but Dagster’s receiving-side metadata conversion rejects it because a `table_schema` value must be a mapping. Use `raw_value={"columns": [{"name": "id", "type": "int"}]}`. Successful external reporting therefore does not mean the materialization was successfully processed by Dagster.

### Q44
On a fresh macOS/Linux installation, these commands write Plus settings under `[cli.plus]` in `~/.config/dg.toml`, subject to `$XDG_CONFIG_HOME` or the full-path override `DG_CLI_CONFIG`. If only the legacy `~/.dagster_cloud_cli/config` contains Plus configuration, the CLI continues reading and updating that file rather than automatically migrating it. If both files contain nonempty Plus configuration, config reads and writes raise a “Found Dagster Plus config in both … Please consolidate your config files” error.

### Q50
Entry points are installed package metadata; editing `pyproject.toml` does not itself update the installed registration, and `uv sync` alone can leave it missing. Moreover, declaring the entry point disables the `[tool.dg.project].registry_modules` discovery fallback, so that list cannot rescue the stale registration. Run `uv pip install -e .` in the project environment, ensure the entry-point target is an importable components module, then verify with `uv run dg list registry-modules` and `uv run dg list components`.

### Q55
In dagster-dbt 0.29.28, `DbtCliResource` defaults to `"dbtf"` if `shutil.which("dbtf")` finds it on `PATH`, otherwise `"dbt"`; this default is selected at module import, and `dbt_executable=` overrides it explicitly. When the executable is Fusion, `.fetch_row_counts()` logs a warning that Fusion lacks the dbt Core adapter and passes events through without adding row-count metadata—it does not fail merely because Fusion is being used.
