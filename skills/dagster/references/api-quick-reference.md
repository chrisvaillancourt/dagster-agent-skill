# Dagster API quick reference

Grep-friendly index of the public `dagster` Python API: exact symbol names, key constructor/decorator parameters, and one-line semantics. Signatures are abbreviated — only the parameters worth reaching for are listed, in declaration order. `⚠` marks a deprecated or superseded symbol; the Use column names the replacement. This file does not explain concepts or show usage patterns; for those see the topic files (`assets.md`, `partitions-and-backfills.md`, `schedules-and-sensors.md`, `declarative-automation.md`, `resources-and-config.md`, `io-managers.md`, `components.md`, `testing-and-asset-checks.md`, `ops-jobs-and-graphs.md`, `pipes-external-execution.md`, `migration-and-legacy.md`). CLI surface (`dg`, `create-dagster`) is in `project-setup-and-cli.md`; integration libraries are in `integrations-catalog.md` and `integrations-dbt-and-elt.md`.

All symbols are importable from the top-level `dagster` package unless a different module is shown. Convention: `import dagster as dg`.

## Contents
- [Definitions and loading](#definitions-and-loading)
- [Assets and specs](#assets-and-specs)
- [Asset checks and freshness](#asset-checks-and-freshness)
- [Automation conditions](#automation-conditions)
- [Partitions](#partitions)
- [Schedules and sensors](#schedules-and-sensors)
- [Jobs, ops, graphs](#jobs-ops-graphs)
- [Config and resources](#config-and-resources)
- [IO managers and contexts](#io-managers-and-contexts)
- [Execution and testing](#execution-and-testing)
- [Instance and run storage](#instance-and-run-storage)
- [Metadata and types](#metadata-and-types)
- [Errors](#errors)
- [Pipes](#pipes)
- [Components](#components)
- [Utilities and GraphQL](#utilities-and-graphql)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Definitions and loading

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.Definitions` | `(assets, asset_checks, schedules, sensors, jobs, resources, executor, loggers, metadata, component_tree)` | Top-level container a code location exposes. |
| `dg.Definitions.merge` | `(*defs) -> Definitions` (static) | Union of several `Definitions`; errors on conflicting resource/logger keys or executors. |
| `dg.Definitions.validate_loadable` | `(defs)` (static) | Assert no key/name conflicts, resolvable asset jobs, satisfied resources. Use in tests. |
| `dg.Definitions.get_job_def` | `(name) -> JobDefinition` | Fetch a job by name; resolves unresolved asset jobs. There is no `resolve_job_def`. |
| `dg.Definitions.get_schedule_def` | `(name)` / `.get_sensor_def(name)` | Fetch schedule/sensor with resources bound. |
| `dg.Definitions.resolve_all_asset_specs` | `() -> Sequence[AssetSpec]` | Flatten every asset into specs; `.resolve_all_asset_keys()` for keys only. |
| `dg.Definitions.map_asset_specs` | `(func, selection=None)` (preview) | Rewrite specs in place across the object. |
| `dg.Definitions.load_asset_value` | `(asset_key, python_type, partition_key, metadata)` | Read an asset through its IO manager; `.get_asset_value_loader()` for several. |
| `@dg.definitions` | `def defs() -> Definitions` or `def defs(context: ComponentLoadContext)` | Lazy entry point; preferred over a module-level `Definitions(...)`. |
| `dg.load_from_defs_folder` | `(path_within_project=)`; `project_root=` is deprecated (warns, removed in 2.0) | Autoload a `defs/` tree in a `dg` project. Primary loader for the modern layout. |
| `dg.load_definitions_from_module` | `(module, resources, loggers, executor)` (preview) | Build `Definitions` from one module; `_modules`, `_package_module`, `_package_name`, `_current_module` variants. |
| `dg.load_assets_from_modules` | `(modules, group_name, key_prefix, ...)` | Collect `@asset` objects from modules; `_current_module` / `_package_module` / `_package_name` variants. |
| `dg.load_asset_checks_from_modules` | `(modules, asset_key_prefix)` | Same for `@asset_check` objects. |
| `⚠ dg.create_repository_using_definitions_args` | `(name, assets, schedules, ...)` | Stopgap for many logical groups in one code location. Prefer one `Definitions`. |
| `⚠ @dg.repository` | `(name, description, metadata)` | Pre-`Definitions` code-location API. Replace with `Definitions`. |

## Assets and specs

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `@dg.asset` | `(name, key, key_prefix, ins, deps, partitions_def, automation_condition, group_name, kinds, owners, tags, metadata, io_manager_key, required_resource_keys, check_specs, backfill_policy, retry_policy, code_version, pool, config_schema, output_required)` | One asset from one function. |
| `@dg.multi_asset` | `(specs, outs, ins, deps, name, can_subset, partitions_def, backfill_policy, check_specs, required_resource_keys, op_tags, pool)` | Several assets from one computation. Prefer `specs=` over `outs=`. |
| `@dg.graph_asset` | `(name, key, ins, config, key_prefix, group_name, partitions_def, metadata, tags, owners, kinds, automation_condition, backfill_policy, code_version)` | One asset produced by a multi-op graph. |
| `@dg.graph_multi_asset` | `(outs, name, ins, partitions_def, backfill_policy, group_name, can_subset, config)` | Several assets from one graph. |
| `dg.AssetSpec` | `(key, deps, description, metadata, group_name, code_version, owners, tags, kinds, partitions_def, automation_condition, backfill_policy, skippable)` | Declarative asset description with no compute attached. |
| `dg.AssetSpec.replace_attributes` | `(**kwargs) -> AssetSpec` | Overwrite fields; `.merge_attributes(deps, metadata, owners, tags, kinds)` unions them instead. |
| `dg.AssetKey` | `(path: str \| Sequence[str])` | Identity of an asset; `.path` is the segment list. |
| `dg.AssetIn` | `(key, key_prefix, metadata, partition_mapping, dagster_type)` | Rebind or configure one input of an asset function. |
| `dg.AssetOut` | `(key, key_prefix, dagster_type, is_required, io_manager_key, metadata, group_name, code_version, automation_condition, backfill_policy, owners, tags, kinds)` | One output of a `@multi_asset`; `.from_spec(spec, ...)` converts a spec. |
| `dg.AssetDep` | `(asset, partition_mapping)` | Dependency edge carrying a partition mapping. |
| `dg.AssetsDefinition` | class | Result of the asset decorators; `.keys`, `.key`, `.node_def`, `.partitions_def`, `.check_specs`, `.can_subset`. |
| `dg.AssetsDefinition.from_graph` | `(graph_def, keys_by_input_name, keys_by_output_name, key_prefix, partitions_def, partition_mappings, group_name, backfill_policy, ...)` | Wrap an existing graph as assets. |
| `⚠ dg.AssetsDefinition.to_source_asset(s)` | `()` | Produces `SourceAsset`. Use an `AssetSpec` with no compute instead. |
| `dg.MaterializeResult` | `(asset_key, metadata, check_results, data_version, tags, value)` | Return value of an asset function reporting a materialization. |
| `dg.ObserveResult` | `(asset_key, metadata, check_results, data_version, tags)` | Return value of an observation function. |
| `dg.AssetMaterialization` | `(asset_key, description, partition, tags, metadata)` | Event object; use with `context.log_event` or `report_runless_asset_event`. |
| `dg.AssetObservation` | `(asset_key, partition, tags, metadata)` | Observation event object. |
| `dg.DataVersion` | `(value: str)` | Opaque content hash returned by observation functions. |
| `@dg.observable_source_asset` | `(name, key_prefix, partitions_def, group_name, automation_condition, metadata, tags, required_resource_keys)` (beta) | Observe an externally managed asset; `@dg.multi_observable_source_asset` for several (beta). |
| `⚠ dg.SourceAsset` | `(key, metadata, io_manager_key, partitions_def, observe_fn, description, tags)` | Deprecated. Use `AssetSpec` for external assets. |
| `dg.map_asset_specs` | `(func, iterable)` | Apply a function to every spec in a list of assets/specs. |
| `dg.AssetSelection.all` | `(include_sources=False)` | Everything; `.materializable()`, `.all_asset_checks()`. |
| `dg.AssetSelection.assets` | `(*assets_defs_or_keys)` | Select by asset object or key; `⚠ .keys(*asset_keys)` is deprecated in favor of it. |
| `dg.AssetSelection.groups` | `(*group_names, include_sources=False)` | Select by group; `.key_prefixes(*prefixes)`, `.tag(key, value)`. |
| `dg.AssetSelection.downstream` | `(depth=None, include_self=True)` | Expand downstream; `.upstream(depth, include_self)` expands upstream. |
| `dg.AssetSelection.roots` | `()` / `.sinks()` / `⚠ .sources()` | Graph endpoints. `.sources()` is deprecated; use `.roots()`. |
| `dg.AssetSelection.checks_for_assets` | `(*assets)` / `.checks(*checks)` / `.without_checks()` | Move between assets and their checks. |
| `dg.AssetSelection.required_multi_asset_neighbors` | `()` | Widen to whole non-subsettable multi-assets. |
| `dg.AssetExecutionContext` | attrs | `.log`, `.instance`, `.partition_key`, `.partition_keys`, `.partition_key_range`, `.partition_time_window`, `.asset_key`, `.selected_asset_keys`, `.selected_asset_check_keys`, `.resources`, `.run_id`, `.add_asset_metadata`, `.log_event`, `.pdb`. |
| `dg.build_asset_context` | `(resources, resources_config, asset_config, instance, partition_key, partition_key_range, run_tags)` | Synthesize a context for unit tests. |

## Asset checks and freshness

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `@dg.asset_check` | `(asset, name, description, blocking, additional_ins, additional_deps, required_resource_keys, metadata, automation_condition, partitions_def, retry_policy)` | One check on one asset. `blocking=True` halts downstream on failure. |
| `@dg.multi_asset_check` | `(specs, name, can_subset, ins, required_resource_keys, config_schema, retry_policy)` | Several checks from one function; yield `AssetCheckResult` per spec. |
| `dg.AssetCheckSpec` | `(name, asset, description, additional_deps, blocking, metadata, automation_condition, partitions_def)` | Declare a check attached to an asset via `check_specs=`. |
| `dg.AssetCheckResult` | `(passed, asset_key, check_name, metadata, severity, description)` | Return/yield value of a check. |
| `dg.AssetCheckSeverity` | `.ERROR` / `.WARN` | Severity of a failed check; `WARN` does not fail the run. |
| `dg.AssetCheckKey` | `(asset_key, name)` | Identity of a check; unique per asset. |
| `dg.AssetCheckEvaluation` | `(asset_key, check_name, passed, metadata, severity)` | Runless check event for `report_runless_asset_event`. Importable from `dagster`, no dedicated reference page. |
| `dg.AssetChecksDefinition` | class | Object produced by the check decorators; pass to `Definitions(asset_checks=...)`. |
| `dg.FreshnessPolicy.cron` | `(deadline_cron, lower_bound_delta, timezone)` | Current freshness API: asset must update by each cron deadline. Pass via `freshness_policy=`. |
| `dg.FreshnessPolicy.time_window` | `(fail_window, warn_window)` | Asset must update within a rolling window. |
| `⚠ dg.build_last_update_freshness_checks` | `(assets, lower_bound_delta, deadline_cron, timezone, blocking)` | Superseded by `FreshnessPolicy`. |
| `⚠ dg.build_time_partition_freshness_checks` | `(assets, deadline_cron, timezone, severity, blocking)` | Superseded by `FreshnessPolicy`. |
| `⚠ dg.build_sensor_for_freshness_checks` | `(freshness_checks, minimum_interval_seconds, name, default_status, tags)` | Only needed with the superseded builders above. |
| `dg.build_metadata_bounds_checks` | `(assets, metadata_key, min_value, max_value, exclusive_min, exclusive_max, severity)` (beta) | Assert a numeric metadata value stays in range. |
| `dg.build_column_schema_change_checks` | `(assets, severity)` (beta) | Fail when `dagster/column_schema` metadata changes. |
| `dg.build_asset_check_context` | `(resources, resources_config, asset_config, instance)` | Context for testing checks directly. |

## Automation conditions

Operands are static methods on `dg.AutomationCondition`. Compose with `&`, `\|`, `~`.

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.AutomationCondition.eager` | `()` | Materialize when deps update; skips missing/in-progress deps, latest time partition only. |
| `dg.AutomationCondition.on_cron` | `(cron_schedule)` | Materialize once per cron tick, after all deps updated since that tick. |
| `dg.AutomationCondition.on_missing` | `()` | Materialize a partition the first time all its deps exist. |
| `dg.AutomationCondition.missing` | `()` | True while never materialized or observed. |
| `dg.AutomationCondition.in_progress` | `()` / `.run_in_progress()` / `.backfill_in_progress()` | True while part of an in-flight run or backfill. |
| `dg.AutomationCondition.execution_failed` | `()` | True when the latest execution failed. |
| `dg.AutomationCondition.newly_updated` | `()` / `.newly_missing()` / `.newly_requested()` | Event: state changed since the previous tick. |
| `dg.AutomationCondition.cron_tick_passed` | `(cron_schedule)` | Event: first evaluation after a cron tick. |
| `dg.AutomationCondition.in_latest_time_window` | `(lookback_delta=None)` | Restrict to the most recent time partition(s). |
| `dg.AutomationCondition.any_deps_match` | `(condition)` / `.all_deps_match(condition)` | Lift a condition over dependencies; chain `.allow(selection)` / `.ignore(selection)`. |
| `dg.AutomationCondition.any_deps_updated` | `()` / `.any_deps_missing()` / `.any_deps_in_progress()` | Composite dep conditions with run grouping built in. |
| `dg.AutomationCondition.all_deps_updated_since_cron` | `(cron_schedule)` | All deps refreshed since the latest tick. |
| `dg.AutomationCondition.all_deps_blocking_checks_passed` | `()` | Gate on upstream blocking checks. |
| `dg.AutomationCondition.check_passed` | `()` / `.check_failed()` / `.any_checks_match(c)` / `.all_checks_match(c)` | Condition on asset checks. |
| `dg.AutomationCondition.code_version_changed` | `()` / `.data_version_changed()` | React to version changes. |
| `dg.AutomationCondition.freshness_passed` | `()` / `.freshness_warned()` / `.freshness_failed()` | React to `FreshnessPolicy` state. |
| `dg.AutomationCondition.asset_matches` | `(key, condition)` | Evaluate a condition against another asset. |
| `dg.AutomationCondition.will_be_requested` | `()` / `.initial_evaluation()` / `.any_downstream_conditions()` (beta) | Run-grouping and bootstrap primitives. |
| `dg.AutomationCondition.any_job_root_assets_match` | `(condition)` / `.all_job_root_assets_match(condition)` (preview) | Attach a condition to an asset job's root assets. |
| `<cond>.since(other)` | `(other)` | True since `<cond>` last became true, until `other` does. `.since_last_handled()` variant. |
| `<cond>.newly_true()` | `()` | Convert a status into a one-tick event. |
| `<cond>.without(sub)` | `(sub)` | Drop a sub-condition from a composite such as `eager()`. |
| `<cond>.replace(old, new)` | `(old: AutomationCondition \| str, new)` | Swap a labeled sub-condition anywhere in the tree. |
| `<cond>.with_label(label)` | `(label)` | Name a composite so the UI and `.replace` can address it. |
| `dg.AutomationConditionSensorDefinition` | `(name, target, default_condition, tags, run_tags, metadata, default_status, minimum_interval_seconds, emit_backfills, use_user_code_server)` | Evaluates conditions; one named `default_automation_condition_sensor` exists implicitly. |
| `dg.evaluate_automation_conditions` | `(defs, instance, asset_selection, evaluation_time, cursor)` | Test conditions without a daemon; result exposes `.total_requested`, `.get_num_requested(key)`. |
| `⚠ dg.AutoMaterializePolicy` | — | Removed from the API reference. Replace with `AutomationCondition`. |

## Partitions

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.DailyPartitionsDefinition` | `(start_date, end_date, minute_offset, hour_offset, timezone, fmt, end_offset, exclusions)` | Daily windows. |
| `dg.HourlyPartitionsDefinition` | `(start_date, end_date, minute_offset, timezone, fmt, end_offset, exclusions)` | Hourly windows. |
| `dg.WeeklyPartitionsDefinition` | `(start_date, end_date, minute_offset, hour_offset, day_offset, timezone, fmt, end_offset, exclusions)` | Weekly windows. |
| `dg.MonthlyPartitionsDefinition` | `(start_date, end_date, minute_offset, hour_offset, day_offset, timezone, fmt, end_offset, exclusions)` | Monthly windows. |
| `dg.TimeWindowPartitionsDefinition` | `(cron_schedule, start, timezone, end, fmt, end_offset, exclusions)` | Arbitrary cron cadence. |
| `dg.StaticPartitionsDefinition` | `(partition_keys: Sequence[str])` | Fixed key list. Keep under ~100k keys. |
| `dg.DynamicPartitionsDefinition` | `(name, partition_fn)` | Keys added at runtime via the instance or a sensor request. |
| `dg.MultiPartitionsDefinition` | `(partitions_defs: Mapping[str, PartitionsDefinition])` | Cross-product of named dimensions. |
| `dg.MultiPartitionKey` | `(keys_by_dimension: Mapping[str, str])` | A `str` subclass; `.keys_by_dimension` recovers the parts. |
| `dg.PartitionKeyRange` | `(start, end)` | Inclusive key range for single-run backfills. |
| `dg.TimeWindow` | `(start, end)` | Datetime bounds of one time partition. |
| `dg.TimeWindowPartitionMapping` | `(start_offset, end_offset, allow_nonexistent_upstream_partitions)` | Offset windows between time-partitioned assets. |
| `dg.IdentityPartitionMapping` | `()` | Same key upstream and downstream (the default for matching defs). |
| `dg.AllPartitionMapping` | `()` / `dg.LastPartitionMapping()` | Depend on every, or only the newest, upstream partition. |
| `dg.StaticPartitionMapping` | `(downstream_partition_keys_by_upstream_partition_key)` | Explicit static key map. |
| `dg.SpecificPartitionsPartitionMapping` | `(partition_keys)` | Pin to a fixed upstream subset. |
| `dg.MultiToSingleDimensionPartitionMapping` | `(partition_dimension_name)` (beta) | Bridge a multi-partitioned asset to a single-dimension one. |
| `dg.MultiPartitionMapping` | `(downstream_mappings_by_upstream_dimension)` (beta) | Per-dimension mappings between multi-partitioned assets. |
| `dg.BackfillPolicy.single_run` | `()` | One run covers the whole backfill range. |
| `dg.BackfillPolicy.multi_run` | `(max_partitions_per_run=1)` | Chunk a backfill into runs. |
| `dg.PartitionedConfig` | `(partitions_def, run_config_for_partition_key_fn, tags_for_partition_key_fn)` | Per-partition run config for op jobs. |
| `@dg.daily_partitioned_config` | `(start_date, minute_offset, hour_offset, timezone, fmt, end_offset, exclusions)` | Decorator form; `hourly_`, `weekly_`, `monthly_` variants. |
| `@dg.static_partitioned_config` | `(partition_keys, tags_for_partition_key_fn)` | Static-key config function. |
| `@dg.dynamic_partitioned_config` | `(partition_fn, tags_for_partition_fn)` | Dynamic-key config function. |

## Schedules and sensors

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `@dg.schedule` | `(cron_schedule, name, target, job, execution_timezone, default_status, tags, metadata, should_execute, required_resource_keys, owners)` | Cron-triggered function returning `RunRequest`s or `SkipReason`. |
| `dg.ScheduleDefinition` | `(name, cron_schedule, job, target, execution_fn, run_config, run_config_fn, execution_timezone, default_status, tags, metadata)` | Non-decorator form; fixed `run_config` needs no function. |
| `dg.build_schedule_from_partitioned_job` | `(job, description, name, default_status, tags)` | Derive a schedule whose cadence follows the job's time partitioning. |
| `dg.ScheduleEvaluationContext` | attrs | `.scheduled_execution_time`, `.instance`, `.resources`, `.log`. |
| `dg.build_schedule_context` | `(instance, scheduled_execution_time)` | Test context for a schedule function. |
| `@dg.sensor` | `(name, job, jobs, target, minimum_interval_seconds, default_status, asset_selection, required_resource_keys, tags, metadata, owners)` | Poll external state and emit runs. |
| `dg.SensorDefinition` | `(evaluation_fn, name, minimum_interval_seconds, job, jobs, target, default_status, asset_selection, tags, metadata)` | Non-decorator form. |
| `dg.SensorEvaluationContext` | attrs | `.cursor`, `.update_cursor(str)`, `.instance`, `.log`, `.resources`, `.repository_def`. |
| `dg.build_sensor_context` | `(instance, cursor, repository_def, resources, definitions, last_sensor_start_time)` | Test context for a sensor function. |
| `dg.RunRequest` | `(run_key, run_config, tags, job_name, asset_selection, asset_check_keys, partition_key, stale_assets_only)` | Request one run. `run_key` dedupes across ticks. |
| `dg.SkipReason` | `(skip_message)` | Explain why a tick produced no runs. |
| `dg.SensorResult` | `(run_requests, skip_reason, cursor, dynamic_partitions_requests, asset_events)` | Return runs, cursor, and partition mutations atomically. |
| `dg.AddDynamicPartitionsRequest` | `(partitions_def_name, partition_keys)` | Add dynamic partitions from a sensor/schedule tick. |
| `dg.DeleteDynamicPartitionsRequest` | `(partitions_def_name, partition_keys)` | Remove dynamic partitions from a tick. |
| `@dg.asset_sensor` | `(asset_key, name, minimum_interval_seconds, job, jobs, default_status, tags, metadata)` | Fire on each materialization of one asset. |
| `⚠ @dg.multi_asset_sensor` | `(monitored_assets, name, job, request_assets, default_status)` | Superseded. Use `AutomationCondition` or a plain `@sensor`. |
| `@dg.run_status_sensor` | `(run_status, name, monitored_jobs, monitor_all_code_locations, request_job, request_jobs, default_status)` | React to runs reaching a status. |
| `@dg.run_failure_sensor` | `(name, monitored_jobs, monitor_all_code_locations, request_job, request_jobs, default_status)` | Shorthand for failure status. |
| `dg.RunStatusSensorContext` | attrs | `.dagster_run`, `.dagster_event`, `.instance`, `.log`, `.partition_key`. `RunFailureSensorContext` adds `.failure_event`, `.get_step_failure_events()`. |
| `dg.build_run_status_sensor_context` | `(sensor_name, dagster_event, dagster_instance, dagster_run, resources, repository_def)` | Test context for status sensors. |
| `dg.DagsterRunStatus` | `.QUEUED .NOT_STARTED .STARTING .STARTED .SUCCESS .FAILURE .CANCELING .CANCELED .MANAGED` | Value for `run_status=`. |
| `dg.DefaultScheduleStatus` | `.RUNNING` / `.STOPPED` | `default_status=` on schedules; `dg.DefaultSensorStatus` for sensors. |
| `dg.JobSelector` | `(location_name, repository_name, job_name)` | Cross-code-location target for `monitored_jobs`; `dg.RepositorySelector` for a whole location. |
| `dg.make_email_on_run_failure_sensor` | `(email_from, email_password, email_to, smtp_host, smtp_port, monitored_jobs, default_status)` | Prebuilt failure-alert sensor. |

## Jobs, ops, graphs

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.define_asset_job` | `(name, selection, config, tags, run_tags, metadata, description, executor_def, hooks, op_retry_policy, partitions_def)` | Job over an asset selection; resolved lazily by `Definitions`. |
| `@dg.job` | `(name, resource_defs, config, tags, run_tags, metadata, logger_defs, executor_def, op_retry_policy, partitions_def, input_values)` | Op job. Resources must be bound at definition time. |
| `@dg.op` | `(name, description, ins, out, config_schema, required_resource_keys, tags, code_version, retry_policy)` | Single computation node. |
| `@dg.graph` | `(name, description, ins, out)` | Composite of ops, usable via `.to_job(...)`. |
| `dg.In` | `(dagster_type, description, default_value, metadata, input_manager_key)` | Op input declaration. |
| `dg.Out` | `(dagster_type, description, is_required, io_manager_key, metadata, code_version)` | Op output declaration. |
| `dg.Output` | `(value, output_name, metadata, data_version, tags)` | Yield to emit a named output with metadata. |
| `dg.DynamicOut` | `(dagster_type, description, io_manager_key, metadata)` | Declare a fan-out output. |
| `dg.DynamicOutput` | `(value, mapping_key, output_name, metadata)` | One element of a fan-out; consume with `.map(...).collect()`. |
| `dg.GraphIn` | `(description)` / `dg.GraphOut(description)` | Graph-level input/output mappings. |
| `dg.Nothing` | type | Dependency edge carrying no data. |
| `dg.RetryPolicy` | `(max_retries, delay, backoff, jitter)` | Automatic step retries. |
| `dg.Backoff` | `.LINEAR` / `.EXPONENTIAL` | Retry delay growth; `dg.Jitter.FULL` / `.PLUS_MINUS` randomizes it. |
| `dg.Failure` | `(description, metadata, allow_retries)` | Raise to fail a step deliberately, with metadata. |
| `dg.RetryRequested` | `(max_retries, seconds_to_wait)` | Raise to request a retry from inside user code. |
| `@dg.success_hook` | `(name, required_resource_keys)` / `@dg.failure_hook(...)` | Side effects after each op; attach via `hooks=`. |
| `dg.HookContext` | attrs | `.op_exception`, `.op_output_values`, `.op_output_metadata`, `.job_name`, `.step_key`, `.log`, `.resources`. |
| `dg.OpExecutionContext` | attrs | `.log`, `.run_id`, `.run_config`, `.op_config`, `.resources`, `.instance`, `.partition_key`, `.retry_number`, `.pdb`. |
| `dg.build_op_context` | `(resources, op_config, resources_config, instance, mapping_key, partition_key, run_tags)` | Test context for an op. |
| `dg.JobDefinition.execute_in_process` | `(run_config, instance, partition_key, raise_on_error, op_selection, input_values, resources)` | Run a job in-process; returns `ExecuteInProcessResult`. |
| `dg.execute_job` | `(job, instance, run_config, tags, raise_on_error, op_selection, reexecution_options)` | Multiprocess execution; `job` must be `reconstructable(...)`. |
| `dg.reconstructable` | `(target)` | Make a job picklable for out-of-process executors. |
| `dg.ReexecutionOptions` | `(parent_run_id, step_selection)` | Re-run a subset of a prior run. |
| `dg.in_process_executor` | ExecutorDefinition | Single-process execution. |
| `dg.multiprocess_executor` | ExecutorDefinition | Default multi-process execution; `dg.multi_or_in_process_executor` selects by config. |
| `@dg.executor` | `(name, config_schema, requirements)` | Custom executor. |

## Config and resources

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.Config` | pydantic base | Typed op/asset config; declare fields, accept as a `config:` parameter. |
| `dg.PermissiveConfig` | pydantic base | Same, but allows undeclared extra fields. |
| `dg.RunConfig` | `(ops=..., resources=..., loggers=..., execution=...)` | Typed container for run config; `.to_config_dict()` lowers it. |
| `dg.ConfigurableResource` | pydantic base | Current resource API: declare fields, implement methods, pass instances to `Definitions(resources=...)`. |
| `dg.ResourceDefinition.hardcoded_resource` | `(value, description)` | Wrap a plain object as a resource (tests, stubs). |
| `dg.ResourceDefinition.mock_resource` | `(description)` | A `MagicMock`-backed resource; `.none_resource()`, `.string_resource()` also exist. |
| `dg.EnvVar` | `(name: str)` | Defer an env var read to run time; `.get_value()` reads it. Do not use `os.getenv` in definitions. |
| `dg.InitResourceContext` | attrs | `.resource_config`, `.resources`, `.instance`, `.log`, `.run`. |
| `dg.build_init_resource_context` | `(resources, config, instance)` | Test context for `@resource` functions. |
| `dg.build_resources` | `(resources, instance, resource_config, dagster_run, log_manager)` | Context manager that initializes resources outside a run. |
| `dg.ConfigMapping` | `(config_fn, config_schema, receive_processed_config_values)` | Reshape config for a graph or job. |
| `dg.configured` | `(configurable, config_schema)` | Bake config into a definition, producing a new one. |
| `dg.validate_run_config` | `(job_def, run_config)` | Type-check run config without executing. |
| `⚠ @dg.resource` | `(config_schema, description, version, required_resource_keys)` | Legacy function-based resource. Prefer `ConfigurableResource`. |
| `⚠ dg.Field` | `(config, default_value, is_required, description, is_secret)` | Legacy config schema. Prefer `Config` / `ConfigurableResource`. |
| `⚠ dg.Shape` / `dg.Permissive` / `dg.Selector` / `dg.Array` / `dg.Noneable` / `dg.Map` | legacy config types | Only for `config_schema=` dicts. Prefer pydantic classes. |
| `⚠ dg.StringSource` / `dg.IntSource` / `dg.BoolSource` | legacy scalar types | Env-var-aware scalars in legacy schemas. Prefer `EnvVar`. |
| `⚠ dg.make_values_resource` | `(**kwargs)` | Shares plain values between ops. Prefer a `ConfigurableResource`. |

## IO managers and contexts

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.ConfigurableIOManager` | pydantic base | Current IO manager API; implement `handle_output` and `load_input`. |
| `dg.ConfigurableIOManagerFactory` | generic base | Build an `IOManager` from config via `create_io_manager(context)`. |
| `dg.IOManager` | ABC | `handle_output(context, obj)` / `load_input(context)`. |
| `dg.UPathIOManager` | ABC | Base for path-backed managers; implement `dump_to_path` / `load_from_path`. Handles partitions and `allow_missing_partitions` metadata. |
| `dg.FilesystemIOManager` | `(base_dir)` | Pickle to local disk. Default when nothing is configured. |
| `dg.InMemoryIOManager` | `()` | Keep outputs in memory; used by `materialize_to_memory`. |
| `dg.IOManagerDefinition.hardcoded_io_manager` | `(value, description)` | Wrap an existing `IOManager` instance. |
| `dg.InputContext` | attrs | `.asset_key`, `.partition_key`, `.asset_partition_keys`, `.asset_partitions_time_window`, `.upstream_output`, `.definition_metadata`, `.resource_config`, `.dagster_type`, `.log`, `.get_asset_identifier()`. |
| `dg.OutputContext` | attrs | `.asset_key`, `.partition_key`, `.asset_spec`, `.definition_metadata`, `.output_metadata`, `.add_output_metadata()`, `.log_event()`, `.get_asset_identifier()`, `.step_key`, `.run_id`. |
| `dg.build_input_context` | `(name, config, definition_metadata, upstream_output, dagster_type, resources, asset_key, partition_key, asset_partitions_def)` | Test an IO manager's `load_input`. |
| `dg.build_output_context` | `(step_key, name, definition_metadata, mapping_key, config, dagster_type, resources, asset_key, partition_key)` | Test an IO manager's `handle_output`. |
| `dg.InputManager` | ABC / `@dg.input_manager(config_schema, input_config_schema, required_resource_keys)` | Load an op input without an upstream output. |
| `⚠ @dg.io_manager` | `(config_schema, description, output_config_schema, input_config_schema, required_resource_keys)` | Function-based form. Prefer `ConfigurableIOManager`. |
| `⚠ dg.fs_io_manager` | IOManagerDefinition | Superseded. Use `FilesystemIOManager` directly. |
| `dg.mem_io_manager` | IOManagerDefinition | Definition form of the in-memory manager. |

## Execution and testing

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.materialize` | `(assets, resources, run_config, partition_key, tags, selection, instance)` | Execute assets in-process; the default test entry point. |
| `⚠ dg.materialize_to_memory` | `(assets, run_config, resources, partition_key, tags, selection)` | Deprecated. Use `materialize` with `InMemoryIOManager`. |
| `dg.ExecuteInProcessResult` | attrs | `.success`, `.run_id`, `.dagster_run`, `.all_events`, `.job_def`. |
| `dg.ExecuteInProcessResult.output_for_node` | `(node_str, output_name="result")` | Read one op output; `.output_value(output_name)` for the top-level output. |
| `dg.ExecuteInProcessResult.asset_value` | `(asset_key)` | Load a materialized asset value through its IO manager. |
| `dg.ExecuteInProcessResult.get_asset_materialization_events` | `()` | Materialization events produced by the run. |
| `dg.ExecuteInProcessResult.get_asset_check_evaluations` | `()` | Check evaluations produced by the run. |
| `dg.JobExecutionResult` | attrs | Same surface as above, returned by `execute_job`. |
| `dg.instance_for_test` | `(overrides, set_dagster_home, temp_dir)` | Context manager yielding a throwaway `DagsterInstance`. |
| `dg.DagsterEvent` | attrs | `.event_type`, `.asset_key`, `.partition`, `.is_step_failure`, `.is_successful_output`. |
| `dg.DagsterEventType` | enum | `.RUN_SUCCESS`, `.RUN_FAILURE`, `.STEP_FAILURE`, `.ASSET_MATERIALIZATION`, `.ASSET_OBSERVATION`, `.ASSET_CHECK_EVALUATION`, `.LOGS_CAPTURED`, and the rest. |
| `dg.check_dagster_type` | `(dagster_type, value) -> TypeCheck` | Assert a value satisfies a Dagster type. |

## Instance and run storage

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.DagsterInstance.get` | `()` (static) | Instance from `DAGSTER_HOME`; `.ephemeral()` and `.local_temp()` for tests. |
| `dg.DagsterInstance.add_dynamic_partitions` | `(partitions_def_name, partition_keys)` | Register dynamic partition keys. |
| `dg.DagsterInstance.delete_dynamic_partition` | `(partitions_def_name, partition_key)` | Remove one key; `.get_dynamic_partitions(name)`, `.has_dynamic_partition(name, key)` to inspect. |
| `dg.DagsterInstance.get_latest_materialization_event` | `(asset_key)` | Most recent materialization event for an asset. |
| `dg.DagsterInstance.report_runless_asset_event` | `(asset_event)` | Record an `AssetMaterialization`, `AssetObservation`, or `AssetCheckEvaluation` outside a run. |
| `dg.DagsterInstance.get_run_records` | `(filters, limit, order_by, ascending)` | Query runs. There is no public `get_runs` or `submit_run`; use `.get_run_by_id(run_id)` for one. |
| `dg.DagsterInstance.fetch_materializations` | `(records_filter, limit, cursor)` | Paged event fetch; `.fetch_observations`, `.fetch_run_status_changes` alongside. |
| `dg.DagsterInstance.get_asset_keys` | `(prefix, limit, cursor)` | Enumerate known asset keys; `.has_asset_key(key)`, `.wipe_assets(keys)`. |
| `dg.DagsterRun` | attrs | `.run_id`, `.job_name`, `.status`, `.tags`, `.is_success`, `.is_failure`, `.is_finished`. |
| `dg.RunsFilter` | `(run_ids, job_name, statuses, tags, snapshot_id, updated_after, created_before, exclude_subruns)` | Filter for run queries. |
| `dg.EventRecordsFilter` | `(event_type, asset_key, asset_partitions, after_cursor, before_cursor, after_timestamp, before_timestamp)` | Filter for event-log queries. |
| `dg.EventLogEntry` | attrs | `.dagster_event_type`, `.message`, `.get_dagster_event()`; `dg.EventLogRecord` wraps it with a storage id. |

## Metadata and types

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.MetadataValue.text` | `(text)` / `.md(str)` / `.url(str)` / `.path(str)` | Scalar display metadata. |
| `dg.MetadataValue.int` | `(value)` / `.float(v)` / `.bool(v)` / `.timestamp(v)` / `.null()` | Numeric and primitive metadata. |
| `dg.MetadataValue.json` | `(data)` / `.python_artifact(obj)` / `.notebook(path)` | Structured metadata. |
| `dg.MetadataValue.table` | `(records, schema=None)` | Inline tabular preview from `TableRecord`s. |
| `dg.MetadataValue.table_schema` | `(schema)` | Attach a `TableSchema`; the key `dagster/column_schema` powers UI column lineage. |
| `dg.MetadataValue.column_lineage` | `(lineage)` | Attach a `TableColumnLineage`. |
| `dg.MetadataValue.asset` | `(asset_key)` / `.job(job_name)` / `.dagster_run(run_id)` / `.pool(name)` | Cross-links inside Dagster. |
| `dg.TableSchema` | `(columns, constraints=None)` | Column list; `.from_name_type_dict({"col": "int"})` builds one quickly. |
| `dg.TableColumn` | `(name, type, description, constraints, tags)` | One column in a `TableSchema`. |
| `dg.TableRecord` | `(data: Mapping[str, Any])` | One row for `MetadataValue.table`. |
| `dg.TableColumnLineage` | `(deps_by_column: Mapping[str, Sequence[TableColumnDep]])` | Column-level lineage payload. |
| `dg.TableColumnDep` | `(asset_key, column_name)` | One upstream column reference. |
| `dg.with_source_code_references` | `(assets_defs)` (beta) | Attach local file references to assets. |
| `dg.link_code_references_to_git` | `(assets_defs, git_url, git_branch, file_path_mapping)` (beta) | Rewrite local references to repository URLs. |
| `dg.AnchorBasedFilePathMapping` | `(local_file_anchor, file_anchor_path_in_repository)` (beta) | Map local paths to repo-relative paths. |
| `dg.DagsterType` | `(type_check_fn, key, name, description, loader, required_resource_keys)` | Custom runtime type. |
| `dg.PythonObjectDagsterType` | `(python_type, name, key, description, loader)` | Wrap a Python class as a Dagster type. |
| `@dg.dagster_type_loader` | `(config_schema)` | Build a typed value from run config. |
| `dg.TypeCheck` | `(success, description, metadata)` | Result of a type check function. |
| `dg.Nothing` | type | No-data dependency edge; declare as an input or output type. |
| `dg.usable_as_dagster_type` | decorator | Mark a class usable directly as a type annotation. |

## Errors

| Symbol | Typical cause |
| --- | --- |
| `dg.DagsterError` | Base class; `.is_user_code_error` distinguishes user from framework faults. |
| `dg.DagsterInvalidDefinitionError` | Definition rules violated: duplicate asset keys, missing dep, bad decorator arguments. Surfaced by `dg check defs`. |
| `dg.DagsterInvalidConfigError` | Run config does not type-check against the schema. |
| `dg.DagsterInvalidConfigDefinitionError` | The config schema itself is malformed. |
| `dg.DagsterInvariantViolationError` | A runtime invariant broke: no `Definitions` at module top level, yielding the wrong result type. |
| `dg.DagsterInvalidSubsetError` | Selected ops or assets do not exist in the job. |
| `dg.DagsterUnknownResourceError` | An op or asset used a resource key it did not declare in `required_resource_keys`. |
| `dg.DagsterResourceFunctionError` | A resource's init function raised. |
| `dg.DagsterExecutionStepExecutionError` | Wrapper around a user exception raised inside a step body. |
| `dg.DagsterExecutionStepNotFoundError` | A requested step key is absent from the plan. |
| `dg.DagsterStepOutputNotFoundError` | A required upstream output was never produced. |
| `dg.DagsterUserCodeExecutionError` | Base for all exceptions wrapping user code. |
| `dg.DagsterTypeCheckDidNotPass` | A `DagsterType` check failed on an input or output. |
| `dg.DagsterSubprocessError` | One or more child processes failed under a multiprocess executor. |
| `dg.DagsterUnmetExecutorRequirementsError` | The executor is incompatible with the instance or storage configuration. |
| `dg.DagsterRunNotFoundError` | Run id absent from run storage. |
| `DagsterExecutionInterruptedError` | Run process received a termination signal, often pod preemption. Not exported in the API reference. |

## Pipes

Orchestration side lives in `dagster`; the external process imports `dagster_pipes` (zero-dependency).

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.PipesSubprocessClient` | `(env, cwd, context_injector, message_reader, forward_termination, forward_stdio, termination_timeout_seconds)` | Launch a local subprocess as a Pipes session. |
| `dg.PipesClient.run` | `(context, extras, **client_args) -> PipesClientCompletedInvocation` | Blocking launch; chain `.get_results()` or `.get_materialize_result()`. |
| `dg.open_pipes_session` | `(context, context_injector, message_reader, extras)` | Context manager for clients that launch work themselves. |
| `dg.PipesSession` | object | `.get_bootstrap_env_vars()`, `.get_bootstrap_cli_arguments()`, `.get_results(implicit_materializations=, metadata=)`, `.get_custom_messages()`. |
| `dg.PipesEnvContextInjector` | `()` | Pass context via env vars (default for subprocess). |
| `dg.PipesFileContextInjector` | `(path)` / `dg.PipesTempFileContextInjector()` | Pass context via a file when env vars are unavailable. |
| `dg.PipesFileMessageReader` | `(path)` / `dg.PipesTempFileMessageReader()` | Read messages from a file; `PipesBlobStoreMessageReader` for object stores. |
| `dg.PipesCompositeMessageReader` | `(readers)` (preview) | Combine several readers in one session. |
| `dagster_pipes.open_dagster_pipes` | `(context_loader, message_writer, params_loader)` | Entry point in the external script; use as a context manager. |
| `dagster_pipes.PipesContext` | object | `.log`, `.asset_key`, `.partition_key`, `.extras`, `.get_extra(key)`, `.run_id`, `.retry_number`. |
| `dagster_pipes.PipesContext.report_asset_materialization` | `(metadata, data_version, asset_key)` | Report a materialization back to Dagster. |
| `dagster_pipes.PipesContext.report_asset_check` | `(check_name, passed, severity, metadata, asset_key)` | Report a check result. |
| `dagster_pipes.PipesContext.report_custom_message` | `(payload)` | Send arbitrary JSON; read with `session.get_custom_messages()`. |
| `dagster_pipes.PipesCliArgsParamsLoader` | `()` | Read bootstrap params from CLI args instead of env vars. |
| `dagster_pipes.DagsterPipesError` | exception | Raised on protocol or bootstrap failures. |

## Components

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.Component` | ABC | Implement `def build_defs(self, context: ComponentLoadContext) -> Definitions`. |
| `dg.Resolvable` | mixin | Derive a YAML schema from the class fields so `defs.yaml` can instantiate it. |
| `dg.Model` | pydantic base | Recommended base alongside `Resolvable`; forbids extra fields. |
| `dg.Resolver` | `(fn, model_field_type, model_field_name)` | Attach to a field annotation to customize YAML resolution. |
| `dg.ResolutionContext` | object | `.resolve_value(value)` renders Jinja templates against the available scope. |
| `dg.ComponentLoadContext` | attrs | `.path`, `.component_path`, `.defs_relative_module_name`, `.load_component()`, `.build_defs()`, `.for_path()`, `.with_rendering_scope()`. |
| `dg.ComponentTree` | object | Structure of components that built a `Definitions`. |
| `dg.StateBackedComponent` | ABC | For components backed by fetched external state; implement `write_state_to_path`, `build_defs_from_state`, `defs_state_config`. |
| `dagster.components.Scaffolder` | ABC | Implement `scaffold(self, request: ScaffoldRequest)`; request exposes `.target_path`, `.type_name`. |
| `@dg.scaffold_with` | `(scaffolder_cls)` | Attach a scaffolder to a component class so `dg scaffold defs` generates files. |
| `@dg.template_var` | `(fn)` | Expose a Python value or function to Jinja in `defs.yaml`. |
| `@dg.component_instance` | `(fn)` | Instantiate a component in Python rather than YAML. |
| `dg.ResolvedAssetSpec` | annotation | `AssetSpec` field resolvable from YAML; `dg.ResolvedAssetKey`, `dg.ResolvedAssetCheckSpec`, `dg.AssetAttributesModel` alongside. |
| `dg.DefsFolderComponent` | `(path, children)` | Built-in component representing a `defs/` subtree. |
| `dg.DefinitionsComponent` | built-in | Loads plain Python `Definitions` from a folder. |
| `dg.PythonScriptComponent` | built-in | Runs a Python script as an asset; `dg.UvRunComponent` runs it under `uv`. |
| `dg.FunctionComponent` | built-in | Wraps a single Python function as assets. |
| `dg.TemplatedSqlComponent` | built-in (preview) | Runs templated SQL; `dg.SqlComponent` is the base (preview). |
| `dagster.components.testing.create_defs_folder_sandbox` | `(project_name)` | Test harness; sandbox exposes `.scaffold_component(...)` and `.load_component_and_build_defs(...)`. |
| `dg.load_from_defs_folder` | `(path_within_project=)` | Loader that turns a `defs/` tree into `Definitions`. |

## Utilities and GraphQL

| Symbol | Signature (key params) | Use |
| --- | --- | --- |
| `dg.file_relative_path` | `(dunderfile, relative_path)` | Path relative to a module file; pass `__file__`. |
| `dg.config_from_files` | `(config_files: Sequence[str])` | Merge YAML config files into a run config dict. |
| `dg.config_from_yaml_strings` | `(yaml_strings)` | Same from in-memory YAML; `config_from_pkg_resources` from package data. |
| `dg.get_dagster_logger` | `(name=None)` | Logger that routes to the Dagster event log from helper modules. |
| `dg.DagsterLogManager` | object | The logger behind `context.log`. |
| `dg.PreviewWarning` / `dg.BetaWarning` / `dg.SupersessionWarning` | warning classes | Filter or assert on lifecycle warnings in tests. |
| `dagster_graphql.DagsterGraphQLClient` | `(hostname, port_number, transport, use_https, timeout, headers, path_prefix)` | Programmatic client against a webserver. |
| `DagsterGraphQLClient.submit_job_execution` | `(job_name, repository_location_name, repository_name, run_config, tags, op_selection, asset_selection) -> run_id` | Launch a run. |
| `DagsterGraphQLClient.get_run_status` | `(run_id) -> DagsterRunStatus` | Poll a run. |
| `DagsterGraphQLClient.reload_repository_location` | `(repository_location_name) -> ReloadRepositoryLocationInfo` | Reload a code location; check `.status` against `ReloadRepositoryLocationStatus`. |
| `⚠ DagsterGraphQLClient.shutdown_repository_location` | `(repository_location_name)` | Deprecated. |
| `dagster_graphql.DagsterGraphQLClientError` | exception | Raised on GraphQL-level failures; carries `invalid_step_key` where relevant. |

There is no `terminate_run` on the GraphQL client; cancel runs through the UI, the GraphQL API directly, or `dg`.

## Legacy and superseded patterns

| Old | Current | Note |
| --- | --- | --- |
| `@repository` / `create_repository_using_definitions_args` | `Definitions` + `@definitions` | One code location, one `Definitions`. |
| `SourceAsset` / `AssetsDefinition.to_source_asset` | `AssetSpec` with no compute | External and observable assets are specs now. |
| `AutoMaterializePolicy` / `AutoMaterializeRule` | `AutomationCondition` | Policy classes are gone from the API reference. |
| `build_last_update_freshness_checks` and friends | `FreshnessPolicy.cron` / `.time_window` | Freshness is an asset attribute, not a generated check. |
| `@multi_asset_sensor` | `AutomationCondition` or a plain `@sensor` | Multi-asset sensor is superseded. |
| `@resource` + `Field` config schema | `ConfigurableResource` + pydantic fields | Same for `@io_manager` to `ConfigurableIOManager`. |
| `fs_io_manager` | `FilesystemIOManager` | Definition-level shorthand superseded by the class. |
| `materialize_to_memory` | `materialize(..., resources={"io_manager": InMemoryIOManager()})` | Deprecated helper. |
| `ExpectationResult` | `AssetCheckResult` | Expectations replaced by asset checks. |
| `MetadataEntry` | `MetadataValue` in a `metadata=` dict | Entry objects are deprecated. |
| `context.get_tag` / `has_tag` / `op_config` on `AssetExecutionContext` | `context.run.tags` / typed `Config` parameter | Op-flavored accessors are deprecated on the asset context. |
| `ScheduleDefinition(environment_vars=...)` | `EnvVar` in resource config | Deprecated parameter. |
| `AssetSelection.keys` / `.sources` | `AssetSelection.assets` / `.roots` | Deprecated selection methods. |

Sibling file `migration-and-legacy.md` covers the migration steps; this table only maps names.

## Sources
- https://docs.dagster.io/api/dagster/definitions — `Definitions`, `@definitions`, module loaders, `create_repository_using_definitions_args`
- https://docs.dagster.io/api/dagster/assets — asset decorators, specs, selection, automation conditions, freshness policy
- https://docs.dagster.io/api/dagster/asset-checks — check decorators, results, freshness check builders
- https://docs.dagster.io/api/dagster/partitions — partitions definitions, mappings, backfill policy, partitioned config
- https://docs.dagster.io/api/dagster/schedules-sensors — schedules, sensors, run requests, sensor results, selectors
- https://docs.dagster.io/api/dagster/jobs — `@job`, `JobDefinition`
- https://docs.dagster.io/api/dagster/ops — `@op`, `In`, `Out`, `Output`, retry policy, `Failure`
- https://docs.dagster.io/api/dagster/graphs — `@graph`, `GraphDefinition`, graph in/out
- https://docs.dagster.io/api/dagster/hooks — success and failure hooks, `HookContext`
- https://docs.dagster.io/api/dagster/dynamic — `DynamicOut`, `DynamicOutput`
- https://docs.dagster.io/api/dagster/execution — `materialize`, `execute_job`, results, execution contexts, executors
- https://docs.dagster.io/api/dagster/config — `Config`, `RunConfig`, legacy config types
- https://docs.dagster.io/api/dagster/resources — `ConfigurableResource`, `ResourceDefinition`, `EnvVar`
- https://docs.dagster.io/api/dagster/io-managers — IO manager classes, input/output contexts
- https://docs.dagster.io/api/dagster/metadata — `MetadataValue`, table metadata, code references
- https://docs.dagster.io/api/dagster/types — `DagsterType`, loaders, `TypeCheck`
- https://docs.dagster.io/api/dagster/errors — exception classes and their causes
- https://docs.dagster.io/api/dagster/internals — `DagsterInstance`, `DagsterRun`, `RunsFilter`, `EventRecordsFilter`, `@executor`
- https://docs.dagster.io/api/dagster/pipes — orchestration-side Pipes clients, sessions, injectors, readers
- https://docs.dagster.io/api/dagster-pipes — `open_dagster_pipes`, `PipesContext`, loaders and writers
- https://docs.dagster.io/api/dagster/components — `Component`, `Resolvable`, `Model`, `Resolver`, built-in components, `load_from_defs_folder`
- https://docs.dagster.io/api/dagster/utilities — path, config file, and logger helpers
- https://docs.dagster.io/api/dagster-graphql — `DagsterGraphQLClient`
- https://docs.dagster.io/api/dagster/external-assets — `report_runless_asset_event`
- https://docs.dagster.io/api/api-lifecycle — preview, beta, GA, superseded, deprecated definitions
- https://docs.dagster.io/guides/automate/declarative-automation/automation-condition-reference — operators, composite and job conditions
