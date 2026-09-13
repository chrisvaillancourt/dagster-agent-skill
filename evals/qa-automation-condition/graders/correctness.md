---
type: llm
focus: last_message
weight: 2
---
Grade the final answer as a Dagster expert. PASS only if ALL of the following hold:
1. The solution attaches a `dg.AutomationCondition` (or `dagster.AutomationCondition`) to the `report` asset via `automation_condition=` (on `@asset` or `AssetSpec`), rather than implementing a custom `@sensor`/`@multi_asset_sensor`/`@asset_sensor` or a schedule.
2. The condition expresses "all upstream deps updated since the last materialization of report" using dependency-aware operators — for example `AutomationCondition.all_deps_updated_since_cron(...)`, or `all_deps_match(AutomationCondition.newly_updated().since(...))`-style composition, or `on_cron("0 * * * *")` combined with a wait-for-all-deps condition, or `eager()` modified to require all deps — AND bounds execution to at most once per hour using a cron tick (e.g. `cron_tick_passed("0 * * * *")`, `on_cron("0 * * * *")`) or an equivalent time gate. A plain `eager()` alone FAILS (it fires on any single dep update and has no hourly cap).
3. It tells the user the automation condition sensor must be enabled (the default `default_automation_condition_sensor` toggled on in the UI, or an `AutomationConditionSensorDefinition` with `default_status=RUNNING`).
4. Code uses `import dagster as dg` style or explicit `dagster` imports; no deprecated APIs (`AutoMaterializePolicy`, `AutoMaterializeRule`, `build_asset_reconciliation_sensor`).
5. No invented method names: every `AutomationCondition` method used must be one of: eager, on_cron, on_missing, missing, in_progress, newly_updated, newly_requested, code_version_changed, initial_evaluation, in_latest_time_window, cron_tick_passed, will_be_requested, any_deps_match, all_deps_match, any_deps_updated, any_deps_missing, any_deps_in_progress, all_deps_updated_since_cron, any_downstream_conditions, since, newly_true, with_label, replace, without, allow, ignore, execution_failed, backfill_in_progress, run_in_progress, check_passed, check_failed, data_version_changed, asset_matches, check_matches. If a method outside this list appears, FAIL.
Otherwise FAIL. Explain which criterion failed.
