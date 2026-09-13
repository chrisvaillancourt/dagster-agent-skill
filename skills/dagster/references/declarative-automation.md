# Declarative Automation

Declarative Automation attaches an `AutomationCondition` to an asset, asset check, or asset job; a sensor evaluates those conditions on a tick and launches runs for whatever is true. This is the recommended automation method for asset-centric pipelines. This file covers enabling the evaluation sensor, the built-in conditions, the full operand/operator vocabulary, customization recipes, arbitrary-Python conditions, job- and check-level automation, migration from imperative sensors, and unit testing. Cron schedules, `@sensor`, `@asset_sensor`, and run-status sensors are in `schedules-and-sensors.md`. Partition mappings and backfills are in `partitions-and-backfills.md`. Asset check authoring is in `testing-and-asset-checks.md`.

## Contents

- [Prerequisite: turn on the evaluation sensor](#prerequisite-turn-on-the-evaluation-sensor)
- [Attaching a condition](#attaching-a-condition)
- [Built-in conditions](#built-in-conditions)
- [Operand reference](#operand-reference)
- [Operator reference](#operator-reference)
- [Customizing built-in conditions](#customizing-built-in-conditions)
- [Arbitrary Python conditions](#arbitrary-python-conditions)
- [Automating asset checks](#automating-asset-checks)
- [Automating jobs](#automating-jobs)
- [Automation condition sensors](#automation-condition-sensors)
- [Migrating from sensors](#migrating-from-sensors)
- [Testing](#testing)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Prerequisite: turn on the evaluation sensor

Nothing runs until a sensor evaluates the conditions. Dagster creates `default_automation_condition_sensor` in a code location when all of these hold:

- The code location contains at least one asset, asset check, or asset job with an `AutomationCondition` (or legacy `AutoMaterializePolicy`).
- Dagster 1.5 or later.
- The code location loads and registers definitions successfully.

The sensor starts `STOPPED`. Toggle it on in the UI under **Automation**, per code location. To ship it on by default, define your own sensor with `default_status=RUNNING` (see [Automation condition sensors](#automation-condition-sensors)). If a default sensor never appears, check the three conditions above before debugging further.

## Attaching a condition

| Target | How |
| --- | --- |
| Asset | `@dg.asset(automation_condition=...)` |
| Asset spec / external asset | `dg.AssetSpec(key=..., automation_condition=...)` |
| Multi-asset | `@dg.multi_asset(specs=[dg.AssetSpec(..., automation_condition=...)])` or `outs={"name": dg.AssetOut(automation_condition=...)}` |
| Asset check | `@dg.asset_check(asset=..., automation_condition=...)` |
| Asset job | `dg.define_asset_job(name, selection=..., automation_condition=...)` (preview) |
| Component YAML | `automation_condition` attribute, or `post_processing.assets[].attributes.automation_condition` |

```python
# src/<project>/defs/assets.py
import dagster as dg


@dg.asset(deps=["upstream"], automation_condition=dg.AutomationCondition.eager())
def eager_asset() -> None: ...
```

In component YAML, the `automation_condition` Jinja scope exposes every static constructor on `AutomationCondition`. Set it on assets the component declares, or reach assets it generated through `post_processing`:

```yaml
# src/<project>/defs/sales/defs.yaml
type: dagster.PythonScriptComponent
attributes:
  execution:
    path: process_sales_data.py
  assets:
    - key: sales_revenue_report
      automation_condition: "{{ automation_condition.on_cron('@daily') }}"
post_processing:
  assets:
    - attributes:
        automation_condition: "{{ automation_condition.eager() }}"
```

Component subclasses can add scope entries via `get_additional_scope()`, making a project-specific condition available as `{{ custom_cron('@daily') }}`. See `components.md`. Verify the wiring with `dg check defs` and `dg list defs`.

## Built-in conditions

Start with one of these three; build from scratch only when none fits. Attach as `automation_condition=dg.AutomationCondition.<name>(...)`.

| Condition | Use when |
| --- | --- |
| `on_cron(cron_schedule)` | The target should update once per cron tick, after upstreams are fresh, regardless of when upstream data lands |
| `eager()` | The target should update as soon as any dependency updates |
| `on_missing()` | Partition fan-in: fill a downstream partition once, as soon as all upstream partitions it depends on exist |

**`on_cron(cron_schedule)`**

- Requests the target once at least one upstream partition of *all* upstream assets updated since the previous cron tick, and the target has not yet been requested or updated in this tick.
- If any upstream does not update within the tick, the target is not requested.
- Time-partitioned assets: only the *latest* time partition. Static and dynamic partitions: *all* partitions.
- The cron tick is a synchronization barrier. Pick an interval wide enough for every upstream to finish inside one window. A very frequent cron (`* * * * *`) makes desynchronization worse, not better.

**`eager()`**

- Any upstream partition unmaterialized: not requested. Any upstream partition in an in-progress run: waits for that run. Target already in an in-progress run: waits.
- Time-partitioned assets: only the *latest* time partition. Static/dynamic: *all* partitions.
- A *materialized* upstream always counts as an update. An *observed* upstream counts only when its data version changed.
- Also executes missing partitions that become missing after the condition is applied.

**`on_missing()`**

- Any upstream partition of any upstream asset missing: not requested.
- Time-partitioned assets: only the *latest* time partition.
- Only considers partitions added to the asset *after* the condition was enabled.
- Does not re-trigger when upstream partitions re-run. Use `eager()` if you need re-materialization, and expect a much higher run volume.

## Operand reference

Base conditions, true or false about a target. For partitioned assets the target is a single partition.

| Operand | True when |
| --- | --- |
| `missing()` | Target has not been executed |
| `newly_missing()` | On the tick the target becomes missing |
| `in_progress()` | Target is part of an in-progress run or backfill that has not executed it |
| `run_in_progress()` | Target is part of an in-progress run that has not executed it |
| `backfill_in_progress()` | Target is part of an in-progress backfill that has not executed it |
| `execution_failed()` | Latest execution of the target failed |
| `newly_updated()` | Target updated since the previous tick |
| `newly_requested()` | Target was requested on the previous tick |
| `will_be_requested()` | Target will be requested on this tick |
| `code_version_changed()` | Target code version changed since the previous tick |
| `data_version_changed()` | Target data version changed since the previous tick |
| `initial_evaluation()` | First evaluation of this expression |
| `cron_tick_passed(cron_schedule)` | A tick of that cron passed since the previous evaluation |
| `in_latest_time_window(lookback_delta=None)` | Target is inside the latest time window; with a `timedelta`, inside that delta of the window end (48h on a daily asset gives the latest two partitions) |
| `check_passed()` / `check_failed()` | Asset check evaluated against the asset's latest materialization and passed / failed |
| `freshness_passed()` / `freshness_warned()` / `freshness_failed()` | Target's freshness policy status |
| `asset_matches(key, condition)` | `condition` is true for the given asset key |
| `executed_with_root_target()` | The latest run that updated the target also executed the root key the overall condition is applied to (used to filter out same-run parent updates) |
| `any_new_update_has_run_tags(tag_values={...})` | A new update came from a run whose tags match |

Two categories matter for composition. **Statuses** stay true for a while (`missing()`). **Events** are true for one tick (`newly_updated()`). Convert between them with `.since()` and `.newly_true()`.

## Operator reference

| Operator | Meaning |
| --- | --- |
| `~A` | NOT |
| `A \| B` | OR |
| `A & B` | AND |
| `A.newly_true()` | A was false last evaluation and is true now |
| `A.since(B)` | A became true more recently than B; stays true until B is true |
| `A.since_last_handled()` | A became true more recently than the target was last handled (requested or updated) |
| `any_deps_match(A)` | A is true for at least one partition of any upstream |
| `all_deps_match(A)` | A is true for at least one partition of each upstream |
| `any_checks_match(A, blocking_only=False)` | A is true for at least one of the target's checks |
| `all_checks_match(A, blocking_only=False)` | A is true for all of the target's checks |
| `any_downstream_conditions()` | Union of all distinct downstream conditions (beta) |
| `.allow(selection)` / `.ignore(selection)` | Restrict or exclude which upstreams a dependency sub-condition considers; takes an `AssetSelection` |
| `.with_label("...")` | Name a sub-condition so it renders as one node in the UI evaluation tree |
| `.replace(old, new)` | Replace every sub-condition matching `old` (an `AutomationCondition` or a label/name string) with `new` |
| `.without(cond)` | Remove a sub-condition from a composite |
| `.resolve_through_virtual()` | Resolve dependency sub-conditions through virtual assets to the nearest non-virtual ancestors (preview) |

Composite shorthands:

| Condition | Meaning |
| --- | --- |
| `any_deps_updated()` | Any dependency updated since the previous tick, or will be requested this tick. Ignores parent updates when the same run also plans to update this target |
| `any_deps_missing()` | Any dependency never materialized or observed, and not requested this tick |
| `any_deps_in_progress()` | Any dependency is in an in-progress run |
| `all_deps_updated_since_cron(cron_schedule)` | All dependencies updated since the latest tick of that cron |
| `all_deps_blocking_checks_passed()` | All upstream blocking checks passed, or will be requested this tick |

**Run grouping.** `any_deps_updated()` and `any_deps_missing()` both fold in `will_be_requested()`, so a chain of eager assets collapses into one run rather than one run per asset.

## Customizing built-in conditions

Built-ins are composite trees. Edit them with `.without()`, `.replace()`, `.allow()`, `.ignore()`, and `&` / `|` rather than rebuilding from operands. Every edit below applies to `eager()`, `on_cron()`, and `on_missing()` alike unless noted.

```python
import datetime

import dagster as dg

# Exclude a config-like upstream from triggering compute (or restrict to a set)
dg.AutomationCondition.eager().ignore(dg.AssetSelection.assets("foo"))
dg.AutomationCondition.eager().allow(dg.AssetSelection.groups("abc"))

# Allow execution even when upstream data is missing
(
    dg.AutomationCondition.eager()
    .without(~dg.AutomationCondition.any_deps_missing())
    .with_label("eager_allow_missing")
)

# Update historical time partitions, not just the latest one
dg.AutomationCondition.on_missing().without(
    dg.AutomationCondition.in_latest_time_window(),
)
dg.AutomationCondition.on_missing().replace(
    dg.AutomationCondition.in_latest_time_window(),
    dg.AutomationCondition.in_latest_time_window(datetime.timedelta(days=7)),
)

# Hold execution until every upstream blocking check has passed
dg.AutomationCondition.eager() & dg.AutomationCondition.all_deps_blocking_checks_passed()
```

`.replace(old, new)` matches by label or name string as well as by condition object, which is how you reach a sub-condition buried inside a built-in:

```python
# Pick up partitions that existed before the condition was enabled
dg.AutomationCondition.on_missing().replace(
    "handled",
    dg.AutomationCondition.newly_requested() | dg.AutomationCondition.newly_updated(),
)

# Count an upstream as updated only when its data version changed
dg.AutomationCondition.eager().replace(
    "any_deps_updated",
    dg.AutomationCondition.any_deps_updated().replace(
        "newly_updated", dg.AutomationCondition.data_version_changed()
    ),
)

# Count only updates produced by automation, ignoring manual runs
dg.AutomationCondition.eager().replace(
    "newly_updated",
    dg.AutomationCondition.newly_updated()
    & dg.AutomationCondition.any_new_update_has_run_tags(
        tag_values={"dagster/from_automation_condition": "true"}
    ),
)
```

**Target a lagging partition.** Materialize the partition five days back on a daily cron by replacing the window sub-condition with a difference of two windows:

```python
five_days_ago = dg.AutomationCondition.in_latest_time_window(
    datetime.timedelta(days=5)
) & ~dg.AutomationCondition.in_latest_time_window(datetime.timedelta(days=4))

condition = dg.AutomationCondition.on_cron("@daily").replace(
    "in_latest_time_window", five_days_ago
)
```

**Execute later than upstreams start landing.** Materialize at 9am but begin watching for upstream data at midnight, by swapping only the dependency-tracking sub-condition:

```python
NINE_AM_CRON = "0 9 * * *"

condition = dg.AutomationCondition.on_cron(NINE_AM_CRON).replace(
    old=dg.AutomationCondition.all_deps_updated_since_cron(NINE_AM_CRON),
    new=dg.AutomationCondition.all_deps_updated_since_cron("0 0 * * *"),
)
```

**Label sub-conditions.** Anything past two operands should carry a label; labels render as collapsible nodes in the UI evaluation tree, and `.replace()` can target them by name.

```python
in_progress_or_failed_deps = dg.AutomationCondition.any_deps_match(
    dg.AutomationCondition.in_progress() | dg.AutomationCondition.execution_failed()
).with_label("any_deps_in_progress_or_failed")
```

**Prevent a burst on sensor reactivation.** A sensor that was off for days will, under plain `eager()`, request every partition that became eligible meanwhile. Preview the damage first in the sensor's **target** section in the UI, then gate on a cron tick so only post-tick updates count:

```python
condition = (
    dg.AutomationCondition.any_deps_match(
        dg.AutomationCondition.newly_updated().since(
            dg.AutomationCondition.cron_tick_passed("0 0 * * *")
        )
        & ~dg.AutomationCondition.executed_with_root_target()
    ).newly_true()
    & ~dg.AutomationCondition.in_progress()
    & dg.AutomationCondition.in_latest_time_window()
)
```

`any_deps_updated() & ~initial_evaluation()` does **not** solve this; it still fires for partitions added during the downtime. Roll the fix out to a small asset subset first, watch a day or two, then expand.

## Arbitrary Python conditions

Preview feature. Subclass `dg.AutomationCondition` and implement `evaluate()`, returning an `AutomationResult` built from an `EntitySubset` rather than a bare bool, so partitioned assets can be partially true.

```python
# src/<project>/defs/automation.py
from datetime import datetime

import dagster as dg

CUTOFF = datetime(2025, 1, 1)


class RecentPartitionsOnly(dg.AutomationCondition):
    """Only allow partitions whose key is on or after CUTOFF."""

    def evaluate(self, context: dg.AutomationContext) -> dg.AutomationResult:
        if not context.partitions_def:
            return dg.AutomationResult(context, context.candidate_subset)
        keys = context.candidate_subset.expensively_compute_partition_keys()
        recent = {pk for pk in keys if datetime.fromisoformat(pk) >= CUTOFF}
        true_subset = context.candidate_subset.compute_intersection_with_partition_keys(
            recent
        )
        return dg.AutomationResult(context, true_subset=true_subset)


condition = dg.AutomationCondition.eager() & ~RecentPartitionsOnly()
```

`context.candidate_subset` shrinks as `&` filters partitions out, so put cheap conditions on the left of an `&`.

Custom conditions only run when the sensor evaluates in your user code server; the daemon has no access to user code. Set `use_user_code_server=True` on an `AutomationConditionSensorDefinition` (see the next section). That flag also eliminates version skew between condition definitions and deployed code, but caps the sensor at 500 assets or checks. Scaffold the file with `dg scaffold defs dagster.sensor src/<project>/defs/sensors.py`.

## Automating asset checks

Conditions attach to checks exactly as they do to assets, so a slow or expensive quality check can run on a lower cadence than the asset it targets.

```python
# src/<project>/defs/asset_checks.py
@dg.asset_check(
    asset="upstream",
    automation_condition=dg.AutomationCondition.on_cron("@hourly"),
)
def on_cron_asset_check() -> dg.AssetCheckResult: ...
```

`on_cron()` on a check: at each tick the check waits for its asset to update, then is requested once. `eager()` on a check: requested whenever any dependency updates, subject to the same missing/in-progress guards as an asset.

## Automating jobs

Preview feature. Use when a set of assets must always execute together in one run instead of being requested independently. Wrap an asset-level condition in a job-scoped operator; both evaluate it only against the job's **root assets**, the assets in the selection with no parents inside the job.

| Operator | True for the job when |
| --- | --- |
| `any_job_root_assets_match(A)` | A is true for at least one root asset |
| `all_job_root_assets_match(A)` | A is true for all root assets |

```python
# src/<project>/defs/jobs.py
analytics_job = dg.define_asset_job(
    "analytics_job",
    selection=[processed_data, report],
    automation_condition=dg.AutomationCondition.all_job_root_assets_match(
        dg.AutomationCondition.eager()
    ),
)
```

- One run per time the condition becomes true, covering the job's full selection. Non-root assets execute without separate evaluation.
- Partitioned jobs evaluate per partition and launch one run per true partition, each tagged with its partition key. Runs are **not** grouped into backfills.
- All assets in a partitioned job must share one partitions definition.
- A job with an `automation_condition` cannot also have partitioned run config; supplying both a condition and a `PartitionedConfig` raises at resolution time.
- Jobs are always evaluated by `default_automation_condition_sensor`. Custom sensors cannot target jobs.

## Automation condition sensors

`AutomationConditionSensorDefinition(name, target=, ...)` evaluates conditions and launches runs. Default cadence is roughly every 30 seconds; complex conditions over many assets take longer.

| Parameter | Purpose |
| --- | --- |
| `target` | `AssetSelection` (or coercible) of assets/checks this sensor evaluates |
| `default_status` | `dg.DefaultSensorStatus.RUNNING` to ship it enabled |
| `minimum_interval_seconds` | Lower bound on evaluation frequency; the real interval is longer if evaluation is slow |
| `run_tags` | Tags attached to every run this sensor launches |
| `tags`, `metadata`, `description` | Annotate the sensor itself for UI search and filtering |
| `emit_backfills` | Emit a backfill instead of individual runs when more than one partition of a single asset is requested. Defaults to `True` |
| `use_user_code_server` | Beta. Evaluate in user code; required for custom condition subclasses; max 500 targets |
| `default_condition` | Beta. Condition applied to selected targets that have none of their own. Requires `use_user_code_server=True` |

```python
# src/<project>/defs/sensors.py
@dg.definitions
def sensors():
    return dg.Definitions(
        sensors=[
            dg.AutomationConditionSensorDefinition(
                "team_a_sensor",
                target=dg.AssetSelection.groups("team_a"),
                default_status=dg.DefaultSensorStatus.RUNNING,
                run_tags={"team": "data-eng", "source": "automation"},
            ),
            dg.AutomationConditionSensorDefinition(
                "team_b_sensor", target=dg.AssetSelection.groups("team_b")
            ),
        ],
    )
```

Split a large code location across several sensors: each evaluation finishes faster, a problem in one sensor does not stall unrelated assets, and you can toggle automation per group. `default_automation_condition_sensor` then covers only what the custom sensors do not.

Every tick's condition tree is recorded per asset in the UI evaluation history, with each sub-condition marked true or false. That is the first place to look when an asset did not run.

## Migrating from sensors

Replace a sensor with Declarative Automation when it triggers downstream assets on upstream updates, waits on a set of dependencies, schedules assets after dependencies are fresh, or fills in missing partitions.

| Sensor pattern | Replacement |
| --- | --- |
| `@asset_sensor` triggering a job on one upstream | `eager()` on the downstream asset |
| Multi-asset sensor waiting for **all** monitored assets | `on_cron()` — **not** `eager()` |
| Multi-asset sensor firing on **any** monitored asset | `eager()` |
| Sensor mapping upstream to downstream partitions | `on_missing()`, or `eager()` if re-materialization is wanted |
| Sensor filtering which upstreams count | `eager().ignore(...)` / `.allow(...)` |
| Sensor polling an external system's information schema | Model the tables as external `AssetSpec`s, keep a sensor that only reports materializations, put `eager()` / `on_cron()` on downstreams |
| Sensor inspecting materialization metadata to decide | Asset check on the upstream plus a condition gating on checks passing |
| Sensor attaching fixed metadata to runs | `run_tags` on the `AutomationConditionSensorDefinition` |
| Sensor computing dynamic `run_config` per event | Move the logic into the asset body, reading the event log at execution time |

Cross-code-location dependencies need no special handling: declare the upstream asset key as a dep and use `eager()`. To gate a downstream on upstream checks:

```python
all_deps_checks_passed = dg.AutomationCondition.all_deps_match(
    dg.AutomationCondition.all_checks_match(
        ~dg.AutomationCondition.check_failed()
        | dg.AutomationCondition.will_be_requested(),
    )
)
condition = dg.AutomationCondition.eager() & all_deps_checks_passed
```

**Keep the sensor** when it performs side effects (Slack, audit logs, external APIs), needs to count events before firing (conditions are boolean per tick, not counters), or builds `run_config` from one-time payloads. A working sensor is a fine reason to keep it.

**Do not build a custom "all deps updated since target" condition.** The obvious `all_deps_match(newly_updated().since(asset_matches(...)))` form has no synchronization barrier: a mid-cycle manual backfill of one dependency satisfies the condition against a stale peer, fires an unwanted run, and the desynchronization can persist indefinitely across cycles. `on_cron()` avoids it because the cron tick resets tracking for every dependency at once. That form also hardcodes the target's own asset key, since a condition cannot currently reference the asset it is attached to.

## Testing

`dg.evaluate_automation_conditions(defs=, instance=, asset_selection=, evaluation_time=, cursor=)` evaluates conditions without running the daemon. Thread `result.cursor` from one tick into the next; without it every call looks like a first evaluation.

```python
# tests/test_automation.py
def test_eager_condition():
    with dg.instance_for_test() as instance:
        defs = [upstream, downstream]
        result = dg.evaluate_automation_conditions(defs=defs, instance=instance)
        assert result.total_requested == 0

        dg.materialize([upstream], instance=instance)
        result = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=result.cursor
        )
        assert result.total_requested == 1
```

Simulate a cross-code-location upstream with an `AssetSpec` dep plus a runless event, then re-evaluate:

```python
external_upstream = dg.AssetSpec(key="external_upstream")  # my_asset deps on this
instance.report_runless_asset_event(
    dg.AssetMaterialization(asset_key="external_upstream")
)
result = dg.evaluate_automation_conditions(
    defs=[external_upstream, my_asset], instance=instance, cursor=result.cursor
)
assert result.total_requested == 1
```

`EvaluateAutomationConditionsResult` exposes `total_requested`, `get_num_requested(key)`, `get_requested_partitions(key)`, and `cursor`. Always assert that the next tick requests nothing; a condition that re-requests forever is the most common authoring bug. Pass `evaluation_time=` to test cron-sensitive conditions deterministically.

## Gotchas

- **Sensor off means nothing runs.** A perfect condition with a stopped sensor produces no runs and no error.
- **Conditions evaluate against the asset graph the sensor sees.** Cross-code-location deps work, but the sensor must be able to load the definitions.
- **`eager()` fires on *any* dependency update, not all.** Use `on_cron()` for all-dependency semantics.
- **`on_cron()` requires upstreams to update inside one tick.** Otherwise the target never runs and the UI shows the unsatisfied sub-condition.
- **Time-partitioned assets only get the latest partition** under all three built-ins. Remove or replace `in_latest_time_window` to change that.
- **`on_missing()` ignores partitions that predate the condition.** Replace the `handled` sub-condition to include them.
- **Observed upstreams count as updated only when the data version changes.** Materialized upstreams always count.
- **No side effects in conditions.** Notifications belong in run-status sensors; validations belong in asset checks.
- **Cursor state is managed for you** through `.since()` and `.since_last_handled()`. "Skip every other materialization" and "batch N events" are not expressible.
- **Less control over run boundaries.** Run grouping merges assets that will be requested on the same tick.
- **Declarative Automation is job-less.** You lose the job as a unit of observation. Keep schedules and jobs while they are still useful.
- **Multiple assets sharing one `on_cron()` materialize independently**, in separate runs, with no ordering between them. Use a job if they must move as a unit.

## Legacy and superseded patterns

| Legacy | Replacement |
| --- | --- |
| `AutoMaterializePolicy.eager()` | `AutomationCondition.eager()` |
| `auto_materialize_policy=` argument | `automation_condition=` |
| `@multi_asset_sensor` for upstream monitoring | Declarative Automation; keep the sensor only for side effects or imperative job targeting |
| `FreshnessPolicy` pre-1.6 | Renamed `LegacyFreshnessPolicy`; see the 1.11 upgrade guide |

The default sensor still recognizes a legacy `AutoMaterializePolicy`, but a code location using one must set `use_sensors: true` in its `auto_materialize` configuration. Toggling the sensor on in the UI is not enough by itself. Further migration detail is in `migration-and-legacy.md`.

## Sources

- https://docs.dagster.io/guides/automate — automation method comparison
- https://docs.dagster.io/guides/automate/declarative-automation — recommended conditions and their behavior
- https://docs.dagster.io/guides/automate/declarative-automation/automation-condition-reference — operands, operators, composites, statuses vs events, run grouping
- https://docs.dagster.io/guides/automate/declarative-automation/automation-condition-sensors — default sensor, additional sensors, default status, run tags
- https://docs.dagster.io/guides/automate/declarative-automation/automating-asset-checks — conditions on asset checks
- https://docs.dagster.io/guides/automate/declarative-automation/automating-jobs — job-scoped conditions, partitioned job restrictions
- https://docs.dagster.io/guides/automate/declarative-automation/migrating-from-sensors — sensor-to-condition mappings, caveats, run config strategies
- https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions — index; subpages for eager, on_cron, on_missing, labels, reactivation, arbitrary Python
- https://docs.dagster.io/guides/build/components/creating-new-components/subclassing-components — `automation_condition` Jinja scope
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/post-processing-components — `post_processing.assets` attributes
- https://docs.dagster.io/api/dagster/assets — `AutomationCondition`, `AutomationConditionSensorDefinition`, `evaluate_automation_conditions`
