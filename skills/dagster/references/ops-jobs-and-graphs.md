# Ops, jobs, and graphs

The imperative layer under assets: `@op` (unit of compute), `@graph` (wiring of ops), `@job` (executable binding of a graph to resources, config, and an executor). Covers op events, dynamic fan-out, hooks, op retries, and executors. Asset definitions themselves are in `assets.md`; partitioned config in `partitions-and-backfills.md`; resources and `Config` classes in `resources-and-config.md`; I/O and input managers in `io-managers.md`; run-level retries and run queues in `operations-and-debugging.md`.

## Contents
- [Assets vs ops: decision rule](#assets-vs-ops-decision-rule)
- [Defining ops](#defining-ops)
- [Op events and exceptions](#op-events-and-exceptions)
- [Graphs](#graphs)
- [Dynamic graphs and fan-out](#dynamic-graphs-and-fan-out)
- [Jobs](#jobs)
- [Job config](#job-config)
- [Executing jobs](#executing-jobs)
- [Op hooks](#op-hooks)
- [Op retries](#op-retries)
- [Executors](#executors)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Assets vs ops: decision rule

The docs are explicit: use assets, not ops, unless you have a reason. Ops documentation is "for Dagster users who need to manage existing ops, or who have complex use cases."

Reach for ops/graphs/jobs when:

| Situation | Use |
| --- | --- |
| Producing/updating a persisted dataset | `@dg.asset` — see `assets.md` |
| Task that produces no asset (notification, admin script, cache warm, API call) | `@dg.op` inside a `@dg.job` |
| One asset whose compute is several discrete, separately-retryable steps | `@dg.graph_asset` (graph-backed asset) |
| Fan-out over a count only known at runtime | `dg.DynamicOut` inside a graph or graph-backed asset |
| Running an existing selection of assets on a schedule | `dg.define_asset_job` — no ops needed |

Dynamic outputs vs plain Python parallelism inside one op/asset: dynamic outputs give per-item observability, per-item retries, and per-item Dagster+ credit cost, but add per-step overhead. Rule of thumb from the docs: dynamic outputs when each item takes 5+ minutes; a multiprocessing pool inside a single op when each item takes seconds to minutes. The hybrid (dynamic ops for stages, a pool inside each) is explicitly endorsed.

## Defining ops

Use when you need a unit of compute that a graph or job will wire up.

```python
# src/<project>/defs/ops.py
class FetchConfig(dg.Config):
    api_endpoint: str


@dg.op(
    ins={"raw": dg.In(description="upstream payload")},
    out=dg.Out(io_manager_key="s3_io_manager"),
    tags={"database": "redshift"},
    pool="api",
    retry_policy=dg.RetryPolicy(max_retries=3, delay=0.2),
)
def fetch(context: dg.OpExecutionContext, raw, config: FetchConfig) -> dict:
    context.log.info(f"run {context.run_id} attempt {context.retry_number}")
    return {"endpoint": config.api_endpoint, "n": len(raw)}
```

`@dg.op` parameters: `name`, `description`, `ins`, `out`, `config_schema`, `required_resource_keys`, `tags`, `code_version`, `retry_policy`, `pool`.

- **Inputs** are function arguments. Add `ins={"x": dg.In(...)}` only to attach a `dagster_type`, `description`, `default_value`, `metadata`, or `input_manager_key`.
- **Outputs** default to a single output named `result`. Return the value directly, or annotate the return type. For several outputs pass a dict: `out={"a": dg.Out(), "b": dg.Out()}` and return a tuple in declared order, or `yield dg.Output(value, output_name="a")`.
- **Config**: annotate a `config:` parameter with a `dg.Config` subclass. `config_schema=` plus `context.op_config` still works but is the older form.
- **Context**: optional first parameter typed `dg.OpExecutionContext`. Useful members: `log`, `op_config`, `resources`, `run_id`, `retry_number`, `instance`, `pdb`, `log_event`, `add_output_metadata`, `selected_output_names`.
- **Ordering-only dependency**: give the downstream op an input of type `dg.Nothing`. It takes no function parameter, so wire it with a keyword argument.

```python
@dg.op(ins={"start": dg.In(dg.Nothing)})
def create_table_2():
    get_database_connection().execute("create table_2 as select * from table_1")


@dg.graph
def nothing_dependency():
    create_table_2(start=create_table_1())
```

Prefer a real data dependency (pass a table name string) over `Nothing` where one exists.

**Op factory**: to vary decorator arguments (not runtime values), write a function returning an `OpDefinition`. Anything that can be a config field should be config instead.

## Op events and exceptions

Use when an op must report something to Dagster beyond its return value.

| Event | Emit with | Purpose |
| --- | --- | --- |
| `dg.Output(value, output_name=, metadata=)` | return or `yield` | Pass data downstream; attach output metadata |
| `dg.AssetMaterialization(asset_key=, description=, metadata=, partition=)` | `context.log_event(...)` or `yield` | Record that an op wrote an external asset |
| `dg.AssetObservation(asset_key=, metadata=)` | `context.log_event(...)` or `yield` | Record metadata without claiming a mutation |
| `dg.ExpectationResult(success=, description=, metadata=)` | `context.log_event(...)` or `yield` | Data-quality result (**deprecated**, removed in 2.0) |
| `dg.Failure(description=, metadata=, allow_retries=)` | `raise` | Structured unrecoverable failure |
| `dg.RetryRequested(max_retries=, seconds_to_wait=)` | `raise` | Ask Dagster to re-run this step |

```python
@dg.op
def my_asset_op(context: dg.OpExecutionContext):
    df = get_some_data()
    store_to_s3(df)
    context.log_event(
        dg.AssetMaterialization(asset_key="s3.my_asset", description="stored in s3")
    )
    my_files = get_files("/path/to/files")
    if not my_files:
        raise dg.Failure(
            description="No files to process",
            metadata={"filepath": dg.MetadataValue.path("/path/to/files")},
        )
    return do_some_transform(df)
```

Rules:

- If you `yield` any event, you must also `yield dg.Output(...)` — returning a value and yielding events are mutually exclusive.
- `dg.Output` is generic: `-> dg.Output[int]`, or `-> tuple[dg.Output[int], dg.Output[str]]` for multi-output ops.
- Metadata values go through `dg.MetadataValue.url / path / md / json / float / int / text`.
- `Failure(allow_retries=False)` bypasses an attached `RetryPolicy` — use it for validation errors and permission errors that a retry cannot fix.
- If you are using asset definitions, do not emit `AssetMaterialization` yourself; the framework does it.
- Use `dg.AssetCheckResult` / `@dg.asset_check` instead of `ExpectationResult` — see `testing-and-asset-checks.md`.

## Graphs

Use when several ops compose into one reusable unit. A `@dg.graph` body calls ops and passes their return values as arguments; it defines structure, not execution.

```python
@dg.graph
def linear():
    add_one(add_one(add_one(return_one())))
```

Composition rules:

- Reusing an op in one graph auto-aliases it (`add_one`, `add_one_2`). Name it yourself with `add_one.alias("second_addition")(...)`.
- One output can feed many downstream inputs. A fixed fan-in is a Python list of op outputs passed to an op that takes `list[...]`.
- Conditional branching: declare `out={"branch_1": dg.Out(is_required=False), "branch_2": dg.Out(is_required=False)}` and `yield` only the taken branch. Ops whose inputs never resolve are skipped. Conditional outputs must be **yielded**, not returned.
- Graphs nest arbitrarily. A sub-graph's parameters become graph inputs; its return value becomes a graph output. Annotate an input with `dg.GraphIn(description=...)`.
- Multiple graph outputs: `@dg.graph(out={"x": dg.GraphOut(), "y": dg.GraphOut()})` and return a dict keyed by output name.
- Sub-graph config nests under the sub-graph's name in run config. `@dg.graph(config={...})` hardcodes it; `@dg.graph(config=<config_mapping>)` maps a narrower schema onto the inner ops.
- `dg.GraphDefinition(name=, node_defs=, dependencies={"add_one": {"number": dg.DependencyDefinition("return_one")}})` builds a graph programmatically, e.g. when generating one from YAML during a migration off another orchestrator.
- Graph-backed assets use `@dg.graph_asset` / `@dg.graph_multi_asset` — see `assets.md`.

**Feeding an unconnected input.** An op input with no upstream output resolves, in this order: an input manager (`dg.In(input_manager_key="...")` bound to an `@dg.input_manager` resource), a `DagsterTypeLoader` on its Dagster type, then `dg.In(default_value=...)`. For built-in types the simplest route is run config, keyed by op name then input name:

```python
my_job.execute_in_process(
    run_config={"ops": {"my_op": {"inputs": {"input_string": {"value": "marmot"}}}}}
)
```

To feed a graph an existing asset's value, pass `some_asset.get_asset_spec()` as the argument; the asset's I/O manager loads it.

## Dynamic graphs and fan-out

Use when the number of parallel branches is unknown until runtime.

```python
@dg.op(out=dg.DynamicOut())
def load_pieces():
    for idx, piece in load_big_data().chunk():
        yield dg.DynamicOutput(piece, mapping_key=str(idx))


@dg.job
def dynamic_graph():
    pieces = load_pieces()
    results = pieces.map(compute_piece)
    merge_and_analyze(results.collect())
```

- `dg.DynamicOut()` takes the same constructor as `dg.Out`. Each `dg.DynamicOutput(value, mapping_key=)` needs a `mapping_key` unique within its output; the key names the cloned downstream steps.
- Inside the composition function a dynamic output **cannot** be passed directly to an op. Call `.map(fn)` (clones downstream ops per item) or `.collect()` (fan-in to a list).
- `.map()` chains: `d.map(echo).map(echo)`, or pass a local function that runs several ops per item. Mix in non-dynamic values with a lambda: `d.map(lambda val: add(val, non_dynamic))`.
- `DynamicOutput` objects may also be **returned** as a list: `-> list[dg.DynamicOutput[str]]`.
- Multiple dynamic outputs come back as a namedtuple; unpack positionally or access by name, then `map`/`collect` each.
- `.collect()` is a synchronization barrier: the collecting op waits for every branch.

## Jobs

Use when you need something launchable, schedulable, and monitorable. Ops cannot run without a job.

```python
# src/<project>/defs/jobs.py
@dg.job(
    resource_defs={"server": prod_server},
    tags={"team": "data"},
    op_retry_policy=dg.RetryPolicy(max_retries=1),
    executor_def=dg.multiprocess_executor,
)
def do_stuff():
    add_one(return_five())


# to_job() builds several environment-specific jobs from one graph:
prod_job = do_stuff_graph.to_job(resource_defs={"server": prod_server}, name="do_stuff_prod")
local_job = do_stuff_graph.to_job(resource_defs={"server": local_server}, name="do_stuff_local")
```

`@dg.job` / `graph.to_job()` parameters: `name`, `resource_defs`, `config`, `tags`, `run_tags`, `metadata`, `logger_defs`, `executor_def`, `op_retry_policy`, `partitions_def`, `input_values`, `hooks`, `owners`, `description`. `to_job()` additionally takes `op_selection`.

**Asset jobs** target a selection of assets instead of ops:

```python
all_assets_job = dg.define_asset_job(name="all_assets_job")
sugary_cereals_job = dg.define_asset_job(
    name="sugary_cereals_job", selection="sugary_cereals", op_retry_policy=dg.RetryPolicy(max_retries=3)
)
```

`define_asset_job` takes `name`, `selection`, `config`, `tags`, `run_tags`, `metadata`, `description`, `executor_def`, `hooks`, `op_retry_policy`, `owners`. Selection accepts the asset-selection string syntax or an `dg.AssetSelection`. Resources are supplied to the assets, not the job. `partitions_def` is deprecated here — partitioning is inferred from the selected assets.

Jobs in a `dg` project live anywhere under `defs/` and load automatically; no `Definitions` object is required. Scaffold one with:

```bash
dg scaffold defs dagster.job src/<project>/defs/jobs.py
dg list defs        # confirm the job is discovered
dg check defs       # confirm definitions load
```

## Job config

Supply `config=` to fix or reshape run config. Three forms:

| Value | Effect |
| --- | --- |
| `dg.RunConfig(ops={...})` or a raw dict | Default run config, editable in the Launchpad |
| `dg.ConfigMapping` (via `@dg.config_mapping`) | Exposes a narrower outer schema that expands to the full one |
| `dg.PartitionedConfig` | Makes an op job partitioned; see `partitions-and-backfills.md` |

```python
class DoSomethingConfig(dg.Config):
    config_param: str


@dg.op
def do_something(config: DoSomethingConfig): ...


@dg.job(config=dg.RunConfig(ops={"do_something": DoSomethingConfig(config_param="stuff")}))
def do_it_all_with_default_config():
    do_something()


# Or narrow the interface with a config mapping:
@dg.config_mapping
def simplified_config(val: SimplifiedConfig) -> dg.RunConfig:
    return dg.RunConfig(ops={"do_something": DoSomethingConfig(config_param=val.simplified_param)})


@dg.job(config=simplified_config)
def do_it_all_with_simplified_config():
    do_something()
```

Values in `config=` are visible and editable in the UI — never put secrets there. `config` and `partitions_def` are mutually exclusive on `@dg.job`.

## Executing jobs

| Route | Command / API | When |
| --- | --- | --- |
| CLI | `dg launch --job my_job [-c config.yaml]` | Launch asynchronously via the instance run launcher |
| CLI (assets) | `dg launch --assets "*"` | Materialize an asset selection without a job |
| UI | Launchpad tab, **Launch Run** | Interactive config editing, op selection |
| Python, in-process | `my_job.execute_in_process(run_config=, op_selection=, resources=, instance=, partition_key=, raise_on_error=)` | Tests and scripts; returns `ExecuteInProcessResult` |
| Python, out-of-process | `dg.execute_job(dg.reconstructable(my_job), instance=, run_config=, op_selection=, reexecution_options=)` | Exercise a real executor; returns `JobExecutionResult` |

`execute_job` needs a persistent `DagsterInstance` (`$DAGSTER_HOME` set) and a reconstructable pointer. A job built by `to_job()` must be wrapped in a module-scope function before `reconstructable()`:

```python
def define_my_job():
    return my_graph.to_job()

with dg.execute_job(dg.reconstructable(define_my_job), instance=instance) as result:
    output_obj = result.output_for_node("some_op")  # outputs load via the job's I/O managers
```

**Op selection syntax** (`op_selection=`, and the same box in the Launchpad): `some_op`, `*some_op` (op + ancestors), `some_op*` (op + descendants), `*some_op*`, `+some_op` (direct parents), `some_op+++` (three hops down).

Re-execute a failed run with `dg.ReexecutionOptions.from_failure(run_id=..., instance=...)`; it cannot be combined with `op_selection`.

## Op hooks

Use for per-op success/failure side effects such as a Slack message. Job-scoped policies belong in a run-failure sensor instead (see `schedules-and-sensors.md`).

```python
@dg.failure_hook(required_resource_keys={"slack"})
def slack_message_on_failure(context: dg.HookContext):
    context.resources.slack.chat_postMessage(channel="#foo", text=f"Op {context.op.name} failed")


# Every op in the job gets the hook:
@dg.job(resource_defs={"slack": slack_resource}, hooks={slack_message_on_failure})
def notif_all():
    a()
    b()


@dg.job(resource_defs={"slack": slack_resource})
def selective_notif():
    a.with_hooks({slack_message_on_failure})()  # only "a" gets it
    b()
```

`@dg.success_hook` is the same shape. Both take `name` and `required_resource_keys`.

`dg.HookContext` exposes `job_name`, `log`, `hook_def`, `op`, `op_config`, `op_exception`, `op_output_values`, `step_key`, `resources`, `required_resource_keys`. Test a hook by invoking it with `dg.build_hook_context(resources={...})`.

Because hooks resolve resources by key, `to_job()` is the clean way to give dev and prod different hook backends — mock the resource with `dg.ResourceDefinition.hardcoded_resource(...)` in dev.

A job-level `hooks={...}` attaches the hook to every op individually; it does not observe job-level events.

## Op retries

```python
@dg.op(
    retry_policy=dg.RetryPolicy(
        max_retries=3, delay=0.2, backoff=dg.Backoff.EXPONENTIAL, jitter=dg.Jitter.PLUS_MINUS
    )
)
def even_better():
    fails_sometimes()
```

- `max_retries` defaults to 1. `delay` is seconds. `Backoff.LINEAR` is `attempt_num * delay`; `Backoff.EXPONENTIAL` is `((2 ** attempt_num) - 1) * delay`. `Jitter.FULL` randomizes between 0 and the delay; `Jitter.PLUS_MINUS` randomizes around it.
- Precedence: op invocation (`problematic.with_retry_policy(policy)()`) beats the op definition, which beats the job's `op_retry_policy=`.
- Asset jobs honour `op_retry_policy=` on `define_asset_job`, and `@dg.asset(retry_policy=...)`.
- For conditional retries, `raise dg.RetryRequested(max_retries=1, seconds_to_wait=1) from e` — always use `raise ... from e` so Dagster captures the original traceback.
- Run-level retries (restarting the whole run after the process dies) are a separate mechanism; see `operations-and-debugging.md`.

## Executors

The executor runs steps inside an already-launched run. The run launcher is a different thing (`deployment-oss.md`).

| Executor | Import | Use when |
| --- | --- | --- |
| `dg.multi_or_in_process_executor` | `dagster` | Default. Multiprocess by default, switchable to in-process by config |
| `dg.in_process_executor` | `dagster` | Serial execution in the run worker; debugging, cheap DAGs |
| `dg.multiprocess_executor` | `dagster` | One process per step on one machine |
| `k8s_job_executor` | `dagster_k8s` | One ephemeral Kubernetes pod per step |
| `docker_executor` | `dagster_docker` | One container per step |
| `celery_executor` | `dagster_celery` | Celery control plane; cross-run op concurrency limits |
| `celery_k8s_job_executor` | `dagster_celery_k8s` | Kubernetes pods with Celery prioritization and queuing |
| `ecs.ecs_executor` | `dagster_aws` | One ephemeral ECS task per step |
| `dask_executor` | `dagster_dask` | One Dask task per step |

Set it per job with `executor_def=` on `@dg.job` or `to_job()`, or once for the whole code location; a job's own `executor_def` wins.

```python
# src/<project>/defs/executor.py — default for the whole code location
@dg.definitions
def executor() -> dg.Definitions:
    return dg.Definitions(executor=dg.multiprocess_executor)
```

Run-config override for the default executor (`in_process:` in place of the whole `multiprocess:` block switches modes):

```yaml
execution:
  config:
    multiprocess:
      max_concurrent: 4
      start_method:
        forkserver: {}
      tag_concurrency_limits:
        - key: database
          value: redshift
          limit: 2
```

`max_concurrent` defaults to `multiprocessing.cpu_count()` (also when set to 0). `forkserver` cuts per-process startup cost but breaks some libraries. `tag_concurrency_limits` matches op `tags` (asset jobs match `op_tags`) and applies **per run only** — cross-run limits need pools (`@dg.op(pool="api")`) or a Celery executor. Step priority comes from the `dagster/priority` op tag, higher first.

The `multiprocess:` / `in_process:` selector keys exist only on the default executor. A job that sets `executor_def=dg.in_process_executor` directly takes that executor's own schema under `execution: config:` (`retries`, `marker_to_close`), and `execution: {config: {in_process: {}}}` is rejected for it.

## Gotchas

- **Ops cannot run alone.** An op needs a job (or a graph-backed asset) to be launchable. Invoking it directly is a unit-testing pattern only — see `testing-and-asset-checks.md`.
- **Op names must be unique within a graph.** Reuse auto-aliases (`add_one_2`); alias explicitly when the generated name would be confusing, and use the aliased name in `op_selection` and in `GraphDefinition` dependency dicts.
- **`execute_in_process` is not representative.** It ignores the job's `executor_def` and substitutes the in-process executor, and swaps the default filesystem I/O manager for an in-memory one. Use `dg.execute_job` when the executor is what you are testing.
- **Yielding and returning do not mix.** Once an op yields anything, its data output must also be yielded as `dg.Output`; conditional `dg.Out(is_required=False)` branches must therefore be yielded, never returned.
- **Dynamic outputs are not ordinary values.** Inside a composition function they accept only `.map()` and `.collect()`.
- **Asset job selection is code-location scoped.** Selected assets must all be present in the `Definitions` the job belongs to; `"my_asset*"` resolves downstream assets within that code location only.
- **Graph-backed assets are all-or-nothing by default.** Subsetting requires `can_subset=True` plus ops that yield their outputs conditionally on `context.selected_output_names`; otherwise selecting one asset materializes others unexpectedly.
- **Three live deprecations.** `ExpectationResult` and `JobDefinition.run_request_for_partition` are removed in 2.0; `define_asset_job(partitions_def=...)` is redundant because partitioning comes from the selected assets.
- The Dagster docs page on executing jobs shows `dg launch --jobs my_job`; the CLI reference defines the option as `--job`. Use `dg launch --job <name>`.

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| `@solid`, `SolidDefinition` | `@dg.op`, `dg.OpDefinition` |
| `@pipeline`, `PipelineDefinition` | `@dg.job` / `@dg.graph`, `dg.JobDefinition` |
| `@composite_solid`, `CompositeSolidDefinition` | `@dg.graph`, `dg.GraphDefinition` |
| `@lambda_solid` | `@dg.op` |
| `ModeDefinition(resource_defs=...)` | `resource_defs=` on `@dg.job` / `to_job()`, or several jobs from one graph |
| `PresetDefinition` | `config=` on `@dg.job` / `to_job()` |
| `InputDefinition` / `OutputDefinition` / `DynamicOutputDefinition` | `dg.In` / `dg.Out` / `dg.DynamicOut` |
| `execute_solid`, `execute_pipeline` | direct op invocation, `job.execute_in_process`, `dg.execute_job` |
| `config_schema=` + `context.op_config` | `config:` parameter annotated with a `dg.Config` subclass |
| `dg.ExpectationResult` | `dg.AssetCheckResult` with `@dg.asset_check` |
| `job.run_request_for_partition(...)` | `dg.RunRequest(partition_key=...)` |
| `DependencyDefinition.solid` | `DependencyDefinition.node` |

The `@solid`/`@pipeline` family was removed in Dagster 1.0.0; code using it will not import. Ops in a pipeline that merely writes data and reports it should usually become assets outright rather than ops in a job.

## Sources
- https://docs.dagster.io/guides/build/ops — op definition, ins/outs, config, context, op factory
- https://docs.dagster.io/guides/build/ops/op-events — Output, AssetMaterialization, AssetObservation, ExpectationResult, Failure, RetryRequested
- https://docs.dagster.io/guides/build/ops/graphs — composition, aliasing, branching, fan-in, Nothing deps, GraphDefinition, asset inputs
- https://docs.dagster.io/guides/build/ops/nesting-graphs — sub-graphs, GraphIn/GraphOut, sub-graph config mapping
- https://docs.dagster.io/guides/build/ops/dynamic-graphs — DynamicOut, DynamicOutput, map, collect, chaining, multiple dynamic outputs
- https://docs.dagster.io/guides/build/ops/op-hooks — success_hook, failure_hook, HookContext, with_hooks, testing hooks
- https://docs.dagster.io/guides/build/ops/op-retries — RetryPolicy, Backoff, Jitter, RetryRequested, allow_retries
- https://docs.dagster.io/guides/build/jobs/op-jobs — @job, to_job, hardcoded config, config mapping
- https://docs.dagster.io/guides/build/jobs/asset-jobs — define_asset_job
- https://docs.dagster.io/guides/build/jobs/job-execution — dg launch, execute_in_process, op selection, executor config
- https://docs.dagster.io/guides/build/jobs/unconnected-inputs — input managers, type loaders, defaults
- https://docs.dagster.io/guides/build/jobs/using-jobs — jobs in defs/, dg scaffold defs dagster.job
- https://docs.dagster.io/guides/build/assets/graph-backed-assets — graph_asset, graph_multi_asset, subsetting
- https://docs.dagster.io/guides/operate/run-executors — executor list, executor_def, code-location executor
- https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-ops — PartitionedConfig for op jobs
- https://docs.dagster.io/examples/best-practices/dynamic-fanout and .../dynamic-vs-parallel — fan-out with graph-backed assets; dynamic outputs vs Python multiprocessing
- https://docs.dagster.io/api/dagster/ops — @op, In, Out, RetryPolicy, Backoff, Jitter, event and exception classes
- https://docs.dagster.io/api/dagster/graphs — @graph, GraphDefinition, to_job, GraphIn, GraphOut, DependencyDefinition
- https://docs.dagster.io/api/dagster/jobs — @job, JobDefinition, reconstructable
- https://docs.dagster.io/api/dagster/execution — execute_job, ReexecutionOptions, executors, OpExecutionContext
- https://docs.dagster.io/api/dagster/dynamic — DynamicOut, DynamicOutput; https://docs.dagster.io/api/dagster/hooks — hook decorators and HookContext
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference — dg launch and dg scaffold options
