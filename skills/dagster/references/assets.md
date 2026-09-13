# Assets

How to define assets, wire up their dependencies, attach metadata/tags/owners/kinds, produce several assets at once, model external assets, version them, generate them from factories, and select them from the UI, CLI, and Python. Partitioning is in `partitions-and-backfills.md`; I/O managers in `io-managers.md`; asset checks in `testing-and-asset-checks.md`; automation conditions and freshness in `declarative-automation.md`; ops and graph internals in `ops-jobs-and-graphs.md`; resources and config in `resources-and-config.md`.

## Contents
- [Defining an asset](#defining-an-asset)
- [Dependencies](#dependencies)
- [Asset execution context](#asset-execution-context)
- [Metadata, tags, owners, and kinds](#metadata-tags-owners-and-kinds)
- [Multi-assets and graph-backed assets](#multi-assets-and-graph-backed-assets)
- [External assets and observations](#external-assets-and-observations)
- [Versioning, data versions, and staleness](#versioning-data-versions-and-staleness)
- [Asset factories](#asset-factories)
- [Asset selection](#asset-selection)
- [Choosing asset granularity](#choosing-asset-granularity)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Defining an asset

Use when you want Dagster to own an object in persistent storage (table, file, model). Scaffold with `dg scaffold defs dagster.asset <path/to/assets.py>`; the file must live under `src/<project>/defs/` to be autoloaded.

```python
# src/<project>/defs/assets.py
import dagster as dg


@dg.asset
def daily_sales() -> None: ...


@dg.asset(
    deps=[daily_sales],
    group_name="sales",
    owners=["bighead@hooli.com", "team:roof"],
)
def weekly_sales_report(context: dg.AssetExecutionContext):
    context.log.info("Loading data for my_dataset")
```

The asset key defaults to the function name. Verify with `dg check defs` then `dg list defs`.

### `@dg.asset` parameters

| Parameter | Type | Notes |
| --- | --- | --- |
| `name` | `str` | Overrides the function name as the last key segment. Letters/digits/`_` only, no Python keywords. |
| `key` | coercible to `AssetKey` | Full key. Mutually exclusive with `name` and `key_prefix`. |
| `key_prefix` | `str \| Sequence[str]` | Prepended to the name, producing a multi-part key. |
| `description` | `str` | Falls back to the function docstring when omitted. |
| `group_name` | `str` | Defaults to `"default"`. Also settable per-folder via `defs.yaml`. |
| `metadata` | `dict[str, Any]` | Definition metadata; see [Metadata](#metadata-tags-owners-and-kinds). |
| `tags` | `Mapping[str, str]` | String keys and values only; for filtering and organizing. |
| `kinds` | `set[str]` | Up to ten; renders technology icons in the UI. |
| `owners` | `Sequence[str]` | Email address or `team:<name>`. |
| `code_version` | `str` | Drives "Unsynced" status and data versions. |
| `deps` | `Sequence[AssetDep \| AssetsDefinition \| AssetSpec \| AssetKey \| str]` | Lineage-only upstreams; no data is loaded. |
| `ins` | `Mapping[str, AssetIn]` | Per-input key/prefix/metadata/partition mapping. |
| `io_manager_key` | `str` | Resource key used to store the return value. Defaults to `"io_manager"`. |
| `partitions_def` | `PartitionsDefinition` | See `partitions-and-backfills.md`. |
| `automation_condition` | `AutomationCondition` | See `declarative-automation.md`. |
| `backfill_policy` | `BackfillPolicy` | Beta. See `partitions-and-backfills.md`. |
| `retry_policy` | `RetryPolicy` | `dg.RetryPolicy(max_retries=..., delay=..., backoff=..., jitter=...)`. |
| `op_tags` | `dict[str, Any]` | Tags on the underlying op / run, not the asset. Values must survive `json.loads(json.dumps(v)) == v`. |
| `pool` | `str` | Concurrency pool governing execution. |
| `config_schema` / `Config` param | — | Prefer a `dg.Config` subclass parameter; see `resources-and-config.md`. |
| `required_resource_keys` | `set[str]` | Legacy access via `context.resources`. Prefer typed resource parameters. |
| `check_specs` | `Sequence[AssetCheckSpec]` | Checks that run inside this asset's function; see `testing-and-asset-checks.md`. |
| `output_required` | `bool` | `False` lets the function `yield` nothing to skip materialization. Only works with `yield`, never `return`. |
| `is_virtual` | `bool` | Preview. Marks a pass-through asset (a database view); staleness resolves through it rather than at it. |
| `hooks` | `AbstractSet[HookDefinition]` | Hooks executed on materialization. |

### Multi-part keys

```python
@dg.asset(key_prefix=["one", "two", "three"])
def upstream_asset():
    return [1, 2, 3]


@dg.asset(ins={"upstream_asset": dg.AssetIn(key_prefix=["one", "two", "three"])})
def downstream_asset(upstream_asset):
    return upstream_asset + [4]
```

In selection strings the key renders slash-separated: `key:"one/two/three/upstream_asset"`.

## Dependencies

Decision rule: if the downstream function needs the upstream *value* in memory, take it as a parameter (or declare it in `ins`) and let an I/O manager load it. If the downstream reads the data itself (SQL against a warehouse, a file it knows the path of), use `deps` — that records lineage only and passes nothing.

```python
# deps: lineage only, you do your own I/O
@dg.asset
def sugary_cereals() -> None:
    execute_query(
        "CREATE TABLE sugary_cereals AS SELECT * FROM cereals WHERE sugar_grams > 10"
    )


@dg.asset(deps=[sugary_cereals])
def shopping_list() -> None:
    execute_query("CREATE TABLE shopping_list AS SELECT * FROM sugary_cereals")
```

```python
# parameter: the I/O manager loads the upstream value
@dg.asset
def combined_data(people: pd.DataFrame, birds: pd.DataFrame) -> pd.DataFrame: ...
```

`deps` accepts the upstream `AssetsDefinition` object, an `AssetSpec`, a bare string, a `dg.AssetKey`, or a `dg.AssetDep` (which adds `partition_mapping=`). Passing a `@multi_asset`'s definition creates dependencies on every asset it produces.

### `dg.AssetIn`

Use when the input name does not match the upstream asset key, or when the input needs its own metadata or partition mapping. Fields: `key`, `key_prefix`, `metadata`, `partition_mapping`, `dagster_type`.

### Across code locations

Assets in different code locations cannot materialize in the same run; trigger downstream work with a sensor or an automation condition. For lineage only, depend on the key:

```python
@dg.asset(deps=[dg.AssetKey("daily_sales_data")])
def weekly_sales_report() -> None: ...
```

To actually load the upstream value, redeclare it locally as an `AssetSpec` carrying an I/O manager key, and include the spec in `Definitions`:

```python
daily_sales_data = dg.AssetSpec(key="daily_sales_data").with_io_manager_key(
    "warehouse_io_manager"
)


@dg.asset
def enriched_sales_data(daily_sales_data: list) -> list: ...


defs = dg.Definitions(
    assets=[daily_sales_data, enriched_sales_data],
    resources={"warehouse_io_manager": warehouse_io_manager},
)
```

## Asset execution context

Declare `context: dg.AssetExecutionContext` as the first parameter to get it.

| Member | Use |
| --- | --- |
| `context.log` | Structured logger (`.info`, `.warning`, ...). |
| `context.asset_key` | Key of the asset being materialized. |
| `context.run.run_id`, `context.run_id` | Current run. |
| `context.instance` | `DagsterInstance`; use for `report_runless_asset_event`, event-log queries. |
| `context.partition_key` | Current partition key. Raises if unpartitioned. |
| `context.has_partition_key` | Guard before reading `partition_key`. |
| `context.partition_keys`, `context.partition_key_range` | Multi-partition runs. |
| `context.partition_time_window` | `TimeWindow` for time-partitioned assets. |
| `context.add_asset_metadata(metadata, asset_key=..., partition_key=...)` | Attach runtime metadata without returning `MaterializeResult`. |
| `context.add_output_metadata(metadata, output_name=...)` | Op-output-level equivalent. |
| `context.asset_key_for_output(output_name)` / `asset_key_for_input(...)` | Map op output/input names to asset keys. |
| `context.selected_asset_keys` | Which assets of a subsettable multi-asset this run must produce. |
| `context.resources` | Legacy resource access; prefer typed parameters. |
| `context.log_event(dg.AssetObservation(...))` | Emit an observation mid-run. |
| `context.get_asset_provenance(key)` | Code/data versions behind the last materialization. |

## Metadata, tags, owners, and kinds

Decision rule: **tags** are string/string pairs for filtering and search; **metadata** is richer structured data for display; **owners** is who to page; **kinds** is which technology the asset lives in.

### Definition vs runtime metadata

Definition metadata is fixed at load time and is visible to I/O managers (`context.definition_metadata` in `handle_output`, `context.upstream_output.definition_metadata` in `load_input`). Runtime metadata is written to the event log per materialization and is rendered as a time series when numeric. Runtime metadata is **not** readable from `InputContext` — an I/O manager cannot read back the metadata a `MaterializeResult` recorded.

```python
@dg.asset(
    metadata={
        "link_to_docs": dg.MetadataValue.url("https://..."),
        "snippet": dg.MetadataValue.md("# Embedded markdown\n..."),
    }
)
def my_asset():
    return dg.MaterializeResult(
        metadata={"dagster/row_count": 1234, "file_size_kb": dg.MetadataValue.int(88)}
    )
```

`dg.MaterializeResult` fields: `asset_key` (optional in `@asset`, required in `@multi_asset`), `metadata`, `tags`, `data_version`, `check_results`, `value`.

### `dg.MetadataValue` constructors

| Constructor | Renders as |
| --- | --- |
| `int`, `float`, `bool`, `null` | Scalars; numerics become a time series in the UI. |
| `text`, `md` | Plain text, Markdown. |
| `url`, `path`, `notebook` | Clickable link, filesystem path, notebook path. |
| `json` | Expandable JSON tree. |
| `table` | `records=[TableRecord(...)]` plus a `schema=TableSchema(...)`. |
| `table_schema` | A `TableSchema`. |
| `column_lineage` | A `TableColumnLineage`. |
| `timestamp` | Unix timestamp rendered as a date. |
| `asset`, `job`, `dagster_run` | Links to another asset, job, or run. |
| `python_artifact`, `pool` | Python object reference, concurrency pool. |
| `value` | Auto-wraps a raw Python value in the right type. |

Raw Python primitives are coerced automatically; anything else must be wrapped in a `MetadataValue` or be JSON-serializable.

### Reserved `dagster/` metadata keys

| Key | Type | Meaning |
| --- | --- | --- |
| `dagster/uri` | `str` | Asset location, e.g. `s3://my_bucket/my_object`. |
| `dagster/table_name` | `str` | Fully qualified table/view identifier. |
| `dagster/row_count` | `int` | Rows in the materialized table. |
| `dagster/partition_row_count` | `int` | Rows in the materialized partition. |
| `dagster/column_schema` | `TableSchema` | Column names, types, descriptions. |
| `dagster/column_lineage` | `TableColumnLineage` | Column-to-column provenance. |
| `dagster/code_references` | `CodeReferencesMetadataValue` | Source links. Definition metadata only, never materialization metadata. |

```python
@dg.asset(
    metadata={
        "dagster/column_schema": dg.TableSchema(
            columns=[dg.TableColumn("name", "string", description="Person name")]
        )
    },
)
def my_asset(): ...
```

`dg.TableSchema.from_name_type_dict({"name": "string", "age": "int"})` is the shorthand. Column lineage uses `dg.TableColumnLineage(deps_by_column={"out_col": [dg.TableColumnDep(asset_key=dg.AssetKey("src"), column_name="in_col")]})`. dbt and similar integrations attach both automatically.

### Tags, labels, and kinds

```python
@dg.asset(tags={"domain": "marketing", "pii": "true"}, kinds={"python", "snowflake"})
def my_asset(): ...
```

Tag keys are `prefix/name` with each segment 63 characters or less, alphanumerics plus `-`, `_`, `.`. Values follow the same character rules. A tag whose value is `""` renders as a bare label. Dagster reserves the `dagster/` prefix: it writes `dagster/kind/{kind}` on assets and `dagster/code_location`, `dagster/user`, `dagster/partition`, `dagster/schedule_name`, `dagster/sensor_name`, `dagster/backfill`, `dagster/parent_run_id`, `dagster/image`, `dagster/op_selection` on runs. Kinds are stored as those `dagster/kind/*` tags; roughly 200 technology names get branded icons (`python`, `snowflake`, `dbt`, `duckdb`, `s3`, `bigquery`, ...). `compute_kind` is superseded by `kinds`.

To set group, owners, tags, or metadata for a whole subdirectory of `defs/`, add a `defs.yaml` configuring `dagster.components.DefsFolderComponent` with attribute transforms, optionally narrowed by a `target` selection string. See `components.md`.

## Multi-assets and graph-backed assets

Use `@dg.multi_asset` when one operation produces several assets: a single API call updating several tables, or a chain that should pass data in memory with no intermediate storage. Scaffold with `dg scaffold defs dagster.multi_asset <path>`.

```python
@dg.multi_asset(specs=[dg.AssetSpec("asset_one"), dg.AssetSpec("asset_two")])
def my_multi_asset():
    yield dg.MaterializeResult(asset_key="asset_one", metadata={"num_rows": 10})
    yield dg.MaterializeResult(asset_key="asset_two", metadata={"num_rows": 24})
```

`specs=` is the form to use when the function does its own I/O; each `AssetSpec` carries its own deps, metadata, tags, group, and automation condition. Use `outs={"name": dg.AssetOut(...)}` instead when an I/O manager should store the returned values; the function then returns a tuple in output order. `AssetOut` accepts `key`, `key_prefix`, `description`, `io_manager_key`, `metadata`, `group_name`, `code_version`, `owners`, `tags`, `kinds`, `automation_condition`, `backfill_policy`, `is_required`, and `AssetOut.from_spec(spec)`.

Other parameters worth knowing: `deps`, `ins`, `can_subset`, `internal_asset_deps`, `partitions_def`, `check_specs`, `group_name`, `code_version`, `retry_policy`, `op_tags`, `pool`.

- `can_subset=True` is required before any single asset of the multi-asset can be materialized alone. The function must then consult `context.selected_asset_keys` and emit only those. Without it, selecting one asset selects all of them.
- `internal_asset_deps={"out_name": {dg.AssetKey("x")}}` corrects the default assumption that every output depends on every input. Keys listed must be inputs to, or produced by, the multi-asset.

`@dg.graph_asset` composes several ops into one asset, exposing op boundaries for re-execution without making intermediates into assets. `@dg.graph_multi_asset` does the same for several outputs. Subsetting a graph-backed asset requires `can_subset=True` *and* ops that yield their outputs conditionally on `context.selected_output_names`; an op that unconditionally returns an output that is also a graph output will materialize that asset unexpectedly. Op mechanics live in `ops-jobs-and-graphs.md`.

```python
@dg.graph_asset
def complex_asset():  # step_one and step_two are @dg.op
    return step_two(step_one())
```

## External assets and observations

Use when another system produces the data and Dagster only needs to know it exists. A bare `dg.AssetSpec` in `Definitions` is an external asset: it appears in the catalog and lineage but Dagster cannot materialize or schedule it.

```python
raw_transactions = dg.AssetSpec("raw_transactions")
stg_data = dg.AssetSpec("stg_data", deps=[raw_transactions])


@dg.asset(deps=[stg_data])
def cleaned_transactions(): ...
```

Three ways to tell Dagster the external asset changed:

1. **Sensor** polling the external system, returning `dg.SensorResult(asset_events=[dg.AssetMaterialization(asset_key=..., metadata=...)], cursor=...)`. See `schedules-and-sensors.md`.
2. **REST API** push: `POST /report_asset_materialization/` with `{"asset_key": ..., "metadata": {...}}`, against `http://localhost:3000` for OSS or `https://<org>.dagster.cloud/<deployment>/` with a `Dagster-Cloud-Api-Token` header for Dagster+.
3. **`DagsterInstance.report_runless_asset_event`**, which is also how you simulate external materializations in tests:

```python
instance.report_runless_asset_event(dg.AssetMaterialization(asset_key="raw_transactions"))
```

`dg.AssetObservation` records metadata about an asset *without* claiming it changed. Log it from a run with `context.log_event(dg.AssetObservation(asset_key=..., metadata=..., partition=...))`. On Dagster+, observations do not consume credits while materializations do, so prefer observations when mirroring events from external systems.

For data-version-driven staleness on an external input, the docs' current recommendation is `@dg.observable_source_asset` (beta), whose function returns a `dg.DataVersion`, a `DataVersionsByPartition`, or a `dg.ObserveResult`:

```python
@dg.observable_source_asset
def foo_source_asset():
    hash_sig = sha256()
    hash_sig.update(bytearray(read_some_file(), "utf8"))
    return dg.DataVersion(hash_sig.hexdigest())
```

The UI exposes an **Observe sources** button when any observable source asset is in the graph. Observations can also run on a schedule via `dg.define_asset_job("observation_job", [foo_source_asset])`; a job's selection must be *only* observable source assets, mixing them with materializable assets raises an error. `@dg.multi_observable_source_asset` (beta) observes several at once.

## Versioning, data versions, and staleness

`code_version` is a string you bump when the asset's logic changes. A data version is a fingerprint of the asset's value; by default Dagster hashes the code version together with the input assets' data versions, so an unversioned asset gets a fresh code version (the run ID) on every run.

Set it with `@dg.asset(code_version="v2")`.

The UI marks an asset **Unsynced** when its code version changed, its dependency set changed, or a parent's data version changed since its last materialization. Unsynced status is not transitive: a grandchild only becomes unsynced once its direct parent is rematerialized with a new data version. **Materialize unsynced** in the UI runs only what is out of date.

Supply your own data version when the computation is random, or when code versions are auto-generated and cosmetic refactors would otherwise churn the hash. Return `dg.MaterializeResult(data_version=dg.DataVersion(...))` from the asset (`dg.Output(value, data_version=...)` is the older form). An explicit data version supersedes the code version for downstream staleness, so a refactor that leaves the value unchanged leaves downstreams synced.

## Asset factories

Use when many assets share one shape and differ only by configuration. A factory is a plain function returning an `AssetsDefinition` or a `dg.Definitions`, merged with `dg.Definitions.merge(...)`:

```python
# src/<project>/defs/etl_factory.py
def build_etl_job(bucket: str, source_object: str, target_object: str, sql: str) -> dg.Definitions:
    # asset keys cannot contain "."
    asset_key = f"etl_{bucket}_{target_object}".replace(".", "_")

    @dg.asset(name=asset_key)
    def etl_asset(context): ...

    return dg.Definitions(assets=[etl_asset])


@dg.definitions
def defs():
    return dg.Definitions.merge(*[build_etl_job(**job) for job in etl_jobs])
```

Downstream assets depend on generated assets by key string, so keep the key-construction rule documented next to the factory. For a list of `AssetSpec`s, `dg.map_asset_specs(func, iterable)` applies `spec.replace_attributes(...)` or `spec.merge_attributes(...)` across specs and `AssetsDefinition`s uniformly — the clean way to stamp owners or tags on a generated set.

The recommended packaging for a factory that non-Python users should configure is a custom component: expose a `dg.Model` with the per-asset fields (including a `deps` field for inter-asset dependencies), and let `defs.yaml` drive it. See `components.md`.

## Asset selection

The same string grammar works in the UI search bars, in `dg list defs --assets '<query>'`, in `dg launch --assets '<query>'`, and as the `selection=` argument to `dg.define_asset_job`.

| Syntax | Selects |
| --- | --- |
| `key:"my_asset"` | Exact key. Multi-part keys use slashes: `key:"my_prefix/raw_data_a"`. |
| `key:"prefix_*"` | Wildcard. Only `key` supports wildcards. |
| `tag:"stage"` / `tag:"stage"="prod"` | Tag presence / tag value. |
| `owner:"alice"` | Owner email or `team:` name. |
| `group:"team1"` | Group. |
| `kind:"table"` | Kind. |
| `code_location:"repo1"` | Code location. |
| `+key:"a"` / `key:"a"+` | All upstream / all downstream. |
| `2+key:"a"+2` | Two layers up and two layers down. |
| `and`, `or`, `not`, `(...)` | Boolean composition. |
| `roots(sel)` / `sinks(sel)` | Assets with no upstream / no downstream *within* the selection. |
| `*` | Everything, e.g. `dg launch --assets "*"`, `roots(*)`. |

Dagster+ only: `column_tag:`, `columns:`, `table_name:`, `status:` (`HEALTHY`/`WARNING`/`DEGRADED`/`UNKNOWN`/`NOT_APPLICABLE`). Branch deployments only: `changed_in_branch:` with `ANY`, `CODE_VERSION`, `DEPENDENCIES`, `METADATA`, `NEW`, `PARTITIONS_DEFINITION`, or `TAGS`.

Worked examples:

```shell
dg list defs --assets 'key:"raw_data_b"+ and +key:"summary_stats_2"'   # path between two assets
dg launch --assets '(key:"raw_data_a" or key:"raw_data_b")+ and +key:"b_c_for_sales"'
dg launch --assets 'group:"sensitive_data"+ and +group:"public_data"'  # path between two groups
dg launch --assets 'key:"raw_data_c"+ and +key:"combo_a_b_c_data"+ and +key:"summary_stats_1"'
dg launch --assets 'key:"summary_stats_1" or key:"summary_stats_2"'
dg launch --assets 'key:"combo_a_b_c_data"+ and kind:"csv"'
dg launch --assets 'owner:"nora@example.com" and not tag:"customer_data"'
dg launch --assets '(owner:"team:sales" and kind:"csv") or group:"public_data"'
dg launch --assets 'not (kind:"s3" or kind:"csv") and key:"raw*"'
dg launch --assets 'sinks(group:"public_data"+)'
```

The Python API mirrors it. `dg.AssetSelection` supports `|` (union), `&` (intersection), and `-` (difference); selecting assets also selects the checks that target them.

```python
dg.AssetSelection.groups("marketing") | dg.AssetSelection.assets("promotion")
dg.AssetSelection.groups("marketing") & dg.AssetSelection.assets("leads").downstream()
dg.AssetSelection.all() - dg.AssetSelection.groups("marketing")
dg.AssetSelection.assets("projections").required_multi_asset_neighbors()
dg.AssetSelection.groups("marketing") - dg.AssetSelection.all_asset_checks()
```

Constructors: `all()`, `all_asset_checks()`, `assets(*keys_or_defs)`, `checks(...)`, `checks_for_assets(...)`, `groups(...)`, `key_prefixes(...)`, `key_substring(...)`, `kind(kind)`, `owner(owner)`, `tag(key, value)`. Traversals and filters: `upstream(depth=, include_self=)`, `downstream(depth=, include_self=)`, `roots()`, `sinks()`, `materializable()`, `required_multi_asset_neighbors()`, `upstream_source_assets()`, `without_checks()`. The kind constructor is singular `kind(...)`, not `kinds(...)`.

## Choosing asset granularity

| Approach | Retry a phase | Materialize one phase | I/O between phases | Best for |
| --- | --- | --- | --- | --- |
| One asset doing extract + load + transform | No | No | In memory | Prototypes, steps that always run together |
| One asset per phase, chained with `deps` or parameters | Yes | Yes | I/O manager or external storage | Production, debugging |
| `@multi_asset` producing several outputs | No, all-or-nothing | Only with `can_subset` | In memory | Tightly coupled work with several downstream consumers |

Modeling assets as *nouns* (the table produced) rather than *verbs* (download, unzip, load) is the point of the abstraction. A chain of task-shaped assets that only works because they share a filesystem should usually collapse into one asset.

## Gotchas

- The function name is the asset key. Renaming the function renames the asset and orphans its materialization history.
- Name assets after the data they produce, not the action. `weekly_sales` not `compute_weekly_sales`.
- Two assets with the same key in one code location fail at load. Run `dg check defs` before pushing.
- `deps` passes no data. A downstream function that also takes the upstream as a *parameter* while listing it in `deps` is a duplicate dependency; pick one.
- Asset keys cannot contain `.`; sanitize generated keys in factories.
- `@multi_asset` without `can_subset=True` always executes every one of its assets, whatever the selection.
- With `outs=`, returned values must match the output order; with `specs=`, yield one `MaterializeResult(asset_key=...)` per asset.
- Metadata values must be JSON-serializable or wrapped in `dg.MetadataValue`. Tag keys and values must be strings.
- `output_required=False` only works with `yield`. A `return` always produces a materialization, even of `None`.
- Runtime metadata is invisible to I/O managers; put anything an I/O manager needs in definition metadata.
- The function name of a `@multi_asset` is not itself an asset; it is the op that materializes the declared assets.
- Assets in different code locations never materialize in the same run.

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| `dg.SourceAsset` | `dg.AssetSpec`; for the I/O manager case, `dg.AssetSpec(...).with_io_manager_key(...)` |
| `non_argument_deps=` | `deps=` |
| `dg.Output(value, metadata=...)` from an asset | `dg.MaterializeResult(metadata=..., data_version=...)` |
| `AssetGroup` | `group_name=` on the asset, or a `defs.yaml` folder transform |
| `compute_kind="dbt"` | `kinds={"dbt"}` |
| `AssetSelection.keys(...)` | `AssetSelection.assets(...)` |
| `AssetSelection.sources()` | `AssetSelection.roots()` |
| `@repository`, `workspace.yaml`, manual `Definitions` assembly | `defs/` autoloading via `create-dagster` + `dg` |

## Sources
- https://docs.dagster.io/guides/build/assets — assets overview, assets vs ops
- https://docs.dagster.io/guides/build/assets/defining-assets — decorators, context, code versions, multi-part keys
- https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies — `deps`, cross-code-location dependencies
- https://docs.dagster.io/guides/build/assets/passing-data-between-assets — external storage vs I/O managers vs combining
- https://docs.dagster.io/guides/build/assets/modeling-etl-pipelines — granularity comparison
- https://docs.dagster.io/guides/build/assets/configuring-assets — `Config` schemas and the Launchpad
- https://docs.dagster.io/guides/build/assets/external-assets — `AssetSpec`, sensors, REST reporting
- https://docs.dagster.io/guides/build/assets/virtual-assets — `is_virtual` (preview)
- https://docs.dagster.io/guides/build/assets/graph-backed-assets — `@graph_asset`, `@graph_multi_asset`, subsetting
- https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching — code versions, data versions, unsynced
- https://docs.dagster.io/guides/build/assets/creating-asset-factories — component and pure-Python factories
- https://docs.dagster.io/guides/build/assets/defining-dependencies-with-asset-factories — `deps` in factory models
- https://docs.dagster.io/guides/build/assets/metadata-and-tags — owners, tags, definition vs runtime metadata, standard keys
- https://docs.dagster.io/guides/build/assets/metadata-and-tags/tags — tag key/value rules, labels, system tags
- https://docs.dagster.io/guides/build/assets/metadata-and-tags/kind-tags — `kinds`, supported icons
- https://docs.dagster.io/guides/build/assets/metadata-and-tags/table-metadata — column schema, row count, column lineage
- https://docs.dagster.io/guides/build/assets/metadata-and-tags/asset-observations — `AssetObservation`, observable source assets
- https://docs.dagster.io/guides/build/assets/metadata-and-tags/adding-attributes-to-assets — folder-level attribute transforms
- https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference — filters, layers, operands, functions
- https://docs.dagster.io/guides/build/assets/asset-selection-syntax/examples — worked selection queries
- https://docs.dagster.io/api/dagster/assets — `@asset`, `AssetSpec`, `multi_asset`, `AssetIn`, `AssetDep`, `AssetSelection`
- https://docs.dagster.io/api/dagster/metadata — `MetadataValue`, `TableSchema`, `TableColumnLineage`
- https://docs.dagster.io/api/dagster/execution — `AssetExecutionContext`
- https://docs.dagster.io/dagster-basics-tutorial/assets — scaffolding and materializing a first asset
