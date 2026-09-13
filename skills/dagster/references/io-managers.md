# I/O managers

How data moves between assets: the built-in I/O managers, wiring and overrides, writing custom ones, and the database / object-store I/O managers shipped by integration libraries. Asset definition itself is in `assets.md`; partition mappings and backfills are in `partitions-and-backfills.md`; resource construction, `EnvVar`, and environment-specific resource swapping are in `resources-and-config.md`; op-level `Out`/`In` mechanics are in `ops-jobs-and-graphs.md`.

## Contents

- [Decision rule: I/O manager or `deps=`](#decision-rule-io-manager-or-deps)
- [Default behavior and wiring](#default-behavior-and-wiring)
- [Per-asset and per-input overrides](#per-asset-and-per-input-overrides)
- [Custom I/O managers](#custom-io-managers)
- [Partitions in a custom I/O manager](#partitions-in-a-custom-io-manager)
- [Path-based stores: `UPathIOManager`](#path-based-stores-upathiomanager)
- [Database I/O managers: shared conventions](#database-io-managers-shared-conventions)
- [Database I/O managers by library](#database-io-managers-by-library)
- [Object-store pickle I/O managers](#object-store-pickle-io-managers)
- [Testing I/O managers](#testing-io-managers)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Decision rule: I/O manager or `deps=`

Use an I/O manager when the asset body is a pure in-memory transform: load upstream, transform, return. Declare the dependency as a **function parameter** and return the value.

| Situation | Use |
| --- | --- |
| Assets share a storage location and a consistent path/table rule | I/O manager |
| Storage differs per environment (local / staging / prod) but logic does not | I/O manager |
| SQL that creates or updates a table in place | `deps=` |
| dbt, Spark-submit, Fivetran, or any tool that writes storage itself | `deps=` |
| Data will not fit in memory (billions of rows) | `deps=` |
| Pipeline gets *more* complicated in order to satisfy the I/O manager | `deps=` |

```python
# I/O manager handles storage: parameter in, value out
@dg.asset
def clean_sales_data(raw_sales_data: pd.DataFrame) -> pd.DataFrame:
    return raw_sales_data.fillna({"amount": 0.0})


# deps= skips I/O managers entirely: nothing is loaded, nothing is stored
@dg.asset(deps=[orders])
def returns():
    conn = get_snowflake_connection()
    conn.execute("CREATE TABLE returns AS SELECT * FROM orders WHERE status = 'RETURNED'")
```

A third option: if two assets only exist to hand data to each other on the same filesystem, collapse them into one asset. See `assets.md`.

## Default behavior and wiring

With no `io_manager` resource configured, Dagster uses `dg.FilesystemIOManager` — pickle files. Base directory resolution, in order:

1. the manager's `base_dir` config value;
2. `storage/` under `local_artifact_storage` in `dagster.yaml`;
3. `storage/` under `$DAGSTER_HOME`;
4. a temporary directory (this is what `dg dev` gets when `DAGSTER_HOME` is unset — storage is ephemeral).

Each asset maps to one path. `AssetKey(["one", "two", "three"])` is stored as file `three` under `<base_dir>/one/two/`. Re-materializing overwrites.

`dg.InMemoryIOManager` stores values in memory; they are garbage-collected after the run, so no run sees another run's values. `dg.materialize_to_memory(...)` forces it for every required I/O manager key and raises `DagsterInvariantViolationError` if you also pass an I/O manager in `resources`.

Provide an I/O manager under the reserved resource key `io_manager` to make it the default for every asset and op:

```python
# src/<project_name>/defs/resources.py
from dagster_duckdb_pandas import DuckDBPandasIOManager

import dagster as dg


@dg.definitions
def resources():
    return dg.Definitions(
        resources={"io_manager": DuckDBPandasIOManager(database="sales.duckdb", schema="public")}
    )
```

Swapping stores means swapping this one object; asset bodies do not change. Scaffold the file with `dg scaffold defs dagster.resources src/<project_name>/defs/resources.py`, then confirm with `dg check defs`.

## Per-asset and per-input overrides

```python
# Route one asset to a different manager
@dg.asset(io_manager_key="warehouse_io_manager")
def iris_dataset() -> pd.DataFrame: ...


@dg.asset(io_manager_key="blob_io_manager")
def iris_plots(iris_dataset): ...


# Load one input differently from how it was stored
@dg.asset(ins={"upstream": dg.AssetIn(key_prefix="public", input_manager_key="numpy_manager")})
def downstream_asset(upstream: np.ndarray) -> tuple:
    return upstream.shape


defs = dg.Definitions(
    assets=[iris_dataset, iris_plots, downstream_asset],
    resources={
        "warehouse_io_manager": SnowflakePandasIOManager(...),
        "blob_io_manager": S3PickleIOManager(...),
        "numpy_manager": NumpyAssetIOManager(),
    },
)
```

`input_manager_key` resolves against the same `resources` dict. For ops the equivalent is `dg.In(input_manager_key=...)` and `dg.Out(io_manager_key=...)`.

Because loading is coupled to how the output was written, define an input manager as a **subclass of the storing I/O manager** that overrides only `load_input`, and factor shared path logic into a helper method both use:

```python
class NumpyAssetIOManager(PandasAssetIOManager):
    def load_input(self, context: dg.InputContext) -> np.ndarray:
        return load_numpy_array(name=self._get_path(context))
```

## Custom I/O managers

Use when you need a storage layout the built-ins do not produce. Subclass `dg.ConfigurableIOManager` (pydantic config fields as class attributes) and implement `handle_output` and `load_input`.

```python
# src/<project_name>/defs/resources.py
import dagster as dg


class MyIOManager(dg.ConfigurableIOManager):
    path_prefix: list[str] = []

    def _get_path(self, context) -> str:
        return "/".join(self.path_prefix + context.asset_key.path)

    def handle_output(self, context: dg.OutputContext, obj):
        write_csv(self._get_path(context), obj)
        context.add_output_metadata({"num_rows": len(obj)})

    def load_input(self, context: dg.InputContext):
        return read_csv(self._get_path(context))
```

Useful context members:

| Member | Available on | Purpose |
| --- | --- | --- |
| `asset_key` / `has_asset_key` | both | Destination path or table name |
| `definition_metadata` | both | Asset-definition metadata (output side); `AssetIn`/`In` metadata (input side) |
| `upstream_output` | `InputContext` | The upstream `OutputContext` — read `.definition_metadata`, `.name`, `.step_key` |
| `dagster_type.typing_type` | both | Dispatch on the annotated Python type |
| `has_partition_key` / `partition_key` | both | Single-partition writes and reads |
| `has_asset_partitions` | both | Whether the asset is partitioned at all |
| `asset_partition_key` / `asset_partition_keys` / `asset_partition_key_range` | both | One or many partitions |
| `asset_partitions_time_window` | both | `dg.TimeWindow` for time-window partitions |
| `add_output_metadata` / `log_event` | `OutputContext` | Attach metadata or emit events at store time |
| `name` / `step_key` / `run_id` | both (`upstream_output` on inputs) | Op-output identity when there is no asset key |
| `resources` / `resource_config` | both | Other resources this manager requires |

Reading destination names from definition metadata keeps table names out of the manager:

```python
class MetadataReadingIOManager(dg.ConfigurableIOManager):
    def handle_output(self, context: dg.OutputContext, obj):
        write_dataframe_to_table(name=context.definition_metadata["table_name"], dataframe=obj)

    def load_input(self, context: dg.InputContext):
        if context.upstream_output:
            return read_dataframe_from_table(
                name=context.upstream_output.definition_metadata["table_name"]
            )
```

When the manager wraps a non-pydantic client or holds state (a connection, a cache), split config from implementation with `dg.ConfigurableIOManagerFactory`:

```python
class ExternalIOManager(dg.IOManager):
    def __init__(self, api_token):
        self._api_token = api_token
        self._cache = {}

    def handle_output(self, context: dg.OutputContext, obj): ...
    def load_input(self, context: dg.InputContext): ...


class ConfigurableExternalIOManager(dg.ConfigurableIOManagerFactory):
    api_token: str

    def create_io_manager(self, context) -> ExternalIOManager:
        return ExternalIOManager(self.api_token)
```

## Partitions in a custom I/O manager

Each `handle_output` call writes exactly one partition; a `load_input` call may need one or many. Filesystem-backed managers map a partition to a file; database-backed managers map it to a row range.

```python
class MyPartitionedIOManager(dg.IOManager):
    def _get_path(self, context) -> str:
        if context.has_partition_key:
            return "/".join(context.asset_key.path + [context.asset_partition_key])
        return "/".join(context.asset_key.path)

    def handle_output(self, context: dg.OutputContext, obj):
        write_csv(self._get_path(context), obj)

    def load_input(self, context: dg.InputContext):
        return read_csv(self._get_path(context))
```

For time-window partitions use `context.asset_partitions_time_window`. For fan-in (a daily downstream over hourly upstream), read `context.asset_partition_keys` or `context.asset_partition_key_range` and return a dict. The default I/O manager and any `UPathIOManager` subclass already do this; the downstream asset must annotate the input as `dict[str, T]`:

```python
@dg.asset(partitions_def=daily_partitions)
def downstream_asset(upstream_asset: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(list(upstream_asset.values()))  # 24 hourly partitions -> 1 day
```

Which upstream partitions arrive is controlled by `dg.AssetIn(partition_mapping=...)`; see `partitions-and-backfills.md`.

## Path-based stores: `UPathIOManager`

Use for any local or cloud filesystem. Subclass it and implement `dump_to_path` / `load_from_path` against a `UPath` (universal-pathlib + fsspec), and partition handling, `PartitionMapping`-aware multi-partition loads, concurrent async loads, and customizable output metadata come for free. Setting the `allow_missing_partitions` metadata value to `True` skips missing partitions instead of raising.

```python
import pandas as pd
from upath import UPath

import dagster as dg


class PandasParquetIOManager(dg.UPathIOManager):
    extension: str = ".parquet"

    def dump_to_path(self, context: dg.OutputContext, obj: pd.DataFrame, path: UPath):
        with path.open("wb") as file:
            obj.to_parquet(file)

    def load_from_path(self, context: dg.InputContext, path: UPath) -> pd.DataFrame:
        with path.open("rb") as file:
            return pd.read_parquet(file)


class S3ParquetIOManager(dg.ConfigurableIOManagerFactory):
    base_path: str

    def create_io_manager(self, context) -> PandasParquetIOManager:
        return PandasParquetIOManager(base_path=UPath(self.base_path))
```

## Database I/O managers: shared conventions

Every library DB I/O manager follows the same rules, so learn them once.

- **Asset key to table.** The last key component is the table name; a `key_prefix` supplies the schema (BigQuery: the dataset).
- **Schema precedence.** `metadata={"schema": ...}` on the asset wins, then the I/O manager's `schema` config, then `key_prefix`, then `public`. For ops, use `Out(metadata={"schema": ...})`.
- **Column subsets.** `dg.AssetIn(key="my_table", metadata={"columns": ["a", "b"]})` loads only those columns, so downstream assets do not pay for the full table.
- **Partitions to a column.** `metadata={"partition_expr": "SPECIES"}` names the column holding partition bounds. Static partitions generate `WHERE [partition_expr] IN (...)`; time-window partitions generate `WHERE [partition_expr] >= [start] AND [partition_expr] < [end]` with bounds formatted `YYYY-MM-DD HH:MM:SS`. `partition_expr` is injected raw, so it may be a SQL expression (`"TO_TIMESTAMP(TIME::INT)"`), and it must be valid in that dialect.
- **Multi-partitions.** Pass a dict: `metadata={"partition_expr": {"date": "TO_TIMESTAMP(TIME::INT)", "species": "SPECIES"}}`. The `WHERE` clauses are concatenated.
- **Unannotated values.** Each `<Engine><Frame>IOManager` loads and stores unannotated inputs/outputs as its own frame type.
- **Mixing frame types.** Subclass the engine base class and return several handlers from `type_handlers()`, plus `default_load_type()` for unannotated assets:

```python
from dagster_snowflake import SnowflakeIOManager
from dagster_snowflake_pandas import SnowflakePandasTypeHandler
from dagster_snowflake_pyspark import SnowflakePySparkTypeHandler


class SnowflakePandasPySparkIOManager(SnowflakeIOManager):
    @staticmethod
    def type_handlers():
        return [SnowflakePandasTypeHandler(), SnowflakePySparkTypeHandler()]

    @staticmethod
    def default_load_type() -> type | None:
        return pd.DataFrame
```

## Database I/O managers by library

| Store | Install | Class | Key config |
| --- | --- | --- | --- |
| DuckDB | `dagster-duckdb-pandas` / `-polars` / `-pyspark` | `DuckDBPandasIOManager`, `DuckDBPolarsIOManager`, `DuckDBPySparkIOManager` | `database`, `schema`, `connection_config` |
| DuckDB (custom) | `dagster-duckdb` | `DuckDBIOManager` subclass with `type_handlers()` | same, plus your `DbTypeHandler`s |
| Snowflake | `dagster-snowflake-pandas` / `-pyspark` / `-polars` | `SnowflakePandasIOManager`, `SnowflakePySparkIOManager`, `SnowflakePolarsIOManager` (beta) | `account`, `user`, `password` or private key, `database`, `schema`, `warehouse`, `role` |
| Snowflake (custom) | `dagster-snowflake` | `SnowflakeIOManager` subclass | same |
| BigQuery | `dagster-gcp-pandas` / `dagster-gcp-pyspark` | `BigQueryPandasIOManager`, `BigQueryPySparkIOManager` | `project`, `dataset`, `write_mode`, `gcp_credentials` |
| BigQuery (custom) | `dagster-gcp` | `BigQueryIOManager` subclass | same |
| Delta Lake | `dagster-deltalake-pandas` / `-polars` | `DeltaLakePandasIOManager`, `DeltaLakePolarsIOManager`, `DeltaLakePyarrowIOManager` | `root_uri`, `storage_options` (`LocalConfig()`, `S3Config(...)`), `schema` |
| ClickHouse | `dagster-clickhouse` + `dagster-clickhouse-pandas` / `-polars` | `ClickhousePandasIOManager`, `ClickhousePolarsIOManager` | connection settings; pandas handler goes through `clickhouse-driver` |
| Iceberg (preview, community) | `dagster-iceberg[pandas\|polars\|daft\|spark]` | `PyArrowIcebergIOManager`, `PandasIcebergIOManager`, `PolarsIcebergIOManager`, `DaftIcebergIOManager`, `SparkIcebergIOManager` | `name`, `config=IcebergCatalogConfig(properties={...})`, `namespace` |
| Polars files (community) | `dagster-polars`, `dagster-polars[delta]` | `PolarsParquetIOManager`, `PolarsDeltaIOManager`, `PolarsBigQueryIOManager` | `base_dir` (any fsspec URI) |

Library-specific behavior worth knowing:

- **DuckDB** — set engine options with `connection_config={"arrow_large_buffer_size": True}`. `DuckDBResource` is the escape hatch for SQL-in-place work.
- **Snowflake** — `SnowflakePySparkIOManager` requires `warehouse` and an active `SparkSession` configured with the Snowflake Spark connector. Storing a Pandas DataFrame with naive timestamps assigns UTC, and the column lands as `TIMESTAMP_NTZ(9)`.
- **BigQuery** — `write_mode` is `"truncate"` (default, preserves schema), `"replace"` (drops and recreates, allowing schema evolution), or `"append"`. It is ignored for partitioned assets, which always replace the targeted partitions. Truncate fails when the DataFrame schema changed.
- **Delta Lake** — `root_uri` and `storage_options` are both required; `key_prefix` becomes the parent folder rather than a SQL schema. S3 needs a locking provider (`AWS_S3_LOCKING_PROVIDER=dynamodb` plus a `delta_rs_lock_table` table, or `S3Config(allow_unsafe_rename=True)`).
- **Iceberg** — the namespace must already exist in the catalog before the first run; create it yourself with `catalog.create_namespace("default")`.
- **Polars** — type annotation drives behavior: `pl.DataFrame` reads/writes eagerly, `pl.LazyFrame` scans and sinks, `Optional[...]` skips writing `None` and skips missing inputs, `dict[str, ...]` loads multiple partitions. Built on `UPathIOManager`, so local and S3/GCS paths work the same. By default each partition is a separate file; set `partition_by` metadata to get storage-native partitioning inside one Delta table.

## Object-store pickle I/O managers

Use for arbitrary Python objects when execution is distributed: each node needs network access and credentials to the bucket. Layout matches `FilesystemIOManager` (one path per asset key, overwritten on re-materialization).

```python
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from dagster_azure.adls2 import ADLS2PickleIOManager, ADLS2Resource, ADLS2SASToken
from dagster_gcp.gcs import GCSPickleIOManager, GCSResource

s3 = S3PickleIOManager(s3_resource=S3Resource(), s3_bucket="my-bucket", s3_prefix="my-prefix")
gcs = GCSPickleIOManager(gcs=GCSResource(project="my-project"), gcs_bucket="my-bucket", gcs_prefix="my-prefix")
adls2 = ADLS2PickleIOManager(
    adls2_file_system="my-fs",
    adls2_prefix="my-prefix",
    adls2=ADLS2Resource(storage_account="my-account", credential=ADLS2SASToken(token="...")),
)
```

To move already-materialized data from one manager to another without re-materializing, `dg.migrate_io_storage(definitions=..., destination_io_manager=..., instance=...)` replays `load_input`/`handle_output` across materialized assets and partitions. It is in **preview** and can break in patch releases.

## Testing I/O managers

Build contexts directly — no run required:

```python
def test_handle_output():
    manager = MyIOManager()
    context = dg.build_output_context(name="abc", step_key="123")
    manager.handle_output(context, 5)
    assert manager.storage_dict[("123", "abc")] == 5


def test_load_input():
    manager = MyIOManager()
    manager.storage_dict[("123", "abc")] = 5
    context = dg.build_input_context(
        upstream_output=dg.build_output_context(name="abc", step_key="123")
    )
    assert manager.load_input(context) == 5
```

Both builders accept `asset_key`, `partition_key`, `definition_metadata`, `dagster_type`, `resources`, and `resource_config`; `build_input_context` also takes `asset_partition_key_range` and `asset_partitions_def`. If any resource is a generator, use them as context managers (`with dg.build_input_context(...) as context:`) or accessing `context.resources` raises `DagsterInvariantViolationError`.

For end-to-end checks, `dg.materialize([asset1, asset2], resources={"io_manager": MyIOManager()})`, or `dg.materialize_to_memory([...])` to bypass storage entirely. See `testing-and-asset-checks.md`.

## Gotchas

- **Pickling is the default.** `FilesystemIOManager` and the S3/GCS/ADLS2 pickle managers serialize with `pickle`; unpicklable objects (open connections, clients, generators, some lambdas) fail at store time. Return a plain data structure, or write a custom manager.
- **Ephemeral default storage.** Without `DAGSTER_HOME` or a `base_dir`, `FilesystemIOManager` writes to a temp directory, so materializations do not survive a restart.
- **Distributed execution.** Multi-process and multi-node executors need storage reachable from every execution node. `InMemoryIOManager` is for unit tests only; a local filesystem path only works if all nodes share that volume.
- **`deps=` bypasses I/O managers.** Nothing is loaded and nothing is stored, so a downstream asset that expects a value must take a parameter instead.
- **Runtime metadata is invisible to I/O managers.** Only definition metadata reaches `context.definition_metadata`. `MaterializeResult(metadata=...)` / `Output(metadata=...)` goes to the event log, and `context.upstream_output.output_metadata` returns `{}` inside `load_input`.
- **`definition_metadata` means different things per side.** In `load_input` it is the `AssetIn`/`In` metadata, not the upstream asset's — use `context.upstream_output.definition_metadata` for that.
- **Mismatched partitions definitions.** Loading several upstream partitions requires a `dict[str, T]` annotation (or no annotation) downstream, plus a `PartitionMapping` on the `AssetIn` when the mapping is not the default.
- **Overwrite semantics.** Path-based managers overwrite the asset's single path on every materialization; there is no versioning unless the store provides it (Delta, Iceberg).
- **Input managers drift.** A subclass that hardcodes the upstream path breaks when the owning team changes it. Share a `_get_path` helper instead.
- **Ops need explicit keys.** `@dg.op(out=dg.Out(io_manager_key="s3_io"))` and `@dg.op(ins={"a": dg.In(input_manager_key="pandas_series")})`; op-level managers can also take per-output/per-input config via `@dg.io_manager(output_config_schema=..., input_config_schema=...)`.

## Legacy and superseded patterns

| Legacy | Current |
| --- | --- |
| `@dg.io_manager` factory function + `dg.IOManager` subclass | `dg.ConfigurableIOManager`, or `dg.ConfigurableIOManagerFactory` for wrapped clients |
| `dg.fs_io_manager` | `dg.FilesystemIOManager` |
| `dg.mem_io_manager` | `dg.InMemoryIOManager` |
| `context.metadata` (deprecated, removed in 2.0) | `context.definition_metadata` |
| `build_output_context(metadata=...)` | `build_output_context(definition_metadata=...)` |
| `build_duckdb_io_manager(type_handlers=...)` | `DuckDBIOManager` subclass with `type_handlers()` |
| `duckdb_pandas_io_manager`, `snowflake_pandas_io_manager`, `bigquery_pandas_io_manager`, `clickhouse_pandas_io_manager` (and polars/pyspark siblings) | `DuckDBPandasIOManager`, `SnowflakePandasIOManager`, `BigQueryPandasIOManager`, `ClickhousePandasIOManager`, … |
| `s3_pickle_io_manager`, `ConfigurablePickledObjectS3IOManager` | `S3PickleIOManager` |
| `ConfigurablePickledObjectGCSIOManager` | `GCSPickleIOManager` |
| `ConfigurablePickledObjectADLS2IOManager` | `ADLS2PickleIOManager` |
| `IOManagerDefinition.hardcoded_io_manager(...)` | Pass the manager instance directly in `Definitions(resources=...)` |

`@dg.input_manager` + `dg.InputManager` still exist for loading inputs that have no upstream output (op sub-selection, externally produced tables), but prefer subclassing the storing I/O manager and overriding `load_input`.

## Sources

- https://docs.dagster.io/guides/build/io-managers — when to use I/O managers, wiring, built-in table
- https://docs.dagster.io/guides/build/io-managers/defining-a-custom-io-manager — custom managers, metadata, partitions, per-input managers
- https://docs.dagster.io/guides/build/assets/passing-data-between-assets — external storage vs I/O managers vs combining assets
- https://docs.dagster.io/api/dagster/io-managers — `IOManager`, `ConfigurableIOManager(Factory)`, `InputContext`/`OutputContext`, `UPathIOManager`, `migrate_io_storage`, build helpers
- https://docs.dagster.io/integrations/libraries/motherduck/reference — DuckDB I/O manager partitions, schemas, multiple frame types
- https://docs.dagster.io/integrations/libraries/snowflake/reference — Snowflake partitions, schemas, PySpark, timestamps
- https://docs.dagster.io/integrations/libraries/gcp/bigquery/reference — BigQuery write modes, partitions, datasets
- https://docs.dagster.io/integrations/libraries/deltalake/reference — Delta Lake partitions, schemas, storage backends
- https://docs.dagster.io/integrations/libraries/polars — Polars type annotations and storage formats
- https://docs.dagster.io/integrations/libraries/iceberg — Iceberg I/O managers and catalog config
- https://docs.dagster.io/integrations/libraries/clickhouse — ClickHouse I/O manager packages
- https://docs.dagster.io/api/libraries/dagster-aws — `S3PickleIOManager`
- https://docs.dagster.io/api/libraries/dagster-azure — `ADLS2PickleIOManager`
- https://docs.dagster.io/api/libraries/dagster-gcp — `GCSPickleIOManager`, `BigQueryIOManager`
