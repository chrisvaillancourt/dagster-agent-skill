# Integrations catalog

Index of `dagster-*` integration libraries, the exact classes/decorators each exports, which ones ship a Component, and short usage patterns for the common ones. Deep coverage of dbt, dlt, Sling, Fivetran and Airbyte lives in `integrations-dbt-and-elt.md`; database I/O managers in `io-managers.md`; Pipes clients (`PipesDockerClient`, `PipesK8sClient`, `PipesGlueClient`, `PipesDatabricksClient`, ...) in `pipes-external-execution.md`; run launchers and executors (`dagster-k8s`, `dagster-celery*`, `dagster-docker`, `dagster-dask`) in `deployment-oss.md`; Airflow migration mechanics in `migration-and-legacy.md`.

Docs column values are paths under `https://docs.dagster.io`.

## Contents
- [Adding an integration](#adding-an-integration)
- [Component-ready integrations](#component-ready-integrations)
- [Catalog: ingestion and ELT](#catalog-ingestion-and-elt)
- [Catalog: transformation and compute](#catalog-transformation-and-compute)
- [Catalog: warehouses, lakes, dataframes and cloud SDKs](#catalog-warehouses-lakes-dataframes-and-cloud-sdks)
- [Catalog: BI and analytics](#catalog-bi-and-analytics)
- [Catalog: ML/AI, data quality, metadata and alerting](#catalog-mlai-data-quality-metadata-and-alerting)
- [Patterns: databases and cloud storage](#patterns-databases-and-cloud-storage)
- [Patterns: alerting and observability](#patterns-alerting-and-observability)
- [Patterns: BI tools](#patterns-bi-tools)
- [Patterns: ML, AI, notebooks and data quality](#patterns-ml-ai-notebooks-and-data-quality)
- [Patterns: compute and external systems](#patterns-compute-and-external-systems)
- [Airflow via Airlift](#airflow-via-airlift)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Adding an integration

Same five steps for every library. Prefer the Component when one exists.

```bash
uv add dagster-snowflake                        # 1. install into the project venv
dg list components --package dagster_snowflake  # 2. does it ship a Component?
dg scaffold defs dagster_snowflake.SnowflakeConnectionComponent snowflake_connection
                                                # 3a. Component path: writes defs.yaml
dg check defs                                   # 4. validate defs + YAML schema
dg launch --assets '*'                          # 5. run it
```

3b. No Component: define the resource in `src/<project>/defs/resources.py` under `@dg.definitions`,
then either type-annotate an asset parameter with it, or call the library's
`load_<tool>_asset_specs` / `build_<tool>_assets_definitions` / `@<tool>_assets` decorator. See
`resources-and-config.md` and `components.md`. Secrets go in `.env` at the project root and are read
with `dg.EnvVar("NAME")` in Python or `"{{ env.NAME }}"` in `defs.yaml`. Never inline credentials.

Support badge: pages tagged `dagster-supported` are maintained in the `dagster` repo. Pages tagged
`community-supported` are maintained in `github.com/dagster-io/community-integrations`; file bugs
there, and expect independent release cadence and thinner API reference.

## Component-ready integrations

Verified `dg scaffold defs <type> <name>` targets. Anything not listed has no Component; use the
Python API.

| Component type | Library | Builds |
| --- | --- | --- |
| `dagster_airbyte.AirbyteWorkspaceComponent` | dagster-airbyte | Airbyte connections as assets |
| `dagster_airlift.core.components.AirflowInstanceComponent` | dagster-airlift | Airflow DAGs/datasets as Dagster jobs + assets |
| `dagster_apprise.apprise_notifications` | dagster-apprise | Run-status notification sensors |
| `dagster_aws.S3ResourceComponent`, `.ParameterStoreResourceComponent`, `.SecretsManagerResourceComponent`, `.SecretsManagerSecretsResourceComponent`, `.SSMResourceComponent`, `.AthenaClientResourceComponent`, `.ECRPublicResourceComponent`, `.RDSResourceComponent`, `.S3FileManagerResourceComponent`, `.S3CredentialsComponent`, `.Boto3CredentialsComponent`, `.AthenaCredentialsComponent` | dagster-aws | AWS resources from YAML |
| `dagster_aws.redshift.RedshiftClientResourceComponent`, `.RedshiftCredentialsComponent` | dagster-aws | Redshift resource from YAML |
| `dagster_azure.ADLS2ResourceComponent`, `.AzureBlobStorageResourceComponent` | dagster-azure | Azure storage resources |
| `dagster_census.CensusComponent` | dagster-census | Census syncs as assets |
| `dagster_clickhouse.ClickhouseQueryComponent` | dagster-clickhouse | Templated SQL assets on ClickHouse |
| `dagster_databricks.DatabricksWorkspaceComponent` | dagster-databricks | State-backed Databricks workspace metadata |
| `dagster_databricks.DatabricksAssetBundleComponent` | dagster-databricks | Databricks Asset Bundle tasks as assets |
| `dagster_dbt.DbtProjectComponent` | dagster-dbt | dbt models as assets |
| `dagster_dbt.DbtCloudComponent` | dagster-dbt | dbt Cloud jobs as assets |
| `dagster_dlt.DltLoadCollectionComponent` | dagster-dlt | dlt sources/pipelines as assets |
| `dagster_evidence.EvidenceProject` | dagster-evidence | Evidence dashboard build |
| `dagster_fivetran.FivetranAccountComponent` | dagster-fivetran | Fivetran connectors as assets |
| `dagster_gcp.BigQueryResourceComponent`, `.GCSResourceComponent`, `.GCSFileManagerResourceComponent`, `.DataprocResourceComponent` | dagster-gcp | GCP resources from YAML |
| `dagster_hightouch.HightouchSyncComponent` | dagster-hightouch | Hightouch syncs as assets |
| `dagster_looker.LookerComponent` | dagster-looker | Looker explores/dashboards as asset specs |
| `dagster_omni.OmniComponent` | dagster-omni | Omni documents/queries as asset specs |
| `dagster_powerbi.PowerBIWorkspaceComponent` | dagster-powerbi | Power BI workspace as asset specs |
| `dagster_sigma.SigmaComponent` | dagster-sigma | Sigma workbooks/datasets as asset specs |
| `dagster_sling.SlingReplicationCollectionComponent` | dagster-sling | Sling replications as assets |
| `dagster_snowflake.SnowflakeConnectionComponent` | dagster-snowflake | Connection consumed by SQL components |
| `dagster_snowflake.SnowflakeDbtProjectComponent` | dagster-snowflake | dbt project wired to a Snowflake connection |
| `dagster_soda.SodaScanComponent` | dagster-soda | SodaCL checks as Dagster asset checks |
| `dagster_spark.SparkDeclarativePipelineComponent` | dagster-pyspark | Spark Declarative Pipelines (Spark 4.0+) datasets |
| `dagster_tableau.TableauComponent` | dagster-tableau | Tableau workbooks/sheets as asset specs |

Core (no extra package): `dagster.TemplatedSqlComponent`, `dagster.PythonScriptComponent`,
`dagster.DefsFolderComponent`. `SigmaComponent` (beta) and `OmniComponent` (preview) are
state-backed: they cache workspace metadata.

## Catalog: ingestion and ELT

| Library | Purpose | Key APIs | Component | Docs |
| --- | --- | --- | --- | --- |
| dagster-airbyte | Airbyte Cloud/OSS connection syncs | `AirbyteWorkspace`, `AirbyteCloudWorkspace`, `load_airbyte_asset_specs`, `load_airbyte_cloud_asset_specs`, `build_airbyte_assets_definitions`, `@airbyte_assets`, `DagsterAirbyteTranslator` | `AirbyteWorkspaceComponent` | `/integrations/libraries/airbyte` |
| dagster-fivetran | Fivetran connector syncs | `FivetranWorkspace`, `load_fivetran_asset_specs`, `build_fivetran_assets_definitions`, `@fivetran_assets`, `build_fivetran_polling_sensor`, `DagsterFivetranTranslator` | `FivetranAccountComponent` | `/integrations/libraries/fivetran` |
| dagster-dlt | dltHub pipelines | `DagsterDltResource`, `@dlt_assets`, `build_dlt_asset_specs`, `DagsterDltTranslator` | `DltLoadCollectionComponent` | `/integrations/libraries/dlt` |
| dagster-sling | Sling replications | `SlingResource`, `SlingConnectionResource`, `@sling_assets`, `DagsterSlingTranslator` | `SlingReplicationCollectionComponent` | `/integrations/libraries/sling` |
| dagster-census | Census reverse-ETL syncs | `CensusResource`, `census_trigger_sync_op`, `CensusOutput` | `CensusComponent` | `/integrations/libraries/census` |
| dagster-hightouch | Hightouch reverse-ETL syncs | `ConfigurableHightouchResource`, `hightouch_sync_op` | `HightouchSyncComponent` | `/integrations/libraries/hightouch` |
| dagster-meltano | Run Meltano ELT jobs (community) | `meltano_resource`, `meltano_run_op` | — | `/integrations/libraries/meltano` |
| dagster-mssql-bcp | Bulk-load SQL Server (community) | — | — | `/integrations/libraries/mssql-bulk-copy-tool` |
| dagster-embedded-elt | Deprecated umbrella package | — | — | `/integrations/libraries/embedded-elt` |

## Catalog: transformation and compute

| Library | Purpose | Key APIs | Component | Docs |
| --- | --- | --- | --- | --- |
| dagster-dbt | dbt Core + dbt Cloud | `DbtCliResource`, `DbtProject`, `@dbt_assets`, `DagsterDbtTranslator`, `DbtCloudWorkspace`, `build_dbt_asset_selection`, `build_schedule_from_dbt_selection`, `build_freshness_checks_from_dbt_assets` | `DbtProjectComponent`, `DbtCloudComponent` | `/integrations/libraries/dbt` |
| dagster-databricks | Databricks jobs, Pipes, bundles | `DatabricksClientResource`, `DatabricksClient`, `PipesDatabricksClient`, `PipesDbfsContextInjector`, `PipesDbfsMessageReader`, `create_databricks_run_now_op`, `create_databricks_submit_run_op` | `DatabricksWorkspaceComponent`, `DatabricksAssetBundleComponent` | `/integrations/libraries/databricks` |
| dagster-pyspark | Local/attached SparkSession; SDP | `PySparkResource` | `SparkDeclarativePipelineComponent` (as `dagster_spark.…`) | `/integrations/libraries/pyspark` |
| dagster-spark | Submit spark-submit jobs (legacy op API) | `create_spark_op`, `construct_spark_shell_command`, `define_spark_config` | — | `/integrations/libraries/spark` |
| dagster-docker | Container run launcher, executor, Pipes | `DockerRunLauncher`, `docker_executor`, `docker_container_op`, `PipesDockerClient` | — | `/integrations/libraries/docker` |
| dagster-k8s | Kubernetes launcher, executor, Pipes | `K8sRunLauncher`, `k8s_job_executor`, `k8s_job_op`, `execute_k8s_job`, `PipesK8sClient` | — | `/integrations/libraries/k8s` |
| dagster-celery, -celery-k8s, -celery-docker | Celery-backed executors | `celery_executor`, `celery_k8s_job_executor`, `CeleryK8sRunLauncher`, `celery_docker_executor` | — | `/integrations/libraries/celery` |
| dagster-dask | Dask-backed executor | `dask_executor` | — | `/integrations/libraries/dask` |
| dagstermill | Papermill-executed notebooks | `define_dagstermill_asset`, `define_dagstermill_op`, `ConfigurableLocalOutputNotebookIOManager`, `get_context`, `yield_result`, `yield_event` | — | `/integrations/libraries/jupyter` |
| dagster-ssh | Remote command/file transfer | `SSHResource` | — | `/integrations/libraries/ssh` |
| dagster-pipes | External-process protocol library | `open_dagster_pipes`, `PipesContext` (+ context loaders / message writers) | — | `/integrations/libraries/pipes` |
| dagster-modal, dagster-ray, dagster-slurm, dagster-nomad, dagster-perian, dagster-prefect, dagster-hex | Community/partner compute backends | see each page | — | `/integrations/libraries/<name>` |

## Catalog: warehouses, lakes, dataframes and cloud SDKs

| Library | Purpose | Key APIs | Component | Docs |
| --- | --- | --- | --- | --- |
| dagster-snowflake | Snowflake resource + I/O manager base | `SnowflakeResource`, `SnowflakeConnection`, `SnowflakeIOManager`, `build_snowflake_io_manager`, `fetch_last_updated_timestamps` | `SnowflakeConnectionComponent`, `SnowflakeDbtProjectComponent` | `/integrations/libraries/snowflake` |
| dagster-snowflake-pandas / -polars / -pyspark | Snowflake I/O managers per DataFrame type | `SnowflakePandasIOManager`, `SnowflakePolarsIOManager`, `SnowflakePySparkIOManager` | — | `/integrations/libraries/snowflake` |
| dagster-gcp | BigQuery, GCS, Dataproc | `BigQueryResource`, `BigQueryIOManager`, `build_bigquery_io_manager`, `GCSResource`, `GCSPickleIOManager`, `GCSFileManagerResource`, `DataprocResource`, `fetch_last_updated_timestamps` | `BigQueryResourceComponent`, `GCSResourceComponent`, `GCSFileManagerResourceComponent`, `DataprocResourceComponent` | `/integrations/libraries/gcp` |
| dagster-gcp-pandas / -pyspark | BigQuery I/O managers | `BigQueryPandasIOManager`, `BigQueryPySparkIOManager` | — | `/integrations/libraries/gcp/bigquery` |
| dagster-duckdb | DuckDB + MotherDuck | `DuckDBResource`, `DuckDBIOManager`, `build_duckdb_io_manager` | — | `/integrations/libraries/motherduck` |
| dagster-duckdb-pandas / -polars / -pyspark | DuckDB I/O managers | `DuckDBPandasIOManager`, `DuckDBPolarsIOManager`, `DuckDBPySparkIOManager` | — | `/integrations/libraries/motherduck` |
| dagster-clickhouse | ClickHouse native protocol | `ClickhouseResource`, `ClickhouseIOManager`, `build_clickhouse_io_manager`, `ClickhouseDbClient`, `format_clickhouse_table_fqn` | `ClickhouseQueryComponent` | `/integrations/libraries/clickhouse` |
| dagster-clickhouse-pandas / -polars | ClickHouse I/O managers | `ClickhousePandasIOManager`, `ClickhousePolarsIOManager` | — | `/integrations/libraries/clickhouse` |
| dagster-postgres | Instance storage backend only | `PostgresRunStorage`, `PostgresEventLogStorage`, `PostgresScheduleStorage` | — | `/integrations/libraries/postgres` |
| dagster-mysql | Instance storage backend + resource | `MySQLRunStorage`, `MySQLEventLogStorage`, `MySQLScheduleStorage`, `MySQLResource` | — | `/integrations/libraries/mysql` |
| dagster-deltalake (+ -pandas, -polars) | Delta Lake tables (community) | `DeltaTableResource`, `DeltaLakeIOManager`, `DeltaLakePyarrowIOManager`, `DeltaLakePandasIOManager`, `DeltaLakePolarsIOManager` | — | `/integrations/libraries/deltalake` |
| dagster-iceberg | Apache Iceberg (community) | `IcebergCatalogConfig`, `IcebergTableResource`, `IcebergIOManager`, `PyArrowIcebergIOManager`, `PandasIcebergIOManager`, `PolarsIcebergIOManager`, `SparkIcebergIOManager`, `DaftIcebergIOManager` | — | `/integrations/libraries/iceberg` |
| dagster-polars | Polars I/O managers (community) | `PolarsParquetIOManager`, `PolarsDeltaIOManager`, `PolarsBigQueryIOManager`, `BasePolarsUPathIOManager` | — | `/integrations/libraries/polars` |
| dagster-pandas | Typed Pandas DataFrames | `create_dagster_pandas_dataframe_type`, `PandasColumn`, `RowCountConstraint`, `StrictColumnsConstraint` | — | `/integrations/libraries/pandas` |
| dagster-teradata, dagster-adbc, dagster-elasticsearch, dagster-obstore, dagster-lakefs | Community storage/DB resources | `ADBCResource`, `ElasticsearchResource`, ... | — | `/integrations/libraries/<name>` |
| dagster-chroma, dagster-qdrant, dagster-weaviate | Vector databases (community) | `ChromaResource`, `QdrantResource`, `WeaviateResource` | — | `/integrations/libraries/<name>` |
| dagster-aws | S3, Secrets Manager, SSM, Athena, Redshift, Glue, EMR, Lambda, ECS, CloudWatch | `dagster_aws.s3.S3Resource`, `.S3PickleIOManager`, `.S3FileManagerResource`, `.S3ComputeLogManager`; `dagster_aws.secretsmanager.SecretsManagerResource`, `.SecretsManagerSecretsResource`; `dagster_aws.ssm.ParameterStoreResource`, `.ParameterStoreTag`, `.SSMResource`; `dagster_aws.redshift.RedshiftClientResource`; `dagster_aws.rds.RDSResource`; `dagster_aws.ecs.EcsRunLauncher`, `.ecs_executor`; `dagster_aws.cloudwatch.cloudwatch_logger`; `dagster_aws.emr.EmrJobRunner`; `dagster_aws.pipes.*` | see [Component list](#component-ready-integrations) | `/integrations/libraries/aws` |
| dagster-gcp | See warehouses table; also GCS compute log + Cloud Run | `GCSResource`, `GCSPickleIOManager`, `gcs_file_manager` | `GCSResourceComponent`, ... | `/integrations/libraries/gcp` |
| dagster-contrib-gcp | Cloud Run run launcher (community) | — | — | `/integrations/libraries/gcp/cloud-run-launcher` |
| dagster-azure | ADLS2 + Blob Storage | `dagster_azure.adls2.ADLS2Resource`, `.ADLS2PickleIOManager`, `.adls2_file_manager`, `.FakeADLS2Resource`; `dagster_azure.blob.AzureBlobStorageResource`, `.AzureBlobComputeLogManager`; `dagster_azure.pipes.clients.PipesAzureMLClient` | `ADLS2ResourceComponent`, `AzureBlobStorageResourceComponent` | `/integrations/libraries/azure` |

## Catalog: BI and analytics

Each library exposes one workspace/organization object that is both credential holder and spec
loader, plus a translator (`DagsterLookerApiTranslator`, `DagsterLookerLkmlTranslator`,
`DagsterTableauTranslator`, `DagsterPowerBITranslator`, `DagsterSigmaTranslator`) for customizing
asset keys, groups and metadata.

| Library | Workspace object | Spec loader | Materializable variant | Component | Docs |
| --- | --- | --- | --- | --- | --- |
| dagster-looker | `LookerResource` | `load_looker_asset_specs`, `build_looker_asset_specs` | `build_looker_pdt_assets_definitions` + `RequestStartPdtBuild` | `LookerComponent` | `/integrations/libraries/looker` |
| dagster-tableau | `TableauCloudWorkspace`, `TableauServerWorkspace` | `load_tableau_asset_specs`, `parse_tableau_external_and_materializable_asset_specs` | `@tableau_assets` + `refresh_and_poll`, `build_tableau_materializable_assets_definition` | `TableauComponent` | `/integrations/libraries/tableau` |
| dagster-powerbi | `PowerBIWorkspace` (+ `PowerBIServicePrincipal` or `PowerBIToken`) | `load_powerbi_asset_specs` | `build_semantic_model_refresh_asset_definition` | `PowerBIWorkspaceComponent` | `/integrations/libraries/powerbi` |
| dagster-sigma | `SigmaOrganization` (+ `SigmaBaseUrl`, `SigmaFilter`) | `load_sigma_asset_specs` | — | `SigmaComponent` | `/integrations/libraries/sigma` |
| dagster-omni | configured inside the Component | — | — | `OmniComponent` | `/integrations/libraries/omni` |
| dagster-evidence | — | — | — | `EvidenceProject` | `/integrations/libraries/evidence` |
| dagster-cube | `CubeResource` (community) | — | — | — | `/integrations/libraries/cube` |

## Catalog: ML/AI, data quality, metadata and alerting

| Library | Purpose | Key APIs | Docs |
| --- | --- | --- | --- |
| dagster-mlflow | MLflow run per Dagster run (beta) | `mlflow_tracking`, `end_mlflow_on_run_finished` | `/integrations/libraries/mlflow` |
| dagster-wandb | W&B artifacts, model registry, Launch (community) | `wandb_resource`, `wandb_artifacts_io_manager`, `WandbArtifactConfiguration`, `run_launch_job`, `run_launch_agent`, `SerializationModule` | `/integrations/libraries/wandb` |
| dagster-openai | OpenAI client with usage metadata | `OpenAIResource`, `with_usage_metadata` | `/integrations/libraries/openai` |
| dagster-anthropic | Anthropic client (community) | `AnthropicResource` | `/integrations/libraries/anthropic` |
| dagster-gemini | Google Gemini client (community) | `GeminiResource` | `/integrations/libraries/gemini` |
| dagster-notdiamond | Model routing (community) | `NotDiamondResource` | `/integrations/libraries/notdiamond` |
| dagster-hf-datasets | Hugging Face Datasets assets (community) | — | `/integrations/libraries/datasets` |
| dagster-pandera | Pandera schema to Dagster type | `pandera_schema_to_dagster_type` | `/integrations/libraries/pandera` |
| dagster-ge | Great Expectations validation ops (beta) | `ge_validation_op_factory` | `/integrations/libraries/great-expectations` |
| dagster-soda | SodaCL checks as asset checks | `SodaScanComponent` | `/integrations/libraries/soda` |
| dagster-patito | Patito model to Dagster type (community) | `dagster_polars.patito.patito_model_to_dagster_type` | `/integrations/libraries/patito` |
| dagster-datahub | Push metadata to DataHub | `DatahubRESTEmitterResource`, `DatahubKafkaEmitterResource` | `/integrations/libraries/datahub` |
| dagster-openlineage | Emit OpenLineage events (community) | `openlineage_sensor` | `/integrations/libraries/openlineage` |
| Atlan, Secoda, OpenMetadata | External catalogs that consume Dagster metadata; configured on their side | — | `/integrations/libraries/atlan`, `/secoda`, `/open-metadata` |
| dagster-slack | Slack messages, failure sensors, hooks | `SlackResource`, `make_slack_on_run_failure_sensor`, `slack_on_failure`, `slack_on_success` | `/integrations/libraries/slack` |
| dagster-msteams | Teams cards and failure sensors | `MSTeamsResource`, `Card`, `make_teams_on_run_failure_sensor`, `teams_on_failure`, `teams_on_success` | `/integrations/libraries/msteams` |
| dagster-pagerduty | Trigger PagerDuty incidents | `PagerDutyService` | `/integrations/libraries/pagerduty` |
| dagster-twilio | SMS from assets/ops | `TwilioResource` | `/integrations/libraries/twilio` |
| dagster-datadog | Publish metrics/events | `DatadogResource` | `/integrations/libraries/datadog` |
| dagster-prometheus | Push to a Pushgateway | `PrometheusResource` | `/integrations/libraries/prometheus` |
| dagster-papertrail | Ship logs to Papertrail | `papertrail_logger` | `/integrations/libraries/papertrail` |
| dagster-github | GitHub App client (issues, PRs) | `GithubResource`, `GithubClient` | `/integrations/libraries/github` |
| dagster-apprise | 70+ notification backends (community) | `AppriseResource`, `AppriseNotificationsConfig`, `apprise_notifications`, `apprise_on_failure`, `apprise_on_success` | `/integrations/libraries/apprise` |
| dagster-dingtalk | DingTalk notifications (community) | — | `/integrations/libraries/dingtalk` |

## Patterns: databases and cloud storage

Use when an asset issues SQL or object-store calls directly instead of handing DataFrames to an I/O
manager (`io-managers.md` covers the other route). Define the resource once under `@dg.definitions`,
then annotate the asset parameter.

```python
# src/<project>/defs/resources.py
from dagster_snowflake import SnowflakeResource

import dagster as dg

snowflake = SnowflakeResource(
    account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),  # required
    user=dg.EnvVar("SNOWFLAKE_USER"),  # required
    password=dg.EnvVar("SNOWFLAKE_PASSWORD"),  # password or private key required
    warehouse="PLANTS",
    schema="IRIS",
    role="WRITER",
)


@dg.definitions
def resources():
    return dg.Definitions(resources={"snowflake": snowflake})


# src/<project>/defs/assets.py
@dg.asset(deps=["iris_dataset"], kinds={"snowflake"})
def iris_setosa(snowflake: SnowflakeResource) -> None:
    with snowflake.get_connection() as conn:
        conn.cursor().execute("create or replace table iris.iris_setosa as (...)")
```

`get_connection()` is a context manager on `SnowflakeResource`, `DuckDBResource` and
`ClickhouseResource`. BigQuery, Redshift and Athena expose `get_client()` instead:
`DuckDBResource(database="/tmp/analytics.duckdb")`, `BigQueryResource(project="my-project")` then
`with bigquery.get_client() as client:`.

`dagster-postgres` is **not** a connection resource. It supplies instance storage
(`PostgresRunStorage`, `PostgresEventLogStorage`, `PostgresScheduleStorage`) configured in
`dagster.yaml` under `storage: postgres:` — see `deployment-oss.md`. To query Postgres from an
asset, wrap psycopg/SQLAlchemy in a `dg.ConfigurableResource`, or use `dagster-adbc`.
`dagster-mysql` is the same for storage but also exports `MySQLResource`.

Object stores and secret stores use the same shape:

```python
from dagster_aws.s3 import S3Resource
from dagster_aws.ssm import ParameterStoreResource, ParameterStoreTag


@dg.asset
def my_s3_asset(s3: S3Resource) -> None:
    s3.get_client().put_object(Bucket="my-bucket", Key="path/df.csv", Body=b"a,b\n1,2\n")


@dg.asset
def from_parameter_store(parameter_store: ParameterStoreResource) -> str:
    return parameter_store.fetch_parameters(parameters=["my-param"])["my-param"]


resources = {
    "s3": S3Resource(region_name="us-west-2"),
    "parameter_store": ParameterStoreResource(
        region_name="us-west-1", parameter_tags=[ParameterStoreTag(key="tier", values=["prod"])]
    ),
}
```

`ParameterStoreResource.parameters_in_environment()` and
`SecretsManagerSecretsResource.secrets_in_environment()` are context managers that inject values
into `os.environ` for the block; `SecretsManagerResource.get_client()` returns the raw boto3 client.
Azure and GCS mirror the shape with `ADLS2Resource`, `AzureBlobStorageResource` and `GCSResource`.

For SQL as YAML, pair `dagster.TemplatedSqlComponent` (keys: `sql_template`, `sql_template_vars`,
`assets`) with `connection: "{{ context.load_component('snowflake_connection') }}"` pointing at a
scaffolded `SnowflakeConnectionComponent`. ClickHouse has its own `ClickhouseQueryComponent`.

## Patterns: alerting and observability

```python
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor

import dagster as dg

slack_on_run_failure = make_slack_on_run_failure_sensor(
    channel="#alerts",
    slack_token=dg.EnvVar("SLACK_TOKEN").get_value(),
    webserver_base_url="https://dagster.example.com",  # deep links to the failed run
    monitor_all_code_locations=True,
)


@dg.run_failure_sensor
def custom_slack_alert(context: dg.RunFailureSensorContext, slack: SlackResource) -> None:
    slack.get_client().chat_postMessage(
        channel="#alerts",
        text=f"{context.dagster_run.job_name} failed: {context.failure_event.message}",
    )
```

`make_slack_on_run_failure_sensor` also accepts `text_fn`, `blocks_fn`, `monitored_jobs`,
`minimum_interval_seconds` and `default_status`. `dagster-msteams` mirrors it with
`make_teams_on_run_failure_sensor`; the hooks `slack_on_failure` / `teams_on_failure` attach at the
op/job level instead. One-liners for the rest:

```python
MSTeamsResource(hook_url=dg.EnvVar("TEAMS_WEBHOOK_URL")).get_client().post_message(payload=card.payload)
PagerDutyService(routing_key="…").EventV2_create(summary="alert", source="localhost", severity="error", event_action="trigger")
DatadogResource(api_key=dg.EnvVar("DD_API_KEY"), app_key=dg.EnvVar("DD_APP_KEY")).get_client().gauge("users.online", 1001)
PrometheusResource(gateway="http://pushgateway:9091").push_to_gateway(job="my_job_label")
GithubResource(github_app_id=dg.EnvVar.int("GITHUB_APP_ID"), github_app_private_rsa_key=dg.EnvVar("GITHUB_PRIVATE_KEY"), github_installation_id=dg.EnvVar.int("GITHUB_INSTALLATION_ID")).get_client().create_issue(...)
```

Dagster+ users should prefer built-in alert policies over sensor-based alerting; see
`dagster-plus.md`.

## Patterns: BI tools

Spec-only form gives lineage and metadata, no execution:

```python
looker_specs = load_looker_asset_specs(looker_resource=LookerResource(...))
sigma_specs = load_sigma_asset_specs(SigmaOrganization(base_url=SigmaBaseUrl.AWS_US, client_id=..., client_secret=...))
power_bi_specs = load_powerbi_asset_specs(PowerBIWorkspace(credentials=PowerBIServicePrincipal(...), workspace_id=...))
tableau_specs = load_tableau_asset_specs(TableauCloudWorkspace(connected_app_client_id=..., pod_name=...))
defs = dg.Definitions(assets=[*looker_specs], resources={"looker": looker_resource})
```

Make them materializable by wrapping. `build_semantic_model_refresh_asset_definition(resource_key=…, spec=spec)`
for Power BI semantic models, selected on `spec.tags.get("dagster-powerbi/asset_type") == "semantic_model"`.
`build_looker_pdt_assets_definitions(resource_key="looker", request_start_pdt_builds=[RequestStartPdtBuild(model_name=…, view_name=…)])`
for Looker persistent derived tables. For Tableau, decorate with `@tableau_assets(workspace=…, name=…, group_name=…)`
and `yield from tableau.refresh_and_poll(context=context)` in the body.

In YAML the credentials nest under a per-library key: `PowerBIWorkspaceComponent` under
`workspace:` with a nested `credentials:`, `LookerComponent` under `looker_resource:`,
`TableauComponent` under `workspace:` with `type: cloud`, `SigmaComponent` under `organization:`,
`OmniComponent` under `workspace:`. All accept `translation:` for asset key/group/tag overrides;
Sigma adds `sigma_filter:`.

## Patterns: ML, AI, notebooks and data quality

```python
from dagster_openai import OpenAIResource, with_usage_metadata

import dagster as dg


@dg.asset(kinds={"openai"})
def summary(context: dg.AssetExecutionContext, openai: OpenAIResource) -> None:
    with openai.get_client(context) as client:
        client.chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": "Say this is a test."}]
        )


defs = dg.Definitions(
    assets=[summary], resources={"openai": OpenAIResource(api_key=dg.EnvVar("OPENAI_API_KEY"))}
)
```

`get_client(context)` logs token usage into materialization metadata for chat and embedding calls.
Wrap other endpoints explicitly:
`client.fine_tuning.jobs.create = with_usage_metadata(context=context, output_name="…", func=client.fine_tuning.jobs.create)`.
Inside a `@multi_asset`, pass `asset_key=` to `get_client`. `AnthropicResource` and `GeminiResource`
follow the same `get_client(context)` shape.

MLflow attaches one MLflow run per Dagster run and is op/job-scoped:

```python
from dagster_mlflow import end_mlflow_on_run_finished, mlflow_tracking


@end_mlflow_on_run_finished
@dg.job(resource_defs={"mlflow": mlflow_tracking})
def training_job():
    train()  # @op(required_resource_keys={"mlflow"})
```

Configure it under `resources.mlflow.config`: `experiment_name`, `mlflow_tracking_uri`,
`parent_run_id`, `mlflow_run_id`, `env`, `env_to_tag`, `extra_tags`. W&B is the analogous pair
`wandb_resource` + `wandb_artifacts_io_manager`.

Notebooks: `define_dagstermill_asset(name=…, notebook_path=dg.file_relative_path(__file__, "../notebooks/x.ipynb"))`
executes the notebook through Papermill. Add `ConfigurableLocalOutputNotebookIOManager()` under the
`output_notebook_io_manager` resource key to persist executed notebooks; inside the notebook use
`dagstermill.get_context()`, `yield_result` and `yield_event`.

Data quality: pass a `pandera.DataFrameModel` subclass through
`pandera_schema_to_dagster_type(StockPrices)` and set it as the asset's `dagster_type=`. The
generated type runs on every materialization and surfaces failing rows as structured metadata.

`dagster-ge` instead builds an op:
`ge_validation_op_factory(name=…, datasource_name=…, data_connector_name=…, data_asset_name=…, suite_name=…, batch_identifiers={…})`
yields an expectation plus a metadata output. It is beta and needs a `great_expectations.yml` with a
`RuntimeDataConnector`. For new work prefer native asset checks (`testing-and-asset-checks.md`) or
`dagster_soda.SodaScanComponent`.

## Patterns: compute and external systems

```python
from dagster_databricks import DatabricksClientResource

import dagster as dg

databricks = DatabricksClientResource(
    host=dg.EnvVar("DATABRICKS_HOST"), token=dg.EnvVar("DATABRICKS_TOKEN")
)


@dg.asset(kinds={"databricks"})
def feature_table(context: dg.AssetExecutionContext, databricks: DatabricksClientResource):
    run = databricks.workspace_client.jobs.run_now(job_id=123).result()
    return dg.MaterializeResult(metadata={"run_id": str(run.run_id)})
```

Prefer `PipesDatabricksClient` over the raw client when you want logs, asset checks and metadata
streamed back; same for `PipesDockerClient`, `PipesK8sClient`, `PipesGlueClient`, `PipesEMRClient`,
`PipesEMRServerlessClient`, `PipesLambdaClient`, `PipesECSClient`, `PipesAzureMLClient`. See
`pipes-external-execution.md`.

Spark in-process uses `PySparkResource(spark_config={"spark.executor.memory": "2g"})` and
`pyspark.spark_session` inside the op. For cluster Spark, submit through Pipes (Databricks, AWS
Glue, EMR, EMR on EKS, EMR Serverless) rather than running Spark in the Dagster process. Spark 4.0+
Declarative Pipelines get their own component, which discovers datasets via `spark-pipelines dry-run`:
`dg scaffold component dagster_spark.SparkDeclarativePipelineComponent my_sdp --pipeline-spec-path ./spark-pipeline.yml`.

## Airflow via Airlift

`dagster-airlift` supports three stages: **observe** (peer a live Airflow instance so its DAGs and
datasets appear in Dagster), **migrate** (task-by-task, with per-task rollback, while the Airflow
DAG structure and history stay intact), **decommission** (delete the Airflow DAG once every task is
proxied). Install `dagster-airlift[core]`.

```bash
dg scaffold defs dagster_airlift.core.components.AirflowInstanceComponent airflow \
  --name my_airflow --auth-type basic_auth
```

The component reads `AIRFLOW_WEBSERVER_URL`, `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD` by default,
creates a `<name>__airflow_monitoring_job_sensor`, surfaces each DAG as a Dagster job and each
Airflow dataset as an asset. Map Dagster assets onto Airflow tasks in `defs.yaml` at task or DAG
granularity. Only basic auth is supported by the component today.

Python API for the same thing: `AirflowInstance`, `AirflowBasicAuthBackend`
(`MwaaSessionAuthBackend` for MWAA), `build_defs_from_airflow_instance`,
`load_airflow_dag_asset_specs`, `assets_with_task_mappings`, `assets_with_dag_mappings`. Proxying
runs in the Airflow process via `dagster_airlift.in_airflow.proxying_to_dagster` plus
`load_proxied_state_from_yaml` and `DefaultProxyTaskToDagsterOperator`. Full walkthrough in
`migration-and-legacy.md`.

## Gotchas

- **Version-pin the whole family.** Every `dagster-*` library releases in lockstep with `dagster`;
  mixing release lines produces import errors and silent behavior drift.
- **DataFrame extras are separate packages.** `dagster-snowflake` does not include
  `SnowflakePandasIOManager`; install `dagster-snowflake-pandas`. Same for
  `dagster-duckdb-{pandas,polars,pyspark}`, `dagster-gcp-{pandas,pyspark}`,
  `dagster-deltalake-{pandas,polars}`, `dagster-clickhouse-{pandas,polars}`.
- **BI assets are external by default.** `load_*_asset_specs` returns `AssetSpec` objects, which
  are not materializable. You get lineage and metadata, not execution, until you wrap them in a
  refresh definition.
- **Workspace objects are dual-purpose.** `FivetranWorkspace`, `AirbyteWorkspace`,
  `TableauCloudWorkspace`, `PowerBIWorkspace`, `SigmaOrganization` and `LookerResource` are both the
  resource you register in `Definitions` and the argument you pass to the spec loader. Register the
  same instance in both places or the assets will not resolve at runtime.
- **`dagster-postgres` is storage, not a connection resource** (`dagster-mysql` too, though it also
  exports `MySQLResource`). **DuckDB docs live under the MotherDuck slug**
  (`/integrations/libraries/motherduck`); one package drives both local DuckDB and MotherDuck.
- **State-backed components cache remote metadata.** `SigmaComponent`, `OmniComponent` and
  `DatabricksWorkspaceComponent` snapshot the upstream workspace; refresh state when the remote
  changes rather than expecting a live read at load time.
- **Community integrations release independently.** Their API reference is not in the main Dagster
  API docs, so `dg list components --package <pkg>` is the reliable discovery path. Validate wiring
  with `dg check defs` before `dg launch`; a missing env var in `defs.yaml` fails there, not mid-run.

## Legacy and superseded patterns

| Superseded | Use instead |
| --- | --- |
| `dagster_embedded_elt.sling` / `.dlt` | `dagster_sling` / `dagster_dlt` |
| `AirbyteResource`, `load_assets_from_airbyte_instance`, `build_airbyte_assets`, `airbyte_sync_op` | `AirbyteWorkspace` / `AirbyteCloudWorkspace` + `load_airbyte_asset_specs` |
| `FivetranResource`, `fivetran_resource`, `load_assets_from_fivetran_instance`, `build_fivetran_assets`, `fivetran_sync_op` | `FivetranWorkspace` + `load_fivetran_asset_specs` |
| `dbt_cloud_resource`, `DbtCloudClientResource`, `load_assets_from_dbt_cloud_job`, `@dbt_cloud_assets`, `dbt_cloud_run_op` | `DbtCloudWorkspace` / `DbtCloudComponent` |
| `pyspark_resource` (configured style) | `PySparkResource` |
| `databricks_client`, `databricks_pyspark_step_launcher` | `DatabricksClientResource`, `PipesDatabricksClient` |
| `slack_resource`, `msteams_resource`, `datadog_resource`, `prometheus_resource`, `pagerduty_resource`, `twilio_resource`, `bigquery_resource`, `gcs_resource`, `dataproc_resource`, `snowflake_resource`, `ssh_resource`, `adls2_resource`, `s3_resource` | `SlackResource`, `MSTeamsResource`, `DatadogResource`, `PrometheusResource`, `PagerDutyService`, `TwilioResource`, `BigQueryResource`, `GCSResource`, `DataprocResource`, `SnowflakeResource`, `SSHResource`, `ADLS2Resource`, `S3Resource` |
| `ConfigurablePickledObjectS3IOManager`, `ConfigurablePickledObjectGCSIOManager`, `ConfigurablePickledObjectADLS2IOManager` | `S3PickleIOManager`, `GCSPickleIOManager`, `ADLS2PickleIOManager` |
| `dagit_base_url=` on Slack/Teams sensors and hooks | `webserver_base_url=` |
| `job_selection=` / `monitor_all_repositories=` on failure sensors | `monitored_jobs=` / `monitor_all_code_locations=` |
| `ge_validation_op_factory` (beta) | native asset checks, or `dagster_soda.SodaScanComponent` |

## Sources
- https://docs.dagster.io/integrations — overview
- https://docs.dagster.io/integrations/libraries — library index, support tags, and the per-library page behind every catalog row
- https://docs.dagster.io/integrations/libraries/snowflake — resource, I/O managers, SQL and dbt components
- https://docs.dagster.io/integrations/libraries/motherduck — dagster-duckdb resource and I/O managers
- https://docs.dagster.io/integrations/libraries/gcp — BigQuery, GCS, Dataproc; https://docs.dagster.io/integrations/libraries/postgres — instance storage
- https://docs.dagster.io/integrations/libraries/aws — S3, Secrets Manager, SSM, Redshift, Athena, Pipes clients
- https://docs.dagster.io/integrations/libraries/looker — LookerResource, PDT builds, LookerComponent
- https://docs.dagster.io/integrations/libraries/tableau — Tableau Cloud/Server workspaces and refresh
- https://docs.dagster.io/integrations/libraries/powerbi — PowerBIWorkspace and semantic model refresh
- https://docs.dagster.io/integrations/libraries/slack — SlackResource and failure sensors
- https://docs.dagster.io/integrations/libraries/openai — OpenAIResource and with_usage_metadata
- https://docs.dagster.io/integrations/libraries/mlflow — mlflow_tracking
- https://docs.dagster.io/integrations/libraries/databricks — resources, components, Pipes
- https://docs.dagster.io/integrations/libraries/spark — Pipes-based Spark and SparkDeclarativePipelineComponent
- https://docs.dagster.io/integrations/libraries/airlift — compatibility and migration stages
- https://docs.dagster.io/migration/airflow-to-dagster/airflow-component-tutorial — AirflowInstanceComponent
- https://docs.dagster.io/integrations/libraries/embedded-elt — deprecation notice
