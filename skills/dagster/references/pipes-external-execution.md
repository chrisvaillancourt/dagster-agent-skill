# Dagster Pipes (external execution)

Dagster Pipes runs code that lives outside the Dagster process — a subprocess, container, Spark job, or cloud compute service — as a Dagster asset, streaming logs, metadata, and asset check results back into the run. This file covers both sides of the protocol: the orchestration side (`dagster` Pipes clients, context injectors, message readers) and the external side (the `dagster-pipes` package). Native in-process assets are in `assets.md`, asset checks in `testing-and-asset-checks.md`, resource wiring in `resources-and-config.md`, and non-Pipes integrations (dbt, Fivetran, Sling) in `integrations-dbt-and-elt.md` and `integrations-catalog.md`.

## Contents
- [When to use Pipes](#when-to-use-pipes)
- [How the protocol works](#how-the-protocol-works)
- [Subprocess quickstart](#subprocess-quickstart)
- [External process API](#external-process-api)
- [Rich metadata from external code](#rich-metadata-from-external-code)
- [Multi-assets, checks, and custom messages](#multi-assets-checks-and-custom-messages)
- [PythonScriptComponent](#pythonscriptcomponent)
- [Pipes clients](#pipes-clients)
- [Client patterns](#client-patterns)
- [Spark patterns](#spark-patterns)
- [Custom launchers with open_pipes_session](#custom-launchers-with-open_pipes_session)
- [Context injectors and message readers](#context-injectors-and-message-readers)
- [Non-Python external code](#non-python-external-code)
- [Testing](#testing)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## When to use Pipes

Use Pipes when the compute must run somewhere Dagster does not: an existing Spark cluster, a Databricks job, a Kubernetes pod, a Lambda function, a Node.js script. The external environment needs no Dagster install — only `dagster-pipes`, a single-file dependency-free Python package that can be pip-installed or vendored.

Do not use Pipes when the code can run in the Dagster process. Pipes assets cannot use IO managers to pass values (see `io-managers.md`), so data must move through object storage or be summarized into metadata and custom messages. You can omit `dagster-pipes` entirely on the external side: Pipes still launches the process and fails the step on a non-zero exit, you just lose log streaming and event reporting.

## How the protocol works

| Term | Meaning |
| --- | --- |
| Orchestration process | Dagster code materializing the asset; launches the external process. |
| External process | The remote/child process running your code. |
| Bootstrap payload | Small key-value bundle telling the external process where context lives and where to write messages. Usually environment variables; CLI args are the alternative. |
| Context payload | JSON derived from `AssetExecutionContext` (asset keys, partition keys, extras). Written by the context injector, read by the context loader. |
| Messages | JSON lines written by the external process: materializations, check results, logs, custom payloads. Written by the message writer, read by the message reader. |

The orchestration side pairs a **context injector** with a **message reader**; the external side pairs a matching **context loader** with a **message writer**, plus a **params loader** that reads the bootstrap payload. Pipes clients instantiate compatible defaults for you.

## Subprocess quickstart

Use when the external code is a script on the same machine. `dg.PipesSubprocessClient` is a resource.

```python
# src/<project>/defs/dagster_code.py
import shutil

import dagster as dg


@dg.asset
def subprocess_asset(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
) -> dg.MaterializeResult:
    cmd = [shutil.which("python"), dg.file_relative_path(__file__, "external_code.py")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
        extras={"foo": "bar"},
        env={"MY_ENV_VAR_IN_SUBPROCESS": "my_value"},
    ).get_materialize_result()
```

```python
# src/<project>/defs/resources.py
import dagster as dg


@dg.definitions
def resources():
    return dg.Definitions(
        resources={"pipes_subprocess_client": dg.PipesSubprocessClient()}
    )
```

```python
# src/<project>/defs/external_code.py
from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    context = PipesContext.get()
    context.log.info(f"extra foo={context.get_extra('foo')}")
    context.report_asset_materialization(metadata={"total_orders": 2})
    context.report_asset_check(check_name="no_empty_order_check", passed=True)


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
```

`run()` returns a `PipesClientCompletedInvocation`. Pick the accessor that matches what the external code reported.

| Accessor | Returns | Use for |
| --- | --- | --- |
| `get_materialize_result()` | one `MaterializeResult` | single-asset Pipes asset |
| `get_asset_check_result()` | one `AssetCheckResult` | `@asset_check` that only reports a check |
| `get_results()` | all events | `@multi_asset`, or materialization plus checks |
| `get_custom_messages()` | list of JSON payloads | data passed back for orchestration-side use |

`PipesSubprocessClient` constructor args: `env`, `cwd`, `context_injector`, `message_reader`, `forward_termination` (SIGINT the subprocess on cancel, default `True`), `forward_stdio` (default `True`), `termination_timeout_seconds` (default 20). `run()` accepts `command`, `context`, `extras`, `env`, `cwd`.

Verify with `dg check defs`, then `dg launch --assets subprocess_asset`.

## External process API

`open_dagster_pipes()` initializes the session and returns the singleton `PipesContext`; retrieve it anywhere else with `PipesContext.get()`. Call it near the process entry point, inside a `with` block so buffered messages flush on exit.

| Member | Notes |
| --- | --- |
| `log` | `logging.Logger` that streams to the Dagster run. Prefer over `print`. |
| `report_asset_materialization(metadata=, data_version=, asset_key=)` | `asset_key` required when multiple assets are in scope. |
| `report_asset_check(check_name=, passed=, severity=, metadata=, asset_key=)` | `severity` defaults to `"ERROR"`. |
| `report_custom_message(payload)` | Any JSON-serializable value; read back with `get_custom_messages()`. |
| `extras`, `get_extra(key)` | User data passed via the client's `extras=`. `get_extra` raises if the key is absent. |
| `asset_key`, `asset_keys` | Singular form raises if 0 or multiple assets are in scope. |
| `partition_key`, `partition_key_range`, `partition_time_window`, `is_partition_step` | `partition_time_window` returns `None` for non-temporal partitions. |
| `code_version`, `provenance`, and `*_by_asset_key` variants | Data-version provenance for the scoped assets. |
| `run_id`, `job_name`, `retry_number`, `is_asset_step` | Run-level context. |
| `close()`, `is_closed` | Idempotent flush; the context manager calls it for you. |

`open_dagster_pipes()` takes `params_loader`, `context_loader`, and `message_writer` to override the defaults (`PipesEnvVarParamsLoader`, `PipesDefaultContextLoader`, `PipesDefaultMessageWriter`).

## Rich metadata from external code

`dagster-pipes` has no dependency on `dagster`, so rich metadata types are expressed as `{"type": ..., "raw_value": ...}` dicts. Plain `float`, `int`, `bool`, `str`, and `None` values pass through without the wrapper.

| `type` | `raw_value` shape |
| --- | --- |
| `int`, `float`, `bool`, `text`, `timestamp` | the scalar |
| `url`, `path`, `notebook` | a string |
| `md` | markdown string |
| `json` | any JSON-serializable value |
| `table` | `{"schema": [<column dicts>], "records": [<row dicts>]}` |
| `table_schema` | list of column dicts: `name`, `type`, `description`, `tags`, `constraints` |
| `table_column_lineage` | `{"table_column_lineage": {"<col>": [{"asset_key": ..., "column": ...}]}}` |

```python
context.report_asset_materialization(
    asset_key="foo",
    metadata={
        "row_count": {"type": "int", "raw_value": 99_000},
        "dashboard": {"type": "url", "raw_value": "http://example.com"},
        "schema": {"type": "table_schema", "raw_value": [{"name": "id", "type": "int"}]},
    },
)
```

## Multi-assets, checks, and custom messages

`report_asset_materialization` may be called only once per asset key. A second call for the same key errors with "Asset has already been materialized". For a `@multi_asset`, set `asset_key` explicitly on every call and collect with `get_results()`.

```python
@dg.multi_asset(specs=[dg.AssetSpec("orders"), dg.AssetSpec("users")])
def subprocess_asset(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
):
    return pipes_subprocess_client.run(command=cmd, context=context).get_results()
```

For checks declared on the asset, add `check_specs=[dg.AssetCheckSpec(name="no_empty_order_check", asset="subprocess_asset")]` to `@dg.asset` and report the check by that name from the external code. For a standalone `@dg.asset_check`, pass `context.op_execution_context` as the client's `context` and set `asset_key` on `report_asset_check`.

Custom messages move data the orchestration side needs but Dagster should not record as metadata — for example, a small summary table that a downstream asset consumes through an IO manager.

```python
result = pipes_subprocess_client.run(command=cmd, context=context)
summary_df = pd.DataFrame(result.get_custom_messages()[0])
return dg.Output(value=summary_df, metadata=result.get_materialize_result().metadata)
```

## PythonScriptComponent

Use when the external code is a Python script and you want no orchestration Python at all. `dagster.PythonScriptComponent` wraps the subprocess client behind YAML. See `components.md` for component mechanics.

```bash
dg scaffold defs dagster.PythonScriptComponent generate_revenue_report
```

```yaml
# src/<project>/defs/generate_revenue_report/defs.yaml
type: dagster.PythonScriptComponent

attributes:
  execution:
    path: process_sales_data.py
  assets:
    - key: sales_revenue_report
      description: "Daily sales revenue report generated from transaction data"
      group_name: "analytics"
      kinds: ["python", "report"]
```

Multiple component instances go in one file separated by `---`; use `deps: [<other key>]` to chain them. The script needs no Pipes code, but adding `open_dagster_pipes()` gives you structured logs and metadata. Confirm with `dg list defs`.

## Pipes clients

Every client exposes `run(context=..., extras=..., ...)` and returns `PipesClientCompletedInvocation`. Install the library named in the package column.

| Client | Package | Key `run()` args | Defaults and notes |
| --- | --- | --- | --- |
| `PipesSubprocessClient` | `dagster` | `command`, `env`, `cwd` | Temp-file context and messages; stdio forwarded. |
| `PipesDockerClient` | `dagster-docker` | `image`, `command`, `env` | Env context injector, messages parsed from the container log stream. |
| `PipesK8sClient` | `dagster-k8s` | `image`, `command`, `namespace`, `env`, `base_pod_spec`, `base_pod_meta`, `ignore_containers`, `pod_wait_timeout`, `delete_pod_on_completion` | Env injector, `PipesK8sPodLogsMessageReader`. |
| `PipesDatabricksClient` | `dagster-databricks` | `task` (a `jobs.SubmitTask`) | `PipesDbfsContextInjector` / `PipesDbfsMessageReader`. Classic clusters only. |
| `PipesDatabricksServerlessClient` | `dagster-databricks` | `task` | Unity Catalog Volume transport via `volume_path=`. Use for serverless, where DBFS is unavailable. |
| `PipesLambdaClient` | `dagster-aws` | `function_name`, `event` | Context injected through the Lambda event; messages parsed from the log tail. |
| `PipesGlueClient` | `dagster-aws` | `start_job_run_params` | Defaults to a CloudWatch message reader on the job's `.../output/` stream. |
| `PipesECSClient` | `dagster-aws` | `run_task_params` (needs `taskDefinition`), `pipes_container_name`, `waiter_config` | Env injector, `PipesCloudWatchMessageReader`. |
| `PipesEMRClient` | `dagster-aws` | `run_job_flow_params` | Starts a new cluster per invocation. Pair with `PipesS3MessageReader`. |
| `PipesEMRServerlessClient` | `dagster-aws` | `start_job_run_params` | `PipesCloudWatchMessageReader`; enable CloudWatch logging in `configurationOverrides`. |
| `PipesEMRContainersClient` | `dagster-aws` | `start_job_run_params` | Preview. `pipes_params_bootstrap_method` defaults to `"args"`. Pair with `PipesS3MessageReader`. |
| `PipesDataprocJobClient` | `dagster-gcp` | `submit_job_params` | Preview. Pair with `PipesGCSMessageReader` and `PipesGCSContextInjector`. |
| `PipesAzureMLClient` | `dagster-azure` | `command` (an `azure.ai.ml.command`) | Pair with the Azure Blob Storage injector and reader. |

Clients that launch remote work also take `forward_termination` (cancel the remote job when the Dagster run is terminated) and a polling interval.

## Client patterns

Every client follows the same shape: declare it as a resource, call `run()` in the asset body, return a result accessor.

```python
from dagster_k8s import PipesK8sClient


@dg.asset
def k8s_pipes_asset(context: dg.AssetExecutionContext, k8s_pipes_client: PipesK8sClient):
    return k8s_pipes_client.run(
        context=context, image="pipes-example:v1", extras={"some_parameter": 1}
    ).get_materialize_result()
```

If the default config does not target the right cluster, set `load_incluster_config`, `kubeconfig_file`, or `kube_context` on the client. Only the first container in `pod.spec.containers` can talk back to Dagster, though every container can read the context.

Override the transport on the client when the defaults do not fit. AWS Glue, for example, can take context from S3 and messages from CloudWatch:

```python
PipesGlueClient(
    client=boto3.client("glue"),
    context_injector=PipesS3ContextInjector(
        client=boto3.client("s3"), bucket=dg.EnvVar("DAGSTER_GLUE_S3_CONTEXT_BUCKET")
    ),
    message_reader=PipesCloudWatchMessageReader(client=boto3.client("logs")),
)
```

Databricks on a classic cluster submits a `jobs.SubmitTask`; include `{"pypi": {"package": "dagster-pipes"}}` in `libraries` and set `new_cluster.cluster_log_conf.dbfs` so driver stdout/stderr are forwarded. On serverless, use `PipesDatabricksServerlessClient(volume_path="/Volumes/<catalog>/<schema>/dagster_pipes")` and load params in the notebook with `PipesDatabricksNotebookWidgetsParamsLoader(dbutils.widgets)` alongside `PipesUnityCatalogVolumesContextLoader` and `PipesUnityCatalogVolumesMessageWriter`.

## Spark patterns

Spark jobs usually cannot take injected environment variables, so pass the bootstrap payload as CLI arguments and route context and messages through object storage.

```python
# PySpark on any Spark cluster, S3 as the transport
with dg.open_pipes_session(
    context=context,
    context_injector=PipesS3ContextInjector(client=s3_client, bucket=bucket),
    message_reader=PipesS3MessageReader(
        client=s3_client, bucket=bucket, include_stdio_in_messages=True
    ),
) as session:
    args = " ".join(f"{k} {v}" for k, v in session.get_bootstrap_cli_arguments().items())
    subprocess.run(f"spark-submit ... s3a://{bucket}/{script} {args}", shell=True, check=True)
return session.get_results()
```

The job script then loads params from those CLI args:

```python
with open_dagster_pipes(
    message_writer=PipesS3MessageWriter(client=boto3.client("s3")),
    context_loader=PipesS3ContextLoader(client=boto3.client("s3")),
    params_loader=PipesCliArgsParamsLoader(),
) as pipes:
    ...
```

`include_stdio_in_messages=True` on the reader tells the external message writer to collect driver stdout/stderr and ship it as Pipes messages, which is how logs get back when the launcher does not forward stdio.

Variants:
- **Scala/Java Spark** — same orchestration code, but the JAR uses the community Java Pipes framework (`context.getLogger().info`, `reportAssetMaterialization`, `reportAssetCheck`). No official Dagster library for the JVM.
- **EMR / EMR Serverless / EMR on EKS** — replace `spark-submit` with the matching client. Package Python dependencies as a PEX file uploaded to S3, then `spark-submit --files s3://<bucket>/venv.pex --conf spark.pyspark.python=./venv.pex`.
- **Spark Connect / Databricks Connect** — not Pipes at all. The asset body is ordinary PySpark code against a `SparkSession` or `DatabricksSession` resource, so IO managers and resources work normally. Prefer this when you want your Spark logic to stay in the Dagster project.

## Custom launchers with open_pipes_session

Use when no client exists for your platform, or you need to stream results while a long job is still running. `dg.open_pipes_session` gives you the session and leaves launching to you.

```python
@dg.asset
def some_pipes_asset(context: dg.AssetExecutionContext) -> Iterator[dg.PipesExecutionResult]:
    with dg.open_pipes_session(
        context=context,
        extras={"foo": "bar"},
        context_injector=dg.PipesTempFileContextInjector(),
        message_reader=dg.PipesTempFileMessageReader(),
    ) as pipes_session:
        env_vars = pipes_session.get_bootstrap_env_vars()
        external_process = launch_external_process(env_vars)
        while not is_external_process_done(external_process):
            yield from pipes_session.get_results()
    yield from pipes_session.get_results()
```

`PipesSession` members worth knowing:

| Member | Use |
| --- | --- |
| `get_bootstrap_env_vars()` | Standard path. Values are JSON, gzipped, base64-encoded. |
| `get_bootstrap_cli_arguments()` | When env vars are unavailable (Spark). zlib-compressed, base64-encoded. |
| `get_results(implicit_materializations=, metadata=)` | Drain buffered events. `metadata=` attaches launch-API values (job ids, URLs) to every result. |
| `get_reported_results()` | Only what the external process explicitly reported, with no implicit materializations. |
| `get_custom_messages()` | Payloads from `report_custom_message`. |
| `get_per_writer_bootstrap_env_vars()` | Preview. One mapping per writer when using `PipesCompositeMessageReader` for multi-process jobs such as Databricks multi-task. |

The opening routine writes the context and starts the reader thread; the closing routine drains remaining messages and cleans up resources. Yield results from inside the `with` block for streaming, and once more after it to catch the tail.

To support an entirely new platform, subclass `dagster.PipesContextInjector` plus `dagster_pipes.PipesContextLoader`, and `dagster.PipesMessageReader` plus `dagster_pipes.PipesMessageWriter` — always in matching pairs. For blob stores, subclass `PipesBlobStoreMessageReader` and `PipesBlobStoreMessageWriter` instead of the bare ABCs; they chunk and buffer messages on a fixed interval (10 seconds by default) so you are not doing one blob round-trip per message. `PipesMessageHandler` processes the decoded messages and rarely needs overriding.

## Context injectors and message readers

Orchestration side (`dagster` and integration libraries):

| Context injector | Message reader | Environment |
| --- | --- | --- |
| `PipesEnvContextInjector` | — | Context inline in env vars; used by Docker, K8s, ECS, EMR, Dataproc. |
| `PipesFileContextInjector(path=)`, `PipesTempFileContextInjector` | `PipesFileMessageReader(path=, include_stdio_in_messages=, cleanup_file=)`, `PipesTempFileMessageReader` | Shared filesystem. Subprocess defaults. |
| `PipesS3ContextInjector(bucket=, client=, key_prefix=)` | `PipesS3MessageReader(bucket=, client=, interval=, log_readers=, include_stdio_in_messages=)` | AWS, Spark on EMR. |
| `PipesGCSContextInjector` | `PipesGCSMessageReader` | GCP Dataproc. |
| `PipesAzureBlobStorageContextInjector` | `PipesAzureBlobStorageMessageReader` | Azure ML. |
| `PipesDbfsContextInjector(client=)` | `PipesDbfsMessageReader(client=, cluster_log_root=, include_stdio_in_messages=, log_readers=)` | Databricks classic clusters. |
| `PipesLambdaEventContextInjector` | `PipesCloudWatchMessageReader`, `PipesK8sPodLogsMessageReader`, `PipesDockerLogsMessageReader` | Log-scraping readers; no separate storage needed. |
| — | `PipesCompositeMessageReader(readers=[...])` | Preview. One reader per external writer for multi-process jobs. |

External side (`dagster_pipes`), which must match:

| Params loader | Context loader | Message writer |
| --- | --- | --- |
| `PipesEnvVarParamsLoader` (default) | `PipesDefaultContextLoader` | `PipesDefaultMessageWriter` (file or stdout/stderr) |
| `PipesCliArgsParamsLoader` | `PipesS3ContextLoader(client=)` | `PipesS3MessageWriter(client=, interval=)` |
| `PipesMappingParamsLoader(event)` | `PipesGCSContextLoader(client=)` | `PipesGCSMessageWriter(client=, interval=)` |
| `PipesDatabricksNotebookWidgetsParamsLoader(dbutils.widgets)` | `PipesDbfsContextLoader`, `PipesUnityCatalogVolumesContextLoader` | `PipesDbfsMessageWriter`, `PipesUnityCatalogVolumesMessageWriter` |
| — | `PipesAzureBlobStorageContextLoader(client=)` | `PipesAzureBlobStorageMessageWriter(client=, interval=)` |

## Non-Python external code

For production TypeScript or JavaScript, use the `@dagster-io/dagster-pipes` npm package. Launch it with the subprocess client and mark the asset's compute kind:

```python
@dg.asset(compute_kind="javascript")
def my_asset(context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient):
    return pipes_subprocess_client.run(
        command=["node", "tensorflow/main.js"],
        context=context,
        extras={"operation_name": "train_model", "config": {"path_to_data": "..."}},
    ).get_materialize_result()
```

Any language can implement the protocol directly. The subprocess client sets two environment variables, `DAGSTER_PIPES_CONTEXT` and `DAGSTER_PIPES_MESSAGES`, each a base64-encoded zlib-compressed JSON object carrying a `path`. Read the context JSON from the first path; append one JSON message per line to the second. Scala and Java use the community Java Pipes framework.

## Testing

Run the external script standalone first. When `open_dagster_pipes()` finds no Dagster bootstrap payload it emits a warning and returns a `MagicMock`, so every `pipes.*` call becomes a no-op instead of crashing.

Then exercise the asset in-process, supplying the client as a resource:

```python
def test_subprocess_asset():
    result = dg.materialize(
        [subprocess_asset],
        resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
    )
    assert result.success
```

`dg check defs` validates the definitions load, `dg list defs` shows the resulting assets, and `dg launch --assets <key>` runs one for real. See `testing-and-asset-checks.md` for assertion helpers.

## Gotchas

- **Asset key must match.** Omit `asset_key` only when exactly one asset is in scope. With a multi-asset, omitting it raises; reporting a key not in scope raises too.
- **One materialization per key.** A second `report_asset_materialization` for the same key fails the step.
- **Non-zero exit fails the step** regardless of what was reported. A clean exit with nothing reported still produces a materialization for the expected assets, since `get_results()` synthesizes implicit ones.
- **Extras must be JSON-serializable.** They are embedded in the context payload.
- **No IO managers.** Pipes assets cannot receive loaded inputs or return values through IO managers. Pass paths via `extras`, return a path string from the asset so an IO manager stores it, or pull an upstream metadata value off the latest materialization event.
- **Long-running or remote jobs need durable transport.** Log-scraping readers (CloudWatch, pod logs, the Lambda log tail) can truncate or lag. Prefer an S3, GCS, Azure Blob, or DBFS message reader for anything long-lived, and set `include_stdio_in_messages=True` when the launcher does not forward stdio.
- **Injector and reader must match the loader and writer.** A DBFS message reader with a default message writer produces a session that reads nothing; the reader's `no_messages_debug_text` is the error you will see.
- **Databricks log latency.** `cluster_log_conf.dbfs` forwarding is bounded by Databricks writing those files every five minutes. With `existing_cluster_id` you cannot set env vars at all, so the script must use `PipesCliArgsParamsLoader`.
- **Env var size.** The bootstrap payload is compressed and base64-encoded, but a large context still bloats CLI arguments. Storing context in S3 or GCS keeps the argument list small.
- **Prefer `pipes.log` over `print`.** Printed output lands in raw compute logs, not the structured event log, and only reaches Dagster if stdio is forwarded or captured.

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| `databricks_pyspark_step_launcher` | `PipesDatabricksClient` or `PipesDatabricksServerlessClient` |
| `emr_pyspark_step_launcher` | `PipesEMRClient`, `PipesEMRServerlessClient`, or `PipesEMRContainersClient` |
| Step launchers generally | Dagster Pipes |

Step launchers still work and are not scheduled for removal, but they receive no new features. Migrating means Dagster no longer manages the Spark runtime or your code's deployment: build the environment in CI/CD, drop `dagster` from the job's dependencies, add `dagster-pipes`, and upload the script and a PEX of its dependencies to object storage before deploying the code location. Keep step launchers if you depend heavily on resources and IO managers inside the remote step.

## Sources
- https://docs.dagster.io/integrations/external-pipelines — Pipes overview
- https://docs.dagster.io/integrations/external-pipelines/using-dagster-pipes — subprocess tutorial index
- https://docs.dagster.io/integrations/external-pipelines/using-dagster-pipes/create-subprocess-asset — defining the subprocess asset
- https://docs.dagster.io/integrations/external-pipelines/using-dagster-pipes/modify-external-code — adding Pipes to existing code
- https://docs.dagster.io/integrations/external-pipelines/using-dagster-pipes/reference — extras, checks, multi-assets, custom messages, rich metadata
- https://docs.dagster.io/integrations/external-pipelines/dagster-pipes-details-and-customization — protocol terms, session lifecycle, custom components
- https://docs.dagster.io/integrations/external-pipelines/kubernetes-pipeline — PipesK8sClient
- https://docs.dagster.io/integrations/external-pipelines/aws/aws-lambda-pipeline — PipesLambdaClient
- https://docs.dagster.io/integrations/external-pipelines/aws/aws-glue-pipeline — PipesGlueClient
- https://docs.dagster.io/integrations/external-pipelines/aws/aws-ecs-pipeline — PipesECSClient
- https://docs.dagster.io/integrations/external-pipelines/aws/aws-emr-pipeline — PipesEMRClient and PEX packaging
- https://docs.dagster.io/integrations/external-pipelines/aws/aws-emr-serverless-pipeline — PipesEMRServerlessClient
- https://docs.dagster.io/integrations/external-pipelines/aws/aws-emr-containers-pipeline — PipesEMRContainersClient
- https://docs.dagster.io/integrations/external-pipelines/gcp-dataproc-pipeline — PipesDataprocJobClient
- https://docs.dagster.io/integrations/external-pipelines/azureml-pipeline — PipesAzureMLClient
- https://docs.dagster.io/integrations/external-pipelines/pyspark-pipeline — PySpark with S3 transport and CLI args
- https://docs.dagster.io/integrations/external-pipelines/scalaspark-pipeline — Scala/Java Spark
- https://docs.dagster.io/integrations/external-pipelines/spark-databricks-connect-pipeline — Spark Connect alternative
- https://docs.dagster.io/integrations/external-pipelines/javascript-pipeline — JavaScript and the raw env var protocol
- https://docs.dagster.io/integrations/external-pipelines/migrating-from-step-launchers-to-pipes — step launcher migration
- https://docs.dagster.io/integrations/libraries/databricks/pipes/classic-clusters — DBFS transport
- https://docs.dagster.io/integrations/libraries/databricks/pipes/serverless-compute — Unity Catalog Volumes transport
- https://docs.dagster.io/integrations/libraries/docker — PipesDockerClient
- https://docs.dagster.io/guides/build/components/building-pipelines-with-components/python-script-component-tutorial — PythonScriptComponent
- https://docs.dagster.io/api/dagster/pipes — orchestration-side API
- https://docs.dagster.io/integrations/libraries/pipes/dagster-pipes — external-side API
