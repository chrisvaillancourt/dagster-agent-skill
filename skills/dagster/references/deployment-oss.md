# Deploying Dagster OSS

How to run self-hosted Dagster in production: the long-running services, how code locations are served in production, and the Docker Compose, Kubernetes/Helm, AWS ECS, GCP and single-machine deployment options. Instance-level knobs (`dagster.yaml` sections, concurrency, run retries, run monitoring, compute log storage details) are in `operations-and-debugging.md`. Dagster+ Serverless/Hybrid is in `dagster-plus.md`. Launching work in external containers from inside an asset is in `pipes-external-execution.md`. Project layout and the `dg` CLI are in `project-setup-and-cli.md`.

## Contents
- [Architecture](#architecture)
- [Code locations in production](#code-locations-in-production)
- [Docker Compose](#docker-compose)
- [Kubernetes with Helm](#kubernetes-with-helm)
- [Kubernetes execution configuration](#kubernetes-execution-configuration)
- [Celery with Kubernetes](#celery-with-kubernetes)
- [AWS: ECS, EC2, RDS, S3](#aws-ecs-ec2-rds-s3)
- [GCP: GCE, Cloud SQL, GCS](#gcp-gce-cloud-sql-gcs)
- [Single machine as a service](#single-machine-as-a-service)
- [Production instance checklist](#production-instance-checklist)
- [CI/CD for OSS](#cicd-for-oss)
- [Upgrades and instance migration](#upgrades-and-instance-migration)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Architecture

Use when: deciding what processes to run and how they talk to each other.

Three kinds of long-running service:

| Service | Command | Replicas |
| --- | --- | --- |
| Webserver | `dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml` | One or more |
| Daemon | `dagster-daemon run` | Exactly one |
| Code location server (one per code location) | `dagster code-server start -h 0.0.0.0 -p 4000 -m <module>` | One per code location |

The daemon runs the scheduler, sensor, run-queue and run-monitoring daemons in threads; which ones are active is derived from `dagster.yaml`. The sensor daemon is always enabled; the run-queue daemon requires `QueuedRunCoordinator`.

Run execution flow for a run launched from the UI:

1. **Run coordinator** (instance-level) — invoked by the webserver; `QueuedRunCoordinator` hands the run to the daemon's queue, `DefaultRunCoordinator` goes straight to the launcher.
2. **Run launcher** (instance-level) — invoked by the daemon; creates a **run worker** (process, container, ECS task, or K8s job).
3. **Run worker** — traverses the execution plan.
4. **Executor** (run config) — runs each step, either in-process, in subprocesses, or in its own container/pod/task.

Runs launched by schedules and sensors follow the same path but start at the daemon, which asks the code server to evaluate each tick (see [Gotchas](#gotchas)). Configuration lives at three levels: instance (`dagster.yaml`), workspace (`workspace.yaml` or `dg.toml`), and run (run config).

In production, definitions load inside the code server process, never inside the webserver. The webserver and daemon both hold gRPC clients to every code server and must be able to reach all of them.

## Code locations in production

Use when: wiring the webserver and daemon to your project code.

A `create-dagster` project exposes its entry point at `src/<project>/definitions.py`, which calls `dg.load_from_defs_folder(...)`. Serve that module:

```bash
# one container/pod per code location
dagster code-server start -h 0.0.0.0 -p 4000 -m my_project.definitions
```

`dagster code-server start` is documented with `-f definitions.py`; the `-m` module form shown above follows `dagster api grpc`, which is the lower-level equivalent and what the Helm chart invokes via `dagsterApiGrpcArgs` (the `code-server start` flag list itself is not in the published CLI reference). Useful flags on both target forms: `-m/--module-name`, `-f/--python-file`, `--package-name`, `-a/--attribute`, `-d/--working-directory`, `--autoload-defs-module-name <module>` (import and recursively search a module for definitions), `-p/--port` or `-s/--socket`, `--location-name`, `--container-image`, `--log-format json`.

Point the host processes at the servers with `workspace.yaml`:

```yaml
# workspace.yaml
load_from:
  - grpc_server:
      host: my_code_server
      port: 4000
      location_name: my_project
  - python_module:
      module_name: other_project.definitions
```

`load_from` entries are one of `grpc_server` (`host`, `port`, `location_name`), `python_module` (`module_name`), or `python_file` (`relative_path`, plus optional `attribute`, `working_directory`, `executable_path`, `location_name`). Prefer `python_module` over `python_file`; prefer `grpc_server` once each location has its own image.

Isolated environments: give each location its own `executable_path` (same host, different virtualenvs) or, better, its own container image. For multi-project repos managed by `dg`, a workspace directory with a root `dg.toml` (`directory_type = "workspace"`, `[[workspace.projects]] path = ...`) drives `dg dev` locally; see `project-setup-and-cli.md`. The published migration page from `workspace.yaml` to `dg.toml` is still a stub, so treat `workspace.yaml` as the supported production wiring for OSS.

Set `DAGSTER_CURRENT_IMAGE` in each code server's environment to the image name. Run launchers and executors that create containers (`DockerRunLauncher`, `K8sRunLauncher`, `docker_executor`, `k8s_job_executor`) use it to launch run workers from the same image. The Helm chart sets it automatically.

Verify a location before shipping it: `dg check defs` (exit code 1 on error) and `dg list defs`.

Renaming a code location, or changing its workspace entry, requires stopping and restarting its schedules and sensors in the UI.

## Docker Compose

Use when: a single host (for example an EC2 box) should run the whole deployment.

Two images. One "system" image for the webserver and daemon holding `dagster`, `dagster-graphql`, `dagster-webserver`, `dagster-postgres`, `dagster-docker`, plus `dagster.yaml` and `workspace.yaml` copied into `DAGSTER_HOME`. One image per code location holding your project:

```dockerfile
# Dockerfile_user_code
FROM python:3.11-slim
RUN pip install dagster dagster-postgres dagster-docker
WORKDIR /opt/dagster/app
COPY . /opt/dagster/app
EXPOSE 4000
HEALTHCHECK --timeout=1s --start-period=3s --interval=3s --retries=20 \
  CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]
# CMD is overridden by run launchers/executors to execute runs and steps
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "my_project.definitions"]
```

Keep `CMD` (not `ENTRYPOINT`) for the code server: the run launcher overrides the command to execute a run in a new container from the same image. For a uv-managed project the equivalent body is `COPY pyproject.toml uv.lock ./`, `COPY src/ src/`, `uv sync --frozen` (unverified — the published Dockerfiles use pip).

The Compose file runs four kinds of service: `postgres` with a `pg_isready` healthcheck, one container per code location, the webserver, and the daemon. Webserver and daemon share the system image, get the Postgres connection as environment variables, and mount `/var/run/docker.sock` so they can launch and terminate run containers on the host Docker daemon. All services join one user-defined bridge network, and both host processes `depends_on` Postgres being healthy.

```yaml
# docker-compose.yaml (excerpt)
services:
  user_code:
    build: {context: ., dockerfile: ./Dockerfile_user_code}
    image: my_user_code_image
    environment:
      DAGSTER_CURRENT_IMAGE: 'my_user_code_image'
      DAGSTER_POSTGRES_HOST: 'postgresql'
    networks: [dagster_network]
  webserver:
    build: {context: ., dockerfile: ./Dockerfile_dagster}
    entrypoint: [dagster-webserver, -h, '0.0.0.0', -p, '3000', -w, workspace.yaml]
    ports: ['3000:3000']
    volumes: ['/var/run/docker.sock:/var/run/docker.sock']
    networks: [dagster_network]
  daemon:
    build: {context: ., dockerfile: ./Dockerfile_dagster}
    entrypoint: [dagster-daemon, run]
    restart: on-failure
    volumes: ['/var/run/docker.sock:/var/run/docker.sock']
    networks: [dagster_network]
```

Select `DockerRunLauncher` in `dagster.yaml` with `module: dagster_docker` / `class: DockerRunLauncher`. To run each step in its own container instead of each run, set `docker_executor` as the job/definitions executor; its run config accepts `registry`, `network`, `networks`, and `container_kwargs` (passed to `containers.create` in the Docker Python API). Configuration set on containers created by `DockerRunLauncher` is also applied to step containers.

## Kubernetes with Helm

Use when: you have a cluster and want per-run isolation without writing your own launcher.

```bash
helm repo add dagster https://dagster-io.github.io/helm
helm repo update
helm show values dagster/dagster > values.yaml
helm upgrade --install dagster dagster/dagster -f values.yaml
```

Pin the chart version to the Dagster version you run: `--version 1.13.0`. The chart generates the instance and workspace configuration for you, so do not hand-write `dagster.yaml` or `workspace.yaml` for a Helm deployment.

Minimum `values.yaml` edits to deploy your own code:

```yaml
dagster-user-deployments:
  deployments:
    - name: 'my-project'
      image:
        repository: 'my-registry/my-project'
        tag: '1'
        pullPolicy: Always # IfNotPresent only for a local image
      dagsterApiGrpcArgs:
        - '--module-name'
        - 'my_project.definitions'
      port: 3030
```

Key value sections:

| Key | Purpose |
| --- | --- |
| `dagster-user-deployments.deployments[]` | One entry per code location: `name`, `image`, `dagsterApiGrpcArgs`, `port`, `env`, `envConfigMaps`, `envSecrets` |
| `dagster-user-deployments.enableSubchart` | Set `false` to manage code locations as a separate Helm release |
| `runLauncher.type` | `K8sRunLauncher` (default) or `CeleryK8sRunLauncher` |
| `runLauncher.config.k8sRunLauncher` | Launcher config: `jobNamespace`, `loadInclusterConfig`, `kubeconfigFile`, `envConfigMaps`, `envSecrets`, `runK8sConfig` |
| `postgresql` | `enabled: false` plus `postgresqlHost`/`Username`/`Password`/`Database` and `service.port` to use an external database |
| `global.postgresqlSecretName` + `generatePostgresqlPasswordSecret: false` | Supply the DB password from a Secret you manage (Vault etc.) |
| `dagsterWebserver.workspace.enabled` + `.servers[].host`/`.port` | Point the webserver at externally-managed code servers |
| `migrate.enabled` | Renders the instance-migration Job template |

Split releases for faster, decoupled code deploys: disable the subchart in the `dagster` release, list the future code-server hosts/ports under `dagsterWebserver.workspace.servers`, then manage code locations with `helm upgrade --install user-code dagster/dagster-user-deployments -f values.yaml`.

Provision a ServiceAccount bound to a Role that can create Jobs and read Pods; the `k8s_job_op` RBAC needs `batch` `jobs`/`jobs/status` and core `pods`, `pods/log`, `pods/status`. Pass secrets as `envSecrets` (mapped with `envFrom`) rather than baking them into images.

`K8sRunLauncher` accepts these `dagster.yaml` keys when configured directly rather than through Helm:

```yaml
run_launcher:
  module: dagster_k8s.launcher
  class: K8sRunLauncher
  config:
    service_account_name: dagster
    job_image: 'my-registry/my-project:latest'
    dagster_home: '/opt/dagster/dagster_home'
    instance_config_map: 'dagster-instance'
    postgres_password_secret: 'dagster-postgresql-secret'
    job_namespace: 'dagster'
    image_pull_policy: 'IfNotPresent'
    env_config_maps: ['dagster-k8s-job-runner-env']
    env_secrets: ['dagster-k8s-some-secret']
    env_vars: ['ENV_VAR=1']
    run_k8s_config:
      container_config: {}
      pod_spec_config: {}
      job_spec_config: {}
```

Completed and failed Jobs (and their Pods) are removed after one day where the TTL controller is available. Tune with the `ttl_seconds_after_finished` key inside `job_spec_config`, or delete them with `kubectl` on clusters without the controller.

## Kubernetes execution configuration

Use when: individual runs or steps need different CPU, memory, node selectors or volumes.

Four scopes, from broadest to narrowest:

| Scope | Where | Applies to |
| --- | --- | --- |
| Deployment | `runLauncher.config.k8sRunLauncher.runK8sConfig` in `values.yaml` (`containerConfig`, `podSpecConfig`, `podTemplateSpecMetadata`, `jobSpecConfig`, `jobMetadata`) | Every run, and every step pod unless overridden |
| Code location | code location config | Every run from that location |
| Job/run | `dagster-k8s/config` tag on a `@job` | That job's run pod only |
| Step | `step_k8s_config` on `k8s_job_executor`, or `dagster-k8s/config` on an op/asset | Each step pod |

Per-run config via the tag, with the same five keys in snake_case:

```python
# src/my_project/defs/jobs.py
import dagster as dg

@dg.job(
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "250m", "memory": "64Mi"},
                    "limits": {"cpu": "500m", "memory": "2560Mi"},
                },
            },
            "pod_spec_config": {"node_selector": {"disktype": "ssd"}},
            "job_spec_config": {"ttl_seconds_after_finished": 7200},
        },
    },
)
def my_job(): ...
```

Per-step pods require `k8s_job_executor`; a job-level tag is **not** propagated to step pods:

```python
# src/my_project/defs/assets.py
from dagster_k8s import k8s_job_executor
import dagster as dg

@dg.asset(
    op_tags={
        "dagster-k8s/config": {
            "container_config": {"resources": {"requests": {"cpu": "200m", "memory": "32Mi"}}},
        }
    }
)
def my_asset(context: dg.AssetExecutionContext):
    context.log.info("running")

my_job = dg.define_asset_job("my_job", selection="my_asset", executor_def=k8s_job_executor)
```

`k8s_job_executor` run config keys: `job_namespace`, `image_pull_policy`, `image_pull_secrets`, `service_account_name`, `env_config_maps`, `env_secrets`, `env_vars`, `job_image` (omit when using user deployments), `max_concurrent` (per-run pod cap, not global), `step_k8s_config`.

Merge rules across scopes default to `DEEP`: dictionaries merge, lists append and de-duplicate, scalars are replaced by the narrower scope, and list fields that cannot be appended meaningfully (`command`, `args`) are replaced. Set `"merge_behavior": "SHALLOW"` inside the tag for whole-key replacement.

Do not reach for `k8s_job_executor` when steps are short: pod scheduling dominates. Prefer `dg.in_process_executor` when parallelism already comes from partitions, and the default `multi_or_in_process_executor` otherwise.

## Celery with Kubernetes

Use when: you must cap how many steps hit a shared resource across *all* running jobs. Per-run limits do not need Celery.

```bash
helm upgrade --install dagster dagster/dagster -f values.yaml \
  --set runLauncher.type=CeleryK8sRunLauncher --set rabbitmq.enabled=true
```

Every job on such an instance must set `executor_def=celery_k8s_job_executor`. Queues and worker counts live under `runLauncher.config.celeryK8sRunLauncher.workerQueues`; route ops with the `dagster-celery/queue` op tag (default queue is `dagster`). Priority comes from `dagster-celery/run_priority` (job tag) plus `dagster-celery/priority` (op tag); the two are summed. In production disable the in-chart RabbitMQ and point `redis` at an external broker (`enabled: true`, `internal: false`, `host`, `port`, `brokerDbNumber`, `backendDbNumber`). Celery adds a step-worker Job per step, so persistent object storage for intermediates is mandatory.

## AWS: ECS, EC2, RDS, S3

Use when: the deployment already lives in ECS, or a single EC2 host is enough.

EC2: run the webserver and daemon as described in [Single machine as a service](#single-machine-as-a-service). RDS: point `storage.postgres.postgres_db` at the instance. S3: use `S3ComputeLogManager` for stdout/stderr and `s3_pickle_io_manager` for op outputs (see `io-managers.md`).

ECS: `EcsRunLauncher` starts one ECS task per run and assumes the rest of the deployment already runs in ECS. By default each run registers its own task definition, inheriting networking, CPU, memory and environment from the launching task and overriding only the container command. With `DefaultRunCoordinator`, UI-launched runs inherit from the webserver task and schedule/sensor runs inherit from the daemon task.

```yaml
# dagster.yaml
run_launcher:
  module: 'dagster_aws.ecs'
  class: 'EcsRunLauncher'
  config:
    task_definition: 'arn:aws:ecs:us-east-1:1234567890:task-definition/my-task-definition:1'
    container_name: 'my_container_name'
    run_resources:
      cpu: '256'
      memory: '512' # MiB
      ephemeral_storage: 128 # GiB
    run_task_kwargs:
      capacityProviderStrategy:
        - capacityProvider: 'FARGATE_SPOT'
    secrets_tag: 'my-tag-name'
    secrets:
      - name: 'MY_API_TOKEN'
        valueFrom: 'arn:aws:secretsmanager:us-east-1:123456789012:secret:FOO-AbCdEf:token::'
```

Other `EcsRunLauncher` config fields: `env_vars`, `include_sidecars` (default `False`), `use_current_ecs_task_config` (default `True`), `run_ecs_tags`, `propagate_tags`, `task_definition_prefix` (default `"run"`). `run_task_kwargs` accepts any `run_task` boto3 argument in camelCase except `taskDefinition` and `overrides`. Any Secrets Manager secret tagged `dagster` is injected as an environment variable unless `secrets_tag` changes the tag.

Per-job and per-step overrides come from tags: `ecs/cpu`, `ecs/memory`, `ecs/ephemeral_storage`, `ecs/run_task_kwargs`, `ecs/task_overrides`, `ecs/container_overrides`. Tags win over run launcher defaults. The launcher always owns the container's `name` and `command`, so those cannot be overridden.

`ecs_executor` launches each step as its own ECS task, inheriting networking, cluster and task definition from the current task; it therefore requires `EcsRunLauncher`. Set it as the executor on `Definitions` and configure `cpu`, `memory`, `ephemeral_storage`, `max_concurrent`, `task_overrides` in run config. Fargate accepts only certain CPU/memory combinations and 20–200 GiB of ephemeral storage.

## GCP: GCE, Cloud SQL, GCS

Use when: running self-hosted Dagster on Google Cloud.

Host the webserver and daemon on GCE (bare VM or Docker) exactly as in [Single machine as a service](#single-machine-as-a-service), or on GKE via the Helm chart. Use Cloud SQL for PostgreSQL as the instance storage:

```yaml
# $DAGSTER_HOME/dagster.yaml
storage:
  postgres:
    postgres_db:
      username: my_username
      password: my_password
      hostname: my_hostname
      db_name: my_database
      port: 5432
```

Compute logs go to GCS with `module: dagster_gcp.gcs.compute_log_manager` / `class: GCSComputeLogManager` and config keys `bucket`, `local_dir`, `prefix`, `upload_interval`. For op outputs use `gcs_pickle_io_manager` with the `gcs` resource, configured with `gcs_bucket` and `gcs_prefix`; outputs land at `gs://<bucket>/dagster/storage/<run id>/files/<op name>.compute`.

## Single machine as a service

Use when: small-scale or staging deployments on one VM.

```bash
export DAGSTER_HOME="/opt/dagster/dagster_home"   # must exist and hold dagster.yaml
dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml
dagster-daemon run
```

Run each as its own supervised service (systemd unit, Docker container, or process supervisor — the docs do not ship unit files). Execution logs are written to `$DAGSTER_HOME/logs`. The daemon needs read access to `dagster.yaml`, `workspace.yaml`, and the code in the workspace; the simplest correct setup runs both processes as the same user with the same virtualenv and the same `DAGSTER_HOME`. Multiple webserver replicas are fine; exactly one daemon is not negotiable. Daemon health shows on the Instance page in the UI.

Useful webserver flags: `-l/--path-prefix` when hosting behind a reverse proxy at a subpath, `--read-only` to disable launching runs and toggling schedules, `--log-format json`, `--db-pool-max-overflow`, `--db-pool-recycle`, `--db-statement-timeout`.

## Production instance checklist

Every item is verifiable before you ship.

- **Postgres storage.** Set `storage.postgres.postgres_db` (requires `dagster-postgres`). SQLite is the default and is unusable across containers or replicas.
- **Object-store compute logs.** `S3ComputeLogManager`, `GCSComputeLogManager`, or the Azure equivalent. `LocalComputeLogManager` loses logs when the run worker's filesystem goes away.
- **Queued runs.** `run_coordinator` set to `QueuedRunCoordinator` with `max_concurrent_runs` and any `tag_concurrency_limits`. This is what makes the run-queue daemon run.
- **Run monitoring on.** `run_monitoring.enabled: true` plus `start_timeout_seconds` / `cancel_timeout_seconds` / `max_runtime_seconds`, so crashed run workers are detected. See `operations-and-debugging.md`.
- **Secrets from the environment.** Use `envSecrets`/`envConfigMaps` (K8s), `secrets`/`secrets_tag` (ECS), or `env_vars` on the launcher. Reference them from code via `dg.EnvVar` (see `resources-and-config.md`). `dagster.yaml` values accept `env:` in place of a literal.
- **`DAGSTER_HOME` set and the directory present** in every webserver, daemon and run-worker environment.
- **Version pinning.** Same Dagster version for `dagster`, `dagster-webserver`, every integration library, every code location image, and the Helm chart.
- **Slow imports accounted for.** Raise `code_servers.local_startup_timeout` above the 180-second default if definitions are slow to load; tune `DAGSTER_GRPC_MAX_WORKERS` and the proxy heartbeat env vars on busy code servers.
- **Health checks.** `dagster api grpc-health-check -p 4000` for code servers; readiness on the webserver's HTTP port.

## CI/CD for OSS

Use when: you want each merge to produce a deployable code location.

1. `pytest` for unit tests and asset checks (`testing-and-asset-checks.md`).
2. `dg check defs` to load and validate every definition. Exit code 1 means broken definitions; it sets `DAGSTER_IS_DEFS_VALIDATION_CLI=1` in the loaded code. In a project it schema-checks `defs.yaml` component files first unless you pass `--no-check-yaml`.
3. Build one image per code location, tagged with the commit SHA. Never reuse a mutable tag: the run launcher resolves images at launch time and a mutable tag makes runs irreproducible.
4. Push, then roll the code-location deployment only. `helm upgrade --install user-code dagster/dagster-user-deployments -f values.yaml --set ...image.tag=$SHA` for the split-release layout; `docker compose up -d <code_location_service>` for Compose.
5. Leave webserver and daemon untouched unless the Dagster version changed. Version changes go through [Upgrades and instance migration](#upgrades-and-instance-migration).

`dg scaffold build-artifacts` generates a `build.yaml` and Dockerfile, but the documented flow for it is Dagster+ Hybrid; for OSS, write and own the Dockerfile.

## Upgrades and instance migration

Instance migration is required whenever the **minor** version changes. Back up PostgreSQL first and read the upgrade guide for that release.

Generic sequence: stop the daemon and webserver, run `dagster instance migrate` with the new version in an environment that has `DAGSTER_HOME` and database access, then restart. Related commands: `dagster instance info`, `dagster instance reindex` (rebuilds indexes over historical runs for performance).

On Kubernetes, the chart ships the migration as a Job:

```bash
helm repo update
helm upgrade --install dagster dagster/dagster -f values.yaml

# capture replica counts, then scale to zero so nothing writes during migration
kubectl scale deploy $WEBSERVER_DEPLOYMENT_NAME --replicas=0
kubectl scale deploy $DAEMON_DEPLOYMENT_NAME --replicas=0

helm template $HELM_DAGSTER_RELEASE_NAME dagster/dagster \
  --set "migrate.enabled=true" \
  --show-only templates/job-instance-migrate.yaml \
  --values values.yaml | kubectl apply -f -

kubectl get pods -l job-name=dagster-instance-migrate   # confirm completion before scaling back up
```

Recover the live values with `helm get values  > values.yaml` if the file is gone, then scale back to the recorded replica counts.

## Gotchas

- **Version skew breaks gRPC.** Webserver, daemon, code servers and run-worker images must all be the same Dagster version. Mismatches surface as deserialization errors from code locations, not as a clear version message.
- **SQLite is single-writer.** Any multi-container or multi-replica deployment needs Postgres (or MySQL). This is the most common cause of locking errors in a Compose or Helm setup.
- **Both host processes need every code server.** The daemon drives schedule and sensor ticks and launches the runs they request, so a code server reachable only from the webserver silently breaks automation.
- **Sensor and schedule code runs in the code server, not the daemon.** For user-defined sensors and schedules in a `grpc_server` location the daemon only asks for each tick over gRPC; the code server evaluates the sensor or schedule function and builds the resources it requests. Env vars those resources read (`dg.EnvVar` fields) must be set on the code server's container. Setting them only on the daemon fails the tick with an "environment variable … is not set" error. A `python_module`/`python_file` location is a subprocess each host process spawns, so there it inherits the daemon's environment. The default automation-condition sensor is the exception: it evaluates in the daemon unless `use_user_code_server=True` (see `declarative-automation.md`).
- **Run workers need the code.** The run worker must use an image with the same project and dependencies as the code server. `DAGSTER_CURRENT_IMAGE` on the code server is what makes that automatic.
- **`DAGSTER_HOME` must exist.** Dagster does not create it. A missing directory falls back to ephemeral storage, and history vanishes on restart.
- **Job-level `dagster-k8s/config` does not reach step pods.** Use `step_k8s_config` on the executor or per-op tags.
- **Tags are ignored by the wrong launcher.** `dagster-k8s/config` only applies under `K8sRunLauncher`/`CeleryK8sRunLauncher`; `ecs/*` tags only under `EcsRunLauncher`. No warning is raised.
- **`ENTRYPOINT` on the code-location image blocks run launching.** The launcher overrides `CMD`.
- **Renaming a code location stops its schedules and sensors** until they are restarted in the UI.
- **GKE Autopilot preemption** during node initialization raises `DagsterExecutionInterruptedError` at random. Fix it with run retries (`max_retries: 1` or higher), not with `safe-to-evict` annotations.
- **Celery instances require the Celery executor on every job.** A job left on the default executor under `CeleryK8sRunLauncher` will not execute as intended.

## Legacy and superseded patterns

| Old | Current |
| --- | --- |
| `dagit` package and `dagit` executable | `dagster-webserver` package and executable (drop-in; `dagit` supported until 2.0) |
| Helm `dagit:` / `ingress.dagit` / `ingress.readOnlyDagit` | `dagsterWebserver:` / `ingress.dagsterWebserver` / `ingress.readOnlyDagsterWebserver` |
| Helm `celery.enabled` / top-level `k8sRunLauncher.enabled` | `runLauncher.type` plus `runLauncher.config.celeryK8sRunLauncher` or `runLauncher.config.k8sRunLauncher` |
| Helm `env_secrets` / `env_config_maps` | `envSecrets` / `envConfigMaps` |
| `@repository` targeted by `workspace.yaml` `attribute:` | A single `Definitions` object per code location; `attribute` is only needed for repositories |
| `dagster.yaml` `auto_materialize` settings | Declarative automation; see `declarative-automation.md` |
| `dagster-cloud` CLI | Dagster+ only; see `dagster-plus.md` |

`workspace.yaml` itself is not deprecated for OSS production wiring: the docs steer new OSS setups to `dg.toml` workspaces, and the webserver/daemon still take `-w workspace.yaml`.

## Sources
- https://docs.dagster.io/deployment — deployment overview, OSS vs Dagster+
- https://docs.dagster.io/deployment/oss/oss-deployment-architecture — long-running services, execution flow, configuration levels
- https://docs.dagster.io/deployment/oss/deployment-options/docker — Docker Compose reference deployment
- https://docs.dagster.io/deployment/oss/deployment-options/deploying-dagster-as-a-service — webserver and daemon on a single machine
- https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes — Helm install and values
- https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/customizing-your-deployment — runK8sConfig, tags, precedence, external DB, split releases, TTL
- https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/kubernetes-and-celery — CeleryK8sRunLauncher, queues, priority, brokers
- https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/migrating-while-upgrading — instance migration Job
- https://docs.dagster.io/deployment/oss/deployment-options/aws — EcsRunLauncher, ECS tags, ecs_executor, secrets, RDS, S3
- https://docs.dagster.io/deployment/oss/deployment-options/gcp — GCE, Cloud SQL, GCS IO manager
- https://docs.dagster.io/deployment/oss/oss-instance-configuration — code_servers timeouts and code server env vars
- https://docs.dagster.io/deployment/execution/dagster-daemon — available daemons, single-daemon rule
- https://docs.dagster.io/deployment/execution/run-launchers — run launcher catalog
- https://docs.dagster.io/deployment/execution/run-monitoring — run monitoring settings
- https://docs.dagster.io/guides/build/projects/workspaces/workspace-yaml — load_from keys, gRPC servers, DAGSTER_CURRENT_IMAGE
- https://docs.dagster.io/guides/build/projects/workspaces/dg-toml — dg.toml workspace configuration
- https://docs.dagster.io/guides/build/projects/creating-projects — project layout and definitions.py entry point
- https://docs.dagster.io/api/clis/cli — dagster api grpc, dagster-webserver, dagster instance commands
- https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference — dg check defs
- https://docs.dagster.io/integrations/libraries/k8s — K8sRunLauncher and k8s_job_executor config
- https://docs.dagster.io/integrations/libraries/docker — DockerRunLauncher and docker_executor
- https://docs.dagster.io/integrations/libraries/aws — EcsRunLauncher parameters, S3ComputeLogManager
- https://docs.dagster.io/integrations/libraries/gcp — GCSComputeLogManager
- https://docs.dagster.io/migration/upgrading — dagit deprecation, Helm value renames, instance migrate
- https://docs.dagster.io/deployment/troubleshooting/dagsterexecutioninterruptederrorgke-autopilot-clusters — GKE Autopilot preemption
