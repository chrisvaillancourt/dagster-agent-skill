# Dagster+

Everything in this file is Dagster+ only (the hosted control plane at `dagster.plus` / `<org>.dagster.cloud`). Covers choosing Serverless vs Hybrid, agents, deploying code, CI/CD, branch deployments, deployment settings, environment variables, auth and RBAC, alerts, Insights, the Dagster+ MCP server, and the `dg plus` / `dg api` CLI surface. Self-hosted OSS deployment (Helm chart for OSS, `dagster.yaml` instance config, run launchers, executors) is in `deployment-oss.md`. Project scaffolding and general `dg` usage is in `project-setup-and-cli.md`. Run-level debugging is in `operations-and-debugging.md`. Items marked **Serverless-only**, **Hybrid-only**, **Pro**, **beta**, or **preview** apply only under that condition.

## Contents

- [Choosing Serverless vs Hybrid](#choosing-serverless-vs-hybrid) | [Serverless runtime](#serverless-runtime) | [Hybrid agents](#hybrid-agents)
- [Deployment configuration files](#deployment-configuration-files) | [CI/CD](#cicd) | [Branch deployments](#branch-deployments)
- [Full deployments and settings](#full-deployments-and-settings) | [Environment variables and secrets](#environment-variables-and-secrets)
- [Authentication, roles, and tokens](#authentication-roles-and-tokens) | [Alerts](#alerts) | [Insights, catalog, and Issues](#insights-catalog-and-issues)
- [Dagster+ MCP server](#dagster-mcp-server) | [`dg plus` command reference](#dg-plus-command-reference) | [`dg api` command reference](#dg-api-command-reference)
- [Migrations](#migrations) | [Gotchas](#gotchas) | [Legacy and superseded patterns](#legacy-and-superseded-patterns) | [Sources](#sources)

## Choosing Serverless vs Hybrid

Decision rule: pick Serverless unless one of the Hybrid triggers below applies. Serverless fits workloads that orchestrate other services (warehouses, ETL tools) or do light in-process compute.

| Concern | Serverless | Hybrid |
| --- | --- | --- |
| Who runs compute | Dagster+ (Dagster Labs is a data processor) | You (your cluster / VPC / machine) |
| Setup cost | Sign up, push to Git | Install and operate an agent |
| Run resources | Fixed: 4 vCPU, 16 GB RAM, 128 GB ephemeral disk | Whatever you configure |
| Code location server | 0.25 vCPU, 1 GB RAM | Whatever you configure |
| Throughput caps | 100 GB bandwidth/day, 4500 step-minutes/day | None imposed by Dagster+ |
| Max concurrent runs | 50 | 500 |
| Infra customization | Not supported (no extra containers) | Full |
| Build artifact | PEX (default) or Docker image | Docker image only |
| Region | US org runs in US, EU org runs in EU | Your infrastructure |

Choose Hybrid when you need substantial compute for a single run, your dataset does not fit in 16 GB, you need to distribute a single run across nodes (Dagster+ runs execute on a single node), or policy forbids Dagster Labs processing your data. Pro customers can request quota increases on the Serverless caps.

Both types support **isolated runs** (own container, default, up to ~3 min to provision) and **non-isolated runs** (reuse the standing code location server process; fast start, shared resources). Enable non-isolated runs in deployment settings, then uncheck `Isolate run environment` in the Launchpad or tag the run `{"dagster/isolation": "disabled"}`. Only one non-isolated run executes at a time by default (`max_concurrent_non_isolated_runs`). A non-isolated run that exhausts the code location server crashes it and takes down that location's sensors and schedules, so keep compute-heavy work isolated, and on Hybrid put non-isolated jobs in their own code location.

## Serverless runtime

**Serverless-only.** Code is packaged as PEX files layered on a Docker image, which skips an image build per change. Customize it as follows.

| Goal | How |
| --- | --- |
| Add dependencies | Declare them in `pyproject.toml` `dependencies` (uv) or `setup.py` `install_requires` (pip). Tarball URLs are allowed |
| Pick a Python version | `PYTHON_VERSION` in the GitHub/GitLab workflow, or `--python-version` on the deploy command |
| Non-Python setup steps | `dagster_cloud_pre_install.sh` / `dagster_cloud_post_install.sh` at repo root (Docker builds only) |
| Custom base image | `dagster-cloud serverless upload-base-image local-image:tag`, then set `SERVERLESS_BASE_IMAGE_TAG` in the workflow |
| Ship data files | Put them under `src/<package>/data/` (uv) or use `package_data` in `setup.py` (pip) |
| Disable PEX | `ENABLE_FAST_DEPLOYS: 'false'` (GitHub) or `DISABLE_FAST_DEPLOYS: true` (GitLab) |
| Private Git dependency | Check the repo out in CI, build a wheel, point `PIP_CONFIG_FILE` at a `find-links` dir, then list the package as a normal dependency |

The Serverless docs list Python 3.9 through 3.13 as supported; `dg plus deploy --python-version` additionally accepts 3.14. A base image built on an M1/M2 Mac is ARM and will not run on AWS ECS, so build with `--platform=linux/amd64`. Custom base images are not supported out of the box in the GitLab workflow.

## Hybrid agents

**Hybrid-only.** The agent is a long-lived process that polls the Dagster+ agent API, launches a code server per code location, and launches a run worker per run. All user code and all database connections stay in your environment; only metadata flows out, and no ingress from Dagster+ is required. The agent is open source (`dagster-io/dagster-cloud`). If unsure which agent to use, pick Kubernetes.

| Agent | Install | Config surface |
| --- | --- | --- |
| Kubernetes | `helm repo add dagster-cloud https://dagster-io.github.io/helm-user-cloud` then `helm --namespace dagster-cloud upgrade --install agent dagster-cloud/dagster-cloud-agent` | Helm `values.yaml` |
| Amazon ECS | CloudFormation stack `https://s3.amazonaws.com/dagster.cloud/cloudformation/ecs-agent-vpc.yaml` (public subnets) or `ecs-agent-vpc-private.yaml` (private subnets + NAT) | Stack parameters, `dagster.yaml` in the task definition |
| Docker | `docker run --network=dagster_cloud_agent -v $PWD/dagster.yaml:/opt/dagster/app/dagster.yaml:ro -v /var/run/docker.sock:/var/run/docker.sock docker.io/dagster/dagster-cloud-agent:latest dagster-cloud agent run /opt/dagster/app` | `dagster.yaml` |
| Local (experiments only) | `pip install dagster-cloud` then `dagster-cloud agent run ~/dagster_home/` | `dagster.yaml` |

Generate an agent token under **Organization Settings > Tokens**. On Kubernetes, store it as `kubectl --namespace dagster-cloud create secret generic dagster-cloud-agent-token --from-literal=DAGSTER_CLOUD_AGENT_TOKEN=<token>`. Elsewhere it goes in `dagster.yaml`:

```yaml
# dagster.yaml
instance_class: { module: dagster_cloud.instance, class: DagsterCloudAgentInstance }
dagster_cloud_api:
  agent_token: { env: DAGSTER_CLOUD_AGENT_TOKEN }
  deployment: prod # or `deployments: [prod, staging]`, never both keys
  branch_deployments: true
  agent_label: 'us-east-1' # optional, shows in the UI
user_code_launcher:
  module: dagster_cloud.workspace.docker
  class: DockerUserCodeLauncher
  config:
    networks: [dagster_cloud_agent]
    server_ttl:
      full_deployments: { enabled: true, ttl_seconds: 300 }
```

`server_ttl` shuts down idle code servers (disabled by default for full deployments, 24 h TTL when enabled). `compute_logs` with `CloudComputeLogManager` and `upload_interval` streams stdout/stderr to Dagster+; `NoOpComputeLogManager` (Helm: `computeLogs.enabled: false`) stops forwarding it.

**Multiple agents.** Set `dagsterCloudAgent.replicas` (Helm), `deploy.replicas` (Compose), or `DesiredCount`/`NumReplicas` (ECS) for redundancy in one environment. When agents cannot see each other's resources (separate clusters or accounts), set `isolated_agents.enabled: true` (Helm: `isolatedAgents.enabled`). **Pro, agent 1.6.0+:** agent queues route a code location to a specific agent: set `agent_queue = "on-prem-agent-queue"` under `[tool.dg.project]` in the project's `pyproject.toml`, and list the queue on the agent (Helm `dagsterCloud.agentQueues.additionalQueues`, or `agent_queues.additional_queues` in `dagster.yaml`).

Locations without `agent_queue` land on the default queue, which every agent serves unless `includeDefaultQueue` is false. Multi-region failover is just a queue reassignment because the agent holds no state: all run history, asset metadata, and schedule/sensor state live in the control plane. You can retarget a location's queue from the UI (**Deployment** > location row > **Modify**), but the next CI/CD deploy overwrites it with the `pyproject.toml` value.

Recommended sizing: agent container 0.25 vCPU / 1 GB to start, code server 0.25 vCPU / 1 GB plus room for imports and heavy init, runs 4 vCPU / 8-16 GB. Scale the run workers (pods, ECS tasks), not the code server, for compute-heavy jobs. Upgrade the agent **before** upgrading code locations; a code location newer than its agent is unsupported. Upgrade the agent roughly every six months; its version shows on **Deployment** > **Agents**.

## Deployment configuration files

`dg plus deploy configure` scaffolds these in the project (or workspace) root. In a workspace, workspace-level files supply defaults and project-level files override them.

| File | Purpose |
| --- | --- |
| `build.yaml` | Docker build settings: `registry` and `directory` |
| `container_context.yaml` | Runtime config for the agent platform (Hybrid) |
| `pyproject.toml` `[tool.dg.project]` | Code location identity, agent queue, prebuilt image |
| `Dockerfile` | Image definition (Hybrid) |

```yaml
# build.yaml
registry: 764506304434.dkr.ecr.us-east-1.amazonaws.com/my-image
directory: ./
```

```yaml
# container_context.yaml
env_vars: [DATABASE_NAME=staging, DATABASE_PASSWORD] # bare key pulls from the agent's env
k8s:
  namespace: my-namespace
  env_secrets: [database-password]
  service_account_name: my_service_account
  server_k8s_config:
    container_config: { resources: { limits: { cpu: 500m, memory: 2560Mi } } }
  run_k8s_config:
    container_config: { resources: { limits: { cpu: 500m, memory: 2560Mi, nvidia.com/gpu: 1 } } }
```

`server_k8s_config` sizes the code server (where schedule and sensor evaluation happens); `run_k8s_config` sizes run workers. ECS uses `ecs.env_vars`, `ecs.secrets` (ARNs, ECS API `Secret` shape), and `ecs.secrets_tags`; Docker uses `docker.env_vars`. Full key lists live in each agent's configuration reference page.

`[tool.dg.project]` keys: `root_module` (required), `code_location_name` (defaults to the directory name), `agent_queue`, and `image` (a prebuilt image, which skips the build step entirely). Projects using `dg.toml` put the same keys under `[project]`. Code requirements: a single entry point (module or package), with `dagster`, `dagster-dg-cli`, and `dagster-cloud` installed in the runtime environment. `workspace.yaml` is not used by Dagster+. Different code locations may run different Dagster versions. Serialized repository and job snapshots are capped at 130 MB, and gRPC's 100 MB message limit usually bites first (raise with `DAGSTER_GRPC_MAX_RX_BYTES` / `DAGSTER_GRPC_MAX_SEND_BYTES` on **both** agent and code server, or shrink the snapshot).

Code location history is tracked automatically. **Deployment** tab > location row > **View history** shows every load, the deployed commit, and added/removed/changed assets; **Rollback to this version** redeploys a previous image (Editor or above).

## CI/CD

One-time setup, run from the project root after `dg plus login`:

```shell
uv add dagster-cloud              # runtime dependency, needed inside the image
uv add --dev dagster-dg-cli       # CI only; excluded by `uv sync --no-dev`
dg plus deploy configure --git-provider github   # or gitlab
dg plus create ci-api-token --description 'my-project CI' | gh secret set DAGSTER_CLOUD_API_TOKEN
git add . && git commit -m "Deploy to Dagster+" && git push origin main
```

`dg plus deploy configure` detects the agent type from your deployment, or take the explicit subcommand: `dg plus deploy configure hybrid --agent-platform k8s|ecs|docker --registry-url <url>` or `dg plus deploy configure serverless [--pex-deploy|--no-pex-deploy]`. GitLab needs `DAGSTER_CLOUD_API_TOKEN` added manually under **Settings > CI/CD > Variables**.

Any other CI provider (or a local script) runs the stages directly. All three environment variables are required (`DAGSTER_BUILD_STATEDIR` must point at a blank or nonexistent directory):

```shell
export DAGSTER_CLOUD_ORGANIZATION=my-org DAGSTER_CLOUD_API_TOKEN=...  # token is a CI secret
export DAGSTER_BUILD_STATEDIR=/tmp/dagster-build-state

dg plus deploy start --deployment=prod --target-path=.  # reads build.yaml, inits statedir
dg plus deploy build-and-push                           # builds and pushes to build.yaml registry
dg plus deploy                                          # updates code locations in Dagster+
```

`dg plus deploy` on its own runs the whole sequence end to end; the subcommands exist for pipelines that need to interleave custom steps.

Environment variables configured in the Dagster+ deployment are fetched automatically during CI code loading, scoped to full vs branch deployment, so credentials needed at import time do not have to be duplicated as CI secrets.

Keep the Dagster+ GitHub Actions current (tags follow Dagster releases, e.g. `@v1.11.13`). Organizations that require third-party actions pinned to full-length commit SHAs need `dagster-io/dagster-cloud-action` v1.13.11 or later, because that release pins the actions it calls internally. Use `ubuntu-latest` or `ubuntu-22.04` runners: the PEX toolchain fails to build `cryptography` wheels on Ubuntu 20.04 self-hosted runners, where the workaround is installing the `dagster-cloud` CLI directly instead of via PEX. See `troubleshooting.md`.

## Branch deployments

Every pull or merge request gets an ephemeral preview deployment that rebuilds on each push. Setup is automatic on Serverless once CI/CD is configured. On Hybrid, an agent must serve them, and the agent must be able to pull branch images from your registry.

Set Helm `dagsterCloud.branchDeployments: true` on a dedicated branch agent; a single agent serving both must also list `dagsterCloud.deployments: [prod]`. On ECS this is the **Enable branch deployments** CloudFormation parameter, and only one agent should have it on.

**Change tracking** diffs branch assets against the base deployment and labels them **New in branch** / **Changed in branch**. Detected changes: new assets, `code_version`, upstream dependencies, partitions definition, tags, and definition metadata. The comparison is against the **current** state of the base deployment by default, so base commits that land after the branch was cut show up as changes. Pin the comparison with `--snapshot-base-condition on-create` (snapshot when the branch is first created) or `on-update` (refresh on every branch update).

**Concurrency for branch deployments is organization-scoped**, unlike full deployments:

| Setting | Scope | Default |
| --- | --- | --- |
| `max_concurrent_branch_deployment_runs` | All branch deployments combined | `50` (max 500 Hybrid / 50 Serverless) |
| `max_concurrent_runs_per_branch_deployment` | Each branch deployment individually | `null` |
| `branch_deployment_tag_concurrency_limits` | All branch deployments, by run tag | `[]` |

Both run limits apply simultaneously; a run dequeues only if neither is reached.

Detect the environment in code with `os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"` and pick the resource set from that. The common pattern for testing against production data is to clone the warehouse schema per PR (named from `DAGSTER_CLOUD_PULL_REQUEST_ID`), point the branch I/O manager at the clone with separate credentials, and drop the clone when the PR closes. Output created by a branch deployment is **not** cleaned up automatically. Environment variables are read-only inside a branch deployment; manage them in the parent full deployment.

## Full deployments and settings

A new organization gets one deployment named `prod`. **Pro** is required for additional full deployments (dev/staging/prod, or per-tenant). Create and delete them under **Organization Settings > Deployments** (Organization Admin). Deleting a deployment deletes its code locations, jobs, schedules, and sensors.

Settings are per deployment, edited in the UI or synced from YAML:

```yaml
# my-settings.yaml
concurrency:
  pools: # granularity 'op' (default) or 'run'; op_granularity_run_buffer also allowed
    granularity: 'run'
    default_limit: 1
  runs:
    max_concurrent_runs: 10 # default 10; max 500 Hybrid / 50 Serverless; 0 blocks launching
    tag_concurrency_limits: [{ key: 'database', value: 'redshift', limit: 5 }]
run_monitoring:
  start_timeout_seconds: 1200    # fail runs stuck in STARTING
  cancel_timeout_seconds: 1200   # force CANCELED after termination request
  max_runtime_seconds: 7200      # no limit by default
  free_slots_after_run_end_seconds: 300
run_retries:
  max_retries: 0
  retry_on_asset_or_op_failure: true
sso_default_role: EDITOR         # or NO_ACCESS to block auto-provisioning
non_isolated_runs:
  enabled: true
  max_concurrent_non_isolated_runs: 1
```

```shell
dg api deployment settings get             # prints current settings, including defaults
dg api deployment settings set my-settings.yaml   # replaces all settings
```

`set` is a full replacement: omitted keys revert to defaults. `free_slots_after_run_end_seconds` is required if you use `dagster/concurrency_key` op tags, otherwise cancelled or failed runs hold their slots indefinitely. Modifying settings needs Editor; creating/deleting deployments needs Organization Admin.

## Environment variables and secrets

Two mechanisms, which can be combined.

| | Dagster+ UI | Agent configuration |
| --- | --- | --- |
| Deployment types | Serverless and Hybrid | Hybrid only |
| Where values live | Dagster+, encrypted with AWS KMS envelope encryption | Your agent's environment or secret store |
| Scope | Deployment (full/branch/local) and code location | Code location (`container_context.yaml`) or whole deployment (agent `dagster.yaml`) |
| Passes through Dagster+ | Yes | Only per-location values; deployment-level values bypass Dagster+ |
| Limits | 1,000 vars per full deployment, 4 KB each, names ≤512 chars | Name charset only |

UI-set variables win over duplicates in agent config. Scope choices are **Full deployment**, **Branch deployments**, and **Local**; only **Local**-scoped variables are exported for local development. Create the same key twice with different scopes to give prod and branch deployments different values.

```shell
dg plus pull env                  # writes Local-scoped vars into ./.env
dg plus create env SNOWFLAKE_PASSWORD --from-local-env --scope full --global
```

`dg plus create env NAME [VALUE]` takes `--scope full|branch|local`, `--global` (deployment level, all locations), `--from-local-env`, and `-y`. Reading variables back is `dg api secret list [--location L] [--scope deployment|organization]` and `dg api secret get NAME --show-value`.

For secrets managed outside Dagster+ on Hybrid, create a Kubernetes secret and reference it per location with `k8s.env_secrets`, or agent-wide with Helm `workspace.envSecrets`. For a vault (AWS Secrets Manager, HashiCorp Vault), inject only the vault credentials this way and fetch the rest from a client inside your Dagster code. Redeploy the code location after editing `container_context.yaml`.

Built-in variables are reserved; reusing the names is an error.

| Variable | Value |
| --- | --- |
| `DAGSTER_CLOUD_DEPLOYMENT_NAME` | Deployment name, e.g. `prod` |
| `DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT` | `1` in a branch deployment |
| `DAGSTER_CLOUD_GIT_SHA`, `_GIT_TIMESTAMP` (Unix seconds), `_GIT_MESSAGE`, `_GIT_BRANCH`, `_GIT_REPO`, `_GIT_AUTHOR_NAME`, `_GIT_AUTHOR_EMAIL` | Commit metadata, branch deployments only |
| `DAGSTER_CLOUD_PULL_REQUEST_ID` | PR/MR ID |
| `DAGSTER_CLOUD_PULL_REQUEST_STATUS` | `OPEN`, `CLOSED`, or `MERGED` |

## Authentication, roles, and tokens

Three token kinds, all organization-scoped:

| Token | Created by | Used for |
| --- | --- | --- |
| Agent token | Organization Admin, **Organization Settings > Tokens** | Authenticating a Hybrid agent to the agent API. Defaults to "Org Agent" (all deployments); untoggle it to scope per deployment |
| User token | Any user, for themselves only | CLI, GraphQL API, MCP header auth. Nobody, including Org Admins, can create or view another user's token |
| CI API token | `dg plus create ci-api-token --description '...'` | CI/CD pipelines |

Roles, most to least permissive: **Organization Admin**, **Admin**, **Editor**, **Launcher** (Pro), **Viewer**. Each role includes everything below it. Custom roles are built under **Organization Settings > Roles**. Roles are assigned per deployment except Organization Admin, which spans the whole organization. **Pro** additionally allows per-code-location and per-branch-deployment overrides, which can only grant more permission, never less. A user's effective role is the most permissive across their individual grant and all team memberships.

| Capability | Viewer | Launcher | Editor | Admin | Org Admin |
| --- | --- | --- | --- | --- | --- |
| View runs, code locations, users, teams | yes | yes | yes | yes | yes |
| Launch, re-execute, terminate, delete runs | no | yes | yes | yes | yes |
| Start/stop schedules and sensors, wipe assets, add dynamic partitions | no | no | yes | yes | yes |
| Create/remove/reload code locations, modify deployment settings, manage env vars and alerts, create branch deployments | no | no | yes | yes | yes |
| Add users, modify team permissions | no | no | no | yes | yes |
| Create/delete deployments, edit user roles, manage agent tokens, SAML, SCIM, billing, audit logs | no | no | no | no | yes |

**Teams** (Pro) group users and carry default deployment, code location, and branch deployment roles. **SSO** is SAML-based, with guides for Okta, Microsoft Entra ID (Azure AD), Google Workspace, PingOne, and OneLogin; new SSO users get `sso_default_role` (default `Viewer`, or `NO_ACCESS` to block auto-provisioning). **SCIM** provisioning is supported for Okta and Entra ID: it syncs users and pushes IdP groups to teams, but does not assign permissions, so map groups to teams and assign roles to those teams. While SCIM is on, new users must be added in the IdP. **Audit logs** are Organization Admin only.

CLI authentication:

```shell
dg plus login                 # browser flow; add --region eu for EU orgs
dg plus config view --show-token
dg plus config set --api-token <T> --organization <ORG> --deployment prod --region us|eu
```

Config lands in `~/.dagster_cloud_cli/config`, shared with the `dagster-cloud` CLI. Non-interactive environments can skip the file entirely with `DAGSTER_CLOUD_API_TOKEN`, `DAGSTER_CLOUD_ORGANIZATION`, and `DAGSTER_CLOUD_DEPLOYMENT`. Precedence is CLI flag, then environment variable, then config file.

## Alerts

Alert policies are **per deployment**. Configure a notification service first, then create policies in the UI or sync YAML.

| Policy type | Fires on | Availability |
| --- | --- | --- |
| Asset | Health status change (healthy/warning/degraded), materialization success/failure, asset check pass/warn/error/execution failure, freshness pass/warn/fail, asset metric thresholds | Both |
| Run | Job success, failure, or exceeding a time limit, optionally filtered by run tags | Both |
| Code location | Location fails to load | Both |
| Automation | Schedule or sensor tick failure | Both |
| Agent downtime | No agent heartbeat in five minutes | Hybrid only |
| Insights metric (experimental) | An Insights metric crosses a threshold over a window | Both |
| Credit budget (experimental) | Monthly credit limit reached | Serverless only |

Notification services: email, Slack (install the app, invite `@Dagster Cloud` to the channel), Microsoft Teams (incoming webhook with a workflow URL), PagerDuty (**Pro**; integration key, integration named `dagster-plus-{service}`), email-to-asset-owners (**Pro** for owner routing), and generic HTTP webhooks (**preview**, allowlisted domains only, with prebuilt recipes for Discord, Jira, and incident.io).

```yaml
# alert_policies.yaml
alert_policies:
  - name: prod asset failures
    description: 'Page on-call when a prod asset fails to materialize'
    enabled: true
    event_types: [ASSET_MATERIALIZATION_FAILURE]
    alert_targets:
      - asset_selection_target:
          asset_selection: 'group:"prod"'
    notification_service:
      slack: { slack_workspace_name: mycompany, slack_channel_name: data-alerts }
    policy_options: { include_description_in_notification: true }
  - name: agent downtime
    enabled: true
    event_types: [AGENT_UNAVAILABLE]
    alert_targets: [] # empty list means all entities of that type
    notification_service: { email: { email_addresses: [ops@example.com] } }
    policy_options: { renotify_interval_minutes: 30 }
```

```shell
dg api alert-policy sync alert_policies.yaml
dg api alert-policy list --json
```

Event types by category: jobs (`JOB_FAILURE`, `JOB_SUCCESS`, `JOB_LONG_RUNNING`); asset materialization and checks (`ASSET_MATERIALIZATION_SUCCESS|FAILURE`, `ASSET_CHECK_PASSED`, `ASSET_CHECK_EXECUTION_FAILURE`, `ASSET_CHECK_SEVERITY_WARN|ERROR`, `ASSET_FRESHNESS_PASS|WARN|FAIL`); asset health (`ASSET_HEALTH_HEALTHY|WARNING|DEGRADED`); schema (`ASSET_TABLE_SCHEMA_CHANGE`); automation (`TICK_FAILURE`); infrastructure (`AGENT_UNAVAILABLE`, `CODE_LOCATION_ERROR`, `CODE_LOCATION_DEPLOY_SUCCESS`); insights (`INSIGHTS_CONSUMPTION_EXCEEDED`, `METRIC_MONITOR_ALERT`).

Target types: `run_result_target`, `long_running_job_threshold_target` (required for `JOB_LONG_RUNNING`), `asset_key_target`, `asset_selection_target`, `asset_group_target`, `asset_selection_view_target`, `favorites_selection_view_target`, `schedule_or_sensor_target`, `code_location_target`, `credit_limit_target`, `insights_*_threshold_target`, and `metric_monitor_*_threshold_target`. `policy_options` supports `include_description_in_notification`, `consecutive_failure_threshold` (1-100, `TICK_FAILURE` only), `renotify_interval_minutes` (`TICK_FAILURE`, `AGENT_UNAVAILABLE`, `CODE_LOCATION_ERROR` only), and `table_schema_change_types` (required for `ASSET_TABLE_SCHEMA_CHANGE`).

Validation rules worth knowing: event types in one policy must all be from the same category; materialization/check, health, and schema asset events cannot be mixed in a single policy; insights and metric-monitor policies require exactly one target. Health alerts fire only on status *transitions*, so a second consecutive failure sends nothing. Alerts on degraded health wait until run retries are exhausted, since retryable failures report warning. External assets never trigger health alerts.

## Insights, catalog, and Issues

**Insights** charts historical health, usage, and cost across assets, saved selections, jobs, and deployments, with 30 days of history. Asset and selection metrics: materialization success rate, average time to resolution, freshness pass rate, check success rate, materialization and failure counts, step execution time. Jobs add run success/failure counts, run duration, step failures, step retries, Dagster credits, observations. Deployments add compute duration, run concurrency, runs in progress, runs queued, and run queue time (view capacity metrics with the **Max** aggregation). Metric names in alert YAML are internal identifiers such as `__dagster_dagster_credits`, `__dagster_runs_queued`, `__dagster_runs_in_progress`, `__dagster_run_queue_time_ms`, and `__dagster_asset_check_warnings`.

**Custom metrics** (**beta**) chart any numeric asset metadata. Emit the value, then register the metadata key under **Deployment > Custom metrics** (up to 500 per deployment; the key cannot be changed after creation). There is no reserved `dagster/...` prefix for this: the registered key must match exactly what your assets emit.

```python
import dagster as dg

@dg.asset
def my_table():
    return dg.MaterializeResult(metadata={"processing_time_ms": 1500})
```

**External cost metrics** (**Pro**) pull Snowflake and BigQuery query runtime and billed usage into Insights and attribute them to assets and jobs. For Snowflake, swap `SnowflakeResource` for `InsightsSnowflakeResource` from `dagster-cloud` and add `create_snowflake_insights_asset_and_schedule` to your definitions; the credentials need `snowflake.account_usage.query_history`. Limits: two million data points per month, 120-day retention, up to 24 hours of ingestion lag.

**Asset catalog** is the search-first view over the global asset graph, filterable by key, kind, group, code location, tags, and owners. Saved **dashboards** built from asset selection syntax can be scoped by owner, kind, or tag and shared with the team (Editor or above). Assets without definitions are hidden by default.

**Issues** (**preview**) group multiple failures with a shared root cause and carry agent-ready context. They are created from a failed run, from an alert notification, from a Dagster+ AI chat, or automatically by proactive monitoring (which files them in **Triage** for you to promote or cancel). **Dagster+ AI** (**preview**) adds chat-based failure summaries, proactive monitoring, and auto-implement, which dispatches Claude Code via GitHub Actions to open a fix PR (scaffold with `dg labs scaffold github-actions-ai-dispatch`; needs a GitHub App installation and an Anthropic API key). **Connections** (**preview**) discover and sync warehouse assets from Snowflake, BigQuery, Postgres, and Databricks into the catalog.

## Dagster+ MCP server

**Beta.** Gives an agent read and write access to your deployment.

| Region | URL |
| --- | --- |
| US | `https://mcp.agent.dagster.cloud/mcp` |
| EU | `https://mcp.agent.eu.dagster.cloud/mcp` |

```shell
# OAuth: permissions follow the signed-in user
claude mcp add --transport http dagster-plus https://mcp.agent.dagster.cloud/mcp

# Header auth: use a service user's token to scope permissions independently
claude mcp add --transport http dagster-plus https://mcp.agent.dagster.cloud/mcp \
  --header "Dagster-Cloud-Organization: <org>" --header "Authorization: Bearer <user token>"
```

With OAuth, run `/mcp` in a `claude` session and authenticate in the browser. Capabilities: view, launch, and terminate runs; read run logs; view and materialize assets; view deployments; full create/update/delete on alert policies and Issues; Insights metrics for runs, assets, and deployments. Assets and deployments cannot be updated or deleted through it.

The MCP server covers roughly the same surface as `dg api`, so an agent with it connected rarely needs to shell out.

## `dg plus` command reference

Install with `pip install dagster-dg-cli`. Global auth comes from `dg plus login` or the `DAGSTER_CLOUD_*` environment variables.

| Command | Key flags |
| --- | --- |
| `dg plus login` | `--region us\|eu` |
| `dg plus config set` | `--api-token`, `-o/--organization`, `-d/--deployment`, `--url`, `--region` |
| `dg plus config view` | `-s/--show-token` |
| `dg plus create ci-api-token` | `--description` |
| `dg plus create env NAME [VALUE]` | `--from-local-env`, `--scope full\|branch\|local`, `--global`, `-y` |
| `dg plus pull env` | `--target-path` |
| `dg plus deploy` | `--deployment`, `--organization`, `--agent-type serverless\|hybrid`, `--build-strategy docker\|python-executable`, `--pex-build-method local\|docker\|docker-fallback`, `--python-version`, `--deployment-type full\|branch`, `--location-name`, `--git-url`, `--commit-hash`, `--status-url`, `--snapshot-base-condition on-create\|on-update`, `--skip-validation`, `-y` |
| `dg plus deploy configure [serverless\|hybrid]` | `--git-provider github\|gitlab`, `--agent-platform k8s\|ecs\|docker`, `--registry-url`, `--python-version`, `--git-root`, `--pex-deploy/--no-pex-deploy` |
| `dg plus deploy start` | Same session flags as `deploy`; the project directory is `--target-path` (there is no `--project-dir`) |
| `dg plus deploy build-and-push` | `--build-strategy`, `--pex-build-method`, `--python-version`, `--location-name` |
| `dg plus deploy set-build-output` | `--image-tag`, `--location-name` (for images built outside `build-and-push`) |
| `dg plus deploy finish` / `notify` | `finish`: `--location-name`, `--target-path`; `notify`: `--project-dir` |
| `dg plus deploy select` / `deselect` | `LOCATION_NAMES...` |
| `dg plus deploy list-locations` / `inspect` / `status` | `--output-format` on `status` |
| `dg plus deploy refresh-defs-state` | `--management-type` |
| `dg plus integrations dbt download-manifest` | `--components`, `--file`, `--key-prefix`, `--output` |
| `dg plus integrations dbt manage-manifest` | `--components`, `--file`, `--source-deployment`, `--key-prefix` |

`--build-strategy python-executable` (PEX) is Serverless-only; Hybrid agents require `docker`. `DAGSTER_CLOUD_DEPLOYMENT`, `DAGSTER_CLOUD_ORGANIZATION`, `DAGSTER_BUILD_STRATEGY`, and `DAGSTER_PEX_BUILD_METHOD` supply defaults for the matching flags.

## `dg api` command reference

REST-like calls against the Dagster+ control plane. **Every command accepts `--json`** for machine-readable output, plus `-d/--deployment`, `-o/--organization`, `--api-token`, `--response-schema` (print the response JSON Schema and exit), and `--view-graphql` (dump the underlying query to stderr). Organization-scoped commands take no deployment flag: `agent`, `organization`, `secret`, and `deployment list/get/delete`. `deployment settings get/set` does take `-d`, and `artifact` takes a long-form `--deployment` with no `-d` alias.

| Object | Subcommands | Notable flags |
| --- | --- | --- |
| `run` | `list`, `get <run_id>`, `get-events <run_id>`, `get-logs <run_id>`, `launch` | `--status QUEUED\|NOT_STARTED\|MANAGED\|STARTING\|STARTED\|SUCCESS\|FAILURE\|CANCELING\|CANCELED` (repeatable), `--job`, `--limit`, `--cursor`; logs take `--step-key`, `--link-only`, `--max-bytes`; events take `--level`, `--event-type`, `--step`; launch takes `-l/--location`, `-j/--job`, `--asset-key`, `--partition`, `--tag`, `--config-json`, `-w/--wait`, `-i/--interval` |
| `asset` | `list`, `get <key>`, `get-health <key>`, `get-events <key>`, `get-evaluations <key>`, `get-partition-status <key>` | `--event-type`, `--partition`, `--before`, `--include-nodes`, `--limit`, `--cursor` |
| `asset-check` | `list`, `get-executions` | `--asset-key`, `--check-name`, `--limit` |
| `job` | `list`, `get <job_name>` | |
| `schedule` / `sensor` | `list`, `get <name>`, `get-ticks <name>` | `--status RUNNING\|STOPPED` on list; `--status`, `--before`, `--after`, `--limit` on ticks |
| `code-location` | `list`, `get <name>`, `add <name>`, `delete <name>` | `--image`, `-m/--module-name`, `-p/--package-name`, `-f/--python-file`, `-w/--working-directory`, `--executable-path`, `-a/--attribute`, `--git-url`, `--commit-hash`, `--location-file/--from` |
| `deployment` | `list`, `get <name>`, `delete <name>`, `settings get`, `settings set <file>` | `--type production\|branch\|all`, `--pr-status OPEN\|CLOSED\|MERGED`, `--allow-delete-full-deployment` |
| `agent` | `list`, `get <agent_id>` | |
| `alert-policy` | `list`, `sync <file>` | |
| `issue` | `list`, `get <id>`, `create`, `update <id>`, `add-link <id>`, `remove-link <id>` | `--title`, `--description`, `--status` (repeatable on list), `--context`, `--run-id`, `--asset-key`, `--created-after`, `--created-before` |
| `secret` | `list`, `get <name>` | `--location`, `--scope deployment\|organization`, `--show-value` |
| `artifact` | `upload <key> <path>`, `download <key> <path>` | `--deployment` |
| `organization` | `settings get`, `settings set <file>`, `saml upload <metadata.xml>`, `saml remove` | |

```shell
dg api run list --status FAILURE --limit 10 --json
dg api run get-logs 5b3c8a91-2e4f-4d7b-9c6a-1f8d3e5b2c4a --step-key ingest_files
dg api asset get-health dim_customers --json
dg api deployment list --type all --pr-status OPEN
dg api code-location add ingestion --image dagster/ingestion:1.12.0 --module-name ingestion.definitions
```

There is no `dg api insights` command; Insights data is reached through the UI, alert policies, or the MCP server.

## Migrations

| From | To | Key steps |
| --- | --- | --- |
| OSS | Dagster+ | Create the organization and pick a deployment type, rewrite CI/CD to the Dagster+ pattern, then either cut over and accumulate fresh metadata or backfill history using the `oss-metadata-to-plus` example |
| Serverless | Hybrid | **Deployment > Agents > Switch to Hybrid**, stand up an agent, then set each location's registry and image in `build.yaml` / `container_context.yaml`. Requires downtime while no agent is attached; run history and metadata are preserved |
| US | EU | Create a new EU organization at `eu.dagster.cloud`, then reconfigure agents, CI/CD (`--region eu`), and SSO/SCIM against `https://<org>.eu.dagster.cloud`. The two regions are separate control planes: run history, schedules, sensors, and operational state do **not** transfer. Pipeline code is unchanged |

## Gotchas

- **Serverless dependencies must be installable as PEX or into a Docker image.** No sidecars, no extra containers, no infrastructure customization.
- **Serverless runs are single-node**, 4 vCPU and 16 GB. Anything larger needs Hybrid.
- **Hybrid branch deployments need registry access from the agent.** The agent pulls branch images itself; Dagster+ never sees the image.
- **Tokens are organization-scoped, not deployment-scoped.** Agent tokens default to serving every deployment; narrow them by untoggling "Org Agent".
- **Alert policies are per deployment.** Syncing a YAML file to `prod` does nothing for `staging`.
- **Deployment settings and organization settings are separate surfaces.** Branch deployment concurrency lives at the organization level, everything else at the deployment level. `dg api deployment settings set` replaces everything, so `get` first.
- **Upgrade the agent before code locations**, never the reverse. **Editing a code location in the UI is temporary**; the next CI/CD deploy overwrites it from `pyproject.toml`.
- **Branch deployment output is not cleaned up.** Drop cloned schemas and tables yourself on PR close.
- **Rate limits**: 40,000 user log events per minute and 100 MB of events per minute per deployment. Requests get a 429 and agents retry automatically. Reduce custom logging or switch to raw compute logs.
- **Credits**: asset materializations reported from a sensor (no step execution) and asset observations both cost zero credits; materializations backed by step execution cost one credit per partition. Sensor evaluations and asset checks are always free.

## Legacy and superseded patterns

| Old | New |
| --- | --- |
| `dagster-cloud` CLI | `dg plus` and `dg api` (`pip install dagster-dg-cli`) |
| `dagster-cloud config setup` | `dg plus login` or `dg plus config set` |
| `dagster-cloud config view` / `set-deployment` | `dg plus config view` / `dg plus config set --deployment` |
| `dagster-cloud serverless deploy-python-executable` / `deploy` | `dg plus deploy --build-strategy python-executable` / `docker` |
| `dagster-cloud deployment add-location` | `dg api code-location add` |
| `dagster-cloud deployment alert-policies sync -a <file>` | `dg api alert-policy sync <file>` |
| `dagster-cloud branch-deployment create-or-update` | `dg plus deploy --deployment-type branch` (the `dagster-cloud` form remains for custom CI on non-GitHub/GitLab providers) |
| Single `dagster_cloud.yaml` with `locations[]` | `build.yaml` + `container_context.yaml` + `pyproject.toml` `[tool.dg.project]` |
| `workspace.yaml` | Not used by Dagster+ at all |

The legacy `dagster_cloud.yaml` is still accepted. Per-location keys are `location_name`, `code_source` (exactly one of `package_name`, `module_name`, `python_file`), `build` (`directory`, `registry`), `image`, `working_directory`, `executable_path`, `container_context`, and `agent_queue`, with shared values allowed under a top-level `defaults` key.

## Sources

- https://docs.dagster.io/deployment/dagster-plus — overview; `/getting-started` for org setup
- https://docs.dagster.io/deployment/dagster-plus/serverless — Serverless limits and Serverless vs Hybrid
- https://docs.dagster.io/deployment/dagster-plus/serverless/runtime-environment — PEX, dependencies, base images, data files
- https://docs.dagster.io/deployment/dagster-plus/run-isolation — isolated and non-isolated runs
- https://docs.dagster.io/deployment/dagster-plus/hybrid — Hybrid overview and recommended resources
- https://docs.dagster.io/deployment/dagster-plus/hybrid/architecture — agent, code server, run, and security model
- https://docs.dagster.io/deployment/dagster-plus/hybrid/kubernetes/setup — Helm install, branch agents, queues, resources
- https://docs.dagster.io/deployment/dagster-plus/hybrid/amazon-ecs/new-vpc — ECS CloudFormation templates
- https://docs.dagster.io/deployment/dagster-plus/hybrid/docker/setup — Docker agent; `/hybrid/local` for the local agent
- https://docs.dagster.io/deployment/dagster-plus/hybrid/multiple — replicas, isolated agents, agent queues, failover
- https://docs.dagster.io/deployment/dagster-plus/management/build-yaml — build.yaml, container_context.yaml, pyproject.toml, legacy dagster_cloud.yaml
- https://docs.dagster.io/deployment/dagster-plus/management/customizing-agent-settings — server TTL, compute logs
- https://docs.dagster.io/deployment/dagster-plus/code-requirements — code entry point and package requirements
- https://docs.dagster.io/deployment/dagster-plus/deploying-code/configuring-ci-cd — GitHub, GitLab, and manual CI/CD
- https://docs.dagster.io/deployment/dagster-plus/deploying-code/adding-project-to-dagster-plus — adding code locations
- https://docs.dagster.io/deployment/dagster-plus/deploying-code/branch-deployments — branch deployments, plus `/change-tracking`, `/configuring-branch-deployments`, `/branch-deployment-concurrency`, `/testing-against-prod-data`
- https://docs.dagster.io/deployment/dagster-plus/deploying-code/full-deployments/full-deployment-settings-reference — settings YAML schema
- https://docs.dagster.io/deployment/dagster-plus/deploying-code/full-deployments/managing-full-deployments — creating and configuring deployments
- https://docs.dagster.io/deployment/dagster-plus/management/environment-variables — UI vs agent config, plus `/built-in`, `/dagster-ui`, `/agent-config`
- https://docs.dagster.io/deployment/dagster-plus/management/tokens/agent-tokens — agent tokens; `/user-tokens` for user and service user tokens
- https://docs.dagster.io/deployment/dagster-plus/authentication-and-access-control/rbac/user-roles-permissions — roles and permissions matrix; `/rbac/teams` for teams
- https://docs.dagster.io/deployment/dagster-plus/authentication-and-access-control/sso — SSO providers; `/scim` for SCIM provisioning
- https://docs.dagster.io/deployment/dagster-plus/management/rate-limits — log and event rate limits; `/snapshot-size-limits`, `/report-external-system-events`
- https://docs.dagster.io/deployment/dagster-plus/dagster-plus-eu — EU region
- https://docs.dagster.io/guides/build/projects/dagster-plus-project-history — code location history and rollback
- https://docs.dagster.io/guides/observe/alerts/yaml-reference — event types, targets, policy options, validation
- https://docs.dagster.io/guides/observe/alerts/alert-policy-types — policy types; `/configuring-an-alert-notification-service`, `/creating-alerts`
- https://docs.dagster.io/guides/observe/insights — metrics and views; `/insights/custom-metrics`, `/insights/snowflake`
- https://docs.dagster.io/guides/observe/asset-catalog — asset catalog; `/asset-catalog/dashboards`
- https://docs.dagster.io/guides/labs/issues — Issues; `/labs/dagster-ai`, `/labs/webhook-alerts`, `/labs/connections`
- https://docs.dagster.io/getting-started/ai-tools/dagster-mcp — Dagster+ MCP server
- https://docs.dagster.io/api/clis/dg-cli/dg-plus — dg plus reference; `/dg-api` and `/configuring-dagster-plus`
- https://docs.dagster.io/api/clis/dagster-cloud-cli/installing-and-configuring — superseded dagster-cloud CLI
- https://docs.dagster.io/migration/oss-to-dagster-plus — OSS to Dagster+; `/serverless-to-hybrid`, `/dagster-plus-us-to-eu`
- https://docs.dagster.io/deployment/troubleshooting/pex-compatibility-issues-github-actions — PEX and runner compatibility
