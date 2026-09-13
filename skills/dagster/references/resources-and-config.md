# Resources and configuration

Covers `dg.ConfigurableResource` (definition, lifecycle, nesting, launch-time config, testing), the Pythonic config system (`dg.Config`, `dg.RunConfig`), and environment variables/secrets (`dg.EnvVar`). I/O managers are a specialized resource and live in `io-managers.md`. Templating env vars into component YAML (`{{ env.X }}`) is in `components.md`. Resource usage inside sensors/schedules is summarized here; their evaluation semantics are in `schedules-and-sensors.md`.

## Contents
- [Defining a resource](#defining-a-resource)
- [Wiring resources into a dg project](#wiring-resources-into-a-dg-project)
- [Using resources in assets, ops, sensors, schedules](#using-resources-in-assets-ops-sensors-schedules)
- [Resource state and lifecycle](#resource-state-and-lifecycle)
- [Resources that depend on resources](#resources-that-depend-on-resources)
- [Launch-time resource configuration](#launch-time-resource-configuration)
- [Wrapping third-party clients and bare Python objects](#wrapping-third-party-clients-and-bare-python-objects)
- [Environment variables and secrets](#environment-variables-and-secrets)
- [Run config with dg.Config](#run-config-with-dgconfig)
- [Advanced config types](#advanced-config-types)
- [Supplying run config at launch](#supplying-run-config-at-launch)
- [Testing resources and config](#testing-resources-and-config)
- [Gotchas](#gotchas)
- [Legacy and superseded patterns](#legacy-and-superseded-patterns)
- [Sources](#sources)

## Defining a resource

Use when an asset, op, sensor, or schedule talks to anything outside the process: an API, a database, a bucket, a BI tool. Subclass `dg.ConfigurableResource` and declare config as typed pydantic fields.

```python
# src/<project>/defs/resources.py
import dagster as dg
import requests
from requests import Response


class MyConnectionResource(dg.ConfigurableResource):
    username: str

    def request(self, endpoint: str) -> Response:
        return requests.get(f"https://my-api.com/{endpoint}")
```

Fields become the resource's config schema. Config values, unlike plain constructor arguments, are displayed in the UI, can be replaced at run launch from the Launchpad/GraphQL/Python API, and can be sourced from environment variables resolved at runtime. Scaffold the file with `dg scaffold defs dagster.resources resources.py`.

## Wiring resources into a dg project

Resources are bound to string keys in a `dg.Definitions`. In a `dg` project, put the binding in `src/<project>/defs/resources.py` behind `@dg.definitions`; the `defs/` autoloader merges it with everything else under `defs/`.

```python
# src/<project>/defs/resources.py
import dagster as dg
from my_project.defs.aresource import AResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"a_resource": AResource(name="foo")})
```

The dictionary key is the binding name. Any asset/op/sensor/schedule anywhere under `defs/` that declares a parameter with that exact name and a resource-typed annotation receives the instance. Nothing needs to be imported into a central file. Verify with `dg check defs`, which fails when an entity requests a key that no `Definitions` provides. See `project-setup-and-cli.md` for the autoloading rules and for the top-level `definitions.py` merge escape hatch.

## Using resources in assets, ops, sensors, schedules

Annotate a parameter with the resource type. The parameter name must equal the resource key.

```python
# src/<project>/defs/assets.py
@dg.asset
def data_from_service(my_conn: MyConnectionResource) -> dict[str, Any]:
    return my_conn.request("/fetch_data").json()


@dg.op
def update_service(my_conn: MyConnectionResource):
    my_conn.request("/update")
```

Sensors, schedules, and asset checks take resources the same way, after their context parameter: `def process_new_users_sensor(context: dg.SensorEvaluationContext, users_api: UsersAPI)`. This is what keeps sensor and schedule logic testable.

## Resource state and lifecycle

Use when the resource needs to open something (token, connection, file handle) before assets run and close it afterwards. `ConfigurableResource` is a pydantic data class, so mutable runtime state must be declared with `pydantic.PrivateAttr` (leading underscore) to keep it out of the config schema.

```python
# src/<project>/defs/resources.py
from pydantic import PrivateAttr


class MyClientResource(dg.ConfigurableResource):
    username: str
    password: str
    _api_token: str = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._api_token = requests.get(
            "https://my-api.com/token", auth=(self.username, self.password)
        ).text

    def get_all_users(self):
        return requests.get(
            "https://my-api.com/users", headers={"Authorization": self._api_token}
        )
```

| Hook | When it runs |
| --- | --- |
| `setup_for_execution(context)` | Once per run, per process, before execution |
| `teardown_after_execution(context)` | Once per run, per process, after execution resolves |
| `yield_for_execution(context)` | Context manager wrapping both; override for connections that must stay open |

With the multiprocess executor each process gets its own instance, so setup and teardown run once per process, not once per run. Do not assume a single initialization. For anything that must stay open for the whole run, override `yield_for_execution` instead of the two hooks:

```python
class MyClientResource(dg.ConfigurableResource):
    username: str
    password: str
    _db_connection: DBConnection = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context: dg.InitResourceContext):
        with get_database_connection(self.username, self.password) as conn:
            self._db_connection = conn
            yield self
```

`dg.InitResourceContext` exposes `instance`, `log`, `resource_config`, `resource_def`, `resources`, and `run` (`None` outside execution). Inside a method that did not receive the context, call `self.get_resource_context()`.

Caching belongs here too, in the resource rather than the asset, so it is shared and reusable. `functools.lru_cache` on a resource method caches in memory for that instance only, so hits are not shared across assets. An external store (pickle on shared disk, DynamoDB, Redis) survives process restarts and is shared; a pickle file assumes every asset runs on the same node.

## Resources that depend on resources

Use when several resources share credentials or a client. Declare the nested resource as a field; Dagster initializes it and makes it available as `self.<field>`.

```python
# src/<project>/defs/resources.py
class CredentialsResource(dg.ConfigurableResource):
    username: str
    password: str


class FileStoreBucket(dg.ConfigurableResource):
    credentials: dg.ResourceDependency[CredentialsResource]
    region: str

    def write(self, data: str):
        get_filestore_client(
            username=self.credentials.username,
            password=self.credentials.password,
            region=self.region,
        ).write(data)


resources = {
    "bucket": FileStoreBucket(
        credentials=CredentialsResource(username="u", password="p"),
        region="us-east-1",
    )
}
```

A plain annotation (`credentials: CredentialsResource`) works too; `dg.ResourceDependency[...]` states the intent explicitly. A nested resource only needs its own entry in the `resources` dict when it requires launch-time configuration.

This is also how to switch implementations per environment, because it keeps sub-resources inside Dagster's resource system where `dg.EnvVar` resolves. A field typed `snowflake: SnowflakeResource` alongside `duckdb: DuckDBResource`, plus an `env: str` field, lets a method return whichever the environment calls for.

## Launch-time resource configuration

Use when a run needs a different table, path, or tenant each time. `configure_at_launch()` registers the resource class without values and defers construction.

```python
# src/<project>/defs/resources.py
@dg.definitions
def resources():
    return dg.Definitions(resources={"db_conn": DatabaseResource.configure_at_launch()})
```

Supply values from the Launchpad, or from a sensor/schedule via `dg.RunConfig(resources=...)`:

```python
# src/<project>/defs/sensors.py
@dg.sensor(job=update_data_job)
def table_update_sensor():
    for table_name in tables:
        yield dg.RunRequest(
            run_config=dg.RunConfig(
                resources={"db_conn": DatabaseResource(table=table_name)}
            )
        )
```

For a nested resource, bind `configure_at_launch()` once, reuse the handle, and also register it under its own key so the Launchpad can reach it:

```python
credentials = CredentialsResource.configure_at_launch()

resources = {
    "credentials": credentials,
    "bucket": FileStoreBucket(credentials=credentials, region="us-east-1"),
}
```

## Wrapping third-party clients and bare Python objects

Two options, depending on whether the client needs config.

**No config:** pass the object directly and annotate the parameter with `dg.ResourceParam[...]` so Dagster does not mistake it for an upstream asset input.

```python
@dg.asset
def public_github_repos(github: dg.ResourceParam[GitHub]):
    return github.organization("dagster-io").repositories()


resources = {"github": GitHub(...)}
```

**Config plus a foreign client class:** parameterize `dg.ConfigurableResource[T]` and build the client in `create_resource`. The asset receives the built client, annotated `dg.ResourceParam[T]`.

```python
class GitHubResource(dg.ConfigurableResource[GitHub]):
    access_token: str

    def create_resource(self, _context) -> GitHub:
        return GitHub(self.access_token)


@dg.asset
def public_github_repos(github: dg.ResourceParam[GitHub]):
    return github.organization("dagster-io").repositories()


resources = {"github": GitHubResource(access_token=dg.EnvVar("GH_TOKEN"))}
```

The published docs page for this pattern spells the base class `dg.ConfigurableResourceFactory[T]` and the parameter `dg.Resource[T]`; neither name is exported from `dagster` as of 1.13, so use the forms above. Most integrations already ship a resource (`SnowflakeResource`, `DuckDBResource`, `S3Resource`); check `integrations-catalog.md` before writing a wrapper.

## Environment variables and secrets

Two mechanisms with different resolution times. Prefer `dg.EnvVar`.

| | `dg.EnvVar("NAME")` | `os.getenv("NAME")` |
| --- | --- | --- |
| Resolved | At run launch | At code-location load |
| Shown in UI | No (masked in Launchpad, Resources page) | Yes |
| Use for | Secrets, per-run values, resource/config fields | Choosing which definitions to build at load time |

```python
# src/<project>/defs/resources.py
resources = {"credentials": CredentialsResource(password=dg.EnvVar("MY_PASSWORD"))}
```

`EnvVar` inherits from `str` and populates any string config field. For non-string fields use the typed constructor, for example `dg.EnvVar.int("DATABASE_PORT")`. In YAML or a raw config dict the equivalent is `{"env": "ENVIRONMENT_VARIABLE_NAME"}`. To read a value in plain Python, `dg.EnvVar("DATABASE_NAME").get_value()`.

**Local development.** Dagster auto-loads a `.env` file found in the folder where the webserver is launched. Reload the workspace in the UI after editing it; the change is not picked up otherwise. `dg list env` shows the environment variables the project uses and whether they are set.

**Per-environment resources.** Select the resource dict with `os.getenv` at load time, because the choice must be made before the run exists:

```python
# src/<project>/defs/resources.py
@dg.definitions
def resources() -> dg.Definitions:
    deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")
    return dg.Definitions(resources=resources_by_deployment()[deployment_name])
```

On Dagster+, set variables in the UI or via agent configuration, and detect branch deployments with `os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1"`. See `dagster-plus.md`. For component YAML, use `{{ env.VAR }}` plus a `requirements.env` list; see `components.md`.

## Run config with dg.Config

Use when a run needs a parameter that is not a partition. If the value enumerates a fixed dimension of the data, use partitions instead (`partitions-and-backfills.md`).

```python
# src/<project>/defs/assets.py
class MyAssetConfig(dg.Config):
    person_name: str


@dg.asset
def greeting(config: MyAssetConfig) -> str:
    return f"hello {config.person_name}"
```

The parameter must be named `config`. Ops are identical (`@dg.op def print_greeting(config: MyOpConfig)`). `dg.Config` subclasses `pydantic.BaseModel`, so defaults, `Optional`, and `pydantic.Field` all behave as in pydantic.

Dagster validates run config against the model and aborts with `dg.DagsterInvalidConfigError` or a pydantic `ValidationError` when it does not match, including on unknown keys.

## Advanced config types

Annotate fields with `pydantic.Field` for descriptions and range validation, both surfaced in the UI: `age: int = Field(gt=0, lt=100, description="...")`.

| Need | Spelling |
| --- | --- |
| Default value | `greeting_phrase: str = "hello"`, or `Field(default="hello", description=...)` |
| Optional, defaults to `None` | `person_name: str \| None = None` |
| Optional type but still required | `person_first_name: str \| None = ...` |
| Lists, dicts, mappings | `user_names: list[str]`, `user_scores: dict[str, int]` |
| Nested schema | a field typed as another `dg.Config` subclass, including inside `dict[str, UserData]` |
| Arbitrary extra keys | subclass `dg.PermissiveConfig`; read extras with `config.dict()` |
| Either-or | discriminated union: `pet: Cat \| Dog = Field(discriminator="pet_type")`, each variant a `dg.Config` with `pet_type: Literal["cat"] = "cat"` |
| Enum | an `enum.Enum` subclass used as the field type |
| Custom validation | pydantic validators as methods on the config class |

YAML and config-dict representations differ from the Python ones for two types. A discriminated union nests under the discriminator value, and an enum is written as its **name**, not its value:

```yaml
pet:
  cat:
    meows: 10
users_list:
  Bob: GUEST
  Alice: ADMIN
```

Validators raise at instantiation. For config supplied in the Launchpad a failing validator does not block the launch button; it raises and prevents the run from starting.

## Supplying run config at launch

**Python.** `dg.RunConfig` takes Pythonic config objects and converts them to config dicts. Its first positional argument is `ops`; asset config also goes under `ops`, keyed by asset name.

```python
asset_result = dg.materialize(
    [greeting],
    run_config=dg.RunConfig({"greeting": MyAssetConfig(person_name="Alice")}),
)

job_result = greeting_job.execute_in_process(
    run_config=dg.RunConfig(
        ops={"print_greeting": MyOpConfig(person_name="Alice")},
        resources={"db_conn": DatabaseResource(table="t")},
    )
)
```

**Schedules and sensors.** Pass `run_config=` on `dg.RunRequest`, either a `dg.RunConfig` or the raw dict: `{"ops": {"configurable_asset": {"config": {"scheduled_date": d}}}}`.

**UI.** Jobs have a **Launchpad** tab. For assets, use the dropdown next to **Materialize** and choose **Open launchpad**; it opens automatically when the asset has config. **Scaffold Missing Config** generates stub values from the schema.

**CLI.** Put the same structure in a YAML file and pass it with `--config`:

```bash
# my_config.yaml -> ops: {op_using_config: {config: {person_name: Alice}}}
dg launch --job my_job --config my_config.yaml
```

The full run config schema has four top-level keys, `execution`, `loggers`, `resources`, and `ops`, each entry nesting its values under a `config` key (`ops` entries may also carry `inputs`).

**Baked into a job.** `dg.define_asset_job(name=..., selection=..., config={...})` fixes run config for every run of that job. The dict must conform to the standard schema and is viewable and editable in the UI, so never put secrets there.

**Reshaping config.** `@dg.config_mapping` exposes a simpler schema on a job or graph and expands it to the child ops' config, then `@dg.job(config=simplified_config)`:

```python
@dg.config_mapping
def simplified_config(val: SimplifiedConfig) -> dg.RunConfig:
    return dg.RunConfig(
        ops={"do_something": DoSomethingConfig(config_param=val.simplified_param)}
    )
```

**Checking config without running.** `dg.validate_run_config(job_def, run_config)` validates a raw config blob against a job and returns the resolved dictionary.

## Testing resources and config

Construct the resource directly; no Dagster machinery required.

```python
# tests/test_resource.py
def test_my_resource():
    assert MyResource(value="foo").get_value() == "foo"


def test_my_resource_with_nesting():
    r = MyResourceRequiresAnother(foo=StringHolderResource(value="foo"), bar="bar")
    assert r.foo.value == "foo"
```

For a resource that reads `get_resource_context()`, build a context and attach it with `with_resource_context`:

```python
with dg.DagsterInstance.ephemeral() as instance:
    context = dg.build_init_resource_context(instance=instance)
    assert (
        MyContextResource(base_path=None)
        .with_resource_context(context)
        .effective_base_path()
        == instance.storage_directory()
    )
```

Sensors and schedules are invoked directly with a fake passed by keyword, which is why nested resources beat module-level globals: `process_new_users_sensor(dg.build_sensor_context(), users_api=FakeUsersAPI())`. `dg.build_resources({...})` is a context manager that initializes and tears down a set of resources outside a run, for integration tests. More patterns in `testing-and-asset-checks.md`.

## Gotchas

- **`EnvVar` outside `Definitions` is never resolved.** A resource constructed at module level and returned from a method keeps the literal string `"DUCKDB_DATABASE"` as its value. Nest sub-resources as typed fields on the parent resource so Dagster's resource system owns their lifecycle.
- **Key must equal parameter name.** A mismatch is not a type error, it is a missing-resource error at load. `dg check defs` catches it.
- **Every asset that requests a key needs it provided.** Providing resources only in one `Definitions` while the asset lives in another autoloaded module is fine; providing it nowhere is not.
- **`EnvVar` populates string fields.** Use `dg.EnvVar.int(...)` for integer fields rather than wrapping `EnvVar` in `int()`.
- **Secrets never go in `define_asset_job(config=...)`, in metadata, or in `@dg.configured`.** Those values render in the UI. Use `dg.EnvVar` on a resource field.
- **No external calls at import time.** Resource construction happens at code-location load; do the network work in `setup_for_execution` or the method body, not in the class body or a module-level call.
- **Setup and teardown run per process.** Anything expensive is paid once per worker under the multiprocess executor.
- **`.env` needs a workspace reload.** Editing the file while the webserver runs does nothing until the workspace is reloaded in the UI.
- **Unknown config keys fail the run** unless the model subclasses `dg.PermissiveConfig`, and enums serialize by name in YAML (`ADMIN`, not `admin`).

## Legacy and superseded patterns

| Legacy | Modern |
| --- | --- |
| `@dg.resource` function + `dg.ResourceDefinition` | subclass `dg.ConfigurableResource` |
| `@dg.asset(required_resource_keys={"foo"})` + `context.resources.foo` | typed parameter `foo: FooResource` |
| `config_schema={...}` on `@dg.op`/`@dg.asset` | `config: MyConfig` parameter with `dg.Config` subclass |
| `context.op_config["word"]` | `config.word` |
| `dg.StringSource` / `dg.IntSource` | `dg.EnvVar` / `dg.EnvVar.int` |
| `dg.Selector` | pydantic discriminated union with `Field(discriminator=...)` |
| `dg.Shape`, `dg.Array`, `dg.Permissive`, `dg.Noneable`, `dg.Map`, `dg.Field` | pydantic types on a `dg.Config` subclass; `dg.PermissiveConfig` for open schemas |
| `dg.with_resources([assets], resource_defs=...)` | `dg.Definitions(resources={...})` |
| `dg.configured(...)` | resource fields, or `configure_at_launch()` |
| `dg.make_values_resource(...)` for sharing values between ops | a `ConfigurableResource` with typed fields |

`ResourceDefinition.hardcoded_resource`, `.mock_resource`, `.none_resource`, and `.string_resource` remain as testing/stub helpers on the legacy class; in modern code, pass the object itself with `dg.ResourceParam` or instantiate the resource directly in the test. Broader migration guidance is in `migration-and-legacy.md`.

## Sources
- https://docs.dagster.io/guides/build/external-resources/defining-resources — subclassing `ConfigurableResource` for assets, ops, sensors, schedules
- https://docs.dagster.io/guides/build/external-resources/using-resources — `defs/resources.py` and `dg scaffold defs dagster.resources`
- https://docs.dagster.io/guides/build/external-resources/configuring-resources — `EnvVar`, `configure_at_launch()`, nested resources
- https://docs.dagster.io/guides/build/external-resources/managing-resource-state — `setup_for_execution`, `teardown_after_execution`, `yield_for_execution`
- https://docs.dagster.io/guides/build/external-resources/testing-configurable-resources — direct construction, `build_init_resource_context`
- https://docs.dagster.io/guides/build/external-resources/using-bare-python-objects-as-resources — `ResourceParam`
- https://docs.dagster.io/guides/build/external-resources/connecting-to-databases — Snowflake resource and per-deployment env vars
- https://docs.dagster.io/guides/operate/configuration/run-configuration — `Config`, `RunConfig`, Launchpad, `dg launch --config`
- https://docs.dagster.io/guides/operate/configuration/advanced-config-types — defaults, data structures, unions, enums, validators, `PermissiveConfig`
- https://docs.dagster.io/guides/operate/configuration/using-environment-variables-and-secrets — `EnvVar` vs `os.getenv`, `.env`, per-environment config, nested-resource pitfall
- https://docs.dagster.io/guides/build/components/using-environment-variables-in-components — `dg list env`, `{{ env.X }}`
- https://docs.dagster.io/examples/best-practices/resource-caching — in-memory vs external caching in resources
- https://docs.dagster.io/dagster-basics-tutorial/resources — `DuckDBResource` and `dg check defs`
- https://docs.dagster.io/api/dagster/resources — `ConfigurableResource`, `InitResourceContext`, `build_resources`, `EnvVar`, legacy `@resource`
- https://docs.dagster.io/api/dagster/config — `Config`, `PermissiveConfig`, `RunConfig`, `ConfigMapping`, legacy config types
