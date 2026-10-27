# Reviewing Dagster code

A checklist for code review: patterns where a project reimplements something Dagster already does, usually less safely. Each smell names what to look for, the built-in to use instead, and the caveat that comes with the built-in. Facts were checked against Dagster 1.13 source; the full API detail lives in the reference each section links.

## Contents
- [How to use this checklist](#how-to-use-this-checklist)
- [Env-reading settings wrapped in a resource_fn](#env-reading-settings-wrapped-in-a-resource_fn)
- [One run-failure sensor per code location](#one-run-failure-sensor-per-code-location)
- [Hand-rolled teardown methods](#hand-rolled-teardown-methods)
- [Sources](#sources)

## How to use this checklist

Grep for the patterns below, then read the surrounding code before raising a finding. A smell is a question, not a verdict: the built-in can be wrong for a reason the author knows (a cross-location dependency they don't want, a legacy resource they can't touch yet). Ask for that reason, and suggest the built-in with its caveat.

```bash
grep -rn "ResourceDefinition(resource_fn" src/
grep -rn "os.getenv\|os.environ\|BaseSettings" src/
grep -rn "run_failure_sensor\|run_status_sensor" src/
grep -rnE "def (teardown|close|cleanup)\(" src/
```

## Env-reading settings wrapped in a resource_fn

**Look for:** a settings class (pydantic `BaseSettings`, a dataclass filled from `os.environ`, or a hand-written `from_env()`) that a function builds and returns through `dg.ResourceDefinition(resource_fn=...)` or `@dg.resource`. Often the function also returns a `ConfigurableResource` it constructed itself.

**Use instead:** a `dg.ConfigurableResource` whose fields are bound to `dg.EnvVar("NAME")` (`dg.EnvVar.int("NAME")` for integers) in `Definitions(resources=...)`. Dagster then masks the values in the UI, resolves them when the resource is built, and runs the resource's lifecycle hooks.

```python
class OrdersDb(dg.ConfigurableResource):
    host: str
    port: int = 5432
    password: str = pydantic.Field(min_length=1)   # rejects an empty value at init

dg.Definitions(resources={"orders_db": OrdersDb(
    host=dg.EnvVar("ORDERS_DB_HOST"),
    port=dg.EnvVar.int("ORDERS_DB_PORT"),
    password=dg.EnvVar("ORDERS_DB_PASSWORD"),
)})
```

**Caveats:**
- `EnvVar` takes no default. Keep defaults as Python field defaults and bind `EnvVar` only for values that must come from the environment.
- `EnvVar` resolves at resource init, not at definitions load, so a missing variable passes a load check and fails the first run or tick with `... environment variable "NAME" which is not set`. An empty string counts as set; add a field constraint where empty is invalid.
- A `ConfigurableResource` returned from a `resource_fn` gets no `setup_for_execution` / `teardown_after_execution` calls (verified on 1.13), so converting the wrapper is also how its hooks start running.
- Detail: `resources-and-config.md`.

## One run-failure sensor per code location

**Look for:** the same `@dg.run_failure_sensor` (or `make_slack_on_run_failure_sensor`) defined in several code locations, each with its own copy of routing config (channel names, recipients, env vars), often via a shared helper that every location imports.

**Use instead:** one sensor in one location with `monitored_jobs=[dg.CodeLocationSelector(location_name="analytics"), ...]` listing every location it should watch, including its own. Once `monitored_jobs` is set, runs from the sensor's own location match only what the list names, so leaving the home location out silently stops its alerts. Without `monitored_jobs` or `monitor_all_code_locations=True`, a sensor watches only its own location.

**Caveats:**
- The watching location becomes a dependency of every location it covers: if it fails to load or its code server is down, nobody gets alerts. Keep it small, and alert on its own health some other way.
- Location names are matched by equality. In-process tests don't exercise cross-location matching (a direct call skips matching; a tick evaluation sees location `None`), so verify the selectors in a real multi-location deployment.
- `CodeLocationSelector` matches only a location whose repository is the default `__repository__`. For a legacy location built from a named `@repository`, use `dg.RepositorySelector(location_name=..., repository_name=...)`.
- The routing resources now run in the watching location's code server, so their env vars move there (see `deployment-oss.md`).
- `monitor_all_code_locations=True` is the alternative when every location should be covered; it can't be combined with `monitored_jobs`.
- Detail: `schedules-and-sensors.md`.

## Hand-rolled teardown methods

**Look for:** a resource with a `teardown()`, `close()` or `cleanup()` method that nothing in the project calls explicitly. Dagster never calls such methods, so connections stay open until the process exits.

**Use instead:** the lifecycle hooks Dagster does call on a directly bound `ConfigurableResource`: `setup_for_execution(context)`, `teardown_after_execution(context)`, or a `@contextmanager` `yield_for_execution(context)` that yields `self` (cleanup after the `yield`).

**Caveats:**
- Hooks run once per run per process, so under the multiprocess executor each step process opens and closes its own connection.
- `teardown_after_execution` runs even when the run fails, but not when `setup_for_execution` raised.
- Overriding `yield_for_execution` replaces the default that calls the other two hooks; call them yourself if you still need them.
- The hooks don't run for a resource returned from `ResourceDefinition(resource_fn=...)`; fix the binding first (see the first smell).
- Detail: `resources-and-config.md`.

## Sources

- Dagster 1.13.26 source: `_config/pythonic_config/resource.py` (lifecycle hooks), `_config/field_utils.py` (`EnvVar`), `_config/source.py` (missing-variable error), `_core/definitions/run_status_sensor_definition.py` (run-status matching), `_grpc/impl.py` (sensor evaluation in the code server)
- https://docs.dagster.io/guides/build/external-resources/managing-resource-state — `setup_for_execution`, `teardown_after_execution`, `yield_for_execution`
- https://docs.dagster.io/guides/operate/configuration/using-environment-variables-and-secrets — `EnvVar`
- https://docs.dagster.io/guides/automate/sensors/run-status-sensors — `monitored_jobs`, `CodeLocationSelector`, `monitor_all_code_locations`
