You are working in a Dagster project (Dagster 1.13.28, managed with uv; run Python and dg as `uv run ...`). Answer the questions below accurately. You may use any tools you have (run commands, write scratch code, read docs or source) to make sure your answers are right; put scratch files under ./scratch/ and do not modify src/. Each answer: 1-4 sentences with concrete API/CLI names. Format exactly as `### Q<id>` followed by the answer.

### Q2
A defs/ module contains a module-level `@dg.asset` and also a `@dg.definitions` function returning `dg.Definitions(...)`. After `dg list defs`, which assets show up and is there a warning?

### Q6
My `dg check defs` in CI behaves differently from production because code branches on `os.getenv('DAGSTER_IS_DEV_CLI')`. Why, and which dg commands set that variable?

### Q11
A daily-partitioned asset has a schedule built with build_schedule_from_partitioned_job(job, hour_of_day=1, minute_of_hour=30). On 2024-05-20 at 01:30 the tick fires. Which partition does it run, and what changes the answer?

### Q20
I have a `@dg.run_failure_sensor` in code location `alerts` that should also watch jobs in code location `analytics`. I set `monitored_jobs=[dg.CodeLocationSelector(location_name='analytics')]`. Will failures of jobs in `alerts` still trigger it?

### Q28
Writing a pytest for a downstream asset `def down(up): ...` in Dagster 1.13: what is wrong with `dg.materialize_to_memory([down], resources={"io_manager": dg.FilesystemIOManager(...)})`, and what changes if `down` instead declares `deps=[up]`?

### Q33
In run-config YAML for an asset whose Config has an `enum.Enum` field and a pydantic discriminated union (`pet: Cat | Dog = Field(discriminator='pet_type')`), how are the enum value and the union written, and what happens with unknown keys?

### Q37
I want `concurrency.runs.max_concurrent_runs` and `run_retries.max_retries` in dagster.yaml to come from environment variables via `{env: NAME}`. Will that load?

### Q45
For Dagster+ branch deployments, I want at most 5 concurrent runs per PR and 100 across all PRs. Is this a deployment setting I set with `dg api deployment settings set`, and what role does it need?

### Q52
Using dagster-sling with the default `DagsterSlingTranslator` (target_prefix "target") and a replication stream `public.accounts`, what asset key does the generated asset get and what is its upstream dep key? How would you reference that asset from a downstream `@dg.asset`?

### Q57
With dagster-fivetran, what is the default effect of Dagster triggering a connector sync on the connector's Fivetran schedule, and which two `FivetranWorkspace` settings change the default behaviors around that sync?
