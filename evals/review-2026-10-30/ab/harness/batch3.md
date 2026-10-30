You are working in a Dagster project (Dagster 1.13.28, managed with uv; run Python and dg as `uv run ...`). Answer the questions below accurately. You may use any tools you have (run commands, write scratch code, read docs or source) to make sure your answers are right; put scratch files under ./scratch/ and do not modify src/. Each answer: 1-4 sentences with concrete API/CLI names. Format exactly as `### Q<id>` followed by the answer.

### Q4
I wrote a custom StateBackedComponent as `@dataclass class X(dg.StateBackedComponent)` with `defs_state: ResolvedDefsStateConfig = DefsStateConfigArgs.local_filesystem()` (copied from the Dagster docstring) and every dg command now errors. What is wrong and what is the fix?

### Q9
I want to run an asset with default backfill settings over a range of daily partitions locally using the dg CLI: `dg launch --assets my_asset --partition-range 2024-05-01...2024-05-07`. Will that work on Dagster 1.13.x, and what has to be true of the asset if not?

### Q13
Inside an I/O manager's load_input, I want the table name stored in the upstream asset's metadata, and also a value that the upstream asset reported via MaterializeResult(metadata=...). What do context.definition_metadata, context.upstream_output.definition_metadata and context.upstream_output.output_metadata give me?

### Q22
I subclassed `dg.AutomationCondition` with a custom `evaluate()` and attached it to an asset. It never evaluates, or errors in the daemon. What do I have to add, and what are the limits?

### Q31
I want to use the new Dagster 1.12+ FreshnessPolicy.time_window(fail_window=timedelta(hours=24)) on an asset in a local OSS project. What extra setup is needed, do I need a sensor or schedule, and what happened to the old FreshnessPolicy class?

### Q35
I have a fresh Dagster 1.13.28 OSS deployment whose dagster.yaml sets only `storage:` (no run_coordinator section). A sensor fires 25 runs at once. Which run coordinator is in effect and how do the runs behave?

### Q43
I'm wiring a custom (non-GitHub) CI to deploy to Dagster+ with the `dg` CLI, in stages: start the session, build and push the image, then tell Dagster+ to update the code locations. Which commands do I run, and what happens if the last stage is just `dg plus deploy`?

### Q48
I'm on Dagster 1.13 and need to model external (not Dagster-produced) assets. Which constructs from older tutorials will fail, and what's the replacement?

### Q54
You want Power BI semantic models to be refreshable and materializable from Dagster, and you are using dagster-powerbi's Component in defs.yaml. How do you enable that without writing Python wrappers?
