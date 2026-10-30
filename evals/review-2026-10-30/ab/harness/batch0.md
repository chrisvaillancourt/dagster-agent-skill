You are working in a Dagster project (Dagster 1.13.28, managed with uv; run Python and dg as `uv run ...`). Answer the questions below accurately. You may use any tools you have (run commands, write scratch code, read docs or source) to make sure your answers are right; put scratch files under ./scratch/ and do not modify src/. Each answer: 1-4 sentences with concrete API/CLI names. Format exactly as `### Q<id>` followed by the answer.

### Q1
In a Dagster 1.13 project, my component's defs.yaml uses `description: "token={{ env.API_TOKEN }}"` and API_TOKEN is not set anywhere. What do `dg check yaml --validate-requirements` and `dg check defs` do, and what ends up in the asset description?

### Q5
I added a `[project.entry-points] "dagster_dg_cli.registry_modules"` entry to pyproject.toml of my uv-managed Dagster project. Do I need `uv pip install -e .`, and what happens to the `registry_modules` list in [tool.dg.project]?

### Q10
An asset uses backfill_policy=dg.BackfillPolicy.single_run() with a daily partitions_def. In the body I call context.partition_key and return a DataFrame. What goes wrong when it is backfilled over several days, and how do you fix it?

### Q19
In Dagster 1.13 I have a daily partitioned job and a hand-written schedule. How do I compute 'the latest partition key as of this tick' for `RunRequest(partition_key=...)`, and what must I not pass?

### Q23
Using Dagster 1.13 declarative automation with `eager()` on a daily-partitioned asset, 30 historical partitions are missing. How many partitions does eager request, and how do I make it cover older ones?

### Q32
I need a resource that wraps a foreign client class (e.g. `GitHub(token)`) with a config field, and an asset that receives the built client. Which base class and annotation do I use in Dagster 1.13 given the docs mention `ConfigurableResourceFactory` and `dg.Resource[T]`?

### Q36
A Pipes external script calls `context.report_asset_materialization(metadata={'schema': {'type': 'table_schema', 'raw_value': [{'name': 'id', 'type': 'int'}]}})` in 1.13.28. What happens, and what should raw_value be?

### Q44
On a fresh laptop I run `dg plus login` or `dg plus config set --organization acme --deployment prod`. Which file does the CLI write the config to, and what breaks if an old `~/.dagster_cloud_cli/config` also has Plus settings?

### Q50
I converted a legacy Dagster project to the `dg` layout by adding a `dagster_dg_cli.registry_modules` entry point to its pyproject.toml, then ran `uv sync`, but `dg list registry-modules` doesn't show my components module. Why, and how do I fix it?

### Q55
What does `DbtCliResource` use as its dbt executable by default in dagster-dbt 0.29.28 (Dagster 1.13.28), and what happens to `.fetch_row_counts()` when that executable is the Fusion engine?
