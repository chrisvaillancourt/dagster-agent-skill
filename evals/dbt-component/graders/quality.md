---
type: llm
focus: trace
weight: 2
---
Grade a transcript where an agent integrated a dbt project into a dg-based Dagster project. PASS only if ALL hold:
1. Preferred: the agent used the dbt component — `dg scaffold defs dagster_dbt.DbtProjectComponent <name> --project-path dbt/jaffle` producing a `defs.yaml` with `type: dagster_dbt.DbtProjectComponent` and a `project:` attribute pointing at the dbt project (a `{{ project_root }}`-relative path is fine). ACCEPTABLE fallback: a Python `@dbt_assets(manifest=...)` definition with `DbtCliResource` and a `DbtProject(...)` whose manifest is prepared (`prepare_if_dev()` or `dbt parse`). Hand-written `load_assets_from_dbt_project` FAILS (superseded).
2. Automation at 05:00 UTC was attached: either an `automation_condition` (`AutomationCondition.on_cron("0 5 * * *")`) applied to the dbt assets (via the component's `post_processing`/`translation` settings or a translator), or a `ScheduleDefinition` with cron `0 5 * * *` over a job selecting the dbt assets. Either is acceptable; if a schedule is used it must target the dbt assets.
3. profiles.yml handling: the dbt project reads profiles from its own dir; the agent either passed `profiles_dir`/`--profiles-dir`, set `DBT_PROFILES_DIR`, or confirmed dbt found the profile (the manifest was generated without a profile error).
4. Verification: `dg check yaml` (if a component was used) and `dg check defs` ran successfully, and `dg list defs` (or equivalent) showed the dbt model asset keys `customers` and `customer_count`.
5. No fabricated YAML keys or Python APIs; `import dagster as dg` style in any Python written.
Otherwise FAIL; cite the criterion and evidence.
