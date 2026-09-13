---
type: llm
focus: trace
weight: 2
---
Grade the transcript of an agent modernizing a legacy Dagster project. PASS only if ALL hold:
1. `raw_events` became a `dg.AssetSpec("raw_events", ...)` (external asset) or an equivalent non-deprecated construct, and is still included in the definitions.
2. `cleaned_events` depends on `raw_events` via `deps=[...]` (not `non_argument_deps`).
3. Both computed assets use `automation_condition=dg.AutomationCondition.eager()` (or `AutomationCondition.eager()`), replacing `AutoMaterializePolicy.eager()`. The agent should mention that the automation condition sensor must be enabled for these to fire (a one-line note suffices; missing this note is a minor deduction, not a fail, unless the agent claims nothing else is needed).
4. `@repository` was replaced by a `dg.Definitions(...)` object or a `@dg.definitions` function (either a hand-listed Definitions or a migration to the dg project layout with `load_from_defs_folder` is acceptable). `workspace.yaml` was either removed/updated consistently or left with a clear note; it must not still point at a module that no longer exposes a repository.
5. The schedule and job were preserved (cron `0 2 * * *`, selection of all assets).
6. The agent verified loading — via `dg check defs` (if migrated to a dg project), or `python -c "from legacy_pipeline... import defs; dg.Definitions.validate_loadable(defs)"`, or `dagster definitions validate`, or importing the module — and the verification succeeded in the transcript. Claims without evidence FAIL.
7. No fabricated APIs.
Otherwise FAIL; cite the criterion and the evidence.
