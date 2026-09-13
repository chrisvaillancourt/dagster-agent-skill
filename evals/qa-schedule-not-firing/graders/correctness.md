---
type: llm
focus: last_message
weight: 2
---
Grade as a Dagster expert. PASS only if ALL hold:
1. Identifies that schedules are STOPPED by default and must be started — via the UI toggle, or by setting `default_status=dg.DefaultScheduleStatus.RUNNING` on the ScheduleDefinition — as the most likely cause.
2. Mentions the timezone dimension: with no `execution_timezone` the cron is evaluated in UTC, so "9am" local may not be 09:00 UTC; shows `execution_timezone="..."` as the fix.
3. Mentions that the daemon must be running (dg dev runs it) and/or how to verify: the schedule's tick history in the UI (Automation/Schedules page) or daemon status page.
4. Does not invent flags or APIs (e.g. `dg schedule start` does not exist unless the answer hedges; `dagster schedule start` is legacy but acceptable if labelled as legacy).
Otherwise FAIL; name the failing criterion.
