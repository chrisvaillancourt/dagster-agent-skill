---
name: qa-schedule-not-firing
description: Diagnose a schedule that never fires under dg dev.
tags: [qa, troubleshooting]
runs: 2
max_turns: 15
timeout_seconds: 600
allowed_tools: [Read, Glob, Grep, Skill]
expected_outcome: Names the real causes (schedule stopped/default_status, daemon, timezone, code location errors) and how to verify each.
---
I added this to my Dagster project:

```python
import dagster as dg

daily_job = dg.define_asset_job("daily_job", selection="*")
daily_schedule = dg.ScheduleDefinition(job=daily_job, cron_schedule="0 9 * * *")
```

`dg check defs` passes and I can see the schedule in the UI when I run `dg dev`, but it has never launched a run, even after leaving it running past 9am. What's wrong and how do I fix and verify it?
