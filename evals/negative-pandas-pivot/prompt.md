---
name: negative-pandas-pivot
description: Unrelated pandas task; the Dagster skill must not activate.
tags: [negative]
runs: 1
max_turns: 5
timeout_seconds: 300
allowed_tools: [Skill]
expected_outcome: Answers the pandas question directly without invoking the dagster skill.
---
Using pandas, show me how to pivot a DataFrame with columns `date`, `region`, `revenue` so that rows are months and columns are regions, summing revenue. Just the code.
