---
name: qa-concurrency-pools
description: Configure op-level pool limits and run-level caps for an OSS deployment.
tags: [qa, operations]
runs: 2
max_turns: 15
timeout_seconds: 600
allowed_tools: [Read, Glob, Grep, Skill]
expected_outcome: Correct dagster.yaml concurrency configuration plus pool= on assets; no invented keys.
---
Our self-hosted (OSS) Dagster deployment overloads Snowflake. I need: at most 3 Snowflake assets executing at the same time across ALL runs, and at most 10 runs in progress in total. Show me exactly what to put in dagster.yaml and what to change on the asset definitions. Keep it minimal and only use real configuration keys.
