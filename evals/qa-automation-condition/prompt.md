---
name: qa-automation-condition
description: Design a cross-asset trigger; the modern answer is AutomationCondition, not a sensor.
tags: [qa, automation]
runs: 2
max_turns: 15
timeout_seconds: 600
allowed_tools: [Read, Glob, Grep, Skill]
expected_outcome: Uses dg.AutomationCondition composition rather than a custom sensor, and explains enabling the automation condition sensor.
---
In our Dagster project, the asset `report` depends on `orders` and `customers`. I want `report` to update automatically once both `orders` and `customers` have been updated since the last time `report` ran, and never more than once per hour. Show me the asset definition code and anything I need to turn on for it to run. We are on a current Dagster 1.13 project created with create-dagster.
