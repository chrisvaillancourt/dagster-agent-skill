# tests/test_defs.py — smoke tests every Dagster project should have.
# create-dagster ships a tests/ package but no runner: `uv add --dev pytest` first.
from datetime import datetime, timezone

import dagster as dg

from my_project.definitions import defs  # the @dg.definitions entry point; call it to get Definitions
from my_project.defs.assets import my_asset
from my_project.defs.schedules import daily_schedule


def test_definitions_load():
    """Fails on duplicate keys, missing resources, bad partition mappings — same check as `dg check defs`."""
    dg.Definitions.validate_loadable(defs())


def test_expected_assets_present():
    keys = set(defs().resolve_all_asset_keys())
    assert dg.AssetKey("my_asset") in keys


def test_daily_schedule_targets_the_right_assets():
    """Check what the schedule runs, not just its cron string.

    A schedule built with `target=` runs an anonymous job; resolve it through the loaded
    Definitions and evaluate one tick so a wrong selection or a `should_execute` bug fails here.
    """
    schedule = defs().resolve_schedule_def("daily_schedule")
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "UTC"
    job = defs().resolve_job_def(schedule.job_name)
    assert {k.to_user_string() for k in job.asset_layer.executable_asset_keys} == {"my_asset"}
    tick = schedule.evaluate_tick(
        dg.build_schedule_context(scheduled_execution_time=datetime(2025, 1, 1, 6, tzinfo=timezone.utc))
    )
    assert len(tick.run_requests) == 1
    assert daily_schedule.name == schedule.name  # the module-level object is the one that got loaded


def test_my_asset_materializes(tmp_path):
    """Materialize a small selection in-process with test resources.

    Every resource the selected assets request must be provided here (a fake/mock subclass of the
    real ConfigurableResource is the usual choice), plus an io_manager when assets return values.
    """
    result = dg.materialize(
        [my_asset],
        resources={
            "io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path)),
            # "warehouse": FakeWarehouseResource(conn="test"),
        },
    )
    assert result.success
