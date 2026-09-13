# tests/test_defs.py — smoke tests every Dagster project should have.
import dagster as dg

from my_project.definitions import defs  # the @dg.definitions entry point; call it to get Definitions
from my_project.defs.assets import my_asset


def test_definitions_load():
    """Fails on duplicate keys, missing resources, bad partition mappings — same check as `dg check defs`."""
    dg.Definitions.validate_loadable(defs())


def test_expected_assets_present():
    keys = set(defs().resolve_all_asset_keys())
    assert dg.AssetKey("my_asset") in keys


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
