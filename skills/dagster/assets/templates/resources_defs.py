# src/my_project/defs/resources.py — bind resources for every asset in the project.
# load_from_defs_folder picks up the Definitions returned here and merges it with the rest of defs/.
import dagster as dg


class WarehouseResource(dg.ConfigurableResource):
    """Thin wrapper around a warehouse client; config fields are pydantic-typed."""

    host: str
    database: str
    user: str
    password: str  # supply via EnvVar so it is resolved at run launch and hidden in the UI

    def query(self, sql: str) -> list[dict]:
        ...  # open a connection using self.host / self.user / ...


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            # key must match the parameter name assets use: def my_asset(warehouse: WarehouseResource)
            "warehouse": WarehouseResource(
                host=dg.EnvVar("WAREHOUSE_HOST"),
                database=dg.EnvVar("WAREHOUSE_DB"),
                user=dg.EnvVar("WAREHOUSE_USER"),
                password=dg.EnvVar("WAREHOUSE_PASSWORD"),
            ),
        }
    )
