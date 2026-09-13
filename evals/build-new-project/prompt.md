Create a new Dagster project named `sales_pipeline` in the current directory (this directory is empty and I have `uv` installed). It needs:

1. An asset `raw_orders` that returns a small in-memory list of order dicts (id, region, amount).
2. An asset `orders_by_region` that depends on `raw_orders` and returns total amount per region, logging the result.
3. A schedule that materializes both every day at 06:00 UTC.

Use current Dagster best practices for project layout and tooling. When you are done, prove the project loads correctly and tell me exactly which command you ran to verify it.
