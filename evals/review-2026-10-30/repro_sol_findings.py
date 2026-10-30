import dagster as dg
writes = []
class Rec(dg.IOManager):
    def handle_output(self, context, obj): writes.append((context.asset_key.path[-1], obj))
    def load_input(self, context): return 1
@dg.asset
def upstream(): return 1
@dg.asset(deps=[upstream])
def downstream(): return 2
dg.materialize([upstream, downstream], resources={"io_manager": Rec()})
print("1 deps writes:", writes)

daily = dg.DailyPartitionsDefinition(start_date="2026-01-05")
weekly = dg.WeeklyPartitionsDefinition(start_date="2026-01-05")
@dg.asset(partitions_def=daily)
def d(): ...
@dg.asset(partitions_def=weekly, deps=[d])
def w(): ...
defs = dg.Definitions(assets=[d, w])
ag = defs.resolve_asset_graph()
print("2 mapping:", type(ag.get_partition_mapping(w.key, d.key)).__name__)
j = dg.define_asset_job("daily_job", selection=[d])
print("3 resolve_job_def:", dg.Definitions(assets=[d], jobs=[j]).resolve_job_def("daily_job").name)
