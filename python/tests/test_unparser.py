from datafusion.context import SessionContext
from datafusion.unparser import Dialect, Unparser


def test_unparser():
    ctx = SessionContext()
    df = ctx.sql("SELECT 1")
    for dialect in [
        Dialect.mysql(),
        Dialect.postgres(),
        Dialect.sqlite(),
        Dialect.duckdb(),
    ]:
        unparser = Unparser(dialect)
        sql = unparser.plan_to_sql(df.logical_plan())
        assert sql == "SELECT 1"
