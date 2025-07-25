import pyarrow as pa
from datafusion import Accumulator, SessionContext, udaf


# Define a user-defined aggregation function (UDAF)
class MyAccumulator(Accumulator):
    """
    Interface of a user-defined accumulation.
    """

    def __init__(self) -> None:
        self._sum = pa.scalar(0.0)

    def update(self, values: pa.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pa.scalar(self._sum.as_py() + pa.compute.sum(values).as_py())

    def merge(self, states: list[pa.Array]) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pa.scalar(self._sum.as_py() + pa.compute.sum(states[0]).as_py())

    def state(self) -> list[pa.Scalar]:
        return [self._sum]
    
    def evaluate(self) -> pa.Scalar:
        return self._sum


my_udaf = udaf(
    MyAccumulator,
    pa.float64(),
    pa.float64(),
    [pa.float64()],
    "stable",
    # This will be the name of the UDAF in SQL
    # If not specified it will by default the same as accumulator class name
    name="my_accumulator",
)

# Create a context
ctx = SessionContext()

# Create a datafusion DataFrame from a Python dictionary
source_df = ctx.from_pydict({"a": [1, 1, 3], "b": [4, 5, 6]}, name="t")
# Dataframe:
# +---+---+
# | a | b |
# +---+---+
# | 1 | 4 |
# | 1 | 5 |
# | 3 | 6 |
# +---+---+

# Register UDF for use in SQL
ctx.register_udaf(my_udaf)

# Query the DataFrame using SQL
result_df = ctx.sql(
    "select a, my_accumulator(b) as b_aggregated from t group by a order by a"
)
# Dataframe:
# +---+--------------+
# | a | b_aggregated |
# +---+--------------+
# | 1 | 9            |
# | 3 | 6            |
# +---+--------------+

result_dict = result_df.to_pydict()
print("Result:", result_dict)
assert result_dict["a"] == [1, 3]
assert result_dict["b_aggregated"] == [9.0, 6.0]
print("Test passed successfully!")