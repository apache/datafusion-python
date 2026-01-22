import datafusion as dfn
from datafusion import lit, col, functions as F
from datafusion.expr import Window, WindowFrame

def main() -> None:
    # Create the context and data
    ctx = dfn.SessionContext()
    df = ctx.from_pydict(
        {"any_row": list(range(10))},
    )
    
    # Add a column of ones
    df = df.select(
        "any_row",
        lit(1).alias("ones"),
    )
    
    # Perform Window functions
    df = df.select(
        "any_row",
        F.sum(col("ones"))\
            .over(Window(window_frame=WindowFrame("rows", None, 0), order_by=col("any_row").sort(ascending=True))) \
            .alias("forward_row_sum"),
        F.sum(col("ones"))\
            .over(Window(window_frame=WindowFrame("rows", None, 0), order_by=col("any_row").sort(ascending=False))) \
            .alias("reverse_row_sum"),
    )
    
    # Collect the intermediate window results (this should work)
    print("Collecting Window Results...")
    df.collect()
    
    # THIS IS THE FIX TEST
    print("Attempting to use .aggregate() instead of .select() ...")
    
    # We use an empty list [] for group_by, and the function for the aggregate
    df.aggregate(
        [], 
        [F.first_value(col("forward_row_sum"), order_by=col("any_row"))]
    ).collect()
    
    print("Success! .aggregate() worked.")

if __name__ == "__main__":
    main()