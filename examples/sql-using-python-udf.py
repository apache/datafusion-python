# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pyarrow as pa
from datafusion import SessionContext, udf, DataFrame

# Print version information for debugging
import datafusion
import pyarrow

print(f"DataFusion version: {datafusion.__version__}")
print(f"PyArrow version: {pyarrow.__version__}")


# Define a user-defined function (UDF) that checks if a value is null
def is_null(array: pa.Array) -> pa.Array:
    """
    A UDF that checks if elements in an array are null.
    Args:
        array (pa.Array): Input PyArrow array
    Returns:
        pa.Array: Boolean array indicating which elements are null
    """
    return array.is_null()


# Create the UDF definition
is_null_arr = udf(
    is_null,  # The Python function to use
    [pa.int64()],  # Input type(s) - here we expect one int64 column
    pa.bool_(),  # Output type - returns boolean
    "stable",  # Volatility - "stable" means same input = same output
    name="is_null"  # SQL name for the function
)

# Create a DataFusion session context
ctx = SessionContext()

try:
    # Method 1: Using DataFrame.from_pydict (for newer DataFusion versions)
    print("\nTrying Method 1: DataFrame.from_pydict")
    df = DataFrame.from_pydict(ctx, {
        "a": [1, 2, 3],
        "b": [4, None, 6]
    })
    df.create_or_replace_table("t")
except Exception as e:
    print(f"Method 1 failed: {e}")

    try:
        # Method 2: Using arrow table directly
        print("\nTrying Method 2: Register arrow table")
        table = pa.table({
            "a": [1, 2, 3],
            "b": [4, None, 6]
        })
        ctx.register_table("t", table)
    except Exception as e:
        print(f"Method 2 failed: {e}")

        # Method 3: Using explicit record batch creation
        print("\nTrying Method 3: Explicit record batch creation")
        # Define the schema for our data
        schema = pa.schema([
            ('a', pa.int64()),  # Column 'a' is int64
            ('b', pa.int64())  # Column 'b' is int64
        ])

        # Create a record batch with our data
        batch = pa.record_batch([
            pa.array([1, 2, 3], type=pa.int64()),  # Data for column 'a'
            pa.array([4, None, 6], type=pa.int64())  # Data for column 'b'
        ], schema=schema)

        # Register the record batch with DataFusion
        # Note: The double list [[batch]] is required by the API
        ctx.register_record_batches("t", [[batch]])

# Register our UDF with the context
ctx.register_udf(is_null_arr)

print("\nExecuting SQL query...")
# Execute a SQL query that uses our UDF
result_df = ctx.sql("select a, is_null(b) as b_is_null from t")

# Expected output:
# +---+-----------+
# | a | b_is_null |
# +---+-----------+
# | 1 | false     |
# | 2 | true      |
# | 3 | false     |
# +---+-----------+

# Convert result to dictionary and display
result_dict = result_df.to_pydict()
print("\nQuery Results:")
print("Result:", result_dict)

# Verify the results
assert result_dict["b_is_null"] == [False, True, False], "Unexpected results from UDF"
print("\nAssert passed - UDF working as expected!")

# Print a formatted version of the results
print("\nFormatted Results:")
print("+---+-----------+")
print("| a | b_is_null |")
print("+---+-----------+")
for i in range(len(result_dict["a"])):
    print(f"| {result_dict['a'][i]} | {str(result_dict['b_is_null'][i]).lower():9} |")
print("+---+-----------+")

