# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example demonstrating CsvReadOptions usage."""

from datafusion import CsvReadOptions, SessionContext

# Create a SessionContext
ctx = SessionContext()

# Example 1: Using CsvReadOptions with default values
print("Example 1: Default CsvReadOptions")
options = CsvReadOptions()
df = ctx.read_csv("data.csv", options=options)

# Example 2: Using CsvReadOptions with custom parameters
print("\nExample 2: Custom CsvReadOptions")
options = CsvReadOptions(
    has_header=True,
    delimiter=",",
    quote='"',
    schema_infer_max_records=1000,
    file_extension=".csv",
)
df = ctx.read_csv("data.csv", options=options)

# Example 3: Using the builder pattern (recommended for readability)
print("\nExample 3: Builder pattern")
options = (
    CsvReadOptions()
    .with_has_header(True)  # noqa: FBT003
    .with_delimiter("|")
    .with_quote("'")
    .with_schema_infer_max_records(500)
    .with_truncated_rows(False)  # noqa: FBT003
    .with_newlines_in_values(True)  # noqa: FBT003
)
df = ctx.read_csv("data.csv", options=options)

# Example 4: Advanced options
print("\nExample 4: Advanced options")
options = (
    CsvReadOptions()
    .with_has_header(True)  # noqa: FBT003
    .with_delimiter(",")
    .with_comment("#")  # Skip lines starting with #
    .with_escape("\\")  # Escape character
    .with_null_regex(r"^(null|NULL|N/A)$")  # Treat these as NULL
    .with_truncated_rows(True)  # noqa: FBT003
    .with_file_compression_type("gzip")  # Read gzipped CSV
    .with_file_extension(".gz")
)
df = ctx.read_csv("data.csv.gz", options=options)

# Example 5: Register CSV table with options
print("\nExample 5: Register CSV table")
options = CsvReadOptions().with_has_header(True).with_delimiter(",")  # noqa: FBT003
ctx.register_csv("my_table", "data.csv", options=options)
df = ctx.sql("SELECT * FROM my_table")

# Example 6: Backward compatibility (without options)
print("\nExample 6: Backward compatibility")
# Still works the old way!
df = ctx.read_csv("data.csv", has_header=True, delimiter=",")

print("\nAll examples completed!")
print("\nFor all available options, see the CsvReadOptions documentation:")
print("  - has_header: bool")
print("  - delimiter: str")
print("  - quote: str")
print("  - terminator: str | None")
print("  - escape: str | None")
print("  - comment: str | None")
print("  - newlines_in_values: bool")
print("  - schema: pa.Schema | None")
print("  - schema_infer_max_records: int")
print("  - file_extension: str")
print("  - table_partition_cols: list[tuple[str, pa.DataType]]")
print("  - file_compression_type: str")
print("  - file_sort_order: list[list[SortExpr]]")
print("  - null_regex: str | None")
print("  - truncated_rows: bool")
