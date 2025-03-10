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

"""The default input source for DataFusion."""

import glob
from pathlib import Path
from typing import Any

from datafusion.common import DataTypeMap, SqlTable
from datafusion.input.base import BaseInputSource


class LocationInputPlugin(BaseInputSource):
    """Input Plugin for everything.

    This can be read in from a file (on disk, remote etc.).
    """

    def is_correct_input(self, input_item: Any, table_name: str, **kwargs: Any) -> bool:  # noqa: ARG002
        """Returns `True` if the input is valid."""
        return isinstance(input_item, str)

    def build_table(
        self,
        input_item: str,
        table_name: str,
        **kwargs: Any,  # noqa: ARG002
    ) -> SqlTable:  # type: ignore[invalid-type-form]
        """Create a table from the input source."""
        extension = Path(input_item).suffix
        file_format = extension.lstrip(".").lower()
        num_rows = 0  # Total number of rows in the file. Used for statistics
        columns = []
        if file_format == "parquet":
            import pyarrow.parquet as pq

            # Read the Parquet metadata
            metadata = pq.read_metadata(input_item)
            num_rows = metadata.num_rows
            # Iterate through the schema and build the SqlTable
            columns = [
                (
                    col.name,
                    DataTypeMap.from_parquet_type_str(col.physical_type),
                )
                for col in metadata.schema
            ]

        elif format == "csv":
            import csv

            # Consume header row and count number of rows for statistics.
            # TODO: Possibly makes sense to have the eager number of rows
            # calculated as a configuration since you must read the entire file
            # to get that information. However, this should only be occurring
            # at table creation time and therefore shouldn't
            # slow down query performance.
            with Path(input_item).open() as file:
                reader = csv.reader(file)
                _header_row = next(reader)
                for _ in reader:
                    num_rows += 1
            # TODO: Need to actually consume this row into reasonable columns
            msg = "TODO: Currently unable to support CSV input files."
            raise RuntimeError(msg)
        else:
            msg = f"Input of format: `{format}` is currently not supported.\
                Only Parquet and CSV."
            raise RuntimeError(msg)

        # Input could possibly be multiple files. Create a list if so
        input_files = glob.glob(input_item)

        return SqlTable(table_name, columns, num_rows, input_files)
