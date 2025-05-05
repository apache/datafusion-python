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
"""Common data types used throughout the DataFusion project."""

from enum import Enum

from ._internal import common as common_internal

# TODO: these should all have proper wrapper classes

DFSchema = common_internal.DFSchema
DataType = common_internal.DataType
DataTypeMap = common_internal.DataTypeMap
PythonType = common_internal.PythonType
RexType = common_internal.RexType
SqlFunction = common_internal.SqlFunction
SqlSchema = common_internal.SqlSchema
SqlStatistics = common_internal.SqlStatistics
SqlTable = common_internal.SqlTable
SqlType = common_internal.SqlType
SqlView = common_internal.SqlView
TableType = common_internal.TableType
TableSource = common_internal.TableSource
Constraints = common_internal.Constraints

__all__ = [
    "Constraints",
    "DFSchema",
    "DataType",
    "DataTypeMap",
    "NullTreatment",
    "PythonType",
    "RexType",
    "SqlFunction",
    "SqlSchema",
    "SqlStatistics",
    "SqlTable",
    "SqlType",
    "SqlView",
    "TableSource",
    "TableType",
]


class NullTreatment(Enum):
    """Describe how null values are to be treated by functions.

    This is used primarily by aggregate and window functions. It can be set on
    these functions using the builder approach described in
    ref:`_window_functions` and ref:`_aggregation` in the online documentation.

    """

    RESPECT_NULLS = common_internal.NullTreatment.RESPECT_NULLS
    IGNORE_NULLS = common_internal.NullTreatment.IGNORE_NULLS
