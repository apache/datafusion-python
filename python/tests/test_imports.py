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

import datafusion
import pytest
from datafusion import (
    AggregateUDF,
    DataFrame,
    ScalarUDF,
    SessionContext,
    functions,
)
from datafusion.common import (
    DFSchema,
)
from datafusion.expr import (
    Aggregate,
    AggregateFunction,
    Alias,
    Analyze,
    Between,
    BinaryExpr,
    Case,
    Cast,
    Column,
    CreateMemoryTable,
    CreateView,
    Distinct,
    DropTable,
    Exists,
    Explain,
    Expr,
    Extension,
    Filter,
    GroupingSet,
    ILike,
    InList,
    InSubquery,
    IsFalse,
    IsNotFalse,
    IsNotNull,
    IsNotTrue,
    IsNotUnknown,
    IsTrue,
    IsUnknown,
    Join,
    JoinConstraint,
    JoinType,
    Like,
    Limit,
    Literal,
    Negative,
    Not,
    Partitioning,
    Placeholder,
    Projection,
    Repartition,
    ScalarSubquery,
    ScalarVariable,
    SimilarTo,
    Sort,
    Subquery,
    SubqueryAlias,
    TableScan,
    TryCast,
    Union,
)


def test_import_datafusion():
    assert datafusion.__name__ == "datafusion"


def test_datafusion_python_version():
    assert datafusion.__version__ is not None


def test_class_module_is_datafusion():
    # context
    for klass in [
        SessionContext,
    ]:
        assert klass.__module__ == "datafusion.context"

    # dataframe
    for klass in [
        DataFrame,
    ]:
        assert klass.__module__ == "datafusion.dataframe"

    # udf
    for klass in [
        AggregateUDF,
        ScalarUDF,
    ]:
        assert klass.__module__ == "datafusion.udf"

    # expressions
    for klass in [Expr, Column, Literal, BinaryExpr, AggregateFunction]:
        assert klass.__module__ == "datafusion.expr"

    # operators
    for klass in [
        Projection,
        TableScan,
        Aggregate,
        Sort,
        Limit,
        Filter,
        Analyze,
        Join,
        JoinType,
        JoinConstraint,
        Union,
        Like,
        ILike,
        SimilarTo,
        ScalarVariable,
        Alias,
        Not,
        IsNotNull,
        IsTrue,
        IsFalse,
        IsUnknown,
        IsNotTrue,
        IsNotFalse,
        IsNotUnknown,
        Negative,
        InList,
        Exists,
        Subquery,
        InSubquery,
        ScalarSubquery,
        GroupingSet,
        Placeholder,
        Case,
        Cast,
        TryCast,
        Between,
        SubqueryAlias,
        Explain,
        Extension,
        CreateMemoryTable,
        CreateView,
        Distinct,
        DropTable,
        Repartition,
        Partitioning,
    ]:
        assert klass.__module__ == "datafusion.expr"

    # schema
    for klass in [DFSchema]:
        assert klass.__module__ == "datafusion.common"


def test_import_from_functions_submodule():
    from datafusion.functions import abs as df_abs
    from datafusion.functions import sin

    assert functions.abs is df_abs
    assert functions.sin is sin

    msg = "cannot import name 'foobar' from 'datafusion.functions'"
    with pytest.raises(ImportError, match=msg):
        from datafusion.functions import foobar  # noqa: F401


def test_classes_are_inheritable():
    class MyExecContext(SessionContext):
        pass

    class MyExpression(Expr):
        pass

    class MyDataFrame(DataFrame):
        pass
