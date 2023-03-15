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

import pytest

import datafusion
from datafusion import (
    AggregateUDF,
    DataFrame,
    SessionContext,
    ScalarUDF,
    functions,
)

from datafusion.common import (
    DFField,
    DFSchema,
)

from datafusion.expr import (
    Expr,
    Column,
    Literal,
    BinaryExpr,
    AggregateFunction,
    Projection,
    TableScan,
    Filter,
    Limit,
    Aggregate,
    Sort,
    Analyze,
    Join,
    JoinType,
    JoinConstraint,
    CrossJoin,
    Union,
    Like,
    ILike,
    SimilarTo,
    ScalarVariable,
    Alias,
    GetIndexedField,
    Not,
    IsNotNull,
    IsTrue,
    IsFalse,
    IsUnknown,
    IsNotTrue,
    IsNotFalse,
    IsNotUnknown,
    Negative,
    ScalarFunction,
    BuiltinScalarFunction,
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
    SubqueryAlias,
    Between,
    Explain,
    Extension,
    CreateMemoryTable,
    CreateView,
    Distinct,
    DropTable,
    Repartition,
    Partitioning,
)


def test_import_datafusion():
    assert datafusion.__name__ == "datafusion"


def test_datafusion_python_version():
    assert datafusion.__version__ is not None


def test_class_module_is_datafusion():
    for klass in [
        SessionContext,
        DataFrame,
        ScalarUDF,
        AggregateUDF,
    ]:
        assert klass.__module__ == "datafusion"

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
        CrossJoin,
        Union,
        Like,
        ILike,
        SimilarTo,
        ScalarVariable,
        Alias,
        GetIndexedField,
        Not,
        IsNotNull,
        IsTrue,
        IsFalse,
        IsUnknown,
        IsNotTrue,
        IsNotFalse,
        IsNotUnknown,
        Negative,
        ScalarFunction,
        BuiltinScalarFunction,
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
    for klass in [DFField, DFSchema]:
        assert klass.__module__ == "datafusion.common"


def test_import_from_functions_submodule():
    from datafusion.functions import abs, sin  # noqa

    assert functions.abs is abs
    assert functions.sin is sin

    msg = "cannot import name 'foobar' from 'datafusion.functions'"
    with pytest.raises(ImportError, match=msg):
        from datafusion.functions import foobar  # noqa


def test_classes_are_inheritable():
    class MyExecContext(SessionContext):
        pass

    class MyExpression(Expr):
        pass

    class MyDataFrame(DataFrame):
        pass
