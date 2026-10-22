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

import importlib

import datafusion
import pytest
from datafusion import (
    DataFrame,
    SessionContext,
    functions,
)
from datafusion.catalog import (
    TableProviderFactory,
    TableProviderFactoryExportable,
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
    HigherOrderFunction,
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
    Lambda,
    LambdaVariable,
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
from datafusion.user_defined import (
    AggregateUDF,
    ScalarUDF,
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
        assert klass.__module__ == "datafusion.user_defined"

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
        HigherOrderFunction,
        Lambda,
        LambdaVariable,
    ]:
        assert klass.__module__ == "datafusion.expr"

    # catalog factory types
    for klass in [TableProviderFactory, TableProviderFactoryExportable]:
        assert klass.__module__ == "datafusion.catalog"

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


@pytest.mark.parametrize(
    "name",
    [
        "PhysicalOptimizerRuleExportable",
        "QueryPlannerExportable",
        "SessionComponentsExportable",
        "SessionPlannerExportable",
    ],
)
def test_extension_protocols_import_from_extensions(name):
    import datafusion.extensions

    assert getattr(datafusion.extensions, name).__module__ == "datafusion.extensions"


def test_classes_are_inheritable():
    class MyExecContext(SessionContext):
        pass

    class MyExpression(Expr):
        pass

    class MyDataFrame(DataFrame):
        pass


def test_root_all_matches_curated_surface():
    """Verify package root __all__ contains only common entry points."""
    expected = [
        "Accumulator",
        "DataFrame",
        "Expr",
        "RuntimeEnvBuilder",
        "SessionConfig",
        "SessionContext",
        "SessionExtensionComponents",
        "col",
        "column",
        "functions",
        "ipc",
        "lit",
        "lit_with_metadata",
        "literal",
        "literal_with_metadata",
        "object_store",
        "read_avro",
        "read_csv",
        "read_json",
        "read_parquet",
        "substrait",
        "udaf",
        "udf",
        "udtf",
        "udwf",
        "unparser",
    ]
    assert datafusion.__all__ == expected
    for name in expected:
        assert name in dir(datafusion)
    for name in datafusion._DEPRECATED_ROOT_NAMES:
        assert name not in dir(datafusion)


@pytest.mark.parametrize(
    ("deprecated_name", "submodule", "target_name"),
    [
        ("DataFrameWriteOptions", "datafusion.dataframe", "DataFrameWriteOptions"),
        ("ExplainFormat", "datafusion.dataframe", "ExplainFormat"),
        ("InsertOp", "datafusion.dataframe", "InsertOp"),
        ("ParquetColumnOptions", "datafusion.dataframe", "ParquetColumnOptions"),
        ("ParquetWriterOptions", "datafusion.dataframe", "ParquetWriterOptions"),
        ("ExecutionPlan", "datafusion.plan", "ExecutionPlan"),
        ("LogicalPlan", "datafusion.plan", "LogicalPlan"),
        ("Metric", "datafusion.plan", "Metric"),
        ("MetricsSet", "datafusion.plan", "MetricsSet"),
        ("PhysicalPartitioning", "datafusion.plan", "PhysicalPartitioning"),
        ("Catalog", "datafusion.catalog", "Catalog"),
        ("Table", "datafusion.catalog", "Table"),
        ("TableProviderFactory", "datafusion.catalog", "TableProviderFactory"),
        (
            "TableProviderFactoryExportable",
            "datafusion.catalog",
            "TableProviderFactoryExportable",
        ),
        ("DFSchema", "datafusion.common", "DFSchema"),
        ("WindowFrame", "datafusion.expr", "WindowFrame"),
        ("string_literal", "datafusion.expr", "string_literal"),
        ("str_lit", "datafusion.expr", "str_lit"),
        ("SQLOptions", "datafusion.context", "SQLOptions"),
        ("CsvReadOptions", "datafusion.options", "CsvReadOptions"),
        ("RecordBatch", "datafusion.record_batch", "RecordBatch"),
        ("RecordBatchStream", "datafusion.record_batch", "RecordBatchStream"),
        ("AggregateUDF", "datafusion.user_defined", "AggregateUDF"),
        ("ScalarUDF", "datafusion.user_defined", "ScalarUDF"),
        ("TableFunction", "datafusion.user_defined", "TableFunction"),
        ("WindowUDF", "datafusion.user_defined", "WindowUDF"),
        (
            "configure_formatter",
            "datafusion.dataframe_formatter",
            "configure_formatter",
        ),
    ],
)
def test_deprecated_root_symbols_emit_warning(deprecated_name, submodule, target_name):
    """Deprecated root names emit warning and resolve to target object."""
    assert deprecated_name not in datafusion.__all__

    expected_mod = importlib.import_module(submodule)
    expected_obj = getattr(expected_mod, target_name)

    expected_msg = (
        f"`datafusion.{deprecated_name}` is deprecated and will be removed in a future "
        f"release. Import `{target_name}` from `{submodule}` instead."
    )

    with pytest.deprecated_call(match=expected_msg):
        actual_obj = getattr(datafusion, deprecated_name)

    assert actual_obj is expected_obj


def test_unknown_root_attribute_raises():
    msg = "module 'datafusion' has no attribute 'nonexistent'"
    with pytest.raises(AttributeError, match=msg):
        _ = datafusion.nonexistent


def test_string_literal_in_expr():
    from datafusion.expr import str_lit, string_literal

    e1 = string_literal("hello")
    e2 = str_lit("hello")
    assert str(e1) == 'Expr(Utf8("hello"))'
    assert str(e2) == 'Expr(Utf8("hello"))'
