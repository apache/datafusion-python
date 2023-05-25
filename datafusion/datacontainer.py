from collections import namedtuple
from typing import Any, Dict, List, Tuple, Union

import cudf
import xdf as pd

ColumnType = Union[str, int]

FunctionDescription = namedtuple(
    "FunctionDescription", ["name", "parameters", "return_type", "aggregation"]
)


class ColumnContainer:
    # Forward declaration
    pass


class ColumnContainer:
    """
    Helper class to store a list of columns,
    which do not necessarily be the ones of the dask dataframe.
    Instead, the container also stores a mapping from "frontend"
    columns (columns with the names and order expected by SQL)
    to "backend" columns (the real column names used by dask)
    to prevent unnecessary renames.
    """

    def __init__(
        self,
        frontend_columns: List[str],
        frontend_backend_mapping: Union[Dict[str, ColumnType], None] = None,
    ):
        assert all(
            isinstance(col, str) for col in frontend_columns
        ), "All frontend columns need to be of string type"
        self._frontend_columns = list(frontend_columns)
        if frontend_backend_mapping is None:
            self._frontend_backend_mapping = {
                col: col for col in self._frontend_columns
            }
        else:
            self._frontend_backend_mapping = frontend_backend_mapping

    def _copy(self) -> ColumnContainer:
        """
        Internal function to copy this container
        """
        return ColumnContainer(
            self._frontend_columns.copy(), self._frontend_backend_mapping.copy()
        )

    def limit_to(self, fields: List[str]) -> ColumnContainer:
        """
        Create a new ColumnContainer, which has frontend columns
        limited to only the ones given as parameter.
        Also uses the order of these as the new column order.
        """
        if not fields:
            return self  # pragma: no cover

        assert all(f in self._frontend_backend_mapping for f in fields)
        cc = self._copy()
        cc._frontend_columns = [str(x) for x in fields]
        return cc

    def rename(self, columns: Dict[str, str]) -> ColumnContainer:
        """
        Return a new ColumnContainer where the frontend columns
        are renamed according to the given mapping.
        Columns not present in the mapping are not touched,
        the order is preserved.
        """
        cc = self._copy()
        for column_from, column_to in columns.items():
            backend_column = self._frontend_backend_mapping[str(column_from)]
            cc._frontend_backend_mapping[str(column_to)] = backend_column

        cc._frontend_columns = [
            str(columns[col]) if col in columns else col
            for col in self._frontend_columns
        ]

        return cc

    def rename_handle_duplicates(
        self, from_columns: List[str], to_columns: List[str]
    ) -> ColumnContainer:
        """
        Same as `rename` but additionally handles presence of
        duplicates in `from_columns`
        """
        cc = self._copy()
        cc._frontend_backend_mapping.update(
            {
                str(column_to): self._frontend_backend_mapping[str(column_from)]
                for column_from, column_to in zip(from_columns, to_columns)
            }
        )

        columns = dict(zip(from_columns, to_columns))
        cc._frontend_columns = [
            str(columns.get(col, col)) for col in self._frontend_columns
        ]

        return cc

    def mapping(self) -> List[Tuple[str, ColumnType]]:
        """
        The mapping from frontend columns to backend columns.
        """
        return list(self._frontend_backend_mapping.items())

    @property
    def columns(self) -> List[str]:
        """
        The stored frontend columns in the correct order
        """
        return self._frontend_columns.copy()

    def add(
        self, frontend_column: str, backend_column: Union[str, None] = None
    ) -> ColumnContainer:
        """
        Return a new ColumnContainer with the
        given column added.
        The column is added at the last position in the column list.
        """
        cc = self._copy()

        frontend_column = str(frontend_column)

        cc._frontend_backend_mapping[frontend_column] = str(
            backend_column or frontend_column
        )
        if frontend_column not in cc._frontend_columns:
            cc._frontend_columns.append(frontend_column)

        return cc

    def get_backend_by_frontend_index(self, index: int) -> str:
        """
        Get back the dask column, which is referenced by the
        frontend (SQL) column with the given index.
        """
        frontend_column = self._frontend_columns[index]
        backend_column = self._frontend_backend_mapping[frontend_column]
        return backend_column

    def get_backend_by_frontend_name(self, column: str) -> str:
        """
        Get back the dask column, which is referenced by the
        frontend (SQL) column with the given name.
        """

        try:
            return self._frontend_backend_mapping[column]
        except KeyError:
            return column

    def make_unique(self, prefix="col"):
        """
        Make sure we have unique column names by calling each column

            <prefix>_<number>

        where <number> is the column index.
        """
        return self.rename(
            columns={str(col): f"{prefix}_{i}" for i, col in enumerate(self.columns)}
        )


class Statistics:
    """
    Statistics are used during the cost-based optimization.
    Currently, only the row count is supported, more
    properties might follow. It needs to be provided by the user.
    """

    def __init__(self, row_count: int) -> None:
        self.row_count = row_count

    def __eq__(self, other):
        if isinstance(other, Statistics):
            return self.row_count == other.row_count
        return False


class DataContainer:
    """
    In SQL, every column operation or reference is done via
    the column index. Some dask operations, such as grouping,
    joining or concatenating preserve the columns in a different
    order than SQL would expect.
    However, we do not want to change the column data itself
    all the time (because this would lead to computational overhead),
    but still would like to keep the columns accessible by name and index.
    For this, we add an additional `ColumnContainer` to each dataframe,
    which does all the column mapping between "frontend"
    (what SQL expects, also in the correct order)
    and "backend" (what dask has).
    """

    def __init__(
        self,
        df: cudf.DataFrame,
        column_container: ColumnContainer,
        statistics: Statistics = None,
        filepath: str = None,
    ):
        self.df = df
        self.column_container = column_container
        self.statistics = statistics
        self.filepath = filepath

    def assign(self) -> cudf.DataFrame:
        """
        Combine the column mapping with the actual data and return
        a dataframe which has the the columns specified in the
        stored ColumnContainer.
        """
        df = self.df[
            [
                self.column_container._frontend_backend_mapping[out_col]
                for out_col in self.column_container.columns
            ]
        ]
        df.columns = self.column_container.columns

        return df


class UDF:
    def __init__(self, func, row_udf: bool, params, return_type=None):
        """
        Helper class that handles different types of UDFs and manages
        how they should be mapped to dask operations. Two versions of
        UDFs are supported - when `row_udf=False`, the UDF is treated
        as expecting series-like objects as arguments and will simply
        run those through the function. When `row_udf=True` a row udf
        is expected and should be written to expect a dictlike object
        containing scalars
        """
        self.row_udf = row_udf
        self.func = func

        self.names = [param[0] for param in params]

        self.meta = (None, return_type)

    def __call__(self, *args, **kwargs):
        if self.row_udf:
            column_args = []
            scalar_args = []
            for operand in args:
                if isinstance(operand, cudf.Series):
                    column_args.append(operand)
                else:
                    scalar_args.append(operand)

            df = column_args[0].to_frame(self.names[0])
            for name, col in zip(self.names[1:], column_args[1:]):
                df[name] = col
            result = df.apply(
                self.func, axis=1, args=tuple(scalar_args), meta=self.meta
            ).astype(self.meta[1])
        else:
            result = self.func(*args, **kwargs)
        return result

    def __eq__(self, other):
        if isinstance(other, UDF):
            return self.func == other.func and self.row_udf == other.row_udf
        return NotImplemented

    def __hash__(self):
        return (self.func, self.row_udf).__hash__()


class SchemaContainer:
    def __init__(self, name: str):
        self.__name__ = name
        self.tables: Dict[str, DataContainer] = {}
        self.statistics: Dict[str, Statistics] = {}
        self.experiments: Dict[str, pd.DataFrame] = {}
        self.models: Dict[str, Tuple[Any, List[str]]] = {}
        self.functions: Dict[str, UDF] = {}
        self.function_lists: List[FunctionDescription] = []
        self.filepaths: Dict[str, str] = {}
