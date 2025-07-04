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
"""HTML formatting utilities for DataFusion DataFrames."""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Optional,
    Protocol,
    runtime_checkable,
)

from datafusion._internal import DataFrame as DataFrameInternal


def _validate_positive_int(value: Any, param_name: str) -> None:
    """Validate that a parameter is a positive integer.

    Args:
        value: The value to validate
        param_name: Name of the parameter (used in error message)

    Raises:
        ValueError: If the value is not a positive integer
    """
    if not isinstance(value, int) or value <= 0:
        msg = f"{param_name} must be a positive integer"
        raise ValueError(msg)


def _validate_bool(value: Any, param_name: str) -> None:
    """Validate that a parameter is a boolean.

    Args:
        value: The value to validate
        param_name: Name of the parameter (used in error message)

    Raises:
        TypeError: If the value is not a boolean
    """
    if not isinstance(value, bool):
        msg = f"{param_name} must be a boolean"
        raise TypeError(msg)


@runtime_checkable
class CellFormatter(Protocol):
    """Protocol for cell value formatters."""

    def __call__(self, value: Any) -> str:
        """Format a cell value to string representation."""
        ...


@runtime_checkable
class StyleProvider(Protocol):
    """Protocol for HTML style providers."""

    def get_cell_style(self) -> str:
        """Get the CSS style for table cells."""
        ...

    def get_header_style(self) -> str:
        """Get the CSS style for header cells."""
        ...


class DefaultStyleProvider:
    """Default implementation of StyleProvider."""

    def get_cell_style(self) -> str:
        """Get the CSS style for table cells.

        Returns:
            CSS style string
        """
        return (
            "border: 1px solid black; padding: 8px; text-align: left; "
            "white-space: nowrap;"
        )

    def get_header_style(self) -> str:
        """Get the CSS style for header cells.

        Returns:
            CSS style string
        """
        return (
            "border: 1px solid black; padding: 8px; text-align: left; "
            "background-color: #f2f2f2; white-space: nowrap; min-width: fit-content; "
            "max-width: fit-content;"
        )


class DataFrameHtmlFormatter:
    """Configurable HTML formatter for DataFusion DataFrames.

    This class handles the HTML rendering of DataFrames for display in
    Jupyter notebooks and other rich display contexts.

    This class supports extension through composition. Key extension points:
    - Provide a custom StyleProvider for styling cells and headers
    - Register custom formatters for specific types
    - Provide custom cell builders for specialized cell rendering

    Args:
        max_cell_length: Maximum characters to display in a cell before truncation
        max_width: Maximum width of the HTML table in pixels
        max_height: Maximum height of the HTML table in pixels
        max_memory_bytes: Maximum memory in bytes for rendered data (default: 2MB)
        min_rows_display: Minimum number of rows to display
        repr_rows: Default number of rows to display in repr output
        enable_cell_expansion: Whether to add expand/collapse buttons for long cell
          values
        custom_css: Additional CSS to include in the HTML output
        show_truncation_message: Whether to display a message when data is truncated
        style_provider: Custom provider for cell and header styles
        use_shared_styles: Whether to load styles and scripts only once per notebook
          session
    """

    def __init__(
        self,
        max_cell_length: int = 25,
        max_width: int = 1000,
        max_height: int = 300,
        max_memory_bytes: int = 2 * 1024 * 1024,  # 2 MB
        min_rows_display: int = 20,
        repr_rows: int = 10,
        enable_cell_expansion: bool = True,
        custom_css: Optional[str] = None,
        show_truncation_message: bool = True,
        style_provider: Optional[StyleProvider] = None,
        use_shared_styles: bool = True,
    ) -> None:
        """Initialize the HTML formatter.

        Parameters
        ----------
        max_cell_length : int, default 25
            Maximum length of cell content before truncation.
        max_width : int, default 1000
            Maximum width of the displayed table in pixels.
        max_height : int, default 300
            Maximum height of the displayed table in pixels.
        max_memory_bytes : int, default 2097152 (2MB)
            Maximum memory in bytes for rendered data.
        min_rows_display : int, default 20
            Minimum number of rows to display.
        repr_rows : int, default 10
            Default number of rows to display in repr output.
        enable_cell_expansion : bool, default True
            Whether to allow cells to expand when clicked.
        custom_css : str, optional
            Custom CSS to apply to the HTML table.
        show_truncation_message : bool, default True
            Whether to show a message indicating that content has been truncated.
        style_provider : StyleProvider, optional
            Provider of CSS styles for the HTML table. If None, DefaultStyleProvider
            is used.
        use_shared_styles : bool, default True
            Whether to use shared styles across multiple tables.

        Raises:
        ------
        ValueError
            If max_cell_length, max_width, max_height, max_memory_bytes,
            min_rows_display, or repr_rows is not a positive integer.
        TypeError
            If enable_cell_expansion, show_truncation_message, or use_shared_styles is
            not a boolean,
            or if custom_css is provided but is not a string,
            or if style_provider is provided but does not implement the StyleProvider
            protocol.
        """
        # Validate numeric parameters
        _validate_positive_int(max_cell_length, "max_cell_length")
        _validate_positive_int(max_width, "max_width")
        _validate_positive_int(max_height, "max_height")
        _validate_positive_int(max_memory_bytes, "max_memory_bytes")
        _validate_positive_int(min_rows_display, "min_rows_display")
        _validate_positive_int(repr_rows, "repr_rows")

        # Validate boolean parameters
        _validate_bool(enable_cell_expansion, "enable_cell_expansion")
        _validate_bool(show_truncation_message, "show_truncation_message")
        _validate_bool(use_shared_styles, "use_shared_styles")

        # Validate custom_css
        if custom_css is not None and not isinstance(custom_css, str):
            msg = "custom_css must be None or a string"
            raise TypeError(msg)

        # Validate style_provider
        if style_provider is not None and not isinstance(style_provider, StyleProvider):
            msg = "style_provider must implement the StyleProvider protocol"
            raise TypeError(msg)

        self.max_cell_length = max_cell_length
        self.max_width = max_width
        self.max_height = max_height
        self.max_memory_bytes = max_memory_bytes
        self.min_rows_display = min_rows_display
        self.repr_rows = repr_rows
        self.enable_cell_expansion = enable_cell_expansion
        self.custom_css = custom_css
        self.show_truncation_message = show_truncation_message
        self.style_provider = style_provider or DefaultStyleProvider()
        self.use_shared_styles = use_shared_styles
        # Registry for custom type formatters
        self._type_formatters: dict[type, CellFormatter] = {}
        # Custom cell builders
        self._custom_cell_builder: Optional[Callable[[Any, int, int, str], str]] = None
        self._custom_header_builder: Optional[Callable[[Any], str]] = None

    def register_formatter(self, type_class: type, formatter: CellFormatter) -> None:
        """Register a custom formatter for a specific data type.

        Args:
            type_class: The type to register a formatter for
            formatter: Function that takes a value of the given type and returns
                a formatted string
        """
        self._type_formatters[type_class] = formatter

    def set_custom_cell_builder(
        self, builder: Callable[[Any, int, int, str], str]
    ) -> None:
        """Set a custom cell builder function.

        Args:
            builder: Function that takes (value, row, col, table_id) and returns HTML
        """
        self._custom_cell_builder = builder

    def set_custom_header_builder(self, builder: Callable[[Any], str]) -> None:
        """Set a custom header builder function.

        Args:
            builder: Function that takes a field and returns HTML
        """
        self._custom_header_builder = builder

    def format_html(
        self,
        batches: list,
        schema: Any,
        has_more: bool = False,
        table_uuid: str | None = None,
    ) -> str:
        """Format record batches as HTML.

        This method is used by DataFrame's _repr_html_ implementation and can be
        called directly when custom HTML rendering is needed.

        Args:
            batches: List of Arrow RecordBatch objects
            schema: Arrow Schema object
            has_more: Whether there are more batches not shown
            table_uuid: Unique ID for the table, used for JavaScript interactions

        Returns:
            HTML string representation of the data

        Raises:
            TypeError: If schema is invalid and no batches are provided
        """
        if not batches:
            return "No data to display"

        # Validate schema
        if schema is None or not hasattr(schema, "__iter__"):
            msg = "Schema must be provided"
            raise TypeError(msg)

        # Generate a unique ID if none provided
        table_uuid = table_uuid or f"df-{id(batches)}"

        # Build HTML components
        html = []

        html.extend(self._build_html_header())

        html.extend(self._build_table_container_start())

        # Add table header and body
        html.extend(self._build_table_header(schema))
        html.extend(self._build_table_body(batches, table_uuid))

        html.append("</table>")
        html.append("</div>")

        # Add footer (JavaScript and messages)
        if self.enable_cell_expansion:
            html.append(self._get_javascript())

        # Always add truncation message if needed (independent of styles)
        if has_more and self.show_truncation_message:
            html.append("<div>Data truncated due to size.</div>")

        return "\n".join(html)

    def format_str(
        self,
        batches: list,
        schema: Any,
        has_more: bool = False,
        table_uuid: str | None = None,
    ) -> str:
        """Format record batches as a string.

        This method is used by DataFrame's __repr__ implementation and can be
        called directly when string rendering is needed.

        Args:
            batches: List of Arrow RecordBatch objects
            schema: Arrow Schema object
            has_more: Whether there are more batches not shown
            table_uuid: Unique ID for the table, used for JavaScript interactions

        Returns:
            String representation of the data

        Raises:
            TypeError: If schema is invalid and no batches are provided
        """
        return DataFrameInternal.default_str_repr(batches, schema, has_more, table_uuid)

    def _build_html_header(self) -> list[str]:
        """Build the HTML header with CSS styles."""
        default_css = self._get_default_css() if self.enable_cell_expansion else ""
        script = f"""
<script>
if (!document.getElementById('df-styles')) {{
  const style = document.createElement('style');
  style.id = 'df-styles';
  style.textContent = `{default_css}`;
  document.head.appendChild(style);
}}
</script>
"""
        html = [script]
        if self.custom_css:
            html.append(f"<style>{self.custom_css}</style>")
        return html

    def _build_table_container_start(self) -> list[str]:
        """Build the opening tags for the table container."""
        html = []
        html.append(
            f'<div style="width: 100%; max-width: {self.max_width}px; '
            f"max-height: {self.max_height}px; overflow: auto; border: "
            '1px solid #ccc;">'
        )
        html.append('<table style="border-collapse: collapse; min-width: 100%">')
        return html

    def _build_table_header(self, schema: Any) -> list[str]:
        """Build the HTML table header with column names."""
        html = []
        html.append("<thead>")
        html.append("<tr>")
        for field in schema:
            if self._custom_header_builder:
                html.append(self._custom_header_builder(field))
            else:
                html.append(
                    f"<th style='{self.style_provider.get_header_style()}'>"
                    f"{field.name}</th>"
                )
        html.append("</tr>")
        html.append("</thead>")
        return html

    def _build_table_body(self, batches: list, table_uuid: str) -> list[str]:
        """Build the HTML table body with data rows."""
        html = []
        html.append("<tbody>")

        row_count = 0
        for batch in batches:
            for row_idx in range(batch.num_rows):
                row_count += 1
                html.append("<tr>")

                for col_idx, column in enumerate(batch.columns):
                    # Get the raw value from the column
                    raw_value = self._get_cell_value(column, row_idx)

                    # Always check for type formatters first to format the value
                    formatted_value = self._format_cell_value(raw_value)

                    # Then apply either custom cell builder or standard cell formatting
                    if self._custom_cell_builder:
                        # Pass both the raw value and formatted value to let the
                        # builder decide
                        cell_html = self._custom_cell_builder(
                            raw_value, row_count, col_idx, table_uuid
                        )
                        html.append(cell_html)
                    else:
                        # Standard cell formatting with formatted value
                        if (
                            len(str(raw_value)) > self.max_cell_length
                            and self.enable_cell_expansion
                        ):
                            cell_html = self._build_expandable_cell(
                                formatted_value, row_count, col_idx, table_uuid
                            )
                        else:
                            cell_html = self._build_regular_cell(formatted_value)
                        html.append(cell_html)

                html.append("</tr>")

        html.append("</tbody>")
        return html

    def _get_cell_value(self, column: Any, row_idx: int) -> Any:
        """Extract a cell value from a column.

        Args:
            column: Arrow array
            row_idx: Row index

        Returns:
            The raw cell value
        """
        try:
            value = column[row_idx]

            if hasattr(value, "as_py"):
                return value.as_py()
        except (AttributeError, TypeError):
            pass
        else:
            return value

    def _format_cell_value(self, value: Any) -> str:
        """Format a cell value for display.

        Uses registered type formatters if available.

        Args:
            value: The cell value to format

        Returns:
            Formatted cell value as string
        """
        # Check for custom type formatters
        for type_cls, formatter in self._type_formatters.items():
            if isinstance(value, type_cls):
                return formatter(value)

        # If no formatter matched, return string representation
        return str(value)

    def _build_expandable_cell(
        self, formatted_value: str, row_count: int, col_idx: int, table_uuid: str
    ) -> str:
        """Build an expandable cell for long content."""
        short_value = str(formatted_value)[: self.max_cell_length]
        return (
            f"<td style='{self.style_provider.get_cell_style()}'>"
            f"<div class='expandable-container'>"
            "<span class='expandable' "
            f"id='{table_uuid}-min-text-{row_count}-{col_idx}'>"
            f"{short_value}</span>"
            "<span class='full-text' "
            f"id='{table_uuid}-full-text-{row_count}-{col_idx}'>"
            f"{formatted_value}</span>"
            f"<button class='expand-btn' "
            f"onclick=\"toggleDataFrameCellText('{table_uuid}',{row_count},{col_idx})\">"
            f"...</button>"
            f"</div>"
            f"</td>"
        )

    def _build_regular_cell(self, formatted_value: str) -> str:
        """Build a regular table cell."""
        return (
            f"<td style='{self.style_provider.get_cell_style()}'>{formatted_value}</td>"
        )

    def _build_html_footer(self, has_more: bool) -> list[str]:
        """Build the HTML footer with JavaScript and messages."""
        html = []

        # Add JavaScript for interactivity only if cell expansion is enabled
        # and we're not using the shared styles approach
        if self.enable_cell_expansion and not self.use_shared_styles:
            html.append(self._get_javascript())

        # Add truncation message if needed
        if has_more and self.show_truncation_message:
            html.append("<div>Data truncated due to size.</div>")

        return html

    def _get_default_css(self) -> str:
        """Get default CSS styles for the HTML table."""
        return """
            .expandable-container {
                display: inline-block;
                max-width: 200px;
            }
            .expandable {
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                display: block;
            }
            .full-text {
                display: none;
                white-space: normal;
            }
            .expand-btn {
                cursor: pointer;
                color: blue;
                text-decoration: underline;
                border: none;
                background: none;
                font-size: inherit;
                display: block;
                margin-top: 5px;
            }
        """

    def _get_javascript(self) -> str:
        """Get JavaScript code for interactive elements."""
        return """
<script>
if (!window.__df_formatter_js_loaded__) {
  window.__df_formatter_js_loaded__ = true;
  window.toggleDataFrameCellText = function (table_uuid, row, col) {
    var shortText = document.getElementById(
      table_uuid + "-min-text-" + row + "-" + col
    );
    var fullText = document.getElementById(
      table_uuid + "-full-text-" + row + "-" + col
    );
    var button = event.target;

    if (fullText.style.display === "none") {
      shortText.style.display = "none";
      fullText.style.display = "inline";
      button.textContent = "(less)";
    } else {
      shortText.style.display = "inline";
      fullText.style.display = "none";
      button.textContent = "...";
    }
  };
}
</script>
"""


class FormatterManager:
    """Manager class for the global DataFrame HTML formatter instance."""

    _default_formatter: DataFrameHtmlFormatter = DataFrameHtmlFormatter()

    @classmethod
    def set_formatter(cls, formatter: DataFrameHtmlFormatter) -> None:
        """Set the global DataFrame HTML formatter.

        Args:
            formatter: The formatter instance to use globally
        """
        cls._default_formatter = formatter
        _refresh_formatter_reference()

    @classmethod
    def get_formatter(cls) -> DataFrameHtmlFormatter:
        """Get the current global DataFrame HTML formatter.

        Returns:
            The global HTML formatter instance
        """
        return cls._default_formatter


def get_formatter() -> DataFrameHtmlFormatter:
    """Get the current global DataFrame HTML formatter.

    This function is used by the DataFrame._repr_html_ implementation to access
    the shared formatter instance. It can also be used directly when custom
    HTML rendering is needed.

    Returns:
        The global HTML formatter instance

    Example:
        >>> from datafusion.html_formatter import get_formatter
        >>> formatter = get_formatter()
        >>> formatter.max_cell_length = 50  # Increase cell length
    """
    return FormatterManager.get_formatter()


def set_formatter(formatter: DataFrameHtmlFormatter) -> None:
    """Set the global DataFrame HTML formatter.

    Args:
        formatter: The formatter instance to use globally

    Example:
        >>> from datafusion.html_formatter import get_formatter, set_formatter
        >>> custom_formatter = DataFrameHtmlFormatter(max_cell_length=100)
        >>> set_formatter(custom_formatter)
    """
    FormatterManager.set_formatter(formatter)


def configure_formatter(**kwargs: Any) -> None:
    """Configure the global DataFrame HTML formatter.

    This function creates a new formatter with the provided configuration
    and sets it as the global formatter for all DataFrames.

    Args:
        **kwargs: Formatter configuration parameters like max_cell_length,
                 max_width, max_height, enable_cell_expansion, etc.

    Raises:
        ValueError: If any invalid parameters are provided

    Example:
        >>> from datafusion.html_formatter import configure_formatter
        >>> configure_formatter(
        ...     max_cell_length=50,
        ...     max_height=500,
        ...     enable_cell_expansion=True,
        ...     use_shared_styles=True
        ... )
    """
    # Valid parameters accepted by DataFrameHtmlFormatter
    valid_params = {
        "max_cell_length",
        "max_width",
        "max_height",
        "max_memory_bytes",
        "min_rows_display",
        "repr_rows",
        "enable_cell_expansion",
        "custom_css",
        "show_truncation_message",
        "style_provider",
        "use_shared_styles",
    }

    # Check for invalid parameters
    invalid_params = set(kwargs) - valid_params
    if invalid_params:
        msg = (
            f"Invalid formatter parameters: {', '.join(invalid_params)}. "
            f"Valid parameters are: {', '.join(valid_params)}"
        )
        raise ValueError(msg)

    # Create and set formatter with validated parameters
    set_formatter(DataFrameHtmlFormatter(**kwargs))


def reset_formatter() -> None:
    """Reset the global DataFrame HTML formatter to default settings.

    This function creates a new formatter with default configuration
    and sets it as the global formatter for all DataFrames.

    Example:
        >>> from datafusion.html_formatter import reset_formatter
        >>> reset_formatter()  # Reset formatter to default settings
    """
    formatter = DataFrameHtmlFormatter()
    set_formatter(formatter)


def _refresh_formatter_reference() -> None:
    """Refresh formatter reference in any modules using it.

    This helps ensure that changes to the formatter are reflected in existing
    DataFrames that might be caching the formatter reference.
    """
    # This is a no-op but signals modules to refresh their reference
