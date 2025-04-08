"""HTML formatting utilities for DataFusion DataFrames.

This module provides a customizable HTML formatter for displaying DataFrames
in rich environments like Jupyter notebooks.

Examples:
    Basic usage with the default formatter:

    >>> import datafusion as df
    >>> # Create a DataFrame
    >>> ctx = df.SessionContext()
    >>> df_obj = ctx.sql("SELECT 1 as id, 'example' as name")
    >>> # The DataFrame will use the default formatter in Jupyter

    Configuring the global formatter:

    >>> from datafusion.html_formatter import configure_formatter
    >>> configure_formatter(
    ...     max_cell_length=50,
    ...     max_height=500,
    ...     enable_cell_expansion=True
    ... )

    Creating a custom formatter with specialized type handling:

    >>> import datetime
    >>> from datafusion.html_formatter import (
    ...     DataFrameHtmlFormatter,
    ...     StyleProvider,
    ...     get_formatter
    ... )
    >>>
    >>> # Create a custom date formatter
    >>> def format_date(date_value):
    ...     return date_value.strftime("%Y-%m-%d")
    >>>
    >>> # Create a custom style provider
    >>> class BlueHeaderStyleProvider(StyleProvider):
    ...     def get_cell_style(self) -> str:
    ...         return "border: 1px solid #ddd; padding: 8px; text-align: left;"
    ...
    ...     def get_header_style(self) -> str:
    ...         return (
    ...             "border: 1px solid #ddd; padding: 8px; "
    ...             "background-color: #4285f4; color: white; "
    ...             "text-align: left; font-weight: bold;"
    ...         )
    >>>
    >>> # Use composition to create a custom formatter
    >>> formatter = DataFrameHtmlFormatter(
    ...     max_cell_length=100,
    ...     style_provider=BlueHeaderStyleProvider()
    ... )
    >>>
    >>> # Register formatters for specific types
    >>> formatter.register_formatter(datetime.date, format_date)
    >>> formatter.register_formatter(float, lambda x: f"{x:.2f}")
    >>>
    >>> # Make it the global formatter
    >>> from datafusion.html_formatter import configure_formatter
    >>> configure_formatter(
    ...     max_cell_length=100,
    ...     style_provider=BlueHeaderStyleProvider()
    ... )
    >>> # Now register the formatters with the global formatter
    >>> current_formatter = get_formatter()
    >>> current_formatter.register_formatter(datetime.date, format_date)
    >>> current_formatter.register_formatter(float, lambda x: f"{x:.2f}")

    Creating custom cell builders for more complex formatting:

    >>> # Custom cell builder for numeric values
    >>> def number_cell_builder(value, row, col, table_id):
    ...     if isinstance(value, (int, float)) and value < 0:
    ...         return f"<td style='background-color: #ffcccc'>{value}</td>"
    ...     elif isinstance(value, (int, float)) and value > 1000:
    ...         return f"<td style='background-color: #ccffcc; font-weight: bold'>{value}</td>"
    ...     else:
    ...         return f"<td>{value}</td>"
    >>>
    >>> formatter.set_custom_cell_builder(number_cell_builder)
"""

from typing import Dict, Optional, Any, Union, List, Callable, Type, Protocol


class CellFormatter(Protocol):
    """Protocol for cell value formatters."""

    def __call__(self, value: Any) -> str:
        """Format a cell value to string representation."""
        ...


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
        return "border: 1px solid black; padding: 8px; text-align: left; white-space: nowrap;"

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
        enable_cell_expansion: Whether to add expand/collapse buttons for long cell values
        custom_css: Additional CSS to include in the HTML output
        show_truncation_message: Whether to display a message when data is truncated
        style_provider: Custom provider for cell and header styles

    Example:
        Create a formatter that adds color-coding for numeric values and custom date formatting:

        >>> # Create custom style provider
        >>> class CustomStyleProvider:
        ...     def get_cell_style(self) -> str:
        ...         return "border: 1px solid #ddd; padding: 8px;"
        ...
        ...     def get_header_style(self) -> str:
        ...         return (
        ...             "border: 1px solid #ddd; padding: 8px; "
        ...             "background-color: #333; color: white;"
        ...         )
        >>>
        >>> # Create the formatter with custom styling
        >>> formatter = DataFrameHtmlFormatter(
        ...     max_cell_length=50,
        ...     style_provider=CustomStyleProvider()
        ... )
        >>>
        >>> # Add custom formatters for specific data types
        >>> import datetime
        >>> formatter.register_formatter(
        ...     datetime.date,
        ...     lambda d: f'<span style="color: blue">{d.strftime("%b %d, %Y")}</span>'
        ... )
        >>>
        >>> # Format large numbers with commas
        >>> formatter.register_formatter(
        ...     int,
        ...     lambda n: f'<span style="font-family: monospace">{n:,}</span>' if n > 1000 else str(n)
        ... )
        >>>
        >>> # Replace the global formatter so all DataFrames use it
        >>> from datafusion.html_formatter import configure_formatter
        >>> configure_formatter(
        ...     max_cell_length=50,
        ...     style_provider=CustomStyleProvider()
        ... )
    """

    def __init__(
        self,
        max_cell_length: int = 25,
        max_width: int = 1000,
        max_height: int = 300,
        enable_cell_expansion: bool = True,
        custom_css: Optional[str] = None,
        show_truncation_message: bool = True,
        style_provider: Optional[StyleProvider] = None,
    ):
        self.max_cell_length = max_cell_length
        self.max_width = max_width
        self.max_height = max_height
        self.enable_cell_expansion = enable_cell_expansion
        self.custom_css = custom_css
        self.show_truncation_message = show_truncation_message
        self.style_provider = style_provider or DefaultStyleProvider()
        # Registry for custom type formatters
        self._type_formatters: Dict[Type, CellFormatter] = {}
        # Custom cell builders
        self._custom_cell_builder: Optional[Callable[[Any, int, int, str], str]] = None
        self._custom_header_builder: Optional[Callable[[Any], str]] = None

    def register_formatter(self, type_class: Type, formatter: CellFormatter) -> None:
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
        table_uuid: Optional[str] = None,
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
        """
        if not batches:
            return "No data to display"

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
        html.extend(self._build_html_footer(has_more))

        return "\n".join(html)

    def _build_html_header(self) -> List[str]:
        """Build the HTML header with CSS styles."""
        html = []
        html.append("<style>")
        html.append(self._get_default_css())
        if self.custom_css:
            html.append(self.custom_css)
        html.append("</style>")
        return html

    def _build_table_container_start(self) -> List[str]:
        """Build the opening tags for the table container."""
        html = []
        html.append(
            f'<div style="width: 100%; max-width: {self.max_width}px; '
            f'max-height: {self.max_height}px; overflow: auto; border: 1px solid #ccc;">'
        )
        html.append('<table style="border-collapse: collapse; min-width: 100%">')
        return html

    def _build_table_header(self, schema: Any) -> List[str]:
        """Build the HTML table header with column names."""
        html = []
        html.append("<thead>")
        html.append("<tr>")
        for field in schema:
            if self._custom_header_builder:
                html.append(self._custom_header_builder(field))
            else:
                html.append(
                    f"<th style='{self.style_provider.get_header_style()}'>{field.name}</th>"
                )
        html.append("</tr>")
        html.append("</thead>")
        return html

    def _build_table_body(self, batches: list, table_uuid: str) -> List[str]:
        """Build the HTML table body with data rows."""
        html = []
        html.append("<tbody>")

        row_count = 0
        for batch in batches:
            for row_idx in range(batch.num_rows):
                row_count += 1
                html.append("<tr>")

                for col_idx, column in enumerate(batch.columns):
                    cell_value = self._format_cell_value(column, row_idx)

                    if (
                        len(str(cell_value)) > self.max_cell_length
                        and self.enable_cell_expansion
                    ):
                        html.append(
                            self._build_expandable_cell(
                                cell_value, row_count, col_idx, table_uuid
                            )
                        )
                    else:
                        html.append(self._build_regular_cell(cell_value))

                html.append("</tr>")

        html.append("</tbody>")
        return html

    def _build_expandable_cell(
        self, cell_value: Any, row_count: int, col_idx: int, table_uuid: str
    ) -> str:
        """Build an expandable cell for long content."""
        # If custom cell builder is provided, use it
        if self._custom_cell_builder:
            return self._custom_cell_builder(cell_value, row_count, col_idx, table_uuid)

        short_value = str(cell_value)[: self.max_cell_length]
        return (
            f"<td style='{self.style_provider.get_cell_style()}'>"
            f"<div class='expandable-container'>"
            f"<span class='expandable' id='{table_uuid}-min-text-{row_count}-{col_idx}'>"
            f"{short_value}</span>"
            f"<span class='full-text' id='{table_uuid}-full-text-{row_count}-{col_idx}'>"
            f"{cell_value}</span>"
            f"<button class='expand-btn' "
            f"onclick=\"toggleDataFrameCellText('{table_uuid}',{row_count},{col_idx})\">"
            f"...</button>"
            f"</div>"
            f"</td>"
        )

    def _build_regular_cell(self, cell_value: Any) -> str:
        """Build a regular table cell."""
        return f"<td style='{self.style_provider.get_cell_style()}'>{cell_value}</td>"

    def _build_html_footer(self, has_more: bool) -> List[str]:
        """Build the HTML footer with JavaScript and messages."""
        html = []

        # Add JavaScript for interactivity
        if self.enable_cell_expansion:
            html.append(self._get_javascript())

        # Add truncation message if needed
        if has_more and self.show_truncation_message:
            html.append("<div>Data truncated due to size.</div>")

        return html

    def _format_cell_value(self, column: Any, row_idx: int) -> str:
        """Format a cell value for display.

        Uses registered type formatters if available.

        Args:
            column: Arrow array
            row_idx: Row index

        Returns:
            Formatted cell value as string
        """
        try:
            value = column[row_idx]

            # Check for custom type formatters
            for type_cls, formatter in self._type_formatters.items():
                if isinstance(value, type_cls):
                    return formatter(value)

            return str(value)
        except (IndexError, TypeError):
            return ""

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
            function toggleDataFrameCellText(table_uuid, row, col) {
                var shortText = document.getElementById(table_uuid + "-min-text-" + row + "-" + col);
                var fullText = document.getElementById(table_uuid + "-full-text-" + row + "-" + col);
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
            }
            </script>
        """


# Global formatter instance to be used by default
_default_formatter = DataFrameHtmlFormatter()


def get_formatter() -> DataFrameHtmlFormatter:
    """Get the current global DataFrame HTML formatter.

    This function is used by the DataFrame._repr_html_ implementation to access
    the shared formatter instance. It can also be used directly when custom
    HTML rendering is needed.

    Returns:
        The global HTML formatter instance
    """
    return _default_formatter


def configure_formatter(**kwargs: Any) -> None:
    """Configure the global DataFrame HTML formatter.

    This function creates a new formatter with the provided configuration
    and sets it as the global formatter for all DataFrames.

    Args:
        **kwargs: Formatter configuration parameters like max_cell_length,
                 max_width, max_height, enable_cell_expansion, etc.
    """
    global _default_formatter
    _default_formatter = DataFrameHtmlFormatter(**kwargs)


def set_style_provider(provider: StyleProvider) -> None:
    """Set a custom style provider for the global formatter.

    This is a convenience function to replace just the style provider
    of the global formatter instance without changing other settings.

    Args:
        provider: A StyleProvider implementation

    Example:
        >>> from datafusion.html_formatter import set_style_provider
        >>>
        >>> class DarkModeStyleProvider:
        ...     def get_cell_style(self) -> str:
        ...         return "border: 1px solid #555; padding: 8px; color: #eee; background-color: #222;"
        ...
        ...     def get_header_style(self) -> str:
        ...         return (
        ...             "border: 1px solid #555; padding: 8px; "
        ...             "color: white; background-color: #111; font-weight: bold;"
        ...         )
        >>>
        >>> # Apply dark mode styling to all DataFrames
        >>> set_style_provider(DarkModeStyleProvider())
    """
    _default_formatter.style_provider = provider
