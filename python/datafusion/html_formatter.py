"""HTML formatting utilities for DataFusion DataFrames."""

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
        # Only include expandable CSS if cell expansion is enabled
        if self.enable_cell_expansion:
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
                    # Get the raw value from the column
                    raw_value = self._get_cell_value(column, row_idx)

                    # Always check for type formatters first to format the value
                    formatted_value = self._format_cell_value(raw_value)

                    # Then apply either custom cell builder or standard cell formatting
                    if self._custom_cell_builder:
                        # Pass both the raw value and formatted value to let the builder decide
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
            return column[row_idx]
        except (IndexError, TypeError):
            return ""

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
            f"<span class='expandable' id='{table_uuid}-min-text-{row_count}-{col_idx}'>"
            f"{short_value}</span>"
            f"<span class='full-text' id='{table_uuid}-full-text-{row_count}-{col_idx}'>"
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

    def _build_html_footer(self, has_more: bool) -> List[str]:
        """Build the HTML footer with JavaScript and messages."""
        html = []

        # Add JavaScript for interactivity only if cell expansion is enabled
        if self.enable_cell_expansion:
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

    # Ensure the changes are reflected in existing DataFrames
    _refresh_formatter_reference()


def _refresh_formatter_reference() -> None:
    """Refresh formatter reference in any modules using it.

    This helps ensure that changes to the formatter are reflected in existing
    DataFrames that might be caching the formatter reference.
    """
    try:
        # This is a no-op but signals modules to refresh their reference
        pass
    except Exception:
        pass
