"""HTML formatting utilities for DataFusion DataFrames."""

from typing import Dict, Optional, Any, Union, List


class DataFrameHtmlFormatter:
    """Configurable HTML formatter for DataFusion DataFrames.

    This class handles the HTML rendering of DataFrames for display in
    Jupyter notebooks and other rich display contexts.

    Args:
        max_cell_length: Maximum characters to display in a cell before truncation
        max_width: Maximum width of the HTML table in pixels
        max_height: Maximum height of the HTML table in pixels
        enable_cell_expansion: Whether to add expand/collapse buttons for long cell values
        custom_css: Additional CSS to include in the HTML output
        show_truncation_message: Whether to display a message when data is truncated
    """

    def __init__(
        self,
        max_cell_length: int = 25,
        max_width: int = 1000,
        max_height: int = 300,
        enable_cell_expansion: bool = True,
        custom_css: Optional[str] = None,
        show_truncation_message: bool = True,
    ):
        self.max_cell_length = max_cell_length
        self.max_width = max_width
        self.max_height = max_height
        self.enable_cell_expansion = enable_cell_expansion
        self.custom_css = custom_css
        self.show_truncation_message = show_truncation_message

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
            html.append(
                "<th style='border: 1px solid black; padding: 8px; "
                "text-align: left; background-color: #f2f2f2; "
                "white-space: nowrap; min-width: fit-content; "
                f"max-width: fit-content;'>{field.name}</th>"
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
        short_value = str(cell_value)[: self.max_cell_length]
        return (
            f"<td style='border: 1px solid black; padding: 8px; "
            f"text-align: left; white-space: nowrap;'>"
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
        return (
            f"<td style='border: 1px solid black; padding: 8px; "
            f"text-align: left; white-space: nowrap;'>{cell_value}</td>"
        )

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

        Args:
            column: Arrow array
            row_idx: Row index

        Returns:
            Formatted cell value as string
        """
        # This is a simplified implementation for Python-side formatting
        # In practice, we'd want to handle different Arrow types appropriately
        try:
            return str(column[row_idx])
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
