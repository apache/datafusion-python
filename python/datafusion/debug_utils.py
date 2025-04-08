"""Debug utilities for DataFusion."""


def check_html_formatter_integration():
    """Debug function to check if DataFrame properly uses the HTML formatter."""
    from datafusion import SessionContext
    from datafusion.html_formatter import get_formatter, configure_formatter

    # Print formatter details
    formatter = get_formatter()
    print(f"Default formatter ID: {id(formatter)}")
    print(f"Has type formatters: {len(formatter._type_formatters)}")

    # Create a test DataFrame
    ctx = SessionContext()
    df = ctx.sql("SELECT 1 as a, 2 as b, 3 as c")

    # Check if DataFrame has _repr_html_ method
    if not hasattr(df, "_repr_html_"):
        print("ERROR: DataFrame does not have _repr_html_ method")
        return

    # Get the _repr_html_ method
    repr_html_method = getattr(df, "_repr_html_")
    print(f"DataFrame _repr_html_ method: {repr_html_method}")

    # Register a custom formatter
    formatter.register_formatter(int, lambda n: f"INT:{n}")
    print("Registered formatter for integers")

    # Generate HTML and check if our formatter was used
    html_output = df._repr_html_()
    print(f"HTML contains our formatter output (INT:1): {'INT:1' in html_output}")

    # If not using our formatter, try to install a monkeypatch
    if "INT:1" not in html_output:
        print("Installing monkeypatch for DataFrame._repr_html_")
        import importlib

        df_module = importlib.import_module("datafusion.dataframe")
        DataFrame = getattr(df_module, "DataFrame")

        # Define the monkeypatch
        def patched_repr_html(self):
            """Patched version of _repr_html_ to use our formatter."""
            from datafusion.html_formatter import get_formatter

            formatter = get_formatter()
            print(f"Patched _repr_html_ using formatter ID: {id(formatter)}")
            return formatter.format_html(self.collect(), self.schema())

        # Apply the monkeypatch
        setattr(DataFrame, "_repr_html_", patched_repr_html)

        # Test again
        df = ctx.sql("SELECT 1 as a, 2 as b, 3 as c")
        html_output = df._repr_html_()
        print(
            f"After monkeypatch, HTML contains our formatter output (INT:1): {'INT:1' in html_output}"
        )
