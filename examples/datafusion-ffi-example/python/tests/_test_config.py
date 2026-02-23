from datafusion import SessionConfig, SessionContext
from datafusion_ffi_example import MyConfig


def test_catalog_provider():
    config = MyConfig()
    config = SessionConfig(
        {"datafusion.catalog.information_schema": "true"}
    ).with_extension(config)
    config.set("my_config.baz_count", "42")
    ctx = SessionContext(config)

    result = ctx.sql("SHOW my_config.baz_count;").collect()
    assert result[0][1][0].as_py() == "42"

    ctx.sql("SET my_config.baz_count=1;")
    result = ctx.sql("SHOW my_config.baz_count;").collect()
    assert result[0][1][0].as_py() == "1"
