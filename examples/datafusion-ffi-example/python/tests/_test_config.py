import pyarrow as pa
from datafusion import SessionConfig
from datafusion_ffi_example import MyConfig

def test_catalog_provider():
    config = MyConfig()
    config = SessionConfig().with_extension(config)
    config.set("my_config.baz_count", "42")
