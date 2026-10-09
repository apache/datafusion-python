"""
DataFusion Python FFI provider example.

Walks the conformance matrix in numbered sections: table provider, functions,
catalog provider, config extension, codec round-trip, then the same bytes
decoded a second time.
"""

import sys

from datafusion import LogicalPlan, SessionConfig, SessionContext, udf

try:
    from datafusion_ffi_example import (
        IsNullUDF,
        MyCatalogProvider,
        MyConfig,
        MyLogicalExtensionCodec,
        MyTableProvider,
    )
except ImportError:
    sys.exit("build the extension first:\n  uv run maturin develop\nSee README.md.")


print("1. table provider")
ctx = SessionContext()
ctx.register_table("numbers", MyTableProvider(1, 6, 1))
ctx.sql('SELECT "A" FROM numbers').show()

print("\n2. functions")
ctx.register_udf(udf(IsNullUDF()))
ctx.sql('SELECT "A", my_custom_is_null("A") AS is_null FROM numbers').show()

print("\n3. catalog provider")
ctx.register_catalog_provider("ffi_catalog", MyCatalogProvider())
ctx.sql("SELECT * FROM ffi_catalog.my_schema.my_table").show()

print("\n4. config extension")
config = MyConfig()
config = SessionConfig(
    {"datafusion.catalog.information_schema": "true"}
).with_extension(config)
config.set("my_config.baz_count", "42")
ctx2 = SessionContext(config)
ctx2.sql("SHOW my_config.baz_count;").show()

print("\n5. codec round-trip")
codec = MyLogicalExtensionCodec()
ctx3 = SessionContext().with_logical_extension_codec(codec)
ctx3.register_table("numbers", MyTableProvider(1, 4, 1))
plan = ctx3.sql('SELECT "A" FROM numbers').logical_plan()
blob = plan.to_bytes(ctx3)
restored = LogicalPlan.from_bytes(ctx3, blob)
ctx3.create_dataframe_from_logical_plan(restored).show()

print("\n6. the same bytes decoded a second time")
restored2 = LogicalPlan.from_bytes(ctx3, blob)
ctx3.create_dataframe_from_logical_plan(restored2).show()
