use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_python::dataframe::PyDataFrame;

#[test]
fn dataframe_into_view_returns_table_provider() {
    // Create an in-memory table with one Int32 column.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

    // Build a DataFrame from the table and convert it into a view.
    let ctx = SessionContext::new();
    let df = ctx.read_table(Arc::new(table)).unwrap();
    let py_df = PyDataFrame::new(df);
    let provider = py_df.into_view().unwrap();

    // Register the view in a new context and ensure it can be queried.
    let ctx = SessionContext::new();
    ctx.register_table("view", provider.into_inner()).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let batches = rt.block_on(async {
        let df = ctx.sql("SELECT * FROM view").await.unwrap();
        df.collect().await.unwrap()
    });

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}
