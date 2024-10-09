use std::{
    ffi::{c_char, c_int, c_void, CString},
    ptr::addr_of,
};

use arrow::array::Array;
use arrow::{
    array::StructArray,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::FFI_ArrowArrayStream,
};
use datafusion::execution::SendableRecordBatchStream;
use futures::{executor::block_on, TryStreamExt};

pub fn record_batch_to_arrow_stream(stream: SendableRecordBatchStream) -> FFI_ArrowArrayStream {
    let private_data = Box::new(RecoredBatchStreamPrivateData {
        stream,
        last_error: None,
    });

    FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release_stream),
        private_data: Box::into_raw(private_data) as *mut c_void,
    }
}

struct RecoredBatchStreamPrivateData {
    stream: SendableRecordBatchStream,
    last_error: Option<CString>,
}

const ENOMEM: i32 = 12;
const EIO: i32 = 5;
const EINVAL: i32 = 22;
const ENOSYS: i32 = 78;

// callback used to drop [FFI_ArrowArrayStream] when it is exported.
unsafe extern "C" fn release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let stream = &mut *stream;

    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;

    let private_data = Box::from_raw(stream.private_data as *mut RecoredBatchStreamPrivateData);
    drop(private_data);

    stream.release = None;
}

// The callback used to get array schema
unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    schema: *mut FFI_ArrowSchema,
) -> c_int {
    ExportedRecordBatchStream { stream }.get_schema(schema)
}

// The callback used to get next array
unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    array: *mut FFI_ArrowArray,
) -> c_int {
    ExportedRecordBatchStream { stream }.get_next(array)
}

// The callback used to get the error from last operation on the `FFI_ArrowArrayStream`
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let mut ffi_stream = ExportedRecordBatchStream { stream };
    // The consumer should not take ownership of this string, we should return
    // a const pointer to it.
    match ffi_stream.get_last_error() {
        Some(err_string) => err_string.as_ptr(),
        None => std::ptr::null(),
    }
}

struct ExportedRecordBatchStream {
    stream: *mut FFI_ArrowArrayStream,
}

impl ExportedRecordBatchStream {
    fn get_private_data(&mut self) -> &mut RecoredBatchStreamPrivateData {
        unsafe { &mut *((*self.stream).private_data as *mut RecoredBatchStreamPrivateData) }
    }

    pub fn get_schema(&mut self, out: *mut FFI_ArrowSchema) -> i32 {
        let private_data = self.get_private_data();
        let stream = &private_data.stream;

        let schema = FFI_ArrowSchema::try_from(stream.schema().as_ref());

        match schema {
            Ok(schema) => {
                unsafe { std::ptr::copy(addr_of!(schema), out, 1) };
                std::mem::forget(schema);
                0
            }
            Err(ref err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                1
            }
        }
    }

    pub fn get_next(&mut self, out: *mut FFI_ArrowArray) -> i32 {
        let private_data = self.get_private_data();

        let maybe_batch = block_on(private_data.stream.try_next());

        match maybe_batch {
            Ok(None) => {
                // Marks ArrowArray released to indicate reaching the end of stream.
                unsafe { std::ptr::write(out, FFI_ArrowArray::empty()) }
                0
            }
            Ok(Some(batch)) => {
                let struct_array = StructArray::from(batch);
                let array = FFI_ArrowArray::new(&struct_array.to_data());

                unsafe { std::ptr::write_unaligned(out, array) };
                0
            }
            Err(err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                1
            }
        }
    }

    pub fn get_last_error(&mut self) -> Option<&CString> {
        self.get_private_data().last_error.as_ref()
    }
}

// /// A `RecordBatchReader` which imports Arrays from `FFI_ArrowArrayStream`.
// /// Struct used to fetch `RecordBatch` from the C Stream Interface.
// /// Its main responsibility is to expose `RecordBatchReader` functionality
// /// that requires [FFI_ArrowArrayStream].
// #[derive(Debug)]
// pub struct ArrowArrayStreamReader {
//     stream: FFI_ArrowArrayStream,
//     schema: SchemaRef,
// }

// /// Gets schema from a raw pointer of `FFI_ArrowArrayStream`. This is used when constructing
// /// `ArrowArrayStreamReader` to cache schema.
// fn get_stream_schema(stream_ptr: *mut FFI_ArrowArrayStream) -> Result<SchemaRef> {
//     let mut schema = FFI_ArrowSchema::empty();

//     let ret_code = unsafe { (*stream_ptr).get_schema.unwrap()(stream_ptr, &mut schema) };

//     if ret_code == 0 {
//         let schema = Schema::try_from(&schema)?;
//         Ok(Arc::new(schema))
//     } else {
//         Err(ArrowError::CDataInterface(format!(
//             "Cannot get schema from input stream. Error code: {ret_code:?}"
//         )))
//     }
// }

// impl ArrowArrayStreamReader {
//     /// Creates a new `ArrowArrayStreamReader` from a `FFI_ArrowArrayStream`.
//     /// This is used to import from the C Stream Interface.
//     #[allow(dead_code)]
//     pub fn try_new(mut stream: FFI_ArrowArrayStream) -> Result<Self> {
//         if stream.release.is_none() {
//             return Err(ArrowError::CDataInterface(
//                 "input stream is already released".to_string(),
//             ));
//         }

//         let schema = get_stream_schema(&mut stream)?;

//         Ok(Self { stream, schema })
//     }

//     /// Creates a new `ArrowArrayStreamReader` from a raw pointer of `FFI_ArrowArrayStream`.
//     ///
//     /// Assumes that the pointer represents valid C Stream Interfaces.
//     /// This function copies the content from the raw pointer and cleans up it to prevent
//     /// double-dropping. The caller is responsible for freeing up the memory allocated for
//     /// the pointer.
//     ///
//     /// # Safety
//     ///
//     /// See [`FFI_ArrowArrayStream::from_raw`]
//     pub unsafe fn from_raw(raw_stream: *mut FFI_ArrowArrayStream) -> Result<Self> {
//         Self::try_new(FFI_ArrowArrayStream::from_raw(raw_stream))
//     }

//     /// Get the last error from `ArrowArrayStreamReader`
//     fn get_stream_last_error(&mut self) -> Option<String> {
//         let get_last_error = self.stream.get_last_error?;

//         let error_str = unsafe { get_last_error(&mut self.stream) };
//         if error_str.is_null() {
//             return None;
//         }

//         let error_str = unsafe { CStr::from_ptr(error_str) };
//         Some(error_str.to_string_lossy().to_string())
//     }
// }

// impl Iterator for ArrowArrayStreamReader {
//     type Item = Result<RecordBatch>;

//     fn next(&mut self) -> Option<Self::Item> {
//         let mut array = FFI_ArrowArray::empty();

//         let ret_code = unsafe { self.stream.get_next.unwrap()(&mut self.stream, &mut array) };

//         if ret_code == 0 {
//             // The end of stream has been reached
//             if array.is_released() {
//                 return None;
//             }

//             let result = unsafe {
//                 from_ffi_and_data_type(array, DataType::Struct(self.schema().fields().clone()))
//             };
//             Some(result.map(|data| RecordBatch::from(StructArray::from(data))))
//         } else {
//             let last_error = self.get_stream_last_error();
//             let err = ArrowError::CDataInterface(last_error.unwrap());
//             Some(Err(err))
//         }
//     }
// }

// impl RecordBatchReader for ArrowArrayStreamReader {
//     fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }
// }

// /// Exports a record batch reader to raw pointer of the C Stream Interface provided by the consumer.
// ///
// /// # Safety
// /// Assumes that the pointer represents valid C Stream Interfaces, both in memory
// /// representation and lifetime via the `release` mechanism.
// #[deprecated(note = "Use FFI_ArrowArrayStream::new")]
// pub unsafe fn export_reader_into_raw(
//     reader: Box<dyn RecordBatchReader + Send>,
//     out_stream: *mut FFI_ArrowArrayStream,
// ) {
//     let stream = FFI_ArrowArrayStream::new(reader);

//     std::ptr::write_unaligned(out_stream, stream);
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     use arrow_schema::Field;

//     use crate::array::Int32Array;
//     use crate::ffi::from_ffi;

//     struct TestRecordBatchReader {
//         schema: SchemaRef,
//         iter: Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
//     }

//     impl TestRecordBatchReader {
//         pub fn new(
//             schema: SchemaRef,
//             iter: Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
//         ) -> Box<TestRecordBatchReader> {
//             Box::new(TestRecordBatchReader { schema, iter })
//         }
//     }

//     impl Iterator for TestRecordBatchReader {
//         type Item = Result<RecordBatch>;

//         fn next(&mut self) -> Option<Self::Item> {
//             self.iter.next()
//         }
//     }

//     impl RecordBatchReader for TestRecordBatchReader {
//         fn schema(&self) -> SchemaRef {
//             self.schema.clone()
//         }
//     }

//     fn _test_round_trip_export(arrays: Vec<Arc<dyn Array>>) -> Result<()> {
//         let schema = Arc::new(Schema::new(vec![
//             Field::new("a", arrays[0].data_type().clone(), true),
//             Field::new("b", arrays[1].data_type().clone(), true),
//             Field::new("c", arrays[2].data_type().clone(), true),
//         ]));
//         let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
//         let iter = Box::new(vec![batch.clone(), batch.clone()].into_iter().map(Ok)) as _;

//         let reader = TestRecordBatchReader::new(schema.clone(), iter);

//         // Export a `RecordBatchReader` through `FFI_ArrowArrayStream`
//         let mut ffi_stream = FFI_ArrowArrayStream::new(reader);

//         // Get schema from `FFI_ArrowArrayStream`
//         let mut ffi_schema = FFI_ArrowSchema::empty();
//         let ret_code = unsafe { get_schema(&mut ffi_stream, &mut ffi_schema) };
//         assert_eq!(ret_code, 0);

//         let exported_schema = Schema::try_from(&ffi_schema).unwrap();
//         assert_eq!(&exported_schema, schema.as_ref());

//         // Get array from `FFI_ArrowArrayStream`
//         let mut produced_batches = vec![];
//         loop {
//             let mut ffi_array = FFI_ArrowArray::empty();
//             let ret_code = unsafe { get_next(&mut ffi_stream, &mut ffi_array) };
//             assert_eq!(ret_code, 0);

//             // The end of stream has been reached
//             if ffi_array.is_released() {
//                 break;
//             }

//             let array = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();

//             let record_batch = RecordBatch::from(StructArray::from(array));
//             produced_batches.push(record_batch);
//         }

//         assert_eq!(produced_batches, vec![batch.clone(), batch]);

//         Ok(())
//     }

//     fn _test_round_trip_import(arrays: Vec<Arc<dyn Array>>) -> Result<()> {
//         let schema = Arc::new(Schema::new(vec![
//             Field::new("a", arrays[0].data_type().clone(), true),
//             Field::new("b", arrays[1].data_type().clone(), true),
//             Field::new("c", arrays[2].data_type().clone(), true),
//         ]));
//         let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
//         let iter = Box::new(vec![batch.clone(), batch.clone()].into_iter().map(Ok)) as _;

//         let reader = TestRecordBatchReader::new(schema.clone(), iter);

//         // Import through `FFI_ArrowArrayStream` as `ArrowArrayStreamReader`
//         let stream = FFI_ArrowArrayStream::new(reader);
//         let stream_reader = ArrowArrayStreamReader::try_new(stream).unwrap();

//         let imported_schema = stream_reader.schema();
//         assert_eq!(imported_schema, schema);

//         let mut produced_batches = vec![];
//         for batch in stream_reader {
//             produced_batches.push(batch.unwrap());
//         }

//         assert_eq!(produced_batches, vec![batch.clone(), batch]);

//         Ok(())
//     }

//     #[test]
//     fn test_stream_round_trip_export() -> Result<()> {
//         let array = Int32Array::from(vec![Some(2), None, Some(1), None]);
//         let array: Arc<dyn Array> = Arc::new(array);

//         _test_round_trip_export(vec![array.clone(), array.clone(), array])
//     }

//     #[test]
//     fn test_stream_round_trip_import() -> Result<()> {
//         let array = Int32Array::from(vec![Some(2), None, Some(1), None]);
//         let array: Arc<dyn Array> = Arc::new(array);

//         _test_round_trip_import(vec![array.clone(), array.clone(), array])
//     }

//     #[test]
//     fn test_error_import() -> Result<()> {
//         let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

//         let iter = Box::new(vec![Err(ArrowError::MemoryError("".to_string()))].into_iter());

//         let reader = TestRecordBatchReader::new(schema.clone(), iter);

//         // Import through `FFI_ArrowArrayStream` as `ArrowArrayStreamReader`
//         let stream = FFI_ArrowArrayStream::new(reader);
//         let stream_reader = ArrowArrayStreamReader::try_new(stream).unwrap();

//         let imported_schema = stream_reader.schema();
//         assert_eq!(imported_schema, schema);

//         let mut produced_batches = vec![];
//         for batch in stream_reader {
//             produced_batches.push(batch);
//         }

//         // The results should outlive the lifetime of the stream itself.
//         assert_eq!(produced_batches.len(), 1);
//         assert!(produced_batches[0].is_err());

//         Ok(())
//     }
// }
