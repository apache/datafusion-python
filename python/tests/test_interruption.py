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

"""Tests for handling interruptions in DataFusion operations."""

import threading
import time
import ctypes

import pytest
import pyarrow as pa
from datafusion import SessionContext


def test_collect_interrupted():
    """Test that a long-running query can be interrupted with Ctrl-C.
    
    This test simulates a Ctrl-C keyboard interrupt by raising a KeyboardInterrupt
    exception in the main thread during a long-running query execution.
    """
    # Create a context and a DataFrame with a query that will run for a while
    ctx = SessionContext()
    
    # Create a recursive computation that will run for some time
    batches = []
    for i in range(10):
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(list(range(i * 1000, (i + 1) * 1000))),
                pa.array([f"value_{j}" for j in range(i * 1000, (i + 1) * 1000)]),
            ],
            names=["a", "b"],
        )
        batches.append(batch)
    
    # Register tables
    ctx.register_record_batches("t1", [batches])
    ctx.register_record_batches("t2", [batches])
    
    # Create a large join operation that will take time to process
    df = ctx.sql("""
        WITH t1_expanded AS (
            SELECT 
                a, 
                b, 
                CAST(a AS DOUBLE) / 1.5 AS c,
                CAST(a AS DOUBLE) * CAST(a AS DOUBLE) AS d
            FROM t1
            CROSS JOIN (SELECT 1 AS dummy FROM t1 LIMIT 5)
        ),
        t2_expanded AS (
            SELECT 
                a,
                b,
                CAST(a AS DOUBLE) * 2.5 AS e,
                CAST(a AS DOUBLE) * CAST(a AS DOUBLE) * CAST(a AS DOUBLE) AS f
            FROM t2
            CROSS JOIN (SELECT 1 AS dummy FROM t2 LIMIT 5)
        )
        SELECT 
            t1.a, t1.b, t1.c, t1.d, 
            t2.a AS a2, t2.b AS b2, t2.e, t2.f
        FROM t1_expanded t1
        JOIN t2_expanded t2 ON t1.a % 100 = t2.a % 100
        WHERE t1.a > 100 AND t2.a > 100
    """)
    
    # Flag to track if the query was interrupted
    interrupted = False
    interrupt_error = None
    main_thread = threading.main_thread()
    
    # This function will be run in a separate thread and will raise 
    # KeyboardInterrupt in the main thread
    def trigger_interrupt():
        """Wait a moment then raise KeyboardInterrupt in the main thread"""
        time.sleep(0.5)  # Give the query time to start
        
        # Use ctypes to raise exception in main thread
        exception = ctypes.py_object(KeyboardInterrupt)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(main_thread.ident), exception)
        if res != 1:
            # If res is 0, the thread ID was invalid
            # If res > 1, we modified multiple threads
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(main_thread.ident), ctypes.py_object(0))
            raise RuntimeError("Failed to raise KeyboardInterrupt in main thread")
    
    # Start a thread to trigger the interrupt
    interrupt_thread = threading.Thread(target=trigger_interrupt)
    interrupt_thread.daemon = True
    interrupt_thread.start()
    
    # Execute the query and expect it to be interrupted
    try:
        df.collect()
    except KeyboardInterrupt:
        interrupted = True
    except Exception as e:
        interrupt_error = e
    
    # Assert that the query was interrupted properly
    assert interrupted, "Query was not interrupted by KeyboardInterrupt"
    assert interrupt_error is None, f"Unexpected error occurred: {interrupt_error}"
    
    # Make sure the interrupt thread has finished
    interrupt_thread.join(timeout=1.0)
