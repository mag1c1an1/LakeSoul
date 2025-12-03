// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Adpated from DataFusion 47.0.0

use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;
use futures::stream::{Fuse, StreamExt};
use std::task::{Context, Poll, ready};

pub mod default_column;
pub mod empty_schema;
pub mod field_cursor;
pub mod projection;
pub mod row_cursor;

/// A new type wrapper around a set of fused [`SendableRecordBatchStream`]
/// that implements debug, and skips over empty [`RecordBatch`]
struct FusedStream(Fuse<SendableRecordBatchStream>);

impl std::fmt::Debug for FusedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedStream").finish()
    }
}

impl FusedStream {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match ready!(self.0.poll_next_unpin(cx)) {
                Some(Ok(b)) if b.num_rows() == 0 => continue,
                r => return Poll::Ready(r),
            }
        }
    }
}
