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

use crate::physical_plan::sorted_merge::cursor::{ArrayValues, CursorArray};
use crate::physical_plan::stream::FusedStream;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use datafusion_common::DataFusionError;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::PhysicalSortExpr;
use futures::Stream;
use futures::stream::StreamExt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// Specialized stream for sorts on single primitive column
pub struct FieldCursorStream<T: CursorArray> {
    /// The physical expressions to sort by
    sort: PhysicalSortExpr,
    /// Input streams
    stream: FusedStream,
    /// Create new reservations for each array
    reservation: MemoryReservation,
    phantom: PhantomData<fn(T) -> T>,
}

impl<T: CursorArray> std::fmt::Debug for FieldCursorStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCursorStream").finish()
    }
}

impl<T: CursorArray> FieldCursorStream<T> {
    pub fn new(
        sort: PhysicalSortExpr,
        stream: SendableRecordBatchStream,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            sort,
            stream: FusedStream(stream.fuse()),
            reservation,
            phantom: Default::default(),
        }
    }

    fn convert_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<ArrayValues<T::Values>, DataFusionError> {
        let value = self.sort.expr.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;
        let size_in_mem = array.get_buffer_memory_size();
        let array = array.as_any().downcast_ref::<T>().expect("field values");
        let mut array_reservation = self.reservation.new_empty();
        array_reservation.try_grow(size_in_mem)?;
        Ok(ArrayValues::new(
            self.sort.options,
            array,
            array_reservation,
        ))
    }
}

impl<T: CursorArray> Stream for FieldCursorStream<T> {
    type Item = Result<(ArrayValues<T::Values>, RecordBatch), DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(ready!(self.stream.poll_next(cx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch)?;
                Ok((cursor, batch))
            })
        }))
    }
}
