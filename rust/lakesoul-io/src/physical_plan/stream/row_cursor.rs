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
 
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr};
use futures::Stream;
use futures::stream::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use crate::physical_plan::sorted_merge::cursor::RowValues;
use crate::physical_plan::stream::FusedStream;

/// A [`PartitionedStream`] that wraps a [`SendableRecordBatchStream`]
/// and computes [`RowValues`] based on the provided [`PhysicalSortExpr`]
#[derive(Debug)]
pub struct RowCursorStream {
    /// Converter to convert output of physical expressions
    converter: RowConverter,
    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// Input streams
    stream: FusedStream,
    /// Tracks the memory used by `converter`
    reservation: MemoryReservation,
}

impl RowCursorStream {
    pub fn try_new(
        schema: &Schema,
        expressions: &LexOrdering,
        stream: SendableRecordBatchStream,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;

        let converter = RowConverter::new(sort_fields)?;
        Ok(Self {
            converter,
            reservation,
            column_expressions: expressions.iter().map(|x| Arc::clone(&x.expr)).collect(),
            stream: FusedStream(stream.fuse()),
        })
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<RowValues> {
        let cols = self
            .column_expressions
            .iter()
            .map(|expr| expr.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;

        let rows = self.converter.convert_columns(&cols)?;
        self.reservation.try_resize(self.converter.size())?;

        // track the memory in the newly created Rows.
        let mut rows_reservation = self.reservation.new_empty();
        rows_reservation.try_grow(rows.size())?;
        Ok(RowValues::new(rows, rows_reservation))
    }
}

impl Stream for RowCursorStream {
    type Item = Result<(RowValues, RecordBatch)>;

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
