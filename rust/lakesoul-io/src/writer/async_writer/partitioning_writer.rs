// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the partitioning writer, which repartitions the record batches by primary keys and range partitions before writing.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};

use datafusion_common::{DataFusionError, HashMap};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_physical_expr::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
    expressions::{Column, col},
};
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, projection::ProjectionExec,
    sorts::sort::SortExec, stream::RecordBatchReceiverStreamBuilder,
};
use parking_lot::RwLock;
use rand::distr::SampleString;
use rootcause::{Report, bail, report};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

use crate::{
    config::{IOConfigBuilder, IOConfigRef, IOSchema},
    helpers::{
        columnar_values_to_partition_desc, columnar_values_to_sub_path,
        get_batch_memory_size, get_columnar_values,
    },
    physical_plan::{
        repartition::RepartitionByRangeAndHashExec,
        sorted_merge::self_incremental_index_column::SelfIncrementalIndexColumnExec,
    },
    transform::uniform_schema,
    writer::async_writer::{
        AsyncBatchWriter, FlushOutputVec, FlusthOutput, MultiPartAsyncWriter,
        ReceiverStreamExec,
    },
};

type NestedFlushOutputResult = Result<JoinSet<Result<FlushOutputVec, Report>>, Report>;
/// Wrap the above async writer with a RepartitionExec to
/// dynamic repartitioning the batches before write to async writer
pub struct PartitioningAsyncWriter {
    /// The schema of the partitioning writer.
    schema: SchemaRef,
    /// The sender to async multi-part file writer.
    sorter_sender: Sender<Result<RecordBatch, DataFusionError>>,
    /// The partitioning execution plan of the partitioning writer for repartitioning.
    partitioning_exec: Arc<dyn ExecutionPlan>,
    /// The join handle of the partitioning execution plan.
    spawned_task: Option<SpawnedTask<Result<FlushOutputVec, Report>>>,
    /// The external error of the partitioning execution plan.
    err: Option<Report>,
    /// The buffered size of the partitioning writer.
    buffered_size: u64,
}
impl PartitioningAsyncWriter {
    pub fn try_new(config: IOConfigRef) -> Result<Self, Report> {
        let (writer_config, range_partitions, schema, partitioning_exec, tx) = {
            let conf = config.read();
            let schema = conf.target_schema.0.clone();
            let receiver_stream_builder = RecordBatchReceiverStreamBuilder::new(
                schema.clone(),
                conf.receiver_capacity,
            );
            let tx = receiver_stream_builder.tx();
            let recv_exec =
                ReceiverStreamExec::new(receiver_stream_builder, schema.clone());

            let partitioning_exec = PartitioningAsyncWriter::create_partitioning_exec(
                recv_exec,
                config.clone(),
            )?;

            // launch one async task per *input* partition

            // TODO need this clone?
            let mut writer_config = conf.clone();

            if !conf.aux_sort_cols.is_empty() {
                let schema = conf.target_schema.0.clone();
                // O(nm), n = number of target schema fields, m = number of aux sort cols
                let proj_indices = schema
                    .fields
                    .iter()
                    .filter(|f| !conf.aux_sort_cols.contains(f.name()))
                    .map(|f| {
                        schema
                            .index_of(f.name().as_str())
                            .map_err(|e| DataFusionError::ArrowError(e, None))
                    })
                    .collect::<Result<Vec<usize>, DataFusionError>>()?;
                let writer_schema = Arc::new(schema.project(&proj_indices)?);
                writer_config.target_schema = IOSchema(uniform_schema(writer_schema));
            }
            (
                Arc::new(RwLock::new(writer_config)),
                conf.range_partitions.clone(),
                schema,
                partitioning_exec,
                tx,
            )
        };

        let mut spawned_tasks = vec![];

        let write_id = rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 16);
        for i in 0..partitioning_exec.output_partitioning().partition_count() {
            let sink_task = SpawnedTask::spawn(Self::pull_and_sink(
                partitioning_exec.clone(),
                i,
                writer_config.clone(),
                Arc::new(range_partitions.clone()), // TODO clone and arc
                write_id.clone(),
            ));

            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            spawned_tasks.push(sink_task);
        }

        let spawned_task = SpawnedTask::spawn(Self::await_and_summary(spawned_tasks));

        Ok(Self {
            schema,
            sorter_sender: tx,
            partitioning_exec,
            spawned_task: Some(spawned_task),
            err: None,
            buffered_size: 0,
        })
    }

    fn create_partitioning_exec(
        input: ReceiverStreamExec,
        config: IOConfigRef,
    ) -> Result<Arc<dyn ExecutionPlan>, Report> {
        let conf = config.read();
        let mut aux_sort_cols = conf.aux_sort_cols.clone();
        let input: Arc<dyn ExecutionPlan> = if conf.stable_sort() {
            aux_sort_cols.push("__self_incremental_index__".to_string());
            info!(
                "input schema of self incremental index exec: {:?}",
                input.schema()
            );
            Arc::new(SelfIncrementalIndexColumnExec::new(Arc::new(input)))
        } else {
            Arc::new(input)
        };
        let input_schema = input.schema();
        let sort_exprs: Vec<PhysicalSortExpr> = conf
            .range_partitions
            .iter()
            .chain(conf.primary_keys.iter())
            // add aux sort cols to sort expr
            .chain(aux_sort_cols.iter())
            .map(|sort_column| {
                let col =
                    Column::new_with_schema(sort_column.as_str(), input_schema.as_ref())?;
                Ok(PhysicalSortExpr {
                    expr: Arc::new(col),
                    options: SortOptions::default(),
                })
            })
            .collect::<Result<Vec<PhysicalSortExpr>, Report>>()?;
        if sort_exprs.is_empty() {
            return Ok(input);
        }

        let sort_exec = Arc::new(SortExec::new(LexOrdering::new(sort_exprs), input));

        // see if we need to prune aux sort cols
        let sort_exec: Arc<dyn ExecutionPlan> = if aux_sort_cols.is_empty() {
            sort_exec
        } else {
            // O(nm), n = number of target schema fields, m = number of aux sort cols
            let proj_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = conf
                .target_schema
                .0
                .fields
                .iter()
                .filter_map(|f| {
                    if aux_sort_cols.contains(f.name()) {
                        // exclude aux sort cols
                        None
                    } else {
                        Some(
                            col(f.name().as_str(), &conf.target_schema.0)
                                .map(|e| (e, f.name().clone())),
                        )
                    }
                })
                .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, DataFusionError>>(
                )?;
            Arc::new(ProjectionExec::try_new(proj_expr, sort_exec)?)
        };

        let exec_plan = if conf.primary_keys.is_empty()
            && conf.range_partitions.is_empty()
        {
            sort_exec
        } else {
            let sorted_schema = sort_exec.schema();

            let range_partitioning_expr: Vec<Arc<dyn PhysicalExpr>> = conf
                .range_partitions
                .iter()
                .map(|col| {
                    let idx = sorted_schema.index_of(col.as_str())?;
                    Ok(Arc::new(Column::new(col.as_str(), idx)) as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<Vec<_>, Report>>()?;

            let hash_partitioning_expr: Vec<Arc<dyn PhysicalExpr>> = conf
                .primary_keys
                .iter()
                .map(|col| {
                    let idx = sorted_schema.index_of(col.as_str())?;
                    Ok(Arc::new(Column::new(col.as_str(), idx)) as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<Vec<_>, Report>>()?;
            let hash_partitioning =
                Partitioning::Hash(hash_partitioning_expr, conf.get_hash_bucket_num()?);

            Arc::new(RepartitionByRangeAndHashExec::try_new(
                sort_exec,
                range_partitioning_expr,
                hash_partitioning,
            )?)
        };

        Ok(exec_plan)
    }

    async fn pull_and_sink(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        config: IOConfigRef,
        range_partitions: Arc<Vec<String>>,
        write_id: String,
    ) -> Result<JoinSet<Result<FlushOutputVec, Report>>, Report> {
        let (config_builder, task_ctx) = {
            let guard = config.read();
            let task_ctx = guard.task_ctx.clone();
            (IOConfigBuilder::from(guard.clone()), task_ctx)
        };

        let mut data = input.execute(partition, task_ctx.clone())?;
        // O(nm), n = number of data fields, m = number of range partitions
        let schema_projection_excluding_range = data
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(
                |(idx, field)| match range_partitions.contains(field.name()) {
                    true => None,
                    false => Some(idx),
                },
            )
            .collect::<Vec<_>>();

        let mut err = None;

        let mut partitioned_writer = HashMap::<String, Box<MultiPartAsyncWriter>>::new();
        let mut flush_join_set = JoinSet::new();
        while let Some(batch_result) = data.next().await {
            match batch_result {
                Ok(batch) => {
                    debug!("write record_batch with {} rows", batch.num_rows());
                    let columnar_values =
                        get_columnar_values(&batch, range_partitions.clone())?;
                    let partition_desc =
                        columnar_values_to_partition_desc(&columnar_values);
                    let partition_sub_path =
                        columnar_values_to_sub_path(&columnar_values);
                    let batch_excluding_range =
                        batch.project(&schema_projection_excluding_range)?;

                    let file_absolute_path = format!(
                        "{}{}part-{}_{:0>4}.parquet",
                        config_builder.prefix(),
                        partition_sub_path,
                        write_id,
                        partition
                    );

                    if !partitioned_writer.contains_key(&partition_desc) {
                        let config = Arc::new(RwLock::new(
                            config_builder
                                .clone()
                                .with_files(vec![file_absolute_path])
                                .build(),
                        ));

                        let writer = MultiPartAsyncWriter::try_new(config).await?;
                        partitioned_writer
                            .insert(partition_desc.clone(), Box::new(writer));
                    }

                    if let Some(async_writer) =
                        partitioned_writer.get_mut(&partition_desc)
                    {
                        async_writer
                            .write_record_batch(batch_excluding_range)
                            .await?;
                    }
                }
                // received abort signal
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = err {
            for (_, writer) in partitioned_writer.into_iter() {
                match writer.abort_and_close().await {
                    Ok(_) => match e {
                        DataFusionError::Internal(ref err_msg)
                            if err_msg == "external abort" =>
                        {
                            debug!("External abort signal received")
                        }
                        _ => return Err(e.into()),
                    },
                    Err(abort_error) => {
                        return Err(abort_error);
                    }
                }
            }
            Ok(flush_join_set)
        } else {
            for (partition_desc, writer) in partitioned_writer.into_iter() {
                flush_join_set.spawn(async move {
                    let writer_flush_results = writer.flush_and_close().await?;
                    Ok(writer_flush_results
                        .into_iter()
                        .map(|mut other| -> FlusthOutput {
                            other.partition_desc = partition_desc.clone();
                            other
                        })
                        .collect::<Vec<_>>())
                });
            }
            Ok(flush_join_set)
        }
    }

    async fn await_and_summary(
        tasks: Vec<SpawnedTask<NestedFlushOutputResult>>,
    ) -> Result<FlushOutputVec, Report> {
        let mut flatten_results = Vec::new();
        for result in futures::future::join_all(tasks).await {
            let part_join_set = result??;
            for part_result in part_join_set.join_all().await {
                flatten_results.extend(part_result?);
            }
        }

        Ok(flatten_results)
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for PartitioningAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), Report> {
        if let Some(err) = &self.err {
            bail!("Already failed by {}", err)
        }

        let memory_size = get_batch_memory_size(&batch)? as u64;
        let send_result = self.sorter_sender.send(Ok(batch)).await;
        self.buffered_size += memory_size;
        match send_result {
            Ok(_) => Ok(()),
            // channel has been closed, indicating error happened during sort write
            Err(e) => {
                if let Some(join_handle) = self.spawned_task.take() {
                    let result = join_handle
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    self.err = result.err();
                    Err(report!("by {:?}", self.err))
                } else {
                    self.err = Some(e.into());
                    Err(report!("by {:?}", self.err))
                }
            }
        }
    }

    async fn flush_and_close(self: Box<Self>) -> Result<FlushOutputVec, Report> {
        if let Some(join_handle) = self.spawned_task {
            let sender = self.sorter_sender;
            drop(sender);
            join_handle.await?
        } else {
            bail!("aborted, cannot flush")
        }
    }

    async fn abort_and_close(self: Box<Self>) -> Result<(), Report> {
        if let Some(join_handle) = self.spawned_task {
            let sender = self.sorter_sender;
            // send abort signal to the task
            sender
                .send(Err(DataFusionError::Internal("external abort".to_string())))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(sender);
            let _ = join_handle
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(())
        } else {
            // previous error has already aborted writer
            Ok(())
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn buffered_size(&self) -> u64 {
        self.buffered_size
    }
}
