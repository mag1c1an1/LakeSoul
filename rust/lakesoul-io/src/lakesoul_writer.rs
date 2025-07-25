// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul Writer Module
//!
//! This module provides functionality for writing data to LakeSoul tables.
//! It supports writing data to Parquet files and includes features like
//! dynamic partitioning, sorting.
//!
//! # Examples
//!
//! ```rust
//! use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;
//! use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
//!
//! let config = LakeSoulIOConfigBuilder::new()
//!     .with_files(vec!["path/to/file.parquet"])
//!     .build();   
//!
//! let runtime = tokio::runtime::Runtime::new().unwrap();
//! let mut writer = SyncSendableMutableLakeSoulWriter::try_new(config, runtime).unwrap();
//! writer.write_batch(record_batch).unwrap();
//! writer.flush_and_close().unwrap();
//! ```
use std::borrow::Borrow;
use std::collections::HashMap;

use arrow::array::{Array as OtherArray, ListArray};
use arrow::datatypes::{DataType, Field};
use arrow_array::RecordBatch;
use arrow_schema::{SchemaBuilder, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use rand::distr::SampleString;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use crate::async_writer::{
    AsyncBatchWriter, MultiPartAsyncWriter, PartitioningAsyncWriter, SortAsyncWriter,
    WriterFlushResult,
};
use crate::helpers::{get_batch_memory_size, get_file_exist_col};
use crate::lakesoul_io_config::{IOSchema, LakeSoulIOConfig};
use crate::local_sensitive_hash::LSH;
use crate::transform::uniform_schema;

pub type SendableWriter = Box<dyn AsyncBatchWriter + Send>;

pub async fn create_writer(
    config: LakeSoulIOConfig,
) -> Result<Box<dyn AsyncBatchWriter + Send>> {
    // if aux sort cols exist, we need to adjust the schema of final writer
    // to exclude all aux sort cols
    let writer_schema: SchemaRef = if !config.aux_sort_cols.is_empty() {
        let schema = config.target_schema.0.clone();
        // O(nm), n = number of target schema fields, m = number of aux sort cols
        let proj_indices = schema
            .fields
            .iter()
            .filter(|f| !config.aux_sort_cols.contains(f.name()))
            .map(|f| {
                schema.index_of(f.name().as_str()).map_err(|e| {
                    DataFusionError::ArrowError(
                        e,
                        Some(format!("Failed to find index of column: {}", f.name())),
                    )
                })
            })
            .collect::<Result<Vec<usize>>>()?;
        Arc::new(schema.project(proj_indices.borrow())?)
    } else {
        config.target_schema.0.clone()
    };

    let mut writer_config = config.clone();
    let writer: Box<dyn AsyncBatchWriter + Send> = if config.use_dynamic_partition {
        Box::new(PartitioningAsyncWriter::try_new(writer_config)?)
    } else if !writer_config.primary_keys.is_empty() && !writer_config.keep_ordering() {
        // sort primary key table
        writer_config.target_schema = IOSchema(uniform_schema(writer_schema));
        if writer_config.files.is_empty() && !writer_config.prefix().is_empty() {
            writer_config.files = vec![format!(
                "{}/part-{}_{:0>4}.parquet",
                writer_config.prefix(),
                rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 16),
                writer_config.hash_bucket_id()
            )];
        }
        let writer = MultiPartAsyncWriter::try_new(writer_config).await?;
        Box::new(SortAsyncWriter::try_new(writer, config)?)
    } else {
        // else multipart
        writer_config.target_schema = IOSchema(uniform_schema(writer_schema));
        if writer_config.files.is_empty() && !writer_config.prefix().is_empty() {
            writer_config.files = vec![format!(
                "{}/part-{}_{:0>4}.parquet",
                writer_config.prefix(),
                rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 16),
                writer_config.hash_bucket_id()
            )];
        }
        let writer = MultiPartAsyncWriter::try_new(writer_config).await?;
        Box::new(writer)
    };
    Ok(writer)
}

// inner is sort writer
// multipart writer
pub struct SyncSendableMutableLakeSoulWriter {
    runtime: Arc<Runtime>,
    schema: SchemaRef,
    config: LakeSoulIOConfig,
    /// The in-progress file writer if any
    in_progress: Option<Arc<Mutex<SendableWriter>>>,
    flush_results: WriterFlushResult,
    lsh_computers: HashMap<String, LSH>,
}

impl SyncSendableMutableLakeSoulWriter {
    pub fn try_new(config: LakeSoulIOConfig, runtime: Runtime) -> Result<Self> {
        let runtime = Arc::new(runtime);
        runtime.clone().block_on(async move {
            let mut config = config.clone();
            let writer_config = config.clone();

            // Initialize HashMap instead of Vec
            let mut lsh_computers = HashMap::new();

            if config.compute_lsh() {
                let mut target_schema_builder =
                    SchemaBuilder::from(&config.target_schema().fields);

                for field in config.target_schema().fields.iter() {
                    if let Some(lsh_bit_width) = field.metadata().get("lsh_bit_width") {
                        let lsh_column_name = format!("{}_LSH", field.name());
                        let lsh_column = Arc::new(Field::new(
                            lsh_column_name.clone(),
                            DataType::List(Arc::new(Field::new(
                                "element",
                                DataType::Int64,
                                true,
                            ))),
                            true,
                        ));
                        target_schema_builder.try_merge(&lsh_column)?;

                        // Store LSH computer with column name as key
                        let lsh = LSH::new(
                            lsh_bit_width.parse().unwrap(),
                            field
                                .metadata()
                                .get("lsh_embedding_dimension")
                                .map(|d| d.parse().unwrap())
                                .unwrap_or(0),
                            config.seed,
                        );
                        lsh_computers.insert(field.name().to_string(), lsh);
                    }
                }
                config.target_schema = IOSchema(Arc::new(target_schema_builder.finish()));
            }

            let writer = create_writer(writer_config).await?;
            let schema = writer.schema();

            if let Some(max_file_size) = config.max_file_size_option() {
                config.max_file_size = Some(max_file_size);
            }

            if let Some(mem_limit) = config.mem_limit() {
                if config.use_dynamic_partition {
                    config.max_file_size = Some((mem_limit as f64 * 0.15) as u64);
                } else if !config.primary_keys.is_empty() && !config.keep_ordering() {
                    config.max_file_size = Some((mem_limit as f64 * 0.2) as u64);
                }
            }

            Ok(SyncSendableMutableLakeSoulWriter {
                in_progress: Some(Arc::new(Mutex::new(writer))),
                runtime,
                schema,
                config,
                flush_results: vec![],
                lsh_computers,
            })
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn config(&self) -> &LakeSoulIOConfig {
        &self.config
    }

    // blocking method for writer record batch.
    // since the underlying multipart upload would accumulate buffers
    // and upload concurrently in background, we only need blocking method here
    // for ffi callers
    pub fn write_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let runtime = self.runtime.clone();

        if record_batch.num_rows() == 0 {
            runtime.block_on(
                async move { self.write_batch_async(record_batch, false).await },
            )
        } else if self.config.compute_lsh() {
            let mut new_columns = record_batch.columns().to_vec();

            for (field_name, lsh_computer) in self.lsh_computers.iter() {
                let lsh_column_name = format!("{}_LSH", field_name);

                if let Some(array) = record_batch.column_by_name(field_name) {
                    let embedding =
                        array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Column {} is not a ListArray",
                                field_name
                            ))
                        })?;

                    let lsh_array = lsh_computer.compute_lsh(&Some(embedding.clone()))?;

                    if let Ok(index) =
                        record_batch.schema().index_of(lsh_column_name.as_str())
                    {
                        new_columns[index] = Arc::new(lsh_array);
                    }
                }
            }

            let new_record_batch =
                RecordBatch::try_new(self.config.target_schema(), new_columns)?;

            runtime.block_on(async move {
                self.write_batch_async(new_record_batch, false).await
            })
        } else {
            runtime.block_on(
                async move { self.write_batch_async(record_batch, false).await },
            )
        }
    }

    #[async_recursion::async_recursion(?Send)]
    async fn write_batch_async(
        &mut self,
        record_batch: RecordBatch,
        do_spill: bool,
    ) -> Result<()> {
        debug!(record_batch_row=?record_batch.num_rows(), do_spill=?do_spill, "write_batch_async");
        let config = self.config().clone();
        if let Some(max_file_size) = self.config().max_file_size {
            // if max_file_size is set, we need to split batch into multiple files
            let in_progress_writer = match &mut self.in_progress {
                Some(writer) => writer,
                x => x.insert(Arc::new(Mutex::new(create_writer(config).await?))),
            };
            let mut guard = in_progress_writer.lock().await;

            let batch_memory_size = get_batch_memory_size(&record_batch)? as u64;
            let batch_rows = record_batch.num_rows() as u64;
            // If exceeds max_file_size, split batch
            if !do_spill && guard.buffered_size() + batch_memory_size > max_file_size {
                let to_write = (batch_rows * (max_file_size - guard.buffered_size()))
                    / batch_memory_size;
                if to_write + 1 < batch_rows {
                    let to_write = to_write as usize + 1;
                    let a = record_batch.slice(0, to_write);
                    let b =
                        record_batch.slice(to_write, record_batch.num_rows() - to_write);
                    drop(guard);
                    self.write_batch_async(a, true).await?;
                    return self.write_batch_async(b, false).await;
                }
            }
            let rb_schema = record_batch.schema();
            guard.write_record_batch(record_batch).await.map_err(|e| {
                DataFusionError::Internal(format!(
                    "err={}, config={:?}, batch_schema={:?}",
                    e,
                    self.config.clone(),
                    rb_schema
                ))
            })?;

            if do_spill {
                drop(guard);
                if let Some(writer) = self.in_progress.take() {
                    let inner_writer = match Arc::try_unwrap(writer) {
                        Ok(inner) => inner,
                        Err(_) => {
                            return Err(DataFusionError::Internal(
                                "Cannot get ownership of inner writer".to_string(),
                            ));
                        }
                    };
                    let writer = inner_writer.into_inner();
                    let results = writer.flush_and_close().await.map_err(|e| {
                        DataFusionError::Internal(format!(
                            "err={}, config={:?}, batch_schema={:?}",
                            e,
                            self.config.clone(),
                            rb_schema
                        ))
                    })?;
                    self.flush_results.extend(results);
                }
            }
            Ok(())
        } else if let Some(inner_writer) = &self.in_progress {
            let inner_writer = inner_writer.clone();
            let mut writer = inner_writer.lock().await;
            writer.write_record_batch(record_batch).await
        } else {
            Err(DataFusionError::Internal(
                "Invalid state of inner writer".to_string(),
            ))
        }
    }

    pub fn flush_and_close(self) -> Result<Vec<u8>> {
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = match Arc::try_unwrap(inner_writer) {
                Ok(inner) => inner,
                Err(_) => {
                    return Err(DataFusionError::Internal(
                        "Cannot get ownership of the inner writer".to_string(),
                    ));
                }
            };
            let runtime = self.runtime;
            runtime.block_on(async move {
                let writer = inner_writer.into_inner();

                let mut grouped_results: HashMap<String, Vec<String>> = HashMap::new();
                let results = writer.flush_and_close().await.map_err(|e| {
                    DataFusionError::Internal(format!(
                        "err={}, config={:?}",
                        e,
                        self.config.clone()
                    ))
                })?;
                for (partition_desc, file, object_meta, metadata) in
                    self.flush_results.into_iter().chain(results)
                {
                    let encoded = format!(
                        "{}\x03{}\x03{}",
                        file,
                        object_meta.size,
                        get_file_exist_col(&metadata)
                    );
                    match grouped_results.get_mut(&partition_desc) {
                        Some(files) => {
                            files.push(encoded);
                        }
                        None => {
                            grouped_results.insert(partition_desc, vec![encoded]);
                        }
                    }
                }
                let mut summary = format!("{}", grouped_results.len());
                for (partition_desc, files) in grouped_results.iter() {
                    summary += "\x01";
                    summary += partition_desc.as_str();
                    summary += "\x02";
                    summary += files.join("\x02").as_str();
                }
                Ok(summary.into_bytes())
            })
        } else {
            Ok(vec![])
        }
    }

    pub fn abort_and_close(self) -> Result<()> {
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = match Arc::try_unwrap(inner_writer) {
                Ok(inner) => inner,
                Err(_) => {
                    return Err(DataFusionError::Internal(
                        "Cannot get ownership of inner writer".to_string(),
                    ));
                }
            };
            let runtime = self.runtime;
            runtime.block_on(async move {
                let writer = inner_writer.into_inner();
                writer.abort_and_close().await
            })
        } else {
            Ok(())
        }
    }

    pub fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        lakesoul_io_config::{LakeSoulIOConfigBuilder, OPTION_KEY_MEM_LIMIT},
        lakesoul_reader::LakeSoulReader,
        lakesoul_writer::{
            AsyncBatchWriter, MultiPartAsyncWriter, SyncSendableMutableLakeSoulWriter,
        },
    };
    use rand::distr::SampleString;

    use arrow::{
        array::{ArrayRef, Int64Array},
        record_batch::RecordBatch,
    };
    use arrow_array::{Array, StringArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::error::Result;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use rand::Rng;
    use std::{fs::File, sync::Arc};
    use tokio::{runtime::Builder, time::Instant};
    use tracing_subscriber::layer::SubscriberExt;

    use super::SortAsyncWriter;
    use arrow::array::{Date32Array, Decimal128Array, TimestampMicrosecondArray};
    use chrono::{NaiveDate, NaiveDateTime};

    #[test]
    fn test_parquet_async_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let col = Arc::new(Int64Array::from_iter_values([3, 2, 1])) as ArrayRef;
            let to_write = RecordBatch::try_from_iter([("col", col)])?;
            let temp_dir = tempfile::tempdir()?;
            let path = temp_dir
                .into_path()
                .join("test.parquet")
                .into_os_string()
                .into_string()
                .unwrap();
            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                .with_thread_num(2)
                .with_batch_size(256)
                .with_max_row_group_size(2)
                .with_schema(to_write.schema())
                .build();
            let mut async_writer = MultiPartAsyncWriter::try_new(writer_conf).await?;
            async_writer.write_record_batch(to_write.clone()).await?;
            Box::new(async_writer).flush_and_close().await?;

            let file = File::open(path.clone())?;
            let mut record_batch_reader =
                ParquetRecordBatchReader::try_new(file, 1024).unwrap();

            let actual_batch = record_batch_reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");

            assert_eq!(to_write.schema(), actual_batch.schema());
            assert_eq!(to_write.num_columns(), actual_batch.num_columns());
            assert_eq!(to_write.num_rows(), actual_batch.num_rows());
            for i in 0..to_write.num_columns() {
                let expected_data = to_write.column(i).to_data();
                let actual_data = actual_batch.column(i).to_data();

                assert_eq!(expected_data, actual_data);
            }

            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                .with_thread_num(2)
                .with_batch_size(256)
                .with_max_row_group_size(2)
                .with_schema(to_write.schema())
                .with_primary_keys(vec!["col".to_string()])
                .build();

            let async_writer = MultiPartAsyncWriter::try_new(writer_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, writer_conf)?;
            async_writer.write_record_batch(to_write.clone()).await?;
            Box::new(async_writer).flush_and_close().await?;

            let file = File::open(path)?;
            let mut record_batch_reader =
                ParquetRecordBatchReader::try_new(file, 1024).unwrap();

            let actual_batch = record_batch_reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");

            let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
            let to_read = RecordBatch::try_from_iter([("col", col)])?;
            assert_eq!(to_read.schema(), actual_batch.schema());
            assert_eq!(to_read.num_columns(), actual_batch.num_columns());
            assert_eq!(to_read.num_rows(), actual_batch.num_rows());
            for i in 0..to_read.num_columns() {
                let expected_data = to_read.column(i).to_data();
                let actual_data = actual_batch.column(i).to_data();

                assert_eq!(expected_data, actual_data);
            }
            Ok(())
        })
    }

    #[test]
    fn test_parquet_async_write_with_aux_sort() -> Result<()> {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let col = Arc::new(Int64Array::from_iter_values([3, 2, 3])) as ArrayRef;
        let col1 = Arc::new(Int64Array::from_iter_values([5, 3, 2])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from_iter_values([3, 2, 1])) as ArrayRef;
        let to_write =
            RecordBatch::try_from_iter([("col", col), ("col1", col1), ("col2", col2)])?;
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir
            .into_path()
            .join("test.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            .with_thread_num(2)
            .with_batch_size(256)
            .with_max_row_group_size(2)
            .with_schema(to_write.schema())
            .with_primary_keys(vec!["col".to_string()])
            .with_aux_sort_column("col2".to_string())
            .build();

        let mut writer =
            SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;
        writer.write_batch(to_write.clone())?;
        writer.flush_and_close()?;

        let file = File::open(path.clone())?;
        let mut record_batch_reader =
            ParquetRecordBatchReader::try_new(file, 1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");
        let col = Arc::new(Int64Array::from_iter_values([2, 3, 3])) as ArrayRef;
        let col1 = Arc::new(Int64Array::from_iter_values([3, 2, 5])) as ArrayRef;
        let to_read = RecordBatch::try_from_iter([("col", col), ("col1", col1)])?;

        assert_eq!(to_read.schema(), actual_batch.schema());
        assert_eq!(to_read.num_columns(), actual_batch.num_columns());
        assert_eq!(to_read.num_rows(), actual_batch.num_rows());
        for i in 0..to_read.num_columns() {
            let expected_data = to_read.column(i).to_data();
            let actual_data = actual_batch.column(i).to_data();

            assert_eq!(expected_data, actual_data);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_s3_read_write() -> Result<()> {
        let common_conf_builder = LakeSoulIOConfigBuilder::new()
            .with_thread_num(2)
            .with_batch_size(8192)
            .with_max_row_group_size(250000)
            .with_object_store_option(
                "fs.s3a.access.key".to_string(),
                "minioadmin1".to_string(),
            )
            .with_object_store_option(
                "fs.s3a.secret.key".to_string(),
                "minioadmin1".to_string(),
            )
            .with_object_store_option(
                "fs.s3a.endpoint".to_string(),
                "http://localhost:9000".to_string(),
            );

        let read_conf = common_conf_builder
            .clone()
            .with_files(vec![
                "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet"
                    .to_string(),
            ])
            .build();
        let mut reader = LakeSoulReader::new(read_conf)?;
        reader.start().await?;

        let schema = reader.schema.clone().unwrap();

        let write_conf = common_conf_builder
            .clone()
            .with_files(vec![
                "s3://lakesoul-test-bucket/data/native-io-test/large_file_written.parquet".to_string(),
            ])
            .with_schema(schema)
            .build();
        let mut async_writer = MultiPartAsyncWriter::try_new(write_conf).await?;

        while let Some(rb) = reader.next_rb().await {
            let rb = rb?;
            async_writer.write_record_batch(rb).await?;
        }

        Box::new(async_writer).flush_and_close().await?;
        drop(reader);

        Ok(())
    }

    #[test]
    fn test_sort_spill_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let common_conf_builder = LakeSoulIOConfigBuilder::new()
                .with_thread_num(2)
                .with_batch_size(8192)
                .with_max_row_group_size(250000);
            let read_conf = common_conf_builder
                .clone()
                .with_files(vec!["large_file.snappy.parquet".to_string()])
                .with_schema(Arc::new(Schema::new(vec![
                    Arc::new(Field::new("uuid", DataType::Utf8, false)),
                    Arc::new(Field::new("ip", DataType::Utf8, false)),
                    Arc::new(Field::new("hostname", DataType::Utf8, false)),
                    Arc::new(Field::new("requests", DataType::Int64, false)),
                    Arc::new(Field::new("name", DataType::Utf8, false)),
                    Arc::new(Field::new("city", DataType::Utf8, false)),
                    Arc::new(Field::new("job", DataType::Utf8, false)),
                    Arc::new(Field::new("phonenum", DataType::Utf8, false)),
                ])))
                .build();
            let mut reader = LakeSoulReader::new(read_conf)?;
            reader.start().await?;

            let schema = reader.schema.clone().unwrap();

            let write_conf = common_conf_builder
                .clone()
                .with_files(vec![
                    "/home/chenxu/program/data/large_file_written.parquet".to_string(),
                ])
                .with_primary_key("uuid".to_string())
                .with_schema(schema)
                .build();
            let async_writer = MultiPartAsyncWriter::try_new(write_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, write_conf)?;

            while let Some(rb) = reader.next_rb().await {
                let rb = rb?;
                async_writer.write_record_batch(rb).await?;
            }

            Box::new(async_writer).flush_and_close().await?;
            drop(reader);

            Ok(())
        })
    }

    #[test]
    fn test_s3_read_sort_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let common_conf_builder = LakeSoulIOConfigBuilder::new()
                .with_thread_num(2)
                .with_batch_size(8192)
                .with_max_row_group_size(250000)
                .with_object_store_option("fs.s3a.access.key".to_string(), "minioadmin1".to_string())
                .with_object_store_option("fs.s3a.secret.key".to_string(), "minioadmin1".to_string())
                .with_object_store_option("fs.s3a.endpoint".to_string(), "http://localhost:9000".to_string());

            let read_conf = common_conf_builder
                .clone()
                .with_files(vec![
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet".to_string(),
                ])
                .build();
            let mut reader = LakeSoulReader::new(read_conf)?;
            reader.start().await?;

            let schema = reader.schema.clone().unwrap();

            let write_conf = common_conf_builder
                .clone()
                .with_files(vec![
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file_written_sorted.parquet".to_string(),
                ])
                .with_schema(schema)
                .with_primary_keys(vec!["str0".to_string(), "str1".to_string(), "int1".to_string()])
                .build();
            let async_writer = MultiPartAsyncWriter::try_new(write_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, write_conf)?;

            while let Some(rb) = reader.next_rb().await {
                let rb = rb?;
                async_writer.write_record_batch(rb).await?;
            }

            Box::new(async_writer).flush_and_close().await?;
            drop(reader);

            Ok(())
        })
    }

    fn create_batch(num_columns: usize, num_rows: usize, str_len: usize) -> RecordBatch {
        let mut rng = rand::rng();
        let mut len_rng = rand::rng();
        let iter = (0..num_columns)
            .map(|i| {
                (
                    format!("col_{}", i),
                    Arc::new(StringArray::from(
                        (0..num_rows)
                            .map(|_| {
                                rand::distr::Alphanumeric.sample_string(
                                    &mut rng,
                                    len_rng.random_range(str_len..str_len * 3),
                                )
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    true,
                )
            })
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    #[test]
    fn test_writer_of_large_columns() -> Result<()> {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let num_batch = 39;
        let num_rows = 100;
        let num_columns = 2000;
        let str_len = 4;

        let to_write = create_batch(num_columns, num_rows, str_len);
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir
            .into_path()
            .join("test.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            .with_thread_num(2)
            .with_batch_size(num_rows)
            .with_max_row_group_size(2000)
            // .with_max_row_group_num_values(4_00_000)
            .with_schema(to_write.schema())
            // .with_primary_keys(vec!["col".to_string()])
            // .with_aux_sort_column("col2".to_string())
            .build();

        let mut writer =
            SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;

        let start = Instant::now();
        for _ in 0..num_batch {
            let once_start = Instant::now();
            writer.write_batch(create_batch(num_columns, num_rows, str_len))?;
            println!(
                "write batch once cost: {}",
                once_start.elapsed().as_millis()
            );
        }
        let flush_start = Instant::now();
        writer.flush_and_close()?;
        println!("flush cost: {}", flush_start.elapsed().as_millis());
        println!(
            "num_batch={}, num_columns={}, num_rows={}, str_len={}, cost_mills={}",
            num_batch,
            num_columns,
            num_rows,
            str_len,
            start.elapsed().as_millis()
        );

        let file = File::open(path.clone())?;
        let mut record_batch_reader =
            ParquetRecordBatchReader::try_new(file, 100_000).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(to_write.schema(), actual_batch.schema());
        assert_eq!(num_columns, actual_batch.num_columns());
        assert_eq!(num_rows * num_batch, actual_batch.num_rows());
        Ok(())
    }

    #[cfg(feature = "dhat-heap")]
    #[global_allocator]
    static ALLOC: dhat::Alloc = dhat::Alloc;

    #[tracing::instrument]
    #[test]
    fn writer_profiling() -> Result<()> {
        use tracing_subscriber::fmt;

        tracing_subscriber::fmt::init();

        let subscriber = fmt::layer().event_format(
            fmt::format::Format::default()
                .with_level(true)
                .with_source_location(true)
                .with_file(true),
        );
        // .with_max_level(Level::TRACE);
        tracing_subscriber::registry().with(subscriber);

        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let num_batch = 100;
        let num_rows = 1000;
        let num_columns = 100;
        let str_len = 4;

        let to_write = create_batch(num_columns, num_rows, str_len);
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir
            .into_path()
            .join("test.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            .with_prefix(
                tempfile::tempdir()?
                    .into_path()
                    .into_os_string()
                    .into_string()
                    .unwrap(),
            )
            .with_thread_num(2)
            .with_batch_size(num_rows)
            // .with_max_row_group_size(2000)
            // .with_max_row_group_num_values(4_00_000)
            .with_schema(to_write.schema())
            .with_primary_keys(
                // (0..num_columns - 1)
                (0..3)
                    .map(|i| format!("col_{}", i))
                    .collect::<Vec<String>>(),
            )
            // .with_aux_sort_column("col2".to_string())
            .with_option(OPTION_KEY_MEM_LIMIT, format!("{}", 1024 * 1024 * 48))
            .set_dynamic_partition(true)
            .with_hash_bucket_num(4)
            // .with_max_file_size(1024 * 1024 * 32)
            .build();

        let mut writer =
            SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;

        let start = Instant::now();
        for _ in 0..num_batch {
            // let once_start = Instant::now();
            writer.write_batch(create_batch(num_columns, num_rows, str_len))?;
            // println!("write batch once cost: {}", once_start.elapsed().as_millis());
        }
        let flush_start = Instant::now();
        writer.flush_and_close()?;
        println!("flush cost: {}", flush_start.elapsed().as_millis());
        println!(
            "num_batch={}, num_columns={}, num_rows={}, str_len={}, cost_mills={}",
            num_batch,
            num_columns,
            num_rows,
            str_len,
            start.elapsed().as_millis()
        );

        // let file = File::open(path.clone())?;
        // let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 100_000).unwrap();

        // let actual_batch = record_batch_reader
        //     .next()
        //     .expect("No batch found")
        //     .expect("Unable to get batch");

        // assert_eq!(to_write.schema(), actual_batch.schema());
        // assert_eq!(num_columns, actual_batch.num_columns());
        // assert_eq!(num_rows * num_batch, actual_batch.num_rows());
        Ok(())
    }

    #[test]
    fn test_writer_with_complex_pk_types() -> Result<()> {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        // 创建测试数据
        let id1 = Arc::new(
            Decimal128Array::from_iter_values([
                123456000, // 1234.56000
            ])
            .with_data_type(DataType::Decimal128(10, 5)),
        ) as ArrayRef;

        let id2 =
            Arc::new(Date32Array::from_iter_values([
                NaiveDate::from_ymd_opt(2024, 2, 1)
                    .unwrap()
                    .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32,
            ])) as ArrayRef;

        let id3 = Arc::new(
            TimestampMicrosecondArray::from_iter_values([NaiveDateTime::parse_from_str(
                "2024-03-02 17:01:01.123456",
                "%Y-%m-%d %H:%M:%S.%f",
            )
            .unwrap()
            .and_utc()
            .timestamp_micros()])
            .with_data_type(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            )),
        ) as ArrayRef;

        let v = Arc::new(StringArray::from_iter(vec![Some("value2")])) as ArrayRef;

        let row_kinds = Arc::new(StringArray::from_iter_values(["insert"])) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id1", DataType::Decimal128(10, 5), false),
            Field::new("id2", DataType::Date32, false),
            Field::new(
                "id3",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                false,
            ),
            Field::new("v", DataType::Utf8, true),
            Field::new("rowKinds", DataType::Utf8, false),
        ]));

        let to_write =
            RecordBatch::try_new(schema.clone(), vec![id1, id2, id3, v, row_kinds])?;

        let temp_dir = tempfile::tempdir()?;
        let prefix = temp_dir
            .path()
            .join("test_pk_types")
            .into_os_string()
            .into_string()
            .unwrap();

        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_prefix(format!("file://{}", prefix))
            .with_thread_num(2)
            .with_batch_size(10240)
            .with_schema(schema)
            .with_primary_keys(vec![
                "id1".to_string(),
                "id2".to_string(),
                "id3".to_string(),
            ])
            .with_hash_bucket_num(2)
            .set_dynamic_partition(true)
            .with_option(OPTION_KEY_MEM_LIMIT, format!("{}", 1024 * 1024 * 50))
            .build();

        let mut writer =
            SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;
        writer.write_batch(to_write.clone())?;
        let result = writer.flush_and_close()?;

        println!("result: {:?}", result);

        // 验证结果不为空
        assert!(result.len() > 1);

        Ok(())
    }
}
