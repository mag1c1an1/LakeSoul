// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul IO Config Module
//!
//! This module provides functionality for configuring LakeSoul IO operations.
//! It includes configuration options for file paths, schema information, partitioning settings,
//! and performance tuning options.
//!
//!
// use std::{
//     collections::HashMap,
//     sync::{Arc, OnceLock},
//     time::Duration,
// };

use datafusion_common::config::ConfigOptions;
// use anyhow::anyhow;
// use arrow::error::ArrowError;
// use arrow_schema::{Schema, SchemaRef};
// use datafusion::error::{DataFusionError, Result};
// use datafusion::execution::SessionStateBuilder;
// use datafusion::execution::context::QueryPlanner;
// use datafusion::execution::memory_pool::FairSpillPool;
// use datafusion::execution::object_store::ObjectStoreUrl;
// use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_expr::Expr;
// use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
// use datafusion::optimizer::optimize_projections::OptimizeProjections;
// use datafusion::optimizer::push_down_filter::PushDownFilter;
// use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
// use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
// use datafusion::prelude::{SessionConfig, SessionContext};
// use datafusion_common::DataFusionError::External;
// use datafusion_substrait::substrait::proto::Plan;
// use derivative::Derivative;
// use object_store::aws::AmazonS3Builder;
// use object_store::{ClientOptions, RetryConfig};
// use tracing::debug;
// use url::{ParseError, Url};

// #[cfg(feature = "hdfs")]
// use crate::hdfs::Hdfs;

// use crate::lakesoul_cache::cache::DiskCache;
// use crate::lakesoul_cache::read_through::ReadThroughCache;

// static LAKESOUL_CACHE: OnceLock<Arc<DiskCache>> = OnceLock::new();
// /// Get and init Lakesoul Cache
// fn get_lakesoul_cache() -> Arc<DiskCache> {
//     LAKESOUL_CACHE
//         .get_or_init(|| -> Arc<DiskCache> {
//             let cache_size = {
//                 match std::env::var("LAKESOUL_CACHE_SIZE") {
//                     Ok(mut s) => {
//                         println!("LAKESOUL_CACHE_SIZE: {}", s);
//                         match s.split_off(s.len() - 3).as_str() {
//                             "KiB" => s.parse::<usize>().unwrap_or(1) * 1024,
//                             "MiB" => s.parse::<usize>().unwrap_or(1) * 1024 * 1024,
//                             "GiB" => {
//                                 println!("LAKESOUL_CACHE_SIZE: {}", s);
//                                 s.parse::<usize>().unwrap_or(1) * 1024 * 1024 * 1024
//                             }
//                             "TiB" => {
//                                 s.parse::<usize>().unwrap_or(1)
//                                     * 1024
//                                     * 1024
//                                     * 1024
//                                     * 1024
//                             }
//                             _ => {
//                                 println!("LAKESOUL_CACHE_SIZE: {}", s);
//                                 1024 * 1024 * 1024
//                             }
//                         }
//                     }
//                     Err(_) => 1024 * 1024 * 1024,
//                 }
//             };
//             println!("LAKESOUL_CACHE_SIZE: {}", cache_size);
//             Arc::new(DiskCache::new(cache_size, 4 * 1024 * 1024))
//         })
//         .clone()
// }

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use datafusion_execution::{
    TaskContext, config::SessionConfig, memory_pool::FairSpillPool,
    runtime_env::RuntimeEnvBuilder,
};
use educe::Educe;
use parking_lot::RwLock;
use rootcause::Report;

use crate::config::object_store::register_object_store;

mod object_store;
// mod session;

/// Key for keeping row order in output
pub static OPTION_KEY_KEEP_ORDERS: &str = "keep_orders";
/// Default value for keeping row order
pub static OPTION_DEFAULT_VALUE_KEEP_ORDERS: &str = "false";
/// Key for memory limit in bytes
pub static OPTION_KEY_MEM_LIMIT: &str = "mem_limit";
/// Key for memory pool size in bytes
pub static OPTION_KEY_POOL_SIZE: &str = "pool_size";
/// Key for hash bucket ID for partitioning
pub static OPTION_KEY_HASH_BUCKET_ID: &str = "hash_bucket_id";
/// Key for number of hash buckets for partitioning
pub static OPTION_KEY_HASH_BUCKET_NUM: &str = "hash_bucket_num";
/// Key for CDC (Change Data Capture) column name
pub static OPTION_KEY_CDC_COLUMN: &str = "cdc_column";
/// Key for indicating if data is compacted
pub static OPTION_KEY_IS_COMPACTED: &str = "is_compacted";
/// Key for skipping merge operation during read
pub static OPTION_KEY_SKIP_MERGE_ON_READ: &str = "skip_merge_on_read";
/// Key for maximum file size in bytes
pub static OPTION_KEY_MAX_FILE_SIZE: &str = "max_file_size";
/// Key for spill dir
pub static OPTION_KEY_SPILL_DIR: &str = "spill_dir";
/// Key for computing Local Sensitive Hash
pub static OPTION_KEY_COMPUTE_LSH: &str = "compute_lsh";
/// Key for using stable sort algorithm
pub static OPTION_KEY_STABLE_SORT: &str = "stable_sort";

/// Key for object store endpoint
pub static OPTION_KEY_OSS_ENDPOINT: &str = "fs.s3a.endpoint";
/// Key for object store bucket
pub static OPTION_KEY_OSS_BUCKET: &str = "fs.s3a.bucket";
/// Key for object store access key
pub static OPTION_KEY_OSS_ACCESS_KEY: &str = "fs.s3a.access.key";
/// Key for object store secret key
pub static OPTION_KEY_OSS_SECRET_KEY: &str = "fs.s3a.secret.key";
/// Key for object store region
pub static OPTION_KEY_OSS_REGION: &str = "fs.s3a.endpoint.region";
/// Key for object store path style access
pub static OPTION_KEY_PATH_STYLE_ACCESS: &str = "fs.s3a.path.style.access";
/// Key for object store signing algorithm
pub static OPTION_KEY_OSS_SIGNING_ALGORITHM: &str = "fs.s3a.s3.signing-algorithm";

#[derive(Debug, Clone)]
pub struct IOSchema(pub(crate) SchemaRef);

impl Default for IOSchema {
    fn default() -> Self {
        IOSchema(Arc::new(Schema::empty()))
    }
}

pub type IOConfigRef = Arc<RwLock<IOConfig>>;

#[derive(Debug, Educe, Clone)]
#[educe(Default)]
/// Configuration for LakeSoul IO operations.
///
/// This struct contains all the necessary parameters for configuring LakeSoul IO operations,
/// including file paths, schema information, partitioning settings, and performance tuning options.
pub struct IOConfig {
    /// Root directory path for files, unescaped
    pub(crate) prefix: String,
    /// List of file paths to read or write, unescaped
    pub(crate) files: Vec<String>,
    /// Names of primary key columns
    pub(crate) primary_keys: Vec<String>,
    /// Names of range partition columns
    pub(crate) range_partitions: Vec<String>,
    /// Number of hash buckets for hash partitioning
    #[educe(Default = "1")]
    pub(crate) hash_bucket_num: String,
    /// Names of columns to select
    #[deprecated(since = "2.5.0", note = "deprecated")]
    pub(crate) columns: Vec<String>,
    /// Names of auxiliary columns for sorting
    pub(crate) aux_sort_cols: Vec<String>,
    /// Encoded java filter predicates as strings
    pub(crate) filter_strs: Vec<String>,
    /// Filter predicates as expressions
    #[deprecated(since = "2.5.0", note = "deprecated")]
    pub(crate) filters: Vec<Expr>,
    // /// Filter predicates as substrait plans
    // pub(crate) filter_protos: Vec<Plan>,
    /// Filter predicates as substrait raw buf
    pub(crate) filter_buf: Vec<Vec<u8>>,
    /// Number of rows per batch for reading/writing
    #[educe(Default = 8192)]
    pub(crate) batch_size: usize,
    /// Maximum number of rows per row group when writing
    #[educe(Default = 250_000)]
    pub(crate) max_row_group_size: usize,
    /// Maximum number of values per row group when writing
    #[educe(Default = 2147483647)]
    pub(crate) max_row_group_num_values: usize,
    /// Number of batches to prefetch
    #[educe(Default = 1)]
    pub(crate) prefetch_size: usize,
    /// Whether to enable Parquet filter pushdown
    #[educe(Default = false)]
    pub(crate) parquet_filter_pushdown: bool,
    /// Target Arrow schema for the reader and writer
    pub(crate) target_schema: IOSchema,
    /// Arrow schema for partition columns
    pub(crate) partition_schema: IOSchema,
    /// Object store configuration options (e.g., S3 credentials)
    pub(crate) object_store_options: HashMap<String, String>,
    /// Merge operators for each column
    pub(crate) merge_operators: HashMap<String, String>,
    /// Default values for columns
    pub(crate) default_column_value: HashMap<String, String>,
    /// Number of threads for parallel processing
    #[educe(Default = 2)]
    pub(crate) thread_num: usize,
    /// Default filesystem URI (compatible with Hadoop's fs.defaultFS)
    pub(crate) default_fs: String,
    /// Additional configuration options
    pub(super) options: HashMap<String, String>,
    /// Whether to use dynamic partitioning
    #[educe(Default = false)]
    pub(crate) use_dynamic_partition: bool,
    /// Whether to infer schema from data
    #[educe(Default = false)]
    pub(crate) inferring_schema: bool,
    /// Memory limit for operations
    #[educe(Default = None)]
    pub(crate) memory_limit: Option<usize>,
    /// Size of memory pool
    #[educe(Default = None)]
    pub(crate) memory_pool_size: Option<usize>,
    /// Maximum file size in bytes
    #[educe(Default = None)]
    pub(crate) max_file_size: Option<u64>,
    /// Random number generator seed
    #[educe(Default = 1234)]
    pub(crate) seed: u64,
    /// Memory buffer capacity
    #[educe(Default = 16 * 1024)]
    pub(crate) memory_buffer_capacity: usize,
    /// Chunk size
    #[educe(Default=128 * 1024 * 1024)]
    pub(crate) multipart_chunk_size: usize,
    /// Receiver capacity
    #[educe(Default = 8)]
    pub(crate) receiver_capacity: usize,
    /// task ctx for ffi reader/writer
    pub(crate) task_ctx: Arc<TaskContext>,
}

impl IOConfig {
    /// Returns the target Arrow schema for the reader and writer
    pub fn target_schema(&self) -> SchemaRef {
        self.target_schema.0.clone()
    }

    /// Returns the Arrow schema for partition columns
    pub fn partition_schema(&self) -> SchemaRef {
        self.partition_schema.0.clone()
    }

    /// Returns a slice of primary key column names
    pub fn primary_keys_slice(&self) -> &[String] {
        &self.primary_keys
    }

    /// Returns a slice of range partition column names
    pub fn range_partitions_slice(&self) -> &[String] {
        &self.range_partitions
    }

    /// Returns a slice of file paths to read or write
    pub fn files_slice(&self) -> &[String] {
        &self.files
    }

    /// Returns a slice of auxiliary sort column names
    pub fn aux_sort_cols_slice(&self) -> &[String] {
        &self.aux_sort_cols
    }

    /// Returns the value of a configuration option by key
    pub fn option(&self, key: &str) -> Option<&String> {
        self.options.get(key)
    }

    /// Returns the root directory path for files
    pub fn prefix(&self) -> &String {
        &self.prefix
    }

    /// Returns whether to keep row order in output
    pub fn keep_ordering(&self) -> bool {
        self.option(OPTION_KEY_KEEP_ORDERS)
            .is_some_and(|x| x.eq("true"))
    }

    /// Returns the memory limit in bytes if set
    pub fn mem_limit(&self) -> Option<usize> {
        self.option(OPTION_KEY_MEM_LIMIT)
            .map(|x| x.parse().unwrap())
    }

    /// Returns the maximum file size in bytes if set
    pub fn max_file_size_option(&self) -> Option<u64> {
        self.option(OPTION_KEY_MAX_FILE_SIZE)
            .map(|x| x.parse().unwrap())
    }

    /// Returns the memory pool size in bytes if set
    pub fn pool_size(&self) -> Option<usize> {
        self.option(OPTION_KEY_POOL_SIZE)
            .map(|x| x.parse().unwrap())
    }

    /// Returns the hash bucket ID for partitioning (defaults to 0)
    pub fn hash_bucket_id(&self) -> usize {
        self.option(OPTION_KEY_HASH_BUCKET_ID)
            .map_or(0, |x| x.parse().unwrap())
    }

    /// Returns the number of hash buckets for partitioning (defaults to 1, equvalent to not partitioning)
    pub fn hash_bucket_num(&self) -> usize {
        self.option(OPTION_KEY_HASH_BUCKET_NUM)
            .map_or(1, |x| x.parse().unwrap())
    }

    // Get hash_bucket_num field directly, not from option
    pub fn get_hash_bucket_num(&self) -> Result<usize, Report> {
        let mut tmp = self.hash_bucket_num.parse::<isize>()?;
        tmp = tmp.max(1);
        Ok(tmp as usize)
    }

    /// Returns the CDC (Change Data Capture) column name if set
    pub fn cdc_column(&self) -> String {
        self.option(OPTION_KEY_CDC_COLUMN)
            .map_or_else(String::new, |x| x.to_string())
    }

    /// Returns whether the data is compacted, default is false
    pub fn is_compacted(&self) -> bool {
        self.option(OPTION_KEY_IS_COMPACTED)
            .is_some_and(|x| x.eq("true"))
    }

    /// Returns whether to skip merge operation during read, default is false
    pub fn skip_merge_on_read(&self) -> bool {
        self.option(OPTION_KEY_SKIP_MERGE_ON_READ)
            .is_some_and(|x| x.eq("true"))
    }

    /// Returns whether to compute Local Sensitive Hash (defaults to true)
    pub fn compute_lsh(&self) -> bool {
        self.option(OPTION_KEY_COMPUTE_LSH)
            .is_none_or(|x| x.eq("true"))
    }

    /// Returns whether to use stable sort algorithm
    pub fn stable_sort(&self) -> bool {
        self.option(OPTION_KEY_STABLE_SORT)
            .is_some_and(|x| x.eq("true"))
    }

    /// Return or init a task context
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx.clone()
    }
}

impl Into<SessionConfig> for &IOConfig {
    fn into(self) -> SessionConfig {
        todo!()
    }
}

#[derive(Debug, Clone)]
/// Builder for LakeSoulIOConfig
///
/// This struct provides a fluent builder interface for creating LakeSoulIOConfig instances.
/// It allows for setting various configuration options using method chaining.
pub struct IOConfigBuilder {
    config: IOConfig,
}

impl IOConfigBuilder {
    /// Creates a new LakeSoulIOConfigBuilder instance with default configuration
    ///
    /// # Returns
    ///
    /// A new LakeSoulIOConfigBuilder instance with default configuration
    pub fn new() -> Self {
        IOConfigBuilder {
            config: IOConfig::default(),
        }
    }

    /// Creates a new LakeSoulIOConfigBuilder instance with object store options
    ///
    /// # Arguments
    ///
    /// * `options` - A HashMap of string key-value pairs representing object store options
    ///
    pub fn new_with_object_store_options(options: HashMap<String, String>) -> Self {
        let mut builder = IOConfigBuilder::new();
        builder.config.object_store_options = options;
        builder
    }

    /// Sets the prefix for the file path
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the file path
    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.config.prefix = prefix;
        self
    }

    /// Adds a file path to the list of files to read or write
    ///
    /// # Arguments
    ///
    /// * `file` - The file path to add
    pub fn with_file(mut self, file: String) -> Self {
        self.config.files.push(file);
        self
    }

    /// Adds a list of file paths to the list of files to read or write
    ///
    /// # Arguments
    ///
    /// * `files` - The list of file paths to add
    pub fn with_files(mut self, files: Vec<impl ToString>) -> Self {
        let files = files.into_iter().map(|x| x.to_string()).collect();
        self.config.files = files;
        self
    }

    /// Adds a primary key to the list of primary keys
    ///
    /// # Arguments
    ///
    /// * `pks` - The primary key to add
    pub fn with_primary_key(mut self, pks: String) -> Self {
        self.config.primary_keys.push(pks);
        self
    }

    /// Adds a list of primary keys to the list of primary keys
    ///
    /// # Arguments
    ///
    /// * `pks` - The list of primary keys to add
    pub fn with_primary_keys(mut self, pks: Vec<String>) -> Self {
        self.config.primary_keys = pks;
        self
    }

    /// Adds a range partition to the list of range partitions
    ///
    /// # Arguments
    ///
    /// * `range_partition` - The range partition to add
    pub fn with_range_partition(mut self, range_partition: String) -> Self {
        self.config.range_partitions.push(range_partition);
        self
    }

    /// Adds a list of range partitions to the list of range partitions
    ///
    /// # Arguments
    ///
    /// * `range_partitions` - The list of range partitions to add
    pub fn with_range_partitions(mut self, range_partitions: Vec<String>) -> Self {
        self.config.range_partitions = range_partitions;
        self
    }

    /// Sets the number of hash buckets for partitioning
    ///
    /// # Arguments
    ///
    /// * `hash_bucket_num` - The number of hash buckets for partitioning
    pub fn with_hash_bucket_num(mut self, hash_bucket_num: String) -> Self {
        self.config.hash_bucket_num = hash_bucket_num;
        self
    }

    /// Adds a column to the list of columns to select
    ///
    /// # Arguments
    ///
    /// * `col` - The column to add
    #[deprecated(
        since = "2.5.0",
        note = "This method is deprecated. Use target_schema instead."
    )]
    #[allow(deprecated)]
    pub fn with_column(mut self, col: String) -> Self {
        self.config.columns.push(String::from(&col));
        self
    }

    /// Adds an auxiliary sort column to the list of auxiliary sort columns
    ///
    /// # Arguments
    ///
    /// * `col` - The auxiliary sort column to add
    pub fn with_aux_sort_column(mut self, col: String) -> Self {
        self.config.aux_sort_cols.push(String::from(&col));
        self
    }

    /// Sets the number of rows per batch for reading/writing
    ///
    /// # Arguments
    ///
    /// * `batch_size` - The number of rows per batch for reading/writing
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    /// Sets the maximum number of rows per row group when writing
    ///
    /// # Arguments
    ///
    /// * `max_row_group_size` - The maximum number of rows per row group when writing
    pub fn with_max_row_group_size(mut self, max_row_group_size: usize) -> Self {
        self.config.max_row_group_size = max_row_group_size;
        self
    }

    /// Sets the maximum number of values per row group when writing
    ///
    /// # Arguments
    ///
    /// * `max_row_group_num_values` - The maximum number of values per row group when writing
    pub fn with_max_row_group_num_values(
        mut self,
        max_row_group_num_values: usize,
    ) -> Self {
        self.config.max_row_group_num_values = max_row_group_num_values;
        self
    }

    /// Sets the number of batches to prefetch
    ///
    /// # Arguments
    ///
    /// * `prefetch_size` - The number of batches to prefetch
    pub fn with_prefetch_size(mut self, prefetch_size: usize) -> Self {
        self.config.prefetch_size = prefetch_size;
        self
    }

    /// Sets whether to enable Parquet filter pushdown
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to enable Parquet filter pushdown
    pub fn with_parquet_filter_pushdown(mut self, enable: bool) -> Self {
        self.config.parquet_filter_pushdown = enable;
        self
    }

    #[deprecated(
        since = "2.5.0",
        note = "This method is deprecated. Use target_schema instead."
    )]
    #[allow(deprecated)]
    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.config.columns = cols;
        self
    }

    /// Sets the target Arrow schema for the reader and writer
    ///
    /// # Arguments
    ///
    /// * `schema` - The target Arrow schema to set
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.config.target_schema = IOSchema(schema);
        self
    }

    /// Sets the Arrow schema for partition columns
    ///
    /// # Arguments
    ///
    /// * `schema` - The Arrow schema for partition columns to set
    pub fn with_partition_schema(mut self, schema: SchemaRef) -> Self {
        self.config.partition_schema = IOSchema(schema);
        self
    }

    /// Adds a filter string to the list of filter strings
    ///
    /// # Arguments
    ///
    /// * `filter_str` - The filter string to add
    pub fn with_filter_str(mut self, filter_str: String) -> Self {
        self.config.filter_strs.push(filter_str);
        self
    }

    // /// Adds a filter proto to the list of filter protos
    // ///
    // /// # Arguments
    // ///
    // /// * `filter_proto` - The filter proto to add
    // pub fn with_filter_proto(mut self, filter_proto: Plan) -> Self {
    //     self.config.filter_protos.push(filter_proto);
    //     self
    // }

    /// Adds a filter protobuf to the list of filter bufs
    ///
    /// # Arguments
    ///
    /// * `filter_buf` - The filter proto buf to add
    pub fn with_filter_buf(mut self, filter_buf: Vec<u8>) -> Self {
        self.config.filter_buf.push(filter_buf);
        self
    }

    #[deprecated(
        since = "2.5.0",
        note = "This method is deprecated. Use with_filter_str and with_filter_proto instead."
    )]
    #[allow(deprecated)]
    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.config.filters = filters;
        self
    }

    /// Adds a merge operator to the list of merge operators
    ///
    /// # Arguments
    ///
    /// * `field_name` - The field name to add the merge operator to
    /// * `merge_op` - The merge operator to add
    pub fn with_merge_op(mut self, field_name: String, merge_op: String) -> Self {
        self.config.merge_operators.insert(field_name, merge_op);
        self
    }

    /// Sets the default value for a column
    ///
    /// # Arguments
    ///
    /// * `field_name` - The field name to set the default value for
    /// * `value` - The default value to set
    pub fn with_default_column_value(
        mut self,
        field_name: String,
        value: String,
    ) -> Self {
        self.config.default_column_value.insert(field_name, value);
        self
    }

    /// Adds an object store option
    ///
    /// # Arguments
    ///
    /// * `key` - The key to add the object store option for
    /// * `value` - The value to add the object store option for
    pub fn with_object_store_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.config
            .object_store_options
            .insert(key.into(), value.into());
        self
    }

    /// Adds an option
    ///
    /// # Arguments
    ///
    /// * `key` - The key to add the option for
    /// * `value` - The value to add the option for
    pub fn with_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.config.options.insert(key.into(), value.into());
        self
    }

    /// Sets the number of threads for parallel processing
    ///
    /// # Arguments
    ///
    /// * `thread_num` - The number of threads for parallel processing
    pub fn with_thread_num(mut self, thread_num: usize) -> Self {
        self.config.thread_num = thread_num;
        self
    }

    /// Sets whether to use dynamic partitioning
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to use dynamic partitioning
    pub fn set_dynamic_partition(mut self, enable: bool) -> Self {
        self.config.use_dynamic_partition = enable;
        self
    }

    /// Sets whether to infer schema from data
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to infer schema from data
    pub fn set_inferring_schema(mut self, enable: bool) -> Self {
        self.config.inferring_schema = enable;
        self
    }

    /// Sets the maximum file size
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum file size to set
    pub fn with_max_file_size(mut self, size: u64) -> Self {
        self.config.max_file_size = Some(size);
        self
    }

    /// Sets the random number generator seed for Local Sensitive Hash
    ///
    /// # Arguments
    ///
    /// * `seed` - The random number generator seed to set
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.config.seed = seed;
        self
    }

    /// Sets the capacity for In Memory Buffer
    ///
    /// # Arguments
    ///
    /// * `capacity` - The capacity for memory buffer
    pub fn with_memory_buffer_capacity(mut self, capacity: usize) -> Self {
        self.config.memory_buffer_capacity = capacity;
        self
    }

    /// Sets the chunk size for multipart upload
    ///
    /// # Arguments
    ///
    /// * `chunk_size` - size used by multipart writer
    pub fn with_multipart_chunk_size(mut self, chunk_size: usize) -> Self {
        self.config.multipart_chunk_size = chunk_size;
        self
    }

    /// Sets the capacity for receiver
    ///
    /// # Arguments
    ///
    /// * `capacity` - receiver's buffer capacity
    pub fn with_receiver_capacity(mut self, capacity: usize) -> Self {
        self.config.receiver_capacity = capacity;
        self
    }

    pub fn try_build(self) -> Result<IOConfig, Report> {
        // try build task_ctx
        let mut config = self.config;
        let mut runtime_conf = RuntimeEnvBuilder::new();
        if let Some(pool_size) = config.pool_size() {
            let memory_pool = FairSpillPool::new(pool_size);
            runtime_conf = runtime_conf.with_memory_pool(Arc::new(memory_pool));
        }
        let runtime = runtime_conf.build()?;
        // firstly, parse default fs if exist
        let default_fs = config
            .object_store_options
            .get("fs.defaultFS")
            .or_else(|| config.object_store_options.get("fs.default.name"))
            .cloned();
        if let Some(fs) = default_fs {
            config.default_fs = fs.clone();
            info!("NativeIO register default fs {}", fs);
            register_object_store(&fs, &mut config, &runtime)?;
        };
        if !config.prefix.is_empty() {
            let prefix = config.prefix.clone();
            info!("NativeIO register prefix fs {}", prefix);
            let normalized_prefix =
                register_object_store(&prefix, &mut config, &runtime)?;
            config.prefix = normalized_prefix;
        } else if let Ok(warehouse_prefix) = std::env::var("LAKESOUL_WAREHOUSE_PREFIX") {
            info!("NativeIO register warehouse prefix {}", warehouse_prefix);
            let normalized_prefix =
                register_object_store(&warehouse_prefix, &mut config, &runtime)?;
            config.prefix = normalized_prefix;
        }
        debug!("{}", &config.prefix);
        // register object store(s) for input/output files' path
        // and replace file names with default fs concatenated if exist
        let files = config.files.clone();
        let normalized_filenames = files
            .into_iter()
            .map(|file_name| register_object_store(&file_name, &mut config, &runtime))
            .collect::<Result<Vec<String>, _>>()?;
        config.files = normalized_filenames;
        info!("NativeIO normalized file names: {:?}", config.files);
        info!("NativeIO final config: {:?}", config);

        let mut sess_conf = SessionConfig::default()
            .with_batch_size(config.batch_size)
            .with_parquet_pruning(true)
            // .with_prefetch(config.prefetch_size)
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(true);
        sess_conf
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
        sess_conf.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
        sess_conf.options_mut().execution.parquet.pushdown_filters =
            config.parquet_filter_pushdown;
        sess_conf.options_mut().execution.target_partitions = 1;
        sess_conf.options_mut().execution.parquet.dictionary_enabled = Some(false);
        sess_conf
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = false;
        config.task_ctx = Arc::new(TaskContext::new(
            None,
            "FFI".into(),
            sess_conf,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(runtime),
        ));
        Ok(config)
    }

    /// Builds the IOConfig instance
    ///
    /// # Returns
    ///
    /// The built IOConfig instance
    pub fn build(self) -> IOConfig {
        self.config
    }

    pub fn schema(&self) -> SchemaRef {
        self.config.target_schema()
    }

    pub fn primary_keys_slice(&self) -> &[String] {
        self.config.primary_keys_slice()
    }

    pub fn aux_sort_cols_slice(&self) -> &[String] {
        self.config.aux_sort_cols_slice()
    }

    pub fn prefix(&self) -> &String {
        &self.config.prefix
    }
}

impl From<IOConfig> for IOConfigBuilder {
    fn from(val: IOConfig) -> Self {
        IOConfigBuilder { config: val }
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::lakesoul_io_config::{LakeSoulIOConfigBuilder, create_session_context};

//     #[test]
//     fn test_path_normalize() {
//         let mut conf = LakeSoulIOConfigBuilder::new()
//             .with_files(vec![
//                 "file:///some/absolute/local/file1",
//                 "/some/absolute/local/file2",
//             ])
//             .build();
//         let _sess_ctx = create_session_context(&mut conf).unwrap();
//         assert_eq!(
//             conf.files,
//             vec![
//                 "file:///some/absolute/local/file1".to_string(),
//                 "file:///some/absolute/local/file2".to_string(),
//             ]
//         );
//         let lakesoulconfigbuilder = LakeSoulIOConfigBuilder::from(conf.clone());
//         let conf = lakesoulconfigbuilder.build();
//         assert_eq!(conf.max_file_size, None);
//         assert_eq!(conf.max_row_group_size, 250000);
//         assert_eq!(conf.max_row_group_num_values, 2147483647);
//         assert_eq!(conf.prefetch_size, 1);
//         assert_eq!(conf.parquet_filter_pushdown, false);
//     }
// }
