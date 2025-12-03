// use crate::config::IOConfig;

// /// Creates a new session context
// ///
// /// # Arguments
// ///
// /// * `config` - A mutable reference to the LakeSoulIOConfig instance
// ///
// /// # Returns
// ///
// /// A new SessionContext instance
// pub fn create_session_context(config: &mut IOConfig) -> Result<SessionContext, Report> {
//     create_session_context_with_planner(config, None)
// }

// /// Creates a new session context with a specific query planner
// ///
// /// # Arguments
// ///
// /// * `config` - A mutable reference to the LakeSoulIOConfig instance
// /// * `planner` - An optional Arc<dyn QueryPlanner + Send + Sync> instance
// ///
// /// # Returns
// ///
// /// A new SessionContext instance
// pub fn create_session_context_with_planner(
//     config: &mut LakeSoulIOConfig,
//     planner: Option<Arc<dyn QueryPlanner + Send + Sync>>,
// ) -> Result<SessionContext> {
//     let mut sess_conf = SessionConfig::default()
//         .with_batch_size(config.batch_size)
//         .with_parquet_pruning(true)
//         // .with_prefetch(config.prefetch_size)
//         .with_information_schema(true)
//         .with_create_default_catalog_and_schema(true);

//     sess_conf
//         .options_mut()
//         .optimizer
//         .enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
//     sess_conf.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
//     sess_conf.options_mut().execution.parquet.pushdown_filters =
//         config.parquet_filter_pushdown;
//     sess_conf.options_mut().execution.target_partitions = 1;
//     sess_conf.options_mut().execution.parquet.dictionary_enabled = Some(false);
//     sess_conf
//         .options_mut()
//         .execution
//         .parquet
//         .schema_force_view_types = false;

//     let mut runtime_conf = RuntimeEnvBuilder::new();
//     if let Some(pool_size) = config.pool_size() {
//         let memory_pool = FairSpillPool::new(pool_size);
//         runtime_conf = runtime_conf.with_memory_pool(Arc::new(memory_pool));
//     }
//     let runtime = runtime_conf.build()?;

//     // firstly, parse default fs if exist
//     let default_fs = config
//         .object_store_options
//         .get("fs.defaultFS")
//         .or_else(|| config.object_store_options.get("fs.default.name"))
//         .cloned();
//     if let Some(fs) = default_fs {
//         config.default_fs = fs.clone();
//         info!("NativeIO register default fs {}", fs);
//         register_object_store(&fs, config, &runtime)?;
//     };

//     if !config.prefix.is_empty() {
//         let prefix = config.prefix.clone();
//         info!("NativeIO register prefix fs {}", prefix);
//         let normalized_prefix = register_object_store(&prefix, config, &runtime)?;
//         config.prefix = normalized_prefix;
//     } else if let Ok(warehouse_prefix) = std::env::var("LAKESOUL_WAREHOUSE_PREFIX") {
//         info!("NativeIO register warehouse prefix {}", warehouse_prefix);
//         let normalized_prefix =
//             register_object_store(&warehouse_prefix, config, &runtime)?;
//         config.prefix = normalized_prefix;
//     }
//     debug!("{}", &config.prefix);

//     // register object store(s) for input/output files' path
//     // and replace file names with default fs concatenated if exist
//     let files = config.files.clone();
//     let normalized_filenames = files
//         .into_iter()
//         .map(|file_name| register_object_store(&file_name, config, &runtime))
//         .collect::<Result<Vec<String>>>()?;
//     config.files = normalized_filenames;
//     info!("NativeIO normalized file names: {:?}", config.files);
//     info!("NativeIO final config: {:?}", config);

//     let builder = SessionStateBuilder::new()
//         .with_config(sess_conf)
//         .with_runtime_env(Arc::new(runtime));
//     let builder = if let Some(planner) = planner {
//         builder.with_query_planner(planner)
//     } else {
//         builder
//     };
//     // create session context
//     // only keep projection/filter rules as others are unnecessary
//     let state = builder
//         .with_analyzer_rules(vec![Arc::new(TypeCoercion {})])
//         .with_optimizer_rules(vec![
//             Arc::new(PushDownFilter {}),
//             Arc::new(OptimizeProjections {}),
//             Arc::new(SimplifyExpressions {}),
//         ])
//         .with_physical_optimizer_rules(vec![Arc::new(ProjectionPushdown {})])
//         .build();

//     Ok(SessionContext::new_with_state(state))
// }
