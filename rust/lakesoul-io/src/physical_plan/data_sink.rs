use datafusion_datasource::sink::DataSink;

pub struct LakeSoulSink {}

// impl DataSink for LakeSoulSink {
//     #[doc = " Returns the data sink as [`Any`](std::any::Any) so that it can be"]
//     #[doc = " downcast to a specific implementation."]
//     fn as_any(&self) -> &dyn Any {
//         todo!()
//     }

//     #[doc = " Returns the sink schema"]
//     fn schema(&self) -> &SchemaRef {
//         todo!()
//     }

//     #[allow(
//         elided_named_lifetimes,
//         clippy::type_complexity,
//         clippy::type_repetition_in_bounds
//     )]
//     fn write_all<'life0, 'life1, 'async_trait>(
//         &'life0 self,
//         data: SendableRecordBatchStream,
//         context: &'life1 Arc<TaskContext>,
//     ) -> ::core::pin::Pin<
//         Box<
//             dyn ::core::future::Future<Output = Result<u64>>
//                 + ::core::marker::Send
//                 + 'async_trait,
//         >,
//     >
//     where
//         'life0: 'async_trait,
//         'life1: 'async_trait,
//         Self: 'async_trait,
//     {
//         todo!()
//     }
// }
