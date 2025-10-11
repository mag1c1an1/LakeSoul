use std::{pin::Pin, sync::Arc, time::Instant};

use arrow_array::RecordBatch;
use arrow_flight::{
    error::FlightError,
    sql::{CommandStatementIngest, client::FlightSqlServiceClient},
};
use datafusion::{execution::RecordBatchStream, physical_plan::execute_stream};
use futures::Stream;
use lakesoul_datafusion::{cli::CoreArgs, create_lakesoul_session_ctx};
use lakesoul_flight::TokenServerClient;
use lakesoul_metadata::{Claims, MetaDataClient};
use tonic::{Request, transport::Channel};

#[tokio::main]
async fn main() {
    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let mut core_args = CoreArgs::default();
    core_args.worker_threads = 8;
    let ctx = create_lakesoul_session_ctx(meta_client.clone(), &core_args).unwrap();

    ctx.sql("DROP TABLE IF EXISTS test_lfs").await.unwrap();

    let create_sql = "
        CREATE EXTERNAL TABLE
        test_lfs
        (o_orderkey BIGINT NOT NULL, o_custkey BIGINT NOT NULL, o_orderstatus STRING NOT NULL , o_totalprice DECIMAL(15,2) NOT NULL ,o_orderdate DATE NOT NULL , o_orderpriority STRING NOT NULL ,o_clerk STRING NOT NULL ,o_shippriority INT NOT NULL ,o_comment STRING NOT NULL)
        STORED AS LAKESOUL
        LOCATION 'file:///data/LAKESOUL/test_lfs_data'
    ";

    ctx.sql(create_sql).await.unwrap();

    let start = Instant::now();
    let df = ctx.sql("select * from orders").await.unwrap();
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.unwrap();
    let stream = execute_stream(plan, task_ctx).unwrap();
    let wrapper = Wrapper { inner: stream };

    let mut client = build_client(None).await;
    let cmd = CommandStatementIngest {
        table_definition_options: None,
        table: String::from("test_lfs"),
        schema: None,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: std::collections::HashMap::new(),
    };

    let ret = client.execute_ingest(cmd, wrapper).await.unwrap();
    println!("{}", ret);
    let elapsed = start.elapsed();
    println!("Time Elapsed: {}", elapsed.as_millis());
}

pub struct Wrapper {
    pub inner: Pin<Box<dyn RecordBatchStream + Send>>,
}

impl Stream for Wrapper {
    type Item = std::result::Result<RecordBatch, FlightError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };

        match inner.poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                std::task::Poll::Ready(Some(Ok(batch)))
            }
            std::task::Poll::Ready(Some(Err(err))) => std::task::Poll::Ready(Some(Err(
                FlightError::ExternalError(Box::new(err)),
            ))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub async fn build_client(claims: Option<Claims>) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new(channel.clone());
    if let Some(c) = claims {
        let mut token_server = TokenServerClient::new(channel);
        let token = token_server.create_token(Request::new(c)).await.unwrap();
        client.set_header("authorization", format!("Bearer {}", token.get_ref().token));
    }

    client
}
