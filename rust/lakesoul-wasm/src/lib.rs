// use std::sync::{Arc, Mutex};
//
// use datafusion::arrow::util::pretty::pretty_format_batches;
// use datafusion::prelude::SessionContext;
// use wasm_bindgen::prelude::*;
//
// pub use error::Result;
// use colored::Colorize;
// mod utils;
//
// mod error {
//     use std::fmt::{Display, Formatter};
//     use std::io;
//
//     use anyhow::{anyhow, Error};
//     use datafusion::arrow::error::ArrowError;
//     use datafusion::error::DataFusionError;
//     use wasm_bindgen::JsValue;
//
//     #[derive(Debug)]
//     pub struct WasmError(Error);
//
//     impl Into<JsValue> for WasmError {
//         fn into(self) -> JsValue {
//             JsValue::from_str("something wrong")
//         }
//     }
//
//     impl From<Error> for WasmError {
//         fn from(value: Error) -> Self {
//             Self(value)
//         }
//     }
//
//     impl From<DataFusionError> for WasmError {
//         fn from(value: DataFusionError) -> Self {
//             WasmError(anyhow!(value))
//         }
//     }
//
//     impl From<ArrowError> for WasmError {
//         fn from(value: ArrowError) -> Self {
//             WasmError(anyhow!(value))
//         }
//     }
//
//     impl From<io::Error> for WasmError {
//         fn from(value: io::Error) -> Self {
//             WasmError(anyhow!(value))
//         }
//     }
//
//
//    impl Display for WasmError {
//        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//            write!(f, "{}", self.0)
//        }
//    }
//
//     pub type Result<T> = std::result::Result<T, WasmError>;
// }
//
// lazy_static::lazy_static! {
//      static ref SQL_CLIENT: Arc<Mutex<SqlClent>> = {
//         Arc::new(Mutex::new(SqlClent::try_new().unwrap()))
//     };
// }
//
//
// #[wasm_bindgen]
// pub fn init() -> bool {
//     let _guard = SQL_CLIENT.lock().unwrap();
//     true
// }
//
//
// #[wasm_bindgen]
// pub fn exec_sql(sql: &str) -> String {
//     let res = SQL_CLIENT.lock().unwrap().run_sql(sql);
//     match res {
//         Ok(s) => s.blue().to_string(),
//         Err(e) => format!("{}", e).red().to_string(),
//     }
// }
//
//
// #[wasm_bindgen]
// extern "C" {
//     fn alert(s: &str);
// }
//
// #[wasm_bindgen]
// pub fn greet(name: &str) {
//     alert(&format!("Hello, {}!", name));
// }
//
//
// struct SqlClent {
//     sc: SessionContext,
//     runtime: tokio::runtime::Runtime,
// }
//
//
// impl SqlClent {
//     fn try_new() -> Result<Self> {
//         let sc = SessionContext::new();
//         let runtime = tokio::runtime::Builder::new_current_thread().build()?;
//         // Ok(Self { sc })
//         Ok(Self { sc, runtime })
//     }
//     fn run_sql(&self, sql: &str) -> Result<String> {
//         self.runtime.block_on(async {
//             let rb = self.sc.sql(sql).await?
//                 .collect().await?;
//             let string = format!("{}", pretty_format_batches(&rb)?);
//             Ok(string)
//         })
//     }
// }
//
//
// mod tests {
//     use super::*;
//
//     #[test]
//     fn start_test() {
//         let b = init();
//         {
//             let guard = SQL_CLIENT.lock().unwrap();
//             let s = guard.run_sql("select 1+1;");
//             println!("{}", s.unwrap());
//         }
//         let x = exec_sql("select * from b;");
//         println!("{}",x);
//
//     }
// }