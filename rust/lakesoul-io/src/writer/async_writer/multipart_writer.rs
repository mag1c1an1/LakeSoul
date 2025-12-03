// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the multipart writer.

use crate::{
    config::IOConfigRef,
    constant::TBD_PARTITION_DESC,
    helpers::get_batch_memory_size,
    transform::{uniform_record_batch, uniform_schema},
    writer::async_writer::{AsyncBatchWriter, FlushOutputVec, FlusthOutput, InMemBuf},
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::BytesMut;
use datafusion_common::project_schema;
use datafusion_datasource::ListingTableUrl;
use datafusion_execution::{TaskContext, object_store::ObjectStoreUrl};
use object_store::WriteMultipart;
use object_store::{ObjectStore, path::Path};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use rootcause::{Report, bail, report};
use std::sync::Arc;
use url::Url;

// An async writer using object_store's multi-part upload feature for cloud storage.
// This writer uses a `VecDeque<u8>` as `std::io::Write` for arrow-rs's ArrowWriter.
// Everytime when a new RowGroup is flushed, the length of the VecDeque would grow.
// At this time, we pass the VecDeque as `bytes::Buf` to `AsyncWriteExt::write_buf` provided
// by object_store, which would drain and copy the content of the VecDeque so that we could reuse it.
// The `CloudMultiPartUpload` itself would try to concurrently upload parts, and
// all parts will be committed to cloud storage by shutdown the `AsyncWrite` object.
pub struct MultiPartAsyncWriter {
    /// The task context of the multi-part async writer.
    task_ctx: Arc<TaskContext>,
    /// The in-memory buffer of the multi-part async writer.
    in_mem_buf: InMemBuf,
    /// The schema of the multi-part async writer.
    schema: SchemaRef,
    /// The multi-part writer of [`object_store::WriteMultipart`] that is used to upload the data to the object store asynchronously.
    writer: WriteMultipart,
    /// The [`ArrowWriter`] of the multi-part async writer.
    arrow_writer: ArrowWriter<InMemBuf>,
    /// The io config of the multi-part async writer.
    config: IOConfigRef,
    /// The object store of the multi-part async writer.
    object_store: Arc<dyn ObjectStore>,
    /// The path of the multi-part async writer.
    path: Path,
    /// The absolute path of the multi-part async writer.
    absolute_path: String,
    /// The number of rows of the multi-part async writer.
    num_rows: u64,
    /// The number of memory bufferd.
    buffered_size: u64,
}

impl MultiPartAsyncWriter {
    pub async fn try_new_with_context(
        config: IOConfigRef,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Self, Report> {
        let io_config = config.read();
        if io_config.files.is_empty() {
            bail!("wrong number of file names provided for writer");
        }
        let file_name = &io_config.files.last().ok_or(report!("wrong file name"))?;

        // local style path should have already been handled in create_session_context,
        // so we don't have to deal with ParseError::RelativeUrlWithoutBase here
        let (object_store, path) = match Url::parse(file_name.as_str()) {
            Ok(url) => Ok((
                task_ctx.runtime_env().object_store(ObjectStoreUrl::parse(
                    &url[..url::Position::BeforePath],
                )?)?,
                Path::from_url_path(url.path())?,
            )),
            Err(e) => Err(report!(e)),
        }?;

        // get underlying multipart uploader
        let multipart_upload = object_store.put_multipart(&path).await?;
        let write_multi_part = WriteMultipart::new_with_chunk_size(
            multipart_upload,
            io_config.multipart_chunk_size,
        );
        let in_mem_buf = InMemBuf::with_capacity(io_config.memory_buffer_capacity);

        let schema = uniform_schema(io_config.target_schema.0.clone());

        // O(nm), n = number of fields, m = number of range partitions
        let schema_projection_excluding_range = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                match io_config.range_partitions.contains(field.name()) {
                    true => None,
                    false => Some(idx),
                }
            })
            .collect::<Vec<_>>();
        let writer_schema =
            project_schema(&schema, Some(&schema_projection_excluding_range))?;

        let max_row_group_size = if io_config.max_row_group_size * schema.fields().len()
            > io_config.max_row_group_num_values
        {
            io_config
                .batch_size
                .max(io_config.max_row_group_num_values / schema.fields().len())
        } else {
            io_config.max_row_group_size
        };
        let arrow_writer = ArrowWriter::try_new(
            in_mem_buf.clone(),
            writer_schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(max_row_group_size)
                    .set_write_batch_size(io_config.batch_size)
                    .set_compression(Compression::ZSTD(ZstdLevel::default()))
                    .set_dictionary_enabled(false) // disable dictionary encoding
                    .build(),
            ),
        )?;

        Ok(MultiPartAsyncWriter {
            task_ctx,
            in_mem_buf,
            schema,
            writer: write_multi_part,
            arrow_writer,
            config: config.clone(),
            object_store,
            path: path,
            absolute_path: file_name.to_string(),
            num_rows: 0,
            buffered_size: 0,
        })
    }

    // pub async fn try_new(mut config: LakeSoulIOConfig) -> Result<Self> {
    //     let task_context = create_session_context(&mut config)?.task_ctx();
    //     Self::try_new_with_context(&mut config, task_context).await
    // }

    async fn write_batch(
        batch: RecordBatch,
        arrow_writer: &mut ArrowWriter<InMemBuf>,
        in_mem_buf: &mut InMemBuf,
        writer: &mut WriteMultipart,
    ) -> Result<(), Report> {
        arrow_writer.write(&batch)?;
        let mut v = in_mem_buf.0.try_borrow_mut().map_err(|e| report!(e))?;
        if !v.is_empty() {
            MultiPartAsyncWriter::write_part(writer, &mut v).await
        } else {
            Ok(())
        }
    }

    pub async fn write_part(
        writer: &mut WriteMultipart,
        in_mem_buf: &mut BytesMut,
    ) -> Result<(), Report> {
        let bytes = in_mem_buf.split().freeze();
        writer.put(bytes);
        Ok(())
    }

    pub fn nun_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn absolute_path(&self) -> String {
        self.absolute_path.clone()
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx.clone()
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for MultiPartAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), Report> {
        let batch = uniform_record_batch(batch)?;
        self.num_rows += batch.num_rows() as u64;
        self.buffered_size += get_batch_memory_size(&batch)? as u64;
        MultiPartAsyncWriter::write_batch(
            batch,
            &mut self.arrow_writer,
            &mut self.in_mem_buf,
            &mut self.writer,
        )
        .await
    }

    async fn flush_and_close(self: Box<Self>) -> Result<FlushOutputVec, Report> {
        debug!(
            "MultiPartAsyncWriter::flush_and_close: {:?}",
            self.arrow_writer
        );
        // close arrow writer to flush remaining rows
        let mut this = *self;
        let arrow_writer = this.arrow_writer;
        let file_path = this.absolute_path.clone();
        let metadata = arrow_writer.close()?;
        let mut v = this.in_mem_buf.0.try_borrow_mut().map_err(|e| report!(e))?;
        if !v.is_empty() {
            MultiPartAsyncWriter::write_part(&mut this.writer, &mut v).await?;
        }
        // shutdown multi-part async writer to complete the upload
        this.writer.finish().await?;
        let path = Path::from_url_path(
            <ListingTableUrl as AsRef<Url>>::as_ref(&ListingTableUrl::parse(&file_path)?)
                .path(),
        )?;
        let object_meta = this.object_store.head(&path).await?;
        Ok(vec![FlusthOutput {
            partition_desc: TBD_PARTITION_DESC.to_string(),
            file_path,
            object_meta,
            file_meta: metadata,
        }])
    }

    async fn abort_and_close(self: Box<Self>) -> Result<(), Report> {
        let this = *self;
        this.writer.abort().await?;
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn buffered_size(&self) -> u64 {
        self.buffered_size
    }
}
