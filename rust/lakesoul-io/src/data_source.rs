// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource] implementation of LakeSoul.

// pub mod empty_schema;
// pub mod file_format;
// pub mod listing;
mod physical_plan;

use datafusion_datasource::{file_scan_config::FileScanConfig, source::DataSource};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, projection::ProjectionExec,
};

use crate::config::IOConfig;

#[derive(Debug)]
pub struct LakeSoulSource {
    io_config: IOConfig,
    scan_config: FileScanConfig,
}

impl LakeSoulSource {
    pub fn new() -> Self {
        todo!()
    }
}

impl DataSource for LakeSoulSource {
    fn open(
        &self,
        partition: usize,
        context: std::sync::Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        todo!()
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion_common::Statistics> {
        todo!()
    }

    fn with_fetch(
        &self,
        _limit: Option<usize>,
    ) -> Option<std::sync::Arc<dyn DataSource>> {
        todo!()
    }

    fn fetch(&self) -> Option<usize> {
        todo!()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> datafusion_common::Result<Option<std::sync::Arc<dyn ExecutionPlan>>> {
        todo!()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<std::sync::Arc<dyn datafusion_physical_expr::PhysicalExpr>>,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> datafusion_common::Result<
        datafusion_physical_plan::filter_pushdown::FilterPushdownPropagation<
            std::sync::Arc<dyn DataSource>,
        >,
    > {
        todo!()
    }
}
