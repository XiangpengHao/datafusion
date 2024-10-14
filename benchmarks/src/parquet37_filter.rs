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

use std::path::PathBuf;

use crate::{AccessLogOpt, BenchmarkRun, CommonOpt};

use arrow::util::pretty;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::execution::cache::cache_unit::Cache37;
use datafusion::logical_expr::{lit, Expr};
use datafusion::physical_plan::collect;
use datafusion::prelude::{col, SessionConfig, SessionContext};
use datafusion::test_util::parquet::TestParquetFile;
use datafusion_common::instant::Instant;

use structopt::StructOpt;

/// Test performance of parquet filter pushdown
///
/// The queries are executed on a synthetic dataset generated during
/// the benchmark execution and designed to simulate web server access
/// logs.
///
/// Example
///
/// dfbench parquet-filter  --path ./data --scale-factor 1.0
///
/// generates the synthetic dataset at `./data/logs.parquet`. The size
/// of the dataset can be controlled through the `size_factor`
/// (with the default value of `1.0` generating a ~1GB parquet file).
///
/// For each filter we will run the query using different
/// `ParquetScanOption` settings.
///
/// Example output:
///
/// Running benchmarks with the following options: Opt { debug: false, iterations: 3, partitions: 2, path: "./data", batch_size: 8192, scale_factor: 1.0 }
/// Generated test dataset with 10699521 rows
/// Executing with filter 'request_method = Utf8("GET")'
/// Using scan options ParquetScanOptions { pushdown_filters: false, reorder_predicates: false, enable_page_index: false }
/// Iteration 0 returned 10699521 rows in 1303 ms
/// Iteration 1 returned 10699521 rows in 1288 ms
/// Iteration 2 returned 10699521 rows in 1266 ms
/// Using scan options ParquetScanOptions { pushdown_filters: true, reorder_predicates: true, enable_page_index: true }
/// Iteration 0 returned 1781686 rows in 1970 ms
/// Iteration 1 returned 1781686 rows in 2002 ms
/// Iteration 2 returned 1781686 rows in 1988 ms
/// Using scan options ParquetScanOptions { pushdown_filters: true, reorder_predicates: false, enable_page_index: true }
/// Iteration 0 returned 1781686 rows in 1940 ms
/// Iteration 1 returned 1781686 rows in 1986 ms
/// Iteration 2 returned 1781686 rows in 1947 ms
/// ...
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Create data files
    #[structopt(flatten)]
    access_log: AccessLogOpt,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

#[derive(Debug)]
struct Parquet37ScanOptions {
    row_group_pruning: bool,
    page_index_pruning: bool,
    bloom_filter_pruning: bool,
    pushdown_filters: bool,
}

impl Parquet37ScanOptions {
    pub fn config(&self) -> SessionConfig {
        let mut config = ConfigOptions::new();
        config.execution.parquet.pruning = self.row_group_pruning;
        config.execution.parquet.bloom_filter_on_read = self.bloom_filter_pruning;
        config.execution.parquet.enable_page_index = self.page_index_pruning;
        config.execution.parquet.pushdown_filters = self.pushdown_filters;
        config.into()
    }
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let test_file = self.access_log.build()?;

        let mut rundata = BenchmarkRun::new();
        let scan_options_matrix = vec![
            Parquet37ScanOptions {
                row_group_pruning: false,
                page_index_pruning: false,
                bloom_filter_pruning: false,
                pushdown_filters: false,
            },
            Parquet37ScanOptions {
                row_group_pruning: true,
                page_index_pruning: false,
                bloom_filter_pruning: false,
                pushdown_filters: false,
            },
            Parquet37ScanOptions {
                row_group_pruning: true,
                page_index_pruning: true,
                bloom_filter_pruning: false,
                pushdown_filters: false,
            },
            Parquet37ScanOptions {
                row_group_pruning: true,
                page_index_pruning: true,
                bloom_filter_pruning: true,
                pushdown_filters: false,
            },
        ];

        let filter_matrix = vec![
            // ("Selective-ish filter", col("request_method").eq(lit("GET"))),
            (
                "Filter everything",
                col("response_status").lt_eq(lit(500_u16)),
            ),
        ];

        for (name, filter_expr) in &filter_matrix {
            println!("Executing '{name}' (filter: {filter_expr})");
            for scan_options in &scan_options_matrix {
                println!("Using scan options {scan_options:?}");
                rundata.start_new_case(&format!(
                    "{name}: {}",
                    parquet_scan_disp(scan_options)
                ));
                for i in 0..self.common.iterations {
                    let config = self.common.update_config(scan_options.config());
                    let ctx = SessionContext::new_with_config(config);

                    let (rows, elapsed) = exec_scan(
                        &ctx,
                        &test_file,
                        filter_expr.clone(),
                        self.common.debug,
                    )
                    .await?;
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    println!("Iteration {i} returned {rows} rows in {ms} ms");
                    rundata.write_iter(elapsed, rows, Cache37::consume_bytes_read());
                }
            }
            println!("\n");
        }
        rundata.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }
}

fn parquet_scan_disp(opts: &Parquet37ScanOptions) -> String {
    format!(
        "row_group={}, page_idx={}, bloom_filter={}",
        opts.row_group_pruning, opts.page_index_pruning, opts.bloom_filter_pruning
    )
}

async fn exec_scan(
    ctx: &SessionContext,
    test_file: &TestParquetFile,
    filter: Expr,
    debug: bool,
) -> Result<(usize, std::time::Duration)> {
    let start = Instant::now();
    let exec = test_file.create_scan(ctx, Some(filter)).await?;

    let task_ctx = ctx.task_ctx();
    let result = collect(exec, task_ctx).await?;
    let elapsed = start.elapsed();
    if debug {
        pretty::print_batches(&result)?;
    }
    let rows = result.iter().map(|b| b.num_rows()).sum();
    Ok((rows, elapsed))
}
