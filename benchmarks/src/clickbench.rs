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

use std::env;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::util::{BenchmarkRun, CommonOpt};
use arrow::util::pretty;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use datafusion_common::exec_datafusion_err;
use datafusion_common::instant::Instant;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::builder::ArrowArrayCache;
use structopt::StructOpt;
use url::Url;

/// Run the clickbench benchmark
///
/// The ClickBench[1] benchmarks are widely cited in the industry and
/// focus on grouping / aggregation / filtering. This runner uses the
/// scripts and queries from [2].
///
/// [1]: https://github.com/ClickHouse/ClickBench
/// [2]: https://github.com/ClickHouse/ClickBench/tree/main/datafusion
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 0 and 42). If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to hits.parquet (single file) or `hits_partitioned`
    /// (partitioned, 100 files)
    #[structopt(
        parse(from_os_str),
        short = "p",
        long = "path",
        default_value = "benchmarks/data/hits.parquet"
    )]
    path: PathBuf,

    /// Path to queries.sql (single file)
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "benchmarks/queries/clickbench/queries.sql"
    )]
    queries_path: PathBuf,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

struct AllQueries {
    queries: Vec<String>,
}

impl AllQueries {
    fn try_new(path: &Path) -> Result<Self> {
        // ClickBench has all queries in a single file identified by line number
        let all_queries = std::fs::read_to_string(path)
            .map_err(|e| exec_datafusion_err!("Could not open {path:?}: {e}"))?;
        Ok(Self {
            queries: all_queries.lines().map(|s| s.to_string()).collect(),
        })
    }

    /// Returns the text of query `query_id`
    fn get_query(&self, query_id: usize) -> Result<&str> {
        self.queries
            .get(query_id)
            .ok_or_else(|| {
                let min_id = self.min_query_id();
                let max_id = self.max_query_id();
                exec_datafusion_err!(
                    "Invalid query id {query_id}. Must be between {min_id} and {max_id}"
                )
            })
            .map(|s| s.as_str())
    }

    fn min_query_id(&self) -> usize {
        0
    }

    fn max_query_id(&self) -> usize {
        self.queries.len() - 1
    }
}
impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let queries = AllQueries::try_new(self.queries_path.as_path())?;
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => queries.min_query_id()..=queries.max_query_id(),
        };

        // configure parquet options
        let mut config = self.common.config();
        {
            let parquet_options = &mut config.options_mut().execution.parquet;
            // The hits_partitioned dataset specifies string columns
            // as binary due to how it was written. Force it to strings
            parquet_options.binary_as_string = true;
        }

        let ctx = SessionContext::new_with_config(config);
        self.register_hits(&ctx).await?;

        let iterations = self.common.iterations;
        let mut benchmark_run = BenchmarkRun::new();
        let debug = self.common.debug;
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let sql = queries.get_query(query_id)?;
            println!("Q{query_id}: {sql}");

            for i in 0..iterations {
                let start = Instant::now();
                // let results = ctx.sql(sql).await?.collect().await?;
                let plan = ctx.sql(sql).await?;
                let (state, plan) = plan.into_parts();

                let plan = state.optimize(&plan)?;
                if debug {
                    println!("=== Optimized logical plan ===\n{plan}\n");
                }
                let physical_plan = state.create_physical_plan(&plan).await?;

                let result = collect(physical_plan.clone(), state.task_ctx()).await?;
                if debug {
                    println!(
                        "=== Physical plan with metrics ===\n{}\n",
                        DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
                            .indent(true)
                    );
                    if !result.is_empty() {
                        // do not call print_batches if there are no batches as the result is confusing
                        // and makes it look like there is a batch with no columns
                        pretty::print_batches(&result)?;
                    }
                }
                let elapsed = start.elapsed();
                let ms = elapsed.as_secs_f64() * 1000.0;
                let row_count: usize = result.iter().map(|b| b.num_rows()).sum();
                println!(
                    "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                );
                if self.common.print_result {
                    pretty::print_batches(&result)?;
                }

                // ArrowArrayCache::get().reset();

                benchmark_run.write_iter(elapsed, row_count);
            }
        }
        benchmark_run.set_cache_stats(ArrowArrayCache::get().stats());
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    /// Registers the `hits.parquet` as a table named `hits`
    async fn register_hits(&self, ctx: &SessionContext) -> Result<()> {
        let options = Default::default();
        let path = self.path.as_os_str().to_str().unwrap();
        let url = Url::parse(&"minio://parquet-oo").unwrap();
        let object_store = AmazonS3Builder::new()
            .with_bucket_name("parquet-oo")
            .with_endpoint("http://c220g5-110910.wisc.cloudlab.us:9000")
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_access_key_id(env::var("MINIO_ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(env::var("MINIO_SECRET_ACCESS_KEY").unwrap())
            .build()?;
        ctx.register_object_store(&url, Arc::new(object_store));

        ctx.register_parquet("hits", &path, options)
            .await
            .map_err(|e| {
                DataFusionError::Context(
                    format!("Registering 'hits' as {path}"),
                    Box::new(e),
                )
            })
    }
}
