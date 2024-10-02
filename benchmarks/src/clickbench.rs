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
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::util::{BenchmarkRun, CommonOpt};
use arrow::ipc::reader::FileReader;
use arrow::util::pretty;
use datafusion::datasource::physical_plan::parquet::Parquet7FileReaderFactory;
use datafusion::execution::cache::cache_unit::Cache37;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::ParquetReadOptions;
use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use datafusion_common::exec_datafusion_err;
use datafusion_common::instant::Instant;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::builder::ArrowArrayCache;
use structopt::StructOpt;
use url::Url;

use crate::{BenchmarkRun, CommonOpt};

use arrow::ipc::writer::FileWriter;

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

    /// Write the answers to a file
    #[structopt(long)]
    write_answers: bool,

    /// Check the answers against the stored answers
    #[structopt(long)]
    skip_answers: bool,

    /// Generate a flamegraph
    #[structopt(parse(from_os_str), long)]
    flamegraph: Option<PathBuf>,
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

    fn should_check_answer(&self, query_id: usize) -> bool {
        if query_id == 3 {
            // Query 3 is a AVG query, which returns a Float64, difficult to compare
            return false;
        }
        let sql = self.get_query(query_id).unwrap();
        if sql.contains("LIMIT") {
            // LIMIT is not deterministic, so we cannot compare the results
            return false;
        }
        true
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
                let profiler_guard = if self.flamegraph.is_some() && i == iterations - 1 {
                    Some(
                        pprof::ProfilerGuardBuilder::default()
                            .frequency(1000)
                            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                            .build()
                            .unwrap(),
                    )
                } else {
                    None
                };
                let start = Instant::now();
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
                if self.write_answers && i == 0 {
                    let file_path = format!(
                        "benchmarks/results/clickbench_answers/Q{}.arrow",
                        query_id
                    );
                    std::fs::create_dir_all("benchmarks/results/clickbench_answers")?;
                    let file = File::create(file_path)?;
                    let mut writer = std::io::BufWriter::new(file);
                    let schema = Arc::new(result[0].schema());
                    let mut arrow_writer = FileWriter::try_new(&mut writer, &schema)?;

                    for batch in &result {
                        arrow_writer.write(batch)?;
                    }
                    arrow_writer.finish()?;
                }
                // Compare results with stored answers
                if !self.skip_answers
                    && !self.write_answers
                    && queries.should_check_answer(query_id)
                {
                    let answer_file_path = format!(
                        "benchmarks/results/clickbench_answers/Q{}.arrow",
                        query_id
                    );
                    let file = File::open(answer_file_path)?;
                    let reader = std::io::BufReader::new(file);
                    let mut arrow_reader = FileReader::try_new(reader, None)?;

                    let mut stored_batches = Vec::new();
                    while let Some(batch) = arrow_reader.next() {
                        stored_batches.push(batch.unwrap());
                    }

                    if result.len() != stored_batches.len() {
                        panic!("Query {} iteration {} result batch count does not match stored answer", query_id, i);
                    } else {
                        for (result_batch, stored_batch) in
                            result.iter().zip(stored_batches.iter())
                        {
                            assert_eq!(result_batch, stored_batch);
                        }
                        println!(
                            "Query {} iteration {} answer check passed",
                            query_id, i
                        );
                    }
                } else {
                    println!("Query {} iteration {} answer not checked", query_id, i);
                }

                if let Some(guard) = profiler_guard {
                    let flamegraph_path = self.flamegraph.as_ref().unwrap();
                    if let Ok(report) = guard.report().build() {
                        let file = File::create(flamegraph_path).unwrap();
                        report.flamegraph(file).unwrap();
                    }
                }

                benchmark_run.write_iter(elapsed, row_count);
            }
        }

        benchmark_run.set_cache_stats(ArrowArrayCache::get().stats());
        benchmark_run.set_parquet_cache_size(Cache37::memory_usage());
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    /// Registers the `hits.parquet` as a table named `hits`
    async fn register_hits(&self, ctx: &SessionContext) -> Result<()> {
        let path = self.path.as_os_str().to_str().unwrap();

        let object_store: Arc<dyn ObjectStore> = if path.starts_with("minio://") {
            let url = Url::parse(path).unwrap();
            let bucket_name = url.host_str().unwrap_or("parquet-oo");
            let object_store = AmazonS3Builder::new()
                .with_bucket_name(bucket_name)
                .with_endpoint("http://c220g5-110910.wisc.cloudlab.us:9000")
                .with_allow_http(true)
                .with_region("us-east-1")
                .with_access_key_id(env::var("MINIO_ACCESS_KEY_ID").unwrap())
                .with_secret_access_key(env::var("MINIO_SECRET_ACCESS_KEY").unwrap())
                .build()?;
            let object_store = Arc::new(object_store);
            ctx.register_object_store(&url, object_store.clone());
            object_store
        } else {
            let url = ObjectStoreUrl::local_filesystem();
            let object_store = ctx.runtime_env().object_store(url).unwrap();
            Arc::new(object_store)
        };

        let mut options: ParquetReadOptions<'_> = Default::default();
        options.reader = Some(Arc::new(Parquet7FileReaderFactory::new(object_store)));

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
