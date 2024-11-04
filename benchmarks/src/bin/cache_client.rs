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

use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::exec_datafusion_err;
use datafusion_flight_table::sql::{FlightSqlDriver, USERNAME};
use datafusion_flight_table::FlightTableFactory;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    queries_path: PathBuf,

    #[structopt(long)]
    query: Option<usize>,
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

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    env_logger::init();
    let options = Options::from_args();
    let all_queries = AllQueries::try_new(options.queries_path.as_path())?;

    let query_id = options.query.unwrap_or(0);
    let sql = all_queries.get_query(query_id)?;

    let ctx = SessionContext::new();
    let mut state = ctx.state();
    state
        .config_mut()
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = false;

    let flight_sql = FlightTableFactory::new(Arc::new(FlightSqlDriver::default()));
    let table = flight_sql
        .open_table(
            "http://localhost:50051",
            HashMap::from([(USERNAME.into(), "whatever".into())]),
            "hits",
        )
        .await?;
    ctx.register_table("hits", Arc::new(table))?;

    let plan = ctx.sql(sql).await?;
    let (state, plan) = plan.into_parts();
    let plan = state.optimize(&plan)?;

    println!("logical plan: {}", plan);
    let physical_plan = state.create_physical_plan(&plan).await?;
    let result = collect(physical_plan.clone(), state.task_ctx()).await?;
    println!(
        "=== Physical plan with metrics ===\n{}\n",
        DisplayableExecutionPlan::with_metrics(physical_plan.as_ref()).indent(true)
    );
    if !result.is_empty() {
        // do not call print_batches if there are no batches as the result is confusing
        // and makes it look like there is a batch with no columns
        pretty::print_batches(&result)?;
    }
    Ok(())
}
