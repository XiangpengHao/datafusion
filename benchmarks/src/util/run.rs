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

use datafusion::{error::Result, DATAFUSION_VERSION};
use datafusion_common::utils::get_available_parallelism;
use parquet::arrow::builder::ArrowCacheStatistics;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    time::{Duration, SystemTime},
};

fn serialize_start_time<S>(start_time: &SystemTime, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ser.serialize_u64(
        start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("current time is later than UNIX_EPOCH")
            .as_secs(),
    )
}
fn serialize_elapsed<S>(elapsed: &Duration, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ms = elapsed.as_secs_f64() * 1000.0;
    ser.serialize_f64(ms)
}
#[derive(Debug, Serialize)]
pub struct RunContext {
    /// Benchmark crate version
    pub benchmark_version: String,
    /// DataFusion crate version
    pub datafusion_version: String,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Start time
    #[serde(serialize_with = "serialize_start_time")]
    pub start_time: SystemTime,
    /// CLI arguments
    pub arguments: Vec<String>,
}

impl Default for RunContext {
    fn default() -> Self {
        Self::new()
    }
}

impl RunContext {
    pub fn new() -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: DATAFUSION_VERSION.to_owned(),
            num_cpus: get_available_parallelism(),
            start_time: SystemTime::now(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
        }
    }
}

/// A single iteration of a benchmark query
#[derive(Debug, Serialize)]
struct QueryIter {
    #[serde(serialize_with = "serialize_elapsed")]
    elapsed: Duration,
    row_count: usize,
}
/// A single benchmark case
#[derive(Debug, Serialize)]
pub struct BenchQuery {
    query: String,
    iterations: Vec<QueryIter>,
    #[serde(serialize_with = "serialize_start_time")]
    start_time: SystemTime,
}

/// collects benchmark run data and then serializes it at the end
pub struct BenchmarkRun {
    context: RunContext,
    queries: Vec<BenchQuery>,
    current_case: Option<usize>,
    arrow_cache_stats: Option<ArrowCacheStatistics>,
    parquet_cache_size: Option<usize>,
}

impl Default for BenchmarkRun {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkRun {
    // create new
    pub fn new() -> Self {
        Self {
            context: RunContext::new(),
            queries: vec![],
            current_case: None,
            arrow_cache_stats: None,
            parquet_cache_size: None,
        }
    }
    /// begin a new case. iterations added after this will be included in the new case
    pub fn start_new_case(&mut self, id: &str) {
        self.queries.push(BenchQuery {
            query: id.to_owned(),
            iterations: vec![],
            start_time: SystemTime::now(),
        });
        if let Some(c) = self.current_case.as_mut() {
            *c += 1;
        } else {
            self.current_case = Some(0);
        }
    }
    /// Write a new iteration to the current case
    pub fn write_iter(&mut self, elapsed: Duration, row_count: usize) {
        if let Some(idx) = self.current_case {
            self.queries[idx]
                .iterations
                .push(QueryIter { elapsed, row_count })
        } else {
            panic!("no cases existed yet");
        }
    }

    pub fn set_cache_stats(&mut self, cache_stats: ArrowCacheStatistics) {
        self.arrow_cache_stats = Some(cache_stats);
    }

    pub fn set_parquet_cache_size(&mut self, size: usize) {
        self.parquet_cache_size = Some(size);
    }

    /// Stringify data into formatted json
    pub fn to_json(&self) -> String {
        let mut output = HashMap::<&str, Value>::new();
        output.insert("context", serde_json::to_value(&self.context).unwrap());
        output.insert("queries", serde_json::to_value(&self.queries).unwrap());
        output.insert(
            "parquet_cache_size",
            serde_json::to_value(&self.parquet_cache_size.unwrap_or(0)).unwrap(),
        );
        output.insert(
            "arrow_cache_size",
            serde_json::to_value(
                &self
                    .arrow_cache_stats
                    .as_ref()
                    .map(|x| x.memory_usage())
                    .unwrap_or(0),
            )
            .unwrap(),
        );
        serde_json::to_string_pretty(&output).unwrap()
    }

    /// Write data as json into output path if it exists.
    pub fn maybe_write_json(
        &mut self,
        maybe_path: Option<impl AsRef<Path>>,
    ) -> Result<()> {
        if let Some(ref path) = maybe_path {
            std::fs::write(path, self.to_json())?;
        };
        // stats is too large so we write into parquet
        if let Some(path) = maybe_path {
            // same filename but parquet extension
            let stats = self.arrow_cache_stats.take();
            if let Some(stats) = stats {
                let path = path.as_ref().with_extension("parquet");
                let mut writer = File::create(path)?;

                let stats_batch = stats.into_record_batch();
                let schema = stats_batch.schema();
                let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
                    &mut writer,
                    schema,
                    None,
                )?;
                writer.write(&stats_batch)?;
                writer.close()?;
            }
        }
        Ok(())
    }
}
