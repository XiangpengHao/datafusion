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

//! Tests for parquet schema handling
use std::{collections::HashMap, fs, path::Path};

use arrow_array::StringViewArray;
use datafusion_physical_plan::{
    displayable, filter::FilterExec, ExecutionPlan, ExecutionPlanVisitor,
};
use tempfile::TempDir;

use super::*;
use datafusion_common::assert_batches_sorted_eq;

struct FilterExecVisitor {
    found: bool,
}

impl FilterExecVisitor {
    fn new() -> Self {
        Self { found: false }
    }
}

impl ExecutionPlanVisitor for FilterExecVisitor {
    type Error = datafusion::error::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> datafusion::error::Result<bool> {
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            self.found = true;
            return Ok(false);
        }
        Ok(true)
    }
}

#[tokio::test]
async fn parquet_read_filter_string_view() {
    let tmp_dir = TempDir::new().unwrap();

    let values = vec![Some("small"), None, Some("Larger than 12 bytes array")];
    let c1: ArrayRef = Arc::new(StringViewArray::from_iter(values.iter()));
    let c2: ArrayRef = Arc::new(StringArray::from_iter(values.iter()));

    let batch =
        RecordBatch::try_from_iter(vec![("c1", c1.clone()), ("c2", c2.clone())]).unwrap();

    let file_name = {
        let table_dir = tmp_dir.path().join("parquet_test");
        std::fs::create_dir(&table_dir).unwrap();
        let file_name = table_dir.join("part-0.parquet");
        let mut writer = ArrowWriter::try_new(
            fs::File::create(&file_name).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        file_name
    };

    let ctx = SessionContext::new();
    ctx.register_parquet("t", file_name.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    async fn display_result(sql: &str, ctx: &SessionContext) {
        let state = ctx.state();
        let l_plan = state.create_logical_plan(sql).await.unwrap();
        println!("logical plan: {}", l_plan.display_indent_schema());

        let df = ctx.sql(sql).await.unwrap();
        let p_plan = df.create_physical_plan().await.unwrap();

        let mut visitor = FilterExecVisitor::new();
        datafusion::physical_plan::accept(p_plan.as_ref(), &mut visitor).unwrap();
        let displayable_p_plan = displayable(p_plan.as_ref());

        println!("physical plan: {}", displayable_p_plan.indent(true));

        let result = ctx.sql(sql).await.unwrap().collect().await.unwrap();

        arrow::util::pretty::print_batches(&result).unwrap();

        for b in result {
            println!("schema: {:?}", b.schema());
        }
    }

    // display_result("SELECT * from t", &ctx).await;
    // display_result("SELECT * from t where c1 <> 'small'", &ctx).await;
    display_result("SELECT * from t where c2 <> 'small'", &ctx).await;
}

#[tokio::test]
async fn schema_merge_ignores_metadata_by_default() {
    // Create several parquet files in same directoty / table with
    // same schema but different metadata
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");

    let options = ParquetReadOptions::default();

    let f1 = Field::new("id", DataType::Int32, true);
    let f2 = Field::new("name", DataType::Utf8, true);

    let schemas = vec![
        // schema level metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "bar")),
        // schema different (incompatible) metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "baz")),
        // schema with no meta
        Schema::new(vec![f1.clone(), f2.clone()]),
        // field level metadata
        Schema::new(vec![
            f1.clone().with_metadata(make_meta("blarg", "bar")),
            f2.clone(),
        ]),
        // incompatible field level metadata
        Schema::new(vec![
            f1.clone().with_metadata(make_meta("blarg", "baz")),
            f2.clone(),
        ]),
        // schema with no meta
        Schema::new(vec![f1, f2]),
    ];
    write_files(table_dir.as_path(), schemas);

    // can be any order
    let expected = [
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 1  | test |",
        "| 2  | test |",
        "| 3  | test |",
        "| 0  | test |",
        "| 5  | test |",
        "| 4  | test |",
        "+----+------+",
    ];

    // Read the parquet files into a dataframe to confirm results
    // (no errors)
    let table_path = table_dir.to_str().unwrap().to_string();

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(&table_path, options.clone())
        .await
        .unwrap();
    let actual = df.collect().await.unwrap();

    assert_batches_sorted_eq!(expected, &actual);
    assert_no_metadata(&actual);

    // also validate it works via SQL interface as well
    ctx.register_parquet("t", &table_path, options)
        .await
        .unwrap();

    let actual = ctx
        .sql("SELECT * from t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_sorted_eq!(expected, &actual);
    assert_no_metadata(&actual);
}

#[tokio::test]
async fn schema_merge_can_preserve_metadata() {
    // Create several parquet files in same directoty / table with
    // same schema but different metadata
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");

    // explicitly disable schema clearing
    let options = ParquetReadOptions::default().skip_metadata(false);

    let f1 = Field::new("id", DataType::Int32, true);
    let f2 = Field::new("name", DataType::Utf8, true);

    let schemas = vec![
        // schema level metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "bar")),
        // schema different (compatible) metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo2", "baz")),
        // schema with no meta
        Schema::new(vec![f1.clone(), f2.clone()]),
    ];
    write_files(table_dir.as_path(), schemas);

    // can be any order
    let expected = [
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 1  | test |",
        "| 2  | test |",
        "| 0  | test |",
        "+----+------+",
    ];

    let mut expected_metadata = make_meta("foo", "bar");
    expected_metadata.insert("foo2".into(), "baz".into());

    // Read the parquet files into a dataframe to confirm results
    // (no errors)
    let table_path = table_dir.to_str().unwrap().to_string();

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(&table_path, options.clone())
        .await
        .unwrap();

    let actual = df.schema().metadata();
    assert_eq!(actual.clone(), expected_metadata,);

    let actual = df.collect().await.unwrap();

    assert_batches_sorted_eq!(expected, &actual);
    assert_metadata(&actual, &expected_metadata);

    // also validate it works via SQL interface as well
    ctx.register_parquet("t", &table_path, options)
        .await
        .unwrap();

    let df = ctx.sql("SELECT * from t").await.unwrap();

    let actual = df.schema().metadata();
    assert_eq!(actual.clone(), expected_metadata);

    let actual = df.collect().await.unwrap();
    assert_batches_sorted_eq!(expected, &actual);
    assert_metadata(&actual, &expected_metadata);
}

fn make_meta(k: impl Into<String>, v: impl Into<String>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(k.into(), v.into());
    meta
}

/// Writes individual files with the specified schemas to temp_path)
///
/// Assumes each schema has an int32 and a string column
fn write_files(table_path: &Path, schemas: Vec<Schema>) {
    fs::create_dir(table_path).expect("Error creating temp dir");

    for (i, schema) in schemas.into_iter().enumerate() {
        let schema = Arc::new(schema);
        let filename = format!("part-{i}.parquet");
        let path = table_path.join(filename);
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        // create mock record batch
        let ids = Arc::new(Int32Array::from(vec![i as i32]));
        let names = Arc::new(StringArray::from(vec!["test"]));
        let rec_batch = RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();
    }
}

fn assert_no_metadata(batches: &[RecordBatch]) {
    // all batches should have no metadata
    for batch in batches {
        assert!(
            batch.schema().metadata().is_empty(),
            "schema had metadata: {:?}",
            batch.schema()
        );
    }
}

fn assert_metadata(batches: &[RecordBatch], expected_metadata: &HashMap<String, String>) {
    // all batches should have no metadata
    for batch in batches {
        assert_eq!(batch.schema().metadata(), expected_metadata,);
    }
}
