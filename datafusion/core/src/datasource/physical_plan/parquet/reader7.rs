use crate::datasource::physical_plan::{FileMeta, ParquetFileMetrics};
use bytes::Bytes;
use datafusion_execution::cache::cache_unit::Cache37;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::ParquetMetaData;
use std::ops::Range;
use std::sync::Arc;

use super::ParquetFileReaderFactory;

/// Doc
#[derive(Debug)]
pub struct Parquet7FileReaderFactory {
    store: Arc<dyn ObjectStore>,
}

impl Parquet7FileReaderFactory {
    /// Doc
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl ParquetFileReaderFactory for Parquet7FileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );
        let store = Arc::clone(&self.store);
        let mut inner = ParquetObjectReader::new(store, file_meta.object_meta);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        Ok(Box::new(Parquet7FileReader {
            inner,
            file_metrics,
        }))
    }
}

/// doc
pub struct Parquet7FileReader {
    /// doc
    pub file_metrics: ParquetFileMetrics,
    /// doc
    pub inner: ParquetObjectReader,
}

impl AsyncFileReader for Parquet7FileReader {
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let total = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total);

        let cache = Cache37::bytes_cache();
        let path = self.inner.meta.location.clone();

        let mut cached_bytes = Vec::new();
        let mut missing_ranges = Vec::new();
        let mut missing_indices = Vec::new();

        for (i, range) in ranges.iter().enumerate() {
            let key = (path.clone(), range.clone());
            if let Some(bytes) = cache.get(&key) {
                cached_bytes.push((i, bytes.clone()));
            } else {
                missing_ranges.push(range.clone());
                missing_indices.push(i);
            }
        }

        if missing_ranges.is_empty() {
            cached_bytes.sort_by_key(|&(i, _)| i);
            let result = cached_bytes
                .into_iter()
                .map(|(_, bytes)| (*bytes).clone())
                .collect();
            return async move { Ok(result) }.boxed();
        }

        let get_bytes = self.inner.get_byte_ranges(missing_ranges);
        async move {
            let bytes = get_bytes.await?;
            let cache = Cache37::bytes_cache();

            for (i, byte) in missing_indices.iter().zip(bytes.iter()) {
                let key = (path.clone(), ranges[*i].clone());
                cache.put(key, Arc::new(byte.clone()));
            }

            let mut result = vec![Bytes::new(); ranges.len()];
            for (i, bytes) in cached_bytes {
                result[i] = (*bytes).clone();
            }
            for (i, byte) in missing_indices.into_iter().zip(bytes) {
                result[i] = byte;
            }
            Ok(result)
        }
        .boxed()
    }

    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics.bytes_scanned.add(range.end - range.start);

        let cache = Cache37::bytes_cache();
        let path = self.inner.meta.location.clone();
        let key = (path.clone(), range.clone());

        if let Some(bytes) = cache.get(&key) {
            let bytes = bytes.clone();
            return async move { Ok((*bytes).clone()) }.boxed();
        }

        let get_bytes = self.inner.get_bytes(range.clone());
        async move {
            let bytes = get_bytes.await?;
            let bytes = Arc::new(bytes);
            let cache = Cache37::bytes_cache();
            cache.put(key, bytes.clone());
            Ok((*bytes).clone())
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let cache = Cache37::meta_cache().read().unwrap();
        let path = &self.inner.meta.location;

        if let Some(meta) = cache.get(path) {
            let meta = meta.clone();
            return async move { Ok(meta) }.boxed();
        }

        drop(cache);

        let path = self.inner.meta.location.clone();
        let get_meta = self.inner.get_metadata();
        async move {
            let meta = get_meta.await?;
            let mut cache = Cache37::meta_cache().write().unwrap();
            cache.entry(path).or_insert(meta.clone());
            Ok(meta)
        }
        .boxed()
    }
}
