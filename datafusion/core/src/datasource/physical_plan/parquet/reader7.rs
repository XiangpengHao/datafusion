use crate::datasource::physical_plan::{FileMeta, ParquetFileMetrics};
use bytes::Bytes;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use futures::FutureExt;
use hashbrown::HashMap;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::ParquetMetaData;
use std::ops::Range;
use std::sync::{Arc, LazyLock, RwLock};

use super::ParquetFileReaderFactory;

static META_DATA_CACHE: LazyLock<RwLock<MetadataCache>> =
    LazyLock::new(|| RwLock::new(MetadataCache::new()));

pub struct MetadataCache {
    map: HashMap<Path, Arc<ParquetMetaData>>,
}

impl MetadataCache {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn get() -> &'static RwLock<MetadataCache> {
        &*META_DATA_CACHE
    }
}

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
        self.inner.get_byte_ranges(ranges)
    }

    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics.bytes_scanned.add(range.end - range.start);
        self.inner.get_bytes(range)
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let cache = MetadataCache::get().read().unwrap();
        let path = &self.inner.meta.location;

        if let Some(meta) = cache.map.get(path) {
            let meta = meta.clone();
            return async move { Ok(meta) }.boxed();
        }

        let path = self.inner.meta.location.clone();
        let get_meta = self.inner.get_metadata();
        async move {
            let meta = get_meta.await?;
            let mut meta_cache = MetadataCache::get().write().unwrap();
            meta_cache.map.entry(path).or_insert(meta.clone());
            Ok(meta)
        }
        .boxed()
    }
}
