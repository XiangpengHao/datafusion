use std::collections::HashMap;

use itertools::Itertools;
use parquet::{
    arrow::{arrow_reader::RowSelection, builder::ArrowArrayCache},
    file::metadata::ParquetMetaData,
};

use crate::datasource::physical_plan::parquet::RowGroupAccess;

use super::ParquetAccessPlan;

/// Filters a [`ParquetAccessPlan`] based on the cached record batches
///
/// This filter happens after page filter and before we make into overall row selection.
pub struct CacheFilter<'a> {
    column_idx: &'a Vec<usize>, // we don't handle nested columns yet.
}

impl<'a> CacheFilter<'a> {
    pub fn new(column_idx: &'a Vec<usize>) -> Self {
        Self { column_idx }
    }

    /// Returns skipped access plan and cached selection
    /// Cached selection is a map from row group index to a list of row ids
    pub fn prune_plan_with_cached_batches(
        &self,
        mut plan: ParquetAccessPlan,
        parquet_metadata: &ParquetMetaData,
    ) -> (ParquetAccessPlan, HashMap<usize, Vec<usize>>) {
        let groups = parquet_metadata.row_groups();
        if groups.is_empty() {
            return (plan, HashMap::new());
        }

        let mut cached_selection = HashMap::new();
        let row_group_indexes = plan.row_group_indexes();
        let cache = ArrowArrayCache::get();
        for row_group_index in row_group_indexes {
            let current_access = plan.row_group_access(row_group_index);
            if matches!(current_access, RowGroupAccess::Skip) {
                continue;
            }

            let row_group_meta = groups.get(row_group_index).unwrap();

            let Some(intersected_ranges) =
                cache.get_cached_ranges_of_columns(row_group_index, &self.column_idx)
            else {
                continue;
            };
            let sorted_ranges = intersected_ranges
                .into_iter()
                .sorted_by_key(|range| range.start)
                .collect_vec();

            let selection_from_cache = RowSelection::from_consecutive_ranges(
                sorted_ranges.iter().map(|r| r.clone()),
                row_group_meta.num_rows() as usize,
            )
            .into_inverted();

            let cached_row_ids = sorted_ranges.iter().map(|r| r.start).collect_vec();

            let old = cached_selection.insert(row_group_index, cached_row_ids);
            assert!(old.is_none());

            let current_access = plan.row_group_access(row_group_index);
            match current_access {
                RowGroupAccess::Scan => {
                    if !selection_from_cache.selects_any() {
                        plan.skip(row_group_index);
                    } else {
                        plan.scan_selection(row_group_index, selection_from_cache);
                    }
                }
                RowGroupAccess::Selection(_s) => {
                    plan.scan_selection(row_group_index, selection_from_cache);
                }
                RowGroupAccess::Skip => {
                    unreachable!()
                }
            }
        }

        (plan, cached_selection)
    }
}
