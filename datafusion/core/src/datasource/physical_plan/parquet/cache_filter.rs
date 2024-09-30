use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use itertools::Itertools;
use parquet::{
    arrow::{
        arrow_reader::{RowSelection, RowSelector},
        builder::ArrowArrayCache,
    },
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
        for row_group_index in row_group_indexes {
            let current_access = plan.row_group_access(row_group_index);
            if matches!(current_access, RowGroupAccess::Skip) {
                continue;
            }

            let row_group_meta = groups.get(row_group_index).unwrap();
            let mut intersected_ranges: Option<HashSet<Range<usize>>> = None;

            for column_idx in self.column_idx.iter() {
                let cache = ArrowArrayCache::get();
                if let Some(cached_ranges) =
                    cache.get_cached_ranges(row_group_index, *column_idx)
                {
                    intersected_ranges = match intersected_ranges {
                        None => Some(cached_ranges),
                        Some(existing_ranges) => {
                            Some(intersect_ranges(existing_ranges, &cached_ranges))
                        }
                    };
                } else {
                    intersected_ranges = None;
                    break;
                }
            }
            let Some(selection) = intersected_ranges else {
                continue;
            };

            let sorted_ranges = selection
                .into_iter()
                .sorted_by_key(|range| range.start)
                .collect_vec();

            let mut selection = Vec::new();
            let starting_row = sorted_ranges.first().unwrap().start;
            let ending_row = sorted_ranges.last().unwrap().end;
            let row_count = row_group_meta.num_rows() as usize;
            if starting_row > 0 {
                selection.push(RowSelector::select(starting_row));
            }
            let mut prev_end = starting_row;
            let mut cached_row_ids = Vec::new();

            for range in sorted_ranges {
                if range.start > prev_end {
                    // Fill the gap with a select
                    selection.push(RowSelector::select(range.start - prev_end));
                }
                selection.push(RowSelector::skip(range.end - range.start));
                cached_row_ids.push(range.start);
                prev_end = range.end;
            }
            if ending_row < row_count {
                selection.push(RowSelector::select(row_count - ending_row));
            }

            let selection = RowSelection::from(selection);
            let old = cached_selection.insert(row_group_index, cached_row_ids);
            assert!(old.is_none());

            let current_access = plan.row_group_access(row_group_index);
            match current_access {
                RowGroupAccess::Scan => {
                    if !selection.selects_any() {
                        plan.skip(row_group_index);
                    } else {
                        plan.scan_selection(row_group_index, selection);
                    }
                }
                RowGroupAccess::Selection(_s) => {
                    plan.scan_selection(row_group_index, selection);
                }
                RowGroupAccess::Skip => {
                    unreachable!()
                }
            }
        }

        (plan, cached_selection)
    }
}

/// Returns the ranges that are present in both `base` and `input`.
/// Ranges in both sets are assumed to be non-overlapping.
///
/// The returned ranges are exactly those that appear in both input sets,
/// preserving their original bounds for use in cache retrieval.
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use std::ops::Range;
///
/// let base: HashSet<Range<usize>> = vec![0..10, 20..30, 40..45].into_iter().collect();
/// let input: HashSet<Range<usize>> = vec![0..10, 25..35, 40..45].into_iter().collect();
/// let result = intersect_ranges(base, input);
/// assert_eq!(result, vec![0..10, 40..45].into_iter().collect::<HashSet<_>>());
/// ```
fn intersect_ranges(
    mut base: HashSet<Range<usize>>,
    input: &HashSet<Range<usize>>,
) -> HashSet<Range<usize>> {
    for range in input {
        if base.contains(range) {
            continue;
        } else {
            base.remove(range);
        }
    }
    base
}
