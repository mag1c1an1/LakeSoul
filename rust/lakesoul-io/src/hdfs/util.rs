// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

// this file is adopted from arrow-rs to make some methods publicly callable

//! Common logic for interacting with remote object stores
use bytes::Bytes;
use futures::{TryStreamExt, stream::StreamExt};

#[cfg(not(target_arch = "wasm32"))]
/// Takes a function and spawns it to a tokio blocking pool if available
pub async fn maybe_spawn_blocking<F, T>(f: F) -> object_store::Result<T>
where
    F: FnOnce() -> object_store::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime.spawn_blocking(f).await?,
        Err(_) => f(),
    }
}

/// Range requests with a gap less than or equal to this,
/// will be coalesced into a single request by [`coalesce_ranges`]
pub const OBJECT_STORE_COALESCE_DEFAULT: u64 = 1024 * 1024;

/// Up to this number of range requests will be performed in parallel by [`coalesce_ranges`]
pub const OBJECT_STORE_COALESCE_PARALLEL: usize = 4;
pub const OBJECT_STORE_COALESCE_MAX: u64 = 16 * 1024 * 1024;

/// Takes a function `fetch` that can fetch a range of bytes and uses this to
/// fetch the provided byte `ranges`
///
/// To improve performance it will:
///
/// * Combine ranges less than `coalesce` bytes apart into a single call to `fetch`
/// * Make multiple `fetch` requests in parallel (up to maximum of 10)
///
pub async fn coalesce_ranges<F, Fut>(
    ranges: &[std::ops::Range<u64>],
    fetch: F,
    coalesce: u64,
) -> object_store::Result<Vec<Bytes>>
where
    F: Send + FnMut(std::ops::Range<u64>) -> Fut,
    Fut: std::future::Future<Output = object_store::Result<Bytes>> + Send,
{
    let fetch_ranges = merge_ranges(ranges, coalesce, OBJECT_STORE_COALESCE_MAX);

    let fetched: Vec<_> = futures::stream::iter(fetch_ranges.iter().cloned())
        .map(fetch)
        .buffered(OBJECT_STORE_COALESCE_PARALLEL)
        .try_collect()
        .await?;

    Ok(ranges
        .iter()
        .map(|range| {
            let idx = fetch_ranges.partition_point(|v| v.start <= range.start) - 1;
            let fetch_range = &fetch_ranges[idx];
            let fetch_bytes = &fetched[idx];

            let start = range.start - fetch_range.start;
            let end = range.end - fetch_range.start;
            fetch_bytes.slice(start as usize..end as usize)
        })
        .collect())
}

/// Returns a sorted list of ranges that cover `ranges`
fn merge_ranges(
    ranges: &[std::ops::Range<u64>],
    coalesce: u64,
    range_max_size: u64,
) -> Vec<std::ops::Range<u64>> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| range.start);

    let mut ret = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != ranges.len() {
        let range_begin = ranges[start_idx].start;
        let mut range_end = ranges[start_idx].end;

        while end_idx != ranges.len()
            && ranges[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
            if range_end - range_begin >= range_max_size {
                break;
            }
        }

        let start = ranges[start_idx].start;
        let end = range_end;
        ret.push(start..end);

        start_idx = end_idx;
        end_idx += 1;
    }

    ret
}
