use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, create_dir, remove_dir_all};
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::data::{
    self, DataSourceInfo, EntryID, EntryIDSlug, EntryIndex, EntryInfo, Field, FieldID,
    SlotMetaTile, SlotTile, SummaryTile, TileID, TileSet,
};
use crate::deferred_data::{CountingDeferredDataSource, DeferredDataSource};
use crate::http::schema::TileRequestRef;
use crate::timestamp::{Interval, Timestamp};

pub struct DataSourceArchiveWriter<T: DeferredDataSource> {
    data_source: CountingDeferredDataSource<T>,
    levels: u32,
    branch_factor: u64,
    min_tile_size: u64,
    path: PathBuf,
    force: bool,
    zstd_compression: i32,
}

fn create_unique_dir<P: AsRef<Path>>(path: P, force: bool) -> io::Result<PathBuf> {
    let mut path = path.as_ref().to_owned();
    if force {
        println!("Removing previous contents of {:?}", &path);
        let _ = remove_dir_all(&path); // ignore failure, we'll catch it on create
        create_dir(&path)?;
    } else if create_dir(&path).is_err() {
        let mut i = 1;
        let retry_limit = 100;
        loop {
            let mut f = path.file_name().unwrap().to_owned();
            f.push(format!(".{}", i));
            let p = path.with_file_name(f);
            let r = create_dir(&p);
            if r.is_ok() {
                path.clone_from(&p);
                break;
            } else if i >= retry_limit {
                // tried too many times, assume this is a permanent failure
                r?;
            }
            i += 1;
        }
    }
    Ok(path)
}

fn write_data<T>(path: PathBuf, data: T, zstd_compression: i32) -> io::Result<()>
where
    T: Serialize,
{
    let mut f = zstd::Encoder::new(File::create(path)?, zstd_compression)?;
    ciborium::into_writer(&data, &mut f).expect("ciborium encoding failed");
    f.finish()?;
    Ok(())
}

fn spawn_write<T>(path: PathBuf, data: T, zstd_compression: i32, scope: &rayon::Scope<'_>)
where
    T: Serialize + Send + Sync + 'static,
{
    scope.spawn(move |_| {
        // FIXME (Elliott): is there a better way to handle I/O failure?
        write_data(path, data, zstd_compression).unwrap();
    });
}

fn walk_entry_list(info: &EntryInfo) -> Vec<EntryID> {
    let mut result = Vec::new();
    fn walk(info: &EntryInfo, entry_id: EntryID, result: &mut Vec<EntryID>) {
        match info {
            EntryInfo::Panel { summary, slots, .. } => {
                if let Some(summary) = summary {
                    walk(summary, entry_id.summary(), result);
                }
                for (i, slot) in slots.iter().enumerate() {
                    walk(slot, entry_id.child(i as u64), result)
                }
            }
            EntryInfo::Slot { .. } => {
                result.push(entry_id);
            }
            EntryInfo::Summary { .. } => {
                result.push(entry_id);
            }
        }
    }
    walk(info, EntryID::root(), &mut result);
    result
}

fn compute_tile_size(tile: &SlotMetaTile, num_items_field: FieldID, min_tile_size: u64) -> u64 {
    let mut result: u64 = 0;
    for row in &tile.data.items {
        for item in row {
            if let Some(num_items) = item.fields.iter().find(|f| f.0 == num_items_field) {
                let Field::U64(count) = num_items.1 else {
                    panic!("Expected Field::U64 value in num_items_field");
                };
                result += count;
            } else {
                result += 1;
            }

            // Once we exceed the min_tile_size we don't care about the result,
            // so just return.
            if result > min_tile_size {
                return result;
            }
        }
    }
    result
}

impl<T: DeferredDataSource> DataSourceArchiveWriter<T> {
    pub fn new(
        data_source: T,
        levels: u32,
        branch_factor: u64,
        min_tile_size: u64,
        path: impl AsRef<Path>,
        force: bool,
        zstd_compression: i32,
    ) -> Self {
        assert!(levels >= 1);
        assert!(branch_factor >= 2);
        Self {
            data_source: CountingDeferredDataSource::new(data_source),
            levels,
            branch_factor,
            min_tile_size,
            path: path.as_ref().to_owned(),
            force,
            zstd_compression,
        }
    }

    fn check_info(&mut self) -> Option<data::Result<DataSourceInfo>> {
        // We requested this once, so we know we'll get zero or one result
        self.data_source.get_infos().pop()
    }

    fn write_info(&self, info: DataSourceInfo, scope: &rayon::Scope<'_>) {
        let path = self.path.join("info");
        spawn_write(path, info, self.zstd_compression, scope);
    }

    fn write_summary_tile(&self, tile: SummaryTile, scope: &rayon::Scope<'_>) {
        let mut path = self.path.join("summary_tile");
        let req = TileRequestRef {
            entry_id: &tile.entry_id,
            tile_id: tile.tile_id,
        };
        path.push(req.to_slug());
        spawn_write(path, tile, self.zstd_compression, scope);
    }

    fn write_slot_tile(&self, tile: SlotTile, scope: &rayon::Scope<'_>) {
        let mut path = self.path.join("slot_tile");
        let req = TileRequestRef {
            entry_id: &tile.entry_id,
            tile_id: tile.tile_id,
        };
        path.push(req.to_slug());
        spawn_write(path, tile, self.zstd_compression, scope);
    }

    fn write_slot_meta_tile(&self, tile: SlotMetaTile, scope: &rayon::Scope<'_>) {
        let mut path = self.path.join("slot_meta_tile");
        let req = TileRequestRef {
            entry_id: &tile.entry_id,
            tile_id: tile.tile_id,
        };
        path.push(req.to_slug());
        spawn_write(path, tile, self.zstd_compression, scope);
    }

    fn progress_summary_tiles(&mut self, scope: &rayon::Scope<'_>) {
        for (tile, _) in self.data_source.get_summary_tiles() {
            let tile = tile.expect("writing summary tile failed");
            self.write_summary_tile(tile, scope);
        }
    }

    fn progress_slot_tiles(&mut self, scope: &rayon::Scope<'_>) {
        for (tile, _) in self.data_source.get_slot_tiles() {
            let tile = tile.expect("writing slot tile failed");
            self.write_slot_tile(tile, scope);
        }
    }

    fn progress_slot_meta_tiles(&mut self, scope: &rayon::Scope<'_>) {
        for (tile, _) in self.data_source.get_slot_meta_tiles() {
            let tile = tile.expect("writing slot meta tile failed");
            self.write_slot_meta_tile(tile, scope);
        }
    }

    fn progress_slot_meta_tiles_over_size(
        &mut self,
        num_items_field: FieldID,
        min_tile_size: u64,
        full: bool,
        scope: &rayon::Scope<'_>,
    ) -> Vec<(TileID, u64, Option<SlotMetaTile>)> {
        let mut result = Vec::new();
        for (tile, _) in self.data_source.get_slot_meta_tiles() {
            let tile = tile.expect("writing slot meta tile failed");

            let size = compute_tile_size(&tile, num_items_field, min_tile_size);
            if !full && size <= min_tile_size {
                // Don't write it now in case we want to request the full tile
                result.push((tile.tile_id, size, Some(tile)));
            } else {
                result.push((tile.tile_id, size, None));
                self.write_slot_meta_tile(tile, scope);
            }
        }
        result
    }

    fn progress_all_remaining(&mut self, min_in_flight_requests: u64, scope: &rayon::Scope<'_>) {
        while self.data_source.outstanding_requests() > min_in_flight_requests {
            self.progress_summary_tiles(scope);
            self.progress_slot_tiles(scope);
            self.progress_slot_meta_tiles(scope);
        }
    }

    fn generate_entry_tiles<'a>(
        &mut self,
        entry_ids: &[EntryID],
        tile_ids: impl IntoIterator<Item = &'a TileID> + std::marker::Copy,
        slot_meta: bool,
        full: bool,
        min_in_flight_requests: u64,
        scope: &rayon::Scope<'_>,
    ) {
        for entry_id in entry_ids {
            match entry_id.last_index().unwrap() {
                EntryIndex::Summary => {
                    for tile_id in tile_ids {
                        self.data_source
                            .fetch_summary_tile(entry_id, *tile_id, full);
                    }
                }
                EntryIndex::Slot(..) => {
                    for tile_id in tile_ids {
                        self.data_source.fetch_slot_tile(entry_id, *tile_id, full);
                        if slot_meta {
                            self.data_source
                                .fetch_slot_meta_tile(entry_id, *tile_id, full);
                        }
                    }
                }
            }

            // Bound the number of in-flight requests so we don't use too much memory.
            self.progress_all_remaining(min_in_flight_requests, scope);
        }
    }

    pub fn write(mut self) -> io::Result<()> {
        self.path = create_unique_dir(&self.path, self.force)?;
        println!("Created output directory {:?}", &self.path);
        create_dir(self.path.join("summary_tile"))?;
        create_dir(self.path.join("slot_tile"))?;
        create_dir(self.path.join("slot_meta_tile"))?;

        self.data_source.fetch_info();
        let mut info = None;
        while info.is_none() {
            info = self.check_info();
        }
        let mut info = info.unwrap().expect("fetch_info failed");

        let num_items_field = info
            .field_schema
            .get_id("Number of Items")
            .expect("Cannot archive a DataSource unless it has a Number of Items field");

        let entry_ids = walk_entry_list(&info.entry_info);
        for entry_id in &entry_ids {
            let entry_dir = format!("{}", EntryIDSlug(entry_id));
            match entry_id.last_index().unwrap() {
                EntryIndex::Summary => {
                    create_dir(self.path.join("summary_tile").join(&entry_dir))?;
                }
                EntryIndex::Slot(..) => {
                    create_dir(self.path.join("slot_tile").join(&entry_dir))?;
                    create_dir(self.path.join("slot_meta_tile").join(&entry_dir))?;
                }
            }
        }

        // For now, this only works on dynamic data sources
        assert!(info.tile_set.tiles.is_empty());

        let mut tile_set = Vec::new();

        let mut last_level: Vec<TileID> = Vec::new(); //TileID(info.interval)];
        let mut last_level_size: Vec<u64> = Vec::new(); //vec![u64::MAX];

        for level in 0..self.levels {
            let tile_ids = if last_level.is_empty() {
                vec![TileID(info.interval)]
            } else {
                last_level
                    .iter()
                    .zip(last_level_size.iter())
                    .flat_map(|(&tile, &size)| {
                        if size <= self.min_tile_size {
                            vec![tile]
                        } else {
                            let branch_factor = self.branch_factor as i64;
                            let duration = tile.0.duration_ns();
                            (0..branch_factor)
                                .map(|i| {
                                    let start = Timestamp(duration * i / branch_factor);
                                    let stop = Timestamp(duration * (i + 1) / branch_factor);
                                    TileID(Interval::new(start, stop).translate(tile.0.start.0))
                                })
                                .collect()
                        }
                    })
                    .collect()
            };

            let fresh_tile_ids: Vec<_> = tile_ids
                .iter()
                .filter(|tile| last_level.binary_search(tile).is_err())
                .copied()
                .collect();

            if fresh_tile_ids.is_empty() {
                break;
            }

            let full = level == self.levels - 1;

            println!(
                "Writing level {} with {} tiles",
                level,
                fresh_tile_ids.len()
            );

            // We're going to do a three-pass algorithm:
            //
            //  1. Fetch meta tiles. If they're big enough write them right
            //     away.
            //
            //  2. If any tile is too small, we need to look at the set as a
            //     whole to figure out the maximum tile size in this
            //     interval. That's because we only have a single tile set for
            //     the entire entry tree. If ALL the tiles are below the
            //     threshold, throw away and refetch all tiles so that we have
            //     a complete, full tile for the last level of the tile set.
            //
            //  3. Otherwise at least one tile is above the threshold so
            //     proceed to write everything out (even if some tiles are
            //     below the threshold) and then fetch and write all the
            //     slot/summary tiles as well.
            //
            // This preserves the property that we minimize refetch (we refetch
            // tiles exactly when we reach the finest level of detail we're
            // going to render) and never fetches a tile twice otherwise.

            const MIN_IN_FLIGHT_REQUESTS: u64 = 100;

            // Initial fetch of meta tiles to compute sizes
            let mut result_sizes = Vec::new();
            rayon::in_place_scope(|s| {
                for entry_id in &entry_ids {
                    match entry_id.last_index().unwrap() {
                        EntryIndex::Summary => {}
                        EntryIndex::Slot(..) => {
                            for tile_id in &fresh_tile_ids {
                                self.data_source
                                    .fetch_slot_meta_tile(entry_id, *tile_id, full);
                            }
                        }
                    }

                    // Bound the number of in-flight requests so we don't use too much memory.
                    while self.data_source.outstanding_requests() > MIN_IN_FLIGHT_REQUESTS {
                        result_sizes.extend(self.progress_slot_meta_tiles_over_size(
                            num_items_field,
                            self.min_tile_size,
                            full,
                            s,
                        ));
                    }
                }

                while self.data_source.outstanding_requests() > 0 {
                    result_sizes.extend(self.progress_slot_meta_tiles_over_size(
                        num_items_field,
                        self.min_tile_size,
                        full,
                        s,
                    ));
                }
            });

            let mut max_size = BTreeMap::new();
            let mut unwritten_tiles = BTreeMap::new();
            for (tile, size, unwritten_tile) in result_sizes {
                max_size
                    .entry(tile)
                    .and_modify(|s| *s = max(*s, size))
                    .or_insert(size);
                let save = unwritten_tiles.entry(tile).or_insert_with(Vec::new);
                if let Some(t) = unwritten_tile {
                    save.push(t);
                }
            }

            last_level_size = tile_ids
                .iter()
                .map(|tile| {
                    if let Some(size) = max_size.get(tile) {
                        *size
                    } else {
                        last_level_size[last_level.binary_search(tile).unwrap()]
                    }
                })
                .collect();
            last_level = tile_ids.clone();

            rayon::in_place_scope(|s| {
                // Figure out which tiles to refetch as full (and if refetch
                // is not required, write the copy we already have)
                let mut refetch_full_tile_ids = BTreeSet::new();
                for (tile, size) in max_size {
                    let unwritten = unwritten_tiles.remove(&tile).unwrap();
                    assert!(!unwritten.is_empty() || full);
                    if size <= self.min_tile_size {
                        refetch_full_tile_ids.insert(tile);
                    } else {
                        for t in unwritten {
                            self.write_slot_meta_tile(t, s);
                        }
                    }
                }

                let fetch_partial_tile_ids: Vec<_> = tile_ids
                    .iter()
                    .filter(|tile| !refetch_full_tile_ids.contains(tile))
                    .copied()
                    .collect();

                // Fetch partial tiles
                self.generate_entry_tiles(
                    &entry_ids,
                    &fetch_partial_tile_ids,
                    false,
                    full,
                    MIN_IN_FLIGHT_REQUESTS,
                    s,
                );

                // Refetch full tiles
                self.generate_entry_tiles(
                    &entry_ids,
                    &refetch_full_tile_ids,
                    true,
                    true,
                    MIN_IN_FLIGHT_REQUESTS,
                    s,
                );

                self.progress_all_remaining(0, s);
            });

            tile_set.push(tile_ids);
        }

        info.tile_set = TileSet { tiles: tile_set };

        rayon::in_place_scope(|s| {
            self.write_info(info, s);
        });

        std::fs::write(
            self.path.join("index.html"),
            "<html>
<script>
window.onload = function() {
  var prof = location
  if(location.protocol !== 'https:') {
    prof = location.replace(`https:${location.href.substring(location.protocol.length)}`);
  }
  window.location.replace(\"https://legion.stanford.edu/prof-viewer/?url=\"+prof.href);
}
</script>
</html>
",
        )?;

        Ok(())
    }
}
