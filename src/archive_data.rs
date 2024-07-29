use std::fs::{create_dir, remove_dir_all, File};
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::data::{DataSourceInfo, EntryID, EntryIDSlug, EntryIndex, EntryInfo, TileID, TileSet};
use crate::deferred_data::{CountingDeferredDataSource, DeferredDataSource};
use crate::http::schema::TileRequestRef;
use crate::timestamp::{Interval, Timestamp};

pub struct DataSourceArchiveWriter<T: DeferredDataSource> {
    data_source: CountingDeferredDataSource<T>,
    levels: u32,
    branch_factor: u64,
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

impl<T: DeferredDataSource> DataSourceArchiveWriter<T> {
    pub fn new(
        data_source: T,
        levels: u32,
        branch_factor: u64,
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
            path: path.as_ref().to_owned(),
            force,
            zstd_compression,
        }
    }

    fn check_info(&mut self) -> Option<DataSourceInfo> {
        // We requested this once, so we know we'll get zero or one result
        self.data_source.get_infos().pop()
    }

    fn write_info(&mut self, info: DataSourceInfo, scope: &rayon::Scope<'_>) {
        let path = self.path.join("info");
        spawn_write(path, info, self.zstd_compression, scope);
    }

    fn write_summary_tiles(&mut self, scope: &rayon::Scope<'_>) {
        for tile in self.data_source.get_summary_tiles() {
            let mut path = self.path.join("summary_tile");
            let req = TileRequestRef {
                entry_id: &tile.entry_id,
                tile_id: tile.tile_id,
            };
            path.push(req.to_slug());
            spawn_write(path, tile, self.zstd_compression, scope);
        }
    }

    fn write_slot_tiles(&mut self, scope: &rayon::Scope<'_>) {
        for tile in self.data_source.get_slot_tiles() {
            let mut path = self.path.join("slot_tile");
            let req = TileRequestRef {
                entry_id: &tile.entry_id,
                tile_id: tile.tile_id,
            };
            path.push(req.to_slug());
            spawn_write(path, tile, self.zstd_compression, scope);
        }
    }

    fn write_slot_meta_tiles(&mut self, scope: &rayon::Scope<'_>) {
        for tile in self.data_source.get_slot_meta_tiles() {
            let mut path = self.path.join("slot_meta_tile");
            let req = TileRequestRef {
                entry_id: &tile.entry_id,
                tile_id: tile.tile_id,
            };
            path.push(req.to_slug());
            spawn_write(path, tile, self.zstd_compression, scope);
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
        let mut info = info.unwrap();

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

        for level in 0..self.levels {
            let num_tiles = self.branch_factor.pow(level) as i64;
            let duration = info.interval.duration_ns();
            let tile_ids: Vec<_> = (0..num_tiles)
                .map(|i| {
                    let start = Timestamp(duration * i / num_tiles);
                    let stop = Timestamp(duration * (i + 1) / num_tiles);
                    TileID(Interval::new(start, stop))
                })
                .collect();
            tile_set.push(tile_ids);
        }

        info.tile_set = TileSet {
            tiles: tile_set.clone(),
        };

        rayon::in_place_scope(|s| {
            self.write_info(info, s);
        });

        for level in 0..self.levels {
            let tile_ids = &tile_set[level as usize];
            let full = level == self.levels - 1;

            println!("Writing level {} with {} tiles", level, tile_ids.len());

            const MAX_IN_FLIGHT_REQUESTS: u64 = 100;

            for entry_id in &entry_ids {
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
                            self.data_source
                                .fetch_slot_meta_tile(entry_id, *tile_id, full);
                        }
                    }
                }

                // Bound the number of in-flight requests so we don't use too much memory.
                rayon::in_place_scope(|s| {
                    while self.data_source.outstanding_requests() > MAX_IN_FLIGHT_REQUESTS {
                        self.write_summary_tiles(s);
                        self.write_slot_tiles(s);
                        self.write_slot_meta_tiles(s);
                    }
                });
            }
        }

        rayon::in_place_scope(|s| {
            while self.data_source.outstanding_requests() > 0 {
                self.write_summary_tiles(s);
                self.write_slot_tiles(s);
                self.write_slot_meta_tiles(s);
            }
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
