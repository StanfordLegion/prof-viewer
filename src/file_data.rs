use std::fs::File;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::data::{
    DataSource, DataSourceDescription, DataSourceInfo, EntryID, SlotMetaTile, SlotMetaTileResult,
    SlotTile, SlotTileResult, SummaryTile, SummaryTileResult, TileID, TileResult,
};
use crate::http::schema::TileRequestRef;

pub struct FileDataSource {
    pub basedir: PathBuf,
}

impl FileDataSource {
    pub fn new(basedir: impl AsRef<Path>) -> Self {
        Self {
            basedir: basedir.as_ref().to_owned(),
        }
    }

    fn read_file<T>(&self, path: impl AsRef<Path>) -> TileResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let f = File::open(path).map_err(|e| e.to_string())?;
        let f = zstd::Decoder::new(f).map_err(|e| e.to_string())?;
        ciborium::from_reader(f).map_err(|e| e.to_string())
    }
}

impl DataSource for FileDataSource {
    fn fetch_description(&self) -> DataSourceDescription {
        DataSourceDescription {
            source_locator: vec![String::from(self.basedir.to_string_lossy())],
        }
    }
    fn fetch_info(&self) -> DataSourceInfo {
        let path = self.basedir.join("info");
        self.read_file::<DataSourceInfo>(&path).unwrap()
    }

    fn fetch_summary_tile(
        &self,
        entry_id: &EntryID,
        tile_id: TileID,
        _full: bool,
    ) -> SummaryTileResult {
        let req = TileRequestRef { entry_id, tile_id };
        let mut path = self.basedir.join("summary_tile");
        path.push(req.to_slug());
        self.read_file::<SummaryTile>(&path)
    }

    fn fetch_slot_tile(&self, entry_id: &EntryID, tile_id: TileID, _full: bool) -> SlotTileResult {
        let req = TileRequestRef { entry_id, tile_id };
        let mut path = self.basedir.join("slot_tile");
        path.push(req.to_slug());
        self.read_file::<SlotTile>(&path)
    }

    fn fetch_slot_meta_tile(
        &self,
        entry_id: &EntryID,
        tile_id: TileID,
        _full: bool,
    ) -> SlotMetaTileResult {
        let req = TileRequestRef { entry_id, tile_id };
        let mut path = self.basedir.join("slot_meta_tile");
        path.push(req.to_slug());
        self.read_file::<SlotMetaTile>(&path)
    }
}
