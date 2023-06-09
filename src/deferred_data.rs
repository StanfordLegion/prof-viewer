use crate::data::{
    DataSourceInfo, DataSourceMut, EntryID, SlotMetaTile, SlotTile, SummaryTile, TileID, TileSet,
};

pub trait DeferredDataSource {
    fn fetch_info(&mut self);
    fn get_infos(&mut self) -> Vec<DataSourceInfo>;
    fn fetch_tile_set(&mut self);
    fn get_tile_sets(&mut self) -> Vec<TileSet>;
    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID);
    fn get_summary_tiles(&mut self) -> Vec<SummaryTile>;
    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID);
    fn get_slot_tiles(&mut self) -> Vec<SlotTile>;
    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID);
    fn get_slot_meta_tiles(&mut self) -> Vec<SlotMetaTile>;
}

pub struct DeferredDataSourceWrapper {
    data_source: Box<dyn DataSourceMut>,
    infos: Vec<DataSourceInfo>,
    tile_sets: Vec<TileSet>,
    summary_tiles: Vec<SummaryTile>,
    slot_tiles: Vec<SlotTile>,
    slot_meta_tiles: Vec<SlotMetaTile>,
}

impl DeferredDataSourceWrapper {
    pub fn new(data_source: Box<dyn DataSourceMut>) -> Self {
        Self {
            data_source,
            infos: Vec::new(),
            tile_sets: Vec::new(),
            summary_tiles: Vec::new(),
            slot_tiles: Vec::new(),
            slot_meta_tiles: Vec::new(),
        }
    }
}

impl DeferredDataSource for DeferredDataSourceWrapper {
    fn fetch_info(&mut self) {
        self.infos.push(self.data_source.fetch_info());
    }

    fn get_infos(&mut self) -> Vec<DataSourceInfo> {
        std::mem::take(&mut self.infos)
    }

    fn fetch_tile_set(&mut self) {
        self.tile_sets.push(self.data_source.fetch_tile_set());
    }

    fn get_tile_sets(&mut self) -> Vec<TileSet> {
        std::mem::take(&mut self.tile_sets)
    }

    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID) {
        self.summary_tiles
            .push(self.data_source.fetch_summary_tile(entry_id, tile_id));
    }

    fn get_summary_tiles(&mut self) -> Vec<SummaryTile> {
        std::mem::take(&mut self.summary_tiles)
    }

    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID) {
        self.slot_tiles
            .push(self.data_source.fetch_slot_tile(entry_id, tile_id));
    }

    fn get_slot_tiles(&mut self) -> Vec<SlotTile> {
        std::mem::take(&mut self.slot_tiles)
    }

    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID) {
        self.slot_meta_tiles
            .push(self.data_source.fetch_slot_meta_tile(entry_id, tile_id));
    }

    fn get_slot_meta_tiles(&mut self) -> Vec<SlotMetaTile> {
        std::mem::take(&mut self.slot_meta_tiles)
    }
}
