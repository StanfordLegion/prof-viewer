use crate::data::{
    DataSource, DataSourceDescription, DataSourceInfo, EntryID, SlotMetaTile, SlotTile,
    SummaryTile, TileID,
};

pub trait DeferredDataSource {
    fn fetch_description(&self) -> DataSourceDescription;
    fn fetch_info(&mut self);
    fn get_infos(&mut self) -> Vec<DataSourceInfo>;
    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool);
    fn get_summary_tiles(&mut self) -> Vec<(SummaryTile, bool)>;
    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool);
    fn get_slot_tiles(&mut self) -> Vec<(SlotTile, bool)>;
    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool);
    fn get_slot_meta_tiles(&mut self) -> Vec<(SlotMetaTile, bool)>;
}

pub struct DeferredDataSourceWrapper<T: DataSource> {
    data_source: T,
    infos: Vec<DataSourceInfo>,
    summary_tiles: Vec<(SummaryTile, bool)>,
    slot_tiles: Vec<(SlotTile, bool)>,
    slot_meta_tiles: Vec<(SlotMetaTile, bool)>,
}

impl<T: DataSource> DeferredDataSourceWrapper<T> {
    pub fn new(data_source: T) -> Self {
        Self {
            data_source,
            infos: Vec::new(),
            summary_tiles: Vec::new(),
            slot_tiles: Vec::new(),
            slot_meta_tiles: Vec::new(),
        }
    }
}

impl<T: DataSource> DeferredDataSource for DeferredDataSourceWrapper<T> {
    fn fetch_description(&self) -> DataSourceDescription {
        self.data_source.fetch_description()
    }

    fn fetch_info(&mut self) {
        self.infos.push(self.data_source.fetch_info());
    }

    fn get_infos(&mut self) -> Vec<DataSourceInfo> {
        std::mem::take(&mut self.infos)
    }

    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.summary_tiles.push((
            self.data_source.fetch_summary_tile(entry_id, tile_id, full),
            full,
        ));
    }

    fn get_summary_tiles(&mut self) -> Vec<(SummaryTile, bool)> {
        std::mem::take(&mut self.summary_tiles)
    }

    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.slot_tiles.push((
            self.data_source.fetch_slot_tile(entry_id, tile_id, full),
            full,
        ));
    }

    fn get_slot_tiles(&mut self) -> Vec<(SlotTile, bool)> {
        std::mem::take(&mut self.slot_tiles)
    }

    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.slot_meta_tiles.push((
            self.data_source
                .fetch_slot_meta_tile(entry_id, tile_id, full),
            full,
        ));
    }

    fn get_slot_meta_tiles(&mut self) -> Vec<(SlotMetaTile, bool)> {
        std::mem::take(&mut self.slot_meta_tiles)
    }
}

pub struct CountingDeferredDataSource<T: DeferredDataSource> {
    data_source: T,
    outstanding_requests: u64,
}

impl<T: DeferredDataSource> CountingDeferredDataSource<T> {
    pub fn new(data_source: T) -> Self {
        Self {
            data_source,
            outstanding_requests: 0,
        }
    }

    pub fn outstanding_requests(&self) -> u64 {
        self.outstanding_requests
    }

    fn start_request(&mut self) {
        self.outstanding_requests += 1;
    }

    fn finish_request<E>(&mut self, result: Vec<E>) -> Vec<E> {
        let count = result.len() as u64;
        assert!(self.outstanding_requests >= count);
        self.outstanding_requests -= count;
        result
    }
}

impl<T: DeferredDataSource> DeferredDataSource for CountingDeferredDataSource<T> {
    fn fetch_description(&self) -> DataSourceDescription {
        self.data_source.fetch_description()
    }

    fn fetch_info(&mut self) {
        self.start_request();
        self.data_source.fetch_info()
    }

    fn get_infos(&mut self) -> Vec<DataSourceInfo> {
        let result = self.data_source.get_infos();
        self.finish_request(result)
    }

    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.start_request();
        self.data_source.fetch_summary_tile(entry_id, tile_id, full)
    }

    fn get_summary_tiles(&mut self) -> Vec<(SummaryTile, bool)> {
        let result = self.data_source.get_summary_tiles();
        self.finish_request(result)
    }

    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.start_request();
        self.data_source.fetch_slot_tile(entry_id, tile_id, full)
    }

    fn get_slot_tiles(&mut self) -> Vec<(SlotTile, bool)> {
        let result = self.data_source.get_slot_tiles();
        self.finish_request(result)
    }

    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.start_request();
        self.data_source
            .fetch_slot_meta_tile(entry_id, tile_id, full)
    }

    fn get_slot_meta_tiles(&mut self) -> Vec<(SlotMetaTile, bool)> {
        let result = self.data_source.get_slot_meta_tiles();
        self.finish_request(result)
    }
}

impl DeferredDataSource for Box<dyn DeferredDataSource> {
    fn fetch_description(&self) -> DataSourceDescription {
        self.as_ref().fetch_description()
    }

    fn fetch_info(&mut self) {
        self.as_mut().fetch_info()
    }

    fn get_infos(&mut self) -> Vec<DataSourceInfo> {
        self.as_mut().get_infos()
    }

    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.as_mut().fetch_summary_tile(entry_id, tile_id, full)
    }

    fn get_summary_tiles(&mut self) -> Vec<(SummaryTile, bool)> {
        self.as_mut().get_summary_tiles()
    }

    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.as_mut().fetch_slot_tile(entry_id, tile_id, full)
    }

    fn get_slot_tiles(&mut self) -> Vec<(SlotTile, bool)> {
        self.as_mut().get_slot_tiles()
    }

    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        self.as_mut().fetch_slot_meta_tile(entry_id, tile_id, full)
    }

    fn get_slot_meta_tiles(&mut self) -> Vec<(SlotMetaTile, bool)> {
        self.as_mut().get_slot_meta_tiles()
    }
}
