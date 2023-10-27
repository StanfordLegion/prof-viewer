use std::collections::VecDeque;

use crate::data::{
    DataSourceDescription, DataSourceInfo, EntryID, EntryIndex, EntryInfo, Field, ItemLink,
    ItemUID, SlotMetaTile, SlotTile, SummaryTile, TileID,
};
use crate::deferred_data::DeferredDataSource;
use crate::timestamp::Interval;

pub struct MergeDeferredDataSource {
    data_sources: Vec<Box<dyn DeferredDataSource>>,
    infos: Vec<VecDeque<DataSourceInfo>>,
    mapping: Vec<u64>,
}

impl MergeDeferredDataSource {
    pub fn new(data_sources: Vec<Box<dyn DeferredDataSource>>) -> Self {
        assert!(!data_sources.is_empty());
        let infos = vec![VecDeque::new(); data_sources.len()];
        Self {
            data_sources,
            infos,
            mapping: Vec::new(),
        }
    }

    fn merge_entry(first: EntryInfo, second: EntryInfo) -> EntryInfo {
        let EntryInfo::Panel {
            short_name,
            long_name,
            summary: first_summary,
            mut slots,
        } = first
        else {
            unreachable!();
        };

        let EntryInfo::Panel {
            summary: second_summary,
            slots: second_slots,
            ..
        } = second
        else {
            unreachable!();
        };

        assert!(first_summary.is_none());
        assert!(second_summary.is_none());

        slots.extend(second_slots);

        EntryInfo::Panel {
            short_name,
            long_name,
            summary: None,
            slots,
        }
    }

    fn compute_mapping(source_infos: &[DataSourceInfo]) -> Vec<u64> {
        // Compute the mapping from old to new entries (basically the offset
        // of each initial slot)
        let slot_lens: Vec<_> = source_infos
            .iter()
            .map(|info| {
                let EntryInfo::Panel { ref slots, .. } = info.entry_info else {
                    unreachable!();
                };
                slots.len() as u64
            })
            .collect();

        let mut mapping = Vec::new();
        let mut sum = 0;
        for slot_len in slot_lens {
            mapping.push(sum);
            sum += slot_len;
        }

        mapping
    }

    fn merge_infos(source_infos: Vec<DataSourceInfo>) -> DataSourceInfo {
        assert!(!source_infos.is_empty());

        // Some fields can't be meaningfully merged, so just assert they're
        // all equivalent.
        let first_info = source_infos.first().unwrap();
        let tile_set = first_info.tile_set.clone();
        let field_schema = first_info.field_schema.clone();

        for info in &source_infos {
            assert_eq!(tile_set, info.tile_set);
            assert_eq!(field_schema, info.field_schema);
        }

        // Merge remaining fields
        // IMPORTANT: entry_info must be kept consistent with compute_mapping
        let interval = source_infos
            .iter()
            .map(|info| info.interval)
            .reduce(Interval::union)
            .unwrap();
        let entry_info = source_infos
            .iter()
            .map(|info| info.entry_info.clone())
            .reduce(Self::merge_entry)
            .unwrap();

        DataSourceInfo {
            entry_info,
            interval,
            tile_set,
            field_schema,
        }
    }

    fn map_src_to_dst_entry(&self, idx: usize, src_entry: &EntryID) -> EntryID {
        src_entry.shift_level0(self.mapping[idx] as i64)
    }

    fn map_dst_to_src_entry(&self, dst_entry: &EntryID) -> (usize, EntryID) {
        let Some(EntryIndex::Slot(level0)) = dst_entry.index(0) else {
            unreachable!();
        };

        let idx = self.mapping.partition_point(|&offset| offset < level0);
        (idx, dst_entry.shift_level0(-(self.mapping[idx] as i64)))
    }

    fn map_src_to_dst_item_uid(&self, idx: usize, item_uid: ItemUID) -> ItemUID {
        ItemUID(item_uid.0 * (self.mapping.len() as u64) + (idx as u64))
    }

    fn map_src_to_dst_summary(&self, idx: usize, tile: SummaryTile) -> SummaryTile {
        SummaryTile {
            entry_id: self.map_src_to_dst_entry(idx, &tile.entry_id),
            tile_id: tile.tile_id,
            data: tile.data,
        }
    }

    fn map_src_to_dst_slot(&self, idx: usize, mut tile: SlotTile) -> SlotTile {
        for items in &mut tile.data.items {
            for item in items {
                item.item_uid = self.map_src_to_dst_item_uid(idx, item.item_uid);
            }
        }

        SlotTile {
            entry_id: self.map_src_to_dst_entry(idx, &tile.entry_id),
            tile_id: tile.tile_id,
            data: tile.data,
        }
    }

    fn map_src_to_dst_field(&self, idx: usize, field: &mut Field) {
        match field {
            Field::ItemLink(ItemLink {
                ref mut item_uid,
                ref mut entry_id,
                ..
            }) => {
                *item_uid = self.map_src_to_dst_item_uid(idx, *item_uid);
                *entry_id = self.map_src_to_dst_entry(idx, entry_id);
            }
            Field::Vec(elts) => {
                for elt in elts {
                    self.map_src_to_dst_field(idx, elt);
                }
            }
            _ => (),
        }
    }

    fn map_src_to_dst_slot_meta(&self, idx: usize, mut tile: SlotMetaTile) -> SlotMetaTile {
        for items in &mut tile.data.items {
            for item in items {
                item.item_uid = self.map_src_to_dst_item_uid(idx, item.item_uid);
                for (_, field) in &mut item.fields {
                    self.map_src_to_dst_field(idx, field);
                }
            }
        }

        SlotMetaTile {
            entry_id: self.map_src_to_dst_entry(idx, &tile.entry_id),
            tile_id: tile.tile_id,
            data: tile.data,
        }
    }
}

impl DeferredDataSource for MergeDeferredDataSource {
    fn fetch_description(&self) -> DataSourceDescription {
        DataSourceDescription {
            source_locator: self
                .data_sources
                .iter()
                .flat_map(|x| x.fetch_description().source_locator)
                .collect::<Vec<_>>(),
        }
    }

    fn fetch_info(&mut self) {
        for data_source in &mut self.data_sources {
            data_source.fetch_info();
        }
    }

    fn get_infos(&mut self) -> Vec<DataSourceInfo> {
        for (data_source, infos) in self.data_sources.iter_mut().zip(self.infos.iter_mut()) {
            infos.extend(data_source.get_infos());
        }

        let max_available = self.infos.iter().map(|infos| infos.len()).max().unwrap();

        let mut result = Vec::new();
        for _ in 0..max_available {
            let source_infos: Vec<_> = self
                .infos
                .iter_mut()
                .map(|infos| infos.pop_front().unwrap())
                .collect();
            self.mapping = Self::compute_mapping(&source_infos);
            result.push(Self::merge_infos(source_infos));
        }
        result
    }

    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        let (idx, src_entry) = self.map_dst_to_src_entry(entry_id);

        self.data_sources[idx].fetch_summary_tile(&src_entry, tile_id, full);
    }

    fn get_summary_tiles(&mut self) -> Vec<SummaryTile> {
        let mut tiles = Vec::new();
        for (idx, data_source) in self.data_sources.iter_mut().enumerate() {
            tiles.extend(
                data_source
                    .get_summary_tiles()
                    .into_iter()
                    .map(|tile| (idx, tile)),
            );
        }

        // Hack: doing this in two stages to avoid mutability conflict
        tiles
            .into_iter()
            .map(|(idx, tile)| self.map_src_to_dst_summary(idx, tile))
            .collect()
    }

    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        let (idx, src_entry) = self.map_dst_to_src_entry(entry_id);

        self.data_sources[idx].fetch_slot_tile(&src_entry, tile_id, full);
    }

    fn get_slot_tiles(&mut self) -> Vec<SlotTile> {
        let mut tiles = Vec::new();
        for (idx, data_source) in self.data_sources.iter_mut().enumerate() {
            tiles.extend(
                data_source
                    .get_slot_tiles()
                    .into_iter()
                    .map(|tile| (idx, tile)),
            );
        }

        // Hack: doing this in two stages to avoid mutability conflict
        tiles
            .into_iter()
            .map(|(idx, tile)| self.map_src_to_dst_slot(idx, tile))
            .collect()
    }

    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        let (idx, src_entry) = self.map_dst_to_src_entry(entry_id);

        self.data_sources[idx].fetch_slot_meta_tile(&src_entry, tile_id, full);
    }

    fn get_slot_meta_tiles(&mut self) -> Vec<SlotMetaTile> {
        let mut tiles = Vec::new();
        for (idx, data_source) in self.data_sources.iter_mut().enumerate() {
            tiles.extend(
                data_source
                    .get_slot_meta_tiles()
                    .into_iter()
                    .map(|tile| (idx, tile)),
            );
        }

        // Hack: doing this in two stages to avoid mutability conflict
        tiles
            .into_iter()
            .map(|(idx, tile)| self.map_src_to_dst_slot_meta(idx, tile))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::data::{FieldSchema, TileSet};
    use crate::timestamp::Timestamp;

    #[test]
    fn test_merge_entry() {
        let first = EntryInfo::Panel {
            short_name: "F".to_string(),
            long_name: "First".to_string(),
            summary: None,
            slots: vec![EntryInfo::Slot {
                short_name: "S1".to_string(),
                long_name: "Slot 1".to_string(),
                max_rows: 1,
            }],
        };
        let second = EntryInfo::Panel {
            short_name: "S".to_string(),
            long_name: "Second".to_string(),
            summary: None,
            slots: vec![EntryInfo::Slot {
                short_name: "S2".to_string(),
                long_name: "Slot 2".to_string(),
                max_rows: 2,
            }],
        };

        let merge = MergeDeferredDataSource::merge_entry(first, second);

        let EntryInfo::Panel {
            short_name,
            long_name,
            summary,
            slots,
        } = merge
        else {
            panic!("unexpected variant result in merge");
        };

        assert_eq!(short_name, "F");
        assert_eq!(long_name, "First");
        assert!(summary.is_none());
        assert_eq!(slots.len(), 2);

        let EntryInfo::Slot {
            short_name: slot0_short_name,
            ..
        } = &slots[0]
        else {
            panic!("unexpected variant result in slot 0");
        };
        assert_eq!(slot0_short_name, "S1");

        let EntryInfo::Slot {
            short_name: slot1_short_name,
            ..
        } = &slots[1]
        else {
            panic!("unexpected variant result in slot 1");
        };
        assert_eq!(slot1_short_name, "S2");
    }

    #[test]
    fn test_merge_info() {
        let first = DataSourceInfo {
            entry_info: EntryInfo::Panel {
                short_name: "F".to_string(),
                long_name: "First".to_string(),
                summary: None,
                slots: vec![
                    EntryInfo::Slot {
                        short_name: "S1".to_string(),
                        long_name: "Slot 1".to_string(),
                        max_rows: 1,
                    },
                    EntryInfo::Slot {
                        short_name: "S2".to_string(),
                        long_name: "Slot 2".to_string(),
                        max_rows: 1,
                    },
                ],
            },
            interval: Interval::new(Timestamp(0), Timestamp(1000)),
            tile_set: TileSet { tiles: Vec::new() },
            field_schema: FieldSchema::new(),
        };
        let second = DataSourceInfo {
            entry_info: EntryInfo::Panel {
                short_name: "S".to_string(),
                long_name: "Second".to_string(),
                summary: None,
                slots: vec![EntryInfo::Slot {
                    short_name: "S3".to_string(),
                    long_name: "Slot 3".to_string(),
                    max_rows: 2,
                }],
            },
            interval: Interval::new(Timestamp(0), Timestamp(2000)),
            tile_set: TileSet { tiles: Vec::new() },
            field_schema: FieldSchema::new(),
        };

        let infos = vec![first, second];

        let mapping = MergeDeferredDataSource::compute_mapping(&infos);
        assert_eq!(mapping, vec![0, 2]);

        let merge = MergeDeferredDataSource::merge_infos(infos);

        assert_eq!(merge.interval, Interval::new(Timestamp(0), Timestamp(2000)));
        assert!(merge.tile_set.tiles.is_empty());

        let EntryInfo::Panel {
            short_name,
            long_name,
            summary,
            slots,
        } = merge.entry_info
        else {
            panic!("unexpected variant result in merge");
        };

        assert_eq!(short_name, "F");
        assert_eq!(long_name, "First");
        assert!(summary.is_none());
        assert_eq!(slots.len(), 3);

        let EntryInfo::Slot {
            short_name: slot0_short_name,
            ..
        } = &slots[0]
        else {
            panic!("unexpected variant result in slot 0");
        };
        assert_eq!(slot0_short_name, "S1");

        let EntryInfo::Slot {
            short_name: slot1_short_name,
            ..
        } = &slots[1]
        else {
            panic!("unexpected variant result in slot 1");
        };
        assert_eq!(slot1_short_name, "S2");

        let EntryInfo::Slot {
            short_name: slot2_short_name,
            ..
        } = &slots[2]
        else {
            panic!("unexpected variant result in slot 2");
        };
        assert_eq!(slot2_short_name, "S3");
    }
}
