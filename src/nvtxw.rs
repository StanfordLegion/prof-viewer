use std::collections::BTreeMap;
use std::ffi::CString;
use std::ffi::{c_char, c_void};
use std::io;
use std::mem::size_of;
use std::path::PathBuf;
use std::ptr::{null, null_mut};

use nvtxw::nvtxw;

use crate::data::{DataSourceInfo, EntryID, EntryIndex, EntryInfo, TileID};
use crate::deferred_data::{CountingDeferredDataSource, DeferredDataSource};

use crate::data::Color32;
use crate::timestamp::Timestamp;

const LEGION_DOMAIN_NAME: &str = "Legion";

pub struct NVTXW<T: DeferredDataSource> {
    data_source: CountingDeferredDataSource<T>,
    o_output: Option<PathBuf>,
    o_merge: Option<PathBuf>,
    zero_time: i64,
}

#[derive(Debug)]
struct EntryContents {
    entry_id: EntryID,
    time_start: Timestamp,
    time_stop: Timestamp,
    color: Color32,
    title: String,
}

type ResultVec = Vec<(EntryID, String, String)>;

fn walk_entry_list(info: &EntryInfo) -> ResultVec {
    let mut result = Vec::new();
    fn walk(info: &EntryInfo, entry_id: EntryID, result: &mut ResultVec, hierarchy: String) {
        match info {
            EntryInfo::Panel {
                summary,
                slots,
                short_name,
                ..
            } => {
                if let Some(summary) = summary {
                    walk(
                        summary,
                        entry_id.summary(),
                        result,
                        if entry_id.level() > 0 {
                            format!("{}/{}", hierarchy, short_name)
                        } else {
                            hierarchy.clone()
                        },
                    );
                }
                for (i, slot) in slots.iter().enumerate() {
                    walk(
                        slot,
                        entry_id.child(i as u64),
                        result,
                        if entry_id.level() > 0 {
                            format!("{}/{}", hierarchy, short_name)
                        } else {
                            hierarchy.clone()
                        },
                    )
                }
            }
            EntryInfo::Slot {
                long_name,
                short_name,
                ..
            } => {
                result.push((
                    entry_id.clone(),
                    long_name.clone(),
                    format!("{}/{}", hierarchy, short_name),
                ));
            }
            EntryInfo::Summary { .. } => {
                /*
                result.push(entry_id);
                */
            }
        }
    }
    walk(
        info,
        EntryID::root(),
        &mut result,
        LEGION_DOMAIN_NAME.to_string(),
    );
    result
}

#[repr(C)]
struct legion_nvtxw_event {
    time_start: u64,
    time_stop: u64,
    name: *const c_char,
    color: u32,
}

// See nvToolsExtPayload.h: nvtxPayloadSchemaAttr_t::schemaId
// See NVTX_PAYLOAD_ENTRY_TYPE_SCHEMA_ID_STATIC_START
const LEGION_NVTXW_PAYLOAD_SCHEMA_ID: u64 = 0x1c0ffee;
const LEGION_NVTXW_PAYLOAD_NAME_SCHEMA_ID: u64 = 0x2c0ffee;

impl<T: DeferredDataSource> NVTXW<T> {
    pub fn new(
        data_source: T,
        o_output: Option<PathBuf>,
        o_merge: Option<PathBuf>,
        zero_time: i64,
    ) -> Self {
        Self {
            data_source: CountingDeferredDataSource::new(data_source),
            o_output,
            o_merge,
            zero_time,
        }
    }

    fn check_info(&mut self) -> Option<DataSourceInfo> {
        // We requested this once, so we know we'll get zero or one result
        self.data_source.get_infos().pop()
    }

    fn write_event(
        interface: nvtxw::InterfaceHandle,
        streams: &BTreeMap<EntryID, nvtxw::StreamHandle>,
        zero_time: i64,
        i: EntryContents,
    ) {
        let c_name = CString::new(i.title).expect("CString::new failed");
        let events = [legion_nvtxw_event {
            time_start: (i.time_start.0 as u64)
                .checked_add(zero_time.try_into().unwrap())
                .expect("time_start overflowed"),
            time_stop: (i.time_stop.0 as u64)
                .checked_add(zero_time.try_into().unwrap())
                .expect("time_stop overflowed"),
            name: c_name.as_ptr(),
            color: ((i.color.r() as u32) << 16)
                | ((i.color.g() as u32) << 8)
                | (i.color.b() as u32)
                | (0xFF << 24),
        }];

        let stream = streams[&i.entry_id];

        let payloads = [
            nvtxw::PayloadData {
                schemaId: LEGION_NVTXW_PAYLOAD_NAME_SCHEMA_ID,
                size: usize::MAX,
                payload: c_name.as_ptr() as *const c_void,
            },
            nvtxw::PayloadData {
                schemaId: LEGION_NVTXW_PAYLOAD_SCHEMA_ID,
                size: size_of::<legion_nvtxw_event>(),
                payload: events.as_ptr() as *const c_void,
            },
        ];

        nvtxw::event_write(interface, stream, &payloads).expect("Failed to write event");
    }

    fn fetch_events(
        data_source: &mut CountingDeferredDataSource<T>,
        entries: &mut BTreeMap<u64, EntryContents>,
        num_requests: u64,
    ) {
        while data_source.outstanding_requests() > num_requests {
            /*
            for tile in data_source.get_summary_tiles() {
            }
            */
            for tile in data_source.get_slot_tiles() {
                let entry_id = tile.entry_id;
                let items = tile.data.items;
                for item_group in items {
                    for item in item_group {
                        let item_uid = item.item_uid;
                        let time_start = item.interval.start;
                        let time_stop = item.interval.stop;
                        let color = item.color;

                        let key = item_uid.0;

                        entries
                            .entry(key)
                            .and_modify(|e| {
                                assert!(e.entry_id == entry_id);

                                e.entry_id = entry_id.clone();
                                e.color = color;
                            })
                            .or_insert_with(|| EntryContents {
                                entry_id: entry_id.clone(),
                                time_start,
                                time_stop,
                                color,
                                title: String::new(),
                            });
                    }
                }
            }
            for tile in data_source.get_slot_meta_tiles() {
                let entry_id = tile.entry_id;
                let items = tile.data.items;
                for item_group in items {
                    for item in item_group {
                        let item_uid = item.item_uid;
                        let time_start = item.original_interval.start;
                        let time_stop = item.original_interval.stop;
                        let title = item.title;

                        let key = item_uid.0;

                        entries
                            .entry(key)
                            .and_modify(|e| {
                                assert!(e.entry_id == entry_id);

                                e.title.clone_from(&title);

                                e.time_start = time_start;
                                e.time_stop = time_stop;
                            })
                            .or_insert_with(|| EntryContents {
                                entry_id: entry_id.clone(),
                                time_start,
                                time_stop,
                                color: Color32::from_rgb(0, 0, 0),
                                title,
                            });
                    }
                }
            }
        }
    }

    pub fn write(mut self) -> io::Result<()> {
        self.data_source.fetch_info();
        let mut info = None;
        while info.is_none() {
            info = self.check_info();
        }
        let info = info.unwrap();

        let entry_ids = walk_entry_list(&info.entry_info);

        let full_range_tile_id = TileID(info.interval);
        let full = true;

        // For now, this only works on dynamic data sources
        assert!(info.tile_set.tiles.is_empty());

        println!("Exporting to NVTXW");

        let interface = nvtxw::initialize_simple().expect("Failed to initialize NVTXW");

        let session = nvtxw::session_begin_simple(interface, self.o_output, self.o_merge)
            .expect("Failed to create session");

        let c_event_name = CString::new("Legion Event").expect("CString::new failed");

        let c_field_name_time_start = CString::new("time_start").expect("CString::new failed");
        let c_field_name_time_stop = CString::new("time_stop").expect("CString::new failed");
        let c_field_name_name = CString::new("name").expect("CString::new failed");
        let c_field_name_color = CString::new("color").expect("CString::new failed");

        // The C string containing the event's name must be provided in a special way.

        let name_schema = [nvtxw::PayloadSchemaEntry {
            flags: nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_EVENT_MESSAGE
                | nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_ARRAY_ZERO_TERMINATED,
            type_: nvtxw::NVTX_PAYLOAD_ENTRY_TYPE_CSTRING,
            name: c_field_name_name.as_ptr(),
            description: null(),
            arrayOrUnionDetail: 0,
            offset: 0,
            semantics: null(),
            reserved: null(),
        }];

        let name_schema_attr = nvtxw::PayloadSchemaAttr {
            fieldMask: nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_TYPE
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_FLAGS
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_ENTRIES
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_NUM_ENTRIES
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_SCHEMA_ID,
            name: null(),
            type_: nvtxw::NVTX_PAYLOAD_SCHEMA_TYPE_DYNAMIC,
            flags: nvtxw::NVTX_PAYLOAD_SCHEMA_FLAG_REFERENCED,
            entries: name_schema.as_ptr(),
            numEntries: name_schema.len(),
            payloadStaticSize: 0,
            packAlign: 0,
            schemaId: LEGION_NVTXW_PAYLOAD_NAME_SCHEMA_ID,
            extension: null_mut(),
        };

        let event_schema = [
            nvtxw::PayloadSchemaEntry {
                flags: nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_RANGE_BEGIN
                    | nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_EVENT_TIMESTAMP,
                type_: nvtxw::NVTX_PAYLOAD_ENTRY_TYPE_UINT64,
                name: c_field_name_time_start.as_ptr(),
                description: null(),
                arrayOrUnionDetail: 0,
                offset: 0,
                semantics: null(),
                reserved: null(),
            },
            nvtxw::PayloadSchemaEntry {
                flags: nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_RANGE_END
                    | nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_EVENT_TIMESTAMP,
                type_: nvtxw::NVTX_PAYLOAD_ENTRY_TYPE_UINT64,
                name: c_field_name_time_stop.as_ptr(),
                description: null(),
                arrayOrUnionDetail: 0,
                offset: 0,
                semantics: null(),
                reserved: null(),
            },
            nvtxw::PayloadSchemaEntry {
                flags: nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_EVENT_MESSAGE
                    | nvtxw::NVTX_PAYLOAD_ENTRY_FLAG_POINTER,
                type_: nvtxw::NVTX_PAYLOAD_ENTRY_TYPE_CSTRING,
                name: c_field_name_name.as_ptr(),
                description: null(),
                arrayOrUnionDetail: 0,
                offset: 0,
                semantics: null(),
                reserved: null(),
            },
            nvtxw::PayloadSchemaEntry {
                flags: 0,
                type_: nvtxw::NVTX_PAYLOAD_ENTRY_TYPE_NVTX_COLOR,
                name: c_field_name_color.as_ptr(),
                description: null(),
                arrayOrUnionDetail: 0,
                offset: 0,
                semantics: null(),
                reserved: null(),
            },
        ];

        let event_schema_attr = nvtxw::PayloadSchemaAttr {
            fieldMask: nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_NAME
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_TYPE
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_ENTRIES
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_NUM_ENTRIES
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_STATIC_SIZE
                | nvtxw::NVTX_PAYLOAD_SCHEMA_ATTR_SCHEMA_ID,
            name: c_event_name.as_ptr(),
            type_: nvtxw::NVTX_PAYLOAD_SCHEMA_TYPE_STATIC,
            flags: nvtxw::NVTX_PAYLOAD_SCHEMA_FLAG_NONE,
            entries: event_schema.as_ptr(),
            numEntries: event_schema.len(),
            payloadStaticSize: size_of::<legion_nvtxw_event>(),
            packAlign: 0,
            schemaId: LEGION_NVTXW_PAYLOAD_SCHEMA_ID,
            extension: null_mut(),
        };

        let mut streams: BTreeMap<EntryID, nvtxw::StreamHandle> = BTreeMap::new();
        for (entry_id, long_name, hierarchy) in &entry_ids {
            let stream_name = format!("{} {}", LEGION_DOMAIN_NAME, long_name);
            let domain_name = hierarchy.to_string();

            let stream = nvtxw::stream_open_simple(interface, session, stream_name, domain_name)
                .expect("Failed to create stream");

            nvtxw::schema_register(interface, stream, &name_schema_attr)
                .expect("Failed to register name schema");

            nvtxw::schema_register(interface, stream, &event_schema_attr)
                .expect("Failed to register event schema");

            streams.insert(entry_id.clone(), stream);
        }

        let zero_time = self.zero_time;

        let mut entries: BTreeMap<u64, EntryContents> = BTreeMap::new();

        for (entry_id, _, _) in &entry_ids {
            match entry_id.last_index().unwrap() {
                EntryIndex::Summary => {
                    /*
                    self.data_source
                        .fetch_summary_tile(entry_id, full_range_tile_id, full);
                    */
                }
                EntryIndex::Slot(..) => {
                    self.data_source
                        .fetch_slot_tile(entry_id, full_range_tile_id, full);
                    self.data_source
                        .fetch_slot_meta_tile(entry_id, full_range_tile_id, full);
                }
            }
        }

        Self::fetch_events(
            &mut self.data_source,
            &mut entries,
            0,
        );

        for (_, i) in entries {
            Self::write_event(interface, &streams, zero_time, i);
        }

        for (_entry_id, stream) in streams {
            nvtxw::stream_end(interface, stream).expect("Failed to end stream");
        }

        nvtxw::session_end(interface, session).expect("Failed to end session");

        Ok(())
    }
}
