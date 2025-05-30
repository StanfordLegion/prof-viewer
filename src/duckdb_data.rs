use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use duckdb::{Connection, params};
use itertools::Itertools;
use regex::Regex;

use crate::app::tile_manager::TileManager;
use crate::arrow_data::{ArrowSchema, FieldType};
use crate::data::{
    self, DataSourceInfo, EntryID, EntryInfo, FieldID, FieldSchema, ItemField, SlotMetaTile,
};
use crate::deferred_data::{CountingDeferredDataSource, DeferredDataSource};

struct EntryRow {
    entry_id: EntryID,
    entry_id_slug: String,
    parent_id_slug: Option<String>,
}

pub struct DataSourceDuckDBWriter<T: DeferredDataSource> {
    data_source: CountingDeferredDataSource<T>,
    path: PathBuf,
    force: bool,
    schema: ArrowSchema,
}

fn sanitize_short(s: &str) -> String {
    sanitize(&s.replace(" ", ""))
}

fn sanitize(s: &str) -> String {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[A-Za-z0-9]+").unwrap());
    let mut result = RE.find_iter(s).map(|m| m.as_str()).join("_");
    result.make_ascii_lowercase();
    result
}

fn walk_entry_list(info: &EntryInfo) -> Vec<EntryRow> {
    let mut result = Vec::new();
    fn walk(
        info: &EntryInfo,
        entry_id: EntryID,
        parent_id_slug: Option<&str>,
        result: &mut Vec<EntryRow>,
    ) {
        match info {
            EntryInfo::Panel {
                short_name, slots, ..
            } => {
                let entry_id_slug = if let Some(parent) = parent_id_slug {
                    format!("{}_{}", parent, sanitize_short(short_name))
                } else {
                    sanitize_short(short_name)
                };

                result.push(EntryRow {
                    entry_id: entry_id.clone(),
                    entry_id_slug: entry_id_slug.clone(),
                    parent_id_slug: parent_id_slug.map(|x| x.to_string()),
                });
                for (i, slot) in slots.iter().enumerate() {
                    walk(slot, entry_id.child(i as u64), Some(&entry_id_slug), result)
                }
            }
            EntryInfo::Slot { short_name, .. } => {
                let entry_id_slug = if let Some(parent) = parent_id_slug {
                    format!("{}_{}", parent, sanitize_short(short_name))
                } else {
                    sanitize_short(short_name)
                };

                result.push(EntryRow {
                    entry_id,
                    entry_id_slug,
                    parent_id_slug: parent_id_slug.map(|x| x.to_string()),
                });
            }
            EntryInfo::Summary { .. } => {
                // No need to track summary entries
            }
        }
    }
    match info {
        EntryInfo::Panel { slots, .. } => {
            for (i, slot) in slots.iter().enumerate() {
                walk(slot, EntryID::root().child(i as u64), None, &mut result)
            }
        }
        _ => unreachable!(), // Root is always a Panel
    }
    result
}

impl<T: DeferredDataSource> DataSourceDuckDBWriter<T> {
    pub fn new(data_source: T, path: impl AsRef<Path>, force: bool) -> Self {
        let schema = ArrowSchema::new();
        Self {
            data_source: CountingDeferredDataSource::new(data_source),
            path: path.as_ref().to_owned(),
            force,
            schema,
        }
    }

    fn check_info(&mut self) -> Option<data::Result<DataSourceInfo>> {
        // We requested this once, so we know we'll get zero or one result
        self.data_source.get_infos().pop()
    }

    fn create_data_source_tables(
        &self,
        conn: &Connection,
        info: &DataSourceInfo,
    ) -> duckdb::Result<()> {
        conn.execute(
            &format!(
                "CREATE TABLE data_source (
                    source_locator TEXT[],
                    interval {},
                    warning_message TEXT,
                )",
                SqlType(&FieldType::Interval),
            ),
            [],
        )?;

        conn.execute(
            "CREATE TABLE entries (
                entry_slug TEXT NOT NULL PRIMARY KEY,
                short_name TEXT NOT NULL,
                long_name TEXT NOT NULL,
                parent_slug TEXT,
                type TEXT NOT NULL,
            )",
            [],
        )?;

        conn.execute(
            &format!(
                "CREATE TABLE items (
                    entry_slug TEXT NOT NULL,
                    item_uid UBIGINT NOT NULL,
                    interval {} NOT NULL,
                    title TEXT NOT NULL,
                )",
                SqlType(&FieldType::Interval),
            ),
            [],
        )?;

        let mut app = conn.appender("data_source")?;
        self.schema
            .append_info(&mut app, &self.data_source.fetch_description(), info)?;
        Ok(())
    }

    fn insert_entry_info(
        &self,
        conn: &Connection,
        info: &EntryInfo,
        entry_rows: &[EntryRow],
    ) -> duckdb::Result<()> {
        let mut app = conn.appender("entries")?;

        for EntryRow {
            entry_id,
            entry_id_slug,
            parent_id_slug,
        } in entry_rows
        {
            let entry_info = info.get(entry_id).unwrap();
            let kind = match entry_info {
                EntryInfo::Panel { .. } => "panel",
                EntryInfo::Slot { .. } => "slot",
                EntryInfo::Summary { .. } => unreachable!(),
            };
            match entry_info {
                EntryInfo::Panel {
                    short_name,
                    long_name,
                    ..
                }
                | EntryInfo::Slot {
                    short_name,
                    long_name,
                    ..
                } => {
                    app.append_row(params![
                        entry_id_slug,
                        short_name,
                        long_name,
                        parent_id_slug,
                        kind
                    ])?;
                }
                EntryInfo::Summary { .. } => {
                    // No need to track summary entries
                }
            }
        }

        Ok(())
    }

    fn add_entry_field(
        &self,
        conn: &Connection,
        field_name: &str,
        field_type: &FieldType,
    ) -> duckdb::Result<()> {
        conn.execute(
            &format!(
                "ALTER TABLE items ADD COLUMN {} {}",
                field_name,
                SqlType(field_type),
            ),
            [],
        )?;

        Ok(())
    }

    fn upgrade_entry_field(
        &self,
        conn: &Connection,
        field_name: &str,
        old_type: &FieldType,
        new_type: &FieldType,
    ) -> duckdb::Result<()> {
        conn.execute(
            &SqlType(old_type).upgrade_column("items", field_name, &SqlType(new_type)),
            [],
        )?;

        Ok(())
    }

    fn write_slot_meta_tile(
        &self,
        conn: &Connection,
        field_schema: &FieldSchema,
        entry_id_slugs: &BTreeMap<EntryID, String>,
        table: &mut SlotMetaTable,
        tile: SlotMetaTile,
    ) -> duckdb::Result<()> {
        let entry_id_slug = entry_id_slugs.get(&tile.entry_id).unwrap();

        let SlotMetaTable {
            field_slots,
            slot_fields,
        } = table;

        let field_names = field_schema.field_names();

        // Discover new fields (not seen in previous tiles) and infer types
        let last_slot = slot_fields.len();
        let mut next_slot = last_slot;
        let mut upgrade_slots_from_type = BTreeMap::new();
        for row in &tile.data.items {
            for item in row {
                for ItemField(field_id, field, _) in &item.fields {
                    let slot = *field_slots.entry(*field_id).or_insert_with(|| {
                        let slot = next_slot;
                        next_slot += 1;
                        slot
                    });
                    let field_type = FieldType::infer_type(field);
                    if slot == slot_fields.len() {
                        let field_name = sanitize(field_names.get(field_id).unwrap());
                        slot_fields.push((field_name, field_type));
                    } else {
                        let old_type = &slot_fields[slot].1;
                        let meet_type = old_type.meet(&field_type);
                        if old_type != &meet_type {
                            upgrade_slots_from_type
                                .entry(slot)
                                .or_insert_with(|| old_type.clone());
                            slot_fields[slot].1 = meet_type;
                        }
                    }
                }
            }
        }

        // Insert new fields discovered since last call
        for (field_name, field_type) in &slot_fields[last_slot..next_slot] {
            self.add_entry_field(conn, field_name, field_type)?;
        }

        // Upgrade fields that have changed type
        for (slot, old_type) in upgrade_slots_from_type {
            let (field_name, new_type) = &slot_fields[slot];
            self.upgrade_entry_field(conn, field_name, &old_type, new_type)?;
        }

        let mut app = conn.appender("items")?;
        self.schema.append_slot_meta_tile(
            &mut app,
            entry_id_slug,
            &tile,
            field_slots,
            slot_fields,
        )?;

        Ok(())
    }

    fn write_slot_meta_tiles(
        &mut self,
        conn: &Connection,
        field_schema: &FieldSchema,
        entry_id_slugs: &BTreeMap<EntryID, String>,
        table: &mut SlotMetaTable,
    ) -> duckdb::Result<()> {
        for (tile, _) in self.data_source.get_slot_meta_tiles() {
            let tile = tile.expect("reading slot meta tile failed");
            self.write_slot_meta_tile(conn, field_schema, entry_id_slugs, table, tile)?;
        }
        Ok(())
    }

    pub fn write(mut self) -> io::Result<()> {
        if self.force && self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }

        // Fetch the information about the data source
        self.data_source.fetch_info();
        let mut info = None;
        while info.is_none() {
            info = self.check_info();
        }
        let info = info.unwrap().expect("fetch_info failed");

        let conn = Connection::open(&self.path).expect("Failed to open DuckDB database");

        self.create_data_source_tables(&conn, &info)
            .expect("Failed to create data source tables");

        let entry_rows = walk_entry_list(&info.entry_info);

        let mut entry_id_slugs = BTreeMap::new();
        for EntryRow {
            entry_id,
            entry_id_slug,
            ..
        } in &entry_rows
        {
            let existing = entry_id_slugs.insert(entry_id.clone(), entry_id_slug.clone());
            assert!(existing.is_none(), "duplicate entry_id_slug");
        }

        self.insert_entry_info(&conn, &info.entry_info, &entry_rows)
            .expect("Failed to insert entry info");

        let mut tm = TileManager::new(info.tile_set, info.interval);
        let mut table = SlotMetaTable::new();

        for EntryRow { entry_id, .. } in &entry_rows {
            let entry_info = info.entry_info.get(entry_id).unwrap();
            match entry_info {
                EntryInfo::Slot { .. } => {}
                _ => {
                    continue;
                }
            };

            // Fetch a covering set of tiles for the entire interval
            const FULL: bool = true;
            let tile_ids = tm.request_tiles(info.interval, FULL);
            for tile_id in &tile_ids {
                self.data_source
                    .fetch_slot_meta_tile(entry_id, *tile_id, FULL);
            }

            const MAX_IN_FLIGHT_REQUESTS: u64 = 100;

            while self.data_source.outstanding_requests() > MAX_IN_FLIGHT_REQUESTS {
                self.write_slot_meta_tiles(&conn, &info.field_schema, &entry_id_slugs, &mut table)
                    .expect("creating slot meta table failed");
            }
        }

        while self.data_source.outstanding_requests() > 0 {
            self.write_slot_meta_tiles(&conn, &info.field_schema, &entry_id_slugs, &mut table)
                .expect("creating slot meta table failed");
        }

        Ok(())
    }
}

struct SlotMetaTable {
    field_slots: BTreeMap<FieldID, usize>,
    slot_fields: Vec<(String, FieldType)>,
}

impl SlotMetaTable {
    fn new() -> Self {
        Self {
            field_slots: BTreeMap::new(),
            slot_fields: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct SqlType<'a>(&'a FieldType);

impl fmt::Display for SqlType<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            FieldType::I64 => write!(f, "BIGINT"),
            FieldType::U64 => write!(f, "UBIGINT"),
            FieldType::String => write!(f, "TEXT"),
            FieldType::Interval => write!(f, "STRUCT(start BIGINT, stop BIGINT, duration BIGINT)"),
            FieldType::ItemLink => write!(
                f,
                "STRUCT(item_uid UBIGINT, title TEXT, interval {}, entry_slug TEXT)",
                SqlType(&FieldType::Interval)
            ),
            FieldType::Vec(v) => write!(f, "{}[]", SqlType(v)),
            FieldType::Empty => write!(f, "BOOLEAN"),
            FieldType::Unknown => panic!("cannot write unknown type"),
        }
    }
}

impl SqlType<'_> {
    fn upgrade_column(&self, table_name: &str, column_name: &str, to_type: &SqlType<'_>) -> String {
        match (self, to_type) {
            (SqlType(FieldType::U64), SqlType(FieldType::ItemLink))
            | (SqlType(FieldType::String), SqlType(FieldType::ItemLink)) => format!(
                "ALTER TABLE {table_name}
                     ALTER COLUMN {column_name} TYPE {to_type}
                     USING {{
                         'item_uid': NULL,
                         'title': {column_name},
                         'interval': {{'start': NULL, 'stop': NULL, 'duration': NULL}},
                         'entry_slug': NULL,
                     }};"
            ),
            _ => panic!("don't know how to perform upgrade from {self:?} to {to_type:?}"),
        }
    }
}
