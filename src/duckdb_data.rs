use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use const_format::formatc;
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
    RE.find_iter(s).map(|m| m.as_str()).join("_")
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
                    format!("{}_{}", parent, sanitize(short_name))
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
                FieldType::Interval.sql_type(),
            ),
            [],
        )?;

        conn.execute(
            "CREATE TABLE entry_info (
                entry_slug TEXT PRIMARY KEY,
                short_name TEXT,
                long_name TEXT,
                parent_slug TEXT,
                type TEXT,
            )",
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
        let mut app = conn.appender("entry_info")?;

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

    fn create_entry_table(&self, conn: &Connection, entry_id_slug: &str) -> duckdb::Result<()> {
        conn.execute(
            &format!(
                "CREATE TABLE {} (
                    item_uid UBIGINT,
                    interval {},
                    title TEXT,
                )",
                entry_id_slug,
                FieldType::Interval.sql_type(),
            ),
            [],
        )?;

        Ok(())
    }

    fn add_entry_field(
        &self,
        conn: &Connection,
        entry_id_slug: &str,
        field_name: &str,
        field_type: FieldType,
    ) -> duckdb::Result<()> {
        conn.execute(
            &format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                entry_id_slug,
                field_name,
                field_type.sql_type(),
            ),
            [],
        )?;

        Ok(())
    }

    fn write_slot_meta_tile(
        &self,
        conn: &Connection,
        field_schema: &FieldSchema,
        entry_id_slugs: &BTreeMap<EntryID, String>,
        tables: &mut SlotMetaTable,
        tile: SlotMetaTile,
    ) -> duckdb::Result<()> {
        let entry_id_slug = entry_id_slugs.get(&tile.entry_id).unwrap();

        // Create the entry table (if it doesn't already exist)
        let mut new_entry = false;
        let (field_slots, slot_fields) =
            tables
                .fields
                .entry(tile.entry_id.clone())
                .or_insert_with(|| {
                    new_entry = true;
                    (BTreeMap::new(), Vec::new())
                });
        if new_entry {
            self.create_entry_table(conn, entry_id_slug)?;
        }

        let field_names = field_schema.field_names();

        // Discover new fields (not seen in previous tiles) and infer types
        let mut slot = slot_fields.len();
        let mut new_field_slots = BTreeMap::new();
        let mut new_field_types = BTreeMap::new();
        for row in &tile.data.items {
            for item in row {
                for ItemField(field_id, field, _) in &item.fields {
                    if !field_slots.contains_key(field_id) {
                        new_field_slots.entry(*field_id).or_insert_with(|| {
                            let result = slot;
                            slot += 1;
                            result
                        });
                        let new_type = FieldType::infer_type(field);
                        new_field_types
                            .entry(*field_id)
                            .and_modify(|field_type: &mut FieldType| {
                                *field_type = field_type.meet(new_type);
                            })
                            .or_insert(new_type);
                    }
                }
            }
        }

        // Update table and tracking data structures
        let mut new_slot_fields = BTreeMap::new();
        for (field_id, slot) in &new_field_slots {
            let field_type = new_field_types.get(field_id).unwrap();
            let old = new_slot_fields.insert(*slot, (*field_id, field_type));
            assert!(old.is_none());
        }
        field_slots.extend(new_field_slots);
        for (slot, (field_id, field_type)) in new_slot_fields {
            assert_eq!(slot, slot_fields.len());
            let field_name = sanitize(field_names.get(&field_id).unwrap());
            self.add_entry_field(conn, entry_id_slug, &field_name, *field_type)?;
            slot_fields.push((field_name, *field_type));
        }

        let mut app = conn.appender(entry_id_slug)?;
        self.schema
            .append_slot_meta_tile(&mut app, &tile, field_slots, slot_fields)?;

        Ok(())
    }

    fn write_slot_meta_tiles(
        &mut self,
        conn: &Connection,
        field_schema: &FieldSchema,
        entry_id_slugs: &BTreeMap<EntryID, String>,
        tables: &mut SlotMetaTable,
    ) -> duckdb::Result<()> {
        for (tile, _) in self.data_source.get_slot_meta_tiles() {
            let tile = tile.expect("reading slot meta tile failed");
            self.write_slot_meta_tile(conn, field_schema, entry_id_slugs, tables, tile)?;
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
        let mut tables = SlotMetaTable::new();

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

            while self.data_source.outstanding_requests() > 0 {
                self.write_slot_meta_tiles(&conn, &info.field_schema, &entry_id_slugs, &mut tables)
                    .expect("creating slot meta table failed");
            }
        }

        Ok(())
    }
}

struct SlotMetaTable {
    fields: BTreeMap<EntryID, (BTreeMap<FieldID, usize>, Vec<(String, FieldType)>)>,
}

impl SlotMetaTable {
    fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
        }
    }
}

impl FieldType {
    const fn sql_type(&self) -> &'static str {
        const fn interval() -> &'static str {
            "STRUCT(start BIGINT, stop BIGINT)"
        }
        const fn item_link() -> &'static str {
            formatc!(
                "STRUCT(item_uid UBIGINT, title TEXT, interval {}, entry_slug TEXT)",
                interval()
            )
        }

        match self {
            FieldType::I64 => "BIGINT",
            FieldType::U64 => "UBIGINT",
            FieldType::String => "TEXT",
            FieldType::Interval => interval(),
            FieldType::ItemLink => item_link(),
            FieldType::Vec => "TEXT",
            FieldType::Empty => "BOOLEAN",
        }
    }
}
