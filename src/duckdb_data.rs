use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use duckdb::{Connection, params};

use crate::app::tile_manager::TileManager;
use crate::data::{
    self, DataSourceInfo, EntryID, EntryInfo, Field, FieldID, FieldSchema, ItemField, SlotMetaTile,
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
}

fn sanitize_short(s: &str) -> String {
    let mut result = s.replace(" ", "").replace("-", "_");
    result.make_ascii_lowercase();
    result
}

fn sanitize(s: &str) -> String {
    let mut result = s.replace(" ", "_").replace("-", "_");
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
        Self {
            data_source: CountingDeferredDataSource::new(data_source),
            path: path.as_ref().to_owned(),
            force,
        }
    }

    fn check_info(&mut self) -> Option<data::Result<DataSourceInfo>> {
        // We requested this once, so we know we'll get zero or one result
        self.data_source.get_infos().pop()
    }

    fn create_info_tables(&self, conn: &Connection, info: &DataSourceInfo) -> duckdb::Result<()> {
        conn.execute(
            "CREATE TABLE data_source_info (
                interval_start_ns BIGINT,
                interval_stop_ns BIGINT,
                warning_message TEXT,
                description TEXT
            )",
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

        let description = self.data_source.fetch_description();
        let description_str = description.source_locator.join(", ");

        conn.execute(
            "INSERT INTO data_source_info (interval_start_ns, interval_stop_ns, warning_message, description)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                info.interval.start.0,
                info.interval.stop.0,
                info.warning_message,
                description_str
            ],
        )?;

        Ok(())
    }

    fn insert_entry_info(
        &self,
        conn: &Connection,
        info: &EntryInfo,
        entry_rows: &[EntryRow],
    ) -> duckdb::Result<()> {
        let mut stmt = conn.prepare(
            "INSERT INTO entry_info (entry_slug, short_name, long_name, parent_slug, type)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;

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
                    stmt.execute(params![
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
                    item_uid BIGINT,
                    interval_start_ns BIGINT,
                    interval_stop_ns BIGINT,
                    title TEXT,
                )",
                entry_id_slug
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
        let entry = tables
            .fields
            .entry(tile.entry_id.clone())
            .or_insert_with(|| {
                new_entry = true;
                BTreeMap::new()
            });
        if new_entry {
            self.create_entry_table(conn, entry_id_slug)?;
        }

        let field_names = field_schema.field_names();

        // Discover new fields (not seen in previous tiles)
        let mut new_fields = BTreeMap::new();
        for row in &tile.data.items {
            for item in row {
                for ItemField(field_id, field, _) in &item.fields {
                    entry.entry(*field_id).or_insert_with(|| {
                        let field_name = sanitize(field_names.get(field_id).unwrap());
                        let field_type = FieldType::infer_type(&field);
                        new_fields.insert(field_name.clone(), field_type);
                        (field_name, field_type)
                    });
                }
            }
        }

        // Add new fields to the table
        for (field_name, field_type) in &new_fields {
            self.add_entry_field(conn, entry_id_slug, field_name, *field_type)?;
        }

        // Prep a statement that includes all fields discovered so far
        let mut columns: Vec<&str> = Vec::new();
        let mut slots = BTreeMap::new();
        let base_columns = ["item_uid", "interval_start_ns", "interval_stop_ns", "title"];
        for field_name in &base_columns {
            columns.push(field_name);
        }
        for (field_id, (field_name, field_type)) in entry {
            slots.insert(field_id, (columns.len(), field_type));
            columns.push(field_name);
        }
        let placeholders: Vec<_> = (1..=columns.len()).map(|i| format!("?{}", i)).collect();

        let mut stmt = conn.prepare(&format!(
            "INSERT INTO {} ({}) VALUES ({})",
            entry_id_slug,
            columns.join(", "),
            placeholders.join(", "),
        ))?;

        // Important: not all items will have all fields; everything else should be NULL
        fn null() -> Box<dyn duckdb::ToSql> {
            Box::new(duckdb::types::Null)
        }
        let mut values: Vec<_> = (0..columns.len()).map(|_| null()).collect();
        for row in &tile.data.items {
            for item in row {
                values[0] = Box::new(item.item_uid.0);
                values[1] = Box::new(item.original_interval.start.0);
                values[2] = Box::new(item.original_interval.stop.0);
                values[3] = Box::new(&item.title);
                for value in &mut values[4..] {
                    *value = null();
                }
                for ItemField(field_id, field, _) in &item.fields {
                    let (slot, field_type) = slots.get(field_id).unwrap();
                    values[*slot] = field_type.sql_value(&field);
                }
                stmt.execute(duckdb::params_from_iter(&values))?;
            }
        }

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

        self.create_info_tables(&conn, &info)
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
    fields: BTreeMap<EntryID, BTreeMap<FieldID, (String, FieldType)>>,
}

impl SlotMetaTable {
    fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum FieldType {
    I64,
    U64,
    String,
    Interval,
    Vec,
    Empty,
}

impl FieldType {
    fn infer_type(field: &Field) -> Self {
        match field {
            Field::I64(..) => FieldType::I64,
            Field::U64(..) => FieldType::U64,
            Field::String(..) => FieldType::String,
            Field::Interval(..) => FieldType::Interval,
            Field::ItemLink(..) => FieldType::String, // for now map to string
            Field::Vec(..) => FieldType::Vec,
            Field::Empty => FieldType::Empty,
        }
    }

    fn sql_type(&self) -> &'static str {
        match self {
            FieldType::I64 => "BIGINT",
            FieldType::U64 => "UBIGINT",
            FieldType::String => "TEXT",
            FieldType::Interval => "TEXT",
            FieldType::Vec => "TEXT",
            FieldType::Empty => "BOOLEAN",
        }
    }

    fn sql_value<'a>(&self, field: &'a Field) -> Box<dyn duckdb::ToSql + 'a> {
        match (self, field) {
            (FieldType::I64, Field::I64(x)) => Box::new(x),
            (FieldType::U64, Field::U64(x)) => Box::new(x),
            (FieldType::String, Field::String(x)) => Box::new(x),
            (FieldType::Interval, Field::Interval(x)) => Box::new(format!("{}", x)),
            (FieldType::String, Field::ItemLink(x)) => Box::new(format!("{:?}", x)),
            (FieldType::Vec, Field::Vec(x)) => Box::new(format!("{:?}", x)),
            (FieldType::Empty, Field::Empty) => Box::new(true),
            (t, v) => unreachable!("mismatch in SQL type {:?} vs value {:?}", t, v),
        }
    }
}
