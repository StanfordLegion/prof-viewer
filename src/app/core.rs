use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

use egui::{
    Color32, NumExt, Pos2, Rect, RichText, ScrollArea, Slider, Stroke, TextStyle, TextWrapMode,
    Vec2,
};
use egui_extras::{Column, TableBuilder};
#[cfg(not(target_arch = "wasm32"))]
use itertools::Itertools;
use log::warn;
use percentage::{Percentage, PercentageInteger};
use regex::{Regex, escape};
use serde::{Deserialize, Serialize};

use crate::app::tile_manager::TileManager;
use crate::data::{
    self, DataSourceInfo, EntryID, EntryIndex, EntryInfo, Field, FieldID, FieldSchema, ItemField,
    ItemLink, ItemMeta, ItemUID, SlotMetaTileData, SlotTileData, SummaryTileData, TileID,
    UtilPoint,
};
use crate::deferred_data::{CountingDeferredDataSource, DeferredDataSource, LruDeferredDataSource};
use crate::timestamp::{
    Interval, Timestamp, TimestampDisplay, TimestampParseError, TimestampUnits,
};

/// Overview:
///   ProfApp -> Context, Window *
///   Window -> Config, Panel
///   Panel -> Summary, { Panel | Slot } *
///   Summary
///   Slot -> Item *
///
/// Context:
///   * Global configuration state (i.e., for all profiles)
///
/// Window:
///   * One Windows per profile
///   * Owns the ScrollArea (there is only **ONE** ScrollArea)
///   * Handles pan/zoom (there is only **ONE** pan/zoom setting)
///
/// Config:
///   * Window configuration state (i.e., specific to a profile)
///
/// Panel:
///   * One Panel for each level of nesting in the profile (root, node, kind)
///   * Table widget for (nested) cells
///   * Each row contains: label, content
///
/// Summary:
///   * Utilization widget
///
/// Slot:
///   * One Slot for each processor, channel, memory
///   * Viewer widget for items

#[derive(Debug, Clone)]
struct Summary {
    entry_id: EntryID,
    color: Color32,
    tiles: BTreeMap<TileID, Option<data::Result<SummaryTileData>>>,
}

#[derive(Debug, Clone)]
struct Slot {
    entry_id: EntryID,
    short_name: String,
    long_name: String,
    expanded: bool,
    max_rows: u64,

    // These maps have to track four different kinds of states:
    //
    //  1. Entry missing: user navigated away before response came back.
    //  2. None: awaiting response.
    //  3. Some(Err(_)): response failed or returned with an error.
    //  4. Some(Ok(_)): successfully completed response.
    tiles: BTreeMap<TileID, Option<data::Result<SlotTileData>>>,
    tile_metas: BTreeMap<TileID, Option<data::Result<SlotMetaTileData>>>,
    tile_metas_full: BTreeMap<TileID, Option<data::Result<SlotMetaTileData>>>,
}

#[derive(Debug, Clone)]
struct Panel<S: Entry> {
    entry_id: EntryID,
    short_name: String,
    long_name: String,
    expanded: bool,

    summary: Option<Summary>,
    slots: Vec<S>,
}

#[derive(Debug, Clone)]
struct ItemLocator {
    // For vertical scroll, we need the item's entry ID and row index
    // (note: reversed, because we're in screen space)
    entry_id: EntryID,
    irow: Option<usize>,

    // If we can't find the item on the initial attempt, we track the ItemUID
    // and attempt to find it once the tile loads
    item_uid: ItemUID,
}

#[derive(Debug, Clone)]
struct ItemDetail {
    // We populate metadata lazily, so there can be a delay until this is full
    meta: Option<ItemMeta>,
    loc: ItemLocator,
}

#[derive(Debug, Clone)]
struct SearchCacheItem {
    item_uid: ItemUID,

    // Cache fields for display
    title: String,

    // For horizontal scroll, we need the item's interval
    interval: Interval,

    // For vertical scroll, we need the item's row index (note: reversed,
    // because we're in screen space)
    irow: usize,
}

#[derive(Debug, Clone)]
struct SearchState {
    title_field: FieldID,

    // Search parameters
    query: String,
    last_query: String,
    search_field: FieldID,
    last_search_field: FieldID,
    whole_word: bool,
    last_whole_word: bool,
    last_word_regex: Option<Regex>,
    include_collapsed_entries: bool,
    last_include_collapsed_entries: bool,
    last_view_interval: Option<Interval>,

    // Cache of matching items
    result_set: BTreeSet<ItemUID>,
    result_cache: BTreeMap<EntryID, BTreeMap<TileID, BTreeMap<ItemUID, SearchCacheItem>>>,
    entry_tree: BTreeMap<u64, BTreeMap<u64, BTreeSet<u64>>>,
}

struct Config {
    field_schema: FieldSchema,

    // Node selection
    min_node: u64,
    max_node: u64,

    // Kind selection
    kinds: Vec<String>,
    kind_filter: BTreeSet<String>,

    // This is just for the local profile
    interval: Interval,
    warning_message: Option<String>,

    data_source: CountingDeferredDataSource<LruDeferredDataSource<Box<dyn DeferredDataSource>>>,

    search_state: SearchState,

    // When the user clicks on an item, we put it here
    items_selected: BTreeMap<ItemUID, ItemDetail>,

    // When the user clicks "Zoom to Item" or a search result, we put it here
    scroll_to_item: Option<ItemLocator>,
    // Sometimes, we cannot find the correct row to scroll to. In this case we
    // populate the following field to track the re-scroll when the item is found
    scroll_to_item_retry: Option<ItemLocator>,

    tile_manager: TileManager,
}

struct Window {
    panel: Panel<Panel<Panel<Slot>>>, // nodes -> kind -> proc/chan/mem
    index: u64,
    config: Config,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
enum IntervalOrigin {
    Zoom,
    Pan,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct IntervalState {
    levels: Vec<Interval>,
    origins: Vec<IntervalOrigin>,
    index: usize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
enum IntervalSelectError {
    InvalidValue,
    NoUnit,
    InvalidUnit,
    StartAfterStop,
    StartAfterEnd,
    StopBeforeStart,
}

impl From<TimestampParseError> for IntervalSelectError {
    fn from(val: TimestampParseError) -> Self {
        match val {
            TimestampParseError::InvalidValue => IntervalSelectError::InvalidValue,
            TimestampParseError::NoUnit => IntervalSelectError::NoUnit,
            TimestampParseError::InvalidUnit => IntervalSelectError::InvalidUnit,
        }
    }
}

impl fmt::Display for IntervalSelectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntervalSelectError::InvalidValue => write!(f, "invalid value"),
            IntervalSelectError::NoUnit => write!(f, "no unit"),
            IntervalSelectError::InvalidUnit => write!(f, "invalid unit"),
            IntervalSelectError::StartAfterStop => write!(f, "start after stop"),
            IntervalSelectError::StartAfterEnd => write!(f, "start after end"),
            IntervalSelectError::StopBeforeStart => write!(f, "stop before start"),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct IntervalSelectState {
    // User-entered strings for the interval start/stop.
    start_buffer: String,
    stop_buffer: String,

    // Parse errors for the respective strings (if any).
    start_error: Option<IntervalSelectError>,
    stop_error: Option<IntervalSelectError>,
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
enum ItemLinkNavigationMode {
    #[default]
    Zoom,
    Pan,
}

impl ItemLinkNavigationMode {
    fn label_text(&self) -> &'static str {
        match *self {
            ItemLinkNavigationMode::Zoom => "Zoom to Item",
            ItemLinkNavigationMode::Pan => "Pan to Item",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct Context {
    #[serde(skip)]
    row_height: f32,
    #[serde(skip)]
    scale_factor: f32,

    #[serde(skip)]
    row_scroll_delta: i32,

    #[serde(skip)]
    subheading_size: f32,

    // This is across all profiles
    #[serde(skip)]
    total_interval: Interval,

    // Visible time range
    #[serde(skip)]
    view_interval: Interval,

    #[serde(skip)]
    drag_origin: Option<Pos2>,

    // Hack: We need to track the screenspace rect where slot/summary
    // data gets drawn. This gets used rendering the cursor, but we
    // only know it when we render slots. So stash it here.
    #[serde(skip)]
    slot_rect: Option<Rect>,

    item_link_mode: ItemLinkNavigationMode,

    debug: bool,

    #[serde(skip)]
    show_controls: bool,

    #[serde(skip)]
    view_interval_history: IntervalState,
    #[serde(skip)]
    interval_select_state: IntervalSelectState,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(default)] // deserialize missing fields as default value
struct ProfApp {
    // Data sources waiting to be turned into windows.
    #[serde(skip)]
    pending_data_sources: VecDeque<Box<dyn DeferredDataSource>>,

    #[serde(skip)]
    windows: Vec<Window>,

    cx: Context,

    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    last_update: Option<Instant>,
}

trait Entry {
    fn new(info: &EntryInfo, entry_id: EntryID) -> Self;

    fn entry_id(&self) -> &EntryID;
    fn label_text(&self) -> &str;
    fn hover_text(&self) -> &str;

    fn find_slot(&self, entry_id: &EntryID, level: u64) -> Option<&Slot>;
    fn find_slot_mut(&mut self, entry_id: &EntryID, level: u64) -> Option<&mut Slot>;
    fn find_summary_mut(&mut self, entry_id: &EntryID, level: u64) -> Option<&mut Summary>;

    fn expand_slot(&mut self, entry_id: &EntryID, level: u64);

    fn inflate_meta(&mut self, config: &mut Config, cx: &mut Context);

    fn search(&mut self, config: &mut Config);

    fn label(&mut self, ui: &mut egui::Ui, rect: Rect, cx: &Context) {
        let response = ui.allocate_rect(
            rect,
            if self.is_expandable() {
                egui::Sense::click()
            } else {
                egui::Sense::hover()
            },
        );

        let style = ui.style();
        let font_id = TextStyle::Body.resolve(style);
        let visuals = if self.is_expandable() {
            style.interact_selectable(&response, false)
        } else {
            *style.noninteractive()
        };

        ui.painter()
            .rect(rect, 0.0, visuals.bg_fill, visuals.bg_stroke);
        let spacing = style.spacing.item_spacing * Vec2::new(1.0, cx.scale_factor);
        let layout = ui.painter().layout(
            self.label_text().to_owned(),
            font_id,
            visuals.text_color(),
            rect.width() - spacing.x * 2.0,
        );
        ui.painter()
            .galley(rect.min + spacing, layout, visuals.text_color());

        if response.clicked() {
            // This will take effect next frame because we can't redraw this widget now
            self.toggle_expanded();
        } else if response.hovered() {
            response.on_hover_text(self.hover_text());
        }
    }

    fn content(
        &mut self,
        ui: &mut egui::Ui,
        rect: Rect,
        viewport: Rect,
        config: &mut Config,
        cx: &mut Context,
    );

    fn height(&self, prefix: Option<&EntryID>, config: &Config, cx: &Context) -> f32;

    fn is_expandable(&self) -> bool;

    fn toggle_expanded(&mut self);
}

impl Summary {
    fn inflate(&mut self, config: &mut Config, cx: &mut Context) {
        const PART: bool = false;
        let tile_ids = config.request_tiles(cx.view_interval, PART);
        Config::invalidate_cache(&tile_ids, &mut self.tiles);
        for tile_id in tile_ids {
            self.tiles.entry(tile_id).or_insert_with(|| {
                config
                    .data_source
                    .fetch_summary_tile(&self.entry_id, tile_id, PART);
                None
            });
        }
    }
}

impl Entry for Summary {
    fn new(info: &EntryInfo, entry_id: EntryID) -> Self {
        if let EntryInfo::Summary { color } = info {
            Self {
                entry_id,
                color: *color,
                tiles: BTreeMap::new(),
            }
        } else {
            unreachable!()
        }
    }

    fn entry_id(&self) -> &EntryID {
        &self.entry_id
    }
    fn label_text(&self) -> &str {
        "avg"
    }
    fn hover_text(&self) -> &str {
        "Utilization Plot of Average Usage Over Time"
    }

    fn find_slot(&self, _entry_id: &EntryID, _level: u64) -> Option<&Slot> {
        unreachable!()
    }

    fn find_slot_mut(&mut self, _entry_id: &EntryID, _level: u64) -> Option<&mut Slot> {
        unreachable!()
    }

    fn find_summary_mut(&mut self, entry_id: &EntryID, level: u64) -> Option<&mut Summary> {
        assert_eq!(entry_id.level(), level);
        assert_eq!(entry_id.index(level - 1)?, EntryIndex::Summary);
        Some(self)
    }

    fn expand_slot(&mut self, _entry_id: &EntryID, _level: u64) {
        unreachable!()
    }

    fn inflate_meta(&mut self, _config: &mut Config, _cx: &mut Context) {
        unreachable!()
    }

    fn search(&mut self, _config: &mut Config) {
        unreachable!()
    }

    fn content(
        &mut self,
        ui: &mut egui::Ui,
        rect: Rect,
        _viewport: Rect,
        config: &mut Config,
        cx: &mut Context,
    ) {
        cx.slot_rect = Some(rect); // Save slot rect for use later

        const TOOLTIP_RADIUS: f32 = 4.0;
        let hover_pos = ui.rect_hover_pos(rect); // where is the mouse hovering?

        self.inflate(config, cx);

        let style = ui.style();
        let visuals = style.noninteractive();
        ui.painter()
            .rect(rect, 0.0, visuals.bg_fill, visuals.bg_stroke);

        let stroke = Stroke::new(visuals.bg_stroke.width, self.color);

        // Conversions to and from screen space coordinates
        let util_to_screen = |util: &UtilPoint| {
            let time = cx.view_interval.unlerp(util.time);
            rect.lerp_inside(Vec2::new(time, 1.0 - util.util))
        };
        let screen_to_util = |screen: Pos2| UtilPoint {
            time: cx
                .view_interval
                .lerp((screen.x - rect.left()) / rect.width()),
            util: 1.0 - (screen.y - rect.top()) / rect.height(),
        };

        // Linear interpolation along the line from p1 to p2
        let interpolate = |p1: Pos2, p2: Pos2, x: f32| {
            let ratio = (x - p1.x) / (p2.x - p1.x);
            Rect::from_min_max(p1, p2).lerp_inside(Vec2::new(ratio, ratio))
        };

        let mut last_util: Option<&UtilPoint> = None;
        let mut last_point: Option<Pos2> = None;
        let mut hover_util = None;
        for tile in self.tiles.values().flatten() {
            let tile = match tile {
                Ok(t) => t,
                Err(e) => {
                    warn!("{}", e);
                    // Paint the entire tile red to indicate the error.
                    ui.painter().rect(rect, 0.0, Color32::RED, Stroke::NONE);
                    return;
                }
            };

            for util in &tile.utilization {
                let mut point = util_to_screen(util);
                if let Some(mut last) = last_point {
                    let last_util = last_util.unwrap();
                    if cx
                        .view_interval
                        .overlaps(Interval::new(last_util.time, util.time))
                    {
                        // Interpolate when out of view
                        if last.x < rect.min.x {
                            last = interpolate(last, point, rect.min.x);
                        }
                        if point.x > rect.max.x {
                            point = interpolate(last, point, rect.max.x);
                        }

                        ui.painter().line_segment([last, point], stroke);

                        if let Some(hover) = hover_pos {
                            if last.x <= hover.x && hover.x < point.x {
                                let interp = interpolate(last, point, hover.x);
                                ui.painter().circle_stroke(
                                    interp,
                                    TOOLTIP_RADIUS,
                                    visuals.fg_stroke,
                                );
                                hover_util = Some(screen_to_util(interp));
                            }
                        }
                    }
                }

                last_point = Some(point);
                last_util = Some(util);
            }
        }

        if let Some(util) = hover_util {
            let time = cx.view_interval.unlerp(util.time);
            let util_rect = Rect::from_min_max(
                rect.lerp_inside(Vec2::new(time - 0.05, 0.0)),
                rect.lerp_inside(Vec2::new(time + 0.05, 1.0)),
            );
            ui.show_tooltip(
                "utilization_tooltip",
                &util_rect,
                format!("{:.0}% Utilization", util.util * 100.0),
            );
        }
    }

    fn height(&self, prefix: Option<&EntryID>, _config: &Config, cx: &Context) -> f32 {
        assert!(prefix.is_none());
        const ROWS: u64 = 4;
        ROWS as f32 * cx.row_height
    }

    fn is_expandable(&self) -> bool {
        false
    }

    fn toggle_expanded(&mut self) {
        unreachable!();
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::I64(value) => write!(f, "{value}"),
            Field::U64(value) => write!(f, "{value}"),
            Field::String(value) => write!(f, "{value}"),
            Field::Interval(value) => write!(f, "{value}"),
            Field::ItemLink(ItemLink { title, .. }) => write!(f, "{title}"),
            Field::Vec(fields) => {
                for (i, field) in fields.iter().enumerate() {
                    write!(f, "{field}")?;
                    if i < fields.len() {
                        write!(f, ", ")?;
                    }
                }
                Ok(())
            }
            Field::Empty => write!(f, ""),
        }
    }
}

struct FieldWithName<'a>(&'a str, &'a Field);

impl fmt::Display for FieldWithName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let FieldWithName(name, value) = self;
        match value {
            Field::Empty => write!(f, "{name}"),
            _ => write!(f, "{name}: {value}"),
        }
    }
}

impl Slot {
    fn rows(&self) -> u64 {
        const UNEXPANDED_ROWS: u64 = 2;
        if self.expanded {
            self.max_rows.at_least(UNEXPANDED_ROWS)
        } else {
            UNEXPANDED_ROWS
        }
    }

    fn inflate(&mut self, config: &mut Config, cx: &mut Context) -> Vec<TileID> {
        const PART: bool = false;
        let tile_ids = config.request_tiles(cx.view_interval, PART);
        Config::invalidate_cache(&tile_ids, &mut self.tiles);
        Config::invalidate_cache(&tile_ids, &mut self.tile_metas);
        for tile_id in &tile_ids {
            self.tiles.entry(*tile_id).or_insert_with(|| {
                config
                    .data_source
                    .fetch_slot_tile(&self.entry_id, *tile_id, false);
                None
            });
        }
        tile_ids
    }

    fn fetch_meta_tile(
        &mut self,
        tile_id: TileID,
        config: &mut Config,
        full: bool,
    ) -> Option<&data::Result<SlotMetaTileData>> {
        let metas = if full {
            &mut self.tile_metas_full
        } else {
            &mut self.tile_metas
        };

        metas
            .entry(tile_id)
            .or_insert_with(|| {
                config
                    .data_source
                    .fetch_slot_meta_tile(&self.entry_id, tile_id, full);
                None
            })
            .as_ref()
    }

    #[allow(clippy::too_many_arguments)]
    fn render_tile(
        &mut self,
        tile_id: TileID,
        rows: u64,
        mut hover_pos: Option<Pos2>,
        ui: &mut egui::Ui,
        rect: Rect,
        viewport: Rect,
        config: &mut Config,
        cx: &mut Context,
    ) -> Option<Pos2> {
        let tile = self.tiles.get(&tile_id).unwrap();

        if !tile.is_some() {
            // Tile hasn't finished loading.
            return hover_pos;
        }
        let tile = tile.as_ref().unwrap();

        let tile = match tile {
            Ok(t) => t,
            Err(e) => {
                warn!("{}", e);
                // Paint the entire tile red to indicate the error.
                ui.painter().rect(rect, 0.0, Color32::RED, Stroke::NONE);
                return hover_pos;
            }
        };

        if !cx.view_interval.overlaps(tile_id.0) {
            return hover_pos;
        }

        // Figure out roughly how large a pixel is on the screen.
        let pixel_ns = (cx.view_interval.duration_ns() as f32 / rect.width()) as i64;

        // Track which item, if any, we're interacting with
        let mut interact_item = None;

        for (row, row_items) in tile.items.iter().enumerate() {
            // Need to reverse the rows because we're working in screen space
            let irow = rows - (row as u64) - 1;

            // We want to do this first on rows, so that we can cut the
            // entire row if we don't need it

            // Compute bounds for the whole row
            let row_min = rect.lerp_inside(Vec2::new(0.0, (irow as f32 + 0.05) / rows as f32));
            let row_max = rect.lerp_inside(Vec2::new(1.0, (irow as f32 + 0.95) / rows as f32));

            // Cull if out of bounds
            // Note: need to shift by rect.min to get to viewport space
            if row_max.y - rect.min.y < viewport.min.y {
                break;
            } else if row_min.y - rect.min.y > viewport.max.y {
                continue;
            }

            // Check if mouse is hovering over this row
            let row_rect = Rect::from_min_max(row_min, row_max);
            let row_hover = hover_pos.is_some_and(|h| row_rect.contains(h));

            // Now handle the items
            for (item_idx, item) in row_items.iter().enumerate() {
                if !cx.view_interval.overlaps(item.interval) {
                    continue;
                }

                // Expand interval to use at least one pixel, but do NOT
                // overlap neighboring items.
                let mut interval = item.interval;
                if interval.duration_ns() < pixel_ns {
                    let expand_ns = (pixel_ns - interval.duration_ns()) / 2;
                    interval = interval.grow(expand_ns).intersection(tile_id.0);
                    if item_idx > 0 {
                        let last_item = &row_items[item_idx - 1];
                        interval = interval.subtract_before(last_item.interval.stop);
                    }
                    if item_idx < row_items.len() - 1 {
                        let next_item = &row_items[item_idx + 1];
                        interval = interval.subtract_after(next_item.interval.start);
                    }
                }

                // Note: the interval is EXCLUSIVE. This turns out to be what
                // we want here, because in screen coordinates interval.stop
                // is the BEGINNING of the interval.stop nanosecond.
                let start = cx.view_interval.unlerp(interval.start).at_least(0.0);
                let stop = cx.view_interval.unlerp(interval.stop).at_most(1.0);
                let min = rect.lerp_inside(Vec2::new(start, (irow as f32 + 0.05) / rows as f32));
                let max = rect.lerp_inside(Vec2::new(stop, (irow as f32 + 0.95) / rows as f32));

                let item_rect = Rect::from_min_max(min, max);
                if row_hover && hover_pos.is_some_and(|h| item_rect.contains(h)) {
                    hover_pos = None;
                    interact_item = Some((row, item_idx, item_rect, tile_id));
                }

                let highlight = config.items_selected.contains_key(&item.item_uid);

                let mut color = item.color;
                if !config.search_state.query.is_empty() {
                    if config.search_state.result_set.contains(&item.item_uid) || highlight {
                        color = Color32::RED;
                    } else {
                        color = color.gamma_multiply(0.2);
                    }
                } else if highlight {
                    color = Color32::RED;
                }

                ui.painter().rect(item_rect, 0.0, color, Stroke::NONE);
            }
        }

        if let Some((row, item_idx, item_rect, tile_id)) = interact_item {
            // Hack: clone here  to avoid mutability conflict.
            let entry_id = self.entry_id.clone();
            const PART: bool = false;
            if let Some(tile_meta) = self.fetch_meta_tile(tile_id, config, PART) {
                let tile_meta = match tile_meta {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("{}", e);
                        ui.show_tooltip("task_tooltip_error", &item_rect, e);
                        return hover_pos;
                    }
                };

                let item_meta = &tile_meta.items[row][item_idx];
                let tooltip_id = ("task_tooltip", item_meta.item_uid.0);
                ui.show_tooltip_ui(tooltip_id, &item_rect, |ui| {
                    ui.label(&item_meta.title);
                    if cx.debug {
                        ui.label(format!("Item UID: {}", item_meta.item_uid.0));
                    }
                    for ItemField(field_id, field, color) in &item_meta.fields {
                        let name = config.field_schema.get_name(*field_id).unwrap();
                        let text = format!("{}", FieldWithName(name, field));
                        if let Some(color) = color {
                            ui.label(RichText::new(text).color(*color));
                        } else {
                            ui.label(text);
                        }
                    }
                    ui.label("(Click to show details.)");
                });

                // Also mark task as selected if the mouse has been clicked
                ui.input(|i| {
                    // A "click" is measured on *release*, assuming certain
                    // properties hold (e.g., the button was held less than
                    // some duration, and it moved less than some amount).
                    if i.pointer.any_click() && i.pointer.primary_released() {
                        let irow = Some(rows as usize - row - 1);
                        match config.items_selected.entry(item_meta.item_uid) {
                            std::collections::btree_map::Entry::Vacant(e) => {
                                e.insert(ItemDetail {
                                    meta: Some(item_meta.clone()),
                                    loc: ItemLocator {
                                        entry_id,
                                        irow,
                                        item_uid: item_meta.item_uid,
                                    },
                                });
                            }
                            std::collections::btree_map::Entry::Occupied(e) => {
                                e.remove_entry();
                            }
                        }
                    }
                });
            }
        }

        hover_pos
    }
}

impl Entry for Slot {
    fn new(info: &EntryInfo, entry_id: EntryID) -> Self {
        if let EntryInfo::Slot {
            short_name,
            long_name,
            max_rows,
        } = info
        {
            Self {
                entry_id,
                short_name: short_name.to_owned(),
                long_name: long_name.to_owned(),
                expanded: true,
                max_rows: *max_rows,
                tiles: BTreeMap::new(),
                tile_metas: BTreeMap::new(),
                tile_metas_full: BTreeMap::new(),
            }
        } else {
            unreachable!()
        }
    }

    fn entry_id(&self) -> &EntryID {
        &self.entry_id
    }
    fn label_text(&self) -> &str {
        &self.short_name
    }
    fn hover_text(&self) -> &str {
        &self.long_name
    }

    fn find_slot(&self, entry_id: &EntryID, level: u64) -> Option<&Slot> {
        assert_eq!(entry_id.level(), level);
        assert!(entry_id.slot_index(level - 1).is_some());
        Some(self)
    }

    fn find_slot_mut(&mut self, entry_id: &EntryID, level: u64) -> Option<&mut Slot> {
        assert_eq!(entry_id.level(), level);
        assert!(entry_id.slot_index(level - 1).is_some());
        Some(self)
    }

    fn find_summary_mut(&mut self, _entry_id: &EntryID, _level: u64) -> Option<&mut Summary> {
        unreachable!()
    }

    fn expand_slot(&mut self, entry_id: &EntryID, level: u64) {
        assert_eq!(entry_id.level(), level);
        assert!(entry_id.slot_index(level - 1).is_some());
        self.expanded = true;
    }

    fn inflate_meta(&mut self, config: &mut Config, cx: &mut Context) {
        const FULL: bool = true;
        let tile_ids = config.request_tiles(cx.view_interval, FULL);
        Config::invalidate_cache(&tile_ids, &mut self.tile_metas_full);
        for tile_id in tile_ids {
            self.fetch_meta_tile(tile_id, config, FULL);
        }
    }

    fn search(&mut self, config: &mut Config) {
        if !config.search_state.start_entry(self) {
            return;
        }

        for (tile_id, tile) in &self.tile_metas_full {
            if let Some(Ok(tile)) = tile {
                if !config.search_state.start_tile(self, *tile_id) {
                    continue;
                }

                for (row, row_items) in tile.items.iter().enumerate() {
                    for item in row_items {
                        if config.search_state.is_match(item) {
                            // Reverse rows because we're in screen space
                            let irow = tile.items.len() - row - 1;
                            config.search_state.insert(self, *tile_id, irow, item);
                        }
                    }
                }
            }
        }
    }

    fn content(
        &mut self,
        ui: &mut egui::Ui,
        rect: Rect,
        viewport: Rect,
        config: &mut Config,
        cx: &mut Context,
    ) {
        cx.slot_rect = Some(rect); // Save slot rect for use later

        let mut hover_pos = ui.rect_hover_pos(rect); // where is the mouse hovering?

        if self.expanded {
            let tile_ids = self.inflate(config, cx);

            let style = ui.style();
            let visuals = style.noninteractive();
            ui.painter()
                .rect(rect, 0.0, visuals.bg_fill, visuals.bg_stroke);

            let rows = self.rows();
            for tile_id in tile_ids {
                hover_pos =
                    self.render_tile(tile_id, rows, hover_pos, ui, rect, viewport, config, cx);
            }
        }
    }

    fn height(&self, _prefix: Option<&EntryID>, _config: &Config, cx: &Context) -> f32 {
        self.rows() as f32 * cx.row_height
    }

    fn is_expandable(&self) -> bool {
        true
    }

    fn toggle_expanded(&mut self) {
        self.expanded = !self.expanded;
    }
}

impl<S: Entry> Panel<S> {
    fn render<T: Entry>(
        ui: &mut egui::Ui,
        rect: Rect,
        viewport: Rect,
        slot: &mut T,
        y: &mut f32,
        config: &mut Config,
        cx: &mut Context,
    ) -> bool {
        const LABEL_WIDTH: f32 = 60.0;
        const COL_PADDING: f32 = 4.0;
        const ROW_PADDING: f32 = 4.0;

        // Compute the size of this slot
        // This is in screen (i.e., rect) space
        let min_y = *y;
        let max_y = min_y + slot.height(None, config, cx);
        *y = max_y + ROW_PADDING;

        // Cull if out of bounds
        // Note: need to shift by rect.min to get to viewport space
        if max_y - rect.min.y < viewport.min.y {
            return false;
        } else if min_y - rect.min.y > viewport.max.y {
            return true;
        }

        // Draw label and content
        let label_min = rect.min.x;
        let label_max = (rect.min.x + LABEL_WIDTH).at_most(rect.max.x);
        let content_min = (label_max + COL_PADDING).at_most(rect.max.x);
        let content_max = rect.max.x;

        let label_subrect =
            Rect::from_min_max(Pos2::new(label_min, min_y), Pos2::new(label_max, max_y));
        let content_subrect =
            Rect::from_min_max(Pos2::new(content_min, min_y), Pos2::new(content_max, max_y));

        // Shift viewport up by the amount consumed
        // Invariant: (0, 0) in viewport is rect.min
        //   (i.e., subtracting rect.min gets us from screen space to viewport space)
        // Note: viewport.min is NOT necessarily (0, 0)
        let content_viewport = viewport.translate(Vec2::new(0.0, rect.min.y - min_y));

        slot.content(ui, content_subrect, content_viewport, config, cx);
        slot.label(ui, label_subrect, cx);

        false
    }

    fn is_slot_visible(slot: &S, config: &Config) -> bool {
        let level = slot.entry_id().level();
        if level == 1 {
            // Apply node filter.
            let index = slot.entry_id().last_slot_index().unwrap();
            index >= config.min_node && index <= config.max_node
        } else if level == 2 {
            // Apply kind filter.
            let kind = slot.label_text();
            config.kind_filter.is_empty() || config.kind_filter.contains(kind)
        } else {
            true
        }
    }
}

impl<S: Entry> Entry for Panel<S> {
    fn new(info: &EntryInfo, entry_id: EntryID) -> Self {
        if let EntryInfo::Panel {
            short_name,
            long_name,
            summary,
            slots,
        } = info
        {
            let expanded = entry_id.level() != 2;
            let summary = summary
                .as_ref()
                .map(|s| Summary::new(s, entry_id.summary()));
            let slots = slots
                .iter()
                .enumerate()
                .map(|(i, s)| S::new(s, entry_id.child(i as u64)))
                .collect();
            Self {
                entry_id,
                short_name: short_name.to_owned(),
                long_name: long_name.to_owned(),
                expanded,
                summary,
                slots,
            }
        } else {
            unreachable!()
        }
    }

    fn entry_id(&self) -> &EntryID {
        &self.entry_id
    }
    fn label_text(&self) -> &str {
        &self.short_name
    }
    fn hover_text(&self) -> &str {
        &self.long_name
    }

    fn find_slot(&self, entry_id: &EntryID, level: u64) -> Option<&Slot> {
        self.slots
            .get(entry_id.slot_index(level)? as usize)?
            .find_slot(entry_id, level + 1)
    }

    fn find_slot_mut(&mut self, entry_id: &EntryID, level: u64) -> Option<&mut Slot> {
        self.slots
            .get_mut(entry_id.slot_index(level)? as usize)?
            .find_slot_mut(entry_id, level + 1)
    }

    fn find_summary_mut(&mut self, entry_id: &EntryID, level: u64) -> Option<&mut Summary> {
        if level < entry_id.level() - 1 {
            self.slots
                .get_mut(entry_id.slot_index(level)? as usize)?
                .find_summary_mut(entry_id, level + 1)
        } else {
            self.summary.as_mut()?.find_summary_mut(entry_id, level + 1)
        }
    }

    fn expand_slot(&mut self, entry_id: &EntryID, level: u64) {
        self.slots
            .get_mut(entry_id.slot_index(level).unwrap() as usize)
            .unwrap()
            .expand_slot(entry_id, level + 1);
        self.expanded = true;
    }

    fn inflate_meta(&mut self, config: &mut Config, cx: &mut Context) {
        let force = config.search_state.include_collapsed_entries;
        if self.expanded || force {
            for slot in &mut self.slots {
                // Apply visibility settings
                if !force && !Self::is_slot_visible(slot, config) {
                    continue;
                }

                slot.inflate_meta(config, cx);
            }
        }
    }

    fn search(&mut self, config: &mut Config) {
        let force = config.search_state.include_collapsed_entries;
        if self.expanded || force {
            for slot in &mut self.slots {
                // Apply visibility settings
                if !force && !Self::is_slot_visible(slot, config) {
                    continue;
                }

                slot.search(config);
            }
        }
    }

    fn content(
        &mut self,
        ui: &mut egui::Ui,
        rect: Rect,
        viewport: Rect,
        config: &mut Config,
        cx: &mut Context,
    ) {
        let mut y = rect.min.y;
        if let Some(summary) = &mut self.summary {
            Self::render(ui, rect, viewport, summary, &mut y, config, cx);
        }

        if self.expanded {
            for slot in &mut self.slots {
                // Apply visibility settings
                if !Self::is_slot_visible(slot, config) {
                    continue;
                }

                if Self::render(ui, rect, viewport, slot, &mut y, config, cx) {
                    break;
                }
            }
        }
    }

    fn height(&self, prefix: Option<&EntryID>, config: &Config, cx: &Context) -> f32 {
        const UNEXPANDED_ROWS: u64 = 2;
        const ROW_PADDING: f32 = 4.0;

        let mut total = 0.0;
        let mut rows: i64 = 0;
        if let Some(summary) = &self.summary {
            total += summary.height(None, config, cx);
            rows += 1;
        } else if !self.expanded {
            // Need some minimum space if this panel has no summary and is collapsed
            total += UNEXPANDED_ROWS as f32 * cx.row_height;
            rows += 1;
        }

        if self.expanded {
            for slot in &self.slots {
                if let Some(prefix) = prefix {
                    // If this is our entry, stop
                    if slot.entry_id() == prefix {
                        break;
                    }
                }

                // Apply visibility settings
                if !Self::is_slot_visible(slot, config) {
                    continue;
                }

                total += slot.height(prefix, config, cx);

                if let Some(prefix) = prefix {
                    // If we're a prefix of the entry, recurse and then stop
                    if prefix.has_prefix(slot.entry_id()) {
                        break;
                    }
                }

                rows += 1;
            }
        }

        total += (rows - 1).at_least(0) as f32 * ROW_PADDING;

        total
    }

    fn is_expandable(&self) -> bool {
        !self.slots.is_empty()
    }

    fn toggle_expanded(&mut self) {
        self.expanded = !self.expanded;
    }
}

impl SearchState {
    fn new(title_id: FieldID) -> Self {
        Self {
            title_field: title_id,

            query: "".to_owned(),
            last_query: "".to_owned(),
            search_field: title_id,
            last_search_field: title_id,
            whole_word: false,
            last_whole_word: false,
            last_word_regex: None,
            include_collapsed_entries: false,
            last_include_collapsed_entries: false,
            last_view_interval: None,

            result_set: BTreeSet::new(),
            result_cache: BTreeMap::new(),
            entry_tree: BTreeMap::new(),
        }
    }

    fn clear(&mut self) {
        self.result_set.clear();
        self.result_cache.clear();
        self.entry_tree.clear();
    }

    fn ensure_valid_cache(&mut self, cx: &Context) {
        let mut invalidate = false;

        // Invalidate when the search query changes.
        if self.query != self.last_query {
            invalidate = true;
            self.last_query.clone_from(&self.query);
        }

        // Invalidate when the search field changes.
        if self.search_field != self.last_search_field {
            invalidate = true;
            self.last_search_field = self.search_field;
        }

        // Invalidate when the whole word setting changes.
        if self.whole_word != self.last_whole_word {
            invalidate = true;
            self.last_whole_word = self.whole_word;
        }

        // Invalidate when EXCLUDING collapsed entries. (I.e., because the
        // searched set shrinks. Growing is ok because search is monotonic.)
        if self.include_collapsed_entries != self.last_include_collapsed_entries
            && !self.include_collapsed_entries
        {
            invalidate = true;
            self.last_include_collapsed_entries = self.include_collapsed_entries;
        }

        // Invalidate when the view interval changes.
        if self.last_view_interval != Some(cx.view_interval) {
            invalidate = true;
            self.last_view_interval = Some(cx.view_interval);
        }

        if invalidate {
            if self.whole_word {
                let regex_string = format!("\\b{}\\b", escape(&self.query));
                self.last_word_regex = Some(Regex::new(&regex_string).unwrap());
            }

            self.clear();
        }
    }

    fn is_string_match(&self, s: &str) -> bool {
        if self.whole_word {
            let Some(regex) = &self.last_word_regex else {
                unreachable!();
            };
            regex.is_match(s)
        } else {
            s.contains(&self.query)
        }
    }

    fn is_field_match(&self, field: &Field) -> bool {
        match field {
            Field::String(s) => self.is_string_match(s),
            Field::ItemLink(ItemLink { title, .. }) => self.is_string_match(title),
            Field::Vec(fields) => fields.iter().any(|f| self.is_field_match(f)),
            _ => false,
        }
    }

    fn is_match(&self, item: &ItemMeta) -> bool {
        let field = self.search_field;
        if field == self.title_field {
            self.is_string_match(&item.title)
        } else if let Some(ItemField(_, value, _)) =
            item.fields.iter().find(|ItemField(x, _, _)| *x == field)
        {
            self.is_field_match(value)
        } else {
            false
        }
    }

    const MAX_SEARCH_RESULTS: usize = 100_000;

    fn start_entry<E: Entry>(&mut self, entry: &E) -> bool {
        // Early exit if we found enough items.
        if self.result_set.len() >= Self::MAX_SEARCH_RESULTS {
            return false;
        }

        // Double lookup is better than cloning unconditionally.
        if !self.result_cache.contains_key(entry.entry_id()) {
            self.result_cache
                .entry(entry.entry_id().clone())
                .or_default();
        }

        // Always recurse into tiles, because results can be fetched
        // asynchronously.
        true
    }

    fn start_tile<E: Entry>(&mut self, entry: &E, tile_id: TileID) -> bool {
        // Early exit if we found enough items.
        if self.result_set.len() >= Self::MAX_SEARCH_RESULTS {
            return false;
        }

        let mut result = true;
        // Always called second, so we know the entry exists.
        let cache = self.result_cache.get_mut(entry.entry_id()).unwrap();
        cache
            .entry(tile_id)
            .and_modify(|_| {
                result = false;
            })
            .or_default();
        result
    }

    fn insert<E: Entry>(&mut self, entry: &E, tile_id: TileID, irow: usize, item: &ItemMeta) {
        if self.result_set.len() >= Self::MAX_SEARCH_RESULTS {
            return;
        }

        // We want each item to appear once, so check the result set first
        // before inserting.
        if self.result_set.insert(item.item_uid) {
            let cache = self.result_cache.get_mut(entry.entry_id()).unwrap();
            let cache = cache.get_mut(&tile_id).unwrap();
            cache
                .entry(item.item_uid)
                .or_insert_with(|| SearchCacheItem {
                    item_uid: item.item_uid,
                    irow,
                    interval: item.original_interval,
                    title: item.title.clone(),
                });
        }
    }

    fn build_entry_tree(&mut self) {
        for (entry_id, cache) in &self.result_cache {
            let cache_size: u64 = cache.values().map(|x| x.len() as u64).sum();
            if cache_size == 0 {
                continue;
            }

            let level0_index = entry_id.slot_index(0).unwrap();
            let level1_index = entry_id.slot_index(1).unwrap();
            let level2_index = entry_id.slot_index(2).unwrap();

            let level0_subtree = self.entry_tree.entry(level0_index).or_default();
            let level1_subtree = level0_subtree.entry(level1_index).or_default();
            level1_subtree.insert(level2_index);
        }
    }
}

impl Config {
    fn new(data_source: Box<dyn DeferredDataSource>, info: DataSourceInfo) -> Self {
        let max_node = info.entry_info.nodes();
        let kinds = info.entry_info.kinds();
        let interval = info.interval;
        let tile_set = info.tile_set;
        let warning_message = info.warning_message;

        let mut field_schema = info.field_schema;
        assert!(!field_schema.contains_name("Title"));
        let title_id = field_schema.insert("Title".to_owned(), true);
        let search_state = SearchState::new(title_id);

        Self {
            field_schema,
            min_node: 0,
            max_node,
            kinds,
            kind_filter: BTreeSet::new(),
            interval,
            warning_message,
            data_source: CountingDeferredDataSource::new(LruDeferredDataSource::new(
                data_source,
                NonZeroUsize::new(1024).unwrap(),
            )),
            search_state,
            items_selected: BTreeMap::new(),
            scroll_to_item: None,
            scroll_to_item_retry: None,
            tile_manager: TileManager::new(tile_set, interval),
        }
    }

    fn request_tiles(&mut self, view_interval: Interval, full: bool) -> Vec<TileID> {
        self.tile_manager.request_tiles(view_interval, full)
    }

    fn invalidate_cache<T>(tile_ids: &[TileID], cache: &mut BTreeMap<TileID, T>) {
        TileManager::invalidate_cache(tile_ids, cache);
    }

    fn scroll_to_item(&mut self, item_loc: ItemLocator) {
        self.scroll_to_item = Some(item_loc.clone());
        self.scroll_to_item_retry = None;

        self.items_selected
            .entry(item_loc.item_uid)
            .or_insert_with(|| ItemDetail {
                meta: None,
                loc: item_loc,
            });
    }
}

impl Window {
    fn new(data_source: Box<dyn DeferredDataSource>, info: DataSourceInfo, index: u64) -> Self {
        Self {
            panel: Panel::new(&info.entry_info, EntryID::root()),
            index,
            config: Config::new(data_source, info),
        }
    }

    fn find_slot(&self, entry_id: &EntryID) -> Option<&Slot> {
        self.panel.find_slot(entry_id, 0)
    }

    fn find_slot_mut(&mut self, entry_id: &EntryID) -> Option<&mut Slot> {
        self.panel.find_slot_mut(entry_id, 0)
    }

    fn find_summary_mut(&mut self, entry_id: &EntryID) -> Option<&mut Summary> {
        self.panel.find_summary_mut(entry_id, 0)
    }

    fn expand_slot(&mut self, entry_id: &EntryID) {
        self.panel.expand_slot(entry_id, 0);
    }

    fn inflate_meta(&mut self, entry_id: &EntryID, cx: &mut Context) {
        // Use the panel version directly to avoid a mutability conflict
        let slot = self.panel.find_slot_mut(entry_id, 0).unwrap();
        slot.inflate_meta(&mut self.config, cx);
    }

    fn find_item_irow(&self, entry_id: &EntryID, item_uid: ItemUID) -> Option<usize> {
        let slot = self.find_slot(entry_id)?;
        for tile in slot.tiles.values() {
            let Some(Ok(tile)) = tile else {
                continue;
            };
            for (row, items) in tile.items.iter().enumerate() {
                for item in items {
                    if item.item_uid == item_uid {
                        let rows = tile.items.len();
                        return Some(rows - row - 1);
                    }
                }
            }
        }
        None
    }

    fn find_item_meta(&self, entry_id: &EntryID, item_uid: ItemUID) -> Option<&ItemMeta> {
        let slot = self.find_slot(entry_id)?;
        for tile in slot.tile_metas_full.values() {
            let Some(Ok(tile)) = tile else {
                continue;
            };
            for items in &tile.items {
                for item in items {
                    if item.item_uid == item_uid {
                        return Some(item);
                    }
                }
            }
        }
        None
    }

    fn content(&mut self, ui: &mut egui::Ui, cx: &mut Context) {
        ui.horizontal(|ui| {
            ui.heading(format!("Profile {}", self.index));
            ui.label(cx.view_interval.to_string());
            if let Some(message) = &self.config.warning_message {
                ui.label(RichText::new(message).color(Color32::RED));
            }
        });

        ScrollArea::vertical()
            .auto_shrink([false; 2])
            .show_viewport(ui, |ui, viewport| {
                let height = self.panel.height(None, &self.config, cx);
                ui.set_height(height);
                ui.set_width(ui.available_width());

                let rect = Rect::from_min_size(ui.min_rect().min, viewport.size());

                let scroll_to = |irow, prefix_height| {
                    let mut item_rect =
                        rect.translate(Vec2::new(0.0, prefix_height + irow as f32 * cx.row_height));
                    item_rect.set_height(cx.row_height);
                    ui.scroll_to_rect(item_rect, Some(egui::Align::Center));
                };

                // First scroll attempt goes to the processor
                if let Some(ItemLocator {
                    ref entry_id, irow, ..
                }) = self.config.scroll_to_item
                {
                    let prefix_height = self.panel.height(Some(entry_id), &self.config, cx);
                    scroll_to(irow.unwrap_or(0), prefix_height);
                    if irow.is_none() {
                        let mut item = None;
                        std::mem::swap(&mut item, &mut self.config.scroll_to_item);
                        self.config.scroll_to_item_retry = item;
                    }
                    self.config.scroll_to_item = None;
                }

                // If we're able to find the item, we do a second scroll to the item
                let mut found_irow = None;
                if let Some(ItemLocator {
                    ref entry_id,
                    irow,
                    item_uid,
                }) = self.config.scroll_to_item_retry
                {
                    assert!(irow.is_none());
                    found_irow = self.find_item_irow(entry_id, item_uid);
                }

                if let Some(ItemLocator { ref entry_id, .. }) = self.config.scroll_to_item_retry {
                    if let Some(irow) = found_irow {
                        let prefix_height = self.panel.height(Some(entry_id), &self.config, cx);
                        scroll_to(irow, prefix_height);
                        self.config.scroll_to_item_retry = None;
                    }
                }

                // Root panel has no label
                self.panel.content(ui, rect, viewport, &mut self.config, cx);
            });
    }

    fn node_selection(&mut self, ui: &mut egui::Ui, cx: &Context) {
        ui.subheading("Node Selection", cx);
        let total = self.panel.slots.len().saturating_sub(1) as u64;
        let min_node = &mut self.config.min_node;
        let max_node = &mut self.config.max_node;
        ui.add(Slider::new(min_node, 0..=total).text("First"));
        if *min_node > *max_node {
            *max_node = *min_node;
        }
        ui.add(Slider::new(max_node, 0..=total).text("Last"));
        if *min_node > *max_node {
            *min_node = *max_node;
        }
    }

    fn filter_by_kind(&mut self, ui: &mut egui::Ui, cx: &Context) {
        ui.subheading("Filter by Kind", cx);
        ui.horizontal_wrapped(|ui| {
            for kind in &self.config.kinds {
                let initial = self.config.kind_filter.contains(kind);
                let mut enabled = initial;
                ui.toggle_value(&mut enabled, kind);
                if initial != enabled {
                    if enabled {
                        self.config.kind_filter.insert(kind.clone());
                    } else {
                        self.config.kind_filter.remove(kind);
                    }
                }
            }
        });
    }

    fn expand_collapse(&mut self, ui: &mut egui::Ui, cx: &Context) {
        let mut toggle_all = |label, toggle| {
            for node in &mut self.panel.slots {
                for kind in &mut node.slots {
                    if kind.expanded == toggle && kind.label_text() == label {
                        kind.toggle_expanded();
                    }
                }
            }
        };

        ui.subheading("Expand/Collapse", cx);
        ui.label("Expand by kind:");
        ui.horizontal_wrapped(|ui| {
            for kind in &self.config.kinds {
                if ui.button(kind).clicked() {
                    toggle_all(kind.to_lowercase(), false);
                }
            }
        });
        ui.label("Collapse by kind:");
        ui.horizontal_wrapped(|ui| {
            for kind in &self.config.kinds {
                if ui.button(kind).clicked() {
                    toggle_all(kind.to_lowercase(), true);
                }
            }
        });
    }

    fn select_interval(&mut self, ui: &mut egui::Ui, cx: &mut Context) {
        ui.subheading("Interval", cx);
        let start_res = ui
            .horizontal(|ui| {
                ui.label("Start:");
                ui.text_edit_singleline(&mut cx.interval_select_state.start_buffer)
            })
            .inner;

        if let Some(error) = cx.interval_select_state.start_error {
            ui.label(RichText::new(error.to_string()).color(Color32::RED));
        }

        let stop_res = ui
            .horizontal(|ui| {
                ui.label("Stop:");
                ui.text_edit_singleline(&mut cx.interval_select_state.stop_buffer)
            })
            .inner;

        if let Some(error) = cx.interval_select_state.stop_error {
            ui.label(RichText::new(error.to_string()).color(Color32::RED));
        }

        if start_res.lost_focus()
            && cx.interval_select_state.start_buffer != cx.view_interval.start.to_string()
        {
            match Timestamp::parse(&cx.interval_select_state.start_buffer) {
                Ok(start) => {
                    // validate timestamp
                    if start > cx.view_interval.stop {
                        cx.interval_select_state.start_error =
                            Some(IntervalSelectError::StartAfterStop);
                        return;
                    }
                    if start > cx.total_interval.stop {
                        cx.interval_select_state.start_error =
                            Some(IntervalSelectError::StartAfterEnd);
                        return;
                    }
                    let target = Interval::new(start, cx.view_interval.stop);
                    ProfApp::zoom(cx, target);
                }
                Err(e) => {
                    cx.interval_select_state.start_error = Some(e.into());
                }
            }
        }
        if stop_res.lost_focus()
            && cx.interval_select_state.stop_buffer != cx.view_interval.stop.to_string()
        {
            match Timestamp::parse(&cx.interval_select_state.stop_buffer) {
                Ok(stop) => {
                    // validate timestamp
                    if stop < cx.view_interval.start {
                        cx.interval_select_state.stop_error =
                            Some(IntervalSelectError::StopBeforeStart);
                        return;
                    }
                    let target = Interval::new(cx.view_interval.start, stop);
                    ProfApp::zoom(cx, target);
                }
                Err(e) => {
                    cx.interval_select_state.stop_error = Some(e.into());
                }
            }
        }
    }

    fn controls(&mut self, ui: &mut egui::Ui, cx: &mut Context) {
        const WIDGET_PADDING: f32 = 8.0;
        ui.heading(format!("Profile {}: Controls", self.index));
        ui.add_space(WIDGET_PADDING);
        self.node_selection(ui, cx);
        ui.add_space(WIDGET_PADDING);
        self.filter_by_kind(ui, cx);
        ui.add_space(WIDGET_PADDING);
        self.expand_collapse(ui, cx);
        ui.add_space(WIDGET_PADDING);
        self.select_interval(ui, cx);
    }

    fn search(&mut self, cx: &mut Context) {
        // Invalidate cache if the search query changed.
        self.config.search_state.ensure_valid_cache(cx);

        // If search query empty, skip search. (Note: do this after
        // invalidating cache, otherwise we get leftover search results when
        // clearing the query.)
        if self.config.search_state.query.is_empty() {
            return;
        }

        // Expand meta tiles. (Including collapsed entries, if requested).
        self.panel.inflate_meta(&mut self.config, cx);

        // Search whatever data we have. Results are cached by entry/tile.
        self.panel.search(&mut self.config);

        // Cache is now full and we can highlight/render the entries.
    }

    fn search_box(&mut self, ui: &mut egui::Ui, cx: &mut Context) {
        ui.horizontal(|ui| {
            // Hack: need to estimate the button width or else the text box
            // overflows. Refer to the source for egui::widgets::Button::ui
            // for calculations.
            let button_label = "✖";
            let button_padding = ui.spacing().button_padding;
            let available_width = ui.available_width() - 2.0 * button_padding.x;
            let button_text: egui::WidgetText = "✖".into();
            let button_text =
                button_text.into_galley(ui, None, available_width, egui::TextStyle::Button);
            let button_size = button_text.size() + 2.0 * button_padding;

            const MARGIN: f32 = 4.0; // From egui::TextEdit::margin
            let query_size =
                ui.available_size().x - button_size.x - ui.spacing().item_spacing.x - 2.0 * MARGIN;
            egui::TextEdit::singleline(&mut self.config.search_state.query)
                .desired_width(query_size)
                .show(ui);
            if ui.button(button_label).clicked() {
                self.config.search_state.query.clear();
            }
        });
        ui.horizontal(|ui| {
            ui.label("Search field:");
            let schema = &self.config.field_schema;
            let search_field = &mut self.config.search_state.search_field;
            egui::ComboBox::from_id_salt("Search field")
                .selected_text(schema.get_name(*search_field).unwrap())
                .show_ui(ui, |ui| {
                    for field in schema.searchable() {
                        let name = schema.get_name(*field).unwrap();
                        ui.selectable_value(search_field, *field, name);
                    }
                });
        });
        ui.checkbox(
            &mut self.config.search_state.whole_word,
            "Match whole words only",
        );
        ui.checkbox(
            &mut self.config.search_state.include_collapsed_entries,
            "Include collapsed processors",
        );

        self.search(cx);
    }

    fn search_results(&mut self, ui: &mut egui::Ui, cx: &mut Context) {
        if self.config.search_state.query.is_empty() {
            ui.label("Enter a search to see results displayed here.");
            return;
        }

        if self.config.search_state.result_set.is_empty() {
            ui.label("No results found. Expand search to include collapsed processors?");

            return;
        }

        let num_results = self.config.search_state.result_set.len();
        if num_results >= SearchState::MAX_SEARCH_RESULTS {
            ui.label(format!(
                "Found {} results. (Limited to {}.)",
                num_results,
                SearchState::MAX_SEARCH_RESULTS
            ));
        } else {
            ui.label(format!("Found {} results.", num_results));
        }

        self.config.search_state.build_entry_tree();

        let mut scroll_target = None;
        ScrollArea::vertical()
            // Hack: estimate size of bottom UI.
            .max_height(ui.available_height() - 70.0)
            .auto_shrink([false; 2])
            .show(ui, |ui| {
                let root_tree = &self.config.search_state.entry_tree;
                for (level0_index, level0_subtree) in root_tree {
                    let level0_slot = &mut self.panel.slots[*level0_index as usize];
                    ui.collapsing(&level0_slot.long_name, |ui| {
                        for (level1_index, level1_subtree) in level0_subtree {
                            let level1_slot = &mut level0_slot.slots[*level1_index as usize];
                            ui.collapsing(&level1_slot.long_name, |ui| {
                                for level2_index in level1_subtree {
                                    let level2_slot =
                                        &mut level1_slot.slots[*level2_index as usize];
                                    ui.collapsing(&level2_slot.long_name, |ui| {
                                        let cache = &self.config.search_state.result_cache;
                                        let cache = cache.get(&level2_slot.entry_id).unwrap();
                                        for tile_cache in cache.values() {
                                            for item in tile_cache.values() {
                                                let button =
                                                    egui::widgets::Button::new(&item.title).small();
                                                if ui.add(button).clicked() {
                                                    let interval = item
                                                        .interval
                                                        .grow(item.interval.duration_ns() / 20);
                                                    ProfApp::zoom(cx, interval);
                                                    scroll_target = Some(ItemLocator {
                                                        entry_id: level2_slot.entry_id.clone(),
                                                        irow: Some(item.irow),
                                                        item_uid: item.item_uid,
                                                    });
                                                    level2_slot.expanded = true;
                                                    level1_slot.expanded = true;
                                                    level0_slot.expanded = true;
                                                }
                                            }
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        if let Some(target) = scroll_target {
            self.config.scroll_to_item(target);
        }
    }

    fn search_controls(&mut self, ui: &mut egui::Ui, cx: &mut Context) {
        const WIDGET_PADDING: f32 = 8.0;
        ui.heading(format!("Profile {}: Search", self.index));
        ui.add_space(WIDGET_PADDING);
        self.search_box(ui, cx);
        ui.add_space(WIDGET_PADDING);
        self.search_results(ui, cx);
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
enum PanDirection {
    Left,
    Right,
}

impl ProfApp {
    /// Called once before the first frame.
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        mut data_sources: Vec<Box<dyn DeferredDataSource>>,
    ) -> Self {
        // This is also where you can customized the look at feel of egui using
        // `cc.egui_ctx.set_visuals` and `cc.egui_ctx.set_fonts`.

        // Load previous app state (if any).
        // Note that you must enable the `persistence` feature for this to work.
        let mut result: Self = if let Some(storage) = cc.storage {
            eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default()
        } else {
            Default::default()
        };

        for data_source in &mut data_sources {
            data_source.fetch_info();
        }
        result.pending_data_sources.clear();
        result.pending_data_sources.extend(data_sources);

        result.windows.clear();

        result.cx.scale_factor = 1.0;
        result.cx.row_scroll_delta = 0;

        #[cfg(not(target_arch = "wasm32"))]
        {
            result.last_update = Some(Instant::now());
        }

        // Set solid scroll bar (default from egui pre-0.24)
        // The new default "thin" style isn't clickable with our canvas widget
        cc.egui_ctx.style_mut(|style| {
            style.spacing.scroll = egui::style::ScrollStyle::solid();
        });

        result
    }

    fn update_interval_select_state(cx: &mut Context) {
        cx.interval_select_state.start_buffer = cx.view_interval.start.to_string();
        cx.interval_select_state.stop_buffer = cx.view_interval.stop.to_string();
        cx.interval_select_state.start_error = None;
        cx.interval_select_state.stop_error = None;
    }

    fn update_view_interval(cx: &mut Context, interval: Interval, origin: IntervalOrigin) {
        cx.view_interval = interval;

        let history = &mut cx.view_interval_history;
        let index = history.index;

        // Only keep at most one Pan origin in a row
        if !history.levels.is_empty()
            && history.origins[index] == IntervalOrigin::Pan
            && origin == IntervalOrigin::Pan
        {
            history.levels.truncate(index);
            history.origins.truncate(index);
        }

        history.levels.truncate(index + 1);
        history.levels.push(interval);
        history.origins.truncate(index + 1);
        history.origins.push(origin);
        history.index = history.levels.len() - 1;
    }

    fn pan(cx: &mut Context, percent: PercentageInteger, dir: PanDirection) {
        if percent.value() == 0 {
            return;
        }

        let duration = percent.apply_to(cx.view_interval.duration_ns());
        let sign = match dir {
            PanDirection::Left => -1,
            PanDirection::Right => 1,
        };
        let interval = cx.view_interval.translate(duration * sign);

        ProfApp::update_view_interval(cx, interval, IntervalOrigin::Pan);
        ProfApp::update_interval_select_state(cx);
    }

    fn zoom(cx: &mut Context, interval: Interval) {
        if cx.view_interval == interval {
            return;
        }

        ProfApp::update_view_interval(cx, interval, IntervalOrigin::Zoom);
        ProfApp::update_interval_select_state(cx);
    }

    fn undo_pan_zoom(cx: &mut Context) {
        if cx.view_interval_history.index == 0 {
            return;
        }
        cx.view_interval_history.index -= 1;
        cx.view_interval = cx.view_interval_history.levels[cx.view_interval_history.index];
        ProfApp::update_interval_select_state(cx);
    }

    fn redo_pan_zoom(cx: &mut Context) {
        if cx.view_interval_history.index + 1 >= cx.view_interval_history.levels.len() {
            return;
        }
        cx.view_interval_history.index += 1;
        cx.view_interval = cx.view_interval_history.levels[cx.view_interval_history.index];
        ProfApp::update_interval_select_state(cx);
    }

    fn zoom_in(cx: &mut Context) {
        let quarter = -cx.view_interval.duration_ns() / 4;
        Self::zoom(cx, cx.view_interval.grow(quarter));
    }

    fn zoom_out(cx: &mut Context) {
        let half = cx.view_interval.duration_ns() / 2;
        Self::zoom(
            cx,
            cx.view_interval.grow(half).intersection(cx.total_interval),
        );
    }

    fn multiply_scale_factor(cx: &mut Context, factor: f32) {
        cx.scale_factor = (cx.scale_factor * factor).clamp(0.25, 4.0);
    }

    fn reset_scale_factor(cx: &mut Context) {
        cx.scale_factor = 1.0;
    }

    fn reset_ui(cx: &mut Context, windows: &mut [Window]) {
        cx.show_controls = false;
        for window in windows.iter_mut() {
            window.config.items_selected.clear();
        }
    }

    fn keyboard(ctx: &egui::Context, cx: &mut Context, windows: &mut [Window]) {
        // Focus is elsewhere, don't check any keys
        if ctx.memory(|m| m.focused().is_some()) {
            return;
        }

        enum Actions {
            ZoomIn,
            ZoomOut,
            UndoZoom,
            RedoZoom,
            ResetZoom,
            Pan(PercentageInteger, PanDirection),
            Scroll(i32),
            ExpandVertical,
            ShrinkVertical,
            ResetVertical,
            ToggleControls,
            ResetUI,
            NoAction,
        }
        let action = ctx.input(|i| {
            if i.modifiers.ctrl {
                if i.modifiers.alt {
                    if i.key_pressed(egui::Key::Plus) || i.key_pressed(egui::Key::Equals) {
                        Actions::ExpandVertical
                    } else if i.key_pressed(egui::Key::Minus) {
                        Actions::ShrinkVertical
                    } else if i.key_pressed(egui::Key::Num0) {
                        Actions::ResetVertical
                    } else {
                        Actions::NoAction
                    }
                } else if i.key_pressed(egui::Key::Plus) || i.key_pressed(egui::Key::Equals) {
                    Actions::ZoomIn
                } else if i.key_pressed(egui::Key::Minus) {
                    Actions::ZoomOut
                } else if i.key_pressed(egui::Key::ArrowLeft) {
                    Actions::UndoZoom
                } else if i.key_pressed(egui::Key::ArrowRight) {
                    Actions::RedoZoom
                } else if i.key_pressed(egui::Key::Num0) {
                    Actions::ResetZoom
                } else {
                    Actions::NoAction
                }
            } else if i.modifiers.shift {
                if i.key_pressed(egui::Key::ArrowLeft) {
                    Actions::Pan(Percentage::from(1), PanDirection::Left)
                } else if i.key_pressed(egui::Key::ArrowRight) {
                    Actions::Pan(Percentage::from(1), PanDirection::Right)
                } else if i.key_pressed(egui::Key::ArrowUp) {
                    Actions::Scroll(1)
                } else if i.key_pressed(egui::Key::ArrowDown) {
                    Actions::Scroll(-1)
                } else {
                    Actions::NoAction
                }
            } else if i.key_pressed(egui::Key::H) {
                Actions::ToggleControls
            } else if i.key_pressed(egui::Key::Escape) {
                Actions::ResetUI
            } else if i.key_pressed(egui::Key::ArrowLeft) {
                Actions::Pan(Percentage::from(5), PanDirection::Left)
            } else if i.key_pressed(egui::Key::ArrowRight) {
                Actions::Pan(Percentage::from(5), PanDirection::Right)
            } else if i.key_pressed(egui::Key::ArrowUp) {
                Actions::Scroll(5)
            } else if i.key_pressed(egui::Key::ArrowDown) {
                Actions::Scroll(-5)
            } else {
                Actions::NoAction
            }
        });
        match action {
            Actions::ZoomIn => ProfApp::zoom_in(cx),
            Actions::ZoomOut => ProfApp::zoom_out(cx),
            Actions::UndoZoom => ProfApp::undo_pan_zoom(cx),
            Actions::RedoZoom => ProfApp::redo_pan_zoom(cx),
            Actions::ResetZoom => ProfApp::zoom(cx, cx.total_interval),
            Actions::Pan(percent, dir) => ProfApp::pan(cx, percent, dir),
            Actions::Scroll(rows) => cx.row_scroll_delta = rows,
            Actions::ExpandVertical => ProfApp::multiply_scale_factor(cx, 2.0),
            Actions::ShrinkVertical => ProfApp::multiply_scale_factor(cx, 0.5),
            Actions::ResetVertical => ProfApp::reset_scale_factor(cx),
            Actions::ToggleControls => cx.show_controls = !cx.show_controls,
            Actions::ResetUI => ProfApp::reset_ui(cx, windows),
            Actions::NoAction => {}
        }
    }

    fn cursor(ui: &mut egui::Ui, cx: &mut Context) {
        // Hack: the UI rect we have at this point is not where the
        // timeline is being drawn. So fish out the coordinates we
        // need to draw the correct rect.

        // Sometimes slot_rect is None when initializing the UI
        if cx.slot_rect.is_none() {
            return;
        }

        let ui_rect = ui.min_rect();
        let slot_rect = cx.slot_rect.unwrap();
        let rect = Rect::from_min_max(
            Pos2::new(slot_rect.min.x, ui_rect.min.y),
            Pos2::new(slot_rect.max.x, ui_rect.max.y),
        );

        let response = ui.allocate_rect(rect, egui::Sense::drag());

        // Handle drag detection
        let mut drag_interval = None;

        let is_active_drag = response.dragged_by(egui::PointerButton::Primary);
        if is_active_drag && response.drag_started() {
            // On the beginning of a drag, save our position so we can
            // calculate the delta
            cx.drag_origin = response.interact_pointer_pos();
        }

        if let Some(origin) = cx.drag_origin {
            // We're in a drag, calculate the drag inetrval
            let current = response.interact_pointer_pos().unwrap();
            let min = origin.x.min(current.x);
            let max = origin.x.max(current.x);

            let start = (min - rect.left()) / rect.width();
            let start = cx.view_interval.lerp(start);
            let stop = (max - rect.left()) / rect.width();
            let stop = cx.view_interval.lerp(stop);

            let interval = Interval::new(start, stop);

            if is_active_drag {
                // Still in drag, draw a rectangle to show the dragged region
                let drag_rect =
                    Rect::from_min_max(Pos2::new(min, rect.min.y), Pos2::new(max, rect.max.y));
                let color = Color32::DARK_GRAY.linear_multiply(0.5);
                ui.painter().rect(drag_rect, 0.0, color, Stroke::NONE);

                drag_interval = Some(interval);
            } else if response.drag_stopped() {
                // Only set view interval if the drag was a certain amount
                const MIN_DRAG_DISTANCE: f32 = 4.0;
                if max - min > MIN_DRAG_DISTANCE {
                    ProfApp::zoom(cx, interval);
                }

                cx.drag_origin = None;
            }
        }

        // Handle hover detection
        if let Some(hover) = response.hover_pos() {
            let visuals = ui.style().interact_selectable(&response, false);

            // Draw vertical line through cursor
            const RADIUS: f32 = 12.0;
            let top = Pos2::new(hover.x, ui.min_rect().min.y);
            let mid_top = Pos2::new(hover.x, (hover.y - RADIUS).at_least(ui.min_rect().min.y));
            let mid_bottom = Pos2::new(hover.x, (hover.y + RADIUS).at_most(ui.min_rect().max.y));
            let bottom = Pos2::new(hover.x, ui.min_rect().max.y);
            ui.painter().line_segment([top, mid_top], visuals.fg_stroke);
            ui.painter()
                .line_segment([mid_bottom, bottom], visuals.fg_stroke);

            // Show timestamp popup
            let time = (hover.x - rect.left()) / rect.width();
            let time = cx.view_interval.lerp(time);

            let label_text = if let Some(drag) = drag_interval {
                format!("{drag}")
            } else {
                let units: TimestampUnits = cx.view_interval.into();
                let time_units = TimestampDisplay {
                    timestamp: time,
                    units,
                    include_units: true,
                };
                format!("t={time_units}")
            };

            ui.show_tooltip_at("timestamp_tooltip", top, label_text);
        }
    }

    fn display_controls(ui: &mut egui::Ui, mode: &mut ItemLinkNavigationMode) {
        fn show_row_ui(
            body: &mut egui_extras::TableBody<'_>,
            label: &str,
            thunk: impl FnMut(&mut egui::Ui),
        ) {
            body.row(20.0, |mut row| {
                row.col(|ui| {
                    ui.strong(label);
                });
                row.col(thunk);
            });
        }

        TableBuilder::new(ui)
            .striped(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
            .column(Column::auto())
            .column(Column::remainder())
            .body(|mut body| {
                let mut show_row = |a, b| {
                    show_row_ui(&mut body, a, |ui| {
                        ui.label(b);
                    });
                };
                show_row("Zoom to Interval", "Click and Drag");
                show_row("Pan 5%", "Left/Right Arrow");
                show_row("Pan 1%", "Shift + Left/Right Arrow");
                show_row("Vertical Scroll", "Up/Down Arrow");
                show_row("Fine Vertical Scroll", "Shift + Up/Down Arrow");
                show_row("Zoom In", "Ctrl + Plus/Equals");
                show_row("Zoom Out", "Ctrl + Minus");
                show_row("Undo Pan/Zoom", "Ctrl + Left Arrow");
                show_row("Redo Pan/Zoom", "Ctrl + Right Arrow");
                show_row("Reset Pan/Zoom", "Ctrl + 0");
                show_row("Expand Vertical Spacing", "Ctrl + Alt + Plus/Equals");
                show_row("Shrink Vertical Spacing", "Ctrl + Alt + Minus");
                show_row("Reset Vertical Spacing", "Ctrl + Alt + 0");
                show_row("Toggle This Window", "H");
                show_row_ui(&mut body, "Item Link Zoom or Pan", |ui: &mut _| {
                    egui::ComboBox::from_id_salt("Item Link Zoom or Pan")
                        .selected_text(format!("{:?}", mode))
                        .show_ui(ui, |ui| {
                            ui.selectable_value(mode, ItemLinkNavigationMode::Zoom, "Zoom");
                            ui.selectable_value(mode, ItemLinkNavigationMode::Pan, "Pan");
                        });
                });
            });
    }

    fn compute_text_height(text: String, width: f32, ui: &mut egui::Ui) -> f32 {
        let style = ui.style();
        let font_id = TextStyle::Body.resolve(style);
        let visuals = style.noninteractive();
        let layout = ui
            .painter()
            .layout(text, font_id, visuals.text_color(), width);

        layout.size().y + style.spacing.item_spacing.y * 2.0
    }

    fn render_field_as_text(
        field: &Field,
        mode: ItemLinkNavigationMode,
    ) -> Vec<(String, Option<&'static str>)> {
        match field {
            Field::I64(value) => vec![(format!("{value}"), None)],
            Field::U64(value) => vec![(format!("{value}"), None)],
            Field::String(value) => vec![(value.to_string(), None)],
            Field::Interval(value) => vec![(format!("{value}"), None)],
            Field::ItemLink(ItemLink { title, .. }) => {
                vec![(title.to_string(), Some(mode.label_text()))]
            }
            Field::Vec(fields) => fields
                .iter()
                .flat_map(|f| Self::render_field_as_text(f, mode))
                .collect(),
            Field::Empty => vec![("".to_string(), None)],
        }
    }

    fn compute_field_height(
        field: &Field,
        width: f32,
        mode: ItemLinkNavigationMode,
        ui: &mut egui::Ui,
    ) -> f32 {
        let text = Self::render_field_as_text(field, mode);
        text.into_iter()
            .map(|(mut v, b)| {
                // Hack: if we have button text, guess how much space it will need
                // by extending the string.
                if let Some(b) = b {
                    v.push(' ');
                    v.push_str(b);
                }
                Self::compute_text_height(v, width, ui)
            })
            .sum()
    }

    fn render_field_as_ui(
        field: &Field,
        color: Option<Color32>,
        mode: ItemLinkNavigationMode,
        ui: &mut egui::Ui,
    ) -> Option<(ItemLocator, Interval)> {
        let mut result = None;
        let label = |ui: &mut egui::Ui, v| {
            if let Some(color) = color {
                ui.add(
                    egui::Label::new(RichText::new(v).color(color)).wrap_mode(TextWrapMode::Wrap),
                );
            } else {
                ui.add(egui::Label::new(v).wrap_mode(TextWrapMode::Wrap));
            }
        };
        let label_button = |ui: &mut egui::Ui, v, b| {
            label(ui, v);
            if let Some(color) = color {
                ui.button(RichText::new(b).color(color)).clicked()
            } else {
                ui.button(b).clicked()
            }
        };
        match field {
            Field::I64(value) => label(ui, &format!("{value}")),
            Field::U64(value) => label(ui, &format!("{value}")),
            Field::String(value) => label(ui, value),
            Field::Interval(value) => label(ui, &format!("{value}")),
            Field::ItemLink(ItemLink {
                title,
                item_uid,
                interval,
                entry_id,
            }) => {
                if label_button(ui, title, mode.label_text()) {
                    result = Some((
                        ItemLocator {
                            entry_id: entry_id.clone(),
                            irow: None,
                            item_uid: *item_uid,
                        },
                        *interval,
                    ));
                }
            }
            Field::Vec(fields) => {
                ui.vertical(|ui| {
                    for f in fields {
                        ui.horizontal(|ui| {
                            if let Some(x) = Self::render_field_as_ui(f, color, mode, ui) {
                                result = Some(x);
                            }
                        });
                    }
                });
            }
            Field::Empty => {}
        }
        result
    }

    fn display_item_details(
        ui: &mut egui::Ui,
        item: &ItemDetail,
        field_schema: &FieldSchema,
        cx: &Context,
    ) -> Option<(ItemLocator, Interval)> {
        let Some(ref item_meta) = item.meta else {
            ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                ui.label("Item will be displayed once data is available.");
            });
            return None;
        };

        let font_id = TextStyle::Body.resolve(ui.style());
        let row_height = ui.fonts(|f| f.row_height(&font_id));

        let mut result: Option<(ItemLocator, Interval)> = None;
        TableBuilder::new(ui)
            .striped(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
            .column(Column::auto())
            .column(Column::remainder())
            .body(|mut body| {
                let mut show_row = |k: &str, field: &Field, color: Option<Color32>| {
                    // We need to manually work out the height of the labels
                    // so that the table knows how large to make each row.
                    let width = body.widths()[1];

                    let ui = body.ui_mut();
                    let height = Self::compute_field_height(field, width, cx.item_link_mode, ui)
                        .max(row_height);

                    body.row(height, |mut row| {
                        row.col(|ui| {
                            if let Some(color) = color {
                                ui.label(RichText::new(k).color(color).strong());
                            } else {
                                ui.strong(k);
                            }
                        });
                        row.col(|ui| {
                            if let Some(x) =
                                Self::render_field_as_ui(field, color, cx.item_link_mode, ui)
                            {
                                result = Some(x);
                            }
                        });
                    });
                };

                show_row("Title", &Field::String(item_meta.title.to_string()), None);
                if cx.debug {
                    show_row("Item UID", &Field::U64(item_meta.item_uid.0), None);
                }
                for ItemField(field_id, field, color) in &item_meta.fields {
                    let name = field_schema.get_name(*field_id).unwrap();
                    show_row(name, field, *color);
                }
            });
        ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
            if ui.button(cx.item_link_mode.label_text()).clicked() {
                result = Some((item.loc.clone(), item_meta.original_interval));
            }
        });
        result
    }
}

impl eframe::App for ProfApp {
    /// Called to save state before shutdown.
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, self);
    }

    /// Called each time the UI needs repainting.
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let Self {
            pending_data_sources,
            windows,
            cx,
            #[cfg(not(target_arch = "wasm32"))]
            last_update,
            ..
        } = self;

        if let Some(mut source) = pending_data_sources.pop_front() {
            // We made one request, so we know there is always zero or one
            // elements in this list.
            if let Some(info) = source.get_infos().pop() {
                // TODO: show this in a more user-friendly way.
                let info = info.expect("fetch_info failed");
                let window = Window::new(source, info, windows.len() as u64);
                if windows.is_empty() {
                    cx.total_interval = window.config.interval;
                } else {
                    cx.total_interval = cx.total_interval.union(window.config.interval);
                }
                ProfApp::zoom(cx, cx.total_interval);
                windows.push(window);
            } else {
                pending_data_sources.push_front(source);
            }
        }

        for window in windows.iter_mut() {
            for (tile, req) in window.config.data_source.get_summary_tiles() {
                if let Some(entry) = window.find_summary_mut(&req.entry_id) {
                    // If the entry doesn't exist, we already zoomed away and
                    // are no longer interested in this tile.
                    entry
                        .tiles
                        .entry(req.tile_id)
                        .and_modify(|t| *t = Some(tile.map(|s| s.data)));
                }
            }

            for (tile, req) in window.config.data_source.get_slot_tiles() {
                if let Some(entry) = window.find_slot_mut(&req.entry_id) {
                    // If the entry doesn't exist, we already zoomed away and
                    // are no longer interested in this tile.
                    entry
                        .tiles
                        .entry(req.tile_id)
                        .and_modify(|t| *t = Some(tile.map(|s| s.data)));
                }
            }

            for (tile, req) in window.config.data_source.get_slot_meta_tiles() {
                if let Some(entry) = window.find_slot_mut(&req.entry_id) {
                    // If the entry doesn't exist, we already zoomed away and
                    // are no longer interested in this tile.
                    let metas = if req.full {
                        &mut entry.tile_metas_full
                    } else {
                        &mut entry.tile_metas
                    };
                    metas
                        .entry(req.tile_id)
                        .and_modify(|t| *t = Some(tile.map(|s| s.data)));
                }
            }
        }

        let mut _fps = 0.0;
        #[cfg(not(target_arch = "wasm32"))]
        {
            let now = Instant::now();
            if let Some(last) = last_update {
                _fps = 1.0 / now.duration_since(*last).as_secs_f64();
            }
            *last_update = Some(now);
        }

        #[cfg(not(target_arch = "wasm32"))]
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Quit").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
            });
        });

        egui::SidePanel::left("side_panel").show(ctx, |ui| {
            let body = TextStyle::Body.resolve(ui.style()).size;
            let heading = TextStyle::Heading.resolve(ui.style()).size;
            // Just set this on every frame for now
            cx.subheading_size = (heading + body) * 0.5;

            const WIDGET_PADDING: f32 = 8.0;
            ui.add_space(WIDGET_PADDING);

            for window in windows.iter_mut() {
                egui::Frame::group(ui.style()).show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    window.controls(ui, cx);
                });
            }

            for window in windows.iter_mut() {
                egui::Frame::group(ui.style()).show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    window.search_controls(ui, cx);
                });
            }

            ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;
                    ui.label("powered by ");
                    ui.hyperlink_to("egui", "https://github.com/emilk/egui");
                    ui.label(" and ");
                    ui.hyperlink_to(
                        "eframe",
                        "https://github.com/emilk/egui/tree/master/crates/eframe",
                    );
                    ui.label(".");
                });

                ui.horizontal(|ui| {
                    egui::widgets::global_theme_preference_buttons(ui);
                });

                ui.horizontal(|ui| {
                    if ui.button("Show Controls").clicked() {
                        cx.show_controls = true;
                    }

                    ui.toggle_value(&mut cx.debug, "🛠 Debug");

                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        if cx.debug {
                            ui.label(format!("FPS: {_fps:.0}"));
                        }
                    }
                });

                ui.separator();
                egui::warn_if_debug_build(ui);
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            // Use body font to figure out how tall to draw rectangles.
            let font_id = TextStyle::Body.resolve(ui.style());
            let row_height = ui.fonts(|f| f.row_height(&font_id));
            // Just set this on every frame for now
            cx.row_height = row_height * cx.scale_factor;

            let y_scroll_delta = cx.row_height * cx.row_scroll_delta as f32;
            ui.scroll_with_delta(Vec2::new(0.0, y_scroll_delta));
            cx.row_scroll_delta = 0;

            let mut remaining = windows.len();
            // Only wrap in a frame if more than one profile
            if remaining > 1 {
                for window in windows.iter_mut() {
                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.push_id(window.index, |ui| {
                            ui.set_height(ui.available_height() / (remaining as f32));
                            ui.set_width(ui.available_width());
                            window.content(ui, cx);
                            remaining -= 1;
                        });
                    });
                }
            } else {
                for window in windows.iter_mut() {
                    window.content(ui, cx);
                }
            }

            Self::cursor(ui, cx);
        });

        egui::Window::new("Controls")
            .open(&mut cx.show_controls)
            .resizable(false)
            .show(ctx, |ui| Self::display_controls(ui, &mut cx.item_link_mode));

        for window in windows.iter_mut() {
            let mut zoom_target = None;

            // Hack: work around mutability conflict
            let mut items_selected = BTreeMap::new();
            std::mem::swap(&mut items_selected, &mut window.config.items_selected);
            items_selected.retain(|_, item| {
                // Populate the item meta if it's not already there
                if item.meta.is_none() {
                    window.inflate_meta(&item.loc.entry_id, cx);
                    if let Some(meta) = window.find_item_meta(&item.loc.entry_id, item.loc.item_uid)
                    {
                        item.meta = Some(meta.clone());
                    }
                }

                let short_title = match &item.meta {
                    Some(meta) => meta.title.chars().take(50).collect(),
                    None => format!("Item <Item UID: {}>", item.loc.item_uid.0),
                };

                let mut enabled = true;
                egui::Window::new(short_title)
                    .id(egui::Id::new(("details_window", item.loc.item_uid.0)))
                    .open(&mut enabled)
                    .resizable(true)
                    .show(ctx, |ui| {
                        let target =
                            Self::display_item_details(ui, item, &window.config.field_schema, cx);
                        if target.is_some() {
                            zoom_target = target;
                        }
                    });
                enabled
            });
            std::mem::swap(&mut items_selected, &mut window.config.items_selected);

            if let Some((item_loc, interval)) = zoom_target {
                let interval = match cx.item_link_mode {
                    // In Zoom mode, put the item in the center of the view
                    // interval with a small amount of padding on either side.
                    ItemLinkNavigationMode::Zoom => interval.grow(interval.duration_ns() / 20),
                    // In Pan mode, maintain the current window size but shift
                    // the center to place the item in the middle of it.
                    ItemLinkNavigationMode::Pan => cx
                        .view_interval
                        .translate(interval.center().0 - cx.view_interval.center().0),
                };
                ProfApp::zoom(cx, interval);
                window.expand_slot(&item_loc.entry_id);
                window.config.scroll_to_item(item_loc);
            }
        }

        Self::keyboard(ctx, cx, windows);

        // Keep repainting as long as we have outstanding requests.
        if !pending_data_sources.is_empty()
            || windows
                .iter()
                .any(|w| w.config.data_source.outstanding_requests() > 0)
        {
            ctx.request_repaint_after(Duration::from_millis(50));
        }
    }
}

trait UiExtra {
    fn subheading(&mut self, text: impl Into<egui::RichText>, cx: &Context) -> egui::Response;
    fn show_tooltip(
        &mut self,
        id_salt: impl core::hash::Hash,
        rect: &Rect,
        text: impl Into<egui::WidgetText>,
    );
    fn show_tooltip_at(
        &mut self,
        id_salt: impl core::hash::Hash,
        suggested_position: Pos2,
        text: impl Into<egui::WidgetText>,
    );
    fn show_tooltip_ui(
        &mut self,
        id_salt: impl core::hash::Hash,
        rect: &Rect,
        add_contents: impl FnOnce(&mut egui::Ui),
    );
    fn rect_hover_pos(&self, rect: Rect) -> Option<Pos2>;
}

impl UiExtra for egui::Ui {
    fn subheading(&mut self, text: impl Into<egui::RichText>, cx: &Context) -> egui::Response {
        self.add(egui::Label::new(
            text.into().heading().size(cx.subheading_size),
        ))
    }

    /// This is a method for showing a fast, very responsive
    /// tooltip. The standard hover methods force a delay (presumably
    /// to confirm the mouse has stopped), this bypasses that. Best
    /// used in situations where the user might quickly skim over the
    /// content (e.g., utilization plots).
    fn show_tooltip(
        &mut self,
        id_salt: impl core::hash::Hash,
        rect: &Rect,
        text: impl Into<egui::WidgetText>,
    ) {
        self.show_tooltip_ui(id_salt, rect, |ui| {
            ui.add(egui::Label::new(text).wrap_mode(egui::TextWrapMode::Extend));
        });
    }
    fn show_tooltip_at(
        &mut self,
        id_salt: impl core::hash::Hash,
        suggested_position: Pos2,
        text: impl Into<egui::WidgetText>,
    ) {
        egui::containers::show_tooltip_at(
            self.ctx(),
            self.layer_id(),
            self.auto_id_with(id_salt),
            suggested_position,
            |ui| {
                ui.add(egui::Label::new(text).wrap_mode(egui::TextWrapMode::Extend));
            },
        );
    }
    fn show_tooltip_ui(
        &mut self,
        id_salt: impl core::hash::Hash,
        rect: &Rect,
        add_contents: impl FnOnce(&mut egui::Ui),
    ) {
        egui::containers::show_tooltip_for(
            self.ctx(),
            self.layer_id(),
            self.auto_id_with(id_salt),
            rect,
            add_contents,
        );
    }

    /// Starting with egui 0.26, ui.allocate_rect() never returns interactions
    /// for multiple overlapping rectangles at once. This method is required to
    /// interact for more complex widgets where we want to e.g., hover one
    /// widget while dragging another widget (where those widgets overlap).
    fn rect_hover_pos(&self, rect: Rect) -> Option<Pos2> {
        if self.rect_contains_pointer(rect) {
            self.input(|i| i.pointer.hover_pos())
        } else {
            None
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_locator(data_sources: &[Box<dyn DeferredDataSource>]) -> String {
    let all_locators = data_sources
        .iter()
        .flat_map(|x| x.fetch_description().source_locator)
        .collect::<Vec<_>>();

    let unique_locators = all_locators.into_iter().unique().collect_vec();

    match &unique_locators[..] {
        [] => "No data source".to_string(),
        [x] => x.to_string(),
        [x, ..] => format!("{} and {} other sources", x, unique_locators.len() - 1),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn start(data_sources: Vec<Box<dyn DeferredDataSource>>) {
    env_logger::try_init().unwrap_or(()); // Log to stderr (if you run with `RUST_LOG=debug`).

    // IMPORTANT: This will be used as the directory name for the storage
    // location for the persisted app.ron configuration. eframe is not good
    // about sanitizing these directory names, so it is VERY IMPORTANT that
    // this be a short, predictable name without weird characters in it.
    let app_name = "Legion Prof";

    // This is what will be displayed as the window's actual title.
    let locator = format!("{} - {}", get_locator(&data_sources), app_name);

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_title(locator),
        ..Default::default()
    };
    eframe::run_native(
        app_name,
        native_options,
        Box::new(|cc| Ok(Box::new(ProfApp::new(cc, data_sources)))),
    )
    .expect("failed to start eframe");
}

#[cfg(target_arch = "wasm32")]
pub fn start(data_sources: Vec<Box<dyn DeferredDataSource>>) {
    use eframe::wasm_bindgen::JsCast as _;

    // Redirect `log` message to `console.log` and friends:
    eframe::WebLogger::init(log::LevelFilter::Debug).ok();

    let web_options = eframe::WebOptions::default();

    wasm_bindgen_futures::spawn_local(async {
        let document = web_sys::window()
            .expect("No window")
            .document()
            .expect("No document");

        let canvas = document
            .get_element_by_id("the_canvas_id")
            .expect("Failed to find the_canvas_id")
            .dyn_into::<web_sys::HtmlCanvasElement>()
            .expect("the_canvas_id was not a HtmlCanvasElement");

        let start_result = eframe::WebRunner::new()
            .start(
                canvas,
                web_options,
                Box::new(|cc| Ok(Box::new(ProfApp::new(cc, data_sources)))),
            )
            .await;

        // Remove the loading text and spinner:
        if let Some(loading_text) = document.get_element_by_id("loading_text") {
            match start_result {
                Ok(_) => {
                    loading_text.remove();
                }
                Err(e) => {
                    loading_text.set_inner_html(
                        "<p> The app has crashed. See the developer console for details. </p>",
                    );
                    panic!("Failed to start eframe: {e:?}");
                }
            }
        }
    });
}
