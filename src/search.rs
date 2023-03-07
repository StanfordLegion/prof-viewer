use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::data::{EntryID, ItemMeta, ItemUID, TileID};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SelectedState {
    pub search: String,

    pattern_count: usize,

    #[serde(skip)]
    search_automaton: Option<AhoCorasick>, // does not implement default

    last_built_string: String,

    pub num_matches: u64,

    pub highlighted_items: BTreeMap<EntryID, Vec<SelectedItem>>,

    pub entries_highlighted: BTreeSet<EntryID>,

    pub selected: Option<SelectedItem>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SelectedItem {
    pub entry_id: EntryID,

    pub tile_id: TileID,

    pub item_uid: ItemUID,

    pub meta: ItemMeta,

    pub row: usize,

    pub index: usize,
}

impl SelectedState {
    pub fn add_highlighted_item(&mut self, selected_item: SelectedItem) {
        let entry_id = selected_item.entry_id.clone();
        let selected_items = self.highlighted_items.entry(entry_id.clone()).or_default();
        selected_items.push(selected_item);
        let mut entry = EntryID::root();

        let mut i = 0;
        while i < entry_id.level() {
            if let Some(depth) = entry_id.slot_index(i) {
                entry = entry.child(depth);
                self.entries_highlighted.insert(entry.clone());
            }
            i += 1;
        }
    }

    pub fn clear_highlighted_items(&mut self) {
        self.highlighted_items.clear();
        self.entries_highlighted.clear();
        self.selected = None;
        self.num_matches = 0;
    }
    pub fn clear_search(&mut self) {
        self.search = String::new();
        self.clear_highlighted_items();
    }

    pub fn build_search_automaton(&mut self) -> &AhoCorasick {
        if self.search != self.last_built_string || self.search_automaton.is_none() {
            let patterns: Vec<&str> = self.search.split(' ').filter(|x| x != &"").collect();
            self.pattern_count = patterns.len();
            let ac = AhoCorasickBuilder::new()
                .ascii_case_insensitive(true)
                .build(patterns);
            self.search_automaton = Some(ac);
            self.last_built_string = self.search.clone();
        }
        self.search_automaton.as_ref().unwrap()
    }

    pub fn search(&mut self, text: &str) -> bool {
        let ac = self.build_search_automaton();
        let lowercase_text = text.to_lowercase();
        let matches = ac.find_iter(&lowercase_text);
        matches.count() == self.pattern_count
    }
}
