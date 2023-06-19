use serde::Deserialize;

use crate::data::{EntryID, EntryIDSlug, SlugParseError, TileID, TileIDSlug};

#[derive(Debug, Clone, Deserialize)]
pub struct TileRequestPath {
    pub entry_id: String,
    pub tile_id: String,
}

#[derive(Debug, Clone)]
pub struct TileRequest {
    pub entry_id: EntryID,
    pub tile_id: TileID,
}

#[derive(Debug, Clone)]
pub struct TileRequestRef<'a> {
    pub entry_id: &'a EntryID,
    pub tile_id: TileID,
}

impl TileRequestPath {
    pub fn parse(&self) -> Result<TileRequest, SlugParseError> {
        Ok(TileRequest {
            entry_id: EntryID::from_slug(&self.entry_id)?,
            tile_id: TileID::from_slug(&self.tile_id)?,
        })
    }
}

impl<'a> TileRequestRef<'a> {
    pub fn to_slug(&self) -> String {
        format!(
            "{}/{}",
            EntryIDSlug(self.entry_id),
            TileIDSlug(self.tile_id)
        )
    }
}
