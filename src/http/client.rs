use std::sync::{Arc, Mutex};

use log::info;

#[cfg(not(target_arch = "wasm32"))]
use reqwest::blocking::{Client, ClientBuilder};
#[cfg(target_arch = "wasm32")]
use reqwest::{Client, ClientBuilder};

use serde::Deserialize;

use url::Url;

use crate::data::{DataSourceInfo, EntryID, SlotMetaTile, SlotTile, SummaryTile, TileID};
use crate::deferred_data::DeferredDataSource;
use crate::http::fetch::{fetch, DataSourceResponse};
use crate::http::schema::TileRequestRef;

pub struct HTTPClientDataSource {
    pub baseurl: Url,
    pub client: Client,
    infos: Arc<Mutex<Vec<DataSourceInfo>>>,
    summary_tiles: Arc<Mutex<Vec<SummaryTile>>>,
    slot_tiles: Arc<Mutex<Vec<SlotTile>>>,
    slot_meta_tiles: Arc<Mutex<Vec<SlotMetaTile>>>,
}

impl HTTPClientDataSource {
    pub fn new(baseurl: Url) -> Self {
        Self {
            baseurl,
            client: ClientBuilder::new().build().unwrap(),
            infos: Arc::new(Mutex::new(Vec::new())),
            summary_tiles: Arc::new(Mutex::new(Vec::new())),
            slot_tiles: Arc::new(Mutex::new(Vec::new())),
            slot_meta_tiles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn request<T>(&mut self, url: Url, container: Arc<Mutex<Vec<T>>>)
    where
        T: 'static + Sync + Send + for<'a> Deserialize<'a>,
    {
        info!("fetch: {}", url);
        let request = self
            .client
            .get(url)
            .header("Accept", "*/*")
            .header("Content-Type", "javascript/json;");
        fetch(
            request,
            move |response: Result<DataSourceResponse, String>| {
                let result = serde_json::from_str::<T>(&response.unwrap().body).unwrap();
                container.lock().unwrap().push(result);
            },
        );
    }
}

impl DeferredDataSource for HTTPClientDataSource {
    fn fetch_info(&mut self) {
        let url = self.baseurl.join("info").expect("invalid baseurl");
        self.request::<DataSourceInfo>(url, self.infos.clone());
    }

    fn get_infos(&mut self) -> Vec<DataSourceInfo> {
        std::mem::take(&mut self.infos.lock().unwrap())
    }

    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID) {
        let req = TileRequestRef { entry_id, tile_id };
        let url = self
            .baseurl
            .join("summary_tile/")
            .and_then(|u| u.join(&req.to_slug()))
            .expect("invalid baseurl");
        self.request::<SummaryTile>(url, self.summary_tiles.clone());
    }

    fn get_summary_tiles(&mut self) -> Vec<SummaryTile> {
        std::mem::take(&mut self.summary_tiles.lock().unwrap())
    }

    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID) {
        let req = TileRequestRef { entry_id, tile_id };
        let url = self
            .baseurl
            .join("slot_tile/")
            .and_then(|u| u.join(&req.to_slug()))
            .expect("invalid baseurl");
        self.request::<SlotTile>(url, self.slot_tiles.clone());
    }

    fn get_slot_tiles(&mut self) -> Vec<SlotTile> {
        std::mem::take(&mut self.slot_tiles.lock().unwrap())
    }

    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID) {
        let req = TileRequestRef { entry_id, tile_id };
        let url = self
            .baseurl
            .join("slot_meta_tile/")
            .and_then(|u| u.join(&req.to_slug()))
            .expect("invalid baseurl");
        self.request::<SlotMetaTile>(url, self.slot_meta_tiles.clone());
    }

    fn get_slot_meta_tiles(&mut self) -> Vec<SlotMetaTile> {
        std::mem::take(&mut self.slot_meta_tiles.lock().unwrap())
    }
}
