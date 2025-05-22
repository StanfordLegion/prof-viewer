use std::sync::{Arc, Mutex};

use bytes::Buf;

use log::info;

#[cfg(not(target_arch = "wasm32"))]
use reqwest::blocking::{Client, ClientBuilder};
#[cfg(target_arch = "wasm32")]
use reqwest::{Client, ClientBuilder};

use serde::Deserialize;

use url::Url;

use crate::data::{
    self, DataSourceDescription, DataSourceInfo, EntryID, SlotMetaTile, SlotTile, SummaryTile,
    TileID,
};
use crate::deferred_data::{
    DeferredDataSource, SlotMetaTileResponse, SlotTileResponse, SummaryTileResponse, TileRequest,
    TileResponse,
};
use crate::http::fetch::{DataSourceResponse, fetch};
use crate::http::schema::TileRequestRef;
use crate::http::url::ensure_directory;

pub struct HTTPClientDataSource {
    pub baseurl: Url,
    pub client: Client,
    infos: Arc<Mutex<Vec<data::Result<DataSourceInfo>>>>,
    summary_tiles: Arc<Mutex<Vec<SummaryTileResponse>>>,
    slot_tiles: Arc<Mutex<Vec<SlotTileResponse>>>,
    slot_meta_tiles: Arc<Mutex<Vec<SlotMetaTileResponse>>>,
}

fn decode_zstd<R>(f: R) -> Result<zstd::Decoder<'static, std::io::BufReader<R>>, String>
where
    R: std::io::Read,
{
    zstd::Decoder::new(f).map_err(|e| format!("zstd decode failed: {}", e))
}

fn decode_ciborium<R, T>(mut f: R) -> data::Result<T>
where
    R: std::io::Read,
    T: 'static + Sync + Send + for<'a> Deserialize<'a>,
{
    // To support older profiles we attempt to decode twice,
    // with/without the Result wrapper.

    let mut bytes = Vec::new();
    f.read_to_end(&mut bytes)
        .map_err(|e| format!("zstd decode failed: {}", e))?;

    match ciborium::from_reader::<data::Result<T>, &[u8]>(&bytes) {
        Ok(x) => x,
        Err(e) => {
            // See if we can decode it the other way, otherwise return the
            // original error.
            ciborium::from_reader::<T, &[u8]>(&bytes)
                .map_err(|_| format!("ciborium decode failed: {}", e))
        }
    }
}

impl HTTPClientDataSource {
    pub fn new(baseurl: Url) -> Self {
        Self {
            baseurl: ensure_directory(&baseurl),
            client: ClientBuilder::new().build().unwrap(),
            infos: Arc::new(Mutex::new(Vec::new())),
            summary_tiles: Arc::new(Mutex::new(Vec::new())),
            slot_tiles: Arc::new(Mutex::new(Vec::new())),
            slot_meta_tiles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn request<T>(&mut self, url: Url, container: Arc<Mutex<Vec<data::Result<T>>>>)
    where
        T: 'static + Sync + Send + for<'a> Deserialize<'a>,
    {
        info!("fetch: {}", url);
        let request = self
            .client
            .get(url)
            .header("Accept", "*/*")
            .header("Content-Type", "application/octet-stream;");
        fetch(
            request,
            move |response: Result<DataSourceResponse, String>| {
                let result = response
                    .and_then(|r| decode_zstd(r.body.reader()))
                    .and_then(decode_ciborium);
                container.lock().unwrap().push(result);
            },
        );
    }

    fn request_extra<T>(
        &mut self,
        url: Url,
        container: Arc<Mutex<Vec<TileResponse<T>>>>,
        extra: TileRequest,
    ) where
        T: 'static + Sync + Send + for<'a> Deserialize<'a>,
    {
        info!("fetch: {}", url);
        let request = self
            .client
            .get(url)
            .header("Accept", "*/*")
            .header("Content-Type", "application/octet-stream;");
        fetch(
            request,
            move |response: Result<DataSourceResponse, String>| {
                let result = response
                    .and_then(|r| decode_zstd(r.body.reader()))
                    .and_then(decode_ciborium);
                container.lock().unwrap().push((result, extra));
            },
        );
    }
}

impl DeferredDataSource for HTTPClientDataSource {
    fn fetch_description(&self) -> DataSourceDescription {
        DataSourceDescription {
            source_locator: vec![self.baseurl.to_string()],
        }
    }

    fn fetch_info(&mut self) {
        let url = self.baseurl.join("info").expect("invalid baseurl");
        self.request::<DataSourceInfo>(url, self.infos.clone());
    }

    fn get_infos(&mut self) -> Vec<data::Result<DataSourceInfo>> {
        std::mem::take(&mut self.infos.lock().unwrap())
    }

    fn fetch_summary_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        let req = TileRequestRef { entry_id, tile_id };
        let mut url = self
            .baseurl
            .join("summary_tile/")
            .and_then(|u| u.join(&req.to_slug()))
            .expect("invalid baseurl");
        url.set_query(Some(&format!("full={}", full)));
        let extra = TileRequest {
            entry_id: entry_id.clone(),
            tile_id,
            full,
        };
        self.request_extra::<SummaryTile>(url, self.summary_tiles.clone(), extra);
    }

    fn get_summary_tiles(&mut self) -> Vec<SummaryTileResponse> {
        std::mem::take(&mut self.summary_tiles.lock().unwrap())
    }

    fn fetch_slot_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        let req = TileRequestRef { entry_id, tile_id };
        let mut url = self
            .baseurl
            .join("slot_tile/")
            .and_then(|u| u.join(&req.to_slug()))
            .expect("invalid baseurl");
        url.set_query(Some(&format!("full={}", full)));
        let extra = TileRequest {
            entry_id: entry_id.clone(),
            tile_id,
            full,
        };
        self.request_extra::<SlotTile>(url, self.slot_tiles.clone(), extra);
    }

    fn get_slot_tiles(&mut self) -> Vec<SlotTileResponse> {
        std::mem::take(&mut self.slot_tiles.lock().unwrap())
    }

    fn fetch_slot_meta_tile(&mut self, entry_id: &EntryID, tile_id: TileID, full: bool) {
        let req = TileRequestRef { entry_id, tile_id };
        let mut url = self
            .baseurl
            .join("slot_meta_tile/")
            .and_then(|u| u.join(&req.to_slug()))
            .expect("invalid baseurl");
        url.set_query(Some(&format!("full={}", full)));
        let extra = TileRequest {
            entry_id: entry_id.clone(),
            tile_id,
            full,
        };
        self.request_extra::<SlotMetaTile>(url, self.slot_meta_tiles.clone(), extra);
    }

    fn get_slot_meta_tiles(&mut self) -> Vec<SlotMetaTileResponse> {
        std::mem::take(&mut self.slot_meta_tiles.lock().unwrap())
    }
}
