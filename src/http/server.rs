use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{
    error, get, http, middleware,
    web::{self, Data},
    App, HttpServer, Responder, Result,
};

use crate::data::DataSource;
use crate::http::schema::TileRequestPath;

struct AppState {
    data_source: Box<dyn DataSource + Send + Sync + 'static>,
}

pub struct DataSourceHTTPServer {
    host: String,
    port: u16,
    state: AppState,
}

#[get("/info")]
async fn fetch_info(state: web::Data<AppState>) -> Result<impl Responder> {
    let result = state.data_source.fetch_info();
    Ok(web::Json(result))
}

#[get("/summary_tile/{entry_id}/{tile_id}")]
async fn fetch_summary_tile(
    req: web::Path<TileRequestPath>,
    state: web::Data<AppState>,
) -> Result<impl Responder> {
    let req = req
        .parse()
        .map_err(|e| error::ErrorBadRequest(format!("bad request: {}", e)))?;
    let result = state
        .data_source
        .fetch_summary_tile(&req.entry_id, req.tile_id);
    Ok(web::Json(result))
}

#[get("/slot_tile/{entry_id}/{tile_id}")]
async fn fetch_slot_tile(
    req: web::Path<TileRequestPath>,
    state: web::Data<AppState>,
) -> Result<impl Responder> {
    let req = req
        .parse()
        .map_err(|e| error::ErrorBadRequest(format!("bad request: {}", e)))?;
    let result = state
        .data_source
        .fetch_slot_tile(&req.entry_id, req.tile_id);
    Ok(web::Json(result))
}

#[get("/slot_meta_tile/{entry_id}/{tile_id}")]
async fn fetch_slot_meta_tile(
    req: web::Path<TileRequestPath>,
    state: web::Data<AppState>,
) -> Result<impl Responder> {
    let req = req
        .parse()
        .map_err(|e| error::ErrorBadRequest(format!("bad request: {}", e)))?;
    let result = state
        .data_source
        .fetch_slot_meta_tile(&req.entry_id, req.tile_id);
    Ok(web::Json(result))
}

impl DataSourceHTTPServer {
    pub fn new(
        host: String,
        port: u16,
        data_source: Box<dyn DataSource + Send + Sync + 'static>,
    ) -> Self {
        Self {
            host,
            port,
            state: AppState { data_source },
        }
    }

    #[actix_web::main]
    pub async fn run(self) -> std::io::Result<()> {
        let state = Data::from(Arc::new(self.state));
        if std::env::var_os("RUST_LOG").is_none() {
            std::env::set_var("RUST_LOG", "info");
        }
        env_logger::init();
        HttpServer::new(move || {
            let cors = Cors::default()
                .send_wildcard()
                .allow_any_origin()
                .allowed_methods(vec!["GET", "POST"])
                .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
                .allowed_header(http::header::CONTENT_TYPE)
                .max_age(3600);
            App::new()
                .wrap(middleware::Logger::default())
                .wrap(middleware::Compress::default())
                .wrap(cors)
                .app_data(state.clone())
                .service(fetch_info)
                .service(fetch_summary_tile)
                .service(fetch_slot_tile)
                .service(fetch_slot_meta_tile)
        })
        .bind((self.host.as_str(), self.port))?
        .run()
        .await
    }
}
