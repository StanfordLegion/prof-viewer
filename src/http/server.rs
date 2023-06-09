use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{
    http, middleware,
    web::{self, Data},
    App, HttpServer, Responder, Result,
};

use crate::data::DataSource;
use crate::http::schema::TileRequest;

pub struct AppState {
    pub data_source: Box<dyn DataSource + Send + Sync + 'static>,
}

pub struct DataSourceHTTPServer {
    pub host: String,
    pub port: u16,
    pub state: AppState,
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

    async fn fetch_info(state: web::Data<AppState>) -> Result<impl Responder> {
        let result = state.data_source.fetch_info();
        Ok(web::Json(result))
    }

    async fn fetch_tile_set(state: web::Data<AppState>) -> Result<impl Responder> {
        let result = state.data_source.fetch_tile_set();
        Ok(web::Json(result))
    }

    async fn fetch_summary_tile(
        req: web::Json<TileRequest>,
        state: web::Data<AppState>,
    ) -> Result<impl Responder> {
        let result = state
            .data_source
            .fetch_summary_tile(&req.entry_id, req.tile_id);
        Ok(web::Json(result))
    }

    async fn fetch_slot_tile(
        req: web::Json<TileRequest>,
        state: web::Data<AppState>,
    ) -> Result<impl Responder> {
        let result = state
            .data_source
            .fetch_slot_tile(&req.entry_id, req.tile_id);
        Ok(web::Json(result))
    }

    async fn fetch_slot_meta_tile(
        req: web::Json<TileRequest>,
        state: web::Data<AppState>,
    ) -> Result<impl Responder> {
        let result = state
            .data_source
            .fetch_slot_meta_tile(&req.entry_id, req.tile_id);
        Ok(web::Json(result))
    }

    #[actix_web::main]
    pub async fn create_server(self) -> std::io::Result<()> {
        let state = Data::from(Arc::new(self.state));
        // FIXME (Elliott): pick a different default logging level?
        std::env::set_var("RUST_LOG", "debug");
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
                .route("/info", web::post().to(Self::fetch_info))
                .route("/tile_set", web::post().to(Self::fetch_tile_set))
                .route("/summary_tile", web::post().to(Self::fetch_summary_tile))
                .route("/slot_tile", web::post().to(Self::fetch_slot_tile))
                .route(
                    "/slot_meta_tile",
                    web::post().to(Self::fetch_slot_meta_tile),
                )
        })
        .bind((self.host.as_str(), self.port))?
        .run()
        .await
    }
}
