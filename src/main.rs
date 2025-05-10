#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use legion_prof_viewer::http::client::HTTPClientDataSource;

use url::Url;

const DEFAULT_URL: &str = "http://127.0.0.1:8080";

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let url = Url::parse(std::env::args().nth(1).as_deref().unwrap_or(DEFAULT_URL))
        .expect("unable to parse URL");

    legion_prof_viewer::app::start(vec![Box::new(HTTPClientDataSource::new(url))]);
}

#[cfg(target_arch = "wasm32")]
fn main() {
    let loc: web_sys::Location = web_sys::window().unwrap().location();
    let href: String = loc.href().expect("unable to get window URL");
    let browser_url = Url::parse(&href).expect("unable to parse location URL");

    let url = Url::parse(
        browser_url
            .query_pairs()
            .find(|(key, _)| key == "url")
            .map(|(_, value)| value)
            .as_deref()
            .unwrap_or(DEFAULT_URL),
    )
    .expect("unable to parse query URL");

    legion_prof_viewer::app::start(vec![Box::new(HTTPClientDataSource::new(url))]);
}
