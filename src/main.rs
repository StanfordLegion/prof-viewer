#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use legion_prof_viewer::deferred_data::DeferredDataSource;
use legion_prof_viewer::http::client::HTTPClientDataSource;

use url::Url;

fn http_ds(url: Url) -> Box<dyn DeferredDataSource> {
    Box::new(HTTPClientDataSource::new(url))
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let ds: Vec<_> = std::env::args()
        .skip(1)
        .map(|arg| http_ds(Url::parse(&arg).expect("unable to parse URL")))
        .collect();

    legion_prof_viewer::app::start(ds);
}

#[cfg(target_arch = "wasm32")]
fn main() {
    let loc: web_sys::Location = web_sys::window().unwrap().location();
    let href: String = loc.href().expect("unable to get window URL");
    let browser_url = Url::parse(&href).expect("unable to parse location URL");

    let ds: Vec<_> = browser_url
        .query_pairs()
        .filter(|(key, _)| key.starts_with("url"))
        .map(|(_, value)| http_ds(Url::parse(&value).expect("unable to parse query URL")))
        .collect();

    legion_prof_viewer::app::start(ds);
}
