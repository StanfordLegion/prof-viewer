#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use legion_prof_viewer::deferred_data::DeferredDataSource;
#[cfg(not(target_arch = "wasm32"))]
use legion_prof_viewer::file_data::FileDataSource;
use legion_prof_viewer::http::client::HTTPClientDataSource;
#[cfg(not(target_arch = "wasm32"))]
use legion_prof_viewer::parallel_data::ParallelDeferredDataSource;

use url::Url;

fn http_ds(url: Url) -> Box<dyn DeferredDataSource> {
    Box::new(HTTPClientDataSource::new(url))
}

#[cfg(not(target_arch = "wasm32"))]
fn file_ds(path: impl AsRef<Path>) -> Box<dyn DeferredDataSource> {
    Box::new(ParallelDeferredDataSource::new(FileDataSource::new(path)))
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let ds: Vec<_> = std::env::args_os()
        .skip(1)
        .map(|arg| {
            arg.into_string()
                .map(|s| Url::parse(&s).map(http_ds).unwrap_or_else(|_| {
                    println!("The argument '{}' does not appear to be a valid URL. Attempting to open it as a local file...", &s);
                    file_ds(&s)
                }))
                .unwrap_or_else(file_ds)
        })
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
