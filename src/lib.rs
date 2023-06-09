#![warn(clippy::all, rust_2018_idioms)]

pub mod app;
pub mod data;
pub mod deferred_data;
pub mod http;
#[cfg(target_arch = "wasm32")]
pub mod logging;
#[cfg(not(target_arch = "wasm32"))]
pub mod parallel_data;
pub mod search;
pub mod timestamp;
