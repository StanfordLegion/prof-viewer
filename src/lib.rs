#![warn(clippy::all, rust_2018_idioms)]

pub mod app;
#[cfg(not(target_arch = "wasm32"))]
pub mod archive_data;
#[cfg(feature = "duckdb")]
mod arrow_data;
pub mod data;
pub mod deferred_data;
#[cfg(feature = "duckdb")]
pub mod duckdb_data;
#[cfg(not(target_arch = "wasm32"))]
pub mod file_data;
pub mod http;
pub mod merge_data;
#[cfg(feature = "nvtxw")]
pub mod nvtxw;
#[cfg(not(target_arch = "wasm32"))]
pub mod parallel_data;
pub mod timestamp;
