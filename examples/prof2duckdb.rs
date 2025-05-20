#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

#[cfg(feature = "duckdb")]
use std::ffi::OsString;
#[cfg(feature = "duckdb")]
use std::path::Path;

#[cfg(feature = "duckdb")]
use clap::Parser;

#[cfg(feature = "duckdb")]
use legion_prof_viewer::{
    deferred_data::DeferredDataSource, duckdb_data::DataSourceDuckDBWriter,
    file_data::FileDataSource, http::client::HTTPClientDataSource,
    parallel_data::ParallelDeferredDataSource,
};

#[cfg(feature = "duckdb")]
use url::Url;

#[cfg(feature = "duckdb")]
#[derive(Debug, Clone, Parser)]
struct Cli {
    #[arg(required = true, help = "URL or path to convert")]
    input: OsString,

    #[arg(
        short,
        long,
        default_value = "legion_prof.duckdb",
        help = "output database pathname"
    )]
    output: OsString,

    #[arg(short, long, help = "overwrite output file if it exists")]
    force: bool,
}

#[cfg(feature = "duckdb")]
fn main() {
    fn http_ds(url: Url) -> Box<dyn DeferredDataSource> {
        Box::new(HTTPClientDataSource::new(url))
    }

    fn file_ds(path: impl AsRef<Path>) -> Box<dyn DeferredDataSource> {
        Box::new(ParallelDeferredDataSource::new(FileDataSource::new(path)))
    }

    let args = Cli::parse();

    let ds = args.input.into_string()
                .map(|s| Url::parse(&s).map(http_ds).unwrap_or_else(|_| {
                    println!("The argument '{}' does not appear to be a valid URL. Attempting to open it as a local file...", &s);
                    file_ds(&s)
                }))
                .unwrap_or_else(file_ds);

    DataSourceDuckDBWriter::new(ds, args.output, args.force)
        .write()
        .expect("writing DuckDB database failed");
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    panic!("Rebuild with --features=duckdb");
}
