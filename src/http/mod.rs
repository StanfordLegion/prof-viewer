pub mod schema;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "client")]
pub mod fetch;
#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
pub mod fetch_native;
#[cfg(all(feature = "client", target_arch = "wasm32"))]
pub mod fetch_web;
#[cfg(feature = "client")]
pub mod url;
