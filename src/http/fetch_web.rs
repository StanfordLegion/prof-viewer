use reqwest::RequestBuilder;

use crate::http::fetch::DataSourceResponse;

/// Spawn an async task.
///
/// A wrapper around `wasm_bindgen_futures::spawn_local`.
/// Only available with the web backend.
pub fn spawn_future<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

pub fn fetch(
    request: RequestBuilder,
    on_done: Box<dyn FnOnce(Result<DataSourceResponse, String>) + Send>,
) {
    spawn_future(async move {
        let response = request
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"));
        let response = match response {
            Ok(r) => r
                .bytes()
                .await
                .map_err(|e| format!("unable to get bytes: {e}")),
            Err(e) => Err(e),
        };
        let response = response.map(|r| DataSourceResponse { body: r });

        on_done(response)
    });
}
