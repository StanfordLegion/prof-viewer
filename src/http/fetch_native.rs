use reqwest::blocking::RequestBuilder;

use crate::http::fetch::DataSourceResponse;

pub fn fetch(
    request: RequestBuilder,
    on_done: Box<dyn FnOnce(Result<DataSourceResponse, String>) + Send>,
) {
    rayon::spawn(move || {
        let response = request
            .send()
            .map_err(|e| format!("request failed: {e}"))
            .and_then(|r| r.bytes().map_err(|e| format!("unable to get bytes: {e}")))
            .map(|r| DataSourceResponse { body: r });

        on_done(response)
    });
}
