mod app;
mod client;
mod model;
mod theme;
mod ui;
mod workspace;

pub use app::VenueFlowApp;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast as _, prelude::*};

/// Starts the same VenueFlow application in an HTML canvas. The Web build is a control-plane
/// client only and has no exchange, credential, database, or artifact access.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub async fn start_web() -> Result<(), JsValue> {
    let window = web_sys::window().ok_or_else(|| JsValue::from_str("window is unavailable"))?;
    let document = window
        .document()
        .ok_or_else(|| JsValue::from_str("document is unavailable"))?;
    let canvas = document
        .get_element_by_id("venueflow-canvas")
        .ok_or_else(|| JsValue::from_str("venueflow-canvas is missing"))?
        .dyn_into::<web_sys::HtmlCanvasElement>()?;
    eframe::WebRunner::new()
        .start(
            canvas,
            eframe::WebOptions::default(),
            Box::new(|creation_context| {
                Ok(Box::new(VenueFlowApp::new(creation_context, String::new())))
            }),
        )
        .await
}
