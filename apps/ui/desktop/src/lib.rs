mod account_center;
mod account_client;
mod app;
mod chart;
mod chart_settings;
mod chart_trading;
mod chart_view;
mod client;
mod copy_relation_view;
mod custom_indicator;
mod diagnostics;
mod execution_view;
mod grid_view;
mod i18n;
mod leader_bot_view;
#[cfg(not(target_arch = "wasm32"))]
mod market;
#[cfg(not(target_arch = "wasm32"))]
mod market_client;
mod model;
mod order_book_view;
mod server_connection;
mod settings_panel;
mod symbol_picker;
mod terminal_feedback;
mod theme;
mod trade_dock;
mod trading;
mod ui;
mod workspace;

pub use app::VenueFlowApp;
pub use diagnostics::init_diagnostics;
pub use server_connection::default_control_endpoint;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast as _, prelude::*};

/// Starts the same VenueFlow application in an HTML canvas. The Web build is a control-plane
/// client only. Credential entry is transient account administration transport;
/// this client has no private exchange connection, database, or artifact access.
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
