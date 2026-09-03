#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use venueflow::VenueFlowApp;

fn main() -> eframe::Result {
    tracing_subscriber::fmt()
        .with_target(false)
        .without_time()
        .init();

    let endpoint =
        std::env::var("VENUE_CONTROL_URL").unwrap_or_else(|_| "http://127.0.0.1:39180".to_owned());
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("VenueFlow — Venue Control Workstation")
            .with_decorations(false)
            .with_maximized(true)
            .with_inner_size([1_680.0, 1_000.0])
            .with_min_inner_size([1_100.0, 700.0]),
        renderer: eframe::Renderer::Wgpu,
        persist_window: true,
        ..Default::default()
    };

    eframe::run_native(
        "VenueFlow",
        native_options,
        Box::new(move |creation_context| {
            Ok(Box::new(VenueFlowApp::new(
                creation_context,
                endpoint.clone(),
            )))
        }),
    )
}
