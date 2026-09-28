#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use venueflow::VenueFlowApp;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(_instance) = single_instance::acquire()? else {
        return Ok(());
    };
    venueflow::init_diagnostics();

    let endpoint = venueflow::default_control_endpoint();
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_icon(eframe::icon_data::from_png_bytes(include_bytes!(
                "../assets/venue.png"
            ))?)
            .with_title("VenueFlow — Venue Control Workstation")
            .with_decorations(false)
            .with_maximized(true)
            .with_inner_size([1_680.0, 1_000.0])
            .with_min_inner_size([1_100.0, 700.0]),
        renderer: eframe::Renderer::Wgpu,
        // Saved borderless geometry can be smaller than the viewport minimum after
        // remote-display changes. Start maximized; workspace/preferences still persist.
        persist_window: false,
        // eframe reads old geometry even when persist_window is false. This hook
        // runs after that restore, before the native window is created.
        window_builder: Some(Box::new(startup_viewport)),
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
    .map_err(Into::into)
}

mod single_instance;

fn startup_viewport(viewport: egui::ViewportBuilder) -> egui::ViewportBuilder {
    viewport
        .with_inner_size([1_680.0, 1_000.0])
        .with_min_inner_size([1_100.0, 700.0])
        .with_maximized(true)
}

#[cfg(test)]
mod tests {
    #[test]
    fn startup_overrides_corrupt_restored_window_geometry() {
        let viewport = super::startup_viewport(
            super::egui::ViewportBuilder::default()
                .with_inner_size([160.0, 64.0])
                .with_maximized(false),
        );
        assert_eq!(viewport.inner_size, Some(super::egui::vec2(1680.0, 1000.0)));
        assert_eq!(viewport.maximized, Some(true));
    }
}
