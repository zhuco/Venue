pub(crate) fn show_mark(ui: &mut egui::Ui) {
    let id = egui::Id::new("venue.brand.texture");
    let cached = ui
        .ctx()
        .data(|data| data.get_temp::<egui::TextureHandle>(id));
    let texture = cached.unwrap_or_else(|| {
        let texture = ui.ctx().load_texture(
            "VENUE",
            egui::ColorImage::from_rgba_unmultiplied(
                [64, 64],
                include_bytes!("../assets/venue-64.rgba"),
            ),
            egui::TextureOptions::LINEAR,
        );
        ui.ctx()
            .data_mut(|data| data.insert_temp(id, texture.clone()));
        texture
    });
    ui.image((texture.id(), egui::vec2(26.0, 26.0)))
        .on_hover_text("VENUE · Markets move further here");
}
