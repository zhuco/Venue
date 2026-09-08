fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::args()
        .nth(1)
        .ok_or("provide the WASM bundle directory")?;
    venueflow::serve_web_preview(std::path::Path::new(&directory))
}
