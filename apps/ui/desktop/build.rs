use std::{env, error::Error, fs, path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=assets/venue.ico");
    if env::var("CARGO_CFG_TARGET_OS")? != "windows" {
        return Ok(());
    }
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let output = PathBuf::from(env::var("OUT_DIR")?);
    let resource = output.join("venue.rc");
    fs::write(
        &resource,
        format!(
            "1 ICON \"{}\"\n",
            root.join("assets/venue.ico")
                .display()
                .to_string()
                .replace('\\', "/")
        ),
    )?;
    let compiled = output.join("venue.res");
    let status = if env::var("CARGO_CFG_TARGET_ENV")? == "msvc" {
        let compiler = find_rc()?;
        Command::new(compiler)
            .arg("/nologo")
            .arg("/fo")
            .arg(&compiled)
            .arg(&resource)
            .status()?
    } else {
        Command::new("windres")
            .arg(&resource)
            .arg("-O")
            .arg("coff")
            .arg("-o")
            .arg(&compiled)
            .status()?
    };
    if !status.success() {
        return Err("VENUE icon resource compilation failed".into());
    }
    println!("cargo:rustc-link-arg-bin=venueflow={}", compiled.display());
    Ok(())
}

fn find_rc() -> Result<PathBuf, Box<dyn Error>> {
    for directory in env::split_paths(&env::var_os("PATH").unwrap_or_default()) {
        let path = directory.join("rc.exe");
        if path.is_file() {
            return Ok(path);
        }
    }
    let kits = PathBuf::from(env::var_os("ProgramFiles(x86)").ok_or("Windows SDK is required")?)
        .join("Windows Kits/10/bin");
    let mut versions = fs::read_dir(kits)?.collect::<Result<Vec<_>, _>>()?;
    versions.sort_by_key(|entry| entry.file_name());
    for version in versions.into_iter().rev() {
        let path = version.path().join("x64/rc.exe");
        if path.is_file() {
            return Ok(path);
        }
    }
    Err("Windows SDK rc.exe is required to embed the VENUE application icon".into())
}
